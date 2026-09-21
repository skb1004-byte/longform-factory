# -*- coding: utf-8 -*-
"""Video asset routing: AI generation or stock footage, driven by style preset."""
from __future__ import annotations
import asyncio
import logging

from pipeline import cancel
from pipeline.cancel import JobCancelled
import subprocess
import re
import os
from pathlib import Path
from typing import List, Optional, Dict, Any

from config import JOBS_DIR, LOCAL_SDXL_ENABLED
from pipeline.asset_utils import download_video, _expand_domain_keyword
from pipeline.style_presets import resolve_style, get_preset, AI_STYLES
from pipeline.stock_search import (
    content_tokens, get_topic_terms, get_job_topic,
    list_candidates, rerank_with_llm,
)
from pipeline.prompt_builder import build_negative
from pipeline.ai_image import (
    build_prompt,
    build_cartoon_prompt,           # backward compat export
    generate_ai_image_local,
    generate_ai_image_wavespeed,
    generate_ai_image_dalle,
    image_to_video_ai,
)
from pipeline.stock_search import (   # split to keep assets.py ≤300 lines
    get_pexels_videos,
    get_pixabay_videos,
    select_best_video,
    NEGATIVE_KEYWORDS,
)

logger = logging.getLogger(__name__)

# Number of distinct AI images to generate per scene (multi-image quality mode).
# Each sub-clip in the Ken Burns loop gets its own unique image → no visual repetition.
N_SUB_IMAGES = 4

# 서브이미지 동시 실행 시 자원별 동시성 캡.
# - WaveSpeed: 문서상 ~3req/s -> 동시 2개까지만 (429 방지)
# - 로컬 SDXL: GPU 1장이라 서버도 락을 걸지만, 클라이언트에서도 미리 캡을 걸어
#   불필요하게 오래 걸리는 HTTP 커넥션이 쌓이는 것을 막는다.
_WAVESPEED_SEM = asyncio.Semaphore(2)
_LOCAL_SDXL_SEM = asyncio.Semaphore(1)

# Composition/angle hints injected per sub-image to maximise visual variety.
_SUB_VIEW_HINTS = [
    "wide establishing shot, expansive full scene view",
    "close-up macro detail, intimate foreground focus",
    "medium shot, balanced mid-range composition",
    "dynamic cinematic angle, dramatic perspective framing",
]

async def search_and_download_assets(
    job_id: str,
    scenes: List,
    image_mode: str = "stock",
    style: str = "",
) -> List:
    """Download assets for each scene.

    Style routing:
      cartoon / cinematic / watercolor / anime / minimal / infographic
        → WaveSpeed (primary) → DALL-E → Pexels stock (fallback)
      stock / news
        → Pexels (primary) → Pixabay (fallback)

    image_mode kept for backward compat; style takes priority.
    """
    resolved = resolve_style(image_mode, style)
    preset = get_preset(resolved)
    fallback_chain = preset["fallback_chain"]
    is_ai = resolved in AI_STYLES

    logger.info(f"[assets] style='{resolved}' source={preset['primary_source']} "
                f"chain={fallback_chain}")

    jobs_dir = JOBS_DIR / job_id
    assets_dir = jobs_dir / "assets"
    assets_dir.mkdir(parents=True, exist_ok=True)
    # 이전 잡의 장부가 남아 후보를 고갈시키지 않도록 매 잡마다 새로 시작한다.
    _USED_URL_REGISTRY.pop(str(assets_dir), None)

    for scene in scenes:
        cancel.check_active("소재 수집")
        # Multi-image reuse check (AI mode): all N sub-images must already exist
        if is_ai:
            sub_paths = [assets_dir / f"{scene.scene_id}_sub{i}.mp4" for i in range(N_SUB_IMAGES)]
            valid_subs = [p for p in sub_paths if p.exists() and p.stat().st_size > 4096]
            if len(valid_subs) == N_SUB_IMAGES:
                scene.asset_urls = [str(p) for p in valid_subs]
                scene.asset_url = scene.asset_urls[0]
                logger.info(f"[assets] reusing {N_SUB_IMAGES} sub-images: {scene.scene_id}")
                continue

        # Legacy single-asset reuse (stock mode or partial AI cache)
        existing = assets_dir / f"{scene.scene_id}_main.mp4"
        if not is_ai and existing.exists() and existing.stat().st_size > 4096:
            scene.asset_url = str(existing)
            logger.info(f"[assets] reusing {existing.name}")
            continue

        if is_ai:
            await _fetch_ai_asset(scene, assets_dir, resolved, preset, fallback_chain)
        else:
            await _fetch_stock_asset(scene, assets_dir, fallback_chain)

    return scenes


# ──────────────────────────────────────────────────────────────────────────────
# AI image path
# ──────────────────────────────────────────────────────────────────────────────

async def _empty_list() -> list:
    """체인에 없는 소스 자리를 채우는 빈 코루틴.

    예전 코드는 asyncio.coroutine(lambda: [])() 을 썼는데 이 API 는 Python 3.11 에서
    제거돼(현재 런타임 3.11.15) 스톡 재검색 경로가 AttributeError 로 죽었다.
    """
    return []


async def _fetch_ai_asset(scene, assets_dir: Path, style: str, preset: dict, chain: list) -> None:
    """Generate N_SUB_IMAGES distinct AI images per scene for maximum visual variety.

    Sub-images run CONCURRENTLY (asyncio.gather) instead of one at a time: each
    is routed through the same fallback chain (local GPU / cloud API / stock),
    but because the calls overlap, a cloud API request and a local-GPU request
    can be in flight at the same time instead of blocking each other. Cloud
    (WaveSpeed) and local (SDXL) each carry their own concurrency cap via
    module-level semaphores -- WaveSpeed to respect its rate limit, local SDXL
    because only one image can render on the GPU at once.

    Falls back to stock footage when all AI sources fail for a given sub-image.
    """
    # 장면별 금지어를 얹는다(예: 한국 소재면 기모노·한푸 차단).
    neg = build_negative(scene, style)
    size = preset.get("size_portrait", "768x1344")
    dur = max(scene.duration_seconds or 5.0, 3.0)
    base_prompt = build_prompt(scene, style)

    logger.info(
        f"[assets] AI multi-image start ({scene.scene_id}, n={N_SUB_IMAGES}, parallel): {base_prompt[:70]}"
    )

    tasks = [
        _generate_one_sub_image(scene, assets_dir, style, chain, base_prompt, sub_i, dur, size, neg)
        for sub_i in range(N_SUB_IMAGES)
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # return_exceptions=True 는 JobCancelled 까지 '결과'로 삼켜 버린다.
    # 그러면 사용자가 중지를 눌러도 여기서 조용히 무시되고 파이프라인이
    # 끝까지 간다. 취소만 골라서 다시 던진다.
    for _r in results:
        if isinstance(_r, JobCancelled):
            raise _r

    collected_urls: list[str] = []
    for sub_i, r in enumerate(results):
        if isinstance(r, Exception):
            logger.warning(f"[assets]   sub{sub_i} task error: {r}")
        elif r:
            collected_urls.append(r)

    if collected_urls:
        scene.asset_urls = collected_urls
        scene.asset_url = collected_urls[0]   # backward compat
        logger.info(f"[assets] {scene.scene_id}: {len(collected_urls)}/{N_SUB_IMAGES} sub-images ready")
    else:
        # Complete failure: try stock fallback for the whole scene
        logger.warning(f"[assets] {scene.scene_id}: all sub-images failed → stock fallback")
        await _fetch_stock_asset(scene, assets_dir, ["pexels", "pixabay"])

    # Final inter-scene delay
    await asyncio.sleep(2)


async def _generate_one_sub_image(
    scene, assets_dir: Path, style: str, chain: list,
    base_prompt: str, sub_i: int, dur: float, size: str, neg: str,
) -> Optional[str]:
    """Generate + animate exactly one sub-image. Returns its output path, or None on total failure.

    Designed to run as one of N concurrent asyncio tasks (see _fetch_ai_asset) --
    every side effect here is scoped to this sub-image's own files, so tasks
    never touch each other's state.
    """
    cancel.check_active(f"AI 이미지 생성 직전 {scene.scene_id}-sub{sub_i}")
    view_hint = _SUB_VIEW_HINTS[sub_i % len(_SUB_VIEW_HINTS)]
    sub_prompt = f"{base_prompt}, {view_hint}"

    img_path = assets_dir / f"{scene.scene_id}_sub{sub_i}.png"
    out = assets_dir / f"{scene.scene_id}_sub{sub_i}.mp4"

    if out.exists() and out.stat().st_size > 4096:
        logger.info(f"[assets]   sub{sub_i} cached: {out.name}")
        return str(out)

    logger.info(f"[assets]   sub{sub_i} prompt: {sub_prompt[:80]}")

    ok = False
    for source in chain:
        if ok:
            break
        cancel.check_active(f"소재 {source} 시도 전 sub{sub_i}")

        if source == "local_sdxl":
            async with _LOCAL_SDXL_SEM:
                cancel.check_active(f"SDXL 생성 직전 sub{sub_i}")
                ok = await generate_ai_image_local(sub_prompt, img_path, size=size, negative_prompt=neg)
            if not ok:
                logger.info(f"[assets]   local SDXL skip -> cloud chain (sub{sub_i})")

        elif source == "wavespeed":
            async with _WAVESPEED_SEM:
                ok = await generate_ai_image_wavespeed(sub_prompt, img_path, size=size, negative_prompt=neg)
                if not ok:
                    logger.info(f"[assets]   WaveSpeed retry in 5s (sub{sub_i})")
                    await asyncio.sleep(5)
                    ok = await generate_ai_image_wavespeed(sub_prompt, img_path, size=size, negative_prompt=neg)

        elif source == "dalle":
            dalle_size = "1024x1792"
            ok = await generate_ai_image_dalle(sub_prompt, img_path, size=dalle_size)

        elif source in ("pexels", "pixabay"):
            # AI entirely failed for this sub-image → stock fallback
            logger.info(f"[assets]   sub{sub_i} AI failed → stock fallback")
            # 서브이미지마다 고유 scene_id 를 줘 스톡 출력 파일이 겹치지 않게 한다.
            # (예전엔 4개가 모두 {scene_id}_main.mp4 에 덮어써서 파일이 깨지고
            #  4장이 전부 같은 영상이 됐다 — 다양성 목적이 무효화됐다.)
            _copy_data = scene.model_dump()
            _copy_data["scene_id"] = f"{scene.scene_id}_sub{sub_i}"
            tmp_scene_copy = type(scene)(**_copy_data)
            await _fetch_stock_asset(tmp_scene_copy, assets_dir, [source])
            if tmp_scene_copy.asset_url:
                return tmp_scene_copy.asset_url
            ok = True
            break

    if ok and img_path.exists() and img_path.stat().st_size > 1024:
        cancel.check_active(f"이미지→영상 직전 sub{sub_i}")
        if await image_to_video_ai(img_path, out, dur, style=style,
                                   scene_keyword=scene.keyword or "",
                                   scene_narration=scene.narration or ""):
            logger.info(f"[assets]   sub{sub_i} → {out.name} OK")
            return str(out)
        logger.warning(f"[assets]   sub{sub_i} image→video failed")
        return None

    if not ok:
        logger.warning(f"[assets]   sub{sub_i} all sources failed")
    return None


# ──────────────────────────────────────────────────────────────────────────────
# Stock footage path
# ──────────────────────────────────────────────────────────────────────────────

# 씬끼리 같은 영상을 집어오는 것을 막는 잡 단위 URL 장부.
#
# used_urls 가 _fetch_stock_asset 안의 지역 변수였던 탓에 씬마다 장부가 새로
# 시작됐고, 씬들이 병렬로 돌면서 서로 같은 스톡 영상을 골랐다(실측: scene_01_v2
# 와 scene_02_v2 가 md5 까지 동일한 49MB 파일). 잡 단위로 공유해야 한다.
# 키는 잡마다 유일한 assets_dir 경로를 쓴다.
# 소재 밝기 게이트
#
# 근거: 스톡 API 가 키워드에 맞다고 돌려준 영상 중에 '거의 검은' 클립이 섞여 온다.
# 실측(해녀 잡): 정상 소재 YAVG 66~166 인데 scene_02_v1 이 32.3 이었고, 그 소재를
# 쓴 4초가 완성본에서 통째로 검은 화면이 됐다.
#
# ffmpeg blackdetect/blackframe 는 '완전한 검정'(기본 pix_th=0.10)을 찾는 필터라
# YAVG 32 짜리 어두운 영상은 잡지 못한다. 그래서 signalstats 의 YAVG(0~255)를 직접 본다.
#
# 버리지 않고 '예비군'으로 강등하는 이유: 예전에 후보를 조건부로 제외했다가
# 뒤 씬들이 소재 1개로 굶은 적이 있다. 밝은 후보가 모자랄 때는 어두운 것이라도
# 쓰는 편이 같은 화면을 계속 보여주는 것보다 낫다.
ASSET_MIN_BRIGHTNESS = float(os.getenv("ASSET_MIN_BRIGHTNESS", "45"))


def _probe_brightness_sync(path: str) -> Optional[float]:
    """앞 5초를 1fps 로 샘플링해 평균 휘도(YAVG, 0~255)를 반환. 실패하면 None."""
    try:
        r = subprocess.run(
            ["ffmpeg", "-t", "5", "-i", str(path), "-vf",
             "fps=1,signalstats,metadata=print:key=lavfi.signalstats.YAVG",
             "-f", "null", "-"],
            capture_output=True, text=True, timeout=30)
    except Exception as e:
        logger.debug(f"[assets] 밝기 측정 실패({path}): {e}")
        return None
    vals = [float(m) for m in re.findall(r"YAVG=([\d.]+)", r.stderr)]
    return (sum(vals) / len(vals)) if vals else None


async def _is_bright_enough(path: str, label: str = "") -> bool:
    """어두운 소재인지 판정. 측정 불가면 통과시킨다(게이트가 파이프라인을 막지 않도록)."""
    y = await asyncio.to_thread(_probe_brightness_sync, path)
    if y is None:
        return True
    if y < ASSET_MIN_BRIGHTNESS:
        logger.warning(
            f"[assets] 어두운 소재 강등: {label or path} (YAVG {y:.1f} < {ASSET_MIN_BRIGHTNESS:.0f})")
        return False
    logger.debug(f"[assets] 밝기 OK: {label or path} (YAVG {y:.1f})")
    return True


_USED_URL_REGISTRY: dict = {}


def _job_used_urls(assets_dir: Path) -> set:
    return _USED_URL_REGISTRY.setdefault(str(assets_dir), set())


async def _cheap_llm(prompt: str) -> str:
    """후보 재정렬처럼 짧고 잦은 판단에 쓸 저비용 모델.

    script.py 를 모듈 최상단에서 import 하면 순환 참조가 되므로 함수 안에서 늦게 가져온다.
    """
    from pipeline import script as _sc
    from config import GEMINI_API_KEY, GROQ_API_KEY, ANTHROPIC_API_KEY
    attempts = []
    if GEMINI_API_KEY:
        attempts.append(("Gemini", lambda: _sc._llm_text_gemini(prompt, max_tokens=64)))
    if GROQ_API_KEY:
        attempts.append(("Groq", lambda: _sc._llm_text_oai(
            "https://api.groq.com/openai/v1/chat/completions",
            GROQ_API_KEY, "qwen/qwen3.8-27b", prompt, max_tokens=64)))
    if ANTHROPIC_API_KEY:
        attempts.append(("Claude", lambda: _sc._llm_text_claude(prompt, max_tokens=64)))
    if not attempts:
        raise RuntimeError("rerank 에 쓸 LLM 키가 없다")
    # 빈 문자열을 돌려주는 모델이 있다(실측: Gemini). 빈 응답은 실패로 보고 다음으로.
    last = ""
    for name, make in attempts:
        try:
            out = await make()
        except Exception as e:
            logger.debug(f"[assets] rerank {name} 실패: {e}")
            continue
        if str(out or "").strip():
            return out
        logger.debug(f"[assets] rerank {name} 빈 응답 → 다음 모델")
        last = out or ""
    return last


async def _pick_candidate(pool_p, pool_x, gate_terms, narration, blocked=None):
    """게이트 통과 후보 중 내레이션에 실제로 맞는 것을 고른다.

    1단계(검색+토큰 게이트)는 '단어가 겹치는가'까지만 본다. 그래서
    '모임(gathering)' 과 '먹구름(gathering clouds)' 을 구분하지 못한다.
    2단계로 설명을 읽고 판단하는 모델을 한 번 더 태운다(retrieve → rerank).
    맞는 것이 없으면 None 을 돌려준다 — 억지로 아무거나 넣지 않는다.
    """
    cancel.check_active("_pick_candidate")
    cands = list_candidates(pool_p, pool_x, topic_terms=gate_terms)
    if blocked:
        cands = [c for c in cands if not blocked(c["url"])]
    if not cands:
        return None
    idx = await rerank_with_llm(narration, [c["desc"] for c in cands],
                                _cheap_llm, topic=get_job_topic())
    if idx < 0:
        return None
    return cands[idx]


async def _fetch_stock_asset(scene, assets_dir: Path, chain: list) -> None:
    """Fetch stock video from Pexels/Pixabay fallback chain."""
    cancel.check_active(f"스톡 소재 직전 {scene.scene_id}")
    keyword = scene.keyword or scene.description or "nature landscape"
    keyword = _expand_domain_keyword(keyword)
    out = assets_dir / f"{scene.scene_id}_main.mp4"

    # 이 씬에서 '주제에 맞다'고 인정할 단어들 = 씬 고유어 + 잡 전체 주제어.
    # main 소재 선택부터 적용해야 첫 컷부터 엉뚱한 그림이 안 들어온다.
    _core = content_tokens(keyword)
    _topic = get_topic_terms()
    gate_terms = list(dict.fromkeys(_core + _topic)) or None

    pexels_r, pixabay_r = [], []
    for source in chain:
        if source == "pexels":
            pexels_r = await get_pexels_videos(keyword)
        elif source == "pixabay":
            pixabay_r = await get_pixabay_videos(keyword)

    shared_used = _job_used_urls(assets_dir)
    pexels_r = [v for v in pexels_r if v.get("url") not in shared_used]
    pixabay_r = [v for v in pixabay_r if v.get("url") not in shared_used]
    _nar = getattr(scene, "narration", "") or getattr(scene, "description", "") or keyword
    best = await _pick_candidate(pexels_r, pixabay_r, gate_terms, _nar)
    if best:
        shared_used.add(best["url"])
    if not best:
        # Expanded keyword retry
        expanded = _expand_domain_keyword(keyword, fallback=True)
        if expanded != keyword:
            p2, px2 = await asyncio.gather(
                get_pexels_videos(expanded) if "pexels" in chain else _empty_list(),
                get_pixabay_videos(expanded) if "pixabay" in chain else _empty_list(),
            )
            best = select_best_video(p2, px2, topic_terms=gate_terms)
    if not best and _topic:
        # 씬 고유어로는 못 찾았다. 주제어로 넓혀 본다.
        # (여기서도 못 찾으면 소재 없이 둔다 — 무관한 그림보다 낫다.)
        _broad = " ".join(_topic[:2])
        logger.info(f"[assets] {scene.scene_id}: 씬 키워드로 적합 소재 없음 "
                    f"→ 주제어로 재검색 {_broad!r}")
        p3, px3 = await asyncio.gather(
            get_pexels_videos(_broad) if "pexels" in chain else _empty_list(),
            get_pixabay_videos(_broad) if "pixabay" in chain else _empty_list(),
        )
        best = select_best_video(p3, px3, topic_terms=_topic)
    if not best:
        logger.warning(f"[assets] {scene.scene_id}: 주제에 맞는 스톡 소재를 찾지 못함 "
                       f"(키워드 {keyword!r}, 기준어 {gate_terms})")

    dark_pool: list[str] = []          # 어두워서 강등된 소재 (모자랄 때만 쓴다)
    if best:
        ok = await download_video(best["url"], out)
        if ok:
            if await _is_bright_enough(str(out), f"{scene.scene_id} main"):
                scene.asset_url = str(out)
                logger.info(f"[assets] stock downloaded: {scene.scene_id} ({keyword})")
            else:
                dark_pool.append(str(out))
        else:
            logger.warning(f"[assets] stock download failed: {scene.scene_id}")
    else:
        logger.warning(f"[assets] no stock found: {keyword}")

    # 서브클립 수만큼 '서로 다른' 영상을 확보한다.
    #
    # 예전에는 씬당 main 1개 + alt 1개만 받아놓고 렌더 단계에서 같은 영상을 4번
    # 잘라 썼다. 그래서 씬이 바뀌기 전까지 화면이 사실상 정지한 것처럼 보였다.
    # 참고 채널들이 3~5초마다 다른 장면으로 넘기는 리듬을 내려면, 잘라 쓸 원본
    # 자체가 여러 개여야 한다. 확보한 만큼만 쓰고 모자라면 있는 것을 재사용한다.
    collected: list[str] = []
    if scene.asset_url:
        collected.append(scene.asset_url)

    # 변형 키워드 사다리.
    #
    # 예전에는 그냥 앞에서부터 잘랐다: "tourists kimchi making event"
    # → "tourists kimchi" → "tourists". 마지막 단계에서 주제어(kimchi)가
    # 사라지고 가장 일반적인 단어만 남아, '한국 김치' 영상에 선글라스 낀
    # 서양인 관광객이 들어갔다. "museum women gathering" → "museum" 도 같은 사고.
    #
    # 이제는 '무엇인지 특정되는 단어'를 반드시 남긴다. 넓힐 때도 일반명사가
    # 아니라 주제어 자체로 좁힌다. 주제어가 없는 키워드면 잡 전체의
    # 주제 기준어로 되돌아간다.
    variant_keywords = [keyword]
    if len(_core) > 1:
        variant_keywords.append(" ".join(_core[:2]))
    if _core:
        variant_keywords.append(_core[0])
    if _topic:
        variant_keywords.append(" ".join(_topic[:2]))
    _expanded = _expand_domain_keyword(keyword, fallback=True)
    if content_tokens(_expanded):
        variant_keywords.append(_expanded)
    # 중복 제거, 순서 유지
    _seen = set()
    variant_keywords = [k for k in variant_keywords
                        if k and not (k in _seen or _seen.add(k))]
    logger.info(f"[assets] {scene.scene_id} 검색 사다리 {variant_keywords} / "
                f"적합성 기준어 {gate_terms}")

    # 지역 집합이 아니라 잡 전체가 공유하는 장부를 그대로 쓴다.
    local_used = set()
    if best:
        local_used.add(best["url"])
    relaxed = {"on": False}

    def _blocked(u: str) -> bool:
        """이미 쓴 영상인가. 완화 모드에서는 '이 씬에서 썼는지'만 따진다."""
        if not u or u in local_used:
            return True
        return (not relaxed["on"]) and (u in shared_used)

    # 1차는 다른 씬이 쓴 것까지 피하고, 2차는 씬 간 재사용을 허용한다.
    #
    # 1차만 돌렸더니 첫 씬이 공용 후보를 전부 가져가 뒤 씬들이 소재 1개로
    # 굶었다(실측: scene_02·03 이 main 하나씩). 씬끼리 같은 영상이 겹치는 것보다
    # 한 씬 안에서 같은 화면이 계속 머무는 쪽이 눈에 더 거슬린다. 그래서
    # 고유성은 '되도록', 씬당 개수 확보는 '반드시' 로 둔다.
    vi = 0
    for kw, _relax in ([(k, False) for k in variant_keywords]
                       + [(k, True) for k in variant_keywords]):
        relaxed["on"] = _relax
        if len(collected) >= N_SUB_IMAGES:
            break
        try:
            pool_p = await get_pexels_videos(kw, per_page=6) if "pexels" in chain else []
            pool_x = await get_pixabay_videos(kw) if "pixabay" in chain else []
        except Exception as e:
            logger.warning(f"[assets] 변형 키워드 검색 실패({kw}): {e}")
            continue

        while len(collected) < N_SUB_IMAGES:
            cand = await _pick_candidate(pool_p, pool_x, gate_terms, _nar,
                                         blocked=_blocked)
            if not cand:
                break
            local_used.add(cand["url"])
            shared_used.add(cand["url"])
            vi += 1
            out_v = assets_dir / f"{scene.scene_id}_v{vi}.mp4"
            if out_v.exists() and out_v.stat().st_size > 4096:
                collected.append(str(out_v))
                continue
            if await download_video(cand["url"], out_v) and out_v.stat().st_size > 4096:
                if not await _is_bright_enough(str(out_v), f"{scene.scene_id} v{vi}"):
                    dark_pool.append(str(out_v))
                    continue
                collected.append(str(out_v))
                logger.info(f"[assets] 추가 소재 {len(collected)}/{N_SUB_IMAGES}: "
                            f"{scene.scene_id} ({kw}, {'재사용허용' if _relax else '고유'})")

    # 재정렬이 전 키워드에서 '맞는 것 없음'을 답해 소재가 하나도 없으면,
    # 그 씬은 화면 없이 넘어간다. 그건 영상이 나레이션보다 짧아지는 사고다.
    # 마지막 수단으로 주제어만으로 한 번 더 찾되, 이때는 재정렬을 건너뛰고
    # 토큰 게이트만 통과한 것 중 최선을 쓴다. '주제에 속하지만 덜 정확한 화면'이
    # '화면 없음'보다는 낫다.
    if not collected and _topic:
        _broad = " ".join(_topic[:2])
        logger.warning(f"[assets] {scene.scene_id}: 적합 소재 0개 → 최후수단 "
                       f"주제어 검색 {_broad!r} (재정렬 생략)")
        try:
            p4, px4 = await asyncio.gather(
                get_pexels_videos(_broad, per_page=6) if "pexels" in chain else _empty_list(),
                get_pixabay_videos(_broad) if "pixabay" in chain else _empty_list(),
            )
            last = select_best_video(p4, px4, topic_terms=_topic)
        except Exception as e:
            logger.warning(f"[assets] 최후수단 검색 실패: {e}")
            last = None
        if last:
            out_last = assets_dir / f"{scene.scene_id}_fallback.mp4"
            if await download_video(last["url"], out_last) and out_last.stat().st_size > 4096:
                collected.append(str(out_last))
                logger.info(f"[assets] {scene.scene_id}: 최후수단 소재 확보")

    # 밝은 소재가 모자라면 강등해둔 것이라도 채운다. 같은 화면이 계속 머무는 것보다 낫다.
    while len(collected) < N_SUB_IMAGES and dark_pool:
        fallback = dark_pool.pop(0)
        collected.append(fallback)
        logger.warning(f"[assets] 밝은 소재 부족 → 강등 소재 투입: {fallback}")

    if collected:
        scene.asset_urls = collected
        scene.asset_url = collected[0]
        if len(collected) > 1:
            scene.alt_asset_url = collected[1]
        logger.info(f"[assets] {scene.scene_id}: 서로 다른 소재 {len(collected)}개 확보")


