# -*- coding: utf-8 -*-
"""Pexels / Pixabay video search helpers — split from assets.py to keep files ≤300 lines."""
from __future__ import annotations
import logging

from pipeline import cancel
import re
from typing import Dict, List, Optional, Any

import httpx

from config import PEXELS_API_KEY, PIXABAY_API_KEY

logger = logging.getLogger(__name__)

NEGATIVE_KEYWORDS = [
    "funeral", "coffin", "death", "corpse", "cemetery", "grave",
    "war", "weapon", "gun", "violence", "blood", "injury",
    "arrest", "handcuff", "prison", "protest", "riot",
    "cigarette", "alcohol", "drug", "nude",
]


# 출력 화면 방향. 쇼츠(세로)인데 가로 소재만 받아오면 화면의 3분의 2가
# 블러 배경으로 죽는다. 렌더 해상도가 정해질 때 set_orientation() 으로 바꾼다.
_ORIENTATION = "landscape"


def _ascii_query(keyword: str) -> str:
    """스톡 API 에 보낼 검색어에서 한글 등 비ASCII를 걷어낸다.

    Pexels/Pixabay 는 한국어 질의를 이해하지 못한다. 그런데 키워드 생성 경로가
    여러 갈래라 어딘가에서 한글이 새어 들어왔다(실측: query=\"korean 전기차 culture\").
    그대로 보내면 결과가 0건이거나 엉뚱한 게 온다. 여기가 마지막 관문이므로
    여기서 한 번 걸러 모든 경로를 한꺼번에 막는다.
    """
    cleaned = re.sub(r"[^0-9A-Za-z\s\-]+", " ", keyword or "")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if not cleaned:
        return "korea"
    return cleaned


def set_orientation(width: int, height: int) -> None:
    """출력 해상도에 맞춰 스톡 검색 방향을 정한다."""
    global _ORIENTATION
    new = "portrait" if height > width else "landscape"
    if new != _ORIENTATION:
        logger.info(f"[stock] 검색 방향 {_ORIENTATION} → {new} ({width}x{height})")
    _ORIENTATION = new


async def get_pexels_videos(keyword: str, per_page: int = 5) -> List[Dict[str, Any]]:
    """Search Pexels with automatic keyword shortening fallback."""
    cancel.check_active("get_pexels_videos")
    if not keyword:
        return []
    words = keyword.split()
    results = await _get_pexels_raw(keyword, per_page)
    if len(results) >= 3 or len(words) <= 2:
        return results
    if len(words) > 3:
        r3 = await _get_pexels_raw(" ".join(words[:3]), per_page)
        if len(r3) > len(results):
            results = r3
    if len(results) >= 3:
        return results
    r2 = await _get_pexels_raw(" ".join(words[:2]), per_page)
    return r2 if len(r2) > len(results) else results


async def _get_pexels_raw(keyword: str, per_page: int = 5) -> List[Dict[str, Any]]:
    if not PEXELS_API_KEY:
        return []
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(
                "https://api.pexels.com/videos/search",
                headers={"Authorization": PEXELS_API_KEY},
                params={"query": _ascii_query(keyword), "per_page": per_page, "orientation": _ORIENTATION},
            )
            resp.raise_for_status()
            videos = resp.json().get("videos", [])
            logger.debug(f"[pexels] '{keyword}': {len(videos)} results")
            return videos
    except Exception as e:
        logger.warning(f"[pexels] error ({keyword}): {e}")
        return []


async def get_pixabay_videos(keyword: str, per_page: int = 5) -> List[Dict[str, Any]]:
    cancel.check_active("get_pixabay_videos")
    if not PIXABAY_API_KEY:
        return []
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(
                "https://pixabay.com/api/videos/",
                params={"key": PIXABAY_API_KEY, "q": _ascii_query(keyword), "per_page": per_page, "min_width": 640},
            )
            resp.raise_for_status()
            videos = resp.json().get("hits", [])
            logger.debug(f"[pixabay] '{keyword}': {len(videos)} results")
            return videos
    except Exception as e:
        logger.warning(f"[pixabay] error ({keyword}): {e}")
        return []


# ─────────────────────────────────────────────────────────────────────────
# 주제 적합성 게이트
#
# 스톡 API 는 '맞는 게 없다'는 답을 하지 않는다. 무엇이든 돌려준다.
# 실측(2026-09-20):
#   "tourists kimchi making event" → Pixabay 1위 = 개기일식(solar eclipse)
#   "museum women gathering"       → Pixabay 1위 = 먹구름 ('gathering clouds' 매칭)
# 그 결과 '한국 김치의 역사' 영상에 페루·발리 전통복 인물과 선글라스 낀
# 서양인 커플이 들어갔다. 렌더링을 아무리 고쳐도 남는 문제였다.
#
# 다행히 후보마다 내용을 알 수 있는 텍스트가 온다:
#   Pexels  → url 슬러그 (예: steaming-hot-korean-kimchi-stew-cooking)
#   Pixabay → tags 쉼표문자열 (예: "kimchi cabbage field food, korean food")
# 이걸 주제 핵심어와 대조해 통과 못 한 후보는 버린다.
#
# 버리고 나서 후보가 0개가 되면, 주제와 무관한 영상을 쓰는 대신
# 상위 개념으로 넓혀 다시 찾는다(호출부 담당). 아무것도 못 찾으면
# 못 찾은 채로 두는 편이 엉뚱한 그림을 넣는 것보다 낫다.
# 이 단어들만으로는 '무엇을 찍은 영상인지' 특정되지 않는다. 스톡 검색에서
# 이 단어들만 남기면 주제와 무관한 아무 영상이나 걸린다.
# (실측: "museum women gathering" → 먹구름, "tourists ..." → 서양인 관광객)
_GENERIC_TOKENS = {
    "the", "and", "for", "with", "from", "into", "over", "this", "that",
    "people", "person", "man", "men", "woman", "women", "child", "children",
    "group", "crowd", "gathering", "event", "festival", "celebration",
    "tourist", "tourists", "traveler", "travelers", "visitor", "visitors",
    "making", "make", "cooking", "eating", "walking", "working", "smiling",
    "museum", "building", "city", "street", "room", "place", "scene", "view",
    "beautiful", "traditional", "modern", "old", "new", "young", "happy",
    "background", "closeup", "close", "shot", "footage", "video", "clip",
    "day", "night", "morning", "evening", "time", "life", "world", "culture",
    "hand", "hands", "face", "table", "food",
}


def content_tokens(keyword: str) -> list:
    """키워드에서 '무엇인지 특정되는' 단어만 남긴다. 일반명사는 버린다."""
    out = []
    for w in str(keyword or "").lower().replace("-", " ").replace("_", " ").split():
        w = "".join(c for c in w if c.isalnum())
        if len(w) >= 3 and w not in _GENERIC_TOKENS and w not in out:
            out.append(w)
    return out


_TOPIC_TERMS: list = []


def set_topic_terms(terms) -> None:
    """이 잡에서 '주제에 맞다'고 인정할 단어들. 소문자 토큰으로 저장."""
    global _TOPIC_TERMS
    out = []
    for t in (terms or []):
        for w in str(t).lower().replace("-", " ").replace("_", " ").split():
            w = "".join(c for c in w if c.isalnum())
            if len(w) >= 3 and w not in out:
                out.append(w)
    _TOPIC_TERMS = out
    logger.info(f"[stock] 주제 적합성 기준어: {out}")


def get_topic_terms() -> list:
    return list(_TOPIC_TERMS)


_JOB_TOPIC = ""


def set_job_topic(topic: str) -> None:
    """이 영상이 무엇에 관한 것인지(사람이 쓴 그대로). 재정렬 판단에 쓴다."""
    global _JOB_TOPIC
    _JOB_TOPIC = (topic or "").strip()


def get_job_topic() -> str:
    return _JOB_TOPIC


def _candidate_text(v: dict) -> str:
    """후보가 무엇을 찍은 영상인지 알려주는 모든 텍스트를 한 덩어리로."""
    parts = [str(v.get("url", "")), str(v.get("pageURL", "")), str(v.get("name", ""))]
    tags = v.get("tags")
    if isinstance(tags, str):
        parts.append(tags)
    elif isinstance(tags, (list, tuple)):
        parts.extend(str(t) for t in tags)
    return " ".join(parts).lower().replace("-", " ").replace("_", " ")


def is_on_topic(v: dict, terms=None) -> bool:
    """후보 설명에 주제 핵심어가 하나라도 들어 있는가.

    기준어가 설정돼 있지 않으면(구버전 호출 등) 통과시킨다 —
    게이트가 파이프라인을 막아서는 안 된다.
    """
    terms = terms if terms is not None else _TOPIC_TERMS
    if not terms:
        return True
    text = _candidate_text(v)
    return any(t in text for t in terms)


def _score(w: int, h: int) -> float:
    """해상도 + 방향 일치도. 방향이 맞으면 크게 가산한다.

    예전에는 해상도만 봐서 세로 영상에도 1920x1080 가로 소재가 항상 1등이었다.
    화질이 조금 낮아도 방향이 맞는 소재가 결과물에서는 훨씬 낫다.
    """
    if w <= 0 or h <= 0:
        return 0.0
    base = min(w, 1920) + min(h, 1080)
    want_portrait = _ORIENTATION == "portrait"
    is_portrait = h > w
    return base + (1500 if want_portrait == is_portrait else 0)


async def probe_stock_coverage(terms: list, chain: list = None) -> int:
    """주제어로 스톡에 '주제에 맞는' 영상이 몇 개나 있는지 미리 센다.

    왜 잡 단위로 한 번에 재는가:
    씬마다 스톡→AI 를 오가면 한 영상 안에 실사 스톡과 AI 생성 이미지가
    섞여 질감이 따로 논다(실측: 실사 폭포 + AI 나무막대 + 유럽 석조건물이
    한 영상에 들어갔다). 어느 쪽으로 갈지는 처음에 한 번 정해야 한다.

    반환: 주제 게이트를 통과한 고유 후보 수.
    """
    chain = chain or ["pexels", "pixabay"]
    if not terms:
        return 99
    # 기준어 전부를 합쳐 재면 안 된다. 'jeju stone statue' 에서 stone/statue 는
    # 아무 석상에나 붙어 22개가 잡히지만 실제로 쓸 만한 건 2~3개였다.
    #
    # 맨 앞 하나만 쓰는 것도 안 된다. 키워드 생성 순서에 따라 'stone' 이
    # 맨 앞에 오면 커버리지가 부풀려져 스톡에 머문다(실측).
    #
    # 기준어마다 따로 재서 '가장 적은 쪽'을 택한다. 주제를 특정하는 단어가
    # 스톡에 없으면, 다른 일반어가 아무리 많이 잡혀도 그 주제를 보여줄 수 없다.
    counts = []
    for term in terms[:3]:
        got = set()
        try:
            p = await get_pexels_videos(term, per_page=8) if "pexels" in chain else []
            x = await get_pixabay_videos(term) if "pixabay" in chain else []
        except Exception as e:
            logger.warning(f"[stock] 커버리지 탐색 실패({term}): {e}")
            continue
        for v in list(p) + list(x):
            if not is_on_topic(v, [term]):
                continue
            k = v.get("url") or v.get("pageURL")
            if k:
                got.add(k)
        counts.append((term, len(got)))
    if not counts:
        return 99
    term, n = min(counts, key=lambda kv: kv[1])
    logger.info(f"[stock] 커버리지 탐색: {counts} → 최소 '{term}' {n}개로 판정")
    return n


async def _probe_unused(terms: list, chain: list = None) -> int:
    probe_terms = terms[:1]
    seen = set()
    for term in probe_terms:
        try:
            p = await get_pexels_videos(term, per_page=8) if "pexels" in chain else []
            x = await get_pixabay_videos(term) if "pixabay" in chain else []
        except Exception as e:
            logger.warning(f"[stock] 커버리지 탐색 실패({term}): {e}")
            continue
        for v in list(p) + list(x):
            if not is_on_topic(v, probe_terms):
                continue
            key = v.get("url") or v.get("pageURL")
            if key:
                seen.add(key)
    logger.info(f"[stock] 커버리지 탐색: 핵심어 {probe_terms} → 적합 후보 {len(seen)}개")
    return len(seen)


def describe_candidate(v: dict) -> str:
    """후보가 무엇을 찍은 영상인지 사람이 읽을 수 있는 한 줄로.

    Pexels 는 tags 가 비어 있는 대신 url 슬러그에 설명이 들어 있고
    (steaming-hot-korean-kimchi-stew-cooking), Pixabay 는 tags 가 쉼표 문자열이다.
    """
    tags = v.get("tags")
    if isinstance(tags, str) and tags.strip():
        return tags.strip()
    if isinstance(tags, (list, tuple)) and tags:
        return ", ".join(str(t) for t in tags)
    url = str(v.get("url") or v.get("pageURL") or "")
    slug = url.rstrip("/").split("/")[-1]
    # 끝에 붙은 숫자 id 제거
    parts = [p for p in slug.replace("_", "-").split("-") if not p.isdigit()]
    return " ".join(parts) if parts else "(설명 없음)"


_RERANK_PROMPT = """영상 한 컷에 쓸 자료화면을 고른다.

영상 주제: {topic}
이 컷 내레이션: {narration}

후보 (영어 설명):
{candidates}

판단 순서 — 이 순서대로 따진다
1) 이 후보가 '영상 주제'와 같은 나라·같은 문화의 것인가?
   아니면 탈락이다. 비슷하게 생겼다는 이유는 통하지 않는다.
   예) 주제가 한국 돌하르방인데 후보가 이스터섬 모아이 → 탈락.
       주제가 한국 김치인데 후보가 페루 전통축제 → 탈락.
       주제가 한국인데 후보가 브라질 예수상 → 탈락.
2) 내레이션이 말하는 대상·장소·행동이 그 후보에 실제로 찍혀 있는가?
   단어만 겹치는 것은 아니다. '모임(gathering)'과 '먹구름(gathering clouds)'은 다르다.
3) 추상적인 배경(빛 번짐, 색 그라데이션, 흐린 나뭇잎 등)은
   내레이션이 그 자체를 말할 때만 고른다. 채우기 용도로 고르지 않는다.

1~3을 모두 통과한 후보가 없으면 반드시 0 을 답한다.
0 은 실패가 아니라 정상적인 답이다. 억지로 고른 화면은 시청자가 바로 알아챈다.

답: 번호 하나만. 다른 말 금지."""


async def rerank_with_llm(narration: str, candidates: list, llm_call,
                          topic: str = "") -> int:
    """후보 중 내레이션에 맞는 것의 인덱스. 없으면 -1. 실패하면 0(1순위 유지).

    토큰 매칭은 '먹구름/모임'처럼 단어만 겹치는 경우를 못 거른다.
    설명을 실제로 읽고 판단하는 단계를 한 겹 둔다(retrieve → rerank).
    LLM 이 실패하면 기존 순위를 그대로 쓴다 — 게이트가 파이프라인을 막지 않는다.
    """
    if not candidates:
        return -1
    if len(candidates) == 1:
        return 0
    lines = "\n".join(f"{i+1}. {c}" for i, c in enumerate(candidates[:8]))
    prompt = _RERANK_PROMPT.format(
        topic=(topic or get_job_topic() or "(주제 미상)").strip()[:80],
        narration=(narration or "").strip()[:200],
        candidates=lines)
    try:
        raw = await llm_call(prompt)
    except Exception as e:
        logger.warning(f"[stock] rerank 실패 → 기존 순위 사용: {e}")
        return 0
    digits = "".join(ch for ch in str(raw or "") if ch.isdigit())
    if not digits:
        logger.warning(f"[stock] rerank 응답 해석 불가({str(raw)[:60]!r}) → 기존 순위")
        return 0
    n = int(digits[:2])
    if n == 0:
        logger.info(f"[stock] rerank: 맞는 후보 없음 — {narration[:30]!r}")
        return -1
    if 1 <= n <= len(candidates):
        logger.info(f"[stock] rerank 선택 {n}: {candidates[n-1][:60]}")
        return n - 1
    return 0


def select_best_video(
    pexels_videos: List[Dict],
    pixabay_videos: List[Dict],
    exclude_url: Optional[str] = None,
    require_topic: bool = True,
    topic_terms: Optional[List[str]] = None,
) -> Optional[Dict[str, Any]]:
    """해상도 순으로 최선의 후보. require_topic 이면 주제에 맞는 것만 본다.

    topic_terms 를 주면 그걸 쓰고, 안 주면 잡 전역 기준어를 쓴다.
    씬마다 기준이 다르고 씬들이 동시에 도는 구조라 전역만 쓰면 서로 덮어쓴다.
    """
    _terms = topic_terms if topic_terms is not None else _TOPIC_TERMS
    candidates = []
    dropped = 0
    for v in pexels_videos:
        url = _extract_pexels_url(v)
        if not url or url == exclude_url or _is_negative(v):
            continue
        if require_topic and not is_on_topic(v, _terms):
            dropped += 1
            continue
        w, h = v.get("width", 0), v.get("height", 0)
        candidates.append({"url": url, "score": _score(w, h), "source": "pexels"})
    for v in pixabay_videos:
        url = _extract_pixabay_url(v)
        if not url or url == exclude_url or _is_negative(v):
            continue
        if require_topic and not is_on_topic(v, _terms):
            dropped += 1
            continue
        videos = v.get("videos", {})
        large = videos.get("large", {}) or videos.get("medium", {})
        w, h = large.get("width", 0), large.get("height", 0)
        candidates.append({"url": url, "score": _score(w, h), "source": "pixabay"})
    if dropped:
        logger.info(f"[stock] 주제 불일치로 제외한 후보 {dropped}개 "
                    f"(기준어 {_terms})")
    if not candidates:
        return None
    candidates.sort(key=lambda x: x["score"], reverse=True)
    return candidates[0]


def list_candidates(
    pexels_videos: List[Dict],
    pixabay_videos: List[Dict],
    topic_terms: Optional[List[str]] = None,
    require_topic: bool = True,
) -> List[Dict[str, Any]]:
    """게이트를 통과한 후보 전부를 점수순으로. rerank 에 넘기기 위한 것."""
    _terms = topic_terms if topic_terms is not None else _TOPIC_TERMS
    out = []
    for v in pexels_videos:
        url = _extract_pexels_url(v)
        if not url or _is_negative(v):
            continue
        if require_topic and not is_on_topic(v, _terms):
            continue
        out.append({"url": url, "score": _score(v.get("width", 0), v.get("height", 0)),
                    "source": "pexels", "desc": describe_candidate(v)})
    for v in pixabay_videos:
        url = _extract_pixabay_url(v)
        if not url or _is_negative(v):
            continue
        if require_topic and not is_on_topic(v, _terms):
            continue
        large = (v.get("videos", {}) or {}).get("large", {}) or (v.get("videos", {}) or {}).get("medium", {})
        out.append({"url": url, "score": _score(large.get("width", 0), large.get("height", 0)),
                    "source": "pixabay", "desc": describe_candidate(v)})
    out.sort(key=lambda x: x["score"], reverse=True)
    return out


def _extract_pexels_url(v: Dict) -> Optional[str]:
    files = v.get("video_files", [])
    hd = [f for f in files if f.get("height", 0) >= 720 and f.get("quality") in ("hd", "sd")]
    if hd:
        hd.sort(key=lambda f: f.get("height", 0), reverse=True)
        return hd[0].get("link")
    return files[0].get("link") if files else None


def _extract_pixabay_url(v: Dict) -> Optional[str]:
    videos = v.get("videos", {})
    for quality in ("large", "medium", "small"):
        url = videos.get(quality, {}).get("url")
        if url:
            return url
    return None


def _is_negative(video: Dict) -> bool:
    text = " ".join([
        str(video.get("user", "")),
        str(video.get("url", "")),
        " ".join(str(t) for t in video.get("tags", [])),
    ]).lower()
    return any(neg in text for neg in NEGATIVE_KEYWORDS)
