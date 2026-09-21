# -*- coding: utf-8 -*-
"""AI image generation: Local SDXL (primary, free) -> WaveSpeed FLUX -> DALL-E 3 (fallback).

Also provides image_to_video_ai(): Kling AI video (primary) → FFmpeg Ken Burns (fallback).
Prompt building logic is in pipeline/prompt_builder.py.
"""
from __future__ import annotations
import asyncio
import base64
import logging

from pipeline import cancel
import subprocess
from pathlib import Path

import httpx

from config import (
    WAVESPEED_API_KEY, OPENAI_API_KEY, KLING_ENABLED,
    HIGGSFIELD_API_KEY_ID, HIGGSFIELD_API_KEY_SECRET,
    LOCAL_SDXL_URL, LOCAL_SDXL_ENABLED,
)

logger = logging.getLogger(__name__)

WAVESPEED_URL = "https://api.wavespeed.ai/api/v2/wavespeed-ai/flux-dev"
DALLE_URL = "https://api.openai.com/v1/images/generations"

_DEFAULT_NEGATIVE = (
    "realistic photo, 3d render, watermark, text, blurry, dark, "
    "violent, nsfw, low quality, deformed"
)

# Re-export prompt builders for backward compatibility
from pipeline.prompt_builder import build_prompt, build_cartoon_prompt  # noqa: E402, F401


async def generate_ai_image_local(
    prompt: str,
    output_path: Path,
    size: str = "768x1344",
    negative_prompt: str = "",
) -> bool:
    """Generate image via the local SDXL-Turbo GPU worker (lf2_sdxl, free/offline).

    Fails soft on any error (service down, model still loading, OOM) so the
    WaveSpeed -> DALL-E -> stock chain below still runs as a safety net.
    """
    cancel.check_active("generate_ai_image_local")
    if not LOCAL_SDXL_ENABLED:
        return False

    try:
        w_str, h_str = size.lower().split("x")
        width, height = int(w_str), int(h_str)
    except Exception:
        width, height = 768, 1344

    payload = {
        "prompt": prompt,
        "negative_prompt": negative_prompt or _DEFAULT_NEGATIVE,
        "width": width,
        "height": height,
        # [2026-09-20] steps=2 / guidance=0.0 은 SDXL-Turbo 기본값이지만,
        # CFG 0 은 '텍스트 조건을 거의 쓰지 않는다'는 뜻이다. 그래서 프롬프트를
        # 아무리 다듬어도 반영되지 않았다 — 돌하르방을 묘사했는데 그리스 대리석
        # 조각이 10/12컷 나오고, negative 로 막아도 소용이 없었던 원인이다.
        #
        # 같은 프롬프트·같은 시드로 A/B 실측(돌하르방):
        #   steps 2  / CFG 0.0 : 형태는 맞지만 흐릿하고 밋밋   5.5초
        #   steps 4  / CFG 1.5 : 선명해짐                      6.6초
        #   steps 8  / CFG 3.0 : 현무암 질감·색감 최적         8.0초  ← 채택
        #   steps 12 / CFG 5.0 : 과포화, 균열 과장             7.8초
        # 장당 2.5초 늘지만(16장 기준 +40초) 프롬프트가 실제로 먹는다.
        "steps": 8,
        "guidance_scale": 3.0,
    }
    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(f"{LOCAL_SDXL_URL}/generate", json=payload)
            if resp.status_code != 200:
                logger.info(f"[ai_image] local SDXL non-200 ({resp.status_code}): {resp.text[:150]}")
                return False
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_bytes(resp.content)
        ok = output_path.exists() and output_path.stat().st_size > 1024
        if ok:
            kb = output_path.stat().st_size // 1024
            logger.info(f"[ai_image] local SDXL OK: {output_path.name} ({kb}KB)")
        return ok
    except Exception as e:
        logger.info(f"[ai_image] local SDXL unreachable/error: {e} -> cloud fallback")
        return False

async def local_image_to_video(
    image_path: Path,
    output_path: Path,
    duration: float,
    style: str = "cartoon",
) -> bool:
    """Convert a static image to video via the local depth-parallax worker (lf2_sdxl).

    The depth model runs on CPU on the worker side (see sdxl-worker/app.py), not
    the GPU that SDXL-Turbo already fills close to capacity -- so this can run
    concurrently with local image generation without competing for VRAM. This
    is a 2.5D parallax + slow zoom, not full diffusion-based motion; it's a
    free/local step up from flat Ken Burns, tried only after every cloud video
    API (WaveSpeed/Higgsfield/Kling) has failed or is unconfigured.
    """
    if not LOCAL_SDXL_ENABLED:
        return False
    try:
        image_b64 = base64.b64encode(image_path.read_bytes()).decode()
    except Exception as e:
        logger.info(f"[ai_image] local i2v: cannot read {image_path.name}: {e}")
        return False

    payload = {
        "image_b64": image_b64,
        "duration": max(duration, 2.0),
        "fps": 24,
        "amplitude": 0.015,
    }
    try:
        async with httpx.AsyncClient(timeout=90.0) as client:
            resp = await client.post(f"{LOCAL_SDXL_URL}/image_to_video", json=payload)
            if resp.status_code != 200:
                logger.info(f"[ai_image] local i2v non-200 ({resp.status_code}): {resp.text[:150]}")
                return False
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_bytes(resp.content)
        ok = output_path.exists() and output_path.stat().st_size > 4096
        if ok:
            kb = output_path.stat().st_size // 1024
            logger.info(f"[ai_image] local i2v OK: {output_path.name} ({kb}KB)")
        return ok
    except Exception as e:
        logger.info(f"[ai_image] local i2v unreachable/error: {e} -> Ken Burns fallback")
        return False

async def generate_ai_image_wavespeed(
    prompt: str,
    output_path: Path,
    size: str = "768x1344",
    negative_prompt: str = "",
) -> bool:
    """Generate image via WaveSpeed FLUX. Returns True on success."""
    if not WAVESPEED_API_KEY:
        logger.warning("[ai_image] WAVESPEED_API_KEY missing")
        return False

    headers = {
        "Authorization": f"Bearer {WAVESPEED_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "prompt": prompt,
        "negative_prompt": negative_prompt or _DEFAULT_NEGATIVE,
        "size": size,
        "num_inference_steps": 28,
        "guidance_scale": 3.5,
        "num_outputs": 1,
    }

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(WAVESPEED_URL, headers=headers, json=payload)
            resp.raise_for_status()
            data = resp.json()

        task_data = data.get("data", {})
        poll_url = (
            task_data.get("urls", {}).get("get")
            or f"https://api.wavespeed.ai/api/v2/predictions/{task_data.get('id', '')}/result"
        )
        if not task_data.get("id"):
            logger.warning(f"[ai_image] WaveSpeed: no task_id: {data}")
            return False

        logger.info(f"[ai_image] WaveSpeed task: {task_data['id']} size={size}")

        for _ in range(50):  # max 150s
            await asyncio.sleep(3)
            async with httpx.AsyncClient(timeout=15.0) as client:
                pr = await client.get(poll_url, headers=headers)
                pr.raise_for_status()
                pd = pr.json().get("data", {})

            status = pd.get("status", "")
            if status in ("completed", "succeeded"):
                outputs = pd.get("outputs", [])
                if outputs:
                    return await _download_image(outputs[0], output_path)
                logger.warning(f"[ai_image] WaveSpeed: {status} but no outputs")
                return False
            elif status in ("failed", "cancelled"):
                logger.warning(f"[ai_image] WaveSpeed {status}: {pd.get('error', '')}")
                return False

        logger.warning("[ai_image] WaveSpeed: polling timeout 150s")
        return False

    except Exception as e:
        logger.warning(f"[ai_image] WaveSpeed error: {e}")
        return False


async def generate_ai_image_dalle(
    prompt: str,
    output_path: Path,
    size: str = "1024x1792",
) -> bool:
    """Generate image via OpenAI DALL-E 3 (fallback)."""
    if not OPENAI_API_KEY:
        logger.warning("[ai_image] OPENAI_API_KEY missing")
        return False

    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": "dall-e-3",
        "prompt": prompt,
        "n": 1,
        "size": size,
        "quality": "standard",
        "response_format": "url",
    }
    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(DALLE_URL, headers=headers, json=payload)
            resp.raise_for_status()
            data = resp.json()
        return await _download_image(data["data"][0]["url"], output_path)
    except Exception as e:
        logger.warning(f"[ai_image] DALL-E error: {e}")
        return False


async def _download_image(url: str, output_path: Path) -> bool:
    """Download image from URL to output_path."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.get(url)
            resp.raise_for_status()
        output_path.write_bytes(resp.content)
        kb = len(resp.content) // 1024
        logger.info(f"[ai_image] saved {output_path.name} ({kb}KB)")
        return output_path.stat().st_size > 1024
    except Exception as e:
        logger.warning(f"[ai_image] download error: {e}")
        return False


# 죽은 프로바이더를 이미지마다 다시 두드리지 않기 위한 차단기.
#
# 실측: 교육 영상 한 편에서 이미지 24장을 만드는 동안 WaveSpeed 401 과
# Kling 404 를 장당 한 번씩, 총 48번 호출했다. 키가 없거나 엔드포인트가
# 사라진 건 다음 이미지라고 달라지지 않는데 장당 3초씩(합계 2~3분) 버렸다.
# 두 번 연속 실패하면 이번 프로세스에서는 접는다. 설정을 저장해 모듈이
# reload 되거나 컨테이너가 재시작되면 다시 0 에서 시작하므로, 키를 새로
# 넣었을 때 영구히 막히는 일은 없다.
_PROVIDER_STRIKES: dict = {}
_STRIKE_LIMIT = 2


def _provider_blocked(name: str) -> bool:
    return _PROVIDER_STRIKES.get(name, 0) >= _STRIKE_LIMIT


def _provider_strike(name: str) -> None:
    n = _PROVIDER_STRIKES.get(name, 0) + 1
    _PROVIDER_STRIKES[name] = n
    if n == _STRIKE_LIMIT:
        logger.warning(
            f"[ai_image] {name} {n}회 연속 실패 → 이번 프로세스에서 더 시도하지 않는다"
        )


def _provider_ok(name: str) -> None:
    _PROVIDER_STRIKES.pop(name, None)


async def image_to_video_ai(
    image_path: Path,
    output_path: Path,
    duration: float,
    style: str = "cartoon",
    scene_keyword: str = "",
    scene_narration: str = "",
) -> bool:
    """Convert static image to video with style-aware AI motion.

    Chain (best → fallback):
      1. WaveSpeed WAN 2.2 i2v  — same API key as FLUX, ultra-fast, style+content aware motion
      2. Higgsfield DoP i2v     — cinematic preset-driven motion, good quality backup
      3. Kling AI image2video   — JWT-based, moderate speed, tertiary backup
      4. FFmpeg Ken Burns       — always available, zero-dependency fallback

    Style-aware: cartoon/cinematic/watercolor/anime/minimal/infographic each get
    distinct motion prompts, further enriched by scene keyword/narration content.
    """
    cancel.check_active("image_to_video_ai")
    if output_path.exists() and output_path.stat().st_size > 4096:
        return True

    # ── Level 1: WaveSpeed WAN 2.2 (primary — same key as FLUX) ─────────────
    if _provider_blocked("WaveSpeed WAN 2.2"):
        pass
    else:
      try:
        from pipeline.wavespeed_video import wavespeed_image_to_video
        ok = await wavespeed_image_to_video(
            image_path=image_path,
            output_path=output_path,
            duration=duration,
            style=style,
            scene_keyword=scene_keyword,
            scene_narration=scene_narration,
        )
        if ok:
            _provider_ok("WaveSpeed WAN 2.2")
            logger.info(f"[ai_image] WAN 2.2 video OK: {output_path.name}")
            return True
        _provider_strike("WaveSpeed WAN 2.2")
        logger.info("[ai_image] WAN 2.2 failed → Higgsfield fallback")
      except Exception as e:
        _provider_strike("WaveSpeed WAN 2.2")
        logger.warning(f"[ai_image] WAN 2.2 exception: {e} → Higgsfield fallback")

    # ── Level 2: Higgsfield DoP (tertiary — cinematic preset motion) ─────
    if (HIGGSFIELD_API_KEY_ID and HIGGSFIELD_API_KEY_SECRET
            and not _provider_blocked("Higgsfield DoP")):
        try:
            from pipeline.higgsfield_video import higgsfield_image_to_video
            ok = await higgsfield_image_to_video(
                image_path=image_path,
                output_path=output_path,
                duration=duration,
                style=style,
                scene_keyword=scene_keyword,
            )
            if ok:
                _provider_ok("Higgsfield DoP")
                logger.info(f"[ai_image] Higgsfield DoP video OK: {output_path.name}")
                return True
            _provider_strike("Higgsfield DoP")
            logger.info("[ai_image] Higgsfield DoP failed → Kling fallback")
        except Exception as e:
            _provider_strike("Higgsfield DoP")
            logger.warning(f"[ai_image] Higgsfield DoP exception: {e} → Kling fallback")

    # ── Level 3: Kling AI (secondary — JWT auth) ──────────────────────────
    if KLING_ENABLED and not _provider_blocked("Kling"):
        try:
            from pipeline.kling_video import kling_image_to_video
            ok = await kling_image_to_video(
                image_path=image_path,
                output_path=output_path,
                duration=duration,
                style=style,
                scene_keyword=scene_keyword,
            )
            if ok:
                _provider_ok("Kling")
                logger.info(f"[ai_image] Kling video OK: {output_path.name}")
                return True
            _provider_strike("Kling")
            logger.info("[ai_image] Kling failed → Ken Burns fallback")
        except Exception as e:
            _provider_strike("Kling")
            logger.warning(f"[ai_image] Kling exception: {e} → Ken Burns fallback")

    # ── Level 3: FFmpeg Ken Burns (always works) ──────────────────────────
    # ── Level 4: Local depth-parallax (GPU-free, no API key needed) ──────
    # "API가 있으면 API로, 아니면 GPU로": 위 3개 클라우드 API가 전부 없거나
    # 실패했을 때만 여기로 온다. 실제로는 깊이추정 모델이 CPU에서 돌아서
    # (SDXL의 GPU 메모리와 안 겹침) 로컬 이미지 생성과 진짜 동시에 돌 수 있다.
    try:
        ok = await local_image_to_video(image_path, output_path, duration, style=style)
        if ok:
            logger.info(f"[ai_image] local depth-parallax video OK: {output_path.name}")
            return True
        logger.info("[ai_image] local depth-parallax failed -> Ken Burns fallback")
    except Exception as e:
        logger.warning(f"[ai_image] local depth-parallax exception: {e} -> Ken Burns fallback")
    # 동기 ffmpeg 호출이라 그대로 두면 최대 120초 이벤트 루프가 멈춘다(병렬 4장 동반 실패).
    return await asyncio.to_thread(image_to_video, image_path, output_path, duration)


def image_to_video(image_path: Path, output_path: Path, duration: float) -> bool:
    """Convert static image to looping video with Ken Burns zoom effect (FFmpeg)."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists() and output_path.stat().st_size > 4096:
        return True

    dur = max(duration, 3.0)
    n_frames = int(dur * 30)

    vf = (
        f"scale=1920:1920:force_original_aspect_ratio=increase,"
        f"crop=1080:1920,"
        f"zoompan=z='min(zoom+0.0015,1.3)':d={n_frames}"
        f":x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)':s=1080x1920:fps=30,"
        f"unsharp=5:5:0.5,eq=brightness=0.02:contrast=1.1:saturation=1.3"
    )
    cmd = [
        "ffmpeg", "-y",
        "-loop", "1", "-i", str(image_path),
        "-vf", vf,
        "-t", str(dur),
        "-c:v", "libx264", "-preset", "fast", "-crf", "23",
        "-pix_fmt", "yuv420p", "-movflags", "+faststart", "-an",
        str(output_path),
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120.0)
        ok = result.returncode == 0 and output_path.exists() and output_path.stat().st_size > 4096
        if ok:
            logger.info(f"[ai_image] Ken Burns OK: {output_path.name}")
        else:
            logger.warning(f"[ai_image] Ken Burns failed: {result.stderr[-200:]}")
        return ok
    except Exception as e:
        logger.error(f"[ai_image] Ken Burns error: {e}")
        return False
