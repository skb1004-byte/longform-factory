# -*- coding: utf-8 -*-
"""Kling AI image-to-video: generates real motion video from static images.

Replaces simple FFmpeg Ken Burns zoom with AI-animated video for dramatically
better visual quality. Falls through to FFmpeg on any API failure.

Auth: JWT (HS256) using KLING_ACCESS_KEY / KLING_SECRET_KEY.
"""
from __future__ import annotations
import asyncio
import base64
import logging
import time
from pathlib import Path

import httpx
import jwt as _jwt

from config import KLING_ACCESS_KEY, KLING_SECRET_KEY

logger = logging.getLogger(__name__)

KLING_API_BASE = "https://api.klingai.com"

# Motion prompt library: style keyword → camera/motion description
_MOTION_PROMPTS: dict[str, str] = {
    "cartoon":     "smooth animated motion, gentle parallax, illustrated world",
    "cinematic":   "slow cinematic dolly zoom, dramatic reveal, film-quality motion",
    "watercolor":  "dreamy soft motion, gentle brush-stroke flow, painterly movement",
    "anime":       "dynamic anime camera motion, expressive movement, vivid colors",
    "minimal":     "clean minimal motion, subtle elegant camera drift",
    "infographic": "smooth data visualization motion, clean reveal animation",
    "default":     "smooth gentle camera movement, cinematic parallax, steady motion",
}


def _make_jwt() -> str:
    """Generate a short-lived JWT token for Kling API authentication."""
    now = int(time.time())
    payload = {
        "iss": KLING_ACCESS_KEY,
        "exp": now + 1800,
        "nbf": now - 5,
    }
    return _jwt.encode(payload, KLING_SECRET_KEY, algorithm="HS256")


def _motion_prompt_for_style(style: str, scene_keyword: str = "") -> str:
    """Choose motion prompt based on style, with keyword enrichment."""
    base = _MOTION_PROMPTS.get(style, _MOTION_PROMPTS["default"])
    # Add food/nature specific motion hints
    kw = scene_keyword.lower()
    if any(k in kw for k in ["food", "cook", "meal", "김치", "요리", "음식"]):
        base += ", steam rising, appetizing close-up motion"
    elif any(k in kw for k in ["nature", "landscape", "자연", "풍경"]):
        base += ", breeze through foliage, organic natural motion"
    return base


async def kling_image_to_video(
    image_path: Path,
    output_path: Path,
    duration: float = 5.0,
    style: str = "default",
    scene_keyword: str = "",
    mode: str = "std",
) -> bool:
    """Generate AI video from static image via Kling API.

    Args:
        image_path: Path to source PNG/JPG image.
        output_path: Where to save the MP4 output.
        duration: Target duration in seconds (determines 5s or 10s Kling mode).
        style: Visual style preset (drives motion prompt selection).
        scene_keyword: Scene keyword for motion hint enrichment.
        mode: "std" (fast) or "pro" (higher quality, slower).

    Returns:
        True on success, False on any failure (caller should fallback to FFmpeg).
    """
    if not KLING_ACCESS_KEY or not KLING_SECRET_KEY:
        logger.debug("[kling] keys not configured, skipping")
        return False

    if not image_path.exists() or image_path.stat().st_size < 1024:
        logger.warning(f"[kling] image missing or too small: {image_path}")
        return False

    # Skip if output already valid
    if output_path.exists() and output_path.stat().st_size > 4096:
        return True

    # Encode image as base64
    try:
        img_bytes = image_path.read_bytes()
        img_b64 = base64.b64encode(img_bytes).decode()
        suffix = image_path.suffix.lower()
        mime = "image/png" if suffix == ".png" else "image/jpeg"
        image_data_uri = f"data:{mime};base64,{img_b64}"
    except Exception as e:
        logger.warning(f"[kling] image encode error: {e}")
        return False

    kling_duration = "5" if duration <= 7.0 else "10"
    motion_prompt = _motion_prompt_for_style(style, scene_keyword)

    payload = {
        "model_name": "kling-v1",
        "image": image_data_uri,
        "prompt": motion_prompt,
        "negative_prompt": "blurry, distorted, low quality, watermark, static",
        "cfg_scale": 0.5,
        "mode": mode,
        "duration": kling_duration,
    }

    try:
        token = _make_jwt()
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

        logger.info(f"[kling] submitting image→video: {image_path.name} dur={kling_duration}s")
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(
                f"{KLING_API_BASE}/v1/videos/image2video",
                headers=headers,
                json=payload,
            )
            resp.raise_for_status()
            data = resp.json()

        if data.get("code") != 0:
            logger.warning(f"[kling] API error {data.get('code')}: {data.get('message')}")
            return False

        task_id = data["data"]["task_id"]
        logger.info(f"[kling] task {task_id} submitted")

        # Poll: Kling typically takes 60-120s per video
        for attempt in range(72):  # max 6 minutes
            await asyncio.sleep(5)
            token = _make_jwt()
            async with httpx.AsyncClient(timeout=15.0) as client:
                pr = await client.get(
                    f"{KLING_API_BASE}/v1/videos/image2video/{task_id}",
                    headers={"Authorization": f"Bearer {token}"},
                )
                pr.raise_for_status()
                pd = pr.json()

            status = pd.get("data", {}).get("task_status", "")
            if status == "succeed":
                videos = pd.get("data", {}).get("task_result", {}).get("videos", [])
                if videos:
                    video_url = videos[0]["url"]
                    logger.info(f"[kling] task {task_id} succeeded, downloading")
                    return await _download_kling_video(video_url, output_path)
                logger.warning(f"[kling] succeed but no videos in result")
                return False
            elif status == "failed":
                msg = pd.get("data", {}).get("task_status_msg", "")
                logger.warning(f"[kling] task {task_id} failed: {msg}")
                return False

        logger.warning(f"[kling] task {task_id} timeout after 360s")
        return False

    except Exception as e:
        logger.warning(f"[kling] error: {e}")
        return False


async def _download_kling_video(url: str, output_path: Path) -> bool:
    """Download Kling-generated video to output_path."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        async with httpx.AsyncClient(timeout=120.0, follow_redirects=True) as client:
            resp = await client.get(url)
            resp.raise_for_status()
        output_path.write_bytes(resp.content)
        size_mb = output_path.stat().st_size / 1_048_576
        logger.info(f"[kling] saved {output_path.name} ({size_mb:.1f}MB)")
        return output_path.stat().st_size > 4096
    except Exception as e:
        logger.warning(f"[kling] download error: {e}")
        return False
