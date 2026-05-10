# -*- coding: utf-8 -*-
"""WaveSpeed WAN 2.2 image-to-video: AI motion from static images.

Uses the same WAVESPEED_API_KEY as FLUX image generation — no additional
credentials needed. Style-aware motion prompts matched to cartoon/cinematic/
watercolor/anime/minimal/infographic/stock presets.

Endpoint: wavespeed-ai/wan-2.2/i2v-720p-ultra-fast
Auth: Bearer WAVESPEED_API_KEY (identical to image gen)
Pattern: POST → poll predictions/{id}/result → download
"""
from __future__ import annotations
import asyncio
import base64
import logging
from pathlib import Path

import httpx

from config import WAVESPEED_API_KEY

logger = logging.getLogger(__name__)

_WAN_URL = "https://api.wavespeed.ai/api/v2/wavespeed-ai/wan-2.2/i2v-720p-ultra-fast"
_POLL_BASE = "https://api.wavespeed.ai/api/v2/predictions"

# ── Style-aware motion prompt library ────────────────────────────────────────
# Each style gets a base motion description → drives Wan 2.2 animation quality.
# Content keyword enrichment is applied on top for better scene matching.

_STYLE_MOTION: dict[str, str] = {
    "cartoon":     "smooth animated scene comes to life, vibrant colors pulse, gentle parallax depth, cartoon world motion",
    "cinematic":   "slow cinematic dolly zoom, dramatic parallax, film-quality motion blur, golden light sweep",
    "watercolor":  "dreamy watercolor flows and blooms, soft painterly brush movement, ethereal color wash",
    "anime":       "dynamic anime camera movement, speed lines, vivid energy burst, expressive character motion",
    "minimal":     "clean elegant camera drift, subtle minimalist reveal, calm serene steady motion",
    "infographic": "smooth data visualization reveal, clean professional slide motion, crisp element animation",
    "stock":       "natural realistic motion, gentle documentary camera drift, authentic environment movement",
    "news":        "steady broadcast camera movement, professional journalism motion, authoritative reveal",
    "default":     "smooth gentle camera movement, cinematic parallax, natural environmental motion",
}

# Content-type motion enrichments (layered on top of style prompt)
_CONTENT_MOTION: dict[str, str] = {
    "food":   "steam rising, appetizing warmth, inviting close detail motion",
    "nature": "breeze through foliage, rippling water, organic natural energy",
    "city":   "urban life flow, light trails, dynamic street motion",
    "tech":   "glowing data streams, futuristic particles, innovation reveal",
    "person": "natural breathing, subtle expression, authentic human motion",
    "fire":   "flickering flame, dancing light, warm glow pulse",
    "water":  "flowing waves, ripple patterns, liquid motion",
    # Korean food motion hints
    "김치":   "fermentation bubble, vibrant red color shimmer",
    "불고기":  "sizzling grill smoke, meat glaze shimmer",
    "라면":   "steam curl rising, broth ripple motion",
    "요리":   "ingredients mixing, kitchen warmth, cooking energy",
}


def _build_motion_prompt(style: str, keyword: str = "", narration: str = "") -> str:
    """Compose motion prompt from style preset + content-type enrichment."""
    base = _STYLE_MOTION.get(style, _STYLE_MOTION["default"])

    # Scan keyword and narration for content-type enrichment
    combined = f"{keyword} {narration}".lower()
    enrichments: list[str] = []
    for key, motion_hint in _CONTENT_MOTION.items():
        if key in combined and motion_hint not in enrichments:
            enrichments.append(motion_hint)
            if len(enrichments) >= 2:
                break

    if enrichments:
        return f"{base}, {', '.join(enrichments)}"
    return base


async def wavespeed_image_to_video(
    image_path: Path,
    output_path: Path,
    duration: float = 5.0,
    style: str = "cartoon",
    scene_keyword: str = "",
    scene_narration: str = "",
) -> bool:
    """Generate AI motion video from static image via WaveSpeed WAN 2.2.

    Uses existing WAVESPEED_API_KEY — no extra credentials.
    Style and content-aware motion prompt → natural, high-quality animation.

    Returns True on success, False on any failure (caller falls back to FFmpeg).
    """
    if not WAVESPEED_API_KEY:
        logger.debug("[wan2v] WAVESPEED_API_KEY missing")
        return False

    if not image_path.exists() or image_path.stat().st_size < 1024:
        logger.warning(f"[wan2v] image missing: {image_path}")
        return False

    if output_path.exists() and output_path.stat().st_size > 4096:
        return True

    # Read and base64-encode the image
    try:
        img_bytes = image_path.read_bytes()
        img_b64 = base64.b64encode(img_bytes).decode()
        suffix = image_path.suffix.lower()
        mime = "image/png" if suffix == ".png" else "image/jpeg"
        image_data_uri = f"data:{mime};base64,{img_b64}"
    except Exception as e:
        logger.warning(f"[wan2v] image encode error: {e}")
        return False

    wan_duration = min(5, max(5, int(round(duration))))  # WAN only supports 5s clips
    motion_prompt = _build_motion_prompt(style, scene_keyword, scene_narration)

    headers = {
        "Authorization": f"Bearer {WAVESPEED_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "image": image_data_uri,
        "prompt": motion_prompt,
        "duration": wan_duration,
        "seed": -1,
    }

    try:
        logger.info(f"[wan2v] submit: {image_path.name} style={style} dur={wan_duration}s")
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(_WAN_URL, headers=headers, json=payload)
            resp.raise_for_status()
            data = resp.json()

        task_data = data.get("data", {})
        task_id = task_data.get("id", "")
        poll_url = (
            task_data.get("urls", {}).get("get")
            or f"{_POLL_BASE}/{task_id}/result"
        )

        if not task_id:
            logger.warning(f"[wan2v] no task_id in response: {data}")
            return False

        logger.info(f"[wan2v] task {task_id}, polling...")

        # Poll — WAN 2.2 ultra-fast typically finishes in 30-90s
        for attempt in range(60):
            await asyncio.sleep(3)
            async with httpx.AsyncClient(timeout=15.0) as client:
                pr = await client.get(poll_url, headers=headers)
                pr.raise_for_status()
                pd = pr.json().get("data", {})

            status = pd.get("status", "")
            if status in ("completed", "succeeded"):
                outputs = pd.get("outputs", [])
                if outputs:
                    logger.info(f"[wan2v] done in ~{(attempt+1)*3}s, downloading")
                    return await _download_wan_video(outputs[0], output_path)
                logger.warning("[wan2v] succeeded but no outputs")
                return False
            elif status in ("failed", "cancelled"):
                logger.warning(f"[wan2v] task {status}: {pd.get('error', '')}")
                return False

        logger.warning(f"[wan2v] polling timeout 180s: {task_id}")
        return False

    except Exception as e:
        logger.warning(f"[wan2v] error: {e}")
        return False


async def _download_wan_video(url: str, output_path: Path) -> bool:
    """Download WAN 2.2 generated video."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        async with httpx.AsyncClient(timeout=120.0, follow_redirects=True) as client:
            resp = await client.get(url)
            resp.raise_for_status()
        output_path.write_bytes(resp.content)
        size_mb = output_path.stat().st_size / 1_048_576
        logger.info(f"[wan2v] saved {output_path.name} ({size_mb:.1f}MB)")
        return output_path.stat().st_size > 4096
    except Exception as e:
        logger.warning(f"[wan2v] download error: {e}")
        return False
