# -*- coding: utf-8 -*-
"""AI image generation: WaveSpeed FLUX (primary) + DALL-E 3 (fallback)."""
from __future__ import annotations
import asyncio
import logging
import subprocess
from pathlib import Path

import httpx

from config import WAVESPEED_API_KEY, OPENAI_API_KEY

logger = logging.getLogger(__name__)

WAVESPEED_URL = "https://api.wavespeed.ai/api/v2/wavespeed-ai/flux-dev"
WAVESPEED_POLL_URL = "https://api.wavespeed.ai/api/v2/predictions/{task_id}"
DALLE_URL = "https://api.openai.com/v1/images/generations"

CARTOON_SYSTEM = (
    "Korean webtoon manhwa art style, vibrant flat colors, clean bold outlines, "
    "professional illustration, no text, no watermarks, cinematic composition"
)
NEGATIVE_PROMPT = (
    "realistic photo, 3d render, watermark, text, blurry, dark, violent, "
    "nsfw, low quality"
)


def build_cartoon_prompt(scene) -> str:
    """Build cartoon-style prompt from scene data."""
    base = scene.description or scene.narration or scene.keyword or "abstract scene"
    # Truncate to avoid overly long prompts
    if len(base) > 100:
        base = base[:100]
    return f"{CARTOON_SYSTEM}, {base}"


async def generate_ai_image_wavespeed(prompt: str, output_path: Path) -> bool:
    """Generate cartoon-style image via WaveSpeed FLUX API. Returns True on success."""
    if not WAVESPEED_API_KEY:
        logger.warning("[ai_image] WAVESPEED_API_KEY missing")
        return False

    headers = {
        "Authorization": f"Bearer {WAVESPEED_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "inputs": {
            "prompt": prompt,
            "negative_prompt": NEGATIVE_PROMPT,
            "num_outputs": 1,
            "aspect_ratio": "9:16",
            "output_format": "png",
            "num_inference_steps": 28,
            "guidance_scale": 3.5,
        }
    }

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(WAVESPEED_URL, headers=headers, json=payload)
            resp.raise_for_status()
            data = resp.json()

        task_id = data.get("data", {}).get("id") or data.get("id")
        if not task_id:
            logger.warning(f"[ai_image] WaveSpeed: no task_id in response: {data}")
            return False

        # Poll until completed (max 60s)
        poll_url = WAVESPEED_POLL_URL.format(task_id=task_id)
        for attempt in range(20):
            await asyncio.sleep(3)
            async with httpx.AsyncClient(timeout=15.0) as client:
                poll_resp = await client.get(poll_url, headers=headers)
                poll_resp.raise_for_status()
                poll_data = poll_resp.json()

            status = poll_data.get("data", {}).get("status") or poll_data.get("status")
            if status == "succeeded":
                outputs = (
                    poll_data.get("data", {}).get("outputs")
                    or poll_data.get("outputs", [])
                )
                if outputs:
                    img_url = outputs[0]
                    return await _download_image(img_url, output_path)
                break
            elif status in ("failed", "cancelled"):
                logger.warning(f"[ai_image] WaveSpeed task {status}: {poll_data}")
                return False

        logger.warning("[ai_image] WaveSpeed: polling timeout")
        return False

    except Exception as e:
        logger.warning(f"[ai_image] WaveSpeed error: {e}")
        return False


async def generate_ai_image_dalle(prompt: str, output_path: Path) -> bool:
    """Generate cartoon-style image via OpenAI DALL-E 3 (fallback)."""
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
        "size": "1024x1792",
        "quality": "standard",
        "response_format": "url",
    }
    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(DALLE_URL, headers=headers, json=payload)
            resp.raise_for_status()
            data = resp.json()
        img_url = data["data"][0]["url"]
        return await _download_image(img_url, output_path)
    except Exception as e:
        logger.warning(f"[ai_image] DALL-E error: {e}")
        return False


async def _download_image(url: str, output_path: Path) -> bool:
    """Download image from URL to output_path."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(url)
            resp.raise_for_status()
        output_path.write_bytes(resp.content)
        kb = len(resp.content) // 1024
        logger.info(f"[ai_image] downloaded {output_path.name} ({kb}KB)")
        return output_path.stat().st_size > 1024
    except Exception as e:
        logger.warning(f"[ai_image] image download error: {e}")
        return False


def image_to_video(image_path: Path, output_path: Path, duration: float) -> bool:
    """Convert static image to looping video with Ken Burns zoom effect."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists() and output_path.stat().st_size > 4096:
        return True

    dur = max(duration, 3.0)
    fps = 30
    n_frames = int(dur * fps)

    # Zoompan: gentle zoom-in over duration, output 1080x1920 portrait
    vf = (
        f"scale=1920:1920:force_original_aspect_ratio=increase,"
        f"crop=1080:1920,(null),"
        f"zoompan=z='min(zoom+0.0015,1.3)':d={n_frames}:x='iw/2-(iw/zoom/2)':"
        f"y='ih/2-(ih/zoom/2)':s=1080x1920:fps={fps},"
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
        if result.returncode == 0 and output_path.exists() and output_path.stat().st_size > 4096:
            logger.info(f"[ai_image] image→video: {output_path.name}")
            return True
        logger.warning(f"[ai_image] image→video failed: {result.stderr[-300:]}")
        return False
    except Exception as e:
        logger.error(f"[ai_image] image→video error: {e}")
        return False
