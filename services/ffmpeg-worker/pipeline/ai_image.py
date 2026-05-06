# -*- coding: utf-8 -*-
"""AI image generation: WaveSpeed FLUX (primary) + DALL-E 3 (fallback)."""
from __future__ import annotations
import asyncio
import logging
import subprocess
from pathlib import Path

import httpx

from config import WAVESPEED_API_KEY, OPENAI_API_KEY
from pipeline.style_presets import get_preset

logger = logging.getLogger(__name__)

WAVESPEED_URL = "https://api.wavespeed.ai/api/v2/wavespeed-ai/flux-dev"
DALLE_URL = "https://api.openai.com/v1/images/generations"

_DEFAULT_NEGATIVE = (
    "realistic photo, 3d render, watermark, text, blurry, dark, "
    "violent, nsfw, low quality, deformed"
)

# Scene context enrichment: keyword fragment → descriptive subject hint
_CONTEXT_MAP = {
    "labor":      "workers in orange uniforms facing authority figures in suits",
    "union":      "union workers protesting, raised fists, solidarity banner",
    "economy":    "economic charts, coins, currency symbols, business setting",
    "finance":    "stock market graphs, financial district, money flow",
    "politics":   "government building, officials at podium, policy documents",
    "law":        "courtroom, gavel, legal books, justice scales",
    "technology": "futuristic devices, glowing screens, innovation lab",
    "business":   "modern office meeting room, executives, corporate environment",
    "people":     "diverse group of people, community gathering, crowd",
    "nature":     "beautiful landscape, lush green scenery, outdoors",
    "city":       "urban cityscape, skyscrapers, busy street scene",
    "success":    "trophy, celebration, triumphant characters, achievement",
    "crisis":     "character looking worried, storm clouds, tense atmosphere",
    "solution":   "lightbulb moment, problem solved, happy characters",
    "health":     "medical setting, doctor, healthcare, wellness",
    "food":       "delicious meal, restaurant, kitchen, cooking",
    "education":  "classroom, students learning, books, school building",
    "travel":     "airplane, passport, world map, adventure",
    "family":     "happy family, home interior, warmth, togetherness",
    "sport":      "athletes competing, stadium, victory, training",
}


def build_prompt(scene, style: str = "cartoon") -> str:
    """Build AI image prompt for the given style preset.

    Uses scene.keyword (English 1-3 words) as the primary subject,
    enriched with _CONTEXT_MAP hints. Falls back to narration[:60].
    """
    preset = get_preset(style)
    prefix = preset.get("prompt_prefix", "")

    keyword = (scene.keyword or "").strip().lower()
    narration = (scene.narration or "").strip()

    # Enrich keyword with context hint
    context_hint = ""
    for key, hint in _CONTEXT_MAP.items():
        if key in keyword:
            context_hint = hint
            break

    if keyword:
        subject = f"{keyword}, {context_hint}" if context_hint else keyword
    elif narration:
        subject = narration[:60].strip()
    else:
        subject = "abstract concept illustration"

    return f"{prefix}, {subject}" if prefix else subject


def build_cartoon_prompt(scene) -> str:
    """Backward-compatible alias for build_prompt(scene, 'cartoon')."""
    return build_prompt(scene, "cartoon")


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


def image_to_video(image_path: Path, output_path: Path, duration: float) -> bool:
    """Convert static image to looping video with Ken Burns zoom effect."""
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
            logger.info(f"[ai_image] image→video OK: {output_path.name}")
        else:
            logger.warning(f"[ai_image] image→video failed: {result.stderr[-200:]}")
        return ok
    except Exception as e:
        logger.error(f"[ai_image] image→video error: {e}")
        return False
