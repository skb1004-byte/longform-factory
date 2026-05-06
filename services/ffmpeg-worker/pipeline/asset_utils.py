# -*- coding: utf-8 -*-
"""Shared asset download and keyword expansion utilities."""
from __future__ import annotations
import logging
import subprocess
from pathlib import Path
from typing import Dict

logger = logging.getLogger(__name__)

# Domain keyword expansion map (English only, Pexels-compatible)
DOMAIN_MAP: Dict[str, str] = {
    "exercise":    "exercise fitness gym",
    "workout":     "workout fitness training",
    "running":     "running jogging outdoor",
    "food":        "food cooking healthy meal",
    "technology":  "technology computer modern",
    "business":    "business office professional",
    "nature":      "nature landscape outdoor",
    "city":        "city urban skyline buildings",
    "health":      "health medical wellness",
    "money":       "money finance investment",
    "education":   "education school learning",
    "travel":      "travel destination outdoor",
    "science":     "science laboratory research",
    "AI":          "artificial intelligence technology robot",
}


def _expand_domain_keyword(keyword: str, fallback: bool = False) -> str:
    """Expand domain-specific keyword to Pexels-friendly phrase.

    Only expands single-word generic keywords. Multi-word keywords (3+ words)
    are returned as-is.
    """
    words = keyword.split()
    if len(words) >= 3:
        return keyword
    lower = keyword.lower()
    for key, expansion in DOMAIN_MAP.items():
        if key.lower() == lower:
            return expansion
    if fallback:
        return words[0] if words else "nature"
    return keyword


async def download_video(
    url: str,
    output_path: Path,
    max_duration: float = 60.0,
) -> bool:
    """Download video with ffmpeg, trimming to max_duration."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists() and output_path.stat().st_size > 4096:
        return True

    cmd = [
        "ffmpeg", "-y", "-t", str(max_duration),
        "-i", url, "-t", str(max_duration),
        "-c:v", "libx264", "-preset", "ultrafast",
        "-movflags", "+faststart", "-an",
        str(output_path)
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120.0)
        if result.returncode == 0 and output_path.exists() and output_path.stat().st_size > 4096:
            kb = output_path.stat().st_size // 1024
            logger.info(f"[assets] downloaded {output_path.name} ({kb}KB)")
            return True
        logger.warning(f"[assets] download failed ({url[:60]}): {result.stderr[-200:]}")
        return False
    except subprocess.TimeoutExpired:
        logger.warning(f"[assets] download timeout: {url[:60]}")
        return False
    except Exception as e:
        logger.error(f"[assets] download error: {e}")
        return False
