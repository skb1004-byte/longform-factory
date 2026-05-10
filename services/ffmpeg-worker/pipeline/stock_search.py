# -*- coding: utf-8 -*-
"""Pexels / Pixabay video search helpers — split from assets.py to keep files ≤300 lines."""
from __future__ import annotations
import logging
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


async def get_pexels_videos(keyword: str, per_page: int = 5) -> List[Dict[str, Any]]:
    """Search Pexels with automatic keyword shortening fallback."""
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
                params={"query": keyword, "per_page": per_page, "orientation": "landscape"},
            )
            resp.raise_for_status()
            videos = resp.json().get("videos", [])
            logger.debug(f"[pexels] '{keyword}': {len(videos)} results")
            return videos
    except Exception as e:
        logger.warning(f"[pexels] error ({keyword}): {e}")
        return []


async def get_pixabay_videos(keyword: str, per_page: int = 5) -> List[Dict[str, Any]]:
    if not PIXABAY_API_KEY:
        return []
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(
                "https://pixabay.com/api/videos/",
                params={"key": PIXABAY_API_KEY, "q": keyword, "per_page": per_page, "min_width": 640},
            )
            resp.raise_for_status()
            videos = resp.json().get("hits", [])
            logger.debug(f"[pixabay] '{keyword}': {len(videos)} results")
            return videos
    except Exception as e:
        logger.warning(f"[pixabay] error ({keyword}): {e}")
        return []


def select_best_video(
    pexels_videos: List[Dict],
    pixabay_videos: List[Dict],
    exclude_url: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Select highest-resolution non-negative video."""
    candidates = []
    for v in pexels_videos:
        url = _extract_pexels_url(v)
        if not url or url == exclude_url or _is_negative(v):
            continue
        w, h = v.get("width", 0), v.get("height", 0)
        candidates.append({"url": url, "score": min(w, 1920) + min(h, 1080), "source": "pexels"})
    for v in pixabay_videos:
        url = _extract_pixabay_url(v)
        if not url or url == exclude_url or _is_negative(v):
            continue
        videos = v.get("videos", {})
        large = videos.get("large", {}) or videos.get("medium", {})
        w, h = large.get("width", 0), large.get("height", 0)
        candidates.append({"url": url, "score": min(w, 1920) + min(h, 1080), "source": "pixabay"})
    if not candidates:
        return None
    candidates.sort(key=lambda x: x["score"], reverse=True)
    return candidates[0]


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
