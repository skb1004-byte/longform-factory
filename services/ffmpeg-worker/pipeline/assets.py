# -*- coding: utf-8 -*-
"""Video asset search and download from Pexels/Pixabay, or AI image generation."""
from __future__ import annotations
import asyncio
import logging
from pathlib import Path
from typing import List, Optional, Dict, Any

import httpx

from config import PEXELS_API_KEY, PIXABAY_API_KEY, JOBS_DIR
from pipeline.asset_utils import download_video, _expand_domain_keyword
from pipeline.ai_image import (
    build_cartoon_prompt,
    generate_ai_image_wavespeed,
    generate_ai_image_dalle,
    image_to_video,
)

logger = logging.getLogger(__name__)

NEGATIVE_KEYWORDS = [
    "funeral", "coffin", "death", "corpse", "cemetery", "grave",
    "war", "weapon", "gun", "violence", "blood", "injury",
    "arrest", "handcuff", "prison", "protest", "riot",
    "cigarette", "alcohol", "drug", "nude",
]


async def search_and_download_assets(
    job_id: str,
    scenes: List,
    image_mode: str = "stock",
) -> List:
    """Download assets for each scene. image_mode: 'stock' | 'ai'."""
    jobs_dir = JOBS_DIR / job_id
    assets_dir = jobs_dir / "assets"
    assets_dir.mkdir(parents=True, exist_ok=True)

    for scene in scenes:
        existing = assets_dir / f"{scene.scene_id}_main.mp4"
        if existing.exists() and existing.stat().st_size > 4096:
            scene.asset_url = str(existing)
            logger.info(f"[assets] reusing {existing.name}")
            continue

        if image_mode == "ai":
            # AI cartoon image → video
            img_path = assets_dir / f"{scene.scene_id}_main.png"
            out = assets_dir / f"{scene.scene_id}_main.mp4"
            prompt = build_cartoon_prompt(scene)
            ok = await generate_ai_image_wavespeed(prompt, img_path)
            if not ok:
                logger.info(f"[assets] WaveSpeed failed → DALL-E fallback ({scene.scene_id})")
                ok = await generate_ai_image_dalle(prompt, img_path)
            if ok and img_path.exists():
                dur = scene.duration_seconds or 5.0
                if image_to_video(img_path, out, dur):
                    scene.asset_url = str(out)
                    logger.info(f"[assets] AI image→video: {scene.scene_id}")
                else:
                    logger.warning(f"[assets] image→video failed: {scene.scene_id}")
            else:
                logger.warning(f"[assets] AI generation failed: {scene.scene_id}")
            continue

        # Stock video mode (Pexels + Pixabay)
        keyword = scene.keyword or scene.description or "nature landscape"
        keyword = _expand_domain_keyword(keyword)

        pexels_r, pixabay_r = await asyncio.gather(
            get_pexels_videos(keyword),
            get_pixabay_videos(keyword),
        )

        best = select_best_video(pexels_r, pixabay_r)
        if best:
            out = assets_dir / f"{scene.scene_id}_main.mp4"
            ok = await download_video(best["url"], out)
            if ok:
                scene.asset_url = str(out)
                logger.info(f"[assets] downloaded: {scene.scene_id} ({keyword})")
            else:
                logger.warning(f"[assets] download failed: {scene.scene_id}")
        else:
            expanded = _expand_domain_keyword(keyword, fallback=True)
            if expanded != keyword:
                pexels_r2, pixabay_r2 = await asyncio.gather(
                    get_pexels_videos(expanded),
                    get_pixabay_videos(expanded),
                )
                best2 = select_best_video(pexels_r2, pixabay_r2)
                if best2:
                    out = assets_dir / f"{scene.scene_id}_main.mp4"
                    ok = await download_video(best2["url"], out)
                    if ok:
                        scene.asset_url = str(out)
                        logger.info(f"[assets] expanded keyword success: {expanded}")
                        continue
            logger.warning(f"[assets] no assets found: {keyword}")

        # Alt asset for variety
        alt_kw = " ".join(keyword.split()[:2]) if len(keyword.split()) > 2 else keyword
        alt_pexels = await get_pexels_videos(alt_kw, per_page=3)
        if alt_pexels:
            alt_best = select_best_video(alt_pexels, [], exclude_url=scene.asset_url)
            if alt_best:
                alt_out = assets_dir / f"{scene.scene_id}_alt.mp4"
                if not alt_out.exists() or alt_out.stat().st_size < 4096:
                    await download_video(alt_best["url"], alt_out)
                if alt_out.exists() and alt_out.stat().st_size > 4096:
                    scene.alt_asset_url = str(alt_out)

    return scenes


async def get_pexels_videos(keyword: str, per_page: int = 5) -> List[Dict[str, Any]]:
    """Search Pexels with automatic fallback (full → 3 words → 2 words)."""
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
    """Call Pexels API for video search."""
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
            data = resp.json()
            videos = data.get("videos", [])
            logger.debug(f"[pexels] '{keyword}': {len(videos)} results")
            return videos
    except Exception as e:
        logger.warning(f"[pexels] search error ({keyword}): {e}")
        return []


async def get_pixabay_videos(keyword: str, per_page: int = 5) -> List[Dict[str, Any]]:
    """Search Pixabay for videos."""
    if not PIXABAY_API_KEY:
        return []
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(
                "https://pixabay.com/api/videos/",
                params={"key": PIXABAY_API_KEY, "q": keyword, "per_page": per_page, "min_width": 640},
            )
            resp.raise_for_status()
            data = resp.json()
            videos = data.get("hits", [])
            logger.debug(f"[pixabay] '{keyword}': {len(videos)} results")
            return videos
    except Exception as e:
        logger.warning(f"[pixabay] search error ({keyword}): {e}")
        return []


def select_best_video(
    pexels_videos: List[Dict],
    pixabay_videos: List[Dict],
    exclude_url: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Select best video: prefer HD, reject negative content."""
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
