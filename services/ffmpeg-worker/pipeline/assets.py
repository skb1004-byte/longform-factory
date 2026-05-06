# -*- coding: utf-8 -*-
"""Video asset routing: AI generation or stock footage, driven by style preset."""
from __future__ import annotations
import asyncio
import logging
from pathlib import Path
from typing import List, Optional, Dict, Any

import httpx

from config import PEXELS_API_KEY, PIXABAY_API_KEY, JOBS_DIR
from pipeline.asset_utils import download_video, _expand_domain_keyword
from pipeline.style_presets import resolve_style, get_preset, AI_STYLES
from pipeline.ai_image import (
    build_prompt,
    build_cartoon_prompt,           # backward compat export
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

    for scene in scenes:
        existing = assets_dir / f"{scene.scene_id}_main.mp4"
        if existing.exists() and existing.stat().st_size > 4096:
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

async def _fetch_ai_asset(scene, assets_dir: Path, style: str, preset: dict, chain: list) -> None:
    """Try each source in fallback chain until one succeeds."""
    img_path = assets_dir / f"{scene.scene_id}_main.png"
    out = assets_dir / f"{scene.scene_id}_main.mp4"
    prompt = build_prompt(scene, style)
    neg = preset.get("negative_prompt", "")
    size = preset.get("size_portrait", "768x1344")
    dur = max(scene.duration_seconds or 5.0, 3.0)

    logger.info(f"[assets] AI prompt ({scene.scene_id}): {prompt[:80]}")

    ok = False
    for source in chain:
        if ok:
            break

        if source == "wavespeed":
            ok = await generate_ai_image_wavespeed(prompt, img_path, size=size, negative_prompt=neg)
            if not ok:
                # One 429 retry with backoff
                logger.info(f"[assets] WaveSpeed retry in 5s ({scene.scene_id})")
                await asyncio.sleep(5)
                ok = await generate_ai_image_wavespeed(prompt, img_path, size=size, negative_prompt=neg)

        elif source == "dalle":
            dalle_size = "1024x1792"  # portrait
            ok = await generate_ai_image_dalle(prompt, img_path, size=dalle_size)

        elif source in ("pexels", "pixabay"):
            # Stock fallback when all AI sources fail
            logger.info(f"[assets] AI failed → stock fallback ({scene.scene_id})")
            await _fetch_stock_asset(scene, assets_dir, [source])
            # Inter-scene delay still needed
            await asyncio.sleep(3)
            return

    if ok and img_path.exists():
        if image_to_video(img_path, out, dur):
            scene.asset_url = str(out)
            logger.info(f"[assets] AI image→video: {scene.scene_id} style={style}")
        else:
            logger.warning(f"[assets] image→video failed: {scene.scene_id}")
    else:
        logger.warning(f"[assets] all AI sources failed: {scene.scene_id}")

    # Rate-limit guard between scenes
    await asyncio.sleep(3)


# ──────────────────────────────────────────────────────────────────────────────
# Stock footage path
# ──────────────────────────────────────────────────────────────────────────────

async def _fetch_stock_asset(scene, assets_dir: Path, chain: list) -> None:
    """Fetch stock video from Pexels/Pixabay fallback chain."""
    keyword = scene.keyword or scene.description or "nature landscape"
    keyword = _expand_domain_keyword(keyword)
    out = assets_dir / f"{scene.scene_id}_main.mp4"

    pexels_r, pixabay_r = [], []
    for source in chain:
        if source == "pexels":
            pexels_r = await get_pexels_videos(keyword)
        elif source == "pixabay":
            pixabay_r = await get_pixabay_videos(keyword)

    best = select_best_video(pexels_r, pixabay_r)
    if not best:
        # Expanded keyword retry
        expanded = _expand_domain_keyword(keyword, fallback=True)
        if expanded != keyword:
            p2, px2 = await asyncio.gather(
                get_pexels_videos(expanded) if "pexels" in chain else asyncio.coroutine(lambda: [])(),
                get_pixabay_videos(expanded) if "pixabay" in chain else asyncio.coroutine(lambda: [])(),
            )
            best = select_best_video(p2, px2)

    if best:
        ok = await download_video(best["url"], out)
        if ok:
            scene.asset_url = str(out)
            logger.info(f"[assets] stock downloaded: {scene.scene_id} ({keyword})")
        else:
            logger.warning(f"[assets] stock download failed: {scene.scene_id}")
    else:
        logger.warning(f"[assets] no stock found: {keyword}")

    # Alt asset for variety
    alt_kw = " ".join(keyword.split()[:2]) if len(keyword.split()) > 2 else keyword
    if "pexels" in chain:
        alt_pexels = await get_pexels_videos(alt_kw, per_page=3)
        if alt_pexels:
            alt_best = select_best_video(alt_pexels, [], exclude_url=scene.asset_url)
            if alt_best:
                alt_out = assets_dir / f"{scene.scene_id}_alt.mp4"
                if not alt_out.exists() or alt_out.stat().st_size < 4096:
                    await download_video(alt_best["url"], alt_out)
                if alt_out.exists() and alt_out.stat().st_size > 4096:
                    scene.alt_asset_url = str(alt_out)


# ──────────────────────────────────────────────────────────────────────────────
# Pexels / Pixabay search helpers
# ──────────────────────────────────────────────────────────────────────────────

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
