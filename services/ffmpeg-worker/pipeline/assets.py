# -*- coding: utf-8 -*-
"""Video asset routing: AI generation or stock footage, driven by style preset."""
from __future__ import annotations
import asyncio
import logging
from pathlib import Path
from typing import List, Optional, Dict, Any

from config import JOBS_DIR
from pipeline.asset_utils import download_video, _expand_domain_keyword
from pipeline.style_presets import resolve_style, get_preset, AI_STYLES
from pipeline.ai_image import (
    build_prompt,
    build_cartoon_prompt,           # backward compat export
    generate_ai_image_wavespeed,
    generate_ai_image_dalle,
    image_to_video_ai,
)
from pipeline.stock_search import (   # split to keep assets.py ≤300 lines
    get_pexels_videos,
    get_pixabay_videos,
    select_best_video,
    NEGATIVE_KEYWORDS,
)

logger = logging.getLogger(__name__)

# Number of distinct AI images to generate per scene (multi-image quality mode).
# Each sub-clip in the Ken Burns loop gets its own unique image → no visual repetition.
N_SUB_IMAGES = 4

# Composition/angle hints injected per sub-image to maximise visual variety.
_SUB_VIEW_HINTS = [
    "wide establishing shot, expansive full scene view",
    "close-up macro detail, intimate foreground focus",
    "medium shot, balanced mid-range composition",
    "dynamic cinematic angle, dramatic perspective framing",
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
        # Multi-image reuse check (AI mode): all N sub-images must already exist
        if is_ai:
            sub_paths = [assets_dir / f"{scene.scene_id}_sub{i}.mp4" for i in range(N_SUB_IMAGES)]
            valid_subs = [p for p in sub_paths if p.exists() and p.stat().st_size > 4096]
            if len(valid_subs) == N_SUB_IMAGES:
                scene.asset_urls = [str(p) for p in valid_subs]
                scene.asset_url = scene.asset_urls[0]
                logger.info(f"[assets] reusing {N_SUB_IMAGES} sub-images: {scene.scene_id}")
                continue

        # Legacy single-asset reuse (stock mode or partial AI cache)
        existing = assets_dir / f"{scene.scene_id}_main.mp4"
        if not is_ai and existing.exists() and existing.stat().st_size > 4096:
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
    """Generate N_SUB_IMAGES distinct AI images per scene for maximum visual variety.

    Each sub-image uses the same base prompt but a unique composition/angle hint
    (wide / close-up / medium / dynamic) so every Ken Burns sub-clip shows a
    genuinely different illustration — eliminating the same-image repetition bug.

    Falls back to stock footage when all AI sources fail for a given sub-image.
    """
    neg = preset.get("negative_prompt", "")
    size = preset.get("size_portrait", "768x1344")
    dur = max(scene.duration_seconds or 5.0, 3.0)
    base_prompt = build_prompt(scene, style)

    logger.info(f"[assets] AI multi-image start ({scene.scene_id}, n={N_SUB_IMAGES}): {base_prompt[:70]}")

    collected_urls: list[str] = []

    for sub_i in range(N_SUB_IMAGES):
        view_hint = _SUB_VIEW_HINTS[sub_i % len(_SUB_VIEW_HINTS)]
        sub_prompt = f"{base_prompt}, {view_hint}"

        img_path = assets_dir / f"{scene.scene_id}_sub{sub_i}.png"
        out = assets_dir / f"{scene.scene_id}_sub{sub_i}.mp4"

        # Skip if this sub-image video already exists and is valid
        if out.exists() and out.stat().st_size > 4096:
            logger.info(f"[assets]   sub{sub_i} cached: {out.name}")
            collected_urls.append(str(out))
            continue

        logger.info(f"[assets]   sub{sub_i} prompt: {sub_prompt[:80]}")

        ok = False
        for source in chain:
            if ok:
                break

            if source == "wavespeed":
                ok = await generate_ai_image_wavespeed(sub_prompt, img_path, size=size, negative_prompt=neg)
                if not ok:
                    logger.info(f"[assets]   WaveSpeed retry in 5s (sub{sub_i})")
                    await asyncio.sleep(5)
                    ok = await generate_ai_image_wavespeed(sub_prompt, img_path, size=size, negative_prompt=neg)

            elif source == "dalle":
                dalle_size = "1024x1792"
                ok = await generate_ai_image_dalle(sub_prompt, img_path, size=dalle_size)

            elif source in ("pexels", "pixabay"):
                # AI entirely failed for this sub-image → stock fallback
                logger.info(f"[assets]   sub{sub_i} AI failed → stock fallback")
                tmp_scene_copy = type(scene)(**scene.model_dump())
                await _fetch_stock_asset(tmp_scene_copy, assets_dir, [source])
                if tmp_scene_copy.asset_url:
                    collected_urls.append(tmp_scene_copy.asset_url)
                ok = True  # mark done so we break the chain loop
                break

        if ok and img_path.exists() and img_path.stat().st_size > 1024:
            if await image_to_video_ai(img_path, out, dur, style=style,
                                       scene_keyword=scene.keyword or "",
                                       scene_narration=scene.narration or ""):
                collected_urls.append(str(out))
                logger.info(f"[assets]   sub{sub_i} → {out.name} OK")
            else:
                logger.warning(f"[assets]   sub{sub_i} image→video failed")
        elif not ok:
            logger.warning(f"[assets]   sub{sub_i} all sources failed")

        # Rate-limit guard between API calls (WaveSpeed: ~3 req/s)
        await asyncio.sleep(3)

    if collected_urls:
        scene.asset_urls = collected_urls
        scene.asset_url = collected_urls[0]   # backward compat
        logger.info(f"[assets] {scene.scene_id}: {len(collected_urls)}/{N_SUB_IMAGES} sub-images ready")
    else:
        # Complete failure: try stock fallback for the whole scene
        logger.warning(f"[assets] {scene.scene_id}: all sub-images failed → stock fallback")
        await _fetch_stock_asset(scene, assets_dir, ["pexels", "pixabay"])

    # Final inter-scene delay
    await asyncio.sleep(2)


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


