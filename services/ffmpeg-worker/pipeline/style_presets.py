# -*- coding: utf-8 -*-
"""Visual style presets for AI image generation and stock footage routing.

Each preset defines:
  prompt_prefix   - Style descriptors prepended to the subject
  negative_prompt - What to exclude from the image
  primary_source  - 'wavespeed' | 'dalle' | 'pexels'
  fallback_chain  - Ordered sources to attempt (stops at first success)
  size_portrait   - WaveSpeed size for 9:16 Shorts
  size_landscape  - WaveSpeed size for 16:9 Longform
"""
from __future__ import annotations
from typing import Dict, Any

STYLE_PRESETS: Dict[str, Dict[str, Any]] = {

    # ── AI styles ─────────────────────────────────────────────────────────
    "cartoon": {
        "name": "Korean Manhwa Cartoon",
        "prompt_prefix": (
            "Korean manhwa webtoon illustration, simple rounded cute characters, "
            "flat cel-shading, bold black outlines, vibrant saturated colors, "
            "dramatic cinematic composition, expressive character poses, "
            "rich background detail, professional digital art, no text, no watermarks"
        ),
        "negative_prompt": (
            "realistic photo, 3d render, photography, watermark, text, logo, "
            "blurry, dark, violent, nsfw, low quality, deformed, ugly"
        ),
        "primary_source": "wavespeed",
        "fallback_chain": ["wavespeed", "dalle", "pexels"],
        "size_portrait": "768x1344",
        "size_landscape": "1344x768",
    },

    "cinematic": {
        "name": "Cinematic Realism",
        "prompt_prefix": (
            "ultra-realistic cinematic photography, professional film look, "
            "dramatic lighting, shallow depth of field, 8k resolution, "
            "photorealistic, high detail, masterful composition, IMAX quality"
        ),
        "negative_prompt": (
            "cartoon, anime, illustration, painting, watermark, text, "
            "low quality, blurry, oversaturated, nsfw, deformed"
        ),
        "primary_source": "wavespeed",
        "fallback_chain": ["wavespeed", "pexels"],
        "size_portrait": "768x1344",
        "size_landscape": "1344x768",
    },

    "watercolor": {
        "name": "Watercolor Art",
        "prompt_prefix": (
            "beautiful watercolor painting, soft brushstrokes, pastel tones, "
            "dreamy artistic style, professional illustration, "
            "impressionist color wash, paper texture, no text, no watermarks"
        ),
        "negative_prompt": (
            "realistic photo, 3d render, harsh lines, watermark, text, "
            "dark, violent, nsfw, low quality"
        ),
        "primary_source": "wavespeed",
        "fallback_chain": ["wavespeed", "dalle", "pexels"],
        "size_portrait": "768x1344",
        "size_landscape": "1344x768",
    },

    "anime": {
        "name": "Anime Style",
        "prompt_prefix": (
            "Japanese anime illustration, vibrant anime art style, "
            "dynamic character poses, clean line art, detailed anime backgrounds, "
            "professional anime production quality, cel shading, no text"
        ),
        "negative_prompt": (
            "realistic, western cartoon, 3d render, watermark, text, "
            "nsfw, low quality, blurry, deformed"
        ),
        "primary_source": "wavespeed",
        "fallback_chain": ["wavespeed", "dalle", "pexels"],
        "size_portrait": "768x1344",
        "size_landscape": "1344x768",
    },

    "minimal": {
        "name": "Minimalist Flat Design",
        "prompt_prefix": (
            "minimalist flat design illustration, clean geometric shapes, "
            "bold solid colors, modern graphic design, vector art style, "
            "Scandinavian aesthetic, simple icons, no text, no watermarks"
        ),
        "negative_prompt": (
            "realistic, photo, complex, cluttered, watermark, text, "
            "dark, nsfw, low quality, busy background, gradients"
        ),
        "primary_source": "wavespeed",
        "fallback_chain": ["wavespeed", "dalle", "pexels"],
        "size_portrait": "768x1344",
        "size_landscape": "1344x768",
    },

    "infographic": {
        "name": "Infographic / News Visual",
        "prompt_prefix": (
            "professional infographic illustration, data visualization style, "
            "clean modern editorial design, bold colors, news magazine artwork, "
            "information design, no text, no watermarks"
        ),
        "negative_prompt": (
            "realistic photo, 3d render, watermark, text, logo, "
            "blurry, nsfw, low quality, dark"
        ),
        "primary_source": "wavespeed",
        "fallback_chain": ["wavespeed", "dalle", "pexels"],
        "size_portrait": "768x1344",
        "size_landscape": "1344x768",
    },

    # ── Stock styles ───────────────────────────────────────────────────────
    "stock": {
        "name": "Stock Footage",
        "prompt_prefix": "",
        "negative_prompt": "",
        "primary_source": "pexels",
        "fallback_chain": ["pexels", "pixabay"],
        "size_portrait": "768x1344",
        "size_landscape": "1344x768",
    },

    "news": {
        "name": "News Stock Footage",
        "prompt_prefix": "",
        "negative_prompt": "",
        "primary_source": "pexels",
        "fallback_chain": ["pexels", "pixabay"],
        "size_portrait": "768x1344",
        "size_landscape": "1344x768",
    },
}

# Legacy image_mode → style key mapping (backward compat)
LEGACY_MODE_MAP: Dict[str, str] = {
    "ai": "cartoon",
    "stock": "stock",
    "news": "news",
}

# Styles backed by AI image generation
AI_STYLES = {"cartoon", "cinematic", "watercolor", "anime", "minimal", "infographic"}

# Styles backed by stock footage
STOCK_STYLES = {"stock", "news"}


def resolve_style(image_mode: str = "stock", style: str = "") -> str:
    """Resolve final style key from explicit style or legacy image_mode."""
    if style and style in STYLE_PRESETS:
        return style
    mapped = LEGACY_MODE_MAP.get(image_mode, image_mode)
    return mapped if mapped in STYLE_PRESETS else "stock"


def get_preset(style: str) -> Dict[str, Any]:
    """Return preset dict, defaulting to 'stock' if style unknown."""
    return STYLE_PRESETS.get(style, STYLE_PRESETS["stock"])


def list_styles() -> Dict[str, str]:
    """Return {style_key: display_name} for all presets."""
    return {k: v["name"] for k, v in STYLE_PRESETS.items()}
