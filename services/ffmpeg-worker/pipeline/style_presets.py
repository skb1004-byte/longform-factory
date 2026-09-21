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
            "blurry, dark, violent, nsfw, low quality, deformed, ugly, collage, split screen, multiple panels, picture frame, gallery wall, poster on wall, montage, grid of images, screen within screen, duplicated subject, cropped subject"
        ),
        "primary_source": "wavespeed",
        "fallback_chain": ["local_sdxl", "wavespeed", "dalle", "pexels"],
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
            "low quality, blurry, oversaturated, nsfw, deformed, collage, split screen, multiple panels, picture frame, gallery wall, poster on wall, montage, grid of images, screen within screen, duplicated subject, cropped subject"
        ),
        "primary_source": "local_sdxl",   # wavespeed 는 401(키 무효)로 죽어 있다.
        # primary 로 두면 씬마다 먼저 시도했다 실패하고 로컬로 내려와,
        # 소재 수집이 9분 넘게 걸렸다(실측). 로컬 GPU 가 무료·즉시이므로
        # 그쪽을 먼저 쓰고, wavespeed 는 키가 복구되면 폴백으로 살아난다.
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
            "dark, violent, nsfw, low quality, collage, split screen, multiple panels, picture frame, gallery wall, poster on wall, montage, grid of images, screen within screen, duplicated subject, cropped subject"
        ),
        "primary_source": "wavespeed",
        "fallback_chain": ["local_sdxl", "wavespeed", "dalle", "pexels"],
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
            "nsfw, low quality, blurry, deformed, collage, split screen, multiple panels, picture frame, gallery wall, poster on wall, montage, grid of images, screen within screen, duplicated subject, cropped subject"
        ),
        "primary_source": "wavespeed",
        "fallback_chain": ["local_sdxl", "wavespeed", "dalle", "pexels"],
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
            "dark, nsfw, low quality, busy background, gradients, collage, split screen, multiple panels, picture frame, gallery wall, poster on wall, montage, grid of images, screen within screen, duplicated subject, cropped subject"
        ),
        "primary_source": "wavespeed",
        "fallback_chain": ["local_sdxl", "wavespeed", "dalle", "pexels"],
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
            "blurry, nsfw, low quality, dark, collage, split screen, multiple panels, picture frame, gallery wall, poster on wall, montage, grid of images, screen within screen, duplicated subject, cropped subject"
        ),
        "primary_source": "wavespeed",
        "fallback_chain": ["local_sdxl", "wavespeed", "dalle", "pexels"],
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


# 이미지 스타일에 맞는 나레이션 문체.
#
# 화면과 말투가 따로 놀면 시청자가 위화감을 느낀다. 카툰 그림에 다큐 내레이션이
# 깔리거나, 수채화 화면에 속사포 문장이 얹히는 식이다.
# 장르(교육/쇼츠/뉴스)가 '무엇을 말할지'를 정한다면, 이미지 스타일은
# '어떻게 말할지'를 정한다. 둘은 겹치지 않으므로 함께 쓴다.
IMAGE_STYLE_VOICE = {
    "cinematic": (
        "장면이 눈앞에 그려지도록 묘사한다. 빛, 질감, 공기의 결을 문장에 담고 "
        "감정이 쌓이도록 호흡을 길게 가져간다. 과장된 감탄사는 쓰지 않는다."
    ),
    "cartoon": (
        "가볍고 친근한 구어체로 쓴다. 문장을 짧게 끊고, 말 거는 듯한 어투를 쓴다. "
        "'~거든요', '~더라고요' 같은 일상 표현을 허용한다. 딱딱한 한자어는 피한다."
    ),
    "watercolor": (
        "잔잔하고 서정적으로 쓴다. 단정하기보다 여운을 남기고, 문장 사이에 "
        "여백을 둔다. 수치 나열보다 인상과 분위기를 앞세운다."
    ),
    "anime": (
        "활기차고 속도감 있게 쓴다. 짧은 문장을 연달아 붙이고, 장면이 전환되는 "
        "느낌을 준다. 감탄과 반전을 한두 번 허용한다."
    ),
    "minimal": (
        "군더더기를 모두 덜어낸다. 한 문장에 한 가지만 담고, 수식어를 최소로 쓴다. "
        "설명하지 말고 사실만 남긴다."
    ),
    "infographic": (
        "숫자와 비교로 설명한다. '몇 개', '몇 배', '언제부터' 같은 구체값을 넣고, "
        "순서를 매겨 단계별로 풀어낸다. 감정 표현은 쓰지 않는다."
    ),
    "stock": (
        "사실 위주로 담백하게 쓴다. 보도문에 가깝게 주어와 서술어를 분명히 하고, "
        "추측은 추측이라고 밝힌다."
    ),
    "news": (
        "보도체로 쓴다. 언제·어디서·누가·무엇을 앞에 두고, 출처가 있는 내용과 "
        "해석을 구분한다. 감정적 수식은 쓰지 않는다."
    ),
}


def image_style_voice(style: str) -> str:
    """이미지 스타일에 맞는 나레이션 문체 지시. 없으면 빈 문자열."""
    return IMAGE_STYLE_VOICE.get((style or "").strip().lower(), "")
