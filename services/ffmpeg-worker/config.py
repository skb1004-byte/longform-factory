# -*- coding: utf-8 -*-
"""
LongForm Factory - FFmpeg Worker Configuration Module

Environment variable loading and constant definitions for the video processing pipeline.
Covers video encoding, audio mixing, timing offsets, API credentials, and data paths.
"""

from __future__ import annotations
import os
from pathlib import Path


# ============================================================================
# Video Encoding Settings
# ============================================================================

OUTPUT_RESOLUTION: str = os.getenv("OUTPUT_RESOLUTION", "1920x1080")
VIDEO_CRF: int = int(os.getenv("VIDEO_CRF", "15"))  # 0-51; lower=better
VIDEO_PRESET: str = os.getenv("VIDEO_PRESET", "medium")  # fast, medium, slow
VIDEO_FPS: int = int(os.getenv("VIDEO_FPS", "30"))


# ============================================================================
# Audio Configuration
# ============================================================================

BGM_VOLUME_DEFAULT: float = float(os.getenv("BGM_VOLUME_DEFAULT", "0.10"))
BGM_VOLUME_DURING_VOICE: float = float(os.getenv("BGM_VOLUME_DURING_VOICE", "0.045"))
AUDIO_LOUDNESS_TARGET: int = int(os.getenv("AUDIO_LOUDNESS_TARGET", "-16"))


# ============================================================================
# Timing & Rhythm Settings
# ============================================================================

SCENE_HEAD_PAD_SEC: float = float(os.getenv("SCENE_HEAD_PAD_SEC", "0.15"))
SCENE_TAIL_PAD_SEC: float = float(os.getenv("SCENE_TAIL_PAD_SEC", "0.35"))
SUBTITLE_LEAD_SEC: float = float(os.getenv("SUBTITLE_LEAD_SEC", "0.15"))
SUBTITLE_LEAD_AFTER_SIL_SEC: float = float(os.getenv("SUBTITLE_LEAD_AFTER_SIL_SEC", "0.08"))


# ============================================================================
# File System Paths
# ============================================================================

DATA_DIR: Path = Path(os.getenv("DATA_DIR", "/data"))
TMP_DIR: Path = DATA_DIR / "tmp"
JOBS_DIR: Path = DATA_DIR / "jobs"
OUTPUT_DIR: Path = DATA_DIR / "output"
BGM_DIR: Path = DATA_DIR / "bgm"


def _ensure_directories() -> None:
    """Create necessary directories, handling permission errors gracefully."""
    for directory in [TMP_DIR, JOBS_DIR, OUTPUT_DIR, BGM_DIR]:
        try:
            directory.mkdir(parents=True, exist_ok=True)
        except (PermissionError, OSError):
            # Fail silently in development environments where /data may not exist
            pass


# Ensure paths exist at import time (where possible)
_ensure_directories()


# ============================================================================
# API Keys & External Service Credentials
# ============================================================================

LF_API_KEY: str = os.getenv("LF_API_KEY", "")
PEXELS_API_KEY: str = os.getenv("PEXELS_API_KEY", "")
PIXABAY_API_KEY: str = os.getenv("PIXABAY_API_KEY", "")
ANTHROPIC_API_KEY: str = os.getenv("ANTHROPIC_API_KEY", "")
ANTHROPIC_MODEL: str = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-6")
OPENAI_API_KEY: str = os.getenv("OPENAI_API_KEY", "")
GEMINI_API_KEY: str = os.getenv("GEMINI_API_KEY", "")


# ============================================================================
# Asset Download Limits
# ============================================================================

MAX_DOWNLOAD_MB: int = int(os.getenv("MAX_DOWNLOAD_MB", "120"))
MAX_SOURCE_CLIP_SEC: int = int(os.getenv("MAX_SOURCE_CLIP_SEC", "45"))


# ============================================================================
# Resolution & Codec Utilities
# ============================================================================

def get_resolution(video_type: str) -> tuple[int, int]:
    """
    Determine video frame dimensions based on video type.

    Args:
        video_type: 'longform', 'shorts', or other format identifier

    Returns:
        Tuple of (width, height) in pixels
    """
    if video_type == "shorts":
        return (1080, 1920)  # Vertical format for shorts
    return (1920, 1080)  # Landscape for longform


def get_crf_for_quality(preset_name: str = "standard") -> int:
    """
    Get CRF value based on quality preset.

    Args:
        preset_name: 'low' (30), 'standard' (15), 'high' (8)

    Returns:
        CRF integer value
    """
    presets = {"low": 30, "standard": 15, "high": 8}
    return presets.get(preset_name, VIDEO_CRF)


def get_preset_for_speed(speed: str = "balanced") -> str:
    """
    Get FFmpeg preset based on speed preference.

    Args:
        speed: 'fast', 'balanced', 'slow'

    Returns:
        FFmpeg preset string
    """
    presets = {"fast": "fast", "balanced": "medium", "slow": "slow"}
    return presets.get(speed, VIDEO_PRESET)
