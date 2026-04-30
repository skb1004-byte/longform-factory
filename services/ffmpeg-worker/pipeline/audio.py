# -*- coding: utf-8 -*-
"""
LongForm Factory - Audio Mixing Pipeline

TTS narration + BGM ducking with loudnorm, fallback to simple amix.
"""

from __future__ import annotations
import subprocess
import logging
import random
from pathlib import Path
from typing import Optional

from config import (
    VIDEO_PRESET, BGM_DIR, BGM_VOLUME_DURING_VOICE, BGM_VOLUME_DEFAULT
)

logger = logging.getLogger(__name__)


def _get_duration(path: Path) -> Optional[float]:
    """Query audio/video duration via ffprobe."""
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
             "-of", "csv=p=0", str(path)],
            capture_output=True, text=True, timeout=10
        )
        raw = result.stdout.strip()
        if raw and raw not in ("N/A", ""):
            return float(raw)
    except Exception:
        pass
    return None


def get_random_bgm() -> Optional[Path]:
    """Select random BGM file from /data/bgm/."""
    try:
        bgm_files = list(BGM_DIR.glob("*.mp3")) + list(BGM_DIR.glob("*.wav"))
        if bgm_files:
            selected = random.choice(bgm_files)
            logger.info(f"selected BGM: {selected.name}")
            return selected
    except Exception:
        pass
    logger.warning("no BGM files available")
    return None


def _run_ffmpeg(cmd: list, timeout: float = 300.0) -> bool:
    """Execute ffmpeg command, log stderr on error."""
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        if result.returncode != 0:
            logger.error(f"ffmpeg error: {result.stderr[-300:]}")
            return False
        return True
    except subprocess.TimeoutExpired:
        logger.error(f"ffmpeg timeout ({timeout}s)")
        return False
    except Exception as e:
        logger.error(f"ffmpeg exception: {e}")
        return False


def _calc_output_duration(
    tts_path: Optional[Path],
    video_path: Path
) -> list:
    """
    Calculate -t trim argument for output video.

    Decision order:
    1. video_dur > tts_dur * 5: demuxer produced bogus timestamps, trust TTS.
    2. tts_dur < video_dur * 0.3: TTS encoding failure, use video_dur.
    3. Normal: use tts_dur + 0.5s tail buffer.
    """
    if not tts_path or not tts_path.exists():
        return []
    tts_dur = _get_duration(tts_path)
    if not tts_dur or tts_dur <= 0:
        return []
    video_dur = _get_duration(video_path)
    if video_dur and video_dur > 0:
        if video_dur > tts_dur * 5.0:
            # Concat demuxer stream-copy produced inflated timestamps — trust TTS
            effective_dur = tts_dur
            logger.warning(
                f"video ({video_dur:.1f}s) >> TTS ({tts_dur:.1f}s) by >5x "
                f"— demuxer timestamp anomaly, using TTS"
            )
        elif tts_dur < video_dur * 0.3:
            # TTS suspiciously short — likely encoding failure, use video
            effective_dur = video_dur
            logger.warning(
                f"TTS ({tts_dur:.2f}s) << video ({video_dur:.2f}s) "
                f"— using video duration"
            )
        else:
            effective_dur = tts_dur
            logger.info(f"TTS trim: {tts_dur:.2f}s + 0.5s")
    else:
        effective_dur = tts_dur
        logger.info(f"TTS trim: {tts_dur:.2f}s + 0.5s")
    return ["-t", str(round(effective_dur + 0.5, 2))]


def mix_audio(
    video_path: Path,
    tts_path: Optional[Path],
    bgm_path: Optional[Path],
    bgm_volume: float,
    output_path: Path
) -> bool:
    """
    Mix TTS narration + BGM with loudnorm. If loudnorm fails, fallback to simple amix.

    Args:
        video_path: input video (video stream)
        tts_path: TTS audio file (narration)
        bgm_path: background music file
        bgm_volume: BGM volume factor (0.0-1.0)
        output_path: output video with mixed audio

    Returns:
        True if successful
    """
    has_tts = tts_path and tts_path.exists() and tts_path.stat().st_size > 1024
    has_bgm = bgm_path and bgm_path.exists()

    # Calculate trim: guard against abnormally short TTS
    tts_trim_args = _calc_output_duration(tts_path if has_tts else None, video_path)

    if has_tts and has_bgm:
        # TTS + BGM mix with ducking
        filter_complex = (
            f"[1:a]loudnorm=I=-16:TP=-1.5:LRA=11,aformat=sample_rates=48000:channel_layouts=stereo[tts];"
            f"[2:a]volume={BGM_VOLUME_DURING_VOICE},aformat=sample_rates=48000:channel_layouts=stereo[bgm];"
            f"[tts][bgm]amix=inputs=2:duration=longest:dropout_transition=2[aout]"
        )
        cmd = [
            "ffmpeg", "-i", str(video_path),
            "-i", str(tts_path), "-i", str(bgm_path),
            "-filter_complex", filter_complex,
            "-map", "0:v", "-map", "[aout]",
            *tts_trim_args,
            "-c:v", "copy", "-c:a", "aac", "-ac", "2", "-b:a", "192k",
            "-shortest", "-y", str(output_path)
        ]
    elif has_tts:
        # TTS only with loudnorm
        filter_complex = "[1:a]loudnorm=I=-16:TP=-1.5:LRA=11[aout]"
        cmd = [
            "ffmpeg", "-i", str(video_path), "-i", str(tts_path),
            "-filter_complex", filter_complex,
            "-map", "0:v", "-map", "[aout]",
            *tts_trim_args,
            "-c:v", "copy", "-c:a", "aac", "-ac", "2", "-b:a", "192k",
            "-shortest", "-y", str(output_path)
        ]
    elif has_bgm:
        # BGM only
        cmd = [
            "ffmpeg", "-i", str(video_path), "-i", str(bgm_path),
            "-filter_complex", f"[1:a]volume={bgm_volume}[aout]",
            "-map", "0:v", "-map", "[aout]",
            "-c:v", "copy", "-c:a", "aac", "-ac", "2", "-b:a", "192k",
            "-y", str(output_path)
        ]
    else:
        # No audio: copy video only
        cmd = ["ffmpeg", "-i", str(video_path), "-c", "copy", "-y", str(output_path)]

    success = _run_ffmpeg(cmd)
    if not success and has_tts:
        # Fallback: simple amix without loudnorm
        logger.warning("loudnorm failed, trying simple amix fallback")
        simple_cmd = [
            "ffmpeg", "-i", str(video_path), "-i", str(tts_path),
            "-map", "0:v", "-map", "1:a",
            *tts_trim_args,
            "-c:v", "copy", "-c:a", "aac", "-ac", "2", "-b:a", "192k",
            "-shortest", "-y", str(output_path)
        ]
        return _run_ffmpeg(simple_cmd)
    return success
