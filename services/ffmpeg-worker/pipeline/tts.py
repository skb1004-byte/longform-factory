# -*- coding: utf-8 -*-
"""
TTS generation and scene duration synchronization.

Orchestrates Edge TTS service calls, manages audio assets, and synchronizes
scene durations based on actual TTS audio length via timestamp analysis.
"""

from __future__ import annotations
import json
import shutil
import logging
from pathlib import Path
from typing import List, Optional, Dict, Any
import httpx
from models import Scene

logger = logging.getLogger(__name__)

TTS_SERVICE_URL: str = "http://lf2_tts:8001/tts"
TMP_DIR: Path = Path("/data/tmp")


async def generate_tts(
    job_id: str,
    scenes: List[Scene]
) -> Dict[str, Any]:
    """
    Call lf2_tts service to generate TTS audio and timestamps.

    Args:
        job_id: Unique job identifier
        scenes: List of Scene objects with narration text

    Returns:
        Dict with keys: 'ok' (bool), 'mp3_path', 'ts_path', 'error' (if failed)
    """
    mp3_path: Path = TMP_DIR / f"{job_id}.mp3"
    ts_path: Path = TMP_DIR / f"{job_id}_timestamps.json"

    # Reuse if both exist and valid
    if mp3_path.exists() and mp3_path.stat().st_size > 1024:
        if ts_path.exists() and _is_valid_timestamps(ts_path):
            logger.info(
                f"[tts] reusing cached TTS: {mp3_path.stat().st_size//1024}KB"
            )
            return {"ok": True, "mp3_path": mp3_path, "ts_path": ts_path}

    # Collect narration text
    narration_parts: List[str] = []
    seen: set = set()

    for s in scenes:
        text: str = (s.narration or s.description or s.keyword or "").strip()
        if text and text not in seen:
            narration_parts.append(text)
            seen.add(text)

    if not narration_parts:
        return {"ok": False, "error": "no narration text"}

    full_text: str = " ".join(narration_parts)
    logger.info(
        f"[tts] generating TTS: {len(full_text)} chars, {len(narration_parts)} scenes"
    )

    try:
        async with httpx.AsyncClient(timeout=300.0) as client:
            resp = await client.post(
                TTS_SERVICE_URL,
                json={
                    "text": full_text,
                    "filename": job_id,
                    "engine": "edge",
                    "edge_voice": "ko-KR-SunHiNeural",
                    "edge_rate": "+15%",
                    "preprocess": True,
                }
            )

            if resp.status_code != 200:
                return {"ok": False, "error": f"TTS HTTP {resp.status_code}"}

            data: dict = resp.json()

        tts_file: str = data.get("file_path", "")
        ts_file: str = data.get("timestamps_path", "")

        # Copy MP3 to standard location
        if tts_file and Path(tts_file).exists():
            if Path(tts_file).resolve() != mp3_path.resolve():
                shutil.copy2(tts_file, mp3_path)
            logger.info(f"[tts] mp3 saved: {mp3_path.stat().st_size//1024}KB")
        else:
            return {"ok": False, "error": "mp3 missing from TTS response"}

        # Copy timestamps if available
        if ts_file and Path(ts_file).exists():
            if Path(ts_file).resolve() != ts_path.resolve():
                shutil.copy2(ts_file, ts_path)
            return {"ok": True, "mp3_path": mp3_path, "ts_path": ts_path}
        else:
            logger.warning("[tts] timestamps missing - ASS subtitles won't be generated")
            return {"ok": True, "mp3_path": mp3_path, "ts_path": None}

    except Exception as e:
        logger.error(f"[tts] generation failed: {e}", exc_info=True)
        return {"ok": False, "error": str(e)}


def sync_scene_durations(
    scenes: List[Scene],
    ts_path: Optional[Path]
) -> List[Scene]:
    """
    Update scene duration_seconds based on TTS timestamps.

    Distributes total TTS duration proportionally across scenes
    based on their narration character count.

    Args:
        scenes: List of Scene objects to update
        ts_path: Path to timestamps JSON file

    Returns:
        Updated list of Scene objects
    """
    if not ts_path or not ts_path.exists():
        return scenes

    try:
        data: dict = json.loads(ts_path.read_text(encoding="utf-8"))
    except Exception as e:
        logger.warning(f"[tts] cannot read timestamps: {e}")
        return scenes

    segments: list = data.get("segments") or []
    if not segments:
        return scenes

    # Calculate total TTS duration from segments
    total_dur: float = max((s.get("end", 0) for s in segments), default=0)
    if total_dur <= 0:
        return scenes

    # Distribute duration by narration character count
    total_chars: int = sum(
        max(len(s.narration or ""), 1) for s in scenes
    )
    remaining: float = total_dur

    for i, scene in enumerate(scenes):
        chars: int = max(len(scene.narration or ""), 1)

        if i == len(scenes) - 1:
            # Last scene: use remaining duration
            dur: float = max(remaining, 2.0)
        else:
            # Proportional allocation by character count
            dur = max((chars / total_chars) * total_dur, 2.0)
            remaining -= dur

        scene.duration_seconds = round(dur, 2)

    logger.info(
        f"[tts] duration sync: total={total_dur:.1f}s across {len(scenes)} scenes"
    )
    return scenes


def _is_valid_timestamps(ts_path: Path) -> bool:
    """
    Validate timestamps JSON file for required structure.

    Args:
        ts_path: Path to timestamps JSON

    Returns:
        True if file has valid segments array
    """
    try:
        data: dict = json.loads(ts_path.read_text(encoding="utf-8"))
        return bool(data.get("segments"))
    except Exception:
        return False
