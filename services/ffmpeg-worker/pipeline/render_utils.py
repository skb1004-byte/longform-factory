# -*- coding: utf-8 -*-
"""
LongForm Factory - Render Utility Aliases

Async wrappers and fallback helpers used by app_v17.py.
Separated to keep render.py under 300 lines.
"""

from __future__ import annotations
import subprocess
import asyncio
from pathlib import Path
from typing import List, TYPE_CHECKING

import logging
logger = logging.getLogger(__name__)


async def prepare_clips(
    job_id: str,
    scenes: list,
    output_dir: Path,
    W: int = 1920,
    H: int = 1080,
) -> List[Path]:
    """Async wrapper around render.prepare_clips_for_longform."""
    from pipeline.render import prepare_clips_for_longform
    loop = asyncio.get_event_loop()
    video_type = "shorts" if H > W else "longform"
    return await loop.run_in_executor(
        None,
        lambda: prepare_clips_for_longform(scenes, video_type, output_dir),
    )


def make_fallback_clip(
    index: int,
    duration: float,
    output_path: Path,
    keyword: str = "",
    W: int = 1920,
    H: int = 1080,
) -> bool:
    """Create a solid-color fallback clip with optional keyword text overlay."""
    color = "0x1a1a2e"
    safe_kw = keyword.replace("'", "").replace(":", "")[:30]
    drawtext = (
        f"drawtext=text='{safe_kw}':fontcolor=white:fontsize=48:"
        f"x=(w-text_w)/2:y=(h-text_h)/2:box=1:boxcolor=black@0.5:boxborderw=10"
    ) if safe_kw else "null"
    vf = "format=yuv420p" if not safe_kw else f"{drawtext},format=yuv420p"
    cmd = [
        "ffmpeg", "-y",
        "-f", "lavfi",
        "-i", f"color=c={color}:s={W}x{H}:rate=30",
        "-t", str(duration),
        "-vf", vf,
        "-c:v", "libx264", "-preset", "ultrafast",
        "-movflags", "+faststart", "-an",
        str(output_path),
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, timeout=30)
        return result.returncode == 0 and Path(output_path).exists()
    except Exception as e:
        logger.warning(f"make_fallback_clip failed: {e}")
        return False
