"""ASS subtitle generation and video burn-in."""
from __future__ import annotations
import json
import subprocess
import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

FONTS_DIR = "/usr/share/fonts/opentype/noto"
ASS_STYLE = (
    "Style: Default,Noto Sans CJK KR,{font_size},&H00FFFF00,&H000000FF,"
    "&H00000000,&H80000000,-1,0,0,0,100,100,0,0,3,1,1,2,10,10,{margin_v},1"
)


def compute_subtitle_style(resolution: str = "1920x1080") -> tuple[int, int]:
    """Compute (font_size, margin_v) based on resolution.

    For vertical (1080x1920): font_size ~75px, margin_v ~200px
    For horizontal (1920x1080): font_size ~36px, margin_v ~50px
    """
    try:
        w_str, h_str = resolution.lower().split("x")
        width, height = int(w_str), int(h_str)
    except Exception:
        width, height = 1920, 1080

    is_vertical = height > width

    if is_vertical:
        font_size = max(64, int(width * 0.07))
        margin_v = max(160, int(height * 0.105))
    else:
        font_size = max(36, int(height * 0.055))
        margin_v = max(50, int(height * 0.065))

    return font_size, margin_v


def create_ass_from_timestamps(
    ts_path: Path,
    output_path: Path,
    resolution: str = "1920x1080",
    lead_sec: float = 0.15,
) -> bool:
    """Generate ASS subtitle from whisper timestamps.json."""
    if not ts_path or not ts_path.exists():
        return False

    try:
        data = json.loads(ts_path.read_text(encoding="utf-8"))
    except Exception as e:
        logger.error(f"[subtitle] cannot read timestamps: {e}")
        return False

    segments = data.get("segments") or []
    if not segments:
        logger.warning("[subtitle] no segments in timestamps")
        return False

    font_size, margin_v = compute_subtitle_style(resolution)
    w, h = (int(x) for x in resolution.lower().split("x"))

    def _t(sec: float) -> str:
        """Convert seconds to ASS timestamp format: h:mm:ss.cc"""
        sec = max(0.0, sec - lead_sec)
        h_ = int(sec // 3600)
        m_ = int((sec % 3600) // 60)
        s_ = sec % 60
        return f"{h_:d}:{m_:02d}:{s_:05.2f}"

    lines = [
        "[Script Info]",
        "ScriptType: v4.00+",
        f"PlayResX: {w}",
        f"PlayResY: {h}",
        "",
        "[V4+ Styles]",
        "Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding",
        ASS_STYLE.format(font_size=font_size, margin_v=margin_v),
        "",
        "[Events]",
        "Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text",
    ]

    for seg in segments:
        start = seg.get("start", 0)
        end = seg.get("end", start + 2)
        text = seg.get("text", "").strip().replace("\n", " ")
        if not text:
            continue
        # limit line length to 25 chars
        if len(text) > 25:
            mid = len(text) // 2
            sp = text.rfind(" ", 0, mid)
            if sp > 0:
                text = text[:sp] + "\\N" + text[sp + 1 :]
        lines.append(f"Dialogue: 0,{_t(start)},{_t(end)},Default,,0,0,0,,{text}")

    try:
        output_path.write_text("\n".join(lines), encoding="utf-8")
        logger.info(f"[subtitle] ASS created: {len(segments)} segments → {output_path.name}")
        return True
    except Exception as e:
        logger.error(f"[subtitle] ASS write failed: {e}")
        return False


def burn_subtitles(
    input_video: Path,
    ass_path: Path,
    output_video: Path,
    resolution: str = "1920x1080",
) -> bool:
    """Burn ASS subtitles into video with fontsdir."""
    if not ass_path.exists():
        return False

    subtitle_filter = f"ass={ass_path}:fontsdir={FONTS_DIR}"
    cmd = [
        "ffmpeg",
        "-i",
        str(input_video),
        "-vf",
        subtitle_filter,
        "-c:v",
        "libx264",
        "-preset",
        "medium",
        "-crf",
        "15",
        "-c:a",
        "copy",
        "-y",
        str(output_video),
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300.0)
        if result.returncode == 0 and output_video.exists():
            logger.info(f"[subtitle] ASS burn-in OK → {output_video.name}")
            return True
        logger.warning(f"[subtitle] ASS burn-in failed: {result.stderr[-200:]}")
        return False
    except Exception as e:
        logger.error(f"[subtitle] burn-in error: {e}")
        return False


def add_subtitles_to_video(
    input_video: Path,
    ts_path: Optional[Path],
    output_video: Path,
    resolution: str = "1920x1080",
) -> bool:
    """Full subtitle pipeline: timestamps → ASS → burn-in.

    If no timestamps provided, copy video as-is.
    If ASS creation fails, fallback to no subtitles.
    """
    if not ts_path or not ts_path.exists():
        logger.info("[subtitle] no timestamps, skipping subtitles")
        import shutil

        shutil.copy2(input_video, output_video)
        return True

    ass_path = output_video.parent / f"{output_video.stem}_sub.ass"

    if not create_ass_from_timestamps(ts_path, ass_path, resolution):
        logger.warning("[subtitle] ASS creation failed, copying video as-is")
        import shutil

        shutil.copy2(input_video, output_video)
        return True

    if not burn_subtitles(input_video, ass_path, output_video, resolution):
        logger.warning("[subtitle] burn-in failed, output without subtitles")
        import shutil

        shutil.copy2(input_video, output_video)

    return True
