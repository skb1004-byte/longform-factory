"""ASS subtitle generation and video burn-in."""
from __future__ import annotations
import json
import re
import subprocess
import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

FONTS_DIR = "/usr/share/fonts/opentype/noto"
FONTS_DIR_TRUETYPE = "/usr/share/fonts/truetype/noto"

# BorderStyle=1 outline (not opaque box), white+thick-black-outline
ASS_STYLE = (
    "Style: Default,Noto Sans CJK KR,{font_size},&H00FFFFFF,&H000000FF,"
    "&H00000000,&H80000000,-1,0,0,0,100,100,0,0,1,3,1,2,10,10,{margin_v},1"
)


def _find_fonts_dir() -> str:
    """Return usable fonts directory."""
    import os
    for d in [FONTS_DIR, FONTS_DIR_TRUETYPE, "/usr/share/fonts", "/usr/local/share/fonts"]:
        if os.path.isdir(d):
            return d
    return FONTS_DIR


def compute_subtitle_style(resolution: str = "1920x1080") -> tuple[int, int]:
    """Return (font_size, margin_v) for given resolution."""
    try:
        w_str, h_str = resolution.lower().split("x")
        width, height = int(w_str), int(h_str)
    except Exception:
        width, height = 1920, 1080

    is_vertical = height > width
    if is_vertical:
        font_size = max(68, int(width * 0.07))
        margin_v = max(180, int(height * 0.105))
    else:
        font_size = max(40, int(height * 0.060))
        margin_v = max(55, int(height * 0.065))

    return font_size, margin_v


def _split_korean_line(text: str, max_chars: int = 18) -> str:
    """Split at space nearest midpoint (eojeol boundary). Fallback: midpoint cut."""
    if len(text) <= max_chars:
        return text
    mid = len(text) // 2
    # Find space closest to midpoint — search left then right
    left = text.rfind(" ", 0, mid + 1)
    right = text.find(" ", mid)
    if left < 0 and right < 0:
        # No spaces (pure Korean): split at midpoint
        return text[:mid] + "\\N" + text[mid:]
    elif left < 0:
        sp = right
    elif right < 0:
        sp = left
    else:
        sp = left if (mid - left) <= (right - mid) else right
    return text[:sp] + "\\N" + text[sp + 1:]


def _merge_short_segments(segments: list, min_dur: float = 0.5) -> list:
    """Merge segments < min_dur with next segment to prevent subtitle flicker."""
    if not segments:
        return segments
    merged: list = []
    i = 0
    while i < len(segments):
        seg = dict(segments[i])
        dur = seg.get("end", 0) - seg.get("start", 0)
        if dur < min_dur and i + 1 < len(segments):
            nxt = segments[i + 1]
            seg["text"] = (seg.get("text", "").strip() + " " + nxt.get("text", "").strip()).strip()
            seg["end"] = nxt.get("end", seg["end"])
            i += 2
        else:
            i += 1
        merged.append(seg)
    return merged


_FMT_STYLE = (
    "Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, "
    "OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, "
    "ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, "
    "Alignment, MarginL, MarginR, MarginV, Encoding"
)
_FMT_EVENT = "Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text"


def _ass_header(w: int, h: int, font_size: int, margin_v: int) -> list[str]:
    """Build ASS file header lines."""
    return [
        "[Script Info]", "ScriptType: v4.00+",
        f"PlayResX: {w}", f"PlayResY: {h}", "",
        "[V4+ Styles]", _FMT_STYLE,
        ASS_STYLE.format(font_size=font_size, margin_v=margin_v), "",
        "[Events]", _FMT_EVENT,
    ]


def _sec_to_ass(sec: float, lead: float = 0.0) -> str:
    """Convert seconds to ASS timestamp h:mm:ss.cc"""
    sec = max(0.0, sec - lead)
    h_ = int(sec // 3600)
    m_ = int((sec % 3600) // 60)
    s_ = sec % 60
    return f"{h_:d}:{m_:02d}:{s_:05.2f}"


def create_ass_from_timestamps(
    ts_path: Path,
    output_path: Path,
    resolution: str = "1920x1080",
    lead_sec: float = 0.15,
) -> bool:
    """Generate ASS from Whisper timestamps.json with short-segment merging."""
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

    # Merge flicker-prone short segments
    segments = _merge_short_segments(segments, min_dur=0.5)

    font_size, margin_v = compute_subtitle_style(resolution)
    w, h = (int(x) for x in resolution.lower().split("x"))
    lines = _ass_header(w, h, font_size, margin_v)

    for seg in segments:
        start = seg.get("start", 0)
        end = seg.get("end", start + 2)
        text = seg.get("text", "").strip().replace("\n", " ")
        if not text:
            continue
        text = _split_korean_line(text)
        lines.append(
            f"Dialogue: 0,{_sec_to_ass(start, lead_sec)},{_sec_to_ass(end, lead_sec)},"
            f"Default,,0,0,0,,{text}"
        )

    try:
        output_path.write_text("\n".join(lines), encoding="utf-8")
        logger.info(f"[subtitle] ASS from timestamps: {len(segments)} segs → {output_path.name}")
        return True
    except Exception as e:
        logger.error(f"[subtitle] ASS write failed: {e}")
        return False


def create_ass_from_narration(
    narration: str,
    tts_duration: float,
    output_path: Path,
    resolution: str = "1920x1080",
) -> bool:
    """Fallback: generate ASS by distributing narration text across TTS duration.

    Used when Whisper timestamps are unavailable. Splits at sentence boundaries
    and assigns timing proportional to character count.
    """
    if not narration or tts_duration <= 0:
        return False

    # Split at sentence-ending punctuation
    phrases = re.split(r"(?<=[.!?。])\s+", narration.strip())
    segments_text: list[str] = []
    for phrase in phrases:
        phrase = phrase.strip()
        if not phrase:
            continue
        if len(phrase) > 24:
            # Further split at commas for very long phrases
            parts = re.split(r"(?<=[,，])\s*", phrase)
            segments_text.extend(p.strip() for p in parts if p.strip())
        else:
            segments_text.append(phrase)

    if not segments_text:
        return False

    total_chars = sum(len(s) for s in segments_text)
    lead_in = 0.3
    usable = max(tts_duration - lead_in, 1.0)

    timestamp_segs: list[dict] = []
    cur = lead_in
    for text in segments_text:
        ratio = len(text) / total_chars if total_chars > 0 else 1 / len(segments_text)
        dur = usable * ratio
        timestamp_segs.append({"start": cur, "end": cur + dur, "text": text})
        cur += dur

    font_size, margin_v = compute_subtitle_style(resolution)
    w, h = (int(x) for x in resolution.lower().split("x"))
    lines = _ass_header(w, h, font_size, margin_v)

    for seg in timestamp_segs:
        text = _split_korean_line(seg["text"])
        lines.append(
            f"Dialogue: 0,{_sec_to_ass(seg['start'])},{_sec_to_ass(seg['end'])},"
            f"Default,,0,0,0,,{text}"
        )

    try:
        output_path.write_text("\n".join(lines), encoding="utf-8")
        logger.info(f"[subtitle] narration ASS: {len(timestamp_segs)} segs → {output_path.name}")
        return True
    except Exception as e:
        logger.error(f"[subtitle] narration ASS write failed: {e}")
        return False


def burn_subtitles(
    input_video: Path,
    ass_path: Path,
    output_video: Path,
    resolution: str = "1920x1080",
) -> bool:
    """Burn ASS subtitles into video."""
    if not ass_path.exists():
        return False
    subtitle_filter = f"ass={ass_path}:fontsdir={_find_fonts_dir()}"
    cmd = [
        "ffmpeg", "-i", str(input_video),
        "-vf", subtitle_filter,
        "-c:v", "libx264", "-preset", "medium", "-crf", "15",
        "-c:a", "copy", "-y", str(output_video),
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300.0)
        if result.returncode == 0 and output_video.exists():
            logger.info(f"[subtitle] burn-in OK → {output_video.name}")
            return True
        logger.warning(f"[subtitle] burn-in failed: {result.stderr[-200:]}")
        return False
    except Exception as e:
        logger.error(f"[subtitle] burn-in error: {e}")
        return False


def add_subtitles_to_video(
    input_video: Path,
    ts_path: Optional[Path],
    output_video: Path,
    resolution: str = "1920x1080",
    narration: Optional[str] = None,
    tts_duration: Optional[float] = None,
) -> bool:
    """Full subtitle pipeline: timestamps (or narration fallback) → ASS → burn-in.

    Priority:
      1. Whisper timestamps.json (best quality)
      2. narration + tts_duration (char-proportional fallback)
      3. No subtitles — copy video as-is
    """
    import shutil

    ass_path = output_video.parent / f"{output_video.stem}_sub.ass"
    ass_ok = False

    # Try Whisper timestamps first
    if ts_path and ts_path.exists():
        ass_ok = create_ass_from_timestamps(ts_path, ass_path, resolution)

    # Fallback: generate from narration text
    if not ass_ok and narration and tts_duration:
        logger.info("[subtitle] Whisper timestamps unavailable — using narration fallback")
        ass_ok = create_ass_from_narration(narration, tts_duration, ass_path, resolution)

    if not ass_ok:
        logger.info("[subtitle] no subtitle source — copying video as-is")
        shutil.copy2(input_video, output_video)
        return True

    if not burn_subtitles(input_video, ass_path, output_video, resolution):
        logger.warning("[subtitle] burn-in failed — output without subtitles")
        shutil.copy2(input_video, output_video)

    return True
