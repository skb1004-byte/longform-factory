# -*- coding: utf-8 -*-
"""
LongForm Factory - Video Rendering Pipeline

Ken Burns clip generation, clip normalization, xfade concat with fallback,
and duration validation.
"""

from __future__ import annotations
import subprocess
import logging
import shutil
from pathlib import Path
from typing import Optional, List

from config import (
    VIDEO_PRESET, VIDEO_CRF, TMP_DIR, OUTPUT_DIR,
    SCENE_HEAD_PAD_SEC, SCENE_TAIL_PAD_SEC, get_resolution
)

logger = logging.getLogger(__name__)

# Ken Burns zoom-pan presets: 6 variations for temporal rhythm
KB_PRESETS = [
    "zoompan=z='min(zoom+{kb_speed},1.06)':x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)':d={fps_d}:s={W}x{H}:fps=30",
    "zoompan=z='if(eq(on,1),1.5,max(zoom-{kb_speed},1.0))':x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)':d={fps_d}:s={W}x{H}:fps=30",
    "zoompan=z='1.3':x='if(lte(on,1),0,min(x+3,iw*0.25))':y='ih/2-(ih/zoom/2)':d={fps_d}:s={W}x{H}:fps=30",
    "zoompan=z='1.3':x='if(lte(on,1),iw*0.25,max(x-3,0))':y='ih/2-(ih/zoom/2)':d={fps_d}:s={W}x{H}:fps=30",
    "zoompan=z='min(zoom+{kb_speed},1.05)':x='iw/2-(iw/zoom/2)':y='if(lte(on,1),0,min(y+2,ih*0.2))':d={fps_d}:s={W}x{H}:fps=30",
    "zoompan=z='min(zoom+{kb_speed_hi},1.06)':x='if(lte(on,1),iw*0.1,max(x-1,0))':y='ih-ih/zoom':d={fps_d}:s={W}x{H}:fps=30",
]

# Template presets: saturation, contrast, vignette
TEMPLATE_CONFIGS = {
    "info":   {"saturation": 1.25, "contrast": 1.10, "vignette": "PI/5"},
    "news":   {"saturation": 1.05, "contrast": 1.15, "vignette": "PI/6"},
    "edu":    {"saturation": 1.15, "contrast": 1.08, "vignette": "PI/5"},
    "ad":     {"saturation": 1.40, "contrast": 1.22, "vignette": "PI/4"},
    "story":  {"saturation": 1.20, "contrast": 1.15, "vignette": "PI/4"},
}


def get_video_duration(video_path: Path) -> Optional[float]:
    """Query video duration via ffprobe. Returns None if N/A or missing."""
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
             "-of", "csv=p=0", str(video_path)],
            capture_output=True, text=True, timeout=10
        )
        raw = result.stdout.strip()
        if raw and raw not in ("N/A", ""):
            return float(raw)
    except Exception:
        pass
    return None


def _is_valid_clip(clip_path: Path) -> bool:
    """Validate clip: exists, >4KB, has valid video stream."""
    try:
        if not clip_path.exists() or clip_path.stat().st_size < 4096:
            return False
        result = subprocess.run(
            ["ffprobe", "-v", "error", "-select_streams", "v:0",
             "-show_entries", "stream=codec_name", "-of", "default=nw=1:nk=1",
             str(clip_path)],
            capture_output=True, text=True, timeout=20
        )
        return result.returncode == 0 and bool(result.stdout.strip())
    except Exception:
        return False


def normalize_clip(clip_path: Path, timeout: float = 45.0) -> Path:
    """Re-encode clip to fix Duration:N/A (concat filter incompatibility)."""
    dur = get_video_duration(clip_path)
    if dur is not None and dur > 0:
        return clip_path
    norm_path = clip_path.with_name(clip_path.stem + "_norm.mp4")
    if norm_path.exists():
        return norm_path
    cmd = [
        "ffmpeg", "-i", str(clip_path),
        "-c:v", "libx264", "-preset", "ultrafast", "-crf", "18",
        "-movflags", "+faststart", "-an", "-y", str(norm_path)
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if result.returncode == 0:
        logger.debug(f"normalize_clip OK: {clip_path.name}")
        return norm_path
    logger.warning(f"normalize_clip failed: {clip_path.name}")
    return clip_path


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


def xfade_batch(clip_paths: list, output: Path, transition: str = "fade") -> bool:
    """Concatenate clips via concat filter (handles Duration:N/A). Fallback to demuxer."""
    original_n = len(clip_paths)
    clip_paths = [cp for cp in clip_paths if _is_valid_clip(cp)]
    dropped = original_n - len(clip_paths)
    if dropped:
        logger.warning(f"xfade_batch: dropped {dropped} invalid clips ({len(clip_paths)} remain)")

    if len(clip_paths) == 0:
        logger.error("xfade_batch: no valid clips")
        return False
    if len(clip_paths) == 1:
        shutil.copy(str(clip_paths[0]), str(output))
        return True

    # Method 1: normalize Duration:N/A clips, then concat filter
    clip_paths = [normalize_clip(cp) for cp in clip_paths]
    inputs = []
    for cp in clip_paths:
        inputs += ["-i", str(cp)]

    n = len(clip_paths)
    # Normalize PTS, concat video
    vparts = "".join(f"[{i}:v:0]setpts=PTS-STARTPTS[v{i}];" for i in range(n))
    vconcat = "".join(f"[v{i}]" for i in range(n))
    fg = f"{vparts}{vconcat}concat=n={n}:v=1:a=0[vout]"

    cmd = ["ffmpeg"] + inputs + [
        "-filter_complex", fg,
        "-map", "[vout]",
        "-c:v", "libx264", "-preset", VIDEO_PRESET, "-crf", str(VIDEO_CRF),
        "-movflags", "+faststart", "-y", str(output)
    ]
    timeout = max(300.0, n * 30)
    if _run_ffmpeg(cmd, timeout=timeout):
        logger.info(f"xfade_batch concat OK: {n} clips -> {output.name}")
        return True

    # Method 2: demuxer concat fallback (stream copy)
    logger.warning("concat filter failed, trying demuxer fallback")
    clip_paths = [cp for cp in clip_paths if _is_valid_clip(cp)]
    if not clip_paths:
        logger.error("demuxer fallback: no valid clips")
        return False
    concat_txt = output.parent / f"_concat_{output.stem}.txt"
    with open(concat_txt, "w") as f:
        for cp in clip_paths:
            f.write(f"file '{cp}'\n")
    cmd2 = ["ffmpeg", "-f", "concat", "-safe", "0", "-i", str(concat_txt),
            "-c", "copy", "-y", str(output)]
    return _run_ffmpeg(cmd2, timeout=timeout)


def prepare_clips_for_longform(
    scenes: list,
    video_type: str = "longform",
    output_dir: Optional[Path] = None
) -> List[Path]:
    """
    Generate Ken Burns clips from scenes.

    Each scene becomes N sub-clips (3-4s each) with Ken Burns effects,
    color grading, fades. Then merged per-scene and trimmed to scene duration.
    """
    if output_dir is None:
        output_dir = OUTPUT_DIR / "clips"
    output_dir.mkdir(parents=True, exist_ok=True)

    W, H = get_resolution(video_type)
    template = TEMPLATE_CONFIGS.get("info", TEMPLATE_CONFIGS["info"])
    clips = []
    kb_counter = 0

    for scene_idx, scene in enumerate(scenes):
        if not hasattr(scene, 'asset_url') or not scene.asset_url:
            logger.warning(f"scene {scene_idx}: no asset_url, skipping")
            continue

        scene_dur = max(getattr(scene, 'duration_seconds', None) or 5.0, 1.5)

        # Probe source duration
        src_dur = get_video_duration(Path(scene.asset_url))
        if src_dur is None:
            src_dur = scene_dur * 3
            needs_loop = True
        else:
            needs_loop = src_dur < scene_dur * 0.95

        # Calculate sub-clips (3s each, max 8)
        SUB_DUR = 3.0
        n_subs = min(8, max(1, int(scene_dur / SUB_DUR)))
        logger.info(f"scene {scene_idx}: {scene_dur:.1f}s -> {n_subs} sub-clips (src={src_dur:.1f}s, loop={needs_loop})")

        scene_clips = []
        base_sub_dur = scene_dur / n_subs

        for sub_i in range(n_subs):
            if sub_i == n_subs - 1:
                sub_dur = max(scene_dur - base_sub_dur * (n_subs - 1), 1.0)
            else:
                sub_dur = base_sub_dur

            fps_d = max(int(sub_dur * 30), 30)
            kb_speed = round(0.0008 * (4.0 / max(sub_dur, 4.0)), 5)
            kb_speed_hi = round(0.001 * (4.0 / max(sub_dur, 4.0)), 5)

            kb_filter = (KB_PRESETS[kb_counter % len(KB_PRESETS)]
                        .replace("{fps_d}", str(fps_d))
                        .replace("{kb_speed}", str(kb_speed))
                        .replace("{kb_speed_hi}", str(kb_speed_hi))
                        .replace("{W}", str(W))
                        .replace("{H}", str(H)))
            kb_counter += 1

            fade_out_st = max(sub_dur - 0.3, sub_dur * 0.9)

            vf = (
                f"scale={W}:{H}:force_original_aspect_ratio=increase,"
                f"crop={W}:{H},"
                f"{kb_filter},"
                f"fade=t=in:st=0:d={SCENE_HEAD_PAD_SEC:.2f},"
                f"fade=t=out:st={fade_out_st:.2f}:d={SCENE_TAIL_PAD_SEC:.2f},"
                f"unsharp=lx=5:ly=5:la=1.2:cx=3:cy=3:ca=0.6,"
                f"eq=brightness=0.03:contrast={template['contrast']}:saturation={template['saturation']}:gamma=0.93,"
                f"curves=preset=increase_contrast,"
                f"colorbalance=rs=.05:gs=-.02:bs=-.03:rm=.02:gm=0:bm=-.02:rh=-.02:gh=.02:bh=.05,"
                f"vignette={template['vignette']},"
                f"format=yuv420p"
            )

            clip_output = output_dir / f"clip_{scene_idx}_{sub_i}.mp4"

            seek_start = 0.0
            if src_dur > 1.0 and n_subs > 1:
                seek_offset = (src_dur - 0.5) * sub_i / max(n_subs - 1, 1)
                seek_start = max(0.0, min(seek_offset, src_dur - 0.5))

            cmd = ["ffmpeg"]
            if needs_loop:
                cmd += ["-stream_loop", "-1"]
            cmd += [
                "-ss", str(seek_start),
                "-i", scene.asset_url,
                "-t", str(sub_dur),
                "-vf", vf,
                "-c:v", "libx264", "-preset", VIDEO_PRESET, "-crf", str(VIDEO_CRF),
                "-movflags", "+faststart", "-an", "-y", str(clip_output)
            ]

            clip_timeout = max(60.0, sub_dur * 20)
            if _run_ffmpeg(cmd, timeout=clip_timeout) and clip_output.exists() and clip_output.stat().st_size >= 4096:
                scene_clips.append(clip_output)
                logger.info(f"  sub-clip OK: {sub_i}/{n_subs} ({sub_dur:.1f}s)")
            else:
                logger.warning(f"  sub-clip failed: {scene_idx}_{sub_i}")

        # Merge sub-clips per scene
        if len(scene_clips) == 1:
            scene_merged = output_dir / f"scene_{scene_idx}_merged.mp4"
            shutil.copy(str(scene_clips[0]), str(scene_merged))
            merged_ok = True
        elif len(scene_clips) > 1:
            scene_merged = output_dir / f"scene_{scene_idx}_merged.mp4"
            merged_ok = xfade_batch(scene_clips, scene_merged)
        else:
            logger.warning(f"scene {scene_idx}: no sub-clips generated")
            continue

        if merged_ok and scene_merged.exists() and scene_merged.stat().st_size > 4096:
            # Trim to exact scene_dur
            scene_final = output_dir / f"scene_{scene_idx}_final.mp4"
            trim_cmd = [
                "ffmpeg", "-i", str(scene_merged),
                "-t", str(round(scene_dur, 3)),
                "-c:v", "copy", "-an", "-y", str(scene_final)
            ]
            trim_timeout = max(60.0, scene_dur * 5)
            if _run_ffmpeg(trim_cmd, timeout=trim_timeout) and scene_final.exists():
                clips.append(scene_final)
                logger.info(f"scene {scene_idx} OK: trimmed to {scene_dur:.2f}s")
            else:
                logger.warning(f"scene {scene_idx} trim failed")
        else:
            logger.warning(f"scene {scene_idx} merge failed")

    logger.info(f"total clips: {len(clips)}")
    return clips

