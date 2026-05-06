# -*- coding: utf-8 -*-
"""
LongForm Factory - Pipeline Orchestration Functions

run_auto_pipeline  : full automated pipeline (script → render → audio → subtitle → thumb)
run_render_pipeline: legacy scenes-based pipeline
"""
from __future__ import annotations
import json, logging, shutil
from pathlib import Path
from typing import Optional, List

from config import (
    TMP_DIR, JOBS_DIR, OUTPUT_DIR,
    get_resolution,
)
from models import Scene, JobStatus, AutoVideoRequest, VideoCreateRequest
from state import JobState
from pipeline.script import split_script_to_scenes, generate_script_from_topic
from pipeline.assets import search_and_download_assets
from pipeline.tts import generate_tts, sync_scene_durations
from pipeline.render import xfade_batch
from pipeline.render_utils import prepare_clips, make_fallback_clip
from pipeline.audio import mix_audio, get_random_bgm
from pipeline.subtitle import add_subtitles_to_video
from pipeline.thumbnail import generate_thumbnail

logger = logging.getLogger(__name__)

LONGFORM_DIR   = OUTPUT_DIR / "longform"
SHORTS_DIR     = OUTPUT_DIR / "shorts"
THUMBNAILS_DIR = OUTPUT_DIR / "thumbnails"

# Injected from app.py at startup to avoid circular import
_set_status_fn = None  # set by app.py: _set_status_fn = _set_status
_load_scenes_fn = None  # set by app.py: _load_scenes_fn = _load_scenes


async def run_auto_pipeline(
    job_id: str,
    request: AutoVideoRequest,
    _CURRENT_JOB_setter,  # callable(str | None)
    set_status,           # async callable
    load_scenes,          # callable
) -> None:
    """Full automated pipeline: script → assets → TTS → render → audio → subtitle → thumbnail."""
    _CURRENT_JOB_setter(job_id)
    state = JobState(job_id)
    state.remember_request(request)
    video_type = request.video_type
    W, H = get_resolution(video_type)
    resolution = f"{W}x{H}"
    out_dir = SHORTS_DIR if video_type == "shorts" else LONGFORM_DIR
    job_tmp = TMP_DIR / job_id
    job_tmp.mkdir(parents=True, exist_ok=True)

    try:
        # Step 1: Script → Scenes
        await set_status(job_id, JobStatus.PROCESSING, 10, "script_splitting")
        if not state.has("scenes_loaded"):
            script = request.script
            if not script:
                script = await generate_script_from_topic(
                    request.topic, request.duration_sec, request.tone
                )
            scenes = await split_script_to_scenes(
                script=script, topic=request.topic,
                video_type=video_type, duration_sec=request.duration_sec,
                tone=request.tone,
            )
            scenes_file = JOBS_DIR / job_id / "scenes.json"
            scenes_file.parent.mkdir(parents=True, exist_ok=True)
            scenes_file.write_text(
                json.dumps([s.model_dump() for s in scenes], ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            state.mark("scenes_loaded", {"count": len(scenes)})
        else:
            scenes = load_scenes(job_id)
        logger.info(f"[{job_id}] {len(scenes)} scenes loaded")

        # Step 2: TTS
        await set_status(job_id, JobStatus.TTS_GENERATING, 20, "tts_generating")
        if not state.has("tts_done"):
            tts_result = await generate_tts(job_id, scenes)
            if tts_result["ok"]:
                state.mark("tts_done", {"mp3": str(tts_result.get("mp3_path", ""))})
            else:
                logger.warning(f"[{job_id}] TTS failed: {tts_result.get('error')}")

        ts_path = TMP_DIR / f"{job_id}_timestamps.json"
        mp3_path = TMP_DIR / f"{job_id}.mp3"
        if not ts_path.exists():
            ts_path = None  # type: ignore

        # Step 3: Sync durations from TTS
        scenes = sync_scene_durations(scenes, ts_path)

        # Step 4: Download assets
        await set_status(job_id, JobStatus.DOWNLOADING_ASSETS, 30, "downloading_assets")
        if not state.has("assets_done"):
            image_mode = getattr(request, "image_mode", "stock")
            scenes = await search_and_download_assets(job_id, scenes, image_mode=image_mode)
            state.mark("assets_done")

        # Step 5: Render clips (Ken Burns)
        await set_status(job_id, JobStatus.RENDERING, 50, "rendering")
        if not state.has("clips_done"):
            clips = await prepare_clips(job_id, scenes, job_tmp, W, H)
            if not clips:
                logger.warning(f"[{job_id}] No clips generated, using fallback visuals")
                clips = []
                for i, scene in enumerate(scenes):
                    fb = job_tmp / f"fallback_{i:02d}.mp4"
                    if make_fallback_clip(i, scene.duration_seconds, fb, scene.keyword, W, H):
                        clips.append(fb)
            state.mark("clips_done", {"count": len(clips)})
        else:
            clips = sorted(job_tmp.glob("clip_*.mp4"))

        if not clips:
            raise RuntimeError("No video clips generated")

        # Step 6: Concat
        raw_concat = job_tmp / "raw_concat.mp4"
        if not state.has("concat_done"):
            if not xfade_batch(clips, raw_concat):
                raise RuntimeError("Video concat failed")
            state.mark("concat_done")

        # Step 7: Mix audio
        mixed = job_tmp / "mixed.mp4"
        if not state.has("audio_done"):
            bgm = get_random_bgm() if request.add_bgm else None
            ok = mix_audio(raw_concat, mp3_path if mp3_path.exists() else None,
                           bgm, request.bgm_volume, mixed)
            if not ok:
                shutil.copy2(raw_concat, mixed)
            state.mark("audio_done")

        # Step 8: Subtitles
        final = job_tmp / "final.mp4"
        if not state.has("subtitle_done"):
            if request.add_subtitles and ts_path and ts_path.exists():
                add_subtitles_to_video(mixed, ts_path, final, resolution)
            else:
                shutil.copy2(mixed, final)
            state.mark("subtitle_done")

        # Step 9: Copy to output
        out_path = out_dir / f"{job_id}.mp4"
        shutil.copy2(final, out_path)
        logger.info(f"[{job_id}] Video saved: {out_path} ({out_path.stat().st_size//1048576}MB)")

        # Step 10: Thumbnail
        await set_status(job_id, JobStatus.RENDERING, 90, "thumbnail")
        thumb_path = THUMBNAILS_DIR / f"{job_id}.jpg"
        generate_thumbnail(out_path, thumb_path, title=request.topic)

        await set_status(job_id, JobStatus.COMPLETED, 100, "completed",
                         output_files={"video": str(out_path), "thumbnail": str(thumb_path)})
        state.mark("completed")
        logger.info(f"[{job_id}] Pipeline complete")

    except Exception as e:
        logger.error(f"[{job_id}] Pipeline error: {e}", exc_info=True)
        state.set_error(str(e))
        await set_status(job_id, JobStatus.FAILED, 0, "failed", error=str(e))
    finally:
        _CURRENT_JOB_setter(None)


async def run_render_pipeline(
    job_id: str,
    request: VideoCreateRequest,
    _CURRENT_JOB_setter,
    set_status,
    load_scenes,
) -> None:
    """Render pipeline for pre-built scenes (legacy /video/create endpoint)."""
    _CURRENT_JOB_setter(job_id)
    state = JobState(job_id)
    res = request.resolution
    W, H = (int(x) for x in res.split("x"))
    video_type = "shorts" if H > W else "longform"
    out_dir = SHORTS_DIR if video_type == "shorts" else LONGFORM_DIR
    job_tmp = TMP_DIR / job_id
    job_tmp.mkdir(parents=True, exist_ok=True)

    try:
        await set_status(job_id, JobStatus.PROCESSING, 10, "loading_scenes")
        if request.scenes:
            scenes = [Scene(**s) if isinstance(s, dict) else s for s in request.scenes]
        else:
            scenes = load_scenes(job_id)
        if not scenes:
            raise ValueError("No scenes found")

        ts_path: Optional[Path] = None
        mp3_path = TMP_DIR / f"{job_id}.mp3"
        if not request.audio_url and not mp3_path.exists():
            await set_status(job_id, JobStatus.TTS_GENERATING, 20, "tts_generating")
            tts_result = await generate_tts(job_id, scenes)
            if tts_result["ok"]:
                ts_path = tts_result.get("ts_path")
        else:
            cand = TMP_DIR / f"{job_id}_timestamps.json"
            if cand.exists():
                ts_path = cand

        scenes = sync_scene_durations(scenes, ts_path)

        await set_status(job_id, JobStatus.DOWNLOADING_ASSETS, 30, "downloading_assets")
        scenes = await search_and_download_assets(job_id, scenes)

        await set_status(job_id, JobStatus.RENDERING, 50, "rendering")
        clips = await prepare_clips(job_id, scenes, job_tmp, W, H)
        raw_concat = job_tmp / "raw_concat.mp4"
        xfade_batch(clips, raw_concat)

        mixed = job_tmp / "mixed.mp4"
        bgm = get_random_bgm() if request.add_bgm else None
        mix_audio(raw_concat, mp3_path if mp3_path.exists() else None,
                  bgm, request.bgm_volume, mixed)

        final = job_tmp / "final.mp4"
        if request.add_subtitles and ts_path and ts_path.exists():
            add_subtitles_to_video(mixed, ts_path, final, res)
        else:
            shutil.copy2(mixed, final)

        out_path = out_dir / f"{job_id}.mp4"
        shutil.copy2(final, out_path)
        thumb_path = THUMBNAILS_DIR / f"{job_id}.jpg"
        generate_thumbnail(out_path, thumb_path, title=request.title or "")

        await set_status(job_id, JobStatus.COMPLETED, 100, "completed",
                         output_files={"video": str(out_path), "thumbnail": str(thumb_path)})
    except Exception as e:
        logger.error(f"[{job_id}] Render error: {e}", exc_info=True)
        await set_status(job_id, JobStatus.FAILED, 0, "failed", error=str(e))
    finally:
        _CURRENT_JOB_setter(None)
