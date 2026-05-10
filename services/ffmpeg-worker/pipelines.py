# -*- coding: utf-8 -*-
"""
LongForm Factory - Pipeline Orchestration Functions

run_auto_pipeline  : full automated pipeline (script → render → audio → subtitle → thumb)
run_render_pipeline: legacy scenes-based pipeline
"""
from __future__ import annotations
import json, logging, re, shutil
from pathlib import Path
from typing import Optional, List


def _safe_filename(text: str, max_len: int = 50) -> str:
    """한국어/특수문자 포함 제목을 안전한 파일명으로 변환."""
    # 파일명 금지 문자 제거
    safe = re.sub(r'[\\/:*?"<>|]', '', text.strip())
    # 공백 → 언더스코어
    safe = re.sub(r'\s+', '_', safe)
    # 연속 언더스코어 정리
    safe = re.sub(r'_+', '_', safe).strip('_')
    return safe[:max_len] if safe else "video"

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
        ts_path = TMP_DIR / f"{job_id}_timestamps.json"
        mp3_path = TMP_DIR / f"{job_id}.mp3"

        # BUG#2 fix: even if tts_done cached, re-run TTS if tmp files were lost
        # (e.g. Docker restart clears /data/tmp)
        _tts_files_missing = (
            not mp3_path.exists() or mp3_path.stat().st_size < 1024
            or not ts_path.exists()
        )
        if not state.has("tts_done") or _tts_files_missing:
            if _tts_files_missing and state.has("tts_done"):
                logger.warning(
                    f"[{job_id}] tts_done cached but tmp files missing — re-generating TTS"
                )
                state.unmark("tts_done")
            tts_result = await generate_tts(job_id, scenes)
            if tts_result["ok"]:
                state.mark("tts_done", {"mp3": str(tts_result.get("mp3_path", ""))})
            else:
                logger.warning(f"[{job_id}] TTS failed: {tts_result.get('error')}")

        if not ts_path.exists():
            ts_path = None  # type: ignore

        # Step 3: Sync durations from TTS
        scenes = sync_scene_durations(scenes, ts_path)

        # Step 4: Download assets
        await set_status(job_id, JobStatus.DOWNLOADING_ASSETS, 30, "downloading_assets")
        if not state.has("assets_done"):
            image_mode = getattr(request, "image_mode", "stock")
            style = getattr(request, "style", "")
            scenes = await search_and_download_assets(job_id, scenes, image_mode=image_mode, style=style)
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
            # Resume: use scene_*_final.mp4 (output of prepare_clips_for_longform)
            # clip_*.mp4 are per-scene sub-clips (30+ files) — NOT the correct resume target
            clips = sorted(job_tmp.glob("scene_*_final.mp4"))
            if not clips:
                # Fallback to sub-clips if scene_final files are missing
                clips = sorted(job_tmp.glob("clip_*.mp4"))

        if not clips:
            raise RuntimeError("No video clips generated")

        # Step 6: Concat
        raw_concat = job_tmp / "raw_concat.mp4"
        if not state.has("concat_done") or not raw_concat.exists():
            if state.has("concat_done") and not raw_concat.exists():
                logger.warning(f"[{job_id}] concat_done cached but raw_concat.mp4 missing — re-concat")
                state.unmark("concat_done")
            if not xfade_batch(clips, raw_concat):
                raise RuntimeError("Video concat failed")
            state.mark("concat_done")

        # Step 7: Mix audio
        mixed = job_tmp / "mixed.mp4"
        if not state.has("audio_done") or not mixed.exists():
            if state.has("audio_done") and not mixed.exists():
                logger.warning(f"[{job_id}] audio_done cached but mixed.mp4 missing — re-mix")
                state.unmark("audio_done")
            bgm = get_random_bgm() if request.add_bgm else None
            ok = mix_audio(raw_concat, mp3_path if mp3_path.exists() else None,
                           bgm, request.bgm_volume, mixed)
            if not ok:
                shutil.copy2(raw_concat, mixed)
            state.mark("audio_done")

        # Step 8: Subtitles
        # BUG#8 fix: three conditions force subtitle re-run:
        #   1. subtitle_done not in state (never ran)
        #   2. final.mp4 is missing despite state saying done (container restart lost /data/tmp)
        #   3. previous run had no timestamps (had_ass=False) but timestamps are now available
        final = job_tmp / "final.mp4"
        _ts_available = bool(ts_path and ts_path.exists() and request.add_subtitles)
        _prev_sub_payload = state.get_payload("subtitle_done")
        _prev_had_ass = _prev_sub_payload.get("had_ass") if _prev_sub_payload else None
        _needs_subtitle = (
            not state.has("subtitle_done")                   # never ran
            or not final.exists()                            # file was lost (container restart)
            or (_prev_had_ass is False and _ts_available)    # ran without ASS; timestamps now available
        )
        if _needs_subtitle:
            if _prev_had_ass is None and state.has("subtitle_done"):
                logger.warning(f"[{job_id}] subtitle_done cached but final.mp4 missing — re-run subtitles")
            elif _prev_had_ass is False and _ts_available:
                logger.info(f"[{job_id}] subtitle was skipped (no timestamps); re-applying with new timestamps")
            if _ts_available:
                add_subtitles_to_video(mixed, ts_path, final, resolution)
                state.mark("subtitle_done", {"had_ass": True})
            else:
                shutil.copy2(mixed, final)
                state.mark("subtitle_done", {"had_ass": False})

        # Step 9: Copy to output (영상 제목으로 파일명 저장)
        title_text = request.topic or request.title or job_id
        safe_title = _safe_filename(title_text)
        out_filename = f"{safe_title}_{job_id[-8:]}.mp4"
        out_path = out_dir / out_filename
        shutil.copy2(final, out_path)
        logger.info(f"[{job_id}] Video saved: {out_path} ({out_path.stat().st_size//1048576}MB)")

        # Step 10: Thumbnail
        await set_status(job_id, JobStatus.RENDERING, 90, "thumbnail")
        thumb_filename = f"{safe_title}_{job_id[-8:]}.jpg"
        thumb_path = THUMBNAILS_DIR / thumb_filename
        generate_thumbnail(out_path, thumb_path, title=title_text)

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
