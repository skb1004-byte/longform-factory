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

from pipeline.content_presets import default_style, get_content
from pipeline.style_presets import AI_STYLES
from pipeline.prompt_builder import set_topic_hint, topic_terms_from_korean
from config import LOCAL_SDXL_ENABLED
from pipeline.stock_search import set_orientation
from pipeline.pause_trim import compress_pauses
from pipeline.content_presets import max_pause as _max_pause

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
from pipeline.qc import qc_media
from pipeline import cancel
from pipeline.cancel import JobCancelled
from pipeline.content_presets import set_content_type
from pipeline.stock_search import (
    set_topic_terms, set_job_topic, content_tokens, probe_stock_coverage,
)
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
    cancel.clear(job_id)
    cancel.set_active(job_id)
    _CURRENT_JOB_setter(job_id)
    state = JobState(job_id)
    state.remember_request(request)
    video_type = request.video_type
    resolution_tier = getattr(request, "resolution_tier", "1080p") or "1080p"
    W, H = get_resolution(video_type, resolution_tier)
    resolution = f"{W}x{H}"
    # 스톡 검색을 출력 방향에 맞춘다. 이게 없으면 세로 영상인데도
    # 가로 소재만 받아와 화면의 3분의 2가 흐린 배경으로 죽는다.
    set_orientation(W, H)
    out_dir = SHORTS_DIR if video_type == "shorts" else LONGFORM_DIR
    job_tmp = TMP_DIR / job_id
    job_tmp.mkdir(parents=True, exist_ok=True)

    try:
        # 프리셋 조회 기준을 잡 시작 시점에 확정한다.
        # tone 은 말투라 프리셋 키와 맞지 않는다 — 목표 글자수·BGM 볼륨이
        # 전부 이 값에 달려 있으므로 스크립트 생성 '전에' 세팅해야 한다.
        set_content_type(getattr(request, "video_type", "") or "")
        # 나레이션 문체를 화면 스타일에 맞춘다(초안 엔드포인트와 같은 기준).
        from pipeline.script import set_image_style as _set_img_style
        _img = (getattr(request, "style", "") or "").strip()
        if not _img:
            _im = (getattr(request, "image_mode", "") or "").strip().lower()
            _img = _im if _im in ("stock", "news") else ""
        _set_img_style(_img)

        cancel.check(job_id, "Step 1 직전")
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
                n_scenes_override=getattr(request, "n_scenes", None),
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
        logger.info(f"[{job_id}] {len(scenes)} scenes loaded (resolution={resolution})")

        cancel.check(job_id, "Step 2 직전")
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
            # TTS 는 잡당 한 번의 긴 호출이라 단계 경계 체크가 안 먹는다.
            tts_result = await cancel.race(generate_tts(
                job_id, scenes,
                voice=getattr(request, "voice", "sunhi"),
                speed=getattr(request, "narration_speed", 1.0),
            ), where="TTS 합성")
            if tts_result["ok"]:
                state.mark("tts_done", {"mp3": str(tts_result.get("mp3_path", ""))})
            else:
                logger.warning(f"[{job_id}] TTS failed: {tts_result.get('error')}")

        if not ts_path.exists():
            ts_path = None  # type: ignore

        cancel.check(job_id, "Step 2.5 직전")
        # Step 2.5: 문장 사이 멈춤 줄이기 (오디오와 타임스탬프를 함께 당긴다)
        # 반드시 sync_scene_durations 앞에서 해야 한다. 씬 길이가 타임스탬프에서
        # 나오므로, 뒤에 하면 영상 길이와 오디오가 어긋난다.
        if mp3_path.exists():
            compress_pauses(mp3_path, ts_path, _max_pause(getattr(request, "tone", "") or ""))

        cancel.check(job_id, "Step 3 직전")
        # Step 3: Sync durations from TTS
        scenes = sync_scene_durations(scenes, ts_path)

        # 이 잡에서 '주제에 맞다'고 볼 기준어를 확정한다.
        #
        # 씬 키워드는 LLM 이 영어로 만들어 주므로, 여러 씬에 공통으로 나오는
        # 내용어가 곧 이 영상의 주제다. 예) kimchi festival kyrgyzstan /
        # koryo people kimchi carrot / tourists kimchi making event → "kimchi".
        # 공통어가 없으면(제주·해녀처럼 씬마다 다른 고유어) 전체 내용어를 모아 쓴다.
        _kw_tokens = [content_tokens(getattr(sc, "keyword", "") or "") for sc in scenes]
        _freq: dict = {}
        for toks in _kw_tokens:
            for t in set(toks):
                _freq[t] = _freq.get(t, 0) + 1
        _shared = [t for t, c in _freq.items() if c >= 2]
        _topic_terms = _shared or [t for toks in _kw_tokens for t in toks]
        # 한국어 주제에서 직접 뽑은 고유명사를 맨 앞에 둔다.
        # 씬 키워드에만 의존하면 LLM 이 'jeju' 를 빼는 순간 기준어가
        # ['statue','stone'] 이 되고, 둘 다 스톡에 흔해 커버리지가 부풀려진다.
        _ko_terms = topic_terms_from_korean(request.topic or request.title or "")
        if _ko_terms:
            _topic_terms = _ko_terms + [t for t in _topic_terms if t not in _ko_terms]
        set_topic_terms(_topic_terms)
        set_job_topic(request.topic or request.title or "")
        set_topic_hint(request.topic or request.title or "")

        cancel.check(job_id, "Step 4 직전")
        # Step 4: Download assets
        await set_status(job_id, JobStatus.DOWNLOADING_ASSETS, 30, "downloading_assets")
        if not state.has("assets_done"):
            image_mode = getattr(request, "image_mode", "stock")
            style = getattr(request, "style", "")
            # 사용자가 이미지 스타일을 고르지 않았으면 장르에 맞는 기본값을 쓴다.
            # (교육→인포그래픽, 쇼츠→시네마틱, 뉴스·다큐→뉴스 스톡 …)
            # 사용자가 직접 고른 값이 있으면 그쪽이 항상 우선한다.
            if not style:
                _auto = default_style(getattr(request, "tone", "") or "")
                # 사용자가 스톡을 지정했는데 프리셋 기본값이 AI 스타일이면 덮지 않는다.
                # AI 경로는 렌더가 3분 → 12분으로 늘고 이미지 API 비용도 든다.
                # 말없이 경로가 바뀌면 안 된다. (실측: image_mode='stock' 요청이
                # 'cinematic' 으로 올라가 AI 생성으로 빠졌다)
                _stockish = str(image_mode).lower() in ("stock", "news")
                if _auto and _stockish and _auto in AI_STYLES:
                    logger.info(
                        f"[{job_id}] 스톡 요청이므로 프리셋 기본 스타일 "
                        f"'{_auto}'(AI) 을 적용하지 않는다 → image_mode '{image_mode}' 유지")
                elif _auto:
                    style = _auto
                    logger.info(f"[{job_id}] 장르 '{request.tone}' → 이미지 스타일 자동 '{style}'")
            # 스톡이 이 주제를 감당할 수 있는지 먼저 한 번 잰다.
            #
            # 씬마다 스톡→AI 를 오가면 한 영상에 실사와 생성 이미지가 섞여
            # 질감이 따로 논다(실측: 실사 폭포 + AI 나무막대 + 유럽 석조건물).
            # '돌하르방'처럼 스톡 라이브러리에 아예 없는 주제는 처음부터
            # 전부 AI 로 가는 편이 일관된 화면을 만든다.
            _MIN_COVERAGE = 6
            if str(image_mode).lower() in ("stock", "news") and not style:
                try:
                    _cov = await probe_stock_coverage(_topic_terms)
                except Exception as _pe:
                    logger.warning(f"[{job_id}] 커버리지 탐색 실패: {_pe}")
                    _cov = 99
                if _cov < _MIN_COVERAGE and LOCAL_SDXL_ENABLED:
                    style = "cinematic"
                    image_mode = "ai"
                    logger.warning(
                        f"[{job_id}] 스톡 적합 후보 {_cov}개(<{_MIN_COVERAGE}) — "
                        f"'{request.topic}' 은 스톡에 없는 주제로 판단 "
                        f"→ 잡 전체를 로컬 SDXL 생성으로 전환")
                elif _cov < _MIN_COVERAGE:
                    logger.warning(
                        f"[{job_id}] 스톡 적합 후보 {_cov}개뿐인데 로컬 SDXL 이 꺼져 있다 "
                        f"→ 같은 화면이 반복될 수 있다")

            scenes = await search_and_download_assets(job_id, scenes, image_mode=image_mode, style=style)
            state.mark("assets_done")

        cancel.check(job_id, "Step 5 직전")
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

        cancel.check(job_id, "Step 6 직전")
        # Step 6: Concat
        raw_concat = job_tmp / "raw_concat.mp4"
        if not state.has("concat_done") or not raw_concat.exists():
            if state.has("concat_done") and not raw_concat.exists():
                logger.warning(f"[{job_id}] concat_done cached but raw_concat.mp4 missing — re-concat")
                state.unmark("concat_done")
            if not xfade_batch(clips, raw_concat):
                raise RuntimeError("Video concat failed")
            state.mark("concat_done")

        cancel.check(job_id, "Step 7 직전")
        # Step 7: Mix audio
        mixed = job_tmp / "mixed.mp4"
        if not state.has("audio_done") or not mixed.exists():
            if state.has("audio_done") and not mixed.exists():
                logger.warning(f"[{job_id}] audio_done cached but mixed.mp4 missing — re-mix")
                state.unmark("audio_done")
            bgm = get_random_bgm() if request.add_bgm else None
            # 장르마다 배경음 존재감이 달라야 한다(브이로그는 크게, 다큐는 작게).
            # 요청값이 기본값(0.3) 그대로면 사용자가 손대지 않은 것으로 보고
            # 프리셋 값을 쓴다. 직접 조정한 값이면 그대로 존중한다.
            _bgm_vol = request.bgm_volume
            # 콘텐츠 종류는 video_type(shorts/longform/edu/news...) 이다.
            # tone 은 '차분한/친근한' 같은 말투라 프리셋 키와 맞지 않는다.
            # 예전엔 tone 으로 찾아서 매칭이 늘 실패했고, 프리셋 볼륨이
            # 한 번도 적용되지 않은 채 요청 기본값 0.3 이 그대로 쓰였다
            # (실측: "BGM 목표 라우드니스 -24.5 LUFS (volume=0.3)").
            _c = (get_content(getattr(request, "video_type", "") or "")
                  or get_content(getattr(request, "tone", "") or ""))
            if _c and abs(_bgm_vol - 0.3) < 1e-6:
                _bgm_vol = float(_c["bgm_volume"])
                logger.info(f"[{job_id}] 콘텐츠 '{request.video_type}' "
                            f"→ BGM 볼륨 {_bgm_vol}")
            elif abs(_bgm_vol - 0.3) < 1e-6:
                logger.warning(f"[{job_id}] '{request.video_type}' 프리셋을 못 찾아 "
                               f"BGM 볼륨 기본값 0.3 사용")
            ok = mix_audio(raw_concat, mp3_path if mp3_path.exists() else None,
                           bgm, _bgm_vol, mixed)
            if not ok:
                shutil.copy2(raw_concat, mixed)
            state.mark("audio_done")

        cancel.check(job_id, "Step 8 직전")
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
                # 검토를 마친 원본 나레이션을 함께 넘겨 Whisper 전사 오인식을 바로잡는다.
                _orig = " ".join((s.narration or "") for s in scenes).strip()
                add_subtitles_to_video(mixed, ts_path, final, resolution, narration=_orig or None)
                state.mark("subtitle_done", {"had_ass": True})
            else:
                shutil.copy2(mixed, final)
                state.mark("subtitle_done", {"had_ass": False})

        cancel.check(job_id, "Step 9 직전")
        # Step 9: Copy to output (영상 제목으로 파일명 저장)
        title_text = request.topic or request.title or job_id
        safe_title = _safe_filename(title_text)
        out_filename = f"{safe_title}_{job_id[-8:]}.mp4"
        out_path = out_dir / out_filename
        shutil.copy2(final, out_path)
        logger.info(f"[{job_id}] Video saved: {out_path} ({out_path.stat().st_size//1048576}MB)")

        cancel.check(job_id, "Step 9.5 직전")
        # Step 9.5: 업로드 전 기술 QC (컨테이너 모양 + 렌더 사고)
        #
        # 렌더가 '성공'해도 못 쓸 파일이 나오는 경우가 실제로 있었다:
        #  - 29초 중 4.0초가 검은 화면(17.61.0) → 유튜브 저품질 신호
        #  - 나레이션 끝 1.39초 잘림 → -shortest 가 짧은 쪽을 따라감
        #  - 39% 무음
        # 파이프라인을 실패시키지는 않는다. 결과에 기록해서 사람이 보고 판단하게 한다.
        _qc_mode = "short" if str(getattr(request, "video_type", "")).startswith("short") else "long"
        try:
            qc = qc_media(out_path, mode=_qc_mode)
        except Exception as _qe:
            logger.error(f"[{job_id}] QC 자체 실패: {_qe}")
            qc = {"ok": False, "errors": [f"QC 실행 실패: {_qe}"], "warnings": [], "facts": {}}
        state.mark("qc", qc)
        if not qc["ok"]:
            logger.error(f"[{job_id}] QC 불합격 — 업로드 전 확인 필요: {qc['errors']}")

        cancel.check(job_id, "Step 10 직전")
        # Step 10: Thumbnail
        await set_status(job_id, JobStatus.RENDERING, 90, "thumbnail")
        thumb_filename = f"{safe_title}_{job_id[-8:]}.jpg"
        thumb_path = THUMBNAILS_DIR / thumb_filename
        generate_thumbnail(out_path, thumb_path, title=title_text)

        await set_status(job_id, JobStatus.COMPLETED, 100, "completed",
                         output_files={"video": str(out_path), "thumbnail": str(thumb_path),
                                       "qc": qc})
        state.mark("completed")
        logger.info(f"[{job_id}] Pipeline complete")

    except JobCancelled:
        # 사용자가 중지를 눌렀다. 실패가 아니므로 error 를 남기지 않는다.
        logger.warning(f"[{job_id}] 사용자 중지")
        state.mark("cancelled", {"by": "user"})
        await set_status(job_id, JobStatus.FAILED, 0, "cancelled",
                         error="사용자가 중지했습니다")
    except Exception as e:
        logger.error(f"[{job_id}] Pipeline error: {e}", exc_info=True)
        state.set_error(str(e))
        await set_status(job_id, JobStatus.FAILED, 0, "failed", error=str(e))
    finally:
        cancel.clear(job_id)
        cancel.set_active("")
        _CURRENT_JOB_setter(None)


async def run_render_pipeline(
    job_id: str,
    request: VideoCreateRequest,
    _CURRENT_JOB_setter,
    set_status,
    load_scenes,
) -> None:
    """Render pipeline for pre-built scenes (legacy /video/create endpoint)."""
    cancel.clear(job_id)
    cancel.set_active(job_id)
    _CURRENT_JOB_setter(job_id)
    state = JobState(job_id)
    res = request.resolution
    W, H = (int(x) for x in res.split("x"))
    set_orientation(W, H)
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
            _orig = " ".join((s.narration or "") for s in scenes).strip()
            add_subtitles_to_video(mixed, ts_path, final, res, narration=_orig or None)
        else:
            shutil.copy2(mixed, final)

        out_path = out_dir / f"{job_id}.mp4"
        shutil.copy2(final, out_path)
        thumb_path = THUMBNAILS_DIR / f"{job_id}.jpg"
        generate_thumbnail(out_path, thumb_path, title=request.title or "")

        await set_status(job_id, JobStatus.COMPLETED, 100, "completed",
                         output_files={"video": str(out_path), "thumbnail": str(thumb_path)})
    except JobCancelled:
        # 사용자가 중지를 눌렀다. 실패가 아니므로 error 를 남기지 않는다.
        logger.warning(f"[{job_id}] 사용자 중지")
        state.mark("cancelled", {"by": "user"})
        await set_status(job_id, JobStatus.FAILED, 0, "cancelled",
                         error="사용자가 중지했습니다")
    except Exception as e:
        logger.error(f"[{job_id}] Render error: {e}", exc_info=True)
        await set_status(job_id, JobStatus.FAILED, 0, "failed", error=str(e))
    finally:
        cancel.clear(job_id)
        cancel.set_active("")
        _CURRENT_JOB_setter(None)
