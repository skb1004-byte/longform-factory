# -*- coding: utf-8 -*-
"""
LongForm Factory - FFmpeg Worker v17.37.0
FastAPI router + auth + helpers.
Pipeline functions are in pipelines.py.
"""
from __future__ import annotations
import json, logging, os, time, asyncio, shutil
import httpx
from datetime import datetime
from pathlib import Path
from typing import Optional, List

from fastapi import FastAPI, HTTPException, BackgroundTasks, Header, Depends, Request
from fastapi.responses import FileResponse, StreamingResponse, HTMLResponse

from config import (
    TMP_DIR, JOBS_DIR, OUTPUT_DIR, BGM_DIR,
    LF_API_KEY,
)
from models import (
    Scene, JobStatus, AutoVideoRequest, VideoCreateRequest,
    VideoCreateResponse, JobStatusResponse,
)
from state import JobState
from pipelines import run_auto_pipeline, run_render_pipeline
from pipeline import cancel

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","msg":"%(message)s"}',
)
logger = logging.getLogger(__name__)

# ── Directories ───────────────────────────────────────────────────────────────
LONGFORM_DIR   = OUTPUT_DIR / "longform"
SHORTS_DIR     = OUTPUT_DIR / "shorts"
THUMBNAILS_DIR = OUTPUT_DIR / "thumbnails"
for _d in [TMP_DIR, JOBS_DIR, LONGFORM_DIR, SHORTS_DIR, THUMBNAILS_DIR, BGM_DIR]:
    _d.mkdir(parents=True, exist_ok=True)

# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(title="LongForm Factory Worker", version="17.61.0")

# ── Global job guard ──────────────────────────────────────────────────────────
_CURRENT_JOB: Optional[str] = None


def _set_current_job(job_id: Optional[str]) -> None:
    global _CURRENT_JOB
    _CURRENT_JOB = job_id


# ── Auth ──────────────────────────────────────────────────────────────────────
def verify_api_key(x_lf_api_key: str = Header(None, alias="X-LF-API-Key")) -> str:
    if LF_API_KEY and x_lf_api_key != LF_API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API Key")
    return x_lf_api_key or ""


# ── Helpers ───────────────────────────────────────────────────────────────────
async def _set_status(
    job_id: str,
    status: JobStatus,
    progress: int,
    step: str,
    output_files: dict = None,
    error: str = None,
) -> None:
    """Write job status to status.json."""
    status_dir = JOBS_DIR / job_id
    status_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "job_id": job_id,
        "status": status.value,
        "progress": progress,
        "step": step,
        "updated_at": datetime.now().isoformat(),
    }
    if output_files:
        payload["output_files"] = output_files
    if error:
        payload["error"] = error
    (status_dir / "status.json").write_text(
        json.dumps(payload, ensure_ascii=False), encoding="utf-8"
    )


def _load_scenes(job_id: str) -> List[Scene]:
    """Load scenes from scenes.json."""
    scenes_file = JOBS_DIR / job_id / "scenes.json"
    if not scenes_file.exists():
        return []
    try:
        data = json.loads(scenes_file.read_text(encoding="utf-8-sig"))
        if isinstance(data, dict) and "scenes" in data:
            data = data["scenes"]
        if isinstance(data, list):
            return [Scene(**s) if isinstance(s, dict) else s for s in data]
    except Exception as e:
        logger.error(f"[load_scenes] {e}")
    return []


# ── Routes ────────────────────────────────────────────────────────────────────

@app.post("/video/auto", response_model=VideoCreateResponse)
async def create_auto_video(
    request: AutoVideoRequest,
    background_tasks: BackgroundTasks,
    _: str = Depends(verify_api_key),
):
    """Main endpoint: automated video creation from topic or script."""
    if _CURRENT_JOB:
        raise HTTPException(status_code=429, detail=f"Job in progress: {_CURRENT_JOB}")
    (JOBS_DIR / request.job_id).mkdir(parents=True, exist_ok=True)
    await _set_status(request.job_id, JobStatus.QUEUED, 0, "queued")
    background_tasks.add_task(
        run_auto_pipeline,
        request.job_id, request,
        _set_current_job, _set_status, _load_scenes,
    )
    return VideoCreateResponse(job_id=request.job_id, status="queued")


@app.post("/video/create", response_model=VideoCreateResponse)
async def create_video(
    request: VideoCreateRequest,
    background_tasks: BackgroundTasks,
    _: str = Depends(verify_api_key),
):
    """Legacy endpoint: render from pre-built scenes."""
    if _CURRENT_JOB:
        raise HTTPException(status_code=429, detail=f"Job in progress: {_CURRENT_JOB}")
    (JOBS_DIR / request.job_id).mkdir(parents=True, exist_ok=True)
    await _set_status(request.job_id, JobStatus.QUEUED, 0, "queued")
    background_tasks.add_task(
        run_render_pipeline,
        request.job_id, request,
        _set_current_job, _set_status, _load_scenes,
    )
    return VideoCreateResponse(job_id=request.job_id, status="queued")


@app.get("/video/{job_id}/status")
async def get_job_status(job_id: str, _: str = Depends(verify_api_key)):
    """Get job processing status."""
    status_file = JOBS_DIR / job_id / "status.json"
    if not status_file.exists():
        raise HTTPException(status_code=404, detail=f"Job not found: {job_id}")
    try:
        return json.loads(status_file.read_text(encoding="utf-8"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/video/{job_id}/cancel")
async def cancel_video(job_id: str, _: str = Depends(verify_api_key)):
    """돌고 있는 잡을 중지한다.

    강제 종료가 아니다. ffmpeg 가 파일을 쓰는 중에 끊으면 깨진 중간 파일이
    남고 다음 실행이 그걸 캐시로 오인한다. 그래서 '중지 요청'만 남기고,
    파이프라인이 다음 단계 경계(렌더 서브클립 하나 / 소재 하나)에서 스스로
    빠져나온다. 보통 3~20초 안에 멈춘다.

    이미 끝난 잡이나 없는 잡에도 200 을 준다 — 버튼을 두 번 눌러도
    에러가 뜨면 안 되기 때문.
    """
    running = (_CURRENT_JOB == job_id)
    cancel.request_cancel(job_id)
    if not running:
        return {"ok": True, "job_id": job_id, "running": False,
                "message": "이 잡은 지금 돌고 있지 않습니다. 중지 표시만 남겼습니다."}
    return {"ok": True, "job_id": job_id, "running": True,
            "message": "중지 요청됨. 진행 중인 단계가 끝나면 멈춥니다(보통 3~20초)."}


@app.get("/video/current")
async def current_video(_: str = Depends(verify_api_key)):
    """지금 도는 잡이 뭔지. 새로고침 후에도 중지 버튼을 살리기 위해 필요하다."""
    return {"current_job": _CURRENT_JOB, "cancel_pending": cancel.pending()}


@app.post("/video/{job_id}/resume")
async def resume_video(
    job_id: str,
    background_tasks: BackgroundTasks,
    _: str = Depends(verify_api_key),
):
    """Resume a failed/interrupted job from checkpoint."""
    if _CURRENT_JOB:
        raise HTTPException(status_code=429, detail=f"Job in progress: {_CURRENT_JOB}")
    state = JobState(job_id)
    request_data = state.get_payload("request")
    if not request_data:
        raise HTTPException(status_code=404, detail="No saved request for resume")
    try:
        req = AutoVideoRequest(**request_data)
        background_tasks.add_task(run_auto_pipeline, job_id, req,
                                   _set_current_job, _set_status, _load_scenes)
    except Exception:
        try:
            req = VideoCreateRequest(**request_data)
            background_tasks.add_task(run_render_pipeline, job_id, req,
                                       _set_current_job, _set_status, _load_scenes)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Cannot restore request: {e}")
    return {"job_id": job_id, "status": "resuming"}


@app.get("/health")
async def health():
    """Health check with disk space."""
    import shutil as _shutil
    stat = _shutil.disk_usage("/data")
    free_gb = stat.free / (1024 ** 3)
    return {
        "status": "ok" if free_gb > 10 else "disk_warning",
        "version": "17.37.0",
        "disk_free_gb": round(free_gb, 1),
        "current_job": _CURRENT_JOB,
    }


@app.get("/videos/list")
async def list_videos():
    """Return metadata for all generated videos in longform/ and shorts/."""
    import os
    result = []
    for vtype, vdir in [("longform", LONGFORM_DIR), ("shorts", SHORTS_DIR)]:
        if not vdir.exists():
            continue
        for f in sorted(vdir.iterdir(), key=lambda x: x.stat().st_mtime, reverse=True):
            if f.suffix.lower() != ".mp4":
                continue
            stat = f.stat()
            result.append({
                "name": f.name,
                "type": vtype,
                "size_mb": round(stat.st_size / 1024 / 1024, 1),
                "created": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M"),
                "url": f"/output/{vtype}/{f.name}",
            })
    return {"videos": result, "total": len(result)}


@app.get("/video/jobs")
async def list_all_jobs(_: str = Depends(verify_api_key)):
    """List all job statuses with title from state.json."""
    skip_dirs = {"pw_queue"}
    results = []
    for job_dir in sorted(JOBS_DIR.iterdir(), key=lambda x: x.stat().st_mtime, reverse=True):
        if not job_dir.is_dir() or job_dir.name in skip_dirs:
            continue
        status_file = job_dir / "status.json"
        if not status_file.exists():
            continue
        try:
            data = json.loads(status_file.read_text(encoding="utf-8"))
            title = data.get("job_id", "")
            scenes_count = 0
            state_file = job_dir / "state.json"
            if state_file.exists():
                try:
                    state = json.loads(state_file.read_text(encoding="utf-8"))
                    req = state.get("request", {})
                    title = req.get("topic") or req.get("title") or title
                except Exception:
                    pass
            scenes_file = job_dir / "scenes.json"
            if scenes_file.exists():
                try:
                    sd = json.loads(scenes_file.read_text(encoding="utf-8-sig"))
                    if isinstance(sd, list):
                        scenes_count = len(sd)
                    elif isinstance(sd, dict):
                        scenes_count = len(sd.get("scenes", []))
                except Exception:
                    pass
            data["title"] = title
            data["scenes"] = scenes_count
            results.append(data)
        except Exception as e:
            logger.error(f"[list_all_jobs] {e}")
    return {"jobs": results[:50], "total": len(results)}


def _cleanup_old_tmp(keep_hours: float = 24.0) -> dict:
    """오래된 렌더 임시파일을 지운다.

    렌더 중간산물(clip_*, *_norm.mp4, final.mp4 …)은 지금까지 아무도 지우지 않아
    /data/tmp 가 19GB 까지 불어 있었다. 완성 영상은 /data/output 에 따로 복사되므로
    tmp 를 지워도 결과물은 잃지 않는다. 다만 이어하기(resume)가 tmp 를 재사용하므로
    최근 것은 남긴다.
    """
    now = time.time()
    freed, removed = 0, 0
    try:
        entries = list(TMP_DIR.iterdir())
    except Exception:
        return {"removed": 0, "freed_mb": 0}
    for p in entries:
        try:
            if (now - p.stat().st_mtime) / 3600 <= keep_hours:
                continue
            if p.is_dir():
                size = sum(f.stat().st_size for f in p.rglob("*") if f.is_file())
                shutil.rmtree(p)
            else:
                size = p.stat().st_size
                p.unlink()
            freed += size
            removed += 1
        except Exception as e:
            logger.warning(f"[tmp] 정리 실패 {p.name}: {e}")
    if removed:
        logger.info(f"[tmp] 오래된 임시파일 {removed}개 정리, {freed/1e9:.2f}GB 확보")
    return {"removed": removed, "freed_mb": round(freed / 1e6)}


@app.post("/maintenance/cleanup-tmp")
async def cleanup_tmp(_: str = Depends(verify_api_key), keep_hours: float = 24.0):
    """수동 임시파일 정리 (기본: 24시간 초과분)."""
    return {"ok": True, **_cleanup_old_tmp(keep_hours)}


@app.delete("/video/jobs")
async def clear_all_jobs(_: str = Depends(verify_api_key)):
    """작업 큐를 비운다.

    JOBS_DIR 아래 잡 메타데이터(scenes.json/status.json/assets 등)만 지운다 --
    이미 완성된 mp4는 LONGFORM_DIR/SHORTS_DIR에 별도로 있어 건드리지 않고,
    지금 한창 처리 중인 잡(_CURRENT_JOB)도 깨지지 않도록 건너뛴다.
    """
    skip_dirs = {"pw_queue"}
    deleted: list[str] = []
    skipped: list[str] = []
    for job_dir in list(JOBS_DIR.iterdir()):
        if not job_dir.is_dir() or job_dir.name in skip_dirs:
            continue
        if job_dir.name == _CURRENT_JOB:
            skipped.append(job_dir.name)
            continue
        try:
            shutil.rmtree(job_dir)
            deleted.append(job_dir.name)
        except Exception as e:
            logger.error(f"[clear_all_jobs] {job_dir.name}: {e}")
    return {"ok": True, "deleted": len(deleted), "skipped_processing": skipped}

@app.get("/video/stream/{vtype}/{filename}")
async def stream_video(vtype: str, filename: str, request: Request):
    """Stream a video file with HTTP Range support for browser <video> playback."""
    if vtype not in ("longform", "shorts", "thumbnails"):
        raise HTTPException(status_code=400, detail="Invalid video type")
    if "/" in filename or "\\" in filename or ".." in filename:
        raise HTTPException(status_code=400, detail="Invalid filename")
    vdir_map = {"longform": LONGFORM_DIR, "shorts": SHORTS_DIR, "thumbnails": THUMBNAILS_DIR}
    fpath = vdir_map[vtype] / filename
    if not fpath.exists() or not fpath.is_file():
        raise HTTPException(status_code=404, detail="File not found")

    file_size = fpath.stat().st_size
    range_header = request.headers.get("Range")

    def file_chunk(start: int, end: int, chunk: int = 1024 * 256):
        with open(fpath, "rb") as f:
            f.seek(start)
            remaining = end - start + 1
            while remaining > 0:
                data = f.read(min(chunk, remaining))
                if not data:
                    break
                remaining -= len(data)
                yield data

    if range_header:
        # Parse "bytes=start-end"
        try:
            byte_range = range_header.replace("bytes=", "").split("-")
            start = int(byte_range[0])
            end = int(byte_range[1]) if byte_range[1] else file_size - 1
        except Exception:
            raise HTTPException(status_code=416, detail="Invalid Range header")
        end = min(end, file_size - 1)
        content_length = end - start + 1
        return StreamingResponse(
            file_chunk(start, end),
            status_code=206,
            media_type="video/mp4",
            headers={
                "Content-Range": f"bytes {start}-{end}/{file_size}",
                "Accept-Ranges": "bytes",
                "Content-Length": str(content_length),
            },
        )

    # No Range header — return full file
    return StreamingResponse(
        file_chunk(0, file_size - 1),
        media_type="video/mp4",
        headers={
            "Accept-Ranges": "bytes",
            "Content-Length": str(file_size),
        },
    )


@app.post("/open-folder/{vtype}")
async def open_output_folder(vtype: str, _: str = Depends(verify_api_key)):
    """Open output folder in Windows Explorer (server-side shell command)."""
    vdir_map = {"longform": LONGFORM_DIR, "shorts": SHORTS_DIR, "thumbnails": THUMBNAILS_DIR}
    if vtype not in vdir_map:
        raise HTTPException(status_code=400, detail="Invalid type")
    folder = vdir_map[vtype]
    import subprocess as _sp, sys as _sys
    try:
        win_path = str(folder).replace("/data/output", "E:\\longform_factory\\v2\\output").replace("/", "\\")
        if _sys.platform.startswith("linux"):
            # Try xdg-open or explorer.exe via WSL
            _sp.Popen(["explorer.exe", win_path], stderr=_sp.DEVNULL)
        else:
            _sp.Popen(["explorer", win_path], stderr=_sp.DEVNULL)
    except Exception as e:
        logger.warning(f"[open_folder] {e}")
    return {"folder": str(folder), "win_path": win_path}


# ── 설정 / API 키 관리 (실제 코드가 소비하는 키만 허용) ─────────────────────────
# .env 파일은 docker run -v "E:\longform_factory\v2\.env:/data/.env" 로 마운트되며
# 동시에 --env-file 로도 읽히는 동일 파일이다. 여기서 저장한 값은 컨테이너를
# 저장 즉시 _apply_env_runtime() 이 os.environ 갱신 + config/pipeline 모듈 reload 를
# 수행하므로 컨테이너 재시작 없이 바로 반영된다.
_ENV_FILE = Path("/data/.env")

_ENV_ALLOWED = {
    # LLM — 나레이션 스크립트 생성 (pipeline/script.py 병렬 fallback 체인)
    "ANTHROPIC_API_KEY", "ANTHROPIC_MODEL", "OPENAI_API_KEY",
    "GEMINI_API_KEY", "GEMINI_MODEL", "GROQ_API_KEY",
    "CEREBRAS_API_KEY", "CEREBRAS_MODEL", "ARLIAI_API_KEY", "ARLIAI_MODEL",
    "DEEPSEEK_API_KEY", "OPENROUTER_API_KEY", "XAI_API_KEY",
    "SCRIPT_CROSS_REVIEW", "SCRIPT_REVIEW_ROUNDS", "SUBTITLE_KARAOKE", "BLUR_PAD_ENABLED",
    # 웹 검색 그라운딩 — pipeline/websearch.py
    "WEB_SEARCH_ENABLED", "WEB_SEARCH_MODE", "WEB_SEARCH_MAX_ARTICLES",
    "WEB_SEARCH_FETCH_BODIES", "WEB_SEARCH_TIMEOUT",
    # 화질·음향 — config.py 가 실제로 소비하는데 허용 목록에서 빠져 있어
    # 대시보드에 보이면서도 저장이 거부되던 키들.
    "VIDEO_CRF", "VIDEO_PRESET", "VIDEO_FPS", "OUTPUT_RESOLUTION",
    "AUDIO_LOUDNESS_TARGET", "BGM_VOLUME_DEFAULT", "BGM_VOLUME_DURING_VOICE",
    "MAX_DOWNLOAD_MB", "MAX_SOURCE_CLIP_SEC",
    # 스톡 영상 — pipeline/stock_search.py
    "PEXELS_API_KEY", "PIXABAY_API_KEY",
    # AI 영상·이미지 생성 — pipeline/ai_image.py, wavespeed_video.py, kling_video.py
    # (실제로 소비되는 키만 포함. Pollo/Apiframe/MagicHour/Stability/SiliconFlow는
    #  현재 소스코드 어디에도 참조가 없어 넣어도 아무 동작을 하지 않으므로 제외했다.)
    "WAVESPEED_API_KEY",
    "KLING_ACCESS_KEY", "KLING_SECRET_KEY", "KLING_MODE", "KLING_ENABLED",
    "HIGGSFIELD_API_KEY_ID", "HIGGSFIELD_API_KEY_SECRET",
    "LOCAL_SDXL_URL", "LOCAL_SDXL_ENABLED",
}

_SECRET_ENV_KEYS = {k for k in _ENV_ALLOWED if any(s in k for s in ("KEY", "SECRET", "TOKEN"))}


def _read_env_file() -> dict:
    result: dict = {}
    if _ENV_FILE.exists():
        for line in _ENV_FILE.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            result[k.strip()] = v.strip()
    return result


def _mask_secret(v: str) -> str:
    if not v:
        return ""
    if len(v) <= 8:
        return "*" * len(v)
    return v[:4] + "*" * (len(v) - 8) + v[-4:]


@app.get("/settings/env")
async def get_env_settings(_: str = Depends(verify_api_key)):
    """저장된 .env 값 조회 (시크릿은 마스킹). 실제 코드가 소비하는 키만 노출."""
    import os as _os
    current = _read_env_file()
    items = []
    for key in sorted(_ENV_ALLOWED):
        raw = current.get(key, "") or _os.getenv(key, "")
        items.append({
            "key": key,
            "value": _mask_secret(raw) if key in _SECRET_ENV_KEYS else raw,
            "configured": bool(raw),
        })
    return {
        "count": len(items),
        "items": items,
        "env_file": str(_ENV_FILE),
        "note": "값 변경은 컨테이너 재생성(docker stop/rm/run 또는 재빌드) 후 적용됩니다.",
    }


# .env 저장값을 '실행 중인 프로세스'에 즉시 반영하기 위해 다시 읽어야 하는 모듈들.
# API 키는 config.py 에서 모듈 레벨 상수로 한 번만 읽히고, 각 pipeline 모듈이
# rom config import KEY 로 자기 네임스페이스에 복사해 간다. 그래서 os.environ 만
# 고쳐서는 반영되지 않고, config 를 먼저 reload 한 뒤 복사해 간 모듈들을 순서대로
# reload 해야 한다. reload 는 같은 모듈 dict 를 다시 채우므로, 다른 모듈이 이미
# import 해 간 함수 객체들도 (globals 를 공유하므로) 갱신된 값을 보게 된다.
# pipeline.assets 는 모듈 레벨 asyncio.Semaphore 를 들고 있어 제외한다 —
# 진행 중 작업의 동시성 가드가 깨질 수 있다.
_ENV_RELOAD_MODULES = [
    "config",
    "pipeline.stock_search",
    "pipeline.wavespeed_video",
    "pipeline.kling_video",
    "pipeline.higgsfield_video",
    "pipeline.prompt_builder",
    "pipeline.style_presets",
    "pipeline.ai_image",
    "pipeline.content_presets",
    "pipeline.pause_trim",
    "pipeline.websearch",
    "pipeline.script",
    "pipeline.subtitle",
    "pipeline.audio",
    "pipeline.render",
    "pipelines",
]


def _apply_env_runtime() -> dict:
    """저장된 .env 를 재시작 없이 즉시 적용한다."""
    import importlib
    import sys

    current = _read_env_file()
    for k, v in current.items():
        if k in _ENV_ALLOWED:
            os.environ[k] = v

    if _CURRENT_JOB:
        logger.info(f"[settings] env 갱신됨 · 모듈 reload 는 작업({_CURRENT_JOB}) 종료까지 보류")
        return {"env": True, "modules": [], "deferred": True,
                "note": f"진행 중 작업({_CURRENT_JOB}) 때문에 모듈 갱신 보류 — 다음 작업부터 적용"}

    reloaded, failed = [], []
    for name in _ENV_RELOAD_MODULES:
        mod = sys.modules.get(name)
        if mod is None:
            continue
        try:
            importlib.reload(mod)
            reloaded.append(name)
        except Exception as e:
            failed.append(f"{name}: {e}")
            logger.warning(f"[settings] reload 실패 {name}: {e}")
    logger.info(f"[settings] 즉시 적용 완료 · reload {len(reloaded)}개 모듈")
    return {"env": True, "modules": reloaded, "failed": failed, "deferred": False}


@app.get("/settings/runtime")
async def get_runtime_settings(_: str = Depends(verify_api_key)):
    """지금 '실행 중인 프로세스'가 실제로 들고 있는 값. 저장 즉시반영이 먹었는지 확인용.

    docker exec 로 파이썬을 새로 띄우면 컨테이너 시작 시점의 환경변수를 보게 되므로
    실행 중 서버의 메모리 상태를 확인할 수 없다. 그래서 서버 자신이 답하게 한다.
    """
    import config as _cfg
    import sys as _sys

    def _mask(v):
        if not v:
            return ""
        return _mask_secret(v)

    script_mod = _sys.modules.get("pipeline.script")
    ai_mod = _sys.modules.get("pipeline.ai_image")
    return {
        "config": {
            "ANTHROPIC_API_KEY": _mask(_cfg.ANTHROPIC_API_KEY),
            "OPENAI_API_KEY": _mask(_cfg.OPENAI_API_KEY),
            "ANTHROPIC_MODEL": _cfg.ANTHROPIC_MODEL,
            "GEMINI_MODEL": _cfg.GEMINI_MODEL,
            "CEREBRAS_MODEL": _cfg.CEREBRAS_MODEL,
            "ARLIAI_MODEL": _cfg.ARLIAI_MODEL,
            "GROQ_API_KEY": _mask(_cfg.GROQ_API_KEY),
            "LOCAL_SDXL_ENABLED": _cfg.LOCAL_SDXL_ENABLED,
            "LOCAL_SDXL_URL": _cfg.LOCAL_SDXL_URL,
        },
        "pipeline_copies": {
            "script.ANTHROPIC_MODEL": getattr(script_mod, "ANTHROPIC_MODEL", None) if script_mod else None,
            "script.GEMINI_MODEL": getattr(script_mod, "GEMINI_MODEL", None) if script_mod else None,
            "script.ANTHROPIC_API_KEY": _mask(getattr(script_mod, "ANTHROPIC_API_KEY", "")) if script_mod else None,
            "ai_image.LOCAL_SDXL_ENABLED": getattr(ai_mod, "LOCAL_SDXL_ENABLED", None) if ai_mod else None,
            "ai_image.OPENAI_API_KEY": _mask(getattr(ai_mod, "OPENAI_API_KEY", "")) if ai_mod else None,
        },
        "current_job": _CURRENT_JOB,
    }


@app.post("/settings/env")
async def set_env_settings(body: dict, _: str = Depends(verify_api_key)):
    """.env 파일에 키 저장. 실제 반영은 컨테이너 재생성 후."""
    updates = body.get("updates") if isinstance(body.get("updates"), dict) else body
    if not isinstance(updates, dict) or not updates:
        raise HTTPException(status_code=400, detail="updates must be a non-empty dict of {KEY: value}")

    rejected = [k for k in updates if k not in _ENV_ALLOWED]
    accepted = {k: str(v) for k, v in updates.items() if k in _ENV_ALLOWED}
    if not accepted:
        raise HTTPException(
            status_code=400,
            detail=f"허용되지 않은 키입니다 (실제 코드에서 사용되지 않음): {rejected}",
        )

    current = _read_env_file()
    current.update(accepted)
    _ENV_FILE.parent.mkdir(parents=True, exist_ok=True)
    lines = [f"{k}={v}" for k, v in sorted(current.items())]
    _ENV_FILE.write_text("\n".join(lines) + "\n", encoding="utf-8")

    applied = _apply_env_runtime()
    _msg = "저장 완료 · 즉시 적용됨 (재시작 불필요)"
    if applied.get("deferred"):
        _msg = "저장 완료 · 진행 중 작업이 끝난 뒤 다음 작업부터 적용됩니다"

    return {
        "ok": True,
        "saved": sorted(accepted.keys()),
        "rejected": rejected,
        "restart_required": False,
        "applied": applied,
        "message": _msg,
    }


@app.post("/script/generate")
async def script_generate(
    body: dict,
    _: str = Depends(verify_api_key),
):
    """AI 초안: 주제 → 한국어 나레이션 스크립트 + 씬별 상세 분해(키워드·나레이션·길이).

    영상 제목만 입력해도 실제 렌더링 파이프라인(run_auto_pipeline)과 동일한
    script → scenes 로직(pipeline/script.py)을 그대로 사용해, 나레이션 전체 +
    씬 단위(비주얼 키워드 / 나레이션 발췌 / 예상 길이) 상세 미리보기까지 반환한다.
    """
    from pipeline.script import generate_script_from_topic, split_script_to_scenes
    topic = body.get("topic", "")
    duration_sec = int(body.get("duration_sec", 300))
    tone = body.get("tone", "professional_documentary")
    video_type = body.get("video_type", "longform")
    if not topic:
        raise HTTPException(status_code=400, detail="topic is required")

    # 목표 글자수·BGM 볼륨·쉼 길이는 전부 '콘텐츠 종류' 프리셋에서 나온다.
    # 이 엔드포인트가 그걸 세팅하지 않으면 직전 잡의 값이 그대로 남아
    # 쇼츠를 뽑는데 롱폼 기준으로 글자수가 잡히는 일이 생긴다.
    # 초안도 실제 렌더와 같은 기준으로 만들어야 미리보기가 의미가 있다.
    from pipeline.content_presets import set_content_type
    from pipeline.prompt_builder import set_topic_hint
    from pipeline.script import set_image_style
    set_content_type(video_type or "")
    set_topic_hint(topic)
    # 화면 스타일에 맞춘 문체로 초안을 쓴다. 미리보기와 실제 렌더가
    # 같은 기준이어야 초안을 보고 판단할 수 있다.
    set_image_style(body.get("image_style") or body.get("style") or "")

    script = await generate_script_from_topic(topic, duration_sec, tone)

    # 모든 LLM 이 실패하면 pipeline 은 '템플릿'을 돌려준다. 주제명만 반복하는
    # 자리표시자라 그대로 쓸 수 없는데, 예전에는 그게 조용히 초안 칸에 채워져
    # 사용자가 쓸 만한 글인 줄 알고 그대로 영상을 만들 수 있었다.
    # 실패는 실패라고 알려야 한다.
    _tmpl_marks = ("다양한 측면에서 살펴볼 수 있는 흥미로운 주제",
                   "기초부터 심화 내용까지 체계적으로 설명",
                   "바로 지금, 여기서부터 시작하면 됩니다")
    if script and sum(1 for m in _tmpl_marks if m in script) >= 2:
        logger.error(f"[script/generate] 모든 LLM 실패 → 템플릿 감지, 초안 거부: {topic}")
        raise HTTPException(
            status_code=503,
            detail=("모든 LLM 제공자가 실패해 초안을 만들지 못했습니다. "
                    "키 상태(잔액·한도)를 확인하거나 잠시 후 다시 시도하세요. "
                    "AI 프로바이더 카드에서 상태를 볼 수 있습니다."))

    scenes_payload: list = []
    try:
        scenes = await split_script_to_scenes(
            script=script, topic=topic, video_type=video_type,
            duration_sec=duration_sec, tone=tone,
        )
        scenes_payload = [
            {
                "scene_id": s.scene_id,
                "keyword": s.keyword,
                "narration": s.narration,
                "duration_seconds": round(s.duration_seconds or 0, 1),
            }
            for s in scenes
        ]
    except Exception as e:
        logger.warning(f"[script_generate] scene split failed (script만 반환): {e}")

    return {
        "script": script,
        "length": len(script),
        "topic": topic,
        "video_type": video_type,
        "scenes": scenes_payload,
        "scene_count": len(scenes_payload),
        "estimated_duration_sec": (
            round(sum(s["duration_seconds"] for s in scenes_payload), 1)
            if scenes_payload else duration_sec
        ),
    }


_PING_TIMEOUT = 5.0
_PROVIDER_META = {
    "anthropic":  {"name": "Anthropic",  "icon": "\U0001F916"},
    "pexels":     {"name": "Pexels",     "icon": "\U0001F4F7"},
    "pixabay":    {"name": "Pixabay",    "icon": "\U0001F5BC"},
    "elevenlabs": {"name": "ElevenLabs", "icon": "\U0001F3B5"},
    "openai":     {"name": "OpenAI",     "icon": "\U0001F4A1"},
    "stability":  {"name": "Stability",  "icon": "\U0001F3A8"},
    "edgetts":    {"name": "Edge TTS",   "icon": "\U0001F5E3"},
    "youtube":    {"name": "YouTube",    "icon": "\u25B6"},
    "gemini":     {"name": "Gemini",     "icon": "\u2728"},
    "local_sdxl": {"name": "Local SDXL", "icon": "\U0001F5A5"},
}


async def _ping_http(name_for_log: str, method: str, url: str, headers: dict | None = None, ok_codes=(200,)) -> dict:
    t0 = time.monotonic()
    try:
        async with httpx.AsyncClient(timeout=_PING_TIMEOUT) as client:
            r = await client.request(method, url, headers=headers or {})
        ms = round((time.monotonic() - t0) * 1000)
        if r.status_code in ok_codes:
            return {"ok": True, "ms": ms, "err": ""}
        return {"ok": False, "ms": ms, "err": f"HTTP {r.status_code}"}
    except Exception as e:
        return {"ok": False, "ms": round((time.monotonic() - t0) * 1000), "err": str(e)[:100]}


async def _ping_anthropic() -> dict:
    key = os.getenv("ANTHROPIC_API_KEY", "")
    model = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-6")
    if not key:
        return {"model": model, "ok": False, "ms": 0, "err": "API \uD0A4 \uBBF8\uC124\uC815"}
    res = await _ping_http("anthropic", "GET", "https://api.anthropic.com/v1/models",
                            {"x-api-key": key, "anthropic-version": "2023-06-01"})
    res["model"] = model
    return res


async def _ping_openai() -> dict:
    key = os.getenv("OPENAI_API_KEY", "")
    if not key:
        return {"model": "gpt-4o", "ok": False, "ms": 0, "err": "API \uD0A4 \uBBF8\uC124\uC815"}
    res = await _ping_http("openai", "GET", "https://api.openai.com/v1/models", {"Authorization": f"Bearer {key}"})
    res["model"] = "gpt-4o"
    return res


async def _ping_gemini() -> dict:
    key = os.getenv("GEMINI_API_KEY", "")
    model = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")
    if not key:
        return {"model": model, "ok": False, "ms": 0, "err": "API \uD0A4 \uBBF8\uC124\uC815"}
    res = await _ping_http("gemini", "GET", f"https://generativelanguage.googleapis.com/v1/models?key={key}")
    res["model"] = model
    return res


async def _ping_pexels() -> dict:
    key = os.getenv("PEXELS_API_KEY", "")
    if not key:
        return {"model": "Video API v1", "ok": False, "ms": 0, "err": "API \uD0A4 \uBBF8\uC124\uC815"}
    res = await _ping_http("pexels", "GET", "https://api.pexels.com/videos/search?query=test&per_page=1",
                            {"Authorization": key})
    res["model"] = "Video API v1"
    return res


async def _ping_pixabay() -> dict:
    key = os.getenv("PIXABAY_API_KEY", "")
    if not key:
        return {"model": "Search API", "ok": False, "ms": 0, "err": "API \uD0A4 \uBBF8\uC124\uC815"}
    res = await _ping_http("pixabay", "GET", f"https://pixabay.com/api/videos/?key={key}&q=test&per_page=3")
    res["model"] = "Search API"
    return res


async def _ping_elevenlabs() -> dict:
    key = os.getenv("ELEVENLABS_API_KEY", "")
    if not key:
        return {"model": "TTS v2", "ok": False, "ms": 0, "err": "API \uD0A4 \uBBF8\uC124\uC815"}
    res = await _ping_http("elevenlabs", "GET", "https://api.elevenlabs.io/v1/user", {"xi-api-key": key})
    res["model"] = "TTS v2"
    return res


async def _ping_stability() -> dict:
    key = os.getenv("STABILITY_API_KEY", "")
    if not key:
        return {"model": "SD XL", "ok": False, "ms": 0, "err": "API \uD0A4 \uBBF8\uC124\uC815"}
    res = await _ping_http("stability", "GET", "https://api.stability.ai/v1/user/account", {"Authorization": f"Bearer {key}"})
    res["model"] = "SD XL"
    return res


async def _ping_edgetts() -> dict:
    t0 = time.monotonic()
    try:
        async with httpx.AsyncClient(timeout=_PING_TIMEOUT) as client:
            r = await client.get("http://lf2_tts:8001/health")
        ms = round((time.monotonic() - t0) * 1000)
        if r.status_code == 200 and (r.json() or {}).get("status") == "healthy":
            return {"model": "ko-KR-SunHi", "ok": True, "ms": ms, "err": ""}
        return {"model": "ko-KR-SunHi", "ok": False, "ms": ms, "err": f"HTTP {r.status_code}"}
    except Exception as e:
        return {"model": "ko-KR-SunHi", "ok": False, "ms": round((time.monotonic() - t0) * 1000), "err": str(e)[:100]}


async def _ping_local_sdxl() -> dict:
    from config import LOCAL_SDXL_URL
    t0 = time.monotonic()
    try:
        async with httpx.AsyncClient(timeout=_PING_TIMEOUT) as client:
            r = await client.get(f"{LOCAL_SDXL_URL}/health")
        ms = round((time.monotonic() - t0) * 1000)
        data = r.json() if r.status_code == 200 else {}
        if data.get("ok"):
            model = data.get("model", "sdxl-turbo")
            device = data.get("device", "?")
            return {"model": f"{model} ({device})", "ok": True, "ms": ms, "err": ""}
        return {"model": "sdxl-turbo", "ok": False, "ms": ms, "err": data.get("error", "not ready")}
    except Exception as e:
        return {"model": "sdxl-turbo", "ok": False, "ms": round((time.monotonic() - t0) * 1000), "err": str(e)[:100]}


async def _ping_youtube() -> dict:
    token = os.getenv("YOUTUBE_REFRESH_TOKEN", "")
    if not token:
        return {"model": "Data API v3", "ok": False, "ms": 0, "err": "\uB9AC\uD504\uB8E8\uC2DC \uD1A0\uD070 \uBBF8\uC124\uC815"}
    t0 = time.monotonic()
    try:
        async with httpx.AsyncClient(timeout=_PING_TIMEOUT) as client:
            r = await client.get("http://lf2_uploader:8003/health")
        ms = round((time.monotonic() - t0) * 1000)
        data = r.json() if r.status_code == 200 else {}
        if data.get("youtube_enabled"):
            return {"model": "Data API v3", "ok": True, "ms": ms, "err": ""}
        return {"model": "Data API v3", "ok": False, "ms": ms, "err": "youtube_enabled=false"}
    except Exception as e:
        return {"model": "Data API v3", "ok": False, "ms": round((time.monotonic() - t0) * 1000), "err": str(e)[:100]}


@app.get("/providers/ping-all")
async def providers_ping_all(_: str = Depends(verify_api_key)):
    """\uB300\uC2DC\uBBFC\uC988 'AI \uD53C\uB4DC\uBC14\uC774\uB512' \uD328\uB808. 9\uAC1C \uD53C\uB4DC\uBC14\uC774\uB512\uB855 \uB3D9\uC2DC \uC2E4\uC81C \uD658\uB1DC(latency)\uD558\uAC70\uB2DD, \uD558\uC790\uB6C4\uAE30 \uCCB4\uD3ED\uB4DC \uB3D9\uC77C \uC5D0\uB77C\uAC80 \uC744.
    \uC2A4\uB808\uB4DC\uD558\uB098\uB77C\uB730 \uD558\uB4DC\uAC74\uB098 \uACF5\uC720 \uB098 \uBAA8\uB4E0 \uD53C\uB4DC\uBC14\uC774\uB512\uAC00 \uB3D9\uC77C\uD55C 404\uB85C \uB098\uD0C0\uB097\uB308\uC77C \uACFC \uAC8C\uB4E0(nginx\uC5D0 /api/providers/ \uB77C\uC6B0\uD305\uAC00 \uC5C6\uC5B4\uC11C\uB2E4).
    """
    ids = ["anthropic", "pexels", "pixabay", "elevenlabs", "openai", "stability", "edgetts", "youtube", "gemini", "local_sdxl"]
    results = await asyncio.gather(
        _ping_anthropic(), _ping_pexels(), _ping_pixabay(), _ping_elevenlabs(),
        _ping_openai(), _ping_stability(), _ping_edgetts(), _ping_youtube(), _ping_gemini(), _ping_local_sdxl(),
        return_exceptions=True,
    )
    providers = []
    for pid, res in zip(ids, results):
        if isinstance(res, Exception):
            res = {"ok": False, "ms": 0, "err": str(res)[:100]}
        entry = {"id": pid, "name": _PROVIDER_META[pid]["name"], "icon": _PROVIDER_META[pid]["icon"]}
        entry.update(res)
        providers.append(entry)
    return {"providers": providers}

@app.get("/providers/ping")
async def providers_ping(_: str = Depends(verify_api_key)):
    """\uB300\uC2DC\uBBFC\uC988 'AI \uD53C\uB4DC\uBC14\uC774\uB512 \uC0C1\uD0DC' \uBAA8\uB2EC\uC758 '\uC7AC\uD655\uC778' \uBC84\uD2BC\uC774 \uC2E4\uC81C\uB85C \uD638\uC6D0\uD558\uB294 \uAC15\uB9DD: /providers/ping-all\uAC00 \uC544\uB2C8\uB8BC \uC774 \uACBD\uB1B4 \uC5D0\uB77C\uAC94\uD3ED\uB4DC. \uAC00 \uC2A4\uC2AC \uBC18\uD658\uD558\uB350 {results: {id: {ok, ms, error}}} \uAD6C\uC870\uB85C \uBC18\uD658\uD55C\uB2E4.
    """
    ids = ["anthropic", "pexels", "pixabay", "elevenlabs", "openai", "stability", "edgetts", "youtube", "gemini", "local_sdxl"]
    values = await asyncio.gather(
        _ping_anthropic(), _ping_pexels(), _ping_pixabay(), _ping_elevenlabs(),
        _ping_openai(), _ping_stability(), _ping_edgetts(), _ping_youtube(), _ping_gemini(), _ping_local_sdxl(),
        return_exceptions=True,
    )
    results = {}
    for pid, res in zip(ids, values):
        if isinstance(res, Exception):
            res = {"ok": False, "ms": 0, "err": str(res)[:100]}
        results[pid] = {
            "ok": bool(res.get("ok")),
            "ms": res.get("ms", 0),
            "error": res.get("err", ""),
        }
    return {"results": results}

@app.get("/")
async def root():
    return {"service": "LongForm Factory Worker", "version": "17.37.0"}


@app.get("/ui", response_class=HTMLResponse)
async def dashboard():
    """Dual-mode dashboard: Stock Video + AI Cartoon."""
    ui_path = Path(__file__).parent / "ui" / "index.html"
    if not ui_path.exists():
        raise HTTPException(status_code=404, detail="UI not found")
    return HTMLResponse(content=ui_path.read_text(encoding="utf-8"))


@app.on_event("startup")
async def startup():
    logger.info("LongForm Factory Worker v17.37.0 started")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8002, reload=False)
