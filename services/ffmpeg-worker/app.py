# -*- coding: utf-8 -*-
"""
LongForm Factory - FFmpeg Worker v17.4.0
FastAPI router + auth + helpers.
Pipeline functions are in pipelines.py.
"""
from __future__ import annotations
import json, logging
from datetime import datetime
from pathlib import Path
from typing import Optional, List

from fastapi import FastAPI, HTTPException, BackgroundTasks, Header, Depends, Request
from fastapi.responses import FileResponse, StreamingResponse

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
app = FastAPI(title="LongForm Factory Worker", version="17.4.0")

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
        data = json.loads(scenes_file.read_text(encoding="utf-8"))
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
        "version": "17.4.0",
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
                    sd = json.loads(scenes_file.read_text(encoding="utf-8"))
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


@app.get("/")
async def root():
    return {"service": "LongForm Factory Worker", "version": "17.4.0"}


@app.on_event("startup")
async def startup():
    logger.info("LongForm Factory Worker v17.4.0 started")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8002, reload=False)
