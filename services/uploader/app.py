"""
LongForm Factory YouTube Uploader Service
lf_uploader:4.0.0  |  Port: 8003

癰궰野?????
  v4.0.0  Auto Topic Pipeline ????: job_id, caption, playlist, ?怨밴묶?곕뗄?? ??살첒?꾨뗀諭? backoff
  v3.2.0  YouTube + Facebook 疫꿸퀡????낆쨮??
"""

import os, logging, asyncio, time, json
from typing import Optional, List, Dict
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, HTTPException, Header, BackgroundTasks
from pydantic import BaseModel, Field
import uvicorn
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
import googleapiclient.discovery
import googleapiclient.errors
import googleapiclient.http
import requests as _requests
from dotenv import load_dotenv

load_dotenv()

# ???? ?닌듼??嚥≪뮄??????????????????????????????????????????????????????????????????????????????????????????????????
class _JsonFormatter(logging.Formatter):
    def format(self, record):
        d = {"time": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
             "level": record.levelname, "name": record.name, "msg": record.getMessage()}
        if record.exc_info:
            d["exc"] = self.formatException(record.exc_info)
        return json.dumps(d, ensure_ascii=False)

_handler = logging.StreamHandler()
_handler.setFormatter(_JsonFormatter())
logging.basicConfig(level=logging.INFO, handlers=[_handler])
logger = logging.getLogger("uploader")

# ???? ??띻펾 癰궰??????????????????????????????????????????????????????????????????????????????????????????????????????
LF_API_KEY              = os.getenv("LF_API_KEY", "")
YOUTUBE_CLIENT_ID       = os.getenv("YOUTUBE_CLIENT_ID", "")
YOUTUBE_CLIENT_SECRET   = os.getenv("YOUTUBE_CLIENT_SECRET", "")
YOUTUBE_REFRESH_TOKEN   = os.getenv("YOUTUBE_REFRESH_TOKEN", "")
YOUTUBE_CHANNEL_ID      = os.getenv("YOUTUBE_CHANNEL_ID", "")
YOUTUBE_UPLOAD_ENABLED  = os.getenv("YOUTUBE_UPLOAD_ENABLED", "true").lower() in ("true","1","yes")
YOUTUBE_DEFAULT_PRIVACY = os.getenv("YOUTUBE_DEFAULT_PRIVACY", "private")
YOUTUBE_DEFAULT_CAT     = os.getenv("YOUTUBE_DEFAULT_CATEGORY_ID", "22")
YOUTUBE_RETRY_COUNT     = int(os.getenv("YOUTUBE_UPLOAD_RETRY_COUNT", "3"))
YOUTUBE_CHUNK_MB        = int(os.getenv("YOUTUBE_UPLOAD_CHUNK_SIZE_MB", "8"))
YOUTUBE_TOKEN_PATH      = os.getenv("YOUTUBE_TOKEN_PATH", "/data/secrets/youtube_token.json")
YOUTUBE_CLIENT_SECRET_PATH = os.getenv("YOUTUBE_CLIENT_SECRET_PATH",
                                        "/data/secrets/youtube_client_secret.json")
FACEBOOK_PAGE_ID        = os.getenv("FACEBOOK_PAGE_ID", "")
FACEBOOK_PAGE_TOKEN     = os.getenv("FACEBOOK_PAGE_TOKEN", "")

VERSION = "4.0.0"

# ???? ??살첒 ?꾨뗀諭??怨몃땾 ??????????????????????????????????????????????????????????????????????????????????????????
class UploadError:
    AUTH_REQUIRED        = "YOUTUBE_AUTH_REQUIRED"
    TOKEN_EXPIRED        = "YOUTUBE_TOKEN_EXPIRED"
    TOKEN_REFRESH_FAILED = "YOUTUBE_TOKEN_REFRESH_FAILED"
    FILE_MISSING         = "YOUTUBE_VIDEO_FILE_MISSING"
    FILE_TOO_LARGE       = "YOUTUBE_VIDEO_FILE_TOO_LARGE"
    UPLOAD_FAILED        = "YOUTUBE_UPLOAD_FAILED"
    THUMBNAIL_FAILED     = "YOUTUBE_THUMBNAIL_FAILED"
    CAPTION_FAILED       = "YOUTUBE_CAPTION_FAILED"
    PLAYLIST_ADD_FAILED  = "YOUTUBE_PLAYLIST_ADD_FAILED"
    QUOTA_EXCEEDED       = "YOUTUBE_QUOTA_EXCEEDED"
    PRIVACY_RESTRICTED   = "YOUTUBE_PRIVACY_RESTRICTED"

# ???? ??낆쨮???怨밴묶 ????????????????????????????????????????????????????????????????????????????????????????????
_JOB_STORE: Dict[str, Dict] = {}

def _set_job(job_id: str, **kw):
    _JOB_STORE.setdefault(job_id, {}).update({"updated_at": datetime.now().isoformat(), **kw})

# ???? Pydantic 筌뤴뫀??????????????????????????????????????????????????????????????????????????????????????????????
class YouTubeUploadRequest(BaseModel):
    """??? YouTube ??낆쨮???遺욧퍕"""
    job_id: Optional[str] = Field(None, description="LF job_id (?怨밴묶 ?곕뗄???")
    video_path: str
    title: str = Field(..., max_length=100)
    description: str = ""
    tags: List[str] = []
    category_id: str = Field(default_factory=lambda: YOUTUBE_DEFAULT_CAT)
    privacy_status: str = Field(default_factory=lambda: YOUTUBE_DEFAULT_PRIVACY)
    thumbnail_path: Optional[str] = None
    caption_path: Optional[str] = None       # SRT/ASS ?癒?춵 ???뵬 野껋럥以?
    playlist_id: Optional[str] = None        # ???쟿?????쎈뱜 ID
    publish_at: Optional[str] = None         # ISO 8601 ??됰튋 野껊슣????볦퍟
    made_for_kids: bool = False

class YouTubeShortRequest(BaseModel):
    job_id: Optional[str] = None
    video_path: str
    title: str
    description: str = ""
    tags: List[str] = []
    privacy_status: str = Field(default_factory=lambda: YOUTUBE_DEFAULT_PRIVACY)
    made_for_kids: bool = False

class YouTubeUploadResponse(BaseModel):
    success: bool
    job_id: Optional[str] = None
    upload_status: str = "upload_completed"
    youtube_video_id: Optional[str] = None
    youtube_url: Optional[str] = None
    title: Optional[str] = None
    uploaded_at: Optional[str] = None
    error: Optional[str] = None
    error_code: Optional[str] = None

class FacebookUploadRequest(BaseModel):
    video_path: str
    title: str
    description: str = ""
    page_id: Optional[str] = None

class ThumbnailUploadRequest(BaseModel):
    video_id: str
    thumbnail_path: str

# ???? FastAPI ????????????????????????????????????????????????????????????????????????????????????????????????????????
app = FastAPI(
    root_path="/api/upload",
    title="LongForm Factory Uploader",
    version=VERSION,
    description="YouTube/Facebook ?癒?짗 ??낆쨮????뺥돩????Auto Topic Pipeline ????"
)

# ???? ?⑤벏???紐꾩쵄 ????????????????????????????????????????????????????????????????????????????????????????????????????
def _verify_key(x_lf_api_key: Optional[str]) -> bool:
    if LF_API_KEY and x_lf_api_key != LF_API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return True

# ???? YouTube OAuth2 ?????곷섧??????????????????????????????????????????????????????????????????????
def _get_youtube():
    """OAuth2 ?癒?봄筌앹빖梨??곗쨮 YouTube client 獄쏆꼹?? ??쎈솭 ??UploadError ??釉???됱뇚."""
    # 獄쎻뫖苡?1: ??띻펾 癰궰??筌욊낯??(REFRESH_TOKEN 獄쎻뫗??
    if YOUTUBE_REFRESH_TOKEN and YOUTUBE_CLIENT_ID and YOUTUBE_CLIENT_SECRET:
        try:
            creds = Credentials(
                token=None,
                refresh_token=YOUTUBE_REFRESH_TOKEN,
                token_uri="https://oauth2.googleapis.com/token",
                client_id=YOUTUBE_CLIENT_ID,
                client_secret=YOUTUBE_CLIENT_SECRET,
            )
            creds.refresh(Request())
            return googleapiclient.discovery.build("youtube", "v3", credentials=creds,
                                                    cache_discovery=False)
        except Exception as e:
            if "invalid_grant" in str(e).lower():
                raise RuntimeError(f"{UploadError.TOKEN_REFRESH_FAILED}: {e}")
            raise RuntimeError(f"{UploadError.TOKEN_EXPIRED}: {e}")

    # 獄쎻뫖苡?2: youtube_token.json ???뵬
    token_path = Path(YOUTUBE_TOKEN_PATH)
    if token_path.exists():
        try:
            creds = Credentials.from_authorized_user_file(str(token_path))
            if creds.expired and creds.refresh_token:
                creds.refresh(Request())
                token_path.write_text(creds.to_json())
            return googleapiclient.discovery.build("youtube", "v3", credentials=creds,
                                                    cache_discovery=False)
        except Exception as e:
            raise RuntimeError(f"{UploadError.TOKEN_REFRESH_FAILED}: {e}")

    raise RuntimeError(UploadError.AUTH_REQUIRED +
                       ": YOUTUBE_REFRESH_TOKEN ?癒?뮉 youtube_token.json ?袁⑹뒄")

# ???? exponential backoff ???????????????????????????????????????????????????????????????????????
def _upload_with_retry(media_insert_request, job_id: Optional[str] = None) -> Dict:
    """resumable upload + exponential backoff. 筌ㅼ뮆? YOUTUBE_RETRY_COUNT ??"""
    response = None
    error = None
    retry = 0
    while response is None:
        try:
            status, response = media_insert_request.next_chunk()
            if status:
                pct = int(status.progress() * 100)
                logger.info(f"[UPLOAD:{job_id}] ??낆쨮??筌욊쑵六? {pct}%")
                if job_id:
                    _set_job(job_id, upload_status="uploading_video", progress=pct)
        except googleapiclient.errors.HttpError as e:
            if e.resp.status in (500, 502, 503, 504) and retry < YOUTUBE_RETRY_COUNT:
                wait = min(2 ** retry * 5, 60)
                logger.warning(f"[UPLOAD:{job_id}] ??뺤쒔 ??살첒 {e.resp.status}, {wait}?????????({retry+1}/{YOUTUBE_RETRY_COUNT})")
                time.sleep(wait)
                retry += 1
                error = e
            elif e.resp.status == 403:
                detail = str(e)
                if "quotaExceeded" in detail:
                    raise RuntimeError(UploadError.QUOTA_EXCEEDED)
                if "forbidden" in detail.lower():
                    raise RuntimeError(UploadError.PRIVACY_RESTRICTED)
                raise
            else:
                raise RuntimeError(f"{UploadError.UPLOAD_FAILED}: {e}")
    if response is None:
        raise RuntimeError(f"{UploadError.UPLOAD_FAILED}: {error}")
    return response

# ???? ???뼎 ??낆쨮????λ땾 ??????????????????????????????????????????????????????????????????????????????????????
def _do_youtube_upload(
    video_path: str, title: str, description: str, tags: List[str],
    category_id: str, privacy_status: str, thumbnail_path: Optional[str],
    caption_path: Optional[str], playlist_id: Optional[str],
    publish_at: Optional[str], made_for_kids: bool, is_short: bool,
    job_id: Optional[str],
) -> Dict:
    """YouTube videos.insert + thumbnails + captions + playlist."""

    # ???뵬 ?醫륁뒞??
    vp = Path(video_path)
    if not vp.exists():
        raise RuntimeError(UploadError.FILE_MISSING)
    size_mb = vp.stat().st_size / 1024 / 1024
    if size_mb > 256 * 1024:  # 256GB YouTube ??쀫립
        raise RuntimeError(UploadError.FILE_TOO_LARGE)

    if job_id:
        _set_job(job_id, upload_status="upload_authenticating")

    youtube = _get_youtube()

    # videos.insert body
    body: Dict = {
        "snippet": {
            "title": title[:100],
            "description": description[:5000],
            "tags": tags[:50],
            "categoryId": category_id,
            "defaultLanguage": "ko",
            "defaultAudioLanguage": "ko",
        },
        "status": {
            "privacyStatus": privacy_status,
            "selfDeclaredMadeForKids": made_for_kids,
            "madeForKids": made_for_kids,
        },
    }
    # ??됰튋 野껊슣??(publishAt?? privacyStatus=private + publishAt 鈺곌퀬鍮)
    if publish_at:
        body["status"]["publishAt"] = publish_at
        body["status"]["privacyStatus"] = "private"

    if job_id:
        _set_job(job_id, upload_status="uploading_video", progress=0)

    logger.info(f"[UPLOAD:{job_id}] videos.insert ??뽰삂: {title[:40]} ({size_mb:.1f}MB)")
    insert_req = youtube.videos().insert(
        part="snippet,status",
        body=body,
        media_body=googleapiclient.http.MediaFileUpload(
            str(vp),
            chunksize=YOUTUBE_CHUNK_MB * 1024 * 1024,
            resumable=True,
        ),
    )
    response = _upload_with_retry(insert_req, job_id)
    video_id = response["id"]
    youtube_url = f"https://www.youtube.com/watch?v={video_id}"
    logger.info(f"[UPLOAD:{job_id}] ??낆쨮???袁⑥┷: {video_id}")

    # thumbnails.set (??쎈솭??猷??袁⑷퍥 ??쎈솭 ?袁⑤뻷)
    if thumbnail_path and Path(thumbnail_path).exists():
        try:
            if job_id:
                _set_job(job_id, upload_status="uploading_thumbnail")
            youtube.thumbnails().set(
                videoId=video_id,
                media_body=googleapiclient.http.MediaFileUpload(
                    thumbnail_path, mimetype="image/jpeg", resumable=False
                ),
            ).execute()
            logger.info(f"[UPLOAD:{job_id}] ?紐껉퐬????쇱젟 ?袁⑥┷: {video_id}")
        except Exception as te:
            logger.warning(f"[UPLOAD:{job_id}] ?紐껉퐬????쎈솭 (?얜똻??: {te}")

    # captions.insert (??쎈솭??猷??袁⑷퍥 ??쎈솭 ?袁⑤뻷)
    if caption_path and Path(caption_path).exists():
        try:
            if job_id:
                _set_job(job_id, upload_status="uploading_caption")
            caption_body = {
                "snippet": {
                    "videoId": video_id,
                    "language": "ko",
                    "name": "Korean",
                    "isDraft": False,
                }
            }
            youtube.captions().insert(
                part="snippet",
                body=caption_body,
                media_body=googleapiclient.http.MediaFileUpload(
                    caption_path, mimetype="application/octet-stream", resumable=False
                ),
            ).execute()
            logger.info(f"[UPLOAD:{job_id}] ?癒?춵 ??낆쨮???袁⑥┷: {video_id}")
        except Exception as ce:
            logger.warning(f"[UPLOAD:{job_id}] ?癒?춵 ??쎈솭 (?얜똻??: {ce}")

    # playlistItems.insert (playlist_id揶쎛 ??됱뱽 ???춸)
    if playlist_id:
        try:
            if job_id:
                _set_job(job_id, upload_status="adding_to_playlist")
            youtube.playlistItems().insert(
                part="snippet",
                body={
                    "snippet": {
                        "playlistId": playlist_id,
                        "resourceId": {"kind": "youtube#video", "videoId": video_id},
                    }
                },
            ).execute()
            logger.info(f"[UPLOAD:{job_id}] ???쟿?????쎈뱜 ?곕떽? ?袁⑥┷: {playlist_id}")
        except Exception as pe:
            logger.warning(f"[UPLOAD:{job_id}] ???쟿?????쎈뱜 ??쎈솭 (?얜똻??: {pe}")

    return {
        "youtube_video_id": video_id,
        "youtube_url": youtube_url,
        "title": title,
        "uploaded_at": datetime.now().isoformat(),
    }

# ???? ??쑬猷욄묾???낆쨮????묐쓠 ??????????????????????????????????????????????????????????????????????????????????
async def _async_upload(job_id: str, req: YouTubeUploadRequest):
    """Background task: async YouTube upload wrapper."""
    try:
        result = await asyncio.to_thread(
            _do_youtube_upload,
            req.video_path, req.title, req.description, req.tags,
            req.category_id, req.privacy_status, req.thumbnail_path,
            req.caption_path, req.playlist_id, req.publish_at,
            req.made_for_kids, False, job_id,
        )
        _set_job(job_id, upload_status="upload_completed",
                 youtube_video_id=result["youtube_video_id"],
                 youtube_url=result["youtube_url"],
                 uploaded_at=result["uploaded_at"],
                 error=None, error_code=None)
    except Exception as e:
        ec = str(e).split(":")[0].strip() if ":" in str(e) else UploadError.UPLOAD_FAILED
        logger.error(f"[UPLOAD:{job_id}] ??쎈솭: {e}")
        _set_job(job_id, upload_status="upload_failed",
                 error=str(e), error_code=ec)

# ???? API ?遺얜굡???????????????????????????????????????????????????????????????????????????????????????????????

@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "service": "lf_uploader",
        "version": VERSION,
        "youtube_enabled": YOUTUBE_UPLOAD_ENABLED,
        "default_privacy": YOUTUBE_DEFAULT_PRIVACY,
        "timestamp": datetime.now().isoformat(),
    }

# ???? ??? 野껋럥以?(/upload/youtube) ????????????????????????????????????????????????????????????????
@app.post("/upload/youtube", response_model=YouTubeUploadResponse, tags=["YouTube"])
@app.post("/api/upload/upload/youtube", response_model=YouTubeUploadResponse, tags=["YouTube"])
async def upload_youtube(
    request: YouTubeUploadRequest,
    background_tasks: BackgroundTasks,
    x_lf_api_key: Optional[str] = Header(None, alias="X-LF-API-Key"),
):
    """
    YouTube ?怨멸맒 ??낆쨮??
    /upload/youtube and /api/upload/upload/youtube dual route support.
    job_id ??됱몵筌???쑬猷욄묾???쎈뻬 ??GET /upload/status/{job_id} 嚥??類ㅼ뵥.
    """
    _verify_key(x_lf_api_key)

    if not YOUTUBE_UPLOAD_ENABLED:
        raise HTTPException(status_code=503, detail="YouTube upload disabled (YOUTUBE_UPLOAD_ENABLED=false)")

    if not Path(request.video_path).exists():
        raise HTTPException(status_code=400,
                            detail=f"{UploadError.FILE_MISSING}: {request.video_path}")

    job_id = request.job_id or f"up_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    _set_job(job_id, upload_status="upload_queued", video_path=request.video_path,
             title=request.title, created_at=datetime.now().isoformat())

    # ??쑬猷욄묾???쎈뻬 (background)
    background_tasks.add_task(_async_upload, job_id, request)

    return YouTubeUploadResponse(
        success=True,
        job_id=job_id,
        upload_status="upload_queued",
        title=request.title,
    )


@app.post("/upload/youtube/short", response_model=YouTubeUploadResponse, tags=["YouTube"])
async def upload_youtube_short(
    request: YouTubeShortRequest,
    background_tasks: BackgroundTasks,
    x_lf_api_key: Optional[str] = Header(None, alias="X-LF-API-Key"),
):
    """YouTube Shorts ??낆쨮??"""
    _verify_key(x_lf_api_key)
    if not Path(request.video_path).exists():
        raise HTTPException(status_code=400, detail=UploadError.FILE_MISSING)

    job_id = request.job_id or f"short_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    full_req = YouTubeUploadRequest(
        job_id=job_id,
        video_path=request.video_path,
        title=request.title,
        description=request.description,
        tags=request.tags,
        privacy_status=request.privacy_status,
        made_for_kids=request.made_for_kids,
    )
    _set_job(job_id, upload_status="upload_queued", created_at=datetime.now().isoformat())
    background_tasks.add_task(_async_upload, job_id, full_req)
    return YouTubeUploadResponse(success=True, job_id=job_id, upload_status="upload_queued")


@app.get("/upload/status/{job_id}", tags=["Status"])
async def get_upload_status(
    job_id: str,
    x_lf_api_key: Optional[str] = Header(None, alias="X-LF-API-Key"),
):
    """??낆쨮???臾믩씜 ?怨밴묶 鈺곌퀬??"""
    _verify_key(x_lf_api_key)
    job = _JOB_STORE.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"job_id '{job_id}' not found")
    return {
        "job_id": job_id,
        "upload_status": job.get("upload_status"),
        "progress": job.get("progress"),
        "youtube_video_id": job.get("youtube_video_id"),
        "youtube_url": job.get("youtube_url"),
        "title": job.get("title"),
        "uploaded_at": job.get("uploaded_at"),
        "error": job.get("error"),
        "error_code": job.get("error_code"),
        "updated_at": job.get("updated_at"),
    }


@app.get("/status/youtube/{video_id}", tags=["Status"])
async def get_youtube_video_status(
    video_id: str,
    x_lf_api_key: Optional[str] = Header(None, alias="X-LF-API-Key"),
):
    """YouTube video_id嚥???낆쨮??筌ｌ꼶???怨밴묶 筌욊낯??鈺곌퀬??"""
    _verify_key(x_lf_api_key)
    try:
        youtube = _get_youtube()
        resp = youtube.videos().list(part="status,snippet", id=video_id).execute()
        if not resp.get("items"):
            raise HTTPException(status_code=404, detail="Video not found")
        item = resp["items"][0]
        return {
            "success": True,
            "video_id": video_id,
            "status": item["status"].get("uploadStatus"),
            "privacy_status": item["status"].get("privacyStatus"),
            "title": item["snippet"].get("title"),
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/thumbnail/upload/{video_id}", tags=["YouTube"])
async def upload_thumbnail(
    video_id: str,
    request: ThumbnailUploadRequest,
    x_lf_api_key: Optional[str] = Header(None, alias="X-LF-API-Key"),
):
    """?紐껉퐬??揶쏆뮆????낆쨮??"""
    _verify_key(x_lf_api_key)
    if not Path(request.thumbnail_path).exists():
        raise HTTPException(status_code=400, detail=UploadError.FILE_MISSING)
    try:
        youtube = _get_youtube()
        youtube.thumbnails().set(
            videoId=video_id,
            media_body=googleapiclient.http.MediaFileUpload(
                request.thumbnail_path, mimetype="image/jpeg", resumable=False
            ),
        ).execute()
        return {"success": True, "video_id": video_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{UploadError.THUMBNAIL_FAILED}: {e}")


@app.post("/upload/facebook", tags=["Facebook"])
async def upload_facebook(
    request: FacebookUploadRequest,
    x_lf_api_key: Optional[str] = Header(None, alias="X-LF-API-Key"),
):
    """Facebook ??륁뵠筌왖 ?怨멸맒 ??낆쨮??"""
    _verify_key(x_lf_api_key)
    page_id = request.page_id or FACEBOOK_PAGE_ID
    if not page_id or not FACEBOOK_PAGE_TOKEN:
        raise HTTPException(status_code=503, detail="Facebook credentials not configured")
    if not Path(request.video_path).exists():
        raise HTTPException(status_code=400, detail=UploadError.FILE_MISSING)
    try:
        with open(request.video_path, "rb") as vf:
            resp = _requests.post(
                f"https://graph.facebook.com/v18.0/{page_id}/videos",
                files={"source": vf},
                data={"title": request.title, "description": request.description,
                      "access_token": FACEBOOK_PAGE_TOKEN},
                timeout=3600,
            )
            resp.raise_for_status()
            data = resp.json()
        post_id = data.get("id", "")
        return {"success": True, "post_id": post_id,
                "post_url": f"https://www.facebook.com/{page_id}/videos/{post_id}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8003, reload=False, log_level="info")
