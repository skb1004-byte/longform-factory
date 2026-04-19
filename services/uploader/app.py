"""
LongForm Factory YouTube/Facebook Video Uploader Service
lf_uploader:3.2.0
Port: 8003
"""

import os
import logging
from typing import Optional
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, HTTPException, Header
import uvicorn
from pydantic import BaseModel

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
import googleapiclient.discovery
import googleapiclient.errors
import googleapiclient.http

import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class YouTubeUploadRequest(BaseModel):
    video_path: str
    title: str
    description: str
    tags: list[str] = []
    category_id: str = "22"
    privacy_status: str = "private"
    thumbnail_path: Optional[str] = None
    made_for_kids: bool = False


class YouTubeShortRequest(BaseModel):
    video_path: str
    title: str
    description: str
    tags: list[str] = []
    privacy_status: str = "private"
    made_for_kids: bool = False


class FacebookUploadRequest(BaseModel):
    video_path: str
    title: str
    description: str
    page_id: Optional[str] = None


class ThumbnailUploadRequest(BaseModel):
    video_id: str
    thumbnail_path: str


class YouTubeUploadResponse(BaseModel):
    success: bool
    video_id: str
    video_url: str
    title: str


class FacebookUploadResponse(BaseModel):
    success: bool
    post_id: str
    post_url: str


class StatusResponse(BaseModel):
    success: bool
    status: str
    video_id: str
    title: Optional[str] = None


app = FastAPI(
    title="LongForm Factory Uploader Service",
    version="3.2.0",
    description="YouTube/Facebook Video Distribution Service"
)

YOUTUBE_CLIENT_ID = os.getenv("YOUTUBE_CLIENT_ID")
YOUTUBE_CLIENT_SECRET = os.getenv("YOUTUBE_CLIENT_SECRET")
YOUTUBE_REFRESH_TOKEN = os.getenv("YOUTUBE_REFRESH_TOKEN")
YOUTUBE_CHANNEL_ID = os.getenv("YOUTUBE_CHANNEL_ID")

FACEBOOK_PAGE_ID = os.getenv("FACEBOOK_PAGE_ID")
FACEBOOK_PAGE_TOKEN = os.getenv("FACEBOOK_PAGE_TOKEN")

LF_API_KEY = os.getenv("LF_API_KEY", "")


def verify_api_key(x_lf_api_key: Optional[str] = Header(None)) -> bool:
    if not LF_API_KEY:
        logger.warning("LF_API_KEY not configured")
        return True
    
    if x_lf_api_key != LF_API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    return True


def get_youtube_service():
    credentials = Credentials(
        token=None,
        refresh_token=YOUTUBE_REFRESH_TOKEN,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=YOUTUBE_CLIENT_ID,
        client_secret=YOUTUBE_CLIENT_SECRET
    )
    
    request = Request()
    credentials.refresh(request)
    
    return googleapiclient.discovery.build(
        "youtube", "v3", credentials=credentials
    )


def upload_to_youtube(
    video_path: str,
    title: str,
    description: str,
    tags: list[str],
    category_id: str,
    privacy_status: str,
    thumbnail_path: Optional[str] = None,
    made_for_kids: bool = False,
    is_short: bool = False
) -> dict:
    
    if not Path(video_path).exists():
        raise FileNotFoundError(f"Video file not found: {video_path}")
    
    try:
        youtube = get_youtube_service()
        
        body = {
            "snippet": {
                "title": title,
                "description": description,
                "tags": tags[:50],
                "categoryId": category_id,
                "defaultLanguage": "ko",
                "defaultAudioLanguage": "ko"
            },
            "status": {
                "privacyStatus": privacy_status,
                "madeForKids": made_for_kids,
            }
        }
        
        if is_short:
            body["status"]["selfDeclaredMadeForKids"] = made_for_kids
        
        request = youtube.videos().insert(
            part="snippet,status",
            body=body,
            media_body=googleapiclient.http.MediaFileUpload(
                video_path,
                chunksize=10 * 1024 * 1024,
                resumable=True
            )
        )
        
        response = None
        retry_count = 0
        max_retries = 3
        
        while response is None and retry_count < max_retries:
            try:
                status, response = request.next_chunk()
                if status:
                    progress = int(status.progress() * 100)
                    logger.info(f"Upload progress: {progress}%")
            except googleapiclient.errors.HttpError as e:
                if e.resp.status in [500, 502, 503, 504]:
                    retry_count += 1
                    logger.warning(f"Server error, retry {retry_count}/{max_retries}")
                    continue
                elif e.resp.status == 403:
                    if "quotaExceeded" in str(e):
                        logger.error("YouTube quota exceeded")
                        raise HTTPException(
                            status_code=429,
                            detail="YouTube quota exceeded"
                        )
                raise
        
        if response is None:
            raise RuntimeError("Video upload failed")
        
        video_id = response["id"]
        
        if thumbnail_path and Path(thumbnail_path).exists():
            try:
                youtube.thumbnails().set(
                    videoId=video_id,
                    media_body=googleapiclient.http.MediaFileUpload(
                        thumbnail_path,
                        mimetype="image/jpeg",
                        resumable=False
                    )
                ).execute()
                logger.info(f"Thumbnail uploaded: {video_id}")
            except Exception as e:
                logger.error(f"Thumbnail upload failed: {e}")
        
        video_url = f"https://www.youtube.com/watch?v={video_id}"
        logger.info(f"YouTube upload success: {video_id}")
        
        return {
            "video_id": video_id,
            "video_url": video_url,
            "title": title
        }
    
    except Exception as e:
        logger.error(f"YouTube upload error: {e}")
        raise


def upload_to_facebook(
    video_path: str,
    title: str,
    description: str,
    page_id: Optional[str] = None
) -> dict:
    
    if not Path(video_path).exists():
        raise FileNotFoundError(f"Video file not found: {video_path}")
    
    page_id = page_id or FACEBOOK_PAGE_ID
    if not page_id:
        raise ValueError("Facebook page_id required")
    
    try:
        upload_url = f"https://graph.facebook.com/v18.0/{page_id}/videos"
        
        with open(video_path, "rb") as video_file:
            files = {"source": video_file}
            data = {
                "title": title,
                "description": description,
                "access_token": FACEBOOK_PAGE_TOKEN
            }
            
            response = requests.post(upload_url, files=files, data=data, timeout=3600)
            response.raise_for_status()
            
            result = response.json()
            
            if "id" not in result:
                raise RuntimeError("Facebook upload response missing ID")
            
            post_id = result["id"]
            post_url = f"https://www.facebook.com/{page_id}/videos/{post_id}"
            
            logger.info(f"Facebook upload success: {post_id}")
            
            return {
                "post_id": post_id,
                "post_url": post_url
            }
    
    except Exception as e:
        logger.error(f"Facebook upload error: {e}")
        raise


def get_youtube_video_status(video_id: str) -> dict:
    
    try:
        youtube = get_youtube_service()
        
        request = youtube.videos().list(
            part="status,snippet",
            id=video_id
        )
        response = request.execute()
        
        if not response["items"]:
            raise HTTPException(status_code=404, detail="Video not found")
        
        video = response["items"][0]
        status = video["status"]["uploadStatus"]
        title = video["snippet"]["title"]
        
        logger.info(f"Video status check: {video_id} = {status}")
        
        return {
            "status": status,
            "title": title
        }
    
    except Exception as e:
        logger.error(f"Status check error: {e}")
        raise


@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "service": "lf_uploader",
        "version": "3.2.0",
        "timestamp": datetime.utcnow().isoformat()
    }


@app.post("/upload/youtube", response_model=YouTubeUploadResponse)
async def upload_youtube(
    request: YouTubeUploadRequest,
    x_lf_api_key: Optional[str] = Header(None)
):
    verify_api_key(x_lf_api_key)
    
    try:
        result = upload_to_youtube(
            video_path=request.video_path,
            title=request.title,
            description=request.description,
            tags=request.tags,
            category_id=request.category_id,
            privacy_status=request.privacy_status,
            thumbnail_path=request.thumbnail_path,
            made_for_kids=request.made_for_kids,
            is_short=False
        )
        
        return YouTubeUploadResponse(
            success=True,
            video_id=result["video_id"],
            video_url=result["video_url"],
            title=result["title"]
        )
    
    except Exception as e:
        logger.error(f"YouTube upload failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/upload/youtube/short", response_model=YouTubeUploadResponse)
async def upload_youtube_short(
    request: YouTubeShortRequest,
    x_lf_api_key: Optional[str] = Header(None)
):
    verify_api_key(x_lf_api_key)
    
    try:
        result = upload_to_youtube(
            video_path=request.video_path,
            title=request.title,
            description=request.description,
            tags=request.tags,
            category_id="22",
            privacy_status=request.privacy_status,
            thumbnail_path=None,
            made_for_kids=request.made_for_kids,
            is_short=True
        )
        
        return YouTubeUploadResponse(
            success=True,
            video_id=result["video_id"],
            video_url=result["video_url"],
            title=result["title"]
        )
    
    except Exception as e:
        logger.error(f"YouTube Shorts upload failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/upload/facebook", response_model=FacebookUploadResponse)
async def upload_facebook(
    request: FacebookUploadRequest,
    x_lf_api_key: Optional[str] = Header(None)
):
    verify_api_key(x_lf_api_key)
    
    try:
        result = upload_to_facebook(
            video_path=request.video_path,
            title=request.title,
            description=request.description,
            page_id=request.page_id
        )
        
        return FacebookUploadResponse(
            success=True,
            post_id=result["post_id"],
            post_url=result["post_url"]
        )
    
    except Exception as e:
        logger.error(f"Facebook upload failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/status/youtube/{video_id}", response_model=StatusResponse)
async def get_youtube_status(
    video_id: str,
    x_lf_api_key: Optional[str] = Header(None)
):
    verify_api_key(x_lf_api_key)
    
    try:
        result = get_youtube_video_status(video_id)
        
        return StatusResponse(
            success=True,
            status=result["status"],
            video_id=video_id,
            title=result["title"]
        )
    
    except Exception as e:
        logger.error(f"Status check failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/thumbnail/upload/{video_id}")
async def upload_thumbnail(
    video_id: str,
    request: ThumbnailUploadRequest,
    x_lf_api_key: Optional[str] = Header(None)
):
    verify_api_key(x_lf_api_key)
    
    if request.video_id != video_id:
        raise HTTPException(status_code=400, detail="Video ID mismatch")
    
    try:
        if not Path(request.thumbnail_path).exists():
            raise FileNotFoundError(f"Thumbnail file not found: {request.thumbnail_path}")
        
        youtube = get_youtube_service()
        
        youtube.thumbnails().set(
            videoId=video_id,
            media_body=googleapiclient.http.MediaFileUpload(
                request.thumbnail_path,
                mimetype="image/jpeg",
                resumable=False
            )
        ).execute()
        
        logger.info(f"Thumbnail uploaded: {video_id}")
        
        return {
            "success": True,
            "video_id": video_id,
            "message": "Thumbnail uploaded successfully"
        }
    
    except Exception as e:
        logger.error(f"Thumbnail upload failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=8003,
        reload=False,
        log_level="info"
    )
