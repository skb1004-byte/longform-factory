"""
LongForm Factory - FFmpeg Worker v15.8.0 (autopatch stream_loop)
롱폼/숏폼 자동화 영상 제작 서비스

주요 기능:
- Pexels/Pixabay 영상 자산 검색 및 다운로드
- FFmpeg 기반 영상 합성 (장편/숏폼)
- 썸네일 생성 및 자막 처리
- 배경음악 믹싱
"""

import os
import shutil
import json
import asyncio
import subprocess
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Dict, Any
from dataclasses import dataclass, asdict, field
from enum import Enum
import logging

from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
import httpx
import aiofiles
from PIL import Image, ImageDraw, ImageFont
import uvicorn


# ============================================================================
# 로깅 설정
# ============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(name)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============================================================================
# 열거형 정의
# ============================================================================
class VideoMode(str, Enum):
    """영상 제작 모드"""
    LONGFORM = "longform"  # 1920x1080 가로형
    SHORTFORM = "shortform"  # 1080x1920 세로형
    MUSIC_VIDEO = "music_video"  # BGM + 자막 뮤직비디오


class JobStatus(str, Enum):
    """작업 상태"""
    PENDING = "pending"
    DOWNLOADING_ASSETS = "downloading_assets"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class AssetType(str, Enum):
    """자산 유형"""
    VIDEO = "video"
    IMAGE = "image"
    AUDIO = "audio"


# ============================================================================
# 리듬 컷 · 자막 선행 파라미터 (환경변수로 override 가능)
# ============================================================================
import os as _rhythm_os
SUBTITLE_LEAD_SEC   = float(_rhythm_os.getenv("SUBTITLE_LEAD_SEC", "0.15"))   # 자막 선행 시간
SCENE_MIN_SEC       = float(_rhythm_os.getenv("SCENE_MIN_SEC", "2.0"))        # 씬 최소 길이
SCENE_MAX_SEC       = float(_rhythm_os.getenv("SCENE_MAX_SEC", "4.0"))        # 씬 최대 길이 (초과 시 분할)
SUBTITLE_MAX_CHARS  = int(_rhythm_os.getenv("SUBTITLE_MAX_CHARS", "15"))      # 자막 한 줄 최대 글자
PAUSE_THRESHOLD_SEC = float(_rhythm_os.getenv("PAUSE_THRESHOLD_SEC", "0.3"))  # [Q2] 쉼으로 인정할 단어 간격

# [Q3] 복합어 보호: 자막 줄바꿈 금지 N-그램
_NO_BREAK_DEFAULT = [
    "진공 챔버", "열 시험", "진동 시험", "우주 환경", "위성 테스트",
    "궤도 진입", "발사체 성능", "지상국 관제", "모듈러 프리팹",
    "딥러닝 모델", "머신러닝 모델", "양자 통신", "양자 광통신",
    "인공지능", "AI", "API", "IoT",
]
_NO_BREAK_ENV = _rhythm_os.getenv("NO_BREAK_TERMS", "")
NO_BREAK_TERMS = _NO_BREAK_DEFAULT + [t.strip() for t in _NO_BREAK_ENV.split(",") if t.strip()]
_NBSP = "\u00a0"  # 줄바꿈 금지용 non-breaking space


# ============================================================================
# Pydantic 데이터 모델
# ============================================================================
class Scene(BaseModel):
    """영상 장면 정의"""
    scene_id: str = Field(..., description="장면 고유 ID")
    keyword: str = Field(..., description="검색 키워드")
    duration_seconds: float = Field(..., ge=0.5, le=120, description="장면 길이(초)")
    description: Optional[str] = Field(None, description="장면 설명")
    asset_url: Optional[str] = Field(None, description="다운로드된 자산 URL")
    asset_type: AssetType = Field(default=AssetType.VIDEO, description="자산 유형")


class AssetsSearchRequest(BaseModel):
    """자산 검색 요청"""
    job_id: str = Field(..., description="작업 ID")
    scenes: List[Scene] = Field(..., min_items=1, description="검색할 장면 목록")
    sources: str = Field(default="pexels,pixabay", description="검색 소스 (쉼표 구분)")


class VideoCreateRequest(BaseModel):
    """영상 생성 요청"""
    job_id: str = Field(..., description="작업 ID")
    mode: VideoMode = Field(default=VideoMode.LONGFORM, description="제작 모드")
    resolution: str = Field(default="1920x1080", description="출력 해상도")
    fps: int = Field(default=30, ge=24, le=60, description="프레임률")
    add_subtitles: bool = Field(default=False, description="자막 추가 여부")
    add_bgm: bool = Field(default=True, description="배경음악 추가 여부")
    bgm_volume: float = Field(default=0.3, ge=0.0, le=1.0, description="배경음악 볼륨(0-1)")
    generate_thumbnail: bool = Field(default=True, description="썸네일 생성")
    generate_shorts: bool = Field(default=True, description="숏폼 생성")
    title: Optional[str] = Field(None, description="썸네일에 표시할 제목")
    subtitle_text: Optional[str] = Field(None, description="뮤직비디오 자막 텍스트")
    audio_url: Optional[str] = Field(None, description="TTS 오디오 경로 (절대경로 또는 /data/tmp/...)")
    output_filename: Optional[str] = Field(None, description="출력 파일명 (기본: job_id.mp4)")
    transition: str = Field(default="fade", description="클립 전환 효과")
    scenes: Optional[list] = Field(None, description="씬 목록 (없으면 scenes.json 로드)")


class JobInfo(BaseModel):
    """작업 상태 정보"""
    job_id: str
    status: JobStatus
    progress: float = Field(default=0.0, ge=0.0, le=100.0)
    output_files: Dict[str, str] = Field(default_factory=dict)
    error: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    duration_seconds: Optional[float] = None


class AssetsSearchResponse(BaseModel):
    """자산 검색 응답"""
    job_id: str
    status: str
    scenes: List[Scene]
    downloaded_count: int
    total_count: int


class VideoCreateResponse(BaseModel):
    """영상 생성 응답"""
    success: bool
    job_id: str
    status: str
    output_files: Dict[str, str] = Field(default_factory=dict)
    duration_seconds: Optional[float] = None
    error: Optional[str] = None


# ============================================================================
# 환경 변수 및 경로 설정
# ============================================================================
PEXELS_API_KEY = os.getenv("PEXELS_API_KEY", "")
PIXABAY_API_KEY = os.getenv("PIXABAY_API_KEY", "")
LF_API_KEY = os.getenv("LF_API_KEY", "")

# 데이터 디렉토리 설정
BASE_DATA_DIR = Path("/data")
JOBS_DIR = BASE_DATA_DIR / "jobs"
TMP_DIR = BASE_DATA_DIR / "tmp"
OUTPUT_DIR = BASE_DATA_DIR / "output"
BGM_DIR = BASE_DATA_DIR / "bgm"

# 출력 디렉토리 구분
LONGFORM_DIR = OUTPUT_DIR / "longform"
SHORTS_DIR = OUTPUT_DIR / "shorts"
THUMBNAILS_DIR = OUTPUT_DIR / "thumbnails"
COMPLETE_DIR = BASE_DATA_DIR / "complete"

# 디렉토리 생성
for directory in [JOBS_DIR, TMP_DIR, OUTPUT_DIR, LONGFORM_DIR, SHORTS_DIR, THUMBNAILS_DIR, BGM_DIR, COMPLETE_DIR, COMPLETE_DIR / 'longform', COMPLETE_DIR / 'shorts', COMPLETE_DIR / 'thumbnails']:
    directory.mkdir(parents=True, exist_ok=True)

logger.info(f"데이터 디렉토리 초기화 완료: {BASE_DATA_DIR}")


# ============================================================================
# FastAPI 앱 초기화
# ============================================================================
app = FastAPI(
    title="LongForm Factory - FFmpeg Worker",
    description="롱폼/숏폼 자동화 영상 제작 서비스",
    version="15.0.0"
)



# ==================== CORS (브라우저 UI 직접 호출 허용) ====================
from fastapi.middleware.cors import CORSMiddleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)
# 동시 영상 생성 제한 (메모리 과부하 방지)
_CURRENT_JOB: Optional[str] = None

# 작업 상태 저장소 (인메모리)
jobs: Dict[str, JobInfo] = {}


# ============================================================================
# 헬퍼 함수들
# ============================================================================

async def update_job_status(
    job_id: str,
    status: JobStatus,
    progress: float = None,
    error: str = None,
    output_files: Dict[str, str] = None,
    duration_seconds: float = None
) -> None:
    """작업 상태 업데이트"""
    if job_id not in jobs:
        jobs[job_id] = JobInfo(
            job_id=job_id,
            status=status,
            created_at=datetime.now(),
            updated_at=datetime.now()
        )
    else:
        job = jobs[job_id]
        job.status = status
        if progress is not None:
            job.progress = progress
        if error:
            job.error = error
        if output_files:
            job.output_files.update(output_files)
        if duration_seconds is not None:
            job.duration_seconds = duration_seconds
        job.updated_at = datetime.now()
    
    logger.info(f"작업 상태 업데이트: {job_id} -> {status.value} (진행률: {jobs[job_id].progress}%)")


async def get_pexels_videos(keyword: str, per_page: int = 5) -> List[Dict[str, Any]]:
    """Pexels API에서 영상 검색"""
    if not PEXELS_API_KEY:
        logger.warning("Pexels API 키가 없습니다")
        return []
    
    url = "https://api.pexels.com/videos/search"
    headers = {"Authorization": PEXELS_API_KEY}
    params = {
        "query": keyword,
        "per_page": per_page,
        "orientation": "landscape"
    }
    
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            videos = data.get("videos", [])
            logger.info(f"Pexels에서 '{keyword}' 검색: {len(videos)}개 결과")
            return videos
    except Exception as e:
        logger.error(f"Pexels 검색 오류 ({keyword}): {e}")
        return []


async def get_pixabay_videos(keyword: str, per_page: int = 5) -> List[Dict[str, Any]]:
    """Pixabay API에서 영상 검색"""
    if not PIXABAY_API_KEY:
        logger.warning("Pixabay API 키가 없습니다")
        return []
    
    url = "https://pixabay.com/api/videos/"
    params = {
        "key": PIXABAY_API_KEY,
        "q": keyword,
        "per_page": per_page,
        "min_width": 640,
        "min_height": 360
    }
    
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            videos = data.get("hits", [])
            logger.info(f"Pixabay에서 '{keyword}' 검색: {len(videos)}개 결과")
            return videos
    except Exception as e:
        logger.error(f"Pixabay 검색 오류 ({keyword}): {e}")
        return []


def select_best_video(pexels_videos: List[Dict], pixabay_videos: List[Dict]) -> Optional[str]:
    """최고 품질의 영상 선택"""
    candidates = []
    
    # Pexels 영상 처리
    for video in pexels_videos:
        video_files = video.get("video_files", [])
        if video_files:
            # 가장 높은 해상도의 파일 선택
            best_file = max(
                video_files,
                key=lambda f: int(f.get("width", 0)) * int(f.get("height", 0))
            )
            if best_file.get("link"):
                candidates.append({
                    "url": best_file["link"],
                    "width": best_file.get("width", 0),
                    "height": best_file.get("height", 0)
                })
    
    # Pixabay 영상 처리
    for video in pixabay_videos:
        video_files = video.get("videos", {})
        # large, medium, small 중에서 large 선택
        if "large" in video_files:
            url = video_files["large"].get("url")
            if url:
                candidates.append({
                    "url": url,
                    "width": video_files["large"].get("width", 0),
                    "height": video_files["large"].get("height", 0)
                })
    
    if candidates:
        # 해상도 기준으로 최고 품질 선택
        best = max(
            candidates,
            key=lambda c: int(c.get("width", 0)) * int(c.get("height", 0))
        )
        logger.info(f"선택된 영상: {best['width']}x{best['height']}")
        return best["url"]
    
    return None


async def download_video(video_url: str, output_path: Path, timeout: float = 120.0, max_duration: float = 60.0) -> bool:
    """영상 다운로드 — ffmpeg으로 직접 다운로드 + 60초 자동 트리밍 (908MB 방지)"""
    try:
        logger.info(f"영상 다운로드 시작 (최대 {max_duration}초): {video_url} -> {output_path}")
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # 1차: ffmpeg으로 스트림 다운로드 + 트리밍
        cmd = [
            "ffmpeg", "-y",
            "-t", str(max_duration),
            "-i", video_url,
            "-t", str(max_duration),
            "-c:v", "libx264", "-preset", "ultrafast", "-crf", "23",
            "-c:a", "aac", "-b:a", "128k",
            "-movflags", "+faststart",
            str(output_path)
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=180)
        if result.returncode == 0 and output_path.exists() and output_path.stat().st_size > 10000:
            file_size = output_path.stat().st_size
            logger.info(f"영상 다운로드 완료 (ffmpeg): {output_path} ({file_size/(1024*1024):.2f}MB)")
            return True

        # 2차 fallback: httpx 스트리밍 (최대 30MB)
        logger.warning(f"ffmpeg 다운로드 실패 — httpx fallback")
        async with httpx.AsyncClient(timeout=timeout) as client:
            async with client.stream("GET", video_url) as response:
                response.raise_for_status()
                downloaded = 0
                max_bytes = 30 * 1024 * 1024  # 30MB 제한
                async with aiofiles.open(output_path, "wb") as f:
                    async for chunk in response.aiter_bytes(chunk_size=65536):
                        await f.write(chunk)
                        downloaded += len(chunk)
                        if downloaded >= max_bytes:
                            logger.info(f"30MB 제한 도달 — 다운로드 중단")
                            break
        file_size = output_path.stat().st_size if output_path.exists() else 0
        logger.info(f"영상 다운로드 완료 (fallback): {output_path} ({file_size/(1024*1024):.2f}MB)")
        return file_size > 10000
    except Exception as e:
        logger.error(f"영상 다운로드 실패: {e}")
        return False

async def search_and_download_assets(job_id: str, scenes: List[Scene]) -> List[Scene]:
    """각 장면에 대해 자산 검색 및 다운로드"""
    job_assets_dir = JOBS_DIR / job_id / "assets"
    job_assets_dir.mkdir(parents=True, exist_ok=True)
    
    updated_scenes = []
    total_scenes = len(scenes)
    
    for idx, scene in enumerate(scenes):
        try:
            # 진행률 업데이트
            progress = (idx / total_scenes) * 100
            await update_job_status(job_id, JobStatus.DOWNLOADING_ASSETS, progress=progress)
            
            logger.info(f"[{idx+1}/{total_scenes}] 장면 '{scene.scene_id}' 검색 중...")
            
            # 병렬로 Pexels와 Pixabay 검색
            pexels_videos, pixabay_videos = await asyncio.gather(
                get_pexels_videos(scene.keyword),
                get_pixabay_videos(scene.keyword)
            )
            
            # 최고 품질의 영상 선택
            best_video_url = select_best_video(pexels_videos, pixabay_videos)
            
            if not best_video_url:
                logger.warning(f"장면 '{scene.scene_id}' 검색 결과 없음")
                updated_scenes.append(scene)
                continue
            
            # 영상 다운로드
            asset_filename = f"{scene.scene_id}.mp4"
            asset_path = job_assets_dir / asset_filename
            
            success = await download_video(best_video_url, asset_path)
            
            if success:
                scene.asset_url = str(asset_path)
                logger.info(f"장면 '{scene.scene_id}' 다운로드 완료: {asset_path}")
            else:
                logger.error(f"장면 '{scene.scene_id}' 다운로드 실패")
            
            updated_scenes.append(scene)
        
        except Exception as e:
            logger.error(f"장면 '{scene.scene_id}' 처리 오류: {e}")
            updated_scenes.append(scene)
    
    await update_job_status(job_id, JobStatus.DOWNLOADING_ASSETS, progress=100.0)
    return updated_scenes


def run_ffmpeg_command(command: List[str], timeout: float = 300.0) -> bool:
    """FFmpeg 커맨드 실행"""
    try:
        logger.info(f"FFmpeg 커맨드 실행: {' '.join(command[:5])}...")
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=timeout
        )
        
        if result.returncode != 0:
            logger.error(f"FFmpeg 오류: {result.stderr}")
            return False
        
        logger.info("FFmpeg 커맨드 성공")
        return True
    except subprocess.TimeoutExpired:
        logger.error("FFmpeg 커맨드 타임아웃")
        return False
    except Exception as e:
        logger.error(f"FFmpeg 실행 오류: {e}")
        return False


async def run_ffmpeg_async(command, timeout: float = 300.0) -> bool:
    """FFmpeg 비동기 실행 (event loop 블로킹 방지)"""
    import asyncio
    loop = asyncio.get_event_loop()
    try:
        return await asyncio.wait_for(
            loop.run_in_executor(None, lambda: run_ffmpeg_command(command, timeout=timeout)),
            timeout=timeout + 30
        )
    except asyncio.TimeoutError:
        return False


async def prepare_clips_for_longform(
    job_id: str,
    scenes: List[Scene],
    output_dir: Path
) -> List[Path]:
    """씬당 3~4 서브클립(각 4~6초) → 총 15~20개 빠른 전환 클립"""
    clips = []

    # 6가지 Ken Burns 프리셋
    KB_PRESETS = [
        "zoompan=z='min(zoom+0.002,1.6)':x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)':d={fps_d}:s=1920x1080:fps=30",
        "zoompan=z='if(eq(on,1),1.5,max(zoom-0.002,1.0))':x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)':d={fps_d}:s=1920x1080:fps=30",
        "zoompan=z='1.3':x='if(lte(on,1),0,min(x+3,iw*0.25))':y='ih/2-(ih/zoom/2)':d={fps_d}:s=1920x1080:fps=30",
        "zoompan=z='1.3':x='if(lte(on,1),iw*0.25,max(x-3,0))':y='ih/2-(ih/zoom/2)':d={fps_d}:s=1920x1080:fps=30",
        "zoompan=z='min(zoom+0.0015,1.4)':x='iw/2-(iw/zoom/2)':y='if(lte(on,1),0,min(y+2,ih*0.2))':d={fps_d}:s=1920x1080:fps=30",
        "zoompan=z='min(zoom+0.0025,1.7)':x='if(lte(on,1),iw*0.1,max(x-1,0))':y='ih-ih/zoom':d={fps_d}:s=1920x1080:fps=30",
    ]

    kb_counter = 0  # 전역 Ken Burns 프리셋 순환

    for scene in scenes:
        if not scene.asset_url:
            logger.warning(f"장면 '{scene.scene_id}' 자산 없음")
            continue

        scene_dur = max(scene.duration_seconds or 5.0, 4.0)

        # 원본 영상 길이 파악
        probe_cmd = [
            "ffprobe", "-v", "error", "-show_entries", "format=duration",
            "-of", "csv=p=0", scene.asset_url
        ]
        try:
            result = subprocess.run(probe_cmd, capture_output=True, text=True, timeout=10)
            src_dur = float(result.stdout.strip()) if result.stdout.strip() else scene_dur * 3
        except Exception:
            src_dur = scene_dur * 3

        # v15.12 fix — 실제 소스 길이 보존 (scene_dur로 부풀리기 X)
        actual_src_dur = src_dur
        # 소스가 씬보다 짧으면 stream_loop 으로 반복 재생
        needs_loop = actual_src_dur < scene_dur * 0.95

        # 서브클립 수 계산 (4~5초짜리로 분할)
        SUB_DUR = 4.5  # 각 서브클립 길이 (초)
        n_subs  = max(1, round(scene_dur / SUB_DUR))
        n_subs  = min(n_subs, 5)  # 최대 5개

        logger.info(f"'{scene.scene_id}': {scene_dur:.1f}초 → {n_subs}개 서브클립 (src={actual_src_dur:.1f}s, loop={needs_loop})")

        for sub_i in range(n_subs):
            sub_dur    = scene_dur / n_subs
            sub_dur    = max(sub_dur, 3.0)
            # v15.12 — seek 은 항상 실제 소스 범위 안에서 분포
            seek_usable = max(actual_src_dur - 0.3, 0.0)
            if n_subs > 1 and seek_usable > 0:
                seek_start = seek_usable * sub_i / n_subs
            else:
                seek_start = 0
            seek_start = max(0, min(seek_start, seek_usable))

            fps_d       = max(int(sub_dur * 30), 30)
            kb_filter   = KB_PRESETS[kb_counter % len(KB_PRESETS)].replace("{fps_d}", str(fps_d))
            kb_counter += 1

            fade_out_st = max(sub_dur - 0.3, sub_dur * 0.9)

            vf = (
                f"scale=1920:1080:force_original_aspect_ratio=increase,"
                f"crop=1920:1080,"
                f"{kb_filter},"
                f"fade=t=in:st=0:d=0.25,"
                f"fade=t=out:st={fade_out_st:.2f}:d=0.25,"
                f"unsharp=lx=3:ly=3:la=0.5,"
                f"eq=brightness=0.02:contrast=1.1:saturation=1.25:gamma=0.95,"
                f"vignette=PI/6,"
                f"format=yuv420p"
            )

            clip_output = output_dir / f"clip_{scene.scene_id}_{sub_i}.mp4"

            command = ["ffmpeg"]
            if needs_loop:
                command += ["-stream_loop", "-1"]
            command += [
                "-ss", str(seek_start),
                "-i", scene.asset_url,
                "-t", str(sub_dur),
                "-vf", vf,
                "-c:v", "libx264", "-preset", "fast", "-crf", "18",
                "-movflags", "+faststart",
                "-an", "-y", str(clip_output)
            ]

            clip_timeout = max(60.0, sub_dur * 20)
            if run_ffmpeg_command(command, timeout=clip_timeout) and clip_output.exists() and clip_output.stat().st_size >= 4096:
                clips.append(clip_output)
                logger.info(f"  서브클립 OK: {clip_output.name} ({sub_dur:.1f}s, seek={seek_start:.1f}s)")
            else:
                sz = clip_output.stat().st_size if clip_output.exists() else 0
                if sz > 0 and sz < 4096:
                    clip_output.unlink(missing_ok=True)  # 빈 파일 삭제
                logger.warning(f"  서브클립 실패 (size={sz}B): {scene.scene_id}_{sub_i} — fallback")
                fallback = ["ffmpeg"]
                if needs_loop:
                    fallback += ["-stream_loop", "-1"]
                fallback += [
                    "-ss", str(seek_start),
                    "-i", scene.asset_url,
                    "-t", str(sub_dur),
                    "-vf", "scale=1920:1080:force_original_aspect_ratio=decrease,pad=1920:1080:(ow-iw)/2:(oh-ih)/2,format=yuv420p",
                    "-c:v", "libx264", "-preset", "fast", "-crf", "20",
                    "-movflags", "+faststart",
                    "-an", "-y", str(clip_output)
                ]
                if run_ffmpeg_command(fallback, timeout=clip_timeout):
                    clips.append(clip_output)

    logger.info(f"총 클립 수: {len(clips)}개")
    return clips



def create_concat_file(clips: List[Path], output_file: Path) -> bool:
    """FFmpeg concat 파일 생성"""
    try:
        with open(output_file, "w") as f:
            for clip in clips:
                f.write(f"file '{clip}'\n")
        
        logger.info(f"Concat 파일 생성: {output_file}")
        return True
    except Exception as e:
        logger.error(f"Concat 파일 생성 오류: {e}")
        return False


def _is_valid_clip(clip_path) -> bool:
    """ffprobe로 클립 유효성 검사 (moov atom · 비디오 스트림 존재 확인)"""
    try:
        p = Path(clip_path)
        if not p.exists() or p.stat().st_size < 4096:
            return False
        r = subprocess.run(
            ["ffprobe", "-v", "error", "-select_streams", "v:0",
             "-show_entries", "stream=codec_name",
             "-of", "default=nw=1:nk=1", str(p)],
            capture_output=True, text=True, timeout=20
        )
        return r.returncode == 0 and bool(r.stdout.strip())
    except Exception:
        return False



def normalize_clip(clip_path: Path, timeout: float = 45.0) -> Path:
    """Duration:N/A 클립을 정규화 — filter_complex 호환을 위해 re-encode"""
    dur = get_video_duration(clip_path)
    if dur is not None and dur > 0:
        return clip_path  # already OK
    norm_path = clip_path.with_name(clip_path.stem + "_norm.mp4")
    if norm_path.exists():
        return norm_path  # cached
    cmd = [
        "ffmpeg", "-i", str(clip_path),
        "-c:v", "libx264", "-preset", "ultrafast", "-crf", "18",
        "-movflags", "+faststart", "-an", "-y", str(norm_path)
    ]
    if run_ffmpeg_command(cmd, timeout=timeout):
        logger.debug(f"normalize_clip OK: {clip_path.name}")
        return norm_path
    logger.warning(f"normalize_clip failed, using original: {clip_path.name}")
    return clip_path

def xfade_batch(clip_paths: list, output: Path, transition: str = "fade") -> bool:
    """클립 배치를 concat filter로 합치기 — xfade보다 안정적 (Duration:N/A 클립 허용)"""
    # 1) 손상된 클립 사전 필터링 (moov atom 없는 파일 제거)
    original_n = len(clip_paths)
    clip_paths = [cp for cp in clip_paths if _is_valid_clip(cp)]
    dropped = original_n - len(clip_paths)
    if dropped:
        logger.warning(f"xfade_batch: 손상된 클립 {dropped}개 제외 (잔여 {len(clip_paths)}개)")

    if len(clip_paths) == 0:
        logger.error("xfade_batch: 유효 클립 0개 — 합치기 불가")
        return False
    if len(clip_paths) == 1:
        shutil.copy(str(clip_paths[0]), str(output))
        return True

    # 방법 1: concat filter — Duration:N/A 클립은 먼저 정규화
    clip_paths = [normalize_clip(cp) for cp in clip_paths]
    inputs = []
    for cp in clip_paths:
        inputs += ["-i", str(cp)]

    n = len(clip_paths)
    # [i:v:0] — 정규화 후 duration이 확정된 단일 비디오 스트림
    vparts = "".join(f"[{i}:v:0]setpts=PTS-STARTPTS[v{i}];" for i in range(n))
    vconcat = "".join(f"[v{i}]" for i in range(n))
    fg = f"{vparts}{vconcat}concat=n={n}:v=1:a=0[vout]"

    cmd = ["ffmpeg", *inputs,
           "-filter_complex", fg,
           "-map", "[vout]",
           "-c:v", "libx264", "-preset", "fast", "-crf", "20",
           "-movflags", "+faststart",
           "-y", str(output)]
    timeout = max(300.0, n * 30)
    if run_ffmpeg_command(cmd, timeout=timeout):
        logger.info(f"xfade_batch concat OK: {n}개 → {output.name}")
        return True

    # 방법 2: concat demuxer fallback (copy, 무손실)
    logger.warning(f"concat filter 실패 → demuxer fallback")
    # fallback 단계에서도 손상 클립 한 번 더 걸러냄
    clip_paths = [cp for cp in clip_paths if _is_valid_clip(cp)]
    if not clip_paths:
        logger.error("demuxer fallback: 유효 클립 0개")
        return False
    concat_txt = output.parent / f"_concat_{output.stem}.txt"
    with open(concat_txt, "w") as f:
        for cp in clip_paths:
            f.write(f"file '{cp}'\n")
    cmd2 = ["ffmpeg", "-f", "concat", "-safe", "0", "-i", str(concat_txt),
            "-c", "copy", "-y", str(output)]
    return run_ffmpeg_command(cmd2, timeout=timeout)


def concatenate_videos(concat_file: Path, output_video: Path, transition: str = "fade") -> bool:
    """영상 파일 연결 (배치 xfade 크로스페이드 트랜지션)"""
    # concat.txt에서 클립 경로 파싱
    with open(concat_file, "r") as f:
        lines = f.readlines()

    clip_paths = [l.split("'")[1] for l in lines if l.startswith("file ")]

    # 유효하지 않은 클립 사전 제거 (크기 < 4KB = 깨진 파일)
    clip_paths = [cp for cp in clip_paths if _is_valid_clip(cp)]
    if not clip_paths:
        logger.error("concatenate_videos: 유효한 클립 없음")
        return False

    if len(clip_paths) < 2:
        # 단일 클립: 그냥 copy
        command = ["ffmpeg", "-f", "concat", "-safe", "0", "-i", str(concat_file),
                   "-c", "copy", "-y", str(output_video)]
        return run_ffmpeg_command(command)
    
    # 2개 이상: 배치 xfade (8개씩 나눠서 처리, 이후 최종 합치기)
    BATCH_SIZE = 8
    temp_dir = output_video.parent
    
    if len(clip_paths) <= BATCH_SIZE:
        # 소수 클립: 직접 xfade
        if xfade_batch(clip_paths, output_video, transition):
            return True
        logger.warning("xfade 실패, 단순 concat fallback")
        fallback = ["ffmpeg", "-f", "concat", "-safe", "0", "-i", str(concat_file),
                    "-c", "copy", "-y", str(output_video)]
        return run_ffmpeg_command(fallback)
    
    # 다수 클립: 배치 분할 처리 — 마지막 고아 배치(≤1 클립)는 직전 배치에 합친다
    batches = [clip_paths[i:i+BATCH_SIZE] for i in range(0, len(clip_paths), BATCH_SIZE)]
    if len(batches) >= 2 and len(batches[-1]) <= 1:
        batches[-2].extend(batches[-1])
        batches.pop()
        logger.info(f"xfade_batch: 마지막 고아 배치 머지 → {len(batches)}개 배치")
    batch_outputs = []
    for bi, batch in enumerate(batches):
        bout = temp_dir / f"batch_{bi}.mp4"
        if not xfade_batch(batch, bout, transition):
            # 배치 실패 시 단순 concat (무효 클립 제외)
            valid_batch = [cp for cp in batch if _is_valid_clip(cp)]
            if not valid_batch:
                logger.warning(f"batch_{bi}: 유효 클립 없음 — 건너뜀")
                continue
            if len(valid_batch) == 1:
                import shutil as _sh
                _sh.copy(str(valid_batch[0]), str(bout))
                batch_outputs.append(bout)
                continue
            bc_txt = temp_dir / f"batch_{bi}_concat.txt"
            with open(bc_txt, "w") as f:
                for cp in valid_batch:
                    f.write(f"file '{cp}'\n")
            run_ffmpeg_command(["ffmpeg", "-f", "concat", "-safe", "0", "-i", str(bc_txt),
                               "-c", "copy", "-y", str(bout)])
        if bout.exists():
            batch_outputs.append(str(bout))
    
    if not batch_outputs:
        return False
    
    if len(batch_outputs) == 1:
        shutil.copy(batch_outputs[0], str(output_video))
        return True
    
    # 배치 결과들을 최종 합치기 — 배치는 이미 xfade 처리됨.
    # 큰 배치들(각 30-50초)을 xfade filter_complex로 재결합하면 메모리 과부하 → 컨테이너 SIGKILL.
    # 따라서 최종 배치 머지는 항상 demuxer concat (stream copy) 사용.
    final_concat = temp_dir / "final_concat.txt"
    with open(final_concat, "w") as f:
        for bp in batch_outputs:
            f.write(f"file '{bp}'\n")
    if run_ffmpeg_command(["ffmpeg", "-f", "concat", "-safe", "0", "-i", str(final_concat),
                           "-c", "copy", "-y", str(output_video)]):
        logger.info(f"최종 배치 머지 OK (demuxer concat): {len(batch_outputs)}개 -> {output_video.name}")
        return True
    
    # 최종 fallback: re-encode concat (stream copy 실패 시 코덱/해상도 불일치)
    logger.warning("demuxer concat 실패 -> filter_complex concat로 재시도 (re-encode)")
    inputs = []
    for bp in batch_outputs:
        inputs.extend(["-i", str(bp)])
    n = len(batch_outputs)
    fg = "".join(f"[{i}:v:0]" for i in range(n)) + f"concat=n={n}:v=1:a=0[v]"
    return run_ffmpeg_command(["ffmpeg"] + inputs + [
        "-filter_complex", fg, "-map", "[v]",
        "-c:v", "libx264", "-preset", "ultrafast", "-crf", "23",
        "-movflags", "+faststart", "-an", "-y", str(output_video)
    ])


def _UNUSED_old_xfade():
    # 구 코드 보관용 (사용 안 함)
    FADE_DUR = 0.5
    offset = 0  # placeholder
    
    if len(clip_paths) == 2:
        fg = f"[0:v][1:v]xfade=transition={transition}:duration={FADE_DUR}:offset={offset:.3f}[vout]"
    else:
        # 첫 번째 트랜지션
        fg = f"[0:v][1:v]xfade=transition={transition}:duration={FADE_DUR}:offset={offset:.3f}[t1];"
        running_dur = durations[0] + durations[1] - FADE_DUR
        
        for i in range(2, len(clip_paths)):
            tag_in = f"t{i-1}"
            tag_out = f"t{i}" if i < len(clip_paths)-1 else "vout"
            offset_i = running_dur - FADE_DUR
            fg += f"[{tag_in}][{i}:v]xfade=transition={transition}:duration={FADE_DUR}:offset={offset_i:.3f}[{tag_out}];"
            running_dur += durations[i] - FADE_DUR
        
        fg = fg.rstrip(";")
    
    command = [
        "ffmpeg",
        *inputs,
        "-filter_complex", fg,
        "-map", "[vout]",
        "-c:v", "libx264",
        "-preset", "fast",
        "-crf", "20",
        "-y",
        str(output_video)
    ]
    
    success = run_ffmpeg_command(command)
    if not success:
        # xfade 실패 시 단순 concat fallback
        logger.warning("xfade 실패, 단순 concat으로 fallback")
        fallback = ["ffmpeg", "-f", "concat", "-safe", "0", "-i", str(concat_file),
                    "-c", "copy", "-y", str(output_video)]
        return run_ffmpeg_command(fallback)
    
    return True


def mix_audio(
    video_path: Path,
    tts_audio_path: Path,
    bgm_path: Optional[Path],
    bgm_volume: float,
    output_video: Path
) -> bool:
    """오디오 믹싱 - loudnorm 정규화 + 나레이션 우선 BGM 더킹"""

    if not tts_audio_path.exists():
        logger.warning(f"TTS 오디오 없음: {tts_audio_path}")
        if bgm_path and bgm_path.exists():
            command = [
                "ffmpeg", "-i", str(video_path), "-i", str(bgm_path),
                "-filter_complex", f"[1:a]volume={bgm_volume}[audio]",
                "-map", "0:v", "-map", "[audio]",
                "-c:v", "copy", "-c:a", "aac", "-b:a", "192k", "-y", str(output_video)
            ]
        else:
            command = ["ffmpeg", "-i", str(video_path), "-c", "copy", "-y", str(output_video)]
        return run_ffmpeg_command(command)

    # TTS 있음: loudnorm으로 나레이션 볼륨 정규화
    if bgm_path and bgm_path.exists() and bgm_volume > 0:
        # TTS 나레이션 + BGM 더킹 믹스
        # BGM은 나레이션 대비 -18dB (약 0.12x) - 배경음악은 조용하게
        actual_bgm_vol = min(bgm_volume * 0.15, 0.12)
        filter_complex = (
            f"[1:a]loudnorm=I=-16:TP=-1.5:LRA=11[tts_norm];"
            f"[2:a]volume={actual_bgm_vol}[bgm_quiet];"
            f"[tts_norm][bgm_quiet]amix=inputs=2:duration=longest:dropout_transition=3[aout]"
        )
        command = [
            "ffmpeg",
            "-i", str(video_path),
            "-i", str(tts_audio_path),
            "-i", str(bgm_path),
            "-filter_complex", filter_complex,
            "-map", "0:v",
            "-map", "[aout]",
            "-c:v", "copy",
            "-c:a", "aac", "-b:a", "192k",
            "-shortest",
            "-y", str(output_video)
        ]
    else:
        # TTS 나레이션만 (loudnorm 정규화)
        filter_complex = "[1:a]loudnorm=I=-16:TP=-1.5:LRA=11[aout]"
        command = [
            "ffmpeg",
            "-i", str(video_path),
            "-i", str(tts_audio_path),
            "-filter_complex", filter_complex,
            "-map", "0:v",
            "-map", "[aout]",
            "-c:v", "copy",
            "-c:a", "aac", "-b:a", "192k",
            "-shortest",
            "-y", str(output_video)
        ]

    success = run_ffmpeg_command(command)
    if not success:
        # loudnorm 실패 시 단순 믹스 fallback
        logger.warning("loudnorm 실패, 단순 믹스 fallback")
        simple_cmd = [
            "ffmpeg", "-i", str(video_path), "-i", str(tts_audio_path),
            "-map", "0:v", "-map", "1:a",
            "-c:v", "copy", "-c:a", "aac", "-b:a", "192k",
            "-shortest", "-y", str(output_video)
        ]
        return run_ffmpeg_command(simple_cmd)
    return True

def add_subtitles_to_video(
    input_video: Path,
    srt_path: Path,
    output_video: Path,
    font_size: int = 52,
    font_color: str = "white",
    outline: bool = True
) -> bool:
    """SRT 자막을 영상에 오버레이 (하단 자막바 스타일)"""
    if not srt_path.exists():
        logger.warning(f"SRT 파일 없음: {srt_path}")
        return False

    # ASS 스타일: 반투명 배경 박스 + 노란 자막 (한국어 폰트)
    style = (
        f"FontName=Noto Sans CJK KR,"
        f"FontSize=48,"
        f"Bold=1,"
        f"PrimaryColour=&H00FFFF00&,"
        f"OutlineColour=&H00000000&,"
        f"BackColour=&HA0000000&,"
        f"BorderStyle=3,"
        f"Outline=3,"
        f"Shadow=1,"
        f"MarginV=50,"
        f"Alignment=2"
    )

    # 경로 내 콜론 이스케이프 (Windows 경로 대비)
    srt_escaped = str(srt_path).replace("\\", "/").replace(":", "\\:")

    command = [
        "ffmpeg",
        "-i", str(input_video),
        "-vf", f"subtitles={srt_escaped}:charenc=UTF-8:force_style='{style}'",
        "-c:v", "libx264",
        "-preset", "fast",
        "-crf", "20",
        "-c:a", "copy",
        "-y",
        str(output_video)
    ]

    success = run_ffmpeg_command(command)
    if not success:
        # SRT 경로 이스케이프 문제로 실패 시 copy fallback
        logger.warning("SRT 오버레이 실패, subtitles 필터 재시도")
        simple_cmd = [
            "ffmpeg", "-i", str(input_video),
            "-vf", f"subtitles='{str(srt_path)}'",
            "-c:v", "libx264", "-preset", "fast", "-crf", "20",
            "-c:a", "copy", "-y", str(output_video)
        ]
        return run_ffmpeg_command(simple_cmd)
    return True

def extract_thumbnail(video_path: Path, output_image: Path, timestamp: str = "3") -> bool:
    """영상에서 썸네일 추출"""
    command = [
        "ffmpeg",
        "-i", str(video_path),
        "-ss", timestamp,
        "-frames:v", "1",
        "-q:v", "2",
        "-y",
        str(output_image)
    ]
    
    return run_ffmpeg_command(command)


def add_text_overlay_to_thumbnail(
    thumbnail_path: Path,
    output_path: Path,
    title: str = "LongForm Video",
    font_size: int = 80
) -> bool:
    """썸네일에 텍스트 오버레이 추가"""
    try:
        # 썸네일 로드
        img = Image.open(thumbnail_path)
        draw = ImageDraw.Draw(img)
        
        # 폰트 설정 (기본 폰트 사용)
        try:
            font = ImageFont.truetype("/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc", font_size)
        except:
            # 폰트 없으면 기본 폰트 사용
            font = ImageFont.load_default()
        
        # 텍스트 위치 (중앙 하단)
        img_width, img_height = img.size
        bbox = draw.textbbox((0, 0), title, font=font)
        text_width = bbox[2] - bbox[0]
        text_height = bbox[3] - bbox[1]
        
        x = (img_width - text_width) // 2
        y = img_height - text_height - 30
        
        # 반투명 배경 추가 (선택사항)
        background_padding = 20
        bg_box = [
            x - background_padding,
            y - background_padding,
            x + text_width + background_padding,
            y + text_height + background_padding
        ]
        draw.rectangle(bg_box, fill=(0, 0, 0, 180))
        
        # 텍스트 그리기
        draw.text((x, y), title, font=font, fill=(255, 255, 255))
        
        # 저장
        img.save(output_path, "JPEG", quality=95)
        logger.info(f"썸네일 생성 완료: {output_path}")
        return True
    
    except Exception as e:
        logger.error(f"썸네일 텍스트 오버레이 오류: {e}")
        return False


def create_shortform_from_longform(
    longform_path: Path,
    output_path: Path,
    max_duration: float = 60.0
) -> bool:
    """장편 영상에서 숏폼(1080x1920) 생성"""
    command = [
        "ffmpeg",
        "-i", str(longform_path),
        "-t", str(max_duration),
        "-vf", "crop=min(iw\\,ih*9/16):min(ih\\,iw*16/9),scale=1080:1920",
        "-c:v", "libx264",
        "-preset", "fast",
        "-c:a", "aac",
        "-b:a", "128k",
        "-y",
        str(output_path)
    ]
    
    return run_ffmpeg_command(command)


def get_video_duration(video_path: Path) -> Optional[float]:
    """영상 길이 조회"""
    try:
        command = [
            "ffprobe",
            "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1:nokey=1",
            str(video_path)
        ]
        
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=10.0
        )
        
        if result.returncode == 0:
            raw = result.stdout.strip()
            if raw and raw not in ("N/A", ""):
                try:
                    return float(raw)
                except ValueError:
                    pass
            # Fallback: csv=p=0 format
            alt_cmd = ["ffprobe","-v","error","-show_entries","format=duration","-of","csv=p=0",str(video_path)]
            alt = subprocess.run(alt_cmd, capture_output=True, text=True, timeout=10.0)
            raw2 = alt.stdout.strip()
            if raw2 and raw2 not in ("N/A", ""):
                try:
                    return float(raw2)
                except ValueError:
                    pass
            return None
    except Exception as e:
        logger.error(f"영상 길이 조회 오류: {e}")
    
    return None


def get_random_bgm() -> Optional[Path]:
    """배경음악 디렉토리에서 랜덤 파일 선택"""
    import random
    
    bgm_files = list(BGM_DIR.glob("*.mp3")) + list(BGM_DIR.glob("*.wav"))
    
    if bgm_files:
        selected = random.choice(bgm_files)
        logger.info(f"선택된 배경음악: {selected}")
        return selected
    
    logger.warning("배경음악 파일 없음")
    return None



def create_srt_from_text(text: str, total_duration: float, output_path: Path) -> bool:
    """
    스크립트 텍스트를 SRT 자막 파일로 변환.
    전체 영상 시간에 맞게 텍스트를 균등 분배.

    Args:
        text: 자막으로 표시할 전체 텍스트
        total_duration: 영상 총 길이 (초)
        output_path: SRT 파일 저장 경로

    Returns:
        성공 여부
    """
    try:
        import re

        # 문장 단위로 분리 (마침표, 느낌표, 물음표, 줄바꿈)
        sentences = [s.strip() for s in re.split(r'[.!?\n]+', text) if s.strip()]

        if not sentences:
            sentences = [text[:50]]  # fallback

        # 한 자막당 최대 글자 수 (2줄 x 25자)
        MAX_CHARS = 40
        chunks = []
        for sentence in sentences:
            # 긴 문장은 MAX_CHARS 단위로 분할
            while len(sentence) > MAX_CHARS:
                chunks.append(sentence[:MAX_CHARS])
                sentence = sentence[MAX_CHARS:]
            if sentence:
                chunks.append(sentence)

        if not chunks:
            return False

        # 각 청크에 시간 균등 배분 (마지막 0.5초는 여유)
        usable_duration = max(total_duration - 0.5, 1.0)
        chunk_duration = usable_duration / len(chunks)

        def sec_to_srt_time(sec: float) -> str:
            sec = max(0.0, sec)
            h = int(sec // 3600)
            m = int((sec % 3600) // 60)
            s = int(sec % 60)
            ms = int((sec - int(sec)) * 1000)
            return f"{h:02d}:{m:02d}:{s:02d},{ms:03d}"

        srt_content = ""
        for i, chunk in enumerate(chunks):
            start = i * chunk_duration
            # 겹침 방지: end는 다음 start보다 0.1초 앞
            end = min((i + 1) * chunk_duration - 0.1, usable_duration)
            srt_content += f"{i+1}\n"
            srt_content += f"{sec_to_srt_time(start)} --> {sec_to_srt_time(end)}\n"
            srt_content += f"{chunk}\n\n"

        output_path.write_text(srt_content, encoding="utf-8")
        logger.info(f"SRT 생성 완료: {len(chunks)}개 자막 구간, {output_path}")
        return True

    except Exception as e:
        logger.error(f"SRT 생성 오류: {e}")
        return False


def create_srt_from_scenes(scenes: list, output_path: Path) -> bool:
    """씬별 description/keyword를 SRT 자막으로 변환 (씬 타이밍 완전 동기화)."""
    try:
        import re

        def sec_to_srt_time(sec: float) -> str:
            sec = max(0.0, sec)
            h = int(sec // 3600)
            m = int((sec % 3600) // 60)
            s = int(sec % 60)
            ms = int((sec - int(sec)) * 1000)
            return f"{h:02d}:{m:02d}:{s:02d},{ms:03d}"

        MAX_CHARS = 28
        srt_entries = []
        current_time = 0.0

        for scene in scenes:
            text = scene.description or scene.keyword or scene.scene_id
            dur = max(scene.duration_seconds or 5.0, 1.0)

            import re as _re
            sentences = [s.strip() for s in _re.split(r'[.!?.]+', text) if s.strip()]
            chunks = []
            for sentence in sentences:
                while len(sentence) > MAX_CHARS:
                    chunks.append(sentence[:MAX_CHARS])
                    sentence = sentence[MAX_CHARS:]
                if sentence:
                    chunks.append(sentence)

            if not chunks:
                chunks = [scene.keyword or scene.scene_id]

            chunk_dur = dur / len(chunks)
            for chunk in chunks:
                start = current_time
                end = current_time + chunk_dur - 0.1
                srt_entries.append((start, end, chunk))
                current_time += chunk_dur

        if not srt_entries:
            return False

        srt_content = ""
        for i, (start, end, txt) in enumerate(srt_entries):
            srt_content += f"{i+1}\n"
            srt_content += f"{sec_to_srt_time(start)} --> {sec_to_srt_time(end)}\n"
            srt_content += f"{txt}\n\n"

        output_path.write_text(srt_content, encoding="utf-8")
        logger.info(f"씬 동기화 SRT 생성: {len(srt_entries)}개 구간")
        return True

    except Exception as e:
        logger.error(f"씬 SRT 생성 오류: {e}")
        return False


def create_music_video(
    clips: List[Path],
    srt_path: Path,
    bgm_path: Optional[Path],
    bgm_volume: float,
    output_path: Path,
    resolution: str = "1920x1080"
) -> bool:
    """
    뮤직비디오 생성: 비디오 클립 연결 + BGM + 자막 오버레이.
    TTS 나레이션 없이 배경음악만 사용.

    Args:
        clips: 비디오 클립 경로 목록
        srt_path: SRT 자막 파일 경로
        bgm_path: BGM 오디오 파일 경로 (None이면 무음)
        bgm_volume: BGM 볼륨 (0-1)
        output_path: 출력 영상 경로
        resolution: 출력 해상도

    Returns:
        성공 여부
    """
    if not clips:
        logger.error("클립 없음")
        return False

    try:
        import tempfile

        # 1) 클립 concat용 임시 txt
        concat_txt = output_path.parent / "mv_concat.txt"
        with open(concat_txt, "w") as f:
            for clip in clips:
                f.write(f"file '{clip}'\n")

        # 2) concat → 임시 combined
        combined = output_path.parent / "mv_combined.mp4"
        concat_cmd = [
            "ffmpeg",
            "-f", "concat", "-safe", "0",
            "-i", str(concat_txt),
            "-c", "copy",
            "-y", str(combined)
        ]
        if not run_ffmpeg_command(concat_cmd):
            logger.error("뮤직비디오 concat 실패")
            return False

        # 3) 자막 스타일 (뮤직비디오 감성: 큰 폰트, 흰색, 굵은 외곽선)
        # 연구 기반 최적값:
        # - FontSize=56: 1920x1080의 5.2% 높이 = 가사 가독성 최적 (YouTube MV 기준)
        # - BorderStyle=3: 반투명 박스 배경 (텍스트 가독성 극대화)
        # - Outline=4: 외곽선 두께 - 어두운/밝은 배경 모두 대응
        # - MarginV=60: 하단 60px - 모바일/TV 안전 영역
        subtitle_style = (
            "FontName=Noto Sans CJK KR,"
            "FontSize=56,"             # 1920x1080 최적 (화면 높이 5.2%)
            "PrimaryColour=&H00FFFFFF,"  # 흰색 텍스트 (AABBGGRR)
            "OutlineColour=&H00000000,"  # 검정 외곽선
            "BackColour=&H80000000,"     # 50% 투명 검정 박스 배경
            "BorderStyle=3,"             # 불투명 박스 배경
            "Outline=4,"                 # 외곽선 두께 4px
            "Shadow=0,"
            "Bold=1,"
            "Alignment=2,"               # 하단 중앙
            "MarginV=60"                 # 하단 60px 여백
        )

        # 4) 자막 필터 문자열 (srt 경로 이스케이프)
        srt_escaped = str(srt_path).replace("\\", "/").replace(":", "\\:")
        subtitle_filter = f"subtitles={srt_escaped}:charenc=UTF-8:force_style='{subtitle_style}'"

        # 5) BGM 포함 여부에 따라 명령 구성
        if bgm_path and bgm_path.exists():
            # BGM + 자막
            cmd = [
                "ffmpeg",
                "-i", str(combined),
                # BGM 반복
                "-i", str(bgm_path),
                "-filter_complex",
                # loudnorm: YouTube 기준 -14 LUFS, TP=-1.5, LRA=11
                f"[1:a]volume={bgm_volume},loudnorm=I=-14:TP=-1.5:LRA=11[bgm]",
                "-vf", subtitle_filter,
                "-map", "0:v",
                "-map", "[bgm]",
                "-c:v", "libx264",
                "-preset", "fast",
                "-c:a", "aac",
                "-b:a", "192k",
                "-shortest",            # 비디오 길이 기준 종료
                "-y", str(output_path)
            ]
        else:
            # 자막만 (무음)
            cmd = [
                "ffmpeg",
                "-i", str(combined),
                "-vf", subtitle_filter,
                "-c:v", "libx264",
                "-preset", "fast",
                "-an",
                "-y", str(output_path)
            ]

        result = run_ffmpeg_command(cmd)
        if result:
            logger.info(f"뮤직비디오 생성 완료: {output_path}")
        return result

    except Exception as e:
        logger.error(f"뮤직비디오 생성 오류: {e}")
        return False

def sync_scene_durations_from_timestamps(
    scenes,
    timestamps_path
):
    """
    TTS 오디오 타임스탬프(ElevenLabs alignment 또는 Whisper segments) 기반
    씬별 비디오 클립 길이 동기화.

    우선순위:
    1. Whisper `segments` 가 있으면 → 씬별 실제 오디오 구간에 정밀 매핑
       (세그먼트를 씬 개수에 맞춰 누적 길이 비례로 분할)
    2. 그 외 → 전체 오디오 길이 기반 비례 배분
       (ElevenLabs character_end_times_seconds[-1] 또는 Whisper duration)

    Returns: duration_seconds 조정된 씬 목록
    """
    import json as _json
    from pathlib import Path as _Path

    if not timestamps_path:
        logger.info("타임스탬프 경로 없음 — scenes.json 추정 duration 사용")
        return scenes

    ts_path = _Path(timestamps_path)
    if not ts_path.exists():
        logger.info(f"타임스탬프 파일 없음: {ts_path} — scenes.json 추정 duration 사용")
        return scenes

    try:
        with open(ts_path, encoding="utf-8") as f:
            ts_data = _json.load(f)

        source = ts_data.get("source", "elevenlabs")
        segments = ts_data.get("segments") or []
        alignment = ts_data.get("alignment") or {}
        end_times = alignment.get("character_end_times_seconds") or []

        # ── 전체 오디오 길이 확보 ──────────────────────────────────────
        total_audio_sec = 0.0
        if segments:
            total_audio_sec = float(segments[-1].get("end", 0.0) or 0.0)
        if total_audio_sec <= 0 and end_times:
            total_audio_sec = float(end_times[-1])
        if total_audio_sec <= 0:
            total_audio_sec = float(ts_data.get("duration") or 0.0)

        if total_audio_sec <= 0:
            logger.warning(
                f"타임스탬프에 길이 정보 없음 (source={source}) — 동기화 스킵"
            )
            return scenes

        logger.info(
            f"타임스탬프 로드: source={source} 총길이={total_audio_sec:.2f}s "
            f"segments={len(segments)}"
        )

        # ── 전략 A: 세그먼트 정밀 매핑 (Whisper segments 있을 때) ──────
        # 씬 개수에 세그먼트를 누적 길이 비례로 분할해 각 씬의 (start,end) 산출
        if segments and len(segments) >= len(scenes) >= 1:
            scene_weights = [max((s.duration_seconds or 5.0), 0.1) for s in scenes]
            total_weight = sum(scene_weights)
            # 누적 경계 초 단위 계산 (오디오 total * (누적 weight / total_weight))
            boundaries = []
            cum = 0.0
            for w in scene_weights[:-1]:
                cum += w
                boundaries.append(total_audio_sec * cum / total_weight)
            boundaries.append(total_audio_sec)

            # 경계를 가장 가까운 세그먼트 경계로 스냅
            seg_ends = [float(seg.get("end", 0.0) or 0.0) for seg in segments]
            snapped = []
            last_end_idx = -1
            for b in boundaries[:-1]:
                # 현재까지 쓴 세그먼트 이후 구간에서 b에 가장 가까운 end 선택
                best_idx = last_end_idx + 1
                best_diff = abs(seg_ends[best_idx] - b) if best_idx < len(seg_ends) else 1e9
                for j in range(last_end_idx + 1, len(seg_ends)):
                    d = abs(seg_ends[j] - b)
                    if d < best_diff:
                        best_diff = d
                        best_idx = j
                    else:
                        # 정렬되어 있으므로 멀어지면 중단
                        if seg_ends[j] > b:
                            break
                # 최소 1개 세그먼트는 남겨야 하므로 끝에서 2개는 남기기
                best_idx = min(best_idx, len(seg_ends) - (len(scenes) - len(snapped)))
                snapped.append(seg_ends[best_idx])
                last_end_idx = best_idx
            snapped.append(total_audio_sec)

            synced = []
            prev = 0.0
            for s, end in zip(scenes, snapped):
                dur = max(1.0, round(end - prev, 2))
                if abs(dur - (s.duration_seconds or 5.0)) > 0.1:
                    old = (s.duration_seconds or 5.0)
                    logger.info(f"씬 '{s.scene_id}' 길이 조정 (segment-snap): {old:.1f}s -> {dur:.1f}s")
                synced.append(s.model_copy(update={"duration_seconds": dur}))
                prev = end
            actual_total = sum(x.duration_seconds for x in synced)
            logger.info(
                f"씬 동기화 완료 (segment-snap, source={source}): "
                f"TTS {total_audio_sec:.1f}s → 실제합계 {actual_total:.1f}s"
            )
            return synced

        # ── 전략 B: 비례 배분 (세그먼트 부족 시 fallback) ───────────────
        total_scene_sec = sum((s.duration_seconds or 5.0) for s in scenes)
        if total_scene_sec <= 0:
            logger.warning("씬 총 길이 0 — 동기화 스킵")
            return scenes

        ratio = total_audio_sec / total_scene_sec
        synced = []
        for s in scenes:
            new_dur = max(1.0, round((s.duration_seconds or 5.0) * ratio, 2))
            if abs(new_dur - (s.duration_seconds or 5.0)) > 0.1:
                logger.info(f"씬 '{s.scene_id}' 길이 조정 (ratio): {s.duration_seconds:.1f}s -> {new_dur:.1f}s")
            synced.append(s.model_copy(update={"duration_seconds": new_dur}))

        actual_total = sum(s.duration_seconds for s in synced)
        logger.info(
            f"씬 동기화 완료 (ratio, source={source}): "
            f"씬합계 {total_scene_sec:.1f}s → TTS {total_audio_sec:.1f}s "
            f"(실제합계 {actual_total:.1f}s)"
        )
        return synced

    except Exception as e:
        logger.error(f"씬 동기화 오류 (원본 사용): {e}", exc_info=True)
        return scenes


# [Q4] silencedetect 파라미터 (환경변수 override 가능)
SILENCE_NOISE_DB = float(_rhythm_os.getenv("SILENCE_NOISE_DB", "-30"))      # 무음 임계 dB
SILENCE_MIN_SEC  = float(_rhythm_os.getenv("SILENCE_MIN_SEC", "0.25"))      # 최소 무음 길이

# [Q5] 자막 무음 스냅 파라미터
SUBTITLE_SNAP_WINDOW_SEC       = float(_rhythm_os.getenv("SUBTITLE_SNAP_WINDOW_SEC", "0.6"))
SUBTITLE_LEAD_AFTER_SIL_SEC    = float(_rhythm_os.getenv("SUBTITLE_LEAD_AFTER_SIL_SEC", "0.08"))
SUBTITLE_TAIL_BEFORE_SIL_SEC   = float(_rhythm_os.getenv("SUBTITLE_TAIL_BEFORE_SIL_SEC", "0.05"))


def _detect_audio_silences(audio_path) -> list:
    """
    ffmpeg silencedetect 로 오디오 내 무음 구간 검출.
    Returns: [(start, end), ...] 단위는 초.
    """
    import subprocess as _sp
    from pathlib import Path as _P
    audio_path = _P(audio_path)
    if not audio_path.exists():
        return []
    try:
        cmd = [
            "ffmpeg", "-hide_banner", "-nostats",
            "-i", str(audio_path),
            "-af", f"silencedetect=noise={SILENCE_NOISE_DB}dB:d={SILENCE_MIN_SEC}",
            "-f", "null", "-"
        ]
        proc = _sp.run(cmd, capture_output=True, text=True, timeout=60)
        silences = []
        cur_start = None
        for line in proc.stderr.splitlines():
            if "silence_start:" in line:
                try:
                    cur_start = float(line.split("silence_start:")[1].strip().split()[0])
                except Exception:
                    cur_start = None
            elif "silence_end:" in line and cur_start is not None:
                try:
                    end_str = line.split("silence_end:")[1].strip().split()[0]
                    end = float(end_str)
                    silences.append((round(cur_start, 3), round(end, 3)))
                except Exception:
                    pass
                cur_start = None
        logger.info(
            f"silencedetect: {len(silences)}개 무음 구간 "
            f"(noise={SILENCE_NOISE_DB}dB, d={SILENCE_MIN_SEC}s)"
        )
        return silences
    except Exception as e:
        logger.warning(f"silencedetect 실패 — {e}")
        return []


def _find_pause_split(seg_start: float, seg_end: float, ts_data: dict):
    """
    [Q2]+[Q4] segment 내부 분할 지점.
    1순위: ts_data['audio_silences'] 의 무음 구간 중간 (실제 음향 검출, 가장 정확)
    2순위: Whisper words 간 0.3초 이상 쉼
    실패 → None (caller가 mid로 fallback)
    """
    # 1) 실제 음향 무음 우선 (양 끝 1초 여유)
    silences = ts_data.get("audio_silences") or []
    best_t = None
    best_dur = 0.0
    for (s_start, s_end) in silences:
        # 무음이 segment 내부에 걸쳐있으면
        if s_end < seg_start + 1.0 or s_start > seg_end - 1.0:
            continue
        clipped_s = max(s_start, seg_start + 1.0)
        clipped_e = min(s_end, seg_end - 1.0)
        if clipped_e <= clipped_s:
            continue
        dur = clipped_e - clipped_s
        if dur > best_dur:
            best_dur = dur
            best_t = round((clipped_s + clipped_e) / 2.0, 3)
    if best_t is not None:
        return best_t

    # 2) Whisper word gaps (한국어에서는 종종 무용지물이지만 fallback)
    words = ts_data.get("words") or []
    if not words:
        return None
    inside = []
    for w in words:
        ws = w.get("start")
        we = w.get("end")
        if ws is None or we is None:
            continue
        if seg_start <= float(ws) <= seg_end:
            inside.append((float(ws), float(we)))
    if len(inside) < 2:
        return None

    best_gap = 0.0
    best_t = None
    for i in range(len(inside) - 1):
        gap = inside[i + 1][0] - inside[i][1]
        if gap >= PAUSE_THRESHOLD_SEC and gap > best_gap:
            t = (inside[i][1] + inside[i + 1][0]) / 2.0
            if t - seg_start >= 1.0 and seg_end - t >= 1.0:
                best_gap = gap
                best_t = round(t, 3)
    return best_t


def rebuild_scenes_from_whisper_segments(scenes, timestamps_path):
    """
    [4] 의미 단위 재분해 + [7] 리듬 컷

    Whisper segments가 있으면 scenes를 **한 문장=한 씬** 기준으로 재구성.
    - 각 segment를 독립 씬으로 생성
    - duration > SCENE_MAX_SEC 이면 2등분
    - keyword는 원본 scenes에서 시간 비율로 승계
    - 세그먼트 부족 / 타임스탬프 없음 → 원본 scenes 그대로 반환

    Returns: 재구성된 씬 리스트 (또는 원본)
    """
    import json as _json
    from pathlib import Path as _Path

    if not timestamps_path:
        return scenes
    ts_path = _Path(timestamps_path)
    if not ts_path.exists():
        return scenes

    try:
        with open(ts_path, encoding="utf-8") as f:
            ts_data = _json.load(f)
        segments = ts_data.get("segments") or []
        if not segments:
            logger.info("Whisper segments 없음 — 의미 재분해 스킵")
            return scenes

        source = ts_data.get("source", "unknown")
        logger.info(f"의미 재분해 시작: segments={len(segments)} (source={source})")

        # [Q4] 오디오에서 실제 무음 구간 검출 (씬 분할 정확도 향상)
        audio_path = ts_data.get("audio_path")
        if audio_path and not ts_data.get("audio_silences"):
            ts_data["audio_silences"] = _detect_audio_silences(audio_path)

        # 원본 씬의 keyword·asset을 시간 비율로 승계하기 위해 누적 경계 계산
        orig_total = sum((s.duration_seconds or 5.0) for s in scenes) or 1.0
        orig_bounds = []  # [(end_sec, scene_idx)]
        cum = 0.0
        for i, s in enumerate(scenes):
            cum += (s.duration_seconds or 5.0)
            orig_bounds.append((cum / orig_total, i))

        def pick_orig_scene(rel_t: float):
            for bound, idx in orig_bounds:
                if rel_t <= bound:
                    return scenes[idx]
            return scenes[-1]

        # 각 세그먼트를 씬으로 (4초 초과 시 2등분)
        total_audio = float(segments[-1].get("end", 0.0) or 0.0)
        if total_audio <= 0:
            logger.info("Whisper 총 길이 0 — 재분해 스킵")
            return scenes

        # [C-1] segment_keywords 가 있으면 키워드 자동 교체
        segment_keywords = ts_data.get("segment_keywords") or []
        # idx(1-based) → ["kw1", "kw2"]
        seg_kw_map = {}
        for item in segment_keywords:
            idx = item.get("idx")
            kws = item.get("keywords") or []
            if isinstance(idx, int) and kws:
                seg_kw_map[idx] = kws
        if seg_kw_map:
            logger.info(f"[C-1] 키워드 매핑 로드: {len(seg_kw_map)}개 segment")

        new_scenes = []
        seg_counter = 0
        for seg_idx, seg in enumerate(segments, start=1):
            seg_start = float(seg.get("start", 0.0) or 0.0)
            seg_end = float(seg.get("end", 0.0) or 0.0)
            seg_text = (seg.get("text") or "").strip()
            seg_dur = max(0.5, seg_end - seg_start)
            if seg_dur <= 0:
                continue

            # 리듬 컷: 4초 초과면 분할
            # [Q2] words 간 쉼(≥ PAUSE_THRESHOLD_SEC) 지점에서 분할 시도
            subclips = []
            if seg_dur > SCENE_MAX_SEC:
                pause_t = _find_pause_split(seg_start, seg_end, ts_data)
                split_t = pause_t if pause_t is not None else seg_start + seg_dur / 2.0
                reason = "pause" if pause_t is not None else "mid"
                logger.info(
                    f"  segment [{seg_start:.2f}-{seg_end:.2f}] 분할 ({reason}): {split_t:.2f}"
                )
                subclips.append((seg_start, split_t, seg_text))
                subclips.append((split_t, seg_end, seg_text))
            else:
                subclips.append((seg_start, seg_end, seg_text))

            for (sub_start, sub_end, sub_text) in subclips:
                sub_dur = max(SCENE_MIN_SEC * 0.5, sub_end - sub_start)  # 최소 1초는 남김
                # 원본 씬에서 keyword 승계 (시간 비율 기반)
                rel_mid = ((sub_start + sub_end) / 2.0) / total_audio
                orig = pick_orig_scene(rel_mid)
                seg_counter += 1

                # [C-1] segment_keywords 가 있으면 keyword 교체 (시각적 매칭)
                kw_override = None
                asset_override = None
                if seg_idx in seg_kw_map:
                    kws = seg_kw_map[seg_idx]
                    # 첫 키워드를 메인, 두 번째는 fallback
                    kw_override = kws[0] if kws else None
                    # 원본 asset_url 은 리셋 (새 키워드로 재다운로드 되도록)
                    asset_override = None

                update_dict = {
                    "scene_id": f"{orig.scene_id}_seg{seg_counter}",
                    "duration_seconds": round(sub_dur, 2),
                    "description": sub_text or orig.description,
                }
                if kw_override:
                    update_dict["keyword"] = kw_override
                    update_dict["asset_url"] = None  # 재검색 트리거
                new_scenes.append(orig.model_copy(update=update_dict))

        if not new_scenes:
            logger.info("재분해 결과 없음 — 원본 사용")
            return scenes

        # ── [C] 짧은 씬 인접 병합 (SCENE_MIN_SEC 미만) ────────────────────
        merged = []
        for sc in new_scenes:
            if merged and sc.duration_seconds < SCENE_MIN_SEC:
                prev = merged[-1]
                combined = round(prev.duration_seconds + sc.duration_seconds, 2)
                # 여전히 너무 크면 병합하지 않음 (SCENE_MAX_SEC 초과 방지)
                if combined <= SCENE_MAX_SEC * 1.25:
                    # 병합: 이전 씬의 duration 확장 + description 이어붙이기
                    new_desc = prev.description or ""
                    if sc.description and sc.description.strip() and sc.description.strip() != prev.description:
                        new_desc = (new_desc + " " + sc.description).strip() if new_desc else sc.description
                    merged[-1] = prev.model_copy(update={
                        "duration_seconds": combined,
                        "description": new_desc,
                    })
                    continue
            merged.append(sc)

        # 첫 씬이 너무 짧으면 다음 씬에 병합
        if len(merged) >= 2 and merged[0].duration_seconds < SCENE_MIN_SEC:
            first = merged.pop(0)
            nxt = merged[0]
            combined = round(first.duration_seconds + nxt.duration_seconds, 2)
            if combined <= SCENE_MAX_SEC * 1.25:
                new_desc = first.description or ""
                if nxt.description and nxt.description.strip() and nxt.description.strip() != first.description:
                    new_desc = (new_desc + " " + nxt.description).strip() if new_desc else nxt.description
                merged[0] = nxt.model_copy(update={
                    "scene_id": first.scene_id,  # 첫 씬 ID 유지
                    "duration_seconds": combined,
                    "description": new_desc,
                    "keyword": first.keyword,    # 키워드도 첫 씬 것 승계
                })
            else:
                merged.insert(0, first)  # 병합 안 하고 되돌림

        if len(merged) != len(new_scenes):
            logger.info(
                f"짧은 씬 병합: {len(new_scenes)}씬 → {len(merged)}씬 "
                f"(SCENE_MIN_SEC={SCENE_MIN_SEC}s)"
            )

        logger.info(
            f"의미 재분해 완료: {len(scenes)}씬 → {len(merged)}씬 "
            f"(총 {sum(s.duration_seconds for s in merged):.1f}s / TTS {total_audio:.1f}s)"
        )
        return merged

    except Exception as e:
        logger.error(f"의미 재분해 오류 (원본 사용): {e}", exc_info=True)
        return scenes


def create_srt_from_whisper_segments(timestamps_path, output_path, lead_sec: float = None) -> bool:
    """
    [8] 자막 0.15초 선행

    Whisper segments 기반 SRT 생성. 각 cue의 start를 lead_sec 만큼 당겨서
    음성보다 먼저 자막이 뜨게 함.

    Returns: 생성 성공 여부
    """
    import json as _json
    from pathlib import Path as _Path

    if not timestamps_path:
        return False
    ts_path = _Path(timestamps_path)
    if not ts_path.exists():
        return False

    if lead_sec is None:
        lead_sec = SUBTITLE_LEAD_SEC

    try:
        with open(ts_path, encoding="utf-8") as f:
            ts_data = _json.load(f)
        segments = ts_data.get("segments") or []
        if not segments:
            return False

        # [Q5] 오디오 무음 구간 로드 (캐시가 있으면 재사용, 없으면 직접 검출)
        audio_silences = ts_data.get("audio_silences") or []
        if not audio_silences:
            ap = ts_data.get("audio_path")
            if ap:
                audio_silences = _detect_audio_silences(ap)

        def _snap_to_silence(t: float, is_start: bool) -> float:
            """
            t 에 가장 가까운 무음 경계를 찾아 스냅.
            is_start=True  : 시작 → 가장 가까운 무음 end + LEAD
            is_start=False : 끝   → 가장 가까운 무음 start - TAIL

            윈도우 내 여러 무음이 있으면 가장 가까운 것 선택.
            t 가 무음 안쪽이면 해당 무음의 경계로 우선 스냅.
            """
            if not audio_silences:
                return t
            win = SUBTITLE_SNAP_WINDOW_SEC

            # t 가 무음 내부에 있는지 먼저 확인
            for (s, e) in audio_silences:
                if s - 0.05 <= t <= e + 0.05:
                    # 무음 안쪽 → 시작이면 무음 end + lead, 끝이면 무음 start - tail
                    if is_start:
                        return round(e + SUBTITLE_LEAD_AFTER_SIL_SEC, 3)
                    else:
                        return round(s - SUBTITLE_TAIL_BEFORE_SIL_SEC, 3)

            best = None
            best_diff = 1e9
            if is_start:
                # 가장 가까운 무음의 end 를 찾음 (t 앞뒤 win 범위 내)
                for (s, e) in audio_silences:
                    if abs(e - t) <= win:
                        diff = abs(e - t)
                        if diff < best_diff:
                            best_diff = diff
                            best = e
                if best is not None:
                    return round(best + SUBTITLE_LEAD_AFTER_SIL_SEC, 3)
            else:
                # 가장 가까운 무음의 start 를 찾음
                for (s, e) in audio_silences:
                    if abs(s - t) <= win:
                        diff = abs(s - t)
                        if diff < best_diff:
                            best_diff = diff
                            best = s
                if best is not None:
                    return round(best - SUBTITLE_TAIL_BEFORE_SIL_SEC, 3)
            return t

        def sec_to_srt(sec: float) -> str:
            sec = max(0.0, sec)
            h = int(sec // 3600)
            m = int((sec % 3600) // 60)
            s = int(sec % 60)
            ms = int((sec - int(sec)) * 1000)
            return f"{h:02d}:{m:02d}:{s:02d},{ms:03d}"

        # [Q3] 복합어 보호 + 자연 줄바꿈
        def wrap_lines(text: str, max_chars: int = SUBTITLE_MAX_CHARS) -> list:
            text = text.strip()
            if len(text) <= max_chars:
                return [text]
            # 1) NO_BREAK_TERMS 내부 공백을 NBSP로 치환 → split 시 한 토큰으로 유지
            protected = text
            for term in NO_BREAK_TERMS:
                if term and term in protected:
                    protected = protected.replace(term, term.replace(" ", _NBSP))
            # 2) 쉼표 뒤에 공백 보장
            protected = protected.replace(",", ", ")
            # split(" ") 로 NBSP 분할 방지 (NBSP는 whitespace로 인식되지만 공백 " " 문자는 아님)
            words = [w for w in protected.split(" ") if w]
            lines = []
            cur = ""
            for w in words:
                if not cur:
                    cur = w
                elif len(cur) + 1 + len(w) <= max_chars:
                    cur = cur + " " + w
                else:
                    lines.append(cur)
                    cur = w
            if cur:
                lines.append(cur)
            # NBSP 복원
            return [ln.replace(_NBSP, " ") for ln in lines]

        cues = []  # [(start, end, [line1, line2])]
        for seg in segments:
            seg_start = float(seg.get("start", 0.0) or 0.0)
            seg_end = float(seg.get("end", 0.0) or 0.0)
            seg_text = (seg.get("text") or "").strip()
            if not seg_text or seg_end <= seg_start:
                continue

            lines = wrap_lines(seg_text, SUBTITLE_MAX_CHARS)
            # 2줄 초과면 cue를 분할 (2줄씩 묶어서)
            cue_chunks = [lines[i:i+2] for i in range(0, len(lines), 2)]
            if not cue_chunks:
                continue
            total_chars = sum(len(l) for l in lines) or 1
            cum_chars = 0
            for chunk in cue_chunks:
                chunk_chars = sum(len(l) for l in chunk)
                ratio_start = cum_chars / total_chars
                cum_chars += chunk_chars
                ratio_end = cum_chars / total_chars
                cue_start = seg_start + (seg_end - seg_start) * ratio_start
                cue_end = seg_start + (seg_end - seg_start) * ratio_end
                cues.append((cue_start, cue_end, chunk))

        if not cues:
            return False

        # [Q5] 자막 무음 스냅 + 선행 적용
        #   1. 먼저 각 cue의 start/end 를 무음에 스냅 시도
        #   2. 스냅 실패한 부분만 기존 lead_sec 방식 적용
        #   3. 이전 cue end 보다 앞서지 않도록 보정
        snapped_count = 0
        adjusted = []
        prev_end = 0.0
        for (start, end, chunk) in cues:
            snap_s = _snap_to_silence(start, is_start=True)
            snap_e = _snap_to_silence(end, is_start=False)

            # 스냅 결과가 원본과 다르면 카운트
            used_snap_s = abs(snap_s - start) > 0.01
            used_snap_e = abs(snap_e - end) > 0.01
            if used_snap_s or used_snap_e:
                snapped_count += 1

            # 스냅 실패 시 lead/tail fallback
            adj_start = snap_s if used_snap_s else max(prev_end, start - lead_sec)
            adj_start = max(prev_end, adj_start)  # 겹침 방지
            adj_end = snap_e if used_snap_e else max(adj_start + 0.3, end - 0.05)
            if adj_end <= adj_start:
                adj_end = adj_start + 0.3
            adjusted.append((adj_start, adj_end, chunk))
            prev_end = adj_end

        if snapped_count > 0:
            logger.info(
                f"자막 무음 스냅: {snapped_count}/{len(cues)} cue "
                f"(window={SUBTITLE_SNAP_WINDOW_SEC}s)"
            )

        srt = []
        for i, (start, end, chunk) in enumerate(adjusted, 1):
            srt.append(str(i))
            srt.append(f"{sec_to_srt(start)} --> {sec_to_srt(end)}")
            for line in chunk:
                srt.append(line)
            srt.append("")
        output_path.write_text("\n".join(srt), encoding="utf-8")
        logger.info(
            f"Whisper SRT 생성: {len(adjusted)}개 cue, lead={lead_sec:.2f}s, "
            f"max_chars={SUBTITLE_MAX_CHARS}"
        )
        return True

    except Exception as e:
        logger.error(f"Whisper SRT 생성 오류: {e}", exc_info=True)
        return False


async def process_video_creation(
    job_id: str,
    request: VideoCreateRequest
) -> None:
    """영상 생성 처리 (백그라운드 작업)"""
    global _CURRENT_JOB
    if _CURRENT_JOB is not None:
        logger.warning(f"동시 실행 거부: {job_id}")
        await update_job_status(job_id, JobStatus.FAILED, error="다른 잡 처리 중")
        return
    _CURRENT_JOB = job_id
    try:
        await update_job_status(job_id, JobStatus.PROCESSING, progress=10.0)
        
        job_assets_dir = JOBS_DIR / job_id / "assets"
        job_temp_dir = TMP_DIR / job_id
        job_temp_dir.mkdir(parents=True, exist_ok=True)
        
        # 장면 로드 — request.scenes 우선, 없으면 파일
        req_scenes = getattr(request, "scenes", None) or []
        scenes_file = JOBS_DIR / job_id / "scenes.json"
        if req_scenes:
            # request body 로 전달된 scenes 사용 (UI/브라우저 경로)
            scenes_data = [
                (s.model_dump(mode="json") if hasattr(s, "model_dump") else s)
                for s in req_scenes
            ]
            # 디스크에도 저장 (rebuild 등에서 참조 가능)
            scenes_file.parent.mkdir(parents=True, exist_ok=True)
            scenes_file.write_text(
                json.dumps(scenes_data, ensure_ascii=False, indent=2),
                encoding="utf-8"
            )
            logger.info(f"요청 scenes 로드: {len(scenes_data)}개 (파일로도 저장)")
        else:
            if not scenes_file.exists():
                raise FileNotFoundError(f"장면 파일 없음: {scenes_file}")
            with open(scenes_file) as f:
                scenes_data = json.load(f)
        # scenes.json 형태 정규화: list | {"scenes":[...]} | 단일 dict 모두 허용
        if isinstance(scenes_data, dict):
            if isinstance(scenes_data.get('scenes'), list):
                scenes_data = scenes_data['scenes']
            else:
                scenes_data = [scenes_data]
        if not isinstance(scenes_data, list):
            raise ValueError(f"scenes.json 형식 오류: list 또는 dict 기대, got {type(scenes_data).__name__}")
        scenes = []
        for idx, s in enumerate(scenes_data):
            if not isinstance(s, dict):
                raise ValueError(f"scenes[{idx}] 형식 오류: dict 기대, got {type(s).__name__}")
            scenes.append(Scene(**s))
        
        logger.info(f"로드된 장면: {len(scenes)}개")

        # TTS 타임스탬프로 씬 길이 동기화 (나레이션-영상 일치)
        # 1순위: job_id 기반 / 2순위: audio_url 파일명 기반 (자산 재활용 시)
        tts_timestamps = TMP_DIR / f"{job_id}_timestamps.json"
        if not tts_timestamps.exists() and getattr(request, "audio_url", None):
            try:
                audio_p = Path(request.audio_url)
                alt_ts = audio_p.with_name(audio_p.stem + "_timestamps.json")
                if alt_ts.exists():
                    tts_timestamps = alt_ts
                    logger.info(f"타임스탬프 fallback 사용: {alt_ts}")
            except Exception as _e:
                logger.warning(f"타임스탬프 fallback 탐색 실패: {_e}")
        scenes = sync_scene_durations_from_timestamps(scenes, tts_timestamps)

        # [4]+[7] Whisper segments 기반 의미 재분해 + 리듬 컷 적용
        scenes = rebuild_scenes_from_whisper_segments(scenes, tts_timestamps)

        # [C-1] segment_keywords 로 keyword 교체된 씬(asset_url=None) 재검색·다운로드
        need_download = [s for s in scenes if s.asset_url is None]
        if need_download:
            logger.info(
                f"[C-1] {len(need_download)}개 씬 재검색·다운로드 필요 "
                f"(총 {len(scenes)}개 중)"
            )
            try:
                refreshed = await search_and_download_assets(job_id, need_download)
                refreshed_map = {s.scene_id: s for s in refreshed}
                scenes = [
                    refreshed_map.get(s.scene_id, s) if s.asset_url is None else s
                    for s in scenes
                ]
            except Exception as _e:
                logger.warning(f"[C-1] 재검색·다운로드 실패 (원본 asset fallback 적용): {_e}")

        # asset_url 여전히 없는 씬은 다른 씬의 asset 으로 fallback
        has_assets = [s for s in scenes if s.asset_url]
        if has_assets:
            fallback_url = has_assets[0].asset_url
            for i, s in enumerate(scenes):
                if s.asset_url is None:
                    scenes[i] = s.model_copy(update={"asset_url": fallback_url})
                    logger.info(f"[C-1] 씬 '{s.scene_id}' fallback asset 적용: {fallback_url}")

        # 뮤직비디오 모드
        if request.mode == VideoMode.MUSIC_VIDEO:
            await update_job_status(job_id, JobStatus.PROCESSING, progress=20.0)
            clips = await prepare_clips_for_longform(job_id, scenes, job_temp_dir)
            if not clips:
                raise ValueError("뮤직비디오용 클립 없음")
            await update_job_status(job_id, JobStatus.PROCESSING, progress=40.0)
            subtitle_text = request.subtitle_text or " ".join(
                s.description or s.keyword for s in scenes
            )
            total_dur = sum((s.duration_seconds or 5.0) for s in scenes)
            srt_path = job_temp_dir / f"{job_id}.srt"
            create_srt_from_text(subtitle_text, total_dur, srt_path)
            await update_job_status(job_id, JobStatus.PROCESSING, progress=60.0)
            bgm = get_random_bgm() if request.add_bgm else None
            bgm_vol = getattr(request, 'bgm_volume', 0.8)
            output_video = LONGFORM_DIR / f"{job_id}.mp4"
            if not create_music_video(clips, srt_path, bgm, bgm_vol, output_video):
                raise RuntimeError("뮤직비디오 생성 실패")
            await update_job_status(job_id, JobStatus.PROCESSING, progress=80.0)
            duration = get_video_duration(output_video)
            output_files = {"longform": str(output_video)}
            if request.generate_thumbnail:
                tp = job_temp_dir / "thumbnail_raw.jpg"
                if extract_thumbnail(output_video, tp):
                    tf = THUMBNAILS_DIR / f"{job_id}_thumb.jpg"
                    if add_text_overlay_to_thumbnail(tp, tf, title=request.title or f"MV {job_id[:8]}"):
                        output_files["thumbnail"] = str(tf)

        # 장편 영상 생성
        elif request.mode == VideoMode.LONGFORM or request.generate_shorts:
            await update_job_status(job_id, JobStatus.PROCESSING, progress=20.0)
            
            # 클립 준비
            clips = await prepare_clips_for_longform(job_id, scenes, job_temp_dir)
            
            if not clips:
                raise ValueError("준비된 클립 없음")
            
            await update_job_status(job_id, JobStatus.PROCESSING, progress=40.0)
            
            # Concat 파일 생성
            concat_file = job_temp_dir / "concat.txt"
            if not create_concat_file(clips, concat_file):
                raise RuntimeError("Concat 파일 생성 실패")
            
            # 영상 연결
            combined_video = job_temp_dir / "combined.mp4"
            if not concatenate_videos(concat_file, combined_video):
                raise RuntimeError("영상 연결 실패")
            
            await update_job_status(job_id, JobStatus.PROCESSING, progress=50.0)
            
            # 오디오 믹싱
            # audio_url이 있으면 우선 사용 (외부 TTS 오디오 지원)
            if getattr(request, "audio_url", None):
                tts_audio = Path(request.audio_url)
            else:
                tts_audio = TMP_DIR / f"{job_id}.mp3"
            bgm = None
            
            if request.add_bgm:
                bgm = get_random_bgm()
            
            output_video = LONGFORM_DIR / f"{job_id}.mp4"
            
            if not mix_audio(combined_video, tts_audio, bgm, request.bgm_volume, output_video):
                logger.warning("오디오 믹싱 실패, 오디오 없이 진행")
                # 오디오 없이 비디오만 복사
                shutil.copy(combined_video, output_video)
            
            await update_job_status(job_id, JobStatus.PROCESSING, progress=70.0)
            
            # 영상 길이 조회
            duration = get_video_duration(output_video)
            
            # 썸네일 생성
            output_files = {
                "longform": str(output_video)
            }
            
            if request.generate_thumbnail:
                thumbnail_path = job_temp_dir / "thumbnail_raw.jpg"
                if extract_thumbnail(output_video, thumbnail_path):
                    thumbnail_final = THUMBNAILS_DIR / f"{job_id}_thumb.jpg"
                    if add_text_overlay_to_thumbnail(
                        thumbnail_path,
                        thumbnail_final,
                        title=request.title or f"Video {job_id[:8]}"
                    ):
                        output_files["thumbnail"] = str(thumbnail_final)
            
            await update_job_status(
                job_id,
                JobStatus.PROCESSING,
                progress=80.0,
                output_files=output_files,
                duration_seconds=duration
            )


            # 자막 오버레이 (add_subtitles=True 이면 항상 생성)
            # 씬 description 있으면 씬 동기화 SRT 우선, 없으면 subtitle_text fallback
            if request.add_subtitles:
                try:
                    srt_path = job_temp_dir / f"{job_id}_narration.srt"
                    srt_ok = False
                    # [8] Whisper timestamps가 있으면 최우선 (단어 단위 정확도 + 선행)
                    if tts_timestamps and tts_timestamps.exists():
                        srt_ok = create_srt_from_whisper_segments(tts_timestamps, srt_path)
                        if srt_ok:
                            logger.info(f"Whisper 자막 사용 (lead={SUBTITLE_LEAD_SEC}s)")
                    if not srt_ok and scenes and any(s.description for s in scenes):
                        srt_ok = create_srt_from_scenes(scenes, srt_path)
                        logger.info("씬 동기화 자막 fallback 사용")
                    if not srt_ok and request.subtitle_text:
                        total_dur = duration or sum((s.duration_seconds or 5.0) for s in scenes)
                        srt_ok = create_srt_from_text(request.subtitle_text, total_dur, srt_path)
                        logger.info("텍스트 자막 fallback 사용")
                    if srt_ok:
                        out_sub = LONGFORM_DIR / f"{job_id}_sub.mp4"
                        if add_subtitles_to_video(output_video, srt_path, out_sub):
                            shutil.move(str(out_sub), str(output_video))
                            output_files["longform"] = str(output_video)
                            logger.info("자막 오버레이 완료")
                except Exception as e:
                    logger.error(f"자막 오류: {e}")

            # 숏폼 생성
            if request.generate_shorts:
                shorts_output = SHORTS_DIR / f"{job_id}_short.mp4"
                if create_shortform_from_longform(output_video, shorts_output):
                    output_files["shorts"] = str(shorts_output)
                    await update_job_status(job_id, JobStatus.PROCESSING, progress=90.0, output_files=output_files)
        
        await update_job_status(
            job_id,
            JobStatus.COMPLETED,
            progress=100.0,
            output_files=output_files,
            duration_seconds=duration
        )
        
        logger.info(f"작업 완료: {job_id}")
        # E드라이브 완성 폴더에 복사
        try:
            for key, src_path in list(output_files.items()):
                src = Path(src_path)
                if src.exists():
                    dest_dir = COMPLETE_DIR / key
                    dest_dir.mkdir(parents=True, exist_ok=True)
                    dest = dest_dir / src.name
                    shutil.copy2(src, dest)
                    output_files[f'complete_{key}'] = str(dest)
                    logger.info(f'완성 폴더 복사: {src.name} -> {dest}')
        except Exception as copy_err:
            logger.warning(f'완성 폴더 복사 실패 (무시): {copy_err}')
        # ── YouTube 자동 업로드 ─────────────────────────────────────────────
        if "longform" in output_files:
            try:
                lf_path = output_files["longform"]
                thumb_path = str(THUMBNAILS_DIR / f"{job_id}_thumb.jpg")
                if "thumbnail" in output_files:
                    thumb_path = output_files["thumbnail"]
                # 제목/설명: scenes.json에서 추출
                yt_title = request.title or ""
                yt_description = ""
                try:
                    sfile = JOBS_DIR / job_id / "scenes.json"
                    if sfile.exists():
                        sdata = json.loads(sfile.read_text(encoding="utf-8"))
                        sc_list = sdata.get("scenes", []) if isinstance(sdata, dict) else sdata
                        if not yt_title:
                            raw_title = sdata.get("title", "") if isinstance(sdata, dict) else ""
                            if not raw_title:
                                kws = [s.get("keyword", "") for s in sc_list if s.get("keyword")]
                                raw_title = " | ".join(kws[:3]) if kws else job_id
                            yt_title = raw_title
                        if not yt_description:
                            desc = sdata.get("description", "") if isinstance(sdata, dict) else ""
                            if not desc:
                                desc = " ".join(s.get("narration", "")[:80] for s in sc_list if s.get("narration", ""))
                            yt_description = desc
                except Exception as _pe:
                    logger.warning(f"scenes.json 파싱 오류: {_pe}")
                if not yt_title:
                    yt_title = job_id
                if not yt_description:
                    yt_description = yt_title
                upload_payload = {
                    "video_path": lf_path,
                    "title": yt_title,
                    "description": yt_description + "\n\n#AI #자동영상 #롱폼",
                    "tags": ["AI", "자동영상", "롱폼", "LongForm"],
                    "privacy_status": "private",
                    "thumbnail_path": thumb_path if Path(thumb_path).exists() else None
                }
                logger.info(f"YouTube 자동 업로드 시작: {yt_title}")
                async with httpx.AsyncClient(timeout=180.0) as yt_client:
                    yt_resp = await yt_client.post(
                        "http://lf2_uploader:8003/upload/youtube",
                        json=upload_payload,
                        headers={"X-LF-API-Key": os.getenv("LF_API_KEY", "")}
                    )
                    if yt_resp.status_code == 200:
                        yt_data = yt_resp.json()
                        yt_url = yt_data.get("video_url", "")
                        logger.info(f"YouTube 자동 업로드 성공: {yt_url}")
                        output_files["youtube_url"] = yt_url
                        await update_job_status(job_id, JobStatus.COMPLETED, progress=100.0, output_files=output_files, duration_seconds=duration)
                    else:
                        logger.warning(f"YouTube 업로드 실패 {yt_resp.status_code}: {yt_resp.text[:300]}")
            except Exception as yt_err:
                logger.warning(f"YouTube 자동 업로드 오류 (무시): {yt_err}")
    
    except Exception as e:
        logger.error(f"영상 생성 오류 ({job_id}): {e}")
        await update_job_status(job_id, JobStatus.FAILED, error=str(e))
    finally:
        _CURRENT_JOB = None


# ============================================================================
# API 엔드포인트
# ============================================================================

@app.get("/health", tags=["System"])
async def health_check():
    """헬스 체크"""
    return {
        "status": "healthy",
        "service": "lf_ffmpeg_worker",
        "version": "15.8.0",
        "timestamp": datetime.now().isoformat()
    }


@app.post("/assets/search", response_model=AssetsSearchResponse, tags=["Assets"])
async def search_assets(request: AssetsSearchRequest, background_tasks: BackgroundTasks):
    """
    Pexels/Pixabay에서 영상 자산 검색 및 다운로드
    
    - job_id: 작업 고유 ID
    - scenes: 검색할 장면 목록
    - sources: 검색 소스 (pexels, pixabay)
    """
    try:
        job_id = request.job_id
        
        # 장면 정보 저장
        scenes_file = JOBS_DIR / job_id / "scenes.json"
        scenes_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(scenes_file, "w") as f:
            json.dump([s.dict() for s in request.scenes], f, indent=2)
        
        # 백그라운드에서 자산 검색 및 다운로드
        updated_scenes = await search_and_download_assets(job_id, request.scenes)
        
        # 업데이트된 장면 저장
        with open(scenes_file, "w") as f:
            json.dump([s.dict() for s in updated_scenes], f, indent=2, default=str)
        
        # 다운로드 성공 개수
        downloaded = sum(1 for s in updated_scenes if s.asset_url)
        
        await update_job_status(job_id, JobStatus.PENDING, progress=100.0)
        
        return AssetsSearchResponse(
            job_id=job_id,
            status="completed",
            scenes=updated_scenes,
            downloaded_count=downloaded,
            total_count=len(updated_scenes)
        )
    
    except Exception as e:
        logger.error(f"자산 검색 오류: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/video/create", response_model=VideoCreateResponse, tags=["Video"])
async def create_video(request: VideoCreateRequest, background_tasks: BackgroundTasks):
    """
    FFmpeg를 이용한 영상 생성
    
    - job_id: 작업 고유 ID
    - mode: longform (1920x1080) 또는 shortform (1080x1920)
    - add_subtitles: 자막 추가 여부
    - add_bgm: 배경음악 추가 여부
    - generate_thumbnail: 썸네일 생성 여부
    - generate_shorts: 숏폼 생성 여부
    """
    try:
        # job_id 정규화 (Windows CR/LF 제거)
        job_id = (request.job_id or "").strip().replace("\r", "").replace("\n", "")
        if not job_id:
            raise HTTPException(status_code=400, detail="job_id empty")
        request.job_id = job_id

        # 중복 POST 거부: 진행 중인 동일 job_id
        existing = jobs.get(job_id)
        if existing and existing.status in (JobStatus.PENDING, JobStatus.PROCESSING):
            logger.warning(
                f"중복 /video/create 거부: {job_id} (현재 {existing.status.value} / {existing.progress or 0}%)"
            )
            return VideoCreateResponse(
                success=True,
                job_id=job_id,
                status=existing.status.value
            )
        if _CURRENT_JOB is not None and _CURRENT_JOB != job_id:
            logger.warning(f"다른 잡 처리 중 ({_CURRENT_JOB}) - {job_id} 큐 지연")

        # 작업 상태 초기화
        await update_job_status(job_id, JobStatus.PROCESSING, progress=5.0)

        # 백그라운드에서 영상 생성
        background_tasks.add_task(process_video_creation, job_id, request)

        return VideoCreateResponse(
            success=True,
            job_id=job_id,
            status="processing"
        )
    
    except Exception as e:
        logger.error(f"영상 생성 요청 오류: {e}")
        await update_job_status(job_id, JobStatus.FAILED, error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/job/{job_id}/status", response_model=JobInfo, tags=["Job"])
async def get_job_status(job_id: str):
    """작업 상태 조회"""
    if job_id not in jobs:
        # 작업이 없으면 PENDING 상태로 초기화
        jobs[job_id] = JobInfo(
            job_id=job_id,
            status=JobStatus.PENDING,
            created_at=datetime.now(),
            updated_at=datetime.now()
        )
    
    return jobs[job_id]


@app.on_event("startup")
async def startup_event():
    """애플리케이션 시작 시 초기화"""
    logger.info("FFmpeg Worker 시작")
    logger.info(f"Pexels API 키: {'설정됨' if PEXELS_API_KEY else '미설정'}")
    logger.info(f"Pixabay API 키: {'설정됨' if PIXABAY_API_KEY else '미설정'}")


@app.on_event("shutdown")
async def shutdown_event():
    """애플리케이션 종료"""
    logger.info("FFmpeg Worker 종료")


if __name__ == "__main__":
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8002,
        workers=2,
        log_level="info"
    )


