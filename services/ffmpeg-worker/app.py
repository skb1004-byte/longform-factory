"""
LongForm Factory - FFmpeg Worker v15.0.0
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


async def download_video(video_url: str, output_path: Path, timeout: float = 30.0) -> bool:
    """영상 다운로드 (스트리밍)"""
    try:
        logger.info(f"영상 다운로드 시작: {video_url} -> {output_path}")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        async with httpx.AsyncClient(timeout=timeout) as client:
            async with client.stream("GET", video_url) as response:
                response.raise_for_status()
                async with aiofiles.open(output_path, "wb") as f:
                    async for chunk in response.aiter_bytes(chunk_size=8192):
                        await f.write(chunk)
        
        file_size = output_path.stat().st_size
        logger.info(f"영상 다운로드 완료: {output_path} ({file_size / (1024*1024):.2f}MB)")
        return True
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


def run_ffmpeg_command(command: List[str]) -> bool:
    """FFmpeg 커맨드 실행"""
    try:
        logger.info(f"FFmpeg 커맨드 실행: {' '.join(command[:5])}...")
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=300.0  # 5분 타임아웃
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


async def prepare_clips_for_longform(
    job_id: str,
    scenes: List[Scene],
    output_dir: Path
) -> List[Path]:
    """장편(1920x1080) 용 클립 준비"""
    clips = []
    job_assets_dir = JOBS_DIR / job_id / "assets"
    
    for scene in scenes:
        if not scene.asset_url:
            logger.warning(f"장면 '{scene.scene_id}' 자산 없음")
            continue
        
        clip_output = output_dir / f"clip_{scene.scene_id}.mp4"
        
        # FFmpeg 커맨드: 트림, 스케일, 패드
        command = [
            "ffmpeg",
            "-i", scene.asset_url,
            "-t", str(scene.duration_seconds),
            "-vf", "scale=1920:1080:force_original_aspect_ratio=decrease,pad=1920:1080:(ow-iw)/2:(oh-ih)/2,setsar=1",
            "-c:v", "libx264",
            "-preset", "fast",
            "-an",  # 오디오 제거
            "-y",  # 덮어쓰기
            str(clip_output)
        ]
        
        if run_ffmpeg_command(command):
            clips.append(clip_output)
            logger.info(f"클립 준비 완료: {clip_output}")
        else:
            logger.error(f"클립 준비 실패: {scene.scene_id}")
    
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


def concatenate_videos(concat_file: Path, output_video: Path) -> bool:
    """영상 파일 연결"""
    command = [
        "ffmpeg",
        "-f", "concat",
        "-safe", "0",
        "-i", str(concat_file),
        "-c", "copy",
        "-y",
        str(output_video)
    ]
    
    return run_ffmpeg_command(command)


def mix_audio(
    video_path: Path,
    tts_audio_path: Path,
    bgm_path: Optional[Path],
    bgm_volume: float,
    output_video: Path
) -> bool:
    """오디오 믹싱 (TTS 나레이션 + 배경음악)"""
    
    if not tts_audio_path.exists():
        logger.warning(f"TTS 오디오 없음: {tts_audio_path}")
        # TTS가 없으면 배경음악만 추가
        if bgm_path and bgm_path.exists():
            command = [
                "ffmpeg",
                "-i", str(video_path),
                "-i", str(bgm_path),
                "-filter_complex", f"[1:a]volume={bgm_volume}[audio]",
                "-map", "0:v",
                "-map", "[audio]",
                "-c:v", "copy",
                "-c:a", "aac",
                "-b:a", "192k",
                "-y",
                str(output_video)
            ]
        else:
            # 오디오 없이 비디오만 복사
            command = [
                "ffmpeg",
                "-i", str(video_path),
                "-c", "copy",
                "-y",
                str(output_video)
            ]
    else:
        if bgm_path and bgm_path.exists():
            # TTS + BGM 믹싱
            command = [
                "ffmpeg",
                "-i", str(video_path),
                "-i", str(tts_audio_path),
                "-i", str(bgm_path),
                "-filter_complex",
                f"[1:a]volume=1.0[narration];[2:a]volume={bgm_volume}[bgm];[narration][bgm]amix=inputs=2:duration=first[audio]",
                "-map", "0:v",
                "-map", "[audio]",
                "-c:v", "copy",
                "-c:a", "aac",
                "-b:a", "192k",
                "-y",
                str(output_video)
            ]
        else:
            # TTS만 추가
            command = [
                "ffmpeg",
                "-i", str(video_path),
                "-i", str(tts_audio_path),
                "-c:v", "copy",
                "-c:a", "aac",
                "-b:a", "192k",
                "-map", "0:v:0",
                "-map", "1:a:0",
                "-y",
                str(output_video)
            ]
    
    return run_ffmpeg_command(command)


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
            duration = float(result.stdout.strip())
            return duration
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
        subtitle_style = (
            "FontName=Noto Sans CJK KR,"
            "FontSize=44,"
            "PrimaryColour=&H00FFFFFF,"    # 흰색 텍스트
            "OutlineColour=&H00000000,"    # 검정 외곽선
            "BackColour=&H40000000,"       # 반투명 배경
            "Outline=3,"
            "Shadow=1,"
            "Bold=1,"
            "Alignment=2,"                 # 하단 중앙
            "MarginV=80"                   # 하단 여백
        )

        # 4) 자막 필터 문자열 (srt 경로 이스케이프)
        srt_escaped = str(srt_path).replace("\\", "/").replace(":", "\\:")
        subtitle_filter = f"subtitles={srt_escaped}:force_style='{subtitle_style}'"

        # 5) BGM 포함 여부에 따라 명령 구성
        if bgm_path and bgm_path.exists():
            # BGM + 자막
            cmd = [
                "ffmpeg",
                "-i", str(combined),
                "-stream_loop", "-1",   # BGM 반복
                "-i", str(bgm_path),
                "-filter_complex",
                f"[1:a]volume={bgm_volume}[bgm]",
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
    ElevenLabs with-timestamps JSON 기반으로 씬별 비디오 클립 길이 동기화.

    전략:
    1. timestamps JSON에서 전체 TTS 오디오 길이 추출
       (character_end_times_seconds 마지막 값)
    2. scenes duration_seconds 합계 대비 비율로 각 씬에 오디오 시간 배분
    3. 조정된 duration_seconds 반환 → 비디오 클립이 TTS와 정확히 일치

    Returns: duration_seconds 조정된 씬 목록
    """
    import json as _json
    from pathlib import Path as _Path

    if not timestamps_path:
        logger.warning("타임스탬프 경로 없음 — 씬 길이 동기화 스킵")
        return scenes

    ts_path = _Path(timestamps_path)
    if not ts_path.exists():
        logger.warning(f"타임스탬프 파일 없음: {ts_path} — 씬 길이 동기화 스킵")
        return scenes

    try:
        with open(ts_path, encoding="utf-8") as f:
            ts_data = _json.load(f)

        alignment = ts_data.get("alignment", {})
        end_times = alignment.get("character_end_times_seconds", [])

        if not end_times:
            logger.warning("alignment 데이터 없음 — 씬 길이 동기화 스킵")
            return scenes

        # 전체 TTS 오디오 길이
        total_audio_sec = float(end_times[-1])
        logger.info(f"TTS 전체 길이: {total_audio_sec:.2f}초")

        total_scene_sec = sum(s.duration_seconds for s in scenes)
        if total_scene_sec <= 0:
            logger.warning("씬 총 길이 0 — 동기화 스킵")
            return scenes

        ratio = total_audio_sec / total_scene_sec
        synced = []
        for s in scenes:
            new_dur = max(1.0, round(s.duration_seconds * ratio, 2))
            if abs(new_dur - s.duration_seconds) > 0.1:
                logger.info(f"씬 '{s.scene_id}' 길이 조정: {s.duration_seconds:.1f}s -> {new_dur:.1f}s")
            synced.append(s.model_copy(update={"duration_seconds": new_dur}))

        actual_total = sum(s.duration_seconds for s in synced)
        logger.info(f"씬 동기화 완료: 씬합계 {total_scene_sec:.1f}s -> TTS {total_audio_sec:.1f}s (실제합계 {actual_total:.1f}s)")
        return synced

    except Exception as e:
        logger.error(f"씬 동기화 오류 (원본 사용): {e}")
        return scenes

async def process_video_creation(
    job_id: str,
    request: VideoCreateRequest
) -> None:
    """영상 생성 처리 (백그라운드 작업)"""
    try:
        await update_job_status(job_id, JobStatus.PROCESSING, progress=10.0)
        
        job_assets_dir = JOBS_DIR / job_id / "assets"
        job_temp_dir = TMP_DIR / job_id
        job_temp_dir.mkdir(parents=True, exist_ok=True)
        
        # 장면 파일 로드
        scenes_file = JOBS_DIR / job_id / "scenes.json"
        if not scenes_file.exists():
            raise FileNotFoundError(f"장면 파일 없음: {scenes_file}")
        
        with open(scenes_file) as f:
            scenes_data = json.load(f)
        scenes = [Scene(**s) for s in scenes_data]
        
        logger.info(f"로드된 장면: {len(scenes)}개")

        # TTS 타임스탬프로 씬 길이 동기화 (나레이션-영상 일치)
        tts_timestamps = TMP_DIR / f"{job_id}_timestamps.json"
        scenes = sync_scene_durations_from_timestamps(scenes, tts_timestamps)
        
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
            total_dur = sum(s.duration_seconds for s in scenes)
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
            tts_audio = TMP_DIR / f"{job_id}.mp3"
            bgm = None
            
            if request.add_bgm:
                bgm = get_random_bgm()
            
            output_video = LONGFORM_DIR / f"{job_id}.mp4"
            
            if not mix_audio(combined_video, tts_audio, bgm, request.bgm_volume, output_video):
                logger.warning("오디오 믹싱 실패, 오디오 없이 진행")
                # 오디오 없이 비디오만 복사
                import shutil
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
    
    except Exception as e:
        logger.error(f"영상 생성 오류 ({job_id}): {e}")
        await update_job_status(job_id, JobStatus.FAILED, error=str(e))


# ============================================================================
# API 엔드포인트
# ============================================================================

@app.get("/health", tags=["System"])
async def health_check():
    """헬스 체크"""
    return {
        "status": "healthy",
        "service": "lf_ffmpeg_worker",
        "version": "15.0.0",
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
        job_id = request.job_id
        
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
