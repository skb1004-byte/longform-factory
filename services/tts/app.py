"""
FastAPI TTS 서비스 - ElevenLabs 기반 음성 합성 API
Service: lf_tts:2.0.0
Port: 8001

주요 기능:
- 단일/배치 TTS 요청 처리
- 한국어 기본 음성 지원 (남성/여성)
- 자동 재시도 (최대 3회)
- 오디오 길이 자동 계산
- ElevenLabs with-timestamps API → 타임스탬프 JSON 저장
- 통합 로깅 및 에러 핸들링
"""

import os
import json
import base64
import logging
import asyncio
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Tuple, Dict, Any

import httpx
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
from mutagen.mp3 import MP3

# ==================== 설정 ====================

# 환경 변수
ELEVENLABS_API_KEY = os.getenv("ELEVENLABS_API_KEY")
if not ELEVENLABS_API_KEY:
    raise RuntimeError("ELEVENLABS_API_KEY 환경 변수가 설정되지 않았습니다.")

ELEVENLABS_API_BASE = "https://api.elevenlabs.io/v1"
OUTPUT_DIR = Path("/data/tmp")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# 기본 음성 ID (ElevenLabs 한국어 음성)
DEFAULT_VOICES = {
    "korean_male": "nPczCjzI2devNBz1zQrb",  # Brian - Deep, Resonant (premade)
    "korean_female": "SAz9YHcvj6GT2YYXdXww"  # River - Neutral, Informative (premade)
}

# 기본 모델
DEFAULT_MODEL = "eleven_multilingual_v2"

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ==================== Pydantic 모델 ====================

class TTSRequest(BaseModel):
    """TTS 단일 요청"""
    text: str = Field(..., min_length=1, max_length=5000, description="변환할 텍스트")
    voice_id: Optional[str] = Field(default=None, description="ElevenLabs 음성 ID")
    voice_preset: Optional[str] = Field(
        default="korean_male",
        description="음성 프리셋: korean_male, korean_female"
    )
    model_id: str = Field(default=DEFAULT_MODEL, description="사용할 모델 ID")
    stability: float = Field(default=0.5, ge=0.0, le=1.0, description="안정성 (0-1)")
    similarity_boost: float = Field(default=0.75, ge=0.0, le=1.0, description="유사성 부스트 (0-1)")
    output_format: str = Field(default="mp3_44100_128", description="출력 포맷")
    filename: Optional[str] = Field(default=None, description="저장 파일명 (확장자 제외)")


class BatchTTSRequest(BaseModel):
    """TTS 배치 요청"""
    items: List[TTSRequest] = Field(..., min_items=1, max_items=100, description="TTS 요청 목록")


class TTSResponse(BaseModel):
    """TTS 응답"""
    success: bool
    file_path: str
    duration_seconds: float
    voice_id: str
    characters: int
    message: Optional[str] = None
    timestamps_path: Optional[str] = None  # 씬 타임스탬프 JSON 경로


class HealthResponse(BaseModel):
    """헬스 체크 응답"""
    status: str
    timestamp: str
    api_key_configured: bool


class Voice(BaseModel):
    """음성 정보"""
    voice_id: str
    name: str
    language: str
    preview_url: Optional[str] = None


# ==================== FastAPI 앱 초기화 ====================

app = FastAPI(
    title="LongForm TTS Service",
    description="ElevenLabs 기반 텍스트-음성 변환 서비스",
    version="2.0.0"
)

# ==================== 유틸리티 함수 ====================

def get_voice_id(voice_id: Optional[str], voice_preset: Optional[str]) -> str:
    """음성 ID 결정"""
    if voice_id:
        return voice_id

    preset = voice_preset or "korean_male"
    if preset not in DEFAULT_VOICES:
        logger.warning(f"알 수 없는 음성 프리셋: {preset}, 기본값 사용")
        return DEFAULT_VOICES["korean_male"]

    return DEFAULT_VOICES[preset]


def get_audio_duration(file_path: Path) -> float:
    """MP3 파일 길이 계산 (초)"""
    try:
        audio = MP3(str(file_path))
        return audio.info.length
    except Exception as e:
        logger.error(f"오디오 길이 계산 실패 ({file_path}): {e}")
        return 0.0


async def call_elevenlabs_tts(
    text: str,
    voice_id: str,
    model_id: str,
    stability: float,
    similarity_boost: float,
    output_format: str,
    max_retries: int = 3
) -> Tuple[bytes, Optional[Dict[str, Any]]]:
    """
    ElevenLabs TTS with-timestamps API 호출 (재시도 로직 포함)

    Args:
        text: 변환할 텍스트
        voice_id: ElevenLabs 음성 ID
        model_id: 모델 ID
        stability: 안정성 (0-1)
        similarity_boost: 유사성 부스트 (0-1)
        output_format: 출력 포맷
        max_retries: 최대 재시도 횟수

    Returns:
        (오디오 바이너리 데이터, alignment 딕셔너리 또는 None)

    Raises:
        HTTPException: API 호출 실패 시
    """
    url = f"{ELEVENLABS_API_BASE}/text-to-speech/{voice_id}/with-timestamps"

    headers = {
        "xi-api-key": ELEVENLABS_API_KEY,
        "Content-Type": "application/json"
    }

    payload = {
        "text": text,
        "model_id": model_id,
        "voice_settings": {
            "stability": stability,
            "similarity_boost": similarity_boost
        },
        "output_format": output_format
    }

    for attempt in range(max_retries):
        try:
            logger.info(f"TTS API 호출 (시도 {attempt + 1}/{max_retries})")

            async with httpx.AsyncClient(timeout=60.0) as client:
                response = await client.post(url, json=payload, headers=headers)

                if response.status_code == 200:
                    # with-timestamps 응답: JSON { audio_base64, alignment }
                    try:
                        data = response.json()
                        audio_b64 = data.get("audio_base64", "")
                        audio_bytes = base64.b64decode(audio_b64)
                        alignment = data.get("alignment", None)
                        logger.info(
                            f"TTS API 성공: {len(audio_bytes)} bytes, "
                            f"정렬 데이터={'있음' if alignment else '없음'}"
                        )
                        return audio_bytes, alignment
                    except (ValueError, KeyError) as parse_err:
                        # JSON 파싱 실패 시 raw bytes로 fallback (구버전 API 호환)
                        logger.warning(f"JSON 파싱 실패, raw bytes 사용: {parse_err}")
                        return response.content, None

                error_msg = response.text
                logger.warning(
                    f"TTS API 실패 (상태: {response.status_code}): {error_msg}"
                )

                if response.status_code >= 500:
                    # 서버 에러: 재시도
                    if attempt < max_retries - 1:
                        await asyncio.sleep(2 ** attempt)  # 지수 백오프
                        continue

                raise HTTPException(
                    status_code=response.status_code,
                    detail=f"ElevenLabs API 에러: {error_msg[:200]}"
                )

        except httpx.TimeoutException as e:
            logger.error(f"TTS API 타임아웃 (시도 {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(2 ** attempt)
                continue
            raise HTTPException(status_code=504, detail="TTS API 타임아웃")

        except HTTPException:
            raise

        except Exception as e:
            logger.error(f"TTS API 호출 예외 (시도 {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(2 ** attempt)
                continue
            raise HTTPException(status_code=500, detail=f"TTS 처리 실패: {str(e)}")

    raise HTTPException(
        status_code=500,
        detail=f"최대 재시도 횟수({max_retries}) 초과"
    )


async def fetch_available_voices() -> List[Voice]:
    """ElevenLabs에서 사용 가능한 음성 목록 조회"""
    url = f"{ELEVENLABS_API_BASE}/voices"

    headers = {
        "xi-api-key": ELEVENLABS_API_KEY
    }

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(url, headers=headers)

            if response.status_code != 200:
                logger.error(f"음성 목록 조회 실패: {response.text}")
                return []

            data = response.json()
            voices = []

            for voice_data in data.get("voices", []):
                voice = Voice(
                    voice_id=voice_data.get("voice_id"),
                    name=voice_data.get("name"),
                    language=voice_data.get("language"),
                    preview_url=voice_data.get("preview_url")
                )
                voices.append(voice)

            logger.info(f"음성 목록 조회 성공: {len(voices)}개")
            return voices

    except Exception as e:
        logger.error(f"음성 목록 조회 중 예외: {e}")
        return []


# ==================== API 엔드포인트 ====================

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """헬스 체크 엔드포인트"""
    return HealthResponse(
        status="healthy",
        timestamp=datetime.utcnow().isoformat(),
        api_key_configured=bool(ELEVENLABS_API_KEY)
    )


@app.get("/voices", response_model=List[Voice])
async def list_voices():
    """
    ElevenLabs에서 사용 가능한 음성 목록 조회

    Returns:
        음성 정보 리스트
    """
    voices = await fetch_available_voices()
    if not voices:
        raise HTTPException(status_code=503, detail="음성 목록 조회 불가")
    return voices


@app.post("/tts", response_model=TTSResponse)
async def tts_convert(request: TTSRequest, background_tasks: BackgroundTasks):
    """
    단일 텍스트를 음성으로 변환

    Args:
        request: TTS 요청 정보
        background_tasks: 백그라운드 작업

    Returns:
        변환 결과 (파일 경로, 길이, 음성 ID 등, 타임스탬프 경로)
    """
    try:
        # 음성 ID 결정
        voice_id = get_voice_id(request.voice_id, request.voice_preset)

        # 파일명 생성
        if request.filename:
            filename = request.filename
        else:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            filename = f"tts_{timestamp}"

        file_path = OUTPUT_DIR / f"{filename}.mp3"

        logger.info(
            f"TTS 변환 시작: 텍스트={len(request.text)}자, "
            f"음성={voice_id}, 모델={request.model_id}"
        )

        # ElevenLabs API 호출 (audio_bytes + alignment 반환)
        audio_data, alignment = await call_elevenlabs_tts(
            text=request.text,
            voice_id=voice_id,
            model_id=request.model_id,
            stability=request.stability,
            similarity_boost=request.similarity_boost,
            output_format=request.output_format
        )

        # 오디오 파일 저장
        file_path.write_bytes(audio_data)
        logger.info(f"오디오 파일 저장: {file_path}")

        # 타임스탬프 JSON 저장
        timestamps_path: Optional[str] = None
        if alignment:
            ts_file = OUTPUT_DIR / f"{filename}_timestamps.json"
            ts_data = {
                "filename": filename,
                "audio_path": str(file_path),
                "text": request.text,
                "alignment": alignment
            }
            ts_file.write_text(json.dumps(ts_data, ensure_ascii=False, indent=2), encoding="utf-8")
            timestamps_path = str(ts_file)
            logger.info(f"타임스탬프 저장: {ts_file}")

        # 오디오 길이 계산
        duration = get_audio_duration(file_path)

        logger.info(
            f"TTS 변환 완료: {file_path} ({duration:.2f}초, {len(audio_data)} bytes)"
        )

        return TTSResponse(
            success=True,
            file_path=str(file_path),
            duration_seconds=duration,
            voice_id=voice_id,
            characters=len(request.text),
            timestamps_path=timestamps_path
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"TTS 변환 중 예외: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"TTS 변환 실패: {str(e)}")


@app.post("/tts/batch")
async def batch_tts(request: BatchTTSRequest, background_tasks: BackgroundTasks):
    """
    여러 텍스트를 배치로 음성 변환

    Args:
        request: 배치 TTS 요청 (최대 100개 항목)

    Returns:
        변환 결과 리스트
    """
    try:
        results = []

        logger.info(f"배치 TTS 변환 시작: {len(request.items)}개 항목")

        for idx, tts_request in enumerate(request.items, 1):
            try:
                logger.info(f"배치 항목 처리 ({idx}/{len(request.items)})")

                # 음성 ID 결정
                voice_id = get_voice_id(tts_request.voice_id, tts_request.voice_preset)

                # 파일명 생성
                if tts_request.filename:
                    filename = tts_request.filename
                else:
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
                    filename = f"batch_tts_{idx:03d}_{timestamp}"

                file_path = OUTPUT_DIR / f"{filename}.mp3"

                # ElevenLabs API 호출
                audio_data, alignment = await call_elevenlabs_tts(
                    text=tts_request.text,
                    voice_id=voice_id,
                    model_id=tts_request.model_id,
                    stability=tts_request.stability,
                    similarity_boost=tts_request.similarity_boost,
                    output_format=tts_request.output_format
                )

                # 파일 저장
                file_path.write_bytes(audio_data)

                # 타임스탬프 JSON 저장
                timestamps_path: Optional[str] = None
                if alignment:
                    ts_file = OUTPUT_DIR / f"{filename}_timestamps.json"
                    ts_data = {
                        "filename": filename,
                        "audio_path": str(file_path),
                        "text": tts_request.text,
                        "alignment": alignment
                    }
                    ts_file.write_text(
                        json.dumps(ts_data, ensure_ascii=False, indent=2),
                        encoding="utf-8"
                    )
                    timestamps_path = str(ts_file)

                # 오디오 길이 계산
                duration = get_audio_duration(file_path)

                results.append(TTSResponse(
                    success=True,
                    file_path=str(file_path),
                    duration_seconds=duration,
                    voice_id=voice_id,
                    characters=len(tts_request.text),
                    timestamps_path=timestamps_path
                ))

            except Exception as e:
                logger.error(f"배치 항목 {idx} 처리 실패: {e}")
                results.append(TTSResponse(
                    success=False,
                    file_path="",
                    duration_seconds=0.0,
                    voice_id="",
                    characters=len(tts_request.text),
                    message=f"처리 실패: {str(e)[:100]}"
                ))

        logger.info(
            f"배치 TTS 변환 완료: "
            f"성공={sum(1 for r in results if r.success)}/{len(results)}"
        )

        return {
            "total": len(results),
            "success_count": sum(1 for r in results if r.success),
            "results": results
        }

    except Exception as e:
        logger.error(f"배치 TTS 처리 중 예외: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"배치 처리 실패: {str(e)}")


# ==================== 루트 엔드포인트 ====================

@app.get("/")
async def root():
    """루트 정보"""
    return {
        "service": "LongForm TTS Service",
        "version": "2.0.0",
        "provider": "ElevenLabs",
        "endpoints": {
            "health": "/health",
            "voices": "/voices",
            "tts": "/tts",
            "batch": "/tts/batch",
            "docs": "/docs",
            "redoc": "/redoc"
        }
    }


# ==================== 실행 ====================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=8001,
        reload=False,
        log_level="info"
    )
