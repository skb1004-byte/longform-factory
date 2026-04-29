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

# ==================== Whisper (단어 단위 타임코드) ====================
# faster-whisper: CTranslate2 기반, CPU int8 양자화로 가볍게 동작
try:
    from faster_whisper import WhisperModel  # type: ignore
    _WHISPER_AVAILABLE = True
except Exception as _whisper_imp_err:  # pragma: no cover
    _WHISPER_AVAILABLE = False
    _whisper_import_error = _whisper_imp_err

# ==================== 로깅 설정 ====================
# Must be initialized before any config section that uses logger.

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ==================== 설정 ====================

# 환경 변수
ELEVENLABS_API_KEY = os.getenv("ELEVENLABS_API_KEY")
if not ELEVENLABS_API_KEY:
    logger.warning("ELEVENLABS_API_KEY 미설정 — ElevenLabs 엔진 비활성화 (edge/kokoro 사용 가능)")

ELEVENLABS_API_BASE = "https://api.elevenlabs.io/v1"
OUTPUT_DIR = Path("/data/tmp")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# 기본 음성 ID (ElevenLabs 한국어 음성)
DEFAULT_VOICES = {
    "korean_male": os.getenv("ELEVENLABS_VOICE_MALE", ""),   # Brian - Deep, Resonant (ElevenLabs)
    "korean_female": os.getenv("ELEVENLABS_VOICE_FEMALE", "")   # River - Neutral, Informative (ElevenLabs)
}

# Edge TTS Korean voices (무료, 네이티브 한국어 발음)
EDGE_VOICES = {
    "korean_female": "ko-KR-SunHiNeural",        # 자연스러운 여성
    "korean_male": "ko-KR-InJoonNeural",          # 뉴스 톤 남성
    "korean_male2": "ko-KR-HyunsuMultilingualNeural",  # 다국어 남성
    "korean_female_slow": "ko-KR-SunHiNeural",    # 느린 강조 여성
}
DEFAULT_ENGINE = "edge"  # "edge" or "elevenlabs"

# 기본 모델
DEFAULT_MODEL = "eleven_v3"

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
    stability: float = Field(default=0.38, ge=0.0, le=1.0, description="안정성 (0-1)")
    similarity_boost: float = Field(default=0.88, ge=0.0, le=1.0, description="유사성 부스트 (0-1)")
    style: float = Field(default=0.60, ge=0.0, le=1.0, description="스타일 강도 (0-1)")
    use_speaker_boost: bool = Field(default=True, description="스피커 부스트 사용")
    engine: str = Field(default="edge", description="TTS 엔진: edge(무료) 또는 elevenlabs")
    edge_voice: str = Field(default="ko-KR-SunHiNeural", description="Edge TTS 음성")
    edge_rate: str = Field(default="-5%", description="Edge TTS 속도")
    output_format: str = Field(default="mp3_44100_128", description="출력 포맷")
    filename: Optional[str] = Field(default=None, description="저장 파일명 (확장자 제외)")
    preprocess: bool = Field(default=True, description="[D] 호흡·리듬 자동 주입 (긴 문장 분할 + silence 삽입)")
    sentence_pause: Optional[float] = Field(default=None, description="마침표 뒤 기본 쉼 (초, None이면 env)")
    comma_pause: Optional[float] = Field(default=None, description="쉼표 뒤 쉼 (초, None이면 env)")
    max_sentence_chars: Optional[int] = Field(default=None, description="이보다 긴 문장 자동 분할")


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


# ---- Whisper 싱글턴 모델 (lazy init) ------------------------------------
_whisper_model = None
_whisper_lock = asyncio.Lock()

WHISPER_MODEL_SIZE = os.getenv("WHISPER_MODEL", "base")  # tiny|base|small|medium|large-v3
WHISPER_DEVICE = os.getenv("WHISPER_DEVICE", "cpu")      # cpu|cuda
WHISPER_COMPUTE = os.getenv("WHISPER_COMPUTE", "int8")   # int8|float16|float32
WHISPER_LANGUAGE = os.getenv("WHISPER_LANGUAGE", "ko")


async def get_whisper_model():
    """lazy-load 싱글턴 Whisper 모델"""
    global _whisper_model
    if not _WHISPER_AVAILABLE:
        return None
    if _whisper_model is None:
        async with _whisper_lock:
            if _whisper_model is None:
                logger.info(
                    f"Whisper 모델 로딩: size={WHISPER_MODEL_SIZE} "
                    f"device={WHISPER_DEVICE} compute={WHISPER_COMPUTE}"
                )
                loop = asyncio.get_event_loop()
                def _load():
                    return WhisperModel(
                        WHISPER_MODEL_SIZE,
                        device=WHISPER_DEVICE,
                        compute_type=WHISPER_COMPUTE,
                    )
                _whisper_model = await loop.run_in_executor(None, _load)
                logger.info("Whisper 모델 로딩 완료")
    return _whisper_model


async def extract_whisper_timestamps(audio_path: Path, language: str = None) -> Optional[Dict[str, Any]]:
    """
    Whisper로 오디오에서 단어·세그먼트 타임코드 추출.

    Returns 스키마:
    {
      "source": "whisper",
      "language": "ko",
      "duration": 123.45,
      "segments": [{"id":0,"start":0.0,"end":3.5,"text":"..."}...],
      "words":    [{"word":"...","start":0.12,"end":0.34}...],
      "alignment": {"character_end_times_seconds": [<audio_total_sec>]}  # ffmpeg-worker 호환
    }
    """
    if not _WHISPER_AVAILABLE:
        logger.warning("faster-whisper 미설치 — 타임코드 추출 스킵")
        return None

    try:
        model = await get_whisper_model()
        if model is None:
            return None

        lang = language or WHISPER_LANGUAGE
        logger.info(f"Whisper 전사 시작: {audio_path.name} (lang={lang})")

        loop = asyncio.get_event_loop()
        def _transcribe():
            segments_gen, info = model.transcribe(
                str(audio_path),
                language=lang,
                word_timestamps=True,
                vad_filter=True,
                vad_parameters={"min_silence_duration_ms": 300},
            )
            seg_list = []
            word_list = []
            for seg in segments_gen:
                seg_list.append({
                    "id": seg.id,
                    "start": round(float(seg.start), 3),
                    "end": round(float(seg.end), 3),
                    "text": (seg.text or "").strip(),
                })
                if getattr(seg, "words", None):
                    for w in seg.words:
                        if w.start is None or w.end is None:
                            continue
                        word_list.append({
                            "word": (w.word or "").strip(),
                            "start": round(float(w.start), 3),
                            "end": round(float(w.end), 3),
                        })
            return seg_list, word_list, info

        seg_list, word_list, info = await loop.run_in_executor(None, _transcribe)

        total_sec = seg_list[-1]["end"] if seg_list else float(info.duration or 0.0)

        result = {
            "source": "whisper",
            "language": info.language,
            "language_probability": round(float(info.language_probability or 0.0), 3),
            "duration": round(total_sec, 3),
            "segments": seg_list,
            "words": word_list,
            # ── ffmpeg-worker 호환: character_end_times_seconds[-1]만 있으면 충분 ──
            "alignment": {
                "character_end_times_seconds": [round(total_sec, 3)],
            },
        }

        logger.info(
            f"Whisper 전사 완료: {len(seg_list)}개 세그먼트, "
            f"{len(word_list)}개 단어, 총 {total_sec:.2f}초"
        )
        return result

    except Exception as e:
        logger.error(f"Whisper 전사 실패 ({audio_path}): {e}", exc_info=True)
        return None


# ---- 구(phrase) 분할 ────────────────────────────────────────────────
# 한국어 접속사·구두점 기준으로 긴 문장을 짧은 구로 쪼갬
KOREAN_CONNECTORS = [
    " 하고 ", " 하며 ", " 하면서 ",
    " 그리고 ", " 또한 ", " 그래서 ", " 그러므로 ",
    " 하지만 ", " 그러나 ", " 그런데 ",
    " 따라서 ", " 즉 ", " 왜냐하면 ",
    " 또는 ", " 혹은 ",
]
# 긴 문장 기준 (이것보다 길면 접속사 기준 추가 분할)
PHRASE_MAX_CHARS = int(os.getenv("PHRASE_MAX_CHARS", "28"))


def split_into_phrases(text: str, max_chars: int = None) -> list:
    """
    문장을 쉼표·접속사 기준으로 sub-phrase로 분할.
    - 1차: 쉼표/일본중문(、，,)
    - 2차: 한국어 접속사 (하고/하며/그리고/하지만…)
    - 각 부분이 여전히 길면 그대로 반환 (음성 길이로 끊어질 것)
    """
    import re as _re
    mc = max_chars if max_chars is not None else PHRASE_MAX_CHARS
    text = text.strip()
    if not text:
        return []

    # 1차 쉼표 기준 분할 (쉼표는 유지하기 위해 split + 복원)
    parts1 = []
    buf = ""
    for ch in text:
        buf += ch
        if ch in ",，、":
            parts1.append(buf.strip())
            buf = ""
    if buf.strip():
        parts1.append(buf.strip())

    # 2차: 긴 부분은 접속사 기준 추가 분할
    out = []
    for part in parts1:
        if len(part) <= mc:
            out.append(part)
            continue
        # 접속사 기준 분할 (접속사는 뒷 파트로 붙음 → "… / 하지만 …")
        rem = part
        while len(rem) > mc:
            cut_at = -1
            cut_len = 0
            for conn in KOREAN_CONNECTORS:
                idx = rem.find(conn, mc // 2)  # 최소 절반 이상 지난 뒤에 자름
                if idx > 0:
                    if cut_at < 0 or idx < cut_at:
                        cut_at = idx
                        cut_len = len(conn)
            if cut_at < 0:
                break
            out.append(rem[:cut_at + 1].strip())   # 공백까지 포함
            rem = rem[cut_at + 1:].strip()          # 접속사부터
        if rem:
            out.append(rem)
    return [p.strip() for p in out if p.strip()]


def _map_phrases_to_words(phrases: list, whisper_words: list) -> list:
    """
    phrase 리스트를 Whisper word 리스트에 문자 수 비율로 매핑 → 각 phrase의 start/end 계산.
    whisper_words 가 비어있으면 None 반환 (caller가 문자 비율 fallback 사용).
    """
    if not whisper_words or not phrases:
        return None
    total_chars = sum(len(p) for p in phrases) or 1
    # word 별 누적 길이도 계산 (원본 길이 ≠ 음성 길이 보정 위해 사용하지 않음 — 단순 문자 비율)
    n_words = len(whisper_words)
    out = []
    cum = 0
    for i, phr in enumerate(phrases):
        start_idx = min(n_words - 1, int(n_words * cum / total_chars))
        cum += len(phr)
        end_idx = min(n_words, int(n_words * cum / total_chars))
        if end_idx <= start_idx:
            end_idx = start_idx + 1
        end_idx = min(end_idx, n_words)

        start_t = float(whisper_words[start_idx].get("start", 0.0) or 0.0)
        end_t = float(whisper_words[end_idx - 1].get("end", 0.0) or 0.0)
        # 마지막 phrase는 마지막 단어 end를 반드시 포함
        if i == len(phrases) - 1:
            end_t = float(whisper_words[-1].get("end", 0.0) or 0.0)
        out.append({
            "id": i,
            "start": round(start_t, 3),
            "end": round(end_t, 3),
            "text": phr,
        })
    return out


# ==================== [C-1] Claude 키워드 추출 ====================
try:
    from anthropic import AsyncAnthropic  # type: ignore
    _ANTHROPIC_AVAILABLE = True
except Exception as _anthr_err:
    _ANTHROPIC_AVAILABLE = False
    _anthr_import_err = _anthr_err

# [I] Claw Code 호환: API_KEY 또는 AUTH_TOKEN 둘 다 지원, BASE_URL 로 프록시 가리키기
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
ANTHROPIC_AUTH_TOKEN = os.getenv("ANTHROPIC_AUTH_TOKEN", "")
ANTHROPIC_BASE_URL = os.getenv("ANTHROPIC_BASE_URL", "")  # 비우면 SDK 기본 (api.anthropic.com)
SEMANTIC_MATCH_ENABLED = os.getenv("SEMANTIC_MATCH_ENABLED", "false").lower() in ("true", "1", "yes")
SEMANTIC_MATCH_MODEL = os.getenv("SEMANTIC_MATCH_MODEL", "claude-haiku-4-5-20251001")
SEMANTIC_MATCH_LANG = os.getenv("SEMANTIC_MATCH_LANG", "en")  # 키워드 언어 (Pexels는 en 권장)

_anthropic_client = None

# ==================== [F] Gemini / [G] Claw Code 호환 ====================
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "gemini").lower().strip()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")
GEMINI_SCRIPT_MODEL = os.getenv("GEMINI_SCRIPT_MODEL", "gemini-2.0-flash")
GEMINI_API_BASE = os.getenv("GEMINI_API_BASE", "https://generativelanguage.googleapis.com/v1beta")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "")
XAI_API_KEY = os.getenv("XAI_API_KEY", "")
XAI_BASE_URL = os.getenv("XAI_BASE_URL", "https://api.x.ai/v1")
DASHSCOPE_API_KEY = os.getenv("DASHSCOPE_API_KEY", "")
DASHSCOPE_BASE_URL = os.getenv("DASHSCOPE_BASE_URL", "https://dashscope.aliyuncs.com/compatible-mode/v1")


async def _call_gemini(prompt: str, max_tokens: int = 2000, temperature: float = 0.7, model: str = None) -> str:
    if not GEMINI_API_KEY:
        raise HTTPException(status_code=503, detail="GEMINI_API_KEY 미설정")
    url = f"{GEMINI_API_BASE}/models/{model or GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"
    body = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "generationConfig": {"maxOutputTokens": max_tokens, "temperature": temperature},
    }
    async with httpx.AsyncClient(timeout=90.0) as client:
        r = await client.post(url, json=body)
    if r.status_code != 200:
        raise HTTPException(status_code=502, detail=f"Gemini {r.status_code}: {r.text[:300]}")
    cands = r.json().get("candidates") or []
    if not cands:
        raise HTTPException(status_code=502, detail="Gemini 응답 비어있음")
    parts = cands[0].get("content", {}).get("parts") or []
    return "".join(p.get("text", "") for p in parts).strip()


async def _call_openai_compat(prompt: str, max_tokens: int = 2000, temperature: float = 0.7,
                               model: str = None, base_url: str = None, api_key: str = None) -> str:
    url_base = (base_url or OPENAI_BASE_URL or "https://api.openai.com/v1").rstrip("/")
    url = f"{url_base}/chat/completions"
    key = api_key or OPENAI_API_KEY
    is_local = any(x in url_base for x in ("localhost", "127.0.0.1", "host.docker.internal"))
    if not key and not is_local:
        raise HTTPException(status_code=503, detail=f"API 키 미설정 (base_url={url_base})")
    headers = {"Content-Type": "application/json"}
    if key:
        headers["Authorization"] = f"Bearer {key}"
    body = {"model": model or "gpt-4o-mini",
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": max_tokens, "temperature": temperature}
    async with httpx.AsyncClient(timeout=90.0) as client:
        r = await client.post(url, json=body, headers=headers)
    if r.status_code != 200:
        raise HTTPException(status_code=502, detail=f"OpenAI-compat {r.status_code}: {r.text[:300]}")
    choices = r.json().get("choices") or []
    if not choices:
        raise HTTPException(status_code=502, detail="OpenAI-compat 응답 비어있음")
    return (choices[0].get("message", {}).get("content") or "").strip()


def _resolve_provider(model: str) -> str:
    m = (model or "").lower().strip()
    if not m:
        return LLM_PROVIDER
    if m.startswith(("claude", "anthropic/")):
        return "anthropic"
    if m.startswith(("grok", "xai/")):
        return "xai"
    if m.startswith(("qwen", "dashscope/")):
        return "dashscope"
    if m.startswith(("gemini", "google/")):
        return "gemini"
    if m.startswith(("gpt", "openai/", "openrouter/", "deepseek", "llama", "ollama/", "mistral", "mixtral", "phi")):
        return "openai_compat"
    return LLM_PROVIDER


async def _call_llm_unified(prompt: str, max_tokens: int = 2000, temperature: float = 0.7, model: str = None) -> str:
    chosen = model or GEMINI_MODEL
    provider = _resolve_provider(chosen)
    logger.info(f"[G] LLM 호출: model={chosen} provider={provider}")
    if provider == "anthropic":
        client = _get_anthropic_client()
        if client is None:
            raise HTTPException(status_code=503, detail="Anthropic 미설정")
        msg = await client.messages.create(
            model=chosen, max_tokens=max_tokens,
            messages=[{"role": "user", "content": prompt}],
        )
        return "".join(b.text for b in msg.content if getattr(b, "type", None) == "text").strip()
    if provider == "gemini":
        return await _call_gemini(prompt, max_tokens=max_tokens, temperature=temperature, model=chosen)
    if provider == "openai_compat":
        return await _call_openai_compat(prompt, max_tokens, temperature, model=chosen)
    if provider == "xai":
        return await _call_openai_compat(prompt, max_tokens, temperature,
                                          model=chosen, base_url=XAI_BASE_URL, api_key=XAI_API_KEY)
    if provider == "dashscope":
        return await _call_openai_compat(prompt, max_tokens, temperature,
                                          model=chosen, base_url=DASHSCOPE_BASE_URL, api_key=DASHSCOPE_API_KEY)
    raise HTTPException(status_code=400, detail=f"알 수 없는 provider={provider}")


# ==================== [H] Provider 병렬 Fallback ====================
# LLM_PROVIDERS="cerebras,groq,openrouter,gemini" 순서대로 시도, 429/실패 시 다음
LLM_PROVIDERS = [p.strip().lower() for p in os.getenv("LLM_PROVIDERS", "").split(",") if p.strip()]

# Provider 프리셋 — base_url / api_key env / 기본 모델
CEREBRAS_API_KEY = os.getenv("CEREBRAS_API_KEY", "")
CEREBRAS_BASE_URL = os.getenv("CEREBRAS_BASE_URL", "https://api.cerebras.ai/v1")
CEREBRAS_MODEL = os.getenv("CEREBRAS_MODEL", "llama3.1-8b")
CEREBRAS_SCRIPT_MODEL = os.getenv("CEREBRAS_SCRIPT_MODEL", "llama-3.3-70b")

GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_BASE_URL = os.getenv("GROQ_BASE_URL", "https://api.groq.com/openai/v1")
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")
GROQ_SCRIPT_MODEL = os.getenv("GROQ_SCRIPT_MODEL", "llama-3.3-70b-versatile")

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")
OPENROUTER_BASE_URL = os.getenv("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1")
OPENROUTER_MODEL = os.getenv("OPENROUTER_MODEL", "deepseek/deepseek-chat")
OPENROUTER_SCRIPT_MODEL = os.getenv("OPENROUTER_SCRIPT_MODEL", "deepseek/deepseek-chat")


async def _call_provider_preset(name: str, prompt: str, max_tokens: int, temperature: float, is_script: bool) -> str:
    """이름(cerebras/groq/openrouter/gemini/anthropic/ollama)로 provider 호출"""
    n = name.lower().strip()
    if n == "cerebras":
        if not CEREBRAS_API_KEY:
            raise HTTPException(status_code=503, detail="CEREBRAS_API_KEY 미설정")
        model = CEREBRAS_SCRIPT_MODEL if is_script else CEREBRAS_MODEL
        return await _call_openai_compat(prompt, max_tokens, temperature,
                                          model=model, base_url=CEREBRAS_BASE_URL, api_key=CEREBRAS_API_KEY)
    if n == "groq":
        if not GROQ_API_KEY:
            raise HTTPException(status_code=503, detail="GROQ_API_KEY 미설정")
        model = GROQ_SCRIPT_MODEL if is_script else GROQ_MODEL
        return await _call_openai_compat(prompt, max_tokens, temperature,
                                          model=model, base_url=GROQ_BASE_URL, api_key=GROQ_API_KEY)
    if n == "openrouter":
        if not OPENROUTER_API_KEY:
            raise HTTPException(status_code=503, detail="OPENROUTER_API_KEY 미설정")
        model = OPENROUTER_SCRIPT_MODEL if is_script else OPENROUTER_MODEL
        return await _call_openai_compat(prompt, max_tokens, temperature,
                                          model=model, base_url=OPENROUTER_BASE_URL, api_key=OPENROUTER_API_KEY)
    if n == "gemini":
        model = GEMINI_SCRIPT_MODEL if is_script else GEMINI_MODEL
        return await _call_gemini(prompt, max_tokens, temperature, model=model)
    if n == "anthropic":
        client = _get_anthropic_client()
        if client is None:
            raise HTTPException(status_code=503, detail="Anthropic 미설정")
        model = SCRIPT_GENERATE_MODEL if is_script else SEMANTIC_MATCH_MODEL
        msg = await client.messages.create(
            model=model, max_tokens=max_tokens,
            messages=[{"role": "user", "content": prompt}],
        )
        return "".join(b.text for b in msg.content if getattr(b, "type", None) == "text").strip()
    if n == "ollama":
        return await _call_openai_compat(prompt, max_tokens, temperature,
                                          model=os.getenv("OLLAMA_MODEL", "qwen2.5:7b"),
                                          base_url=os.getenv("OLLAMA_BASE_URL", "http://host.docker.internal:11434/v1"))
    raise HTTPException(status_code=400, detail=f"알 수 없는 provider 프리셋: {n}")


async def _call_llm_with_fallback(prompt: str, max_tokens: int = 2000, temperature: float = 0.7, is_script: bool = False) -> str:
    """
    LLM_PROVIDERS 순서대로 시도, 429/502/503/쿼터 실패 시 다음 provider 로 자동 전환.
    모든 실패 시 마지막 에러를 raise.
    """
    providers = LLM_PROVIDERS or [LLM_PROVIDER]
    errors = []
    for name in providers:
        try:
            logger.info(f"[H] fallback 시도: {name} (script={is_script})")
            text = await _call_provider_preset(name, prompt, max_tokens, temperature, is_script)
            logger.info(f"[H] 성공: {name} ({len(text)}자)")
            return text
        except HTTPException as e:
            # quota/rate/auth → 다음 provider, 그 외는 즉시 raise
            status = e.status_code
            detail_lower = (str(e.detail) or "").lower()
            retriable = (
                status in (429, 402, 503, 502)
                or "quota" in detail_lower or "rate" in detail_lower
                or "credit" in detail_lower or "미설정" in str(e.detail)
            )
            errors.append(f"{name}: {status} {str(e.detail)[:80]}")
            if retriable and name != providers[-1]:
                logger.warning(f"[H] {name} 실패({status}) → 다음 provider 로")
                continue
            if name == providers[-1]:
                raise HTTPException(status_code=502, detail=f"모든 provider 실패: {' | '.join(errors)}")
            raise
        except Exception as e:
            errors.append(f"{name}: {type(e).__name__}: {str(e)[:80]}")
            if name == providers[-1]:
                raise HTTPException(status_code=502, detail=f"모든 provider 실패: {' | '.join(errors)}")
            logger.warning(f"[H] {name} 예외 → 다음 provider")
            continue
    raise HTTPException(status_code=502, detail=f"providers 빈 리스트: {errors}")


def _get_anthropic_client():
    """Anthropic 또는 Anthropic 호환 프록시(Claw Code 등) 클라이언트 lazy init."""
    global _anthropic_client
    if not _ANTHROPIC_AVAILABLE:
        return None
    # API_KEY 우선, 없으면 AUTH_TOKEN
    auth = ANTHROPIC_API_KEY or ANTHROPIC_AUTH_TOKEN
    if not auth:
        return None
    if _anthropic_client is None:
        kwargs = {"api_key": auth}
        if ANTHROPIC_BASE_URL:
            # Claw Code / 자체 프록시 가리키기 (예: http://host.docker.internal:8080)
            kwargs["base_url"] = ANTHROPIC_BASE_URL
            logger.info(f"[I] Anthropic client → base_url={ANTHROPIC_BASE_URL}")
        _anthropic_client = AsyncAnthropic(**kwargs)
    return _anthropic_client


async def extract_keywords_from_segments(segments: list, lang: str = None) -> Optional[list]:
    """
    [C-1] 각 segment 텍스트에서 영상 검색용 시각 명사 키워드 추출.

    Returns: [{"idx": N, "keywords": ["keyword1", "keyword2"]}, ...]
             None (실패 또는 비활성)
    """
    if not segments:
        return None
    if not SEMANTIC_MATCH_ENABLED:
        logger.info("SEMANTIC_MATCH_ENABLED=false — 키워드 추출 스킵")
        return None

    client = _get_anthropic_client()
    if client is None:
        logger.warning("Anthropic 미설정 — 키워드 추출 스킵")
        return None

    kw_lang = lang or SEMANTIC_MATCH_LANG
    numbered = "\n".join(
        f"{i + 1}. {(s.get('text') or '').strip()}"
        for i, s in enumerate(segments)
    )
    lang_instruction = (
        "Output keywords in English (for stock video search like Pexels/Pixabay)."
        if kw_lang == "en" else
        "Output keywords in the same language as the input."
    )

    prompt = f"""다음 각 문장에서 영상 검색에 쓸 시각적 명사 키워드 2개를 추출해줘.

{SELF_CHECK_RULES}

조건:
- 구체적이고 시각화 가능한 단어 (추상 개념·조사·동사 제외)
- 한 키워드는 3~5 단어 영어 구문 (Pexels 최적화)
- 반드시 "장비·사람·실제 장면" 명사 포함 (engineer, laboratory, factory 등)
- {lang_instruction}
- 순수 JSON 배열만 반환. 다른 설명·마크다운 블록 절대 없이.

문장 목록:
{numbered}

출력 형식:
[
  {{"idx": 1, "keywords": ["keyword_a", "keyword_b"]}},
  {{"idx": 2, "keywords": ["keyword_a", "keyword_b"]}}
]"""

    try:
        # [H] fallback 체인 우선, 없으면 단일 provider
        if LLM_PROVIDERS:
            raw = await _call_llm_with_fallback(prompt, max_tokens=1500, temperature=0.3, is_script=False)
        else:
            raw = await _call_llm_unified(prompt, max_tokens=1500, temperature=0.3, model=GEMINI_MODEL)

        # ```json 블록 제거
        if raw.startswith("```"):
            raw = raw.split("```", 2)[1]
            if raw.startswith("json"):
                raw = raw[4:]
            raw = raw.rsplit("```", 1)[0].strip()

        import json as _json
        parsed = _json.loads(raw)
        if not isinstance(parsed, list):
            logger.warning(f"키워드 응답 형식 이상: {raw[:200]}")
            return None
        logger.info(
            f"[C-1] 키워드 추출 완료: {len(parsed)}개 segment "
            f"(model={SEMANTIC_MATCH_MODEL}, lang={kw_lang})"
        )
        return parsed

    except Exception as e:
        logger.error(f"[C-1] 키워드 추출 실패: {e}", exc_info=True)
        return None


def align_original_text_to_segments(original_text: str, segments: list, whisper_words: list = None) -> list:
    """
    [A]+[Q1] 원본 텍스트를 Whisper segment/word 타임코드에 재매핑.

    확장 전략:
    1. 원본을 문장 → 문장을 phrase로 추가 분할 (쉼표·접속사 기준)
    2. whisper_words 가 있으면 word-level 타임코드로 정밀 매핑
    3. 없으면 문장 수 기준 segment와 매칭 후 문자 비율 재분할
    """
    import re as _re
    if not segments or not original_text:
        return segments

    # 원본 문장 분할 → 각 문장 내 phrase 추가 분할 (Q1)
    sentences = [
        s.strip()
        for s in _re.split(r"(?<=[.!?。！？])\s+", original_text.strip())
        if s.strip()
    ]
    if not sentences:
        sentences = [original_text.strip()]

    phrases = []
    for sent in sentences:
        for phr in split_into_phrases(sent):
            phrases.append(phr)
    if not phrases:
        phrases = [original_text.strip()]

    # word-level 매핑 시도 (가장 정밀)
    if whisper_words and len(whisper_words) >= len(phrases):
        mapped = _map_phrases_to_words(phrases, whisper_words)
        if mapped:
            logger.info(
                f"phrase→word 매핑: {len(sentences)}문장 → {len(phrases)}구 "
                f"(words={len(whisper_words)})"
            )
            return mapped

    # 없으면 segment 기준 문자 비율 재분할
    n_sent = len(phrases)
    n_seg = len(segments)

    if n_sent == 0 or n_seg == 0:
        return segments

    # 원본 Whisper 텍스트 백업
    def with_whisper_backup(seg, idx_in_sent=None, text_override=None):
        return {
            **seg,
            "text": text_override if text_override is not None else seg.get("text", ""),
            "whisper_text": seg.get("text", ""),
        }

    # Case 1: 1:1 매핑
    if n_sent == n_seg:
        return [
            with_whisper_backup(seg, i, phr)
            for i, (seg, phr) in enumerate(zip(segments, phrases))
        ]

    # Case 2: 원본 phrase가 더 많음 → segments 타임축을 문자 비율로 재분할
    if n_sent > n_seg:
        total_start = float(segments[0].get("start", 0.0) or 0.0)
        total_end = float(segments[-1].get("end", 0.0) or 0.0)
        total_dur = max(0.001, total_end - total_start)
        total_chars = sum(len(s) for s in phrases) or 1

        whisper_joined = " ".join((s.get("text") or "").strip() for s in segments)

        out = []
        cum = 0
        for i, phr in enumerate(phrases):
            s_start = total_start + total_dur * (cum / total_chars)
            cum += len(phr)
            s_end = total_start + total_dur * (cum / total_chars)
            if i == n_sent - 1:
                s_end = total_end
            out.append({
                "id": i,
                "start": round(s_start, 3),
                "end": round(s_end, 3),
                "text": phr,
                "whisper_text": whisper_joined,
            })
        return out

    # Case 3: segment가 더 많음 → 앞에서부터 매핑, 남는 segment들은 마지막 phrase에 병합
    out = []
    for i in range(n_sent - 1):
        seg = segments[i]
        out.append({
            **seg,
            "text": phrases[i],
            "whisper_text": seg.get("text", ""),
        })
    last_segs = segments[n_sent - 1:]
    merged_start = float(last_segs[0].get("start", 0.0) or 0.0)
    merged_end = float(last_segs[-1].get("end", 0.0) or 0.0)
    merged_whisper = " ".join((s.get("text") or "").strip() for s in last_segs)
    out.append({
        "id": n_sent - 1,
        "start": round(merged_start, 3),
        "end": round(merged_end, 3),
        "text": phrases[-1],
        "whisper_text": merged_whisper,
    })
    return out



# ==================== [D] 호흡·리듬 주입 ====================
# 스크립트 preprocess + 문장별 TTS + silence 삽입

# 호흡 파라미터 (환경변수 override)
SENTENCE_PAUSE_SEC   = float(os.getenv("SENTENCE_PAUSE_SEC", "0.35"))   # 마침표 뒤 기본 쉼
COMMA_PAUSE_SEC      = float(os.getenv("COMMA_PAUSE_SEC", "0.20"))      # 쉼표 뒤 쉼 (문장 분할 시)
MAX_SENTENCE_CHARS   = int(os.getenv("MAX_SENTENCE_CHARS", "30"))       # 이보다 긴 문장은 자동 분할
AUTO_BREATH_ENABLED  = os.getenv("AUTO_BREATH_ENABLED", "true").lower() in ("true", "1", "yes")

# Pause 마커 패턴: "(0.3초)", "(300ms)", "<pause=300ms>", "<break time=\"300ms\"/>"
import re as _breath_re
_PAUSE_PATTERNS = [
    (_breath_re.compile(r"\((\d+(?:\.\d+)?)\s*초\)"), lambda m: float(m.group(1))),
    (_breath_re.compile(r"\((\d+)\s*ms\)"), lambda m: int(m.group(1)) / 1000.0),
    (_breath_re.compile(r"<pause\s*=\s*(\d+)\s*ms\s*/?>", _breath_re.I), lambda m: int(m.group(1)) / 1000.0),
    (_breath_re.compile(r'<break\s+time\s*=\s*"?(\d+)\s*ms"?\s*/?>', _breath_re.I), lambda m: int(m.group(1)) / 1000.0),
]


def preprocess_script(text: str,
                      max_chars: int = None,
                      sent_pause: float = None,
                      comma_pause: float = None) -> list:
    """
    [D1+D2] 스크립트를 "읽기용" 세그먼트 리스트로 변환.

    반환: [{"text": "...", "pause_after": 0.35}, ...]
      - 각 chunk 는 단일 문장 (또는 쉼표로 쪼개진 단일 구)
      - pause_after : 해당 chunk 뒤에 삽입할 silence 초

    처리 순서:
    1. Pause 마커 추출 → 마커 위치는 placeholder 로 치환
    2. 마침표/물음표/느낌표로 문장 분할
    3. MAX_SENTENCE_CHARS 초과 문장은 쉼표/접속사 기준 추가 분할 (split_into_phrases 재사용)
    4. 각 chunk 뒤 기본 pause 부여; 원래 마커 위치엔 지정 pause
    """
    import re as _re
    max_chars = max_chars if max_chars is not None else MAX_SENTENCE_CHARS
    sent_pause = sent_pause if sent_pause is not None else SENTENCE_PAUSE_SEC
    comma_pause = comma_pause if comma_pause is not None else COMMA_PAUSE_SEC

    if not text or not text.strip():
        return []

    # 1) 마커 추출 → 텍스트에서 제거하고 위치 기억
    # 간단: 마커를 플레이스홀더 "\x00PAUSE:0.3\x00" 로 치환
    working = text
    for pat, to_sec in _PAUSE_PATTERNS:
        def _repl(m, conv=to_sec):
            try:
                sec = conv(m)
            except Exception:
                return ""
            return f"\x00PAUSE:{sec:.3f}\x00"
        working = pat.sub(_repl, working)

    # 2) 마침표·물음표·느낌표로 문장 분할
    sentences_raw = [s.strip() for s in _re.split(r"(?<=[.!?。！？])\s+", working.strip()) if s.strip()]
    if not sentences_raw:
        sentences_raw = [working.strip()]

    chunks = []
    for sent in sentences_raw:
        # 문장 안에 마커가 있으면 분리
        # "안녕하세요.\x00PAUSE:0.5\x00 다음 문장" 같은 형태 — 하지만 이미 문장 단위 split 후라 드묾
        parts = _re.split(r"(\x00PAUSE:\d+\.\d+\x00)", sent)
        for part in parts:
            if not part:
                continue
            m = _re.match(r"\x00PAUSE:(\d+\.\d+)\x00", part)
            if m:
                # 직전 chunk 의 pause_after 를 지정값으로 override
                try:
                    p_sec = float(m.group(1))
                except Exception:
                    p_sec = sent_pause
                if chunks:
                    chunks[-1]["pause_after"] = p_sec
                continue
            # 일반 텍스트 — 긴 문장은 split_into_phrases 로 추가 분할
            sub = part.strip()
            if not sub:
                continue
            if len(sub) > max_chars:
                subs = split_into_phrases(sub, max_chars)
            else:
                subs = [sub]
            # 각 sub chunk 에 쉼 부여
            for i, s2 in enumerate(subs):
                is_last_sub = (i == len(subs) - 1)
                # 마지막 sub 는 문장 쉼, 중간은 쉼표 쉼
                p = sent_pause if is_last_sub else comma_pause
                chunks.append({"text": s2, "pause_after": p})

    return chunks


async def _generate_silence_mp3(duration_sec: float, output_path: Path, sample_rate: int = 24000):
    """ffmpeg 로 지정 초 길이의 무음 mp3 생성"""
    import subprocess as _sp
    cmd = [
        "ffmpeg", "-hide_banner", "-nostats", "-y",
        "-f", "lavfi",
        "-i", f"anullsrc=r={sample_rate}:cl=mono",
        "-t", f"{max(0.05, duration_sec):.3f}",
        "-q:a", "9", "-acodec", "libmp3lame",
        str(output_path)
    ]
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, lambda: _sp.run(cmd, capture_output=True, timeout=15))


async def _concat_mp3s(parts: list, output_path: Path):
    """ffmpeg concat demuxer 로 여러 mp3 합치기"""
    import subprocess as _sp
    concat_list = output_path.parent / f"{output_path.stem}_concat.txt"
    concat_list.write_text(
        "\n".join(f"file '{p.as_posix()}'" for p in parts),
        encoding="utf-8"
    )
    cmd = [
        "ffmpeg", "-hide_banner", "-nostats", "-y",
        "-f", "concat", "-safe", "0",
        "-i", str(concat_list),
        "-c:a", "libmp3lame", "-b:a", "128k",
        str(output_path)
    ]
    loop = asyncio.get_event_loop()
    proc = await loop.run_in_executor(
        None,
        lambda: _sp.run(cmd, capture_output=True, text=True, timeout=60)
    )
    try:
        concat_list.unlink()
    except Exception:
        pass
    return proc.returncode == 0


async def call_edge_tts_segmented(
    chunks: list,
    voice: str = "ko-KR-SunHiNeural",
    rate: str = "-5%",
    pitch: str = "+0Hz",
    output_path: str = None
) -> bytes:
    """
    [D3] 문장별 Edge TTS + silence concat.
    각 chunk 를 개별 TTS → 뒤에 pause_after 만큼 silence mp3 삽입 → 전체 concat.
    """
    import edge_tts as edge
    import tempfile, os as _os

    if not chunks:
        return b""

    out_path = Path(output_path) if output_path else Path(tempfile.mktemp(suffix=".mp3"))
    work_dir = Path(tempfile.mkdtemp(prefix="lf_tts_seg_"))

    try:
        parts = []
        for i, chunk in enumerate(chunks):
            txt = chunk["text"].strip()
            if not txt:
                continue
            seg_path = work_dir / f"seg_{i:03d}.mp3"
            # [AG-2] retry 3x on transient NoAudioReceived / network errors
            last_err = None
            for _attempt in range(3):
                try:
                    comm = edge.Communicate(text=txt, voice=voice, rate=rate, pitch=pitch)
                    await comm.save(str(seg_path))
                    if seg_path.exists() and seg_path.stat().st_size > 500:
                        last_err = None
                        break
                    last_err = Exception(f"empty output chunk {i}")
                except Exception as _tts_err:
                    last_err = _tts_err
                    logger.warning(
                        f"[AG-2] Edge TTS 실패 (chunk {i}, attempt {_attempt+1}/3): {_tts_err}"
                    )
                    await asyncio.sleep(0.8 * (_attempt + 1))
            if last_err is not None:
                # After 3 retries still failing -> skip this chunk with silence to preserve overall timing
                logger.error(f"[AG-2] Edge TTS chunk {i} 3회 실패 — 무음으로 대체: {last_err}")
                approx_dur = max(0.3, len(txt) * 0.06)  # rough Korean char timing
                await _generate_silence_mp3(approx_dur, seg_path)
                if not seg_path.exists() or seg_path.stat().st_size < 100:
                    raise last_err  # [AG-2] MARKER v1
            parts.append(seg_path)

            # 문장 뒤 pause
            pause = float(chunk.get("pause_after", 0.0) or 0.0)
            if pause > 0.02:  # 20ms 이하는 무시
                sil_path = work_dir / f"sil_{i:03d}.mp3"
                await _generate_silence_mp3(pause, sil_path)
                if sil_path.exists():
                    parts.append(sil_path)

        if not parts:
            return b""

        # concat
        ok = await _concat_mp3s(parts, out_path)
        if not ok:
            # fallback: 단순 이어붙이기 실패 시 첫 파트만 반환
            logger.warning("silence concat 실패 — 첫 세그먼트만 반환")
            return parts[0].read_bytes()

        audio_bytes = out_path.read_bytes()
        logger.info(
            f"[D3] segmented TTS 완료: {len(chunks)}개 chunk, "
            f"{sum(1 for p in parts if 'sil_' in p.name)}개 silence, {len(audio_bytes)} bytes"
        )
        return audio_bytes

    finally:
        # 임시 파일 정리
        try:
            for p in work_dir.iterdir():
                try:
                    p.unlink()
                except Exception:
                    pass
            work_dir.rmdir()
        except Exception:
            pass


async def call_edge_tts(
    text: str,
    voice: str = "ko-KR-SunHiNeural",
    rate: str = "-5%",
    pitch: str = "+0Hz",
    output_path: str = None
) -> bytes:
    """Microsoft Edge TTS (무료, 네이티브 한국어 발음)"""
    import edge_tts as edge
    import tempfile, os

    tmp_path = output_path or tempfile.mktemp(suffix=".mp3")
    comm = edge.Communicate(text=text, voice=voice, rate=rate, pitch=pitch)
    await comm.save(tmp_path)
    
    with open(tmp_path, "rb") as f:
        audio_bytes = f.read()
    
    if not output_path:
        os.unlink(tmp_path)
    
    logger.info(f"Edge TTS 완료: {len(audio_bytes)} bytes, voice={voice}")
    return audio_bytes


# ==================== [K] Kokoro TTS ====================
# Kokoro: 82M param 경량 로컬 TTS
# KPipeline(lang_code='ko') → 한국어 지원
# voice 목록: af_heart, af_bella, am_adam, bf_emma, bm_george, af_nicole 등
KOKORO_VOICES = {
    "korean_female": "af_heart",   # warm female (default)
    "korean_male":   "am_adam",    # male
    "korean_female2": "af_bella",  # bella female
    "korean_female3": "af_nicole", # nicole female
}
_kokoro_pipeline = None
_kokoro_lock = asyncio.Lock()

try:
    from kokoro import KPipeline  # type: ignore
    _KOKORO_AVAILABLE = True
except ImportError:
    _KOKORO_AVAILABLE = False
    logger.warning("kokoro 미설치 — kokoro 엔진 비활성화")


async def _get_kokoro_pipeline():
    """Kokoro pipeline singleton lazy init.
    Kokoro v0.9+ LANG_CODES: a/b/e/f/h/i/p/j/z (no 'ko').
    Falls back gracefully — callers will use edge TTS instead.
    """
    global _kokoro_pipeline, _KOKORO_AVAILABLE
    if not _KOKORO_AVAILABLE:
        return None
    if _kokoro_pipeline is None:
        async with _kokoro_lock:
            if _kokoro_pipeline is None:
                loop = asyncio.get_event_loop()
                def _load():
                    # Try 'a' (American English phonemizer) for Korean text input.
                    # KPipeline with lang_code='ko' raises AssertionError on kokoro>=0.9.
                    try:
                        return KPipeline(lang_code='a', repo_id='hexgrad/Kokoro-82M')
                    except Exception as e:
                        logger.warning(f"Kokoro KPipeline 초기화 실패: {e} — 엔진 비활성화")
                        return None
                result = await loop.run_in_executor(None, _load)
                if result is None:
                    _KOKORO_AVAILABLE = False
                else:
                    _kokoro_pipeline = result
                    logger.info("Kokoro 파이프라인 초기화 완료 (lang=a)")
    return _kokoro_pipeline


async def call_kokoro_tts(
    text: str,
    voice: str = "af_heart",
    speed: float = 1.0,
    output_path: str = None,
) -> bytes:
    """
    Kokoro TTS — 로컬 82M 파라미터 모델
    Returns MP3 bytes (WAV → ffmpeg 변환)
    """
    import tempfile, subprocess, os as _os
    import soundfile as sf
    import numpy as np

    pipeline = await _get_kokoro_pipeline()
    if pipeline is None:
        raise HTTPException(status_code=503, detail="Kokoro TTS 엔진을 사용할 수 없습니다.")

    # voice preset 처리
    resolved_voice = KOKORO_VOICES.get(voice, voice)

    loop = asyncio.get_event_loop()
    def _synth():
        samples_list = []
        sample_rate = 24000
        for _, _, audio in pipeline(text, voice=resolved_voice, speed=speed, split_pattern=r'\n+'):
            if audio is not None and len(audio) > 0:
                samples_list.append(audio)
        if not samples_list:
            return np.zeros(100, dtype=np.float32), sample_rate
        combined = np.concatenate(samples_list)
        return combined, sample_rate

    audio_arr, sr = await loop.run_in_executor(None, _synth)

    # WAV 임시 저장 → ffmpeg → MP3
    wav_tmp = tempfile.mktemp(suffix=".wav")
    mp3_tmp = output_path or tempfile.mktemp(suffix=".mp3")
    try:
        await loop.run_in_executor(None, lambda: sf.write(wav_tmp, audio_arr, sr))
        cmd = [
            "ffmpeg", "-y", "-i", wav_tmp,
            "-ar", "44100", "-ab", "128k", "-f", "mp3", mp3_tmp
        ]
        result = subprocess.run(cmd, capture_output=True, timeout=60)
        if result.returncode != 0:
            raise RuntimeError(f"ffmpeg 변환 실패: {result.stderr.decode()[:200]}")
        with open(mp3_tmp, "rb") as f:
            audio_bytes = f.read()
    finally:
        for p in [wav_tmp]:
            try:
                _os.unlink(p)
            except Exception:
                pass
        if not output_path:
            try:
                _os.unlink(mp3_tmp)
            except Exception:
                pass

    logger.info(f"Kokoro TTS 완료: {len(audio_bytes)} bytes, voice={resolved_voice}")
    return audio_bytes


async def call_elevenlabs_tts(
    text: str,
    voice_id: str,
    model_id: str,
    stability: float,
    similarity_boost: float,
    output_format: str,
    style: float = 0.60,
    use_speaker_boost: bool = True,
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
            "similarity_boost": similarity_boost,
            "style": style,
            "use_speaker_boost": use_speaker_boost
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
        _engine = getattr(request, "engine", "edge")
        if _engine == "kokoro":
            # [K] Kokoro 로컬 TTS
            _kokoro_voice = getattr(request, "voice_preset", "korean_female") or "korean_female"
            audio_data = await call_kokoro_tts(
                text=request.text,
                voice=_kokoro_voice,
                speed=1.0,
            )
            alignment = None
        elif _engine == "edge":
            _do_preprocess = getattr(request, "preprocess", True) and AUTO_BREATH_ENABLED
            if _do_preprocess:
                chunks = preprocess_script(
                    request.text,
                    max_chars=getattr(request, "max_sentence_chars", None),
                    sent_pause=getattr(request, "sentence_pause", None),
                    comma_pause=getattr(request, "comma_pause", None),
                )
                logger.info(
                    f"[D1+D2] preprocess: {len(chunks)}개 chunk "
                    f"(sent_pause={SENTENCE_PAUSE_SEC}s, comma_pause={COMMA_PAUSE_SEC}s)"
                )
                # 마커 제거된 clean text (자막·원본복구에 사용)
                request._clean_text = " ".join((c.get("text") or "").strip() for c in chunks).strip()
                audio_data = await call_edge_tts_segmented(
                    chunks,
                    voice=getattr(request, "edge_voice", "ko-KR-SunHiNeural"),
                    rate=getattr(request, "edge_rate", "-5%"),
                )
                alignment = None
            else:
                audio_data = await call_edge_tts(
                    text=request.text,
                    voice=getattr(request, "edge_voice", "ko-KR-SunHiNeural"),
                    rate=getattr(request, "edge_rate", "-5%"),
                )
                alignment = None
        else:
            audio_data, alignment = await call_elevenlabs_tts(
                text=request.text,
                voice_id=voice_id,
                model_id=request.model_id,
                stability=request.stability,
                similarity_boost=request.similarity_boost,
                style=getattr(request, "style", 0.60),
                use_speaker_boost=getattr(request, "use_speaker_boost", True),
                output_format=request.output_format
            )

        # 오디오 파일 저장
        file_path.write_bytes(audio_data)
        logger.info(f"오디오 파일 저장: {file_path}")

        # 타임스탬프 JSON 저장
        # - ElevenLabs alignment가 있으면 우선 사용
        # - 없으면 Whisper로 단어 단위 타임코드 추출 (Edge TTS 등)
        timestamps_path: Optional[str] = None
        ts_file = OUTPUT_DIR / f"{filename}_timestamps.json"

        if alignment:
            ts_data = {
                "filename": filename,
                "audio_path": str(file_path),
                "text": request.text,
                "source": "elevenlabs",
                "alignment": alignment,
            }
            ts_file.write_text(json.dumps(ts_data, ensure_ascii=False, indent=2), encoding="utf-8")
            timestamps_path = str(ts_file)
            logger.info(f"타임스탬프 저장 (ElevenLabs): {ts_file}")
        else:
            whisper_ts = await extract_whisper_timestamps(file_path)
            if whisper_ts is not None:
                # [A] 원본 텍스트로 교체 (오인식 방지)
                raw_segments = whisper_ts.get("segments", [])
                # preprocess 되었다면 clean text (마커 제거) 우선
                _src_text = getattr(request, "_clean_text", None) or request.text
                aligned_segments = align_original_text_to_segments(
                    _src_text, raw_segments, whisper_ts.get("words", [])
                )
                whisper_ts_corrected = {
                    **whisper_ts,
                    "segments": aligned_segments,
                    "segments_whisper_raw": raw_segments,
                    "text_correction_applied": True,
                }

                # [C-1] 각 segment 에서 키워드 추출 (SEMANTIC_MATCH_ENABLED=true 일 때)
                segment_keywords = await extract_keywords_from_segments(aligned_segments)
                if segment_keywords:
                    whisper_ts_corrected["segment_keywords"] = segment_keywords
                logger.info(
                    f"원본 텍스트 재매핑: {len(raw_segments)} Whisper seg "
                    f"→ {len(aligned_segments)} aligned seg"
                )

                ts_data = {
                    "filename": filename,
                    "audio_path": str(file_path),
                    "text": getattr(request, "_clean_text", None) or request.text,
                    "text_original": request.text,
                    **whisper_ts_corrected,
                }
                ts_file.write_text(json.dumps(ts_data, ensure_ascii=False, indent=2), encoding="utf-8")
                timestamps_path = str(ts_file)
                logger.info(f"타임스탬프 저장 (Whisper+원본복구): {ts_file}")

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
                    style=getattr(tts_request, 'style', 0.60),
                    use_speaker_boost=getattr(tts_request, 'use_speaker_boost', True),
                    output_format=tts_request.output_format
                )

                # 파일 저장
                file_path.write_bytes(audio_data)

                # 타임스탬프 JSON 저장 (ElevenLabs alignment 우선, 없으면 Whisper)
                timestamps_path: Optional[str] = None
                ts_file = OUTPUT_DIR / f"{filename}_timestamps.json"
                if alignment:
                    ts_data = {
                        "filename": filename,
                        "audio_path": str(file_path),
                        "text": tts_request.text,
                        "source": "elevenlabs",
                        "alignment": alignment,
                    }
                    ts_file.write_text(
                        json.dumps(ts_data, ensure_ascii=False, indent=2),
                        encoding="utf-8"
                    )
                    timestamps_path = str(ts_file)
                else:
                    whisper_ts = await extract_whisper_timestamps(file_path)
                    if whisper_ts is not None:
                        ts_data = {
                            "filename": filename,
                            "audio_path": str(file_path),
                            "text": tts_request.text,
                            **whisper_ts,
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


# ==================== 스크립트 생성·파일 파싱 ====================

SCRIPT_GENERATE_MODEL = os.getenv("SCRIPT_GENERATE_MODEL", "claude-sonnet-4-6")

class ScriptGenerateRequest(BaseModel):
    topic: str = Field(..., min_length=1, max_length=500, description="주제 또는 제목")
    style: str = Field(default="info", description="info | story | explain | news | ad")
    duration_target_sec: int = Field(default=60, ge=15, le=3000, description="목표 길이(초)")
    tone: Optional[str] = Field(default=None, description="톤 힌트 (친근/전문/감성 등)")
    language: str = Field(default="ko", description="ko | en")


class ScriptGenerateResponse(BaseModel):
    success: bool
    script: str
    model: str
    topic: str
    style: str
    characters: int


STYLE_HINTS = {
    "info":    "정보성·튜토리얼 톤. 또박또박, 논리적 순서로. 숫자 예시 활용.",
    "story":   "내러티브·감정 리듬. 시간순·사건 중심. 시작은 훅으로.",
    "explain": "기술/개념 설명. 비유→핵심→결론. 단정적 문장.",
    "news":    "뉴스·속보 톤. 육하원칙. 짧은 문장 연속.",
    "ad":      "광고·마케팅. 후킹 → 문제 → 해결 → CTA. 짧고 강하게.",
}


def _build_script_prompt(req: ScriptGenerateRequest, extracted_text: str = "") -> str:
    style_hint = STYLE_HINTS.get(req.style, STYLE_HINTS["info"])
    tone_line = f"\n톤: {req.tone}" if req.tone else ""
    # 길이→글자 수 대략: ko 기준 3.5자/초, en 기준 2.2단어/초
    # Edge TTS ko-KR SunHi @ -5% -> ~3.2 chars/sec. Subtract ~10% for pauses.
    target_chars = int(req.duration_target_sec * 3.2 * 0.9) if req.language == "ko" else int(req.duration_target_sec * 2.2 * 5)

    header = ""
    if extracted_text:
        header = "\n\n원본 문서 내용:\n---\n" + extracted_text[:4000] + "\n---\n\n위 문서의 내용을 바탕으로"

    return f"""당신은 한국어 영상 나레이션 스크립트 작가입니다.

주제: {req.topic}{header}
장르: {req.style} — {style_hint}{tone_line}
목표 길이: 정확히 {req.duration_target_sec}초 분량
필수 글자수: 최소 {target_chars}자 이상 (미만이면 실패)
필수 문장수: 최소 {max(10, target_chars // 20)}개 이상

작성 규칙 (반드시 지킬 것):
1. 짧은 문장. 한 문장은 28자를 넘지 않는다. 긴 문장은 마침표로 끊는다.
2. Pause 마커를 적극 사용한다:
   - 문장 사이: (0.4초) 또는 (0.5초)
   - 강조 전: (0.3초)
   - 주제 전환: (0.6초)
   - 결론 직전: (0.8초)
3. 쉼표 대신 마침표. "매우 중요하며," → "매우 중요합니다."
4. 복합명사는 띄어쓰기 유지 ("진공 챔버", "열 시험", "우주 환경" 등).
5. 숫자·수치는 별도 문장으로. "3단계를 통과합니다. (0.4초) 하나, 둘, 셋."
6. 마크다운·제목·해설 없이 스크립트 본문만 출력한다.
7. 서두·맺음말 군더더기 금지. 바로 본론부터.

예시 스타일:
오늘은 위성 테스트에 대해 이야기합니다.
(0.5초)
위성은 우주 환경에서 작동해야 합니다.
(0.4초)
그래서 진공 챔버에서 검증합니다.

이제 위 주제에 대한 스크립트를 작성하세요. 스크립트 외 다른 텍스트 금지."""


async def _generate_script_with_claude(req: ScriptGenerateRequest, extracted_text: str = "") -> str:
    """스크립트 생성 — Claw Code 호환 라우팅. 이름은 레거시 유지."""
    prompt = _build_script_prompt(req, extracted_text)
    target_tokens = max(1000, min(16000, int(req.duration_target_sec * 40)))  # scale for 50-min scripts
    chosen_model = SCRIPT_GENERATE_MODEL if LLM_PROVIDER == "anthropic" else GEMINI_SCRIPT_MODEL
    try:
        # [H] LLM_PROVIDERS 있으면 fallback, 없으면 단일 provider
        if LLM_PROVIDERS:
            text = await _call_llm_with_fallback(prompt, max_tokens=target_tokens,
                                                  temperature=0.85, is_script=True)
        else:
            text = await _call_llm_unified(prompt, max_tokens=target_tokens,
                                            temperature=0.85, model=chosen_model)
        # ```xxx``` 블록 있으면 제거
        if text.startswith("```"):
            text = text.split("```", 2)[1]
            if text.startswith(("json", "text", "markdown")):
                text = text.split("\n", 1)[1] if "\n" in text else text
            text = text.rsplit("```", 1)[0].strip()
        return text
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"스크립트 생성 실패: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"스크립트 생성 실패: {str(e)[:200]}")


def _extract_text_from_file(filename: str, content: bytes) -> str:
    """업로드 파일에서 텍스트 추출. PDF / DOCX / TXT / MD 지원."""
    ext = filename.lower().rsplit(".", 1)[-1] if "." in filename else ""
    if ext in ("txt", "md"):
        # 인코딩 추측
        for enc in ("utf-8", "utf-8-sig", "cp949", "euc-kr"):
            try:
                return content.decode(enc)
            except UnicodeDecodeError:
                continue
        return content.decode("utf-8", errors="replace")

    if ext == "pdf":
        try:
            import pypdf  # type: ignore
            import io
            reader = pypdf.PdfReader(io.BytesIO(content))
            pages = [p.extract_text() or "" for p in reader.pages]
            return "\n".join(pages)
        except ImportError:
            raise HTTPException(status_code=501, detail="pypdf 미설치 — PDF 처리 불가")
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"PDF 파싱 오류: {e}")

    if ext == "docx":
        try:
            import docx  # type: ignore
            import io
            d = docx.Document(io.BytesIO(content))
            return "\n".join(p.text for p in d.paragraphs)
        except ImportError:
            raise HTTPException(status_code=501, detail="python-docx 미설치 — DOCX 처리 불가")
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"DOCX 파싱 오류: {e}")

    raise HTTPException(status_code=415, detail=f"지원하지 않는 형식: .{ext} (지원: txt, md, pdf, docx)")


@app.post("/script/generate", response_model=ScriptGenerateResponse)
async def script_generate(req: ScriptGenerateRequest):
    """주제/제목 기반 스크립트 생성"""
    logger.info(f"[E2] 스크립트 생성: topic='{req.topic[:40]}' style={req.style} dur={req.duration_target_sec}s")
    script = await _generate_script_with_claude(req)
    actual_model = GEMINI_SCRIPT_MODEL if LLM_PROVIDER != "anthropic" else SCRIPT_GENERATE_MODEL
    return ScriptGenerateResponse(
        success=True, script=script,
        model=f"{LLM_PROVIDER}/{actual_model}", topic=req.topic,
        style=req.style, characters=len(script),
    )


from fastapi import UploadFile, File, Form

@app.post("/script/from_file", response_model=ScriptGenerateResponse)
async def script_from_file(
    file: UploadFile = File(...),
    style: str = Form("info"),
    duration_target_sec: int = Form(60),  # max 3000
    tone: Optional[str] = Form(None),
    language: str = Form("ko"),
    topic_override: Optional[str] = Form(None),
):
    """업로드 파일(PDF/DOCX/TXT)에서 스크립트 생성"""
    max_mb = int(os.getenv("FILE_UPLOAD_MAX_MB", "50"))
    content = await file.read()
    if len(content) > max_mb * 1024 * 1024:
        raise HTTPException(status_code=413, detail=f"파일 크기 초과 ({max_mb}MB)")

    logger.info(f"[E3] 파일→스크립트: {file.filename} ({len(content)} bytes)")
    text = _extract_text_from_file(file.filename, content)
    if not text.strip():
        raise HTTPException(status_code=400, detail="문서에서 텍스트를 추출할 수 없음")

    topic = topic_override or (file.filename.rsplit(".", 1)[0] if "." in file.filename else file.filename)
    req = ScriptGenerateRequest(
        topic=topic, style=style,
        duration_target_sec=duration_target_sec,
        tone=tone, language=language,
    )
    script = await _generate_script_with_claude(req, extracted_text=text)
    actual_model = GEMINI_SCRIPT_MODEL if LLM_PROVIDER != "anthropic" else SCRIPT_GENERATE_MODEL
    return ScriptGenerateResponse(
        success=True, script=script,
        model=f"{LLM_PROVIDER}/{actual_model}", topic=topic,
        style=style, characters=len(script),
    )



# ==================== [T] YouTube 메타데이터 자동 생성 ====================

class MetadataRequest(BaseModel):
    script: str = Field(..., min_length=10, max_length=10000)
    style: str = Field(default="info")
    language: str = Field(default="ko")


class MetadataResponse(BaseModel):
    success: bool
    title: str
    description: str
    tags: List[str]
    cta: Optional[str] = None
    thumbnail_copy: Optional[str] = None
    model: str


METADATA_PROMPT_TEMPLATE = """당신은 한국어 유튜브 영상 메타데이터 작성 전문가입니다.

다음 나레이션 스크립트를 바탕으로 유튜브 업로드용 메타데이터를 작성하세요.

스크립트:
---
{script}
---

장르: {style}

반드시 지킬 것:
1. 제목(title): 40자 이내, 클릭 유도형, 숫자나 호기심 자극
2. 설명(description): 150~250자, 본문 요약 + 해시태그 5개 (맨 끝에)
3. 태그(tags): 정확히 10개의 관련 키워드 (짧게)
4. CTA(cta): "구독/알림/댓글" 유도 한 줄 (선택)
5. 썸네일 카피(thumbnail_copy): 큰 글자용 5~10자 짧은 헤드라인
6. 순수 JSON 만 반환. ```json 블록 금지. 설명 문장 금지.

출력 형식 (엄격히 이 스키마):
{{
  "title": "...",
  "description": "...",
  "tags": ["...", "...", ...],
  "cta": "...",
  "thumbnail_copy": "..."
}}"""


@app.post("/metadata/generate", response_model=MetadataResponse)
async def metadata_generate(req: MetadataRequest):
    """스크립트 → YouTube 메타데이터 (제목·설명·태그·CTA·썸네일 카피)"""
    prompt = METADATA_PROMPT_TEMPLATE.format(script=req.script[:4000], style=req.style)
    logger.info(f"[T] 메타데이터 생성: {len(req.script)}자 script, style={req.style}")

    try:
        # LLM_PROVIDERS fallback 또는 단일 provider
        if LLM_PROVIDERS:
            raw = await _call_llm_with_fallback(prompt, max_tokens=1000, temperature=0.7, is_script=False)
        else:
            raw = await _call_llm_unified(prompt, max_tokens=1000, temperature=0.7, model=GEMINI_SCRIPT_MODEL)

        # ```json 블록 제거
        if raw.startswith("```"):
            raw = raw.split("```", 2)[1]
            if raw.startswith("json"):
                raw = raw[4:]
            raw = raw.rsplit("```", 1)[0].strip()

        import json as _json
        data = _json.loads(raw)

        return MetadataResponse(
            success=True,
            title=data.get("title", "")[:80],
            description=data.get("description", "")[:500],
            tags=data.get("tags", [])[:15],
            cta=data.get("cta"),
            thumbnail_copy=data.get("thumbnail_copy"),
            model=f"{LLM_PROVIDER}/{GEMINI_SCRIPT_MODEL}",
        )
    except _json.JSONDecodeError as e:
        logger.error(f"[T] JSON 파싱 실패: {raw[:300]}")
        raise HTTPException(status_code=502, detail=f"LLM 응답 JSON 파싱 실패: {e}")
    except Exception as e:
        logger.error(f"[T] 메타데이터 생성 실패: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e)[:200])



# ==================== [W] 전체 씬 자동 생성 ====================

class ScenesGenerateRequest(BaseModel):
    topic: Optional[str] = Field(default=None, description="주제/제목 (script 없을 때)")
    script: Optional[str] = Field(default=None, description="기존 스크립트 (있으면 씬만 생성)")
    style: str = Field(default="info")
    target_scenes: int = Field(default=7, ge=3, le=20)
    target_duration_sec: int = Field(default=60, ge=15, le=600)
    language: str = Field(default="ko")


class SceneItem(BaseModel):
    scene_id: str
    keyword: str              # Pexels 검색용 (영어)
    duration_seconds: float
    description: str          # 한국어 한 줄 묘사
    asset_type: str = "video"


class ScenesGenerateResponse(BaseModel):
    success: bool
    scenes: List[SceneItem]
    script: Optional[str] = None   # topic 주면 스크립트도 같이 생성 반환
    model: str



# ==================== [AB] LLM 공통 자기검증 규칙 ====================
SELF_CHECK_RULES = """
────── 자기검증 체크리스트 (keyword 하나마다 반드시 통과) ──────
1. 이 keyword 로 Pexels 검색하면 어떤 영상이 실제 나올까? (머릿속 상상)
2. 상상 결과가 스크립트 의미와 맞는가?
   - "비용/cost/가격" → 집·마을 나오면 실패. "money calculator chart dollar"
   - "큐브샛/cubesat" → 루빅스큐브·퍼즐 나오면 실패. "nanosatellite space technology"
   - "경제/economy" → 도시 야경 나오면 실패. "stock market chart trading screen"
   - "AI/인공지능" → 추상 그래픽 나오면 실패. "server data center hardware"
   - "진공/vacuum" → 진공청소기 나오면 실패. "vacuum chamber spacecraft testing"
   - "시험/test" → 학교 시험 나오면 실패. "aerospace testing laboratory equipment"
   - "양자/quantum" → 추상 파동 나오면 실패. "optical fiber laser laboratory"
3. 추상어 1단어 단독 금지. 반드시 3~6단어 영어 구문.
4. "장비·사람·실제 장면" 명사가 하나 이상 포함.
   예: "engineer", "laboratory", "chamber", "factory", "clean room", "office", "machine"
5. 실패 시 다시 써라. 통과 시 출력.
"""

SCENES_PROMPT = """당신은 한국어 유튜브 영상 시나리오 작가 + 영상 자료 큐레이터입니다.
Pexels/Pixabay 에서 실제로 "그 의미에 맞는 영상"이 나오도록 키워드를 설계하는 것이 핵심입니다.

{input_desc}

스타일: {style}
목표 씬 수: {n_scenes}개
목표 총 길이: 약 {dur}초

{self_check}

────────────────────────────────────────────
규칙 1) 씬 내용과 영상이 정확히 일치해야 함 (이게 가장 중요)
   스크립트 주제어 → 반드시 이런 구체 키워드로:
   ■ 설계·CAD           → "engineering blueprint CAD design aerospace"
   ■ 제조·조립           → "clean room spacecraft assembly engineer white suit"
   ■ 진공 시험          → "vacuum chamber thermal vacuum testing spacecraft"
   ■ 진동 시험          → "vibration testing shaker table aerospace"
   ■ 열 시험             → "thermal chamber testing temperature satellite"
   ■ 방사선 시험         → "radiation testing laboratory shielding aerospace"
   ■ EMC 시험           → "EMC testing anechoic chamber electronics"
   ■ 인증·검증           → "engineer inspecting spacecraft laboratory"
   ■ 발사                → "rocket launch orange flame space"
   ■ 궤도·우주          → "earth orbit satellite from space"
   ■ 위성 운용           → "satellite ground station control room monitors"
   ■ 지상국·관제         → "ground station control room monitors operators"

   비용·경제·금액 (절대 village/town 이 나오면 안 됨):
   ■ 비용·예산·가격     → "money calculator budget chart dollar"
   ■ 경제·시장          → "stock market trading chart screen business"
   ■ 투자·수익          → "money growth chart investor business professional"
   ■ 1조원·100만 달러  → "money stack cash finance bank"

   산업·비즈니스:
   ■ 우주 산업           → "rocket launch satellite factory manufacturing"
   ■ 산업·공장          → "factory manufacturing industrial machinery"
   ■ 스타트업·기업       → "office team laptop computer meeting professional"

규칙 2) 전문용어는 반드시 Pexels 가 이해하는 용어로 풀어쓸 것
   ❌ "cubesat"     → 장난감 큐브·루빅스큐브 나옴 (절대 금지)
   ✅ "nanosatellite small satellite space technology"
   ❌ "cube sat"    → 같은 문제
   ✅ "small satellite deploy space station"
   ❌ "cost"        → 동네·집·마을 영상 (절대 금지)
   ✅ "money calculator budget chart dollar"
   ❌ "quantum"     → 추상 그래픽
   ✅ "optical fiber laser laboratory"
   ❌ "AI"           → 랜덤 그래픽
   ✅ "server data center hardware" 또는 "robot arm manufacturing"
   ❌ "test" (단독) → 시험 실패·학교 시험
   ✅ "aerospace testing laboratory equipment engineer"

규칙 3) keyword 는 3~6 단어 영어 구문 (Pexels 검색 최적화)
   - 동사·형용사 포함 OK: "scientist examining chip microscope"
   - 복합 명사 허용: "rocket engine nozzle flame test"
   - 장난감·일러스트 피하려면 구체 장면 명시: "real laboratory", "engineer wearing lab coat", "industrial factory"

규칙 4) 씬마다 시각적으로 완전히 다른 장면
   - 같은 단어 2회 이상 반복 금지
   - 전체 씬 배치: 훅(임팩트) → 본론(다양한 각도) → 결론(미래·상징)

규칙 5) description 은 한국어 시각 묘사 ("~하는 장면")
   - 스크립트 해당 문장과 일치해야 함

규칙 6) duration_seconds 는 2.5~4.0 사이 랜덤, 총합이 {dur}초 ±15% 내

규칙 7) 순수 JSON 배열만 반환. ```json 블록·다른 설명 절대 금지.

────────────────────────────────────────────
예시 (주제: "위성이 우주에서 살아남는 법"):
[
  {{"scene_id":"s1","keyword":"rocket launch orange flame space","duration_seconds":3.0,"description":"로켓이 발사되는 장면"}},
  {{"scene_id":"s2","keyword":"engineering blueprint CAD design","duration_seconds":3.4,"description":"위성 설계도를 그리는 모습"}},
  {{"scene_id":"s3","keyword":"vacuum chamber laboratory testing","duration_seconds":2.8,"description":"진공 챔버에서 검증하는 과정"}},
  {{"scene_id":"s4","keyword":"nanosatellite deploy space station","duration_seconds":3.2,"description":"우주 정거장에서 소형 위성이 분리되는 장면"}},
  {{"scene_id":"s5","keyword":"earth orbit satellite from space","duration_seconds":3.6,"description":"지구 궤도를 도는 위성의 모습"}}
]
────────────────────────────────────────────"""


@app.post("/scenes/generate", response_model=ScenesGenerateResponse)
async def scenes_generate(req: ScenesGenerateRequest):
    """주제 or 스크립트 → scenes[] 자동 생성"""
    if not req.topic and not req.script:
        raise HTTPException(status_code=400, detail="topic 또는 script 중 하나는 필수")

    # topic 만 있으면 스크립트 먼저 생성
    generated_script = None
    if not req.script and req.topic:
        sg_req = ScriptGenerateRequest(
            topic=req.topic, style=req.style,
            duration_target_sec=req.target_duration_sec,
            language=req.language,
        )
        generated_script = await _generate_script_with_claude(sg_req)
        req.script = generated_script

    input_desc = f'스크립트:\n---\n{req.script[:3500]}\n---'
    prompt = SCENES_PROMPT.format(
        input_desc=input_desc,
        style=req.style,
        n_scenes=req.target_scenes,
        dur=req.target_duration_sec,
        self_check=SELF_CHECK_RULES,
    )

    logger.info(f"[W] 씬 자동 생성: script={len(req.script)}자 target={req.target_scenes}씬 / {req.target_duration_sec}s")

    try:
        if LLM_PROVIDERS:
            raw = await _call_llm_with_fallback(prompt, max_tokens=2000, temperature=0.7, is_script=False)
        else:
            raw = await _call_llm_unified(prompt, max_tokens=2000, temperature=0.7, model=GEMINI_SCRIPT_MODEL)

        if raw.startswith("```"):
            raw = raw.split("```", 2)[1]
            if raw.startswith("json"):
                raw = raw[4:]
            raw = raw.rsplit("```", 1)[0].strip()

        import json as _json
        import re as _re
        # [AQ-4] Extract JSON array from verbose LLM response (ignore preamble/postscript)
        data = None
        try:
            data = _json.loads(raw)
        except _json.JSONDecodeError:
            # Try finding the first [...] block
            m = _re.search(r"(\[[\s\S]*\])", raw)
            if m:
                try:
                    data = _json.loads(m.group(1))
                    logger.info(f"[W] JSON array 추출 성공 (raw prefix: {raw[:80]})")
                except _json.JSONDecodeError:
                    pass
            if data is None:
                # Try finding {"scenes": [...]} style
                m2 = _re.search(r"\{[\s\S]*\}", raw)
                if m2:
                    try:
                        wrap = _json.loads(m2.group(0))
                        if isinstance(wrap, dict) and "scenes" in wrap:
                            data = wrap["scenes"]
                            logger.info(f"[W] wrapped JSON 추출 성공")
                    except _json.JSONDecodeError:
                        pass
        if data is None:
            raise _json.JSONDecodeError("no JSON block found", raw, 0)
        # Accept {"scenes": [...]} dict shape
        if isinstance(data, dict) and "scenes" in data:
            data = data["scenes"]
        if not isinstance(data, list):
            raise HTTPException(status_code=502, detail=f"응답 형식 이상: {raw[:200]}")

        scenes = []
        for i, item in enumerate(data[:req.target_scenes * 2]):
            if not isinstance(item, dict):
                continue
            scenes.append(SceneItem(
                scene_id=item.get("scene_id") or f"s{i+1}",
                keyword=(item.get("keyword") or "").strip()[:50] or "technology",
                duration_seconds=max(1.5, min(8.0, float(item.get("duration_seconds", 3.0)))),
                description=(item.get("description") or "").strip()[:200],
                asset_type="video",
            ))

        if not scenes:
            raise HTTPException(status_code=502, detail="유효한 씬 생성 실패")

        logger.info(f"[W] 씬 {len(scenes)}개 생성 완료")
        return ScenesGenerateResponse(
            success=True, scenes=scenes,
            script=generated_script,
            model=f"{LLM_PROVIDER}/{GEMINI_SCRIPT_MODEL}",
        )
    except _json.JSONDecodeError as e:
        logger.error(f"[W] JSON 파싱 실패: {raw[:300]}")
        raise HTTPException(status_code=502, detail=f"JSON 파싱 실패: {e}")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[W] scenes 생성 실패: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e)[:200])


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=8001,
        reload=False,
        log_level="info"
    )
