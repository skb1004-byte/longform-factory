"""
LongForm Factory AI-MCP Service v3.0.0
병렬 멀티 프로바이더 + 자동 폴백 + 오프라인 AI(Ollama) 통합
"""

import os
import sys
import time
import json
import asyncio
import logging
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime
from enum import Enum
import traceback

from fastapi import FastAPI, HTTPException, Header, Depends, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings
import uvicorn

# AI 프로바이더 SDK
import anthropic
import google.generativeai as genai
from openai import OpenAI, AsyncOpenAI
import httpx
import requests

# ==================== 로깅 ====================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ==================== 설정 ====================

class Settings(BaseSettings):
    """환경변수 기반 설정"""
    # API 키
    anthropic_api_key: str = Field(default="", alias="ANTHROPIC_API_KEY")
    gemini_api_key: str = Field(default="", alias="GEMINI_API_KEY")
    openai_api_key: str = Field(default="", alias="OPENAI_API_KEY")
    cerebras_api_key: str = Field(default="", alias="CEREBRAS_API_KEY")
    openrouter_api_key: str = Field(default="", alias="OPENROUTER_API_KEY")
    arliai_api_key: str = Field(default="", alias="ARLIAI_API_KEY")
    lf_api_key: str = Field(default="", alias="LF_API_KEY")

    # 모델 지정
    anthropic_model: str = Field(default="claude-3-5-sonnet-20241022", alias="ANTHROPIC_MODEL")
    gemini_model: str = Field(default="gemini-1.5-pro", alias="GEMINI_MODEL")
    openai_model: str = Field(default="gpt-4o-mini", alias="OPENAI_MODEL")
    cerebras_model: str = Field(default="llama-3.3-70b", alias="CEREBRAS_MODEL")
    openrouter_model: str = Field(default="meta-llama/llama-3.1-8b-instruct:free", alias="OPENROUTER_MODEL")
    arliai_model: str = Field(default="Meta-Llama-3.1-70B-Instruct", alias="ARLIAI_MODEL")

    # Ollama (오프라인 AI)
    ollama_url: str = Field(default="http://host.docker.internal:11434", alias="OLLAMA_URL")
    ollama_model: str = Field(default="qwen2.5:7b", alias="OLLAMA_MODEL")
    ollama_model_fast: str = Field(default="qwen2.5:3b", alias="OLLAMA_MODEL_FAST")
    ollama_model_quality: str = Field(default="qwen2.5:14b", alias="OLLAMA_MODEL_QUALITY")

    # GPT4All (오프라인 AI - OpenAI 호환 서버)
    gpt4all_url: str = Field(default="http://host.docker.internal:4891", alias="GPT4ALL_URL")
    gpt4all_model: str = Field(default="Meta-Llama-3-8B-Instruct.Q4_0.gguf", alias="GPT4ALL_MODEL")

    # RALF 재시도 설정
    ralf_max_retries: int = Field(default=2, alias="RALF_MAX_RETRIES")
    ralf_base_delay: float = Field(default=0.5, alias="RALF_BASE_DELAY")

    # 병렬 실행 타임아웃
    parallel_timeout: float = Field(default=60.0, alias="PARALLEL_TIMEOUT")

    # 서비스 설정
    service_name: str = "lf_ai_mcp"
    service_version: str = "3.0.0"
    service_port: int = 8010

    class Config:
        env_file = ".env"
        case_sensitive = False


settings = Settings()


# ==================== Enum 정의 ====================

class ProviderEnum(str, Enum):
    """지원하는 AI 프로바이더"""
    CLAUDE = "claude"
    GEMINI = "gemini"
    OPENAI = "openai"
    CEREBRAS = "cerebras"
    OPENROUTER = "openrouter"
    ARLIAI = "arliai"
    OLLAMA = "ollama"      # 오프라인 AI (Ollama)
    GPT4ALL = "gpt4all"  # 오프라인 AI (GPT4All)
    AUTO = "auto"           # 자동 병렬 선택


class ContentTypeEnum(str, Enum):
    """콘텐츠 타입"""
    LONGFORM = "longform"
    SHORTFORM = "shortform"
    SCRIPT = "script"
    TITLE = "title"
    DESCRIPTION = "description"
    TAGS = "tags"


class StyleEnum(str, Enum):
    """영상 스타일"""
    EDUCATIONAL = "educational"
    ENTERTAINING = "entertaining"
    PROFESSIONAL = "professional"
    CASUAL = "casual"
    CINEMATIC = "cinematic"


# ==================== Pydantic 모델 ====================

class GenerateRequest(BaseModel):
    """일반 생성 요청"""
    prompt: str = Field(..., min_length=1, description="생성할 내용 프롬프트")
    provider: ProviderEnum = Field(default=ProviderEnum.AUTO, description="AI 프로바이더 (auto=병렬)")
    model: Optional[str] = Field(default=None, description="모델명 (기본값: 프로바이더 기본값)")
    max_tokens: int = Field(default=2000, ge=100, le=128000, description="최대 토큰")
    temperature: float = Field(default=0.7, ge=0.0, le=2.0, description="창의성 (0.0-2.0)")
    system_prompt: Optional[str] = Field(default=None, description="시스템 프롬프트")
    topic: Optional[str] = Field(default=None, description="주제")
    content_type: ContentTypeEnum = Field(default=ContentTypeEnum.LONGFORM, description="콘텐츠 타입")


class ScriptGenerateRequest(BaseModel):
    """동영상 스크립트 생성 요청"""
    topic: str = Field(..., min_length=1, description="주제")
    duration_seconds: int = Field(default=300, ge=30, le=3600, description="영상 길이 (초)")
    style: StyleEnum = Field(default=StyleEnum.EDUCATIONAL, description="영상 스타일")
    language: str = Field(default="ko", description="언어 코드 (ko/en)")
    target_audience: str = Field(default="general", description="타겟 오디언스")
    provider: ProviderEnum = Field(default=ProviderEnum.AUTO, description="AI 프로바이더 (auto=병렬)")
    temperature: float = Field(default=0.7, ge=0.0, le=2.0)
    # n8n 호환 필드
    target_duration: Optional[int] = Field(default=None, description="target_duration (n8n 호환)")
    format: Optional[str] = Field(default=None, description="출력 형식")


class TitleGenerateRequest(BaseModel):
    """영상 제목 생성 요청"""
    topic: str = Field(..., min_length=1, description="주제")
    style: StyleEnum = Field(default=StyleEnum.EDUCATIONAL)
    content_type: str = Field(default="video", description="콘텐츠 타입")
    language: str = Field(default="ko")
    provider: ProviderEnum = Field(default=ProviderEnum.AUTO)


class DescriptionGenerateRequest(BaseModel):
    """YouTube 설명 생성 요청"""
    title: str = Field(..., min_length=1)
    topic: str = Field(..., min_length=1)
    video_url: Optional[str] = Field(default=None)
    duration_seconds: Optional[int] = Field(default=None)
    language: str = Field(default="ko")
    include_hashtags: bool = Field(default=True)
    provider: ProviderEnum = Field(default=ProviderEnum.AUTO)


class TagsGenerateRequest(BaseModel):
    """태그 생성 요청"""
    topic: str = Field(..., min_length=1)
    content_type: str = Field(default="video")
    language: str = Field(default="ko")
    count: int = Field(default=15, ge=5, le=50)
    provider: ProviderEnum = Field(default=ProviderEnum.AUTO)


class GenerateResponse(BaseModel):
    """생성 응답"""
    success: bool
    content: str
    provider: str
    model: str
    tokens_used: Optional[int] = None
    generation_time: float = Field(..., description="생성 시간 (초)")
    error: Optional[str] = None
    # n8n 호환 추가 필드
    title: Optional[str] = None
    description: Optional[str] = None
    tags: Optional[List[str]] = None
    script_text: Optional[str] = None
    scenes: Optional[List[Dict]] = None
    shorts_title: Optional[str] = None


class ScriptSection(BaseModel):
    """스크립트 섹션"""
    type: str
    duration_seconds: int
    content: str


class ScriptGenerateResponse(BaseModel):
    """스크립트 생성 응답"""
    success: bool
    topic: str
    language: str
    total_duration_seconds: int
    sections: List[Dict[str, Any]] = []
    intro: str = ""
    main_points: List[str] = []
    outro: str = ""
    hooks: List[str] = []
    provider: str
    model: str
    generation_time: float
    error: Optional[str] = None
    # n8n 호환 추가 필드
    title: Optional[str] = None
    description: Optional[str] = None
    tags: Optional[List[str]] = None
    script_text: Optional[str] = None
    scenes: Optional[List[Dict]] = None
    shorts_title: Optional[str] = None


class TitlesResponse(BaseModel):
    """제목 생성 응답 (5개)"""
    success: bool
    titles: List[str] = Field(default_factory=list)
    provider: str
    model: str
    generation_time: float
    error: Optional[str] = None


class TagsResponse(BaseModel):
    """태그 생성 응답"""
    success: bool
    tags: List[str] = Field(default_factory=list)
    provider: str
    model: str
    generation_time: float
    error: Optional[str] = None


# ==================== AI 프로바이더 클래스 ====================

class AIProvider:
    """AI 프로바이더 기본 클래스"""

    def __init__(self, provider: ProviderEnum, model: str, api_key: str):
        self.provider = provider
        self.model = model
        self.api_key = api_key

    async def generate(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        max_tokens: int = 2000,
        temperature: float = 0.7,
    ) -> Dict[str, Any]:
        """텍스트 생성"""
        raise NotImplementedError


class ClaudeProvider(AIProvider):
    """Anthropic Claude 프로바이더"""

    def __init__(self, model: str, api_key: str):
        super().__init__(ProviderEnum.CLAUDE, model, api_key)
        self.client = anthropic.Anthropic(api_key=api_key)

    async def generate(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        max_tokens: int = 2000,
        temperature: float = 0.7,
    ) -> Dict[str, Any]:
        start_time = time.time()
        messages = [{"role": "user", "content": prompt}]
        kwargs = {
            "model": self.model,
            "max_tokens": max_tokens,
            "temperature": temperature,
            "messages": messages,
        }
        if system_prompt:
            kwargs["system"] = system_prompt

        def _call(): return self.client.messages.create(**kwargs)
        response = await asyncio.to_thread(_call)
        return {
            "success": True,
            "content": response.content[0].text,
            "tokens_used": response.usage.output_tokens,
            "generation_time": time.time() - start_time,
        }


class GeminiProvider(AIProvider):
    """Google Gemini 프로바이더"""

    def __init__(self, model: str, api_key: str):
        super().__init__(ProviderEnum.GEMINI, model, api_key)
        genai.configure(api_key=api_key)
        self._model_name = model
        self.model = model  # 문자열로 유지

    async def generate(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        max_tokens: int = 2000,
        temperature: float = 0.7,
    ) -> Dict[str, Any]:
        start_time = time.time()
        model_instance = genai.GenerativeModel(self._model_name)
        full_prompt = f"{system_prompt}\n\n{prompt}" if system_prompt else prompt
        def _call(): return model_instance.generate_content(
            full_prompt,
            generation_config=genai.types.GenerationConfig(
                max_output_tokens=max_tokens,
                temperature=temperature,
            ),
        )
        response = await asyncio.to_thread(_call)
        return {
            "success": True,
            "content": response.text,
            "tokens_used": None,
            "generation_time": time.time() - start_time,
        }


class OpenAICompatibleProvider(AIProvider):
    """OpenAI 호환 프로바이더 (OpenAI, Cerebras, OpenRouter, ArliAI)"""

    def __init__(self, provider: ProviderEnum, model: str, api_key: str, base_url: str):
        super().__init__(provider, model, api_key)
        self.base_url = base_url
        self.client = OpenAI(api_key=api_key, base_url=base_url)

    async def generate(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        max_tokens: int = 2000,
        temperature: float = 0.7,
    ) -> Dict[str, Any]:
        start_time = time.time()
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})

        def _call(): return self.client.chat.completions.create(
            model=self.model,
            messages=messages,
            max_tokens=max_tokens,
            temperature=temperature,
        )
        response = await asyncio.to_thread(_call)
        return {
            "success": True,
            "content": response.choices[0].message.content,
            "tokens_used": response.usage.completion_tokens if response.usage else None,
            "generation_time": time.time() - start_time,
        }


class OllamaProvider(AIProvider):
    """오프라인 Ollama 프로바이더 (qwen2.5, llama 등)"""

    def __init__(self, model: str, base_url: str):
        super().__init__(ProviderEnum.OLLAMA, model, "")
        self.base_url = base_url.rstrip("/")

    async def generate(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        max_tokens: int = 2000,
        temperature: float = 0.7,
    ) -> Dict[str, Any]:
        start_time = time.time()
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})

        async with httpx.AsyncClient(timeout=120.0) as client:
            response = await client.post(
                f"{self.base_url}/v1/chat/completions",
                json={
                    "model": self.model,
                    "messages": messages,
                    "max_tokens": max_tokens,
                    "temperature": temperature,
                    "stream": False,
                },
                headers={"Content-Type": "application/json"},
            )
            response.raise_for_status()
            data = response.json()

        content = data["choices"][0]["message"]["content"]
        tokens_used = data.get("usage", {}).get("completion_tokens")
        return {
            "success": True,
            "content": content,
            "tokens_used": tokens_used,
            "generation_time": time.time() - start_time,
        }


# ==================== 병렬 폴백 시스템 ====================

# 쿼터/크레딧 오류 감지 키워드
QUOTA_ERROR_PATTERNS = [
    "credit balance", "quota exceeded", "rate limit", "429",
    "insufficient_quota", "exceeded your current quota",
    "billing", "payment required", "402",
    "too many requests", "resource_exhausted",
    "daily limit", "monthly limit", "free tier",
    "RESOURCE_EXHAUSTED", "rateLimitExceeded",
]


def is_quota_error(error: Exception) -> bool:
    """쿼터/크레딧 오류 여부 판단"""
    err_str = str(error).lower()
    return any(p.lower() in err_str for p in QUOTA_ERROR_PATTERNS)


def is_auth_error(error: Exception) -> bool:
    """인증 오류 여부 판단"""
    err_str = str(error).lower()
    return any(p in err_str for p in ["401", "403", "unauthorized", "invalid api key", "authentication"])


def get_provider_instance(provider: ProviderEnum, model: Optional[str] = None) -> Optional[AIProvider]:
    """프로바이더 인스턴스 생성 (키 없으면 None 반환)"""
    try:
        if provider == ProviderEnum.CLAUDE:
            if not settings.anthropic_api_key:
                return None
            return ClaudeProvider(model or settings.anthropic_model, settings.anthropic_api_key)

        elif provider == ProviderEnum.GEMINI:
            if not settings.gemini_api_key:
                return None
            return GeminiProvider(model or settings.gemini_model, settings.gemini_api_key)

        elif provider == ProviderEnum.OPENAI:
            if not settings.openai_api_key:
                return None
            return OpenAICompatibleProvider(
                provider, model or settings.openai_model,
                settings.openai_api_key, "https://api.openai.com/v1"
            )

        elif provider == ProviderEnum.CEREBRAS:
            if not settings.cerebras_api_key:
                return None
            return OpenAICompatibleProvider(
                provider, model or settings.cerebras_model,
                settings.cerebras_api_key, "https://api.cerebras.ai/v1"
            )

        elif provider == ProviderEnum.OPENROUTER:
            if not settings.openrouter_api_key:
                return None
            return OpenAICompatibleProvider(
                provider, model or settings.openrouter_model,
                settings.openrouter_api_key, "https://openrouter.ai/api/v1"
            )

        elif provider == ProviderEnum.ARLIAI:
            if not settings.arliai_api_key:
                return None
            return OpenAICompatibleProvider(
                provider, model or settings.arliai_model,
                settings.arliai_api_key, "https://api.arliai.com/v1"
            )

        elif provider == ProviderEnum.OLLAMA:
            return OllamaProvider(model or settings.ollama_model, settings.ollama_url)

    except Exception as e:
        logger.warning(f"프로바이더 인스턴스 생성 실패 [{provider}]: {e}")
        return None

    return None


async def try_provider(
    provider_inst: AIProvider,
    prompt: str,
    system_prompt: Optional[str],
    max_tokens: int,
    temperature: float,
) -> Dict[str, Any]:
    """단일 프로바이더 시도 - 성공 시 결과 반환, 실패 시 예외"""
    logger.info(f"시도 중: {provider_inst.provider.value} / {provider_inst.model}")
    result = await provider_inst.generate(prompt, system_prompt, max_tokens, temperature)
    result["provider_name"] = provider_inst.provider.value
    result["model_name"] = provider_inst.model
    logger.info(f"✅ 성공: {provider_inst.provider.value} ({result.get('generation_time', 0):.1f}초)")
    return result


async def parallel_generate(
    prompt: str,
    system_prompt: Optional[str],
    max_tokens: int,
    temperature: float,
    requested_provider: ProviderEnum = ProviderEnum.AUTO,
    model: Optional[str] = None,
) -> Dict[str, Any]:
    """
    병렬 멀티 프로바이더 생성
    - AUTO: 온라인 전체 병렬 시도 → 첫 성공 사용 → 모두 실패 시 Ollama 폴백
    - 특정 provider: 해당 프로바이더 우선, 실패 시 자동 폴백
    """

    # 단일 프로바이더 요청이고 AUTO가 아닌 경우
    if requested_provider not in (ProviderEnum.AUTO,):
        primary = get_provider_instance(requested_provider, model)
        if primary:
            try:
                return await asyncio.wait_for(
                    try_provider(primary, prompt, system_prompt, max_tokens, temperature),
                    timeout=settings.parallel_timeout,
                )
            except Exception as e:
                logger.warning(f"⚠️ {requested_provider.value} 실패: {e}")
                if not is_quota_error(e) and not is_auth_error(e):
                    raise  # 쿼터/인증 오류가 아니면 폴백 없이 에러
                logger.info("쿼터/인증 오류 → 자동 폴백 시작")
        # 폴백으로 AUTO 모드 진행
        requested_provider = ProviderEnum.AUTO

    # ── 병렬 온라인 프로바이더 목록 ──
    ONLINE_PROVIDERS = [
        ProviderEnum.CLAUDE,
        ProviderEnum.GEMINI,
        ProviderEnum.OPENROUTER,
        ProviderEnum.CEREBRAS,
        ProviderEnum.ARLIAI,
        ProviderEnum.OPENAI,
    ]

    # 키가 있는 프로바이더만 필터링
    online_instances = []
    for p in ONLINE_PROVIDERS:
        inst = get_provider_instance(p, model)
        if inst:
            online_instances.append(inst)

    if not online_instances:
        logger.warning("온라인 프로바이더 없음 → Ollama 단독 사용")
        ollama = get_provider_instance(ProviderEnum.OLLAMA, model)
        if not ollama:
            raise RuntimeError("사용 가능한 AI 프로바이더가 없습니다")
        return await try_provider(ollama, prompt, system_prompt, max_tokens, temperature)

    logger.info(f"🚀 병렬 실행: {[i.provider.value for i in online_instances]}")

    # ── asyncio.gather로 모두 동시 실행 ──
    tasks = [
        asyncio.create_task(
            asyncio.wait_for(
                try_provider(inst, prompt, system_prompt, max_tokens, temperature),
                timeout=settings.parallel_timeout,
            )
        )
        for inst in online_instances
    ]

    errors = []
    # as_completed 패턴: 가장 먼저 성공한 것을 사용
    pending = set(tasks)
    while pending:
        done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            exc = task.exception()
            if exc is None:
                result = task.result()
                # 나머지 태스크 취소
                for t in pending:
                    t.cancel()
                logger.info(f"🏆 채택: {result.get('provider_name')} ({result.get('generation_time', 0):.1f}초)")
                return result
            else:
                errors.append(str(exc))
                logger.warning(f"❌ 프로바이더 실패: {exc}")

    # ── 모든 온라인 프로바이더 실패 → Ollama 폴백 ──
    # GPT4All 폴백 시도
    gpt4all = get_provider_instance(ProviderEnum.GPT4ALL)
    if gpt4all:
        try:
            result = await asyncio.wait_for(
                try_provider(gpt4all, prompt, system_prompt, max_tokens, temperature),
                timeout=120.0,
            )
            logger.info("✅ GPT4All 폴백 성공")
            return result
        except Exception as e:
            errors.append(f"GPT4All: {str(e)}")
            logger.warning(f"⚠️ GPT4All 실패: {e}")

    logger.warning(f"모든 온라인 프로바이더 실패 ({len(errors)}개). Ollama 폴백 시도...")
    ollama = get_provider_instance(ProviderEnum.OLLAMA, settings.ollama_model)
    if ollama:
        try:
            result = await asyncio.wait_for(
                try_provider(ollama, prompt, system_prompt, max_tokens, temperature),
                timeout=180.0,  # Ollama는 더 느릴 수 있음
            )
            logger.info("✅ Ollama 폴백 성공")
            return result
        except Exception as e:
            errors.append(f"Ollama: {str(e)}")
            logger.error(f"❌ Ollama도 실패: {e}")

    raise RuntimeError(
        f"모든 AI 프로바이더 실패. 오류 목록:\n" + "\n".join(errors[:5])
    )


# ==================== 헬퍼 함수 ====================

def verify_api_key(api_key: str = Header(None, alias="X-LF-API-Key")):
    """LF_API_KEY 인증"""
    if not settings.lf_api_key:
        logger.warning("LF_API_KEY 미설정 - 인증 스킵")
        return True
    if api_key != settings.lf_api_key:
        raise HTTPException(status_code=403, detail="Invalid API key")
    return True


# get_provider 호환성 유지 (기존 코드 참조용)
def get_provider(provider: ProviderEnum, model: Optional[str] = None) -> AIProvider:
    inst = get_provider_instance(provider, model)
    if not inst:
        raise ValueError(f"프로바이더 인스턴스 생성 실패: {provider}")
    return inst


def build_korean_prompt(
    content_type: ContentTypeEnum,
    topic: str,
    additional_context: Optional[str] = None,
) -> str:
    """한국 최적화 프롬프트 생성"""
    prompts = {
        ContentTypeEnum.LONGFORM: f"""다음 주제에 대해 상세한 장편 콘텐츠를 작성해주세요.
주제: {topic}

요구사항:
- 2000단어 이상의 상세한 내용
- 명확한 구조(서론, 본론, 결론)
- 한국어로 전문적이고 자연스러운 표현
- SEO 최적화된 키워드 포함
{f'추가 정보: {additional_context}' if additional_context else ''}""",

        ContentTypeEnum.SHORTFORM: f"""다음 주제에 대해 짧고 임팩트 있는 숏폼 콘텐츠를 작성해주세요.
주제: {topic}

요구사항:
- 300-500단어의 간결한 내용
- 훅 역할을 하는 오프닝
- 핵심만 담은 메인 메시지
- 명확한 CTA(Call-to-Action)
{f'추가 정보: {additional_context}' if additional_context else ''}""",

        ContentTypeEnum.SCRIPT: f"""다음 주제로 영상 스크립트를 작성해주세요.
주제: {topic}

요구사항:
- 자연스러운 스피킹 톤
- 카메라를 바라보듯 작성
- 시청자 집중도 유지
{f'추가 정보: {additional_context}' if additional_context else ''}""",

        ContentTypeEnum.TITLE: f"""다음 주제의 영상을 위해 클릭유도력 높은 제목을 작성해주세요.
주제: {topic}

요구사항:
- 40자 이내의 간결한 표현
- 호기심 자극 또는 가치 제시
- 한글과 숫자/기호 활용
{f'추가 정보: {additional_context}' if additional_context else ''}""",

        ContentTypeEnum.DESCRIPTION: f"""다음 정보로 YouTube 설명글을 작성해주세요.
주제: {topic}

요구사항:
- 첫 3줄에 핵심 요약
- #해시태그 포함
- 시청자 참여 유도 문구
{f'추가 정보: {additional_context}' if additional_context else ''}""",

        ContentTypeEnum.TAGS: f"""다음 주제의 영상을 위해 검색 최적화 태그를 생성해주세요.
주제: {topic}

요구사항:
- 2-3단어 조합의 태그
- 검색 트렌드 반영
{f'추가 정보: {additional_context}' if additional_context else ''}""",
    }
    return prompts.get(content_type, prompts[ContentTypeEnum.LONGFORM])


def build_script_prompt(
    topic: str,
    duration_seconds: int,
    style: StyleEnum,
    language: str,
    target_audience: str,
) -> Tuple[str, str]:
    """스크립트 생성 프롬프트 + 시스템 프롬프트 반환"""
    lang_name = "한국어" if language == "ko" else "English"
    script_prompt = f"""다음 요구사항으로 {duration_seconds}초 길이의 영상 스크립트를 작성해주세요.

주제: {topic}
스타일: {style.value}
대상 시청자: {target_audience}
언어: {lang_name}

출력 형식 (반드시 JSON으로만):
{{
    "title": "영상 제목 (40자 이내)",
    "intro": "오프닝 (10-15초 분량)",
    "main_points": [
        "메인 포인트 1 (약 {duration_seconds // 3}초)",
        "메인 포인트 2 (약 {duration_seconds // 3}초)",
        "메인 포인트 3 (약 {duration_seconds // 3}초)"
    ],
    "outro": "마무리 (10-15초 분량, CTA 포함)",
    "hooks": [
        "시청자 주목 후킹 1",
        "시청자 주목 후킹 2"
    ],
    "description": "YouTube 설명글 (200자)",
    "tags": ["태그1", "태그2", "태그3", "태그4", "태그5"],
    "shorts_title": "Shorts용 짧은 제목 (20자 이내)",
    "scenes": [
        {{"scene_id": 1, "duration": 10, "description": "장면 설명", "visual": "시각적 요소"}}
    ]
}}

모든 내용은 자연스러운 스피킹 톤으로 작성해주세요."""

    system_prompt = f"당신은 프로 영상 시나리오 작가입니다. {style.value} 스타일로 정확히 {duration_seconds}초 길이의 스크립트를 작성합니다. 반드시 JSON 형식으로만 응답하세요."
    return script_prompt, system_prompt


# ==================== FastAPI 앱 ====================

app = FastAPI(
    title=f"LongForm Factory AI-MCP v3",
    version=settings.service_version,
    description="병렬 멀티 프로바이더 AI Content Generation Service (Claude/Gemini/OpenRouter/Cerebras/ArliAI/OpenAI/Ollama)"
)


@app.get("/health", tags=["system"])
async def health_check():
    """헬스 체크"""
    # 사용 가능한 프로바이더 목록
    available = []
    for p in [ProviderEnum.CLAUDE, ProviderEnum.GEMINI, ProviderEnum.OPENAI,
              ProviderEnum.CEREBRAS, ProviderEnum.OPENROUTER, ProviderEnum.ARLIAI,
              ProviderEnum.OLLAMA, ProviderEnum.GPT4ALL]:
        inst = get_provider_instance(p)
        if inst:
            available.append(p.value)

    return {
        "status": "healthy",
        "service": settings.service_name,
        "version": settings.service_version,
        "timestamp": datetime.utcnow().isoformat(),
        "available_providers": available,
        "offline_ai": {
            "ollama_url": settings.ollama_url,
            "models": [settings.ollama_model_fast, settings.ollama_model, settings.ollama_model_quality],
        }
    }


@app.post("/generate", response_model=GenerateResponse, tags=["generation"])
async def generate_content(
    request: GenerateRequest,
    api_key: bool = Depends(verify_api_key),
) -> GenerateResponse:
    """
    일반 텍스트 생성 (병렬 멀티 프로바이더)

    - provider=auto (기본값): 모든 프로바이더 병렬 시도, 첫 성공 채택
    - provider=ollama: 오프라인 AI 단독 사용
    - 쿼터/크레딧 오류 시 자동으로 다음 프로바이더로 폴백
    """
    start_time = time.time()
    try:
        logger.info(f"생성 요청: provider={request.provider}, topic={request.topic}")

        system_prompt = request.system_prompt or build_korean_prompt(
            request.content_type, request.topic or request.prompt
        )

        result = await parallel_generate(
            prompt=request.prompt,
            system_prompt=system_prompt,
            max_tokens=request.max_tokens,
            temperature=request.temperature,
            requested_provider=request.provider,
            model=request.model,
        )

        return GenerateResponse(
            success=True,
            content=result["content"],
            provider=result.get("provider_name", "unknown"),
            model=result.get("model_name", "unknown"),
            tokens_used=result.get("tokens_used"),
            generation_time=time.time() - start_time,
        )

    except Exception as e:
        logger.error(f"생성 오류: {traceback.format_exc()}")
        return GenerateResponse(
            success=False,
            content="",
            provider=request.provider.value,
            model="unknown",
            generation_time=time.time() - start_time,
            error=str(e),
        )


@app.post("/generate/script", response_model=ScriptGenerateResponse, tags=["generation"])
async def generate_script(
    request: ScriptGenerateRequest,
    api_key: bool = Depends(verify_api_key),
) -> ScriptGenerateResponse:
    """
    영상 스크립트 생성 (병렬 멀티 프로바이더, n8n 호환)

    - topic: 주제
    - duration_seconds 또는 target_duration: 영상 길이
    - style: educational/entertaining/professional/casual/cinematic
    - provider=auto (기본값): 병렬 처리
    """
    start_time = time.time()

    # target_duration 호환 처리 (n8n 워크플로우)
    duration = request.duration_seconds
    if request.target_duration and request.target_duration > 0:
        duration = request.target_duration

    try:
        logger.info(f"스크립트 생성: topic={request.topic}, duration={duration}초, provider={request.provider}")

        script_prompt, system_prompt = build_script_prompt(
            request.topic, duration, request.style, request.language, request.target_audience
        )

        result = await parallel_generate(
            prompt=script_prompt,
            system_prompt=system_prompt,
            max_tokens=4000,
            temperature=request.temperature,
            requested_provider=request.provider,
        )

        # JSON 파싱
        try:
            content = result["content"]
            # ```json ... ``` 마크다운 제거
            if "```" in content:
                parts = content.split("```")
                for i, part in enumerate(parts):
                    if i % 2 == 1:  # 코드 블록 내부
                        cleaned = part.strip()
                        if cleaned.startswith("json"):
                            cleaned = cleaned[4:].strip()
                        try:
                            script_data = json.loads(cleaned)
                            break
                        except Exception:
                            continue
                else:
                    script_data = {}
            else:
                script_data = json.loads(content.strip())
        except json.JSONDecodeError:
            logger.warning("스크립트 JSON 파싱 실패 - 원본 반환")
            script_data = {"raw_content": result["content"]}

        # script_text 조합 (TTS용)
        def _extract_str(val):
            """dict이면 text/content/description에서 문자열 추출"""
            if isinstance(val, str):
                return val
            if isinstance(val, dict):
                for k in ("text", "content", "description", "narration", "script"):
                    if k in val and isinstance(val[k], str):
                        return val[k]
                return str(val)
            return str(val) if val is not None else ""

        intro = _extract_str(script_data.get("intro", ""))
        raw_mp = script_data.get("main_points", [])
        if isinstance(raw_mp, list):
            main_points = [_extract_str(p) for p in raw_mp]
        else:
            main_points = []
        outro = _extract_str(script_data.get("outro", ""))
        script_text = intro + "\n\n" + "\n\n".join(main_points) + "\n\n" + outro

        def _to_str_list(val):
            """리스트의 각 항목을 문자열로 변환"""
            if not isinstance(val, list):
                return []
            result_list = []
            for item in val:
                if isinstance(item, str):
                    result_list.append(item)
                elif isinstance(item, dict):
                    # dict에서 text/content/description/scene_id 중 문자열 추출
                    for k in ("text", "content", "description", "title", "hook"):
                        if k in item and isinstance(item[k], str):
                            result_list.append(item[k])
                            break
                    else:
                        result_list.append(str(item))
                else:
                    result_list.append(str(item))
            return result_list

        hooks_raw = script_data.get("hooks", [])
        tags_raw = script_data.get("tags", [])
        scenes_raw = script_data.get("scenes", [])

        return ScriptGenerateResponse(
            success=True,
            topic=request.topic,
            language=request.language,
            total_duration_seconds=duration,
            intro=intro,
            main_points=main_points,
            outro=outro,
            hooks=_to_str_list(hooks_raw),
            sections=scenes_raw if isinstance(scenes_raw, list) else [],
            provider=result.get("provider_name", "unknown"),
            model=result.get("model_name", "unknown"),
            generation_time=time.time() - start_time,
            # n8n 호환 추가 필드
            title=str(script_data.get("title", request.topic) or request.topic),
            description=str(script_data.get("description", "") or ""),
            tags=_to_str_list(tags_raw),
            script_text=script_text,
            scenes=scenes_raw if isinstance(scenes_raw, list) else [],
            shorts_title=str(script_data.get("shorts_title", "") or ""),
        )

    except Exception as e:
        logger.error(f"스크립트 생성 오류: {traceback.format_exc()}")
        return ScriptGenerateResponse(
            success=False,
            topic=request.topic,
            language=request.language,
            total_duration_seconds=duration,
            provider=request.provider.value,
            model="unknown",
            generation_time=time.time() - start_time,
            error=str(e),
        )


@app.post("/generate/title", response_model=TitlesResponse, tags=["generation"])
async def generate_titles(
    request: TitleGenerateRequest,
    api_key: bool = Depends(verify_api_key),
) -> TitlesResponse:
    """영상 제목 5개 생성 (병렬 멀티 프로바이더)"""
    start_time = time.time()
    try:
        title_prompt = f"""다음 주제의 영상을 위해 YouTube 클릭유도력이 높은 제목 5개를 생성해주세요.

주제: {request.topic}
스타일: {request.style.value}
언어: {"한국어" if request.language == "ko" else "English"}

각 제목은:
- 40자 이내
- 숫자, 기호 활용
- 호기심 자극 또는 가치 제시

형식: 각 제목을 줄 바꿈으로 구분해서 번호 없이 제시"""

        result = await parallel_generate(
            prompt=title_prompt,
            system_prompt="당신은 YouTube 콘텐츠 마케팅 전문가입니다. 클릭률 높은 제목을 작성합니다.",
            max_tokens=500,
            temperature=0.8,
            requested_provider=request.provider,
        )

        titles = [
            line.strip()
            for line in result["content"].split("\n")
            if line.strip() and not line.startswith(("#", "-", "*", "1.", "2.", "3.", "4.", "5."))
        ][:5]
        # 번호 포함된 경우도 처리
        if not titles:
            titles = [
                line.strip().lstrip("0123456789.） ").strip()
                for line in result["content"].split("\n")
                if line.strip()
            ][:5]

        return TitlesResponse(
            success=True,
            titles=titles,
            provider=result.get("provider_name", "unknown"),
            model=result.get("model_name", "unknown"),
            generation_time=time.time() - start_time,
        )

    except Exception as e:
        logger.error(f"제목 생성 오류: {traceback.format_exc()}")
        return TitlesResponse(
            success=False,
            titles=[],
            provider=request.provider.value,
            model="unknown",
            generation_time=time.time() - start_time,
            error=str(e),
        )


@app.post("/generate/description", response_model=GenerateResponse, tags=["generation"])
async def generate_description(
    request: DescriptionGenerateRequest,
    api_key: bool = Depends(verify_api_key),
) -> GenerateResponse:
    """YouTube 설명글 생성 (병렬 멀티 프로바이더)"""
    start_time = time.time()
    try:
        desc_prompt = f"""다음 정보로 YouTube 설명글을 작성해주세요.

제목: {request.title}
주제: {request.topic}
{f'영상 길이: {request.duration_seconds}초' if request.duration_seconds else ''}
{f'URL: {request.video_url}' if request.video_url else ''}
해시태그 포함: {request.include_hashtags}

요구사항:
- 첫 3줄에 핵심 요약
- {('해시태그 5-10개 포함' if request.include_hashtags else '해시태그 제외')}
- 시청자 참여 유도
- 300-400단어 분량"""

        result = await parallel_generate(
            prompt=desc_prompt,
            system_prompt="당신은 YouTube 콘텐츠 최적화 전문가입니다.",
            max_tokens=1000,
            temperature=0.7,
            requested_provider=request.provider,
        )

        return GenerateResponse(
            success=True,
            content=result["content"],
            provider=result.get("provider_name", "unknown"),
            model=result.get("model_name", "unknown"),
            tokens_used=result.get("tokens_used"),
            generation_time=time.time() - start_time,
        )

    except Exception as e:
        logger.error(f"설명글 생성 오류: {traceback.format_exc()}")
        return GenerateResponse(
            success=False,
            content="",
            provider=request.provider.value,
            model="unknown",
            generation_time=time.time() - start_time,
            error=str(e),
        )


@app.post("/generate/tags", response_model=TagsResponse, tags=["generation"])
async def generate_tags(
    request: TagsGenerateRequest,
    api_key: bool = Depends(verify_api_key),
) -> TagsResponse:
    """YouTube 태그 생성 (병렬 멀티 프로바이더)"""
    start_time = time.time()
    try:
        tags_prompt = f"""다음 주제의 영상을 위해 YouTube 검색 최적화 태그 {request.count}개를 생성해주세요.

주제: {request.topic}
언어: {"한국어" if request.language == "ko" else "English"}

형식: 각 태그를 줄 바꿈으로 구분"""

        result = await parallel_generate(
            prompt=tags_prompt,
            system_prompt="당신은 YouTube SEO 전문가입니다.",
            max_tokens=1000,
            temperature=0.6,
            requested_provider=request.provider,
        )

        tags = [
            tag.strip()
            for tag in result["content"].split("\n")
            if tag.strip() and not tag.startswith(("#", "-", "*"))
        ][:request.count]

        return TagsResponse(
            success=True,
            tags=tags,
            provider=result.get("provider_name", "unknown"),
            model=result.get("model_name", "unknown"),
            generation_time=time.time() - start_time,
        )

    except Exception as e:
        logger.error(f"태그 생성 오류: {traceback.format_exc()}")
        return TagsResponse(
            success=False,
            tags=[],
            provider=request.provider.value,
            model="unknown",
            generation_time=time.time() - start_time,
            error=str(e),
        )


@app.get("/providers", tags=["system"])
async def list_providers():
    """사용 가능한 프로바이더 상태 조회"""
    status = {}
    for p in ProviderEnum:
        if p == ProviderEnum.AUTO:
            continue
        inst = get_provider_instance(p)
        status[p.value] = {
            "available": inst is not None,
            "model": inst.model if inst else None,
            "type": "offline" if p in (ProviderEnum.OLLAMA, ProviderEnum.GPT4ALL) else "online",
        }
    return {"providers": status, "parallel_mode": "enabled"}


@app.get("/", tags=["system"])
async def root():
    """루트 엔드포인트"""
    return {
        "service": settings.service_name,
        "version": settings.service_version,
        "mode": "병렬 멀티 프로바이더",
        "providers": "claude|gemini|openrouter|cerebras|arliai|openai|ollama",
        "endpoints": [
            "GET /health",
            "GET /providers",
            "GET /docs",
            "POST /generate",
            "POST /generate/script",
            "POST /generate/title",
            "POST /generate/description",
            "POST /generate/tags",
        ],
    }


if __name__ == "__main__":
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=settings.service_port,
        workers=1,
        log_level="info",
    )
