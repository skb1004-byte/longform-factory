"""
LongForm Factory AI-MCP Service v2.0.0
Fast API 기반 멀티 프로바이더 AI 스크립트 생성 서비스
"""

import os
import sys
import time
import json
import asyncio
import logging
from typing import Optional, List, Dict, Any
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
import requests

# ==================== 설정 ====================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class Settings(BaseSettings):
    """환경변수 기반 설정"""
    # API 키
    anthropic_api_key: str = Field(default="", alias="ANTHROPIC_API_KEY")
    gemini_api_key: str = Field(default="", alias="GEMINI_API_KEY")
    openai_api_key: str = Field(default="", alias="OPENAI_API_KEY")
    lf_api_key: str = Field(default="", alias="LF_API_KEY")
    
    # 모델 지정
    anthropic_model: str = Field(default="claude-3-5-sonnet-20241022", alias="ANTHROPIC_MODEL")
    gemini_model: str = Field(default="gemini-1.5-pro", alias="GEMINI_MODEL")
    openai_model: str = Field(default="gpt-4", alias="OPENAI_MODEL")
    
    # RALF 재시도 설정
    ralf_max_retries: int = Field(default=3, alias="RALF_MAX_RETRIES")
    ralf_base_delay: float = Field(default=1.0, alias="RALF_BASE_DELAY")
    
    # 서비스 설정
    service_name: str = "lf_ai_mcp"
    service_version: str = "2.0.0"
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
    provider: ProviderEnum = Field(default=ProviderEnum.CLAUDE, description="AI 프로바이더")
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
    provider: ProviderEnum = Field(default=ProviderEnum.CLAUDE, description="AI 프로바이더")
    temperature: float = Field(default=0.7, ge=0.0, le=2.0)


class TitleGenerateRequest(BaseModel):
    """영상 제목 생성 요청"""
    topic: str = Field(..., min_length=1, description="주제")
    style: StyleEnum = Field(default=StyleEnum.EDUCATIONAL)
    content_type: str = Field(default="video", description="콘텐츠 타입")
    language: str = Field(default="ko")
    provider: ProviderEnum = Field(default=ProviderEnum.CLAUDE)


class DescriptionGenerateRequest(BaseModel):
    """YouTube 설명 생성 요청"""
    title: str = Field(..., min_length=1)
    topic: str = Field(..., min_length=1)
    video_url: Optional[str] = Field(default=None)
    duration_seconds: Optional[int] = Field(default=None)
    language: str = Field(default="ko")
    include_hashtags: bool = Field(default=True)
    provider: ProviderEnum = Field(default=ProviderEnum.CLAUDE)


class TagsGenerateRequest(BaseModel):
    """태그 생성 요청"""
    topic: str = Field(..., min_length=1)
    content_type: str = Field(default="video")
    language: str = Field(default="ko")
    count: int = Field(default=15, ge=5, le=50)
    provider: ProviderEnum = Field(default=ProviderEnum.CLAUDE)


class GenerateResponse(BaseModel):
    """생성 응답"""
    success: bool
    content: str
    provider: str
    model: str
    tokens_used: Optional[int] = None
    generation_time: float = Field(..., description="생성 시간 (초)")
    error: Optional[str] = None


class ScriptSection(BaseModel):
    """스크립트 섹션"""
    type: str  # intro, main_point, outro
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


class TitlesResponse(BaseModel):
    """제목 생성 응답 (5개)"""
    success: bool
    titles: List[str] = Field(default_factory=list, max_length=5)
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
        """Claude API 호출"""
        try:
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

            response = self.client.messages.create(**kwargs)
            
            generation_time = time.time() - start_time
            
            return {
                "success": True,
                "content": response.content[0].text,
                "tokens_used": response.usage.output_tokens,
                "generation_time": generation_time,
            }
        except Exception as e:
            logger.error(f"Claude API 오류: {str(e)}")
            raise


class GeminiProvider(AIProvider):
    """Google Gemini 프로바이더"""

    def __init__(self, model: str, api_key: str):
        super().__init__(ProviderEnum.GEMINI, model, api_key)
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel(model)

    async def generate(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        max_tokens: int = 2000,
        temperature: float = 0.7,
    ) -> Dict[str, Any]:
        """Gemini API 호출"""
        try:
            start_time = time.time()
            
            full_prompt = prompt
            if system_prompt:
                full_prompt = f"{system_prompt}\n\n{prompt}"
            
            response = self.model.generate_content(
                full_prompt,
                generation_config=genai.types.GenerationConfig(
                    max_output_tokens=max_tokens,
                    temperature=temperature,
                ),
            )
            
            generation_time = time.time() - start_time
            
            return {
                "success": True,
                "content": response.text,
                "tokens_used": None,  # Gemini은 토큰 정보 미제공
                "generation_time": generation_time,
            }
        except Exception as e:
            logger.error(f"Gemini API 오류: {str(e)}")
            raise


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
        """OpenAI 호환 API 호출"""
        try:
            start_time = time.time()
            
            messages = []
            if system_prompt:
                messages.append({"role": "system", "content": system_prompt})
            messages.append({"role": "user", "content": prompt})
            
            response = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                max_tokens=max_tokens,
                temperature=temperature,
            )
            
            generation_time = time.time() - start_time
            
            return {
                "success": True,
                "content": response.choices[0].message.content,
                "tokens_used": response.usage.completion_tokens,
                "generation_time": generation_time,
            }
        except Exception as e:
            logger.error(f"{self.provider.value} API 오류: {str(e)}")
            raise


# ==================== RALF 재시도 로직 ====================


async def ralf_retry(
    func,
    max_retries: int = None,
    base_delay: float = None,
):
    """
    RALF(Red-Green-Blue-Loop) 기반 재시도 로직
    - RED: 실패 재현
    - GREEN: 설정 체크
    - BLUE(REFACTOR): 재시도
    """
    max_retries = max_retries or settings.ralf_max_retries
    base_delay = base_delay or settings.ralf_base_delay
    
    for attempt in range(max_retries):
        try:
            logger.info(f"시도 {attempt + 1}/{max_retries}")
            result = await func()
            logger.info(f"성공: {attempt + 1}회차")
            return result
        except Exception as e:
            if attempt == max_retries - 1:
                logger.error(f"최대 재시도 횟수 초과: {str(e)}")
                raise
            
            # 지수 백오프
            delay = base_delay * (2 ** attempt)
            logger.warning(f"재시도 예정 ({delay}초 대기): {str(e)}")
            await asyncio.sleep(delay)


# ==================== 헬퍼 함수 ====================


def get_provider(
    provider: ProviderEnum,
    model: Optional[str] = None,
    temperature: float = 0.7,
) -> AIProvider:
    """프로바이더 인스턴스 생성"""
    
    # 모델명 결정
    if model is None:
        if provider == ProviderEnum.CLAUDE:
            model = settings.anthropic_model
        elif provider == ProviderEnum.GEMINI:
            model = settings.gemini_model
        elif provider == ProviderEnum.OPENAI:
            model = settings.openai_model
        else:
            model = settings.openai_model  # 호환 프로바이더는 OpenAI 모델 사용
    
    # 프로바이더 인스턴스화
    if provider == ProviderEnum.CLAUDE:
        if not settings.anthropic_api_key:
            raise ValueError("ANTHROPIC_API_KEY 미설정")
        return ClaudeProvider(model, settings.anthropic_api_key)
    
    elif provider == ProviderEnum.GEMINI:
        if not settings.gemini_api_key:
            raise ValueError("GEMINI_API_KEY 미설정")
        return GeminiProvider(model, settings.gemini_api_key)
    
    elif provider == ProviderEnum.OPENAI:
        if not settings.openai_api_key:
            raise ValueError("OPENAI_API_KEY 미설정")
        return OpenAICompatibleProvider(
            provider,
            model,
            settings.openai_api_key,
            "https://api.openai.com/v1"
        )
    
    elif provider == ProviderEnum.CEREBRAS:
        if not settings.openai_api_key:
            raise ValueError("OPENAI_API_KEY 미설정 (Cerebras 호환)")
        return OpenAICompatibleProvider(
            provider,
            model or "llama-3.1-70b",
            settings.openai_api_key,
            "https://api.cerebras.ai/v1"
        )
    
    elif provider == ProviderEnum.OPENROUTER:
        if not settings.openai_api_key:
            raise ValueError("OPENAI_API_KEY 미설정 (OpenRouter 호환)")
        return OpenAICompatibleProvider(
            provider,
            model or "meta-llama/llama-3.1-70b-instruct:free",
            settings.openai_api_key,
            "https://openrouter.ai/api/v1"
        )
    
    elif provider == ProviderEnum.ARLIAI:
        if not settings.openai_api_key:
            raise ValueError("OPENAI_API_KEY 미설정 (ArliAI 호환)")
        return OpenAICompatibleProvider(
            provider,
            model or "claude-3-5-sonnet",
            settings.openai_api_key,
            "https://api.arliai.com/v1"
        )
    
    else:
        raise ValueError(f"미지원 프로바이더: {provider}")


def verify_api_key(api_key: str = Header(None, alias="X-LF-API-Key")):
    """LF_API_KEY 인증"""
    if not settings.lf_api_key:
        logger.warning("LF_API_KEY 미설정 - 인증 스킵")
        return True
    
    if api_key != settings.lf_api_key:
        raise HTTPException(status_code=403, detail="Invalid API key")
    
    return True


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
- 제스처/액션 지시사항 포함
- 시청자 집중도 유지
{f'추가 정보: {additional_context}' if additional_context else ''}""",

        ContentTypeEnum.TITLE: f"""다음 주제의 영상을 위해 클릭유도력 높은 제목을 작성해주세요.
주제: {topic}

요구사항:
- 40자 이내의 간결한 표현
- 호기심 자극 또는 가치 제시
- 한글과 숫자/기호 활용
- 검색 최적화
{f'추가 정보: {additional_context}' if additional_context else ''}""",

        ContentTypeEnum.DESCRIPTION: f"""다음 정보로 YouTube 설명글을 작성해주세요.
주제: {topic}

요구사항:
- 첫 3줄에 핵심 요약
- 링크 및 타임스탬프 포함 가능
- #해시태그 포함
- 시청자 참여 유도 문구
{f'추가 정보: {additional_context}' if additional_context else ''}""",

        ContentTypeEnum.TAGS: f"""다음 주제의 영상을 위해 검색 최적화 태그를 생성해주세요.
주제: {topic}

요구사항:
- 2-3단어 조합의 태그
- 검색 트렌드 반영
- 경합성(Competitiveness) 고려
- 주제 관련성 높음
{f'추가 정보: {additional_context}' if additional_context else ''}""",
    }
    
    return prompts.get(content_type, prompts[ContentTypeEnum.LONGFORM])


# ==================== FastAPI 앱 ====================

app = FastAPI(
    title=f"{settings.service_name}",
    version=settings.service_version,
    description="LongForm Factory AI Content Generation Service"
)


@app.get("/health", tags=["system"])
async def health_check():
    """헬스 체크"""
    return {
        "status": "healthy",
        "service": settings.service_name,
        "version": settings.service_version,
        "timestamp": datetime.utcnow().isoformat(),
    }


@app.post("/generate", response_model=GenerateResponse, tags=["generation"])
async def generate_content(
    request: GenerateRequest,
    api_key: bool = Depends(verify_api_key),
) -> GenerateResponse:
    """
    일반 텍스트 생성 엔드포인트
    
    - prompt: 생성할 내용 프롬프트
    - provider: claude/gemini/openai/cerebras/openrouter/arliai
    - model: 모델명 (선택사항)
    - max_tokens: 최대 토큰 수
    - temperature: 창의성 (0.0-2.0)
    - system_prompt: 시스템 프롬프트 (선택사항)
    """
    try:
        logger.info(f"생성 요청: provider={request.provider}, model={request.model}")
        start_time = time.time()
        
        # 시스템 프롬프트 생성 (미제공 시)
        system_prompt = request.system_prompt or build_korean_prompt(
            request.content_type,
            request.topic or request.prompt
        )
        
        # 프로바이더 인스턴스화
        provider = get_provider(request.provider, request.model)
        
        # RALF 재시도 로직
        async def _generate():
            return await provider.generate(
                prompt=request.prompt,
                system_prompt=system_prompt,
                max_tokens=request.max_tokens,
                temperature=request.temperature,
            )
        
        result = await ralf_retry(_generate)
        total_time = time.time() - start_time
        
        return GenerateResponse(
            success=True,
            content=result["content"],
            provider=request.provider.value,
            model=provider.model,
            tokens_used=result.get("tokens_used"),
            generation_time=total_time,
        )
    
    except Exception as e:
        logger.error(f"생성 오류: {traceback.format_exc()}")
        return GenerateResponse(
            success=False,
            content="",
            provider=request.provider.value,
            model=request.model or "unknown",
            generation_time=time.time() - start_time,
            error=str(e),
        )


@app.post("/generate/script", response_model=ScriptGenerateResponse, tags=["generation"])
async def generate_script(
    request: ScriptGenerateRequest,
    api_key: bool = Depends(verify_api_key),
) -> ScriptGenerateResponse:
    """
    영상 스크립트 생성 (구조화된 출력)
    
    - topic: 주제
    - duration_seconds: 영상 길이
    - style: educational/entertaining/professional/casual/cinematic
    - language: ko/en
    - target_audience: 타겟 오디언스
    """
    try:
        logger.info(f"스크립트 생성 요청: topic={request.topic}, duration={request.duration_seconds}초")
        start_time = time.time()
        
        # 스크립트 프롬프트
        script_prompt = f"""다음 요구사항으로 {request.duration_seconds}초 길이의 영상 스크립트를 작성해주세요.

주제: {request.topic}
스타일: {request.style.value}
대상 시청자: {request.target_audience}
언어: {"한국어" if request.language == "ko" else "English"}

출력 형식 (JSON):
{{
    "intro": "오프닝 (10-15초 분량)",
    "main_points": [
        "메인 포인트 1 (약 {request.duration_seconds // 3}초)",
        "메인 포인트 2 (약 {request.duration_seconds // 3}초)",
        "메인 포인트 3 (약 {request.duration_seconds // 3}초)"
    ],
    "outro": "마무리 (10-15초 분량, CTA 포함)",
    "hooks": [
        "시청자 주목 후킹 1",
        "시청자 주목 후킹 2"
    ],
    "visual_notes": ["카메라 각도", "배경 제안", "BGM 분위기"]
}}

모든 내용은 자연스러운 스피킹 톤으로 작성해주세요."""
        
        provider = get_provider(request.provider)
        
        async def _generate():
            return await provider.generate(
                prompt=script_prompt,
                system_prompt=f"당신은 프로 영상 시나리오 작가입니다. {request.style.value} 스타일로 정확히 {request.duration_seconds}초 길이의 스크립트를 작성합니다.",
                max_tokens=4000,
                temperature=request.temperature,
            )
        
        result = await ralf_retry(_generate)
        total_time = time.time() - start_time
        
        # JSON 파싱 시도
        try:
            # ```json ... ``` 마크다운 형식 제거
            content = result["content"]
            if content.startswith("```"):
                content = content.split("```")[1]
                if content.startswith("json"):
                    content = content[4:]
                content = content.split("```")[0]
            
            script_data = json.loads(content.strip())
        except json.JSONDecodeError:
            logger.warning("스크립트 JSON 파싱 실패 - 원본 반환")
            script_data = {
                "intro": "",
                "main_points": [],
                "outro": "",
                "hooks": [],
                "raw_content": result["content"]
            }
        
        return ScriptGenerateResponse(
            success=True,
            topic=request.topic,
            language=request.language,
            total_duration_seconds=request.duration_seconds,
            intro=script_data.get("intro", ""),
            main_points=script_data.get("main_points", []),
            outro=script_data.get("outro", ""),
            hooks=script_data.get("hooks", []),
            provider=request.provider.value,
            model=provider.model,
            generation_time=total_time,
        )
    
    except Exception as e:
        logger.error(f"스크립트 생성 오류: {traceback.format_exc()}")
        return ScriptGenerateResponse(
            success=False,
            topic=request.topic,
            language=request.language,
            total_duration_seconds=request.duration_seconds,
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
    """
    영상 제목 5개 생성
    
    - topic: 주제
    - style: 영상 스타일
    - language: ko/en
    """
    try:
        logger.info(f"제목 생성 요청: topic={request.topic}")
        start_time = time.time()
        
        title_prompt = f"""다음 주제의 영상을 위해 YouTube 클릭유도력이 높은 제목 5개를 생성해주세요.

주제: {request.topic}
스타일: {request.style.value}
콘텐츠 타입: {request.content_type}
언어: {"한국어" if request.language == "ko" else "English"}

각 제목은:
- 40자 이내
- 숫자, 기호 활용
- 호기심 자극 또는 가치 제시
- 검색 최적화

형식: 각 제목을 줄 바꿈으로 구분해서 번호 없이 제시"""
        
        provider = get_provider(request.provider)
        
        async def _generate():
            return await provider.generate(
                prompt=title_prompt,
                system_prompt="당신은 YouTube 콘텐츠 마케팅 전문가입니다. 클릭률 높은 제목을 작성합니다.",
                max_tokens=500,
                temperature=0.8,
            )
        
        result = await ralf_retry(_generate)
        total_time = time.time() - start_time
        
        # 제목 파싱
        titles = [
            line.strip()
            for line in result["content"].split("\n")
            if line.strip() and not line.startswith(("#", "-", "*"))
        ][:5]
        
        return TitlesResponse(
            success=True,
            titles=titles,
            provider=request.provider.value,
            model=provider.model,
            generation_time=total_time,
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
    """
    YouTube 설명글 생성
    
    - title: 영상 제목
    - topic: 주제
    - video_url: 영상 URL (선택사항)
    - duration_seconds: 영상 길이 (선택사항)
    """
    try:
        logger.info(f"설명글 생성 요청: title={request.title}")
        start_time = time.time()
        
        desc_prompt = f"""다음 정보로 YouTube 설명글을 작성해주세요.

제목: {request.title}
주제: {request.topic}
{f'영상 길이: {request.duration_seconds}초' if request.duration_seconds else ''}
{f'URL: {request.video_url}' if request.video_url else ''}
해시태그 포함: {request.include_hashtags}

요구사항:
- 첫 3줄에 핵심 요약 (매력적이고 정보성 있게)
- 영상 주요 타임스탐프 포함 (해당하는 경우)
- {('해시태그 5-10개 포함' if request.include_hashtags else '해시태그 제외')}
- 시청자 참여 유도 (댓글, 좋아요, 구독 권유)
- 300-400단어 분량"""
        
        provider = get_provider(request.provider)
        
        async def _generate():
            return await provider.generate(
                prompt=desc_prompt,
                system_prompt="당신은 YouTube 콘텐츠 최적화 전문가입니다. 클릭률과 엔게이지먼트를 높이는 설명글을 작성합니다.",
                max_tokens=1000,
                temperature=0.7,
            )
        
        result = await ralf_retry(_generate)
        total_time = time.time() - start_time
        
        return GenerateResponse(
            success=True,
            content=result["content"],
            provider=request.provider.value,
            model=provider.model,
            tokens_used=result.get("tokens_used"),
            generation_time=total_time,
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
    """
    YouTube 태그 생성
    
    - topic: 주제
    - count: 생성할 태그 개수 (5-50, 기본 15)
    - language: ko/en
    """
    try:
        logger.info(f"태그 생성 요청: topic={request.topic}, count={request.count}")
        start_time = time.time()
        
        tags_prompt = f"""다음 주제의 영상을 위해 YouTube 검색 최적화 태그 {request.count}개를 생성해주세요.

주제: {request.topic}
콘텐츠 타입: {request.content_type}
언어: {"한국어" if request.language == "ko" else "English"}

태그 요구사항:
- 2-4단어 조합
- 검색 트렌드 반영
- 경합성(Competitiveness) 고려
- 주제 관련성 100%
- 구체적인 키워드 우선

형식: 각 태그를 줄 바꿈으로 구분, 쉼표는 태그 내부에만 사용"""
        
        provider = get_provider(request.provider)
        
        async def _generate():
            return await provider.generate(
                prompt=tags_prompt,
                system_prompt="당신은 YouTube SEO 전문가입니다. 검색 가시성을 높이는 최적화 태그를 생성합니다.",
                max_tokens=1000,
                temperature=0.6,
            )
        
        result = await ralf_retry(_generate)
        total_time = time.time() - start_time
        
        # 태그 파싱
        tags = [
            tag.strip()
            for tag in result["content"].split("\n")
            if tag.strip() and not tag.startswith(("#", "-", "*"))
        ][:request.count]
        
        return TagsResponse(
            success=True,
            tags=tags,
            provider=request.provider.value,
            model=provider.model,
            generation_time=total_time,
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


@app.get("/", tags=["system"])
async def root():
    """루트 엔드포인트"""
    return {
        "service": settings.service_name,
        "version": settings.service_version,
        "port": settings.service_port,
        "endpoints": [
            "GET /health",
            "GET /docs (Swagger UI)",
            "POST /generate (일반 생성)",
            "POST /generate/script (스크립트)",
            "POST /generate/title (제목)",
            "POST /generate/description (설명글)",
            "POST /generate/tags (태그)",
        ],
    }


if __name__ == "__main__":
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=settings.service_port,
        workers=2,
        log_level="info",
    )
