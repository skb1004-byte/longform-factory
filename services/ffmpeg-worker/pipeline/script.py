# -*- coding: utf-8 -*-
"""
Scene splitting and script generation.

Fallback chain (parallel): Groq → DeepSeek → Gemini → Cerebras → ArliAI → OpenRouter → OpenAI → Claude

Strategy:
  - generate_script_from_topic(): parallel race (첫 성공 반환)
  - split_script_to_scenes(): sequential (JSON 파싱 필요)
  - 모든 프롬프트 한국어 강제
"""
from __future__ import annotations
import asyncio
import json
import re
import logging
from typing import List, Optional, Any
import httpx
from config import (
    ANTHROPIC_API_KEY, ANTHROPIC_MODEL,
    OPENAI_API_KEY,
    GEMINI_API_KEY, GEMINI_MODEL,
    GROQ_API_KEY,
    CEREBRAS_API_KEY, CEREBRAS_MODEL,
    ARLIAI_API_KEY, ARLIAI_MODEL,
    DEEPSEEK_API_KEY,
    OPENROUTER_API_KEY,
)
from models import Scene

logger = logging.getLogger(__name__)

# LLM 타임아웃 (초) - 빠른 폴백을 위해 짧게
LLM_TIMEOUT = 20.0

# 키워드 프리셋 (한국어 관련, 최후 fallback용)
_KEYWORD_FALLBACK = [
    "korean culture", "food cooking", "nature korea",
    "technology future", "lifestyle wellness",
    "history tradition", "city urban", "education learning",
    "family community", "economy business",
]

# 씬 분할 프롬프트 (한국어 강제)
_SPLIT_PROMPT = """\
다음 스크립트를 정확히 {n}개의 씬으로 분할하세요.
주제: {topic}
스크립트:
{script}

반드시 JSON 배열만 반환 (마크다운 코드블록 금지):
[{{"scene_id":"scene_01","keyword":"영어 1-3단어","narration":"씬 나레이션 텍스트","duration_seconds":10.0}}]

규칙:
- keyword: 영어 1-3 단어 (Pexels/Pixabay 검색용, 주제와 관련된 단어)
- narration: 한국어 나레이션 (스크립트에서 정확히 발췌)
- duration_seconds: 글자수 / 4.5 로 계산
- 정확히 {n}개 씬 반환
"""

# 스크립트 생성 프롬프트
def _script_prompt(topic: str, duration_sec: int, tone: str) -> str:
    target_chars = int(duration_sec * 4.5)
    return (
        f"주제: {topic}\n"
        f"톤: {tone}\n"
        f"목표 길이: {duration_sec}초 ({target_chars}자)\n\n"
        f"위 주제로 한국어 유튜브 나레이션 스크립트를 작성하세요.\n"
        f"- 자연스러운 한국어 구어체 (반드시 한국어)\n"
        f"- 약 {target_chars}자 분량 (부족하면 내용 추가)\n"
        f"- 마크다운 없이 순수 텍스트만\n"
        f"- 영어 사용 절대 금지 (고유명사 제외)\n"
    )


# ============================================================================
# Public API
# ============================================================================

async def generate_script_from_topic(
    topic: str,
    duration_sec: int = 60,
    tone: str = "neutral",
) -> str:
    """병렬 레이스로 스크립트 생성. 첫 성공 반환. 전부 실패 시 템플릿 생성."""
    prompt = _script_prompt(topic, duration_sec, tone)
    min_len = max(50, int(duration_sec * 2))  # 최소 글자수 (duration 기반)

    tasks = _build_text_tasks(prompt)
    result = await _parallel_race(tasks, min_len=min_len)

    if result:
        logger.info(f"[script] ✅ script generated ({len(result)}자)")
        return result

    # 최종 fallback: 주제 기반 템플릿
    logger.error(f"[script] ❌ all APIs failed, generating template for: {topic}")
    return _generate_template_script(topic, duration_sec, tone)


async def split_script_to_scenes(
    script: str,
    topic: str,
    video_type: str = "longform",
    duration_sec: int = 60,
    tone: str = "neutral",
) -> List[Scene]:
    """스크립트를 씬으로 분할. Sequential fallback chain."""
    n_scenes: int = 3 if video_type == "shorts" else 5
    prompt = _SPLIT_PROMPT.format(n=n_scenes, topic=topic, script=script)

    for name, coro in _scene_splitter_chain(prompt, n_scenes):
        result = await coro
        if result:
            logger.info(f"[script] ✅ {len(result)} scenes from {name}")
            # 키워드 보정 (topic 관련으로)
            result = _enrich_keywords(result, topic)
            return result

    logger.warning("[script] all scene APIs failed, using local text split")
    return _split_script_locally(script or topic, topic, n_scenes, duration_sec)


# ============================================================================
# Parallel race helper
# ============================================================================

async def _parallel_race(
    tasks: List[tuple[str, Any]],
    min_len: int = 20,
) -> Optional[str]:
    """모든 task를 병렬 실행. 조건 만족하는 첫 결과 반환."""
    if not tasks:
        return None

    # (name, coro) 목록에서 태스크 생성
    named_tasks: List[tuple[str, asyncio.Task]] = []
    for name, coro in tasks:
        t = asyncio.create_task(coro)
        named_tasks.append((name, t))

    all_tasks = [t for _, t in named_tasks]
    result: Optional[str] = None

    # as_completed: 완료 순서대로 처리
    for coro in asyncio.as_completed(all_tasks, timeout=LLM_TIMEOUT + 10):
        try:
            text = await coro
            if text and len(text) >= min_len:
                # 어느 provider인지 역추적
                for n, t in named_tasks:
                    if t.done() and not t.cancelled() and not t.exception():
                        try:
                            if t.result() == text:
                                logger.info(f"[script] ✅ parallel winner: {n} ({len(text)}자)")
                                break
                        except Exception:
                            pass
                result = text
                break
        except asyncio.TimeoutError:
            logger.warning("[script] parallel race timeout")
            break
        except Exception as e:
            logger.debug(f"[script] parallel task error: {e}")

    # 남은 태스크 모두 취소
    for t in all_tasks:
        if not t.done():
            t.cancel()
    # 취소 완료 대기
    await asyncio.gather(*all_tasks, return_exceptions=True)

    return result


# ============================================================================
# Task builders
# ============================================================================

def _build_text_tasks(prompt: str) -> List[tuple[str, Any]]:
    """스크립트 텍스트 생성 병렬 태스크 목록."""
    tasks = []

    # 1. Groq (빠름, 한국어 양호)
    if GROQ_API_KEY:
        tasks.append(("Groq-70b", _llm_text_oai(
            "https://api.groq.com/openai/v1/chat/completions",
            GROQ_API_KEY,
            "llama-3.3-70b-versatile",
            prompt,
            system="당신은 한국어 전문 유튜브 스크립트 작가입니다. 반드시 한국어로만 답변하세요.",
        )))
        tasks.append(("Groq-8b", _llm_text_oai(
            "https://api.groq.com/openai/v1/chat/completions",
            GROQ_API_KEY,
            "llama-3.1-8b-instant",
            prompt,
            system="당신은 한국어 유튜브 스크립트 작가입니다. 한국어로만 답변하세요.",
        )))

    # 2. DeepSeek (한국어 최강)
    if DEEPSEEK_API_KEY:
        tasks.append(("DeepSeek", _llm_text_oai(
            "https://api.deepseek.com/v1/chat/completions",
            DEEPSEEK_API_KEY,
            "deepseek-chat",
            prompt,
            system="당신은 한국어 유튜브 스크립트 전문가입니다. 반드시 한국어로만 답변하세요.",
        )))

    # 3. Gemini
    if GEMINI_API_KEY:
        tasks.append(("Gemini", _llm_text_gemini(prompt)))

    # 4. Cerebras
    if CEREBRAS_API_KEY:
        tasks.append(("Cerebras", _llm_text_oai(
            "https://api.cerebras.ai/v1/chat/completions",
            CEREBRAS_API_KEY,
            CEREBRAS_MODEL,
            prompt,
        )))

    # 5. ArliAI
    if ARLIAI_API_KEY:
        tasks.append(("ArliAI", _llm_text_oai(
            "https://api.arliai.com/v1/chat/completions",
            ARLIAI_API_KEY,
            ARLIAI_MODEL,
            prompt,
        )))

    # 6. OpenRouter (무료 모델)
    if OPENROUTER_API_KEY:
        tasks.append(("OpenRouter-llama", _llm_text_openrouter(
            prompt,
            "meta-llama/llama-3.3-70b-instruct:free",
        )))
        tasks.append(("OpenRouter-deepseek", _llm_text_openrouter(
            prompt,
            "deepseek/deepseek-chat-v3-0324:free",
        )))

    # 7. OpenAI (유료, 마지막)
    if OPENAI_API_KEY:
        tasks.append(("OpenAI", _llm_text_oai(
            "https://api.openai.com/v1/chat/completions",
            OPENAI_API_KEY,
            "gpt-4o-mini",
            prompt,
        )))

    # 8. Claude (마지막, 크레딧 아낌)
    if ANTHROPIC_API_KEY:
        tasks.append(("Claude", _llm_text_claude(prompt)))

    return tasks


def _scene_splitter_chain(prompt: str, n_scenes: int):
    """씬 분할용 Sequential generator. JSON 파싱 필요 = sequential이 안전."""
    if GROQ_API_KEY:
        yield "Groq-70b",   _call_groq_scenes(prompt, n_scenes, "llama-3.3-70b-versatile")
        yield "Groq-8b",    _call_groq_scenes(prompt, n_scenes, "llama-3.1-8b-instant")
    if DEEPSEEK_API_KEY:
        yield "DeepSeek",   _call_oai_scenes("https://api.deepseek.com/v1/chat/completions", DEEPSEEK_API_KEY, "deepseek-chat", prompt, n_scenes)
    if GEMINI_API_KEY:
        yield "Gemini",     _call_gemini_scenes(prompt, n_scenes)
    if CEREBRAS_API_KEY:
        yield "Cerebras",   _call_oai_scenes("https://api.cerebras.ai/v1/chat/completions", CEREBRAS_API_KEY, CEREBRAS_MODEL, prompt, n_scenes)
    if ARLIAI_API_KEY:
        yield "ArliAI",     _call_oai_scenes("https://api.arliai.com/v1/chat/completions", ARLIAI_API_KEY, ARLIAI_MODEL, prompt, n_scenes)
    if OPENROUTER_API_KEY:
        yield "OpenRouter", _call_oai_scenes(
            "https://openrouter.ai/api/v1/chat/completions",
            OPENROUTER_API_KEY, "meta-llama/llama-3.3-70b-instruct:free",
            prompt, n_scenes,
            extra_headers={"HTTP-Referer": "https://longform.spacek.io", "X-Title": "LongForm Factory"},
        )
    if OPENAI_API_KEY:
        yield "OpenAI",     _call_oai_scenes("https://api.openai.com/v1/chat/completions", OPENAI_API_KEY, "gpt-4o-mini", prompt, n_scenes)
    if ANTHROPIC_API_KEY:
        yield "Claude",     _call_claude_scenes(prompt, n_scenes)


# ============================================================================
# LLM helpers (text)
# ============================================================================

async def _llm_text_oai(
    url: str,
    api_key: str,
    model: str,
    prompt: str,
    system: str = "반드시 한국어로만 답변하세요.",
    max_tokens: int = 1500,
) -> str:
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    payload = {
        "model": model,
        "max_tokens": max_tokens,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": prompt},
        ],
    }
    try:
        async with httpx.AsyncClient(timeout=LLM_TIMEOUT) as c:
            resp = await c.post(url, headers=headers, json=payload)
            if resp.status_code == 200:
                return resp.json()["choices"][0]["message"]["content"].strip()
            logger.warning(f"[llm] {model} HTTP {resp.status_code}: {resp.text[:80]}")
    except Exception as e:
        logger.debug(f"[llm] {model} error: {e}")
    return ""


async def _llm_text_openrouter(prompt: str, model: str) -> str:
    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://longform.spacek.io",
        "X-Title": "LongForm Factory",
    }
    payload = {
        "model": model,
        "max_tokens": 1500,
        "messages": [
            {"role": "system", "content": "반드시 한국어로만 답변하세요."},
            {"role": "user", "content": prompt},
        ],
    }
    try:
        async with httpx.AsyncClient(timeout=LLM_TIMEOUT) as c:
            resp = await c.post(
                "https://openrouter.ai/api/v1/chat/completions",
                headers=headers, json=payload,
            )
            if resp.status_code == 200:
                return resp.json()["choices"][0]["message"]["content"].strip()
            logger.warning(f"[llm] OpenRouter/{model} HTTP {resp.status_code}: {resp.text[:80]}")
    except Exception as e:
        logger.debug(f"[llm] OpenRouter/{model} error: {e}")
    return ""


async def _llm_text_gemini(prompt: str) -> str:
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"
    payload = {
        "contents": [{"parts": [{"text": "반드시 한국어로만 답변하세요.\n\n" + prompt}]}],
        "generationConfig": {"maxOutputTokens": 1500},
    }
    try:
        async with httpx.AsyncClient(timeout=LLM_TIMEOUT) as c:
            resp = await c.post(url, headers={"Content-Type": "application/json"}, json=payload)
            if resp.status_code == 200:
                return resp.json()["candidates"][0]["content"]["parts"][0]["text"].strip()
            logger.warning(f"[llm] Gemini HTTP {resp.status_code}: {resp.text[:80]}")
    except Exception as e:
        logger.debug(f"[llm] Gemini error: {e}")
    return ""


async def _llm_text_claude(prompt: str) -> str:
    payload = {
        "model": ANTHROPIC_MODEL,
        "max_tokens": 1500,
        "system": "당신은 한국어 유튜브 스크립트 전문가입니다. 반드시 한국어로만 답변하세요.",
        "messages": [{"role": "user", "content": prompt}],
    }
    try:
        async with httpx.AsyncClient(timeout=LLM_TIMEOUT) as c:
            resp = await c.post(
                "https://api.anthropic.com/v1/messages",
                headers={"x-api-key": ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"},
                json=payload,
            )
            if resp.status_code == 200:
                return resp.json()["content"][0]["text"].strip()
            logger.warning(f"[llm] Claude HTTP {resp.status_code}: {resp.text[:80]}")
    except Exception as e:
        logger.debug(f"[llm] Claude error: {e}")
    return ""


# ============================================================================
# LLM helpers (scenes)
# ============================================================================

async def _call_oai_scenes(
    url: str,
    api_key: str,
    model: str,
    prompt: str,
    n_scenes: int,
    extra_headers: dict = None,
) -> Optional[List[Scene]]:
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    if extra_headers:
        headers.update(extra_headers)
    payload = {
        "model": model,
        "max_tokens": 1500,
        "messages": [
            {"role": "system", "content": "반드시 JSON 배열만 반환. 마크다운 코드블록 절대 금지. 한국어 나레이션."},
            {"role": "user", "content": prompt},
        ],
    }
    try:
        async with httpx.AsyncClient(timeout=LLM_TIMEOUT) as c:
            resp = await c.post(url, headers=headers, json=payload)
            if resp.status_code != 200:
                logger.warning(f"[scene] {model} HTTP {resp.status_code}")
                return None
            raw = resp.json()["choices"][0]["message"]["content"].strip()
            return _parse_scenes_json(raw, n_scenes)
    except Exception as e:
        logger.warning(f"[scene] {model} error: {e}")
    return None


async def _call_groq_scenes(prompt: str, n_scenes: int, model: str = "llama-3.3-70b-versatile") -> Optional[List[Scene]]:
    return await _call_oai_scenes(
        "https://api.groq.com/openai/v1/chat/completions",
        GROQ_API_KEY, model, prompt, n_scenes,
    )


async def _call_gemini_scenes(prompt: str, n_scenes: int) -> Optional[List[Scene]]:
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"
    payload = {
        "contents": [{"parts": [{"text": "반드시 JSON 배열만 반환. 마크다운 코드블록 절대 금지.\n\n" + prompt}]}],
        "generationConfig": {"maxOutputTokens": 1500},
    }
    try:
        async with httpx.AsyncClient(timeout=LLM_TIMEOUT) as c:
            resp = await c.post(url, headers={"Content-Type": "application/json"}, json=payload)
            if resp.status_code != 200:
                logger.warning(f"[scene] Gemini HTTP {resp.status_code}")
                return None
            raw = resp.json()["candidates"][0]["content"]["parts"][0]["text"].strip()
            return _parse_scenes_json(raw, n_scenes)
    except Exception as e:
        logger.warning(f"[scene] Gemini error: {e}")
    return None


async def _call_claude_scenes(prompt: str, n_scenes: int) -> Optional[List[Scene]]:
    payload = {
        "model": ANTHROPIC_MODEL,
        "max_tokens": 1500,
        "system": "반드시 JSON 배열만 반환. 마크다운 코드블록 절대 금지. 한국어 나레이션.",
        "messages": [{"role": "user", "content": prompt}],
    }
    try:
        async with httpx.AsyncClient(timeout=LLM_TIMEOUT) as c:
            resp = await c.post(
                "https://api.anthropic.com/v1/messages",
                headers={"x-api-key": ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"},
                json=payload,
            )
            if resp.status_code != 200:
                logger.warning(f"[scene] Claude HTTP {resp.status_code}")
                return None
            raw = resp.json()["content"][0]["text"].strip()
            return _parse_scenes_json(raw, n_scenes)
    except Exception as e:
        logger.warning(f"[scene] Claude error: {e}")
    return None


# ============================================================================
# JSON parsing + local fallbacks
# ============================================================================

def _parse_scenes_json(raw: str, n_scenes: int) -> Optional[List[Scene]]:
    try:
        raw = re.sub(r'```(?:json)?\s*|\s*```', '', raw).strip()
        if raw.startswith("{"):
            obj = json.loads(raw)
            for key in ("scenes", "data", "result", "items"):
                if key in obj and isinstance(obj[key], list):
                    raw = json.dumps(obj[key])
                    break
        scenes_data: list = json.loads(raw)
        if not isinstance(scenes_data, list) or not scenes_data:
            return None
        # Validate required fields
        valid = []
        for i, s in enumerate(scenes_data[:n_scenes]):
            if not isinstance(s, dict):
                continue
            s.setdefault("scene_id", f"scene_{i+1:02d}")
            s.setdefault("narration", s.get("text", s.get("content", "")))
            s.setdefault("keyword", "korean culture")
            s.setdefault("duration_seconds", max(round(len(s.get("narration", "")) / 4.5, 1), 3.0))
            valid.append(Scene(**s))
        return valid if valid else None
    except Exception as e:
        logger.warning(f"[script] JSON parse failed: {e} | raw={raw[:120]}")
    return None


def _enrich_keywords(scenes: List[Scene], topic: str) -> List[Scene]:
    """키워드가 너무 generic하면 주제 기반으로 보정."""
    generic = {"nature landscape", "city skyline", "people working",
               "technology innovation", "healthy lifestyle"}
    for s in scenes:
        if s.keyword in generic or not s.keyword:
            # 주제에서 영어 키워드 파생
            topic_en = _topic_to_keyword(topic, s.keyword)
            s.keyword = topic_en
    return scenes


def _topic_to_keyword(topic: str, fallback: str) -> str:
    """한국어 주제에서 Pexels 검색용 영어 키워드 생성."""
    mapping = {
        "비빔밥": "korean bibimbap food",
        "한식": "korean traditional food",
        "요리": "cooking kitchen",
        "건강": "healthy lifestyle",
        "운동": "exercise fitness",
        "여행": "travel landscape",
        "한국": "korea culture",
        "역사": "history ancient",
        "기술": "technology innovation",
        "음악": "music performance",
        "영화": "cinema movie",
        "교육": "education learning",
        "경제": "economy business",
        "환경": "environment nature",
        "우주": "space cosmos",
    }
    topic_lower = topic.lower()
    for ko, en in mapping.items():
        if ko in topic_lower:
            return en
    return fallback or "korean culture"


def _generate_template_script(topic: str, duration_sec: int, tone: str) -> str:
    """모든 API 실패 시 템플릿 기반 한국어 스크립트 생성."""
    templates = {
        "friendly": f"""안녕하세요! 오늘은 '{topic}'에 대해 알아보겠습니다.
{topic}은 우리 일상에서 매우 중요한 주제입니다.
먼저 {topic}의 기본 개념부터 살펴보겠습니다.
{topic}을 제대로 이해하면 많은 도움이 됩니다.
자세한 내용을 하나씩 알아볼까요?
{topic}의 핵심 포인트를 정리해 드리겠습니다.
이 내용을 잘 기억하시면 앞으로 큰 도움이 될 것입니다.
오늘 영상이 도움이 되셨다면 좋아요와 구독 부탁드립니다!""",
        "professional": f"""'{topic}'에 대한 전문적인 분석을 시작하겠습니다.
{topic}은 현대 사회에서 중요한 위치를 차지하고 있습니다.
전문가들의 견해에 따르면, {topic}의 핵심 요소는 다음과 같습니다.
첫째, {topic}의 기본 원리를 이해하는 것이 중요합니다.
둘째, 실제 사례를 통해 {topic}을 파악할 수 있습니다.
셋째, {topic}의 미래 전망은 매우 긍정적입니다.
이상으로 {topic}에 대한 분석을 마치겠습니다.""",
        "neutral": f"""오늘 주제는 '{topic}'입니다.
{topic}에 대해 알아보겠습니다.
{topic}은 다양한 측면에서 살펴볼 수 있습니다.
첫 번째로 {topic}의 기본 개념을 설명드리겠습니다.
두 번째로 {topic}의 주요 특징을 살펴보겠습니다.
세 번째로 {topic}의 실용적인 활용 방법을 알아보겠습니다.
마지막으로 {topic}에 대한 정리를 해보겠습니다.
이 영상이 도움이 되셨으면 합니다.""",
    }
    base = templates.get(tone, templates["neutral"])
    # duration에 맞게 반복 확장
    target = int(duration_sec * 4.5)
    while len(base) < target:
        base += f"\n{topic}에 대해 더 알아볼 내용이 있습니다. {topic}은 계속해서 발전하고 있으며, 앞으로도 중요한 주제가 될 것입니다."
    return base


def _split_script_locally(
    script: str,
    topic: str,
    n_scenes: int,
    duration_sec: int,
) -> List[Scene]:
    """로컬 텍스트 분할 (API 전부 실패 시)."""
    raw_sentences = re.split(r'(?<=[.!?。~\n])\s*', script.strip())
    sentences = [s.strip() for s in raw_sentences if s.strip() and len(s.strip()) > 3]

    # 스크립트가 너무 짧으면 주제로 확장
    if len(sentences) < n_scenes:
        template = _generate_template_script(topic, duration_sec, "neutral")
        raw_sentences = re.split(r'(?<=[.!?\n])\s*', template.strip())
        sentences = [s.strip() for s in raw_sentences if s.strip()]

    per_scene = max(1, len(sentences) // n_scenes)
    scene_texts: List[str] = []
    for i in range(n_scenes):
        start = i * per_scene
        end = start + per_scene if i < n_scenes - 1 else len(sentences)
        chunk = " ".join(sentences[start:end]).strip()
        scene_texts.append(chunk or topic)

    # 주제 관련 키워드
    base_kw = _topic_to_keyword(topic, "korean culture")
    kw_variants = [
        base_kw,
        f"{base_kw} close up",
        f"{base_kw} detail",
        f"korean {topic[:4]} culture" if len(topic) > 3 else base_kw,
        f"{base_kw} lifestyle",
    ]

    return [
        Scene(
            scene_id=f"scene_{i + 1:02d}",
            keyword=kw_variants[i % len(kw_variants)],
            narration=scene_texts[i],
            duration_seconds=max(round(len(scene_texts[i]) / 4.5, 1), 3.0),
        )
        for i in range(n_scenes)
    ]
