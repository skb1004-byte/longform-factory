# -*- coding: utf-8 -*-
"""
Scene splitting and script generation via Claude API.

Converts user scripts into structured scene definitions with keywords,
narration, and timing. Supports both manual script splitting and
auto-generation from topic when no script is provided.
"""

from __future__ import annotations
import json
import re
import logging
from typing import List, Optional
import httpx
from config import ANTHROPIC_API_KEY, ANTHROPIC_MODEL
from models import Scene

logger = logging.getLogger(__name__)


async def split_script_to_scenes(
    script: str,
    topic: str,
    video_type: str = "longform",
    duration_sec: int = 60,
    tone: str = "neutral",
) -> List[Scene]:
    """
    Split user script into scenes using Claude API.

    Args:
        script: Full narration script text
        topic: Video topic for context
        video_type: 'longform' (5 scenes) or 'shorts' (3 scenes)
        duration_sec: Total video duration target
        tone: Narration tone (neutral, formal, casual)

    Returns:
        List of Scene objects with keywords, narration, and timing
    """
    n_scenes: int = 3 if video_type == "shorts" else 5

    prompt = f"""당신은 영상 씬 분할 전문가입니다.
다음 스크립트를 정확히 {n_scenes}개 씬으로 분할하세요.

주제: {topic}
톤: {tone}
총 길이: {duration_sec}초

스크립트:
{script}

각 씬을 아래 JSON 형식으로 반환:
[
  {{
    "scene_id": "scene_01",
    "keyword": "영어 키워드 1-3단어 (Pexels 검색용, 시각적으로 명확한 표현)",
    "narration": "원문 나레이션 텍스트 (수정 금지, 원문 그대로)",
    "duration_seconds": 12.0
  }}
]

규칙:
- keyword: 반드시 영어, 최대 3단어, 동작/장소/사물 위주
- narration: 스크립트 원문에서 해당 씬 부분 그대로 (요약/수정/생략 금지)
- duration_seconds: narration 글자수 / 4.5 로 계산
- 반드시 순수 JSON 배열만 반환, 마크다운 펜스(```json) 절대 금지
- 정확히 {n_scenes}개 씬"""

    for attempt in range(2):
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.post(
                    "https://api.anthropic.com/v1/messages",
                    headers={
                        "x-api-key": ANTHROPIC_API_KEY,
                        "anthropic-version": "2023-06-01",
                        "content-type": "application/json",
                    },
                    json={
                        "model": ANTHROPIC_MODEL,
                        "max_tokens": 1500,
                        "messages": [{"role": "user", "content": prompt}],
                    }
                )
                resp.raise_for_status()
                data: dict = resp.json()
                raw: str = data["content"][0]["text"].strip()

                # Strip markdown fences if present
                raw = re.sub(r'```json\s*|\s*```', '', raw).strip()

                # Parse JSON
                scenes_data: list = json.loads(raw)
                if not isinstance(scenes_data, list):
                    raise ValueError("Expected JSON array")

                # Build Scene objects, limit to n_scenes
                scenes: List[Scene] = [
                    Scene(**s) for s in scenes_data[:n_scenes]
                ]
                logger.info(
                    f"[script] {len(scenes)} scenes from Claude (attempt {attempt+1})"
                )
                return scenes

        except Exception as e:
            logger.warning(f"[script] Claude attempt {attempt+1} failed: {e}")

    # Fallback: dummy scenes from topic
    logger.warning("[script] Claude failed, using dummy scenes")
    return _make_dummy_scenes(topic, n_scenes, duration_sec)


async def generate_script_from_topic(
    topic: str,
    duration_sec: int = 60,
    tone: str = "neutral",
) -> str:
    """
    Generate a script from topic when user didn't provide one.

    Args:
        topic: Video subject/theme
        duration_sec: Target duration in seconds
        tone: Narration tone

    Returns:
        Generated Korean narration script text
    """
    prompt = f"""주제: {topic}
톤: {tone}
목표 길이: {duration_sec}초

위 주제로 한국어 유튜브 나레이션 스크립트를 작성하세요.
- 자연스러운 구어체
- 약 {int(duration_sec * 4.5)}자 분량
- 마크다운 없이 순수 텍스트만"""

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": ANTHROPIC_API_KEY,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": ANTHROPIC_MODEL,
                    "max_tokens": 1000,
                    "messages": [{"role": "user", "content": prompt}],
                }
            )
            resp.raise_for_status()
            data: dict = resp.json()
            return data["content"][0]["text"].strip()

    except Exception as e:
        logger.error(f"[script] script generation failed: {e}")
        return topic  # Minimal fallback


def _make_dummy_scenes(
    topic: str,
    n_scenes: int,
    duration_sec: int
) -> List[Scene]:
    """
    Create fallback scenes when Claude is unavailable.

    Args:
        topic: Video topic
        n_scenes: Number of scenes to create
        duration_sec: Total duration to distribute

    Returns:
        List of dummy Scene objects
    """
    per_dur: float = duration_sec / n_scenes
    # Use topic words as keywords
    words: List[str] = topic.split()[:3]
    keyword: str = " ".join(words) if words else "nature landscape"

    return [
        Scene(
            scene_id=f"scene_{i+1:02d}",
            keyword=keyword,
            narration=topic,
            duration_seconds=per_dur,
        )
        for i in range(n_scenes)
    ]
