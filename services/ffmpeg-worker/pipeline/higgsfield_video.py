# -*- coding: utf-8 -*-
"""Higgsfield DoP image-to-video: cinematic AI motion from a static keyframe.

공식 문서(docs.higgsfield.ai) 및 OpenAPI 스펙 기준. 2026-09-18 확인.

인증:  "Authorization: Key {HIGGSFIELD_API_KEY_ID}:{HIGGSFIELD_API_KEY_SECRET}"
흐름:  1) POST /files/generate-upload-url  → presigned 업로드 URL 발급
       2) PUT  {upload_url}               → 실제 이미지 업로드 (Higgsfield 인증 헤더 금지)
       3) POST /higgsfield-ai/dop/turbo   → {prompt, image_url} 제출
       4) GET  {status_url} 폴링          → completed 시 결과 영상 다운로드

이전 구현이 틀렸던 부분 (실제 키가 있어도 실패했을 코드):
  - 엔드포인트에 존재하지 않는 /v1/ 세그먼트가 들어가 있었다 (404)
  - 이미지를 base64 data URI로 "image" 필드에 실어 보냈다. 스펙상 필드명은
    "image_url" 이고 format:uri — 공개 HTTPS URL 이어야 해 presigned 업로드가 선행돼야 한다.
  - 스펙에 없는 "duration" 필드를 보냈다. DoP 길이는 모델 티어로 정해진다.

어떤 단계에서 실패하든 False를 돌려주므로 호출부가 Kling → 로컬 depth → Ken Burns로 폴백한다.
"""
from __future__ import annotations
import asyncio
import logging
from pathlib import Path
from typing import Optional

import httpx

from config import HIGGSFIELD_API_KEY_ID, HIGGSFIELD_API_KEY_SECRET

logger = logging.getLogger(__name__)

HIGGSFIELD_API_BASE = "https://api.higgsfield.ai"
HIGGSFIELD_UPLOAD_URL = f"{HIGGSFIELD_API_BASE}/files/generate-upload-url"

# DoP 티어: turbo(빠름) / lite / standard. 스펙에서 확인된 실제 경로 — /v1/ 없음.
HIGGSFIELD_DOP_TIER = "turbo"
HIGGSFIELD_DOP_ENDPOINT = f"{HIGGSFIELD_API_BASE}/higgsfield-ai/dop/{HIGGSFIELD_DOP_TIER}"

_MIME_BY_SUFFIX = {
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".webp": "image/webp",
    ".gif": "image/gif",
}

_MOTION_PROMPTS: dict[str, str] = {
    "cartoon":     "smooth animated motion, gentle parallax, illustrated world",
    "cinematic":   "slow cinematic dolly zoom, dramatic reveal, film-quality motion",
    "watercolor":  "dreamy soft motion, gentle brush-stroke flow, painterly movement",
    "anime":       "dynamic anime camera motion, expressive movement, vivid colors",
    "minimal":     "clean minimal motion, subtle elegant camera drift",
    "infographic": "smooth data visualization motion, clean reveal animation",
    "news":        "steady handheld motion, subtle documentary-style camera drift",
    "default":     "smooth gentle camera movement, cinematic parallax, steady motion",
}


def _auth_headers() -> dict:
    return {
        "Authorization": f"Key {HIGGSFIELD_API_KEY_ID}:{HIGGSFIELD_API_KEY_SECRET}",
        "Content-Type": "application/json",
    }


def _motion_prompt_for_style(style: str, scene_keyword: str = "") -> str:
    base = _MOTION_PROMPTS.get(style, _MOTION_PROMPTS["default"])
    kw = scene_keyword.lower()
    if any(k in kw for k in ["food", "cook", "meal", "김치", "요리", "음식"]):
        base += ", steam rising, appetizing close-up motion"
    elif any(k in kw for k in ["nature", "landscape", "자연", "풍경"]):
        base += ", breeze through foliage, organic natural motion"
    return base


async def _upload_image(image_path: Path) -> Optional[str]:
    """이미지를 Higgsfield 스토리지에 올리고 공개 URL을 돌려준다.

    presigned URL 은 1시간 뒤 만료되지만 바로 생성 요청을 이어 넣으므로 충분하다.
    presigned 스토리지에는 Higgsfield 인증 헤더를 보내면 안 된다(문서 명시).
    """
    mime = _MIME_BY_SUFFIX.get(image_path.suffix.lower())
    if not mime:
        logger.warning(f"[higgsfield] 지원하지 않는 이미지 형식: {image_path.suffix}")
        return None

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            r = await client.post(HIGGSFIELD_UPLOAD_URL, headers=_auth_headers(),
                                  json={"content_type": mime})
            if r.status_code != 200:
                logger.warning(f"[higgsfield] 업로드 URL 발급 실패 {r.status_code}: {r.text[:160]}")
                return None
            info = r.json()

        upload_url = info.get("upload_url")
        public_url = info.get("public_url")
        upload_headers = info.get("upload_headers") or {"Content-Type": mime}
        if not upload_url or not public_url:
            logger.warning(f"[higgsfield] 업로드 응답에 URL 없음: {str(info)[:160]}")
            return None

        data = image_path.read_bytes()
        async with httpx.AsyncClient(timeout=120.0) as client:
            pr = await client.put(upload_url, headers=upload_headers, content=data)
            if pr.status_code not in (200, 201, 204):
                logger.warning(f"[higgsfield] 이미지 PUT 실패 {pr.status_code}: {pr.text[:160]}")
                return None

        logger.info(f"[higgsfield] 업로드 완료: {image_path.name} ({len(data)//1024}KB)")
        return public_url

    except Exception as e:
        logger.warning(f"[higgsfield] 업로드 오류: {e}")
        return None


async def higgsfield_image_to_video(
    image_path: Path,
    output_path: Path,
    duration: float = 5.0,
    style: str = "default",
    scene_keyword: str = "",
) -> bool:
    """정지 이미지를 Higgsfield DoP로 영상화한다. 실패 시 False (호출부가 폴백)."""
    if not HIGGSFIELD_API_KEY_ID or not HIGGSFIELD_API_KEY_SECRET:
        logger.debug("[higgsfield] 키 미설정 — 건너뜀")
        return False

    if not image_path.exists() or image_path.stat().st_size < 1024:
        logger.warning(f"[higgsfield] 이미지 없음/너무 작음: {image_path}")
        return False

    if output_path.exists() and output_path.stat().st_size > 4096:
        return True

    image_url = await _upload_image(image_path)
    if not image_url:
        return False

    payload = {
        "prompt": _motion_prompt_for_style(style, scene_keyword),
        "image_url": image_url,
        "enhance_prompt": True,
    }

    try:
        logger.info(f"[higgsfield] DoP({HIGGSFIELD_DOP_TIER}) 제출: {image_path.name}")
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(HIGGSFIELD_DOP_ENDPOINT, headers=_auth_headers(), json=payload)
            if resp.status_code != 200:
                logger.warning(f"[higgsfield] 생성 요청 실패 {resp.status_code}: {resp.text[:200]}")
                return False
            data = resp.json()

        request_id = data.get("request_id")
        status_url = data.get("status_url")
        if not request_id or not status_url:
            logger.warning(f"[higgsfield] request_id/status_url 없음: {str(data)[:160]}")
            return False

        logger.info(f"[higgsfield] 요청 {request_id} 큐 등록")

        for _ in range(60):   # 5초 x 60 = 최대 5분
            await asyncio.sleep(5)
            async with httpx.AsyncClient(timeout=15.0) as client:
                pr = await client.get(status_url, headers=_auth_headers())
                if pr.status_code != 200:
                    logger.warning(f"[higgsfield] 상태조회 실패 {pr.status_code}")
                    return False
                pd = pr.json()

            status = str(pd.get("status", "")).lower()
            if status in ("completed", "succeeded", "success"):
                video_url = _extract_video_url(pd)
                if video_url:
                    logger.info(f"[higgsfield] 요청 {request_id} 완료 - 다운로드")
                    return await _download_higgsfield_video(video_url, output_path)
                logger.warning(f"[higgsfield] 완료됐는데 영상 URL 없음: {str(pd)[:200]}")
                return False
            if status in ("failed", "nsfw", "canceled", "cancelled", "error"):
                logger.warning(f"[higgsfield] 요청 {request_id} {status}: {str(pd.get('error'))[:160]}")
                return False

        logger.warning(f"[higgsfield] 요청 {request_id} 5분 타임아웃")
        return False

    except Exception as e:
        logger.warning(f"[higgsfield] 오류: {e}")
        return False


def _extract_video_url(payload: dict) -> Optional[str]:
    """응답 구조가 모델/버전마다 달라서 흔한 위치를 모두 훑는다."""
    for getter in (
        lambda d: (d.get("video") or {}).get("url"),
        lambda d: (d.get("result") or {}).get("url"),
        lambda d: ((d.get("result") or {}).get("video") or {}).get("url"),
        lambda d: (d.get("output") or {}).get("url"),
        lambda d: d.get("video_url"),
        lambda d: d.get("url"),
    ):
        try:
            url = getter(payload)
            if isinstance(url, str) and url.startswith("http"):
                return url
        except Exception:
            continue
    results = payload.get("results") or payload.get("outputs")
    if isinstance(results, list):
        for item in results:
            if isinstance(item, dict):
                url = item.get("url") or (item.get("video") or {}).get("url")
                if isinstance(url, str) and url.startswith("http"):
                    return url
    return None


async def _download_higgsfield_video(url: str, output_path: Path) -> bool:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        async with httpx.AsyncClient(timeout=120.0, follow_redirects=True) as client:
            resp = await client.get(url)
            resp.raise_for_status()
        output_path.write_bytes(resp.content)
        size_mb = output_path.stat().st_size / 1_048_576
        logger.info(f"[higgsfield] 저장 {output_path.name} ({size_mb:.1f}MB)")
        return output_path.stat().st_size > 4096
    except Exception as e:
        logger.warning(f"[higgsfield] 다운로드 오류: {e}")
        return False