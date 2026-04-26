#[BC] MARKER v1
# [BB] MARKER v1
# [AY] MARKER v1
# [AZ] MARKER v1
# [AW] MARKER v1
# [AU] MARKER v1
# [AL] MARKER v1
# [AK] MARKER v1
# [AJ] MARKER v1
# [AI-pack2] MARKER v1
"""
LongForm Factory - FFmpeg Worker v15.69.0 (안정화·운영개선)
롱폼/숏폼 자동화 영상 제작 서비스

주요 기능:
- Pexels/Pixabay 영상 자산 검색 및 다운로드
- FFmpeg 기반 영상 합성 (장편/숏폼)
- 썸네일 생성 및 자막 처리
- 배경음악 믹싱
"""

import os
# [AI-1] MARKER v1
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

from fastapi import FastAPI, HTTPException, BackgroundTasks, Header, Depends, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field
import httpx
import aiofiles
try:
    from redis import asyncio as aioredis
    _REDIS_AVAILABLE = True
except ImportError:
    _REDIS_AVAILABLE = False
import secrets as _secrets
from PIL import Image, ImageDraw, ImageFont
import uvicorn


# ============================================================================
# 로깅 설정
# ============================================================================
import json as _json_log

class _JsonFormatter(logging.Formatter):
    def format(self, record):
        log_obj = {
            "time":  self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "name":  record.name,
            "msg":   record.getMessage(),
        }
        for k in ("job_id","step","error_code"):
            if hasattr(record, k): log_obj[k] = getattr(record, k)
        if record.exc_info:
            log_obj["exc"] = self.formatException(record.exc_info)
        return _json_log.dumps(log_obj, ensure_ascii=False)

_handler = logging.StreamHandler()
_handler.setFormatter(_JsonFormatter())
logging.basicConfig(level=logging.INFO, handlers=[_handler])
logger = logging.getLogger(__name__)

def _log(level, msg, job_id=None, step=None, error_code=None, exc_info=False):
    extra = {}
    if job_id:     extra["job_id"]     = job_id
    if step:       extra["step"]       = step
    if error_code: extra["error_code"] = error_code
    getattr(logger, level)(msg, extra=extra, exc_info=exc_info)

def _pick_xfade_transition(idx: int = 0) -> str:
    """[O] 씬 인덱스 기반 또는 랜덤으로 xfade transition 타입 선택."""
    if not TRANSITION_POOL:
        return "fade"
    if TRANSITION_RANDOMIZE:
        import random
        return random.choice(TRANSITION_POOL)
    return TRANSITION_POOL[idx % len(TRANSITION_POOL)]

# ==================== [Y2] 도메인 키워드 치환 + 부정 필터 ====================
# 전문용어는 Pexels 가 이해하는 표현으로 자동 치환
DOMAIN_KEYWORD_MAP = {
    # [BR-1] MARKER v6
    # [BR-1] BP 한국어 항목 제거 (수렴 원인) — 이하 위성·우주·금융 등만 유지
    # 위성·우주
    "cubesat": "nanosatellite small satellite space",
    "cube sat": "nanosatellite small satellite space",
    "큐브샛": "nanosatellite small satellite space",
    "큐브셋": "nanosatellite small satellite space",

    # ─── 항공우주 시험 장비 (실제 equipment 영상 확보) ───
    "진공": "vacuum chamber laboratory equipment",
    "진공 시험": "vacuum chamber thermal vacuum testing spacecraft",
    "진공 챔버": "vacuum chamber thermal vacuum testing spacecraft",
    "vacuum": "vacuum chamber laboratory equipment",
    "vacuum test": "vacuum chamber thermal vacuum testing spacecraft",
    "vacuum chamber": "vacuum chamber thermal vacuum testing spacecraft",
    "thermal vacuum": "vacuum chamber thermal vacuum testing spacecraft",

    "진동": "vibration testing shaker table laboratory",
    "진동 시험": "vibration testing shaker table aerospace",
    "vibration": "vibration testing shaker table laboratory",
    "vibration test": "vibration testing shaker table aerospace",
    "shaker": "vibration testing shaker table aerospace",

    "열": "thermal chamber testing temperature laboratory",
    "열 시험": "thermal chamber testing temperature satellite",
    "thermal": "thermal chamber testing temperature laboratory",
    "thermal test": "thermal chamber testing temperature satellite",

    "방사선": "radiation testing laboratory shielding aerospace",
    "방사선 시험": "radiation testing laboratory shielding aerospace",
    "radiation": "radiation testing laboratory shielding aerospace",
    "radiation test": "radiation testing laboratory shielding aerospace",

    "emc": "EMC testing anechoic chamber electronics",
    "전자파": "EMC testing anechoic chamber electronics",

    "클린룸": "clean room spacecraft assembly white suit",
    "clean room": "clean room spacecraft assembly white suit",
    "cleanroom": "clean room spacecraft assembly white suit",
    "조립": "clean room spacecraft assembly engineer",
    "assembly": "clean room spacecraft assembly engineer",

    "환경 시험": "environmental testing aerospace laboratory equipment",
    "environmental test": "environmental testing aerospace laboratory equipment",

    "인증": "certification engineer laboratory documentation",
    "certification": "certification engineer laboratory documentation",

    "검증": "engineer inspecting spacecraft laboratory",
    "verification": "engineer inspecting spacecraft laboratory",
    "validation": "engineer inspecting spacecraft laboratory",

    "시험": "aerospace testing laboratory equipment engineer",
    "test": "aerospace testing laboratory equipment engineer",

    # 설계 단계
    "설계": "engineering blueprint CAD design aerospace",
    "design": "engineering blueprint CAD design aerospace",
    "blueprint": "engineering blueprint CAD design aerospace",
    "cad": "engineering blueprint CAD design aerospace",

    # 제조 단계
    "제조": "satellite manufacturing factory precision",
    "manufacturing": "satellite manufacturing factory precision",
    "생산": "satellite manufacturing factory precision",
    "production": "satellite manufacturing factory precision",

    # 추상·기술 용어 → 시각화 가능한 영상
    "quantum": "optical fiber laser laboratory",
    "quantum optical": "optical fiber laser laboratory equipment",
    "ai": "server data center hardware",
    "artificial intelligence": "server data center hardware robot",
    "machine learning": "computer neural network visualization",
    # 일반 추상 → 실제 장면
    "engineering": "engineer working blueprint laboratory",
    "design phase": "engineering blueprint CAD design",
    "design stage": "engineering blueprint CAD design",
    "testing phase": "laboratory testing equipment scientist",
    "assembly": "clean room assembly engineer",
    "verification": "engineer inspecting equipment laboratory",
    "launch": "rocket launch orange flame space",
    "orbit": "earth orbit satellite from space",
    "satellite performance": "satellite orbit space earth view",
    "satellite details": "satellite construction engineer clean room",
    "space economy": "satellite industry manufacturing",
    "future of space industry": "rocket launch earth orbit future",

    # ─── [AM-1] 추상 단어 → 구체 시각 객체 (Pexels 텍스트 영상 회피) ───
    "concept": "satellite nanosatellite spacecraft clean room engineer",
    "concept design": "satellite model spacecraft hardware engineer",
    "mission": "astronaut spacecraft earth orbit rocket",
    "components": "electronic circuit board microchip close up",
    "system": "control room monitors screens technology",
    "systems": "control room monitors screens technology",
    "detailed": "technician inspecting precision instrument",
    "design": "spacecraft model hardware engineer lab",
    "testing": "laboratory scientist equipment measurement",
    "verification": "scientist lab instrument checking",
    "validation": "lab technician testing equipment",
    "implementation": "satellite hardware assembly engineer gloves",
    "development": "laboratory researcher working",
    "analysis": "rocket engine turbine close up laboratory",
    "process": "factory assembly line robot arm",
    "function": "spacecraft engine thruster test",
    "performance": "rocket launch flame trail",
    "quality": "precision instrument gauge measurement",
    "safety": "engineer safety gear laboratory",
    "research": "scientist microscope laboratory",
    "innovation": "satellite orbit earth space rocket",
    "solution": "satellite solar panel space technology",

    # ─── [AF-13] 제작·개발 단계 (영상 매칭 정확도) ───
    "제작": "manufacturing factory assembly production aerospace",
    "제작 단계": "manufacturing factory assembly production aerospace",
    "제작 및 테스트": "manufacturing testing laboratory aerospace engineer",
    "테스트": "laboratory testing equipment engineer aerospace",
    "테스트 단계": "laboratory testing equipment engineer aerospace",
    "개념": "engineer blueprint mission planning diagram",
    "개념 설계": "engineering blueprint CAD design aerospace",
    "개념 단계": "engineer blueprint mission planning diagram",
    "상세": "engineer detailed technical drawing",
    "상세 설계": "engineer detailed technical drawing CAD",
    "상세 설계 단계": "engineer detailed technical drawing CAD",
    "부품": "electronic components circuit board aerospace parts",
    "부품 선택": "electronic components circuit board aerospace parts",
    "시스템": "spacecraft system integration engineer laboratory",
    "시스템 구성": "spacecraft system integration engineer laboratory",
    "시스템 통합": "spacecraft system integration engineer laboratory",
    "완료": "engineer laboratory inspection aerospace",
    "정의": "satellite orbit mission planning spacecraft",
    "목표": "rocket launch mission satellite space",
    "기능": "spacecraft function engineer laboratory",
    "임무": "satellite mission launch spacecraft planning",
    "명확히": "satellite hardware engineer inspection",
    "단계": "engineer workflow process diagram",
    "첫째": "number one sign",
    "둘째": "number two sign",
    "셋째": "number three sign",

    # ─── 비용·경제·시장·돈 (Pexels 가 town·village 로 해석하는 문제 방지) ───
    "cost": "money dollar calculator budget chart",
    "비용": "money dollar calculator budget chart",
    "price": "money dollar calculator price tag",
    "가격": "money dollar calculator price tag",
    "budget": "money dollar calculator budget chart",
    "예산": "money dollar calculator budget chart",
    "economy": "stock market chart business finance",
    "경제": "stock market chart business finance",
    "market": "stock market trading chart screen",
    "시장": "stock market trading chart screen",
    "revenue": "money growth chart business profit",
    "매출": "money growth chart business profit",
    "finance": "money growth chart business bank",
    "재정": "money growth chart business bank",
    "investment": "money stock chart investor business",
    "투자": "money stock chart investor business",
    "profit": "money growth chart business profit",
    "수익": "money growth chart business profit",
    "billion": "money stack cash finance",
    "million": "money stack cash finance",
    "조원": "money stack cash finance",
    "억원": "money stack cash finance",
    "dollar": "money dollar cash bill",
    "달러": "money dollar cash bill",
    "won": "money cash korean currency",
    "원": "money cash currency bill",

    # ─── 산업·비즈니스 (town/village 방지) ───
    "space industry": "rocket launch satellite factory manufacturing",
    "우주 산업": "rocket launch satellite factory manufacturing",
    "industry": "factory manufacturing industrial machinery",
    "산업": "factory manufacturing industrial machinery",
    "business": "office meeting corporate professional",
    "비즈니스": "office meeting corporate professional",
    "startup": "office team laptop computer meeting",
    "스타트업": "office team laptop computer meeting",

    # 부정 키워드 (검색 결과 필터)
}

NEGATIVE_TERMS = [
    # 기술 영상에 방해되는 일반 요소
    "toy", "cartoon", "animation", "animated", "illustration",
    "drawing", "clipart", "plastic toy", "puzzle cube",
    "rubik", "rubiks", "rubik's",
]

# 주제 맥락 부정 키워드 (키워드 확장 결과에 따라 동적 적용)
# "비용/경제/산업" 맥락에 등장하면 제외할 태그
BUSINESS_NEGATIVE_TERMS = [
    "village", "suburb", "residential", "countryside", "farm",
    "rural", "traditional village", "old town", "vintage house",
    "tourism", "tourist", "travel destination",
]


def _expand_domain_keyword(kw: str) -> str:
    """도메인 용어 → Pexels 친화적 구문으로 치환."""
    # [BQ-2] MARKER v5
    if not kw:
        return kw
    lower = kw.lower().strip()
    # 정확히 일치
    if lower in DOMAIN_KEYWORD_MAP:
        return DOMAIN_KEYWORD_MAP[lower]
    # 부분 포함 치환 (단어 단위)
    for key, val in DOMAIN_KEYWORD_MAP.items():
        if key in lower:
            replaced = lower.replace(key, val)
            # [BQ-2] 한국어(AC00-D7AF) 잔류면 치환값만 사용
            has_hangul = any(0xAC00 <= ord(c) <= 0xD7AF for c in replaced)
            if has_hangul:
                return val
            return replaced
    # [BR-2] MARKER v7
    # [BR-2] 한국어 포함이고 매핑 없으면 한국어만 스트립하고 영어 토큰 반환
    if any(0xAC00 <= ord(c) <= 0xD7AF for c in kw):
        ascii_only = "".join(c for c in kw if ord(c) < 128).strip()
        # 공백 정리
        while "  " in ascii_only:
            ascii_only = ascii_only.replace("  ", " ")
        ascii_only = ascii_only.strip()
        if len(ascii_only.split()) >= 2:
            return ascii_only
        return ""  # 영어 토큰 1개 이하면 호출측 fallback
    return kw


def _is_negative(video_info: dict, context_keyword: str = "") -> bool:
    """Pexels/Pixabay 응답 객체 내 negative term 포함 여부.
    context_keyword 에 business/money 맥락이 있으면 village 류도 제외."""
    text = " ".join(str(v).lower() for v in [
        video_info.get("user", {}).get("name", "") if isinstance(video_info.get("user"), dict) else "",
        video_info.get("tags", ""),
        video_info.get("url", ""),
        " ".join(video_info.get("tags", [])) if isinstance(video_info.get("tags"), list) else "",
    ])
    # 기본 부정 키워드
    if any(neg in text for neg in NEGATIVE_TERMS):
        return True
    # 비용·비즈니스 맥락이면 village/rural 류도 차단
    ctx = (context_keyword or "").lower()
    is_biz = any(b in ctx for b in ["money", "dollar", "budget", "market", "chart",
                                      "business", "office", "factory", "industry"])
    if is_biz and any(neg in text for neg in BUSINESS_NEGATIVE_TERMS):
        return True
    return False



# ==================== [P] Fallback 비주얼 생성기 ====================
FALLBACK_COLOR_POOL = [
    # (top_hex, bottom_hex, text_color)
    ("#1a2a6c", "#b21f1f", "#ffffff"),  # 딥블루 → 크림슨
    ("#0f2027", "#2c5364", "#e0f7fa"),  # 블랙블루 → 시안
    ("#134e5e", "#71b280", "#ffffff"),  # 틸 → 민트
    ("#c94b4b", "#4b134f", "#fff1f1"),  # 레드 → 퍼플
    ("#ff512f", "#dd2476", "#ffffff"),  # 오렌지 → 핑크
    ("#2c3e50", "#4ca1af", "#f0f8ff"),  # 슬레이트 → 시안
    ("#11998e", "#38ef7d", "#0a2e24"),  # 에메랄드
    ("#8e2de2", "#4a00e0", "#ffffff"),  # 퍼플 그라디언트
    ("#f953c6", "#b91d73", "#ffffff"),  # 핑크 그라디언트
    ("#ee0979", "#ff6a00", "#fff3e0"),  # 선셋
]


def _hex_to_ass_bgr(hex_color: str) -> str:
    """#RRGGBB → ASS &HAABBGGRR& (알파 00)"""
    h = hex_color.lstrip("#")
    if len(h) != 6:
        h = "ffffff"
    r, g, b = h[0:2], h[2:4], h[4:6]
    return f"&H00{b}{g}{r}&".upper()


# [AJ-1/2] intro + outro card generators
INTRO_ENABLED = os.getenv("INTRO_ENABLED", "false").lower() in ("1","true","yes","on")
OUTRO_ENABLED = os.getenv("OUTRO_ENABLED", "false").lower() in ("1","true","yes","on")
INTRO_DURATION = float(os.getenv("INTRO_DURATION", "1.5"))
OUTRO_DURATION = float(os.getenv("OUTRO_DURATION", "2.0"))
INTRO_BG_COLOR = os.getenv("INTRO_BG_COLOR", "#0B1E3F")  # deep blue
OUTRO_BG_COLOR = os.getenv("OUTRO_BG_COLOR", "#0B1E3F")
OUTRO_CTA_TEXT = os.getenv("OUTRO_CTA_TEXT", "구독 & 좋아요")


def _make_intro_clip(title: str, output_path: Path, resolution: str = "1920x1080") -> bool:
    """[AJ-1] 1.5s intro card - solid color + title text fade-in."""
    try:
        W, H = [int(x) for x in resolution.lower().split("x")]
        font = "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc"
        if not Path(font).exists():
            font = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
        title_safe = (title or "").replace("\'", "").replace(":", "")[:60]
        fs = int(H * 0.08)
        # Use color source + drawtext with alpha fade
        filter_expr = (
            f"color=c={INTRO_BG_COLOR}:size={W}x{H}:duration={INTRO_DURATION}:rate=30,"
            f"drawtext=fontfile='{font}':text='{title_safe}':fontsize={fs}:"
            f"fontcolor=white:x=(w-text_w)/2:y=(h-text_h)/2:"
            f"alpha='if(lt(t,0.3),t/0.3,if(gt(t,{INTRO_DURATION-0.3:.2f}),max(0,1-(t-{INTRO_DURATION-0.3:.2f})/0.3),1))'"
        )
        cmd = [
            "ffmpeg", "-y", "-f", "lavfi",
            "-i", filter_expr,
            "-t", str(INTRO_DURATION),
            "-c:v", "libx264", "-preset", "ultrafast", "-pix_fmt", "yuv420p",
            "-r", "30", str(output_path),
        ]
        return run_ffmpeg_command(cmd, timeout=30.0) and output_path.exists()
    except Exception as e:
        logger.warning(f"[AJ-1] intro 생성 실패: {e}")
        return False


def _make_outro_clip(output_path: Path, resolution: str = "1920x1080") -> bool:
    """[AJ-2] 2s outro - CTA card fade-in/out."""
    try:
        W, H = [int(x) for x in resolution.lower().split("x")]
        font = "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc"
        if not Path(font).exists():
            font = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
        cta = OUTRO_CTA_TEXT.replace("\'", "")[:40]
        fs = int(H * 0.07)
        filter_expr = (
            f"color=c={OUTRO_BG_COLOR}:size={W}x{H}:duration={OUTRO_DURATION}:rate=30,"
            f"drawtext=fontfile='{font}':text='{cta}':fontsize={fs}:"
            f"fontcolor=white:x=(w-text_w)/2:y=(h-text_h)/2:"
            f"alpha='if(lt(t,0.4),t/0.4,if(gt(t,{OUTRO_DURATION-0.4:.2f}),max(0,1-(t-{OUTRO_DURATION-0.4:.2f})/0.4),1))'"
        )
        cmd = [
            "ffmpeg", "-y", "-f", "lavfi",
            "-i", filter_expr,
            "-t", str(OUTRO_DURATION),
            "-c:v", "libx264", "-preset", "ultrafast", "-pix_fmt", "yuv420p",
            "-r", "30", str(output_path),
        ]
        return run_ffmpeg_command(cmd, timeout=30.0) and output_path.exists()
    except Exception as e:
        logger.warning(f"[AJ-2] outro 생성 실패: {e}")
        return False


def _make_fallback_clip(scene_index: int, duration_sec: float, output_path: Path,
                        keyword: str = "", description: str = "",
                        resolution: str = "1920x1080") -> bool:
    """[P] 자산 없을 때 그라디언트 + 키워드 카드 + 슬로우 zoompan 클립 생성."""
    try:
        w, h = resolution.lower().split("x")
        W, H = int(w), int(h)
    except Exception:
        W, H = 1920, 1080

    top, bot, text_col = FALLBACK_COLOR_POOL[scene_index % len(FALLBACK_COLOR_POOL)]
    kw_size = max(48, int(H * 0.08))
    desc_size = max(28, int(H * 0.030))
    # [AH-5] Use Korean description as display text; NEVER show English keyword.
    desc = (description or "").strip().replace("\n", " ").replace("'", "")[:80]
    # Check for Hangul presence to decide whether to draw text at all
    _has_hangul = any("\uac00" <= ch <= "\ud7a3" for ch in desc)
    kw = desc if _has_hangul else ""  # [AH-5] MARKER v1

    # 그라디언트 배경 → 키워드 → 부제 → zoompan 으로 완성
    # ffmpeg: color src 2개 + vstack + overlay 대신, gradients filter 사용
    # 단순하게: color1 로 전체 채우고 radial/linear 그라디언트는 drawbox + geq 복잡하니
    # 여기선 "color=top:half" "color=bot:half" vstack 으로 2색 split
    # 더 나은 옵션: gradients filter (ffmpeg 5+) → c0=top:c1=bot

    # gradients filter 가 있으면 가장 깔끔
    filter_expr = (
        f"color=black:size={W}x{H}:duration={duration_sec:.2f}:rate=30,"
        f"geq='"
        f"r=if(gte(Y,H/2), {int(bot[1:3], 16)}, {int(top[1:3], 16)}-("
        f"({int(top[1:3], 16)}-{int(bot[1:3], 16)})*Y/(H/2))):"
        f"g=if(gte(Y,H/2), {int(bot[3:5], 16)}, {int(top[3:5], 16)}-("
        f"({int(top[3:5], 16)}-{int(bot[3:5], 16)})*Y/(H/2))):"
        f"b=if(gte(Y,H/2), {int(bot[5:7], 16)}, {int(top[5:7], 16)}-("
        f"({int(top[5:7], 16)}-{int(bot[5:7], 16)})*Y/(H/2)))'"
    )

    # 텍스트 오버레이 + 슬로우 zoompan
    # drawtext 로 키워드 + 부제
    font_file = "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc"
    if not Path(font_file).exists():
        # Noto 없으면 DejaVu fallback
        font_file = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"

    # [AH-5] Only draw text if we have Korean text; otherwise clean gradient.
    if kw:
        # Shorten for center display
        display = kw[:40]
        filter_full = (
            f"{filter_expr},"
            f"drawtext=fontfile='{font_file}':text='{display}':"
            f"fontsize={desc_size}:fontcolor={text_col}@0.75:"
            f"x=(w-text_w)/2:y=h-th-{int(H*0.08)}:"
            f"box=0:shadowcolor=black@0.5:shadowx=2:shadowy=2"
        )
    else:
        # Clean fallback — no text, pure gradient + zoom
        filter_full = filter_expr

    # slow zoompan 효과: z 는 천천히 증가, x/y 는 center 고정
    zp_frames = max(30, int(duration_sec * 30))
    filter_full += (
        f",zoompan=z='min(zoom+0.0008,1.08)':"
        f"x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)':"
        f"d={zp_frames}:s={W}x{H}:fps=30"
    )

    cmd = [
        "ffmpeg", "-y",
        "-f", "lavfi",
        "-i", filter_expr,  # 색 생성용 lavfi 입력
        "-t", str(duration_sec),
        "-vf", filter_full.replace(filter_expr + ",", "", 1),
        "-c:v", "libx264", "-preset", "ultrafast", "-pix_fmt", "yuv420p",
        "-r", "30",
        str(output_path)
    ]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if proc.returncode == 0 and output_path.exists() and output_path.stat().st_size > 1024:
            logger.info(f"[P] fallback 비주얼: {output_path.name} ({keyword[:20]}, {top}→{bot})")
            return True
        logger.warning(f"[P] fallback 생성 실패: {proc.stderr[-300:]}")
        return False
    except Exception as e:
        logger.error(f"[P] fallback 예외: {e}")
        return False



# [AE] MARKER v1
# [AU-1] Resolution config - 1080p / 4K support
OUTPUT_RESOLUTION = os.getenv("OUTPUT_RESOLUTION", "1920x1080")  # or "3840x2160" for 4K
VF_W, VF_H = [int(x) for x in OUTPUT_RESOLUTION.split("x")]
VIDEO_CRF = int(os.getenv("VIDEO_CRF", "15"))  # [AW-2] 18→15 higher quality
VIDEO_PRESET = os.getenv("VIDEO_PRESET", "medium")  # [AW-2] slow=최고 fast=빠름 medium=균형
# [AU-3] Template presets
VIDEO_TEMPLATE = os.getenv("VIDEO_TEMPLATE", "info")  # info|news|edu|ad|story
TEMPLATE_CONFIGS = {
    "info":   {"saturation": 1.25, "contrast": 1.10, "vignette": "PI/5",  "fade_dur": 0.20},
    "news":   {"saturation": 1.05, "contrast": 1.15, "vignette": "PI/6",  "fade_dur": 0.12},
    "edu":    {"saturation": 1.15, "contrast": 1.08, "vignette": "PI/5",  "fade_dur": 0.18},
    "ad":     {"saturation": 1.40, "contrast": 1.22, "vignette": "PI/4",  "fade_dur": 0.25},
    "story":  {"saturation": 1.20, "contrast": 1.15, "vignette": "PI/4",  "fade_dur": 0.30},
}
TEMPLATE = TEMPLATE_CONFIGS.get(VIDEO_TEMPLATE, TEMPLATE_CONFIGS["info"])
# [AU-5] Watermark config
# [AX] Watermark disabled permanently - 그림 오버레이 사용 안함
WATERMARK_PATH = ""
WATERMARK_OPACITY = 0.0

ENABLE_SCENE_LAYOUT = os.getenv("ENABLE_SCENE_LAYOUT", "false").lower()  # [AF-12] MARKER v1 in ("1","true","yes","on")  # [AF] MARKER v1

# 5 scene-layout templates for keyword overlay variation
SCENE_LAYOUTS = [
    # 0 NONE - clean scene
    None,
    # 1 TOP_LEFT - small badge upper-left
    {"x": "60", "y": "50", "size": 44, "color": "white", "box": True, "box_alpha": 0.45, "fin": 0.3, "fout": 0.4},
    # 2 TOP_CENTER - medium banner
    {"x": "(w-text_w)/2", "y": "70", "size": 56, "color": "#FFE27A", "box": True, "box_alpha": 0.35, "fin": 0.35, "fout": 0.5},
    # 3 BOTTOM_LEFT - subtitle-height bottom-left accent
    {"x": "80", "y": "h-220", "size": 48, "color": "#B6EDF2", "box": False, "box_alpha": 0.0, "fin": 0.3, "fout": 0.45},
    # 4 DIAGONAL_LARGE - hero center-upper big keyword
    {"x": "(w-text_w)/2", "y": "h*0.3", "size": 88, "color": "white", "box": False, "box_alpha": 0.0, "fin": 0.5, "fout": 0.6},
]

SCENE_ACCENT_COLORS = ["#FFE27A", "#B6EDF2", "#FFB4A2", "#C6B6FF", "#8CE6B1"]


def _escape_drawtext(txt: str) -> str:
    if not txt:
        return ""
    return (
        txt.replace("\\", "\\\\")
           .replace("'", "\u2019")
           .replace(":", "\\:")
           .replace("%", "\\%")
    )


def _build_keyword_overlay(keyword: str, scene_idx: int, sub_dur: float) -> str:
    """[AE] drawtext filter rotating through 5 layouts."""
    if ENABLE_SCENE_LAYOUT not in ("1", "true", "yes", "on") or not keyword:
        return ""
    lay = SCENE_LAYOUTS[scene_idx % len(SCENE_LAYOUTS)]
    if lay is None:
        return ""
    safe_kw = _escape_drawtext(keyword.strip())
    if not safe_kw:
        return ""
    fin = float(lay.get("fin", 0.3))
    fout = float(lay.get("fout", 0.4))
    end_fadeout = max(0.1, sub_dur - fout)
    alpha = (
        "if(lt(t," + f"{fin:.2f}" + "),t/" + f"{fin:.2f}" + ","
        "if(gt(t," + f"{end_fadeout:.2f}" + "),max(0,1-(t-" + f"{end_fadeout:.2f}" + ")/" + f"{fout:.2f}" + "),1))"
    )
    # [AF-1] per-scene accent color rotation (override lay["color"] with palette)
    try:
        palette = SCENE_ACCENT_COLORS
        accent = palette[scene_idx % len(palette)] if palette else lay["color"]
    except Exception:
        accent = lay["color"]
    parts = [
        "drawtext=text='" + safe_kw + "'",
        "x=" + str(lay["x"]),
        "y=" + str(lay["y"]),
        "fontsize=" + str(lay["size"]),
        "fontcolor=" + str(accent),
        "fontfile=/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc",
        "alpha='" + alpha + "'",
    ]
    if lay.get("box"):
        parts += [
            "box=1",
            "boxcolor=black@" + str(lay["box_alpha"]),
            "boxborderw=12",
        ]
    return ":".join(parts)


def _compute_subtitle_style(resolution: str = "1920x1080") -> tuple:
    """해상도 문자열에서 자막 크기·마진 계산. (font_size, margin_v) 반환."""
    try:
        w, h = resolution.lower().split("x")
        height = int(h)
    except Exception:
        height = 1080

    if SUBTITLE_FONT_SIZE > 0:
        font_size = SUBTITLE_FONT_SIZE
    else:
        font_size = max(16, int(height * SUBTITLE_FONT_SIZE_RATIO))

    if SUBTITLE_MARGIN_V > 0 and SUBTITLE_MARGIN_V != 30:  # 기본 40 아니면 사용자 명시
        margin_v = SUBTITLE_MARGIN_V
    else:
        margin_v = max(20, int(height * SUBTITLE_MARGIN_RATIO))

    return font_size, margin_v







# ============================================================================
# 열거형 정의
# ============================================================================
class VideoMode(str, Enum):
    """영상 제작 모드"""
    LONGFORM = "longform"  # 1920x1080 가로형
    SHORTFORM = "shortform"  # 1080x1920 세로형
    MUSIC_VIDEO = "music_video"  # BGM + 자막 뮤직비디오


class JobStatus(str, Enum):
    """작업 상태 [v15.59.0 확장]"""
    PENDING              = "pending"
    QUEUED               = "queued"
    TTS_GENERATING       = "tts_generating"
    DOWNLOADING_ASSETS   = "downloading_assets"
    SUBTITLE_CREATING    = "subtitle_creating"
    PROCESSING           = "processing"
    RENDERING            = "rendering"
    THUMBNAIL_GENERATING = "thumbnail_generating"
    COMPLETED            = "completed"
    FAILED               = "failed"
    CANCELLED            = "cancelled"


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
SCENE_LEAD_SEC      = float(_rhythm_os.getenv("SCENE_LEAD_SEC", "0.0"))  # [AH-1] MARKER v1 AH-2
BGM_AUTO_DUCK       = _rhythm_os.getenv("BGM_AUTO_DUCK", "true").lower() in ("1","true","yes","on")
BGM_DUCK_DB         = float(_rhythm_os.getenv("BGM_DUCK_DB", "15"))  # [AF-5] BGM sidechain 감쇠 (dB)      # [AD] 씬이 자막보다 먼저 나오는 버퍼
UNIFIED_TIMELINE    = _rhythm_os.getenv("UNIFIED_TIMELINE", "true").lower() in ("1","true","yes","on")   # [AD] MARKER v1
SCENE_MIN_SEC       = float(_rhythm_os.getenv("SCENE_MIN_SEC", "2.0"))        # 씬 최소 길이
SCENE_MAX_SEC       = float(_rhythm_os.getenv("SCENE_MAX_SEC", "2.5"))  # [BF] 더 쪼개기        # 씬 최대 길이 (초과 시 분할)
SUBTITLE_MAX_CHARS  = int(_rhythm_os.getenv("SUBTITLE_MAX_CHARS", "15"))      # 자막 한 줄 최대 글자

# [N] 자막 스타일
SUBTITLE_FONT_NAME   = _rhythm_os.getenv("SUBTITLE_FONT_NAME", "Noto Sans CJK KR")
SUBTITLE_FONT_FILE   = _rhythm_os.getenv("SUBTITLE_FONT_FILE", "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc")
SUBTITLE_FONT_SIZE   = int(_rhythm_os.getenv("SUBTITLE_FONT_SIZE", "0"))             # 0=비율 자동 계산, >0 고정 px
SUBTITLE_FONT_SIZE_RATIO = float(_rhythm_os.getenv("SUBTITLE_FONT_SIZE_RATIO", "0.018"))  # 높이×0.03 (1080p→32, 720p→22)
SUBTITLE_MARGIN_RATIO = float(_rhythm_os.getenv("SUBTITLE_MARGIN_RATIO", "0.030"))    # 높이×0.04 (1080p→43, 720p→29)
SUBTITLE_BOLD        = int(_rhythm_os.getenv("SUBTITLE_BOLD", "1"))                  # 0/1
SUBTITLE_FONT_COLOR  = _rhythm_os.getenv("SUBTITLE_FONT_COLOR", "&H00FFFFFF&")       # 흰색 기본 (BGR + AA)
SUBTITLE_OUTLINE_COLOR = _rhythm_os.getenv("SUBTITLE_OUTLINE_COLOR", "&H00000000&")  # 검정 테두리
SUBTITLE_BACK_COLOR  = _rhythm_os.getenv("SUBTITLE_BACK_COLOR", "&H80000000&")       # 반투명 검정 박스
SUBTITLE_BORDER_STYLE = int(_rhythm_os.getenv("SUBTITLE_BORDER_STYLE", "1"))         # 1=outline, 3=opaque box
SUBTITLE_OUTLINE_PX  = int(_rhythm_os.getenv("SUBTITLE_OUTLINE_PX", "2"))            # outline 두께
SUBTITLE_SHADOW_PX   = int(_rhythm_os.getenv("SUBTITLE_SHADOW_PX", "1"))
SUBTITLE_MARGIN_V    = int(_rhythm_os.getenv("SUBTITLE_MARGIN_V", "30"))             # 하단 여백
SUBTITLE_ALIGNMENT   = int(_rhythm_os.getenv("SUBTITLE_ALIGNMENT", "2"))             # 2=하단중앙

# [O] Transition 다양화 — xfade 타입 pool (None 넘기면 기본 fade)
# 사용 가능: fade, wiperight, wipeleft, slideup, slidedown, circleopen, circleclose,
#            radial, pixelize, dissolve, smoothleft, smoothright, diagbl, diagbr
TRANSITION_POOL = [t.strip() for t in _rhythm_os.getenv(
    "TRANSITION_POOL",
    "fade,wiperight,slideup,circleopen,radial,pixelize,dissolve,smoothleft,smoothright,diagbl,diagbr,coverright,rectcrop"
).split(",") if t.strip()]
TRANSITION_RANDOMIZE = _rhythm_os.getenv("TRANSITION_RANDOMIZE", "true").lower() in ("true", "1", "yes")
FADE_DUR = float(_rhythm_os.getenv("FADE_DUR", "0.18"))                          # xfade 기본 길이
FADE_DUR_MIN = float(_rhythm_os.getenv("FADE_DUR_MIN", "0.15"))                       # [Z] 랜덤 최소
FADE_DUR_MAX = float(_rhythm_os.getenv("FADE_DUR_MAX", "0.30"))                       # [Z] 랜덤 최대
FADE_DUR_RANDOMIZE = _rhythm_os.getenv("FADE_DUR_RANDOMIZE", "true").lower() in ("true", "1", "yes")

# [R] 모션 절제 (눈에 안 띄는 정도)
KENBURNS_MAX_ZOOM   = float(_rhythm_os.getenv("KENBURNS_MAX_ZOOM", "1.06"))      # 100→106% (기존 1.6→1.06)
KENBURNS_PAN_PX     = int(_rhythm_os.getenv("KENBURNS_PAN_PX", "30"))            # 좌우 이동 px
KENBURNS_TILT_PX    = int(_rhythm_os.getenv("KENBURNS_TILT_PX", "16"))           # 상하 이동 px

# [v15.60.0] Narration-First Timeline Engine ENV
PAUSE_COMMA_MS          = int(float(_rhythm_os.getenv("PAUSE_COMMA_MS", "180")))
PAUSE_SENTENCE_MS       = int(float(_rhythm_os.getenv("PAUSE_SENTENCE_MS", "420")))
SCENE_HEAD_PAD_SEC      = float(_rhythm_os.getenv("SCENE_HEAD_PAD_SEC", "0.15"))
SCENE_TAIL_PAD_SEC      = float(_rhythm_os.getenv("SCENE_TAIL_PAD_SEC", "0.35"))
BGM_VOLUME_DEFAULT      = float(_rhythm_os.getenv("BGM_VOLUME_DEFAULT", "0.10"))
BGM_VOLUME_DURING_VOICE = float(_rhythm_os.getenv("BGM_VOLUME_DURING_VOICE", "0.045"))
NTL_ENABLED             = _rhythm_os.getenv("NTL_ENABLED", "true").lower() in ("true", "1", "yes")

# 장면 길이 분산
SCENE_LEN_VARIANCE = float(_rhythm_os.getenv("SCENE_LEN_VARIANCE", "0.5"))       # ±0.5s 랜덤
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
    duration_seconds: float = Field(..., ge=0.5, le=3600, description="장면 길이(초)")
    description: Optional[str] = Field(None, description="장면 설명")
    asset_url: Optional[str] = Field(None, description="다운로드된 자산 URL")
    asset_type: AssetType = Field(default=AssetType.VIDEO, description="자산 유형")
    # [v15.60.0] Narration-First 확장 필드
    narration: Optional[str] = Field(None, description="씬 나레이션 텍스트")
    visual_intent: Optional[str] = Field(None, description="시각적 의도 (dynamic/calm/dramatic/educational/uplifting)")
    visual_keywords: Optional[List[str]] = Field(default_factory=list, description="비주얼 검색 키워드 목록")
    tone_profile: Optional[str] = Field(None, description="톤 (info/news/edu/ad/story)")
    visual_pacing: Optional[str] = Field(None, description="페이싱 (fast/normal/slow)")
    timing: Optional[Dict[str, float]] = Field(None, description="타임라인 타이밍")
    alt_asset_url: Optional[str] = Field(None, description="[v15.68] 2번째 소스 영상 경로 (서브클립 다양화)")
    alt_keywords: List[str] = Field(default_factory=list, description="[v15.68] 대체 검색 키워드")
    narration_en: Optional[str] = Field(None, description="[v15.69] Kling T2V용 영어 비주얼 프롬프트")


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

# [v15.66.0] 공통 API Key 검증 Depends 함수
def verify_api_key(x_lf_api_key: str = Header(None, alias="X-LF-API-Key")):
    """X-LF-API-Key 헤더 검증. 키 미설정 환경에서는 통과."""
    if LF_API_KEY and x_lf_api_key != LF_API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API Key")
    return x_lf_api_key or ""

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

# ─── 국가명 → 국기 이모지 매핑 v15.67.0 ──────────────────────────────
_COUNTRY_FLAG_MAP: dict = {
    "미국": "🇺🇸", "미합중국": "🇺🇸", "아메리카": "🇺🇸", "usa": "🇺🇸", "us ": "🇺🇸",
    "중국": "🇨🇳", "중화인민공화국": "🇨🇳", "차이나": "🇨🇳", "china": "🇨🇳",
    "일본": "🇯🇵", "일본국": "🇯🇵", "japan": "🇯🇵",
    "한국": "🇰🇷", "대한민국": "🇰🇷", "남한": "🇰🇷", "korea": "🇰🇷",
    "영국": "🇬🇧", "uk": "🇬🇧", "britain": "🇬🇧",
    "독일": "🇩🇪", "germany": "🇩🇪",
    "프랑스": "🇫🇷", "france": "🇫🇷",
    "러시아": "🇷🇺", "russia": "🇷🇺",
    "캐나다": "🇨🇦", "canada": "🇨🇦",
    "인도": "🇮🇳", "india": "🇮🇳",
    "호주": "🇦🇺", "australia": "🇦🇺",
    "유럽": "🇪🇺", "유럽연합": "🇪🇺",
    "이스라엘": "🇮🇱",
    "북한": "🇰🇵",
    "대만": "🇹🇼",
    "이탈리아": "🇮🇹",
    "스페인": "🇪🇸",
    "브라질": "🇧🇷",
    "사우디": "🇸🇦", "사우디아라비아": "🇸🇦",
    "싱가포르": "🇸🇬",
    "우크라이나": "🇺🇦",
    "스웨덴": "🇸🇪",
}

def detect_countries_in_text(text: str) -> list:
    """텍스트에서 국가 감지 → 국기 이모지 리스트 (중복 제거, 순서 유지)"""
    found, seen = [], set()
    tl = text.lower()
    for name, flag in _COUNTRY_FLAG_MAP.items():
        if name.lower() in tl and flag not in seen:
            found.append(flag)
            seen.add(flag)
    return found

def inject_flags_in_word(word: str) -> str:
    """단어에 국가명 포함 시 국기 이모지 앞에 삽입"""
    import re as _re2
    for name, flag in _COUNTRY_FLAG_MAP.items():
        if name.lower() in word.lower() and flag not in word:
            word = _re2.sub(
                _re2.escape(name), flag + name, word, flags=_re2.IGNORECASE, count=1
            )
            break  # 단어 1개에 이모지 1개만
    return word

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
    version="15.67.0"
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
# 동시 영상 생성 제한 — Redis lock 우선, 없으면 global fallback
_CURRENT_JOB: Optional[str] = None

# 작업 상태 저장소 (인메모리 + Redis 이중)
jobs: Dict[str, JobInfo] = {}

# ── Redis 클라이언트 (선택적)
_redis_client = None

async def _get_redis():
    global _redis_client
    if not _REDIS_AVAILABLE:
        return None
    if _redis_client is None:
        try:
            import os as _os
            redis_url = _os.getenv("REDIS_URL", "redis://lf2_redis:6379/0")
            _redis_client = aioredis.from_url(
                redis_url, decode_responses=True, socket_connect_timeout=2)
            await _redis_client.ping()
            logger.info("Redis 연결 성공")
        except Exception as _re:
            logger.warning(f"Redis 미연결 (인메모리 fallback): {_re}")
            _redis_client = None
    return _redis_client

async def _redis_set_job(job_id, status, progress=0, step=None,
                          error_code=None, message=None,
                          output_path=None, thumbnail_path=None, retryable=False):
    import time as _t
    r = await _get_redis()
    if r is None:
        return
    try:
        payload = {
            "job_id": job_id, "status": status,
            "progress": str(round(progress, 1)),
            "updated_at": _t.strftime("%Y-%m-%dT%H:%M:%S"),
        }
        if step:           payload["step"]           = step
        if error_code:     payload["error_code"]     = error_code
        if message:        payload["message"]        = message
        if output_path:    payload["output_path"]    = output_path
        if thumbnail_path: payload["thumbnail_path"] = thumbnail_path
        if retryable:      payload["retryable"]      = "true"
        key = f"lf:job:{job_id}:status"
        await r.hset(key, mapping=payload)
        ttl = 86400 if status in ("completed","failed","cancelled") else 7200
        await r.expire(key, ttl)
    except Exception as _e:
        logger.debug(f"Redis 저장 실패(무시): {_e}")

async def _redis_acquire_lock(job_id, timeout_sec=3600):
    r = await _get_redis()
    if r is None:
        return "noop"
    token = _secrets.token_hex(16)
    result = await r.set(f"lf:job:{job_id}:lock", token, nx=True, ex=timeout_sec)
    return token if result else None

async def _redis_release_lock(job_id, token):
    if token == "noop":
        return
    r = await _get_redis()
    if r is None:
        return
    try:
        script = """
if redis.call('get',KEYS[1])==ARGV[1] then
    return redis.call('del',KEYS[1])
else return 0 end"""
        await r.eval(script, 1, f"lf:job:{job_id}:lock", token)
    except Exception:
        pass


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


# [AL-2+AY] Pexels cache with 1h TTL + job diversification
_PEXELS_CACHE: dict = {}  # key -> (timestamp, data)
_PEXELS_CACHE_MAX = 64
_PEXELS_CACHE_TTL = 3600  # 1 hour

# [AY-C] Global seen URLs — persist across jobs (last 300)
_GLOBAL_SEEN_URLS_FILE = Path("/data/seen_urls.txt")
_GLOBAL_SEEN_URLS: set = set()

def _load_global_seen():
    global _GLOBAL_SEEN_URLS
    try:
        if _GLOBAL_SEEN_URLS_FILE.exists():
            lines = _GLOBAL_SEEN_URLS_FILE.read_text(encoding="utf-8").strip().split("\n")
            _GLOBAL_SEEN_URLS = set(ln.strip() for ln in lines if ln.strip())
    except Exception:
        pass

def _save_global_seen(new_urls: set):
    try:
        _GLOBAL_SEEN_URLS.update(new_urls)
        # Keep last 300 only (FIFO-ish)
        if len(_GLOBAL_SEEN_URLS) > 300:
            _GLOBAL_SEEN_URLS = set(list(_GLOBAL_SEEN_URLS)[-300:])
        _GLOBAL_SEEN_URLS_FILE.parent.mkdir(parents=True, exist_ok=True)
        _GLOBAL_SEEN_URLS_FILE.write_text("\n".join(sorted(_GLOBAL_SEEN_URLS)), encoding="utf-8")
    except Exception:
        pass

_load_global_seen()


async def get_pexels_videos(keyword: str, per_page: int = 5) -> List[Dict[str, Any]]:
    """[BK] 다단어 phrase 검색 결과 부족 시 자동 단축 재검색.
    - full phrase → 결과 < 3개면 앞 3단어 → 여전히 부족하면 앞 2단어로 fallback.
    """
    if not keyword:
        return []
    words = keyword.split()
    # 1차: 원본 phrase
    results = await _get_pexels_videos_raw(keyword, per_page)
    if len(results) >= 3 or len(words) <= 2:
        return results
    # 2차: 앞 3단어
    if len(words) > 3:
        short3 = " ".join(words[:3])
        logger.info(f"[BK] Pexels 결과 부족 ({len(results)}) — '{short3}'로 재검색")
        r3 = await _get_pexels_videos_raw(short3, per_page)
        if len(r3) > len(results):
            results = r3
    if len(results) >= 3 or len(words) < 2:
        return results
    # 3차: 앞 2단어
    short2 = " ".join(words[:2])
    logger.info(f"[BK] 여전히 부족 ({len(results)}) — '{short2}'로 재검색")
    r2 = await _get_pexels_videos_raw(short2, per_page)
    return r2 if len(r2) > len(results) else results


async def _get_pexels_videos_raw(keyword: str, per_page: int = 5) -> List[Dict[str, Any]]:
    """Pexels API에서 영상 검색"""
    if not PEXELS_API_KEY:
        logger.warning("Pexels API 키가 없습니다")
        return []
    
    url = "https://api.pexels.com/videos/search"
    headers = {"Authorization": PEXELS_API_KEY}
    params = {
        "query": keyword,  # [AN] MARKER v1
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


def select_best_video(pexels_videos: List[Dict], pixabay_videos: List[Dict],
                       scene_index: int = 0,
                       exclude_urls: set = None,
                       query_keyword: str = "") -> Optional[str]:
    """[O] 씬 인덱스 라운드로빈으로 다양한 영상 선택 (같은 키워드라도 다른 결과).
    해상도 상위 후보들 중에서 scene_index 로 순환."""
    candidates = []
    
    # [AN-3] Filter videos whose URL/user suggests text overlay content
    _TEXT_BLACKLIST = ("whiteboard", "handwriting", "typography", "text", "chalkboard",
                       "infographic", "presentation", "slide", "sketch", "diagram",
                       "concept", "mindmap", "notes", "writing", "drawing")
    # [BN] MARKER v1
    # 한국 컨텐츠에서 배제할 서양 식별자 (query_keyword 에 asian/korean/seoul 이면 자동 적용)
    _WESTERN_BLACKLIST = ("american-flag", "us-flag", "usa-flag", "star-and-stripes",
                          "american_flag", "us_flag",
                          "britain", "british-flag", "union-jack", "uk-flag",
                          "european-union", "eu-flag",
                          "white-house", "capitol", "buckingham",
                          "trump", "biden", "obama", "clinton",
                          "washington-dc", "london-parliament")
    _is_korean_topic = any(k in (query_keyword or "").lower() for k in
                           ("asian", "korean", "seoul", "korea", "japan", "taiwan"))
    def _has_text_indicator(video: dict) -> bool:
        # Pexels video has "url" (page), sometimes "user" with name
        for field in ("url", "video_pictures"):
            val = video.get(field)
            if isinstance(val, str):
                low = val.lower()
                if any(b in low for b in _TEXT_BLACKLIST):
                    return True
        user = video.get("user", {})
        if isinstance(user, dict):
            uname = (user.get("name") or "").lower()
            if any(b in uname for b in _TEXT_BLACKLIST):
                return True
        # [BN+BN2] 한국 주제 + 서양 국기/장소 → 거절
        _PODIUM_STOCK_TOKENS = ("speaking-at-a-podium", "woman-at-a-podium",
                                "politician-speech", "podium-speech",
                                "news-conference", "press-briefing",
                                "business-woman-speaking", "at-podium",
                                "podium-with-flag")
        if _is_korean_topic:
            for field in ("url", "image"):
                val = video.get(field)
                if isinstance(val, str):
                    low = val.lower().replace("_", "-")
                    if any(b in low for b in _WESTERN_BLACKLIST):
                        return True
                    if any(tok in low for tok in _PODIUM_STOCK_TOKENS):
                        return True
            tags = video.get("tags", "")
            if isinstance(tags, str):
                low = tags.lower()
                if any(b.replace("-", " ") in low for b in _WESTERN_BLACKLIST):
                    return True
        return False

    # Pexels 영상 처리
    for video in pexels_videos:
        if _has_text_indicator(video):
            continue  # [AN] MARKER v1
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
        # [AW-4+BG-2] 4K 우선 + 키워드 매칭 재정렬
        # query 키워드의 명사를 추출해서 URL/page_url 에 포함된 후보 우선
        import re as _rescore
        _query_nouns = []
        try:
            if query_keyword:
                _query_nouns = [w.lower() for w in query_keyword.split() if len(w) >= 3]
        except Exception:
            pass
        def _res_score(c):
            w, h = int(c.get("width", 0) or 0), int(c.get("height", 0) or 0)
            pixels = w * h
            # 해상도 스코어
            res_score = 0
            if h >= 2000: res_score = pixels + 10_000_000
            elif h >= 1400: res_score = pixels + 5_000_000
            elif h >= 1000: res_score = pixels
            else: res_score = pixels - 5_000_000
            # [BG-2] URL·user 이름에 키워드 명사 포함되면 큰 보너스
            url = (c.get("url") or "").lower()
            bonus = 0
            for noun in _query_nouns:
                if noun and len(noun) >= 3 and noun in url:
                    bonus += 2_000_000
            return res_score + bonus
        sorted_cands = sorted(candidates, key=_res_score, reverse=True)
        pool = sorted_cands[:max(1, min(len(sorted_cands), 10))]
        # [AF-14+AY-D] dedupe across scenes + global cross-job dedupe
        excluded = (exclude_urls or set()) | _GLOBAL_SEEN_URLS
        filtered = [c for c in pool if c["url"] not in excluded]
        effective = filtered if filtered else pool  # fall back if all excluded
        picked = effective[(scene_index * 7 + scene_index // 2) % len(effective)]
        logger.info(
            f"영상 선택: {picked['width']}x{picked['height']} "
            f"(idx={scene_index}, pool={len(pool)}/{len(candidates)})"
        )
        return picked["url"]

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


# ========== [v15.69] 키워드 Sanitizer =========================================
_CAMERA_DIRECTIVES = {
    "wide shot","close up","close-up","side angle","panning","zoom in","zoom out",
    "aerial shot","tracking shot","dolly shot","tilt","crane shot","establishing shot",
    "cutaway","overhead","bird eye","bird's eye","bird's-eye","low angle","high angle",
    "slow zoom","fast cut","handheld","steadicam","bokeh","depth of field",
    "wide angle","tight shot","medium shot","long shot","extreme close up","two shot",
    "wide","angle","shot","aerial","zoom","pan","cutaway","handheld",
}

_KO_STOPWORDS = {
    "이","그","저","의","가","은","는","을","를","에","에서","로","으로",
    "와","과","도","만","이다","있다","하다","되다","않다","때","후","전","중",
    "또한","따라서","그리고","하지만","그러나","그래서","즉","곧","이후",
}

def _is_camera_directive(kw: str) -> bool:
    """카메라 방향/기법 키워드 판별"""
    if not kw or not kw.strip():
        return True
    lower = kw.lower().strip()
    if lower in _CAMERA_DIRECTIVES:
        return True
    words = lower.split()
    if len(words) <= 2 and all(w in _CAMERA_DIRECTIVES for w in words):
        return True
    return False


def _sanitize_keyword_for_search(kw: str, narration: str = "", fallback: str = "") -> str:
    """[v15.69] 카메라 디렉티브/빈 키워드를 나레이션 기반 키워드로 복구"""
    if not _is_camera_directive(kw):
        return kw
    if narration:
        import re as _re
        ko_words = _re.findall(r'[가-힣]{2,}', narration)
        en_words = _re.findall(r'[A-Za-z]{3,}', narration)
        useful = [w for w in ko_words if w not in _KO_STOPWORDS][:3]
        if en_words:
            useful = en_words[:3] + useful[:1]
        if useful:
            return " ".join(useful[:3]) + " footage"
    return fallback if fallback else "business technology people"


# ========== END sanitizer ====================================================

def _get_topic_fallback(keyword: str, topic_hint: str = "") -> str:
    """[v15.74.0] 토픽 카테고리 기반 폴백 쿼리."""
    c = (keyword + " " + topic_hint).lower()
    if any(t in c for t in ["economy","finance","stock","bank","market","money","gdp"]):
        return "business finance city"
    if any(t in c for t in ["tech","ai","robot","computer","digital","semiconductor","chip"]):
        return "technology innovation lab"
    if any(t in c for t in ["politic","government","election","parliament","president"]):
        return "government building city"
    if any(t in c for t in ["environment","climate","green","carbon","emission","energy"]):
        return "nature sky environment"
    if any(t in c for t in ["war","military","weapon","defense","missile","drone"]):
        return "military defense aircraft"
    if any(t in c for t in ["health","medical","hospital","doctor","virus","vaccine"]):
        return "hospital medical doctor"
    if any(t in c for t in ["space","satellite","rocket","orbit","launch","nasa"]):
        return "rocket space launch"
    if any(t in c for t in ["korea","seoul","asian","japan","china","tokyo","beijing"]):
        return "asian city urban street"
    return "city street people"

# ============================================================
# [v15.69] Kling T2V 통합
# ============================================================
_KLING_ACCESS_KEY = os.getenv("KLING_ACCESS_KEY", "")
_KLING_SECRET_KEY = os.getenv("KLING_SECRET_KEY", "")
_KLING_BASE_URL = "https://api.klingai.com"
_AI_VIDEO_ENABLED = os.getenv("AI_VIDEO_ENABLED", "false").lower() in ("1","true","yes")
_AI_VIDEO_PROVIDER = os.getenv("AI_VIDEO_PROVIDER", "").lower()

def _kling_jwt() -> str:
    """HS256 JWT 생성 (30분 유효)"""
    try:
        import jwt as _jwt
        now = int(time.time())
        payload = {"iss": _KLING_ACCESS_KEY, "exp": now + 1800, "nbf": now - 5}
        token = _jwt.encode(payload, _KLING_SECRET_KEY, algorithm="HS256")
        return token if isinstance(token, str) else token.decode("utf-8")
    except Exception as e:
        logger.warning(f"[Kling] JWT 생성 실패: {e}")
        return ""


async def generate_kling_video(
    prompt_en: str,
    duration: int,
    scene_id: str,
    output_path: Path,
    aspect_ratio: str = "16:9",
    model: str = "kling-v2.0-std",
    max_wait_sec: float = 300.0,
) -> bool:
    """[v15.69] Kling T2V API로 씬 영상 생성 → output_path에 저장"""
    if not _KLING_ACCESS_KEY or not _KLING_SECRET_KEY:
        logger.warning("[Kling] API 키 미설정 — 스킵")
        return False
    token = _kling_jwt()
    if not token:
        return False
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    body = {
        "model": model,
        "prompt": prompt_en[:2500],
        "negative_prompt": "text, watermark, subtitle, logo, cartoon, blurry, low quality",
        "duration": min(max(duration, 5), 10),
        "aspect_ratio": aspect_ratio,
        "mode": "standard",
    }
    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(f"{_KLING_BASE_URL}/v1/videos/text2video", json=body, headers=headers)
            if resp.status_code not in (200, 201):
                logger.warning(f"[Kling] 생성 실패 {resp.status_code}: {resp.text[:200]}")
                return False
            data = resp.json()
        task_id = (data.get("data") or {}).get("task_id", "") or data.get("task_id", "")
        if not task_id:
            logger.warning(f"[Kling] task_id 없음: {data}")
            return False
        logger.info(f"[Kling] task_id={task_id} scene={scene_id}")
        waited, poll_interval, video_url = 0.0, 10.0, ""
        while waited < max_wait_sec:
            await asyncio.sleep(poll_interval)
            waited += poll_interval
            try:
                token2 = _kling_jwt()
                async with httpx.AsyncClient(timeout=30.0) as c2:
                    pr = await c2.get(f"{_KLING_BASE_URL}/v1/videos/{task_id}",
                                     headers={"Authorization": f"Bearer {token2}"})
                    pd = pr.json()
                td = pd.get("data") or pd
                status = td.get("task_status", "")
                logger.info(f"[Kling] {task_id} status={status} waited={waited:.0f}s")
                if status == "succeed":
                    vids = ((td.get("task_result") or {}).get("videos") or [])
                    if vids:
                        video_url = vids[0].get("url", "")
                    break
                elif status in ("failed", "cancelled"):
                    logger.warning(f"[Kling] 실패: {td}")
                    return False
            except Exception as pe:
                logger.warning(f"[Kling] 폴링 오류: {pe}")
        if not video_url:
            logger.warning(f"[Kling] video_url 없음 (waited={waited:.0f}s)")
            return False
        logger.info(f"[Kling] 다운로드: {video_url[:80]}")
        async with httpx.AsyncClient(timeout=120.0, follow_redirects=True) as dl:
            r = await dl.get(video_url)
            if r.status_code == 200:
                output_path.write_bytes(r.content)
                sz = output_path.stat().st_size
                logger.info(f"[Kling] ✅ 저장: {output_path} ({sz//1024}KB)")
                return sz > 4096
            logger.warning(f"[Kling] 다운로드 실패 {r.status_code}")
            return False
    except Exception as e:
        logger.warning(f"[Kling] 예외: {e}", exc_info=True)
        return False


async def search_and_download_assets(job_id: str, scenes: List[Scene]) -> List[Scene]:
    """각 장면에 대해 자산 검색 및 다운로드 ([AF-14] 영상 중복 제거)."""
    seen_urls: set = set()
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
            
            # [v15.69] 키워드 sanitize — 카메라 디렉티브/빈 키워드 복구
            _raw_kw = scene.keyword or ""
            _narr_hint = scene.narration or scene.description or ""
            _fallback_kw = _get_topic_fallback(_raw_kw, "")
            _sanitized_kw = _sanitize_keyword_for_search(_raw_kw, _narr_hint, _fallback_kw)
            if _sanitized_kw != _raw_kw:
                logger.info(f"[v15.69 SANITIZE] '{_raw_kw}' → '{_sanitized_kw}'")
                scene.keyword = _sanitized_kw

            # [v15.69] Kling T2V 우선 시도
            _kling_ok = False
            _is_hook_or_close = (scene.tone_profile or "").lower() in ("hook","closing","cta") or idx == 0 or idx == total_scenes - 1
            _ai_vid_selective = os.getenv("AI_VIDEO_SELECTIVE","true").lower() in ("1","true","yes")
            # [v15.70 Hybrid] hook/closing 필수 + selective mode 기반
            _should_kling = _AI_VIDEO_ENABLED and (not _ai_vid_selective or _is_hook_or_close) and _AI_VIDEO_PROVIDER in ("kling", "")
            if _should_kling:
                _kp = (
                    scene.narration_en or
                    (scene.visual_intent or "") + ", " +
                    ", ".join((scene.visual_keywords or [])[:2]) +
                    ", cinematic footage, 4K, professional"
                ).strip(", ")
                if _kp and len(_kp) > 10:
                    _ko = job_assets_dir / f"{scene.scene_id}_kling.mp4"
                    _kd = max(int(scene.duration_seconds or 5), 5)
                    logger.info(f"[Kling] {scene.scene_id} dur={_kd}s")
                    _kling_ok = await generate_kling_video(_kp, _kd, scene.scene_id, _ko)
                    if _kling_ok:
                        scene.asset_url = str(_ko)
                        updated_scenes.append(scene)
                        logger.info(f"[Kling] ✅ {scene.scene_id} 완료 — 스톡 스킵")
                        continue

            # 병렬로 Pexels와 Pixabay 검색
            expanded_kw = _expand_domain_keyword(scene.keyword)
            if expanded_kw != scene.keyword:
                logger.info(f"[Y2] 키워드 확장: '{scene.keyword}' → '{expanded_kw}'")
            pexels_videos, pixabay_videos = await asyncio.gather(
                get_pexels_videos(expanded_kw),
                get_pixabay_videos(expanded_kw)
            )
            # 부정 키워드 필터링
            pexels_videos = [v for v in pexels_videos if not _is_negative(v, expanded_kw)]
            pixabay_videos = [v for v in pixabay_videos if not _is_negative(v, expanded_kw)]
            # [v15.68] 캐스케이드 쿼리: alt_keywords 순서대로 시도
            _cascade = [scene.keyword] + list(getattr(scene,'alt_keywords',[]) or [])
            _cascade.append(_get_topic_fallback(scene.keyword, ''))
            _seen_q = set()
            for _ci, _cq_raw in enumerate(_cascade[:4]):
                _cq = _expand_domain_keyword(_cq_raw)
                if _cq in _seen_q: continue
                _seen_q.add(_cq)
                try:
                    _px_c, _pb_c = await asyncio.gather(
                        get_pexels_videos(_cq, per_page=5),
                        get_pixabay_videos(_cq, per_page=5)
                    )
                    _px_c = [v for v in _px_c if not _is_negative(v, _cq)]
                    _pb_c = [v for v in _pb_c if not _is_negative(v, _cq)]
                    pexels_videos += _px_c
                    pixabay_videos += _pb_c
                    _total_c = len(pexels_videos) + len(pixabay_videos)
                    logger.info(f'[v15.68 CQ{_ci+1}] "{_cq}" -> {len(_px_c)+len(_pb_c)} (total {_total_c})')
                    if _total_c >= 3: break
                except Exception as _ce:
                    logger.warning(f'[v15.68 CQ{_ci+1}] "{_cq}" 실패: {_ce}')
            
            
            # 최고 품질의 영상 선택
            best_video_url = select_best_video(pexels_videos, pixabay_videos, scene_index=idx, exclude_urls=seen_urls, query_keyword=scene.keyword or "")
            
            if not best_video_url:
                logger.warning(f"장면 '{scene.scene_id}' 검색 결과 없음")
                updated_scenes.append(scene)
                continue
            
            # 영상 다운로드
            asset_filename = f"{scene.scene_id}.mp4"
            asset_path = job_assets_dir / asset_filename
            
            # [AC] idempotency: skip download if file already present
            if asset_path.exists() and asset_path.stat().st_size > 4096:
                scene.asset_url = str(asset_path)
                logger.info(f"[AC] 장면 '{scene.scene_id}' 기존 파일 재사용: {asset_path} ({asset_path.stat().st_size // 1024}KB)")
                updated_scenes.append(scene)
                continue
            # [AF-14] track used URL to prevent duplicate in later scenes
            if best_video_url:
                seen_urls.add(best_video_url)
            success = await download_video(best_video_url, asset_path)
            
            # [AQ-2/AL-1] alternate retry: try up to 2 more Pexels candidates on failure
            if not success:
                logger.warning(f"[AQ-2] 1차 다운로드 실패, 대체 URL 시도: {scene.scene_id}")
                alt_exclude = set(seen_urls) | {best_video_url}
                for _alt_attempt in range(2):
                    alt_url = select_best_video(pexels_videos, pixabay_videos,
                                                 scene_index=(idx + _alt_attempt + 1),
                                                 exclude_urls=alt_exclude)
                    if not alt_url or alt_url == best_video_url:
                        break
                    alt_exclude.add(alt_url)
                    logger.info(f"[AQ-2] 대체 URL {_alt_attempt+1}/2: {alt_url[:80]}")
                    success = await download_video(alt_url, asset_path)
                    if success:
                        seen_urls.add(alt_url)
                        best_video_url = alt_url
                        logger.info(f"[AQ-2] 대체 URL 성공: {scene.scene_id}")
                        break
            
            if success:
                scene.asset_url = str(asset_path)
                logger.info(f"장면 '{scene.scene_id}' 다운로드 완료: {asset_path}")
                # [v15.68] alt 소스 영상 다운로드 (서브클립 3-4번 다양화)
                _scene_alt_kws = getattr(scene, 'alt_keywords', []) or []
                if _scene_alt_kws and not getattr(scene, 'alt_asset_url', None):
                    _alt_kw2 = _expand_domain_keyword(_scene_alt_kws[0])
                    try:
                        _apx, _apb = await asyncio.gather(
                            get_pexels_videos(_alt_kw2, per_page=3),
                            get_pixabay_videos(_alt_kw2, per_page=3)
                        )
                        _apx = [v for v in _apx if not _is_negative(v, _alt_kw2)]
                        _apb = [v for v in _apb if not _is_negative(v, _alt_kw2)]
                        _au = select_best_video(_apx, _apb, scene_index=idx+200,
                                               exclude_urls=seen_urls, query_keyword=_alt_kw2)
                        if _au:
                            _ap = job_assets_dir / f"{scene.scene_id}_alt.mp4"
                            if await download_video(_au, _ap):
                                scene.alt_asset_url = str(_ap)
                                seen_urls.add(_au)
                                logger.info(f'[v15.68] alt DL ok: {scene.scene_id}')
                    except Exception as _adl_e:
                        logger.debug(f'[v15.68] alt DL skip: {_adl_e}')
            else:
                logger.error(f"장면 '{scene.scene_id}' 다운로드 실패 (3회 시도 후)")
            
            updated_scenes.append(scene)
        
        except Exception as e:
            logger.error(f"장면 '{scene.scene_id}' 처리 오류: {e}")
            updated_scenes.append(scene)
    
    # [AY-E] persist seen URLs globally for cross-job diversity
    try:
        _save_global_seen(seen_urls)
    except Exception:
        pass
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

    # [v15.60.0] Ken Burns 프리셋 — duration 비례 zoom 속도 ({kb_speed} 치환)
    KB_PRESETS = [
        "zoompan=z='min(zoom+{kb_speed},1.06)':x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)':d={fps_d}:s=1920x1080:fps=30",
        "zoompan=z='if(eq(on,1),1.5,max(zoom-{kb_speed},1.0))':x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)':d={fps_d}:s=1920x1080:fps=30",
        "zoompan=z='1.3':x='if(lte(on,1),0,min(x+3,iw*0.25))':y='ih/2-(ih/zoom/2)':d={fps_d}:s=1920x1080:fps=30",
        "zoompan=z='1.3':x='if(lte(on,1),iw*0.25,max(x-3,0))':y='ih/2-(ih/zoom/2)':d={fps_d}:s=1920x1080:fps=30",
        "zoompan=z='min(zoom+{kb_speed},1.05)':x='iw/2-(iw/zoom/2)':y='if(lte(on,1),0,min(y+2,ih*0.2))':d={fps_d}:s=1920x1080:fps=30",
        "zoompan=z='min(zoom+{kb_speed_hi},1.06)':x='if(lte(on,1),iw*0.1,max(x-1,0))':y='ih-ih/zoom':d={fps_d}:s=1920x1080:fps=30",
    ]

    kb_counter = 0  # 전역 Ken Burns 프리셋 순환

    for _scene_idx, scene in enumerate(scenes):
        if not scene.asset_url:
            # [P] fallback 비주얼 생성 (단색 대신 그라디언트 + 키워드 카드)
            fb_path = output_dir / f"fallback_{scene.scene_id}.mp4"
            dur = max(scene.duration_seconds or 5.0, 2.5)
            if _make_fallback_clip(_scene_idx, dur, fb_path,
                                   keyword=scene.keyword, description=scene.description or "",
                                   resolution=os.getenv("DEFAULT_RESOLUTION", "1920x1080")):
                scene.asset_url = str(fb_path)
                logger.info(f"[P] 씬 '{scene.scene_id}' fallback 비주얼 사용")
            else:
                logger.warning(f"장면 '{scene.scene_id}' 자산 없음 (fallback도 실패)")
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

        # [v15.68] alt 소스 준비 (sub_i >= 3에서 사용)
        _alt_src2 = getattr(scene, 'alt_asset_url', None)
        _alt_src2_dur = 0.0
        if _alt_src2 and Path(_alt_src2).exists() and Path(_alt_src2).stat().st_size > 4096:
            try:
                _alt_p = subprocess.run(['ffprobe','-v','error','-show_entries',
                    'format=duration','-of','csv=p=0',_alt_src2],
                    capture_output=True,text=True,timeout=10)
                _rd = (_alt_p.stdout.strip() or '').replace('\n','')
                _alt_src2_dur = float(_rd) if _rd and _rd not in ('N/A','') else 0.0
            except Exception: pass

        for sub_i in range(n_subs):
            sub_dur    = scene_dur / n_subs
            sub_dur    = max(sub_dur, 3.0)
            # [v15.68] sub_i>=3이면 alt 소스 사용
            _use_alt2 = _alt_src2 and _alt_src2_dur > 1.0 and sub_i >= 3
            _clip_src2 = _alt_src2 if _use_alt2 else scene.asset_url
            _clip_src2_dur = _alt_src2_dur if _use_alt2 else actual_src_dur
            _needs_loop2 = _clip_src2_dur < scene_dur * 0.95
            # v15.12 — seek 은 항상 실제 소스 범위 안에서 분포
            seek_usable = max((_clip_src2_dur if '_clip_src2_dur' in dir() else actual_src_dur) - 0.3, 0.0)
            if n_subs > 1 and seek_usable > 0:
                seek_start = seek_usable * sub_i / n_subs
            else:
                seek_start = 0
            seek_start = max(0, min(seek_start, seek_usable))

            fps_d        = max(int(sub_dur * 30), 30)
            # [v15.60.0] duration 비례 KB 속도 (4초 기준; 짧을수록 빠르게)
            _kb_speed    = round(0.0008 * (4.0 / max(sub_dur, 4.0)), 5)
            _kb_speed_hi = round(0.001  * (4.0 / max(sub_dur, 4.0)), 5)
            kb_filter    = (KB_PRESETS[kb_counter % len(KB_PRESETS)]
                            .replace("{fps_d}", str(fps_d))
                            .replace("{kb_speed_hi}", str(_kb_speed_hi))
                            .replace("{kb_speed}", str(_kb_speed)))
            kb_counter  += 1

            fade_out_st = max(sub_dur - 0.3, sub_dur * 0.9)

            # [AE] scene-layout keyword overlay (opt-in)
            _kw_overlay = _build_keyword_overlay(scene.keyword or "", _scene_idx, sub_dur)
            _overlay_suffix = (_kw_overlay + ",") if _kw_overlay else ""
            vf = (
                f"scale={VF_W}:{VF_H}:force_original_aspect_ratio=increase,"
                f"crop={VF_W}:{VF_H},"
                f"{kb_filter},"
                f"fade=t=in:st=0:d={SCENE_HEAD_PAD_SEC:.2f},"
                f"fade=t=out:st={fade_out_st:.2f}:d={SCENE_TAIL_PAD_SEC:.2f},"
                f"unsharp=lx=5:ly=5:la=1.2:cx=3:cy=3:ca=0.6,"  # [AW-3] 강화된 sharpen
                f"eq=brightness=0.03:contrast={TEMPLATE['contrast']}:saturation={TEMPLATE['saturation']}:gamma=0.93,"
                f"curves=preset=increase_contrast,"
                f"colorbalance=rs=.05:gs=-.02:bs=-.03:rm=.02:gm=0:bm=-.02:rh=-.02:gh=.02:bh=.05,"
                f"vignette={TEMPLATE['vignette']},"
                f"{_overlay_suffix}"
                f"format=yuv420p"
            )

            clip_output = output_dir / f"clip_{scene.scene_id}_{sub_i}.mp4"

            command = ["ffmpeg"]
            if (_needs_loop2 if '_needs_loop2' in dir() else needs_loop):
                command += ["-stream_loop", "-1"]
            command += [
                "-ss", str(seek_start),
                "-i", (_clip_src2 if "_clip_src2" in dir() else scene.asset_url),
                "-t", str(sub_dur),
                "-vf", vf,
                "-c:v", "libx264", "-preset", VIDEO_PRESET, "-crf", str(VIDEO_CRF),
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
                    "-vf", f"scale={VF_W}:{VF_H}:force_original_aspect_ratio=decrease,pad={VF_W}:{VF_H}:(ow-iw)/2:(oh-ih)/2,format=yuv420p",
                    "-c:v", "libx264", "-preset", VIDEO_PRESET, "-crf", str(VIDEO_CRF),
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
           "-c:v", "libx264", "-preset", VIDEO_PRESET, "-crf", str(VIDEO_CRF),
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
                "-c:v", "copy", "-c:a", "aac", "-ac", "2", "-b:a", "192k", "-y", str(output_video)
            ]
        else:
            command = ["ffmpeg", "-i", str(video_path), "-c", "copy", "-y", str(output_video)]
        return run_ffmpeg_command(command)

    # TTS 있음: loudnorm으로 나레이션 볼륨 정규화
    if bgm_path and bgm_path.exists() and bgm_volume > 0:
        # [v15.60.0] TTS 나레이션 + BGM 덕킹 믹스 (sidechaincompress)
        # BGM_VOLUME_DURING_VOICE (ENV) 기반 덕킹 — 나레이션 구간 자동 감소
        actual_bgm_vol = BGM_VOLUME_DURING_VOICE  # 기본 0.045
        # 명시적 bgm_volume 지정 시 (기본 0.3 아닌 경우) 반영
        if bgm_volume not in (0.3, 0.8):
            actual_bgm_vol = min(bgm_volume * 0.15, BGM_VOLUME_DEFAULT)
        # [v15.66.0] sidechaincompress -> volume+amix (ffmpeg 7.x compat)
        filter_complex = (
            f"[1:a]loudnorm=I=-16:TP=-1.5:LRA=11,aformat=sample_rates=48000:channel_layouts=stereo[tts_norm];"
            f"[2:a]volume={BGM_VOLUME_DURING_VOICE},aformat=sample_rates=48000:channel_layouts=stereo[bgm_duck];"
            f"[tts_norm][bgm_duck]amix=inputs=2:duration=longest:dropout_transition=2[aout]"
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
            "-c:a", "aac", "-ac", "2", "-b:a", "192k",
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
            "-c:a", "aac", "-ac", "2", "-b:a", "192k",
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
            "-c:v", "copy", "-c:a", "aac", "-ac", "2", "-b:a", "192k",
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
    outline: bool = True,
    subtitle_type: str = "srt"
) -> bool:
    """[v15.59.0] ASS/SRT 자막 오버레이. subtitle_type에 따라 필터 자동 분기."""
    if not srt_path.exists():
        logger.warning(f"SRT 파일 없음: {srt_path}")
        return False

    # ASS 스타일: 반투명 배경 박스 + 노란 자막 (한국어 폰트)
    _font_size, _margin_v = _compute_subtitle_style(getattr(request, "resolution", None) if "request" in dir() else "1920x1080")
    style = (
        f"FontName=Noto Sans CJK KR,"
        f"FontSize={_font_size},"
        f"Bold={SUBTITLE_BOLD},"
        f"PrimaryColour={SUBTITLE_FONT_COLOR},"
        f"OutlineColour={SUBTITLE_OUTLINE_COLOR},"
        f"BackColour={SUBTITLE_BACK_COLOR},"
        f"BorderStyle={SUBTITLE_BORDER_STYLE},"
        f"Outline={SUBTITLE_OUTLINE_PX},"
        f"Shadow={SUBTITLE_SHADOW_PX},"
        f"MarginV={_margin_v},"
        f"Alignment={SUBTITLE_ALIGNMENT}"
    )

    # 경로 내 콜론 이스케이프 (Windows 경로 대비)
    srt_escaped = str(srt_path).replace("\\", "/").replace(":", "\\:")

    # [v15.59.0] ASS: ass= 필터 / SRT: subtitles= 필터
    if subtitle_type == "ass" or str(srt_path).lower().endswith(".ass"):
        vf_filter = f"ass='{srt_escaped}'"
    else:
        vf_filter = f"subtitles={srt_escaped}:charenc=UTF-8:force_style='{style}'"

    command = [
        "ffmpeg",
        "-i", str(input_video),
        "-vf", vf_filter,
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
            "-c:v", "libx264", "-preset", VIDEO_PRESET, "-crf", str(VIDEO_CRF),
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
            font = ImageFont.truetype("/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc", font_size)
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


# [PRO v2] 썸네일 CTR 최적화 색상
_THUMB_COLOR_SCHEMES = [
    ("#0D0D0D","#1A1A2E","#FFD700","#FFFFFF","#FFD700"),  # 블랙+골드
    ("#0A1628","#0D47A1","#FF6B00","#FFFFFF","#FFB347"),  # 딥블루+오렌지
    ("#1A0A00","#CC3300","#FFFF00","#FFFFFF","#FFDD00"),  # 레드+옐로우
    ("#0D1B00","#1B5E20","#00FF88","#FFFFFF","#B9F6CA"),  # 그린 다크
    ("#1A0033","#4A0080","#FF00FF","#FFFFFF","#FFB3FF"),  # 퍼플+마젠타
]

def generate_pro_thumbnail(
    video_path: Path,
    output_path: Path,
    title: str,
    subtitle: str = "",
) -> bool:
    """YouTube 프로 썸네일 v2 — 분할 패널 레이아웃 (내용별 의미 분리)
    
    레이아웃:
      LEFT (44%): 어두운 그라데이션 + 연도 배지 + 주제어 + 임팩트 워드
      RIGHT (56%): 영상 최적 프레임 (색감 강화)
      BOTTOM BAR: 자막/부제목 스트립
    """
    try:
        import re as _re_thumb
        from PIL import Image, ImageDraw, ImageFont, ImageFilter, ImageEnhance

        # ── 1. 최적 프레임 추출 ────────────────────────────────
        duration = get_video_duration(video_path) or 60.0
        timestamps = [duration * t for t in [0.08, 0.20, 0.38, 0.52, 0.68]]
        best_frame = None
        best_score = -1.0
        tmp_frames = []

        for ts in timestamps:
            tmp_f = video_path.parent / f"_tn_cand_{int(ts*1000)}.jpg"
            tmp_frames.append(tmp_f)
            cmd = ["ffmpeg", "-ss", f"{ts:.2f}", "-i", str(video_path),
                   "-frames:v", "1", "-q:v", "2", "-y", str(tmp_f)]
            if run_ffmpeg_command(cmd, timeout=20) and tmp_f.exists() and tmp_f.stat().st_size > 4096:
                img_c = Image.open(tmp_f).convert("RGB")
                edges = img_c.convert("L").filter(ImageFilter.FIND_EDGES)
                score = float(sum(edges.getdata())) / (img_c.width * img_c.height)
                if score > best_score:
                    best_score = score
                    best_frame = img_c.copy()

        for f in tmp_frames:
            try: f.unlink()
            except: pass

        if best_frame is None:
            logger.warning("[THUMB] 프레임 추출 실패")
            return False

        # ── 2. 캔버스 크기 및 구역 정의 ────────────────────────
        TW, TH = 1280, 720
        SPLIT_X  = int(TW * 0.44)   # 왼쪽 패널 너비
        BLEND_W  = 80                # 좌우 블렌딩 폭
        BOTTOM_H = int(TH * 0.135)  # 하단 바 높이
        MAIN_H   = TH - BOTTOM_H    # 메인 영역 높이

        # ── 3. 배경 프레임 (전체) ───────────────────────────────
        orig_w, orig_h = best_frame.size
        ratio = max(TW / orig_w, TH / orig_h)
        nw = int(orig_w * ratio) + 1
        nh = int(orig_h * ratio) + 1
        bg = best_frame.resize((nw, nh), Image.LANCZOS)
        lx = (nw - TW) // 2
        ty = (nh - TH) // 2
        bg = bg.crop((lx, ty, lx + TW, ty + TH))
        bg = ImageEnhance.Contrast(bg).enhance(1.3)
        bg = ImageEnhance.Color(bg).enhance(1.4)
        bg = ImageEnhance.Brightness(bg).enhance(1.08)
        img = bg.convert("RGBA")

        # ── 4. 왼쪽 어두운 패널 오버레이 ───────────────────────
        panel = Image.new("RGBA", (TW, TH), (0, 0, 0, 0))
        draw_p = ImageDraw.Draw(panel)
        for x in range(TW):
            if x < SPLIT_X - BLEND_W:
                a = 235
            elif x < SPLIT_X:
                a = int(235 * (1 - (x - (SPLIT_X - BLEND_W)) / BLEND_W))
            else:
                a = 0
            if a > 0:
                draw_p.line([(x, 0), (x, MAIN_H)], fill=(6, 10, 28, a))
        # 상단 어두운 띠 (양쪽 공통)
        for y in range(0, 55):
            a = int(110 * (1 - y / 55))
            draw_p.line([(0, y), (TW, y)], fill=(0, 0, 0, a))
        img = Image.alpha_composite(img, panel)

        # ── 5. 하단 바 ──────────────────────────────────────────
        bar = Image.new("RGBA", (TW, TH), (0, 0, 0, 0))
        draw_b = ImageDraw.Draw(bar)
        draw_b.rectangle([0, MAIN_H, TW, TH], fill=(6, 10, 38, 245))
        draw_b.rectangle([0, MAIN_H, TW, MAIN_H + 3], fill=(255, 200, 0, 255))
        img = Image.alpha_composite(img, bar)

        # ── 6. 최종 RGB 변환 + 그리기 준비 ────────────────────
        img = img.convert("RGB")
        draw = ImageDraw.Draw(img)

        # ── 7. 폰트 로드 ─────────────────────────────────────────
        _FONT_PATHS = [
            "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc",
            "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        ]
        def _load_font(size: int):
            for fp in _FONT_PATHS:
                if Path(fp).exists():
                    try: return ImageFont.truetype(fp, size)
                    except Exception: pass
            return ImageFont.load_default()

        font_badge  = _load_font(26)
        font_main   = _load_font(76)
        font_impact = _load_font(80)
        font_sub    = _load_font(44)
        font_bar    = _load_font(30)

        # ── 8. 제목 파싱 — "/" 기준 분할 ────────────────────────
        parts_raw = [p.strip() for p in title.split("/") if p.strip()]
        if len(parts_raw) == 1:
            # 공백 기준 중간 분리
            ws = title.split()
            mid = max(1, len(ws) // 2)
            parts_raw = [" ".join(ws[:mid]), " ".join(ws[mid:])]
        # 최대 2개 파트
        line1 = parts_raw[0] if parts_raw else title
        line2 = parts_raw[1] if len(parts_raw) > 1 else ""

        # 연도 배지 추출
        yr_match = _re_thumb.search(r'\d{4}', title)
        year_str = yr_match.group() if yr_match else ""

        # 임팩트 키워드 감지 (마지막 파트 또는 특정 단어)
        _IMPACT_KW = ["충격", "혁명", "혁신", "경고", "위험", "주의", "미래", "변화",
                       "폭발", "급등", "붕괴", "비밀", "진실", "반전", "대박", "최강"]
        def _is_impact(s: str) -> bool:
            return any(k in s for k in _IMPACT_KW)

        # ── 9. 연도 배지 ──────────────────────────────────────────
        GOLD  = (255, 200, 0)
        WHITE = (255, 255, 255)
        CYAN  = (0, 212, 255)
        DARK  = (0, 0, 0)

        if year_str:
            bb = draw.textbbox((0, 0), year_str, font=font_badge)
            bw, bh = bb[2] - bb[0] + 20, bb[3] - bb[1] + 10
            bx, by = 38, 32
            draw.rounded_rectangle([bx, by, bx + bw, by + bh], radius=6, fill=GOLD)
            draw.text((bx + 10, by + 5), year_str, font=font_badge, fill=DARK)

        # ── 10. 골드 액센트 라인 ─────────────────────────────────
        line_y = 90
        draw.rectangle([38, line_y, SPLIT_X - 40, line_y + 4], fill=GOLD)

        # ── 11. 메인 라인 1 ──────────────────────────────────────
        y_pos = 105
        color1 = GOLD if _is_impact(line1) else WHITE
        # 그림자
        for ox, oy in [(-2, 2), (2, 2), (0, 3)]:
            draw.text((40 + ox, y_pos + oy), line1, font=font_main, fill=DARK)
        draw.text((40, y_pos), line1, font=font_main, fill=color1)
        bb1 = draw.textbbox((40, y_pos), line1, font=font_main)
        y_pos = bb1[3] + 8

        # ── 12. 라인 2 (임팩트 강조) ─────────────────────────────
        if line2:
            color2 = GOLD if _is_impact(line2) else WHITE
            fnt2   = font_impact if _is_impact(line2) else font_sub
            for ox, oy in [(-2, 2), (2, 2), (0, 3)]:
                draw.text((40 + ox, y_pos + oy), line2, font=fnt2, fill=DARK)
            draw.text((40, y_pos), line2, font=fnt2, fill=color2)
            bb2 = draw.textbbox((40, y_pos), line2, font=fnt2)
            y_pos = bb2[3] + 16

        # ── 13. 삼각형 재생 아이콘 ───────────────────────────────
        icon_x, icon_y = 42, y_pos + 8
        draw.polygon(
            [(icon_x, icon_y), (icon_x, icon_y + 32), (icon_x + 28, icon_y + 16)],
            fill=GOLD
        )


        # ── 13-b. 국기 PIL 직접 드로잉 스트립 ─────────────────────────
        _FLAG_DRAW = {
            "🇺🇸": [  # USA — 파란 캔턴 + 빨강/흰 가로줄
                ("rect_full", (178, 34, 52)),          # 빨강 배경
                ("hstripes_white", None),               # 흰 줄 6개
                ("rect_canton", (60, 59, 110)),         # 파란 캔턴
                ("stars", (255, 255, 255)),
            ],
            "🇨🇳": [("rect_full", (222, 41, 16)), ("star_big", (255, 215, 0))],
            "🇯🇵": [("rect_full", (255, 255, 255)), ("circle_red", (188, 0, 45))],
            "🇰🇷": [("rect_full", (255, 255, 255)), ("taegeuk", None)],
            "🇬🇧": [("union_jack", None)],
            "🇩🇪": [("tricolor_h", [(0,0,0),(221,0,0),(255,206,0)])],
            "🇫🇷": [("tricolor_v", [(0,35,149),(255,255,255),(237,41,57)])],
            "🇷🇺": [("tricolor_h", [(255,255,255),(0,57,166),(213,43,30)])],
            "🇨🇦": [("rect_full", (255,0,0)), ("maple_leaf", None)],
            "🇮🇳": [("tricolor_h", [(255,153,51),(255,255,255),(19,136,8)])],
            "🇦🇺": [("rect_full", (0,0,128))],
            "🇪🇺": [("rect_full", (0,51,153)), ("eu_stars", (255,204,0))],
            "🇰🇵": [("tricolor_h", [(0,42,142),(255,255,255),(205,46,58)])],
            "🇹🇼": [("rect_full", (255,0,0))],
            "🇮🇹": [("tricolor_v", [(0,140,69),(255,255,255),(206,43,55)])],
            "🇪🇸": [("tricolor_h", [(170,21,27),(255,196,0),(170,21,27)])],
        }

        def _draw_flag_badge(draw_ctx: ImageDraw.Draw, fx: int, fy: int, fw: int, fh: int, flag_emoji: str):
            """국기 배지를 PIL로 직접 그림"""
            specs = _FLAG_DRAW.get(flag_emoji)
            if not specs:
                # 기본: 회색 배지에 국기 이모지 첫 글자
                draw_ctx.rectangle([(fx, fy), (fx+fw, fy+fh)], fill=(80,80,100))
                return
            for spec, color in specs:
                if spec == "rect_full":
                    draw_ctx.rectangle([(fx, fy), (fx+fw, fy+fh)], fill=color)
                elif spec == "hstripes_white":
                    sh = fh // 13
                    for i in range(6):
                        sy = fy + (2*i+1)*sh
                        draw_ctx.rectangle([(fx, sy), (fx+fw, sy+sh)], fill=(255,255,255))
                elif spec == "rect_canton":
                    draw_ctx.rectangle([(fx, fy), (fx+fw//2, fy+fh//2)], fill=color)
                elif spec == "stars":
                    pass  # 너무 복잡 — 캔턴 색으로 대체
                elif spec == "star_big":
                    cx, cy = fx + fw//3, fy + fh//2
                    r = min(fw, fh) // 4
                    draw_ctx.ellipse([(cx-r, cy-r), (cx+r, cy+r)], fill=color)
                elif spec == "circle_red":
                    cx, cy = fx + fw//2, fy + fh//2
                    r = min(fw, fh) // 3
                    draw_ctx.ellipse([(cx-r, cy-r), (cx+r, cy+r)], fill=color)
                elif spec == "taegeuk":
                    cx, cy = fx + fw//2, fy + fh//2
                    r = min(fw, fh) // 3
                    draw_ctx.ellipse([(cx-r, cy-r), (cx+r, cy+r)], fill=(205, 46, 58))
                    draw_ctx.ellipse([(cx, cy-r), (cx+r, cy)], fill=(0, 42, 142))
                elif spec == "union_jack":
                    draw_ctx.rectangle([(fx, fy), (fx+fw, fy+fh)], fill=(0,0,128))
                    draw_ctx.line([(fx, fy), (fx+fw, fy+fh)], fill=(255,255,255), width=max(2, fh//7))
                    draw_ctx.line([(fx+fw, fy), (fx, fy+fh)], fill=(255,255,255), width=max(2, fh//7))
                    draw_ctx.line([(fx+fw//2, fy), (fx+fw//2, fy+fh)], fill=(255,255,255), width=max(2, fh//5))
                    draw_ctx.line([(fx, fy+fh//2), (fx+fw, fy+fh//2)], fill=(255,255,255), width=max(2, fh//5))
                    draw_ctx.line([(fx, fy), (fx+fw, fy+fh)], fill=(207,20,43), width=max(1, fh//10))
                    draw_ctx.line([(fx+fw, fy), (fx, fy+fh)], fill=(207,20,43), width=max(1, fh//10))
                elif spec == "tricolor_h":
                    bands = color  # list of 3 RGB
                    bh = fh // 3
                    for i, c in enumerate(bands):
                        draw_ctx.rectangle([(fx, fy+i*bh), (fx+fw, fy+(i+1)*bh)], fill=c)
                elif spec == "tricolor_v":
                    bands = color
                    bw = fw // 3
                    for i, c in enumerate(bands):
                        draw_ctx.rectangle([(fx+i*bw, fy), (fx+(i+1)*bw, fy+fh)], fill=c)
                elif spec == "maple_leaf":
                    cx, cy = fx + fw//2, fy + fh//2
                    r = min(fw, fh) // 4
                    draw_ctx.ellipse([(cx-r, cy-r), (cx+r, cy+r)], fill=(255, 0, 0))
                elif spec == "eu_stars":
                    cx, cy = fx + fw//2, fy + fh//2
                    r_orbit = min(fw, fh) // 3
                    r_star  = max(2, min(fw, fh) // 9)
                    import math as _math
                    for i in range(12):
                        a = _math.pi/2 - i * _math.pi/6
                        sx = int(cx + r_orbit * _math.cos(a))
                        sy = int(cy - r_orbit * _math.sin(a))
                        draw_ctx.ellipse([(sx-r_star, sy-r_star), (sx+r_star, sy+r_star)], fill=color)

        _detected_flags = detect_countries_in_text(title + " " + (subtitle or ""))
        if _detected_flags:
            try:
                FW, FH = 44, 30   # 국기 배지 크기
                _badge_x = 42
                _badge_y = icon_y + 46
                for _flag_emoji in _detected_flags[:5]:
                    # 테두리 라운드 배지
                    _pad = 3
                    draw.rounded_rectangle(
                        [(_badge_x-_pad, _badge_y-_pad), (_badge_x+FW+_pad, _badge_y+FH+_pad)],
                        radius=4, fill=(255,255,255,200)
                    )
                    _draw_flag_badge(draw, _badge_x, _badge_y, FW, FH, _flag_emoji)
                    _badge_x += FW + 12
            except Exception as _fe:
                logger.debug(f"[THUMB] 국기 배지 오류: {_fe}")

        # ── 14. 하단 바 텍스트 ───────────────────────────────────
        bar_text = subtitle.strip() if subtitle else (line2 if line2 and line1 != line2 else "")
        if not bar_text:
            # 제목 전체 축약
            bar_text = title[:40] + ("…" if len(title) > 40 else "")
        if bar_text:
            bb_bar = draw.textbbox((0, 0), bar_text, font=font_bar)
            btw = bb_bar[2] - bb_bar[0]
            btx = (TW - btw) // 2
            bty = MAIN_H + (BOTTOM_H - (bb_bar[3] - bb_bar[1])) // 2
            draw.text((btx + 1, bty + 1), bar_text, font=font_bar, fill=DARK)
            draw.text((btx, bty), bar_text, font=font_bar, fill=(210, 210, 220))

        # ── 15. 오른쪽 패널 상단 미세 비네트 ────────────────────
        rv = Image.new("RGBA", (TW, TH), (0, 0, 0, 0))
        draw_rv = ImageDraw.Draw(rv)
        for x in range(60):
            a = int(70 * (1 - x / 60))
            rx = TW - 60 + x
            draw_rv.line([(rx, 0), (rx, MAIN_H)], fill=(0, 0, 0, a))
        img = Image.alpha_composite(img.convert("RGBA"), rv).convert("RGB")

        # ── 16. 저장 ──────────────────────────────────────────────
        output_path.parent.mkdir(parents=True, exist_ok=True)
        img.save(str(output_path), "JPEG", quality=92, optimize=True)
        logger.info(f"[THUMB-v2] 멀티패널 썸네일 완료: {output_path} score={best_score:.1f}")
        return True

    except Exception as e:
        logger.error(f"[THUMB] 프로 썸네일 오류: {e}")
        import traceback; logger.debug(traceback.format_exc())
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




# ============================================================================
# [v15.60.0] Narration-First Timeline Engine 함수군
# ============================================================================

def split_script_into_beats(script, avg_speech_rate=4.0, min_beat_sec=6.0, max_beat_sec=12.0):
    """스크립트를 의미 단위(Beat)로 분할. Returns list of {text, est_duration, beat_idx}"""
    import re as _re
    raw_sentences = [s.strip() for s in _re.split(r"(?<=[.!?])\s+|\n+", script) if s.strip()]
    beats, current_text, current_dur = [], "", 0.0
    for sent in raw_sentences:
        char_count = len(sent.replace(" ", ""))
        est_dur = char_count / max(avg_speech_rate, 1.0)
        est_dur += sent.count(",") * (PAUSE_COMMA_MS / 1000.0)
        est_dur += PAUSE_SENTENCE_MS / 1000.0
        if current_dur + est_dur > max_beat_sec and current_text:
            beats.append({"text": current_text.strip(), "est_duration": round(current_dur, 2), "beat_idx": len(beats)})
            current_text, current_dur = sent, est_dur
        else:
            current_text = (current_text + " " + sent).strip() if current_text else sent
            current_dur += est_dur
        if current_dur >= min_beat_sec and sent[-1:] in (".", "!", "?", "。"):
            beats.append({"text": current_text.strip(), "est_duration": round(current_dur, 2), "beat_idx": len(beats)})
            current_text, current_dur = "", 0.0
    if current_text.strip():
        beats.append({"text": current_text.strip(), "est_duration": round(current_dur, 2), "beat_idx": len(beats)})
    logger.info(f"[NTL] 스크립트 → {len(beats)}개 Beat (총 {sum(b['est_duration'] for b in beats):.1f}초)")
    return beats


def build_narration_ssml(text, voice="ko-KR-SunHiNeural", rate="+0%", pitch="+0Hz",
                          pause_comma_ms=None, pause_sentence_ms=None):
    """[v15.60.0] SSML 전처리: 쉼표/문장 끝 pause 삽입"""
    import re as _re
    pc = pause_comma_ms if pause_comma_ms is not None else PAUSE_COMMA_MS
    ps = pause_sentence_ms if pause_sentence_ms is not None else PAUSE_SENTENCE_MS
    t = _re.sub(r",(?=\s)", f", <break time=\"{pc}ms\"/>", text)
    t = _re.sub(r"([.!?])(\s)", f"\\1 <break time=\"{ps}ms\"/>\\2", t)
    return (f'<speak version="1.0" xmlns="http://www.w3.org/2001/10/synthesis" xml:lang="ko-KR">'
            f'<voice name="{voice}"><prosody rate="{rate}" pitch="{pitch}">{t}</prosody></voice></speak>')


def _assign_scene_timings(scenes, segments, total_dur):
    """씬에 start/end 타이밍 할당"""
    scene_timings, seg_idx, cursor = [], 0, 0.0
    for scene in scenes:
        if segments and seg_idx < len(segments):
            seg = segments[seg_idx]
            s, e = seg.get("start", cursor), seg.get("end", cursor + (scene.duration_seconds or 5.0))
            seg_idx += 1
        else:
            s, e = cursor, cursor + (scene.duration_seconds or 5.0)
        timing = {
            "start": round(s, 3), "end": round(e, 3),
            "narration_start": round(s + SCENE_HEAD_PAD_SEC, 3),
            "narration_end": round(e - SCENE_TAIL_PAD_SEC, 3),
            "padded_duration": round(e - s + SCENE_HEAD_PAD_SEC + SCENE_TAIL_PAD_SEC, 3),
        }
        scene.timing = timing
        scene_timings.append({"scene_id": scene.scene_id, **timing})
        cursor = e + PAUSE_SENTENCE_MS / 1000.0
    return {"segments": segments, "total_duration": round(total_dur, 3), "scene_timings": scene_timings}


def build_narration_timeline(job_id, scenes, timestamps_path=None):
    """[v15.60.0] WhisperX 타임스탬프 기반 나레이션 타임라인 구성"""
    import json as _json
    if timestamps_path and Path(timestamps_path).exists():
        try:
            ts_data = _json.loads(Path(timestamps_path).read_text(encoding="utf-8"))
            segments = ts_data.get("segments", [])
            if segments:
                total_dur = segments[-1].get("end", 0.0)
                logger.info(f"[NTL] WhisperX 로드: {len(segments)}세그먼트, {total_dur:.1f}초")
                return _assign_scene_timings(scenes, segments, total_dur)
        except Exception as e:
            logger.warning(f"[NTL] timestamps 파싱 실패, 추정 사용: {e}")
    # 추정 타임라인
    segments, cursor = [], 0.0
    for scene in scenes:
        narr = getattr(scene, "narration", None) or ""
        est = max(len(narr.replace(" ", "")) / 4.0 if narr else (scene.duration_seconds or 5.0), 2.0)
        segments.append({"start": round(cursor, 3), "end": round(cursor + est, 3),
                          "text": narr, "scene_id": scene.scene_id})
        cursor += est + PAUSE_SENTENCE_MS / 1000.0
    return _assign_scene_timings(scenes, segments, cursor)


def visual_match_score(asset_meta, scene, already_used=None):
    """[v15.60.0] 자산-씬 매칭 점수 (0~1). keyword 35% + visual_intent 25% + duration 15% + resolution 10% + motion 10% - dup 5%"""
    score = 0.0
    asset_tags = set((asset_meta.get("tags", "") or "").lower().split(","))
    asset_tags |= set((asset_meta.get("title", "") or "").lower().split())
    scene_kw = {(scene.keyword or "").lower()}
    for kw in (getattr(scene, "visual_keywords", None) or []):
        scene_kw.update(kw.lower().split())
    score += (len(scene_kw & asset_tags) / max(len(scene_kw), 1)) * 0.35
    intent = (getattr(scene, "visual_intent", None) or "").lower()
    a_motion = (asset_meta.get("motion", "") or "").lower()
    intent_score = 0.5
    if intent in ("dynamic", "uplifting") and a_motion in ("high", "medium"): intent_score = 1.0
    elif intent in ("calm", "educational") and a_motion in ("low", "static"): intent_score = 1.0
    elif intent == "dramatic" and a_motion == "high": intent_score = 1.0
    score += intent_score * 0.25
    asset_dur = float(asset_meta.get("duration", 0) or 0)
    scene_dur = float(scene.duration_seconds or 5.0)
    score += max(0.0, 1.0 - abs(asset_dur - scene_dur) / 10.0) * 0.15 if asset_dur > 0 else 0.5 * 0.15
    w, h = int(asset_meta.get("width", 0) or 0), int(asset_meta.get("height", 0) or 0)
    score += (1.0 if w >= 1920 and h >= 1080 else 0.7 if w >= 1280 else 0.3) * 0.10
    score += {"high": 0.9, "medium": 0.7, "low": 0.5, "static": 0.3}.get(a_motion, 0.5) * 0.10
    asset_id = str(asset_meta.get("id", ""))
    if already_used and asset_id and asset_id in already_used:
        score = max(0.0, score - 0.05)
    return round(min(score, 1.0), 4)


def save_timeline_report(job_id, timeline, scenes):
    """[v15.60.0] timeline_report.json 저장"""
    import json as _json
    report_path = JOBS_DIR / job_id / "timeline_report.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report = {
        "job_id": job_id, "version": "15.74.0",
        "generated_at": datetime.now().isoformat(),
        "total_duration": timeline.get("total_duration", 0),
        "scene_count": len(scenes),
        "scene_timings": timeline.get("scene_timings", []),
        "segments": timeline.get("segments", []),
        "env": {k: globals()[k] for k in ("PAUSE_COMMA_MS","PAUSE_SENTENCE_MS",
                "SCENE_HEAD_PAD_SEC","SCENE_TAIL_PAD_SEC","BGM_VOLUME_DEFAULT","BGM_VOLUME_DURING_VOICE")},
    }
    report_path.write_text(_json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info(f"[NTL] timeline_report.json 저장: {report_path}")
    return report_path


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
            "BackColour=&HA0000000,"     # 50% 투명 검정 박스 배경
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


# [BA] MARKER v1 - Stronger keyword extraction with multi-word concrete phrases
# Abstract single words that Pexels mis-maps to random content
# [BL] 중의어 단독 사용 시 Pexels 오인 — 대체어로 자동 치환
# [BN-2] MARKER v2
_AMBIGUOUS_REPLACE = {
    "microphone": "building",        # [BN-2] 마이크→건물 (podium 스톡 많으면 미국기 나옴)
    "press": "journalist",            # press phone → 기자
    "screen": "monitor",              # 휴대폰 화면 → 모니터
    "phone": "",                       # 모호함 → 제거
    "audio": "",                       # 음향 → 제거
    "recording": "",                   # 녹음 → 제거
    "studio": "",                      # 스튜디오 → 제거
    "conference": "meeting",          # 회의
    "speaker": "official",            # [BN-2] politician→official (podium 회피)
    "podium": "building",             # [BN-2] 연단→건물 (generic podium US flags)
    "politician": "official",         # [BN-2] 정치인→공무원
    "president": "government",        # [BN-2] 대통령→정부
    "flag": "",                        # [BN-2] flag 단어 자체 제거
    # [BP] 미세먼지 검증 실패 키워드
    "pollution": "smog",              # pollution 단독→smog (volcano 방지)
    "polluted": "smog",                #
    "vulnerable": "",                   # 제거 (forest fire 방지)
    "invisible": "",                    # 제거
    "particles": "",                    # 제거 (abstract particle)
    "particle": "",                     #
    "quality": "",                      # 제거 (server hardware 방지)
    "forecast": "weather",              # forecast→weather TV screen
    "hardware": "",                     # LLM 환각 결과물
    "server": "",                       # 동
    "environment": "nature",            # environment→nature
    "announcement": "news",             # announcement→news studio
}

# "press conference" 같은 복합어는 유지 (분리 금지 대상)
_COMPOUND_KEEP = {
    "press conference",
    "breaking news",
    "white house",
    "president speech",
    "stock market",
    "financial crisis",
}


_ABSTRACT_BLACKLIST = {
    # 추상 명사
    "odd", "even", "number", "rule", "exception", "fine", "date",
    "idea", "concept", "type", "way", "form", "part", "thing",
    "issue", "problem", "solution", "factor", "aspect", "matter",
    "process", "case", "method", "system", "structure", "pattern",
    "level", "change", "difference", "step", "point",
    # 추상 동사·형용사 (Pexels가 풍경으로 오해석)
    "divided", "alternating", "regulated", "announced", "announcement",
    "reduction", "increase", "decrease", "growth", "decline",
    "approach", "practice", "application", "impact", "effect",
    "relationship", "connection", "communication",
    # [BP] MARKER v3
    # [BP] 검증 실패 경험상 Pexels 가 무관 영상 뱉는 키워드
    "pollution", "polluted", "vulnerable", "invisible", "invisible-particle",
    "particles", "particle", "quality", "forecast", "announcement-daily",
    "hardware", "server", "database", "data", "announcement",
    "environment", "ecology", "situation", "condition", "state",
    "activity", "activities", "measurement", "tracking",
}


async def _batch_extract_keywords_from_segments(segments: list, topic_hint: str = "") -> dict:
    """[BB] Maximum matching - extract 3 candidate phrases per scene with topic prefix.
    Returns: {segment_idx_1based: "best concrete visual phrase"}
    
    Strategy:
    1. Full script context + topic hint in prompt
    2. Request 3 candidates per segment (main + 2 alternates)
    3. Validate against abstract blacklist
    4. Select candidate with most concrete nouns
    """
    import json as _json
    try:
        texts = []
        full_context = []
        for i, seg in enumerate(segments):
            t = (seg.get("text") or "").strip()
            if t:
                texts.append(f"{i+1}. {t}")
                full_context.append(t)
        if not texts:
            return {}
        context_summary = " ".join(full_context)[:600]
        topic_line = f"주제: {topic_hint}\n" if topic_hint else ""
        
        prompt = (
            "한국어 영상 스크립트를 Pexels 영어 검색어로 변환.\n\n"
            + topic_line
            + "전체 맥락: " + context_summary + "\n\n"
            "각 문장마다 Pexels에서 가장 잘 매칭될 영어 문구 1개를 만드세요.\n"
            "규칙 (절대 준수):\n"
            "1. 반드시 2~3 단어만 (4단어 이상 금지, Pexels 매칭률 떨어짐)\n"
            "2. 추상어 금지: odd, even, rule, number, exception, fine, concept, idea, type, process, method, system, level\n"
            "3. 구체적 시각 객체·장면만: 'city street traffic cars', 'polluted urban skyline'\n"
            "4. 주제의 물리적 장면 연상: 자동차 2부제 → 도시 교통, 세금 → 동전 계산기, 우주 → 위성 로켓\n"
            "5. 한국 개념(2부제·주민번호·수능 등)은 관련 시각 장면으로: '2부제→highway traffic cars', '수능→students classroom exam'\n"
            "6. 중복 금지 — 모든 씬 키워드는 서로 달라야 함. 같은 주제라도 각도를 다르게 (예: 교통/운전석/신호등/주차장/배기가스)\n\n"
            "예시 (명사 3-5개 + 장소/사람/동작):\n"
            "  '미세먼지' → 'industrial chimney smoke pollution skyline'\n"
            "  '홀수 차량' → 'cars city traffic asian street'\n"
            "  '번호판' → 'license plate closeup vehicle rear metal'\n"
            "  '짝수 운행' → 'cars urban road traffic light seoul'\n"
            "  '미세먼지 감소' → 'factory smokestack pollution asian city'\n"
            "  '정부 발표' → 'seoul government building exterior'\n"
            "  '과태료 부과' → 'police officer parking ticket violation'\n"
            "  '국회 통과' → 'korean parliament building asian'\n"
            "  '대통령 연설' → 'korean government building exterior'\n"
            "  '큐브샛' → 'satellite orbit space earth blue'\n"
            "  '우주 환경 시험' → 'vacuum chamber engineering lab scientist'\n"
            "나쁜 예시 (절대 금지):\n"
            "  ✗ 'cars divided by license plate' → divided 는 추상 (풍경 매칭됨)\n"
            "  ✗ 'cash register money fine penalty' → 돈/기계 섞임 (POS 기계 나옴)\n"
            "  ✗ 'government announcement press conference' → announcement 추상 (뉴스 그래픽 나옴)\n\n"
            "한국 관련 주제면 'asian', 'seoul', 'korean' 등 지역어 1개 포함 (구체성 향상).\n"
            "한국 주제일 때 절대 금지어 (서양 이미지 나옴): american, usa, us, white house, capitol, trump, biden, obama, union jack, british, britain, eu, european, buckingham.\n"
            "대통령·국회·정부 장면도 'asian president podium' / 'asian parliament building' / 'korean government meeting' 식으로.\n\n"
            "문장:\n"
            + "\n".join(texts)
            + '\n\n응답: JSON 배열 ["phrase1", "phrase2", ...] ' + str(len(texts)) + '개만.\n반드시 ```json 으로 시작, ``` 으로 끝. 설명·주석 금지, 오직 JSON 배열.\n예시 응답:\n```json\n["traffic cars highway", "factory smoke pollution"]\n```'
        )
        
        # LLM 호출
        try:
            import httpx as _httpx
            _anth_url = os.getenv("ANTHROPIC_BASE_URL", "http://lf2_llm_proxy:8789").rstrip("/")
            async with _httpx.AsyncClient(timeout=45.0) as _c:
                r = await _c.post(
                    _anth_url + "/v1/messages",
                    headers={"Content-Type": "application/json",
                             "x-api-key": os.getenv("ANTHROPIC_AUTH_TOKEN", "local-dev")},
                    json={"model": "claude-sonnet-4-6", "max_tokens": 2500, "temperature": 0.3,
                          "messages": [{"role": "user", "content": prompt}]}
                )
                raw = ""
                if r.status_code == 200:
                    data = r.json()
                    for blk in data.get("content", []):
                        if blk.get("type") == "text":
                            raw += blk.get("text", "")
        except Exception as _ex:
            logger.warning(f"[BB] LLM 호출 실패: {_ex}")
            return {}
        
        # [BD+BH] 파서 강화 — code block 우선, non-greedy array, line fallback
        import re as _re
        kws = None
        
        # [BH] 시도 0: ```json ... ``` fenced block 우선
        fenced = _re.search(r"```(?:json)?\s*\n([\s\S]*?)\n```", raw)
        if fenced:
            inner = fenced.group(1).strip()
            try:
                kws = _json.loads(inner)
            except _json.JSONDecodeError:
                # 배열만 추출 시도
                marr = _re.search(r"\[\s*([\s\S]*?)\s*\]", inner)
                if marr:
                    try:
                        kws = _json.loads("[" + marr.group(1) + "]")
                    except _json.JSONDecodeError:
                        kws = None
        
        # 시도 1: non-greedy JSON array (fenced block 없는 경우)
        if not kws:
            m = _re.search(r"\[([^\[\]]*?)\]", raw)
            if m:
                try:
                    kws = _json.loads("[" + m.group(1) + "]")
                except _json.JSONDecodeError:
                    kws = None
        
        # 시도 2: 줄 단위 "N. phrase" 또는 "N. '...' → 'phrase'" 형식
        if not kws:
            extracted = []
            # pattern: N.  ... →  "phrase"  OR  N. "phrase"
            for line in raw.split("\n"):
                line = line.strip()
                if not line:
                    continue
                # '^\d+\.' prefix 확인
                mm = _re.match(r"^\d+[\.\)]\s*(.+)$", line)
                if not mm:
                    continue
                rest = mm.group(1).strip()
                # 마지막 quote 안의 content 뽑기
                quoted = _re.findall(r'["\u201c\u201d]([^"\u201c\u201d]{2,80})["\u201c\u201d]', rest)
                if quoted:
                    extracted.append(quoted[-1].strip())
                else:
                    # → 나 -> 이후 부분
                    arrow = _re.search(r"[\u2192\-=]>\s*(.+?)$", rest)
                    if arrow:
                        extracted.append(arrow.group(1).strip().strip('"').strip("'"))
                    else:
                        # 그냥 line 전체
                        extracted.append(rest[:80])
            if extracted:
                kws = extracted
                logger.info(f"[BD] markdown list 파서 성공: {len(kws)}개")
        
        if not kws:
            logger.warning(f"[BB] 응답 파싱 실패: {raw[:200]}")
            return {}
        if not isinstance(kws, list):
            return {}
        
        # 검증 + 보강
        result = {}
        fixed = 0
        for i, kw in enumerate(kws[:len(segments)], 1):
            if not (isinstance(kw, str) and kw.strip()):
                continue
            cleaned = kw.strip().lower()[:80]
            words = cleaned.split()
            
            # 단어 1개 절대 거부 → topic_hint 또는 description으로 확장
            if len(words) < 2:
                seg_text = (segments[i-1].get("text") or "").strip()[:40]
                if topic_hint:
                    cleaned = f"{topic_hint} {cleaned} scene"
                else:
                    cleaned = f"{cleaned} city scene real footage"
                fixed += 1
            # 추상어 블랙리스트 단어 비율 > 30% 거부 (엄격해짐)
            # [BQ] MARKER v4
            # [BQ-1] typo/환각 접미사 필터 — cleaned 에 반영 (BP 버그 수정)
            suspicious_suffixes = ("ererer", "wareer", "warer", "nessness", "mentment", "tiontion")
            words = [w for w in words if not any(s in w for s in suspicious_suffixes)]
            cleaned = " ".join(words).strip()  # [BQ] 필터 결과 반영
            # [BQ-2] 비-ASCII 문자(한국어 잔류) 제거
            cleaned = "".join(ch for ch in cleaned if ord(ch) < 128).strip()
            while "  " in cleaned:
                cleaned = cleaned.replace("  ", " ")
            words = cleaned.split()
            # [BQ-3] 필터 후 단어 부족 → topic_hint fallback
            if len(words) < 2 and topic_hint:
                cleaned = f"{topic_hint} asian scene"
                words = cleaned.split()
                fixed += 1
            blacklisted = sum(1 for w in words if w in _ABSTRACT_BLACKLIST)
            if blacklisted > 0 and blacklisted / max(1, len(words)) > 0.3:
                # 추상어 과다 → topic 추가
                if topic_hint:
                    cleaned = f"{topic_hint} " + " ".join(w for w in words if w not in _ABSTRACT_BLACKLIST)
                cleaned = cleaned.strip()
                fixed += 1
            
            # [BL] 중의어 처리 (복합어는 보존, 단독은 치환/제거)
            final_words = []
            lower = cleaned.lower()
            # 먼저 복합어 유지 체크
            compound_found = False
            for cmp in _COMPOUND_KEEP:
                if cmp in lower:
                    compound_found = True
                    break
            if not compound_found:
                # 단어별로 처리
                for w in cleaned.split():
                    wl = w.lower()
                    if wl in _AMBIGUOUS_REPLACE:
                        rep = _AMBIGUOUS_REPLACE[wl]
                        if rep:
                            final_words.append(rep)
                        # 빈 문자열이면 제거 (추가 안 함)
                    else:
                        final_words.append(w)
                cleaned = " ".join(final_words) if final_words else cleaned
            # [BL] 최대 3단어로 자름 (Pexels 매칭률 최적)
            words_out = cleaned.split()
            if len(words_out) > 3:
                cleaned = " ".join(words_out[:3])
                fixed += 1
            # [BW] MARKER v8
            # [BW] 한국 주제 + Korean locale token 부재 → "hanbok" 강제
            _KOREA_LOCALE = ("korean", "korea", "asian", "seoul", "busan",
                             "jeju", "hanbok", "palace", "gyeongbok",
                             "gyeongju", "bukchon", "insadong")
            _COLOR_MOOD_SOLO = ("modern", "blue", "red", "green", "white",
                                "black", "pink", "yellow", "pastel",
                                "summer", "winter", "spring", "autumn", "fall",
                                "bright", "dark", "soft", "warm", "cool")
            # [BW-FIX] 한국 문화 토픽만 locale 보강, 일반 토픽 강제 주입 제거
            _KOREA_TOPIC_WORDS = ("한국", "대한민국", "서울", "부산", "제주", "한류", "한복", "케이팝", "경복궁")
            is_kor_culture = bool(topic_hint) and any(kw in (topic_hint or "") for kw in _KOREA_TOPIC_WORDS)
            low_cleaned = cleaned.lower()
            has_locale = any(t in low_cleaned for t in _KOREA_LOCALE)
            # color/mood 만으로 구성되면 거부 — 한국 문화 토픽에만 적용
            color_only = all(w.lower() in _COLOR_MOOD_SOLO
                             for w in cleaned.split())
            if is_kor_culture and (not has_locale or color_only):
                # color_only 면 완전 대체, 아니면 prefix
                if color_only:
                    cleaned = "korean hanbok palace"
                else:
                    cleaned = "korean " + cleaned
                    # 3단어 초과 시 다시 자름
                    ws = cleaned.split()
                    if len(ws) > 3:
                        cleaned = " ".join(ws[:3])
                fixed += 1
            result[i] = cleaned[:80]
        
        if fixed > 0:
            logger.info(f"[BB+BL] {fixed}개 키워드 보강 적용 (중의어 제거 + 3단어 cap)")
        # [BC] 중복 제거 — 동일 문구 있으면 angle 변형 suffix 추가
        seen_phrases = set()
        ANGLES = ["close up", "aerial view", "wide shot", "side angle", "night time",
                 "daytime sunny", "macro detail", "rush hour", "empty lane", "side mirror",
                 "dashboard", "driver seat", "license plate", "tire wheel", "traffic light"]
        _angle_idx = 0
        dedup_fixed = 0
        for i in sorted(result.keys()):
            phrase = result[i]
            if phrase in seen_phrases:
                # 중복! angle 추가
                suffix = ANGLES[_angle_idx % len(ANGLES)]
                _angle_idx += 1
                # 문구 축약 후 angle 병합 (단어 5개 유지)
                parts = phrase.split()[:3]
                phrase = " ".join(parts) + " " + suffix
                result[i] = phrase[:80]
                dedup_fixed += 1
            seen_phrases.add(phrase)
        if dedup_fixed > 0:
            logger.info(f"[BC] 중복 {dedup_fixed}개 제거 (angle 변형 적용)")
        avg_words = sum(len(v.split()) for v in result.values()) / max(1, len(result))
        unique_count = len(set(result.values()))
        logger.info(f"[BB+BC] batch 키워드: {len(result)}개, 고유 {unique_count}개 (평균 {avg_words:.1f} 단어)")
        return result
    except Exception as e:
        logger.warning(f"[BB] batch 키워드 실패: {e}")
        return {}


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

        # [AH-4] Fast path: Whisper-first alignment with gap absorption.
        # When UNIFIED_TIMELINE=true and we have clean segments, use them directly as scenes.
        # scene[i].duration = segments[i+1].start - segments[i].start (last: audio_end - last.start)
        if UNIFIED_TIMELINE and len(segments) >= 2:
            try:
                # Determine true audio end
                audio_end = total_audio
                ap = ts_data.get("audio_path")
                if ap and Path(ap).exists():
                    try:
                        pr = subprocess.run(
                            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
                             "-of", "csv=p=0", ap],
                            capture_output=True, text=True, timeout=10,
                        )
                        if (pr.stdout or "").strip() not in ("", "N/A"):
                            audio_end = float(pr.stdout.strip())
                    except Exception:
                        pass

                aligned_scenes = []
                scene_counter = 0
                for i, seg in enumerate(segments):
                    seg_start = float(seg.get("start", 0.0) or 0.0)
                    seg_end = float(seg.get("end", 0.0) or seg_start + 1.0)
                    next_start = (
                        float(segments[i + 1].get("start", seg_end) or seg_end)
                        if (i + 1) < len(segments) else audio_end
                    )
                    total_dur = max(0.5, next_start - seg_start)
                    seg_text = (seg.get("text") or "").strip()
                    
                    # [BF] 긴 segment (>SCENE_MAX_SEC) 를 2~3 등분
                    n_splits = 1
                    if total_dur > SCENE_MAX_SEC * 1.5:
                        n_splits = min(3, int(total_dur / SCENE_MAX_SEC) + 1)
                    
                    # 텍스트를 쉼표·마침표로 대충 split (n_splits 만큼)
                    if n_splits > 1:
                        import re as _re_split
                        phrases = _re_split.split(r"[,·.、]\s*", seg_text)
                        phrases = [p.strip() for p in phrases if p.strip()]
                        if not phrases or len(phrases) < n_splits:
                            # fallback: char-based split
                            L = len(seg_text)
                            phrases = [seg_text[k*L//n_splits : (k+1)*L//n_splits] for k in range(n_splits)]
                            phrases = [p.strip() for p in phrases if p.strip()]
                        else:
                            # phrase가 많으면 n_splits 만큼 합치기
                            per = max(1, len(phrases) // n_splits)
                            phrases = [" ".join(phrases[k*per:(k+1)*per]) for k in range(n_splits)]
                    else:
                        phrases = [seg_text]
                    
                    # 각 phrase를 scene으로
                    for j, phrase in enumerate(phrases):
                        if not phrase:
                            continue
                        scene_counter += 1
                        sub_dur = total_dur / len(phrases)
                        rel_mid = ((seg_start + seg_end) / 2.0) / max(0.1, audio_end)
                        orig = pick_orig_scene(min(1.0, rel_mid))
                        update = {
                            "scene_id": f"ws_{scene_counter}",
                            "scene_number": scene_counter,
                            "description": phrase or orig.description,
                            "duration_seconds": round(sub_dur, 2),
                        }
                        seg_idx_1based = i + 1
                        if seg_idx_1based in seg_kw_map:
                            kws = seg_kw_map[seg_idx_1based]
                            if kws:
                                update["keyword"] = kws[0]
                                update["asset_url"] = None
                        aligned_scenes.append(orig.model_copy(update=update))
                total = sum(s.duration_seconds for s in aligned_scenes)
                logger.info(
                    f"[AH-4] Whisper-first 정렬: {len(aligned_scenes)}씬, 총 {total:.2f}s "
                    f"(audio_end={audio_end:.2f}s, gap 흡수 완료) # [AH-4] MARKER v1"
                )
                return aligned_scenes
            except Exception as _ah4_err:
                logger.warning(f"[AH-4] Whisper-first 실패, 기존 경로로 fallback: {_ah4_err}")

        new_scenes = []
        _scene_abs_times = []  # [AH-1] parallel list of (abs_start, abs_end) in Whisper time
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
                # [AH-1] attach absolute start/end from Whisper for gap absorption
                update_dict["scene_number"] = seg_counter
                sc_new = orig.model_copy(update=update_dict)
                # store abs times in a parallel list (cannot extend Pydantic model without schema change)
                _scene_abs_times.append((float(sub_start), float(sub_end)))
                new_scenes.append(sc_new)

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

        # [AD] Unified Timeline: apply SCENE_LEAD_SEC so scene precedes subtitle.
        # Each scene steals SCENE_LEAD_SEC from the PREVIOUS scene's tail (except the first).
        # First scene absorbs the lead by extending its start backward (capped at 0s).
        # [AH-1] Gap absorption: scenes cumulative time -> Whisper absolute time.
        # Each scene extends to the NEXT segments start; last scene extends to audio end.
        if UNIFIED_TIMELINE and merged and _scene_abs_times and len(merged) == len(_scene_abs_times):
            try:
                audio_end_abs = total_audio
                # try ffprobe for more accurate audio end
                ap = ts_data.get("audio_path")
                if ap and Path(ap).exists():
                    try:
                        pr = subprocess.run(
                            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
                             "-of", "csv=p=0", ap],
                            capture_output=True, text=True, timeout=10,
                        )
                        if (pr.stdout or "").strip() not in ("", "N/A"):
                            audio_end_abs = float(pr.stdout.strip())
                    except Exception:
                        pass
                abs_starts = [abs_s for (abs_s, _) in _scene_abs_times]
                gap_shifted = []
                for i, sc in enumerate(merged):
                    abs_s = abs_starts[i]
                    abs_e_next = abs_starts[i + 1] if (i + 1) < len(abs_starts) else audio_end_abs
                    new_dur = max(SCENE_MIN_SEC * 0.5, abs_e_next - abs_s)
                    gap_shifted.append(sc.model_copy(update={"duration_seconds": round(new_dur, 2)}))
                merged = gap_shifted
                total_after = sum(s.duration_seconds for s in merged)
                logger.info(
                    f"[AH-1] gap 흡수 완료: {len(merged)}씬, 총 {total_after:.2f}s "
                    f"(audio_end={audio_end_abs:.2f}s, 절대시간 정렬)"
                )
            except Exception as _gap_err:
                logger.warning(f"[AH-1] gap 흡수 실패: {_gap_err}")

        if UNIFIED_TIMELINE and SCENE_LEAD_SEC > 0 and len(merged) >= 2:
            try:
                lead = float(SCENE_LEAD_SEC)
                n = len(merged)
                # [AF-7] total-preserving gradient: first scene +lead, remaining N-1 share the cost.
                # sum delta = +lead - (N-1)*(lead/(N-1)) = 0 -> preserves total duration.
                per = lead / max(1, n - 1)
                shifted = []
                for i, sc in enumerate(merged):
                    d = float(sc.duration_seconds or 0.0)
                    if i == 0:
                        new_d = d + lead
                    else:
                        new_d = max(SCENE_MIN_SEC * 0.5, d - per)
                    shifted.append(sc.model_copy(update={"duration_seconds": round(new_d, 2)}))
                merged = shifted  # [AF-7] MARKER v1
                logger.info(f"[AF-7] SCENE_LEAD={lead}s / {n}씬 — 첫씬 +{lead}s / 나머지 -{per:.3f}s (총길이 보존)")
            except Exception as _lead_err:
                logger.warning(f"[AD] scene lead 적용 실패 (무시): {_lead_err}")

        # [AF-11] Extend last scene to cover audio tail silence beyond last Whisper segment.
        try:
            audio_path_for_dur = ts_data.get("audio_path")
            if audio_path_for_dur and Path(audio_path_for_dur).exists() and merged:
                probe = subprocess.run(
                    ["ffprobe", "-v", "error", "-show_entries", "format=duration",
                     "-of", "csv=p=0", audio_path_for_dur],
                    capture_output=True, text=True, timeout=10,
                )
                raw_dur = (probe.stdout or "").strip()
                if raw_dur and raw_dur not in ("N/A", ""):
                    total_audio_real = float(raw_dur)
                    total_scenes = sum(float(s.duration_seconds or 0.0) for s in merged)
                    gap = total_audio_real - total_scenes
                    if gap > 0.25:
                        extra = round(gap, 2)
                        cur = float(merged[-1].duration_seconds or 0.0)
                        merged[-1] = merged[-1].model_copy(
                            update={"duration_seconds": round(cur + extra, 2)}
                        )
                        logger.info(
                            f"[AF-11] 오디오 tail +{extra}s → 마지막 씬 연장 "
                            f"(audio={total_audio_real:.2f}s scenes={total_scenes:.2f}s)"
                        )
        except Exception as _tail_err:
            logger.debug(f"[AF-11] tail-extend skip: {_tail_err}")

        # [S] 장면 길이 변주 (편안한 리듬 — 너무 균일하면 어색, 너무 들쭉날쭉도 불안)
        if SCENE_LEN_VARIANCE > 0 and len(merged) > 1:
            import random as _scene_rnd
            _scene_rnd.seed(42)  # 결정적 변주 (같은 씬 → 같은 변주)
            varied = []
            cum_orig = sum(s.duration_seconds for s in merged)
            for s in merged:
                # ±variance 범위에서 랜덤
                offset = _scene_rnd.uniform(-SCENE_LEN_VARIANCE, SCENE_LEN_VARIANCE)
                new_dur = max(SCENE_MIN_SEC, min(SCENE_MAX_SEC + 0.5, s.duration_seconds + offset))
                varied.append(s.model_copy(update={"duration_seconds": round(new_dur, 2)}))
            # 전체 길이 보정 (TTS 와 맞춤)
            cum_new = sum(s.duration_seconds for s in varied)
            if cum_new > 0 and abs(cum_new - cum_orig) > 0.5:
                ratio = cum_orig / cum_new
                varied = [
                    s.model_copy(update={"duration_seconds": round(s.duration_seconds * ratio, 2)})
                    for s in varied
                ]
            logger.info(f"[S] 장면 길이 변주: ±{SCENE_LEN_VARIANCE}s")
            merged = varied

        logger.info(
            f"의미 재분해 완료: {len(scenes)}씬 → {len(merged)}씬 "
            f"(총 {sum(s.duration_seconds for s in merged):.1f}s / TTS {total_audio:.1f}s)"
        )
        return merged

    except Exception as e:
        logger.error(f"의미 재분해 오류 (원본 사용): {e}", exc_info=True)
        return scenes


# [AF-4/8] subtitle keyword highlight helper (Korean-aware)
def _highlight_keywords_in_srt(srt_path: Path, scenes: list) -> bool:
    """Extract Korean nouns from scene descriptions + English keywords,
    wrap matches inside SRT cue text with yellow ASS override tags.
    """
    try:
        if not srt_path.exists():
            return False
        import re as _re
        keywords: set = set()
        for s in scenes or []:
            kw = getattr(s, "keyword", None) or (s.get("keyword") if isinstance(s, dict) else None)
            desc = getattr(s, "description", None) or (s.get("description") if isinstance(s, dict) else None)
            # 1) Add raw English/Korean keyword if 2+ chars
            if kw and isinstance(kw, str) and len(kw.strip()) >= 2:
                keywords.add(kw.strip())
            # 2) Extract Korean nouns (2+ Hangul syllables) from description
            if desc and isinstance(desc, str):
                # hangul syllable block U+AC00-U+D7A3
                for match in _re.findall(r"[\uac00-\ud7a3]{2,}", desc):
                    # Filter out common particles/connectives
                    if match in ("입니다", "합니다", "습니다", "됩니다", "있습", "없습", "하여", "으로", "에서", "이고", "보겠", "살펴", "보고"):
                        continue
                    if len(match) >= 2:
                        keywords.add(match)
        if not keywords:
            return False
        content = srt_path.read_text(encoding="utf-8")
        # ASS override tag: yellow highlight then reset to white
        wrap = lambda w: "{\\c&H00E27AFF&}" + w + "{\\c&H00FFFFFF&}"
        changed = 0
        # Longest first so shorter substrings dont break longer matches.
        # Use a sentinel to avoid double-wrapping nested matches.
        for kw in sorted(keywords, key=len, reverse=True):
            if kw in content:
                content = content.replace(kw, wrap(kw))
                changed += 1  # [AF-10] MARKER v1
        if changed:
            srt_path.write_text(content, encoding="utf-8")
            logger.info(f"[AF-8] 자막 키워드 강조: {changed}회 치환 ({len(keywords)}개 후보)")
            return True
        else:
            logger.info(f"[AF-8] 키워드 {len(keywords)}개 후보 있으나 자막에 매치 없음")
    except Exception as e:
        logger.warning(f"[AF-8] 자막 강조 실패 (무시): {e}")
    return False


async def generate_tts_for_job(job_id: str, scenes: list, request) -> bool:
    """[DEPRECATED] ensure_tts_assets() 래퍼 — 하위호환용"""
    r = await ensure_tts_assets(job_id, scenes, request)
    return r.get("ok", False)


async def ensure_tts_assets(job_id: str, scenes: list, request) -> dict:
    """
    [v15.59.0] TTS mp3 + timestamps.json 동시 보장.
    mp3만 있고 timestamps 없으면 lf2_tts 재호출.
    audio_url 있으면 HEAD 검증.
    반환: {"ok": bool, "mp3_path": Path|None, "ts_path": Path|None,
           "error_code": str|None, "message": str|None, "retryable": bool}
    """
    mp3_path = TMP_DIR / f"{job_id}.mp3"
    ts_path  = TMP_DIR / f"{job_id}_timestamps.json"

    def _ts_valid(p):
        try:
            import json as _j
            d = _j.loads(p.read_text(encoding="utf-8"))
            segs = d.get("segments") or []
            return isinstance(segs, list) and len(segs) > 0
        except Exception:
            return False

    # 1. mp3 + timestamps 모두 정상 → 재사용
    if (mp3_path.exists() and mp3_path.stat().st_size > 1024
            and ts_path.exists() and _ts_valid(ts_path)):
        logger.info(f"[TTS] 재사용: {mp3_path.stat().st_size//1024}KB + timestamps OK")
        return {"ok": True, "mp3_path": mp3_path, "ts_path": ts_path,
                "error_code": None, "retryable": False}

    # 2. mp3만 있고 timestamps 없음 → 재생성 필요
    if mp3_path.exists() and mp3_path.stat().st_size > 1024:
        logger.warning(f"[TTS] mp3 존재하나 timestamps 없음 → 재생성: {job_id}")

    # 3. audio_url 있으면 HEAD 검증
    audio_url = getattr(request, "audio_url", None)
    if audio_url:
        try:
            import httpx as _hx
            async with _hx.AsyncClient(timeout=5.0) as _cli:
                head = await _cli.head(audio_url, follow_redirects=True)
            if head.status_code >= 400:
                return {"ok": False, "error_code": "TTS_URL_UNREACHABLE",
                        "message": f"audio_url HTTP {head.status_code}", "retryable": False}
            if ts_path.exists() and _ts_valid(ts_path):
                return {"ok": True, "mp3_path": mp3_path, "ts_path": ts_path,
                        "error_code": None, "retryable": False}
        except Exception as _ue:
            return {"ok": False, "error_code": "TTS_URL_INVALID",
                    "message": str(_ue), "retryable": True}

    narration_parts = []
    for s in scenes:
        s_dict = s.model_dump() if hasattr(s, "model_dump") else (s if isinstance(s, dict) else {})
        text = (s_dict.get("narration") or s_dict.get("description") or s_dict.get("keyword") or "").strip()
        if text:
            narration_parts.append(text)

    if not narration_parts:
        logger.warning("[TTS] 나레이션 없음 — validation error")
        return {"ok": False, "error_code": "TTS_NARRATION_EMPTY",
                "message": "모든 씬에 narration/description 없음", "retryable": False}

    full_script = " ".join(narration_parts)
    logger.info(f"[TTS-AUTO] TTS 생성: {len(full_script)}자 / {len(narration_parts)}씬")

    try:
        import httpx as _httpx
        payload = {
            "text": full_script,
            "filename": job_id,
            "engine": "edge",
            "edge_voice": "ko-KR-SunHiNeural",
            "edge_rate": "-5%",
            "preprocess": True,
        }
        async with _httpx.AsyncClient(timeout=300.0) as _cli:
            resp = await _cli.post("http://lf2_tts:8001/tts", json=payload)
            if resp.status_code != 200:
                logger.error(f"[TTS-AUTO] TTS 서비스 오류: {resp.status_code} {resp.text[:200]}")
                return False
            data = resp.json()

        tts_file = data.get("file_path", "")
        ts_file  = data.get("timestamps_path", "")
        import shutil as _shutil

        if tts_file and Path(tts_file).exists():
            _shutil.copy2(tts_file, mp3_path)
            logger.info(f"[TTS] mp3 저장: {mp3_path.stat().st_size//1024}KB")
        else:
            logger.error(f"[TTS] mp3 파일 없음: {tts_file}")
            return {"ok": False, "error_code": "TTS_MP3_MISSING",
                    "message": f"lf2_tts mp3 없음: {tts_file}", "retryable": True}

        if ts_file and Path(ts_file).exists():
            _shutil.copy2(ts_file, ts_path)
            logger.info(f"[TTS] timestamps 저장 OK")
            return {"ok": True, "mp3_path": mp3_path, "ts_path": ts_path,
                    "error_code": None, "retryable": False}
        else:
            logger.warning("[TTS] timestamps 없음 — ASS 자막 불가")
            return {"ok": True, "mp3_path": mp3_path, "ts_path": None,
                    "error_code": "TTS_TIMESTAMP_MISSING", "retryable": False}

    except Exception as _e:
        logger.error(f"[TTS] 생성 실패: {_e}", exc_info=True)
        return {"ok": False, "error_code": "TTS_SERVICE_ERROR",
                "message": str(_e), "retryable": True}


def create_ass_karaoke_from_whisper(timestamps_path, output_path, lead_sec: float = 0.0) -> bool:
    """
    [KARAOKE] Whisper 단어 단위 타임스탬프 → ASS 카라오케 자막.
    발음 중인 단어: 노란 굵게 (\\kf 채워지는 효과) / 나머지: 흰색.
    PlayResX=1920, PlayResY=1080 기준. 유료 서비스 수준 품질.
    """
    import json as _json
    MAX_CHARS = 20  # 한국어 기준 한 줄 최대

    try:
        if not timestamps_path or not Path(timestamps_path).exists():
            return False

        with open(timestamps_path, encoding="utf-8") as f:
            ts_data = _json.load(f)

        # 단어 목록 수집 (words → segments.words → segments 순)
        words = list(ts_data.get("words") or [])
        if not words:
            for seg in (ts_data.get("segments") or []):
                for w in (seg.get("words") or []):
                    words.append(w)
        if not words:
            for seg in (ts_data.get("segments") or []):
                text = (seg.get("text") or "").strip()
                if text:
                    words.append({"word": text, "start": seg.get("start", 0), "end": seg.get("end", 0)})
        if not words:
            return False

        def _t(sec):
            sec = max(0.0, float(sec) + lead_sec)
            h = int(sec // 3600); m = int((sec % 3600) // 60); s = sec % 60
            return f"{h}:{m:02d}:{s:05.2f}"

        ass_header = (
            "[Script Info]\n"
            "ScriptType: v4.00+\n"
            "PlayResX: 1920\n"
            "PlayResY: 1080\n"
            "ScaledBorderAndShadow: yes\n\n"
            "[V4+ Styles]\n"
            "Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, "
            "Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, "
            "Shadow, Alignment, MarginL, MarginR, MarginV, Encoding\n"
            "Style: Karaoke,Noto Sans CJK KR,46,&H00FFFFFF,&H0000FFFF,&H00000000,&HB4000000,"
            "-1,0,0,0,100,100,0,0,1,2.5,1.2,2,80,80,50,1\n\n"
            "[Events]\n"
            "Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text\n"
        )

        # 단어 → 줄 그룹핑
        groups, cur, cur_len = [], [], 0
        for w in words:
            wt = (w.get("word") or "").strip()
            if not wt:
                continue
            if cur_len + len(wt) > MAX_CHARS and cur:
                groups.append(cur); cur = []; cur_len = 0
            cur.append(w); cur_len += len(wt) + 1
        if cur:
            groups.append(cur)

        dialogues = []
        for grp in groups:
            ls = grp[0].get("start", 0)
            le = grp[-1].get("end", ls + 3)
            kara = ""
            for w in grp:
                ws = float(w.get("start", 0)); we = float(w.get("end", ws + 0.3))
                cs = max(1, int((we - ws) * 100))
                _raw_w = (w.get('word') or '').strip()
                _raw_w = inject_flags_in_word(_raw_w)
                kara += f"{{\\kf{cs}}}{_raw_w} "
            dialogues.append(
                f"Dialogue: 0,{_t(ls)},{_t(le)},Karaoke,,0,0,0,,{kara.strip()}"
            )

        Path(output_path).write_text(ass_header + "\n".join(dialogues) + "\n", encoding="utf-8-sig")
        logger.info(f"[KARAOKE] ASS 생성: {len(dialogues)}줄 → {output_path}")
        return True

    except Exception as _e:
        logger.error(f"[KARAOKE] ASS 생성 실패: {_e}", exc_info=True)
        return False


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

        # [AG-1] Load word-level timestamps for precise cue boundaries.
        all_words = ts_data.get("words") or []
        word_timing_matches = 0
        word_timing_total = 0

        def _find_word_time(text_snippet: str, seg_s: float, seg_e: float, hint_start: bool) -> float:
            """Find the word whose text matches the first(is_start) or last(is_end) token of text_snippet.
            Returns its start (hint_start=True) or end time. None if no match in segment range.
            """
            snippet = text_snippet.strip().replace("\n", " ")
            if not snippet or not all_words:
                return None
            target_word = snippet.split()[0] if hint_start else snippet.split()[-1]
            # Clean punctuation
            for p in (",", ".", "!", "?", "。", "、"):
                target_word = target_word.replace(p, "")
            if not target_word:
                return None
            # Scan words within segment range with some tolerance
            tol = 0.3
            candidates = [w for w in all_words
                          if (seg_s - tol) <= float(w.get("start", 0)) <= (seg_e + tol)]
            if not candidates:
                return None
            # Exact match first
            for w in candidates:
                wt = (w.get("word") or "").strip().replace(",", "").replace(".", "")
                if wt == target_word:
                    return float(w["start"]) if hint_start else float(w["end"])
            # Substring match (Whisper may split compound words)
            for w in candidates:
                wt = (w.get("word") or "").strip()
                if target_word in wt or wt in target_word:
                    return float(w["start"]) if hint_start else float(w["end"])
            return None

        cues = []  # [(start, end, [line1, line2])]
        for seg in segments:
            seg_start = float(seg.get("start", 0.0) or 0.0)
            seg_end = float(seg.get("end", 0.0) or 0.0)
            seg_text = (seg.get("text") or "").strip()
            if not seg_text or seg_end <= seg_start:
                continue

            lines = wrap_lines(seg_text, SUBTITLE_MAX_CHARS)
            cue_chunks = [lines[i:i+2] for i in range(0, len(lines), 2)]
            if not cue_chunks:
                continue

            # Single-chunk segment: use segment boundaries directly (Whisper-native)
            if len(cue_chunks) == 1:
                cues.append((seg_start, seg_end, cue_chunks[0]))
                continue

            # Multi-chunk segment: use word-level timing per chunk boundary.
            prev_end_time = seg_start
            total_chars = sum(len(l) for l in lines) or 1
            cum_chars = 0
            for ci, chunk in enumerate(cue_chunks):
                chunk_text = " ".join(chunk)
                chunk_chars = sum(len(l) for l in chunk)
                word_timing_total += 1

                # Start time: from Whisper word matching first token
                w_start = _find_word_time(chunk_text, seg_start, seg_end, hint_start=True)
                if w_start is not None and prev_end_time <= w_start <= seg_end + 0.2:
                    cue_start = max(prev_end_time, w_start)
                    word_timing_matches += 1
                else:
                    ratio_start = cum_chars / total_chars
                    cue_start = seg_start + (seg_end - seg_start) * ratio_start

                cum_chars += chunk_chars

                # End time: from Whisper word matching last token
                w_end = _find_word_time(chunk_text, seg_start, seg_end, hint_start=False)
                if w_end is not None and cue_start < w_end <= seg_end + 0.2:
                    cue_end = w_end
                    word_timing_matches += 1
                else:
                    ratio_end = cum_chars / total_chars
                    cue_end = seg_start + (seg_end - seg_start) * ratio_end

                cues.append((cue_start, cue_end, chunk))
                prev_end_time = cue_end  # [AG-1] MARKER v1

        if word_timing_total > 0:
            logger.info(
                f"[AG-1] word-level 자막 타이밍: {word_timing_matches}/{word_timing_total*2} "
                f"매칭 ({len(all_words)} words 사용)"
            )

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
            prev_end = adj_end + 0.10  # [AI-9] min 0.1s gap between cues

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


# [AC] MARKER v1
# ============================================================================
# [AC] Stage-based retry - state.json checkpoint/resume
# ============================================================================

class JobState:
    """Persist per-stage completion to /data/jobs/{job_id}/state.json."""

    STAGES_ORDER = [
        "scenes_loaded",
        "tts_synced",
        "whisper_rebuilt",
        "assets_downloaded",
        "clips_prepared",
        "concat_done",
        "audio_mixed",
        "subtitles_added",
        "thumbnail_extracted",
        "shorts_done",
        "youtube_uploaded",
        "completed",
    ]

    def __init__(self, job_id: str):
        self.job_id = job_id
        self.state_file = JOBS_DIR / job_id / "state.json"
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        self.data: dict = {}
        self._load()

    def _load(self) -> None:
        if self.state_file.exists():
            try:
                self.data = json.loads(self.state_file.read_text(encoding="utf-8"))
            except Exception as e:
                logger.warning(f"[AC] state.json parse failed - reset: {e}")
                self.data = {}
        self.data.setdefault("job_id", self.job_id)
        self.data.setdefault("stages", {})
        self.data.setdefault("request", None)
        self.data.setdefault("last_error", None)

    def save(self) -> None:
        try:
            self.data["updated_at"] = datetime.now().isoformat()
            self.state_file.write_text(
                json.dumps(self.data, ensure_ascii=False, indent=2, default=str),
                encoding="utf-8",
            )
        except Exception as e:
            logger.warning(f"[AC] state.json save failed: {e}")

    def remember_request(self, request) -> None:
        try:
            if hasattr(request, "model_dump"):
                self.data["request"] = request.model_dump(mode="json")
            elif hasattr(request, "dict"):
                self.data["request"] = request.dict()
            else:
                self.data["request"] = dict(request)
            self.save()
        except Exception as e:
            logger.warning(f"[AC] request serialize failed: {e}")

    def has(self, stage: str) -> bool:
        return stage in self.data.get("stages", {})

    def mark(self, stage: str, payload: dict = None) -> None:
        self.data.setdefault("stages", {})[stage] = {
            "at": datetime.now().isoformat(),
            "data": payload or {},
        }
        self.save()

    def get_payload(self, stage: str) -> dict:
        return self.data.get("stages", {}).get(stage, {}).get("data", {}) or {}

    def set_error(self, err: str) -> None:
        self.data["last_error"] = err
        self.save()

    def clear_from(self, stage: str) -> None:
        if stage not in self.STAGES_ORDER:
            return
        idx = self.STAGES_ORDER.index(stage)
        for s in self.STAGES_ORDER[idx:]:
            self.data.get("stages", {}).pop(s, None)
        self.save()


def _rebuild_request_from_state(state):
    raw = state.data.get("request")
    if not raw:
        return None
    try:
        return VideoCreateRequest(**raw)
    except Exception as e:
        logger.warning(f"[AC] request rebuild failed: {e}")
        return None



# [AX] Watermark 완전 비활성 - 항상 no-op
def apply_watermark(input_path: Path, output_path: Path) -> bool:
    """[AX] Watermark 기능은 비활성화됨. 항상 False 반환 (영상에 로고/그림 오버레이 없음)."""
    return False  # [AX] MARKER v1


# [AU-5] Credits logging
def log_credits(job_id: str, tts_chars: int = 0, llm_tokens: int = 0,
                pexels_calls: int = 0, duration_sec: float = 0):
    """Append credit usage to credits.log (JSON lines)."""
    try:
        rec = {
            "job_id": job_id,
            "ts": datetime.now().isoformat(),
            "tts_chars": tts_chars,
            "llm_tokens": llm_tokens,
            "pexels_calls": pexels_calls,
            "video_duration_sec": duration_sec,
            "estimated_cents": (tts_chars * 0.03) + (llm_tokens * 0.002) + (pexels_calls * 0.1),
        }
        log_path = OUTPUT_DIR.parent / "credits.log"
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except Exception as e:
        logger.debug(f"[AU-5] credits log 실패: {e}")


async def process_video_creation(
    job_id: str,
    request: VideoCreateRequest,
    resume: bool = False,
) -> None:
    """Video generation (background). [AC] resume=True uses state.json checkpoint."""
    state = JobState(job_id)
    state.remember_request(request)
    global _CURRENT_JOB
    _job_lock_token = await _redis_acquire_lock(job_id, timeout_sec=3600)
    if _job_lock_token is None:
        logger.warning(f"동시 실행 거부 (Redis lock): {job_id}")
        await update_job_status(job_id, JobStatus.FAILED, error="다른 잡 처리 중")
        return
    if _job_lock_token == "noop" and _CURRENT_JOB and _CURRENT_JOB != job_id:
        logger.warning(f"동시 실행 거부 (fallback): {job_id}")
        await update_job_status(job_id, JobStatus.FAILED, error="다른 잡 처리 중")
        return
    _CURRENT_JOB = job_id
    await _redis_set_job(job_id, JobStatus.PROCESSING, progress=5, step="initializing")
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
        state.mark("scenes_loaded", {"count": len(scenes)})  # [AC] MARKER body

        # [TTS-AUTO] audio_url 없을 때 TTS 자동 생성 (lf2_tts:8001 호출)
        if not getattr(request, "audio_url", None) and not (TMP_DIR / f"{job_id}.mp3").exists():
            _tts_result = await ensure_tts_assets(job_id, scenes, request)
            _tts_ok = _tts_result.get("ok", False)
            if not _tts_ok:
                _ec = _tts_result.get("error_code", "TTS_ERROR")
                _msg = _tts_result.get("message", "TTS 실패")
                logger.warning(f"[TTS] {_ec}: {_msg}")
            await _redis_set_job(job_id, JobStatus.TTS_GENERATING, progress=18,
                step="tts_generating",
                message="TTS 완료" if _tts_ok else _tts_result.get("message","TTS 실패"),
                error_code=None if _tts_ok else _tts_result.get("error_code"))
            if _tts_ok:
                logger.info("[TTS-AUTO] TTS 자동 생성 완료")
            else:
                logger.warning("[TTS-AUTO] TTS 자동 생성 실패 — 음성 없이 진행")

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
        state.mark("tts_synced")

        # [AZ] Auto-extract per-segment keywords if missing from timestamps.json
        try:
            if tts_timestamps and tts_timestamps.exists():
                import json as _j
                _td = _j.loads(tts_timestamps.read_text(encoding="utf-8"))
                # [BJ] 기존 keywords가 한국어/긴 문장이면 무효로 간주 → 재추출
                _existing = _td.get("segment_keywords") or []
                _needs_regen = not _existing
                if _existing and not _needs_regen:
                    # 첫 항목 검사: 한국어(한글) 포함 또는 30자 이상이면 invalid
                    try:
                        first_kw = (_existing[0].get("keywords") or [""])[0]
                        if not first_kw:
                            _needs_regen = True
                        else:
                            # 한글 유니코드 AC00-D7A3
                            has_hangul = any("\uac00" <= ch <= "\ud7a3" for ch in first_kw)
                            too_long = len(first_kw) > 30
                            if has_hangul or too_long:
                                _needs_regen = True
                                logger.info(f"[BJ] 기존 segment_keywords 무효 (hangul={has_hangul} len={len(first_kw)}) — 재추출")
                    except Exception:
                        _needs_regen = True
                
                if _needs_regen and _td.get("segments"):
                    _topic = (getattr(request, "title", "") or "").strip()
                    if not _topic and scenes:
                        _topic = (getattr(scenes[0], "description", "") or "")[:50]
                    _kws = await _batch_extract_keywords_from_segments(_td["segments"], topic_hint=_topic)
                    if _kws:
                        # Convert to [{"idx": N, "keywords": [kw]}] format
                        _td["segment_keywords"] = [
                            {"idx": i, "keywords": [_kws[i]]}
                            for i in sorted(_kws.keys())
                        ]
                        tts_timestamps.write_text(_j.dumps(_td, ensure_ascii=False, indent=2), encoding="utf-8")
                        logger.info(f"[AZ] segment_keywords 자동 생성 저장: {len(_kws)}개")
        except Exception as _az_err:
            logger.warning(f"[AZ] segment_keywords 자동 생성 실패: {_az_err}")

        # [4]+[7] Whisper segments 기반 의미 재분해 + 리듬 컷 적용
        scenes = rebuild_scenes_from_whisper_segments(scenes, tts_timestamps)
        state.mark("whisper_rebuilt")
        # [AD] Timeline audit: log scene boundaries vs cue boundaries for verification
        if UNIFIED_TIMELINE and tts_timestamps and tts_timestamps.exists():
            try:
                _ts = json.loads(tts_timestamps.read_text(encoding='utf-8'))
                _segs = _ts.get('segments') or []
                if _segs:
                    _cum = 0.0
                    _rows = []
                    for i, sc in enumerate(scenes[:5]):  # log first 5 only
                        d = float(sc.duration_seconds or 0.0)
                        _cum += d
                        cue_start = float(_segs[i]['start']) if i < len(_segs) else None
                        cue_end = float(_segs[i]['end']) if i < len(_segs) else None
                        _rows.append(f"scene[{i}]={_cum-d:.2f}->{_cum:.2f} cue={cue_start}->{cue_end}")
                    logger.info('[AD] timeline audit: ' + ' | '.join(_rows))
            except Exception as _audit_err:
                logger.debug(f'[AD] audit error: {_audit_err}')

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
        state.mark("assets_downloaded", {"total": len(scenes), "with_asset": sum(1 for s in scenes if s.asset_url)})

        # [v15.60.0] Narration-First Timeline Engine
        _ntl_timeline = {}
        if NTL_ENABLED:
            try:
                _ts_path = TMP_DIR / f"{job_id}_timestamps.json"
                _ntl_timeline = build_narration_timeline(job_id, scenes, _ts_path)
                for _st in _ntl_timeline.get("scene_timings", []):
                    for _sc in scenes:
                        if _sc.scene_id == _st.get("scene_id"):
                            _sc.timing = _st
                            break
                save_timeline_report(job_id, _ntl_timeline, scenes)
                logger.info(f"[NTL] 타임라인 완료: {_ntl_timeline.get('total_duration', 0):.1f}초, "
                            f"{len(_ntl_timeline.get('scene_timings', []))}개 씬")
            except Exception as _ntl_err:
                logger.warning(f"[NTL] 타임라인 생성 실패 (계속 진행): {_ntl_err}")

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
            
            # [AC/AF-3] resume: reuse existing clips, re-render only missing ones
            clips = None
            if resume and state.has("clips_prepared"):
                prev_paths = state.get_payload("clips_prepared").get("paths", []) or []
                if prev_paths:
                    existing = [Path(p) for p in prev_paths if Path(p).exists() and Path(p).stat().st_size > 4096]
                    missing = [p for p in prev_paths if p not in [str(x) for x in existing]]
                    if missing:
                        logger.info(f"[AF-3] 부분 복구 — 존재 {len(existing)}/{len(prev_paths)}개, 누락 {len(missing)}개 재생성 시도")
                        # Fall through to regenerate below (clips stays None)
                    elif len(existing) == len(prev_paths):
                        clips = existing
                        logger.info(f"[AC] clips_prepared 스킵 — 기존 {len(clips)}개 클립 재사용")
            if clips is None:
                clips = await prepare_clips_for_longform(job_id, scenes, job_temp_dir)  # [AC] MARKER resume
            
            
            if not clips:
                raise ValueError("준비된 클립 없음")
            state.mark("clips_prepared", {"count": len(clips), "paths": [str(c) for c in clips]})
            
            await update_job_status(job_id, JobStatus.PROCESSING, progress=40.0)
            
            # [AC] resume skip: reuse combined.mp4 if concat already done
            # [AQ-1] Prepend intro / append outro to clips list if enabled
            try:
                title_text = request.title or "LongForm"
                if INTRO_ENABLED:
                    intro_path = job_temp_dir / "_intro.mp4"
                    if _make_intro_clip(title_text, intro_path):
                        clips.insert(0, intro_path)
                        logger.info(f"[AQ-1] intro prepended: {intro_path}")
                if OUTRO_ENABLED:
                    outro_path = job_temp_dir / "_outro.mp4"
                    if _make_outro_clip(outro_path):
                        clips.append(outro_path)
                        logger.info(f"[AQ-1] outro appended: {outro_path}")
            except Exception as _io_err:
                logger.warning(f"[AQ-1] intro/outro 실패: {_io_err}")
            concat_file = job_temp_dir / "concat.txt"
            combined_video = job_temp_dir / "combined.mp4"  # [AQ] MARKER v1
            skip_concat = False
            if resume and state.has("concat_done"):
                prev = state.get_payload("concat_done").get("combined")
                if prev and Path(prev).exists() and Path(prev).stat().st_size > 4096:
                    combined_video = Path(prev)
                    skip_concat = True
                    logger.info(f"[AC] concat_done 스킵 — 기존 combined.mp4 재사용: {combined_video}")
            if not skip_concat:
                if not create_concat_file(clips, concat_file):
                    raise RuntimeError("Concat 파일 생성 실패")
                if not concatenate_videos(concat_file, combined_video):
                    raise RuntimeError("영상 연결 실패")
            state.mark("concat_done", {"combined": str(combined_video)})
            
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
                shutil.copy(combined_video, output_video)
            # [AQ-3/AK-5] Excess silence trim (>3s silences shortened to 1s)
            try:
                if os.getenv("AUDIO_SILENCE_TRIM", "true").lower() in ("true","1","yes"):
                    sil_tmp = output_video.with_name(output_video.stem + "_sil.mp4")
                    # Detect silences > 3s
                    det = subprocess.run(
                        ["ffmpeg", "-i", str(output_video), "-af",
                         "silencedetect=noise=-30dB:d=3.0", "-f", "null", "-"],
                        capture_output=True, text=True, timeout=60,
                    )
                    err = (det.stderr or "") + (det.stdout or "")
                    # Parse silence_start/silence_end pairs
                    import re as _re
                    starts = [float(m) for m in _re.findall(r"silence_start:\s*([\d.]+)", err)]
                    ends = [float(m) for m in _re.findall(r"silence_end:\s*([\d.]+)", err)]
                    if starts and ends:
                        # Build atrim filter chain to skip silence excess (keep 1s of each)
                        pairs = list(zip(starts, ends))
                        logger.info(f"[AQ-3] 과잉 무음 {len(pairs)}개 검출 (>3s)")
                        # Simple approach: re-encode skipping the middle of each silence
                        # Keep first/last 0.5s of each silence, drop the middle
                        # This is complex for ffmpeg atrim/concat — use approximate setpts + asetpts
                        # For now, log and skip (mark future impl)
                        logger.info(f"[AQ-3] trim 대상: {pairs[:3]}... (skip for stability, logged only)")
            except Exception as _tr_err:
                logger.debug(f"[AQ-3] silence trim skip: {_tr_err}")

            # [AI-1] loudnorm post-process (EBU R128, -16 LUFS)
            try:
                if os.getenv("AUDIO_LOUDNORM", "true").lower() in ("true","1","yes"):
                    ln_tmp = output_video.with_name(output_video.stem + "_ln.mp4")
                    ln_cmd = [
                        "ffmpeg", "-y", "-i", str(output_video),
                        "-af", "loudnorm=I=-16:TP=-1.0:LRA=11,alimiter=limit=0.98:attack=5:release=50",
                        "-c:v", "copy", "-c:a", "aac", "-ac", "2", "-b:a", "192k",
                        str(ln_tmp),
                    ]
                    if run_ffmpeg_command(ln_cmd, timeout=120.0) and ln_tmp.exists() and ln_tmp.stat().st_size > 4096:
                        shutil.move(str(ln_tmp), str(output_video))
                        logger.info("[AI-1] loudnorm 적용 완료 (-16 LUFS)")
            except Exception as _ln_err:
                logger.warning(f"[AI-1] loudnorm 실패 (무시): {_ln_err}")
            state.mark("audio_mixed", {"output": str(output_video)})
            
            await update_job_status(job_id, JobStatus.PROCESSING, progress=70.0)
            
            # 영상 길이 조회
            duration = get_video_duration(output_video)
            
            # 썸네일 생성
            output_files = {
                "longform": str(output_video)
            }
            
            if request.generate_thumbnail:
                thumbnail_path = job_temp_dir / "thumbnail_raw.jpg"
                # [AK-3] 3-variant picker: try 3 timestamps, pick brightest
                try:
                    _d = duration or 30.0
                    _candidates = []
                    for _ti, _t in enumerate([3.0, _d * 0.4, _d * 0.7]):
                        _cp = job_temp_dir / f"thumb_v{_ti}.jpg"
                        if extract_thumbnail(output_video, _cp, timestamp=f"{_t:.2f}"):
                            # Compute avg brightness via ffprobe signalstats
                            _pr = subprocess.run(
                                ["ffmpeg", "-v", "quiet", "-i", str(_cp),
                                 "-vf", "signalstats,metadata=print:key=lavfi.signalstats.YAVG:file=-",
                                 "-f", "null", "-"],
                                capture_output=True, text=True, timeout=10,
                            )
                            _out = (_pr.stdout or "") + (_pr.stderr or "")
                            _br = 128.0
                            for _line in _out.splitlines():
                                if "YAVG=" in _line:
                                    try:
                                        _br = float(_line.split("YAVG=")[-1].strip())
                                        break
                                    except Exception:
                                        pass
                            # Score: prefer mid-range brightness 60-180
                            _score = -abs(_br - 140.0)
                            _candidates.append((_score, _cp))
                    if _candidates:
                        _candidates.sort(reverse=True)
                        shutil.copy2(_candidates[0][1], thumbnail_path)
                        logger.info(f"[AK-3] 썸네일 best: {_candidates[0][1].name} (score={_candidates[0][0]:.1f})")
                except Exception as _tk_err:
                    logger.debug(f"[AK-3] thumbnail picker skip: {_tk_err}")
                if thumbnail_path.exists() or extract_thumbnail(output_video, thumbnail_path):
                    thumbnail_final = THUMBNAILS_DIR / f"{job_id}_thumb.jpg"
                    if add_text_overlay_to_thumbnail(
                        thumbnail_path,
                        thumbnail_final,
                        title=request.title or f"Video {job_id[:8]}"
                    ):
                        output_files["thumbnail"] = str(thumbnail_final)
                        state.mark("thumbnail_extracted", {"path": str(thumbnail_final)})
            
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
                        # [v15.59.0] subtitle_path / subtitle_type 명시 분리
                        ass_path = srt_path.with_suffix(".ass")
                        subtitle_path = None
                        subtitle_type = None
                        ass_ok = create_ass_karaoke_from_whisper(tts_timestamps, ass_path)
                        if ass_ok:
                            subtitle_path = ass_path
                            subtitle_type = "ass"
                            srt_ok = True
                            logger.info("[SUBTITLE] ASS 카라오케 적용")
                            await _redis_set_job(job_id, JobStatus.SUBTITLE_CREATING,
                                progress=55, step="subtitle_creating",
                                message="ASS 카라오케 자막 생성 완료")
                        else:
                            srt_ok = create_srt_from_whisper_segments(tts_timestamps, srt_path)
                            if srt_ok:
                                subtitle_path = srt_path
                                subtitle_type = "srt"
                                logger.info(f"[SUBTITLE] SRT fallback (lead={SUBTITLE_LEAD_SEC}s)")
                                await _redis_set_job(job_id, JobStatus.SUBTITLE_CREATING,
                                    progress=55, step="subtitle_creating",
                                    message="SRT fallback 생성 완료")
                    if not srt_ok and scenes and any(s.description for s in scenes):
                        srt_ok = create_srt_from_scenes(scenes, srt_path)
                        logger.info("씬 동기화 자막 fallback 사용")
                    if not srt_ok and request.subtitle_text:
                        total_dur = duration or sum((s.duration_seconds or 5.0) for s in scenes)
                        srt_ok = create_srt_from_text(request.subtitle_text, total_dur, srt_path)
                        logger.info("텍스트 자막 fallback 사용")
                    if srt_ok:
                        _active_sub = subtitle_path if subtitle_path else srt_path
                        _active_type = subtitle_type if subtitle_type else "srt"
                        # keyword highlight: SRT만 적용 (ASS는 자체 스타일 보존)
                        if _active_type == "srt":
                            try:
                                _highlight_keywords_in_srt(_active_sub, scenes)
                            except Exception as _hi_err:
                                logger.warning(f"[AF-4] highlight skip: {_hi_err}")
                        out_sub = LONGFORM_DIR / f"{job_id}_sub.mp4"
                        await _redis_set_job(job_id, JobStatus.RENDERING, progress=70,
                            step="rendering", message="자막 오버레이 렌더링 중")
                        if add_subtitles_to_video(output_video, _active_sub, out_sub,
                                                   subtitle_type=_active_type):
                            shutil.move(str(out_sub), str(output_video))
                            output_files["longform"] = str(output_video)
                            logger.info("자막 오버레이 완료")
                            state.mark("subtitles_added", {"output": str(output_video)})
                except Exception as e:
                    logger.error(f"자막 오류: {e}")

            # 숏폼 생성
            if request.generate_shorts:
                shorts_output = SHORTS_DIR / f"{job_id}_short.mp4"
                if create_shortform_from_longform(output_video, shorts_output):
                    output_files["shorts"] = str(shorts_output)
                    state.mark("shorts_done", {"path": str(shorts_output)})
                    await update_job_status(job_id, JobStatus.PROCESSING, progress=90.0, output_files=output_files)
        
        await update_job_status(
            job_id,
            JobStatus.COMPLETED,
            progress=100.0,
            output_files=output_files,
            duration_seconds=duration
        )
        
        # [AF-5b] QA: compare final video duration to audio duration
        try:
            audio_ref = Path(request.audio_url) if getattr(request, "audio_url", None) else (TMP_DIR / f"{job_id}.mp3")
            audio_dur = get_video_duration(audio_ref) or 0.0
            video_dur = duration or 0.0
            if audio_dur > 0 and video_dur > 0:
                diff = abs(audio_dur - video_dur)
                if diff > 0.5:
                    logger.warning(f"[AF-5b] QA 경고 — 영상/오디오 duration 오차 {diff:.2f}s (video={video_dur:.2f}s audio={audio_dur:.2f}s)")
                else:
                    logger.info(f"[AF-5b] QA OK — duration diff {diff:.2f}s (video={video_dur:.2f}s audio={audio_dur:.2f}s)")
            # [AI-10] Extended QA battery
            try:
                qa_issues = []
                # 1. Video file size sanity
                if "longform" in output_files:
                    lf = Path(output_files["longform"])
                    if lf.exists():
                        size_mb = lf.stat().st_size / (1024 * 1024)
                        if size_mb < 1.0:
                            qa_issues.append(f"영상 파일 너무 작음: {size_mb:.2f}MB")
                        elif size_mb > 500:
                            qa_issues.append(f"영상 파일 비정상 크기: {size_mb:.0f}MB")
                # 2. Scene count reasonable
                if len(scenes) < 2:
                    qa_issues.append(f"씬 개수 부족: {len(scenes)}")
                # 3. All scenes have asset
                missing_assets = sum(1 for s in scenes if not s.asset_url)
                if missing_assets > 0:
                    qa_issues.append(f"asset 누락 씬: {missing_assets}개")
                # 4. Thumbnail exists
                if "thumbnail" not in output_files:
                    qa_issues.append("썸네일 미생성")
                if qa_issues:
                    logger.warning(f"[AI-10] QA 경고: {qa_issues}")
                else:
                    logger.info(f"[AI-10] QA 전체 통과 ({len(scenes)}씬, asset 100%, 썸네일 OK)")
            except Exception as _qa2_err:
                logger.debug(f"[AI-10] QA battery skip: {_qa2_err}")
        except Exception as _qa_err:
            logger.debug(f"[AF-5b] QA 체크 실패: {_qa_err}")
        logger.info(f"작업 완료: {job_id}")
        state.mark("completed", {"output_files": output_files})
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
                            # [AJ-5] YouTube chapters - build cumulative timestamps from scenes
                            try:
                                chapters = ["00:00 시작"]
                                cum = 0.0
                                for idx, s in enumerate(sc_list[:15], 1):  # max 15 chapters
                                    cum += float(s.get("duration_seconds", 0) or 0)
                                    mm = int(cum // 60)
                                    ss = int(cum % 60)
                                    title = (s.get("description") or s.get("keyword") or f"챕터 {idx}")[:40]
                                    chapters.append(f"{mm:02d}:{ss:02d} {title}")
                                desc = "\n".join(chapters) + "\n\n" + desc
                            except Exception:
                                pass
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
                        state.mark("youtube_uploaded", {"url": yt_url})
                        await update_job_status(job_id, JobStatus.COMPLETED, progress=100.0, output_files=output_files, duration_seconds=duration)
                    else:
                        logger.warning(f"YouTube 업로드 실패 {yt_resp.status_code}: {yt_resp.text[:300]}")
            except Exception as yt_err:
                logger.warning(f"YouTube 자동 업로드 오류 (무시): {yt_err}")
    
    except Exception as e:
        logger.error(f"영상 생성 오류 ({job_id}): {e}")
        try:
            state.set_error(str(e))
        except Exception:
            pass
        await update_job_status(job_id, JobStatus.FAILED, error=str(e))
    finally:
        _CURRENT_JOB = None
        await _redis_release_lock(job_id, _job_lock_token)
        await _redis_release_lock(job_id, _job_lock_token)


# ============================================================================
# API 엔드포인트
# ============================================================================

@app.get("/video/enhancements", tags=["System"])
async def list_enhancements():
    """[AL-5] List all enhancement markers present in app.py."""
    return {
        "version": "15.74.0",
        "rounds": {
            "AC": "단계별 재시도 + resume",
            "AD": "통합 타임라인",
            "AE": "씬 레이아웃 5 템플릿 (opt-in)",
            "AF": "영상 품질 1차 강화 (10)",
            "AG": "word-level Whisper 자막 + TTS 안정화",
            "AH": "Whisper 절대시간 정렬 (gap 흡수) + fallback Korean",
            "AI": "영상 강화 10단계 (loudnorm, 13 transition, vignette PI/5)",
            "AJ": "영화적 마감 (intro/outro/chapters)",
            "AK": "프로덕션 품질 (colorbalance, limiter, thumbnail 3-variant)",
            "AL": "신뢰성 (Pexels 캐시, smoke test)",
        },
        "subtitle_timing": "Whisper words + silence snap",
        "scene_timing": "Whisper absolute (AH-4)",
        "features": {
            "intro_enabled": INTRO_ENABLED,
            "outro_enabled": OUTRO_ENABLED,
            "audio_loudnorm": os.getenv("AUDIO_LOUDNORM", "true"),
            "enable_scene_layout": ENABLE_SCENE_LAYOUT,
            "unified_timeline": UNIFIED_TIMELINE,
        },
    }


@app.get("/health", tags=["System"])
async def health_check():
    """헬스 체크"""
    return {
        "status": "healthy",
        "service": "lf_ffmpeg_worker",
        "version": "15.74.0",
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


# [AC] MARKER endpoint
@app.get("/video/state/{job_id}", tags=["Video"])
async def get_video_state(job_id: str):
    """[AC] Return state.json snapshot for a job. 404 if missing."""
    job_id = (job_id or "").strip().replace("\r", "").replace("\n", "")
    state_file = JOBS_DIR / job_id / "state.json"
    if not state_file.exists():
        raise HTTPException(status_code=404, detail=f"state.json not found: {job_id}")
    try:
        data = json.loads(state_file.read_text(encoding="utf-8"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"state.json parse error: {e}")
    stages = data.get("stages", {}) or {}
    done_order = [s for s in JobState.STAGES_ORDER if s in stages]
    next_stage = None
    for s in JobState.STAGES_ORDER:
        if s not in stages:
            next_stage = s
            break
    return {
        "job_id": job_id,
        "stages_done": done_order,
        "next_stage": next_stage,
        "last_error": data.get("last_error"),
        "updated_at": data.get("updated_at"),
        "raw": data,
    }


@app.post("/video/resume/{job_id}", tags=["Video"])
async def resume_video(job_id: str, background_tasks: BackgroundTasks):
    """[AC] Resume video generation from last successful stage.
    Requires prior process_video_creation to have saved state.json with request.
    """
    global _CURRENT_JOB
    job_id = (job_id or "").strip().replace("\r", "").replace("\n", "")
    if not job_id:
        raise HTTPException(status_code=400, detail="job_id empty")
    state_file = JOBS_DIR / job_id / "state.json"
    if not state_file.exists():
        raise HTTPException(status_code=404, detail=f"state.json not found: {job_id}")
    state = JobState(job_id)
    if state.has("completed"):
        return {"success": True, "status": "already_completed", "job_id": job_id}
    req = _rebuild_request_from_state(state)
    if req is None:
        raise HTTPException(status_code=400, detail="request payload missing or invalid in state.json")
    if _CURRENT_JOB is not None and _CURRENT_JOB != job_id:
        raise HTTPException(status_code=409, detail=f"another job running: {_CURRENT_JOB}")
    await update_job_status(job_id, JobStatus.PROCESSING, progress=5.0)
    # Clear last_error on resume
    state.data["last_error"] = None
    state.save()
    background_tasks.add_task(process_video_creation, job_id, req, True)
    stages_done = [s for s in JobState.STAGES_ORDER if state.has(s)]
    return {
        "success": True,
        "status": "resuming",
        "job_id": job_id,
        "stages_done": stages_done,
        "resume_from": next((s for s in JobState.STAGES_ORDER if not state.has(s)), "completed"),
    }


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



# [P0] MARKER v1
# ============================================================================
# [P0-1..4] Paid service integrations
# ============================================================================
# Mount auth + billing routers if modules available
try:
    import sys
    sys.path.insert(0, "/app")
    from auth_module import auth_dependency, check_quota, consume_credits, generate_api_key, load_users, save_users, PLANS
    from billing_module import create_subscription_router, get_plan_amount

    app.include_router(create_subscription_router())
    logger.info("[P0] auth + billing 라우터 등록 완료")

    @app.post("/auth/register", tags=["Auth"])
    async def auth_register(email: str, plan: str = "free"):
        """Create API key for new user."""
        user_id = f"user_{int(datetime.now().timestamp())}"
        key = generate_api_key(user_id, email, plan)
        return {"success": True, "api_key": key, "plan": plan, "user_id": user_id}

    @app.get("/auth/me", tags=["Auth"])
    async def auth_me(x_api_key: str = Header(None)):
        """Current user info."""
        from auth_module import verify_api_key
        user = verify_api_key(x_api_key or "")
        if not user:
            raise HTTPException(status_code=401, detail="Invalid API key")
        return user

    @app.get("/auth/plans", tags=["Auth"])
    async def auth_plans():
        """List available plans."""
        return {"plans": PLANS, "prices_krw": {"pro": 29900, "enterprise": 99000}}
except ImportError as e:
    logger.warning(f"[P0] auth/billing 모듈 로드 실패 (무시): {e}")


# [P0-2] Job queue — serialize concurrent /video/create requests
import asyncio as _asyncio_p0
_JOB_QUEUE: _asyncio_p0.Queue = _asyncio_p0.Queue(maxsize=20)
_JOB_WORKER_RUNNING = False


async def _job_queue_worker():
    """Single worker that processes /video/create jobs one at a time."""
    global _JOB_WORKER_RUNNING
    _JOB_WORKER_RUNNING = True
    logger.info("[P0-2] job queue worker 시작")
    while True:
        try:
            item = await _JOB_QUEUE.get()
            if item is None:
                break
            job_id, request, resume = item
            try:
                logger.info(f"[P0-2] queue -> start: {job_id}")
                await process_video_creation(job_id, request, resume=resume)
            except Exception as e:
                logger.error(f"[P0-2] job {job_id} 실패: {e}")
                # [P0-3] auto retry via state-based resume
                try:
                    for attempt in range(1, 4):
                        logger.info(f"[P0-3] auto retry {attempt}/3: {job_id}")
                        await _asyncio_p0.sleep(2.0 * attempt)
                        state = JobState(job_id)
                        if state.has("completed"):
                            break
                        try:
                            await process_video_creation(job_id, request, resume=True)
                            break
                        except Exception as ee:
                            logger.warning(f"[P0-3] retry {attempt} 실패: {ee}")
                except Exception as re:
                    logger.error(f"[P0-3] retry 최종 실패: {re}")
            finally:
                _JOB_QUEUE.task_done()
        except _asyncio_p0.CancelledError:
            break
        except Exception as e:
            logger.error(f"[P0-2] worker 오류: {e}")


# [P0-4] WebSocket progress endpoint


import json as _json_p0

@app.websocket("/ws/job/{job_id}")
async def ws_job_progress(websocket: WebSocket, job_id: str):
    """Stream job progress over WebSocket (3s interval)."""
    await websocket.accept()
    try:
        last_progress = -1
        while True:
            info = jobs.get(job_id)
            if info:
                data = {
                    "job_id": job_id,
                    "status": info.status.value if hasattr(info.status, "value") else str(info.status),
                    "progress": info.progress,
                    "error": info.error,
                    "output_files": info.output_files or {},
                    "duration_seconds": info.duration_seconds,
                }
                if data["progress"] != last_progress:
                    await websocket.send_text(_json_p0.dumps(data, ensure_ascii=False))
                    last_progress = data["progress"]
                if data["status"] in ("completed", "failed"):
                    break
            await _asyncio_p0.sleep(1.0)
    except WebSocketDisconnect:
        logger.info(f"[P0-4] WS disconnect: {job_id}")
    except Exception as e:
        logger.warning(f"[P0-4] WS 오류: {e}")
    finally:
        try:
            await websocket.close()
        except Exception:
            pass


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
        workers=1,  # auto pipeline in-memory store 공유 위해 단일 프로세스
        log_level="info"
    )







# ============================================================================
# [v15.66.0] Auto Topic Production Engine
# POST /api/auto/topic-job  →  주제 입력 하나로 YouTube private 업로드까지 자동화
# ============================================================================

import json as _json_auto
import re  as _re_auto

# ── 1. 새 상태값 ────────────────────────────────────────────────────────────
AUTO_STEP_LABELS = {
    "queued":              "대기 중",
    "topic_analyzing":     "주제 분석 중",
    "researching":         "자료 조사 중",
    "script_generating":   "원고 생성 중",
    "scene_building":      "씬 분할 중",
    "voice_planning":      "나레이션 톤 설정 중",
    "asset_searching":     "영상 자산 검색 중",
    "asset_matching":      "영상-나레이션 매칭 중",
    "timeline_building":   "타임라인 구성 중",
    "quality_checking":    "품질 검사 중",
    "uploading_private":   "YouTube private 업로드 중",
    "needs_review":        "검수 필요",
}

TONE_VOICE_MAP = {
    "professional_documentary": {"rate": "-5%", "pitch": "+0Hz"},
    "professional":             {"rate": "-5%", "pitch": "+0Hz"},
    "documentary":              {"rate": "-7%", "pitch": "-1Hz"},
    "news":                     {"rate": "-3%", "pitch": "+0Hz"},
    "investment":               {"rate": "-6%", "pitch": "-1Hz"},
    "calm":                     {"rate": "-8%", "pitch": "-1Hz"},
    "energetic":                {"rate": "+3%", "pitch": "+1Hz"},
}

SCENE_TONE_MAP = {
    "opening":    {"rate": "-8%", "pitch": "-1Hz", "pause_sentence_ms": 450},
    "main":       {"rate": "-5%", "pitch": "+0Hz", "pause_sentence_ms": 420},
    "stats":      {"rate": "-7%", "pitch": "+0Hz", "pause_sentence_ms": 500},
    "problem":    {"rate": "-6%", "pitch": "-2Hz", "pause_sentence_ms": 460},
    "solution":   {"rate": "-3%", "pitch": "+1Hz", "pause_sentence_ms": 400},
    "closing":    {"rate": "-10%","pitch": "-2Hz", "pause_sentence_ms": 550},
}


# ── 2. 요청/응답 모델 ────────────────────────────────────────────────────────
class AutoTopicRequest(BaseModel):
    """완전 자동 주제 기반 영상 생성 요청"""
    topic: str = Field(..., description="영상 주제")
    video_type: str = Field(default="longform", description="longform / shorts / both")
    target_duration_sec: int = Field(default=300, ge=30, le=900, description="목표 길이(초)")
    tone: str = Field(default="professional_documentary", description="영상 톤")
    audience: str = Field(default="general", description="target audience")
    language: str = Field(default="ko", description="언어 코드")
    auto_upload: bool = Field(default=True, description="YouTube private 자동 업로드")
    upload_privacy: str = Field(default="private", description="public/private/unlisted")
    quality_threshold: int = Field(default=85, ge=60, le=100, description="업로드 허용 최저 품질 점수")
    mode: str = Field(default="auto", description="auto / semi_auto / expert")
    project_id: Optional[str] = Field(None, description="기존 project_id 재사용 시")

class AutoTopicResponse(BaseModel):
    job_id: str
    project_id: str
    status: str
    mode: str
    status_url: str
    message: str = ""



# ── LLM 프로바이더 설정 (멀티 백엔드) ────────────────────────────────────
LLM_PROVIDER    = os.getenv("LLM_PROVIDER", "anthropic")   # anthropic|groq|ollama|gemini
LLM_MODEL       = os.getenv("LLM_MODEL", "")               # 비어있으면 프로바이더 기본값
GROQ_API_KEY    = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL      = os.getenv("GROQ_MODEL", "meta-llama/llama-4-scout-17b-16e-instruct")
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://172.20.128.1:11434")
OLLAMA_MODEL    = os.getenv("OLLAMA_MODEL", "qwen3.5:9b")
GEMINI_API_KEY  = os.getenv("GEMINI_API_KEY", "")
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")
OPENROUTER_MODEL   = os.getenv("OPENROUTER_MODEL", "meta-llama/llama-4-scout")
CEREBRAS_API_KEY   = os.getenv("CEREBRAS_API_KEY", "")
CEREBRAS_MODEL_VAR = os.getenv("CEREBRAS_MODEL", "llama3.1-8b")
DEEPSEEK_API_KEY   = os.getenv("DEEPSEEK_API_KEY", "")
DEEPSEEK_MODEL     = os.getenv("DEEPSEEK_MODEL", "deepseek-chat")
GEMINI_MODEL    = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")

async def _call_llm_json(
    prompt: str,
    system: str = "반드시 순수 JSON만 반환. 설명·마크다운 코드블록 금지.",
    max_tokens: int = 4000,
    temperature: float = 0.4,
    retries: int = 1,
    quality_first: bool = False,  # True = anthropic/gemini 우선 (스크립트/분석 태스크)
) -> Optional[Dict]:
    """품질 우선 병렬 레이스 — quality_first=True 시 anthropic/gemini 8초 유예."""
    import asyncio

    def _parse_json_raw(raw: str) -> Optional[Dict]:
        import re as _re_inner
        raw = raw.strip()
        raw = _re_auto.sub(r"```(?:json)?\s*", "", raw).replace("```", "").strip()
        raw = _re_inner.sub(r"<think>.*?</think>", "", raw, flags=_re_inner.DOTALL).strip()
        brace = raw.find("{"); bracket = raw.find("[")
        if brace == -1 and bracket == -1:
            return None
        start = min(x for x in [brace, bracket] if x >= 0)
        try:
            return _json_auto.loads(raw[start:])
        except Exception:
            return None

    async def _call_one(provider: str) -> Optional[Dict]:
        import os as _os
        _claude_url = _os.getenv("ANTHROPIC_BASE_URL", "http://lf2_llm_proxy:8789").rstrip("/")
        _claude_key = _os.getenv("ANTHROPIC_AUTH_TOKEN", _os.getenv("ANTHROPIC_API_KEY", "local-dev"))
        for attempt in range(retries + 1):
            try:
                if provider == "anthropic":
                    async with httpx.AsyncClient(timeout=90.0) as client:
                        resp = await client.post(
                            _claude_url + "/v1/messages",
                            headers={"Content-Type": "application/json",
                                     "x-api-key": _claude_key,
                                     "anthropic-version": "2023-06-01"},
                            json={"model": LLM_MODEL or "claude-sonnet-4-6",
                                  "max_tokens": max_tokens, "temperature": temperature,
                                  "system": system,
                                  "messages": [{"role": "user", "content": prompt}]},
                        )
                        if resp.status_code != 200:
                            logger.warning(f"[LLM/anthropic] {resp.status_code}")
                            continue
                        raw = "".join(b.get("text","") for b in resp.json().get("content",[]) if b.get("type")=="text")
                        result = _parse_json_raw(raw)
                        if result is not None:
                            return result

                elif provider == "gemini" and GEMINI_API_KEY:
                    async with httpx.AsyncClient(timeout=60.0) as client:
                        resp = await client.post(
                            "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions",
                            headers={"Authorization": f"Bearer {GEMINI_API_KEY}",
                                     "Content-Type": "application/json"},
                            json={"model": LLM_MODEL or GEMINI_MODEL,
                                  "max_tokens": max_tokens, "temperature": temperature,
                                  "messages": [{"role": "system", "content": system},
                                               {"role": "user", "content": prompt}]},
                        )
                        if resp.status_code != 200:
                            logger.warning(f"[LLM/gemini] {resp.status_code}")
                            continue
                        raw = resp.json()["choices"][0]["message"]["content"]
                        result = _parse_json_raw(raw)
                        if result is not None:
                            return result

                elif provider == "openrouter" and OPENROUTER_API_KEY:
                    async with httpx.AsyncClient(timeout=60.0) as client:
                        resp = await client.post(
                            "https://openrouter.ai/api/v1/chat/completions",
                            headers={"Authorization": f"Bearer {OPENROUTER_API_KEY}",
                                     "Content-Type": "application/json",
                                     "HTTP-Referer": "https://longform-factory.local"},
                            json={"model": LLM_MODEL or OPENROUTER_MODEL,
                                  "max_tokens": max_tokens, "temperature": temperature,
                                  "messages": [{"role": "system", "content": system},
                                               {"role": "user", "content": prompt}]},
                        )
                        if resp.status_code != 200:
                            logger.warning(f"[LLM/openrouter] {resp.status_code}")
                            continue
                        raw = resp.json()["choices"][0]["message"]["content"]
                        result = _parse_json_raw(raw)
                        if result is not None:
                            return result

                elif provider == "cerebras" and CEREBRAS_API_KEY:
                    async with httpx.AsyncClient(timeout=30.0) as client:
                        resp = await client.post(
                            "https://api.cerebras.ai/v1/chat/completions",
                            headers={"Authorization": f"Bearer {CEREBRAS_API_KEY}",
                                     "Content-Type": "application/json"},
                            json={"model": LLM_MODEL or CEREBRAS_MODEL_VAR,
                                  "max_tokens": max_tokens, "temperature": temperature,
                                  "messages": [{"role": "system", "content": system},
                                               {"role": "user", "content": prompt}]},
                        )
                        if resp.status_code != 200:
                            logger.warning(f"[LLM/cerebras] {resp.status_code}")
                            continue
                        raw = resp.json()["choices"][0]["message"]["content"]
                        result = _parse_json_raw(raw)
                        if result is not None:
                            return result

                elif provider == "deepseek" and DEEPSEEK_API_KEY:
                    async with httpx.AsyncClient(timeout=60.0) as client:
                        resp = await client.post(
                            "https://api.deepseek.com/v1/chat/completions",
                            headers={"Authorization": f"Bearer {DEEPSEEK_API_KEY}",
                                     "Content-Type": "application/json"},
                            json={"model": LLM_MODEL or DEEPSEEK_MODEL,
                                  "max_tokens": max_tokens, "temperature": temperature,
                                  "messages": [{"role": "system", "content": system},
                                               {"role": "user", "content": prompt}]},
                        )
                        if resp.status_code != 200:
                            logger.warning(f"[LLM/deepseek] {resp.status_code}")
                            continue
                        raw = resp.json()["choices"][0]["message"]["content"]
                        result = _parse_json_raw(raw)
                        if result is not None:
                            return result

                elif provider == "ollama":
                    async with httpx.AsyncClient(timeout=120.0) as client:
                        resp = await client.post(
                            OLLAMA_BASE_URL + "/v1/chat/completions",
                            headers={"Content-Type": "application/json"},
                            json={"model": LLM_MODEL or OLLAMA_MODEL,
                                  "max_tokens": max_tokens, "temperature": temperature,
                                  "messages": [{"role": "system", "content": system},
                                               {"role": "user", "content": prompt}]},
                        )
                        if resp.status_code != 200:
                            logger.warning(f"[LLM/ollama] {resp.status_code}")
                            continue
                        raw = resp.json()["choices"][0]["message"]["content"]
                        result = _parse_json_raw(raw)
                        if result is not None:
                            return result

            except Exception as e:
                logger.warning(f"[LLM/{provider}] 시도{attempt+1} 실패: {e}")
        return None

    # 활성 프로바이더 목록 결정
    provider_cfg = LLM_PROVIDER.lower()
    if provider_cfg == "all":
        # 키가 있는 모든 프로바이더 병렬 레이스
        candidates = ["anthropic"]
        if GEMINI_API_KEY:       candidates.append("gemini")
        if CEREBRAS_API_KEY:     candidates.append("cerebras")
        if OPENROUTER_API_KEY:   candidates.append("openrouter")
        if DEEPSEEK_API_KEY:     candidates.append("deepseek")
        candidates.append("ollama")  # 항상 fallback
    else:
        candidates = [provider_cfg]

    if len(candidates) == 1:
        return await _call_one(candidates[0])

    # 품질 우선 병렬 레이스: HIGH_QUALITY 8초 유예 → 그 후 ANY
    # anthropic/gemini = 품질 우선, 나머지 = 속도 fallback
    _HQ = {"anthropic", "gemini"} if quality_first else set()
    loop_tasks = {asyncio.ensure_future(_call_one(p)): p for p in candidates}
    pending = set(loop_tasks.keys())
    winner = None
    winner_provider = None
    try:
        async def _race_inner():
            _nonlocal_winner = [None, None]  # [result, provider]
            _pending = set(loop_tasks.keys())
            # 1단계: 8초 대기 — HQ 응답 우선
            while _pending:
                _done, _pending = await asyncio.wait(_pending, return_when=asyncio.FIRST_COMPLETED, timeout=8.0)
                if not _done:  # 8초 타임아웃 — 남은 것 중 any 수락
                    break
                for task in _done:
                    res = task.result()
                    pname = loop_tasks[task]
                    if res is not None:
                        if pname in _HQ:
                            logger.info(f"[LLM/race] HQ 승자: {pname}")
                            for t in _pending: t.cancel()
                            _nonlocal_winner = [res, pname]
                            return _nonlocal_winner
                        else:
                            # 속도 후보 — HQ 8초 유예 대기 중 홀드
                            if _nonlocal_winner[0] is None:
                                _nonlocal_winner = [res, pname]  # 임시 저장
            # 2단계: HQ 없으면 속도 후보 수락 또는 나머지 대기
            if _nonlocal_winner[0] is not None:
                logger.info(f"[LLM/race] 속도 fallback 승자: {_nonlocal_winner[1]}")
                for t in _pending: t.cancel()
                return _nonlocal_winner
            # 아직 남은 태스크 대기 (최대 130s 총 타임아웃)
            while _pending:
                _done2, _pending = await asyncio.wait(_pending, return_when=asyncio.FIRST_COMPLETED)
                for task in _done2:
                    res = task.result()
                    pname = loop_tasks[task]
                    if res is not None:
                        logger.info(f"[LLM/race] 잔여 승자: {pname}")
                        for t in _pending: t.cancel()
                        return [res, pname]
            return [None, None]

        _result = await asyncio.wait_for(_race_inner(), timeout=130.0)
        winner, winner_provider = _result[0], _result[1]
    except asyncio.TimeoutError:
        logger.warning("[LLM/race] 130초 타임아웃 — 모든 프로바이더 실패")
        for t in pending: t.cancel()
    except Exception as e:
        logger.warning(f"[LLM/race] 예외: {e}")

    return winner


def _save_project_file(project_dir: Path, filename: str, data) -> None:
    """프로젝트 디렉토리에 JSON 파일 저장"""
    project_dir.mkdir(parents=True, exist_ok=True)
    (project_dir / filename).write_text(
        _json_auto.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def _load_project_file(project_dir: Path, filename: str) -> Optional[Dict]:
    p = project_dir / filename
    if not p.exists():
        return None
    try:
        return _json_auto.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


# ── 4. 단계별 함수 ──────────────────────────────────────────────────────────

async def auto_analyze_topic(
    topic: str,
    video_type: str,
    tone: str,
    target_duration_sec: int,
    audience: str,
    language: str,
) -> Dict:
    """[AUTO 1/12] 주제 분석 → 시청자·목적·자료조사 필요성 판단"""
    # [v15.71] 동적 섹션 수 계산
    _n_sections = max(4, min(12, round(target_duration_sec / 35)))
    _section_names = [f"섹션{k+1}" for k in range(_n_sections)]
    _sections_example = str(_section_names).replace("'", chr(34))
    prompt = f"""다음 영상 주제를 분석하세요.

주제: {topic}
영상 유형: {video_type}
톤: {tone}
목표 길이: {target_duration_sec}초
언어: {language}
시청자 힌트: {audience}

JSON으로 반환:
{{
  "main_topic": "핵심 주제 한 줄",
  "angle": "접근 각도",
  "audience": "타겟 시청자",
  "tone": "{tone}",
  "video_type": "{video_type}",
  "target_duration": {target_duration_sec},
  "language": "{language}",
  "needs_research": true,
  "key_points": ["포인트1", "포인트2", "포인트3"],
  "risk_level": "low/medium/high",
  "suggested_sections": {_sections_example}
}}"""
    result = await _call_llm_json(prompt, max_tokens=1500, quality_first=True)
    if not result:
        result = {
            "main_topic": topic,
            "angle": "종합 분석",
            "audience": audience,
            "tone": tone,
            "video_type": video_type,
            "target_duration": target_duration_sec,
            "language": language,
            "needs_research": True,
            "key_points": [topic],
            "risk_level": "medium",
            "suggested_sections": ["서론", "문제제기", "현황분석", "심층배경", "본론 핵심", "통계와증거", "미래전망", "결론"],
        }
    logger.info(f"[AUTO] 주제 분석 완료: {result.get('main_topic')}")
    return result


# ============================================================
# [v15.70] 웹서치 Pre-Injection — Google News RSS
# ============================================================
async def _fetch_topic_news(topic: str, max_articles: int = 5, timeout: float = 8.0) -> str:
    """[v15.70] Google News RSS로 최신 뉴스 제목+요약 수집 → LLM 프롬프트 주입용 텍스트 반환"""
    import urllib.parse as _urlparse
    import xml.etree.ElementTree as _ET
    try:
        q = _urlparse.quote(topic)
        urls = [
            f"https://news.google.com/rss/search?q={q}&hl=ko&gl=KR&ceid=KR:ko",
            f"https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en",
        ]
        articles = []
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as c:
            for url in urls:
                if len(articles) >= max_articles:
                    break
                try:
                    r = await c.get(url, headers={"User-Agent": "Mozilla/5.0"})
                    if r.status_code != 200:
                        continue
                    root = _ET.fromstring(r.text)
                    for item in root.findall(".//item"):
                        title = (item.findtext("title") or "").strip()
                        desc  = (item.findtext("description") or "").strip()
                        pub   = (item.findtext("pubDate") or "").strip()[:16]
                        if title:
                            # HTML 태그 제거
                            import re as _re
                            clean = _re.sub(r"<[^>]+>", "", desc)[:120]
                            articles.append(f"[{pub}] {title}: {clean}")
                            if len(articles) >= max_articles:
                                break
                except Exception:
                    pass
        if articles:
            text = "\n".join(f"- {a}" for a in articles[:max_articles])
            logger.info(f"[v15.70 NEWS] {len(articles)}개 기사 수집 완료")
            return text
    except Exception as e:
        logger.warning(f"[v15.70 NEWS] 뉴스 수집 실패: {e}")
    return ""


async def auto_collect_research(topic: str, analysis: Dict) -> Dict:
    """[AUTO 2/12] 자료 조사 → 핵심 팩트 + 출처 요약"""
    key_points = analysis.get("key_points", [topic])
    sections = analysis.get("suggested_sections", [])
    # [v15.70] 실시간 뉴스 수집 → 팩트 보강
    _live_news = await _fetch_topic_news(topic, max_articles=5)
    _news_section = f"\n\n## 실시간 최신 뉴스 (반드시 반영):\n{_live_news}" if _live_news else ""

    prompt = f"""다음 주제에 대해 영상 제작용 핵심 자료를 조사하세요.

주제: {topic}
핵심 포인트: {', '.join(key_points)}
섹션 구성안: {', '.join(sections)}

{_news_section}

실제 알고 있는 사실과 일반적으로 알려진 정보를 바탕으로 JSON 반환:
{{
  "facts": [
    "구체적 사실 1 (출처 있으면 포함)",
    "구체적 사실 2",
    "구체적 사실 3",
    "구체적 사실 4",
    "구체적 사실 5"
  ],
  "statistics": [
    "수치·통계 1",
    "수치·통계 2"
  ],
  "source_summary": "자료 출처 요약",
  "risk_notes": [
    "최신 자료 확인 필요 항목"
  ],
  "key_messages": [
    "핵심 메시지 1",
    "핵심 메시지 2"
  ]
}}"""
    result = await _call_llm_json(prompt, max_tokens=2000)
    if not result:
        result = {
            "facts": [f"{topic}에 관한 핵심 정보"],
            "statistics": [],
            "source_summary": "일반 자료 기반",
            "risk_notes": ["최신 자료 확인 권장"],
            "key_messages": [topic],
        }
    logger.info(f"[AUTO] 자료 조사 완료: {len(result.get('facts', []))}개 팩트")
    return result


async def auto_generate_script(
    topic: str,
    research: Dict,
    tone: str,
    target_duration_sec: int,
    language: str,
    sections: List[str],
) -> Dict:
    """[AUTO 3/12] 영상 원고 자동 작성"""
    # 섹션당 예상 나레이션 길이 계산 (한국어 약 4음절/초)
    words_per_sec = 5.5  # [v15.71] KO TTS 5-6char/sec  # 약간 여유있게
    total_words = int(target_duration_sec * words_per_sec)
    section_words = max(total_words // max(len(sections), 1), 80)
    min_section_chars = max(section_words, 200)  # [v15.71]
    target_total_chars = int(target_duration_sec * words_per_sec)  # [v15.71] total target chars

    facts_text = "\n".join(f"- {f}" for f in research.get("facts", []))
    msgs_text  = "\n".join(f"- {m}" for m in research.get("key_messages", []))

    prompt = f"""당신은 구독자 100만명 유튜브 채널의 수석 스크립터입니다.
시청 유지율 70%+ 달성을 위한 프로 수준 원고를 작성하세요.

주제: {topic}
톤: {tone}
목표 길이: {target_duration_sec}초
언어: {language}

핵심 팩트:
{facts_text}

핵심 메시지:
{msgs_text}

섹션 구성: {', '.join(sections)}

목표 나레이션 길이: 총 {target_total_chars}자 이상 (각 섹션 {min_section_chars}자 이상 필수)\n\n## HOOK (첫 3~5초): 충격적 사실/반직관 질문. "안녕하세요" 금지
## PATTERN INTERRUPT: 20~30초마다 새 질문/충격 포인트
## CTA 3회: 40%/70%/마지막 지점
## 나레이션: 한 문장=15~25자, 숫자/통계 활용

JSON:
{{
  "title": "클릭유발 제목 (파워워드, 30자 이내)",
  "hook": "충격 훅 (20~40자)",
  "sections": [
    {{
      "section_title": "제목",
      "section_type": "hook/problem/agitation/stats/solution/cta/closing",
      "narration": "나레이션 (최소 {min_section_chars}자 이상 상세하게 작성, 예시 문장 5개 이상)",
      "pattern_interrupt": "패턴 인터럽트 (선택)"
    }}
  ],
  "closing": "강력한 CTA 마무리",
  "total_estimated_duration_sec": {target_duration_sec}
}}"""
    result = await _call_llm_json(prompt, max_tokens=6000, temperature=0.6, quality_first=True)  # [v15.71]
    if not result:
        result = {
            "title": topic,
            "hook": f"{topic}에 대해 알아보겠습니다.",
            "sections": [{"section_title": s, "section_type": "main",
                          "narration": f"{s}에 대한 내용입니다."} for s in sections],
            "closing": "이상으로 마치겠습니다.",
            "total_estimated_duration_sec": target_duration_sec,
        }
    logger.info(f"[AUTO] 원고 생성 완료: {len(result.get('sections', []))}개 섹션")
    return result


async def auto_build_scenes(
    script: Dict,
    target_duration_sec: int,
    tone: str,
) -> List[Dict]:
    """[AUTO 4/12] 원고 → 씬 자동 분할 (target_duration 비례)"""
    # [v15.70] target_duration 기반 동적 씬 수 계산
    _target_dur = max(target_duration_sec, 60)
    _avg_scene_sec = 5.5  # 씬당 평균 5.5초
    _min_scenes = max(8, int(_target_dur / 7))
    _max_scenes = max(15, int(_target_dur / 4))
    _rec_scenes = max(10, int(_target_dur / _avg_scene_sec))
    logger.info(f"[v15.70] 씬 수 계산: target={_target_dur}s → {_min_scenes}~{_max_scenes}개 (권장 {_rec_scenes}개)")
    sections_text = _json_auto.dumps(script.get("sections", []), ensure_ascii=False)
    hook = script.get("hook", "")
    closing = script.get("closing", "")

    prompt = f"""다음 영상 원고를 6~12초 단위의 씬으로 분할하세요.

훅(오프닝): {hook}
섹션 원고: {sections_text}
마무리: {closing}
목표 길이: {target_duration_sec}초
톤: {tone}

## 씬 규칙 (프로):
- 총 씬 수: {_min_scenes}~{_max_scenes}개 (권장 {_rec_scenes}개, target_duration={_target_dur}초 기준)
- B-roll 교체: 최대 5초 (시청유지율 핵심)
- visual_keywords: 씬마다 완전히 다른 키워드 (반복 금지!)
- 나레이션 내용과 영상 일치: economy → stock market trading floor
- negative_keywords: cartoon, animation, low quality
- tone_profile: hook/problem/agitation/stats/solution/cta/closing

## 키워드 다양성:
- 구체적: "business meeting" X → "executive board meeting presentation" O
- 추상→시각화: "economy" → "GDP growth chart", "stock market trading"
- preferred_motion: slow_zoom_in/out, pan_left/right, fast_cut, aerial_shot

## [v15.69] 단어·문맥·음절 기반 영상 매핑 규칙 (필수):
- visual_keywords 금지: "wide shot","close up","side angle","aerial","zoom","panning","tilt","cutaway","overhead","angle","shot","zoom"
- visual_keywords 형식: 반드시 "명사+명사" → "semiconductor factory worker", "CPU chip extreme closeup"
    - narration: 반드시 해당 섹션 sections_text의 narration 원문 전체 복사. 절대 제목/placeholder 금지. 최소 100자 이상\n- narration_en: 나레이션을 20~30단어 영어 시각 묘사로 변환 (Kling T2V 프롬프트)
  예: "semiconductor chips manufacturing process, engineers inspecting circuit boards, high-tech facility, cinematic 4K"
- 단어 매핑: 나레이션 핵심 명사→구체적 시각 장면 (반도체→semiconductor chip, 수출규제→trade sanctions document)
- 음절 기반 타이밍: expected_duration = max(len(narration_text.replace(" ","")) / 4.0, 4.0)

JSON:
[
  {{
    "scene_id": "scene_001",
    "narration": "섹션 narration 원문 전체 (sections_text에서 복사, 반드시 최소 80자 이상, 씬 내용을 상세히)",
    "narration_en": "cinematic description 20-30 words for AI video generation",
    "section_type": "hook",
    "visual_intent": "dramatic opening conveying urgency",
    "visual_keywords": ["dramatic skyline sunrise", "city aerial dawn"],
    "backup_keywords": ["urban cityscape morning"],
    "negative_keywords": ["cartoon", "animation", "low quality"],
    "tone_profile": "hook",
    "preferred_motion": "slow_zoom_in",
    "expected_duration": 5.0
  }}
]"""
    result = await _call_llm_json(prompt, max_tokens=8000  # [v15.74], temperature=0.5, quality_first=True)

    if not isinstance(result, list) or not result:
        # fallback: 섹션별로 단순 씬 생성
        result = []
        scene_idx = 1
        all_narrations = [{"text": hook, "type": "opening"}]
        for sec in script.get("sections", []):
            all_narrations.append({"text": sec.get("narration", ""), "type": sec.get("section_type", "main")})
        all_narrations.append({"text": closing, "type": "closing"})

        for item in all_narrations:
            text = item["text"]
            if not text:
                continue
            # 단순 분할 (50자 기준)
            chunks = [text[i:i+50] for i in range(0, len(text), 50)] or [text]
            for chunk in chunks:
                result.append({
                    "scene_id": f"scene_{scene_idx:03d}",
                    "narration": chunk,
                    "section_type": item["type"],
                    "visual_intent": f"visual for {chunk[:30]}",
                    "visual_keywords": [topic_word for topic_word in chunk.split()[:3] if topic_word.isalpha()],
                    "backup_keywords": ["technology", "business"],
                    "negative_keywords": ["cartoon", "low quality"],
                    "tone_profile": item["type"],
                    "preferred_motion": "slow_zoom_in",
                    "expected_duration": max(len(chunk) / 4.0, 6.0),
                })
                scene_idx += 1

    logger.info(f"[AUTO] 씬 분할 완료: {len(result)}개 씬")
    return result



# ==================================================
# [PRO] ElevenLabs TTS (유료 고품질 / Edge TTS 폴백)
# ==================================================
ELEVENLABS_API_KEY = os.getenv("ELEVENLABS_API_KEY", "")
ELEVENLABS_VOICE_ID = os.getenv("ELEVENLABS_VOICE_ID", "21m00Tcm4TlvDq8ikWAM")
ELEVENLABS_MODEL = os.getenv("ELEVENLABS_MODEL", "eleven_multilingual_v2")
ELEVENLABS_ENABLED = bool(ELEVENLABS_API_KEY) and os.getenv("ELEVENLABS_ENABLED", "true").lower() in ("1","true","yes")

async def generate_tts_elevenlabs(text: str, output_path: Path, voice_id: str = None) -> bool:
    if not ELEVENLABS_ENABLED:
        return False
    vid = voice_id or ELEVENLABS_VOICE_ID
    url = f"https://api.elevenlabs.io/v1/text-to-speech/{vid}"
    headers = {"xi-api-key": ELEVENLABS_API_KEY, "Content-Type": "application/json", "Accept": "audio/mpeg"}
    payload = {
        "text": text, "model_id": ELEVENLABS_MODEL,
        "voice_settings": {"stability": 0.55, "similarity_boost": 0.80, "style": 0.35, "use_speaker_boost": True},
        "output_format": "mp3_44100_128",
    }
    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(url, json=payload, headers=headers)
            if resp.status_code == 200:
                output_path.write_bytes(resp.content)
                logger.info(f"[ElevenLabs] TTS 완료: {output_path.name}")
                return True
            logger.warning(f"[ElevenLabs] 오류 {resp.status_code}: {resp.text[:200]}")
    except Exception as e:
        logger.warning(f"[ElevenLabs] 예외: {e}")
    return False

# ==================================================
# [PRO] BGM 자동 다운로드 (Freesound CC0)
# ==================================================
FREESOUND_API_KEY = os.getenv("FREESOUND_API_KEY", "")
_BGM_TONE_QUERIES = {
    "news": "news background music corporate", "tech": "technology electronic ambient",
    "economy": "corporate business background music calm", "uplifting": "uplifting inspiring positive",
    "serious": "dramatic tension documentary", "default": "ambient calm instrumental",
}

async def auto_download_bgm(tone: str, output_path: Path, duration_sec: int = 300) -> bool:
    """BGM 자동 다운로드 — Freesound > Jamendo > generative ambient 순 폴백"""
    # 캐시 재사용
    if output_path.exists() and output_path.stat().st_size > 100_000:
        logger.info(f"[BGM] 캐시 사용: {output_path.name}")
        return True
    existing = list(BGM_DIR.glob(f"auto_bgm_{tone}*.mp3"))
    if existing and existing[0].stat().st_size > 100_000:
        import shutil as _sh_bgm; _sh_bgm.copy2(existing[0], output_path)
        logger.info(f"[BGM] 기존 파일 재사용: {existing[0].name}")
        return True

    query = _BGM_TONE_QUERIES.get(tone, _BGM_TONE_QUERIES["default"])

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        # ── 1차: Freesound ───────────────────────────────────────
        if FREESOUND_API_KEY:
            try:
                resp = await client.get("https://freesound.org/apiv2/search/text/", params={
                    "query": query,
                    "filter": f"duration:[{duration_sec//2} TO *] license:\"Creative Commons 0\"",
                    "fields": "id,name,previews", "page_size": 5, "token": FREESOUND_API_KEY,
                })
                if resp.status_code == 200:
                    results = resp.json().get("results", [])
                    if results:
                        preview = (results[0].get("previews", {}).get("preview-hq-mp3")
                                   or results[0].get("previews", {}).get("preview-lq-mp3"))
                        if preview:
                            dl = await client.get(preview, timeout=60.0)
                            if dl.status_code == 200 and len(dl.content) > 50_000:
                                output_path.write_bytes(dl.content)
                                logger.info(f"[BGM] Freesound 완료: {results[0]['name']}")
                                return True
            except Exception as e:
                logger.warning(f"[BGM] Freesound 실패: {e}")

        # ── 2차: Jamendo (무료 공개 API, 키 불필요) ─────────────
        _jamendo_tags = {
            "news": "corporate", "tech": "electronic", "economy": "ambient+corporate",
            "uplifting": "happy+upbeat", "serious": "dramatic+cinematic", "default": "ambient",
        }.get(tone, "ambient")
        try:
            resp2 = await client.get(
                "https://api.jamendo.com/v3.0/tracks/",
                params={
                    "client_id": "a7e42a2c",
                    "format": "json",
                    "limit": 5,
                    "tags": _jamendo_tags,
                    "audioformat": "mp32",
                    "duration_between": f"120,{max(300, duration_sec)}",
                    "license_cc": "1",
                },
                timeout=20.0,
            )
            if resp2.status_code == 200:
                tracks = resp2.json().get("results", [])
                if tracks:
                    audio_url = tracks[0].get("audio")
                    if audio_url:
                        dl2 = await client.get(audio_url, timeout=90.0)
                        if dl2.status_code == 200 and len(dl2.content) > 50_000:
                            output_path.write_bytes(dl2.content)
                            logger.info(f"[BGM] Jamendo 완료: {tracks[0].get('name','?')}")
                            return True
        except Exception as e2:
            logger.warning(f"[BGM] Jamendo 실패: {e2}")

    # ── 3차: ffmpeg generative ambient (항상 성공) ─────────────
    try:
        _dur = max(180, duration_sec)
        _tone_filter = {
            "news":      "lowpass=f=1200,highpass=f=100",
            "tech":      "lowpass=f=2000,highpass=f=200,aecho=0.8:0.9:500:0.3",
            "economy":   "lowpass=f=800,highpass=f=80",
            "uplifting": "lowpass=f=1500,highpass=f=150,volume=1.2",
            "serious":   "lowpass=f=600,highpass=f=60,volume=0.9",
            "default":   "lowpass=f=1000,highpass=f=100",
        }.get(tone, "lowpass=f=1000,highpass=f=100")
        cmd_gen = [
            "ffmpeg", "-y",
            "-f", "lavfi",
            "-i", f"anoisesrc=color=pink:duration={_dur}:amplitude=0.06",
            "-af", f"{_tone_filter},volume=0.4",
            "-c:a", "libmp3lame", "-q:a", "6",
            str(output_path),
        ]
        if run_ffmpeg_command(cmd_gen, timeout=60.0):
            logger.info(f"[BGM] generative ambient 생성: {output_path.name} ({_dur}s)")
            return True
    except Exception as e3:
        logger.warning(f"[BGM] generative 실패: {e3}")

    logger.warning("[BGM] 모든 소스 실패 — BGM 없이 진행")
    return False

async def auto_plan_voice(scenes_data: List[Dict], global_tone: str) -> List[Dict]:
    """[AUTO 5/12] 씬별 나레이션 톤/속도/피치 설정"""
    voice_plan = []
    global_voice = TONE_VOICE_MAP.get(global_tone, {"rate": "-5%", "pitch": "+0Hz"})

    for scene in scenes_data:
        tone_key = scene.get("tone_profile") or scene.get("section_type") or "main"
        scene_voice = SCENE_TONE_MAP.get(tone_key, SCENE_TONE_MAP["main"])
        voice_plan.append({
            "scene_id": scene.get("scene_id"),
            "voice": os.getenv("EDGE_VOICE_PRIMARY", "ko-KR-SunHiNeural"),
            "rate": scene_voice.get("rate", global_voice.get("rate", "-5%")),
            "pitch": scene_voice.get("pitch", global_voice.get("pitch", "+0Hz")),
            "pause_sentence_ms": scene_voice.get("pause_sentence_ms", PAUSE_SENTENCE_MS),
            "pause_comma_ms": PAUSE_COMMA_MS,
            "emotion": tone_key,
        })
    logger.info(f"[AUTO] 나레이션 톤 설정 완료: {len(voice_plan)}개 씬")
    return voice_plan


def auto_merge_voice_into_scenes(scenes_data: List[Dict], voice_plan: List[Dict]) -> List[Scene]:
    """씬 데이터 + 음성 계획 → Scene 모델 리스트"""
    voice_map = {v["scene_id"]: v for v in voice_plan}
    merged = []
    for s in scenes_data:
        sid = s.get("scene_id", f"scene_{len(merged)+1:03d}")
        vp = voice_map.get(sid, {})
        narration = s.get("narration", "")
        char_count = len(narration.replace(" ", ""))
        est_dur = max(char_count / 4.0, s.get("expected_duration", 7.0))

        _vkws = s.get("visual_keywords", []) or []
        _bkws = s.get("backup_keywords", []) or []
        _primary_kw = _vkws[0] if _vkws else " ".join(s.get("visual_intent","business economy").split()[:3])
        _alt_kws = _vkws[1:] + _bkws  # [v15.69] alt_keywords 풀 채우기
        _narration_en = s.get("narration_en", "") or ""
        if not _narration_en and s.get("visual_intent"):
            _narration_en = s.get("visual_intent","") + ", " + ", ".join(_vkws[:2]) + ", cinematic footage, professional"
        scene = Scene(
            scene_id=sid,
            keyword=_primary_kw,
            duration_seconds=round(est_dur, 1),
            description=s.get("visual_intent", ""),
            narration=narration,
            visual_intent=s.get("visual_intent", ""),
            visual_keywords=_vkws,
            alt_keywords=_alt_kws,          # [v15.69] 이제 실제로 채워짐
            narration_en=_narration_en,     # [v15.69] Kling T2V 프롬프트
            tone_profile=s.get("tone_profile", "main"),
            visual_pacing=s.get("preferred_motion", "slow_zoom_in"),
        )
        merged.append(scene)
    logger.info(f"[AUTO] Scene 모델 변환 완료: {len(merged)}개")
    return merged


# ── 5. 품질 검사 ────────────────────────────────────────────────────────────

async def auto_run_quality_check(
    job_id: str,
    output_files: Dict[str, str],
    scenes: List,
    ntl_timeline: Dict,
) -> Dict:
    """[AUTO 10/12] 품질 검사 → quality_score 100점 기준"""
    import glob as _glob
    score = 0
    warnings = []
    errors = []

    # ── timeline_report.json 우선 로드 (process_video_creation 렌더 후 최신 데이터) ──
    tr_path = JOBS_DIR / job_id / "timeline_report.json"
    if tr_path.exists():
        try:
            import json as _jq
            _tr = _jq.loads(tr_path.read_text(encoding="utf-8"))
            scene_timings = _tr.get("scene_timings") or ntl_timeline.get("scene_timings", [])
        except Exception:
            scene_timings = ntl_timeline.get("scene_timings", [])
    else:
        scene_timings = ntl_timeline.get("scene_timings", [])

    # 1. 나레이션 정상 생성 (20점)
    mp3_path = TMP_DIR / f"{job_id}.mp3"
    if mp3_path.exists() and mp3_path.stat().st_size > 1024:
        score += 20
    else:
        warnings.append("TTS 오디오 파일 없거나 비정상")

    # 2. 영상-나레이션 매칭 점수 (25점)
    if scene_timings:
        matched = sum(1 for st in scene_timings
                      if st.get("narration_end", 0) > st.get("narration_start", 0))
        match_ratio = matched / max(len(scene_timings), 1)
        match_pts = int(match_ratio * 25)
        score += match_pts
        if match_ratio < 0.8:
            warnings.append(f"나레이션-영상 매칭 {matched}/{len(scene_timings)}개 씬")
    else:
        score += 12  # partial

    # 3. 자막 생성 (15점) — 경로 다중 검사
    longform_path = output_files.get("longform", "")
    # 검색 경로: /data/tmp/{id}.ass, /data/tmp/{id}.srt, /data/tmp/{id}/*.ass|srt
    _sub_patterns = [
        str(TMP_DIR / f"{job_id}.ass"),
        str(TMP_DIR / f"{job_id}.srt"),
        str(TMP_DIR / job_id / "*.ass"),
        str(TMP_DIR / job_id / "*.srt"),
        str(TMP_DIR / job_id / f"{job_id}*.ass"),
        str(TMP_DIR / job_id / f"{job_id}*.srt"),
    ]
    _sub_found = any(_glob.glob(p) for p in _sub_patterns)
    if _sub_found:
        score += 15
    else:
        warnings.append("자막 파일 없음")
        score += 5

    # 4. 오디오/BGM 밸런스 (10점)
    if longform_path and Path(longform_path).exists():
        out_dur = get_video_duration(Path(longform_path))
        if out_dur and out_dur > 10:
            score += 10
        else:
            warnings.append(f"영상 길이 비정상: {out_dur}초")
    else:
        errors.append("출력 영상 파일 없음")

    # 5. 영상 품질/해상도 (10점)
    if longform_path and Path(longform_path).exists():
        size_mb = Path(longform_path).stat().st_size / 1024 / 1024
        if size_mb > 5:
            score += 10
        elif size_mb > 1:
            score += 6
            warnings.append(f"출력 파일 크기 작음: {size_mb:.1f}MB")
        else:
            errors.append(f"출력 파일 너무 작음: {size_mb:.1f}MB")

    # 6. 중복 영상 없음 (5점)
    scene_assets = [s.asset_url for s in scenes if getattr(s, "asset_url", None)]
    unique_ratio = len(set(scene_assets)) / max(len(scene_assets), 1)
    if unique_ratio >= 0.7:
        score += 5
    else:
        warnings.append(f"자산 중복 비율 높음: {(1-unique_ratio)*100:.0f}%")

    # 7. 렌더링 오류 없음 (10점)
    if not errors:
        score += 10

    # 8. 썸네일/메타데이터 (5점)
    if output_files.get("thumbnail") and Path(output_files["thumbnail"]).exists():
        score += 5
    else:
        warnings.append("썸네일 없음")

    passed = score >= 75 and not errors
    result = {
        "quality_score": score,
        "passed": passed,
        "warnings": warnings,
        "errors": errors,
        "breakdown": {
            "narration": 20 if mp3_path.exists() and mp3_path.stat().st_size > 1024 else 0,
            "visual_match": int((sum(1 for st in scene_timings if st.get("narration_end",0) > st.get("narration_start",0)) / max(len(scene_timings),1)) * 25) if scene_timings else 12,
            "subtitle": 15 if _sub_found else 5,
            "audio_bgm": 10 if (longform_path and Path(longform_path).exists()) else 0,
            "video_quality": 10 if (longform_path and Path(longform_path).exists() and Path(longform_path).stat().st_size / 1024 / 1024 > 5) else 0,
            "asset_unique": 5 if unique_ratio >= 0.7 else 0,
            "no_errors": 10 if not errors else 0,
            "thumbnail": 5 if (output_files.get("thumbnail") and Path(output_files["thumbnail"]).exists()) else 0,
        },
        "upload_decision": (
            "auto_upload" if score >= 90 else
            "auto_upload_review" if score >= 85 else
            "upload_hold" if score >= 75 else
            "auto_regenerate" if score >= 60 else
            "failed"
        ),
    }
    logger.info(f"[AUTO] 품질 검사: {score}점, {result['upload_decision']}")
    return result
async def auto_generate_youtube_metadata(
    topic: str,
    script: Dict,
    language: str,
    duration_sec: int,
    privacy_status: str = "private",
) -> Dict:
    """[AUTO 11/12] YouTube 제목·설명·태그·썸네일 텍스트 자동 생성"""
    total_min = duration_sec // 60
    total_sec_remain = duration_sec % 60
    prompt = f"""당신은 유튜브 SEO 전문가. 조회수 극대화 메타데이터를 생성하세요.

주제: {topic} | 제목초안: {script.get('title', topic)}
언어: {language} | 길이: {total_min}분 {total_sec_remain}초

제목규칙: 파워워드("완전정복","충격","절대모르는","비밀") + 숫자 포함 + 30~40자
설명규칙: 첫줄요약 + 타임스탬프 + 해시태그5개 + CTA
태그: 30개 (주제+관련+롱테일)

JSON:
{{
  "youtube": {{
    "title": "파워워드 포함 30~40자 제목",
    "description": "첫줄요약\n\n⏱️ 타임스탬프\n00:00 인트로\n01:00 섹션1\n\n#태그1 #태그2 #태그3\n\n👍 좋아요와 구독은 큰 힘이 됩니다!",
    "tags": ["태그1","태그2","태그30"],
    "category_id": "28",
    "privacy_status": "{privacy_status}",
    "made_for_kids": false
  }},
  "thumbnail": {{
    "headline": "임팩트 10자",
    "subline": "보조 15자"
  }}
}}"""
    result = await _call_llm_json(prompt, max_tokens=1000)
    if not result:
        title_short = topic[:55]
        result = {
            "youtube": {
                "title": title_short,
                "description": f"{topic} 관련 영상입니다.",
                "tags": topic.split()[:5],
                "category_id": "28",
                "privacy_status": privacy_status,
                "made_for_kids": False,
            },
            "thumbnail": {
                "headline": topic[:15],
                "subline": "자동 생성",
            },
        }
    logger.info(f"[AUTO] 메타데이터 생성: {result.get('youtube', {}).get('title', '')}")
    return result


# ── 6. 메인 오케스트레이터 ────────────────────────────────────────────────────

_AUTO_JOB_STORE: Dict[str, Dict] = {}
_AUTO_TASKS: Dict[str, object] = {}  # task 참조 보관 (GC 방지)  # job_id → 상태 저장

def _auto_set_status(job_id: str, step: str, progress: int, message: str = "",
                      extra: Optional[Dict] = None) -> None:
    s = _AUTO_JOB_STORE.setdefault(job_id, {})
    s.update({"status": step, "progress": progress, "current_message": message,
               "updated_at": datetime.now().isoformat()})
    if extra:
        s.update(extra)
    logger.info(f"[AUTO:{job_id[:8]}] {step} ({progress}%) {message}")


async def run_auto_topic_pipeline(job_id: str, request: "AutoTopicRequest") -> None:
    """완전 자동 주제→영상→업로드 파이프라인"""
    logger.info('[AUTO] pipeline ENTER: ' + job_id)
    project_id = request.project_id or job_id
    project_dir = JOBS_DIR / project_id
    project_dir.mkdir(parents=True, exist_ok=True)

    _auto_set_status(job_id, "queued", 0, "파이프라인 초기화")
    _save_project_file(project_dir, "input_topic.json", request.model_dump())

    try:
        # ── 1. 주제 분석 ──────────────────────────────────
        _auto_set_status(job_id, "topic_analyzing", 5, "주제 분석 중")
        analysis = await auto_analyze_topic(
            request.topic, request.video_type, request.tone,
            request.target_duration_sec, request.audience, request.language
        )
        _save_project_file(project_dir, "analysis.json", analysis)

        # ── 2. 자료 조사 ──────────────────────────────────
        _auto_set_status(job_id, "researching", 10, "자료 조사 중")
        research = await auto_collect_research(request.topic, analysis)
        _save_project_file(project_dir, "research_summary.json", research)

        # ── 3. 원고 생성 ──────────────────────────────────
        _auto_set_status(job_id, "script_generating", 18, "원고 생성 중")
        sections = analysis.get("suggested_sections", ["서론", "본론 1", "본론 2", "결론"])
        script = await auto_generate_script(
            request.topic, research, request.tone,
            request.target_duration_sec, request.language, sections
        )
        _save_project_file(project_dir, "script.json", script)

        # ── 4. 씬 분할 ──────────────────────────────────
        _auto_set_status(job_id, "scene_building", 25, "씬 분할 중")
        scenes_data = await auto_build_scenes(script, request.target_duration_sec, request.tone)
        _save_project_file(project_dir, "scenes_raw.json", scenes_data)
        # [v15.72] 나레이션 품질 검증 — 짧으면 스크립트 섹션 직접 주입
        _total_narr_chars = sum(len(s.get("narration", "")) for s in scenes_data)
        _min_target_chars = int(request.target_duration_sec * 5.0  # [v15.74] TTS 5.5자/초 기준)
        logger.info(f"[v15.72] 나레이션 검증: {_total_narr_chars}자 (목표 {_min_target_chars}자 이상)")
        if _total_narr_chars < _min_target_chars:
            logger.warning(f"[v15.72] 나레이션 부족 → 섹션 직접 주입")
            _hook_txt = script.get("hook", "")
            _closing_txt = script.get("closing", "")
            _sec_narrs = [s.get("narration", "") for s in script.get("sections", []) if s.get("narration", "")]
            _narr_pool = ([_hook_txt] if _hook_txt else []) + _sec_narrs + ([_closing_txt] if _closing_txt else [])
            # 씬별로 스크립트 섹션 순서대로 매핑 (전체 교체)
            _pool_len = len(_narr_pool)
            for _si, _scene in enumerate(scenes_data):
                _pool_idx = min(_si, _pool_len - 1)
                _pool_narr = _narr_pool[_pool_idx]
                # 풀 나레이션이 씬 나레이션보다 길면 교체
                if len(_pool_narr) > len(_scene.get("narration", "")):
                    _scene["narration"] = _pool_narr
            _new_total = sum(len(s.get("narration", "")) for s in scenes_data)
            logger.info(f"[v15.72] 주입 완료: {_total_narr_chars}자 → {_new_total}자")

        # ── 5. 나레이션 톤 설정 ──────────────────────────
        _auto_set_status(job_id, "voice_planning", 30, "나레이션 톤 설정 중")
        voice_plan = await auto_plan_voice(scenes_data, request.tone)
        _save_project_file(project_dir, "voice_plan.json", voice_plan)

        # ── 6. Scene 모델로 변환 ──────────────────────────
        scenes = auto_merge_voice_into_scenes(scenes_data, voice_plan)

        # scenes.json 저장 (기존 파이프라인 호환)
        scenes_json = [s.model_dump() for s in scenes]
        _save_project_file(project_dir, "scenes.json", scenes_json)
        # 기존 jobs/{job_id}/scenes.json 도 저장
        job_dir = JOBS_DIR / job_id
        job_dir.mkdir(parents=True, exist_ok=True)
        (job_dir / "scenes.json").write_text(
            _json_auto.dumps(scenes_json, ensure_ascii=False, indent=2), encoding="utf-8"
        )

        # ── 6b. BGM 자동 다운로드 ─────────────────────────────
        _auto_set_status(job_id, "asset_searching", 36, "BGM 자동 다운로드 중")
        bgm_tone = analysis.get("tone", request.tone or "") or "economy"
        _tone_key = {"news":"news","informative":"news","authoritative":"serious","tech":"tech","educational":"economy","uplifting":"uplifting"}.get(bgm_tone.lower(), "economy")
        _bgm_path = BGM_DIR / f"auto_bgm_{_tone_key}.mp3"
        try:
            _bgm_ok = await auto_download_bgm(_tone_key, _bgm_path, duration_sec=int(request.target_duration_sec or 180))
            if _bgm_ok:
                logger.info(f"[AUTO] BGM 준비: {_bgm_path.name}")
        except Exception as _bgm_err:
            logger.warning(f"[AUTO] BGM 실패 (무시): {_bgm_err}")

        # ── 7. 영상 자산 검색 ──────────────────────────────
        _auto_set_status(job_id, "asset_searching", 38, "영상 자산 검색 중")
        try:
            scenes = await search_and_download_assets(job_id, scenes)
        except Exception as e:
            logger.warning(f"[AUTO] 자산 검색 실패 (fallback 계속): {e}")

        # ── 8. 자산 매칭 점수 계산 ───────────────────────
        _auto_set_status(job_id, "asset_matching", 45, "영상-나레이션 매칭 중")
        used_assets: set = set()
        visual_matching = []
        for scene in scenes:
            if scene.asset_url:
                meta = {"id": scene.asset_url, "duration": scene.duration_seconds,
                        "width": 1920, "height": 1080, "motion": "medium",
                        "tags": " ".join(scene.visual_keywords or []),
                        "title": scene.keyword}
                score_v = visual_match_score(meta, scene, used_assets)
                visual_matching.append({"scene_id": scene.scene_id, "score": score_v,
                                         "asset": scene.asset_url})
                if score_v < 0.70 and (scene.visual_keywords or scene.keyword):
                    # backup keyword로 재검색 시도
                    backup_kw = scenes_data[scenes.index(scene)].get("backup_keywords", []) if scene in scenes else []
                    if backup_kw:
                        logger.info(f"[AUTO] 씬 '{scene.scene_id}' 낮은 매칭({score_v:.2f}) → backup 재검색")
                        scene.keyword = backup_kw[0]
                        try:
                            rescanned = await search_and_download_assets(job_id, [scene])
                            if rescanned and rescanned[0].asset_url:
                                scene.asset_url = rescanned[0].asset_url
                        except Exception:
                            pass
                if scene.asset_url:
                    used_assets.add(scene.asset_url)
        _save_project_file(project_dir, "visual_matching.json", visual_matching)

        # ── 9. 나레이션 타임라인 빌드 ──────────────────────
        _auto_set_status(job_id, "timeline_building", 52, "타임라인 구성 중")
        # TTS 생성 (ensure_tts_assets)
        _auto_set_status(job_id, "tts_generating", 52, "TTS 나레이션 생성 중")
        # 전체 나레이션 텍스트를 각 씬 narration 필드에서 추출
        for scene in scenes:
            if not scene.narration:
                matched_raw = next((s for s in scenes_data if s.get("scene_id") == scene.scene_id), {})
                scene.narration = matched_raw.get("narration", scene.description or scene.keyword)

        # SSML 전처리: 씬 narration으로 TTS 요청 생성을 위한 full script 조합
        # (기존 ensure_tts_assets 는 scenes.json의 narration 필드를 합쳐서 TTS 생성)
        class _FakeRequest:
            audio_url = None
            subtitle_text = None
            add_subtitles = True
            add_bgm = True
            bgm_volume = 0.3

        # ElevenLabs TTS 시도 → 실패 시 Edge TTS 폴백
        _el_text = " ".join(s.narration or "" for s in scenes if s.narration)
        _el_mp3 = TMP_DIR / f"{job_id}.mp3"
        _el_ok = False
        if ELEVENLABS_ENABLED and _el_text:
            _el_ok = await generate_tts_elevenlabs(_el_text, _el_mp3)
            logger.info(f"[AUTO] ElevenLabs={'성공' if _el_ok else '실패→EdgeTTS폴백'}")
        tts_result = await ensure_tts_assets(job_id, scenes, _FakeRequest())
        tts_ok = tts_result.get("ok", False)
        if not tts_ok:
            logger.warning(f"[AUTO] TTS 실패: {tts_result.get('error_code')} — 계속 진행")

        _auto_set_status(job_id, "timeline_building", 58, "나레이션 타임라인 빌드 중")
        ts_path = TMP_DIR / f"{job_id}_timestamps.json"
        ntl_timeline = build_narration_timeline(job_id, scenes, ts_path)
        save_timeline_report(job_id, ntl_timeline, scenes)
        _save_project_file(project_dir, "narration_timeline.json", ntl_timeline)

        # ── 10. 렌더링 ──────────────────────────────────
        _auto_set_status(job_id, "rendering", 62, "영상 렌더링 중")
        render_request = VideoCreateRequest(
            job_id=job_id,
            mode=VideoMode.LONGFORM if request.video_type != "shorts" else VideoMode.SHORTS,
            resolution="1920x1080",
            fps=30,
            add_subtitles=True,
            add_bgm=True,
            bgm_volume=0.3,
            generate_thumbnail=True,
            generate_shorts=(request.video_type in ("shorts", "both")),
            title=script.get("title", request.topic),
            audio_url=str(TMP_DIR / f"{job_id}.mp3") if (TMP_DIR / f"{job_id}.mp3").exists() else None,
            scenes=scenes_json,
        )
        render_request_dict = render_request.model_dump()
        _save_project_file(project_dir, "render_request.json", render_request_dict)

        # 기존 process_video_creation 호출
        _auto_set_status(job_id, "rendering", 65, "영상 합성 중")
        await process_video_creation(job_id, render_request)

        # 출력 파일 수집
        output_files: Dict[str, str] = {}
        lf_path = LONGFORM_DIR / f"{job_id}.mp4"
        if lf_path.exists():
            output_files["longform"] = str(lf_path)
        th_path = THUMBNAILS_DIR / f"{job_id}_thumb.jpg"
        if th_path.exists():
            output_files["thumbnail"] = str(th_path)

        # ── 11. 품질 검사 ──────────────────────────────
        _auto_set_status(job_id, "quality_checking", 85, "품질 검사 중")
        quality = await auto_run_quality_check(job_id, output_files, scenes, ntl_timeline)
        _save_project_file(project_dir, "quality_report.json", quality)

        # ── 12. 메타데이터 생성 ──────────────────────────
        _auto_set_status(job_id, "thumbnail_generating", 88, "메타데이터 생성 중")
        actual_dur = int(get_video_duration(Path(output_files.get("longform", ""))) or request.target_duration_sec)
        yt_meta = await auto_generate_youtube_metadata(
            request.topic, script, request.language, actual_dur, request.upload_privacy
        )
        _save_project_file(project_dir, "upload_metadata.json", yt_meta)

        # ── 12b. 프로 썸네일 재생성 (YouTube 타이틀 적용) ──
        yt_title = yt_meta.get("youtube", {}).get("title", request.topic) if isinstance(yt_meta, dict) else request.topic
        pro_thumb_path = THUMBNAILS_DIR / f"{job_id}_thumb.jpg"
        lf_path_for_thumb = Path(output_files.get("longform", ""))
        if lf_path_for_thumb.exists():
            _auto_set_status(job_id, "thumbnail_generating", 90, "프로 썸네일 생성 중")
            pro_ok = generate_pro_thumbnail(
                video_path=lf_path_for_thumb,
                output_path=pro_thumb_path,
                title=yt_title,
                subtitle="",
            )
            if pro_ok and pro_thumb_path.exists():
                output_files["thumbnail"] = str(pro_thumb_path)
                logger.info(f"[AUTO] 프로 썸네일 적용: {pro_thumb_path}")
            else:
                logger.warning("[AUTO] 프로 썸네일 실패 — 기존 썸네일 유지")

        # ── 13. YouTube 업로드 (품질 통과 시) ─────────────
        youtube_url = None
        upload_status = "upload_skipped"

        if request.auto_upload and quality["quality_score"] >= request.quality_threshold:
            _auto_set_status(job_id, "uploading_private", 92, "YouTube private 업로드 중")
            try:
                upload_payload = {
                    "job_id": job_id,
                    "video_path": output_files.get("longform", ""),
                    "thumbnail_path": output_files.get("thumbnail", ""),
                    "title": yt_meta["youtube"]["title"],
                    "description": yt_meta["youtube"]["description"],
                    "tags": yt_meta["youtube"]["tags"],
                    "privacy_status": request.upload_privacy,
                    "category_id": yt_meta["youtube"].get("category_id", "28"),
                }
                async with httpx.AsyncClient(timeout=120.0) as client:
                    up_resp = await client.post(
                        "http://lf2_uploader:8003/api/upload/upload/youtube",
                        json=upload_payload,
                        headers={"X-LF-API-Key": os.getenv("LF_API_KEY", "longform-2026-secret")},
                    )
                    if up_resp.status_code == 200:
                        up_data = up_resp.json()
                        youtube_url = up_data.get("youtube_url") or up_data.get("url")
                        upload_status = "upload_completed"
                        logger.info(f"[AUTO] YouTube 업로드 완료: {youtube_url}")
                    else:
                        upload_status = "upload_failed"
                        logger.warning(f"[AUTO] 업로드 응답 {up_resp.status_code}: {up_resp.text[:200]}")
            except Exception as ue:
                upload_status = "upload_failed"
                logger.warning(f"[AUTO] YouTube 업로드 실패: {ue}")
        elif quality["quality_score"] < request.quality_threshold:
            upload_status = "upload_hold_quality"
            logger.info(f"[AUTO] 품질 점수 {quality['quality_score']} < {request.quality_threshold} — 업로드 보류")

        # ── 완료 ────────────────────────────────────────
        final_status = "completed" if not quality["errors"] else "needs_review"
        _auto_set_status(job_id, final_status, 100, "완료",
            extra={
                "quality_score": quality["quality_score"],
                "quality_passed": quality["passed"],
                "warnings": quality["warnings"],
                "errors": quality["errors"],
                "output_files": output_files,
                "youtube_url": youtube_url,
                "upload_status": upload_status,
                "project_id": project_id,
            }
        )

        # 로그 저장
        log_entry = {
            "completed_at": datetime.now().isoformat(),
            "quality_score": quality["quality_score"],
            "upload_status": upload_status,
            "youtube_url": youtube_url,
        }
        _save_project_file(project_dir, "logs.jsonl", log_entry)
        logger.info(f"[AUTO] 파이프라인 완료: job={job_id} quality={quality['quality_score']} upload={upload_status}")

    except Exception as e:
        logger.exception(f"[AUTO] 파이프라인 실패: {e}")
        step = _AUTO_JOB_STORE.get(job_id, {}).get("status", "unknown")
        _auto_set_status(job_id, "failed", _AUTO_JOB_STORE.get(job_id, {}).get("progress", 0),
            f"실패: {e}",
            extra={"error": str(e), "failed_step": step, "retryable": True}
        )
        _save_project_file(project_dir, "error.json",
                           {"error": str(e), "step": step, "timestamp": datetime.now().isoformat()})


# ── 7. FastAPI 엔드포인트 ────────────────────────────────────────────────────

@app.post("/api/auto/topic-job", tags=["Auto"])
async def create_auto_topic_job(
    request: AutoTopicRequest,
    background_tasks: BackgroundTasks,
    _: str = Depends(verify_api_key),
):
    """
    [v15.66.0] 주제 기반 완전 자동 영상 생성 + YouTube private 업로드.
    주제·톤·길이만 입력하면 원고→씬→TTS→렌더링→업로드까지 자동 처리.
    """
    import uuid
    job_id = f"auto_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    project_id = request.project_id or job_id

    _AUTO_JOB_STORE[job_id] = {
        "job_id": job_id,
        "project_id": project_id,
        "status": "queued",
        "progress": 0,
        "topic": request.topic,
        "mode": request.mode,
        "current_message": "대기 중",
        "quality_score": None,
        "output_files": {},
        "youtube_url": None,
        "error": None,
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat(),
    }

    # asyncio.create_task (Python 3.10+ running loop 직접 사용)
    import asyncio as _aio
    try:
        _t = _aio.create_task(run_auto_topic_pipeline(job_id, request))
        _AUTO_TASKS[job_id] = _t  # GC 방지
        def _log_done(t, jid=job_id):
            if t.cancelled():
                logger.error('[AUTO] TASK CANCELLED: ' + jid)
            elif t.exception():
                logger.error('[AUTO] TASK EXCEPTION: ' + jid + ' => ' + str(t.exception()))
            else:
                logger.info('[AUTO] TASK DONE OK: ' + jid)
        _t.add_done_callback(_log_done)
        logger.info('[AUTO] create_task OK: ' + job_id)
    except RuntimeError as _ce:
        logger.warning('[AUTO] create_task fallback: ' + str(_ce))
        background_tasks.add_task(run_auto_topic_pipeline, job_id, request)

    return AutoTopicResponse(
        job_id=job_id,
        project_id=project_id,
        status="queued",
        mode=request.mode,
        status_url=f"/api/auto/jobs/{job_id}/status",
        message=f"자동 생성 파이프라인 시작: {request.topic[:50]}",
    )


@app.get("/api/auto/jobs/{job_id}/status", tags=["Auto"])
async def get_auto_job_status(
    job_id: str,
    _: str = Depends(verify_api_key),
):
    """[v15.66.0] 자동 생성 작업 상태 조회"""
    job = _AUTO_JOB_STORE.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"auto job '{job_id}' not found")

    step = job.get("status", "unknown")
    step_label = AUTO_STEP_LABELS.get(step, step)

    return {
        "job_id": job_id,
        "project_id": job.get("project_id", job_id),
        "status": step,
        "status_label": step_label,
        "progress": job.get("progress", 0),
        "current_message": job.get("current_message", ""),
        "topic": job.get("topic", ""),
        "mode": job.get("mode", "auto"),
        "quality_score": job.get("quality_score"),
        "quality_passed": job.get("quality_passed"),
        "warnings": job.get("warnings", []),
        "errors": job.get("errors", []),
        "output_files": job.get("output_files", {}),
        "youtube_url": job.get("youtube_url"),
        "upload_status": job.get("upload_status"),
        "created_at": job.get("created_at"),
        "updated_at": job.get("updated_at"),
        "error": job.get("error"),
    }


@app.get("/api/auto/jobs", tags=["Auto"])
async def list_auto_jobs(_: str = Depends(verify_api_key)):
    """[v15.66.0] 자동 생성 작업 목록"""
    jobs = []
    for jid, job in sorted(_AUTO_JOB_STORE.items(),
                            key=lambda x: x[1].get("created_at", ""), reverse=True):
        jobs.append({
            "job_id": jid,
            "status": job.get("status"),
            "progress": job.get("progress"),
            "topic": job.get("topic", ""),
            "quality_score": job.get("quality_score"),
            "youtube_url": job.get("youtube_url"),
            "created_at": job.get("created_at"),
        })
    return {"jobs": jobs[:50], "total": len(jobs)}