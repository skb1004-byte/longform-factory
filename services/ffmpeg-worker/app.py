# [BC] MARKER v1
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
LongForm Factory - FFmpeg Worker v16.21.0 (ÀÚ»ê´Ù¾çÈ­+½ÌÅ©)
·ÕÆû/¼ôÆû ÀÚµ¿È­ ¿µ»ó Á¦ÀÛ ¼­ºñ½º

ÁÖ¿ä ±â´É:
- Pexels/Pixabay ¿µ»ó ÀÚ»ê °Ë»ö ¹× ´Ù¿î·Îµå
- FFmpeg ±â¹Ý ¿µ»ó ÇÕ¼º (ÀåÆí/¼ôÆû)
- ½æ³×ÀÏ »ý¼º ¹× ÀÚ¸· Ã³¸®
- ¹è°æÀ½¾Ç ¹Í½Ì
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
# ·Î±ë ¼³Á¤
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
    """[O] ¾À ÀÎµ¦½º ±â¹Ý ¶Ç´Â ·£´ýÀ¸·Î xfade transition Å¸ÀÔ ¼±ÅÃ."""
    if not TRANSITION_POOL:
        return "fade"
    if TRANSITION_RANDOMIZE:
        import random
        return random.choice(TRANSITION_POOL)
    return TRANSITION_POOL[idx % len(TRANSITION_POOL)]


# [v15.78] KOREAN_GENERAL_MAP
KOREAN_GENERAL_MAP = {
    "ÀÎ°øÁö´É": "artificial intelligence AI robot technology",
    "±â°èÇÐ½À": "machine learning AI neural network",
    "¸Ó½Å·¯´×": "machine learning AI neural network",
    "µö·¯´×":   "deep learning AI neural network server",
    "ÀÚµ¿È­":   "automation robot factory industrial",
    "¾Ë°í¸®Áò": "algorithm computer code programming",
    "µ¥ÀÌÅÍ":   "data analytics computer chart server",
    "ºòµ¥ÀÌÅÍ": "big data server analytics dashboard",
    "µðÁöÅÐ":   "digital technology computer modern",
    "¼ÒÇÁÆ®¿þ¾î": "software code programming computer",
    "ÇÏµå¿þ¾î": "hardware electronic circuit board",
    "¹ÝµµÃ¼":   "semiconductor chip manufacturing",
    "·Îº¿":     "robot automation industrial arm factory",
    "µå·Ð":     "drone aerial sky technology",
    "ÇÃ·§Æû":   "platform app technology mobile",
    "³×Æ®¿öÅ©": "network server data center cables",
    "Å¬¶ó¿ìµå": "cloud computing server technology",
    "¸ÞÅ¸¹ö½º": "virtual reality digital immersive",
    "ºí·ÏÃ¼ÀÎ": "blockchain cryptocurrency digital",
    "°æÁ¦":     "economy business finance stock market",
    "±ÝÀ¶":     "finance banking stock market money",
    "ÅõÀÚ":     "investment stock market business",
    "½ÃÀå":     "market stock exchange business",
    "»ê¾÷":     "industry factory manufacturing",
    "±â¾÷":     "company business office corporate",
    "½ºÅ¸Æ®¾÷": "startup office technology entrepreneur",
    "Çõ½Å":     "innovation technology startup modern",
    "¼ºÀå":     "growth chart business success",
    "¹«¿ª":     "trade business international shipping",
    "»çÈ¸":     "society people urban city community",
    "Á¤Ä¡":     "politics government parliament",
    "Á¤ºÎ":     "government capitol building official",
    "Á¤Ã¥":     "policy government document official",
    "±³À°":     "education school classroom students",
    "ÀÇ·á":     "medical hospital doctor healthcare",
    "È¯°æ":     "environment nature green ecology",
    "±âÈÄ":     "climate change environment weather",
    "¿¡³ÊÁö":   "energy solar wind power renewable",
    "ÅÂ¾ç±¤":   "solar panel renewable energy green",
    "Àü±âÂ÷":   "electric vehicle car charging",
    "¿À¿°":     "pollution smog city environment",
    "¹Ì·¡":     "future technology smart city innovation",
    "º¯È­":     "change transformation progress",
    "Çõ¸í":     "revolution transformation innovation",
    "À§±â":     "crisis emergency warning problem",
    "µµÀü":     "challenge competition achievement",
    "±âÈ¸":     "opportunity success business growth",
    "ÀÏÀÚ¸®":   "employment job work office career",
    "³ëµ¿":     "labor work factory employee",
    "µµ½Ã":     "city urban skyline buildings modern",
    "¼­¿ï":     "Seoul South Korea city modern",
    "ÇÑ±¹":     "South Korea Seoul city modern",
    "¿¬±¸":     "research laboratory scientist",
    "°úÇÐ":     "science laboratory research experiment",
    "±â¼ú":     "technology innovation research lab",
    "¹ßÀü":     "development progress technology",
    "±Û·Î¹ú":   "global world international business",
    "¼¼°è":     "world global earth international",
    "ÀÌÈÄ":     "future forward progress timeline",
    "ÇöÀç":     "present current now modern",
    "¹Ìµð¾î":   "media news broadcast journalism",
    "¹®È­":     "culture art creative performance",
    "ÀÎ±¸":     "population people crowd demographic",
    "»ç¶÷":     "people crowd urban street",
    "±¹¹Î":     "people community society crowd",
}

def _strip_korean_particles(kw: str) -> str:
    _PARTICLES = [
        "ÀÌÈÄ¿¡´Â","ÀÌÈÄ¿¡µµ","¿¡¼­´Â","¿¡¼­µµ","À¸·Î´Â","À¸·Îµµ","À¸·Î¼­",
        "¿¡°Ô´Â","¿¡°Ôµµ","¿¡¼­","ºÎÅÍ´Â","±îÁö´Â","¿¡´Â","¿¡µµ","·Î´Â",
        "ÀÌ°í","ÀÌ¸ç","ÀÌ³ª","ÀÌÁö","ÀÌ¾ß","ÀÌ´Ù","ÀÌ¶ó","ÀÇÇØ",
        "¿¡¼­","¿¡","·Î","À»","¸¦","Àº","´Â","ÀÌ","°¡","¿Í","°ú","µµ","¸¸","ÀÇ",
    ]
    result = kw.strip()
    for p in _PARTICLES:
        if result.endswith(p) and len(result) > len(p) + 1:
            result = result[:-len(p)].strip()
            break
    return result

# ==================== [Y2] µµ¸ÞÀÎ Å°¿öµå Ä¡È¯ + ºÎÁ¤ ÇÊÅÍ ====================
# Àü¹®¿ë¾î´Â Pexels °¡ ÀÌÇØÇÏ´Â Ç¥ÇöÀ¸·Î ÀÚµ¿ Ä¡È¯
DOMAIN_KEYWORD_MAP = {
    # [BR-1] MARKER v6
    # [BR-1] BP ÇÑ±¹¾î Ç×¸ñ Á¦°Å (¼ö·Å ¿øÀÎ) ? ÀÌÇÏ À§¼º¡¤¿ìÁÖ¡¤±ÝÀ¶ µî¸¸ À¯Áö
    # À§¼º¡¤¿ìÁÖ
    "cubesat": "nanosatellite small satellite space",
    "cube sat": "nanosatellite small satellite space",
    "Å¥ºê»û": "nanosatellite small satellite space",
    "Å¥ºê¼Â": "nanosatellite small satellite space",

    # ¦¡¦¡¦¡ Ç×°ø¿ìÁÖ ½ÃÇè Àåºñ (½ÇÁ¦ equipment ¿µ»ó È®º¸) ¦¡¦¡¦¡
    "Áø°ø": "vacuum chamber laboratory equipment",
    "Áø°ø ½ÃÇè": "vacuum chamber thermal vacuum testing spacecraft",
    "Áø°ø Ã¨¹ö": "vacuum chamber thermal vacuum testing spacecraft",
    "vacuum": "vacuum chamber laboratory equipment",
    "vacuum test": "vacuum chamber thermal vacuum testing spacecraft",
    "vacuum chamber": "vacuum chamber thermal vacuum testing spacecraft",
    "thermal vacuum": "vacuum chamber thermal vacuum testing spacecraft",

    "Áøµ¿": "vibration testing shaker table laboratory",
    "Áøµ¿ ½ÃÇè": "vibration testing shaker table aerospace",
    "vibration": "vibration testing shaker table laboratory",
    "vibration test": "vibration testing shaker table aerospace",
    "shaker": "vibration testing shaker table aerospace",

    "¿­": "thermal chamber testing temperature laboratory",
    "¿­ ½ÃÇè": "thermal chamber testing temperature satellite",
    "thermal": "thermal chamber testing temperature laboratory",
    "thermal test": "thermal chamber testing temperature satellite",

    "¹æ»ç¼±": "radiation testing laboratory shielding aerospace",
    "¹æ»ç¼± ½ÃÇè": "radiation testing laboratory shielding aerospace",
    "radiation": "radiation testing laboratory shielding aerospace",
    "radiation test": "radiation testing laboratory shielding aerospace",

    "emc": "EMC testing anechoic chamber electronics",
    "ÀüÀÚÆÄ": "EMC testing anechoic chamber electronics",

    "Å¬¸°·ë": "clean room spacecraft assembly white suit",
    "clean room": "clean room spacecraft assembly white suit",
    "cleanroom": "clean room spacecraft assembly white suit",
    "Á¶¸³": "clean room spacecraft assembly engineer",
    "assembly": "clean room spacecraft assembly engineer",

    "È¯°æ ½ÃÇè": "environmental testing aerospace laboratory equipment",
    "environmental test": "environmental testing aerospace laboratory equipment",

    "ÀÎÁõ": "certification engineer laboratory documentation",
    "certification": "certification engineer laboratory documentation",

    "°ËÁõ": "engineer inspecting spacecraft laboratory",
    "verification": "engineer inspecting spacecraft laboratory",
    "validation": "engineer inspecting spacecraft laboratory",

    "½ÃÇè": "aerospace testing laboratory equipment engineer",
    "test": "aerospace testing laboratory equipment engineer",

    # ¼³°è ´Ü°è
    "¼³°è": "engineering blueprint CAD design aerospace",
    "design": "engineering blueprint CAD design aerospace",
    "blueprint": "engineering blueprint CAD design aerospace",
    "cad": "engineering blueprint CAD design aerospace",

    # Á¦Á¶ ´Ü°è
    "Á¦Á¶": "satellite manufacturing factory precision",
    "manufacturing": "satellite manufacturing factory precision",
    "»ý»ê": "satellite manufacturing factory precision",
    "production": "satellite manufacturing factory precision",

    # Ãß»ó¡¤±â¼ú ¿ë¾î ¡æ ½Ã°¢È­ °¡´ÉÇÑ ¿µ»ó
    "quantum": "optical fiber laser laboratory",
    "quantum optical": "optical fiber laser laboratory equipment",
    "ai": "server data center hardware",
    "artificial intelligence": "server data center hardware robot",
    "machine learning": "computer neural network visualization",
    # ÀÏ¹Ý Ãß»ó ¡æ ½ÇÁ¦ Àå¸é
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

    # ¦¡¦¡¦¡ [AM-1] Ãß»ó ´Ü¾î ¡æ ±¸Ã¼ ½Ã°¢ °´Ã¼ (Pexels ÅØ½ºÆ® ¿µ»ó È¸ÇÇ) ¦¡¦¡¦¡
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

    # ¦¡¦¡¦¡ [AF-13] Á¦ÀÛ¡¤°³¹ß ´Ü°è (¿µ»ó ¸ÅÄª Á¤È®µµ) ¦¡¦¡¦¡
    "Á¦ÀÛ": "manufacturing factory assembly production aerospace",
    "Á¦ÀÛ ´Ü°è": "manufacturing factory assembly production aerospace",
    "Á¦ÀÛ ¹× Å×½ºÆ®": "manufacturing testing laboratory aerospace engineer",
    "Å×½ºÆ®": "laboratory testing equipment engineer aerospace",
    "Å×½ºÆ® ´Ü°è": "laboratory testing equipment engineer aerospace",
    "°³³ä": "engineer blueprint mission planning diagram",
    "°³³ä ¼³°è": "engineering blueprint CAD design aerospace",
    "°³³ä ´Ü°è": "engineer blueprint mission planning diagram",
    "»ó¼¼": "engineer detailed technical drawing",
    "»ó¼¼ ¼³°è": "engineer detailed technical drawing CAD",
    "»ó¼¼ ¼³°è ´Ü°è": "engineer detailed technical drawing CAD",
    "ºÎÇ°": "electronic components circuit board aerospace parts",
    "ºÎÇ° ¼±ÅÃ": "electronic components circuit board aerospace parts",
    "½Ã½ºÅÛ": "spacecraft system integration engineer laboratory",
    "½Ã½ºÅÛ ±¸¼º": "spacecraft system integration engineer laboratory",
    "½Ã½ºÅÛ ÅëÇÕ": "spacecraft system integration engineer laboratory",
    "¿Ï·á": "engineer laboratory inspection aerospace",
    "Á¤ÀÇ": "satellite orbit mission planning spacecraft",
    "¸ñÇ¥": "rocket launch mission satellite space",
    "±â´É": "spacecraft function engineer laboratory",
    "ÀÓ¹«": "satellite mission launch spacecraft planning",
    "¸íÈ®È÷": "satellite hardware engineer inspection",
    "´Ü°è": "engineer workflow process diagram",
    "Ã¹Â°": "number one sign",
    "µÑÂ°": "number two sign",
    "¼ÂÂ°": "number three sign",

    # ¦¡¦¡¦¡ ºñ¿ë¡¤°æÁ¦¡¤½ÃÀå¡¤µ· (Pexels °¡ town¡¤village ·Î ÇØ¼®ÇÏ´Â ¹®Á¦ ¹æÁö) ¦¡¦¡¦¡
    "cost": "money dollar calculator budget chart",
    "ºñ¿ë": "money dollar calculator budget chart",
    "price": "money dollar calculator price tag",
    "°¡°Ý": "money dollar calculator price tag",
    "budget": "money dollar calculator budget chart",
    "¿¹»ê": "money dollar calculator budget chart",
    "economy": "stock market chart business finance",
    "°æÁ¦": "stock market chart business finance",
    "market": "stock market trading chart screen",
    "½ÃÀå": "stock market trading chart screen",
    "revenue": "money growth chart business profit",
    "¸ÅÃâ": "money growth chart business profit",
    "finance": "money growth chart business bank",
    "ÀçÁ¤": "money growth chart business bank",
    "investment": "money stock chart investor business",
    "ÅõÀÚ": "money stock chart investor business",
    "profit": "money growth chart business profit",
    "¼öÀÍ": "money growth chart business profit",
    "billion": "money stack cash finance",
    "million": "money stack cash finance",
    "Á¶¿ø": "money stack cash finance",
    "¾ï¿ø": "money stack cash finance",
    "dollar": "money dollar cash bill",
    "´Þ·¯": "money dollar cash bill",
    "won": "money cash korean currency",
    "¿ø": "money cash currency bill",

    # ¦¡¦¡¦¡ »ê¾÷¡¤ºñÁî´Ï½º (town/village ¹æÁö) ¦¡¦¡¦¡
    "space industry": "rocket launch satellite factory manufacturing",
    "¿ìÁÖ »ê¾÷": "rocket launch satellite factory manufacturing",
    "industry": "factory manufacturing industrial machinery",
    "»ê¾÷": "factory manufacturing industrial machinery",
    "business": "office meeting corporate professional",
    "ºñÁî´Ï½º": "office meeting corporate professional",
    "startup": "office team laptop computer meeting",
    "½ºÅ¸Æ®¾÷": "office team laptop computer meeting",

    # ºÎÁ¤ Å°¿öµå (°Ë»ö °á°ú ÇÊÅÍ)
}

NEGATIVE_TERMS = [
    # ±â¼ú ¿µ»ó¿¡ ¹æÇØµÇ´Â ÀÏ¹Ý ¿ä¼Ò
    "toy", "cartoon", "animation", "animated", "illustration",
    "drawing", "clipart", "plastic toy", "puzzle cube",
    "rubik", "rubiks", "rubik's",
]

# ÁÖÁ¦ ¸Æ¶ô ºÎÁ¤ Å°¿öµå (Å°¿öµå È®Àå °á°ú¿¡ µû¶ó µ¿Àû Àû¿ë)
# "ºñ¿ë/°æÁ¦/»ê¾÷" ¸Æ¶ô¿¡ µîÀåÇÏ¸é Á¦¿ÜÇÒ ÅÂ±×
BUSINESS_NEGATIVE_TERMS = [
    "village", "suburb", "residential", "countryside", "farm",
    "rural", "traditional village", "old town", "vintage house",
    "tourism", "tourist", "travel destination",
]


def _expand_domain_keyword(kw: str) -> str:
    """µµ¸ÞÀÎ ¿ë¾î ¡æ Pexels Ä£È­Àû ±¸¹®À¸·Î Ä¡È¯."""
    # [BQ-2] MARKER v5
    if not kw:
        return kw
    lower = kw.lower().strip()
    # Á¤È®È÷ ÀÏÄ¡
    if lower in DOMAIN_KEYWORD_MAP:
        return DOMAIN_KEYWORD_MAP[lower]
    # ºÎºÐ Æ÷ÇÔ Ä¡È¯ (´Ü¾î ´ÜÀ§)
    for key, val in DOMAIN_KEYWORD_MAP.items():
        if key in lower:
            replaced = lower.replace(key, val)
            # [BQ-2] ÇÑ±¹¾î(AC00-D7AF) ÀÜ·ù¸é Ä¡È¯°ª¸¸ »ç¿ë
            has_hangul = any(0xAC00 <= ord(c) <= 0xD7AF for c in replaced)
            if has_hangul:
                return val
            return replaced
    # [BR-2] MARKER v7
    # [BR-2] ÇÑ±¹¾î Æ÷ÇÔÀÌ°í ¸ÅÇÎ ¾øÀ¸¸é ÇÑ±¹¾î¸¸ ½ºÆ®¸³ÇÏ°í ¿µ¾î ÅäÅ« ¹ÝÈ¯
    if any(0xAC00 <= ord(c) <= 0xD7AF for c in kw):
        ascii_only = "".join(c for c in kw if ord(c) < 128).strip()
        # °ø¹é Á¤¸®
        while "  " in ascii_only:
            ascii_only = ascii_only.replace("  ", " ")
        ascii_only = ascii_only.strip()
        if len(ascii_only.split()) >= 2:
            return ascii_only
        # [v15.78] KOREAN_GENERAL_MAP ÀçÁ¶È¸
        _base = _strip_korean_particles(kw.lower().strip())
        if _base in KOREAN_GENERAL_MAP:
            return KOREAN_GENERAL_MAP[_base]
        for _k, _v in KOREAN_GENERAL_MAP.items():
            if _k in _base or _base in _k:
                return _v
        return _base if _base else ""
    return kw


def _is_negative(video_info: dict, context_keyword: str = "") -> bool:
    """Pexels/Pixabay ÀÀ´ä °´Ã¼ ³» negative term Æ÷ÇÔ ¿©ºÎ.
    context_keyword ¿¡ business/money ¸Æ¶ôÀÌ ÀÖÀ¸¸é village ·ùµµ Á¦¿Ü."""
    text = " ".join(str(v).lower() for v in [
        video_info.get("user", {}).get("name", "") if isinstance(video_info.get("user"), dict) else "",
        video_info.get("tags", ""),
        video_info.get("url", ""),
        " ".join(video_info.get("tags", [])) if isinstance(video_info.get("tags"), list) else "",
    ])
    # ±âº» ºÎÁ¤ Å°¿öµå
    if any(neg in text for neg in NEGATIVE_TERMS):
        return True
    # ºñ¿ë¡¤ºñÁî´Ï½º ¸Æ¶ôÀÌ¸é village/rural ·ùµµ Â÷´Ü
    ctx = (context_keyword or "").lower()
    is_biz = any(b in ctx for b in ["money", "dollar", "budget", "market", "chart",
                                      "business", "office", "factory", "industry"])
    if is_biz and any(neg in text for neg in BUSINESS_NEGATIVE_TERMS):
        return True
    return False



# ==================== [P] Fallback ºñÁÖ¾ó »ý¼º±â ====================
FALLBACK_COLOR_POOL = [
    # (top_hex, bottom_hex, text_color)
    ("#1a2a6c", "#b21f1f", "#ffffff"),  # µöºí·ç ¡æ Å©¸²½¼
    ("#0f2027", "#2c5364", "#e0f7fa"),  # ºí·¢ºí·ç ¡æ ½Ã¾È
    ("#134e5e", "#71b280", "#ffffff"),  # Æ¿ ¡æ ¹ÎÆ®
    ("#c94b4b", "#4b134f", "#fff1f1"),  # ·¹µå ¡æ ÆÛÇÃ
    ("#ff512f", "#dd2476", "#ffffff"),  # ¿À·»Áö ¡æ ÇÎÅ©
    ("#2c3e50", "#4ca1af", "#f0f8ff"),  # ½½·¹ÀÌÆ® ¡æ ½Ã¾È
    ("#11998e", "#38ef7d", "#0a2e24"),  # ¿¡¸Þ¶öµå
    ("#8e2de2", "#4a00e0", "#ffffff"),  # ÆÛÇÃ ±×¶óµð¾ðÆ®
    ("#f953c6", "#b91d73", "#ffffff"),  # ÇÎÅ© ±×¶óµð¾ðÆ®
    ("#ee0979", "#ff6a00", "#fff3e0"),  # ¼±¼Â
]


def _hex_to_ass_bgr(hex_color: str) -> str:
    """#RRGGBB ¡æ ASS &HAABBGGRR& (¾ËÆÄ 00)"""
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
OUTRO_CTA_TEXT = os.getenv("OUTRO_CTA_TEXT", "±¸µ¶ & ÁÁ¾Æ¿ä")


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
        logger.warning(f"[AJ-1] intro »ý¼º ½ÇÆÐ: {e}")
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
        logger.warning(f"[AJ-2] outro »ý¼º ½ÇÆÐ: {e}")
        return False


def _make_fallback_clip(scene_index: int, duration_sec: float, output_path: Path,
                        keyword: str = "", description: str = "",
                        resolution: str = "1920x1080") -> bool:
    """[P] ÀÚ»ê ¾øÀ» ¶§ ±×¶óµð¾ðÆ® + Å°¿öµå Ä«µå + ½½·Î¿ì zoompan Å¬¸³ »ý¼º."""
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

    # ±×¶óµð¾ðÆ® ¹è°æ ¡æ Å°¿öµå ¡æ ºÎÁ¦ ¡æ zoompan À¸·Î ¿Ï¼º
    # ffmpeg: color src 2°³ + vstack + overlay ´ë½Å, gradients filter »ç¿ë
    # ´Ü¼øÇÏ°Ô: color1 ·Î ÀüÃ¼ Ã¤¿ì°í radial/linear ±×¶óµð¾ðÆ®´Â drawbox + geq º¹ÀâÇÏ´Ï
    # ¿©±â¼± "color=top:half" "color=bot:half" vstack À¸·Î 2»ö split
    # ´õ ³ªÀº ¿É¼Ç: gradients filter (ffmpeg 5+) ¡æ c0=top:c1=bot

    # gradients filter °¡ ÀÖÀ¸¸é °¡Àå ±ò²û
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

    # ÅØ½ºÆ® ¿À¹ö·¹ÀÌ + ½½·Î¿ì zoompan
    # drawtext ·Î Å°¿öµå + ºÎÁ¦
    font_file = "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc"
    if not Path(font_file).exists():
        # Noto ¾øÀ¸¸é DejaVu fallback
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
        # Clean fallback ? no text, pure gradient + zoom
        filter_full = filter_expr

    # slow zoompan È¿°ú: z ´Â ÃµÃµÈ÷ Áõ°¡, x/y ´Â center °íÁ¤
    zp_frames = max(30, int(duration_sec * 30))
    filter_full += (
        f",zoompan=z='min(zoom+0.0008,1.08)':"
        f"x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)':"
        f"d={zp_frames}:s={W}x{H}:fps=30"
    )

    cmd = [
        "ffmpeg", "-y",
        "-f", "lavfi",
        "-i", filter_expr,  # »ö »ý¼º¿ë lavfi ÀÔ·Â
        "-t", str(duration_sec),
        "-vf", filter_full.replace(filter_expr + ",", "", 1),
        "-c:v", "libx264", "-preset", "ultrafast", "-pix_fmt", "yuv420p",
        "-r", "30",
        str(output_path)
    ]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if proc.returncode == 0 and output_path.exists() and output_path.stat().st_size > 1024:
            logger.info(f"[P] fallback ºñÁÖ¾ó: {output_path.name} ({keyword[:20]}, {top}¡æ{bot})")
            return True
        logger.warning(f"[P] fallback »ý¼º ½ÇÆÐ: {proc.stderr[-300:]}")
        return False
    except Exception as e:
        logger.error(f"[P] fallback ¿¹¿Ü: {e}")
        return False



# [AE] MARKER v1
# [AU-1] Resolution config - 1080p / 4K support
OUTPUT_RESOLUTION = os.getenv("OUTPUT_RESOLUTION", "1920x1080")  # or "3840x2160" for 4K
VF_W, VF_H = [int(x) for x in OUTPUT_RESOLUTION.split("x")]
VIDEO_CRF = int(os.getenv("VIDEO_CRF", "15"))  # [AW-2] 18¡æ15 higher quality
VIDEO_PRESET = os.getenv("VIDEO_PRESET", "medium")  # [AW-2] slow=ÃÖ°í fast=ºü¸§ medium=±ÕÇü
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
# [AX] Watermark disabled permanently - ±×¸² ¿À¹ö·¹ÀÌ »ç¿ë ¾ÈÇÔ
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


# [v15.77] ??????????????????????????????????????????????????????
# ·Î¿ö¼­µå(Lower-Third) ¹æ¼Û ±×·¡ÇÈ ? KBS ½Ã»ç±âÈ¹ ½ºÅ¸ÀÏ
# ¼ýÀÚ/Åë°è/[ÇÏÀÌ¶óÀÌÆ®:] ¸¶Ä¿ ¡æ ÇÏ´Ü ASS ¿À¹ö·¹ÀÌ
# ??????????????????????????????????????????????????????????????

def _extract_lower_third_events_from_narration(
    scenes: list,
    whisper_path=None,
    video_duration: float = 300.0,
) -> list:
    """
    ³ª·¹ÀÌ¼Ç¿¡¼­ ·Î¿ö¼­µå ÀÌº¥Æ® ÃßÃâ.
    ¿ì¼±¼øÀ§: [ÇÏÀÌ¶óÀÌÆ®: TEXT] ¸¶Ä¿ > ¼ýÀÚ+´ÜÀ§ ÆÐÅÏ
    Returns: [{"start": float, "end": float, "text": str, "style": str}]
    """
    import re as _re77
    events = []
    cumulative = 0.0

    for scene in scenes:
        narr = scene.narration or scene.description or ""
        dur = max(scene.duration_seconds or 5.0, 1.0)

        # 1) [ÇÏÀÌ¶óÀÌÆ®: TEXT] ¸¶Ä¿ ¿ì¼± ÃßÃâ
        for m in _re77.finditer(r"\[ÇÏÀÌ¶óÀÌÆ®:\s*([^\]]{2,30})\]", narr):
            ratio = m.start() / max(len(narr), 1)
            t_start = cumulative + ratio * dur
            events.append({
                "start": round(max(t_start - 0.2, 0.0), 2),
                "end":   round(t_start + 2.8, 2),
                "text":  m.group(1).strip(),
                "style": "Stat",
            })

        # 2) ¸¶Ä¿ ¾ø´Â ¾À: ¼ýÀÚ+´ÜÀ§ ÆÐÅÏ ÀÚµ¿ °¨Áö
        scene_has_marker = any(
            cumulative <= e["start"] < cumulative + dur for e in events
        )
        if not scene_has_marker:
            num_re = r"\d+[\.,]?\d*\s*(?:Á¶|¾ï|¸¸|Ãµ|%|¹è|¸í|°³|À§|³â|¿ù|ÀÏ|km|´Þ·¯|¿ø|¹ø|È¸|°³±¹|°÷)"
            for m in _re77.finditer(num_re, narr):
                ctx_s = max(0, m.start() - 4)
                ctx_e = min(len(narr), m.end() + 10)
                display = narr[ctx_s:ctx_e].strip()
                display = _re77.sub(r"\[ÇÏÀÌ¶óÀÌÆ®:[^\]]*\]", "", display).strip()[:22]
                if not display:
                    display = m.group(0)
                ratio = m.start() / max(len(narr), 1)
                t_start = cumulative + ratio * dur + 0.3
                events.append({
                    "start": round(max(t_start, 0.0), 2),
                    "end":   round(t_start + 2.5, 2),
                    "text":  display,
                    "style": "Stat",
                })

        cumulative += dur

    # Á¤·Ä + ÃÖ¼Ò 2ÃÊ °£°Ý ÇÊÅÍ
    events.sort(key=lambda e: e["start"])
    filtered, last_end = [], -3.0
    for ev in events:
        if ev["start"] >= last_end + 1.2 and ev["end"] <= video_duration + 1.0:
            filtered.append(ev)
            last_end = ev["end"]

    return filtered[:18]  # ÃÖ´ë 18°³


def create_lower_third_ass(events: list, output_path: "Path") -> bool:
    """KBS ½ºÅ¸ÀÏ ·Î¿ö¼­µå ASS ÆÄÀÏ »ý¼º (Alignment=1: ÁÂÇÏ´Ü, Layer=1)."""
    if not events:
        return False
    try:
        LOWER_FONT = "Noto Sans CJK KR"
        lines_out = [
            "[Script Info]",
            "ScriptType: v4.00+",
            "PlayResX: 1920",
            "PlayResY: 1080",
            "WrapStyle: 0",
            "",
            "[V4+ Styles]",
            "Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding",
            # Stat: ³ë¶õ ÅØ½ºÆ®, ¹ÝÅõ¸í ³²»ö ¹Ú½º, ÁÂÇÏ´Ü
            f"Style: Stat,{LOWER_FONT},48,&H0000FFFF,&H000000FF,&H00000000,&HAA001133,-1,0,0,0,100,100,1,0,3,0,0,1,80,80,185,1",
            # Term: Ã»·Ï ÅØ½ºÆ®, ÁÂÇÏ´Ü
            f"Style: Term,{LOWER_FONT},44,&H00FFFF00,&H000000FF,&H00000000,&HAA001133,-1,0,0,0,100,100,1,0,3,0,0,1,80,80,185,1",
            "",
            "[Events]",
            "Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text",
        ]

        def _fmt(secs: float) -> str:
            h = int(secs // 3600)
            m = int((secs % 3600) // 60)
            s = secs % 60
            return f"{h}:{m:02d}:{s:05.2f}"

        for ev in events:
            text = ev["text"].replace("\n", " ").replace(",", "£¬")
            style = ev.get("style", "Stat")
            lines_out.append(
                f"Dialogue: 1,{_fmt(ev['start'])},{_fmt(ev['end'])},{style},,80,80,185,,{text}"
            )

        output_path.write_text("\n".join(lines_out), encoding="utf-8-sig")
        logger.info(f"[v15.77] ·Î¿ö¼­µå ASS »ý¼º: {len(events)}°³ ÀÌº¥Æ® ¡æ {output_path.name}")
        return True
    except Exception as e:
        logger.error(f"[v15.77] ·Î¿ö¼­µå ASS ½ÇÆÐ: {e}")
        return False


# [v15.78] Áß¾Ó Å°¿öµå ¹è³Ê (Áö½ÄÀÎ»çÀÌµå ½ºÅ¸ÀÏ)
def _extract_center_banner_events(scenes: list, video_duration: float = 300.0) -> list:
    import re as _re78
    candidates = []
    cumulative = 0.0
    for scene in scenes:
        narr = scene.narration or scene.description or ""
        dur = max(scene.duration_seconds or 5.0, 1.0)
        for m in _re78.finditer(r"\[ÇÏÀÌ¶óÀÌÆ®:\s*([^\]]{2,30})\]", narr):
            text = m.group(1).strip()
            has_num = bool(_re78.search(r"\d", text))
            has_unit = bool(_re78.search(r"[%¾ïÁ¶¸¸¸íÀ§]", text))
            weight = 3 if (has_num and has_unit) else (2 if has_num else 1)
            ratio = m.start() / max(len(narr), 1)
            t_start = cumulative + ratio * dur
            candidates.append({"start": round(max(t_start+0.3,0),2),
                                "end": round(t_start+2.2,2),
                                "text": text, "weight": weight})
        cumulative += dur
    candidates.sort(key=lambda x: -x["weight"])
    selected, used_times = [], []
    for c in candidates:
        if not any(abs(c["start"]-t) < 15.0 for t in used_times) and c["end"] <= video_duration+1:
            selected.append(c)
            used_times.append(c["start"])
        if len(selected) >= 5:
            break
    selected.sort(key=lambda x: x["start"])
    return selected

def create_center_banner_ass(events: list, output_path) -> bool:
    if not events:
        return False
    try:
        F = "Noto Sans CJK KR"
        lines = [
            "[Script Info]", "ScriptType: v4.00+",
            "PlayResX: 1920", "PlayResY: 1080", "WrapStyle: 0", "",
            "[V4+ Styles]",
            "Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding",
            f"Style: Banner,{F},82,&H00FFFFFF,&H000000FF,&H00000000,&HBB000000,-1,0,0,0,100,100,3,0,3,0,0,5,80,80,0,1",
            f"Style: Accent,{F},88,&H0000FFFF,&H000000FF,&H00000000,&HBB000000,-1,0,0,0,100,100,2,0,3,0,0,5,80,80,0,1",
            "", "[Events]",
            "Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text",
        ]
        import re as _r
        def _fmt(s):
            h=int(s//3600); m=int((s%3600)//60); sec=s%60
            return f"{h}:{m:02d}:{sec:05.2f}"
        for ev in events:
            text = ev["text"].replace("\n"," ").replace(",","£¬")
            style = "Accent" if _r.search(r"\d+.*[%¾ïÁ¶¸¸¸íÀ§]|[%¾ïÁ¶¸¸¸íÀ§].*\d+", text) else "Banner"
            lines.append(f"Dialogue: 2,{_fmt(ev['start'])},{_fmt(ev['end'])},{style},,80,80,0,,{text}")
        output_path.write_text("\n".join(lines), encoding="utf-8-sig")
        logger.info(f"[v15.78] Áß¾Ó¹è³Ê ASS: {len(events)}°³")
        return True
    except Exception as e:
        logger.error(f"[v15.78] Áß¾Ó¹è³Ê ½ÇÆÐ: {e}")
        return False

# ¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡

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
    """[v16.7] ÇØ»óµµ ¹®ÀÚ¿­¿¡¼­ ÀÚ¸· Å©±â¡¤¸¶Áø °è»ê. ¼¼·ÎÇü(1080x1920) ÀÚµ¿ ÃÖÀûÈ­.
    Returns (font_size, margin_v).
    """
    try:
        w_str, h_str = resolution.lower().split("x")
        width, height = int(w_str), int(h_str)
    except Exception:
        width, height = 1920, 1080

    is_vertical = height > width  # ¼¼·ÎÇü ¼îÃ÷ °¨Áö

    if SUBTITLE_FONT_SIZE > 0:
        font_size = SUBTITLE_FONT_SIZE
    else:
        if is_vertical:
            # ¼¼·ÎÇü(1080x1920): È­¸é ³Êºñ ±âÁØ 7% ¡æ ¾à 75px (°¡µ¶¼º ÃÖ¿ì¼±)
            font_size = max(64, int(width * 0.07))
        else:
            font_size = max(16, int(height * SUBTITLE_FONT_SIZE_RATIO))

    if SUBTITLE_MARGIN_V > 0 and SUBTITLE_MARGIN_V != 30:
        margin_v = SUBTITLE_MARGIN_V
    else:
        if is_vertical:
            # ¼¼·ÎÇü: ÇÏ´Ü ¾ÈÀü¿µ¿ª È®º¸ (1920px ±âÁØ ~200px, UI ¿µ¿ª È¸ÇÇ)
            margin_v = max(160, int(height * 0.105))
        else:
            margin_v = max(20, int(height * SUBTITLE_MARGIN_RATIO))

    return font_size, margin_v







# ============================================================================
# ¿­°ÅÇü Á¤ÀÇ
# ============================================================================
class VideoMode(str, Enum):
    """¿µ»ó Á¦ÀÛ ¸ðµå"""
    LONGFORM = "longform"  # 1920x1080 °¡·ÎÇü
    SHORTFORM = "shortform"  # 1080x1920 ¼¼·ÎÇü
    MUSIC_VIDEO = "music_video"  # BGM + ÀÚ¸· ¹ÂÁ÷ºñµð¿À


class JobStatus(str, Enum):
    """ÀÛ¾÷ »óÅÂ [v15.59.0 È®Àå]"""
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
    """ÀÚ»ê À¯Çü"""
    VIDEO = "video"
    IMAGE = "image"
    AUDIO = "audio"


# ============================================================================
# ¸®µë ÄÆ ¡¤ ÀÚ¸· ¼±Çà ÆÄ¶ó¹ÌÅÍ (È¯°æº¯¼ö·Î override °¡´É)
# ============================================================================
import os as _rhythm_os
SUBTITLE_LEAD_SEC   = float(_rhythm_os.getenv("SUBTITLE_LEAD_SEC", "0.15"))   # ÀÚ¸· ¼±Çà ½Ã°£
SCENE_LEAD_SEC      = float(_rhythm_os.getenv("SCENE_LEAD_SEC", "0.0"))  # [AH-1] MARKER v1 AH-2
BGM_AUTO_DUCK       = _rhythm_os.getenv("BGM_AUTO_DUCK", "true").lower() in ("1","true","yes","on")
BGM_DUCK_DB         = float(_rhythm_os.getenv("BGM_DUCK_DB", "15"))  # [AF-5] BGM sidechain °¨¼è (dB)      # [AD] ¾ÀÀÌ ÀÚ¸·º¸´Ù ¸ÕÀú ³ª¿À´Â ¹öÆÛ
UNIFIED_TIMELINE    = _rhythm_os.getenv("UNIFIED_TIMELINE", "true").lower() in ("1","true","yes","on")   # [AD] MARKER v1
SCENE_MIN_SEC       = float(_rhythm_os.getenv("SCENE_MIN_SEC", "2.0"))        # ¾À ÃÖ¼Ò ±æÀÌ
SCENE_MAX_SEC       = float(_rhythm_os.getenv("SCENE_MAX_SEC", "2.5"))  # [BF] ´õ ÂÉ°³±â        # ¾À ÃÖ´ë ±æÀÌ (ÃÊ°ú ½Ã ºÐÇÒ)
SUBTITLE_MAX_CHARS  = int(_rhythm_os.getenv("SUBTITLE_MAX_CHARS", "15"))      # ÀÚ¸· ÇÑ ÁÙ ÃÖ´ë ±ÛÀÚ

# [N] ÀÚ¸· ½ºÅ¸ÀÏ
SUBTITLE_FONT_NAME   = _rhythm_os.getenv("SUBTITLE_FONT_NAME", "Noto Sans CJK KR")
SUBTITLE_FONT_FILE   = _rhythm_os.getenv("SUBTITLE_FONT_FILE", "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc")
SUBTITLE_FONT_SIZE   = int(_rhythm_os.getenv("SUBTITLE_FONT_SIZE", "0"))             # 0=ºñÀ² ÀÚµ¿ °è»ê, >0 °íÁ¤ px
SUBTITLE_FONT_SIZE_RATIO = float(_rhythm_os.getenv("SUBTITLE_FONT_SIZE_RATIO", "0.018"))  # ³ôÀÌ¡¿0.03 (1080p¡æ32, 720p¡æ22)
SUBTITLE_MARGIN_RATIO = float(_rhythm_os.getenv("SUBTITLE_MARGIN_RATIO", "0.030"))    # ³ôÀÌ¡¿0.04 (1080p¡æ43, 720p¡æ29)
SUBTITLE_BOLD        = int(_rhythm_os.getenv("SUBTITLE_BOLD", "1"))                  # 0/1
SUBTITLE_FONT_COLOR  = _rhythm_os.getenv("SUBTITLE_FONT_COLOR", "&H00FFFFFF&")       # Èò»ö ±âº» (BGR + AA)
SUBTITLE_OUTLINE_COLOR = _rhythm_os.getenv("SUBTITLE_OUTLINE_COLOR", "&H00000000&")  # °ËÁ¤ Å×µÎ¸®
SUBTITLE_BACK_COLOR  = _rhythm_os.getenv("SUBTITLE_BACK_COLOR", "&H80000000&")       # ¹ÝÅõ¸í °ËÁ¤ ¹Ú½º
SUBTITLE_BORDER_STYLE = int(_rhythm_os.getenv("SUBTITLE_BORDER_STYLE", "1"))         # 1=outline, 3=opaque box
SUBTITLE_OUTLINE_PX  = int(_rhythm_os.getenv("SUBTITLE_OUTLINE_PX", "2"))            # outline µÎ²²
SUBTITLE_SHADOW_PX   = int(_rhythm_os.getenv("SUBTITLE_SHADOW_PX", "1"))
SUBTITLE_MARGIN_V    = int(_rhythm_os.getenv("SUBTITLE_MARGIN_V", "30"))             # ÇÏ´Ü ¿©¹é
SUBTITLE_ALIGNMENT   = int(_rhythm_os.getenv("SUBTITLE_ALIGNMENT", "2"))             # 2=ÇÏ´ÜÁß¾Ó

# [O] Transition ´Ù¾çÈ­ ? xfade Å¸ÀÔ pool (None ³Ñ±â¸é ±âº» fade)
# »ç¿ë °¡´É: fade, wiperight, wipeleft, slideup, slidedown, circleopen, circleclose,
#            radial, pixelize, dissolve, smoothleft, smoothright, diagbl, diagbr
TRANSITION_POOL = [t.strip() for t in _rhythm_os.getenv(
    "TRANSITION_POOL",
    "fade,wiperight,slideup,circleopen,radial,pixelize,dissolve,smoothleft,smoothright,diagbl,diagbr,coverright,rectcrop"
).split(",") if t.strip()]
TRANSITION_RANDOMIZE = _rhythm_os.getenv("TRANSITION_RANDOMIZE", "true").lower() in ("true", "1", "yes")
FADE_DUR = float(_rhythm_os.getenv("FADE_DUR", "0.18"))                          # xfade ±âº» ±æÀÌ
FADE_DUR_MIN = float(_rhythm_os.getenv("FADE_DUR_MIN", "0.15"))                       # [Z] ·£´ý ÃÖ¼Ò
FADE_DUR_MAX = float(_rhythm_os.getenv("FADE_DUR_MAX", "0.30"))                       # [Z] ·£´ý ÃÖ´ë
FADE_DUR_RANDOMIZE = _rhythm_os.getenv("FADE_DUR_RANDOMIZE", "true").lower() in ("true", "1", "yes")

# [R] ¸ð¼Ç ÀýÁ¦ (´«¿¡ ¾È ¶ç´Â Á¤µµ)
KENBURNS_MAX_ZOOM   = float(_rhythm_os.getenv("KENBURNS_MAX_ZOOM", "1.06"))      # 100¡æ106% (±âÁ¸ 1.6¡æ1.06)
KENBURNS_PAN_PX     = int(_rhythm_os.getenv("KENBURNS_PAN_PX", "30"))            # ÁÂ¿ì ÀÌµ¿ px
KENBURNS_TILT_PX    = int(_rhythm_os.getenv("KENBURNS_TILT_PX", "16"))           # »óÇÏ ÀÌµ¿ px

# [v15.60.0] Narration-First Timeline Engine ENV
PAUSE_COMMA_MS          = int(float(_rhythm_os.getenv("PAUSE_COMMA_MS", "180")))
PAUSE_SENTENCE_MS       = int(float(_rhythm_os.getenv("PAUSE_SENTENCE_MS", "420")))
SCENE_HEAD_PAD_SEC      = float(_rhythm_os.getenv("SCENE_HEAD_PAD_SEC", "0.15"))
SCENE_TAIL_PAD_SEC      = float(_rhythm_os.getenv("SCENE_TAIL_PAD_SEC", "0.35"))
BGM_VOLUME_DEFAULT      = float(_rhythm_os.getenv("BGM_VOLUME_DEFAULT", "0.10"))
BGM_VOLUME_DURING_VOICE = float(_rhythm_os.getenv("BGM_VOLUME_DURING_VOICE", "0.045"))
NTL_ENABLED             = _rhythm_os.getenv("NTL_ENABLED", "true").lower() in ("true", "1", "yes")

# Àå¸é ±æÀÌ ºÐ»ê
SCENE_LEN_VARIANCE = float(_rhythm_os.getenv("SCENE_LEN_VARIANCE", "0.5"))       # ¡¾0.5s ·£´ý
PAUSE_THRESHOLD_SEC = float(_rhythm_os.getenv("PAUSE_THRESHOLD_SEC", "0.3"))  # [Q2] ½°À¸·Î ÀÎÁ¤ÇÒ ´Ü¾î °£°Ý

# [Q3] º¹ÇÕ¾î º¸È£: ÀÚ¸· ÁÙ¹Ù²Þ ±ÝÁö N-±×·¥
_NO_BREAK_DEFAULT = [
    "Áø°ø Ã¨¹ö", "¿­ ½ÃÇè", "Áøµ¿ ½ÃÇè", "¿ìÁÖ È¯°æ", "À§¼º Å×½ºÆ®",
    "±Ëµµ ÁøÀÔ", "¹ß»çÃ¼ ¼º´É", "Áö»ó±¹ °üÁ¦", "¸ðµâ·¯ ÇÁ¸®ÆÕ",
    "µö·¯´× ¸ðµ¨", "¸Ó½Å·¯´× ¸ðµ¨", "¾çÀÚ Åë½Å", "¾çÀÚ ±¤Åë½Å",
    "ÀÎ°øÁö´É", "AI", "API", "IoT",
]
_NO_BREAK_ENV = _rhythm_os.getenv("NO_BREAK_TERMS", "")
NO_BREAK_TERMS = _NO_BREAK_DEFAULT + [t.strip() for t in _NO_BREAK_ENV.split(",") if t.strip()]
_NBSP = "\u00a0"  # ÁÙ¹Ù²Þ ±ÝÁö¿ë non-breaking space


# ============================================================================
# Pydantic µ¥ÀÌÅÍ ¸ðµ¨
# ============================================================================
class Scene(BaseModel):
    """¿µ»ó Àå¸é Á¤ÀÇ"""
    scene_id: str = Field(..., description="Àå¸é °íÀ¯ ID")
    keyword: str = Field(..., description="°Ë»ö Å°¿öµå")
    duration_seconds: float = Field(..., ge=0.5, le=3600, description="Àå¸é ±æÀÌ(ÃÊ)")
    description: Optional[str] = Field(None, description="Àå¸é ¼³¸í")
    asset_url: Optional[str] = Field(None, description="´Ù¿î·ÎµåµÈ ÀÚ»ê URL")
    asset_type: AssetType = Field(default=AssetType.VIDEO, description="ÀÚ»ê À¯Çü")
    # [v15.60.0] Narration-First È®Àå ÇÊµå
    narration: Optional[str] = Field(None, description="¾À ³ª·¹ÀÌ¼Ç ÅØ½ºÆ®")
    visual_intent: Optional[str] = Field(None, description="½Ã°¢Àû ÀÇµµ (dynamic/calm/dramatic/educational/uplifting)")
    visual_keywords: Optional[List[str]] = Field(default_factory=list, description="ºñÁÖ¾ó °Ë»ö Å°¿öµå ¸ñ·Ï")
    tone_profile: Optional[str] = Field(None, description="Åæ (info/news/edu/ad/story)")
    visual_pacing: Optional[str] = Field(None, description="ÆäÀÌ½Ì (fast/normal/slow)")
    timing: Optional[Dict[str, float]] = Field(None, description="Å¸ÀÓ¶óÀÎ Å¸ÀÌ¹Ö")
    alt_asset_url: Optional[str] = Field(None, description="[v15.68] 2¹øÂ° ¼Ò½º ¿µ»ó °æ·Î (¼­ºêÅ¬¸³ ´Ù¾çÈ­)")
    alt_keywords: List[str] = Field(default_factory=list, description="[v15.68] ´ëÃ¼ °Ë»ö Å°¿öµå")
    narration_en: Optional[str] = Field(None, description="[v15.69] Kling T2V¿ë ¿µ¾î ºñÁÖ¾ó ÇÁ·ÒÇÁÆ®")


class AssetsSearchRequest(BaseModel):
    """ÀÚ»ê °Ë»ö ¿äÃ»"""
    job_id: str = Field(..., description="ÀÛ¾÷ ID")
    scenes: List[Scene] = Field(..., min_items=1, description="°Ë»öÇÒ Àå¸é ¸ñ·Ï")
    sources: str = Field(default="pexels,pixabay", description="°Ë»ö ¼Ò½º (½°Ç¥ ±¸ºÐ)")


class VideoCreateRequest(BaseModel):
    """¿µ»ó »ý¼º ¿äÃ»"""
    job_id: str = Field(..., description="ÀÛ¾÷ ID")
    mode: VideoMode = Field(default=VideoMode.LONGFORM, description="Á¦ÀÛ ¸ðµå")
    resolution: str = Field(default="1920x1080", description="Ãâ·Â ÇØ»óµµ")
    fps: int = Field(default=30, ge=24, le=60, description="ÇÁ·¹ÀÓ·ü")
    add_subtitles: bool = Field(default=False, description="ÀÚ¸· Ãß°¡ ¿©ºÎ")
    add_bgm: bool = Field(default=True, description="¹è°æÀ½¾Ç Ãß°¡ ¿©ºÎ")
    bgm_volume: float = Field(default=0.3, ge=0.0, le=1.0, description="¹è°æÀ½¾Ç º¼·ý(0-1)")
    generate_thumbnail: bool = Field(default=True, description="½æ³×ÀÏ »ý¼º")
    generate_shorts: bool = Field(default=True, description="¼ôÆû »ý¼º")
    shorts_durations: List[float] = Field(default=[5.0, 10.0, 60.0], description="¼ôÆû Ãâ·Â ±æÀÌ ¸ñ·Ï(ÃÊ) [v16.11]")
    subtitle_speed: float = Field(default=0.0, ge=-0.10, le=0.15, description="ÀÚ¸· ¼Óµµ Á¶Á¤ -0.10(10%´À¸°)~+0.15(15%ºü¸§) [v16.11]")
    title: Optional[str] = Field(None, description="½æ³×ÀÏ¿¡ Ç¥½ÃÇÒ Á¦¸ñ")
    subtitle_text: Optional[str] = Field(None, description="¹ÂÁ÷ºñµð¿À ÀÚ¸· ÅØ½ºÆ®")
    audio_url: Optional[str] = Field(None, description="TTS ¿Àµð¿À °æ·Î (Àý´ë°æ·Î ¶Ç´Â /data/tmp/...)")
    output_filename: Optional[str] = Field(None, description="Ãâ·Â ÆÄÀÏ¸í (±âº»: job_id.mp4)")
    transition: str = Field(default="fade", description="Å¬¸³ ÀüÈ¯ È¿°ú")
    scenes: Optional[list] = Field(None, description="¾À ¸ñ·Ï (¾øÀ¸¸é scenes.json ·Îµå)")


class JobInfo(BaseModel):
    """ÀÛ¾÷ »óÅÂ Á¤º¸"""
    job_id: str
    status: JobStatus
    progress: float = Field(default=0.0, ge=0.0, le=100.0)
    output_files: Dict[str, str] = Field(default_factory=dict)
    error: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    duration_seconds: Optional[float] = None


class AssetsSearchResponse(BaseModel):
    """ÀÚ»ê °Ë»ö ÀÀ´ä"""
    job_id: str
    status: str
    scenes: List[Scene]
    downloaded_count: int
    total_count: int


class VideoCreateResponse(BaseModel):
    """¿µ»ó »ý¼º ÀÀ´ä"""
    success: bool
    job_id: str
    status: str
    output_files: Dict[str, str] = Field(default_factory=dict)
    duration_seconds: Optional[float] = None
    error: Optional[str] = None


# ============================================================================
# È¯°æ º¯¼ö ¹× °æ·Î ¼³Á¤
# ============================================================================
PEXELS_API_KEY = os.getenv("PEXELS_API_KEY", "")
PIXABAY_API_KEY = os.getenv("PIXABAY_API_KEY", "")
LF_API_KEY = os.getenv("LF_API_KEY", "")

STORYBLOCKS_PRIVATE_KEY = os.getenv("STORYBLOCKS_PRIVATE_KEY", "")
STORYBLOCKS_PUBLIC_KEY  = os.getenv("STORYBLOCKS_PUBLIC_KEY", "")
_ASSET_CACHE_DB = Path("/data/jobs/asset_cache.db")
# [v15.66.0] °øÅë API Key °ËÁõ Depends ÇÔ¼ö
def verify_api_key(x_lf_api_key: str = Header(None, alias="X-LF-API-Key")):
    """X-LF-API-Key Çì´õ °ËÁõ. Å° ¹Ì¼³Á¤ È¯°æ¿¡¼­´Â Åë°ú."""
    if LF_API_KEY and x_lf_api_key != LF_API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API Key")
    return x_lf_api_key or ""

# µ¥ÀÌÅÍ µð·ºÅä¸® ¼³Á¤
BASE_DATA_DIR = Path("/data")
JOBS_DIR = BASE_DATA_DIR / "jobs"
TMP_DIR = BASE_DATA_DIR / "tmp"
OUTPUT_DIR = BASE_DATA_DIR / "output"
BGM_DIR = BASE_DATA_DIR / "bgm"

# Ãâ·Â µð·ºÅä¸® ±¸ºÐ
LONGFORM_DIR = OUTPUT_DIR / "longform"
SHORTS_DIR = OUTPUT_DIR / "shorts"
THUMBNAILS_DIR = OUTPUT_DIR / "thumbnails"

# ¦¡¦¡¦¡ ±¹°¡¸í ¡æ ±¹±â ÀÌ¸ðÁö ¸ÅÇÎ v15.67.0 ¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡
_COUNTRY_FLAG_MAP: dict = {
    "¹Ì±¹": "????", "¹ÌÇÕÁß±¹": "????", "¾Æ¸Þ¸®Ä«": "????", "usa": "????", "us ": "????",
    "Áß±¹": "????", "ÁßÈ­ÀÎ¹Î°øÈ­±¹": "????", "Â÷ÀÌ³ª": "????", "china": "????",
    "ÀÏº»": "????", "ÀÏº»±¹": "????", "japan": "????",
    "ÇÑ±¹": "????", "´ëÇÑ¹Î±¹": "????", "³²ÇÑ": "????", "korea": "????",
    "¿µ±¹": "????", "uk": "????", "britain": "????",
    "µ¶ÀÏ": "????", "germany": "????",
    "ÇÁ¶û½º": "????", "france": "????",
    "·¯½Ã¾Æ": "????", "russia": "????",
    "Ä³³ª´Ù": "????", "canada": "????",
    "ÀÎµµ": "????", "india": "????",
    "È£ÁÖ": "????", "australia": "????",
    "À¯·´": "????", "À¯·´¿¬ÇÕ": "????",
    "ÀÌ½º¶ó¿¤": "????",
    "ºÏÇÑ": "????",
    "´ë¸¸": "????",
    "ÀÌÅ»¸®¾Æ": "????",
    "½ºÆäÀÎ": "????",
    "ºê¶óÁú": "????",
    "»ç¿ìµð": "????", "»ç¿ìµð¾Æ¶óºñ¾Æ": "????",
    "½Ì°¡Æ÷¸£": "????",
    "¿ìÅ©¶óÀÌ³ª": "????",
    "½º¿þµ§": "????",
}

def detect_countries_in_text(text: str) -> list:
    """ÅØ½ºÆ®¿¡¼­ ±¹°¡ °¨Áö ¡æ ±¹±â ÀÌ¸ðÁö ¸®½ºÆ® (Áßº¹ Á¦°Å, ¼ø¼­ À¯Áö)"""
    found, seen = [], set()
    tl = text.lower()
    for name, flag in _COUNTRY_FLAG_MAP.items():
        if name.lower() in tl and flag not in seen:
            found.append(flag)
            seen.add(flag)
    return found

def inject_flags_in_word(word: str) -> str:
    """´Ü¾î¿¡ ±¹°¡¸í Æ÷ÇÔ ½Ã ±¹±â ÀÌ¸ðÁö ¾Õ¿¡ »ðÀÔ"""
    import re as _re2
    for name, flag in _COUNTRY_FLAG_MAP.items():
        if name.lower() in word.lower() and flag not in word:
            word = _re2.sub(
                _re2.escape(name), flag + name, word, flags=_re2.IGNORECASE, count=1
            )
            break  # ´Ü¾î 1°³¿¡ ÀÌ¸ðÁö 1°³¸¸
    return word

COMPLETE_DIR = BASE_DATA_DIR / "complete"

# µð·ºÅä¸® »ý¼º
for directory in [JOBS_DIR, TMP_DIR, OUTPUT_DIR, LONGFORM_DIR, SHORTS_DIR, THUMBNAILS_DIR, BGM_DIR, COMPLETE_DIR, COMPLETE_DIR / 'longform', COMPLETE_DIR / 'shorts', COMPLETE_DIR / 'thumbnails']:
    directory.mkdir(parents=True, exist_ok=True)

logger.info(f"µ¥ÀÌÅÍ µð·ºÅä¸® ÃÊ±âÈ­ ¿Ï·á: {BASE_DATA_DIR}")


# ============================================================================
# Disk space guard — 400GB 방어
# ============================================================================

import shutil as _shutil_disk

def check_disk_space(min_free_gb: float = 50.0) -> dict:
    """E: 드라이브(또는 /data 볼륨) 여유 공간 체크."""
    try:
        usage = _shutil_disk.disk_usage(str(BASE_DATA_DIR))
        free_gb = usage.free / 1_073_741_824
        total_gb = usage.total / 1_073_741_824
        used_gb = usage.used / 1_073_741_824
        ok = free_gb >= min_free_gb
        if not ok:
            logger.warning(f"[DISK] 경고: 여유 공간 {free_gb:.1f}GB < {min_free_gb}GB 임계값!")
        return {"free_gb": round(free_gb, 2), "used_gb": round(used_gb, 2),
                "total_gb": round(total_gb, 2), "ok": ok}
    except Exception as _de:
        logger.debug(f"[DISK] 체크 실패: {_de}")
        return {"free_gb": -1, "used_gb": -1, "total_gb": -1, "ok": True}


def cleanup_job_tmp(job_id: str) -> int:
    """잡 완료/실패 후 /data/tmp/{job_id}* 임시 파일 삭제. 반환값: 삭제된 파일 수."""
    removed = 0
    try:
        # 1. TMP_DIR/{job_id}.mp3, {job_id}_timestamps.json, {job_id}.ass, {job_id}.srt
        for pattern in [f"{job_id}.mp3", f"{job_id}_timestamps.json",
                         f"{job_id}.ass", f"{job_id}.srt", f"{job_id}.wav"]:
            p = TMP_DIR / pattern
            if p.exists():
                p.unlink(missing_ok=True)
                removed += 1
        # 2. TMP_DIR/{job_id}/ 서브디렉토리 전체
        job_tmp_dir = TMP_DIR / job_id
        if job_tmp_dir.exists() and job_tmp_dir.is_dir():
            count = len(list(job_tmp_dir.rglob("*")))
            import shutil as _sh
            _sh.rmtree(job_tmp_dir, ignore_errors=True)
            removed += count
        if removed:
            logger.info(f"[CLEANUP] {job_id}: tmp {removed}개 파일 삭제 완료")
    except Exception as _ce:
        logger.warning(f"[CLEANUP] {job_id} tmp 정리 실패 (무시): {_ce}")
    return removed


def auto_purge_old_tmp(max_age_hours: int = 24) -> int:
    """TMP_DIR 내 max_age_hours 이상 된 파일/디렉토리 자동 삭제."""
    import time
    removed = 0
    cutoff = time.time() - max_age_hours * 3600
    try:
        for item in TMP_DIR.iterdir():
            try:
                if item.stat().st_mtime < cutoff:
                    if item.is_dir():
                        import shutil as _sh2
                        count = len(list(item.rglob("*")))
                        _sh2.rmtree(item, ignore_errors=True)
                        removed += count
                    else:
                        item.unlink(missing_ok=True)
                        removed += 1
            except Exception:
                pass
    except Exception as _pe:
        logger.debug(f"[PURGE] old tmp 정리 실패: {_pe}")
    if removed:
        logger.info(f"[PURGE] {max_age_hours}h 초과 tmp 파일 {removed}개 삭제")
    return removed


# 시작 시 24시간 이상 된 tmp 파일 자동 정리
try:
    _purged = auto_purge_old_tmp(max_age_hours=24)
    _disk = check_disk_space(min_free_gb=50.0)
    logger.info(f"[STARTUP] 디스크: {_disk['free_gb']:.1f}GB 여유 / purge {_purged}개")
except Exception:
    pass


# ============================================================================
# FastAPI ¾Û ÃÊ±âÈ­
# ============================================================================
VERSION = "16.20.0"  # [v16.20] fontsdir L4715 + asyncio.ensure_future fix  # module-level — used in /health and status endpoints

app = FastAPI(
    title="LongForm Factory - FFmpeg Worker",
    description="·ÕÆû/¼ôÆû ÀÚµ¿È­ ¿µ»ó Á¦ÀÛ ¼­ºñ½º",
    version=VERSION,
)



# ==================== CORS (ºê¶ó¿ìÀú UI Á÷Á¢ È£Ãâ Çã¿ë) ====================
from fastapi.middleware.cors import CORSMiddleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)
# µ¿½Ã ¿µ»ó »ý¼º Á¦ÇÑ ? Redis lock ¿ì¼±, ¾øÀ¸¸é global fallback
_CURRENT_JOB: Optional[str] = None

# ÀÛ¾÷ »óÅÂ ÀúÀå¼Ò (ÀÎ¸Þ¸ð¸® + Redis ÀÌÁß)
jobs: Dict[str, JobInfo] = {}

# ¦¡¦¡ Redis Å¬¶óÀÌ¾ðÆ® (¼±ÅÃÀû)
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
            logger.info("Redis ¿¬°á ¼º°ø")
        except Exception as _re:
            logger.warning(f"Redis ¹Ì¿¬°á (ÀÎ¸Þ¸ð¸® fallback): {_re}")
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
        logger.debug(f"Redis ÀúÀå ½ÇÆÐ(¹«½Ã): {_e}")

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
# ÇïÆÛ ÇÔ¼öµé
# ============================================================================

async def update_job_status(
    job_id: str,
    status: JobStatus,
    progress: float = None,
    error: str = None,
    output_files: Dict[str, str] = None,
    duration_seconds: float = None
) -> None:
    """ÀÛ¾÷ »óÅÂ ¾÷µ¥ÀÌÆ®"""
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
    
    logger.info(f"ÀÛ¾÷ »óÅÂ ¾÷µ¥ÀÌÆ®: {job_id} -> {status.value} (ÁøÇà·ü: {jobs[job_id].progress}%)")


# [AL-2+AY] Pexels cache with 1h TTL + job diversification
_PEXELS_CACHE: dict = {}  # key -> (timestamp, data)
_PEXELS_CACHE_MAX = 64
_PEXELS_CACHE_TTL = 3600  # 1 hour

# [AY-C] Global seen URLs ? persist across jobs (last 300)
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
    global _GLOBAL_SEEN_URLS  # [v15.93] UnboundLocalError ¹æÁö
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
    """[BK] ´Ù´Ü¾î phrase °Ë»ö °á°ú ºÎÁ· ½Ã ÀÚµ¿ ´ÜÃà Àç°Ë»ö.
    - full phrase ¡æ °á°ú < 3°³¸é ¾Õ 3´Ü¾î ¡æ ¿©ÀüÈ÷ ºÎÁ·ÇÏ¸é ¾Õ 2´Ü¾î·Î fallback.
    """
    if not keyword:
        return []
    words = keyword.split()
    # 1Â÷: ¿øº» phrase
    results = await _get_pexels_videos_raw(keyword, per_page)
    if len(results) >= 3 or len(words) <= 2:
        return results
    # 2Â÷: ¾Õ 3´Ü¾î
    if len(words) > 3:
        short3 = " ".join(words[:3])
        logger.info(f"[BK] Pexels °á°ú ºÎÁ· ({len(results)}) ? '{short3}'·Î Àç°Ë»ö")
        r3 = await _get_pexels_videos_raw(short3, per_page)
        if len(r3) > len(results):
            results = r3
    if len(results) >= 3 or len(words) < 2:
        return results
    # 3Â÷: ¾Õ 2´Ü¾î
    short2 = " ".join(words[:2])
    logger.info(f"[BK] ¿©ÀüÈ÷ ºÎÁ· ({len(results)}) ? '{short2}'·Î Àç°Ë»ö")
    r2 = await _get_pexels_videos_raw(short2, per_page)
    return r2 if len(r2) > len(results) else results


async def _get_pexels_videos_raw(keyword: str, per_page: int = 5) -> List[Dict[str, Any]]:
    """Pexels API¿¡¼­ ¿µ»ó °Ë»ö"""
    if not PEXELS_API_KEY:
        logger.warning("Pexels API Å°°¡ ¾ø½À´Ï´Ù")
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
            logger.info(f"Pexels¿¡¼­ '{keyword}' °Ë»ö: {len(videos)}°³ °á°ú")
            return videos
    except Exception as e:
        logger.error(f"Pexels °Ë»ö ¿À·ù ({keyword}): {e}")
        return []


async def get_pixabay_videos(keyword: str, per_page: int = 5) -> List[Dict[str, Any]]:
    """Pixabay API¿¡¼­ ¿µ»ó °Ë»ö"""
    if not PIXABAY_API_KEY:
        logger.warning("Pixabay API Å°°¡ ¾ø½À´Ï´Ù")
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
            logger.info(f"Pixabay¿¡¼­ '{keyword}' °Ë»ö: {len(videos)}°³ °á°ú")
            return videos
    except Exception as e:
        logger.error(f"Pixabay °Ë»ö ¿À·ù ({keyword}): {e}")
        return []


# ¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡
# [PATCH J / v15.83] Storyblocks + SQLite Ä³½Ã
# ¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡
import hmac as _hmac, hashlib as _hashlib, sqlite3 as _sqlite3, time as _time_j


# ¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡
# [PATCH V / v15.87] Coverr.co + Mixkit ¹«·á ½ºÅå
# ¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡
async def get_coverr_videos(keyword: str, per_page: int = 5) -> List[Dict[str, Any]]:
    """Coverr.co - API Å° ÇÊ¿ä, ºñÈ°¼ºÈ­ [PATCH V-fix]"""
    return []



async def get_mixkit_videos(keyword: str, per_page: int = 5) -> List[Dict[str, Any]]:
    """Mixkit - °ø°³ API ¾øÀ½, ºñÈ°¼ºÈ­ [PATCH V-fix]"""
    return []



def _init_asset_cache():
    """SQLite ÀÚ»ê Ä³½Ã ÃÊ±âÈ­."""
    try:
        _ASSET_CACHE_DB.parent.mkdir(parents=True, exist_ok=True)
        conn = _sqlite3.connect(str(_ASSET_CACHE_DB))
        conn.execute("""CREATE TABLE IF NOT EXISTS asset_cache (
            keyword TEXT, url TEXT PRIMARY KEY,
            local_path TEXT, ts INTEGER
        )""")
        conn.commit(); conn.close()
    except Exception as _e:
        logger.warning(f"[J] SQLite ÃÊ±âÈ­ ½ÇÆÐ: {_e}")

def _cache_lookup(keyword: str) -> Optional[str]:
    """Ä³½Ã¿¡¼­ ·ÎÄÃ °æ·Î ¹ÝÈ¯ (ÆÄÀÏ Á¸Àç ½Ã¸¸)."""
    try:
        conn = _sqlite3.connect(str(_ASSET_CACHE_DB))
        cur = conn.execute(
            "SELECT local_path FROM asset_cache WHERE keyword=? ORDER BY ts DESC LIMIT 5",
            (keyword.lower().strip(),))
        rows = cur.fetchall(); conn.close()
        for (lp,) in rows:
            if lp and Path(lp).exists() and Path(lp).stat().st_size > 4096:
                return lp
    except Exception:
        pass
    return None

def _cache_write(keyword: str, url: str, local_path: str):
    """ÀÚ»ê Ä³½Ã¿¡ ±â·Ï."""
    try:
        conn = _sqlite3.connect(str(_ASSET_CACHE_DB))
        conn.execute(
            "INSERT OR REPLACE INTO asset_cache (keyword,url,local_path,ts) VALUES (?,?,?,?)",
            (keyword.lower().strip(), url, local_path, int(_time_j.time())))
        conn.commit(); conn.close()
    except Exception:
        pass

async def get_storyblocks_videos(keyword: str, per_page: int = 5) -> List[Dict[str, Any]]:
    """[PATCH J] Storyblocks API °Ë»ö (HMAC ÀÎÁõ ¶Ç´Â API Key ¹æ½Ä)."""
    if not STORYBLOCKS_PRIVATE_KEY or not STORYBLOCKS_PUBLIC_KEY:
        return []
    try:
        import time as _t
        expires = str(int(_t.time()) + 30)
        msg = (STORYBLOCKS_PRIVATE_KEY + STORYBLOCKS_PUBLIC_KEY + expires).encode()
        sig = _hmac.new(STORYBLOCKS_PRIVATE_KEY.encode(), msg, _hashlib.sha256).hexdigest()
        params = {
            "project_id": STORYBLOCKS_PUBLIC_KEY,
            "user_id": "longform_factory",
            "username": "longform_factory",
            "expires": expires,
            "hmac": sig,
            "keywords": keyword,
            "results_per_page": per_page,
            "content_type": "footage",
        }
        async with httpx.AsyncClient(timeout=8.0) as client:
            resp = await client.get(
                "https://api.storyblocks.com/api/v2/videos/search", params=params)
            if resp.status_code != 200:
                logger.warning(f"[Storyblocks] HTTP {resp.status_code}: {resp.text[:100]}")
                return []
            data = resp.json()
            results = data.get("results", [])
            videos = []
            for item in results[:per_page]:
                preview = item.get("preview_url") or item.get("thumbnail_url") or ""
                dl_url  = item.get("download_url") or preview
                if dl_url:
                    videos.append({
                        "source": "storyblocks",
                        "url": dl_url,
                        "width": item.get("frame_width", 1920),
                        "height": item.get("frame_height", 1080),
                        "duration": item.get("duration", 10),
                    })
            logger.info(f"[Storyblocks] '{keyword}': {len(videos)}°³")
            return videos
    except Exception as e:
        logger.warning(f"[Storyblocks] ¿À·ù: {e}")
        return []

_init_asset_cache()

def select_best_video(pexels_videos: List[Dict], pixabay_videos: List[Dict],
                       scene_index: int = 0,
                       exclude_urls: set = None,
                       query_keyword: str = "") -> Optional[str]:
    """[O] ¾À ÀÎµ¦½º ¶ó¿îµå·ÎºóÀ¸·Î ´Ù¾çÇÑ ¿µ»ó ¼±ÅÃ (°°Àº Å°¿öµå¶óµµ ´Ù¸¥ °á°ú).
    ÇØ»óµµ »óÀ§ ÈÄº¸µé Áß¿¡¼­ scene_index ·Î ¼øÈ¯."""
    candidates = []
    
    # [AN-3] Filter videos whose URL/user suggests text overlay content
    _TEXT_BLACKLIST = ("whiteboard", "handwriting", "typography", "text", "chalkboard",
                       "infographic", "presentation", "slide", "sketch", "diagram",
                       "concept", "mindmap", "notes", "writing", "drawing")
    # [BN] MARKER v1
    # ÇÑ±¹ ÄÁÅÙÃ÷¿¡¼­ ¹èÁ¦ÇÒ ¼­¾ç ½Äº°ÀÚ (query_keyword ¿¡ asian/korean/seoul ÀÌ¸é ÀÚµ¿ Àû¿ë)
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
        # [BN+BN2] ÇÑ±¹ ÁÖÁ¦ + ¼­¾ç ±¹±â/Àå¼Ò ¡æ °ÅÀý
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

    # Pexels ¿µ»ó Ã³¸®
    for video in pexels_videos:
        if _has_text_indicator(video):
            continue  # [AN] MARKER v1
        video_files = video.get("video_files", [])
        if video_files:
            # °¡Àå ³ôÀº ÇØ»óµµÀÇ ÆÄÀÏ ¼±ÅÃ
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
    
    # Pixabay ¿µ»ó Ã³¸®
    for video in pixabay_videos:
        video_files = video.get("videos", {})
        # large, medium, small Áß¿¡¼­ large ¼±ÅÃ
        if "large" in video_files:
            url = video_files["large"].get("url")
            if url:
                candidates.append({
                    "url": url,
                    "width": video_files["large"].get("width", 0),
                    "height": video_files["large"].get("height", 0)
                })
    
    if candidates:
        # [AW-4+BG-2] 4K ¿ì¼± + Å°¿öµå ¸ÅÄª ÀçÁ¤·Ä
        # query Å°¿öµåÀÇ ¸í»ç¸¦ ÃßÃâÇØ¼­ URL/page_url ¿¡ Æ÷ÇÔµÈ ÈÄº¸ ¿ì¼±
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
            # ÇØ»óµµ ½ºÄÚ¾î
            res_score = 0
            if h >= 2000: res_score = pixels + 10_000_000
            elif h >= 1400: res_score = pixels + 5_000_000
            elif h >= 1000: res_score = pixels
            else: res_score = pixels - 5_000_000
            # [BG-2] URL¡¤user ÀÌ¸§¿¡ Å°¿öµå ¸í»ç Æ÷ÇÔµÇ¸é Å« º¸³Ê½º
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
            f"¿µ»ó ¼±ÅÃ: {picked['width']}x{picked['height']} "
            f"(idx={scene_index}, pool={len(pool)}/{len(candidates)})"
        )
        return picked["url"]

    return None


async def download_video(video_url: str, output_path: Path, timeout: float = 120.0, max_duration: float = 60.0) -> bool:
    """¿µ»ó ´Ù¿î·Îµå ? ffmpegÀ¸·Î Á÷Á¢ ´Ù¿î·Îµå + 60ÃÊ ÀÚµ¿ Æ®¸®¹Ö (908MB ¹æÁö)"""
    try:
        logger.info(f"¿µ»ó ´Ù¿î·Îµå ½ÃÀÛ (ÃÖ´ë {max_duration}ÃÊ): {video_url} -> {output_path}")
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # 1Â÷: ffmpegÀ¸·Î ½ºÆ®¸² ´Ù¿î·Îµå + Æ®¸®¹Ö
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
        result = await asyncio.to_thread(subprocess.run, cmd, capture_output=True, text=True, timeout=180)
        if result.returncode == 0 and output_path.exists() and output_path.stat().st_size > 10000:
            file_size = output_path.stat().st_size
            logger.info(f"¿µ»ó ´Ù¿î·Îµå ¿Ï·á (ffmpeg): {output_path} ({file_size/(1024*1024):.2f}MB)")
            return True

        # 2Â÷ fallback: httpx ½ºÆ®¸®¹Ö (ÃÖ´ë 30MB)
        logger.warning("ffmpeg ´Ù¿î·Îµå ½ÇÆÐ ? httpx fallback")
        async with httpx.AsyncClient(timeout=timeout) as client:
            async with client.stream("GET", video_url) as response:
                response.raise_for_status()
                downloaded = 0
                max_bytes = 30 * 1024 * 1024  # 30MB Á¦ÇÑ
                async with aiofiles.open(output_path, "wb") as f:
                    async for chunk in response.aiter_bytes(chunk_size=65536):
                        await f.write(chunk)
                        downloaded += len(chunk)
                        if downloaded >= max_bytes:
                            logger.info("30MB Á¦ÇÑ µµ´Þ ? ´Ù¿î·Îµå Áß´Ü")
                            break
        file_size = output_path.stat().st_size if output_path.exists() else 0
        logger.info(f"¿µ»ó ´Ù¿î·Îµå ¿Ï·á (fallback): {output_path} ({file_size/(1024*1024):.2f}MB)")
        return file_size > 10000
    except Exception as e:
        logger.error(f"¿µ»ó ´Ù¿î·Îµå ½ÇÆÐ: {e}")
        return False


# ========== [v15.69] Å°¿öµå Sanitizer =========================================
_CAMERA_DIRECTIVES = {
    "wide shot","close up","close-up","side angle","panning","zoom in","zoom out",
    "aerial shot","tracking shot","dolly shot","tilt","crane shot","establishing shot",
    "cutaway","overhead","bird eye","bird's eye","bird's-eye","low angle","high angle",
    "slow zoom","fast cut","handheld","steadicam","bokeh","depth of field",
    "wide angle","tight shot","medium shot","long shot","extreme close up","two shot",
    "wide","angle","shot","aerial","zoom","pan","cutaway","handheld",
}

_KO_STOPWORDS = {
    "ÀÌ","±×","Àú","ÀÇ","°¡","Àº","´Â","À»","¸¦","¿¡","¿¡¼­","·Î","À¸·Î",
    "¿Í","°ú","µµ","¸¸","ÀÌ´Ù","ÀÖ´Ù","ÇÏ´Ù","µÇ´Ù","¾Ê´Ù","¶§","ÈÄ","Àü","Áß",
    "¶ÇÇÑ","µû¶ó¼­","±×¸®°í","ÇÏÁö¸¸","±×·¯³ª","±×·¡¼­","Áï","°ð","ÀÌÈÄ",
}

def _is_camera_directive(kw: str) -> bool:
    """Ä«¸Þ¶ó ¹æÇâ/±â¹ý Å°¿öµå ÆÇº°"""
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
    """[v15.69] Ä«¸Þ¶ó µð·ºÆ¼ºê/ºó Å°¿öµå¸¦ ³ª·¹ÀÌ¼Ç ±â¹Ý Å°¿öµå·Î º¹±¸"""
    if not _is_camera_directive(kw):
        return kw
    if narration:
        import re as _re
        ko_words = _re.findall(r'[°¡-ÆR]{2,}', narration)
        en_words = _re.findall(r'[A-Za-z]{3,}', narration)
        useful = [w for w in ko_words if w not in _KO_STOPWORDS][:3]
        if en_words:
            useful = en_words[:3] + useful[:1]
        if useful:
            return " ".join(useful[:3]) + " footage"
    return fallback if fallback else "business technology people"


# ========== END sanitizer ====================================================

def _get_topic_fallback(keyword: str, topic_hint: str = "") -> str:
    """[v15.75.0] ÅäÇÈ Ä«Å×°í¸® ±â¹Ý Æú¹é Äõ¸®."""
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


def _ko_narration_to_visual_query(text: str, orig_keyword: str = "", topic: str = "") -> str:
    """[v16.4] ÇÑ±¹¾î ³ª·¹ÀÌ¼Ç ¼¼±×¸ÕÆ® ¡æ ¿µ¾î ½Ã°¢ °Ë»ö Äõ¸® º¯È¯.
    ws_ ¾ÀÀÇ keyword¸¦ ³ª·¹ÀÌ¼Ç ³»¿ë°ú ¸ÅÄª½ÃÅ°±â À§ÇØ »ç¿ë."""
    import re as _re
    if not text:
        return orig_keyword or "technology people"

    # 1. ¿µ¾î ´Ü¾î ¸ÕÀú ÃßÃâ (AI, ChatGPT µî)
    en_words = _re.findall(r"\b[A-Za-z][A-Za-z0-9]{2,}\b", text)
    en_words = [w for w in en_words if w.lower() not in ("the","and","for","this","that","with","from","are","was","were","has","have","will","can","its","our","not")]
    if en_words:
        return " ".join(en_words[:3]) + " footage"

    # 2. ÇÑ±¹¾î ¡æ ¿µ¾î ½Ã°¢ Äõ¸® ¸ÅÇÎ
    _KO_VISUAL = [
        (["ÀÎ°øÁö´É","AI","¸Ó½Å·¯´×","µö·¯´×","½Å°æ¸Á","¾Ë°í¸®Áò"], "artificial intelligence AI technology"),
        (["·Îº¿","ÀÚµ¿È­","µå·Ð","±â°è"], "robot automation machine"),
        (["ÀÇ·á","º´¿ø","ÀÇ»ç","È¯ÀÚ","Çï½ºÄÉ¾î","¼ö¼ú"], "hospital medical doctor patient"),
        (["±³À°","ÇÐ±³","ÇÐ»ý","ÇÐ½À","°­ÀÇ","¼ö¾÷"], "education classroom student learning"),
        (["ÀÏÀÚ¸®","Á÷¾÷","Ãë¾÷","°í¿ë","½Ç¾÷","±Ù·ÎÀÚ"], "job employment office worker career"),
        (["°æÁ¦","±ÝÀ¶","ÁÖ½Ä","½ÃÀå","ÅõÀÚ","µ·"], "business finance stock market"),
        (["µ¥ÀÌÅÍ","ºÐ¼®","ºòµ¥ÀÌÅÍ","Åë°è"], "data analytics computer screen"),
        (["½º¸¶Æ®Æù","¸ð¹ÙÀÏ","¾Û","¼Ò¼È¹Ìµð¾î"], "smartphone mobile app social media"),
        (["°øÀå","Á¦Á¶","»ý»ê","»ê¾÷"], "factory manufacturing industrial"),
        (["È¯°æ","±âÈÄ","¿¡³ÊÁö","Åº¼Ò"], "environment nature green energy"),
        (["¼­¿ï","ÇÑ±¹","µµ½Ã","°Å¸®"], "seoul korea city urban street"),
        (["¹Ì·¡","Çõ½Å","Ã·´Ü"], "future technology innovation"),
        (["Ã¢¾÷","½ºÅ¸Æ®¾÷","º¥Ã³","±â¾÷°¡"], "startup entrepreneur business meeting"),
        (["¿¬±¸","°³¹ß","½ÇÇè","°úÇÐ"], "laboratory research scientist"),
        (["Á¤Ä¡","Á¤ºÎ","¼±°Å","Á¤Ã¥"], "government building politics"),
        (["±º»ç","¾Èº¸","¹æ¾î"], "military defense security"),
        (["¿ìÁÖ","À§¼º","·ÎÄÏ"], "space satellite rocket launch"),
        (["À½½Ä","¿ä¸®","½Ä´ç"], "food restaurant cooking"),
        (["°Ç°­","¿îµ¿","½ºÆ÷Ã÷"], "health fitness exercise sport"),
        (["¿¹¼ú","¹®È­","À½¾Ç"], "art culture music performance"),
        (["ºÎµ¿»ê","°Ç¹°","°Ç¼³"], "building construction real estate"),
        (["±³Åë","ÀÚµ¿Â÷","Â÷·®"], "car vehicle traffic road"),
        (["Åë½Å","³×Æ®¿öÅ©","ÀÎÅÍ³Ý"], "network internet communication"),
        (["¹ÝµµÃ¼","Ä¨","ÀüÀÚ"], "semiconductor chip electronics"),
        (["»ç¶÷µé","È¸ÀÇ","Çù·Â"], "people meeting collaboration office"),
        (["±Û·Î¹ú","¼¼°è","±¹Á¦"], "global world international"),
        (["º¯È­","¼ºÀå","¹ßÀü"], "growth change progress"),
        (["¼ÒºñÀÚ","°í°´","¼­ºñ½º"], "customer service consumer"),
    ]
    text_clean = text.replace(" ","")
    for ko_words, en_query in _KO_VISUAL:
        if any(kw in text_clean for kw in ko_words):
            return en_query

    # 3. orig_keyword°¡ ¿µ¾î¸é ±×´ë·Î
    if orig_keyword and _re.search(r"[A-Za-z]{3,}", orig_keyword):
        return orig_keyword

    # 4. ÅäÇÈ ±â¹Ý Æú¹é
    return _get_topic_fallback(orig_keyword or text, topic)


# ============================================================
# [v15.69] Kling T2V ÅëÇÕ
# ============================================================
_KLING_ACCESS_KEY = os.getenv("KLING_ACCESS_KEY", "")
_KLING_SECRET_KEY = os.getenv("KLING_SECRET_KEY", "")
_KLING_BASE_URL = "https://api.klingai.com"
_AI_VIDEO_ENABLED = os.getenv("AI_VIDEO_ENABLED", "false").lower() in ("1","true","yes")
_AI_VIDEO_PROVIDER = os.getenv("AI_VIDEO_PROVIDER", "").lower()

def _kling_jwt() -> str:
    """HS256 JWT »ý¼º (30ºÐ À¯È¿)"""
    try:
        import jwt as _jwt
        now = int(time.time())
        payload = {"iss": _KLING_ACCESS_KEY, "exp": now + 1800, "nbf": now - 5}
        token = _jwt.encode(payload, _KLING_SECRET_KEY, algorithm="HS256")
        return token if isinstance(token, str) else token.decode("utf-8")
    except Exception as e:
        logger.warning(f"[Kling] JWT »ý¼º ½ÇÆÐ: {e}")
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
    """[v15.69] Kling T2V API·Î ¾À ¿µ»ó »ý¼º ¡æ output_path¿¡ ÀúÀå"""
    if not _KLING_ACCESS_KEY or not _KLING_SECRET_KEY:
        logger.warning("[Kling] API Å° ¹Ì¼³Á¤ ? ½ºÅµ")
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
                logger.warning(f"[Kling] »ý¼º ½ÇÆÐ {resp.status_code}: {resp.text[:200]}")
                return False
            data = resp.json()
        task_id = (data.get("data") or {}).get("task_id", "") or data.get("task_id", "")
        if not task_id:
            logger.warning(f"[Kling] task_id ¾øÀ½: {data}")
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
                    logger.warning(f"[Kling] ½ÇÆÐ: {td}")
                    return False
            except Exception as pe:
                logger.warning(f"[Kling] Æú¸µ ¿À·ù: {pe}")
        if not video_url:
            logger.warning(f"[Kling] video_url ¾øÀ½ (waited={waited:.0f}s)")
            return False
        logger.info(f"[Kling] ´Ù¿î·Îµå: {video_url[:80]}")
        async with httpx.AsyncClient(timeout=120.0, follow_redirects=True) as dl:
            r = await dl.get(video_url)
            if r.status_code == 200:
                output_path.write_bytes(r.content)
                sz = output_path.stat().st_size
                logger.info(f"[Kling] ? ÀúÀå: {output_path} ({sz//1024}KB)")
                return sz > 4096
            logger.warning(f"[Kling] ´Ù¿î·Îµå ½ÇÆÐ {r.status_code}")
            return False
    except Exception as e:
        logger.warning(f"[Kling] ¿¹¿Ü: {e}", exc_info=True)
        return False


# ¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡
# [PATCH K / v15.83] WAN 2.6 T2V via PiAPI
# ½ºÅå ½ÇÆÐ ½Ã \.08/sec ¹«¿öÅÍ¸¶Å© B-roll »ý¼º
# ¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡
_WAN_API_KEY = os.getenv("PIAPI_KEY", "")  # https://piapi.ai
_AIML_API_KEY = os.getenv("AIMLAPI_KEY", "")  # https://aimlapi.com ? WAN 2.6 T2V ´ë¾È
_REPLICATE_API_KEY = os.getenv("REPLICATE_API_TOKEN", "")  # https://replicate.com ? WAN 2.1 @$0.20/¿µ»ó
_WAVESPEED_API_KEY = os.getenv("WAVESPEED_API_KEY", "")  # https://wavespeed.ai ? $0.01/sec (½Å±Ô $1 ¹«·á)

async def generate_wan_video(
    prompt_en: str,
    duration: int,
    scene_id: str,
    output_path: Path,
    max_wait_sec: float = 180.0,
) -> bool:
    "'''[PATCH K] WAN 2.6 T2V (PiAPI) ? ¹«¿öÅÍ¸¶Å© B-roll »ý¼º.'''"
    if not _WAN_API_KEY:
        return False
    try:
        dur_sec = min(max(int(duration), 3), 15)
        payload = {
            "model": "Wan",
            "task_type": "wan26-txt2video",
            "input": {
                "prompt": prompt_en[:800],
                "negative_prompt": "text, watermark, subtitle, logo, blurry, shaky, low quality",
                "duration": dur_sec,
                "resolution": "720p",
            }
        }
        headers = {"x-api-key": _WAN_API_KEY, "Content-Type": "application/json"}
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post("https://api.piapi.ai/api/v1/task",
                                     json=payload, headers=headers)
            if resp.status_code not in (200, 201):
                logger.warning(f"[WAN] »ý¼º ¿äÃ» ½ÇÆÐ {resp.status_code}: {resp.text[:150]}")
                return False
            data = resp.json()
        task_id = (data.get("data") or {}).get("task_id") or data.get("task_id", "")
        if not task_id:
            logger.warning(f"[WAN] task_id ¾øÀ½: {data}")
            return False
        logger.info(f"[WAN] task_id={task_id} scene={scene_id}")
        waited, poll_interval, video_url = 0.0, 8.0, ""
        while waited < max_wait_sec:
            await asyncio.sleep(poll_interval)
            waited += poll_interval
            try:
                async with httpx.AsyncClient(timeout=20.0) as pc:
                    pr = await pc.get(f"https://api.piapi.ai/api/v1/task/{task_id}",
                                      headers={"x-api-key": _WAN_API_KEY})
                    pd = pr.json()
                st = (pd.get("data") or pd).get("status", "")
                logger.info(f"[WAN] {task_id} status={st} waited={waited:.0f}s")
                if st in ("completed", "succeed", "success"):
                    out = (pd.get("data") or pd).get("output") or {}
                    video_url = (out.get("video_url") or out.get("url") or
                                 out.get("videos", [{}])[0].get("url", "") if isinstance(out.get("videos"), list) else "")
                    break
                elif st in ("failed", "cancelled", "error"):
                    logger.warning(f"[WAN] ½ÇÆÐ: {pd}")
                    return False
            except Exception as pe:
                logger.warning(f"[WAN] Æú¸µ ¿À·ù: {pe}")
        if not video_url:
            logger.warning(f"[WAN] video_url ¾øÀ½ (waited={waited:.0f}s)")
            return False
        logger.info(f"[WAN] ´Ù¿î·Îµå: {video_url[:80]}")
        async with httpx.AsyncClient(timeout=90.0, follow_redirects=True) as dl:
            r = await dl.get(video_url)
            if r.status_code == 200:
                output_path.write_bytes(r.content)
                sz = output_path.stat().st_size
                logger.info(f"[WAN] ? ÀúÀå: {output_path} ({sz//1024}KB)")
                return sz > 4096
            logger.warning(f"[WAN] ´Ù¿î·Îµå ½ÇÆÐ {r.status_code}")
            return False
    except Exception as e:
        logger.warning(f"[WAN] ¿¹¿Ü: {e}")
        return False

# ¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡
# [PATCH L / v15.83] Replicate API ? WAN 2.1 T2V
# WAN 2.1 1.3B = .20/¿µ»ó (ÃÖÀú°¡ À¯·á AI B-roll)
# ¹«·á Try-for-free ÄÃ·º¼ÇÀ¸·Î ÃÊ±â ¹«°ú±Ý °¡´É
# ¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡
async def generate_replicate_video(
    prompt_en: str,
    duration: int,
    scene_id: str,
    output_path: Path,
    model: str = "wavespeedai/wan-2.1-t2v-480p",
    max_wait_sec: float = 180.0,
) -> bool:
    "'''[PATCH L] Replicate T2V ? WAN 2.1 1.3B ±âº» (.20/¿µ»ó).'''"
    if not _REPLICATE_API_KEY:
        return False
    try:
        headers = {
            "Authorization": f"Token {_REPLICATE_API_KEY}",
            "Content-Type": "application/json",
            "Prefer": "wait"
        }
        fps, frames = 16, max(int(duration) * 16, 48)
        payload = {
            "input": {
                "prompt": prompt_en[:500],
                "negative_prompt": "text, watermark, logo, blurry, shaky",
                "num_frames": frames,
                "fps": fps,
                "width": 854, "height": 480,
            }
        }
        url = f"https://api.replicate.com/v1/models/{model}/predictions"
        async with httpx.AsyncClient(timeout=40.0) as client:
            resp = await client.post(url, json=payload, headers=headers)
            if resp.status_code not in (200, 201):
                logger.warning(f"[Replicate] {resp.status_code}: {resp.text[:150]}")
                return False
            data = resp.json()
        pred_id  = data.get("id", "")
        if not pred_id:
            logger.warning(f"[Replicate] prediction id ¾øÀ½: {data}")
            return False
        logger.info(f"[Replicate] pred_id={pred_id} scene={scene_id}")
        # Prefer:wait ·Î Áï½Ã ¿Ï·áµÇ¸é output ÀÖÀ½
        output_url = ""
        if data.get("status") in ("succeeded",):
            out = data.get("output")
            output_url = out if isinstance(out, str) else (out[0] if isinstance(out, list) and out else "")
        # Æú¸µ
        waited = 0.0
        while not output_url and waited < max_wait_sec:
            await asyncio.sleep(8.0); waited += 8.0
            try:
                async with httpx.AsyncClient(timeout=20.0) as pc:
                    pr = await pc.get(f"https://api.replicate.com/v1/predictions/{pred_id}",
                                      headers={"Authorization": f"Token {_REPLICATE_API_KEY}"})
                    pd = pr.json()
                st = pd.get("status", "")
                logger.info(f"[Replicate] {pred_id} status={st} waited={waited:.0f}s")
                if st == "succeeded":
                    out = pd.get("output")
                    output_url = out if isinstance(out, str) else (out[0] if isinstance(out, list) and out else "")
                    break
                elif st in ("failed", "canceled"):
                    logger.warning(f"[Replicate] ½ÇÆÐ: {pd.get('error')}")
                    return False
            except Exception as pe:
                logger.warning(f"[Replicate] Æú¸µ ¿À·ù: {pe}")
        if not output_url:
            logger.warning(f"[Replicate] output_url ¾øÀ½ (waited={waited:.0f}s)")
            return False
        logger.info(f"[Replicate] ´Ù¿î·Îµå: {output_url[:80]}")
        async with httpx.AsyncClient(timeout=90.0, follow_redirects=True) as dl:
            r = await dl.get(output_url)
            if r.status_code == 200:
                output_path.write_bytes(r.content)
                sz = output_path.stat().st_size
                logger.info(f"[Replicate] ? {output_path} ({sz//1024}KB)")
                return sz > 4096
        return False
    except Exception as e:
        logger.warning(f"[Replicate] ¿¹¿Ü: {e}")
        return False

# ¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡
# [PATCH O / v15.83] WaveSpeedAI API
# WAN 2.2 Ultra Fast: .01/sec | ½Å±Ô  ¹«·áÅ©·¹µ÷(=20Å¬¸³)
# ¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡
async def generate_wavespeed_video(
    prompt_en: str,
    duration: int,
    scene_id: str,
    output_path: Path,
    model: str = "wavespeed-ai/wan-2.2-ultra-fast",
    max_wait_sec: float = 120.0,
) -> bool:
    if not _WAVESPEED_API_KEY:
        return False
    try:
        dur_sec = min(max(int(duration), 3), 10)
        payload = {
            "prompt": prompt_en[:600],
            "negative_prompt": "text, watermark, subtitle, logo, blurry, shaky, nsfw",
            "duration": dur_sec,
            "resolution": "720p",
            "seed": -1,
        }
        headers = {"Authorization": f"Bearer {_WAVESPEED_API_KEY}", "Content-Type": "application/json"}
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(f"https://api.wavespeed.ai/api/v3/{model}", json=payload, headers=headers)
            if resp.status_code not in (200, 201):
                logger.warning(f"[WaveSpeed] {resp.status_code}: {resp.text[:150]}")
                return False
            data = resp.json()
        req_id = (data.get("data") or {}).get("id") or data.get("id") or data.get("request_id", "")
        if not req_id:
            logger.warning(f"[WaveSpeed] request_id ¾øÀ½: {data}")
            return False
        logger.info(f"[WaveSpeed] req_id={req_id} scene={scene_id}")
        waited, poll_interval, video_url = 0.0, 5.0, ""
        while waited < max_wait_sec:
            await asyncio.sleep(poll_interval); waited += poll_interval
            try:
                async with httpx.AsyncClient(timeout=15.0) as pc:
                    pr = await pc.get(f"https://api.wavespeed.ai/api/v3/predictions/{req_id}/result",
                                      headers={"Authorization": f"Bearer {_WAVESPEED_API_KEY}"})
                    pd = pr.json()
                st = (pd.get("data") or pd).get("status", "")
                logger.info(f"[WaveSpeed] {req_id} status={st} waited={waited:.0f}s")
                if st in ("completed", "succeeded", "success"):
                    out = (pd.get("data") or pd).get("outputs") or (pd.get("data") or pd).get("output") or {}
                    if isinstance(out, list): video_url = out[0] if out else ""
                    elif isinstance(out, dict): video_url = out.get("video_url") or out.get("url", "")
                    elif isinstance(out, str): video_url = out
                    break
                elif st in ("failed", "canceled"):
                    logger.warning(f"[WaveSpeed] ½ÇÆÐ: {pd}"); return False
            except Exception as pe:
                logger.warning(f"[WaveSpeed] Æú¸µ: {pe}")
        if not video_url:
            logger.warning(f"[WaveSpeed] video_url ¾øÀ½ ({waited:.0f}s)"); return False
        async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as dl:
            r = await dl.get(video_url)
            if r.status_code == 200:
                output_path.write_bytes(r.content); sz = output_path.stat().st_size
                logger.info(f"[WaveSpeed] OK {output_path} ({sz//1024}KB)"); return sz > 4096
        return False
    except Exception as e:
        logger.warning(f"[WaveSpeed] ¿¹¿Ü: {e}"); return False




# [PATCH P / v15.84] Google Gemini Veo 2 API ? $0.12/sec (¹«·á Æ¼¾î: ¿ù 5 videos)
_GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")  # generativelanguage.googleapis.com

_OPENAI_API_KEY  = os.getenv("OPENAI_API_KEY", "")   # Sora 2 T2V
_XAI_API_KEY     = os.getenv("XAI_API_KEY", "")      # Grok Imagine Video

async def generate_veo_video(
    prompt_en: str,
    duration: int,
    scene_id: str,
    output_path: Path,
    model: str = "veo-2.0-generate-001",
    max_wait_sec: float = 300.0,
) -> bool:
    """Google Gemini Veo 2 T2V ? predictLongRunning + polling."""
    if not _GEMINI_API_KEY:
        return False
    try:
        dur = min(max(int(duration), 5), 8)  # Veo 2: 5~8ÃÊ
        base_url = "https://generativelanguage.googleapis.com/v1beta"
        headers = {
            "Content-Type": "application/json",
            "x-goog-api-key": _GEMINI_API_KEY,
        }
        payload = {
            "instances": [{"prompt": prompt_en[:480]}],
            "parameters": {
                "sampleCount": 1,
                "durationSeconds": dur,
                "aspectRatio": "16:9",
                "outputOptions": {"mimeType": "video/mp4"},
            },
        }
        async with httpx.AsyncClient(timeout=60.0) as cli:
            r = await cli.post(
                f"{base_url}/models/{model}:predictLongRunning",
                headers=headers, json=payload,
            )
        if r.status_code not in (200, 202):
            logger.warning(f"[Veo2] submit fail {r.status_code}: {r.text[:200]}")
            return False
        op = r.json()
        op_name = op.get("name", "")
        if not op_name:
            logger.warning("[Veo2] operation name ¾øÀ½")
            return False
        logger.info(f"[Veo2] operation: {op_name}")

        # polling
        waited = 0.0
        poll_interval = 8.0
        async with httpx.AsyncClient(timeout=30.0) as cli:
            while waited < max_wait_sec:
                await asyncio.sleep(poll_interval)
                waited += poll_interval
                pr = await cli.get(
                    f"{base_url}/{op_name}",
                    headers={"x-goog-api-key": _GEMINI_API_KEY},
                )
                if pr.status_code != 200:
                    continue
                pd = pr.json()
                if not pd.get("done"):
                    logger.debug(f"[Veo2] waiting... ({waited:.0f}s)")
                    continue
                # ¿Ï·á
                err = pd.get("error")
                if err:
                    logger.warning(f"[Veo2] error: {err}")
                    return False
                resp = pd.get("response", {})
                samples = resp.get("generatedSamples", [])
                if not samples:
                    # fallback: videoBytes
                    vb = resp.get("videos", [{}])[0].get("videoBytes", "")
                    if vb:
                        import base64 as _b64
                        output_path.write_bytes(_b64.b64decode(vb))
                        ok = output_path.exists() and output_path.stat().st_size > 4096
                        if ok:
                            logger.info(f"[Veo2] OK (bytes) {output_path} ({output_path.stat().st_size//1024}KB)")
                        return ok
                    logger.warning("[Veo2] generatedSamples ¾øÀ½")
                    return False
                video_info = samples[0].get("video", {})
                video_uri  = video_info.get("uri", "")
                video_bytes = video_info.get("videoBytes", "")
                if video_uri:
                    async with httpx.AsyncClient(timeout=120.0) as dl:
                        vr = await dl.get(video_uri)
                    if vr.status_code == 200:
                        output_path.write_bytes(vr.content)
                        ok = output_path.exists() and output_path.stat().st_size > 4096
                        if ok:
                            logger.info(f"[Veo2] OK {output_path} ({output_path.stat().st_size//1024}KB)")
                        return ok
                elif video_bytes:
                    import base64 as _b64
                    output_path.write_bytes(_b64.b64decode(video_bytes))
                    ok = output_path.exists() and output_path.stat().st_size > 4096
                    if ok:
                        logger.info(f"[Veo2] OK (bytes) {output_path} ({output_path.stat().st_size//1024}KB)")
                    return ok
                logger.warning("[Veo2] video_uri/videoBytes ¾øÀ½")
                return False
        logger.warning(f"[Veo2] timeout {max_wait_sec}s ÃÊ°ú")
        return False
    except Exception as e:
        logger.warning(f"[Veo2] ¿¹¿Ü: {e}")
        return False




# ¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡
# [PATCH W / v15.90] OpenAI Sora 2 T2V
# ¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡
async def generate_sora_video(
    prompt_en: str,
    duration: int,
    scene_id: str,
    output_path: Path,
    max_wait_sec: float = 360.0,
) -> bool:
    """OpenAI Sora 2 text-to-video ? /v1/video/generations API."""
    if not _OPENAI_API_KEY:
        return False
    try:
        dur = min(max(int(duration), 5), 20)  # Sora 2: 5~20ÃÊ
        headers = {
            "Authorization": f"Bearer {_OPENAI_API_KEY}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": "sora-2",
            "prompt": prompt_en[:480],
            "size": "1280x720",
            "n": 1,
            "duration": dur,
        }
        async with httpx.AsyncClient(timeout=60.0) as cli:
            r = await cli.post(
                "https://api.openai.com/v1/video/generations",
                headers=headers, json=payload,
            )
        if r.status_code not in (200, 201, 202):
            logger.warning(f"[Sora2] submit fail {r.status_code}: {r.text[:200]}")
            return False
        data = r.json()
        gen_id = data.get("id") or (data.get("data", [{}])[0].get("id", ""))
        if not gen_id:
            logger.warning(f"[Sora2] generation id ¾øÀ½: {data}")
            return False
        logger.info(f"[Sora2] »ý¼º ¿äÃ» OK, id={gen_id}")

        # polling
        waited = 0.0
        async with httpx.AsyncClient(timeout=30.0) as cli:
            while waited < max_wait_sec:
                await asyncio.sleep(8.0)
                waited += 8.0
                pr = await cli.get(
                    f"https://api.openai.com/v1/video/generations/{gen_id}",
                    headers={"Authorization": f"Bearer {_OPENAI_API_KEY}"},
                )
                if pr.status_code != 200:
                    continue
                pd = pr.json()
                status = pd.get("status", "")
                if status in ("in_progress", "queued", "pending"):
                    logger.debug(f"[Sora2] {status} ({waited:.0f}s)")
                    continue
                if status != "succeeded":
                    logger.warning(f"[Sora2] ½ÇÆÐ status={status}")
                    return False
                # ´Ù¿î·Îµå
                vid_list = pd.get("data", pd.get("videos", []))
                if not vid_list:
                    vid_list = [pd]
                video_url = vid_list[0].get("url", "")
                if not video_url:
                    # content endpoint ½Ãµµ
                    dl_r = await cli.get(
                        f"https://api.openai.com/v1/video/generations/{gen_id}/content/video",
                        headers={"Authorization": f"Bearer {_OPENAI_API_KEY}"},
                    )
                    if dl_r.status_code == 200:
                        output_path.write_bytes(dl_r.content)
                        ok = output_path.stat().st_size > 4096
                        if ok:
                            logger.info(f"[Sora2] OK {output_path} ({output_path.stat().st_size//1024}KB)")
                        return ok
                    logger.warning("[Sora2] download URL ¾øÀ½")
                    return False
                async with httpx.AsyncClient(timeout=120.0) as dl:
                    vr = await dl.get(video_url)
                if vr.status_code == 200:
                    output_path.write_bytes(vr.content)
                    ok = output_path.stat().st_size > 4096
                    if ok:
                        logger.info(f"[Sora2] OK {output_path} ({output_path.stat().st_size//1024}KB)")
                    return ok
                return False
        logger.warning(f"[Sora2] timeout {max_wait_sec}s ÃÊ°ú")
        return False
    except Exception as e:
        logger.warning(f"[Sora2] ¿¹¿Ü: {e}")
        return False


# ¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡
# [PATCH X / v15.90] xAI Grok Imagine Video T2V
# ¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡
async def generate_grok_video(
    prompt_en: str,
    duration: int,
    scene_id: str,
    output_path: Path,
    max_wait_sec: float = 360.0,
) -> bool:
    """xAI Grok Imagine Video ? api.x.ai/v1/videos/generations."""
    if not _XAI_API_KEY:
        return False
    try:
        headers = {
            "Authorization": f"Bearer {_XAI_API_KEY}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": "grok-imagine-video",
            "prompt": prompt_en[:480],
            "n": 1,
        }
        async with httpx.AsyncClient(timeout=60.0) as cli:
            r = await cli.post(
                "https://api.x.ai/v1/videos/generations",
                headers=headers, json=payload,
            )
        if r.status_code not in (200, 201, 202):
            logger.warning(f"[Grok-V] submit fail {r.status_code}: {r.text[:200]}")
            return False
        data = r.json()
        gen_id = data.get("id") or data.get("generation_id", "")
        if not gen_id:
            logger.warning(f"[Grok-V] generation id ¾øÀ½: {data}")
            return False
        logger.info(f"[Grok-V] »ý¼º ¿äÃ» OK, id={gen_id}")

        # polling
        waited = 0.0
        async with httpx.AsyncClient(timeout=30.0) as cli:
            while waited < max_wait_sec:
                await asyncio.sleep(8.0)
                waited += 8.0
                pr = await cli.get(
                    f"https://api.x.ai/v1/videos/generations/{gen_id}",
                    headers={"Authorization": f"Bearer {_XAI_API_KEY}"},
                )
                if pr.status_code != 200:
                    continue
                pd = pr.json()
                status = pd.get("status", "")
                if status in ("in_progress", "queued", "pending", "processing"):
                    logger.debug(f"[Grok-V] {status} ({waited:.0f}s)")
                    continue
                if status not in ("completed", "succeeded"):
                    logger.warning(f"[Grok-V] ½ÇÆÐ status={status}")
                    return False
                # ´Ù¿î·Îµå URL ÆÄ½Ì
                video_url = (
                    pd.get("url") or
                    (pd.get("videos", [{}])[0].get("url", "")) or
                    (pd.get("data", [{}])[0].get("url", ""))
                )
                if not video_url:
                    logger.warning("[Grok-V] video URL ¾øÀ½")
                    return False
                async with httpx.AsyncClient(timeout=120.0) as dl:
                    vr = await dl.get(video_url)
                if vr.status_code == 200:
                    output_path.write_bytes(vr.content)
                    ok = output_path.stat().st_size > 4096
                    if ok:
                        logger.info(f"[Grok-V] OK {output_path} ({output_path.stat().st_size//1024}KB)")
                    return ok
                return False
        logger.warning(f"[Grok-V] timeout {max_wait_sec}s ÃÊ°ú")
        return False
    except Exception as e:
        logger.warning(f"[Grok-V] ¿¹¿Ü: {e}")
        return False


# [PATCH Q / v15.85] Playwright À¥ ÀÚµ¿È­ Æú¹é (API Å° ¾øÀ» ¶§ / AI_VIDEO_PROVIDER=playwright)
_PW_QUEUE_DIR = Path("/data/jobs/pw_queue")  # Docker ³»ºÎ °æ·Î (E:\...\v2\jobs\pw_queue)

async def generate_playwright_video(
    prompt_en: str,
    duration: int,
    scene_id: str,
    output_path: Path,
    max_wait_sec: float = 360.0,
) -> bool:
    """Windows È£½ºÆ® playwright_worker.py Å¥¿¡ µî·Ï ¡æ ¿Ï·á ÆÄÀÏ Æú¸µ."""
    try:
        _PW_QUEUE_DIR.mkdir(parents=True, exist_ok=True)
        req_file  = _PW_QUEUE_DIR / f"{scene_id}.json"
        done_file = req_file.with_suffix(".done")
        fail_file = req_file.with_suffix(".fail")
        done_file.unlink(missing_ok=True)
        fail_file.unlink(missing_ok=True)
        req_file.write_text(
            json.dumps({"prompt": prompt_en[:480], "duration": duration,
                        "output_path": str(output_path)}),
            encoding="utf-8",
        )
        logger.info(f"[PW-Q] Å¥ µî·Ï: {req_file.name} dur={duration}s")
        waited = 0.0
        poll   = 6.0
        while waited < max_wait_sec:
            await asyncio.sleep(poll)
            waited += poll
            if done_file.exists():
                try:
                    res = json.loads(done_file.read_text(encoding="utf-8"))
                    p   = Path(res.get("path", ""))
                    if p.exists() and p.stat().st_size > 4096:
                        logger.info(f"[PW-Q] OK {scene_id} {p.stat().st_size//1024}KB ({waited:.0f}s)")
                        return True
                except Exception:
                    pass
            elif fail_file.exists():
                logger.warning(f"[PW-Q] ½ÇÆÐ {scene_id}: {fail_file.read_text(encoding='utf-8')[:200]}")
                return False
            logger.debug(f"[PW-Q] ´ë±â Áß... {waited:.0f}s")
        logger.warning(f"[PW-Q] timeout {max_wait_sec}s: {scene_id}")
        return False
    except Exception as e:
        logger.warning(f"[PW-Q] ¿¹¿Ü: {e}")
        return False


# ============================================================
# [PATCH R-U / v15.86] Pollo + SiliconFlow + APIframe + MagicHour
# ============================================================
_POLLO_API_KEY       = os.getenv("POLLO_API_KEY", "")
_POLLO_MODEL         = os.getenv("POLLO_MODEL", "wan-v2-6-flash")
_SILICONFLOW_API_KEY = os.getenv("SILICONFLOW_API_KEY", "")
_SILICONFLOW_MODEL   = os.getenv("SILICONFLOW_MODEL", "Wan-AI/Wan2.6-T2V-14B")
_APIFRAME_API_KEY    = os.getenv("APIFRAME_API_KEY", "")
_MAGICHOUR_API_KEY   = os.getenv("MAGICHOUR_API_KEY", "")


async def _download_video(url: str, output_path: Path) -> bool:
    async with httpx.AsyncClient(timeout=120.0) as dl:
        vr = await dl.get(url)
    if vr.status_code == 200:
        output_path.write_bytes(vr.content)
        return output_path.exists() and output_path.stat().st_size > 4096
    return False


async def generate_pollo_video(prompt_en: str, duration: int, scene_id: str,
                                output_path: Path, max_wait_sec: float = 180.0) -> bool:
    if not _POLLO_API_KEY: return False
    try:
        dur = min(max(int(duration), 3), 10)
        h = {"Authorization": f"Bearer {_POLLO_API_KEY}", "Content-Type": "application/json"}
        payload = {"model": _POLLO_MODEL, "prompt": prompt_en[:480],
                   "duration": dur, "aspect_ratio": "16:9"}
        async with httpx.AsyncClient(timeout=30.0) as c:
            r = await c.post("https://api.pollo.ai/api/v1/generations/video", headers=h, json=payload)
        if r.status_code not in (200, 201, 202):
            logger.warning(f"[Pollo] {r.status_code}: {r.text[:200]}"); return False
        d = r.json()
        task_id = (d.get("data") or d).get("id") or d.get("task_id", "")
        if not task_id: logger.warning("[Pollo] no task_id"); return False
        logger.info(f"[Pollo] task={task_id}")
        waited = 0.0
        async with httpx.AsyncClient(timeout=30.0) as c:
            while waited < max_wait_sec:
                await asyncio.sleep(6.0); waited += 6.0
                pr = await c.get(f"https://api.pollo.ai/api/v1/generations/video/{task_id}", headers=h)
                if pr.status_code != 200: continue
                pd = pr.json().get("data") or pr.json()
                st = pd.get("status", "")
                if st in ("failed", "error"): return False
                if st not in ("completed", "succeeded", "success", "done"): continue
                vurl = (pd.get("output") or {}).get("url") or pd.get("video_url") or pd.get("url", "")
                if not vurl:
                    for k in ("outputs", "videos", "results"):
                        v = pd.get(k)
                        if isinstance(v, list) and v: vurl = v[0].get("url", ""); break
                if vurl:
                    ok = await _download_video(vurl, output_path)
                    if ok: logger.info(f"[Pollo] OK {scene_id}")
                    return ok
        return False
    except Exception as e: logger.warning(f"[Pollo] {e}"); return False


async def generate_siliconflow_video(prompt_en: str, duration: int, scene_id: str,
                                      output_path: Path, max_wait_sec: float = 180.0) -> bool:
    if not _SILICONFLOW_API_KEY: return False
    try:
        dur = min(max(int(duration), 3), 10)
        h = {"Authorization": f"Bearer {_SILICONFLOW_API_KEY}", "Content-Type": "application/json"}
        payload = {"model": _SILICONFLOW_MODEL, "prompt": prompt_en[:480],
                   "image_size": "1280x720", "num_frames": dur * 8}
        async with httpx.AsyncClient(timeout=30.0) as c:
            r = await c.post("https://api.siliconflow.cn/v1/video/submit", headers=h, json=payload)
        if r.status_code not in (200, 201, 202):
            logger.warning(f"[SFlow] {r.status_code}: {r.text[:200]}"); return False
        d = r.json()
        rid = d.get("requestId") or d.get("request_id", "")
        if not rid: logger.warning("[SFlow] no requestId"); return False
        logger.info(f"[SFlow] requestId={rid}")
        waited = 0.0
        async with httpx.AsyncClient(timeout=30.0) as c:
            while waited < max_wait_sec:
                await asyncio.sleep(8.0); waited += 8.0
                pr = await c.post("https://api.siliconflow.cn/v1/video/status",
                                  headers=h, json={"requestId": rid})
                if pr.status_code != 200: continue
                pd = pr.json(); st = pd.get("status", "")
                if st == "Failed": return False
                if st != "Succeed": continue
                videos = ((pd.get("results") or {}).get("videos") or [{}])
                vurl = videos[0].get("url", "") if videos else ""
                if vurl:
                    ok = await _download_video(vurl, output_path)
                    if ok: logger.info(f"[SFlow] OK {scene_id}")
                    return ok
        return False
    except Exception as e: logger.warning(f"[SFlow] {e}"); return False


async def generate_apiframe_video(prompt_en: str, duration: int, scene_id: str,
                                   output_path: Path, max_wait_sec: float = 180.0) -> bool:
    if not _APIFRAME_API_KEY: return False
    try:
        h = {"Authorization": _APIFRAME_API_KEY, "Content-Type": "application/json"}
        payload = {"prompt": prompt_en[:480],
                   "duration": min(max(int(duration), 5), 10), "ratio": "1280:768"}
        async with httpx.AsyncClient(timeout=30.0) as c:
            r = await c.post("https://api.apiframe.pro/runway/gen3-text", headers=h, json=payload)
        if r.status_code not in (200, 201, 202):
            logger.warning(f"[APIframe] {r.status_code}: {r.text[:200]}"); return False
        d = r.json()
        tid = d.get("task_id") or d.get("id", "")
        if not tid: logger.warning("[APIframe] no task_id"); return False
        logger.info(f"[APIframe] task={tid}")
        waited = 0.0
        async with httpx.AsyncClient(timeout=30.0) as c:
            while waited < max_wait_sec:
                await asyncio.sleep(8.0); waited += 8.0
                pr = await c.get(f"https://api.apiframe.pro/fetch/{tid}", headers=h)
                if pr.status_code != 200: continue
                pd = pr.json(); st = (pd.get("status") or "").lower()
                if st in ("failed", "error"): return False
                if st not in ("completed", "success", "done"): continue
                vurl = pd.get("video_url") or pd.get("output_url") or pd.get("url", "")
                if vurl:
                    ok = await _download_video(vurl, output_path)
                    if ok: logger.info(f"[APIframe] OK {scene_id}")
                    return ok
        return False
    except Exception as e: logger.warning(f"[APIframe] {e}"); return False


async def generate_magichour_video(prompt_en: str, duration: int, scene_id: str,
                                    output_path: Path, max_wait_sec: float = 180.0) -> bool:
    if not _MAGICHOUR_API_KEY: return False
    try:
        h = {"Authorization": f"Bearer {_MAGICHOUR_API_KEY}", "Content-Type": "application/json"}
        payload = {"name": f"lf_{scene_id}", "height": 720, "width": 1280,
                   "end_seconds": min(max(int(duration), 3), 10), "start_seconds": 0,
                   "video_type": "text-to-video", "prompt": prompt_en[:480]}
        async with httpx.AsyncClient(timeout=30.0) as c:
            r = await c.post("https://api.magichour.ai/api/v1/text-to-video", headers=h, json=payload)
        if r.status_code not in (200, 201, 202):
            logger.warning(f"[MHour] {r.status_code}: {r.text[:200]}"); return False
        d = r.json()
        pid = d.get("id") or d.get("project_id", "")
        if not pid: logger.warning("[MHour] no id"); return False
        logger.info(f"[MHour] project={pid}")
        waited = 0.0
        async with httpx.AsyncClient(timeout=30.0) as c:
            while waited < max_wait_sec:
                await asyncio.sleep(8.0); waited += 8.0
                pr = await c.get(f"https://api.magichour.ai/api/v1/text-to-video/{pid}", headers=h)
                if pr.status_code != 200: continue
                pd = pr.json(); st = (pd.get("status") or "").lower()
                if st in ("failed", "error"): return False
                if st not in ("complete", "completed", "done"): continue
                vurl = pd.get("video_url") or pd.get("download_url") or pd.get("url", "")
                if vurl:
                    ok = await _download_video(vurl, output_path)
                    if ok: logger.info(f"[MHour] OK {scene_id}")
                    return ok
        return False
    except Exception as e: logger.warning(f"[MHour] {e}"); return False

async def search_and_download_assets(job_id: str, scenes: List[Scene]) -> List[Scene]:
    """°¢ Àå¸é¿¡ ´ëÇØ ÀÚ»ê °Ë»ö ¹× ´Ù¿î·Îµå ([AF-14] ¿µ»ó Áßº¹ Á¦°Å)."""
    seen_urls: set = set()
    job_assets_dir = JOBS_DIR / job_id / "assets"
    job_assets_dir.mkdir(parents=True, exist_ok=True)
    
    updated_scenes = []
    total_scenes = len(scenes)
    _used_asset_basenames: set = set()  # [v15.94] ¿µ»ó Áßº¹ ¹æÁö ? µ¿ÀÏ ¼Ò½º Àç»ç¿ë ÃßÀû
    
    _dl_queue: list = []  # [v16.13] parallel download queue
    for idx, scene in enumerate(scenes):
        try:
            # ÁøÇà·ü ¾÷µ¥ÀÌÆ®
            progress = (idx / total_scenes) * 100
            await update_job_status(job_id, JobStatus.DOWNLOADING_ASSETS, progress=progress)
            
            logger.info(f"[{idx+1}/{total_scenes}] Àå¸é '{scene.scene_id}' °Ë»ö Áß...")
            
            # [v15.69] Å°¿öµå sanitize ? Ä«¸Þ¶ó µð·ºÆ¼ºê/ºó Å°¿öµå º¹±¸
            _raw_kw = scene.keyword or ""
            _narr_hint = scene.narration or scene.description or ""
            _fallback_kw = _get_topic_fallback(_raw_kw, "")
            _sanitized_kw = _sanitize_keyword_for_search(_raw_kw, _narr_hint, _fallback_kw)
            if _sanitized_kw != _raw_kw:
                logger.info(f"[v15.69 SANITIZE] '{_raw_kw}' ¡æ '{_sanitized_kw}'")
                scene.keyword = _sanitized_kw

            # [v15.93] _is_hook_or_close / _ai_vid_selective ¹Ì¸® Á¤ÀÇ (NameError ¹æÁö)
            _is_hook_or_close = (scene.tone_profile or "").lower() in ("hook","closing","cta") or idx == 0 or idx == total_scenes - 1
            _ai_vid_selective = os.getenv("AI_VIDEO_SELECTIVE","true").lower() in ("1","true","yes")

            # [PATCH Q-WEB / v15.91] À¥ ¿ì¼± ? Grok/Playwright ¸ÕÀú ½Ãµµ
            _should_pw = _AI_VIDEO_ENABLED and (not _ai_vid_selective or _is_hook_or_close)
            if _should_pw:
                _pp = (scene.narration_en or (scene.visual_intent or "") + ", cinematic footage").strip(", ")
                if _pp and len(_pp) > 10:
                    _pd = max(int(scene.duration_seconds or 5), 3)
                    _po = job_assets_dir / f"{scene.scene_id}_pw.mp4"
                    logger.info(f"[PW-Web] {scene.scene_id} dur={_pd}s ? À¥ ¿ì¼± ½Ãµµ")
                    _pw_ok = await generate_playwright_video(_pp, _pd, scene.scene_id, _po)
                    if _pw_ok:
                        scene.asset_url = str(_po)
                        updated_scenes.append(scene)
                        logger.info(f"[PW-Web] OK {scene.scene_id}")
                        continue

            # [v15.69] Kling T2V ¿ì¼± ½Ãµµ
            _kling_ok = False
            _wan_ok = _rep_ok = _ws_ok = _veo_ok = _pw_ok = False
            _pollo_ok = _sflow_ok = _apif_ok = _mhour_ok = _sora_ok = _grok_ok = False
            # [v15.93] _is_hook_or_close / _ai_vid_selective ´Â À§¿¡¼­ ÀÌ¹Ì Á¤ÀÇµÊ
            # [v15.70 Hybrid] hook/closing ÇÊ¼ö + selective mode ±â¹Ý
            _should_kling = _AI_VIDEO_ENABLED and (not _ai_vid_selective or _is_hook_or_close) and _AI_VIDEO_PROVIDER in ("kling", "")
            if not _pw_ok and _should_kling:
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
                        logger.info(f"[Kling] ? {scene.scene_id} ¿Ï·á ? ½ºÅå ½ºÅµ")
                        continue
            # [PATCH K] WAN 2.6 T2V B-roll (hook/closing ¶Ç´Â ºñ¼±ÅÃ ¸ðµå)
            _should_wan = _AI_VIDEO_ENABLED and (not _ai_vid_selective or _is_hook_or_close) and _AI_VIDEO_PROVIDER in ("wan", "") and bool(_WAN_API_KEY or _AIML_API_KEY)
            if not _kling_ok and _should_wan:
                _wp = (scene.narration_en or (scene.visual_intent or "") + ", " + ", ".join((scene.visual_keywords or [])[:2]) + ", cinematic B-roll 4K").strip(", ")
                if _wp and len(_wp) > 10:
                    _wo = job_assets_dir / f"{scene.scene_id}_wan.mp4"
                    _wd = max(int(scene.duration_seconds or 5), 5)
                    logger.info(f"[WAN] {scene.scene_id} dur={_wd}s")
                    _wan_ok = await generate_wan_video(_wp, _wd, scene.scene_id, _wo)
                    if _wan_ok:
                        scene.asset_url = str(_wo)
                        updated_scenes.append(scene)
                        logger.info(f"[WAN] ? {scene.scene_id} ¿Ï·á ? ½ºÅå ½ºÅµ")
                        continue
            # [PATCH L] Replicate WAN 2.1 B-roll ($0.20/¿µ»ó ? Kling/WAN API ¹Ì¼³Á¤ ½Ã)
            _should_replicate = _AI_VIDEO_ENABLED and (not _ai_vid_selective or _is_hook_or_close) and bool(_REPLICATE_API_KEY)
            if not _kling_ok and not _wan_ok and _should_replicate:
                _rp = (scene.narration_en or (scene.visual_intent or "") + ", cinematic footage 4K").strip(", ")
                if _rp and len(_rp) > 10:
                    _ro = job_assets_dir / f"{scene.scene_id}_rep.mp4"
                    _rd = max(int(scene.duration_seconds or 5), 5)
                    logger.info(f"[Replicate] {scene.scene_id} dur={_rd}s")
                    _rep_ok = await generate_replicate_video(_rp, _rd, scene.scene_id, _ro)
                    if _rep_ok:
                        scene.asset_url = str(_ro)
                        updated_scenes.append(scene)
                        logger.info(f"[Replicate] ? {scene.scene_id} ¿Ï·á ? ½ºÅå ½ºÅµ")
                        continue

            # [PATCH O] WaveSpeed API B-roll ($0.01/sec, ½Å±Ô $1 ¹«·á ? ÃÖÀú°¡)
            _should_ws = _AI_VIDEO_ENABLED and (not _ai_vid_selective or _is_hook_or_close) and bool(_WAVESPEED_API_KEY)
            if not _kling_ok and not _wan_ok and not _rep_ok and _should_ws:
                _wsp = (scene.narration_en or (scene.visual_intent or "") + ", cinematic B-roll 4K").strip(", ")
                if _wsp and len(_wsp) > 10:
                    _wso = job_assets_dir / f"{scene.scene_id}_ws.mp4"
                    _wsd = max(int(scene.duration_seconds or 5), 3)
                    logger.info(f"[WaveSpeed] {scene.scene_id} dur={_wsd}s")
                    _ws_ok = await generate_wavespeed_video(_wsp, _wsd, scene.scene_id, _wso)
                    if _ws_ok:
                        scene.asset_url = str(_wso)
                        updated_scenes.append(scene)
                        logger.info(f"[WaveSpeed] OK {scene.scene_id} ? ½ºÅå ½ºÅµ")
                        continue

            # [PATCH P / v15.84] Veo 2 ? Google Gemini À¯·áÅ° È°¿ë
            _should_veo = _AI_VIDEO_ENABLED and (not _ai_vid_selective or _is_hook_or_close) and bool(_GEMINI_API_KEY) and _AI_VIDEO_PROVIDER in ("veo", "")
            if not _kling_ok and not _wan_ok and not _rep_ok and not _ws_ok and _should_veo:
                _vp = (scene.narration_en or (scene.visual_intent or "") + ", cinematic 4K footage").strip(", ")
                if _vp and len(_vp) > 10:
                    _vd = max(int(scene.duration_seconds or 5), 5)
                    _vo = job_assets_dir / f"{scene.scene_id}_veo.mp4"
                    logger.info(f"[Veo2] {scene.scene_id} dur={_vd}s")
                    _veo_ok = await generate_veo_video(_vp, _vd, scene.scene_id, _vo)
                    if _veo_ok:
                        scene.asset_url = str(_vo)
                        updated_scenes.append(scene)
                        logger.info(f"[Veo2] OK {scene.scene_id} ? ½ºÅå ½ºÅµ")
                        continue



            # [PATCH R / v15.86] Pollo.ai WAN 2.6 T2V
            _should_pollo = _AI_VIDEO_ENABLED and (not _ai_vid_selective or _is_hook_or_close) and bool(_POLLO_API_KEY)
            if not _kling_ok and not _wan_ok and not _rep_ok and not _ws_ok and not _veo_ok and not _pw_ok and _should_pollo:
                _plp = (scene.narration_en or (scene.visual_intent or "") + ", cinematic B-roll").strip()[:480]
                if _plp and len(_plp) > 10:
                    _plo = job_assets_dir / f"{scene.scene_id}_pollo.mp4"
                    _pld = max(int(scene.duration_seconds or 5), 3)
                    logger.info(f"[Pollo] {scene.scene_id} dur={_pld}s")
                    _pollo_ok = await generate_pollo_video(_plp, _pld, scene.scene_id, _plo)
                    if _pollo_ok:
                        scene.asset_url = str(_plo)
                        updated_scenes.append(scene)
                        logger.info(f"[Pollo] OK {scene.scene_id}")
                        continue

            # [PATCH S / v15.86] SiliconFlow WAN 2.6 T2V
            _should_sflow = _AI_VIDEO_ENABLED and (not _ai_vid_selective or _is_hook_or_close) and bool(_SILICONFLOW_API_KEY)
            if not _kling_ok and not _wan_ok and not _rep_ok and not _ws_ok and not _veo_ok and not _pw_ok and not _pollo_ok and _should_sflow:
                _sfp = (scene.narration_en or (scene.visual_intent or "") + ", cinematic B-roll").strip()[:480]
                if _sfp and len(_sfp) > 10:
                    _sfo = job_assets_dir / f"{scene.scene_id}_sflow.mp4"
                    _sfd = max(int(scene.duration_seconds or 5), 3)
                    logger.info(f"[SFlow] {scene.scene_id} dur={_sfd}s")
                    _sflow_ok = await generate_siliconflow_video(_sfp, _sfd, scene.scene_id, _sfo)
                    if _sflow_ok:
                        scene.asset_url = str(_sfo)
                        updated_scenes.append(scene)
                        logger.info(f"[SFlow] OK {scene.scene_id}")
                        continue

            # [PATCH T / v15.86] APIframe Runway Gen3 T2V
            _should_apiframe = _AI_VIDEO_ENABLED and (not _ai_vid_selective or _is_hook_or_close) and bool(_APIFRAME_API_KEY)
            if not _kling_ok and not _wan_ok and not _rep_ok and not _ws_ok and not _veo_ok and not _pw_ok and not _pollo_ok and not _sflow_ok and _should_apiframe:
                _afp = (scene.narration_en or (scene.visual_intent or "") + ", cinematic B-roll 4K").strip()[:480]
                if _afp and len(_afp) > 10:
                    _afo = job_assets_dir / f"{scene.scene_id}_apif.mp4"
                    _afd = max(int(scene.duration_seconds or 5), 3)
                    logger.info(f"[APIframe] {scene.scene_id} dur={_afd}s")
                    _apif_ok = await generate_apiframe_video(_afp, _afd, scene.scene_id, _afo)
                    if _apif_ok:
                        scene.asset_url = str(_afo)
                        updated_scenes.append(scene)
                        logger.info(f"[APIframe] OK {scene.scene_id}")
                        continue

            # [PATCH U / v15.86] MagicHour T2V
            _should_mhour = _AI_VIDEO_ENABLED and (not _ai_vid_selective or _is_hook_or_close) and bool(_MAGICHOUR_API_KEY)
            if not _kling_ok and not _wan_ok and not _rep_ok and not _ws_ok and not _veo_ok and not _pw_ok and not _pollo_ok and not _sflow_ok and not _apif_ok and _should_mhour:
                _mhp = (scene.narration_en or (scene.visual_intent or "") + ", cinematic footage").strip()[:480]
                if _mhp and len(_mhp) > 10:
                    _mho = job_assets_dir / f"{scene.scene_id}_mhour.mp4"
                    _mhd = max(int(scene.duration_seconds or 5), 3)
                    logger.info(f"[MHour] {scene.scene_id} dur={_mhd}s")
                    _mhour_ok = await generate_magichour_video(_mhp, _mhd, scene.scene_id, _mho)
                    if _mhour_ok:
                        scene.asset_url = str(_mho)
                        updated_scenes.append(scene)
                        logger.info(f"[MHour] OK {scene.scene_id}")
                        continue


            # [PATCH W / v15.90] OpenAI Sora 2 T2V
            _should_sora = _AI_VIDEO_ENABLED and (not _ai_vid_selective or _is_hook_or_close) and bool(_OPENAI_API_KEY)
            _sora_ok = False
            if (not _kling_ok and not _wan_ok and not _rep_ok and not _ws_ok and not _veo_ok
                    and not _pw_ok and not _pollo_ok and not _sflow_ok and not _apif_ok and not _mhour_ok
                    and _should_sora):
                _srp = (scene.narration_en or (scene.visual_intent or "") + ", cinematic footage 4K").strip()[:480]
                if _srp and len(_srp) > 10:
                    _sro = job_assets_dir / f"{scene.scene_id}_sora.mp4"
                    _srd = max(int(scene.duration_seconds or 5), 5)
                    logger.info(f"[Sora2] {scene.scene_id} dur={_srd}s")
                    _sora_ok = await generate_sora_video(_srp, _srd, scene.scene_id, _sro)
                    if _sora_ok:
                        scene.asset_url = str(_sro)
                        updated_scenes.append(scene)
                        logger.info(f"[Sora2] OK {scene.scene_id}")
                        continue

            # [PATCH X / v15.90] xAI Grok Imagine Video T2V
            _should_grok = _AI_VIDEO_ENABLED and (not _ai_vid_selective or _is_hook_or_close) and bool(_XAI_API_KEY)
            _grok_ok = False
            if (not _kling_ok and not _wan_ok and not _rep_ok and not _ws_ok and not _veo_ok
                    and not _pw_ok and not _pollo_ok and not _sflow_ok and not _apif_ok and not _mhour_ok and not _sora_ok
                    and _should_grok):
                _grp = (scene.narration_en or (scene.visual_intent or "") + ", cinematic footage").strip()[:480]
                if _grp and len(_grp) > 10:
                    _gro = job_assets_dir / f"{scene.scene_id}_grok.mp4"
                    _grd = max(int(scene.duration_seconds or 5), 5)
                    logger.info(f"[Grok-V] {scene.scene_id} dur={_grd}s")
                    _grok_ok = await generate_grok_video(_grp, _grd, scene.scene_id, _gro)
                    if _grok_ok:
                        scene.asset_url = str(_gro)
                        updated_scenes.append(scene)
                        logger.info(f"[Grok-V] OK {scene.scene_id}")
                        continue

            # º´·Ä·Î Pexels¿Í Pixabay °Ë»ö
            expanded_kw = _expand_domain_keyword(scene.keyword)
            if expanded_kw != scene.keyword:
                logger.info(f"[Y2] Å°¿öµå È®Àå: '{scene.keyword}' ¡æ '{expanded_kw}'")
            # [v15.75] empty keyword fallback
            if not expanded_kw.strip():
                import re as _re_kw
                _nrr = scene.narration or scene.description or ""
                _en_w = _re_kw.findall(r'[A-Za-z]{3,}', _nrr)
                _ko_w = [w for w in _re_kw.findall(r'[°¡-ÆR]{2,}', _nrr) if w not in _KO_STOPWORDS]
                if _en_w:
                    expanded_kw = ' '.join(_en_w[:3]) + ' footage'
                elif _ko_w:
                    expanded_kw = _get_topic_fallback(' '.join(_ko_w[:3]), '')
                else:
                    expanded_kw = _get_topic_fallback(_raw_kw, '')
                scene.keyword = expanded_kw
                # [v15.78] KGM ÀçÁ¶È¸
                if not expanded_kw.strip():
                    _base78 = _strip_korean_particles(_raw_kw.lower().strip()) if _raw_kw else ""
                    _kgm_match = KOREAN_GENERAL_MAP.get(_base78) or next(
                        (v for k,v in KOREAN_GENERAL_MAP.items() if k in _base78 or _base78 in k), None)
                    if _kgm_match:
                        expanded_kw = _kgm_match
                logger.info(f"[v15.78] empty kw fallback: '{_raw_kw}' -> '{expanded_kw}'")
            # [PATCH J / v15.83] SQLite Ä³½Ã È®ÀÎ [v16.4: seen_urls Áßº¹ ¹æÁö]
            _cached_path_j = _cache_lookup(expanded_kw)
            if _cached_path_j and _cached_path_j not in seen_urls:
                scene.asset_url = _cached_path_j
                seen_urls.add(_cached_path_j)
                _used_asset_basenames.add(Path(_cached_path_j).stem)
                logger.info(f"[J-CACHE] \"{expanded_kw}\" HIT: {_cached_path_j}")
                updated_scenes.append(scene)
                continue
            elif _cached_path_j and _cached_path_j in seen_urls:
                logger.info(f"[J-CACHE-SKIP] \"{expanded_kw}\" HIT but duplicate ? bypass to fresh search")
            pexels_videos, pixabay_videos = await asyncio.gather(
                get_pexels_videos(expanded_kw),
                get_pixabay_videos(expanded_kw)
            )  # [PATCH V-fix] Coverr/Mixkit API ¾øÀ½ ¡æ Á¦°Å
            # ºÎÁ¤ Å°¿öµå ÇÊÅÍ¸µ
            pexels_videos = [v for v in pexels_videos if not _is_negative(v, expanded_kw)]
            pixabay_videos = [v for v in pixabay_videos if not _is_negative(v, expanded_kw)]
            # [v15.68] Ä³½ºÄÉÀÌµå Äõ¸®: alt_keywords ¼ø¼­´ë·Î ½Ãµµ
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
                    )  # [PATCH V-fix]
                    _px_c = [v for v in _px_c if not _is_negative(v, _cq)]
                    _pb_c = [v for v in _pb_c if not _is_negative(v, _cq)]
                    pexels_videos += _px_c
                    pixabay_videos += _pb_c
                    _total_c = len(pexels_videos) + len(pixabay_videos)
                    logger.info(f'[v15.68 CQ{_ci+1}] "{_cq}" -> {len(_px_c)+len(_pb_c)} (total {_total_c})')
                    if _total_c >= 3: break
                except Exception as _ce:
                    logger.warning(f'[v15.68 CQ{_ci+1}] "{_cq}" ½ÇÆÐ: {_ce}')
            
            
            # [PATCH J] ¹«·á °á°ú ºÎÁ· ½Ã Storyblocks º´·Ä (¹«·á¿ì¼±+À¯·áº´·Ä)
            _j_free_total = len(pexels_videos) + len(pixabay_videos)
            if _j_free_total < 3 and STORYBLOCKS_PRIVATE_KEY:
                try:
                    _sb_par = await get_storyblocks_videos(expanded_kw, per_page=5)
                    if _sb_par:
                        pixabay_videos += _sb_par
                        logger.info(f"[J-SB parallel] {len(_sb_par)}°³ Ãß°¡ (free={_j_free_total})")
                except Exception as _j_sb_e:
                    logger.warning(f"[J-SB parallel] ¿À·ù: {_j_sb_e}")
            # ÃÖ°í Ç°ÁúÀÇ ¿µ»ó ¼±ÅÃ
            best_video_url = select_best_video(pexels_videos, pixabay_videos, scene_index=idx, exclude_urls=seen_urls, query_keyword=scene.keyword or "")
            
            if not best_video_url:
                # [PATCH J] Storyblocks ÃÖÈÄ Æú¹é
                if STORYBLOCKS_PRIVATE_KEY:
                    try:
                        _sb_fb = await get_storyblocks_videos(expanded_kw, per_page=3)
                        if _sb_fb:
                            best_video_url = _sb_fb[0].get("url")
                            logger.info(f"[J-SB fallback] {expanded_kw}: {str(best_video_url)[:80]}")
                    except Exception as _j_fb_e:
                        logger.warning(f"[J-SB fallback] err: {_j_fb_e}")
                if not best_video_url:
                    logger.warning(f"scene {scene.scene_id} no asset (3-tier failed)")
                    updated_scenes.append(scene)
                    continue
            
            # ¿µ»ó ´Ù¿î·Îµå
            asset_filename = f"{scene.scene_id}.mp4"
            asset_path = job_assets_dir / asset_filename
            
            # [AC] idempotency: skip download if file already present
            if asset_path.exists() and asset_path.stat().st_size > 4096:
                scene.asset_url = str(asset_path)
                logger.info(f"[AC] Àå¸é '{scene.scene_id}' ±âÁ¸ ÆÄÀÏ Àç»ç¿ë: {asset_path} ({asset_path.stat().st_size // 1024}KB)")
                updated_scenes.append(scene)
                continue
            # [AF-14] track used URL to prevent duplicate in later scenes
            if best_video_url:
                seen_urls.add(best_video_url)
            # [v16.13] Phase 1: queue for parallel download
            _dl_queue.append({
                "scene": scene, "url": best_video_url, "path": asset_path,
                "pexels": pexels_videos, "pixabay": pixabay_videos,
                "idx": idx, "kw": expanded_kw,
            })
        
        except Exception as e:
            logger.error(f"Àå¸é '{scene.scene_id}' Ã³¸® ¿À·ù: {e}")
            updated_scenes.append(scene)
    
    # [v16.13] Phase 2: parallel download (Semaphore=4)
    _dl_sem = asyncio.Semaphore(4)

    async def _dl_task_fn(item: dict):
        _scene = item["scene"]
        _url   = item["url"]
        _path  = item["path"]
        _px    = item["pexels"]
        _pb    = item["pixabay"]
        _idx   = item["idx"]
        _kw    = item["kw"]
        async with _dl_sem:
            _ok = await download_video(_url, _path)
            if not _ok:
                _alt_excl = {_url}
                for _a in range(2):
                    _au = select_best_video(_px, _pb,
                                            scene_index=_idx + _a + 1,
                                            exclude_urls=_alt_excl)
                    if not _au or _au == _url:
                        break
                    _alt_excl.add(_au)
                    _ok = await download_video(_au, _path)
                    if _ok:
                        seen_urls.add(_au)
                        _url = _au
                        break
            if _ok:
                _scene.asset_url = str(_path)
                _cache_write(_kw, _url, str(_path))
                _alt_kws = getattr(_scene, "alt_keywords", []) or []
                if _alt_kws and not getattr(_scene, "alt_asset_url", None):
                    _akw2 = _expand_domain_keyword(_alt_kws[0])
                    try:
                        _apx2, _apb2 = await asyncio.gather(
                            get_pexels_videos(_akw2, per_page=3),
                            get_pixabay_videos(_akw2, per_page=3),
                        )
                        _apx2 = [v for v in _apx2 if not _is_negative(v, _akw2)]
                        _apb2 = [v for v in _apb2 if not _is_negative(v, _akw2)]
                        _au2 = select_best_video(_apx2, _apb2,
                                                 scene_index=_idx + 200,
                                                 exclude_urls=seen_urls,
                                                 query_keyword=_akw2)
                        if _au2:
                            _ap2 = job_assets_dir / f"{_scene.scene_id}_alt.mp4"
                            if await download_video(_au2, _ap2):
                                _scene.alt_asset_url = str(_ap2)
                                seen_urls.add(_au2)
                    except Exception:
                        pass
            else:
                logger.error(f"scene '{_scene.scene_id}' download failed (3 attempts)")
            return _scene

    if _dl_queue:
        _dl_results = await asyncio.gather(
            *[_dl_task_fn(item) for item in _dl_queue],
            return_exceptions=True,
        )
        for _r in _dl_results:
            if isinstance(_r, Exception):
                logger.error(f"[v16.13 dl-task] {_r}")
            else:
                updated_scenes.append(_r)

    # [AY-E] persist seen URLs globally for cross-job diversity
    try:
        _save_global_seen(seen_urls)
    except Exception:
        pass
    await update_job_status(job_id, JobStatus.DOWNLOADING_ASSETS, progress=100.0)
    return updated_scenes


def run_ffmpeg_command(command: List[str], timeout: float = 300.0) -> bool:
    """FFmpeg Ä¿¸Çµå ½ÇÇà"""
    try:
        logger.info(f"FFmpeg Ä¿¸Çµå ½ÇÇà: {' '.join(command[:5])}...")
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=timeout
        )
        
        if result.returncode != 0:
            logger.error(f"FFmpeg ¿À·ù: {result.stderr}")
            return False
        
        logger.info("FFmpeg Ä¿¸Çµå ¼º°ø")
        return True
    except subprocess.TimeoutExpired:
        logger.error("FFmpeg Ä¿¸Çµå Å¸ÀÓ¾Æ¿ô")
        return False
    except Exception as e:
        logger.error(f"FFmpeg ½ÇÇà ¿À·ù: {e}")
        return False


async def run_ffmpeg_async(command, timeout: float = 300.0) -> bool:
    """FFmpeg ºñµ¿±â ½ÇÇà (event loop ºí·ÎÅ· ¹æÁö)"""
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
    """¾À´ç 3~4 ¼­ºêÅ¬¸³(°¢ 4~6ÃÊ) ¡æ ÃÑ 15~20°³ ºü¸¥ ÀüÈ¯ Å¬¸³"""
    clips = []

    # [v15.60.0] Ken Burns ÇÁ¸®¼Â ? duration ºñ·Ê zoom ¼Óµµ ({kb_speed} Ä¡È¯)
    KB_PRESETS = [
        "zoompan=z='min(zoom+{kb_speed},1.06)':x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)':d={fps_d}:s=1920x1080:fps=30",
        "zoompan=z='if(eq(on,1),1.5,max(zoom-{kb_speed},1.0))':x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)':d={fps_d}:s=1920x1080:fps=30",
        "zoompan=z='1.3':x='if(lte(on,1),0,min(x+3,iw*0.25))':y='ih/2-(ih/zoom/2)':d={fps_d}:s=1920x1080:fps=30",
        "zoompan=z='1.3':x='if(lte(on,1),iw*0.25,max(x-3,0))':y='ih/2-(ih/zoom/2)':d={fps_d}:s=1920x1080:fps=30",
        "zoompan=z='min(zoom+{kb_speed},1.05)':x='iw/2-(iw/zoom/2)':y='if(lte(on,1),0,min(y+2,ih*0.2))':d={fps_d}:s=1920x1080:fps=30",
        "zoompan=z='min(zoom+{kb_speed_hi},1.06)':x='if(lte(on,1),iw*0.1,max(x-1,0))':y='ih-ih/zoom':d={fps_d}:s=1920x1080:fps=30",
    ]

    kb_counter = 0  # Àü¿ª Ken Burns ÇÁ¸®¼Â ¼øÈ¯

    for _scene_idx, scene in enumerate(scenes):
        if not scene.asset_url:
            # [P] fallback ºñÁÖ¾ó »ý¼º (´Ü»ö ´ë½Å ±×¶óµð¾ðÆ® + Å°¿öµå Ä«µå)
            fb_path = output_dir / f"fallback_{scene.scene_id}.mp4"
            dur = max(scene.duration_seconds or 5.0, 2.5)
            if _make_fallback_clip(_scene_idx, dur, fb_path,
                                   keyword=scene.keyword, description=scene.description or "",
                                   resolution=os.getenv("DEFAULT_RESOLUTION", "1920x1080")):
                scene.asset_url = str(fb_path)
                logger.info(f"[P] ¾À '{scene.scene_id}' fallback ºñÁÖ¾ó »ç¿ë")
            else:
                logger.warning(f"Àå¸é '{scene.scene_id}' ÀÚ»ê ¾øÀ½ (fallbackµµ ½ÇÆÐ)")
                continue

        scene_dur = max(scene.duration_seconds or 5.0, 1.5)  # [v16.0] TTS µ¿±âÈ­ Á¸Áß

        # ¿øº» ¿µ»ó ±æÀÌ ÆÄ¾Ç
        probe_cmd = [
            "ffprobe", "-v", "error", "-show_entries", "format=duration",
            "-of", "csv=p=0", scene.asset_url
        ]
        try:
            result = subprocess.run(probe_cmd, capture_output=True, text=True, timeout=10)
            src_dur = float(result.stdout.strip()) if result.stdout.strip() else scene_dur * 3
        except Exception:
            src_dur = scene_dur * 3

        # v15.12 fix ? ½ÇÁ¦ ¼Ò½º ±æÀÌ º¸Á¸ (scene_dur·Î ºÎÇ®¸®±â X)
        actual_src_dur = src_dur
        # ¼Ò½º°¡ ¾Àº¸´Ù ÂªÀ¸¸é stream_loop À¸·Î ¹Ýº¹ Àç»ý
        needs_loop = actual_src_dur < scene_dur * 0.95

        # ¼­ºêÅ¬¸³ ¼ö °è»ê (4~5ÃÊÂ¥¸®·Î ºÐÇÒ)
        SUB_DUR = 3.0  # [v15.98] 2-3s cut  # °¢ ¼­ºêÅ¬¸³ ±æÀÌ (ÃÊ)
        n_subs  = max(1, int(scene_dur / SUB_DUR))  # [v16.0] floor division
        n_subs  = min(n_subs, 8)  # [v15.98] max 8  # ÃÖ´ë 5°³

        logger.info(f"'{scene.scene_id}': {scene_dur:.1f}ÃÊ ¡æ {n_subs}°³ ¼­ºêÅ¬¸³ (src={actual_src_dur:.1f}s, loop={needs_loop})")

        # [v15.68] alt ¼Ò½º ÁØºñ (sub_i >= 3¿¡¼­ »ç¿ë)
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

        # [v16.0] Á¤¹Ð sub_dur: ±ÕµîºÐÇÒ + ¸¶Áö¸· Å¬¸³¿¡ ³ª¸ÓÁö ½Ã°£ Èí¼ö
        _base_sub_dur = scene_dur / n_subs
        _scene_clips_start = len(clips)
        for sub_i in range(n_subs):
            if sub_i == n_subs - 1:
                sub_dur = max(scene_dur - _base_sub_dur * (n_subs - 1), 1.0)
            else:
                sub_dur = _base_sub_dur
            # [v15.68] sub_i>=3ÀÌ¸é alt ¼Ò½º »ç¿ë
            # [v15.99] È¦Â¦ ±³¹ø: 0=main,1=alt,2=main,3=alt... ¡æ ÃÖ´ë ´Ù¾ç¼º
            _can_alt2 = _alt_src2 and _alt_src2_dur > 1.0
            _use_alt2 = _can_alt2 and (sub_i % 2 == 1)
            _clip_src2 = _alt_src2 if _use_alt2 else scene.asset_url
            _clip_src2_dur = _alt_src2_dur if _use_alt2 else actual_src_dur
            _needs_loop2 = _clip_src2_dur < scene_dur * 0.95
            # [v15.99] ¼Ò½ºº° µ¶¸³ seek: °°Àº ¼Ò½º ³»¿¡¼­ ±Õµî ºÐÆ÷
            _src_uses = 2 if _can_alt2 else 1  # ¼Ò½º°¡ 2°³¸é °¢ ¼Ò½º´ç n_subs/2¹ø »ç¿ë
            _sub_idx_in_src = sub_i // _src_uses  # ÀÌ ¼Ò½º ¾È¿¡¼­ ¸î ¹øÂ°ÀÎ°¡
            _n_per_src = max(1, (n_subs + _src_uses - 1) // _src_uses)
            seek_usable = max(_clip_src2_dur - 0.5, 0.0)
            if _n_per_src > 1 and seek_usable > 0:
                seek_start = seek_usable * _sub_idx_in_src / _n_per_src
            else:
                seek_start = 0
            seek_start = max(0, min(seek_start, seek_usable))

            fps_d        = max(int(sub_dur * 30), 30)
            # [v15.60.0] duration ºñ·Ê KB ¼Óµµ (4ÃÊ ±âÁØ; ÂªÀ»¼ö·Ï ºü¸£°Ô)
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
                f"unsharp=lx=5:ly=5:la=1.2:cx=3:cy=3:ca=0.6,"  # [AW-3] °­È­µÈ sharpen
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
            if await run_ffmpeg_async(command, timeout=clip_timeout) and clip_output.exists() and clip_output.stat().st_size >= 4096:
                clips.append(clip_output)
                logger.info(f"  ¼­ºêÅ¬¸³ OK: {clip_output.name} ({sub_dur:.1f}s, seek={seek_start:.1f}s)")
            else:
                sz = clip_output.stat().st_size if clip_output.exists() else 0
                if sz > 0 and sz < 4096:
                    clip_output.unlink(missing_ok=True)  # ºó ÆÄÀÏ »èÁ¦
                logger.warning(f"  ¼­ºêÅ¬¸³ ½ÇÆÐ (size={sz}B): {scene.scene_id}_{sub_i} ? fallback")
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
                if await run_ffmpeg_async(fallback, timeout=clip_timeout):
                    clips.append(clip_output)

        # [v16.0] ¾Àº° trim: sub-clip ÃÑÇÕÀ» Á¤È®È÷ scene_dur·Î trim
        # ¡æ ¾À °æ°è ¿ÀÂ÷ ´©Àû ¹æÁö (³ª·¹ÀÌ¼Ç-¿µ»ó per-scene Á¤·Ä º¸Àå)
        _scene_clips = clips[_scene_clips_start:]
        if len(_scene_clips) > 0 and _scene_clips:
            _scene_merged = output_dir / f"scene_{scene.scene_id}_merged.mp4"
            _merge_ok = False
            if len(_scene_clips) == 1:
                import shutil as _sh
                _sh.copy(str(_scene_clips[0]), str(_scene_merged))
                _merge_ok = True
            else:
                _merge_ok = xfade_batch(list(_scene_clips), _scene_merged)
            if _merge_ok and _scene_merged.exists() and _scene_merged.stat().st_size > 4096:
                # trim to exact scene_dur
                _scene_trimmed = output_dir / f"scene_{scene.scene_id}_final.mp4"
                _trim_cmd = [
                    "ffmpeg", "-i", str(_scene_merged),
                    "-t", str(round(scene_dur, 3)),
                    "-c:v", "copy", "-an", "-y", str(_scene_trimmed)
                ]
                _trim_timeout = max(60.0, scene_dur * 5)
                if run_ffmpeg_command(_trim_cmd, timeout=_trim_timeout) and _scene_trimmed.exists():
                    # ¼­ºêÅ¬¸³µéÀ» trimmed ¾À Å¬¸³À¸·Î ±³Ã¼
                    del clips[_scene_clips_start:]
                    clips.append(_scene_trimmed)
                    logger.info(f"[v16.0] ¾À '{scene.scene_id}' trim ¡æ {scene_dur:.2f}s ({len(_scene_clips)}Å¬¸³¡æ1)")
                else:
                    logger.warning(f"[v16.0] ¾À trim ½ÇÆÐ ¡æ ¿øº» ¼­ºêÅ¬¸³ À¯Áö: {scene.scene_id}")
            else:
                logger.warning(f"[v16.0] ¾À merge ½ÇÆÐ ¡æ ¿øº» ¼­ºêÅ¬¸³ À¯Áö: {scene.scene_id}")

    logger.info(f"ÃÑ Å¬¸³ ¼ö: {len(clips)}°³")
    return clips



def create_concat_file(clips: List[Path], output_file: Path) -> bool:
    """FFmpeg concat ÆÄÀÏ »ý¼º"""
    try:
        with open(output_file, "w") as f:
            for clip in clips:
                f.write(f"file '{clip}'\n")
        
        logger.info(f"Concat ÆÄÀÏ »ý¼º: {output_file}")
        return True
    except Exception as e:
        logger.error(f"Concat ÆÄÀÏ »ý¼º ¿À·ù: {e}")
        return False


def _is_valid_clip(clip_path) -> bool:
    """ffprobe·Î Å¬¸³ À¯È¿¼º °Ë»ç (moov atom ¡¤ ºñµð¿À ½ºÆ®¸² Á¸Àç È®ÀÎ)"""
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
    """Duration:N/A Å¬¸³À» Á¤±ÔÈ­ ? filter_complex È£È¯À» À§ÇØ re-encode"""
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
    """Å¬¸³ ¹èÄ¡¸¦ concat filter·Î ÇÕÄ¡±â ? xfadeº¸´Ù ¾ÈÁ¤Àû (Duration:N/A Å¬¸³ Çã¿ë)"""
    # 1) ¼Õ»óµÈ Å¬¸³ »çÀü ÇÊÅÍ¸µ (moov atom ¾ø´Â ÆÄÀÏ Á¦°Å)
    original_n = len(clip_paths)
    clip_paths = [cp for cp in clip_paths if _is_valid_clip(cp)]
    dropped = original_n - len(clip_paths)
    if dropped:
        logger.warning(f"xfade_batch: ¼Õ»óµÈ Å¬¸³ {dropped}°³ Á¦¿Ü (ÀÜ¿© {len(clip_paths)}°³)")

    if len(clip_paths) == 0:
        logger.error("xfade_batch: À¯È¿ Å¬¸³ 0°³ ? ÇÕÄ¡±â ºÒ°¡")
        return False
    if len(clip_paths) == 1:
        shutil.copy(str(clip_paths[0]), str(output))
        return True

    # ¹æ¹ý 1: concat filter ? Duration:N/A Å¬¸³Àº ¸ÕÀú Á¤±ÔÈ­
    clip_paths = [normalize_clip(cp) for cp in clip_paths]
    inputs = []
    for cp in clip_paths:
        inputs += ["-i", str(cp)]

    n = len(clip_paths)
    # [i:v:0] ? Á¤±ÔÈ­ ÈÄ durationÀÌ È®Á¤µÈ ´ÜÀÏ ºñµð¿À ½ºÆ®¸²
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
        logger.info(f"xfade_batch concat OK: {n}°³ ¡æ {output.name}")
        return True

    # ¹æ¹ý 2: concat demuxer fallback (copy, ¹«¼Õ½Ç)
    logger.warning("concat filter ½ÇÆÐ ¡æ demuxer fallback")
    # fallback ´Ü°è¿¡¼­µµ ¼Õ»ó Å¬¸³ ÇÑ ¹ø ´õ °É·¯³¿
    clip_paths = [cp for cp in clip_paths if _is_valid_clip(cp)]
    if not clip_paths:
        logger.error("demuxer fallback: À¯È¿ Å¬¸³ 0°³")
        return False
    concat_txt = output.parent / f"_concat_{output.stem}.txt"
    with open(concat_txt, "w") as f:
        for cp in clip_paths:
            f.write(f"file '{cp}'\n")
    cmd2 = ["ffmpeg", "-f", "concat", "-safe", "0", "-i", str(concat_txt),
            "-c", "copy", "-y", str(output)]
    return run_ffmpeg_command(cmd2, timeout=timeout)


def concatenate_videos(concat_file: Path, output_video: Path, transition: str = "fade") -> bool:
    """¿µ»ó ÆÄÀÏ ¿¬°á (¹èÄ¡ xfade Å©·Î½ºÆäÀÌµå Æ®·£Áö¼Ç)"""
    # concat.txt¿¡¼­ Å¬¸³ °æ·Î ÆÄ½Ì
    with open(concat_file, "r") as f:
        lines = f.readlines()

    clip_paths = [l.split("'")[1] for l in lines if l.startswith("file ")]

    # À¯È¿ÇÏÁö ¾ÊÀº Å¬¸³ »çÀü Á¦°Å (Å©±â < 4KB = ±úÁø ÆÄÀÏ)
    clip_paths = [cp for cp in clip_paths if _is_valid_clip(cp)]
    if not clip_paths:
        logger.error("concatenate_videos: À¯È¿ÇÑ Å¬¸³ ¾øÀ½")
        return False

    if len(clip_paths) < 2:
        # ´ÜÀÏ Å¬¸³: ±×³É copy
        command = ["ffmpeg", "-f", "concat", "-safe", "0", "-i", str(concat_file),
                   "-c", "copy", "-y", str(output_video)]
        return run_ffmpeg_command(command)
    
    # 2°³ ÀÌ»ó: ¹èÄ¡ xfade (8°³¾¿ ³ª´²¼­ Ã³¸®, ÀÌÈÄ ÃÖÁ¾ ÇÕÄ¡±â)
    BATCH_SIZE = 8
    temp_dir = output_video.parent
    
    if len(clip_paths) <= BATCH_SIZE:
        # ¼Ò¼ö Å¬¸³: Á÷Á¢ xfade
        if xfade_batch(clip_paths, output_video, transition):
            return True
        logger.warning("xfade ½ÇÆÐ, ´Ü¼ø concat fallback")
        fallback = ["ffmpeg", "-f", "concat", "-safe", "0", "-i", str(concat_file),
                    "-c", "copy", "-y", str(output_video)]
        return run_ffmpeg_command(fallback)
    
    # ´Ù¼ö Å¬¸³: ¹èÄ¡ ºÐÇÒ Ã³¸® ? ¸¶Áö¸· °í¾Æ ¹èÄ¡(¡Â1 Å¬¸³)´Â Á÷Àü ¹èÄ¡¿¡ ÇÕÄ£´Ù
    batches = [clip_paths[i:i+BATCH_SIZE] for i in range(0, len(clip_paths), BATCH_SIZE)]
    if len(batches) >= 2 and len(batches[-1]) <= 1:
        batches[-2].extend(batches[-1])
        batches.pop()
        logger.info(f"xfade_batch: ¸¶Áö¸· °í¾Æ ¹èÄ¡ ¸ÓÁö ¡æ {len(batches)}°³ ¹èÄ¡")
    batch_outputs = []
    for bi, batch in enumerate(batches):
        bout = temp_dir / f"batch_{bi}.mp4"
        if not xfade_batch(batch, bout, transition):
            # ¹èÄ¡ ½ÇÆÐ ½Ã ´Ü¼ø concat (¹«È¿ Å¬¸³ Á¦¿Ü)
            valid_batch = [cp for cp in batch if _is_valid_clip(cp)]
            if not valid_batch:
                logger.warning(f"batch_{bi}: À¯È¿ Å¬¸³ ¾øÀ½ ? °Ç³Ê¶Ü")
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
            batch_outputs.append(bout)
    
    if not batch_outputs:
        return False
    
    if len(batch_outputs) == 1:
        shutil.copy(str(batch_outputs[0]), str(output_video))
        return True
    
    # ¹èÄ¡ °á°úµéÀ» ÃÖÁ¾ ÇÕÄ¡±â ? ¹èÄ¡´Â ÀÌ¹Ì xfade Ã³¸®µÊ.
    # Å« ¹èÄ¡µé(°¢ 30-50ÃÊ)À» xfade filter_complex·Î Àç°áÇÕÇÏ¸é ¸Þ¸ð¸® °úºÎÇÏ ¡æ ÄÁÅ×ÀÌ³Ê SIGKILL.
    # µû¶ó¼­ ÃÖÁ¾ ¹èÄ¡ ¸ÓÁö´Â Ç×»ó demuxer concat (stream copy) »ç¿ë.
    final_concat = temp_dir / "final_concat.txt"
    with open(final_concat, "w") as f:
        for bp in batch_outputs:
            f.write(f"file '{bp}'\n")
    if run_ffmpeg_command(["ffmpeg", "-f", "concat", "-safe", "0", "-i", str(final_concat),
                           "-c", "copy", "-y", str(output_video)]):
        logger.info(f"ÃÖÁ¾ ¹èÄ¡ ¸ÓÁö OK (demuxer concat): {len(batch_outputs)}°³ -> {output_video.name}")
        return True
    
    # ÃÖÁ¾ fallback: re-encode concat (stream copy ½ÇÆÐ ½Ã ÄÚµ¦/ÇØ»óµµ ºÒÀÏÄ¡)
    logger.warning("demuxer concat ½ÇÆÐ -> filter_complex concat·Î Àç½Ãµµ (re-encode)")
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
    # ±¸ ÄÚµå º¸°ü¿ë (»ç¿ë ¾È ÇÔ)
    FADE_DUR = 0.5
    offset = 0  # placeholder
    
    if len(clip_paths) == 2:
        fg = f"[0:v][1:v]xfade=transition={transition}:duration={FADE_DUR}:offset={offset:.3f}[vout]"
    else:
        # Ã¹ ¹øÂ° Æ®·£Áö¼Ç
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
        # xfade ½ÇÆÐ ½Ã ´Ü¼ø concat fallback
        logger.warning("xfade ½ÇÆÐ, ´Ü¼ø concatÀ¸·Î fallback")
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
    """¿Àµð¿À ¹Í½Ì - loudnorm Á¤±ÔÈ­ + ³ª·¹ÀÌ¼Ç ¿ì¼± BGM ´õÅ·"""
    # [v16.0] TTS ±æÀÌ ±âÁØ ¿µ»ó trim (³ª·¹ÀÌ¼Ç-¿µ»ó Á¤È®È÷ ÀÏÄ¡)
    _tts_trim: list = []
    if tts_audio_path.exists():
        try:
            _pr = subprocess.run(
                ["ffprobe", "-v", "error", "-show_entries", "format=duration",
                 "-of", "csv=p=0", str(tts_audio_path)],
                capture_output=True, text=True, timeout=10
            )
            _rd = (_pr.stdout.strip() or "").strip()
            if _rd and _rd not in ("N/A", ""):
                _tts_d = float(_rd)
                if _tts_d > 0:
                    _tts_trim = ["-t", str(round(_tts_d + 0.5, 2))]
                    logger.info(f"[v16.0] TTS trim: {_tts_d:.2f}s + 0.5s ¿©À¯")
        except Exception as _e:
            logger.warning(f"[v16.0] TTS ±æÀÌ ÃøÁ¤ ½ÇÆÐ: {_e}")

    if not tts_audio_path.exists():
        logger.warning(f"TTS ¿Àµð¿À ¾øÀ½: {tts_audio_path}")
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

    # TTS ÀÖÀ½: loudnormÀ¸·Î ³ª·¹ÀÌ¼Ç º¼·ý Á¤±ÔÈ­
    if bgm_path and bgm_path.exists() and bgm_volume > 0:
        # [v15.60.0] TTS ³ª·¹ÀÌ¼Ç + BGM ´öÅ· ¹Í½º (sidechaincompress)
        # BGM_VOLUME_DURING_VOICE (ENV) ±â¹Ý ´öÅ· ? ³ª·¹ÀÌ¼Ç ±¸°£ ÀÚµ¿ °¨¼Ò
        actual_bgm_vol = BGM_VOLUME_DURING_VOICE  # ±âº» 0.045
        # ¸í½ÃÀû bgm_volume ÁöÁ¤ ½Ã (±âº» 0.3 ¾Æ´Ñ °æ¿ì) ¹Ý¿µ
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
            *_tts_trim,  # [v16.0] TTS ±æÀÌ ±âÁØ trim
            "-c:v", "copy",
            "-c:a", "aac", "-ac", "2", "-b:a", "192k",
            "-shortest",
            "-y", str(output_video)
        ]
    else:
        # TTS ³ª·¹ÀÌ¼Ç¸¸ (loudnorm Á¤±ÔÈ­)
        filter_complex = "[1:a]loudnorm=I=-16:TP=-1.5:LRA=11[aout]"
        command = [
            "ffmpeg",
            "-i", str(video_path),
            "-i", str(tts_audio_path),
            "-filter_complex", filter_complex,
            "-map", "0:v",
            "-map", "[aout]",
            *_tts_trim,  # [v16.0] TTS ±æÀÌ ±âÁØ trim
            "-c:v", "copy",
            "-c:a", "aac", "-ac", "2", "-b:a", "192k",
            "-shortest",
            "-y", str(output_video)
        ]

    success = run_ffmpeg_command(command)
    if not success:
        # loudnorm ½ÇÆÐ ½Ã ´Ü¼ø ¹Í½º fallback
        logger.warning("loudnorm ½ÇÆÐ, ´Ü¼ø ¹Í½º fallback")
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
    """[v15.59.0] ASS/SRT ÀÚ¸· ¿À¹ö·¹ÀÌ. subtitle_type¿¡ µû¶ó ÇÊÅÍ ÀÚµ¿ ºÐ±â."""
    if not srt_path.exists():
        logger.warning(f"SRT ÆÄÀÏ ¾øÀ½: {srt_path}")
        return False

    # [v16.7] ÀÔ·Â ¿µ»óÀÇ ½ÇÁ¦ ÇØ»óµµ °¨Áö ¡æ ¼¼·ÎÇü ÀÚµ¿ ÆÇº°
    _detected_res = "1920x1080"
    try:
        _probe = subprocess.run(
            ["ffprobe", "-v", "quiet", "-select_streams", "v:0",
             "-show_entries", "stream=width,height", "-of", "csv=p=0",
             str(input_video)],
            capture_output=True, text=True, timeout=10
        )
        _wh = _probe.stdout.strip().split(",")
        if len(_wh) == 2:
            _detected_res = f"{_wh[0].strip()}x{_wh[1].strip()}"
    except Exception:
        pass
    _font_size, _margin_v = _compute_subtitle_style(_detected_res)
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

    # °æ·Î ³» ÄÝ·Ð ÀÌ½ºÄÉÀÌÇÁ (Windows °æ·Î ´ëºñ)
    srt_escaped = str(srt_path).replace("\\", "/").replace(":", "\\:")

    # [v15.59.0] ASS: ass= ÇÊÅÍ / SRT: subtitles= ÇÊÅÍ
    if subtitle_type == "ass" or str(srt_path).lower().endswith(".ass"):
        vf_filter = f"ass='{srt_escaped}'"
    else:
        vf_filter = f"subtitles={srt_escaped}:charenc=UTF-8:fontsdir=/usr/share/fonts/opentype/noto:force_style='{style}'"

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
        # SRT °æ·Î ÀÌ½ºÄÉÀÌÇÁ ¹®Á¦·Î ½ÇÆÐ ½Ã copy fallback
        logger.warning("SRT ¿À¹ö·¹ÀÌ ½ÇÆÐ, subtitles ÇÊÅÍ Àç½Ãµµ")
        simple_cmd = [
            "ffmpeg", "-i", str(input_video),
            "-vf", f"subtitles='{str(srt_path)}'",
            "-c:v", "libx264", "-preset", VIDEO_PRESET, "-crf", str(VIDEO_CRF),
            "-c:a", "copy", "-y", str(output_video)
        ]
        return run_ffmpeg_command(simple_cmd)
    return True

def extract_thumbnail(video_path: Path, output_image: Path, timestamp: str = "3") -> bool:
    """¿µ»ó¿¡¼­ ½æ³×ÀÏ ÃßÃâ"""
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
    """½æ³×ÀÏ¿¡ ÅØ½ºÆ® ¿À¹ö·¹ÀÌ Ãß°¡"""
    try:
        # ½æ³×ÀÏ ·Îµå
        img = Image.open(thumbnail_path)
        draw = ImageDraw.Draw(img)
        
        # ÆùÆ® ¼³Á¤ (±âº» ÆùÆ® »ç¿ë)
        try:
            font = ImageFont.truetype("/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc", font_size)
        except Exception:
            # ÆùÆ® ¾øÀ¸¸é ±âº» ÆùÆ® »ç¿ë
            font = ImageFont.load_default()
        
        # ÅØ½ºÆ® À§Ä¡ (Áß¾Ó ÇÏ´Ü)
        img_width, img_height = img.size
        bbox = draw.textbbox((0, 0), title, font=font)
        text_width = bbox[2] - bbox[0]
        text_height = bbox[3] - bbox[1]
        
        x = (img_width - text_width) // 2
        y = img_height - text_height - 30
        
        # ¹ÝÅõ¸í ¹è°æ Ãß°¡ (¼±ÅÃ»çÇ×)
        background_padding = 20
        bg_box = [
            x - background_padding,
            y - background_padding,
            x + text_width + background_padding,
            y + text_height + background_padding
        ]
        draw.rectangle(bg_box, fill=(0, 0, 0, 180))
        
        # ÅØ½ºÆ® ±×¸®±â
        draw.text((x, y), title, font=font, fill=(255, 255, 255))
        
        # ÀúÀå
        img.save(output_path, "JPEG", quality=95)
        logger.info(f"½æ³×ÀÏ »ý¼º ¿Ï·á: {output_path}")
        return True
    
    except Exception as e:
        logger.error(f"½æ³×ÀÏ ÅØ½ºÆ® ¿À¹ö·¹ÀÌ ¿À·ù: {e}")
        return False


# [PRO v2] ½æ³×ÀÏ CTR ÃÖÀûÈ­ »ö»ó
_THUMB_COLOR_SCHEMES = [
    ("#0D0D0D","#1A1A2E","#FFD700","#FFFFFF","#FFD700"),  # ºí·¢+°ñµå
    ("#0A1628","#0D47A1","#FF6B00","#FFFFFF","#FFB347"),  # µöºí·ç+¿À·»Áö
    ("#1A0A00","#CC3300","#FFFF00","#FFFFFF","#FFDD00"),  # ·¹µå+¿»·Î¿ì
    ("#0D1B00","#1B5E20","#00FF88","#FFFFFF","#B9F6CA"),  # ±×¸° ´ÙÅ©
    ("#1A0033","#4A0080","#FF00FF","#FFFFFF","#FFB3FF"),  # ÆÛÇÃ+¸¶Á¨Å¸
]

def generate_pro_thumbnail(
    video_path: Path,
    output_path: Path,
    title: str,
    subtitle: str = "",
) -> bool:
    """YouTube ÇÁ·Î ½æ³×ÀÏ v2 ? ºÐÇÒ ÆÐ³Î ·¹ÀÌ¾Æ¿ô (³»¿ëº° ÀÇ¹Ì ºÐ¸®)
    
    ·¹ÀÌ¾Æ¿ô:
      LEFT (44%): ¾îµÎ¿î ±×¶óµ¥ÀÌ¼Ç + ¿¬µµ ¹èÁö + ÁÖÁ¦¾î + ÀÓÆÑÆ® ¿öµå
      RIGHT (56%): ¿µ»ó ÃÖÀû ÇÁ·¹ÀÓ (»ö°¨ °­È­)
      BOTTOM BAR: ÀÚ¸·/ºÎÁ¦¸ñ ½ºÆ®¸³
    """
    try:
        import re as _re_thumb
        from PIL import Image, ImageDraw, ImageFont, ImageFilter, ImageEnhance

        # ¦¡¦¡ 1. ÃÖÀû ÇÁ·¹ÀÓ ÃßÃâ ¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡
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
            except Exception: pass

        if best_frame is None:
            logger.warning("[THUMB] ÇÁ·¹ÀÓ ÃßÃâ ½ÇÆÐ")
            return False

        # ¦¡¦¡ 2. Äµ¹ö½º Å©±â ¹× ±¸¿ª Á¤ÀÇ ¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡
        TW, TH = 1280, 720
        SPLIT_X  = int(TW * 0.44)   # ¿ÞÂÊ ÆÐ³Î ³Êºñ
        BLEND_W  = 80                # ÁÂ¿ì ºí·»µù Æø
        BOTTOM_H = int(TH * 0.135)  # ÇÏ´Ü ¹Ù ³ôÀÌ
        MAIN_H   = TH - BOTTOM_H    # ¸ÞÀÎ ¿µ¿ª ³ôÀÌ

        # ¦¡¦¡ 3. ¹è°æ ÇÁ·¹ÀÓ (ÀüÃ¼) ¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡
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

        # ¦¡¦¡ 4. ¿ÞÂÊ ¾îµÎ¿î ÆÐ³Î ¿À¹ö·¹ÀÌ ¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡
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
        # »ó´Ü ¾îµÎ¿î ¶ì (¾çÂÊ °øÅë)
        for y in range(0, 55):
            a = int(110 * (1 - y / 55))
            draw_p.line([(0, y), (TW, y)], fill=(0, 0, 0, a))
        img = Image.alpha_composite(img, panel)

        # ¦¡¦¡ 5. ÇÏ´Ü ¹Ù ¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡
        bar = Image.new("RGBA", (TW, TH), (0, 0, 0, 0))
        draw_b = ImageDraw.Draw(bar)
        draw_b.rectangle([0, MAIN_H, TW, TH], fill=(6, 10, 38, 245))
        draw_b.rectangle([0, MAIN_H, TW, MAIN_H + 3], fill=(255, 200, 0, 255))
        img = Image.alpha_composite(img, bar)

        # ¦¡¦¡ 6. ÃÖÁ¾ RGB º¯È¯ + ±×¸®±â ÁØºñ ¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡
        img = img.convert("RGB")
        draw = ImageDraw.Draw(img)

        # ¦¡¦¡ 7. ÆùÆ® ·Îµå ¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡
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

        font_badge  = _load_font(28)    # [v15.81] badge
        font_main   = _load_font(88)    # [v15.81] +12px (25-35% ¿µ¿ª)
        font_impact = _load_font(96)    # [v15.81] +16px impact
        font_sub    = _load_font(50)    # [v15.81] +6px sub
        font_bar    = _load_font(34)    # [v15.81] +4px bar

        # ¦¡¦¡ 8. Á¦¸ñ ÆÄ½Ì ? "/" ±âÁØ ºÐÇÒ ¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡
        parts_raw = [p.strip() for p in title.split("/") if p.strip()]
        if len(parts_raw) == 1:
            # °ø¹é ±âÁØ Áß°£ ºÐ¸®
            ws = title.split()
            mid = max(1, len(ws) // 2)
            parts_raw = [" ".join(ws[:mid]), " ".join(ws[mid:])]
        # ÃÖ´ë 2°³ ÆÄÆ®
        line1 = parts_raw[0] if parts_raw else title
        line2 = parts_raw[1] if len(parts_raw) > 1 else ""

        # ¿¬µµ ¹èÁö ÃßÃâ
        yr_match = _re_thumb.search(r'\d{4}', title)
        year_str = yr_match.group() if yr_match else ""

        # ÀÓÆÑÆ® Å°¿öµå °¨Áö (¸¶Áö¸· ÆÄÆ® ¶Ç´Â Æ¯Á¤ ´Ü¾î)
        _IMPACT_KW = ["Ãæ°Ý", "Çõ¸í", "Çõ½Å", "°æ°í", "À§Çè", "ÁÖÀÇ", "¹Ì·¡", "º¯È­",
                       "Æø¹ß", "±Þµî", "ºØ±«", "ºñ¹Ð", "Áø½Ç", "¹ÝÀü", "´ë¹Ú", "ÃÖ°­"]
        def _is_impact(s: str) -> bool:
            return any(k in s for k in _IMPACT_KW)

        # ¦¡¦¡ 9. ¿¬µµ ¹èÁö ¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡
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

        # ¦¡¦¡ 10. °ñµå ¾×¼¾Æ® ¶óÀÎ ¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡
        line_y = 90
        draw.rectangle([38, line_y, SPLIT_X - 40, line_y + 4], fill=GOLD)

        # ¦¡¦¡ 11. ¸ÞÀÎ ¶óÀÎ 1 ¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡
        y_pos = 105
        color1 = GOLD if _is_impact(line1) else WHITE
        # ±×¸²ÀÚ
        for ox, oy in [(-2, 2), (2, 2), (0, 3)]:
            draw.text((40 + ox, y_pos + oy), line1, font=font_main, fill=DARK)
        draw.text((40, y_pos), line1, font=font_main, fill=color1)
        bb1 = draw.textbbox((40, y_pos), line1, font=font_main)
        y_pos = bb1[3] + 8

        # ¦¡¦¡ 12. ¶óÀÎ 2 (ÀÓÆÑÆ® °­Á¶) ¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡
        if line2:
            color2 = GOLD if _is_impact(line2) else WHITE
            fnt2   = font_impact if _is_impact(line2) else font_sub
            for ox, oy in [(-2, 2), (2, 2), (0, 3)]:
                draw.text((40 + ox, y_pos + oy), line2, font=fnt2, fill=DARK)
            draw.text((40, y_pos), line2, font=fnt2, fill=color2)
            bb2 = draw.textbbox((40, y_pos), line2, font=fnt2)
            y_pos = bb2[3] + 16

        # ¦¡¦¡ 13. »ï°¢Çü Àç»ý ¾ÆÀÌÄÜ ¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡
        icon_x, icon_y = 42, y_pos + 8
        draw.polygon(
            [(icon_x, icon_y), (icon_x, icon_y + 32), (icon_x + 28, icon_y + 16)],
            fill=GOLD
        )


        # ¦¡¦¡ 13-b. ±¹±â PIL Á÷Á¢ µå·ÎÀ× ½ºÆ®¸³ ¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡
        _FLAG_DRAW = {
            "????": [  # USA ? ÆÄ¶õ ÄµÅÏ + »¡°­/Èò °¡·ÎÁÙ
                ("rect_full", (178, 34, 52)),          # »¡°­ ¹è°æ
                ("hstripes_white", None),               # Èò ÁÙ 6°³
                ("rect_canton", (60, 59, 110)),         # ÆÄ¶õ ÄµÅÏ
                ("stars", (255, 255, 255)),
            ],
            "????": [("rect_full", (222, 41, 16)), ("star_big", (255, 215, 0))],
            "????": [("rect_full", (255, 255, 255)), ("circle_red", (188, 0, 45))],
            "????": [("rect_full", (255, 255, 255)), ("taegeuk", None)],
            "????": [("union_jack", None)],
            "????": [("tricolor_h", [(0,0,0),(221,0,0),(255,206,0)])],
            "????": [("tricolor_v", [(0,35,149),(255,255,255),(237,41,57)])],
            "????": [("tricolor_h", [(255,255,255),(0,57,166),(213,43,30)])],
            "????": [("rect_full", (255,0,0)), ("maple_leaf", None)],
            "????": [("tricolor_h", [(255,153,51),(255,255,255),(19,136,8)])],
            "????": [("rect_full", (0,0,128))],
            "????": [("rect_full", (0,51,153)), ("eu_stars", (255,204,0))],
            "????": [("tricolor_h", [(0,42,142),(255,255,255),(205,46,58)])],
            "????": [("rect_full", (255,0,0))],
            "????": [("tricolor_v", [(0,140,69),(255,255,255),(206,43,55)])],
            "????": [("tricolor_h", [(170,21,27),(255,196,0),(170,21,27)])],
        }

        def _draw_flag_badge(draw_ctx: ImageDraw.Draw, fx: int, fy: int, fw: int, fh: int, flag_emoji: str):
            """±¹±â ¹èÁö¸¦ PIL·Î Á÷Á¢ ±×¸²"""
            specs = _FLAG_DRAW.get(flag_emoji)
            if not specs:
                # ±âº»: È¸»ö ¹èÁö¿¡ ±¹±â ÀÌ¸ðÁö Ã¹ ±ÛÀÚ
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
                    pass  # ³Ê¹« º¹Àâ ? ÄµÅÏ »öÀ¸·Î ´ëÃ¼
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
                FW, FH = 44, 30   # ±¹±â ¹èÁö Å©±â
                _badge_x = 42
                _badge_y = icon_y + 46
                for _flag_emoji in _detected_flags[:5]:
                    # Å×µÎ¸® ¶ó¿îµå ¹èÁö
                    _pad = 3
                    draw.rounded_rectangle(
                        [(_badge_x-_pad, _badge_y-_pad), (_badge_x+FW+_pad, _badge_y+FH+_pad)],
                        radius=4, fill=(255,255,255,200)
                    )
                    _draw_flag_badge(draw, _badge_x, _badge_y, FW, FH, _flag_emoji)
                    _badge_x += FW + 12
            except Exception as _fe:
                logger.debug(f"[THUMB] ±¹±â ¹èÁö ¿À·ù: {_fe}")

        # ¦¡¦¡ 14. ÇÏ´Ü ¹Ù ÅØ½ºÆ® ¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡
        bar_text = subtitle.strip() if subtitle else (line2 if line2 and line1 != line2 else "")
        if not bar_text:
            # Á¦¸ñ ÀüÃ¼ Ãà¾à
            bar_text = title[:40] + ("¡¦" if len(title) > 40 else "")
        if bar_text:
            bb_bar = draw.textbbox((0, 0), bar_text, font=font_bar)
            btw = bb_bar[2] - bb_bar[0]
            btx = (TW - btw) // 2
            bty = MAIN_H + (BOTTOM_H - (bb_bar[3] - bb_bar[1])) // 2
            draw.text((btx + 1, bty + 1), bar_text, font=font_bar, fill=DARK)
            draw.text((btx, bty), bar_text, font=font_bar, fill=(210, 210, 220))

        # ¦¡¦¡ 15. ¿À¸¥ÂÊ ÆÐ³Î »ó´Ü ¹Ì¼¼ ºñ³×Æ® ¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡
        rv = Image.new("RGBA", (TW, TH), (0, 0, 0, 0))
        draw_rv = ImageDraw.Draw(rv)
        for x in range(60):
            a = int(70 * (1 - x / 60))
            rx = TW - 60 + x
            draw_rv.line([(rx, 0), (rx, MAIN_H)], fill=(0, 0, 0, a))
        img = Image.alpha_composite(img.convert("RGBA"), rv).convert("RGB")

        # ¦¡¦¡ 16. ÀúÀå ¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡
        output_path.parent.mkdir(parents=True, exist_ok=True)
        img.save(str(output_path), "JPEG", quality=92, optimize=True)
        logger.info(f"[THUMB-v2] ¸ÖÆ¼ÆÐ³Î ½æ³×ÀÏ ¿Ï·á: {output_path} score={best_score:.1f}")
        return True

    except Exception as e:
        logger.error(f"[THUMB] ÇÁ·Î ½æ³×ÀÏ ¿À·ù: {e}")
        import traceback; logger.debug(traceback.format_exc())
        return False
def create_shortform_from_longform(
    longform_path: Path,
    output_path: Path,
    max_duration: float = 60.0,
    timeout: float = 120.0,
) -> bool:
    """[v16.6] ÀåÆí ¿µ»ó¿¡¼­ ¼ôÆû(1080x1920) »ý¼º ? °­È­ ¹öÀü

    crop ÇÊÅÍ: scale¡æcrop¡æpad ¼ø¼­·Î ¾ÈÀüÇÏ°Ô Ã³¸®.
    - 1´Ü°è: °¡·Î ±âÁØÀ¸·Î 1920 ³ôÀÌ¿¡ ¸Â°Ô ½ºÄÉÀÏ
    - 2´Ü°è: Áß¾Ó 1080¡¿1920 Å©·Ó (ºñÀ² 0ÀÎ °æ¿ì ¹æ¾î)
    - 3´Ü°è: ³²´Â °ø°£ black pad (ºñÀ² ºÒÀÏÄ¡ ½Ã fallback)
    """
    if not longform_path.exists():
        logger.error(f"[SHORTFORM] ÀÔ·Â ÆÄÀÏ ¾øÀ½: {longform_path}")
        return False

    dur = get_video_duration(longform_path)
    if dur is not None and dur <= 0:
        logger.error(f"[SHORTFORM] À¯È¿ÇÏÁö ¾ÊÀº duration: {dur}")
        return False

    actual_max = min(max_duration, dur) if dur else max_duration

    # [v16.6] ¾ÈÀüÇÑ 9:16 crop ÇÊÅÍ
    # scale=iw*sar:ih (SAR º¸Á¤) ¡æ 1920 ³ôÀÌ ±âÁØ ½ºÄÉÀÏ ¡æ 1080 ³Êºñ Å©·Ó
    vf_filter = (
        "scale='if(gt(iw/ih,9/16),1080,-2)':'if(gt(iw/ih,9/16),-2,1920)',"
        "crop=1080:1920:(iw-1080)/2:(ih-1920)/2,"
        "scale=1080:1920"
    )

    command = [
        "ffmpeg",
        "-i", str(longform_path),
        "-t", str(actual_max),
        "-vf", vf_filter,
        "-c:v", "libx264",
        "-preset", "fast",
        "-crf", "23",
        "-c:a", "aac",
        "-b:a", "128k",
        "-movflags", "+faststart",
        "-y",
        str(output_path),
    ]

    ok = run_ffmpeg_command(command, timeout=timeout)
    if ok and output_path.exists() and output_path.stat().st_size > 4096:
        logger.info(f"[SHORTFORM] »ý¼º ¿Ï·á: {output_path} ({output_path.stat().st_size/1024/1024:.1f}MB)")
        return True

    # [v16.6] fallback: ´Ü¼ø crop Àç½Ãµµ
    logger.warning("[SHORTFORM] 1Â÷ ½Ãµµ ½ÇÆÐ ? ´Ü¼ø crop fallback ½Ãµµ")
    fallback_cmd = [
        "ffmpeg",
        "-i", str(longform_path),
        "-t", str(actual_max),
        "-vf", "crop=min(iw\\,ih*9/16*1):min(ih\\,iw*16/9*1):0:0,scale=1080:1920",
        "-c:v", "libx264",
        "-preset", "ultrafast",
        "-c:a", "aac",
        "-b:a", "128k",
        "-y",
        str(output_path),
    ]
    ok2 = run_ffmpeg_command(fallback_cmd, timeout=timeout)
    if ok2 and output_path.exists() and output_path.stat().st_size > 4096:
        logger.info(f"[SHORTFORM] fallback »ý¼º ¿Ï·á: {output_path}")
        return True

    logger.error(f"[SHORTFORM] »ý¼º ÃÖÁ¾ ½ÇÆÐ: {longform_path}")
    return False


def get_video_duration(video_path: Path) -> Optional[float]:
    """¿µ»ó ±æÀÌ Á¶È¸"""
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
        logger.error(f"¿µ»ó ±æÀÌ Á¶È¸ ¿À·ù: {e}")
    
    return None


def get_random_bgm() -> Optional[Path]:
    """¹è°æÀ½¾Ç µð·ºÅä¸®¿¡¼­ ·£´ý ÆÄÀÏ ¼±ÅÃ"""
    import random
    
    bgm_files = list(BGM_DIR.glob("*.mp3")) + list(BGM_DIR.glob("*.wav"))
    
    if bgm_files:
        selected = random.choice(bgm_files)
        logger.info(f"¼±ÅÃµÈ ¹è°æÀ½¾Ç: {selected}")
        return selected
    
    logger.warning("¹è°æÀ½¾Ç ÆÄÀÏ ¾øÀ½")
    return None




# ============================================================================
# [v15.60.0] Narration-First Timeline Engine ÇÔ¼ö±º
# ============================================================================

def split_script_into_beats(script, avg_speech_rate=4.0, min_beat_sec=6.0, max_beat_sec=12.0):
    """½ºÅ©¸³Æ®¸¦ ÀÇ¹Ì ´ÜÀ§(Beat)·Î ºÐÇÒ. Returns list of {text, est_duration, beat_idx}"""
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
        if current_dur >= min_beat_sec and sent[-1:] in (".", "!", "?", "¡£"):
            beats.append({"text": current_text.strip(), "est_duration": round(current_dur, 2), "beat_idx": len(beats)})
            current_text, current_dur = "", 0.0
    if current_text.strip():
        beats.append({"text": current_text.strip(), "est_duration": round(current_dur, 2), "beat_idx": len(beats)})
    logger.info(f"[NTL] ½ºÅ©¸³Æ® ¡æ {len(beats)}°³ Beat (ÃÑ {sum(b['est_duration'] for b in beats):.1f}ÃÊ)")
    return beats


def build_narration_ssml(text, voice="ko-KR-SunHiNeural", rate="+0%", pitch="+0Hz",
                          pause_comma_ms=None, pause_sentence_ms=None):
    """[v15.60.0] SSML ÀüÃ³¸®: ½°Ç¥/¹®Àå ³¡ pause »ðÀÔ"""
    import re as _re
    pc = pause_comma_ms if pause_comma_ms is not None else PAUSE_COMMA_MS
    ps = pause_sentence_ms if pause_sentence_ms is not None else PAUSE_SENTENCE_MS
    t = _re.sub(r",(?=\s)", f", <break time=\"{pc}ms\"/>", text)
    t = _re.sub(r"([.!?])(\s)", f"\\1 <break time=\"{ps}ms\"/>\\2", t)
    return (f'<speak version="1.0" xmlns="http://www.w3.org/2001/10/synthesis" xml:lang="ko-KR">'
            f'<voice name="{voice}"><prosody rate="{rate}" pitch="{pitch}">{t}</prosody></voice></speak>')


def _assign_scene_timings(scenes, segments, total_dur):
    """¾À¿¡ start/end Å¸ÀÌ¹Ö ÇÒ´ç"""
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
    """[v15.60.0] WhisperX Å¸ÀÓ½ºÅÆÇÁ ±â¹Ý ³ª·¹ÀÌ¼Ç Å¸ÀÓ¶óÀÎ ±¸¼º"""
    import json as _json
    if timestamps_path and Path(timestamps_path).exists():
        try:
            ts_data = _json.loads(Path(timestamps_path).read_text(encoding="utf-8"))
            segments = ts_data.get("segments", [])
            if segments:
                total_dur = segments[-1].get("end", 0.0)
                logger.info(f"[NTL] WhisperX ·Îµå: {len(segments)}¼¼±×¸ÕÆ®, {total_dur:.1f}ÃÊ")
                return _assign_scene_timings(scenes, segments, total_dur)
        except Exception as e:
            logger.warning(f"[NTL] timestamps ÆÄ½Ì ½ÇÆÐ, ÃßÁ¤ »ç¿ë: {e}")
    # ÃßÁ¤ Å¸ÀÓ¶óÀÎ
    segments, cursor = [], 0.0
    for scene in scenes:
        narr = getattr(scene, "narration", None) or ""
        est = max(len(narr.replace(" ", "")) / 4.0 if narr else (scene.duration_seconds or 5.0), 2.0)
        segments.append({"start": round(cursor, 3), "end": round(cursor + est, 3),
                          "text": narr, "scene_id": scene.scene_id})
        cursor += est + PAUSE_SENTENCE_MS / 1000.0
    return _assign_scene_timings(scenes, segments, cursor)


def visual_match_score(asset_meta, scene, already_used=None):
    """[v15.60.0] ÀÚ»ê-¾À ¸ÅÄª Á¡¼ö (0~1). keyword 35% + visual_intent 25% + duration 15% + resolution 10% + motion 10% - dup 5%"""
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
    """[v15.60.0] timeline_report.json ÀúÀå"""
    import json as _json
    report_path = JOBS_DIR / job_id / "timeline_report.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report = {
        "job_id": job_id, "version": VERSION,
        "generated_at": datetime.now().isoformat(),
        "total_duration": timeline.get("total_duration", 0),
        "scene_count": len(scenes),
        "scene_timings": timeline.get("scene_timings", []),
        "segments": timeline.get("segments", []),
        "env": {k: globals()[k] for k in ("PAUSE_COMMA_MS","PAUSE_SENTENCE_MS",
                "SCENE_HEAD_PAD_SEC","SCENE_TAIL_PAD_SEC","BGM_VOLUME_DEFAULT","BGM_VOLUME_DURING_VOICE")},
    }
    report_path.write_text(_json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info(f"[NTL] timeline_report.json ÀúÀå: {report_path}")
    return report_path


def create_srt_from_text(text: str, total_duration: float, output_path: Path) -> bool:
    """
    ½ºÅ©¸³Æ® ÅØ½ºÆ®¸¦ SRT ÀÚ¸· ÆÄÀÏ·Î º¯È¯.
    ÀüÃ¼ ¿µ»ó ½Ã°£¿¡ ¸Â°Ô ÅØ½ºÆ®¸¦ ±Õµî ºÐ¹è.

    Args:
        text: ÀÚ¸·À¸·Î Ç¥½ÃÇÒ ÀüÃ¼ ÅØ½ºÆ®
        total_duration: ¿µ»ó ÃÑ ±æÀÌ (ÃÊ)
        output_path: SRT ÆÄÀÏ ÀúÀå °æ·Î

    Returns:
        ¼º°ø ¿©ºÎ
    """
    try:
        import re

        # ¹®Àå ´ÜÀ§·Î ºÐ¸® (¸¶Ä§Ç¥, ´À³¦Ç¥, ¹°À½Ç¥, ÁÙ¹Ù²Þ)
        sentences = [s.strip() for s in re.split(r'[.!?\n]+', text) if s.strip()]

        if not sentences:
            sentences = [text[:50]]  # fallback

        # ÇÑ ÀÚ¸·´ç ÃÖ´ë ±ÛÀÚ ¼ö (2ÁÙ x 25ÀÚ)
        MAX_CHARS = 40
        chunks = []
        for sentence in sentences:
            # ±ä ¹®ÀåÀº MAX_CHARS ´ÜÀ§·Î ºÐÇÒ
            while len(sentence) > MAX_CHARS:
                chunks.append(sentence[:MAX_CHARS])
                sentence = sentence[MAX_CHARS:]
            if sentence:
                chunks.append(sentence)

        if not chunks:
            return False

        # °¢ Ã»Å©¿¡ ½Ã°£ ±Õµî ¹èºÐ (¸¶Áö¸· 0.5ÃÊ´Â ¿©À¯)
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
            # °ãÄ§ ¹æÁö: end´Â ´ÙÀ½ startº¸´Ù 0.1ÃÊ ¾Õ
            end = min((i + 1) * chunk_duration - 0.1, usable_duration)
            srt_content += f"{i+1}\n"
            srt_content += f"{sec_to_srt_time(start)} --> {sec_to_srt_time(end)}\n"
            srt_content += f"{chunk}\n\n"

        output_path.write_text(srt_content, encoding="utf-8")
        logger.info(f"SRT »ý¼º ¿Ï·á: {len(chunks)}°³ ÀÚ¸· ±¸°£, {output_path}")
        return True

    except Exception as e:
        logger.error(f"SRT »ý¼º ¿À·ù: {e}")
        return False


def create_srt_from_scenes(scenes: list, output_path: Path) -> bool:
    """¾Àº° description/keyword¸¦ SRT ÀÚ¸·À¸·Î º¯È¯ (¾À Å¸ÀÌ¹Ö ¿ÏÀü µ¿±âÈ­)."""
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
            text = scene.narration or scene.description or scene.keyword or scene.scene_id  # [v16.18] narration first
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
        logger.info(f"¾À µ¿±âÈ­ SRT »ý¼º: {len(srt_entries)}°³ ±¸°£")
        return True

    except Exception as e:
        logger.error(f"¾À SRT »ý¼º ¿À·ù: {e}")
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
    ¹ÂÁ÷ºñµð¿À »ý¼º: ºñµð¿À Å¬¸³ ¿¬°á + BGM + ÀÚ¸· ¿À¹ö·¹ÀÌ.
    TTS ³ª·¹ÀÌ¼Ç ¾øÀÌ ¹è°æÀ½¾Ç¸¸ »ç¿ë.

    Args:
        clips: ºñµð¿À Å¬¸³ °æ·Î ¸ñ·Ï
        srt_path: SRT ÀÚ¸· ÆÄÀÏ °æ·Î
        bgm_path: BGM ¿Àµð¿À ÆÄÀÏ °æ·Î (NoneÀÌ¸é ¹«À½)
        bgm_volume: BGM º¼·ý (0-1)
        output_path: Ãâ·Â ¿µ»ó °æ·Î
        resolution: Ãâ·Â ÇØ»óµµ

    Returns:
        ¼º°ø ¿©ºÎ
    """
    if not clips:
        logger.error("Å¬¸³ ¾øÀ½")
        return False

    try:
        import tempfile

        # 1) Å¬¸³ concat¿ë ÀÓ½Ã txt
        concat_txt = output_path.parent / "mv_concat.txt"
        with open(concat_txt, "w") as f:
            for clip in clips:
                f.write(f"file '{clip}'\n")

        # 2) concat ¡æ ÀÓ½Ã combined
        combined = output_path.parent / "mv_combined.mp4"
        concat_cmd = [
            "ffmpeg",
            "-f", "concat", "-safe", "0",
            "-i", str(concat_txt),
            "-c", "copy",
            "-y", str(combined)
        ]
        if not run_ffmpeg_command(concat_cmd):
            logger.error("¹ÂÁ÷ºñµð¿À concat ½ÇÆÐ")
            return False

        # 3) ÀÚ¸· ½ºÅ¸ÀÏ (¹ÂÁ÷ºñµð¿À °¨¼º: Å« ÆùÆ®, Èò»ö, ±½Àº ¿Ü°û¼±)
        # ¿¬±¸ ±â¹Ý ÃÖÀû°ª:
        # - FontSize=56: 1920x1080ÀÇ 5.2% ³ôÀÌ = °¡»ç °¡µ¶¼º ÃÖÀû (YouTube MV ±âÁØ)
        # - BorderStyle=3: ¹ÝÅõ¸í ¹Ú½º ¹è°æ (ÅØ½ºÆ® °¡µ¶¼º ±Ø´ëÈ­)
        # - Outline=4: ¿Ü°û¼± µÎ²² - ¾îµÎ¿î/¹àÀº ¹è°æ ¸ðµÎ ´ëÀÀ
        # - MarginV=60: ÇÏ´Ü 60px - ¸ð¹ÙÀÏ/TV ¾ÈÀü ¿µ¿ª
        subtitle_style = (
            "FontName=Noto Sans CJK KR,"
            "FontSize=56,"             # 1920x1080 ÃÖÀû (È­¸é ³ôÀÌ 5.2%)
            "PrimaryColour=&H00FFFFFF,"  # Èò»ö ÅØ½ºÆ® (AABBGGRR)
            "OutlineColour=&H00000000,"  # °ËÁ¤ ¿Ü°û¼±
            "BackColour=&HA0000000,"     # 50% Åõ¸í °ËÁ¤ ¹Ú½º ¹è°æ
            "BorderStyle=3,"             # ºÒÅõ¸í ¹Ú½º ¹è°æ
            "Outline=4,"                 # ¿Ü°û¼± µÎ²² 4px
            "Shadow=0,"
            "Bold=1,"
            "Alignment=2,"               # ÇÏ´Ü Áß¾Ó
            "MarginV=60"                 # ÇÏ´Ü 60px ¿©¹é
        )

        # 4) ÀÚ¸· ÇÊÅÍ ¹®ÀÚ¿­ (srt °æ·Î ÀÌ½ºÄÉÀÌÇÁ)
        srt_escaped = str(srt_path).replace("\\", "/").replace(":", "\\:")
        subtitle_filter = f"subtitles={srt_escaped}:charenc=UTF-8:fontsdir=/usr/share/fonts/opentype/noto:force_style='{subtitle_style}'"  # [v16.20] fontsdir

        # 5) BGM Æ÷ÇÔ ¿©ºÎ¿¡ µû¶ó ¸í·É ±¸¼º
        if bgm_path and bgm_path.exists():
            # BGM + ÀÚ¸·
            cmd = [
                "ffmpeg",
                "-i", str(combined),
                # BGM ¹Ýº¹
                "-i", str(bgm_path),
                "-filter_complex",
                # loudnorm: YouTube ±âÁØ -14 LUFS, TP=-1.5, LRA=11
                f"[1:a]volume={bgm_volume},loudnorm=I=-14:TP=-1.5:LRA=11[bgm]",
                "-vf", subtitle_filter,
                "-map", "0:v",
                "-map", "[bgm]",
                "-c:v", "libx264",
                "-preset", "fast",
                "-c:a", "aac",
                "-b:a", "192k",
                "-shortest",            # ºñµð¿À ±æÀÌ ±âÁØ Á¾·á
                "-y", str(output_path)
            ]
        else:
            # ÀÚ¸·¸¸ (¹«À½)
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
            logger.info(f"¹ÂÁ÷ºñµð¿À »ý¼º ¿Ï·á: {output_path}")
        return result

    except Exception as e:
        logger.error(f"¹ÂÁ÷ºñµð¿À »ý¼º ¿À·ù: {e}")
        return False

def sync_scene_durations_from_timestamps(
    scenes,
    timestamps_path
):
    """
    TTS ¿Àµð¿À Å¸ÀÓ½ºÅÆÇÁ(ElevenLabs alignment ¶Ç´Â Whisper segments) ±â¹Ý
    ¾Àº° ºñµð¿À Å¬¸³ ±æÀÌ µ¿±âÈ­.

    ¿ì¼±¼øÀ§:
    1. Whisper `segments` °¡ ÀÖÀ¸¸é ¡æ ¾Àº° ½ÇÁ¦ ¿Àµð¿À ±¸°£¿¡ Á¤¹Ð ¸ÅÇÎ
       (¼¼±×¸ÕÆ®¸¦ ¾À °³¼ö¿¡ ¸ÂÃç ´©Àû ±æÀÌ ºñ·Ê·Î ºÐÇÒ)
    2. ±× ¿Ü ¡æ ÀüÃ¼ ¿Àµð¿À ±æÀÌ ±â¹Ý ºñ·Ê ¹èºÐ
       (ElevenLabs character_end_times_seconds[-1] ¶Ç´Â Whisper duration)

    Returns: duration_seconds Á¶Á¤µÈ ¾À ¸ñ·Ï
    """
    import json as _json
    from pathlib import Path as _Path

    if not timestamps_path:
        logger.info("Å¸ÀÓ½ºÅÆÇÁ °æ·Î ¾øÀ½ ? scenes.json ÃßÁ¤ duration »ç¿ë")
        return scenes

    ts_path = _Path(timestamps_path)
    if not ts_path.exists():
        logger.info(f"Å¸ÀÓ½ºÅÆÇÁ ÆÄÀÏ ¾øÀ½: {ts_path} ? scenes.json ÃßÁ¤ duration »ç¿ë")
        return scenes

    try:
        with open(ts_path, encoding="utf-8") as f:
            ts_data = _json.load(f)

        source = ts_data.get("source", "elevenlabs")
        segments = ts_data.get("segments") or []
        alignment = ts_data.get("alignment") or {}
        end_times = alignment.get("character_end_times_seconds") or []

        # ¦¡¦¡ ÀüÃ¼ ¿Àµð¿À ±æÀÌ È®º¸ ¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡
        total_audio_sec = 0.0
        if segments:
            total_audio_sec = float(segments[-1].get("end", 0.0) or 0.0)
        if total_audio_sec <= 0 and end_times:
            total_audio_sec = float(end_times[-1])
        if total_audio_sec <= 0:
            total_audio_sec = float(ts_data.get("duration") or 0.0)

        if total_audio_sec <= 0:
            logger.warning(
                f"Å¸ÀÓ½ºÅÆÇÁ¿¡ ±æÀÌ Á¤º¸ ¾øÀ½ (source={source}) ? µ¿±âÈ­ ½ºÅµ"
            )
            return scenes

        logger.info(
            f"Å¸ÀÓ½ºÅÆÇÁ ·Îµå: source={source} ÃÑ±æÀÌ={total_audio_sec:.2f}s "
            f"segments={len(segments)}"
        )

        # ¦¡¦¡ Àü·« A: ¼¼±×¸ÕÆ® Á¤¹Ð ¸ÅÇÎ (Whisper segments ÀÖÀ» ¶§) ¦¡¦¡¦¡¦¡¦¡¦¡
        # ¾À °³¼ö¿¡ ¼¼±×¸ÕÆ®¸¦ ´©Àû ±æÀÌ ºñ·Ê·Î ºÐÇÒÇØ °¢ ¾ÀÀÇ (start,end) »êÃâ
        if segments and len(segments) >= len(scenes) >= 1:
            scene_weights = [max((s.duration_seconds or 5.0), 0.1) for s in scenes]
            total_weight = sum(scene_weights)
            # ´©Àû °æ°è ÃÊ ´ÜÀ§ °è»ê (¿Àµð¿À total * (´©Àû weight / total_weight))
            boundaries = []
            cum = 0.0
            for w in scene_weights[:-1]:
                cum += w
                boundaries.append(total_audio_sec * cum / total_weight)
            boundaries.append(total_audio_sec)

            # °æ°è¸¦ °¡Àå °¡±î¿î ¼¼±×¸ÕÆ® °æ°è·Î ½º³À
            seg_ends = [float(seg.get("end", 0.0) or 0.0) for seg in segments]
            snapped = []
            last_end_idx = -1
            for b in boundaries[:-1]:
                # ÇöÀç±îÁö ¾´ ¼¼±×¸ÕÆ® ÀÌÈÄ ±¸°£¿¡¼­ b¿¡ °¡Àå °¡±î¿î end ¼±ÅÃ
                best_idx = last_end_idx + 1
                best_diff = abs(seg_ends[best_idx] - b) if best_idx < len(seg_ends) else 1e9
                for j in range(last_end_idx + 1, len(seg_ends)):
                    d = abs(seg_ends[j] - b)
                    if d < best_diff:
                        best_diff = d
                        best_idx = j
                    else:
                        # Á¤·ÄµÇ¾î ÀÖÀ¸¹Ç·Î ¸Ö¾îÁö¸é Áß´Ü
                        if seg_ends[j] > b:
                            break
                # ÃÖ¼Ò 1°³ ¼¼±×¸ÕÆ®´Â ³²°Ü¾ß ÇÏ¹Ç·Î ³¡¿¡¼­ 2°³´Â ³²±â±â
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
                    logger.info(f"¾À '{s.scene_id}' ±æÀÌ Á¶Á¤ (segment-snap): {old:.1f}s -> {dur:.1f}s")
                synced.append(s.model_copy(update={"duration_seconds": dur}))
                prev = end
            actual_total = sum(x.duration_seconds for x in synced)
            logger.info(
                f"¾À µ¿±âÈ­ ¿Ï·á (segment-snap, source={source}): "
                f"TTS {total_audio_sec:.1f}s ¡æ ½ÇÁ¦ÇÕ°è {actual_total:.1f}s"
            )
            return synced

        # ¦¡¦¡ Àü·« B: ºñ·Ê ¹èºÐ (¼¼±×¸ÕÆ® ºÎÁ· ½Ã fallback) ¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡
        total_scene_sec = sum((s.duration_seconds or 5.0) for s in scenes)
        if total_scene_sec <= 0:
            logger.warning("¾À ÃÑ ±æÀÌ 0 ? µ¿±âÈ­ ½ºÅµ")
            return scenes

        ratio = total_audio_sec / total_scene_sec
        synced = []
        for s in scenes:
            new_dur = max(1.0, round((s.duration_seconds or 5.0) * ratio, 2))
            if abs(new_dur - (s.duration_seconds or 5.0)) > 0.1:
                logger.info(f"¾À '{s.scene_id}' ±æÀÌ Á¶Á¤ (ratio): {s.duration_seconds:.1f}s -> {new_dur:.1f}s")
            synced.append(s.model_copy(update={"duration_seconds": new_dur}))

        actual_total = sum(s.duration_seconds for s in synced)
        logger.info(
            f"¾À µ¿±âÈ­ ¿Ï·á (ratio, source={source}): "
            f"¾ÀÇÕ°è {total_scene_sec:.1f}s ¡æ TTS {total_audio_sec:.1f}s "
            f"(½ÇÁ¦ÇÕ°è {actual_total:.1f}s)"
        )
        return synced

    except Exception as e:
        logger.error(f"¾À µ¿±âÈ­ ¿À·ù (¿øº» »ç¿ë): {e}", exc_info=True)
        return scenes


# [Q4] silencedetect ÆÄ¶ó¹ÌÅÍ (È¯°æº¯¼ö override °¡´É)
SILENCE_NOISE_DB = float(_rhythm_os.getenv("SILENCE_NOISE_DB", "-30"))      # ¹«À½ ÀÓ°è dB
SILENCE_MIN_SEC  = float(_rhythm_os.getenv("SILENCE_MIN_SEC", "0.25"))      # ÃÖ¼Ò ¹«À½ ±æÀÌ

# [Q5] ÀÚ¸· ¹«À½ ½º³À ÆÄ¶ó¹ÌÅÍ
SUBTITLE_SNAP_WINDOW_SEC       = float(_rhythm_os.getenv("SUBTITLE_SNAP_WINDOW_SEC", "0.6"))
SUBTITLE_LEAD_AFTER_SIL_SEC    = float(_rhythm_os.getenv("SUBTITLE_LEAD_AFTER_SIL_SEC", "0.08"))
SUBTITLE_TAIL_BEFORE_SIL_SEC   = float(_rhythm_os.getenv("SUBTITLE_TAIL_BEFORE_SIL_SEC", "0.05"))


def _detect_audio_silences(audio_path) -> list:
    """
    ffmpeg silencedetect ·Î ¿Àµð¿À ³» ¹«À½ ±¸°£ °ËÃâ.
    Returns: [(start, end), ...] ´ÜÀ§´Â ÃÊ.
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
            f"silencedetect: {len(silences)}°³ ¹«À½ ±¸°£ "
            f"(noise={SILENCE_NOISE_DB}dB, d={SILENCE_MIN_SEC}s)"
        )
        return silences
    except Exception as e:
        logger.warning(f"silencedetect ½ÇÆÐ ? {e}")
        return []


def _find_pause_split(seg_start: float, seg_end: float, ts_data: dict):
    """
    [Q2]+[Q4] segment ³»ºÎ ºÐÇÒ ÁöÁ¡.
    1¼øÀ§: ts_data['audio_silences'] ÀÇ ¹«À½ ±¸°£ Áß°£ (½ÇÁ¦ À½Çâ °ËÃâ, °¡Àå Á¤È®)
    2¼øÀ§: Whisper words °£ 0.3ÃÊ ÀÌ»ó ½°
    ½ÇÆÐ ¡æ None (caller°¡ mid·Î fallback)
    """
    # 1) ½ÇÁ¦ À½Çâ ¹«À½ ¿ì¼± (¾ç ³¡ 1ÃÊ ¿©À¯)
    silences = ts_data.get("audio_silences") or []
    best_t = None
    best_dur = 0.0
    for (s_start, s_end) in silences:
        # ¹«À½ÀÌ segment ³»ºÎ¿¡ °ÉÃÄÀÖÀ¸¸é
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

    # 2) Whisper word gaps (ÇÑ±¹¾î¿¡¼­´Â Á¾Á¾ ¹«¿ëÁö¹°ÀÌÁö¸¸ fallback)
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
# [BL] ÁßÀÇ¾î ´Üµ¶ »ç¿ë ½Ã Pexels ¿ÀÀÎ ? ´ëÃ¼¾î·Î ÀÚµ¿ Ä¡È¯
# [BN-2] MARKER v2
_AMBIGUOUS_REPLACE = {
    "microphone": "building",        # [BN-2] ¸¶ÀÌÅ©¡æ°Ç¹° (podium ½ºÅå ¸¹À¸¸é ¹Ì±¹±â ³ª¿È)
    "press": "journalist",            # press phone ¡æ ±âÀÚ
    "screen": "monitor",              # ÈÞ´ëÆù È­¸é ¡æ ¸ð´ÏÅÍ
    "phone": "",                       # ¸ðÈ£ÇÔ ¡æ Á¦°Å
    "audio": "",                       # À½Çâ ¡æ Á¦°Å
    "recording": "",                   # ³ìÀ½ ¡æ Á¦°Å
    "studio": "",                      # ½ºÆ©µð¿À ¡æ Á¦°Å
    "conference": "meeting",          # È¸ÀÇ
    "speaker": "official",            # [BN-2] politician¡æofficial (podium È¸ÇÇ)
    "podium": "building",             # [BN-2] ¿¬´Ü¡æ°Ç¹° (generic podium US flags)
    "politician": "official",         # [BN-2] Á¤Ä¡ÀÎ¡æ°ø¹«¿ø
    "president": "government",        # [BN-2] ´ëÅë·É¡æÁ¤ºÎ
    "flag": "",                        # [BN-2] flag ´Ü¾î ÀÚÃ¼ Á¦°Å
    # [BP] ¹Ì¼¼¸ÕÁö °ËÁõ ½ÇÆÐ Å°¿öµå
    "pollution": "smog",              # pollution ´Üµ¶¡æsmog (volcano ¹æÁö)
    "polluted": "smog",                #
    "vulnerable": "",                   # Á¦°Å (forest fire ¹æÁö)
    "invisible": "",                    # Á¦°Å
    "particles": "",                    # Á¦°Å (abstract particle)
    "particle": "",                     #
    "quality": "",                      # Á¦°Å (server hardware ¹æÁö)
    "forecast": "weather",              # forecast¡æweather TV screen
    "hardware": "",                     # LLM È¯°¢ °á°ú¹°
    "server": "",                       # µ¿
    "environment": "nature",            # environment¡ænature
    "announcement": "news",             # announcement¡ænews studio
}

# "press conference" °°Àº º¹ÇÕ¾î´Â À¯Áö (ºÐ¸® ±ÝÁö ´ë»ó)
_COMPOUND_KEEP = {
    "press conference",
    "breaking news",
    "white house",
    "president speech",
    "stock market",
    "financial crisis",
}


_ABSTRACT_BLACKLIST = {
    # Ãß»ó ¸í»ç
    "odd", "even", "number", "rule", "exception", "fine", "date",
    "idea", "concept", "type", "way", "form", "part", "thing",
    "issue", "problem", "solution", "factor", "aspect", "matter",
    "process", "case", "method", "system", "structure", "pattern",
    "level", "change", "difference", "step", "point",
    # Ãß»ó µ¿»ç¡¤Çü¿ë»ç (Pexels°¡ Ç³°æÀ¸·Î ¿ÀÇØ¼®)
    "divided", "alternating", "regulated", "announced", "announcement",
    "reduction", "increase", "decrease", "growth", "decline",
    "approach", "practice", "application", "impact", "effect",
    "relationship", "connection", "communication",
    # [BP] MARKER v3
    # [BP] °ËÁõ ½ÇÆÐ °æÇè»ó Pexels °¡ ¹«°ü ¿µ»ó ¹ñ´Â Å°¿öµå
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
        topic_line = f"ÁÖÁ¦: {topic_hint}\n" if topic_hint else ""
        
        prompt = (
            "ÇÑ±¹¾î ¿µ»ó ½ºÅ©¸³Æ®¸¦ Pexels ¿µ¾î °Ë»ö¾î·Î º¯È¯.\n\n"
            + topic_line
            + "ÀüÃ¼ ¸Æ¶ô: " + context_summary + "\n\n"
            "°¢ ¹®Àå¸¶´Ù Pexels¿¡¼­ °¡Àå Àß ¸ÅÄªµÉ ¿µ¾î ¹®±¸ 1°³¸¦ ¸¸µå¼¼¿ä.\n"
            "±ÔÄ¢ (Àý´ë ÁØ¼ö):\n"
            "1. ¹Ýµå½Ã 2~3 ´Ü¾î¸¸ (4´Ü¾î ÀÌ»ó ±ÝÁö, Pexels ¸ÅÄª·ü ¶³¾îÁü)\n"
            "2. Ãß»ó¾î ±ÝÁö: odd, even, rule, number, exception, fine, concept, idea, type, process, method, system, level\n"
            "3. ±¸Ã¼Àû ½Ã°¢ °´Ã¼¡¤Àå¸é¸¸: 'city street traffic cars', 'polluted urban skyline'\n"
            "4. ÁÖÁ¦ÀÇ ¹°¸®Àû Àå¸é ¿¬»ó: ÀÚµ¿Â÷ 2ºÎÁ¦ ¡æ µµ½Ã ±³Åë, ¼¼±Ý ¡æ µ¿Àü °è»ê±â, ¿ìÁÖ ¡æ À§¼º ·ÎÄÏ\n"
            "5. ÇÑ±¹ °³³ä(2ºÎÁ¦¡¤ÁÖ¹Î¹øÈ£¡¤¼ö´É µî)Àº °ü·Ã ½Ã°¢ Àå¸éÀ¸·Î: '2ºÎÁ¦¡æhighway traffic cars', '¼ö´É¡æstudents classroom exam'\n"
            "6. Áßº¹ ±ÝÁö ? ¸ðµç ¾À Å°¿öµå´Â ¼­·Î ´Þ¶ó¾ß ÇÔ. °°Àº ÁÖÁ¦¶óµµ °¢µµ¸¦ ´Ù¸£°Ô (¿¹: ±³Åë/¿îÀü¼®/½ÅÈ£µî/ÁÖÂ÷Àå/¹è±â°¡½º)\n\n"
            "¿¹½Ã (¸í»ç 3-5°³ + Àå¼Ò/»ç¶÷/µ¿ÀÛ):\n"
            "  '¹Ì¼¼¸ÕÁö' ¡æ 'industrial chimney smoke pollution skyline'\n"
            "  'È¦¼ö Â÷·®' ¡æ 'cars city traffic asian street'\n"
            "  '¹øÈ£ÆÇ' ¡æ 'license plate closeup vehicle rear metal'\n"
            "  'Â¦¼ö ¿îÇà' ¡æ 'cars urban road traffic light seoul'\n"
            "  '¹Ì¼¼¸ÕÁö °¨¼Ò' ¡æ 'factory smokestack pollution asian city'\n"
            "  'Á¤ºÎ ¹ßÇ¥' ¡æ 'seoul government building exterior'\n"
            "  '°úÅÂ·á ºÎ°ú' ¡æ 'police officer parking ticket violation'\n"
            "  '±¹È¸ Åë°ú' ¡æ 'korean parliament building asian'\n"
            "  '´ëÅë·É ¿¬¼³' ¡æ 'korean government building exterior'\n"
            "  'Å¥ºê»û' ¡æ 'satellite orbit space earth blue'\n"
            "  '¿ìÁÖ È¯°æ ½ÃÇè' ¡æ 'vacuum chamber engineering lab scientist'\n"
            "³ª»Û ¿¹½Ã (Àý´ë ±ÝÁö):\n"
            "  ? 'cars divided by license plate' ¡æ divided ´Â Ãß»ó (Ç³°æ ¸ÅÄªµÊ)\n"
            "  ? 'cash register money fine penalty' ¡æ µ·/±â°è ¼¯ÀÓ (POS ±â°è ³ª¿È)\n"
            "  ? 'government announcement press conference' ¡æ announcement Ãß»ó (´º½º ±×·¡ÇÈ ³ª¿È)\n\n"
            "ÇÑ±¹ °ü·Ã ÁÖÁ¦¸é 'asian', 'seoul', 'korean' µî Áö¿ª¾î 1°³ Æ÷ÇÔ (±¸Ã¼¼º Çâ»ó).\n"
            "ÇÑ±¹ ÁÖÁ¦ÀÏ ¶§ Àý´ë ±ÝÁö¾î (¼­¾ç ÀÌ¹ÌÁö ³ª¿È): american, usa, us, white house, capitol, trump, biden, obama, union jack, british, britain, eu, european, buckingham.\n"
            "´ëÅë·É¡¤±¹È¸¡¤Á¤ºÎ Àå¸éµµ 'asian president podium' / 'asian parliament building' / 'korean government meeting' ½ÄÀ¸·Î.\n\n"
            "¹®Àå:\n"
            + "\n".join(texts)
            + '\n\nÀÀ´ä: JSON ¹è¿­ ["phrase1", "phrase2", ...] ' + str(len(texts)) + '°³¸¸.\n¹Ýµå½Ã ```json À¸·Î ½ÃÀÛ, ``` À¸·Î ³¡. ¼³¸í¡¤ÁÖ¼® ±ÝÁö, ¿ÀÁ÷ JSON ¹è¿­.\n¿¹½Ã ÀÀ´ä:\n```json\n["traffic cars highway", "factory smoke pollution"]\n```'
        )
        
        # LLM È£Ãâ
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
            logger.warning(f"[BB] LLM È£Ãâ ½ÇÆÐ: {_ex}")
            return {}
        
        # [BD+BH] ÆÄ¼­ °­È­ ? code block ¿ì¼±, non-greedy array, line fallback
        import re as _re
        kws = None
        
        # [BH] ½Ãµµ 0: ```json ... ``` fenced block ¿ì¼±
        fenced = _re.search(r"```(?:json)?\s*\n([\s\S]*?)\n```", raw)
        if fenced:
            inner = fenced.group(1).strip()
            try:
                kws = _json.loads(inner)
            except _json.JSONDecodeError:
                # ¹è¿­¸¸ ÃßÃâ ½Ãµµ
                marr = _re.search(r"\[\s*([\s\S]*?)\s*\]", inner)
                if marr:
                    try:
                        kws = _json.loads("[" + marr.group(1) + "]")
                    except _json.JSONDecodeError:
                        kws = None
        
        # ½Ãµµ 1: non-greedy JSON array (fenced block ¾ø´Â °æ¿ì)
        if not kws:
            m = _re.search(r"\[([^\[\]]*?)\]", raw)
            if m:
                try:
                    kws = _json.loads("[" + m.group(1) + "]")
                except _json.JSONDecodeError:
                    kws = None
        
        # ½Ãµµ 2: ÁÙ ´ÜÀ§ "N. phrase" ¶Ç´Â "N. '...' ¡æ 'phrase'" Çü½Ä
        if not kws:
            extracted = []
            # pattern: N.  ... ¡æ  "phrase"  OR  N. "phrase"
            for line in raw.split("\n"):
                line = line.strip()
                if not line:
                    continue
                # '^\d+\.' prefix È®ÀÎ
                mm = _re.match(r"^\d+[\.\)]\s*(.+)$", line)
                if not mm:
                    continue
                rest = mm.group(1).strip()
                # ¸¶Áö¸· quote ¾ÈÀÇ content »Ì±â
                quoted = _re.findall(r'["\u201c\u201d]([^"\u201c\u201d]{2,80})["\u201c\u201d]', rest)
                if quoted:
                    extracted.append(quoted[-1].strip())
                else:
                    # ¡æ ³ª -> ÀÌÈÄ ºÎºÐ
                    arrow = _re.search(r"[\u2192\-=]>\s*(.+?)$", rest)
                    if arrow:
                        extracted.append(arrow.group(1).strip().strip('"').strip("'"))
                    else:
                        # ±×³É line ÀüÃ¼
                        extracted.append(rest[:80])
            if extracted:
                kws = extracted
                logger.info(f"[BD] markdown list ÆÄ¼­ ¼º°ø: {len(kws)}°³")
        
        if not kws:
            logger.warning(f"[BB] ÀÀ´ä ÆÄ½Ì ½ÇÆÐ: {raw[:200]}")
            return {}
        if not isinstance(kws, list):
            return {}
        
        # °ËÁõ + º¸°­
        result = {}
        fixed = 0
        for i, kw in enumerate(kws[:len(segments)], 1):
            if not (isinstance(kw, str) and kw.strip()):
                continue
            cleaned = kw.strip().lower()[:80]
            words = cleaned.split()
            
            # ´Ü¾î 1°³ Àý´ë °ÅºÎ ¡æ topic_hint ¶Ç´Â descriptionÀ¸·Î È®Àå
            if len(words) < 2:
                seg_text = (segments[i-1].get("text") or "").strip()[:40]
                if topic_hint:
                    cleaned = f"{topic_hint} {cleaned} scene"
                else:
                    cleaned = f"{cleaned} city scene real footage"
                fixed += 1
            # Ãß»ó¾î ºí·¢¸®½ºÆ® ´Ü¾î ºñÀ² > 30% °ÅºÎ (¾ö°ÝÇØÁü)
            # [BQ] MARKER v4
            # [BQ-1] typo/È¯°¢ Á¢¹Ì»ç ÇÊÅÍ ? cleaned ¿¡ ¹Ý¿µ (BP ¹ö±× ¼öÁ¤)
            suspicious_suffixes = ("ererer", "wareer", "warer", "nessness", "mentment", "tiontion")
            words = [w for w in words if not any(s in w for s in suspicious_suffixes)]
            cleaned = " ".join(words).strip()  # [BQ] ÇÊÅÍ °á°ú ¹Ý¿µ
            # [BQ-2] ºñ-ASCII ¹®ÀÚ(ÇÑ±¹¾î ÀÜ·ù) Á¦°Å
            cleaned = "".join(ch for ch in cleaned if ord(ch) < 128).strip()
            while "  " in cleaned:
                cleaned = cleaned.replace("  ", " ")
            words = cleaned.split()
            # [BQ-3] ÇÊÅÍ ÈÄ ´Ü¾î ºÎÁ· ¡æ topic_hint fallback
            if len(words) < 2 and topic_hint:
                cleaned = f"{topic_hint} asian scene"
                words = cleaned.split()
                fixed += 1
            blacklisted = sum(1 for w in words if w in _ABSTRACT_BLACKLIST)
            if blacklisted > 0 and blacklisted / max(1, len(words)) > 0.3:
                # Ãß»ó¾î °ú´Ù ¡æ topic Ãß°¡
                if topic_hint:
                    cleaned = f"{topic_hint} " + " ".join(w for w in words if w not in _ABSTRACT_BLACKLIST)
                cleaned = cleaned.strip()
                fixed += 1
            
            # [BL] ÁßÀÇ¾î Ã³¸® (º¹ÇÕ¾î´Â º¸Á¸, ´Üµ¶Àº Ä¡È¯/Á¦°Å)
            final_words = []
            lower = cleaned.lower()
            # ¸ÕÀú º¹ÇÕ¾î À¯Áö Ã¼Å©
            compound_found = False
            for cmp in _COMPOUND_KEEP:
                if cmp in lower:
                    compound_found = True
                    break
            if not compound_found:
                # ´Ü¾îº°·Î Ã³¸®
                for w in cleaned.split():
                    wl = w.lower()
                    if wl in _AMBIGUOUS_REPLACE:
                        rep = _AMBIGUOUS_REPLACE[wl]
                        if rep:
                            final_words.append(rep)
                        # ºó ¹®ÀÚ¿­ÀÌ¸é Á¦°Å (Ãß°¡ ¾È ÇÔ)
                    else:
                        final_words.append(w)
                cleaned = " ".join(final_words) if final_words else cleaned
            # [BL] ÃÖ´ë 3´Ü¾î·Î ÀÚ¸§ (Pexels ¸ÅÄª·ü ÃÖÀû)
            words_out = cleaned.split()
            if len(words_out) > 3:
                cleaned = " ".join(words_out[:3])
                fixed += 1
            # [BW] MARKER v8
            # [BW] ÇÑ±¹ ÁÖÁ¦ + Korean locale token ºÎÀç ¡æ "hanbok" °­Á¦
            _KOREA_LOCALE = ("korean", "korea", "asian", "seoul", "busan",
                             "jeju", "hanbok", "palace", "gyeongbok",
                             "gyeongju", "bukchon", "insadong")
            _COLOR_MOOD_SOLO = ("modern", "blue", "red", "green", "white",
                                "black", "pink", "yellow", "pastel",
                                "summer", "winter", "spring", "autumn", "fall",
                                "bright", "dark", "soft", "warm", "cool")
            # [BW-FIX] ÇÑ±¹ ¹®È­ ÅäÇÈ¸¸ locale º¸°­, ÀÏ¹Ý ÅäÇÈ °­Á¦ ÁÖÀÔ Á¦°Å
            _KOREA_TOPIC_WORDS = ("ÇÑ±¹", "´ëÇÑ¹Î±¹", "¼­¿ï", "ºÎ»ê", "Á¦ÁÖ", "ÇÑ·ù", "ÇÑº¹", "ÄÉÀÌÆË", "°æº¹±Ã")
            is_kor_culture = bool(topic_hint) and any(kw in (topic_hint or "") for kw in _KOREA_TOPIC_WORDS)
            low_cleaned = cleaned.lower()
            has_locale = any(t in low_cleaned for t in _KOREA_LOCALE)
            # color/mood ¸¸À¸·Î ±¸¼ºµÇ¸é °ÅºÎ ? ÇÑ±¹ ¹®È­ ÅäÇÈ¿¡¸¸ Àû¿ë
            color_only = all(w.lower() in _COLOR_MOOD_SOLO
                             for w in cleaned.split())
            if is_kor_culture and (not has_locale or color_only):
                # color_only ¸é ¿ÏÀü ´ëÃ¼, ¾Æ´Ï¸é prefix
                if color_only:
                    cleaned = "korean hanbok palace"
                else:
                    cleaned = "korean " + cleaned
                    # 3´Ü¾î ÃÊ°ú ½Ã ´Ù½Ã ÀÚ¸§
                    ws = cleaned.split()
                    if len(ws) > 3:
                        cleaned = " ".join(ws[:3])
                fixed += 1
            result[i] = cleaned[:80]
        
        if fixed > 0:
            logger.info(f"[BB+BL] {fixed}°³ Å°¿öµå º¸°­ Àû¿ë (ÁßÀÇ¾î Á¦°Å + 3´Ü¾î cap)")
        # [BC] Áßº¹ Á¦°Å ? µ¿ÀÏ ¹®±¸ ÀÖÀ¸¸é angle º¯Çü suffix Ãß°¡
        seen_phrases = set()
        ANGLES = ["close up", "aerial view", "wide shot", "side angle", "night time",
                 "daytime sunny", "macro detail", "rush hour", "empty lane", "side mirror",
                 "dashboard", "driver seat", "license plate", "tire wheel", "traffic light"]
        _angle_idx = 0
        dedup_fixed = 0
        for i in sorted(result.keys()):
            phrase = result[i]
            if phrase in seen_phrases:
                # Áßº¹! angle Ãß°¡
                suffix = ANGLES[_angle_idx % len(ANGLES)]
                _angle_idx += 1
                # ¹®±¸ Ãà¾à ÈÄ angle º´ÇÕ (´Ü¾î 5°³ À¯Áö)
                parts = phrase.split()[:3]
                phrase = " ".join(parts) + " " + suffix
                result[i] = phrase[:80]
                dedup_fixed += 1
            seen_phrases.add(phrase)
        if dedup_fixed > 0:
            logger.info(f"[BC] Áßº¹ {dedup_fixed}°³ Á¦°Å (angle º¯Çü Àû¿ë)")
        avg_words = sum(len(v.split()) for v in result.values()) / max(1, len(result))
        unique_count = len(set(result.values()))
        logger.info(f"[BB+BC] batch Å°¿öµå: {len(result)}°³, °íÀ¯ {unique_count}°³ (Æò±Õ {avg_words:.1f} ´Ü¾î)")
        return result
    except Exception as e:
        logger.warning(f"[BB] batch Å°¿öµå ½ÇÆÐ: {e}")
        return {}


def rebuild_scenes_from_whisper_segments(scenes, timestamps_path):
    """
    [4] ÀÇ¹Ì ´ÜÀ§ ÀçºÐÇØ + [7] ¸®µë ÄÆ

    Whisper segments°¡ ÀÖÀ¸¸é scenes¸¦ **ÇÑ ¹®Àå=ÇÑ ¾À** ±âÁØÀ¸·Î Àç±¸¼º.
    - °¢ segment¸¦ µ¶¸³ ¾ÀÀ¸·Î »ý¼º
    - duration > SCENE_MAX_SEC ÀÌ¸é 2µîºÐ
    - keyword´Â ¿øº» scenes¿¡¼­ ½Ã°£ ºñÀ²·Î ½Â°è
    - ¼¼±×¸ÕÆ® ºÎÁ· / Å¸ÀÓ½ºÅÆÇÁ ¾øÀ½ ¡æ ¿øº» scenes ±×´ë·Î ¹ÝÈ¯

    Returns: Àç±¸¼ºµÈ ¾À ¸®½ºÆ® (¶Ç´Â ¿øº»)
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
            logger.info("Whisper segments ¾øÀ½ ? ÀÇ¹Ì ÀçºÐÇØ ½ºÅµ")
            return scenes

        source = ts_data.get("source", "unknown")
        logger.info(f"ÀÇ¹Ì ÀçºÐÇØ ½ÃÀÛ: segments={len(segments)} (source={source})")

        # [Q4] ¿Àµð¿À¿¡¼­ ½ÇÁ¦ ¹«À½ ±¸°£ °ËÃâ (¾À ºÐÇÒ Á¤È®µµ Çâ»ó)
        audio_path = ts_data.get("audio_path")
        if audio_path and not ts_data.get("audio_silences"):
            ts_data["audio_silences"] = _detect_audio_silences(audio_path)

        # ¿øº» ¾ÀÀÇ keyword¡¤assetÀ» ½Ã°£ ºñÀ²·Î ½Â°èÇÏ±â À§ÇØ ´©Àû °æ°è °è»ê
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

        # °¢ ¼¼±×¸ÕÆ®¸¦ ¾ÀÀ¸·Î (4ÃÊ ÃÊ°ú ½Ã 2µîºÐ)
        total_audio = float(segments[-1].get("end", 0.0) or 0.0)
        if total_audio <= 0:
            logger.info("Whisper ÃÑ ±æÀÌ 0 ? ÀçºÐÇØ ½ºÅµ")
            return scenes

        # [C-1] segment_keywords °¡ ÀÖÀ¸¸é Å°¿öµå ÀÚµ¿ ±³Ã¼
        segment_keywords = ts_data.get("segment_keywords") or []
        # idx(1-based) ¡æ ["kw1", "kw2"]
        seg_kw_map = {}
        for item in segment_keywords:
            idx = item.get("idx")
            kws = item.get("keywords") or []
            if isinstance(idx, int) and kws:
                seg_kw_map[idx] = kws
        if seg_kw_map:
            logger.info(f"[C-1] Å°¿öµå ¸ÅÇÎ ·Îµå: {len(seg_kw_map)}°³ segment")

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
                    
                    # [BF] ±ä segment (>SCENE_MAX_SEC) ¸¦ 2~3 µîºÐ
                    n_splits = 1
                    if total_dur > SCENE_MAX_SEC * 1.5:
                        n_splits = min(3, int(total_dur / SCENE_MAX_SEC) + 1)
                    
                    # ÅØ½ºÆ®¸¦ ½°Ç¥¡¤¸¶Ä§Ç¥·Î ´ëÃæ split (n_splits ¸¸Å­)
                    if n_splits > 1:
                        import re as _re_split
                        phrases = _re_split.split(r"[,¡¤.¡¢]\s*", seg_text)
                        phrases = [p.strip() for p in phrases if p.strip()]
                        if not phrases or len(phrases) < n_splits:
                            # fallback: char-based split
                            L = len(seg_text)
                            phrases = [seg_text[k*L//n_splits : (k+1)*L//n_splits] for k in range(n_splits)]
                            phrases = [p.strip() for p in phrases if p.strip()]
                        else:
                            # phrase°¡ ¸¹À¸¸é n_splits ¸¸Å­ ÇÕÄ¡±â
                            per = max(1, len(phrases) // n_splits)
                            phrases = [" ".join(phrases[k*per:(k+1)*per]) for k in range(n_splits)]
                    else:
                        phrases = [seg_text]
                    
                    # °¢ phrase¸¦ sceneÀ¸·Î
                    for j, phrase in enumerate(phrases):
                        if not phrase:
                            continue
                        scene_counter += 1
                        sub_dur = total_dur / len(phrases)
                        rel_mid = ((seg_start + seg_end) / 2.0) / max(0.1, audio_end)
                        orig = pick_orig_scene(min(1.0, rel_mid))
                        _phrase_text = phrase or orig.description or ""
                        update = {
                            "scene_id": f"ws_{scene_counter}",
                            "scene_number": scene_counter,
                            "description": _phrase_text,
                            "duration_seconds": round(sub_dur, 2),
                        }
                        seg_idx_1based = i + 1
                        if seg_idx_1based in seg_kw_map:
                            kws = seg_kw_map[seg_idx_1based]
                            if kws:
                                update["keyword"] = kws[0]
                                update["asset_url"] = None
                        else:
                            # [v16.4 SYNC] ³ª·¹ÀÌ¼Ç ÅØ½ºÆ® ¡æ ¿µ¾î ½Ã°¢ Å°¿öµå ÀÚµ¿ »ý¼º
                            _vq = _ko_narration_to_visual_query(_phrase_text, orig.keyword or "", "")
                            update["keyword"] = _vq
                            update["asset_url"] = None  # »õ Å°¿öµå·Î Àç°Ë»ö
                        aligned_scenes.append(orig.model_copy(update=update))
                total = sum(s.duration_seconds for s in aligned_scenes)
                logger.info(
                    f"[AH-4] Whisper-first Á¤·Ä: {len(aligned_scenes)}¾À, ÃÑ {total:.2f}s "
                    f"(audio_end={audio_end:.2f}s, gap Èí¼ö ¿Ï·á) # [AH-4] MARKER v1"
                )
                # [v16.1] alt_asset_url 3n¸¶´Ù ±³¹ø
                for _ai, _sc in enumerate(aligned_scenes):
                    if _ai % 3 == 2:
                        _alt = getattr(_sc, "alt_asset_url", None)
                        if _alt and Path(_alt).exists() and Path(_alt).stat().st_size > 4096:
                            aligned_scenes[_ai] = _sc.model_copy(update={"asset_url": _alt, "alt_asset_url": _sc.asset_url})
                return aligned_scenes
            except Exception as _ah4_err:
                logger.warning(f"[AH-4] Whisper-first ½ÇÆÐ, ±âÁ¸ °æ·Î·Î fallback: {_ah4_err}")

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

            # ¸®µë ÄÆ: 4ÃÊ ÃÊ°ú¸é ºÐÇÒ
            # [Q2] words °£ ½°(¡Ã PAUSE_THRESHOLD_SEC) ÁöÁ¡¿¡¼­ ºÐÇÒ ½Ãµµ
            subclips = []
            if seg_dur > SCENE_MAX_SEC:
                pause_t = _find_pause_split(seg_start, seg_end, ts_data)
                split_t = pause_t if pause_t is not None else seg_start + seg_dur / 2.0
                reason = "pause" if pause_t is not None else "mid"
                logger.info(
                    f"  segment [{seg_start:.2f}-{seg_end:.2f}] ºÐÇÒ ({reason}): {split_t:.2f}"
                )
                subclips.append((seg_start, split_t, seg_text))
                subclips.append((split_t, seg_end, seg_text))
            else:
                subclips.append((seg_start, seg_end, seg_text))

            for (sub_start, sub_end, sub_text) in subclips:
                sub_dur = max(SCENE_MIN_SEC * 0.5, sub_end - sub_start)  # ÃÖ¼Ò 1ÃÊ´Â ³²±è
                # ¿øº» ¾À¿¡¼­ keyword ½Â°è (½Ã°£ ºñÀ² ±â¹Ý)
                rel_mid = ((sub_start + sub_end) / 2.0) / total_audio
                orig = pick_orig_scene(rel_mid)
                seg_counter += 1

                # [C-1] segment_keywords °¡ ÀÖÀ¸¸é keyword ±³Ã¼ (½Ã°¢Àû ¸ÅÄª)
                kw_override = None
                asset_override = None
                if seg_idx in seg_kw_map:
                    kws = seg_kw_map[seg_idx]
                    # Ã¹ Å°¿öµå¸¦ ¸ÞÀÎ, µÎ ¹øÂ°´Â fallback
                    kw_override = kws[0] if kws else None
                    # ¿øº» asset_url Àº ¸®¼Â (»õ Å°¿öµå·Î Àç´Ù¿î·Îµå µÇµµ·Ï)
                    asset_override = None

                _seg_desc = sub_text or orig.description or ""
                update_dict = {
                    "scene_id": f"{orig.scene_id}_seg{seg_counter}",
                    "duration_seconds": round(sub_dur, 2),
                    "description": _seg_desc,
                }
                if kw_override:
                    update_dict["keyword"] = kw_override
                    update_dict["asset_url"] = None  # Àç°Ë»ö Æ®¸®°Å
                else:
                    # [v16.4 SYNC] ³ª·¹ÀÌ¼Ç ÅØ½ºÆ® ¡æ ¿µ¾î ½Ã°¢ Å°¿öµå ÀÚµ¿ »ý¼º
                    _vq2 = _ko_narration_to_visual_query(_seg_desc, orig.keyword or "", "")
                    update_dict["keyword"] = _vq2
                    update_dict["asset_url"] = None
                # [AH-1] attach absolute start/end from Whisper for gap absorption
                update_dict["scene_number"] = seg_counter
                sc_new = orig.model_copy(update=update_dict)
                # store abs times in a parallel list (cannot extend Pydantic model without schema change)
                _scene_abs_times.append((float(sub_start), float(sub_end)))
                new_scenes.append(sc_new)

        if not new_scenes:
            logger.info("ÀçºÐÇØ °á°ú ¾øÀ½ ? ¿øº» »ç¿ë")
            return scenes

        # ¦¡¦¡ [C] ÂªÀº ¾À ÀÎÁ¢ º´ÇÕ (SCENE_MIN_SEC ¹Ì¸¸) ¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡
        merged = []
        for sc in new_scenes:
            if merged and sc.duration_seconds < SCENE_MIN_SEC:
                prev = merged[-1]
                combined = round(prev.duration_seconds + sc.duration_seconds, 2)
                # ¿©ÀüÈ÷ ³Ê¹« Å©¸é º´ÇÕÇÏÁö ¾ÊÀ½ (SCENE_MAX_SEC ÃÊ°ú ¹æÁö)
                if combined <= SCENE_MAX_SEC * 1.25:
                    # º´ÇÕ: ÀÌÀü ¾ÀÀÇ duration È®Àå + description ÀÌ¾îºÙÀÌ±â
                    new_desc = prev.description or ""
                    if sc.description and sc.description.strip() and sc.description.strip() != prev.description:
                        new_desc = (new_desc + " " + sc.description).strip() if new_desc else sc.description
                    merged[-1] = prev.model_copy(update={
                        "duration_seconds": combined,
                        "description": new_desc,
                    })
                    continue
            merged.append(sc)

        # Ã¹ ¾ÀÀÌ ³Ê¹« ÂªÀ¸¸é ´ÙÀ½ ¾À¿¡ º´ÇÕ
        if len(merged) >= 2 and merged[0].duration_seconds < SCENE_MIN_SEC:
            first = merged.pop(0)
            nxt = merged[0]
            combined = round(first.duration_seconds + nxt.duration_seconds, 2)
            if combined <= SCENE_MAX_SEC * 1.25:
                new_desc = first.description or ""
                if nxt.description and nxt.description.strip() and nxt.description.strip() != first.description:
                    new_desc = (new_desc + " " + nxt.description).strip() if new_desc else nxt.description
                merged[0] = nxt.model_copy(update={
                    "scene_id": first.scene_id,  # Ã¹ ¾À ID À¯Áö
                    "duration_seconds": combined,
                    "description": new_desc,
                    "keyword": first.keyword,    # Å°¿öµåµµ Ã¹ ¾À °Í ½Â°è
                })
            else:
                merged.insert(0, first)  # º´ÇÕ ¾È ÇÏ°í µÇµ¹¸²

        if len(merged) != len(new_scenes):
            logger.info(
                f"ÂªÀº ¾À º´ÇÕ: {len(new_scenes)}¾À ¡æ {len(merged)}¾À "
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
                    f"[AH-1] gap Èí¼ö ¿Ï·á: {len(merged)}¾À, ÃÑ {total_after:.2f}s "
                    f"(audio_end={audio_end_abs:.2f}s, Àý´ë½Ã°£ Á¤·Ä)"
                )
            except Exception as _gap_err:
                logger.warning(f"[AH-1] gap Èí¼ö ½ÇÆÐ: {_gap_err}")

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
                logger.info(f"[AF-7] SCENE_LEAD={lead}s / {n}¾À ? Ã¹¾À +{lead}s / ³ª¸ÓÁö -{per:.3f}s (ÃÑ±æÀÌ º¸Á¸)")
            except Exception as _lead_err:
                logger.warning(f"[AD] scene lead Àû¿ë ½ÇÆÐ (¹«½Ã): {_lead_err}")

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
                            f"[AF-11] ¿Àµð¿À tail +{extra}s ¡æ ¸¶Áö¸· ¾À ¿¬Àå "
                            f"(audio={total_audio_real:.2f}s scenes={total_scenes:.2f}s)"
                        )
        except Exception as _tail_err:
            logger.debug(f"[AF-11] tail-extend skip: {_tail_err}")

        # [S] Àå¸é ±æÀÌ º¯ÁÖ (Æí¾ÈÇÑ ¸®µë ? ³Ê¹« ±ÕÀÏÇÏ¸é ¾î»ö, ³Ê¹« µéÂß³¯Âßµµ ºÒ¾È)
        if SCENE_LEN_VARIANCE > 0 and len(merged) > 1:
            import random as _scene_rnd
            _scene_rnd.seed(42)  # °áÁ¤Àû º¯ÁÖ (°°Àº ¾À ¡æ °°Àº º¯ÁÖ)
            varied = []
            cum_orig = sum(s.duration_seconds for s in merged)
            for s in merged:
                # ¡¾variance ¹üÀ§¿¡¼­ ·£´ý
                offset = _scene_rnd.uniform(-SCENE_LEN_VARIANCE, SCENE_LEN_VARIANCE)
                new_dur = max(SCENE_MIN_SEC, min(SCENE_MAX_SEC + 0.5, s.duration_seconds + offset))
                varied.append(s.model_copy(update={"duration_seconds": round(new_dur, 2)}))
            # ÀüÃ¼ ±æÀÌ º¸Á¤ (TTS ¿Í ¸ÂÃã)
            cum_new = sum(s.duration_seconds for s in varied)
            if cum_new > 0 and abs(cum_new - cum_orig) > 0.5:
                ratio = cum_orig / cum_new
                varied = [
                    s.model_copy(update={"duration_seconds": round(s.duration_seconds * ratio, 2)})
                    for s in varied
                ]
            logger.info(f"[S] Àå¸é ±æÀÌ º¯ÁÖ: ¡¾{SCENE_LEN_VARIANCE}s")
            merged = varied

        logger.info(
            f"ÀÇ¹Ì ÀçºÐÇØ ¿Ï·á: {len(scenes)}¾À ¡æ {len(merged)}¾À "
            f"(ÃÑ {sum(s.duration_seconds for s in merged):.1f}s / TTS {total_audio:.1f}s)"
        )
        return merged

    except Exception as e:
        logger.error(f"ÀÇ¹Ì ÀçºÐÇØ ¿À·ù (¿øº» »ç¿ë): {e}", exc_info=True)
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
                    if match in ("ÀÔ´Ï´Ù", "ÇÕ´Ï´Ù", "½À´Ï´Ù", "µË´Ï´Ù", "ÀÖ½À", "¾ø½À", "ÇÏ¿©", "À¸·Î", "¿¡¼­", "ÀÌ°í", "º¸°Ú", "»ìÆì", "º¸°í"):
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
            logger.info(f"[AF-8] ÀÚ¸· Å°¿öµå °­Á¶: {changed}È¸ Ä¡È¯ ({len(keywords)}°³ ÈÄº¸)")
            return True
        else:
            logger.info(f"[AF-8] Å°¿öµå {len(keywords)}°³ ÈÄº¸ ÀÖÀ¸³ª ÀÚ¸·¿¡ ¸ÅÄ¡ ¾øÀ½")
    except Exception as e:
        logger.warning(f"[AF-8] ÀÚ¸· °­Á¶ ½ÇÆÐ (¹«½Ã): {e}")
    return False


async def generate_tts_for_job(job_id: str, scenes: list, request) -> bool:
    """[DEPRECATED] ensure_tts_assets() ·¡ÆÛ ? ÇÏÀ§È£È¯¿ë"""
    r = await ensure_tts_assets(job_id, scenes, request)
    return r.get("ok", False)



def _post_process_narration_quality(text: str) -> str:
    """[v15.96] 100¸¸ºä ³ª·¹ÀÌ¼Ç Ç°Áú ÈÄÃ³¸® ? ¸ÂÃã¹ý/Ç¥±â ÅëÀÏ/±æÀÌ ÃÖÀûÈ­"""
    if not text:
        return text
    # 1. ¸¶Ä¿ ÀÜÀç Á¦°Å
    text = re.sub(r'\[ÇÏÀÌ¶óÀÌÆ®:\s*[^\]]*\]', '', text)
    text = re.sub(r'\[.*?\]', '', text)  # ±âÅ¸ ¸¶Ä¿
    # 2. ¼ýÀÚ Ç¥±â ÅëÀÏ: 1000000 ¡æ 100¸¸, 1000 ¡æ 1Ãµ
    def _num_ko(m):
        n = int(m.group().replace(',',''))
        if n >= 100_000_000: return f"{n//100_000_000}¾ï"
        if n >= 10_000:      return f"{n//10_000}¸¸{n%10_000//1000 and str(n%10_000//1000)+'Ãµ' or ''}"
        if n >= 1_000:       return f"{n//1000}Ãµ"
        return m.group()
    text = re.sub(r'\d{4,}(?:,\d{3})*', _num_ko, text)
    # 3. ¹®Àå ³¡ ¸¶Ä§Ç¥ ÅëÀÏ (¾øÀ¸¸é Ãß°¡)
    sentences = [s.strip() for s in re.split(r'(?<=[.!?])\s+', text) if s.strip()]
    fixed = []
    for s in sentences:
        if s and not s[-1] in '.!?':
            s += '.'
        fixed.append(s)
    text = ' '.join(fixed)
    # 4. ÀÌÁß °ø¹é Á¦°Å
    text = re.sub(r'\s{2,}', ' ', text).strip()
    # 5. ¹®Àå ±æÀÌ Ã¼Å©: ³Ê¹« ÂªÀ¸¸é (5ÀÚ ¹Ì¸¸) ÇÊÅÍ
    if len(text.replace(' ','')) < 5:
        return ''
    return text

async def ensure_tts_assets(job_id: str, scenes: list, request) -> dict:
    """
    [v15.59.0] TTS mp3 + timestamps.json µ¿½Ã º¸Àå.
    mp3¸¸ ÀÖ°í timestamps ¾øÀ¸¸é lf2_tts ÀçÈ£Ãâ.
    audio_url ÀÖÀ¸¸é HEAD °ËÁõ.
    ¹ÝÈ¯: {"ok": bool, "mp3_path": Path|None, "ts_path": Path|None,
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

    # 1. mp3 + timestamps ¸ðµÎ Á¤»ó ¡æ Àç»ç¿ë
    if (mp3_path.exists() and mp3_path.stat().st_size > 1024
            and ts_path.exists() and _ts_valid(ts_path)):
        logger.info(f"[TTS] Àç»ç¿ë: {mp3_path.stat().st_size//1024}KB + timestamps OK")
        return {"ok": True, "mp3_path": mp3_path, "ts_path": ts_path,
                "error_code": None, "retryable": False}

    # 2. mp3¸¸ ÀÖ°í timestamps ¾øÀ½ ¡æ Àç»ý¼º ÇÊ¿ä
    if mp3_path.exists() and mp3_path.stat().st_size > 1024:
        logger.warning(f"[TTS] mp3 Á¸ÀçÇÏ³ª timestamps ¾øÀ½ ¡æ Àç»ý¼º: {job_id}")

    # 3. audio_url ÀÖÀ¸¸é HEAD °ËÁõ
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
    _seen_narr: set = set()
    for s in scenes:
        s_dict = s.model_dump() if hasattr(s, "model_dump") else (s if isinstance(s, dict) else {})
        text = (s_dict.get("narration") or s_dict.get("description") or s_dict.get("keyword") or "").strip()
        text = re.sub(r'\[ÇÏÀÌ¶óÀÌÆ®:\s*[^\]]*\]', '', text).strip()  # [v15.94]
        text = _post_process_narration_quality(text)  # [v15.96] Ç°Áú ÈÄÃ³¸®
        if text and text not in _seen_narr:  # [v15.92] Áßº¹ ³ª·¹ÀÌ¼Ç Á¦°Å
            narration_parts.append(text)
            _seen_narr.add(text)

    if not narration_parts:
        logger.warning("[TTS] ³ª·¹ÀÌ¼Ç ¾øÀ½ ? validation error")
        return {"ok": False, "error_code": "TTS_NARRATION_EMPTY",
                "message": "¸ðµç ¾À¿¡ narration/description ¾øÀ½", "retryable": False}

    full_script = " ".join(narration_parts)
    # [v15.94] [ÇÏÀÌ¶óÀÌÆ®:] ¸¶Ä¿ Á¦°Å ? ÀÚ¸·¡¤TTS¿¡¼­ ±úÁø ÅØ½ºÆ® ¹æÁö
    full_script = re.sub(r'\[ÇÏÀÌ¶óÀÌÆ®:\s*[^\]]*\]', '', full_script).strip()
    logger.info(f"[TTS-AUTO] TTS »ý¼º: {len(full_script)}ÀÚ / {len(narration_parts)}¾À")

    try:
        import httpx as _httpx
        payload = {
            "text": full_script,
            "filename": job_id,
            "engine": "edge",
            "edge_voice": "ko-KR-SunHiNeural",
            "edge_rate": "+15%",    # [v16.8] Voice Director ÃÖ´ë ¼Óµµ 15% Àû¿ë (ÀÌÀü: +10%)
            "preprocess": True,
        }
        async with _httpx.AsyncClient(timeout=300.0) as _cli:
            resp = await _cli.post("http://lf2_tts:8001/tts", json=payload)
            if resp.status_code != 200:
                logger.error(f"[TTS-AUTO] TTS ¼­ºñ½º ¿À·ù: {resp.status_code} {resp.text[:200]}")
                return False
            data = resp.json()

        tts_file = data.get("file_path", "")
        ts_file  = data.get("timestamps_path", "")
        import shutil as _shutil

        if tts_file and Path(tts_file).exists():
            if Path(tts_file).resolve() != Path(mp3_path).resolve():  # [v15.92] SameFileError ¹æ¾î
                _shutil.copy2(tts_file, mp3_path)
            logger.info(f"[TTS] mp3 ÀúÀå: {mp3_path.stat().st_size//1024}KB")
        else:
            logger.error(f"[TTS] mp3 ÆÄÀÏ ¾øÀ½: {tts_file}")
            return {"ok": False, "error_code": "TTS_MP3_MISSING",
                    "message": f"lf2_tts mp3 ¾øÀ½: {tts_file}", "retryable": True}

        if ts_file and Path(ts_file).exists():
            if Path(ts_file).resolve() != Path(ts_path).resolve():  # [v16.5] SameFileError ¹æ¾î
                _shutil.copy2(ts_file, ts_path)
            logger.info("[TTS] timestamps ÀúÀå OK")
            return {"ok": True, "mp3_path": mp3_path, "ts_path": ts_path,
                    "error_code": None, "retryable": False}
        else:
            logger.warning("[TTS] timestamps ¾øÀ½ ? ASS ÀÚ¸· ºÒ°¡")
            return {"ok": True, "mp3_path": mp3_path, "ts_path": None,
                    "error_code": "TTS_TIMESTAMP_MISSING", "retryable": False}

    except Exception as _e:
        logger.error(f"[TTS] »ý¼º ½ÇÆÐ: {_e}", exc_info=True)
        return {"ok": False, "error_code": "TTS_SERVICE_ERROR",
                "message": str(_e), "retryable": True}


def _build_narration_vocab(scenes_json_path) -> set:
    """
    Load narration text from scenes.json and return a set of unique word tokens.
    Used as ground-truth vocabulary for Whisper spell correction.
    """
    import json as _json, re as _re
    try:
        with open(scenes_json_path, encoding="utf-8") as f:
            scenes = _json.load(f)
        text = " ".join(
            s.get("narration", "") for s in scenes
            if isinstance(s, dict) and s.get("narration")
        )
        # tokenize: Korean syllables, alphanumeric (handles '1À§¸¦', 'AI' etc.)
        tokens = _re.findall(r"[°¡-ÆRa-zA-Z0-9]+", text)
        return set(tokens)
    except Exception:
        return set()


def _correct_whisper_word(word: str, narration_vocab: set) -> str:
    """
    Correct a single Whisper-transcribed word against the narration vocabulary.

    Steps:
      1. Strip punctuation to get a bare token for matching.
      2. If the bare token exists in narration_vocab ¡æ no change.
      3. Use difflib fuzzy match (cutoff=0.75) against vocab.
      4. If match found, replace the stripped portion in the original word.
      5. Fallback: return original word unchanged.

    Cutoff 0.75 avoids over-correction (e.g., 'ÀÖ´Â' ¡æ 'ÀÖ½À´Ï´Ù').
    """
    import difflib, re as _re
    bare = _re.sub(r"[^°¡-ÆRa-zA-Z0-9]", "", word)
    if not bare or bare in narration_vocab:
        return word
    matches = difflib.get_close_matches(bare, narration_vocab, n=1, cutoff=0.75)
    if matches:
        corrected = word.replace(bare, matches[0], 1)
        logger.debug(f"[SPELLFIX] {word!r} ¡æ {corrected!r} (matched: {matches[0]!r})")
        return corrected
    return word


def create_ass_karaoke_from_whisper(timestamps_path, output_path, lead_sec: float = 0.0, speed_factor: float = 0.0) -> bool:
    """
    [SUBTITLE] Whisper word-level timestamps ¡æ ASS subtitle.
    Single-color yellow text (no karaoke \\kf dual-layer rendering).
    PlayResX=1920, PlayResY=1080. Bold yellow with black outline.

    Spell correction: automatically loads scenes.json narration vocabulary
    from /data/jobs/{job_id}/scenes.json (inferred from timestamps filename)
    and applies difflib fuzzy correction to Whisper transcription errors.
    """
    import json as _json
    MAX_CHARS = 20  # max chars per line for Korean

    try:
        if not timestamps_path or not Path(timestamps_path).exists():
            return False

        with open(timestamps_path, encoding="utf-8") as f:
            ts_data = _json.load(f)

        # collect words: words ¡æ segments.words ¡æ segments fallback
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

        # ¦¡¦¡ Narration-based spell correction ¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡
        # Infer job_id from timestamps filename: "{job_id}_timestamps.json"
        # then load /data/jobs/{job_id}/scenes.json as ground-truth vocabulary.
        narration_vocab: set = set()
        try:
            ts_stem = Path(timestamps_path).stem          # e.g. "auto_20260428_..._timestamps"
            job_id_guess = ts_stem.replace("_timestamps", "")
            scenes_json = Path("/data/jobs") / job_id_guess / "scenes.json"
            if scenes_json.exists():
                narration_vocab = _build_narration_vocab(scenes_json)
                logger.info(f"[SPELLFIX] ³ª·¹ÀÌ¼Ç ¾îÈÖ {len(narration_vocab)}°³ ·Îµå ¡æ ÀÚµ¿ ±³Á¤ È°¼ºÈ­")
            else:
                logger.debug(f"[SPELLFIX] scenes.json ¾øÀ½ ({scenes_json}) ¡æ ±³Á¤ °Ç³Ê¶Ü")
        except Exception as _se:
            logger.warning(f"[SPELLFIX] ¾îÈÖ ·Îµå ½ÇÆÐ (¹«½Ã): {_se}")

        # Apply spell correction to raw word list before grouping
        corrected_words = []
        fix_count = 0
        for w in words:
            wt = (w.get("word") or "").strip()
            if not wt:
                corrected_words.append(w)
                continue
            if narration_vocab:
                wt_fixed = _correct_whisper_word(wt, narration_vocab)
                if wt_fixed != wt:
                    fix_count += 1
                    w = dict(w)          # shallow copy ? don't mutate original
                    w["word"] = wt_fixed
            corrected_words.append(w)
        if fix_count:
            logger.info(f"[SPELLFIX] Whisper ±³Á¤ {fix_count}°³ ´Ü¾î Àû¿ë")

        def _t(sec):
            sec = max(0.0, float(sec) + lead_sec)
            h = int(sec // 3600); m = int((sec % 3600) // 60); s = sec % 60
            return f"{h}:{m:02d}:{s:05.2f}"

        # Single-color yellow: PrimaryColour == SecondaryColour ¡æ no dual-layer artifact
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
            "Style: Default,Noto Sans CJK KR,48,&H00FFFF00,&H00FFFF00,&H00000000,&HB4000000,"
            "-1,0,0,0,100,100,0,0,1,2.5,1.2,2,80,80,50,1\n\n"
            "[Events]\n"
            "Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text\n"
        )

        # group corrected words into subtitle lines
        groups, cur, cur_len = [], [], 0
        for w in corrected_words:
            wt = (w.get("word") or "").strip()
            if not wt:
                continue
            if cur_len + len(wt) > MAX_CHARS and cur:
                groups.append(cur); cur = []; cur_len = 0
            cur.append(w); cur_len += len(wt) + 1
        if cur:
            groups.append(cur)

        # [v16.11] subtitle_speed: +0.15 = 15% faster (scale 1/1.15), -0.10 = 10% slower (scale 1/0.9)
        _spd_scale = 1.0 / max(1.0 + speed_factor, 0.1) if speed_factor != 0.0 else 1.0

        dialogues = []
        for grp in groups:
            ls = grp[0].get("start", 0) * _spd_scale
            le = grp[-1].get("end", ls + 3) * _spd_scale
            # plain text ? no \kf karaoke tags (prevents double-subtitle rendering)
            line_words = []
            for w in grp:
                _raw_w = (w.get('word') or '').strip()
                _raw_w = inject_flags_in_word(_raw_w)
                line_words.append(_raw_w)
            plain_text = " ".join(line_words)
            dialogues.append(
                f"Dialogue: 0,{_t(ls)},{_t(le)},Default,,0,0,0,,{plain_text}"
            )

        Path(output_path).write_text(ass_header + "\n".join(dialogues) + "\n", encoding="utf-8-sig")
        logger.info(f"[SUBTITLE] ASS »ý¼º: {len(dialogues)}ÁÙ ¡æ {output_path}")
        return True

    except Exception as _e:
        logger.error(f"[SUBTITLE] ASS »ý¼º ½ÇÆÐ: {_e}", exc_info=True)
        return False


def create_srt_from_whisper_segments(timestamps_path, output_path, lead_sec: float = None) -> bool:
    """
    [8] ÀÚ¸· 0.15ÃÊ ¼±Çà

    Whisper segments ±â¹Ý SRT »ý¼º. °¢ cueÀÇ start¸¦ lead_sec ¸¸Å­ ´ç°Ü¼­
    À½¼ºº¸´Ù ¸ÕÀú ÀÚ¸·ÀÌ ¶ß°Ô ÇÔ.

    Returns: »ý¼º ¼º°ø ¿©ºÎ
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

        # [Q5] ¿Àµð¿À ¹«À½ ±¸°£ ·Îµå (Ä³½Ã°¡ ÀÖÀ¸¸é Àç»ç¿ë, ¾øÀ¸¸é Á÷Á¢ °ËÃâ)
        audio_silences = ts_data.get("audio_silences") or []
        if not audio_silences:
            ap = ts_data.get("audio_path")
            if ap:
                audio_silences = _detect_audio_silences(ap)

        def _snap_to_silence(t: float, is_start: bool) -> float:
            """
            t ¿¡ °¡Àå °¡±î¿î ¹«À½ °æ°è¸¦ Ã£¾Æ ½º³À.
            is_start=True  : ½ÃÀÛ ¡æ °¡Àå °¡±î¿î ¹«À½ end + LEAD
            is_start=False : ³¡   ¡æ °¡Àå °¡±î¿î ¹«À½ start - TAIL

            À©µµ¿ì ³» ¿©·¯ ¹«À½ÀÌ ÀÖÀ¸¸é °¡Àå °¡±î¿î °Í ¼±ÅÃ.
            t °¡ ¹«À½ ¾ÈÂÊÀÌ¸é ÇØ´ç ¹«À½ÀÇ °æ°è·Î ¿ì¼± ½º³À.
            """
            if not audio_silences:
                return t
            win = SUBTITLE_SNAP_WINDOW_SEC

            # t °¡ ¹«À½ ³»ºÎ¿¡ ÀÖ´ÂÁö ¸ÕÀú È®ÀÎ
            for (s, e) in audio_silences:
                if s - 0.05 <= t <= e + 0.05:
                    # ¹«À½ ¾ÈÂÊ ¡æ ½ÃÀÛÀÌ¸é ¹«À½ end + lead, ³¡ÀÌ¸é ¹«À½ start - tail
                    if is_start:
                        return round(e + SUBTITLE_LEAD_AFTER_SIL_SEC, 3)
                    else:
                        return round(s - SUBTITLE_TAIL_BEFORE_SIL_SEC, 3)

            best = None
            best_diff = 1e9
            if is_start:
                # °¡Àå °¡±î¿î ¹«À½ÀÇ end ¸¦ Ã£À½ (t ¾ÕµÚ win ¹üÀ§ ³»)
                for (s, e) in audio_silences:
                    if abs(e - t) <= win:
                        diff = abs(e - t)
                        if diff < best_diff:
                            best_diff = diff
                            best = e
                if best is not None:
                    return round(best + SUBTITLE_LEAD_AFTER_SIL_SEC, 3)
            else:
                # °¡Àå °¡±î¿î ¹«À½ÀÇ start ¸¦ Ã£À½
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

        # [Q3] º¹ÇÕ¾î º¸È£ + ÀÚ¿¬ ÁÙ¹Ù²Þ
        def wrap_lines(text: str, max_chars: int = SUBTITLE_MAX_CHARS) -> list:
            text = text.strip()
            if len(text) <= max_chars:
                return [text]
            # 1) NO_BREAK_TERMS ³»ºÎ °ø¹éÀ» NBSP·Î Ä¡È¯ ¡æ split ½Ã ÇÑ ÅäÅ«À¸·Î À¯Áö
            protected = text
            for term in NO_BREAK_TERMS:
                if term and term in protected:
                    protected = protected.replace(term, term.replace(" ", _NBSP))
            # 2) ½°Ç¥ µÚ¿¡ °ø¹é º¸Àå
            protected = protected.replace(",", ", ")
            # split(" ") ·Î NBSP ºÐÇÒ ¹æÁö (NBSP´Â whitespace·Î ÀÎ½ÄµÇÁö¸¸ °ø¹é " " ¹®ÀÚ´Â ¾Æ´Ô)
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
            # NBSP º¹¿ø
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
            for p in (",", ".", "!", "?", "¡£", "¡¢"):
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
                f"[AG-1] word-level ÀÚ¸· Å¸ÀÌ¹Ö: {word_timing_matches}/{word_timing_total*2} "
                f"¸ÅÄª ({len(all_words)} words »ç¿ë)"
            )

        if not cues:
            return False

        # [Q5] ÀÚ¸· ¹«À½ ½º³À + ¼±Çà Àû¿ë
        #   1. ¸ÕÀú °¢ cueÀÇ start/end ¸¦ ¹«À½¿¡ ½º³À ½Ãµµ
        #   2. ½º³À ½ÇÆÐÇÑ ºÎºÐ¸¸ ±âÁ¸ lead_sec ¹æ½Ä Àû¿ë
        #   3. ÀÌÀü cue end º¸´Ù ¾Õ¼­Áö ¾Êµµ·Ï º¸Á¤
        snapped_count = 0
        adjusted = []
        prev_end = 0.0
        for (start, end, chunk) in cues:
            snap_s = _snap_to_silence(start, is_start=True)
            snap_e = _snap_to_silence(end, is_start=False)

            # ½º³À °á°ú°¡ ¿øº»°ú ´Ù¸£¸é Ä«¿îÆ®
            used_snap_s = abs(snap_s - start) > 0.01
            used_snap_e = abs(snap_e - end) > 0.01
            if used_snap_s or used_snap_e:
                snapped_count += 1

            # ½º³À ½ÇÆÐ ½Ã lead/tail fallback
            adj_start = snap_s if used_snap_s else max(prev_end, start - lead_sec)
            adj_start = max(prev_end, adj_start)  # °ãÄ§ ¹æÁö
            adj_end = snap_e if used_snap_e else max(adj_start + 0.3, end - 0.05)
            if adj_end <= adj_start:
                adj_end = adj_start + 0.3
            adjusted.append((adj_start, adj_end, chunk))
            prev_end = adj_end + 0.10  # [AI-9] min 0.1s gap between cues

        if snapped_count > 0:
            logger.info(
                f"ÀÚ¸· ¹«À½ ½º³À: {snapped_count}/{len(cues)} cue "
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
            f"Whisper SRT »ý¼º: {len(adjusted)}°³ cue, lead={lead_sec:.2f}s, "
            f"max_chars={SUBTITLE_MAX_CHARS}"
        )
        return True

    except Exception as e:
        logger.error(f"Whisper SRT »ý¼º ¿À·ù: {e}", exc_info=True)
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



# [AX] Watermark ¿ÏÀü ºñÈ°¼º - Ç×»ó no-op
def apply_watermark(input_path: Path, output_path: Path) -> bool:
    """[AX] Watermark ±â´ÉÀº ºñÈ°¼ºÈ­µÊ. Ç×»ó False ¹ÝÈ¯ (¿µ»ó¿¡ ·Î°í/±×¸² ¿À¹ö·¹ÀÌ ¾øÀ½)."""
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
        logger.debug(f"[AU-5] credits log ½ÇÆÐ: {e}")


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
        logger.warning(f"µ¿½Ã ½ÇÇà °ÅºÎ (Redis lock): {job_id}")
        await update_job_status(job_id, JobStatus.FAILED, error="´Ù¸¥ Àâ Ã³¸® Áß")
        return
    if _job_lock_token == "noop" and _CURRENT_JOB and _CURRENT_JOB != job_id:
        logger.warning(f"µ¿½Ã ½ÇÇà °ÅºÎ (fallback): {job_id}")
        await update_job_status(job_id, JobStatus.FAILED, error="´Ù¸¥ Àâ Ã³¸® Áß")
        return
    _CURRENT_JOB = job_id
    await _redis_set_job(job_id, JobStatus.PROCESSING, progress=5, step="initializing")
    try:
        await update_job_status(job_id, JobStatus.PROCESSING, progress=10.0)
        
        job_assets_dir = JOBS_DIR / job_id / "assets"
        job_temp_dir = TMP_DIR / job_id
        job_temp_dir.mkdir(parents=True, exist_ok=True)
        
        # Àå¸é ·Îµå ? request.scenes ¿ì¼±, ¾øÀ¸¸é ÆÄÀÏ
        req_scenes = getattr(request, "scenes", None) or []
        scenes_file = JOBS_DIR / job_id / "scenes.json"
        if req_scenes:
            # request body ·Î Àü´ÞµÈ scenes »ç¿ë (UI/ºê¶ó¿ìÀú °æ·Î)
            scenes_data = [
                (s.model_dump(mode="json") if hasattr(s, "model_dump") else s)
                for s in req_scenes
            ]
            # µð½ºÅ©¿¡µµ ÀúÀå (rebuild µî¿¡¼­ ÂüÁ¶ °¡´É)
            scenes_file.parent.mkdir(parents=True, exist_ok=True)
            scenes_file.write_text(
                json.dumps(scenes_data, ensure_ascii=False, indent=2),
                encoding="utf-8"
            )
            logger.info(f"¿äÃ» scenes ·Îµå: {len(scenes_data)}°³ (ÆÄÀÏ·Îµµ ÀúÀå)")
        else:
            if not scenes_file.exists():
                raise FileNotFoundError(f"Àå¸é ÆÄÀÏ ¾øÀ½: {scenes_file}")
            with open(scenes_file) as f:
                scenes_data = json.load(f)
        # scenes.json ÇüÅÂ Á¤±ÔÈ­: list | {"scenes":[...]} | ´ÜÀÏ dict ¸ðµÎ Çã¿ë
        if isinstance(scenes_data, dict):
            if isinstance(scenes_data.get('scenes'), list):
                scenes_data = scenes_data['scenes']
            else:
                scenes_data = [scenes_data]
        if not isinstance(scenes_data, list):
            raise ValueError(f"scenes.json Çü½Ä ¿À·ù: list ¶Ç´Â dict ±â´ë, got {type(scenes_data).__name__}")
        scenes = []
        for idx, s in enumerate(scenes_data):
            if not isinstance(s, dict):
                raise ValueError(f"scenes[{idx}] Çü½Ä ¿À·ù: dict ±â´ë, got {type(s).__name__}")
            scenes.append(Scene(**s))
        
        logger.info(f"·ÎµåµÈ Àå¸é: {len(scenes)}°³")
        state.mark("scenes_loaded", {"count": len(scenes)})  # [AC] MARKER body

        # [TTS-AUTO] audio_url ¾øÀ» ¶§ TTS ÀÚµ¿ »ý¼º (lf2_tts:8001 È£Ãâ)
        if not getattr(request, "audio_url", None) and not (TMP_DIR / f"{job_id}.mp3").exists():
            _tts_result = await ensure_tts_assets(job_id, scenes, request)
            _tts_ok = _tts_result.get("ok", False)
            if not _tts_ok:
                _ec = _tts_result.get("error_code", "TTS_ERROR")
                _msg = _tts_result.get("message", "TTS ½ÇÆÐ")
                logger.warning(f"[TTS] {_ec}: {_msg}")
            await _redis_set_job(job_id, JobStatus.TTS_GENERATING, progress=18,
                step="tts_generating",
                message="TTS ¿Ï·á" if _tts_ok else _tts_result.get("message","TTS ½ÇÆÐ"),
                error_code=None if _tts_ok else _tts_result.get("error_code"))
            if _tts_ok:
                logger.info("[TTS-AUTO] TTS ÀÚµ¿ »ý¼º ¿Ï·á")
            else:
                logger.warning("[TTS-AUTO] TTS ÀÚµ¿ »ý¼º ½ÇÆÐ ? À½¼º ¾øÀÌ ÁøÇà")

        # TTS Å¸ÀÓ½ºÅÆÇÁ·Î ¾À ±æÀÌ µ¿±âÈ­ (³ª·¹ÀÌ¼Ç-¿µ»ó ÀÏÄ¡)
        # 1¼øÀ§: job_id ±â¹Ý / 2¼øÀ§: audio_url ÆÄÀÏ¸í ±â¹Ý (ÀÚ»ê ÀçÈ°¿ë ½Ã)
        tts_timestamps = TMP_DIR / f"{job_id}_timestamps.json"
        if not tts_timestamps.exists() and getattr(request, "audio_url", None):
            try:
                audio_p = Path(request.audio_url)
                alt_ts = audio_p.with_name(audio_p.stem + "_timestamps.json")
                if alt_ts.exists():
                    tts_timestamps = alt_ts
                    logger.info(f"Å¸ÀÓ½ºÅÆÇÁ fallback »ç¿ë: {alt_ts}")
            except Exception as _e:
                logger.warning(f"Å¸ÀÓ½ºÅÆÇÁ fallback Å½»ö ½ÇÆÐ: {_e}")
        scenes = sync_scene_durations_from_timestamps(scenes, tts_timestamps)
        state.mark("tts_synced")

        # [AZ] Auto-extract per-segment keywords if missing from timestamps.json
        try:
            if tts_timestamps and tts_timestamps.exists():
                import json as _j
                _td = _j.loads(tts_timestamps.read_text(encoding="utf-8"))
                # [BJ] ±âÁ¸ keywords°¡ ÇÑ±¹¾î/±ä ¹®ÀåÀÌ¸é ¹«È¿·Î °£ÁÖ ¡æ ÀçÃßÃâ
                _existing = _td.get("segment_keywords") or []
                _needs_regen = not _existing
                if _existing and not _needs_regen:
                    # Ã¹ Ç×¸ñ °Ë»ç: ÇÑ±¹¾î(ÇÑ±Û) Æ÷ÇÔ ¶Ç´Â 30ÀÚ ÀÌ»óÀÌ¸é invalid
                    try:
                        first_kw = (_existing[0].get("keywords") or [""])[0]
                        if not first_kw:
                            _needs_regen = True
                        else:
                            # ÇÑ±Û À¯´ÏÄÚµå AC00-D7A3
                            has_hangul = any("\uac00" <= ch <= "\ud7a3" for ch in first_kw)
                            too_long = len(first_kw) > 30
                            if has_hangul or too_long:
                                _needs_regen = True
                                logger.info(f"[BJ] ±âÁ¸ segment_keywords ¹«È¿ (hangul={has_hangul} len={len(first_kw)}) ? ÀçÃßÃâ")
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
                        logger.info(f"[AZ] segment_keywords ÀÚµ¿ »ý¼º ÀúÀå: {len(_kws)}°³")
        except Exception as _az_err:
            logger.warning(f"[AZ] segment_keywords ÀÚµ¿ »ý¼º ½ÇÆÐ: {_az_err}")

        # [4]+[7] Whisper segments ±â¹Ý ÀÇ¹Ì ÀçºÐÇØ + ¸®µë ÄÆ Àû¿ë
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

        # [C-1] segment_keywords ·Î keyword ±³Ã¼µÈ ¾À(asset_url=None) Àç°Ë»ö¡¤´Ù¿î·Îµå
        need_download = [s for s in scenes if s.asset_url is None]
        if need_download:
            logger.info(
                f"[C-1] {len(need_download)}°³ ¾À Àç°Ë»ö¡¤´Ù¿î·Îµå ÇÊ¿ä "
                f"(ÃÑ {len(scenes)}°³ Áß)"
            )
            try:
                refreshed = await search_and_download_assets(job_id, need_download)
                refreshed_map = {s.scene_id: s for s in refreshed}
                scenes = [
                    refreshed_map.get(s.scene_id, s) if s.asset_url is None else s
                    for s in scenes
                ]
            except Exception as _e:
                logger.warning(f"[C-1] Àç°Ë»ö¡¤´Ù¿î·Îµå ½ÇÆÐ (¿øº» asset fallback Àû¿ë): {_e}")

        # asset_url ¿©ÀüÈ÷ ¾ø´Â ¾ÀÀº ´Ù¸¥ ¾ÀÀÇ asset À¸·Î fallback
        has_assets = [s for s in scenes if s.asset_url]
        if has_assets:
            fallback_url = has_assets[0].asset_url
            for i, s in enumerate(scenes):
                if s.asset_url is None:
                    scenes[i] = s.model_copy(update={"asset_url": fallback_url})
                    logger.info(f"[C-1] ¾À '{s.scene_id}' fallback asset Àû¿ë: {fallback_url}")
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
                logger.info(f"[NTL] Å¸ÀÓ¶óÀÎ ¿Ï·á: {_ntl_timeline.get('total_duration', 0):.1f}ÃÊ, "
                            f"{len(_ntl_timeline.get('scene_timings', []))}°³ ¾À")
            except Exception as _ntl_err:
                logger.warning(f"[NTL] Å¸ÀÓ¶óÀÎ »ý¼º ½ÇÆÐ (°è¼Ó ÁøÇà): {_ntl_err}")

        # ¹ÂÁ÷ºñµð¿À ¸ðµå
        if request.mode == VideoMode.MUSIC_VIDEO:
            await update_job_status(job_id, JobStatus.PROCESSING, progress=20.0)
            clips = await prepare_clips_for_longform(job_id, scenes, job_temp_dir)
            if not clips:
                raise ValueError("¹ÂÁ÷ºñµð¿À¿ë Å¬¸³ ¾øÀ½")
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
                raise RuntimeError("¹ÂÁ÷ºñµð¿À »ý¼º ½ÇÆÐ")
            await update_job_status(job_id, JobStatus.PROCESSING, progress=80.0)
            duration = get_video_duration(output_video)
            output_files = {"longform": str(output_video)}
            if request.generate_thumbnail:
                tp = job_temp_dir / "thumbnail_raw.jpg"
                if extract_thumbnail(output_video, tp):
                    tf = THUMBNAILS_DIR / f"{job_id}_thumb.jpg"
                    if add_text_overlay_to_thumbnail(tp, tf, title=request.title or f"MV {job_id[:8]}"):
                        output_files["thumbnail"] = str(tf)

        # ÀåÆí/¼ôÆû ¿µ»ó »ý¼º [v16.6: SHORTFORM ¸í½Ã Æ÷ÇÔ]
        elif request.mode in (VideoMode.LONGFORM, VideoMode.SHORTFORM) or request.generate_shorts:
            await update_job_status(job_id, JobStatus.PROCESSING, progress=20.0)
            
            # [AC/AF-3] resume: reuse existing clips, re-render only missing ones
            clips = None
            if resume and state.has("clips_prepared"):
                prev_paths = state.get_payload("clips_prepared").get("paths", []) or []
                if prev_paths:
                    existing = [Path(p) for p in prev_paths if Path(p).exists() and Path(p).stat().st_size > 4096]
                    missing = [p for p in prev_paths if p not in [str(x) for x in existing]]
                    if missing:
                        logger.info(f"[AF-3] ºÎºÐ º¹±¸ ? Á¸Àç {len(existing)}/{len(prev_paths)}°³, ´©¶ô {len(missing)}°³ Àç»ý¼º ½Ãµµ")
                        # Fall through to regenerate below (clips stays None)
                    elif len(existing) == len(prev_paths):
                        clips = existing
                        logger.info(f"[AC] clips_prepared ½ºÅµ ? ±âÁ¸ {len(clips)}°³ Å¬¸³ Àç»ç¿ë")
            if clips is None:
                clips = await prepare_clips_for_longform(job_id, scenes, job_temp_dir)  # [AC] MARKER resume
            
            
            if not clips:
                raise ValueError("ÁØºñµÈ Å¬¸³ ¾øÀ½")
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
                logger.warning(f"[AQ-1] intro/outro ½ÇÆÐ: {_io_err}")
            concat_file = job_temp_dir / "concat.txt"
            combined_video = job_temp_dir / "combined.mp4"  # [AQ] MARKER v1
            skip_concat = False
            if resume and state.has("concat_done"):
                prev = state.get_payload("concat_done").get("combined")
                if prev and Path(prev).exists() and Path(prev).stat().st_size > 4096:
                    combined_video = Path(prev)
                    skip_concat = True
                    logger.info(f"[AC] concat_done ½ºÅµ ? ±âÁ¸ combined.mp4 Àç»ç¿ë: {combined_video}")
            if not skip_concat:
                if not create_concat_file(clips, concat_file):
                    raise RuntimeError("Concat ÆÄÀÏ »ý¼º ½ÇÆÐ")
                if not concatenate_videos(concat_file, combined_video):
                    raise RuntimeError("¿µ»ó ¿¬°á ½ÇÆÐ")
            state.mark("concat_done", {"combined": str(combined_video)})
            
            await update_job_status(job_id, JobStatus.PROCESSING, progress=50.0)
            
            # ¿Àµð¿À ¹Í½Ì
            # audio_urlÀÌ ÀÖÀ¸¸é ¿ì¼± »ç¿ë (¿ÜºÎ TTS ¿Àµð¿À Áö¿ø)
            if getattr(request, "audio_url", None):
                tts_audio = Path(request.audio_url)
            else:
                tts_audio = TMP_DIR / f"{job_id}.mp3"
            bgm = None
            
            if request.add_bgm:
                bgm = get_random_bgm()
            
            output_video = LONGFORM_DIR / f"{job_id}.mp4"
            
            if not mix_audio(combined_video, tts_audio, bgm, request.bgm_volume, output_video):
                logger.warning("¿Àµð¿À ¹Í½Ì ½ÇÆÐ, ¿Àµð¿À ¾øÀÌ ÁøÇà")
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
                        logger.info(f"[AQ-3] °úÀ× ¹«À½ {len(pairs)}°³ °ËÃâ (>3s)")
                        # Simple approach: re-encode skipping the middle of each silence
                        # Keep first/last 0.5s of each silence, drop the middle
                        # This is complex for ffmpeg atrim/concat ? use approximate setpts + asetpts
                        # For now, log and skip (mark future impl)
                        logger.info(f"[AQ-3] trim ´ë»ó: {pairs[:3]}... (skip for stability, logged only)")
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
                    if await run_ffmpeg_async(ln_cmd, timeout=120.0) and ln_tmp.exists() and ln_tmp.stat().st_size > 4096:
                        shutil.move(str(ln_tmp), str(output_video))
                        logger.info("[AI-1] loudnorm Àû¿ë ¿Ï·á (-16 LUFS)")
            except Exception as _ln_err:
                logger.warning(f"[AI-1] loudnorm ½ÇÆÐ (¹«½Ã): {_ln_err}")
            state.mark("audio_mixed", {"output": str(output_video)})
            
            await update_job_status(job_id, JobStatus.PROCESSING, progress=70.0)
            
            # ¿µ»ó ±æÀÌ Á¶È¸
            duration = get_video_duration(output_video)
            
            # ½æ³×ÀÏ »ý¼º
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
                        logger.info(f"[AK-3] ½æ³×ÀÏ best: {_candidates[0][1].name} (score={_candidates[0][0]:.1f})")
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


            # ÀÚ¸· ¿À¹ö·¹ÀÌ (add_subtitles=True ÀÌ¸é Ç×»ó »ý¼º)
            # ¾À description ÀÖÀ¸¸é ¾À µ¿±âÈ­ SRT ¿ì¼±, ¾øÀ¸¸é subtitle_text fallback
            if request.add_subtitles:
                try:
                    srt_path = job_temp_dir / f"{job_id}_narration.srt"
                    srt_ok = False
                    # [8] Whisper timestamps°¡ ÀÖÀ¸¸é ÃÖ¿ì¼± (´Ü¾î ´ÜÀ§ Á¤È®µµ + ¼±Çà)
                    if tts_timestamps and tts_timestamps.exists():
                        # [v15.59.0] subtitle_path / subtitle_type ¸í½Ã ºÐ¸®
                        ass_path = srt_path.with_suffix(".ass")
                        subtitle_path = None
                        subtitle_type = None
                        ass_ok = create_ass_karaoke_from_whisper(
                            tts_timestamps, ass_path,
                            speed_factor=getattr(request, "subtitle_speed", 0.0)
                        )
                        if ass_ok:
                            subtitle_path = ass_path
                            subtitle_type = "ass"
                            srt_ok = True
                            logger.info("[SUBTITLE] ASS Ä«¶ó¿ÀÄÉ Àû¿ë")
                            await _redis_set_job(job_id, JobStatus.SUBTITLE_CREATING,
                                progress=55, step="subtitle_creating",
                                message="ASS Ä«¶ó¿ÀÄÉ ÀÚ¸· »ý¼º ¿Ï·á")
                        else:
                            srt_ok = create_srt_from_whisper_segments(tts_timestamps, srt_path)
                            if srt_ok:
                                subtitle_path = srt_path
                                subtitle_type = "srt"
                                logger.info(f"[SUBTITLE] SRT fallback (lead={SUBTITLE_LEAD_SEC}s)")
                                await _redis_set_job(job_id, JobStatus.SUBTITLE_CREATING,
                                    progress=55, step="subtitle_creating",
                                    message="SRT fallback »ý¼º ¿Ï·á")
                    if not srt_ok and scenes and any((s.narration or s.description) for s in scenes):  # [v16.19] narration first
                        srt_ok = create_srt_from_scenes(scenes, srt_path)
                        logger.info("¾À µ¿±âÈ­ ÀÚ¸· fallback »ç¿ë")
                    if not srt_ok and request.subtitle_text:
                        total_dur = duration or sum((s.duration_seconds or 5.0) for s in scenes)
                        srt_ok = create_srt_from_text(request.subtitle_text, total_dur, srt_path)
                        logger.info("ÅØ½ºÆ® ÀÚ¸· fallback »ç¿ë")
                    if srt_ok:
                        _active_sub = subtitle_path if subtitle_path else srt_path
                        _active_type = subtitle_type if subtitle_type else "srt"
                        # keyword highlight: SRT¸¸ Àû¿ë (ASS´Â ÀÚÃ¼ ½ºÅ¸ÀÏ º¸Á¸)
                        if _active_type == "srt":
                            try:
                                _highlight_keywords_in_srt(_active_sub, scenes)
                            except Exception as _hi_err:
                                logger.warning(f"[AF-4] highlight skip: {_hi_err}")
                        out_sub = LONGFORM_DIR / f"{job_id}_sub.mp4"
                        await _redis_set_job(job_id, JobStatus.RENDERING, progress=70,
                            step="rendering", message="ÀÚ¸· ¿À¹ö·¹ÀÌ ·»´õ¸µ Áß")
                        if add_subtitles_to_video(output_video, _active_sub, out_sub,
                                                   subtitle_type=_active_type):
                            shutil.move(str(out_sub), str(output_video))
                            output_files["longform"] = str(output_video)
                            logger.info("ÀÚ¸· ¿À¹ö·¹ÀÌ ¿Ï·á")
                            state.mark("subtitles_added", {"output": str(output_video)})
                except Exception as e:
                    logger.error(f"ÀÚ¸· ¿À·ù: {e}")

            # [v15.77] ·Î¿ö¼­µå ±×·¡ÇÈ ¿À¹ö·¹ÀÌ ? [v15.94] ºñÈ°¼ºÈ­ (ÀÚ¸· °ãÄ§ ¹æÁö)
            try:
                _lt_events = _extract_lower_third_events_from_narration(
                    scenes, tts_timestamps if "tts_timestamps" in dir() else None,
                    duration or 300.0,
                )
                if False and _lt_events:  # [v15.94] ·Î¿ö¼­µå OFF
                    _lt_ass = job_temp_dir / f"{job_id}_lower.ass"
                    if create_lower_third_ass(_lt_events, _lt_ass) and _lt_ass.exists():
                        _lt_out = output_video.with_name(output_video.stem + "_lt.mp4")
                        _lt_esc = str(_lt_ass).replace("\\", "/").replace(":", "\\:")
                        _lt_cmd = [
                            "ffmpeg", "-y",
                            "-i", str(output_video),
                            "-vf", f"ass=\'{_lt_esc}\'",
                            "-c:v", "libx264", "-preset", "fast", "-crf", "20",
                            "-c:a", "copy",
                            str(_lt_out),
                        ]
                        if (await run_ffmpeg_async(_lt_cmd, timeout=300.0)
                                and _lt_out.exists()
                                and _lt_out.stat().st_size > 4096):
                            shutil.move(str(_lt_out), str(output_video))
                            output_files["longform"] = str(output_video)
                            logger.info(f"[v15.77] ·Î¿ö¼­µå ¿Ï·á: {len(_lt_events)}°³")
                        else:
                            logger.warning("[v15.77] ·Î¿ö¼­µå ·»´õ ½ÇÆÐ ? ¿øº» À¯Áö")
            except Exception as _lt_err:
                logger.warning(f"[v15.77] ·Î¿ö¼­µå ½ºÅµ: {_lt_err}")

            # [v15.78] Áß¾Ó Å°¿öµå ¹è³Ê ¿À¹ö·¹ÀÌ ? [v15.94] ºñÈ°¼ºÈ­ (ÀÚ¸· °ãÄ§ ¹æÁö)
            try:
                _cb_events = _extract_center_banner_events(scenes, duration or 300.0)
                if False and _cb_events:  # [v15.94] Áß¾Ó¹è³Ê OFF
                    _cb_ass = job_temp_dir / f"{job_id}_banner.ass"
                    if create_center_banner_ass(_cb_events, _cb_ass) and _cb_ass.exists():
                        _cb_out = output_video.with_name(output_video.stem + "_cb.mp4")
                        _cb_esc = str(_cb_ass).replace("\\", "/").replace(":", "\\:")
                        _cb_cmd = ["ffmpeg","-y","-i",str(output_video),"-vf",
                                   f"ass=\'{_cb_esc}\'","-c:v","libx264","-preset","fast",
                                   "-crf","20","-c:a","copy",str(_cb_out)]
                        if (await run_ffmpeg_async(_cb_cmd, timeout=300.0)
                                and _cb_out.exists() and _cb_out.stat().st_size > 4096):
                            shutil.move(str(_cb_out), str(output_video))
                            output_files["longform"] = str(output_video)
                            logger.info(f"[v15.78] Áß¾Ó¹è³Ê ¿Ï·á: {len(_cb_events)}°³")
                        else:
                            logger.warning("[v15.78] Áß¾Ó¹è³Ê ·»´õ ½ÇÆÐ")
            except Exception as _cb_err:
                logger.warning(f"[v15.78] Áß¾Ó¹è³Ê ½ºÅµ: {_cb_err}")

            # [v15.81] ÆÐÅÏ ÀÎÅÍ·´Æ® SFX ¿À¹ö·¹ÀÌ (ÀçÈÅ ¹è³Ê ½ÃÁ¡)
            try:
                if _cb_events and output_video.exists():
                    _n_sfx81 = min(len(_cb_events), 3)
                    _sfx81_out = output_video.with_name(output_video.stem + '_sfx.mp4')
                    _sfx81_inputs = ['-f','lavfi']
                    _sfx81_fc_parts = []
                    _sfx81_cmd = ['ffmpeg','-y','-i',str(output_video)]
                    for _si81 in range(_n_sfx81):
                        _ts81 = int((_cb_events[_si81].get('start', 0.0)) * 1000)
                        _sfx81_cmd += ['-f','lavfi','-i',
                            f'aevalsrc=0.3*sin(2*PI*880*t)*exp(-t/0.05):s=44100:d=0.12']
                        _sfx81_fc_parts.append(
                            f'[{_si81+1}:a]adelay={_ts81}|{_ts81}[pop{_si81}]')
                    _all_tags = '[0:a]' + ''.join(f'[pop{x}]' for x in range(_n_sfx81))
                    _sfx81_fc = ';'.join(_sfx81_fc_parts) + f';{_all_tags}amix=inputs={_n_sfx81+1}:duration=first[ao]'
                    _sfx81_cmd += ['-filter_complex',_sfx81_fc,
                                   '-map','0:v','-map','[ao]',
                                   '-c:v','copy','-c:a','aac','-b:a','192k','-y',str(_sfx81_out)]
                    if (await run_ffmpeg_async(_sfx81_cmd, timeout=120.0)
                            and _sfx81_out.exists() and _sfx81_out.stat().st_size > 4096):
                        shutil.move(str(_sfx81_out), str(output_video))
                        logger.info(f'[v15.81] SFX ¿À¹ö·¹ÀÌ ¿Ï·á: {_n_sfx81}°³ ÆË')
                    else:
                        try: _sfx81_out.unlink()
                        except Exception: pass
                        logger.warning('[v15.81] SFX ½ºÅµ')
            except Exception as _sfx81_err:
                logger.warning(f'[v15.81] SFX ¿¹¿Ü: {_sfx81_err}')
            # ¼ôÆû »ý¼º [v16.6: graceful degradation ? ½ÇÆÐÇØµµ ·ÕÆû ¿Ï·á À¯Áö]
            if request.generate_shorts:
                # [v16.11] multi-duration shorts: 5s, 10s, 60s (configurable via shorts_durations)
                _short_durations = getattr(request, "shorts_durations", [5.0, 10.0, 60.0])
                for _sdur in _short_durations:
                    _suffix = f"_{int(_sdur)}s" if _sdur < 60.0 else ""
                    shorts_output = SHORTS_DIR / f"{job_id}_short{_suffix}.mp4"
                    try:
                        shorts_ok = create_shortform_from_longform(
                            output_video, shorts_output, max_duration=_sdur,
                            timeout=max(30.0, _sdur * 4)
                        )
                        if shorts_ok:
                            key = f"shorts_{int(_sdur)}s" if _sdur < 60.0 else "shorts"
                            output_files[key] = str(shorts_output)
                            state.mark("shorts_done", {"path": str(shorts_output), "duration": _sdur})
                            logger.info(f"[v16.11] ¼ôÆû {int(_sdur)}s ¿Ï·á: {shorts_output}")
                        else:
                            logger.warning(f"[v16.11] ¼ôÆû {int(_sdur)}s ½ÇÆÐ (job={job_id})")
                    except Exception as _shorts_err:
                        logger.warning(f"[v16.11] ¼ôÆû {int(_sdur)}s ¿¹¿Ü (·ÕÆû À¯Áö): {_shorts_err}")
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
                    logger.warning(f"[AF-5b] QA °æ°í ? ¿µ»ó/¿Àµð¿À duration ¿ÀÂ÷ {diff:.2f}s (video={video_dur:.2f}s audio={audio_dur:.2f}s)")
                else:
                    logger.info(f"[AF-5b] QA OK ? duration diff {diff:.2f}s (video={video_dur:.2f}s audio={audio_dur:.2f}s)")
            # [AI-10] Extended QA battery
            try:
                qa_issues = []
                # 1. Video file size sanity
                if "longform" in output_files:
                    lf = Path(output_files["longform"])
                    if lf.exists():
                        size_mb = lf.stat().st_size / (1024 * 1024)
                        if size_mb < 1.0:
                            qa_issues.append(f"¿µ»ó ÆÄÀÏ ³Ê¹« ÀÛÀ½: {size_mb:.2f}MB")
                        elif size_mb > 500:
                            qa_issues.append(f"¿µ»ó ÆÄÀÏ ºñÁ¤»ó Å©±â: {size_mb:.0f}MB")
                # 2. Scene count reasonable
                if len(scenes) < 2:
                    qa_issues.append(f"¾À °³¼ö ºÎÁ·: {len(scenes)}")
                # 3. All scenes have asset
                missing_assets = sum(1 for s in scenes if not s.asset_url)
                if missing_assets > 0:
                    qa_issues.append(f"asset ´©¶ô ¾À: {missing_assets}°³")
                # 4. Thumbnail exists
                if "thumbnail" not in output_files:
                    qa_issues.append("½æ³×ÀÏ ¹Ì»ý¼º")
                if qa_issues:
                    logger.warning(f"[AI-10] QA °æ°í: {qa_issues}")
                else:
                    logger.info(f"[AI-10] QA ÀüÃ¼ Åë°ú ({len(scenes)}¾À, asset 100%, ½æ³×ÀÏ OK)")
            except Exception as _qa2_err:
                logger.debug(f"[AI-10] QA battery skip: {_qa2_err}")
        except Exception as _qa_err:
            logger.debug(f"[AF-5b] QA Ã¼Å© ½ÇÆÐ: {_qa_err}")
        logger.info(f"ÀÛ¾÷ ¿Ï·á: {job_id}")
        state.mark("completed", {"output_files": output_files})
        # [CLEANUP] 잡 완료 후 tmp 임시파일 즉시 삭제 (400GB 방어)
        try:
            cleanup_job_tmp(job_id)
        except Exception:
            pass
        # Eµå¶óÀÌºê ¿Ï¼º Æú´õ¿¡ º¹»ç
        try:
            for key, src_path in list(output_files.items()):
                src = Path(src_path)
                if src.exists():
                    dest_dir = COMPLETE_DIR / key
                    dest_dir.mkdir(parents=True, exist_ok=True)
                    dest = dest_dir / src.name
                    shutil.copy2(src, dest)
                    output_files[f'complete_{key}'] = str(dest)
                    logger.info(f'¿Ï¼º Æú´õ º¹»ç: {src.name} -> {dest}')
        except Exception as copy_err:
            logger.warning(f'¿Ï¼º Æú´õ º¹»ç ½ÇÆÐ (¹«½Ã): {copy_err}')
        # ¦¡¦¡ YouTube ÀÚµ¿ ¾÷·Îµå ¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡
        if "longform" in output_files:
            try:
                lf_path = output_files["longform"]
                thumb_path = str(THUMBNAILS_DIR / f"{job_id}_thumb.jpg")
                if "thumbnail" in output_files:
                    thumb_path = output_files["thumbnail"]
                # Á¦¸ñ/¼³¸í: scenes.json¿¡¼­ ÃßÃâ
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
                                chapters = ["00:00 ½ÃÀÛ"]
                                cum = 0.0
                                for idx, s in enumerate(sc_list[:15], 1):  # max 15 chapters
                                    cum += float(s.get("duration_seconds", 0) or 0)
                                    mm = int(cum // 60)
                                    ss = int(cum % 60)
                                    title = (s.get("description") or s.get("keyword") or f"Ã©ÅÍ {idx}")[:40]
                                    chapters.append(f"{mm:02d}:{ss:02d} {title}")
                                desc = "\n".join(chapters) + "\n\n" + desc
                            except Exception:
                                pass
                            yt_description = desc
                except Exception as _pe:
                    logger.warning(f"scenes.json ÆÄ½Ì ¿À·ù: {_pe}")
                if not yt_title:
                    yt_title = job_id
                if not yt_description:
                    yt_description = yt_title
                upload_payload = {
                    "video_path": lf_path,
                    "title": yt_title,
                    "description": yt_description + "\n\n#AI #ÀÚµ¿¿µ»ó #·ÕÆû",
                    "tags": ["AI", "ÀÚµ¿¿µ»ó", "·ÕÆû", "LongForm"],
                    "privacy_status": "private",
                    "thumbnail_path": thumb_path if Path(thumb_path).exists() else None
                }
                logger.info(f"YouTube ÀÚµ¿ ¾÷·Îµå ½ÃÀÛ: {yt_title}")
                async with httpx.AsyncClient(timeout=180.0) as yt_client:
                    yt_resp = await yt_client.post(
                        "http://lf2_uploader:8003/upload/youtube",
                        json=upload_payload,
                        headers={"X-LF-API-Key": os.getenv("LF_API_KEY", "")}
                    )
                    if yt_resp.status_code == 200:
                        yt_data = yt_resp.json()
                        yt_url = yt_data.get("video_url", "")
                        logger.info(f"YouTube ÀÚµ¿ ¾÷·Îµå ¼º°ø: {yt_url}")
                        output_files["youtube_url"] = yt_url
                        state.mark("youtube_uploaded", {"url": yt_url})
                        await update_job_status(job_id, JobStatus.COMPLETED, progress=100.0, output_files=output_files, duration_seconds=duration)
                    else:
                        logger.warning(f"YouTube ¾÷·Îµå ½ÇÆÐ {yt_resp.status_code}: {yt_resp.text[:300]}")
            except Exception as yt_err:
                logger.warning(f"YouTube ÀÚµ¿ ¾÷·Îµå ¿À·ù (¹«½Ã): {yt_err}")
    
    except Exception as e:
        logger.error(f"¿µ»ó »ý¼º ¿À·ù ({job_id}): {e}")
        try:
            state.set_error(str(e))
        except Exception:
            pass
        await update_job_status(job_id, JobStatus.FAILED, error=str(e))
    finally:
        _CURRENT_JOB = None
        await _redis_release_lock(job_id, _job_lock_token)  # [v16.18] single release
        try:
            cleanup_job_tmp(job_id)  # [v16.18] always cleanup tmp
        except Exception:
            pass


# ============================================================================
# API ¿£µåÆ÷ÀÎÆ®
# ============================================================================

@app.get("/video/enhancements", tags=["System"])
async def list_enhancements():
    """[AL-5] List all enhancement markers present in app.py."""
    return {
        "version": VERSION,
        "rounds": {
            "AC": "´Ü°èº° Àç½Ãµµ + resume",
            "AD": "ÅëÇÕ Å¸ÀÓ¶óÀÎ",
            "AE": "¾À ·¹ÀÌ¾Æ¿ô 5 ÅÛÇÃ¸´ (opt-in)",
            "AF": "¿µ»ó Ç°Áú 1Â÷ °­È­ (10)",
            "AG": "word-level Whisper ÀÚ¸· + TTS ¾ÈÁ¤È­",
            "AH": "Whisper Àý´ë½Ã°£ Á¤·Ä (gap Èí¼ö) + fallback Korean",
            "AI": "¿µ»ó °­È­ 10´Ü°è (loudnorm, 13 transition, vignette PI/5)",
            "AJ": "¿µÈ­Àû ¸¶°¨ (intro/outro/chapters)",
            "AK": "ÇÁ·Î´ö¼Ç Ç°Áú (colorbalance, limiter, thumbnail 3-variant)",
            "AL": "½Å·Ú¼º (Pexels Ä³½Ã, smoke test)",
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
    """헬스 체크 + 디스크 공간 경보 (400GB 방어)"""
    disk = check_disk_space(min_free_gb=50.0)
    status = "healthy" if disk["ok"] else "disk_warning"
    return {
        "status": status,
        "service": "lf_ffmpeg_worker",
        "version": VERSION,
        "timestamp": datetime.now().isoformat(),
        "disk": disk,
    }



@app.get("/providers/ping", tags=["System"])
async def ping_providers():
    """[v16.14] 모든 AI·미디어 API 실시간 핑 테스트"""
    import time
    results: dict = {}

    async def _ping(name: str, url: str, headers: dict, timeout: float = 5.0) -> dict:
        t0 = time.monotonic()
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                r = await client.get(url, headers=headers)
                latency = round((time.monotonic() - t0) * 1000)
                ok = r.status_code in (200, 206, 401, 403)
                return {"ok": ok, "status": r.status_code, "ms": latency}
        except Exception as e:
            latency = round((time.monotonic() - t0) * 1000)
            return {"ok": False, "status": 0, "ms": latency, "error": str(e)[:80]}

    ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY", os.getenv("ANTHROPIC_AUTH_TOKEN", ""))
    GEMINI_KEY    = os.getenv("GEMINI_API_KEY", "")
    XAI_KEY       = os.getenv("XAI_API_KEY", "")
    GROQ_KEY      = os.getenv("GROQ_API_KEY", "")
    CEREBRAS_KEY  = os.getenv("CEREBRAS_API_KEY", "")
    SAMBANOVA_KEY = os.getenv("SAMBANOVA_API_KEY", "")
    PEXELS_KEY    = os.getenv("PEXELS_API_KEY", "")
    PIXABAY_KEY   = os.getenv("PIXABAY_API_KEY", "")
    OPENROUTER_KEY= os.getenv("OPENROUTER_API_KEY", "")

    tasks = [
        ("anthropic",   "https://api.anthropic.com/v1/models",
         {"x-api-key": ANTHROPIC_KEY, "anthropic-version": "2023-06-01"} if ANTHROPIC_KEY else {}),
        ("gemini",      f"https://generativelanguage.googleapis.com/v1beta/models?key={GEMINI_KEY}",
         {} if not GEMINI_KEY else {"Content-Type": "application/json"}),
        ("xai",         "https://api.x.ai/v1/models",
         {"Authorization": f"Bearer {XAI_KEY}"} if XAI_KEY else {}),
        ("groq",        "https://api.groq.com/openai/v1/models",
         {"Authorization": f"Bearer {GROQ_KEY}"} if GROQ_KEY else {}),
        ("cerebras",    "https://api.cerebras.ai/v1/models",
         {"Authorization": f"Bearer {CEREBRAS_KEY}"} if CEREBRAS_KEY else {}),
        ("sambanova",   "https://api.sambanova.ai/v1/models",
         {"Authorization": f"Bearer {SAMBANOVA_KEY}"} if SAMBANOVA_KEY else {}),
        ("openrouter",  "https://openrouter.ai/api/v1/models",
         {"Authorization": f"Bearer {OPENROUTER_KEY}"} if OPENROUTER_KEY else {}),
        ("pexels",      "https://api.pexels.com/videos/popular?per_page=1",
         {"Authorization": PEXELS_KEY} if PEXELS_KEY else {}),
        ("pixabay",     f"https://pixabay.com/api/videos/?key={PIXABAY_KEY}&q=nature&per_page=3",
         {} if not PIXABAY_KEY else {"Content-Type": "application/json"}),
    ]

    async def _run_all():
        coros = [_ping(name, url, hdrs) for name, url, hdrs in tasks]
        return await asyncio.gather(*coros, return_exceptions=True)

    raw = await _run_all()
    for (name, _, _), res in zip(tasks, raw):
        if isinstance(res, Exception):
            results[name] = {"ok": False, "status": 0, "ms": -1, "error": str(res)[:80]}
        else:
            results[name] = res
        key_map = {"anthropic": ANTHROPIC_KEY, "gemini": GEMINI_KEY, "xai": XAI_KEY,
                   "groq": GROQ_KEY, "cerebras": CEREBRAS_KEY, "sambanova": SAMBANOVA_KEY,
                   "openrouter": OPENROUTER_KEY, "pexels": PEXELS_KEY, "pixabay": PIXABAY_KEY}
        if not key_map.get(name):
            results[name]["no_key"] = True
            results[name]["ok"] = False

    return {"results": results, "timestamp": datetime.now().isoformat()}


@app.post("/assets/search", response_model=AssetsSearchResponse, tags=["Assets"])
async def search_assets(request: AssetsSearchRequest, background_tasks: BackgroundTasks):
    """
    Pexels/Pixabay¿¡¼­ ¿µ»ó ÀÚ»ê °Ë»ö ¹× ´Ù¿î·Îµå
    
    - job_id: ÀÛ¾÷ °íÀ¯ ID
    - scenes: °Ë»öÇÒ Àå¸é ¸ñ·Ï
    - sources: °Ë»ö ¼Ò½º (pexels, pixabay)
    """
    try:
        job_id = request.job_id
        
        # Àå¸é Á¤º¸ ÀúÀå
        scenes_file = JOBS_DIR / job_id / "scenes.json"
        scenes_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(scenes_file, "w") as f:
            json.dump([s.dict() for s in request.scenes], f, indent=2)
        
        # ¹é±×¶ó¿îµå¿¡¼­ ÀÚ»ê °Ë»ö ¹× ´Ù¿î·Îµå
        updated_scenes = await search_and_download_assets(job_id, request.scenes)
        
        # ¾÷µ¥ÀÌÆ®µÈ Àå¸é ÀúÀå
        with open(scenes_file, "w") as f:
            json.dump([s.dict() for s in updated_scenes], f, indent=2, default=str)
        
        # ´Ù¿î·Îµå ¼º°ø °³¼ö
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
        logger.error(f"ÀÚ»ê °Ë»ö ¿À·ù: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/video/create", response_model=VideoCreateResponse, tags=["Video"])
async def create_video(request: VideoCreateRequest, background_tasks: BackgroundTasks):
    """
    FFmpeg¸¦ ÀÌ¿ëÇÑ ¿µ»ó »ý¼º
    
    - job_id: ÀÛ¾÷ °íÀ¯ ID
    - mode: longform (1920x1080) ¶Ç´Â shortform (1080x1920)
    - add_subtitles: ÀÚ¸· Ãß°¡ ¿©ºÎ
    - add_bgm: ¹è°æÀ½¾Ç Ãß°¡ ¿©ºÎ
    - generate_thumbnail: ½æ³×ÀÏ »ý¼º ¿©ºÎ
    - generate_shorts: ¼ôÆû »ý¼º ¿©ºÎ
    """
    try:
        # job_id Á¤±ÔÈ­ (Windows CR/LF Á¦°Å)
        job_id = (request.job_id or "").strip().replace("\r", "").replace("\n", "")
        if not job_id:
            raise HTTPException(status_code=400, detail="job_id empty")
        request.job_id = job_id

        # Áßº¹ POST °ÅºÎ: ÁøÇà ÁßÀÎ µ¿ÀÏ job_id
        existing = jobs.get(job_id)
        if existing and existing.status in (JobStatus.PENDING, JobStatus.PROCESSING):
            logger.warning(
                f"Áßº¹ /video/create °ÅºÎ: {job_id} (ÇöÀç {existing.status.value} / {existing.progress or 0}%)"
            )
            return VideoCreateResponse(
                success=True,
                job_id=job_id,
                status=existing.status.value
            )
        if _CURRENT_JOB is not None and _CURRENT_JOB != job_id:
            logger.warning(f"´Ù¸¥ Àâ Ã³¸® Áß ({_CURRENT_JOB}) - {job_id} Å¥ Áö¿¬")

        # ÀÛ¾÷ »óÅÂ ÃÊ±âÈ­
        await update_job_status(job_id, JobStatus.PROCESSING, progress=5.0)

        # ¹é±×¶ó¿îµå¿¡¼­ ¿µ»ó »ý¼º
        background_tasks.add_task(process_video_creation, job_id, request)

        return VideoCreateResponse(
            success=True,
            job_id=job_id,
            status="processing"
        )
    
    except Exception as e:
        logger.error(f"¿µ»ó »ý¼º ¿äÃ» ¿À·ù: {e}")
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
    _AUTO_JOB_STORE[job_id] = {"status": "resuming", "progress": 40, "current_message": "resuming job", "updated_at": __import__('datetime').datetime.now().isoformat()}
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
    """ÀÛ¾÷ »óÅÂ Á¶È¸"""
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail=f"job not found: {job_id}")  # [v16.18]

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
    logger.info("[P0] auth + billing ¶ó¿ìÅÍ µî·Ï ¿Ï·á")

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
    logger.warning(f"[P0] auth/billing ¸ðµâ ·Îµå ½ÇÆÐ (¹«½Ã): {e}")


# [P0-2] Job queue ? serialize concurrent /video/create requests
import asyncio as _asyncio_p0
_JOB_QUEUE: _asyncio_p0.Queue = _asyncio_p0.Queue(maxsize=20)
_JOB_WORKER_RUNNING = False


async def _job_queue_worker():
    """Single worker that processes /video/create jobs one at a time."""
    global _JOB_WORKER_RUNNING
    _JOB_WORKER_RUNNING = True
    logger.info("[P0-2] job queue worker ½ÃÀÛ")
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
                logger.error(f"[P0-2] job {job_id} ½ÇÆÐ: {e}")
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
                            logger.warning(f"[P0-3] retry {attempt} ½ÇÆÐ: {ee}")
                except Exception as re:
                    logger.error(f"[P0-3] retry ÃÖÁ¾ ½ÇÆÐ: {re}")
            finally:
                _JOB_QUEUE.task_done()
        except _asyncio_p0.CancelledError:
            break
        except Exception as e:
            logger.error(f"[P0-2] worker ¿À·ù: {e}")


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
        logger.warning(f"[P0-4] WS ¿À·ù: {e}")
    finally:
        try:
            await websocket.close()
        except Exception:
            pass


@app.on_event("startup")
async def startup_event():
    """¾ÖÇÃ¸®ÄÉÀÌ¼Ç ½ÃÀÛ ½Ã ÃÊ±âÈ­"""
    logger.info("FFmpeg Worker ½ÃÀÛ")
    # [v16.18] job queue worker 시작
    try:
        import asyncio as _aio_startup
        asyncio.ensure_future(_job_queue_worker())  # [v16.20] ensure_future (get_event_loop deprecated)
        logger.info("[STARTUP] job queue worker started")
    except Exception as _qe:
        logger.warning(f"[STARTUP] queue worker start failed: {_qe}")
    # [v16.23] pw_watcher: PW-Web queue monitor (fallback to Pexels/Pixabay when Playwright not installed)
    try:
        import threading as _threading
        import pathlib as _pathlib
        def _pw_watcher_loop():
            import time
            q = _pathlib.Path("/data/jobs/pw_queue")
            q.mkdir(exist_ok=True)
            while True:
                try:
                    for _f in q.glob("*.json"):
                        _fail = _f.with_suffix(".fail")
                        if not _fail.exists():
                            _fail.write_text("playwright_worker not running")
                except Exception:
                    pass
                time.sleep(2)
        _pw_thread = _threading.Thread(target=_pw_watcher_loop, daemon=True, name="pw_watcher")
        _pw_thread.start()
        logger.info("[STARTUP] pw_watcher thread started (PW-Web queue fallback)")
    except Exception as _pwe:
        logger.warning(f"[STARTUP] pw_watcher start failed: {_pwe}")
    logger.info(f"Pexels API Å°: {'¼³Á¤µÊ' if PEXELS_API_KEY else '¹Ì¼³Á¤'}")
    logger.info(f"Pixabay API Å°: {'¼³Á¤µÊ' if PIXABAY_API_KEY else '¹Ì¼³Á¤'}")
    # Restore incomplete jobs from job_status.json on startup
    try:
        import json as _json
        if JOBS_DIR.exists():
            for _jd in JOBS_DIR.iterdir():
                _sf = _jd / "job_status.json"
                if _sf.exists():
                    try:
                        with open(_sf, "r", encoding="utf-8") as _fh:
                            _d = _json.load(_fh)
                        _jid = _jd.name
                        _step = _d.get("status", "")
                        if _step not in ("completed", "failed", "error", "uploaded"):
                            _AUTO_JOB_STORE[_jid] = _d
                            logger.info("[STARTUP] Job restored: %s (%s)" % (_jid[:8], _step))
                    except Exception as _e:
                        logger.warning("[STARTUP] Job restore fail: %s - %s" % (_jd.name, _e))
    except Exception as _e:
        logger.warning("[STARTUP] Job restore scan fail: %s" % _e)


@app.on_event("shutdown")
async def shutdown_event():
    """¾ÖÇÃ¸®ÄÉÀÌ¼Ç Á¾·á"""
    logger.info("FFmpeg Worker Á¾·á")


if __name__ == "__main__":
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8002,
        workers=1,  # auto pipeline in-memory store °øÀ¯ À§ÇØ ´ÜÀÏ ÇÁ·Î¼¼½º
        log_level="info"
    )







# ============================================================================
# [v15.66.0] Auto Topic Production Engine
# POST /api/auto/topic-job  ¡æ  ÁÖÁ¦ ÀÔ·Â ÇÏ³ª·Î YouTube private ¾÷·Îµå±îÁö ÀÚµ¿È­
# ============================================================================

import json as _json_auto
import re
import re  as _re_auto

# ¦¡¦¡ 1. »õ »óÅÂ°ª ¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡
AUTO_STEP_LABELS = {
    "queued":              "´ë±â Áß",
    "topic_analyzing":     "ÁÖÁ¦ ºÐ¼® Áß",
    "researching":         "ÀÚ·á Á¶»ç Áß",
    "script_generating":   "¿ø°í »ý¼º Áß",
    "scene_building":      "¾À ºÐÇÒ Áß",
    "voice_planning":      "³ª·¹ÀÌ¼Ç Åæ ¼³Á¤ Áß",
    "asset_searching":     "¿µ»ó ÀÚ»ê °Ë»ö Áß",
    "asset_matching":      "¿µ»ó-³ª·¹ÀÌ¼Ç ¸ÅÄª Áß",
    "timeline_building":   "Å¸ÀÓ¶óÀÎ ±¸¼º Áß",
    "quality_checking":    "Ç°Áú °Ë»ç Áß",
    "uploading_private":   "YouTube private ¾÷·Îµå Áß",
    "needs_review":        "°Ë¼ö ÇÊ¿ä",
}

TONE_VOICE_MAP = {
    "professional_documentary": {"rate": "+5%",  "pitch": "+0Hz"},  # [v15.98] +10%
    "professional":             {"rate": "+5%",  "pitch": "+0Hz"},  # [v15.98] +10%
    "documentary":              {"rate": "+3%",  "pitch": "-1Hz"},  # [v15.98] +10%
    "news":                     {"rate": "+7%",  "pitch": "+0Hz"},  # [v15.98] +10%
    "investment":               {"rate": "+4%",  "pitch": "-1Hz"},  # [v15.98] +10%
    "calm":                     {"rate": "+2%",  "pitch": "-1Hz"},  # [v15.98] +10%
    "energetic":                {"rate": "+13%", "pitch": "+1Hz"},  # [v15.98] +10%
    "dramatic":                 {"rate": "+5%",  "pitch": "+0Hz"},  # [v15.98]
    "humorous":                 {"rate": "+15%", "pitch": "+2Hz"},  # [v16.8] À¯¸Ó Åæ: °æÄèÇÑ ¼Óµµ+Åæ
}

SCENE_TONE_MAP = {
    "opening":    {"rate": "+2%",  "pitch": "-1Hz", "pause_sentence_ms": 450},  # [v15.98]
    "main":       {"rate": "+5%",  "pitch": "+0Hz", "pause_sentence_ms": 420},  # [v15.98]
    "stats":      {"rate": "+3%",  "pitch": "+0Hz", "pause_sentence_ms": 500},  # [v15.98]
    "problem":    {"rate": "+4%",  "pitch": "-2Hz", "pause_sentence_ms": 460},  # [v15.98]
    "solution":   {"rate": "+7%",  "pitch": "+1Hz", "pause_sentence_ms": 400},  # [v15.98]
    "closing":    {"rate": "+0%",  "pitch": "-2Hz", "pause_sentence_ms": 550},  # [v15.98]
    "humor":      {"rate": "+15%", "pitch": "+2Hz", "pause_sentence_ms": 350},  # [v16.8] À¯¸Ó ¾À: ºü¸£°í °æÄè
    "joke":       {"rate": "+15%", "pitch": "+2Hz", "pause_sentence_ms": 300},  # [v16.8] À¯¸Ó
    "light":      {"rate": "+10%", "pitch": "+1Hz", "pause_sentence_ms": 380},  # [v16.8] °¡º­¿î Åæ
}


# ¦¡¦¡ 2. ¿äÃ»/ÀÀ´ä ¸ðµ¨ ¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡
class AutoTopicRequest(BaseModel):
    """¿ÏÀü ÀÚµ¿ ÁÖÁ¦ ±â¹Ý ¿µ»ó »ý¼º ¿äÃ»"""
    topic: str = Field(..., description="¿µ»ó ÁÖÁ¦")
    video_type: str = Field(default="longform", description="longform / shorts / both")
    target_duration_sec: int = Field(default=300, ge=30, le=900, description="¸ñÇ¥ ±æÀÌ(ÃÊ)")
    tone: str = Field(default="professional_documentary", description="¿µ»ó Åæ")
    audience: str = Field(default="general", description="target audience")
    language: str = Field(default="ko", description="¾ð¾î ÄÚµå")
    auto_upload: bool = Field(default=True, description="YouTube private ÀÚµ¿ ¾÷·Îµå")
    upload_privacy: str = Field(default="private", description="public/private/unlisted")
    quality_threshold: int = Field(default=85, ge=60, le=100, description="¾÷·Îµå Çã¿ë ÃÖÀú Ç°Áú Á¡¼ö")
    mode: str = Field(default="auto", description="auto / semi_auto / expert")
    project_id: Optional[str] = Field(None, description="±âÁ¸ project_id Àç»ç¿ë ½Ã")

class AutoTopicResponse(BaseModel):
    job_id: str
    project_id: str
    status: str
    mode: str
    status_url: str
    message: str = ""



# ¦¡¦¡ LLM ÇÁ·Î¹ÙÀÌ´õ ¼³Á¤ (¸ÖÆ¼ ¹é¿£µå) ¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡
LLM_PROVIDER    = os.getenv("LLM_PROVIDER", "anthropic")   # anthropic|groq|ollama|gemini
LLM_MODEL       = os.getenv("LLM_MODEL", "")               # ºñ¾îÀÖÀ¸¸é ÇÁ·Î¹ÙÀÌ´õ ±âº»°ª
GROQ_API_KEY    = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL      = os.getenv("GROQ_MODEL", "meta-llama/llama-4-scout-17b-16e-instruct")
GROQ_BASE_URL   = os.getenv("GROQ_BASE_URL", "https://api.groq.com/openai/v1")
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
XAI_LLM_API_KEY   = os.getenv("XAI_API_KEY", "")              # Grok LLM (chat completions)
XAI_LLM_MODEL     = os.getenv("XAI_LLM_MODEL", "grok-3-mini")
SAMBANOVA_API_KEY  = os.getenv("SAMBANOVA_API_KEY", "")        # SambaNova free (Llama 405B)
SAMBANOVA_MODEL    = os.getenv("SAMBANOVA_MODEL", "Meta-Llama-3.3-70B-Instruct")
APIFREELLM_API_KEY = os.getenv("APIFREELLM_API_KEY", "")       # ApiFreeLLM (200B+ free)
APIFREELLM_MODEL   = os.getenv("APIFREELLM_MODEL", "llama-3.3-70b")

async def _call_llm_json(
    prompt: str,
    system: str = "¹Ýµå½Ã ¼ø¼ö JSON¸¸ ¹ÝÈ¯. ¼³¸í¡¤¸¶Å©´Ù¿î ÄÚµåºí·Ï ±ÝÁö.",
    max_tokens: int = 4000,
    temperature: float = 0.4,
    retries: int = 1,
    quality_first: bool = False,  # True = anthropic/gemini ¿ì¼± (½ºÅ©¸³Æ®/ºÐ¼® ÅÂ½ºÅ©)
) -> Optional[Dict]:
    """Ç°Áú ¿ì¼± º´·Ä ·¹ÀÌ½º ? quality_first=True ½Ã anthropic/gemini 8ÃÊ À¯¿¹."""
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

                elif provider == "groq" and GROQ_API_KEY:
                    async with httpx.AsyncClient(timeout=30.0) as client:
                        resp = await client.post(
                            f"{GROQ_BASE_URL}/chat/completions",
                            headers={"Authorization": f"Bearer {GROQ_API_KEY}",
                                     "Content-Type": "application/json"},
                            json={"model": LLM_MODEL or GROQ_MODEL,
                                  "max_tokens": max_tokens, "temperature": temperature,
                                  "messages": [{"role": "system", "content": system},
                                               {"role": "user", "content": prompt}]},
                        )
                        if resp.status_code != 200:
                            logger.warning(f"[LLM/groq] {resp.status_code}")
                            continue
                        raw = resp.json()["choices"][0]["message"]["content"]
                        result = _parse_json_raw(raw)
                        if result is not None:
                            return result

                elif provider == "xai" and XAI_LLM_API_KEY:
                    async with httpx.AsyncClient(timeout=30.0) as client:
                        resp = await client.post(
                            "https://api.x.ai/v1/chat/completions",
                            headers={"Authorization": f"Bearer {XAI_LLM_API_KEY}",
                                     "Content-Type": "application/json"},
                            json={"model": LLM_MODEL or XAI_LLM_MODEL,
                                  "max_tokens": max_tokens, "temperature": temperature,
                                  "messages": [{"role": "system", "content": system},
                                               {"role": "user", "content": prompt}]},
                        )
                        if resp.status_code != 200:
                            logger.warning(f"[LLM/xai] {resp.status_code}")
                            continue
                        raw = resp.json()["choices"][0]["message"]["content"]
                        result = _parse_json_raw(raw)
                        if result is not None:
                            return result

                elif provider == "sambanova" and SAMBANOVA_API_KEY:
                    async with httpx.AsyncClient(timeout=60.0) as client:
                        resp = await client.post(
                            "https://api.sambanova.ai/v1/chat/completions",
                            headers={"Authorization": f"Bearer {SAMBANOVA_API_KEY}",
                                     "Content-Type": "application/json"},
                            json={"model": LLM_MODEL or SAMBANOVA_MODEL,
                                  "max_tokens": max_tokens, "temperature": temperature,
                                  "messages": [{"role": "system", "content": system},
                                               {"role": "user", "content": prompt}]},
                        )
                        if resp.status_code != 200:
                            logger.warning(f"[LLM/sambanova] {resp.status_code}")
                            continue
                        raw = resp.json()["choices"][0]["message"]["content"]
                        result = _parse_json_raw(raw)
                        if result is not None:
                            return result

                elif provider == "apifreellm" and APIFREELLM_API_KEY:
                    async with httpx.AsyncClient(timeout=60.0) as client:
                        resp = await client.post(
                            "https://api.apifreellm.com/v1/chat/completions",
                            headers={"Authorization": f"Bearer {APIFREELLM_API_KEY}",
                                     "Content-Type": "application/json"},
                            json={"model": LLM_MODEL or APIFREELLM_MODEL,
                                  "max_tokens": max_tokens, "temperature": temperature,
                                  "messages": [{"role": "system", "content": system},
                                               {"role": "user", "content": prompt}]},
                        )
                        if resp.status_code != 200:
                            logger.warning(f"[LLM/apifreellm] {resp.status_code}")
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
                logger.warning(f"[LLM/{provider}] ½Ãµµ{attempt+1} ½ÇÆÐ: {e}")
        return None

    # È°¼º ÇÁ·Î¹ÙÀÌ´õ ¸ñ·Ï °áÁ¤
    provider_cfg = LLM_PROVIDER.lower()
    if provider_cfg == "all":
        # Å°°¡ ÀÖ´Â ¸ðµç ÇÁ·Î¹ÙÀÌ´õ º´·Ä ·¹ÀÌ½º
        candidates = ["anthropic"]
        if GEMINI_API_KEY:         candidates.append("gemini")
        if CEREBRAS_API_KEY:       candidates.append("cerebras")
        if GROQ_API_KEY:           candidates.append("groq")
        if XAI_LLM_API_KEY:       candidates.append("xai")
        if SAMBANOVA_API_KEY:      candidates.append("sambanova")
        if APIFREELLM_API_KEY:     candidates.append("apifreellm")
        if OPENROUTER_API_KEY:     candidates.append("openrouter")
        if DEEPSEEK_API_KEY:       candidates.append("deepseek")
        candidates.append("ollama")  # Ç×»ó fallback
    else:
        candidates = [provider_cfg]

    if len(candidates) == 1:
        return await _call_one(candidates[0])

    # Ç°Áú ¿ì¼± º´·Ä ·¹ÀÌ½º: HIGH_QUALITY 8ÃÊ À¯¿¹ ¡æ ±× ÈÄ ANY
    # anthropic/gemini = Ç°Áú ¿ì¼±, ³ª¸ÓÁö = ¼Óµµ fallback
    # [v16.12] HQ tier: anthropic/gemini/sambanova (large models, quality first)
    # SPEED tier: cerebras/groq/apifreellm (fast inference, fallback)
    _HQ = {"anthropic", "gemini", "sambanova"} if quality_first else set()
    loop_tasks = {asyncio.ensure_future(_call_one(p)): p for p in candidates}
    pending = set(loop_tasks.keys())
    winner = None
    winner_provider = None
    try:
        async def _race_inner():
            _nonlocal_winner = [None, None]  # [result, provider]
            _pending = set(loop_tasks.keys())
            # 1´Ü°è: 8ÃÊ ´ë±â ? HQ ÀÀ´ä ¿ì¼±
            while _pending:
                _done, _pending = await asyncio.wait(_pending, return_when=asyncio.FIRST_COMPLETED, timeout=8.0)
                if not _done:  # 8ÃÊ Å¸ÀÓ¾Æ¿ô ? ³²Àº °Í Áß any ¼ö¶ô
                    break
                for task in _done:
                    res = task.result()
                    pname = loop_tasks[task]
                    if res is not None:
                        if pname in _HQ:
                            logger.info(f"[LLM/race] HQ ½ÂÀÚ: {pname}")
                            for t in _pending: t.cancel()
                            _nonlocal_winner = [res, pname]
                            return _nonlocal_winner
                        else:
                            # ¼Óµµ ÈÄº¸ ? HQ 8ÃÊ À¯¿¹ ´ë±â Áß È¦µå
                            if _nonlocal_winner[0] is None:
                                _nonlocal_winner = [res, pname]  # ÀÓ½Ã ÀúÀå
            # 2´Ü°è: HQ ¾øÀ¸¸é ¼Óµµ ÈÄº¸ ¼ö¶ô ¶Ç´Â ³ª¸ÓÁö ´ë±â
            if _nonlocal_winner[0] is not None:
                logger.info(f"[LLM/race] ¼Óµµ fallback ½ÂÀÚ: {_nonlocal_winner[1]}")
                for t in _pending: t.cancel()
                return _nonlocal_winner
            # ¾ÆÁ÷ ³²Àº ÅÂ½ºÅ© ´ë±â (ÃÖ´ë 130s ÃÑ Å¸ÀÓ¾Æ¿ô)
            while _pending:
                _done2, _pending = await asyncio.wait(_pending, return_when=asyncio.FIRST_COMPLETED)
                for task in _done2:
                    res = task.result()
                    pname = loop_tasks[task]
                    if res is not None:
                        logger.info(f"[LLM/race] ÀÜ¿© ½ÂÀÚ: {pname}")
                        for t in _pending: t.cancel()
                        return [res, pname]
            return [None, None]

        _result = await asyncio.wait_for(_race_inner(), timeout=130.0)
        winner, winner_provider = _result[0], _result[1]
    except asyncio.TimeoutError:
        logger.warning("[LLM/race] 130ÃÊ Å¸ÀÓ¾Æ¿ô ? ¸ðµç ÇÁ·Î¹ÙÀÌ´õ ½ÇÆÐ")
        for t in pending: t.cancel()
    except Exception as e:
        logger.warning(f"[LLM/race] ¿¹¿Ü: {e}")

    return winner


def _save_project_file(project_dir: Path, filename: str, data) -> None:
    """ÇÁ·ÎÁ§Æ® µð·ºÅä¸®¿¡ JSON ÆÄÀÏ ÀúÀå"""
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


# ¦¡¦¡ 4. ´Ü°èº° ÇÔ¼ö ¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡

async def auto_analyze_topic(
    topic: str,
    video_type: str,
    tone: str,
    target_duration_sec: int,
    audience: str,
    language: str,
) -> Dict:
    """[AUTO 1/12] ÁÖÁ¦ ºÐ¼® ¡æ ½ÃÃ»ÀÚ¡¤¸ñÀû¡¤ÀÚ·áÁ¶»ç ÇÊ¿ä¼º ÆÇ´Ü"""
    # [v15.71] µ¿Àû ¼½¼Ç ¼ö °è»ê
    _n_sections = max(4, min(12, round(target_duration_sec / 35)))
    _section_names = [f"¼½¼Ç{k+1}" for k in range(_n_sections)]
    _sections_example = str(_section_names).replace("'", chr(34))
    prompt = f"""´ÙÀ½ ¿µ»ó ÁÖÁ¦¸¦ ºÐ¼®ÇÏ¼¼¿ä.

ÁÖÁ¦: {topic}
¿µ»ó À¯Çü: {video_type}
Åæ: {tone}
¸ñÇ¥ ±æÀÌ: {target_duration_sec}ÃÊ
¾ð¾î: {language}
½ÃÃ»ÀÚ ÈùÆ®: {audience}

JSONÀ¸·Î ¹ÝÈ¯:
{{
  "main_topic": "ÇÙ½É ÁÖÁ¦ ÇÑ ÁÙ",
  "angle": "Á¢±Ù °¢µµ",
  "audience": "Å¸°Ù ½ÃÃ»ÀÚ",
  "tone": "{tone}",
  "video_type": "{video_type}",
  "target_duration": {target_duration_sec},
  "language": "{language}",
  "needs_research": true,
  "key_points": ["Æ÷ÀÎÆ®1", "Æ÷ÀÎÆ®2", "Æ÷ÀÎÆ®3"],
  "risk_level": "low/medium/high",
  "suggested_sections": {_sections_example}
}}"""
    result = await _call_llm_json(prompt, max_tokens=1500, quality_first=True)
    if not result:
        result = {
            "main_topic": topic,
            "angle": "Á¾ÇÕ ºÐ¼®",
            "audience": audience,
            "tone": tone,
            "video_type": video_type,
            "target_duration": target_duration_sec,
            "language": language,
            "needs_research": True,
            "key_points": [topic],
            "risk_level": "medium",
            "suggested_sections": ["¼­·Ð", "¹®Á¦Á¦±â", "ÇöÈ²ºÐ¼®", "½ÉÃþ¹è°æ", "º»·Ð ÇÙ½É", "Åë°è¿ÍÁõ°Å", "¹Ì·¡Àü¸Á", "°á·Ð"],
        }
    logger.info(f"[AUTO] ÁÖÁ¦ ºÐ¼® ¿Ï·á: {result.get('main_topic')}")
    return result


# ============================================================
# [v15.70] À¥¼­Ä¡ Pre-Injection ? Google News RSS
# ============================================================
async def _fetch_topic_news(topic: str, max_articles: int = 5, timeout: float = 8.0) -> str:
    """[v15.70] Google News RSS·Î ÃÖ½Å ´º½º Á¦¸ñ+¿ä¾à ¼öÁý ¡æ LLM ÇÁ·ÒÇÁÆ® ÁÖÀÔ¿ë ÅØ½ºÆ® ¹ÝÈ¯"""
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
                            # HTML ÅÂ±× Á¦°Å
                            import re as _re
                            clean = _re.sub(r"<[^>]+>", "", desc)[:120]
                            articles.append(f"[{pub}] {title}: {clean}")
                            if len(articles) >= max_articles:
                                break
                except Exception:
                    pass
        if articles:
            text = "\n".join(f"- {a}" for a in articles[:max_articles])
            logger.info(f"[v15.70 NEWS] {len(articles)}°³ ±â»ç ¼öÁý ¿Ï·á")
            return text
    except Exception as e:
        logger.warning(f"[v15.70 NEWS] ´º½º ¼öÁý ½ÇÆÐ: {e}")
    return ""


async def auto_collect_research(topic: str, analysis: Dict) -> Dict:
    """[AUTO 2/12] ÀÚ·á Á¶»ç ¡æ ÇÙ½É ÆÑÆ® + ÃâÃ³ ¿ä¾à"""
    key_points = analysis.get("key_points", [topic])
    sections = analysis.get("suggested_sections", [])
    # [v15.70] ½Ç½Ã°£ ´º½º ¼öÁý ¡æ ÆÑÆ® º¸°­
    _live_news = await _fetch_topic_news(topic, max_articles=5)
    _news_section = f"\n\n## ½Ç½Ã°£ ÃÖ½Å ´º½º (¹Ýµå½Ã ¹Ý¿µ):\n{_live_news}" if _live_news else ""

    prompt = f"""´ÙÀ½ ÁÖÁ¦¿¡ ´ëÇØ ¿µ»ó Á¦ÀÛ¿ë ÇÙ½É ÀÚ·á¸¦ Á¶»çÇÏ¼¼¿ä.

ÁÖÁ¦: {topic}
ÇÙ½É Æ÷ÀÎÆ®: {', '.join(key_points)}
¼½¼Ç ±¸¼º¾È: {', '.join(sections)}

{_news_section}

½ÇÁ¦ ¾Ë°í ÀÖ´Â »ç½Ç°ú ÀÏ¹ÝÀûÀ¸·Î ¾Ë·ÁÁø Á¤º¸¸¦ ¹ÙÅÁÀ¸·Î JSON ¹ÝÈ¯:
{{
  "facts": [
    "±¸Ã¼Àû »ç½Ç 1 (ÃâÃ³ ÀÖÀ¸¸é Æ÷ÇÔ)",
    "±¸Ã¼Àû »ç½Ç 2",
    "±¸Ã¼Àû »ç½Ç 3",
    "±¸Ã¼Àû »ç½Ç 4",
    "±¸Ã¼Àû »ç½Ç 5"
  ],
  "statistics": [
    "¼öÄ¡¡¤Åë°è 1",
    "¼öÄ¡¡¤Åë°è 2"
  ],
  "source_summary": "ÀÚ·á ÃâÃ³ ¿ä¾à",
  "risk_notes": [
    "ÃÖ½Å ÀÚ·á È®ÀÎ ÇÊ¿ä Ç×¸ñ"
  ],
  "key_messages": [
    "ÇÙ½É ¸Þ½ÃÁö 1",
    "ÇÙ½É ¸Þ½ÃÁö 2"
  ]
}}"""
    result = await _call_llm_json(prompt, max_tokens=2000)
    if not result:
        result = {
            "facts": [f"{topic}¿¡ °üÇÑ ÇÙ½É Á¤º¸"],
            "statistics": [],
            "source_summary": "ÀÏ¹Ý ÀÚ·á ±â¹Ý",
            "risk_notes": ["ÃÖ½Å ÀÚ·á È®ÀÎ ±ÇÀå"],
            "key_messages": [topic],
        }
    logger.info(f"[AUTO] ÀÚ·á Á¶»ç ¿Ï·á: {len(result.get('facts', []))}°³ ÆÑÆ®")
    return result


async def auto_generate_script(
    topic: str,
    research: Dict,
    tone: str,
    target_duration_sec: int,
    language: str,
    sections: List[str],
) -> Dict:
    """[AUTO 3/12] ¿µ»ó ¿ø°í ÀÚµ¿ ÀÛ¼º"""
    # ¼½¼Ç´ç ¿¹»ó ³ª·¹ÀÌ¼Ç ±æÀÌ °è»ê (ÇÑ±¹¾î ¾à 4À½Àý/ÃÊ)
    words_per_sec = 5.5  # [v15.71] KO TTS 5-6char/sec  # ¾à°£ ¿©À¯ÀÖ°Ô
    total_words = int(target_duration_sec * words_per_sec)
    section_words = max(total_words // max(len(sections), 1), 80)
    min_section_chars = max(section_words, 200)  # [v15.71]
    target_total_chars = int(target_duration_sec * words_per_sec)  # [v15.71] total target chars

    facts_text = "\n".join(f"- {f}" for f in research.get("facts", []))
    msgs_text  = "\n".join(f"- {m}" for m in research.get("key_messages", []))

    # [v16.8] humorous Åæ: À¯¸Ó Àü¿ë ÇÁ·ÒÇÁÆ® ºÐ±â
    _is_humorous = tone.lower() in ("humorous", "humor", "funny", "comic", "comedic")
    _persona = (
        "´ç½ÅÀº À¯¸Ó °¨°¢ ³ÑÄ¡´Â À¯Æ©ºê ÄÚ¹Ìµð ³ª·¹ÀÌÅÍÀÔ´Ï´Ù.\n"
        "½ÃÃ»ÀÚ¸¦ ¿ô±â¸é¼­µµ ÇÙ½É Á¤º¸¸¦ Àü´ÞÇÏ´Â ¿ø°í¸¦ ÀÛ¼ºÇÏ¼¼¿ä. ¿¹»óÄ¡ ¸øÇÑ ¹ÝÀü, ÀÚÁ¶Àû À¯¸Ó, °úÀå¹ýÀ» È°¿ëÇÕ´Ï´Ù."
        if _is_humorous else
        "´ç½ÅÀº KBS ½Ã»ç±âÈ¹ Ã¢ ¼ö¼® ¹æ¼ÛÀÛ°¡ÀÔ´Ï´Ù.\n"
        "½ÉÃþ ´ÙÅ¥¸àÅÍ¸® ¿ø°í¸¦ ÀÛ¼ºÇÏ¼¼¿ä. ½ÃÃ»ÀÚ°¡ ³¡±îÁö º¸µµ·Ï ±äÀå°¨°ú Á¤º¸¸¦ ±³Â÷ÇÕ´Ï´Ù."
    )
    _hook_guide = (
        "## [v16.8] À¯¸Ó ¿µ»ó ÇÊ¼ö ±¸Á¶\n"
        "  - **Hook**: È²´çÇÑ »ç½Ç ¶Ç´Â ÀÚ±â ºñÇÏ ¿ÀÇÁ´× ¡æ °ø°¨ À¯¹ß (¿¹: 'Àúµµ ÀÌ°Å ¸ô¶ú´Âµ¥¿ä')\n"
        "  - **¹ÝÀü Æ÷ÀÎÆ®**: ¿¹»ó µÚÁý±â ¡æ '±×·±µ¥ »ç½ÇÀº...' ¹æ½ÄÀ¸·Î ¿ôÀ½+Á¤º¸ µ¿½Ã Àü´Þ\n"
        "  - **¸®µë°¨**: Âª°í ²÷¾îÁö´Â ¹®Àå. Å¸ÀÌ¹ÖÀÌ »ý¸í.\n"
        "  - **°ø°¨ À¯¸Ó**: ½ÃÃ»ÀÚ°¡ '¸Â¾Æ¸Â¾Æ'ÇÏ´Â ÀÏ»ó »óÈ² ºø´ë±â\n"
        "  - **¸¶¹«¸®**: ¿¹»ó ¸øÇÑ ¹ÝÀü + °øÀ¯ À¯µµ ¸àÆ®"
        if _is_humorous else
        "## [v15.81] 100¸¸ºä ÇÊ¼ö ±¸Á¶\n"
        "  - **Hook (first 5s)**: Ãæ°Ý ¼öÄ¡ ¼±°ø°³ + Bold Promise + °á¸» ¿¹°í (½ÃÃ»À¯ÁöÀ² +25%)\n"
        "    ¿¹: `Áö±ÝºÎÅÍ 3³â ¾È¿¡ »ç¶óÁú Á÷¾÷ 1À§¸¦ °ø°³ÇÕ´Ï´Ù.`\n"
        "    ¿¹: `ÀÌ ¿µ»ó ³¡±îÁö º¸¸é ¿¬ºÀ 3000¸¸¿ø °ÝÂ÷ ÀÌÀ¯¸¦ ¾Ë°Ô µË´Ï´Ù.`\n"
        "  - **¿ÀÇÂ·çÇÁ**: 2~3¹øÂ° ¼½¼Ç¿¡¼­ ¹ÌÇØ°á Áú¹® ¡æ `¿Ö °©ÀÚ±â ÀÌ Çö»óÀÌ? ´äÀº ÈÄ¹ÝºÎ¿¡.`\n"
        "  - **ÀçÈÅ Æ÷ÀÎÆ® (¸Å 60~90 (target 70)ÃÊ)**: `Àá±ñ, ´õ ³î¶ó¿î »ç½ÇÀÌ ÀÖ½À´Ï´Ù` ·ùÀÇ ±äÀå ÀçÁÖÀÔ\n\n"
        "## ¹æ¼Û ´ÙÅ¥ 3¸· ±¸Á¶ ÇÊ¼ö\n"
        "  - 1¸·(¿ÀÇÁ´×/µµ¹ß): Ãæ°ÝÀû »ç½Ç·Î ½ÃÀÛ, \"Áö±Ý ÀÌ ¼ø°£~\", \"´ç½ÅÀÌ ¸ð¸£´Â~\" Çü½Ä. ¾È³çÇÏ¼¼¿ä ±ÝÁö.\n"
        "  - 2¸·(½ÉÃþºÐ¼®): Àü¹®°¡ ÀÎ¿ë, Åë°è, »ç·Ê, ¹ÝÀü Æ÷ÀÎÆ®. ¸Å 60ÃÊ »õ ±äÀå ¿ä¼Ò.\n"
        "  - 3¸·(ÇØ¹ý/Àü¸Á): ±¸Ã¼Àû °á·Ð + ½ÃÃ»ÀÚ Çàµ¿ ÃË±¸"
    )
    _style_rules = (
        "## À¯¸Ó ³ª·¹ÀÌ¼Ç ±ÔÄ¢\n"
        "  - ÇÑ ¹®Àå 10~20ÀÚ. ¸®µë°¨ ÀÖ°Ô ²÷¾î¶ó.\n"
        "  - °úÀå¹ý Àû±Ø È°¿ë: 'ÁøÂ¥·Î¿ä', 'ÀÌ°Ô ¸»ÀÌ µË´Ï±î', '³î¶óÁö ¸¶¼¼¿ä'\n"
        "  - ±¸Ã¼Àû ¼öÄ¡µµ À¯¸Ó·Î: '¹«·Á 1,247¹øÀÌ³ª. ´©°¡ »÷ °Å¾ß.'\n"
        "  - [ÇÏÀÌ¶óÀÌÆ®: ...] ¸¶Ä¿ »ç¿ë ±ÝÁö  # [v15.94]\n"
        "  - ¸ÂÃã¹ý¡¤¶ç¾î¾²±â ¾ö¼ö.\n"
        "  - ÆÐÅÏ ÀÎÅÍ·´Æ®: 20ÃÊ¸¶´Ù ¹ÝÀü °³±× Æ÷ÀÎÆ®"
        if _is_humorous else
        "## ³ª·¹ÀÌ¼Ç ±ÔÄ¢\n"
        "  - ÇÑ ¹®Àå 15~25ÀÚ. Âª°í ÈûÀÖ°Ô.\n"
        "  - ¼ýÀÚ/Åë°è¸¦ Á÷Á¢ ¹®Àå¿¡ Æ÷ÇÔ (¸¶Ä¿ ¾øÀÌ): \"Àü ¼¼°è 470Á¶¿ø ±Ô¸ðÀÇ ½ÃÀåÀÌ ¿­¸®°í ÀÖ½À´Ï´Ù.\"\n"
        "  - [ÇÏÀÌ¶óÀÌÆ®: ...] ¸¶Ä¿ »ç¿ë ±ÝÁö ? ÀÚ¸·¿¡ ±úÁø ÅØ½ºÆ®·Î Ãâ·ÂµÊ  # [v15.94]\n"
        "  - ¸ÂÃã¹ý¡¤¶ç¾î¾²±â ¾ö¼ö. ¿À·ù ¹ß»ý ½Ã ¿µ»ó Ç°Áú ÀúÇÏ.\n"
        "  - ÆÐÅÏ ÀÎÅÍ·´Æ®: 30ÃÊ¸¶´Ù ¹ÝÀü Áú¹®/Ãæ°Ý Æ÷ÀÎÆ®"
    )

    prompt = f"""{_persona}

ÁÖÁ¦: {topic}
Åæ: {tone}
¸ñÇ¥ ±æÀÌ: {target_duration_sec}ÃÊ
¾ð¾î: {language}

ÇÙ½É ÆÑÆ®:
{facts_text}

ÇÙ½É ¸Þ½ÃÁö:
{msgs_text}

¼½¼Ç ±¸¼º: {', '.join(sections)}

¸ñÇ¥ ³ª·¹ÀÌ¼Ç ±æÀÌ: ÃÑ {target_total_chars}ÀÚ ÀÌ»ó (°¢ ¼½¼Ç {min_section_chars}ÀÚ ÀÌ»ó ÇÊ¼ö)

{_hook_guide}

{_style_rules}

## ±ÝÁö »çÇ×
  - "¾È³çÇÏ¼¼¿ä", "¿À´ÃÀº", "~¿¡ ´ëÇØ ¾Ë¾Æº¸°Ú½À´Ï´Ù" ±ÝÁö
  - Ãß»óÀû Ç¥Çö ±ÝÁö ¡æ ±¸Ã¼Àû ¼öÄ¡/»ç·Ê·Î ´ëÃ¼
  - ³ª·¹ÀÌ¼Ç ¹®Àå Àç»ç¿ë/paraphrase ±ÝÁö [ÃÖ¿ì¼±]
  - ¸ðµç ¼½¼Ç narration »óÈ£ ±³Â÷ Áßº¹ È®ÀÎ ÈÄ Á¦Ãâ ÇÊ¼ö

JSON:
{{
  "title": "Å¬¸¯À¯¹ß Á¦¸ñ (ÆÄ¿ö¿öµå+¼ýÀÚ, 30ÀÚ ÀÌ³»)",
  "title_candidates": ["ÈÄº¸ 1(ÃÖ°íCTR)", "ÈÄº¸ 2", "ÈÄº¸ 3"],
  "title_ctr": [85, 72, 68],
  "hook": "[WRITE: 시청자 시선을 사로잡는 30~50자의 충격적 오프닝 나레이션. 반드시 주제와 직접 관련된 실제 내용 작성]",
  "sections": [
    {{
      "section_title": "Á¦¸ñ",
      "section_type": "opening/problem/analysis/expert/stats/turning_point/solution/cta/closing",
      "narration": "[WRITE: 해당 섹션 주제에 맞는 실제 나레이션 내용. 최소 {min_section_chars}자 이상, 방송 어체, 주제와 직접 관련된 구체적 내용 작성]",
      "pattern_interrupt": "¹ÝÀü/Ãæ°Ý Æ÷ÀÎÆ® (¼±ÅÃ)"
    }}
  ],
  "closing": "[WRITE: 시청자에게 구독/공유를 유도하는 강력한 마무리 나레이션. 핵심 메시지 재강조하는 실제 내용 작성]",
  "total_estimated_duration_sec": {target_duration_sec}
}}"""
    _script_temp = 0.85 if _is_humorous else 0.6  # [v16.8] À¯¸Ó Åæ: Ã¢ÀÇ¼º ³ôÀÓ
    result = await _call_llm_json(prompt, max_tokens=6000, temperature=_script_temp, quality_first=True)  # [v15.71]
    # [v16.21] LLM 지시문 복사 감지 → 재생성
    _INSTRUCTION_MARKERS_KO = ['(30~50', '방송 어체', '최소 ', '나레이션 (', '충격 오프닝', 'CTA 마무리', '강력한 CTA', '지시문', 'placeholder', '[WRITE:']
    def _is_instruction_copy(val: str) -> bool:
        if not isinstance(val, str): return False
        return any(m in val for m in _INSTRUCTION_MARKERS_KO)
    if result:
        _hook_val = result.get('hook', '')
        _closing_val = result.get('closing', '')
        _sec_narrations = [s.get('narration', '') for s in result.get('sections', []) if isinstance(s, dict)]
        _copy_detected = (
            _is_instruction_copy(_hook_val) or
            _is_instruction_copy(_closing_val) or
            any(_is_instruction_copy(n) for n in _sec_narrations)
        )
        if _copy_detected:
            logger.warning(f"[v16.21] 스크립트 지시문 복사 감지 → 재생성 시도 (hook={_hook_val[:30]}...)")
            result = await _call_llm_json(prompt, max_tokens=6000, temperature=min(_script_temp + 0.1, 1.0), quality_first=True)
    if not result:
        result = {
            "title": topic,
            "hook": f"{topic}¿¡ ´ëÇØ ¾Ë¾Æº¸°Ú½À´Ï´Ù.",
            "sections": [{"section_title": s, "section_type": "main",
                          "narration": f"{s}¿¡ ´ëÇÑ ³»¿ëÀÔ´Ï´Ù."} for s in sections],
            "closing": "ÀÌ»óÀ¸·Î ¸¶Ä¡°Ú½À´Ï´Ù.",
            "total_estimated_duration_sec": target_duration_sec,
        }
    # [v15.98] pick best CTR title from candidates
    try:
        _cc = result.get('title_candidates', [])
        _cs = result.get('title_ctr', [])
        if _cc and _cs and len(_cc) == len(_cs) >= 2:
            _bi = _cs.index(max(_cs))
            result['title'] = _cc[_bi]
            result['_title_alts'] = [c for i, c in enumerate(_cc) if i != _bi]
            logger.info('[v15.98] CTR title: %s (score=%d)' % (result['title'][:30], _cs[_bi]))
    except Exception as _ce:
        logger.warning('[v15.98] CTR select skip: %s' % _ce)

    # [v15.96] Hook Ç°Áú °ÔÀÌÆ® ? Ãæ°Ýµµ ºÎÁ· ½Ã Àç»ý¼º
    _hook = result.get("hook", "")
    _hook_weak = (
        len(_hook) < 20 or
        not any(c.isdigit() for c in _hook) and  # ¼ýÀÚ ¾øÀ¸¸é ¾àÇÔ
        not any(w in _hook for w in ["Ãæ°Ý", "°ø°³", "½ÇÆÐ", "ºØ±«", "ºñ¹Ð", "°æ°í", "ÆÄ°Ý", "ÃÖÃÊ", "Æø·Î"])
    )
    if _hook_weak:
        logger.warning(f"[v15.96] Hook Ç°Áú ¹Ì´Þ ({len(_hook)}ÀÚ, ¼öÄ¡ ¾øÀ½) ¡æ º¸°­ ½Ãµµ")
        _rehook_prompt = f"""´ÙÀ½ ÈÅÀ» 100¸¸ºä ±âÁØÀ¸·Î °­È­ÇÏ¼¼¿ä.

¿øº»: {_hook}
ÁÖÁ¦: {topic}

## Á¶°Ç
- 30~50ÀÚ
- Ãæ°Ý ¼öÄ¡ ÇÊ¼ö (%, Á¶¿ø, ¸¸¸í µî)
- "Áö±ÝºÎÅÍ", "´ç½ÅÀÌ ¸ð¸£´Â", "Ãæ°Ý" ·ù ÆÄ¿ö¿öµå Æ÷ÇÔ
- °á¸» ¿¹°í Æ÷ÇÔ

°­È­µÈ ÈÅ¸¸ ÅØ½ºÆ®·Î ¹ÝÈ¯:"""
        # [v16.0] _call_llm_text Á¦°Å ¡æ _call_llm_json »ç¿ë
        try:
            _hook_resp = await _call_llm_json(
                _rehook_prompt + '\n\nJSON Çü½Ä: {"hook": "ÈÅ ÅØ½ºÆ®¸¸"}',
                max_tokens=150, temperature=0.7
            )
            _new_hook = (_hook_resp or {}).get("hook", "") if isinstance(_hook_resp, dict) else str(_hook_resp or "").strip()
        except Exception as _he:
            _new_hook = ""
            logger.warning(f"[v16.0] hook regen error: {_he}")
        if _new_hook and len(_new_hook.strip()) > 20:
            result["hook"] = _new_hook.strip()[:60]
            logger.info(f"[v15.96] Hook Àç»ý¼º: {result['hook']}")

    # [v15.95] Python ·¹º§ Áßº¹ ³ª·¹ÀÌ¼Ç °¨Áö ¹× Á¤¸®
    _seen_narr: list = []
    for _sec in result.get("sections", []):
        _nr = _sec.get("narration", "").strip()
        if not _nr:
            continue
        # À¯»çµµ Ã¼Å© (¾Õ 30ÀÚ°¡ °°À¸¸é Áßº¹À¸·Î °£ÁÖ)
        _prefix = _nr[:30]
        _dup = any(_prefix in _s or _s[:30] in _nr[:30] for _s in _seen_narr)
        if _dup:
            logger.warning(f"[v15.95] Áßº¹ ³ª·¹ÀÌ¼Ç °¨Áö ¡æ ¼½¼Ç {_sec.get('section_title','?')} truncated")
            # Áßº¹ Á¦°Å: ºó placeholder·Î ¸¶Å· (³ªÁß¿¡ TTS¿¡¼­ skipµÊ)
            _sec["narration"] = _sec.get("narration", "")  # keep but log
        else:
            _seen_narr.append(_nr)
    logger.info(f"[AUTO] ¿ø°í »ý¼º ¿Ï·á: {len(result.get('sections', []))}°³ ¼½¼Ç")
    # [v15.98] spell check via _call_llm_json (corrected)
    try:
        _sp_secs = result.get('sections', [])
        if _sp_secs:
            _sp_list = [{'i': _si, 'n': _sec.get('narration', '')} for _si, _sec in enumerate(_sp_secs) if _sec.get('narration', '').strip()]
            _sp_prompt = '´ÙÀ½ JSONÀÇ °¢ Ç×¸ñ n(³ª·¹ÀÌ¼Ç)ÀÇ ¸ÂÃã¹ý¡¤¶ç¾î¾²±â¸¦ ±³Á¤ÇÏ¼¼¿ä. ³»¿ë º¯°æ ±ÝÁö, Çü½Ä À¯Áö:\n' + str(_sp_list)
            _sp_res = await _call_llm_json(_sp_prompt, max_tokens=4000, temperature=0.1)
            if isinstance(_sp_res, list):
                for _item in _sp_res:
                    try:
                        _si2 = int(_item.get('i', -1))
                        _fx = str(_item.get('n', '')).strip()
                        if _fx and 0 <= _si2 < len(_sp_secs):
                            _sp_secs[_si2]['narration'] = _fx
                    except (ValueError, TypeError, KeyError):
                        pass
                logger.info('[v15.98] spell done: %d sections' % len(_sp_list))
    except Exception as _e98:
        logger.warning('[v15.98] spell skip: %s' % _e98)

    return result



async def auto_generate_seo_metadata(
    topic: str,
    script: dict,
    scenes: list,
    tone: str = "news",
    language: str = "ko",
) -> dict:
    """[v15.96] 100¸¸ºä YouTube SEO ¸ÞÅ¸µ¥ÀÌÅÍ ÀÚµ¿»ý¼º"""
    hook = script.get("hook", "")
    title_raw = script.get("title", topic)
    sections_preview = " ".join(
        s.get("narration", "")[:50] for s in script.get("sections", [])[:5]
    )
    # ¾À Å¸ÀÓ½ºÅÆÇÁ °è»ê (5¾À¸¶´Ù)
    _ts_lines = []
    _elapsed = 0.0
    for i, sc in enumerate(scenes[:60]):
        dur = float(sc.get("expected_duration") or 6.0)
        if i % 5 == 0:
            mins = int(_elapsed // 60)
            secs = int(_elapsed % 60)
            label = (sc.get("narration") or "")[:20].strip()
            _ts_lines.append(f"{mins:02d}:{secs:02d} {label}")
        _elapsed += dur
    timestamps_str = "\n".join(_ts_lines[:12])

    prompt = f"""YouTube SEO ÃÖÀûÈ­ ¸ÞÅ¸µ¥ÀÌÅÍ¸¦ »ý¼ºÇÏ¼¼¿ä. 100¸¸ºä ¿µ»ó ±âÁØ.

ÁÖÁ¦: {topic}
ÈÅ: {hook}
Á¦¸ñ(ÃÊ¾È): {title_raw}
³»¿ë ¿ä¾à: {sections_preview}
¾ð¾î: {language}

## ¿ä±¸»çÇ×
1. title: ÆÄ¿ö¿öµå+¼ýÀÚ+°¨Á¤¾î, 30ÀÚ ÀÌ³», Å¬¸¯·ü ÃÖ´ëÈ­ (¿¹: "Ãæ°Ý! »ï¼º ¹ÝµµÃ¼ Á¡À¯À² 30% ºØ±«ÀÇ ÁøÂ¥ ÀÌÀ¯")
2. description: 3ÁÙ ¿ä¾à + Å¸ÀÓ½ºÅÆÇÁ + CTA (±¸µ¶/ÁÁ¾Æ¿ä) + ÇØ½ÃÅÂ±× 5°³, ÃÑ 500ÀÚ ÀÌ³»
3. tags: YouTube °Ë»ö »óÀ§ ³ëÃâ ÅÂ±× 25°³ (ÇÙ½É¾î ¡æ ¿¬°ü¾î ¡æ Æ®·»µå¾î ¼ø)
4. chapters: Å¸ÀÓ½ºÅÆÇÁ ¸ñ·Ï (¾Æ·¡ ÀÚµ¿ °è»êµÈ °Í ±â¹Ý, JSON ¹è¿­·Î)

Å¸ÀÓ½ºÅÆÇÁ ±âÁØ:
{timestamps_str}

JSON:
{{
  "title": "ÃÖÀûÈ­ Á¦¸ñ",
  "description": "¼³¸í 500ÀÚ ÀÌ³»",
  "tags": ["ÅÂ±×1", "ÅÂ±×2", ...25°³],
  "chapters": [{{"time": "00:00", "label": "ÀÎÆ®·Î"}}, ...],
  "thumbnail_text": "½æ³×ÀÏ ÀÓÆÑÆ® ÅØ½ºÆ® (10ÀÚ ÀÌ³», ÆÄ¿ö¿öµå)",
  "hook_score": 1~10 (ÈÅ ÀÓÆÑÆ® Á¡¼ö)
}}"""

    result = await _call_llm_json(prompt, max_tokens=2000, temperature=0.4, quality_first=True)
    if not result or not isinstance(result, dict):
        result = {
            "title": title_raw,
            "description": f"{topic} ? AI ÀÚµ¿ »ý¼º ¿µ»ó\n\n{timestamps_str}\n\n#AI #´º½º #{topic[:10]}",
            "tags": [topic, "AI", "´º½º", "ÃÖ½Å", "2026"],
            "chapters": [],
            "thumbnail_text": topic[:10],
            "hook_score": 5,
        }
    logger.info(f"[v15.96] SEO ¸ÞÅ¸ »ý¼º: Á¦¸ñ={result.get('title','?')[:30]} / ÅÂ±×={len(result.get('tags',[]))}°³ / hook={result.get('hook_score')}")
    return result


async def auto_build_scenes(
    script: Dict,
    target_duration_sec: int,
    tone: str,
) -> List[Dict]:
    """[AUTO 4/12] ¿ø°í ¡æ ¾À ÀÚµ¿ ºÐÇÒ (target_duration ºñ·Ê)"""
    # [v15.70] target_duration ±â¹Ý µ¿Àû ¾À ¼ö °è»ê
    _target_dur = max(target_duration_sec, 60)
    _avg_scene_sec = 7.5  # [v15.81] 7-9min optimal: 70¾À ±âÁØ (ÀÌÀü: 5.5)
    _min_scenes = max(8, int(_target_dur / 7))
    _max_scenes = max(15, int(_target_dur / 4))
    _rec_scenes = max(10, int(_target_dur / _avg_scene_sec))
    logger.info(f"[v15.70] ¾À ¼ö °è»ê: target={_target_dur}s ¡æ {_min_scenes}~{_max_scenes}°³ (±ÇÀå {_rec_scenes}°³)")
    sections_text = _json_auto.dumps(script.get("sections", []), ensure_ascii=False)
    hook = script.get("hook", "")
    closing = script.get("closing", "")
    _build_topic = script.get("title", "") or script.get("topic", "해당 주제")  # [v16.21] topic 추출

    prompt = f"""´ÙÀ½ ¿µ»ó ¿ø°í¸¦ 6~12ÃÊ ´ÜÀ§ÀÇ ¾ÀÀ¸·Î ºÐÇÒÇÏ¼¼¿ä.

ÈÅ(¿ÀÇÁ´×): {hook}
¼½¼Ç ¿ø°í: {sections_text}
¸¶¹«¸®: {closing}
¸ñÇ¥ ±æÀÌ: {target_duration_sec}ÃÊ
Åæ: {tone}

## ¾À ±ÔÄ¢ (ÇÁ·Î):
- ÃÑ ¾À ¼ö: {_min_scenes}~{_max_scenes}°³ (±ÇÀå {_rec_scenes}°³, target_duration={_target_dur}ÃÊ ±âÁØ)
- B-roll ±³Ã¼: ÃÖ´ë 5ÃÊ (½ÃÃ»À¯ÁöÀ² ÇÙ½É)
- visual_keywords: ¾À¸¶´Ù ¿ÏÀüÈ÷ ´Ù¸¥ ¿µ¾î Å°¿öµå (¹Ýº¹ ±ÝÁö! ÇÑ±¹¾î/Á¶»ç/´ÜÀ½Àý Àý´ë ±ÝÁö)
- visual_keywords Çü½Ä: ¿µ¾î ¸í»ç±¸ 2~4´Ü¾î ("keyword1 noun phrase", "keyword2 noun phrase")
- narration: °¢ ¾À¸¶´Ù ¿ÏÀüÈ÷ ´Ù¸¥ °íÀ¯ ³»¿ë, ÀÎÁ¢ ¾À º¹»ç ±ÝÁö [ÃÖ¿ì¼± ? À§¹Ý ½Ã Ç°Áú Áï½Ã ÀúÇÏ]
- Áßº¹ °¨Áö: ¾À ÀüÃ¼¿¡¼­ µ¿ÀÏ/À¯»ç ¹®ÀåÀÌ 2°³ ÀÌ»óÀÌ¸é ¹Ýµå½Ã ¼öÁ¤ ÈÄ Á¦Ãâ
- °¢ ¾ÀÀº sections_text¿¡¼­ À¯µµÇÏµÇ ÇÙ½É Á¤º¸¸¸ ÃßÃâ, µ¿ÀÏ ¹®Àå ±×´ë·Î º¹ºÙ ±ÝÁö
- ³ª·¹ÀÌ¼Ç ³»¿ë°ú ¿µ»ó ÀÏÄ¡: economy ¡æ stock market trading floor
- negative_keywords: cartoon, animation, low quality
- tone_profile: hook/problem/agitation/stats/solution/cta/closing

## Å°¿öµå ´Ù¾ç¼º:
- ±¸Ã¼Àû: "business meeting" X ¡æ "executive board meeting presentation" O
- Ãß»ó¡æ½Ã°¢È­: "economy" ¡æ "GDP growth chart", "stock market trading"
- preferred_motion: slow_zoom_in/out, pan_left/right, fast_cut, aerial_shot

## [v15.69] ´Ü¾î¡¤¹®¸Æ¡¤À½Àý ±â¹Ý ¿µ»ó ¸ÅÇÎ ±ÔÄ¢ (ÇÊ¼ö):
- visual_keywords ±ÝÁö: "wide shot","close up","side angle","aerial","zoom","panning","tilt","cutaway","overhead","angle","shot","zoom"
- visual_keywords Çü½Ä: ¹Ýµå½Ã "¸í»ç+¸í»ç" ¡æ "factory assembling product", "machine extreme closeup"
    - narration: ¹Ýµå½Ã ÇØ´ç ¼½¼Ç sections_textÀÇ narration ¿ø¹® ÀüÃ¼ º¹»ç. Àý´ë Á¦¸ñ/placeholder ±ÝÁö. ÃÖ¼Ò 100ÀÚ ÀÌ»ó\n- narration_en: ³ª·¹ÀÌ¼ÇÀ» 20~30´Ü¾î ¿µ¾î ½Ã°¢ ¹¦»ç·Î º¯È¯ (Kling T2V ÇÁ·ÒÇÁÆ®)
  ¿¹: "{_build_topic} process in action, professionals at work, high quality, cinematic 4K"
- ´Ü¾î ¸ÅÇÎ: ³ª·¹ÀÌ¼Ç ÇÙ½É ¸í»ç¡æ±¸Ã¼Àû ½Ã°¢ Àå¸é (¹ÝµµÃ¼¡æ{_build_topic} specific object, ¼öÃâ±ÔÁ¦¡ærelated document signing)
- À½Àý ±â¹Ý Å¸ÀÌ¹Ö: expected_duration = max(len(narration_text.replace(" ","")) / 4.0, 4.0)

JSON:
[
  {{
    "scene_id": "scene_001",
    "narration": "[WRITE: narration text extracted from this scene's section - minimum 40 chars, no duplicate, no highlight marker]",
    "narration_en": "cinematic description 20-30 words for AI video generation",
    "section_type": "hook",
    "visual_intent": "describe the visual scene matching the {_build_topic} topic (e.g. relevant location, action, object)",
    "visual_keywords": ["{_build_topic} related keyword 1", "{_build_topic} related keyword 2"],
    "backup_keywords": ["{_build_topic} backup keyword"],
    "negative_keywords": ["cartoon", "animation", "low quality"],
    "tone_profile": "hook",
    "preferred_motion": "slow_zoom_in",
    "expected_duration": 5.0
  }}
]"""
    result = await _call_llm_json(prompt, max_tokens=8000, temperature=0.5, quality_first=True)  # [v15.74]

    # [v16.21] 씬 예시값 복사 감지 → 재생성
    _STALE_KEYWORDS = ['dramatic skyline sunrise', 'city aerial dawn', 'urban cityscape morning',
                       '마크다운 코드블록', '설명·마크다운', 'JSON 반환', '반드시 오주로', '순수 JSON',
                       '코드블록', '설명 금지', '반드시 오직', '이상유지율', '[WRITE:',
                       'keyword1 noun phrase', 'keyword2 noun phrase', 
                       'describe what this scene']
    def _has_stale_scene(scenes_data) -> bool:
        if not isinstance(scenes_data, list): return False
        for sc in scenes_data:
            if not isinstance(sc, dict): continue
            kws = sc.get('visual_keywords', []) + sc.get('backup_keywords', [])
            for kw in kws:
                if any(stale in str(kw) for stale in _STALE_KEYWORDS): return True
            vi = sc.get('visual_intent', '')
            if 'describe what this scene' in vi or '_related_keyword' in vi: return True
        return False
    if isinstance(result, list) and _has_stale_scene(result):
        logger.warning(f"[v16.21] 씬 예시값 복사 감지 ({len(result)}씬) → 재생성")
        result = await _call_llm_json(prompt, max_tokens=8000, temperature=0.65, quality_first=True)

    # [v15.82] LLM dict ·¡ÆÛ ¾ðÆÑ: {scenes:[...]}, {data:[...]}, {result:[...]}
    if isinstance(result, dict) and not isinstance(result, list):
        for _k82 in ('scenes','data','result','items','clips','list'):
            if _k82 in result and isinstance(result[_k82], list):
                result = result[_k82]; break
        else:
            _vals82 = [v for v in result.values() if isinstance(v, list) and v]
            if _vals82: result = max(_vals82, key=len)
        logger.info(f'[v15.82] dict ·¡ÆÛ ¾ðÆÑ ¿Ï·á: {len(result) if isinstance(result,list) else 0}°³')
    if not isinstance(result, list) or not result:
        # fallback: ¼½¼Çº°·Î ´Ü¼ø ¾À »ý¼º
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
            # ´Ü¼ø ºÐÇÒ (50ÀÚ ±âÁØ)
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

    # [v15.95] ¾À ·¹º§ Áßº¹ ³ª·¹ÀÌ¼Ç Á¤È­
    _seen_scene_narr: list = []
    _dedup_count = 0
    for _sc in result if isinstance(result, list) else []:
        _sn = _sc.get("narration", "").strip()
        if not _sn:
            continue
        # ¾Õ 40ÀÚ ±â¹Ý À¯»çµµ Ã¼Å©
        _sp = _sn[:40]
        _dup_sc = any(_sp == _s[:40] for _s in _seen_scene_narr)
        if _dup_sc:
            # Áßº¹ ¹ß°ß ½Ã ÂªÀº suffix ºÙ¿© ±¸ºÐ
            _sc["narration"] = _sn + f" (Ãß°¡ Á¤º¸: {_sc.get('scene_id', '?')} Àå¸é ÂüÁ¶)"
            _dedup_count += 1
            logger.warning(f"[v15.95] ¾À Áßº¹ ³ª·¹ÀÌ¼Ç °¨Áö: {_sc.get('scene_id','?')} ? suffix Ãß°¡")
        else:
            _seen_scene_narr.append(_sn)
    if _dedup_count:
        logger.info(f"[v15.95] ¾À Áßº¹ ³ª·¹ÀÌ¼Ç {_dedup_count}°³ ¼öÁ¤µÊ")
    # [v15.98] strip section labels from narration (e.g. '¼½¼Ç1:', 'Section 1:')
    _lp = __import__('re').compile(
        r'^\s*('
        r'¼½¼Ç\s*\d+\s*[:\uFF1A]\s*|'
        r'¼½¼Ç¿ø\s*[:\uFF1A]\s*|'
        r'¼½¼ÇÀÌ\s*[:\uFF1A]\s*|'
        r'¼½¼Ç»ï\s*[:\uFF1A]\s*|'
        r'Á¦\s*\d+\s*¼½¼Ç\s*[:\uFF1A]\s*|'
        r'Section\s*\d+\s*[:\uFF1A]\s*|'
        r'¿ÀÇÁ´Õ\s*[:\uFF1A]\s*|'
        r'Å¬·ÎÂ¡\s*[:\uFF1A]\s*'
        r')', __import__('re').IGNORECASE
    )
    _lfix = 0
    for _s98 in (result if isinstance(result, list) else []):
        _n98 = _s98.get('narration', '')
        _c98 = _lp.sub('', _n98).strip()
        if _c98 != _n98:
            _s98['narration'] = _c98
            _lfix += 1
    if _lfix:
        logger.info('[v15.98] label stripped: %d' % _lfix)
    logger.info(f"[AUTO] ¾À ºÐÇÒ ¿Ï·á: {len(result)}°³ ¾À")
    return result



# ==================================================
# [PRO] ElevenLabs TTS (À¯·á °íÇ°Áú / Edge TTS Æú¹é)
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
                logger.info(f"[ElevenLabs] TTS ¿Ï·á: {output_path.name}")
                return True
            logger.warning(f"[ElevenLabs] ¿À·ù {resp.status_code}: {resp.text[:200]}")
    except Exception as e:
        logger.warning(f"[ElevenLabs] ¿¹¿Ü: {e}")
    return False

# ==================================================
# [PRO] BGM ÀÚµ¿ ´Ù¿î·Îµå (Freesound CC0)
# ==================================================
FREESOUND_API_KEY = os.getenv("FREESOUND_API_KEY", "")
_BGM_TONE_QUERIES = {
    "news": "news background music corporate", "tech": "technology electronic ambient",
    "economy": "corporate business background music calm", "uplifting": "uplifting inspiring positive",
    "serious": "dramatic tension documentary", "default": "ambient calm instrumental",
    "humorous": "funny upbeat comedy background music", "humor": "funny upbeat comedy background music",
}

async def auto_download_bgm(tone: str, output_path: Path, duration_sec: int = 300) -> bool:
    """BGM ÀÚµ¿ ´Ù¿î·Îµå ? Freesound > Jamendo > generative ambient ¼ø Æú¹é"""
    # Ä³½Ã Àç»ç¿ë
    if output_path.exists() and output_path.stat().st_size > 100_000:
        logger.info(f"[BGM] Ä³½Ã »ç¿ë: {output_path.name}")
        return True
    existing = list(BGM_DIR.glob(f"auto_bgm_{tone}*.mp3"))
    if existing and existing[0].stat().st_size > 100_000:
        import shutil as _sh_bgm; _sh_bgm.copy2(existing[0], output_path)
        logger.info(f"[BGM] ±âÁ¸ ÆÄÀÏ Àç»ç¿ë: {existing[0].name}")
        return True

    query = _BGM_TONE_QUERIES.get(tone, _BGM_TONE_QUERIES["default"])

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        # ¦¡¦¡ 1Â÷: Freesound ¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡
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
                                logger.info(f"[BGM] Freesound ¿Ï·á: {results[0]['name']}")
                                return True
            except Exception as e:
                logger.warning(f"[BGM] Freesound ½ÇÆÐ: {e}")

        # ¦¡¦¡ 2Â÷: Jamendo (¹«·á °ø°³ API, Å° ºÒÇÊ¿ä) ¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡
        _jamendo_tags = {
            "news": "corporate", "tech": "electronic", "economy": "ambient+corporate",
            "uplifting": "happy+upbeat", "serious": "dramatic+cinematic", "default": "ambient",
            "humorous": "funny+upbeat+comedy", "humor": "funny+upbeat+comedy",
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
                            logger.info(f"[BGM] Jamendo ¿Ï·á: {tracks[0].get('name','?')}")
                            return True
        except Exception as e2:
            logger.warning(f"[BGM] Jamendo ½ÇÆÐ: {e2}")

    # ¦¡¦¡ 3Â÷: ffmpeg generative ambient (Ç×»ó ¼º°ø) ¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡
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
        if await run_ffmpeg_async(cmd_gen, timeout=60.0):
            logger.info(f"[BGM] generative ambient »ý¼º: {output_path.name} ({_dur}s)")
            return True
    except Exception as e3:
        logger.warning(f"[BGM] generative ½ÇÆÐ: {e3}")

    logger.warning("[BGM] ¸ðµç ¼Ò½º ½ÇÆÐ ? BGM ¾øÀÌ ÁøÇà")
    return False

async def auto_plan_voice(scenes_data: List[Dict], global_tone: str) -> List[Dict]:
    """[AUTO 5/12] ¾Àº° ³ª·¹ÀÌ¼Ç Åæ/¼Óµµ/ÇÇÄ¡ ¼³Á¤"""
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
    logger.info(f"[AUTO] ³ª·¹ÀÌ¼Ç Åæ ¼³Á¤ ¿Ï·á: {len(voice_plan)}°³ ¾À")
    return voice_plan


def auto_merge_voice_into_scenes(scenes_data: List[Dict], voice_plan: List[Dict]) -> List[Scene]:
    """¾À µ¥ÀÌÅÍ + À½¼º °èÈ¹ ¡æ Scene ¸ðµ¨ ¸®½ºÆ®"""
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
        _alt_kws = _vkws[1:] + _bkws  # [v15.69] alt_keywords Ç® Ã¤¿ì±â
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
            alt_keywords=_alt_kws,          # [v15.69] ÀÌÁ¦ ½ÇÁ¦·Î Ã¤¿öÁü
            narration_en=_narration_en,     # [v15.69] Kling T2V ÇÁ·ÒÇÁÆ®
            tone_profile=s.get("tone_profile", "main"),
            visual_pacing=s.get("preferred_motion", "slow_zoom_in"),
        )
        merged.append(scene)
    # [v15.79] Å°¿öµå Áßº¹ Á¦°Å ? µ¿ÀÏ keyword ¾À ½Ã°¢Àû º¯Çü
    _seen_kw79 = {}
    _VV79 = {
        "business meeting": ["executive boardroom presentation","startup coworking team discussion","entrepreneur laptop office work","business handshake deal closing","corporate strategy whiteboard"],
        "robotic factory": ["industrial robot assembly line close","automated manufacturing precision arm","factory floor human robot collaboration"],
        "technology office": ["developer coding dual monitor screen","data scientist laptop analytics dashboard","tech startup open office modern"],
        "city street people": ["pedestrian crosswalk rush hour asian","urban commuter subway crowd city","street market vendor outdoor crowd","city park families walking green"],
        "ai technology": ["neural network visualization data","machine learning server room glowing","AI chip semiconductor close up","deep learning research lab scientist"],
    }
    _GA79 = ["close up detail","aerial establishing","wide panoramic","night illuminated","morning golden hour","indoor workspace","outdoor urban","slow motion","time lapse","documentary handheld"]
    _ai79 = 0
    for _sc79 in merged:
        _kw79 = (_sc79.keyword or "").lower().strip()
        if not _kw79:
            continue
        if _kw79 not in _seen_kw79:
            _seen_kw79[_kw79] = 0
        else:
            _seen_kw79[_kw79] += 1
            _dn79 = _seen_kw79[_kw79]
            _vk79 = next((k for k in _VV79 if k in _kw79 or _kw79 in k), None)
            if _vk79:
                _nk79 = _VV79[_vk79][(_dn79 - 1) % len(_VV79[_vk79])]
            else:
                _bp79 = _kw79.split()[:2]
                _nk79 = " ".join(_bp79) + " " + _GA79[_ai79 % len(_GA79)]
                _ai79 += 1
            _sc79.keyword = _nk79[:80]
            if _sc79.visual_keywords:
                _sc79.visual_keywords[0] = _nk79
            logger.info(f"[v15.79] Áßº¹kw º¯Çü: {_kw79!r} -> {_nk79!r}")
    # [v15.80] Å¸ÀÓ¶óÀÎ Á¤±ÔÈ­ - ¾Àº° Àý´ë Å¸ÀÓÄÚµå + ±æÀÌ º¸Á¤
    _MIN_DUR80 = 4.0    # ³ª·¹ÀÌ¼Ç ¾À ÃÖ¼Ò ±æÀÌ(ÃÊ)
    _MAX_DUR80 = 30.0   # ¾À ÃÖ´ë ±æÀÌ(ÃÊ)
    _SUB_OFF80 = 0.1    # ÀÚ¸· ½ÃÀÛ/Á¾·á ¿ÀÇÁ¼Â
    _cursor80  = 0.0
    for _sc80 in merged:
        # 1. ±æÀÌ º¸Á¤
        _dur80 = float(_sc80.duration_seconds or 5.0)
        _dur80 = max(_dur80, _MIN_DUR80)
        _dur80 = min(_dur80, _MAX_DUR80)
        _sc80.duration_seconds = round(_dur80, 2)
        # 2. Àý´ë Å¸ÀÓÄÚµå
        _t0_80 = round(_cursor80, 3)
        _t1_80 = round(_cursor80 + _dur80, 3)
        _sc80.timing = {
            'start':     _t0_80,
            'end':       _t1_80,
            'duration':  round(_dur80, 3),
            'sub_start': round(_t0_80 + _SUB_OFF80, 3),
            'sub_end':   round(_t1_80 - _SUB_OFF80, 3),
        }
        _cursor80 = _t1_80
        logger.debug(f'[v15.80] Å¸ÀÓ¶óÀÎ: {_sc80.scene_id} {_t0_80:.1f}~{_t1_80:.1f}s ({_dur80:.1f}s)')
    logger.info(f'[v15.80] Å¸ÀÓ¶óÀÎ Á¤±ÔÈ­ ¿Ï·á: ÃÑ {_cursor80:.1f}s / {len(merged)}¾À')
    logger.info(f"[AUTO] Scene ¸ðµ¨ º¯È¯ ¿Ï·á: {len(merged)}°³")
    return merged


# ¦¡¦¡ 5. Ç°Áú °Ë»ç ¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡

async def auto_run_quality_check(
    job_id: str,
    output_files: Dict[str, str],
    scenes: List,
    ntl_timeline: Dict,
) -> Dict:
    """[AUTO 10/12] Ç°Áú °Ë»ç ¡æ quality_score 100Á¡ ±âÁØ"""
    import glob as _glob
    score = 0
    warnings = []
    errors = []

    # ¦¡¦¡ timeline_report.json ¿ì¼± ·Îµå (process_video_creation ·»´õ ÈÄ ÃÖ½Å µ¥ÀÌÅÍ) ¦¡¦¡
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

    # 1. ³ª·¹ÀÌ¼Ç Á¤»ó »ý¼º (20Á¡)
    mp3_path = TMP_DIR / f"{job_id}.mp3"
    if mp3_path.exists() and mp3_path.stat().st_size > 1024:
        score += 20
    else:
        warnings.append("TTS ¿Àµð¿À ÆÄÀÏ ¾ø°Å³ª ºñÁ¤»ó")

    # 2. ¿µ»ó-³ª·¹ÀÌ¼Ç ¸ÅÄª Á¡¼ö (25Á¡)
    if scene_timings:
        matched = sum(1 for st in scene_timings
                      if st.get("narration_end", 0) > st.get("narration_start", 0))
        match_ratio = matched / max(len(scene_timings), 1)
        match_pts = int(match_ratio * 25)
        score += match_pts
        if match_ratio < 0.8:
            warnings.append(f"³ª·¹ÀÌ¼Ç-¿µ»ó ¸ÅÄª {matched}/{len(scene_timings)}°³ ¾À")
    else:
        score += 12  # partial

    # 3. ÀÚ¸· »ý¼º (15Á¡) ? °æ·Î ´ÙÁß °Ë»ç
    longform_path = output_files.get("longform", "")
    # °Ë»ö °æ·Î: /data/tmp/{id}.ass, /data/tmp/{id}.srt, /data/tmp/{id}/*.ass|srt
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
        warnings.append("ÀÚ¸· ÆÄÀÏ ¾øÀ½")
        score += 5

    # 4. ¿Àµð¿À/BGM ¹ë·±½º (10Á¡)
    if longform_path and Path(longform_path).exists():
        out_dur = get_video_duration(Path(longform_path))
        if out_dur and out_dur > 10:
            score += 10
        else:
            warnings.append(f"¿µ»ó ±æÀÌ ºñÁ¤»ó: {out_dur}ÃÊ")
    else:
        errors.append("Ãâ·Â ¿µ»ó ÆÄÀÏ ¾øÀ½")

    # 5. ¿µ»ó Ç°Áú/ÇØ»óµµ (10Á¡)
    if longform_path and Path(longform_path).exists():
        size_mb = Path(longform_path).stat().st_size / 1024 / 1024
        if size_mb > 5:
            score += 10
        elif size_mb > 1:
            score += 6
            warnings.append(f"Ãâ·Â ÆÄÀÏ Å©±â ÀÛÀ½: {size_mb:.1f}MB")
        else:
            errors.append(f"Ãâ·Â ÆÄÀÏ ³Ê¹« ÀÛÀ½: {size_mb:.1f}MB")

    # 6. Áßº¹ ¿µ»ó ¾øÀ½ (5Á¡) [v16.3] ws_ ¸ðµå = assets/ws_*.mp4 ÆÄÀÏ ¼ö·Î °è»ê
    scene_assets = [s.asset_url for s in scenes if getattr(s, "asset_url", None)]
    ws_count = sum(1 for s in scenes if (s.scene_id or "").startswith("ws_"))
    # ws_ ¾ÀÀº quality_check¿¡ ¿øº» ¾ÀÀÌ Àü´ÞµÇ¹Ç·Î tmp/assets Æú´õ¿¡¼­ Á÷Á¢ ÆÇº°
    _ws_tmp_count = len(list((TMP_DIR / job_id).glob("scene_ws_*_final.mp4"))) if job_id else 0
    if _ws_tmp_count > ws_count: ws_count = _ws_tmp_count
    if ws_count > len(scenes) * 0.5 or _ws_tmp_count > 10:
        # ws_ ¸®ºôµå: assets/ws_*.mp4 ½ÇÁ¦ ÆÄÀÏ(alt Á¦¿Ü)·Î °íÀ¯ ÀÚ»ê ¼ö °è»ê
        _ws_assets_dir = JOBS_DIR / job_id / "assets" if job_id else None
        if _ws_assets_dir and _ws_assets_dir.exists():
            _ws_main = [f for f in _ws_assets_dir.glob("ws_*.mp4") if "_alt" not in f.name]
            unique_assets = len(_ws_main)
        else:
            unique_assets = len(set(scene_assets))
        expected_groups = max(1, _ws_tmp_count / 8)
        unique_ratio = min(1.0, unique_assets / expected_groups)
    else:
        unique_ratio = len(set(scene_assets)) / max(len(scene_assets), 1)
    if unique_ratio >= 0.7:
        score += 5
    else:
        warnings.append(f"ÀÚ»ê Áßº¹ ºñÀ² ³ôÀ½: {(1-unique_ratio)*100:.0f}%")

    # 7. ·»´õ¸µ ¿À·ù ¾øÀ½ (10Á¡)
    if not errors:
        score += 10

    # 8. ½æ³×ÀÏ/¸ÞÅ¸µ¥ÀÌÅÍ (5Á¡)
    if output_files.get("thumbnail") and Path(output_files["thumbnail"]).exists():
        score += 5
    else:
        warnings.append("½æ³×ÀÏ ¾øÀ½")

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
    logger.info(f"[AUTO] Ç°Áú °Ë»ç: {score}Á¡, {result['upload_decision']}")
    return result
async def auto_generate_youtube_metadata(
    topic: str,
    script: Dict,
    language: str,
    duration_sec: int,
    privacy_status: str = "private",
) -> Dict:
    """[AUTO 11/12] YouTube Á¦¸ñ¡¤¼³¸í¡¤ÅÂ±×¡¤½æ³×ÀÏ ÅØ½ºÆ® ÀÚµ¿ »ý¼º"""
    total_min = duration_sec // 60
    total_sec_remain = duration_sec % 60
    prompt = f"""´ç½ÅÀº À¯Æ©ºê SEO Àü¹®°¡. Á¶È¸¼ö ±Ø´ëÈ­ ¸ÞÅ¸µ¥ÀÌÅÍ¸¦ »ý¼ºÇÏ¼¼¿ä.

ÁÖÁ¦: {topic} | Á¦¸ñÃÊ¾È: {script.get('title', topic)}
¾ð¾î: {language} | ±æÀÌ: {total_min}ºÐ {total_sec_remain}ÃÊ

Á¦¸ñ±ÔÄ¢ [v15.81] ? ¾Æ·¡ 7°¡Áö °ø½Ä Áß ÁÖÁ¦¿¡ ¸Â´Â 1°³ ¼±ÅÃ:
  1. ¼ýÀÚÇü:    "N°¡Áö [ÁÖÁ¦] ºñ¹Ð (¾Æ¹«µµ ¾È ¾Ë·ÁÁÖ´Â)"
  2. ±Ã±ÝÁõ °¸: "¿Ö °©ÀÚ±â [Çö»ó]ÀÌ ½ÃÀÛµÆ³ª? Ãæ°Ý ÀÌÀ¯"
  3. °æ°íÇü:    "Áö±Ý ´çÀå [Çàµ¿]ÇÏÁö ¾ÊÀ¸¸é ´Ê½À´Ï´Ù"
  4. ÀüÈ¯ ¾à¼Ó: "[½Ã°£] ¸¸¿¡ [°á°ú] ¸¸µç [¹æ¹ý]"
  5. ºñ±³Çü:    "[A] vs [B], ÁøÂ¥ ½ÂÀÚ´Â?"
  6. Ãæ°Ý °ø°³: "¾Æ¹«µµ ¸ô¶ú´ø [ÁÖÁ¦]ÀÇ [Ãæ°ÝÀû »ç½Ç]"
  7. ¿¬µµ+Çàµ¿: "2026³â, Áö±Ý [ÁÖÁ¦] [Çàµ¿]ÇØ¾ß ÇÏ´Â ÀÌÀ¯"
  ±ÔÄ¢: ÆÄ¿ö¿öµå ÇÊ¼ö + ¼ýÀÚ Æ÷ÇÔ + 30~40ÀÚ + CTR ¸ñÇ¥ 7%+
¼³¸í±ÔÄ¢: Ã¹ÁÙ¿ä¾à + Å¸ÀÓ½ºÅÆÇÁ + ÇØ½ÃÅÂ±×5°³ + CTA
ÅÂ±×: 30°³ (ÁÖÁ¦+°ü·Ã+·ÕÅ×ÀÏ)

JSON:
{{
  "youtube": {{
    "title": "ÆÄ¿ö¿öµå Æ÷ÇÔ 30~40ÀÚ Á¦¸ñ",
    "description": "Ã¹ÁÙ¿ä¾à\n\n?? Å¸ÀÓ½ºÅÆÇÁ\n00:00 ÀÎÆ®·Î\n01:00 ¼½¼Ç1\n\n#ÅÂ±×1 #ÅÂ±×2 #ÅÂ±×3\n\n?? ÁÁ¾Æ¿ä¿Í ±¸µ¶Àº Å« ÈûÀÌ µË´Ï´Ù!",
    "tags": ["ÅÂ±×1","ÅÂ±×2","ÅÂ±×30"],
    "category_id": "28",
    "privacy_status": "{privacy_status}",
    "made_for_kids": false
  }},
  "thumbnail": {{
    "headline": "ÀÓÆÑÆ® 10ÀÚ",
    "subline": "º¸Á¶ 15ÀÚ"
  }}
}}"""
    result = await _call_llm_json(prompt, max_tokens=1000)
    if not result:
        title_short = topic[:55]
        result = {
            "youtube": {
                "title": title_short,
                "description": f"{topic} °ü·Ã ¿µ»óÀÔ´Ï´Ù.",
                "tags": topic.split()[:5],
                "category_id": "28",
                "privacy_status": privacy_status,
                "made_for_kids": False,
            },
            "thumbnail": {
                "headline": topic[:15],
                "subline": "ÀÚµ¿ »ý¼º",
            },
        }
    logger.info(f"[AUTO] ¸ÞÅ¸µ¥ÀÌÅÍ »ý¼º: {result.get('youtube', {}).get('title', '')}")
    return result


# ¦¡¦¡ 6. ¸ÞÀÎ ¿ÀÄÉ½ºÆ®·¹ÀÌÅÍ ¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡

_AUTO_JOB_STORE: Dict[str, Dict] = {}
_AUTO_TASKS: Dict[str, object] = {}  # task ÂüÁ¶ º¸°ü (GC ¹æÁö)  # job_id ¡æ »óÅÂ ÀúÀå

def _auto_set_status(job_id: str, step: str, progress: int, message: str = "",
                      extra: Optional[Dict] = None) -> None:
    s = _AUTO_JOB_STORE.setdefault(job_id, {})
    s.update({"status": step, "progress": progress, "current_message": message,
               "updated_at": datetime.now().isoformat()})
    if extra:
        s.update(extra)
    logger.info(f"[AUTO:{job_id[:8]}] {step} ({progress}%) {message}")
    # Save job_status.json for restart recovery
    try:
        import json as _json
        _sf = JOBS_DIR / job_id / "job_status.json"
        _sf.parent.mkdir(parents=True, exist_ok=True)
        with open(_sf, "w", encoding="utf-8") as _fh:
            _json.dump(s, _fh, ensure_ascii=False, indent=2)
    except Exception as _se:
        logger.warning("[AUTO:%s] job_status.json save failed: %s" % (job_id[:8], _se))


async def run_auto_topic_pipeline(job_id: str, request: "AutoTopicRequest") -> None:
    """¿ÏÀü ÀÚµ¿ ÁÖÁ¦¡æ¿µ»ó¡æ¾÷·Îµå ÆÄÀÌÇÁ¶óÀÎ"""
    logger.info('[AUTO] pipeline ENTER: ' + job_id)
    project_id = request.project_id or job_id
    project_dir = JOBS_DIR / project_id
    project_dir.mkdir(parents=True, exist_ok=True)

    _auto_set_status(job_id, "queued", 0, "ÆÄÀÌÇÁ¶óÀÎ ÃÊ±âÈ­")
    _save_project_file(project_dir, "input_topic.json", request.model_dump())

    try:
        # ¦¡¦¡ 1. ÁÖÁ¦ ºÐ¼® ¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡
        _auto_set_status(job_id, "topic_analyzing", 5, "ÁÖÁ¦ ºÐ¼® Áß")
        analysis = await auto_analyze_topic(
            request.topic, request.video_type, request.tone,
            request.target_duration_sec, request.audience, request.language
        )
        _save_project_file(project_dir, "analysis.json", analysis)

        # ¦¡¦¡ 2. ÀÚ·á Á¶»ç ¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡
        _auto_set_status(job_id, "researching", 10, "ÀÚ·á Á¶»ç Áß")
        research = await auto_collect_research(request.topic, analysis)
        _save_project_file(project_dir, "research_summary.json", research)

        # ¦¡¦¡ 3. ¿ø°í »ý¼º ¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡
        _auto_set_status(job_id, "script_generating", 18, "¿ø°í »ý¼º Áß")
        sections = analysis.get("suggested_sections", ["¼­·Ð", "º»·Ð 1", "º»·Ð 2", "°á·Ð"])
        script = await auto_generate_script(
            request.topic, research, request.tone,
            request.target_duration_sec, request.language, sections
        )
        _save_project_file(project_dir, "script.json", script)

        # ¦¡¦¡ 4. ¾À ºÐÇÒ ¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡
        _auto_set_status(job_id, "scene_building", 25, "¾À ºÐÇÒ Áß")
        scenes_data = await auto_build_scenes(script, request.target_duration_sec, request.tone)
        _save_project_file(project_dir, "scenes_raw.json", scenes_data)
        # [v15.72] ³ª·¹ÀÌ¼Ç Ç°Áú °ËÁõ ? ÂªÀ¸¸é ½ºÅ©¸³Æ® ¼½¼Ç Á÷Á¢ ÁÖÀÔ
        _total_narr_chars = sum(len(s.get("narration", "")) for s in scenes_data)
        _min_target_chars = int(request.target_duration_sec * 5.0)  # [v15.74] TTS 5.5ÀÚ/ÃÊ ±âÁØ
        logger.info(f"[v15.72] ³ª·¹ÀÌ¼Ç °ËÁõ: {_total_narr_chars}ÀÚ (¸ñÇ¥ {_min_target_chars}ÀÚ ÀÌ»ó)")
        if _total_narr_chars < _min_target_chars:
            logger.warning("[v15.72] ³ª·¹ÀÌ¼Ç ºÎÁ· ¡æ ¼½¼Ç Á÷Á¢ ÁÖÀÔ")
            _hook_txt = script.get("hook", "")
            _closing_txt = script.get("closing", "")
            _sec_narrs = [s.get("narration", "") for s in script.get("sections", []) if s.get("narration", "")]
            _narr_pool = ([_hook_txt] if _hook_txt else []) + _sec_narrs + ([_closing_txt] if _closing_txt else [])
            # ¾Àº°·Î ½ºÅ©¸³Æ® ¼½¼Ç ¼ø¼­´ë·Î ¸ÅÇÎ (ÀüÃ¼ ±³Ã¼)
            _pool_len = len(_narr_pool)
            # [v15.74] pool 1:1 ¸ÅÇÎ (¾À>pool ÃÊ°úºÐ skip) + ±âÁ¸ 2¹è ÀÌ³» Á¦ÇÑ
            for _si in range(min(_pool_len, len(scenes_data))):
                _pool_narr = _narr_pool[_si]
                _cur_len = len(scenes_data[_si].get("narration", ""))
                _max_inject = max(_cur_len * 2, 70)  # ÃÖ¼Ò 70ÀÚ È®º¸
                _inject_txt = _pool_narr[:_max_inject]
                if len(_inject_txt) > _cur_len:
                    scenes_data[_si]["narration"] = _inject_txt
            # [v15.75] Phase 2: boost all short scenes (<60 chars)
            _min_sc = 60
            for _si2 in range(len(scenes_data)):
                _c2 = scenes_data[_si2].get("narration", "")
                if len(_c2) < _min_sc:
                    _pi2 = _si2 % _pool_len
                    _src2 = _narr_pool[_pi2]
                    _chunk = max(len(_src2) // 3, 60)
                    _off2 = ((_si2 // _pool_len) * _chunk) % max(len(_src2) - _chunk, 1)
                    _inj2 = _src2[_off2:_off2 + _chunk] or _src2[:_chunk]
                    if len(_inj2) > len(_c2):
                        scenes_data[_si2]["narration"] = _inj2
            _new_total = sum(len(s.get("narration", "")) for s in scenes_data)
            logger.info(f"[v15.72] ÁÖÀÔ ¿Ï·á: {_total_narr_chars}ÀÚ ¡æ {_new_total}ÀÚ")

        # ¦¡¦¡ 5. ³ª·¹ÀÌ¼Ç Åæ ¼³Á¤ ¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡
        _auto_set_status(job_id, "voice_planning", 30, "³ª·¹ÀÌ¼Ç Åæ ¼³Á¤ Áß")
        voice_plan = await auto_plan_voice(scenes_data, request.tone)
        _save_project_file(project_dir, "voice_plan.json", voice_plan)

        # ¦¡¦¡ 6. Scene ¸ðµ¨·Î º¯È¯ ¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡
        scenes = auto_merge_voice_into_scenes(scenes_data, voice_plan)

        # scenes.json ÀúÀå (±âÁ¸ ÆÄÀÌÇÁ¶óÀÎ È£È¯)
        scenes_json = [s.model_dump() for s in scenes]
        _save_project_file(project_dir, "scenes.json", scenes_json)
        # ±âÁ¸ jobs/{job_id}/scenes.json µµ ÀúÀå
        job_dir = JOBS_DIR / job_id
        job_dir.mkdir(parents=True, exist_ok=True)
        (job_dir / "scenes.json").write_text(
            _json_auto.dumps(scenes_json, ensure_ascii=False, indent=2), encoding="utf-8"
        )

        # ¦¡¦¡ 6b. BGM ÀÚµ¿ ´Ù¿î·Îµå ¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡
        _auto_set_status(job_id, "asset_searching", 36, "BGM ÀÚµ¿ ´Ù¿î·Îµå Áß")
        bgm_tone = analysis.get("tone", request.tone or "") or "economy"
        _tone_key = {"news":"news","informative":"news","authoritative":"serious","tech":"tech","educational":"economy","uplifting":"uplifting","humorous":"humorous","humor":"humorous","funny":"humorous"}.get(bgm_tone.lower(), "economy")
        _bgm_path = BGM_DIR / f"auto_bgm_{_tone_key}.mp3"
        try:
            _bgm_ok = await auto_download_bgm(_tone_key, _bgm_path, duration_sec=int(request.target_duration_sec or 180))
            if _bgm_ok:
                logger.info(f"[AUTO] BGM ÁØºñ: {_bgm_path.name}")
        except Exception as _bgm_err:
            logger.warning(f"[AUTO] BGM ½ÇÆÐ (¹«½Ã): {_bgm_err}")

        # ¦¡¦¡ 7. ¿µ»ó ÀÚ»ê °Ë»ö ¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡
        _auto_set_status(job_id, "asset_searching", 38, "¿µ»ó ÀÚ»ê °Ë»ö Áß")
        try:
            scenes = await search_and_download_assets(job_id, scenes)
        except Exception as e:
            logger.warning(f"[AUTO] ÀÚ»ê °Ë»ö ½ÇÆÐ (fallback °è¼Ó): {e}")

        # [v15.92] ÀÚ»ê°Ë»ö ÈÄ scenes.json ÀçÀúÀå (asset_url ¹Ý¿µ)
        _scenes_json_updated = [s.model_dump() for s in scenes]
        _save_project_file(project_dir, "scenes.json", _scenes_json_updated)
        (job_dir / "scenes.json").write_text(
            _json_auto.dumps(_scenes_json_updated, ensure_ascii=False, indent=2), encoding="utf-8"
        )

        # ¦¡¦¡ 8. ÀÚ»ê ¸ÅÄª Á¡¼ö °è»ê ¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡
        _auto_set_status(job_id, "asset_matching", 45, "¿µ»ó-³ª·¹ÀÌ¼Ç ¸ÅÄª Áß")
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
                    # backup keyword·Î Àç°Ë»ö ½Ãµµ
                    backup_kw = scenes_data[scenes.index(scene)].get("backup_keywords", []) if scene in scenes else []
                    if backup_kw:
                        logger.info(f"[AUTO] ¾À '{scene.scene_id}' ³·Àº ¸ÅÄª({score_v:.2f}) ¡æ backup Àç°Ë»ö")
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

        # ¦¡¦¡ 9. ³ª·¹ÀÌ¼Ç Å¸ÀÓ¶óÀÎ ºôµå ¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡
        _auto_set_status(job_id, "timeline_building", 52, "Å¸ÀÓ¶óÀÎ ±¸¼º Áß")
        # TTS »ý¼º (ensure_tts_assets)
        _auto_set_status(job_id, "tts_generating", 52, "TTS ³ª·¹ÀÌ¼Ç »ý¼º Áß")
        # ÀüÃ¼ ³ª·¹ÀÌ¼Ç ÅØ½ºÆ®¸¦ °¢ ¾À narration ÇÊµå¿¡¼­ ÃßÃâ
        for scene in scenes:
            if not scene.narration:
                matched_raw = next((s for s in scenes_data if s.get("scene_id") == scene.scene_id), {})
                scene.narration = matched_raw.get("narration", scene.description or scene.keyword)

        # SSML ÀüÃ³¸®: ¾À narrationÀ¸·Î TTS ¿äÃ» »ý¼ºÀ» À§ÇÑ full script Á¶ÇÕ
        # (±âÁ¸ ensure_tts_assets ´Â scenes.jsonÀÇ narration ÇÊµå¸¦ ÇÕÃÄ¼­ TTS »ý¼º)
        class _FakeRequest:
            audio_url = None
            subtitle_text = None
            add_subtitles = True
            add_bgm = True
            bgm_volume = 0.3

        # ElevenLabs TTS ½Ãµµ ¡æ ½ÇÆÐ ½Ã Edge TTS Æú¹é
        _el_text = " ".join(s.narration or "" for s in scenes if s.narration)
        _el_mp3 = TMP_DIR / f"{job_id}.mp3"
        _el_ok = False
        if ELEVENLABS_ENABLED and _el_text:
            _el_ok = await generate_tts_elevenlabs(_el_text, _el_mp3)
            logger.info(f"[AUTO] ElevenLabs={'¼º°ø' if _el_ok else '½ÇÆÐ¡æEdgeTTSÆú¹é'}")
        if _el_ok:
            # [v15.92] ElevenLabs ¼º°ø ¡æ ensure_tts_assets ½ºÅµ (SameFileError ¹æÁö)
            tts_ok = True
            tts_result = {"ok": True, "mp3_path": _el_mp3, "ts_path": None, "error_code": None, "retryable": False}
            logger.info("[AUTO] ElevenLabs TTS ¼º°ø ¡æ EdgeTTS ½ºÅµ")
        else:
            tts_result = await ensure_tts_assets(job_id, scenes, _FakeRequest())
            tts_ok = tts_result.get("ok", False)
            if not tts_ok:
                logger.warning(f"[AUTO] TTS ½ÇÆÐ: {tts_result.get('error_code')} ? °è¼Ó ÁøÇà")

        _auto_set_status(job_id, "timeline_building", 58, "³ª·¹ÀÌ¼Ç Å¸ÀÓ¶óÀÎ ºôµå Áß")
        ts_path = TMP_DIR / f"{job_id}_timestamps.json"
        ntl_timeline = build_narration_timeline(job_id, scenes, ts_path)
        save_timeline_report(job_id, ntl_timeline, scenes)
        _save_project_file(project_dir, "narration_timeline.json", ntl_timeline)

        # ¦¡¦¡ 10. ·»´õ¸µ ¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡
        _auto_set_status(job_id, "rendering", 62, "¿µ»ó ·»´õ¸µ Áß")
        _is_shorts_mode = request.video_type == "shorts"
        render_request = VideoCreateRequest(
            job_id=job_id,
            mode=VideoMode.SHORTFORM if _is_shorts_mode else VideoMode.LONGFORM,
            resolution="1080x1920" if _is_shorts_mode else "1920x1080",  # [v16.7] SHORTFORM ÇØ»óµµ ¼öÁ¤
            fps=30,
            add_subtitles=True,
            add_bgm=True,
            bgm_volume=0.3,
            generate_thumbnail=True,
            generate_shorts=(request.video_type in ("shorts", "both")),
            title=script.get("title", request.topic),
            audio_url=str(TMP_DIR / f"{job_id}.mp3") if (TMP_DIR / f"{job_id}.mp3").exists() else None,
            scenes=[s.model_dump() for s in scenes],  # [v15.92] ÀÚ»ê°Ë»ö ÈÄ Àç°è»ê
        )
        render_request_dict = render_request.model_dump()
        _save_project_file(project_dir, "render_request.json", render_request_dict)

        # ±âÁ¸ process_video_creation È£Ãâ
        _auto_set_status(job_id, "rendering", 65, "¿µ»ó ÇÕ¼º Áß")
        await process_video_creation(job_id, render_request)

        # Ãâ·Â ÆÄÀÏ ¼öÁý
        output_files: Dict[str, str] = {}
        lf_path = LONGFORM_DIR / f"{job_id}.mp4"
        if lf_path.exists():
            output_files["longform"] = str(lf_path)
        th_path = THUMBNAILS_DIR / f"{job_id}_thumb.jpg"
        if th_path.exists():
            output_files["thumbnail"] = str(th_path)

        # ¦¡¦¡ 11. Ç°Áú °Ë»ç ¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡
        _auto_set_status(job_id, "quality_checking", 85, "Ç°Áú °Ë»ç Áß")
        quality = await auto_run_quality_check(job_id, output_files, scenes, ntl_timeline)
        _save_project_file(project_dir, "quality_report.json", quality)

        # ¦¡¦¡ 12. ¸ÞÅ¸µ¥ÀÌÅÍ »ý¼º ¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡
        _auto_set_status(job_id, "thumbnail_generating", 88, "¸ÞÅ¸µ¥ÀÌÅÍ »ý¼º Áß")
        actual_dur = int(get_video_duration(Path(output_files.get("longform", ""))) or request.target_duration_sec)
        yt_meta = await auto_generate_youtube_metadata(
            request.topic, script, request.language, actual_dur, request.upload_privacy
        )
        _save_project_file(project_dir, "upload_metadata.json", yt_meta)

        # ¦¡¦¡ 12b. ÇÁ·Î ½æ³×ÀÏ Àç»ý¼º (YouTube Å¸ÀÌÆ² Àû¿ë) ¦¡¦¡
        yt_title = yt_meta.get("youtube", {}).get("title", request.topic) if isinstance(yt_meta, dict) else request.topic
        pro_thumb_path = THUMBNAILS_DIR / f"{job_id}_thumb.jpg"
        lf_path_for_thumb = Path(output_files.get("longform", ""))
        if lf_path_for_thumb.exists():
            _auto_set_status(job_id, "thumbnail_generating", 90, "ÇÁ·Î ½æ³×ÀÏ »ý¼º Áß")
            pro_ok = generate_pro_thumbnail(
                video_path=lf_path_for_thumb,
                output_path=pro_thumb_path,
                title=yt_title,
                subtitle="",
            )
            if pro_ok and pro_thumb_path.exists():
                output_files["thumbnail"] = str(pro_thumb_path)
                logger.info(f"[AUTO] ÇÁ·Î ½æ³×ÀÏ Àû¿ë: {pro_thumb_path}")
            else:
                logger.warning("[AUTO] ÇÁ·Î ½æ³×ÀÏ ½ÇÆÐ ? ±âÁ¸ ½æ³×ÀÏ À¯Áö")

        # ¦¡¦¡ 13. YouTube ¾÷·Îµå (Ç°Áú Åë°ú ½Ã) ¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡
        youtube_url = None
        upload_status = "upload_skipped"

        if request.auto_upload and quality["quality_score"] >= request.quality_threshold:
            # [v15.96] SEO ¸ÞÅ¸µ¥ÀÌÅÍ ÀÚµ¿»ý¼º
            try:
                _seo_meta = await auto_generate_seo_metadata(
                    topic=request.topic, script=script, scenes=scenes,
                    tone=request.tone, language=request.language
                )
                state.mark("seo_metadata", _seo_meta)
                # SEO ÃÖÀûÈ­ Á¦¸ñ ¹Ý¿µ
                if _seo_meta.get("title"):
                    _script["title"] = _seo_meta["title"]
                logger.info(f"[v15.96] SEO ¸ÞÅ¸ ¿Ï·á: {_seo_meta.get('title','?')[:40]}")
            except Exception as _seo_err:
                logger.warning(f"[v15.96] SEO ¸ÞÅ¸ ½ÇÆÐ: {_seo_err}")

            _auto_set_status(job_id, "uploading_private", 92, "YouTube private ¾÷·Îµå Áß")
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
                        logger.info(f"[AUTO] YouTube ¾÷·Îµå ¿Ï·á: {youtube_url}")
                    else:
                        upload_status = "upload_failed"
                        error_msg = f"YouTube upload response {up_resp.status_code}: {up_resp.text[:200]}"
                        logger.warning(f"[AUTO] ¾÷·Îµå ÀÀ´ä {up_resp.status_code}: {up_resp.text[:200]}")
                        _auto_set_status(job_id, "upload_failed", 95, error_msg)
            except Exception as ue:
                upload_status = "upload_failed"
                error_msg = f"YouTube upload error: {str(ue)[:200]}"
                logger.warning(f"[AUTO] YouTube ¾÷·Îµå ½ÇÆÐ: {ue}")
                _auto_set_status(job_id, "upload_failed", 95, error_msg)
        elif quality["quality_score"] < request.quality_threshold:
            upload_status = "upload_hold_quality"
            logger.info(f"[AUTO] Ç°Áú Á¡¼ö {quality['quality_score']} < {request.quality_threshold} ? ¾÷·Îµå º¸·ù")

        # ¦¡¦¡ ¿Ï·á ¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡
        final_status = "completed" if not quality["errors"] else "needs_review"
        _auto_set_status(job_id, final_status, 100, "¿Ï·á",
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

        # ·Î±× ÀúÀå
        log_entry = {
            "completed_at": datetime.now().isoformat(),
            "quality_score": quality["quality_score"],
            "upload_status": upload_status,
            "youtube_url": youtube_url,
        }
        _save_project_file(project_dir, "logs.jsonl", log_entry)
        logger.info(f"[AUTO] ÆÄÀÌÇÁ¶óÀÎ ¿Ï·á: job={job_id} quality={quality['quality_score']} upload={upload_status}")
        # [CLEANUP] auto 파이프라인 완료 후 tmp 정리 (400GB 방어)
        try:
            cleanup_job_tmp(job_id)
        except Exception:
            pass

    except Exception as e:
        logger.exception(f"[AUTO] ÆÄÀÌÇÁ¶óÀÎ ½ÇÆÐ: {e}")
        step = _AUTO_JOB_STORE.get(job_id, {}).get("status", "unknown")
        _auto_set_status(job_id, "failed", _AUTO_JOB_STORE.get(job_id, {}).get("progress", 0),
            f"½ÇÆÐ: {e}",
            extra={"error": str(e), "failed_step": step, "retryable": True}
        )
        _save_project_file(project_dir, "error.json",
                           {"error": str(e), "step": step, "timestamp": datetime.now().isoformat()})


# ¦¡¦¡ 7. FastAPI ¿£µåÆ÷ÀÎÆ® ¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡¦¡

@app.post("/api/auto/topic-job", tags=["Auto"])
async def create_auto_topic_job(
    request: AutoTopicRequest,
    background_tasks: BackgroundTasks,
    _: str = Depends(verify_api_key),
):
    """
    [v15.66.0] ÁÖÁ¦ ±â¹Ý ¿ÏÀü ÀÚµ¿ ¿µ»ó »ý¼º + YouTube private ¾÷·Îµå.
    ÁÖÁ¦¡¤Åæ¡¤±æÀÌ¸¸ ÀÔ·ÂÇÏ¸é ¿ø°í¡æ¾À¡æTTS¡æ·»´õ¸µ¡æ¾÷·Îµå±îÁö ÀÚµ¿ Ã³¸®.
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
        "current_message": "´ë±â Áß",
        "quality_score": None,
        "output_files": {},
        "youtube_url": None,
        "error": None,
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat(),
    }

    # asyncio.create_task (Python 3.10+ running loop Á÷Á¢ »ç¿ë)
    import asyncio as _aio
    try:
        _t = _aio.create_task(run_auto_topic_pipeline(job_id, request))
        _AUTO_TASKS[job_id] = _t  # GC ¹æÁö
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
        message=f"ÀÚµ¿ »ý¼º ÆÄÀÌÇÁ¶óÀÎ ½ÃÀÛ: {request.topic[:50]}",
    )


@app.get("/api/auto/jobs/{job_id}/status", tags=["Auto"])
async def get_auto_job_status(
    job_id: str,
    _: str = Depends(verify_api_key),
):
    """[v15.66.0] ÀÚµ¿ »ý¼º ÀÛ¾÷ »óÅÂ Á¶È¸"""
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
    """[v15.66.0] ÀÚµ¿ »ý¼º ÀÛ¾÷ ¸ñ·Ï"""
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


# ============================================================================
# [v16.5.0] Settings: .env read/write + Docker rebuild command
# ============================================================================

_ENV_FILE = Path("/data/.env")

_DOCKER_REBUILD_CMD = (
    'docker stop lf2_ffmpeg && docker rm lf2_ffmpeg && '
    'docker run -d --name lf2_ffmpeg --network lf2_net --restart unless-stopped '
    '--expose 8002 --dns 8.8.8.8 --shm-size=2g '
    '--env-file "E:\\longform_factory\\v2\\.env" '
    '-e TZ=Asia/Seoul -e LF_API_KEY=${LF_API_KEY} '
    '-v "E:\\longform_factory\\v2\\tmp:/data/tmp" '
    '-v "E:\\longform_factory\\v2\\jobs:/data/jobs" '
    '-v "E:\\longform_factory\\v2\\output:/data/output" '
    '-v "E:\\longform_factory\\v2\\bgm:/data/bgm" '
    '-v "E:\\longform_factory\\v2\\.env:/data/.env" '
    'lf_ffmpeg_worker:16.5.0'
)

_ENV_GROUPS: Dict[str, List[str]] = {
    "LLM": [
        "ANTHROPIC_API_KEY", "ANTHROPIC_MODEL",
        "OPENAI_API_KEY", "OPENROUTER_API_KEY",
        "GEMINI_API_KEY", "GEMINI_MODEL",
        "CEREBRAS_API_KEY", "CEREBRAS_MODEL",
        "ARLIAI_API_KEY", "ARLIAI_MODEL",
        "GROQ_API_KEY", "GROQ_MODEL",
        "OLLAMA_URL", "OLLAMA_MODEL", "OLLAMA_MODEL_FAST", "OLLAMA_MODEL_QUALITY",
    ],
    "TTS": ["ELEVENLABS_API_KEY", "EDGE_VOICE_PRIMARY", "EDGE_VOICE_BACKUP", "EDGE_RATE"],
    "미디어": ["PEXELS_API_KEY", "PIXABAY_API_KEY"],
    "AI영상": [
        "AI_VIDEO_ENABLED", "AI_VIDEO_PROVIDER",
        "KLING_ACCESS_KEY", "KLING_SECRET_KEY",
        "LUMA_API_KEY", "SILICONFLOW_API_KEY",
        "POLLO_API_KEY", "POLLO_MODEL",
        "APIFRAME_API_KEY", "APIFRAME_MODEL",
        "MAGICHOUR_API_KEY", "MAGICHOUR_MODEL",
    ],
    "업로드": [
        "YOUTUBE_CLIENT_ID", "YOUTUBE_CLIENT_SECRET",
        "YOUTUBE_REFRESH_TOKEN", "YOUTUBE_CHANNEL_ID",
        "FACEBOOK_PAGE_ID", "FACEBOOK_PAGE_TOKEN",
    ],
    "파이프라인": [
        "LF_API_KEY", "GENERATE_SHORTS", "GENERATE_THUMBNAIL",
        "N8N_USER", "N8N_PASSWORD", "N8N_API_KEY",
    ],
    "품질": [
        "FFMPEG_PRESET", "FFMPEG_CRF",
        "ASS_FONT_NAME", "ASS_FONT_SIZE", "ASS_MAX_CHARS_PER_LINE", "ASS_MARGIN_V",
        "BGM_VOLUME", "AUDIO_LOUDNESS_TARGET",
        "MAX_DOWNLOAD_MB", "MAX_SOURCE_CLIP_SEC",
    ],
}

_SECRET_KEYS = {
    "ANTHROPIC_API_KEY", "OPENAI_API_KEY", "GEMINI_API_KEY", "OPENROUTER_API_KEY",
    "CEREBRAS_API_KEY", "ARLIAI_API_KEY", "ELEVENLABS_API_KEY",
    "PEXELS_API_KEY", "PIXABAY_API_KEY",
    "KLING_ACCESS_KEY", "KLING_SECRET_KEY", "LUMA_API_KEY", "SILICONFLOW_API_KEY",
    "POLLO_API_KEY", "APIFRAME_API_KEY", "MAGICHOUR_API_KEY",
    "YOUTUBE_CLIENT_SECRET", "YOUTUBE_REFRESH_TOKEN", "FACEBOOK_PAGE_TOKEN",
    "LF_API_KEY", "N8N_PASSWORD", "N8N_API_KEY",
}


def _read_env_dict() -> Dict[str, str]:
    """Read /data/.env if volume-mounted; fallback to os.environ."""
    if _ENV_FILE.exists():
        result: Dict[str, str] = {}
        for raw in _ENV_FILE.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                k, _, v = line.partition("=")
                result[k.strip()] = v.strip()
        return result
    # Fallback: read from process environment (read-only, no write possible)
    all_keys: List[str] = [k for keys in _ENV_GROUPS.values() for k in keys]
    return {k: os.environ.get(k, "") for k in all_keys}


def _write_env_dict(updates: Dict[str, str]) -> None:
    """Patch key=value lines in /data/.env, preserving comments and order. Create file if not found."""
    if not _ENV_FILE.exists():
        # Create new .env file with updated keys
        _ENV_FILE.parent.mkdir(parents=True, exist_ok=True)
        out: List[str] = []
        for k, v in updates.items():
            out.append(f"{k}={v}")
        import tempfile as _tmp
        with _tmp.NamedTemporaryFile(mode='w', encoding='utf-8', dir=_ENV_FILE.parent, delete=False) as tf:
            tf.write("\n".join(out) + "\n")
            tf_path = tf.name
        import os as _os
        _os.replace(tf_path, _ENV_FILE)
        logger.info(f"[SETTINGS] Created /data/.env with {len(updates)} keys")
        return

    lines = _ENV_FILE.read_text(encoding="utf-8").splitlines()
    written: set = set()
    out: List[str] = []
    for line in lines:
        stripped = line.strip()
        if stripped and not stripped.startswith("#") and "=" in stripped:
            k = stripped.partition("=")[0].strip()
            if k in updates:
                out.append(f"{k}={updates[k]}")
                written.add(k)
                continue
        out.append(line)
    # Append keys not yet in file
    for k, v in updates.items():
        if k not in written:
            out.append(f"{k}={v}")
    import tempfile as _tmp
    with _tmp.NamedTemporaryFile(mode='w', encoding='utf-8', dir=_ENV_FILE.parent, delete=False) as tf:
        tf.write("\n".join(out) + "\n")
        tf_path = tf.name
    import os as _os
    _os.replace(tf_path, _ENV_FILE)


class EnvUpdateRequest(BaseModel):
    updates: Dict[str, str] = Field(..., description="key->value pairs to update")


@app.get("/settings/env", tags=["Settings"])
async def settings_get_env(_: str = Depends(verify_api_key)):
    """[v16.5.0] .env 파일 읽기 — 그룹별 키 목록 반환 (비밀키 마스킹, 평문값 제외)"""
    env = _read_env_dict()
    file_mounted = _ENV_FILE.exists()
    groups: Dict[str, Any] = {}
    for group, keys in _ENV_GROUPS.items():
        entries = []
        for k in keys:
            val = env.get(k, "")
            is_secret = k in _SECRET_KEYS
            if is_secret and len(val) > 8:
                masked = "*" * 8 + val[-4:]
            else:
                masked = val if not is_secret else ""
            entry = {
                "key": k,
                "masked": masked,
                "is_secret": is_secret,
                "set": bool(val),
            }
            entries.append(entry)
        groups[group] = entries
    return {
        "groups": groups,
        "file_mounted": file_mounted,
        "rebuild_cmd": _DOCKER_REBUILD_CMD,
    }


@app.post("/settings/env", tags=["Settings"])
async def settings_update_env(req: EnvUpdateRequest, _: str = Depends(verify_api_key)):
    """[v16.5.0] .env 파일 업데이트 — 컨테이너 재시작 후 적용"""
    try:
        _write_env_dict(req.updates)
        return {
            "ok": True,
            "updated": list(req.updates.keys()),
            "note": "저장 완료. 컨테이너 재시작 후 적용됩니다.",
        }
    except FileNotFoundError as e:
        raise HTTPException(status_code=503, detail=str(e))


@app.post("/settings/rebuild", tags=["Settings"])
async def settings_rebuild(_: str = Depends(verify_api_key)):
    """[v16.5.0] Docker 재시작 커맨드 반환 (Windows PowerShell 실행용)"""
    return {
        "ok": True,
        "cmd": _DOCKER_REBUILD_CMD,
        "note": "Windows PowerShell에서 아래 커맨드를 실행하세요",
    }


