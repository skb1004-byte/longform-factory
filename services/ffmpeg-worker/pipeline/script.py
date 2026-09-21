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
from pipeline import cancel
from typing import List, Optional, Any
import httpx
from config import (
    SCRIPT_CROSS_REVIEW, SCRIPT_REVIEW_ROUNDS,
    WEB_SEARCH_ENABLED, WEB_SEARCH_MODE, WEB_SEARCH_MAX_ARTICLES,
    WEB_SEARCH_FETCH_BODIES, WEB_SEARCH_TIMEOUT,
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
from pipeline.websearch import search_news, format_headlines, needs_search
from pipeline.content_presets import chars_per_sec, get_content, density_hint
from pipeline.style_presets import image_style_voice

logger = logging.getLogger(__name__)

# LLM 타임아웃 (초) - 짧은 영상: 20초, 긴 영상: generate_script에서 동적 설정
# 30초는 긴 한국어 스크립트(1800~3600자)를 받기엔 짧아서, 품질 좋은 모델일수록
# ReadTimeout 으로 잘려나가고 템플릿 폴백으로 떨어졌다 (Claude 실측 31초 타임아웃).
LLM_TIMEOUT = 120.0

# 키워드 프리셋 (한국어 관련, 최후 fallback용)
_KEYWORD_FALLBACK = [
    "korean culture", "food cooking", "nature korea",
    "technology future", "lifestyle wellness",
    "history tradition", "city urban", "education learning",
    "family community", "economy business",
]

# Regex to detect LLM placeholder keywords (e.g. "영어 1-3단어", "한글 2단어")
# LLMs sometimes copy the instruction text verbatim as the keyword value.
_PLACEHOLDER_KW = re.compile(
    r'(?:영어|한글)\s*\d*[-~]?\d*\s*단어'
    r'|^[가-힣\s]+$',  # pure Korean string (no English) — invalid as Pexels search term
    re.IGNORECASE,
)

# 씬 분할 프롬프트 (한국어 강제, 나레이션 보존 필수)
_SPLIT_PROMPT = """\
다음 스크립트를 정확히 {n}개의 씬으로 분할하세요.
주제: {topic}
스크립트 (전체 {total_chars}자 — 각 씬당 최소 {min_chars}자 이상 나레이션 필수):
{script}

반드시 JSON 배열만 반환 (마크다운 코드블록 금지):
[{{"scene_id":"scene_01","keyword":"nature sunset","narration":"씬 나레이션 텍스트","duration_seconds":0.0}}]

규칙 (엄격히 준수):
- keyword: 이 씬 나레이션에서 '카메라로 찍을 수 있는 것'을 영어 2~4단어로. 한국어 절대 금지.
  · 그 문장이 말하는 사람·장소·사물·행동을 먼저 쓰세요. 주제 명사는 그 장면에 실제로 보일 때만 넣습니다.
  · 추상어 금지: value, growth, caution, policy, strategy, impact, trend 같은 말은 영상으로 찍히지 않습니다.
  · 인물 구성을 반드시 반영: '혼자'면 alone, '가족'이면 family, '아이'가 없으면 child 를 넣지 마세요.
  · 국가·지역이 명시되면 반영: '해외 관광객'은 tourists, '한국'이면 korea 를 붙입니다.
  · [중요] 영상 전체의 주제 소재를 keyword 에서 빼지 마세요. 문장이 '세계로 퍼진다',
    '온라인에서', '역사가 깊다' 처럼 추상적이어도, 그 개념을 상징으로 바꾸지 말고
    주제 소재가 그 상황에 놓인 장면을 쓰세요.
    스톡 영상 검색은 상징을 문자 그대로 가져옵니다 — '퍼진다'를 globe map 으로 쓰면
    지구본과 세계지도 영상이 오고, 시청자는 김치 영상에서 지도를 보게 됩니다(실측).
  · 지나치게 구체적인 고유명사만으로 채우지 마세요. 스톡 라이브러리에 없습니다.
    주제 소재를 함께 넣어야 없을 때 그 소재로 되돌아갈 수 있습니다.
  예)
    "퇴근 후 혼자 보내는 두 시간" → "woman alone apartment evening"
    "해외 관광객 지출이 크게 늘었어요" → "tourists shopping street seoul"   (달러 지폐 X)
    "정부 지원 사업이 진행 중입니다" → "office consulting meeting"           (policy X)
    "그래도 아직 방심하기는 일러요" → "empty shop street quiet"              (사람 얼굴 X)
    [주제=김치] "김치가 지구 반대편까지 퍼지고 있습니다"
        → "kimchi jars table"        (globe map spreading X — 지구본이 나옵니다)
    [주제=김치] "김치박물관 누리집에 따르면"
        → "kimchi museum display"    (museum website screen X — 노트북이 나옵니다)
    [주제=김치] "키르기스스탄 고려인 행사가 열렸고요"
        → "kimchi making hands"      (kyrgyzstan koryo-saram event X — 스톡에 없습니다)
- narration: 스크립트 해당 구간을 그대로 발췌 (요약·압축·생략 절대 금지, 각 씬 최소 {min_chars}자)
- 스크립트 전체를 {n}등분하여 모든 내용 포함 (내용 누락 금지)
- duration_seconds: 해당 씬 narration 글자수 / 5.26 로 계산 (실측 TTS 속도)
- 정확히 {n}개 씬 반환
"""

# http(s):// 가 붙은 주소뿐 아니라 'www.yedam.kr', 'yedam.kr' 처럼 스킴 없이 적은
# 도메인도 잡는다. 실측으로 제목에 'WWW.YEDAM.KR' 을 넣었는데 스킴이 없어 링크로
# 인식되지 않았고, 사이트를 한 번도 보지 않은 채 모델이 내용을 지어냈다
# (실제로는 학교 교육용 서비스인데 기업 채용 플랫폼으로 서술됨).
_URL_RE = re.compile(
    r"(?:https?://[^\s<>\"'\)\]]+)"
    r"|(?:\bwww\.[A-Za-z0-9\-]+(?:\.[A-Za-z0-9\-]+)+(?:/[^\s<>\"'\)\]]*)?)"
    r"|(?:\b[A-Za-z0-9\-]+(?:\.[A-Za-z0-9\-]+)*\.(?:kr|com|net|org|io|ai|co\.kr|go\.kr|or\.kr)"
    r"(?:/[^\s<>\"'\)\]]*)?)",
    re.I,
)
_TAG_RE = re.compile(r"<[^>]+>")
_SCRIPT_STYLE_RE = re.compile(r"<(script|style|noscript)[^>]*>.*?</\1>", re.S | re.I)
_WS_RE = re.compile(r"[ \t\r\f\v]+")
_BLANKLINE_RE = re.compile(r"\n{3,}")

URL_FETCH_TIMEOUT = 20.0
URL_MAX_CHARS = 4000          # 링크 1개당 본문에서 가져올 최대 글자수
URL_MAX_COUNT = 3             # 한 요청에서 처리할 최대 링크 수


def extract_urls(text: str) -> list[str]:
    """주제 문자열에 섞인 http(s) 링크를 순서대로 뽑는다 (중복 제거)."""
    seen, out = set(), []
    for u in _URL_RE.findall(text or ""):
        u = u.rstrip(".,)]}\"'")
        if not u.lower().startswith(("http://", "https://")):
            u = "https://" + u          # 스킴 없이 적은 도메인 보정
        key = u.lower()
        if key not in seen:
            seen.add(key)
            out.append(u)
    return out[:URL_MAX_COUNT]


_JS_JUNK_RE = re.compile(
    r"function\s*\(|=>|\$\(|\.hide\(|\.show\(|toggleClass|addEventListener"
    r"|document\.|window\.|var\s+\w+\s*=|\}\s*\)|;\s*$"
)


def _hangul_ratio(s: str) -> float:
    if not s:
        return 0.0
    return sum(1 for c in s if "\uac00" <= c <= "\ud7a3") / len(s)


def _html_to_text(html: str) -> str:
    """의존성 없이 HTML에서 읽을 수 있는 본문만 남긴다.

    별도 파서를 넣지 않은 이유: 나레이션 근거로 쓸 '사실 문장'만 필요하고,
    레이아웃 복원이 필요 없기 때문이다. script/style 을 먼저 통째로 지운 뒤
    태그를 제거하고 공백을 정리한다.
    """
    html = _SCRIPT_STYLE_RE.sub(" ", html)
    text = _TAG_RE.sub("\n", html)
    for ent, ch in (("&nbsp;", " "), ("&amp;", "&"), ("&lt;", "<"),
                    ("&gt;", ">"), ("&quot;", '"'), ("&#39;", "'")):
        text = text.replace(ent, ch)
    text = _WS_RE.sub(" ", text)
    lines = [ln.strip() for ln in text.split("\n")]
    # 메뉴·버튼 같은 짧은 조각과, 인라인 핸들러에서 흘러나온 JS 잔해를 걸러낸다.
    # (실측: YTN 기사 본문에 인라인 onclick 의 jQuery 코드가 섞여 들어왔다)
    lines = [ln for ln in lines if len(ln) >= 20 and not _JS_JUNK_RE.search(ln)]
    # 한국어 문서라면 한글이 거의 없는 줄은 본문이 아니라 내비게이션·코드다.
    _joined = " ".join(lines)
    if _joined and _hangul_ratio(_joined) > 0.15:
        lines = [ln for ln in lines if sum(1 for c in ln if "\uac00" <= c <= "\ud7a3") >= 4]
    return _BLANKLINE_RE.sub("\n\n", "\n".join(lines)).strip()


_BLOCKED_HOST_RE = re.compile(
    r"^(localhost|127\.|0\.|10\.|169\.254\.|192\.168\.|172\.(1[6-9]|2\d|3[01])\.|\[?::1\]?|.*\.local)",
    re.I,
)


def _is_safe_url(url: str) -> bool:
    """외부 공개 페이지만 허용한다.

    이 기능은 사용자가 준 링크 본문을 그대로 프롬프트에 넣기 때문에, 내부망 주소를
    막지 않으면 컨테이너가 lf2_net 의 이웃 서비스나 클라우드 메타데이터(169.254.169.254)를
    읽어 그 내용이 나레이션·영상으로 새어나갈 수 있다.
    """
    try:
        from urllib.parse import urlparse

        p = urlparse(url)
        if p.scheme not in ("http", "https"):
            return False
        host = (p.hostname or "").strip()
        if not host or _BLOCKED_HOST_RE.match(host):
            return False
        # 점이 없는 호스트명 = 도커 네트워크 내부 서비스명(lf2_tts 등)
        if "." not in host:
            return False
        return True
    except Exception:
        return False


async def fetch_url_facts(urls: list[str]) -> str:
    """링크 본문을 받아 LLM 프롬프트에 넣을 사실 근거 텍스트로 만든다.

    링크가 주어지면 그 내용이 곧 사실 출처가 되므로, 모델이 지어내는 수치·연도·
    인명이 크게 줄어든다. 실패한 링크는 조용히 건너뛰고 나머지로 진행한다.
    """
    if not urls:
        return ""
    chunks = []
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; LongFormFactory/1.0)",
        "Accept-Language": "ko,en;q=0.8",
    }
    async with httpx.AsyncClient(timeout=URL_FETCH_TIMEOUT, follow_redirects=True,
                                 headers=headers) as client:
        for url in urls:
            if not _is_safe_url(url):
                logger.warning(f"[url] 내부망/비정상 주소 차단: {url}")
                continue
            try:
                r = await client.get(url)
                if r.status_code != 200:
                    logger.warning(f"[url] {url} HTTP {r.status_code}")
                    continue
                # 대용량 페이지 하나로 컨테이너가 죽지 않도록 상한을 둔다.
                if len(r.content) > 3_000_000:
                    logger.warning(f"[url] 본문이 너무 큼({len(r.content)//1024}KB) → 건너뜀: {url}")
                    continue
                ctype = r.headers.get("content-type", "")
                body = r.text if "html" in ctype or "text" in ctype else ""
                if not body:
                    logger.warning(f"[url] {url} 본문 없음 (content-type={ctype})")
                    continue
                text = _html_to_text(body)[:URL_MAX_CHARS]
                if len(text) < 100:
                    logger.warning(f"[url] {url} 추출 본문이 너무 짧음({len(text)}자)")
                    continue
                chunks.append(f"[출처] {url}\n{text}")
                logger.info(f"[url] ✅ {url} 본문 {len(text)}자 확보")
            except Exception as e:
                logger.warning(f"[url] {url} 오류: {type(e).__name__}: {str(e)[:80]}")
    return "\n\n".join(chunks)


async def _enrich_topic(topic: str) -> str:
    """뭉뚱그린 주제를 구체적 사실 5~8개로 미리 앵커링 (실패해도 빈 문자열 → 메인 프롬프트 그대로 진행).

    "조선시대 역사" 같은 짧은 제목만 던져도, 이 프리스텝이 먼저 구체적인 사실·숫자·이름·사례를
    뽑아내고 메인 스크립트 프롬프트에 "반드시 포함할 정보"로 주입해 결과물의 디테일을 끌어올린다.
    Groq만 사용 (가장 빠름) + 8초 타임아웃 — 전체 파이프라인 지연을 최소화.
    """
    if not GROQ_API_KEY:
        return ""
    enrich_prompt = (
        f"주제: {topic}\n\n"
        f"위 주제에 대해 시청자가 몰랐을 법한 구체적 사실·숫자·날짜·이름·사례를 5~8개 뽑아주세요.\n"
        f"뭉뚱그린 일반론이 아니라 검증 가능한 구체적 정보여야 합니다.\n"
        f"각 줄은 20~50자 내외, 한 줄에 하나씩. 설명 없이 목록만 출력 (번호나 불릿 기호 없이 줄바꿈으로만 구분).\n"
        f"반드시 한국어로만 작성."
    )
    try:
        result = await asyncio.wait_for(
            _llm_text_oai(
                "https://api.groq.com/openai/v1/chat/completions",
                GROQ_API_KEY,
                "openai/gpt-oss-120b",  # 2026-06-17 Groq deprecate -> gpt-oss (script.py 다른 호출과 동일)
                enrich_prompt,
                system="당신은 정확한 사실만 다루는 한국어 리서치 보조원입니다.",
                max_tokens=600,
            ),
            timeout=8.0,
        )
        result = (result or "").strip()
        if result:
            logger.info(f"[script] 주제 앵커링 완료 ({len(result)}자)")
        return result
    except Exception as e:
        logger.debug(f"[script] 주제 앵커링 실패 (무시하고 진행): {e}")
        return ""


# 스크립트 생성 프롬프트
_JOB_IMAGE_STYLE = ""


def set_image_style(style: str) -> None:
    """이 잡의 이미지 스타일. 나레이션 문체를 화면과 맞추는 데 쓴다."""
    global _JOB_IMAGE_STYLE
    _JOB_IMAGE_STYLE = (style or "").strip()


def get_image_style() -> str:
    return _JOB_IMAGE_STYLE


def _script_prompt(topic: str, duration_sec: int, tone: str, key_facts: str = "") -> str:
    # 한국어 TTS 실측 속도: 5.56자/초 (ko-KR-SunHiNeural, rate -5%).
    # 예전 주석의 7.0~7.6자/초는 측정 오류였고, 그 값으로 잡으면 영상이 목표보다 짧아진다.
    # 카테고리마다 적정 호흡이 다르다. 쇼츠는 빽빽하게, 교육은 여유 있게.
    target_chars = int(duration_sec * chars_per_sec(tone))
    _c = get_content(tone)
    _isv = image_style_voice(_JOB_IMAGE_STYLE)
    facts_block = (
        f"[반드시 포함할 구체적 정보]\n{key_facts}\n"
        f"위 정보들을 적어도 3개 이상 스크립트 본문에 구체적으로 녹여내세요. 그냥 나열하지 말고 자연스러운 문장으로 풀어서 설명하세요.\n\n"
    ) if key_facts else ""
    return (
        f"주제: {topic}\n"
        f"장르: {_c['label'] if _c else tone}\n"
        f"목표 길이: {duration_sec}초 ({target_chars}자)\n\n"
        f"위 주제로 한국어 유튜브 나레이션 스크립트를 작성하세요.\n\n"
        f"{facts_block}"
        + (f"[이 장르의 구조]\n{_c['structure']}\n\n[이 장르의 말투]\n{_c['voice']}\n\n"
            f"[정보 밀도]\n{density_hint(tone)}\n\n"
           if _c else "")
        # 화면과 말투가 따로 놀지 않도록 이미지 스타일의 문체도 함께 준다.
        # 장르가 '무엇을 말할지', 이미지 스타일이 '어떻게 말할지'를 정한다.
        + ((f"[화면 스타일에 맞춘 문체]\n{_isv}\n"
            f"이 영상의 화면은 '{_JOB_IMAGE_STYLE}' 스타일로 만들어집니다. "
            f"나레이션도 그 화면과 어울리는 문체로 쓰세요.\n\n")
           if _isv else "")
        + ("" if _c else (
        f"[구조 — 설명문 나열이 아니라 '이어지는 이야기'로]\n"
        f"- 도입(첫 2~3문장): 시청자가 이미 겪었거나 궁금해할 상황을 툭 던지며 시작. "
        f"인사말·자기소개·'오늘은 ~에 대해 알아보겠습니다' 같은 상투적 시작 절대 금지.\n"
        f"- 전개: 한 문장이 끝나면 다음 문장이 궁금해지도록 이어 붙이세요. "
        f"구체적인 숫자·장소·시점·사례를 넣어 장면이 그려지게 쓰세요.\n"
        f"- 마무리: 요약 한 문장으로 담백하게 끝내기.\n\n"
        f"[문체 — 중요]\n"
        f"- 정보를 '나열'하지 말고 '말하듯' 이어가세요. 옆에서 설명해 주는 말투입니다.\n"
        f"- 다음 표현은 절대 쓰지 마세요: '첫 번째로', '두 번째로', '마지막으로', "
        f"'~에 대해 알아보겠습니다', '~살펴보겠습니다', '다양한 측면에서', '체계적으로'.\n"
        f"- 대신 이렇게 이어가세요: '~인데요', '~거든요', '~더군요', '~습니다', "
        f"'그런데 여기서', '문제는', '이유는 이렇습니다'.\n"
        f"- 한 문장은 짧게. 귀로 듣고 한 번에 이해되는 길이로 끊으세요.\n\n"
        ))
        + (
        f"[필수 규칙]\n"
        f"1. 반드시 한국어로만 작성. 중국어·일본어·터키어·베트남어·스페인어 등 한국어 이외의 외국어 단어 절대 사용 금지.\n"
        f"2. 영어는 고유명사(국가명, 인명, 브랜드)만 허용. 일반 영어 단어 사용 금지.\n"
        f"3. 분량은 {int(target_chars * 0.9)}~{int(target_chars * 1.1)}자. "
        f"{int(target_chars * 1.1)}자를 절대 넘기지 마세요 — 넘으면 영상 길이가 어긋납니다. "
        f"길어지면 내용을 더 넣지 말고 문장을 짧게 다듬으세요.\n"
        f"4. 마크다운 없이 순수 텍스트만 (제목·번호·불릿 금지).\n"
        f"5. 자연스러운 한국어 구어체로, 옆에서 말해주듯 작성. 보고서·설명문 말투 금지.\n"
        f"6. 같은 표현 반복 금지. 다양한 어휘로 풍부하게 서술.\n"
        f"7. 목표 재생 시간은 {duration_sec}초입니다. 이 길이에 맞춰 내용의 폭을 정하세요.\n\n"
        f"[출력 형식]\n"
        f"한국어 나레이션 텍스트만 출력. 설명·주석·인사말 없이 바로 본문 시작.\n"
        )
    )


def _script_max_tokens(duration_sec: int) -> int:
    """Duration-proportional token budget. Korean ~2 tokens/char, 7.0 chars/sec."""
    chars_needed = int(duration_sec * 5.6)
    tokens_needed = int(chars_needed * 2.5)  # Korean token overhead
    return min(8000, max(1500, tokens_needed))


def _scene_max_tokens(script_len: int, n_scenes: int = 5) -> int:
    """Scene JSON output max_tokens. Must preserve full narration text.
    Korean JSON: narration_chars * 2.5 tokens + JSON overhead per scene.
    """
    # narration = script_len chars + JSON structure overhead (200 chars × n_scenes)
    total_chars = script_len + n_scenes * 200
    tokens = int(total_chars * 2.5)
    return min(6000, max(2000, tokens))


# ============================================================================
# Public API
# ============================================================================

# ── AI 릴레이 검토 ────────────────────────────────────────────────────────────
# 한 모델이 혼자 쓰면 그 모델의 약점이 그대로 남는다. 실측으로 빠른 모델은 사실을
# 지어내고(온돌을 '물이 순환하는 구조'로 설명), 외국어를 섞었다. 그래서 초안을
# 다른 모델에게 차례로 넘기며 역할을 나눠 고치게 한다.
#
# 각 단계는 '실패하면 직전 결과를 그대로 들고 다음으로' 넘어가므로 품질이 뒤로
# 가는 일이 없다. 단계 수는 SCRIPT_REVIEW_ROUNDS 로 조절한다(0이면 검토 없음).
_REVIEW_ROLES = [
    (
        "사실검증",
        """당신은 사실 검증 담당입니다. 아래 나레이션 초안에서 다음만 고치세요.

1. 사실과 다른 내용을 바로잡으세요. 확실하지 않은 수치·연도·인명·기관명은 지어내지 말고,
   단정을 피한 일반적 서술로 바꾸세요.
2. 한자, 중국어, 영어 단어, 번역투를 자연스러운 한국어로 바꾸세요.
   (AI, GPS 같은 널리 쓰이는 대문자 약어는 그대로 두세요.)
3. 같은 문장·표현의 반복을 지우고 다른 내용으로 채우세요.""",
    ),
    (
        "문장다듬기",
        """당신은 나레이션 문장 다듬기 담당입니다. 내용과 사실관계는 그대로 두고 다음만 고치세요.

1. 귀로 들었을 때 이해되는 문장으로 바꾸세요. 한 문장이 길면 끊으세요.
2. 딱딱한 문어체를 영상 나레이션에 맞는 자연스러운 존댓말 구어체로 바꾸세요.
3. 앞뒤 문장이 매끄럽게 이어지도록 연결을 다듬으세요.""",
    ),
]

_REVIEW_TEMPLATE = """{role_instruction}

분량 규칙(엄수): 초안이 {draft_len}자이므로 결과도 {draft_len}자 내외여야 합니다. 요약해서 줄이지도, 내용을 덧붙여 늘리지도 마세요. 문장을 고치는 것이지 새로 쓰는 것이 아닙니다.
설명이나 머리말 없이 고쳐진 나레이션 본문 전체만 출력하세요.

[주제] {topic}

[초안]
{draft}"""


def _review_candidates(prompt: str, max_tokens: int):
    """검토를 맡길 모델 후보 (정확도 높은 순)."""
    out = []
    if ANTHROPIC_API_KEY:
        out.append(("Claude", lambda: _llm_text_claude(prompt, max_tokens=max_tokens)))
    if GEMINI_API_KEY:
        out.append(("Gemini", lambda: _llm_text_gemini(prompt, max_tokens=max_tokens)))
    if GROQ_API_KEY:
        out.append(("Groq-qwen", lambda: _llm_text_oai(
            "https://api.groq.com/openai/v1/chat/completions",
            GROQ_API_KEY, "qwen/qwen3.8-27b", prompt, max_tokens=max_tokens,
        )))
    return out


async def _run_one_review(draft: str, topic: str, role_name: str, role_instruction: str,
                          skip: set) -> tuple[str, Optional[str]]:
    """한 단계 검토. (결과, 사용한 모델명) — 실패하면 (초안, None)."""
    prompt = _REVIEW_TEMPLATE.format(
        role_instruction=role_instruction, topic=topic, draft=draft, draft_len=len(draft)
    )
    # 모델 출력 상한(8192)을 넘기면 HTTP 400 으로 검토가 통째로 무력화되므로 캡을 둔다.
    review_tokens = max(3000, min(8000, int(len(draft) * 2.5)))

    for name, make_coro in _review_candidates(prompt, review_tokens):
        if name in skip:          # 같은 모델이 연달아 자기 글을 보지 않게 한다
            continue
        try:
            fixed = await make_coro()
        except Exception as e:
            logger.warning(f"[review:{role_name}] {name} 예외: {type(e).__name__}: {str(e)[:80]}")
            continue

        if not fixed:
            logger.info(f"[review:{role_name}] {name} 빈 응답 → 다음 후보")
            continue
        # 분량 이탈은 양쪽 다 막아야 한다. 짧아지면 요약해버린 것이고, 길어지면
        # 영상 길이가 통째로 틀어진다 — 실측으로 30초 요청이 287자→727자(2.5배)로
        # 불어 130초 분량이 나왔다. 검토는 '고치는' 일이지 '늘리는' 일이 아니다.
        if len(fixed) < len(draft) * 0.6:
            logger.warning(
                f"[review:{role_name}] {name} 결과가 너무 짧음({len(fixed)}자 < 초안의 60%) → 건너뜀"
            )
            continue
        if len(fixed) > len(draft) * 1.25:
            logger.warning(
                f"[review:{role_name}] {name} 결과가 너무 김({len(fixed)}자 > 초안의 125%) → 건너뜀"
            )
            continue
        ok, why = _korean_quality(fixed)
        if not ok:
            logger.warning(f"[review:{role_name}] {name} 품질 탈락({why}) → 건너뜀")
            continue

        logger.info(f"[review:{role_name}] ✅ {name} ({len(draft)}자 → {len(fixed)}자)")
        return fixed, name

    logger.info(f"[review:{role_name}] 모든 후보 실패 → 직전 원고 유지")
    return draft, None


async def _condense_to_budget(script: str, target_chars: int) -> Optional[str]:
    """원고를 목표 길이에 맞게 '고르게' 줄인다. 실패하면 None.

    문장 단위 트림은 한 문장이 60~70자인 한국어에서 너무 거칠다. 하나만 버려도
    20%가 날아간다(실측: 195자 → 126자, 목표 157자). 문장을 통째로 버리는 대신
    각 문장에서 군더더기를 덜어내면 목표에 훨씬 가깝게 맞출 수 있다.

    새 사실을 지어내지 않는 것이 이 단계의 유일한 위험이므로, 원문에 없는
    숫자·고유명사를 만들지 말라고 못박고 결과 길이도 검증한다.
    """
    prompt = (
        f"아래 한국어 나레이션을 {target_chars}자 내외로 줄이세요.\n\n"
        f"[규칙]\n"
        f"- 문장을 통째로 버리지 말고, 각 문장에서 군더더기를 덜어 고르게 줄이세요.\n"
        f"- 첫 문장(도입)과 마지막 문장(마무리)은 반드시 남기세요.\n"
        f"- 원문에 없는 숫자·연도·인명·고유명사를 절대 새로 만들지 마세요.\n"
        f"- 한국어 구어체를 유지하고, 결과는 나레이션 본문만 출력하세요.\n\n"
        f"[원문 {len(script)}자]\n{script}\n"
    )
    try:
        tasks = _build_text_tasks(prompt, max_tokens=max(600, int(target_chars * 4)))
        out = await _parallel_race(tasks, min_len=max(40, int(target_chars * 0.7)), timeout=35.0)
    except Exception as e:
        logger.warning(f"[script] 압축 오류: {type(e).__name__} {e}")
        return None
    if not out:
        return None
    out = out.strip()
    # 여기서는 '말이 되는 길이인가'만 본다. 트림본과 어느 쪽이 목표에 가까운지는
    # 호출부가 정한다 — 판정을 두 군데 두면 132자가 하한 133자에 1자 차이로
    # 걸려 더 나쁜 126자가 채택되는 일이 생긴다(실측).
    lo, hi = int(target_chars * 0.60), int(target_chars * 1.20)
    if not (lo <= len(out) <= hi):
        logger.info(f"[script] 압축 결과 {len(out)}자가 상식 범위({lo}~{hi}) 밖 → 사용 안 함")
        return None
    logger.info(f"[script] 압축 성공: {len(script)}자 → {len(out)}자 (목표 {target_chars}자)")
    return out


async def _cross_review(draft: str, topic: str) -> str:
    """초안을 여러 모델에게 차례로 넘기며 역할별로 고치게 한다.

    단계마다 직전 단계를 맡은 모델은 제외해서, 자기가 쓴 글을 자기가 검토하는
    상황을 피한다. 어느 단계가 실패해도 직전 결과가 그대로 살아남는다.
    """
    if not SCRIPT_CROSS_REVIEW or not draft:
        return draft

    rounds = max(0, min(SCRIPT_REVIEW_ROUNDS, len(_REVIEW_ROLES)))
    if rounds == 0:
        return draft

    current = draft
    last_used: Optional[str] = None
    for role_name, role_instruction in _REVIEW_ROLES[:rounds]:
        skip = {last_used} if last_used else set()
        current, used = await _run_one_review(current, topic, role_name, role_instruction, skip)
        if used:
            last_used = used
    return current


# 주제에 들어있는 '실존 고유명사' 후보 — 도메인, 영문 브랜드 표기, ㈜/주식회사 등.
_ENTITY_RE = re.compile(
    r"(?:[A-Za-z0-9\-]+\.(?:kr|com|net|org|io|ai))"      # 도메인
    r"|(?:㈜\s*[가-힣A-Za-z0-9]+)"                        # ㈜회사명
    r"|(?:주식회사\s*[가-힣A-Za-z0-9]+)",
    re.I,
)


def find_named_entities(topic: str) -> list[str]:
    """주제에서 '실제로 존재하는 대상'을 가리키는 표기를 찾는다."""
    seen, out = set(), []
    for m in _ENTITY_RE.findall(topic or ""):
        k = m.strip().lower()
        if k and k not in seen:
            seen.add(k)
            out.append(m.strip())
    return out


def _ungrounded_entity_warning(entities: list[str]) -> str:
    """근거 자료 없이 실존 대상을 설명하려 할 때 프롬프트에 붙일 제약.

    실측 사고: 제목에 'WWW.YEDAM.KR' 을 넣었는데 링크로 인식되지 않아 사이트를
    한 번도 읽지 못했고, 모델이 '인적성 검사'라는 단어만 보고 기업 채용 플랫폼으로
    지어냈다. 실제로는 학교 교육용 서비스라 내용 전체가 사실과 달랐다.
    근거를 못 구했으면 지어내지 말고 일반론으로 쓰게 강제한다.
    """
    names = ", ".join(entities[:3])
    return (
        f"\n\n[매우 중요 — 사실 제약]\n"
        f"'{names}' 에 대한 확인된 자료를 구하지 못했습니다.\n"
        f"- 이 대상이 무엇을 하는 곳인지 추측해서 단정하지 마세요.\n"
        f"- 구체적인 기능, 서비스 구성, 이용 대상, 가격, 정책, 고객사, 수치, 연혁을 지어내지 마세요.\n"
        f"- 대신 이 이름이 속한 '분야 일반'에 대한 내용으로 쓰고, 개별 서비스의 구체적 사항은\n"
        f"  '해당 홈페이지에서 직접 확인해 보시기 바랍니다' 같은 안내로 대체하세요.\n"
    )


def _trim_to_budget(script: str, target_chars: int, min_chars: int) -> str:
    """상한을 넘긴 원고를 문장 경계에서 잘라 길이 예산에 맞춘다.

    모델은 '몇 자 이상'은 지켜도 상한은 잘 안 지킨다(실측: 45초 요청에 440자 =
    79초 분량). 영상 길이는 나레이션 길이가 그대로 결정하므로 상한이 없으면
    요청한 시간과 결과물이 계속 어긋난다.

    [2026-09-20 재작성] 예전 방식은 '상한에서 마지막 문장 길이를 뺀 예산'으로
    앞 문장을 채웠다. 마지막 문장이 길면 예산이 거의 남지 않아 앞 문장을 하나만
    남기고 끝났다 — 실측으로 255자 원고가 116자가 됐다(목표 157자, -26%).
    30초 요청에 17.4초 영상이 나온 원인이다.

    이제는 '문장 조합 후보를 모두 만들고 목표에 가장 가까운 것을 고른다'.
    문장 중간에서는 절대 끊지 않는다. 도입과 마무리를 지키는 조합도 후보에
    넣어, 구조를 살리면서 길이를 맞출 수 있으면 그쪽이 선택되게 한다.
    """
    hi = int(target_chars * 1.15)      # 이 이상은 벌점을 줘서 사실상 배제
    if len(script) <= int(target_chars * 1.06):
        return script

    parts = [p for p in re.split(r"(?<=[.!?\u2026])\s+", script.strip()) if p]
    if len(parts) <= 1:
        return script

    cands = []
    # (1) 앞에서부터 k문장
    for k in range(1, len(parts) + 1):
        cands.append(" ".join(parts[:k]))
    # (2) 앞 k문장 + 마지막 문장 — 결론이 날아가지 않게
    if len(parts) >= 3:
        for k in range(1, len(parts) - 1):
            cands.append(" ".join(parts[:k] + [parts[-1]]))

    def cost(c: str) -> float:
        d = abs(len(c) - target_chars)
        if len(c) > hi:
            d += 1000          # 상한 초과는 사실상 탈락
        if len(c) < target_chars * 0.75:
            d += 300           # 너무 짧은 것도 피한다
        return d

    best = min(cands, key=cost)
    # 원문 그대로가 더 가까우면 손대지 않는다.
    if cost(script) <= cost(best):
        logger.info(
            f"[script] 잘라도 목표에서 더 멀어짐({len(script)}자 → {len(best)}자, "
            f"목표 {target_chars}자) → 원문 유지")
        return script
    logger.info(
        f"[script] 길이 상한 적용: {len(script)}자 → {len(best)}자 "
        f"(목표 {target_chars}자, 문장 {len(parts)}개 중 선택)")
    return best


async def _web_search_facts(topic: str) -> str:
    """주제를 웹에서 검색해 '실제 보도' 기반 근거 텍스트를 만든다.

    링크를 직접 주지 않아도 시의성 주제를 다룰 수 있게 하는 경로다.
    빈 문자열을 돌려주면 호출부가 기존 _enrich_topic 앵커링으로 넘어가므로,
    검색이 실패하거나 느려도 스크립트 생성 자체는 절대 막히지 않는다.
    """
    if not WEB_SEARCH_ENABLED or WEB_SEARCH_MODE == "off":
        return ""
    # auto 모드: 시의성 표현이나 실존 대상이 있을 때만. 가상·추상 주제는 검색해도
    # 무관한 기사만 붙어 오히려 품질을 떨어뜨리고 지연만 는다.
    if WEB_SEARCH_MODE != "always" and not (needs_search(topic) or find_named_entities(topic)):
        logger.debug("[script] 시의성/실존 대상 없음 → 웹 검색 생략")
        return ""

    try:
        hits = await asyncio.wait_for(
            search_news(topic, limit=WEB_SEARCH_MAX_ARTICLES),
            timeout=WEB_SEARCH_TIMEOUT,
        )
    except asyncio.TimeoutError:
        logger.warning(f"[script] 웹 검색 {WEB_SEARCH_TIMEOUT}초 초과 → 생략")
        return ""
    except Exception as e:
        logger.warning(f"[script] 웹 검색 오류: {type(e).__name__} {e}")
        return ""
    if not hits:
        return ""

    headlines = format_headlines(hits)
    body_urls = [h["url"] for h in hits if h.get("url")][:WEB_SEARCH_FETCH_BODIES]
    body = ""
    if body_urls:
        try:
            body = await fetch_url_facts(body_urls)
        except Exception as e:
            logger.warning(f"[script] 검색 본문 수집 실패: {type(e).__name__} {e}")
    if body:
        logger.info(f"[script] 웹 근거 확보: 기사 {len(hits)}건 + 본문 {len(body)}자")
        return (headlines + "\n\n" + body).strip()
    if headlines:
        # 본문을 못 얻어도 제목·언론사·날짜는 실재하는 출처다. 근거 없음보다 낫다.
        logger.info(f"[script] 웹 근거(헤드라인만) {len(hits)}건")
        return headlines
    return ""


async def generate_script_from_topic(
    topic: str,
    duration_sec: int = 60,
    tone: str = "neutral",
) -> str:
    """병렬 레이스로 스크립트 생성. 첫 성공 반환. 전부 실패 시 템플릿 생성.

    주제 앵커링(_enrich_topic)을 먼저 실행해 구체적 사실을 프롬프트에 주입한다.
    앵커링이 실패하거나 8초를 넘기면 건너뛰고 기존 방식(주제만)으로 그대로 진행 — 절대 막히지 않음.
    """
    # 주제에 링크가 들어 있으면 그 페이지 본문을 사실 근거로 쓴다.
    # (링크가 곧 출처이므로 모델이 수치·연도를 지어낼 여지가 줄어든다.)
    urls = extract_urls(topic)
    # '출처가 있는 근거'인지 구분한다. _enrich_topic 은 모델이 주제를 부풀린 결과일 뿐
    # 실제 출처가 아니어서, 이걸 근거로 취급하면 실존 대상 가드가 통째로 무력화된다.
    grounded = False
    if urls:
        logger.info(f"[script] 링크 {len(urls)}개 감지 → 본문 수집")
        key_facts = await fetch_url_facts(urls)
        if key_facts:
            grounded = True
            topic = _URL_RE.sub("", topic).strip() or topic
        else:
            logger.warning("[script] 링크 본문 수집 실패 → 일반 앵커링으로 진행")
            key_facts = await _enrich_topic(topic)
    else:
        # 링크가 없으면 스스로 검색해서 근거를 만든다. 실패하면 기존 앵커링.
        key_facts = await _web_search_facts(topic)
        if key_facts:
            grounded = True
        else:
            key_facts = await _enrich_topic(topic)
    prompt = _script_prompt(topic, duration_sec, tone, key_facts=key_facts)

    # 실제 출처 없이 실존 대상을 설명하려 하면 지어내지 못하도록 못을 박는다.
    if not grounded:
        _entities = find_named_entities(topic)
        if _entities:
            logger.warning(
                f"[script] 근거 자료 없이 실존 대상 감지: {_entities} → 사실 제약 프롬프트 추가"
            )
            prompt += _ungrounded_entity_warning(_entities)
    # 최소 글자수. 하한 200자가 짧은 영상의 발목을 잡고 있었다 — 30초 요청이면
    # 목표가 168자인데 최소 200자를 강제하니, 그걸 채우는 순간 36초가 되고
    # 씬 패딩까지 더해 40초가 나왔다(실측). 구조적으로 30초를 맞출 수 없던 셈이다.
    # 하한을 낮추고, 최소값이 목표치를 넘지 못하도록 캡을 씌운다.
    _target_chars = int(duration_sec * chars_per_sec(tone))
    # 하한은 목표에 연동해야 한다. duration_sec * 4.8 로 고정돼 있어서
    # 목표가 189자로 올라가도 하한은 144자에 머물렀고, 병렬 레이스가 162자에서
    # 만족하고 끝났다(실측: 30초 요청 → 25.2초, -16%).
    # 목표의 92% 를 하한으로 두되, 모든 후보가 탈락해 생성 자체가 실패하는
    # 일이 없도록 절대 하한(초당 4.8자)도 함께 지킨다.
    # [2026-09-21] 하한에 절대 상한을 씌운다.
    #
    # 어제 하한을 '목표의 92%' 로 바꿨는데, 30초(하한 181자)에서는 잘 맞았지만
    # 5분에서는 하한이 1598자가 됐다. 그 길이를 한 번에 써 내는 모델이 없어
    # Groq·Claude 가 200 OK 로 응답했는데도 전부 탈락했고, 파이프라인이
    # "all APIs failed" 로 판단해 템플릿(주제명만 반복하는 자리표시자)을 냈다.
    # 사용자가 본 게 그 템플릿이다.
    #
    # 긴 원고는 한 번의 호출로 목표치를 채울 수 없다. 하한은 '이 정도는 나와야
    # 쓸 수 있다' 수준이면 충분하고, 길이 맞추기는 뒤의 트림·압축이 한다.
    _MIN_LEN_CAP = 700
    _floor = max(60, min(int(duration_sec * 4.8), _MIN_LEN_CAP))
    min_len = min(_target_chars, max(_floor, min(int(_target_chars * 0.92),
                                                 _MIN_LEN_CAP)))
    max_tokens = _script_max_tokens(duration_sec)
    # 긴 스크립트는 생성 시간 더 필요 (최소 30초, 300초 영상 → 60초)
    race_timeout = max(75.0, min(200.0, duration_sec * 0.35))
    logger.info(f"[script] target={duration_sec}s min_len={min_len}자 max_tokens={max_tokens} timeout={race_timeout}s")

    tasks = _build_text_tasks(prompt, max_tokens=max_tokens)
    result = await _parallel_race(tasks, min_len=min_len, timeout=race_timeout)
    result = _strip_llm_preamble(result) if result else result

    if result:
        logger.info(f"[script] ✅ script generated ({len(result)}자)")
        reviewed = _strip_llm_preamble(await _cross_review(result, topic))
        # 검토 단계가 원고를 깎아 목표 길이에 한참 못 미치면 초안을 쓴다.
        # (실측: 뉴스·다큐가 목표 156자인데 검토 뒤 121자 → 23초, -23%)
        if len(reviewed) < _target_chars * 0.85 <= len(result):
            logger.info(
                f"[script] 검토본이 너무 짧다({len(reviewed)}자 < 목표 {_target_chars}자) "
                f"→ 초안({len(result)}자) 사용"
            )
            reviewed = result
        trimmed = _trim_to_budget(reviewed, _target_chars, min_len)
        # 문장 단위 트림이 목표보다 10% 넘게 깎아냈다면, 버리는 대신 줄여본다.
        if len(trimmed) < _target_chars * 0.90:
            condensed = await _condense_to_budget(reviewed, _target_chars)
            if condensed and abs(len(condensed) - _target_chars) < abs(len(trimmed) - _target_chars):
                return condensed
        return trimmed

    # 최종 fallback: 주제 기반 템플릿
    logger.error(f"[script] ❌ all APIs failed, generating template for: {topic}")
    return _generate_template_script(topic, duration_sec, tone)


# 원고 본문에는 나올 수 없고, 모델이 답변 머리말로만 쓰는 표현들.
#
# 처음에 "확인해", "검토" 같은 넓은 말을 넣었더니 정상 원고를 잘랐다
# (오탐 실측: "확인해보니 이 제품은 문제가 없었습니다." → 통째로 삭제).
# 원고에 자연스럽게 등장할 수 있는 말은 전부 뺐다. 놓치는 쪽이 지우는 쪽보다 낫다.
_PREAMBLE_MARKERS = (
    "iteration", "revised script", "revised version", "final version",
    "here is", "here's the", "sure,", "certainly,",
    "수정본", "수정안", "개선본", "최종본", "완성본", "결과물",
    "확인해봅시다", "확인해보겠습니다", "살펴봅시다", "시작하겠습니다",
    "알겠습니다", "다음은 ", "아래는 ",
)


def _strip_llm_preamble(text: str) -> str:
    """모델이 원고 앞에 붙인 잡담·메타 서두를 걷어낸다.

    실측 사고: 교차검토 모델이 "확인해봅시다. Text iteration 2: 김치가 유네스코…"
    라고 답했고, 그 문장 전체가 나레이션이 되어 TTS 가 "확인해봅시다 텍스트
    이터레이션 이" 를 읽었다. 프롬프트에는 iteration 이라는 말이 없다 —
    모델이 스스로 붙인 것이다. 그래서 프롬프트를 고쳐서는 막을 수 없고,
    받은 뒤에 걷어내야 한다.

    보수적으로 동작한다. 지울 근거가 분명할 때만 지운다:
      - 코드펜스(```) 로 감싼 경우 안쪽만 취한다
      - 앞쪽 40자 이내의 짧은 조각이 콜론으로 끝나고 메타 단어를 포함하면 버린다
      - 첫 문장이 짧고(25자 이하) 메타 단어를 포함하면 버린다
    """
    if not text:
        return text
    t = text.strip()

    # 코드펜스
    if t.startswith("```"):
        parts = t.split("```")
        if len(parts) >= 3:
            body = parts[1]
            if "\n" in body:
                body = body.split("\n", 1)[1]
            t = body.strip()

    for _ in range(3):          # 서두가 두세 겹 붙는 경우가 있다
        low = t.lower()

        # "Text iteration 2:" 처럼 콜론으로 끝나는 짧은 머리말
        ci = t.find(":")
        if 0 < ci <= 40:
            head = low[:ci]
            if any(m in head for m in _PREAMBLE_MARKERS):
                t = t[ci + 1:].strip()
                continue

        # "확인해봅시다." 같은 짧은 메타 첫 문장
        cut = -1
        for mark in (". ", "! ", "? ", ".\n", "。"):
            p = t.find(mark)
            if p != -1 and (cut == -1 or p < cut):
                cut = p + len(mark)
        if 0 < cut <= 26:
            head = low[:cut]
            if any(m in head for m in _PREAMBLE_MARKERS):
                t = t[cut:].strip()
                continue
        break

    t = t.strip().strip('"').strip("'").strip()
    if t != text.strip():
        logger.info(f"[script] 서두 제거: {text.strip()[:40]!r} → {t[:40]!r}")
    return t or text.strip()


def _count_sentences(script: str) -> int:
    """Count sentences (문장) — the minimum unit of complete content.

    문장 (sentence): 완결된 내용을 나타내는 최소의 단위.
    Each scene = one sentence. This ensures every scene carries a single
    complete idea without mixing unrelated content.

    Korean LLM output often omits spaces after sentence-ending punctuation,
    so we normalise first before splitting.
    """
    # Insert space after punctuation before Korean/uppercase (handles no-space output)
    normalized = re.sub(r'([.!?！？。])([가-힣A-Z])', r'\1 \2', script.strip())
    # Split on punctuation + whitespace
    parts = re.split(r'(?<=[.!?！？。])\s+', normalized)
    # Filter: ≥4 chars AND at least one letter (excludes '17.', '3.14')
    meaningful = [
        p for p in parts
        if len(p.strip()) >= 4 and re.search(r'[가-힣a-zA-Z]', p)
    ]
    return max(1, len(meaningful))


async def split_script_to_scenes(
    script: str,
    topic: str,
    video_type: str = "longform",
    duration_sec: int = 60,
    tone: str = "neutral",
    n_scenes_override: int | None = None,
) -> List[Scene]:
    """스크립트를 씬으로 분할. Sequential fallback chain.
    narration 보존 검증: 원본 스크립트의 40% 미만이면 압축된 것으로 판단 → skip.

    n_scenes is driven by sentence count: each sentence gets at least 1 scene.
    Capped at 10 (shorts) or 20 (longform) to prevent asset overload.
    n_scenes_override: 대시보드 '씬 수' 선택값(3/5/7/10)이 주어지면 자동 계산 대신
    이 값을 사용한다 (안전 상한 max_n_scenes는 그대로 적용).
    """
    base_n_scenes: int = 3 if video_type == "shorts" else 5
    max_n_scenes: int = 10 if video_type == "shorts" else 20
    para_count = _count_sentences(script)
    total_chars = len(script)
    # Cap n_scenes by char budget: at least 40 chars of narration per scene
    # Prevents requesting 20 scenes from a 200-char script (causes LLM repetition)
    max_from_chars = max(base_n_scenes, total_chars // 40)
    if n_scenes_override:
        n_scenes: int = max(1, min(int(n_scenes_override), max_n_scenes))
    else:
        n_scenes = min(max(para_count, base_n_scenes), max_from_chars, max_n_scenes)
    # 씬당 최소 글자수: 균등 분할의 50% (LLM 앵커링용)
    min_chars = max(50, total_chars // n_scenes // 2)
    # 씬 JSON 출력에 필요한 max_tokens (나레이션 보존 필수)
    scene_max_tokens = _scene_max_tokens(total_chars, n_scenes)

    prompt = _SPLIT_PROMPT.format(
        n=n_scenes, topic=topic, script=script,
        total_chars=total_chars, min_chars=min_chars,
    )
    logger.info(
        f"[script] split: {total_chars}자 → {n_scenes}씬 "
        f"(sentences={para_count}, base={base_n_scenes}, "
        f"char_cap={max_from_chars}, abs_cap={max_n_scenes}), "
        f"min_chars={min_chars}, max_tokens={scene_max_tokens}"
    )

    for name, coro in _scene_splitter_chain(prompt, n_scenes, scene_max_tokens):
        result = await coro
        if not result:
            continue
        total_narration = sum(len(s.narration or "") for s in result)
        # 분할은 '쪼개기'지 '다시 쓰기'가 아니다. 하한만 막아뒀더니 반대쪽이
        # 뚫려 있었다 — 실측: 여행 카테고리에서 176자 원고가 씬 나레이션 218자
        # (+24%)로 불어나 30초 요청이 41초 영상이 됐다. 스크립트 단계에서 잡아둔
        # 길이 예산이 여기서 통째로 무너지므로 상한도 같이 막는다.
        if total_narration > total_chars * 1.12 and total_chars > 100:
            logger.warning(
                f"[script] {name} narration inflated: {total_narration}자 "
                f"({total_narration * 100 // max(total_chars, 1)}% of {total_chars}자) > 112% "
                f"→ skip (분할기가 내용을 덧붙임)"
            )
            continue
        # 나레이션이 통째로 비어 오는 경우가 실제로 있었다.
        # (실측: "1 scenes from Groq-120b (narration=0자)" → 3.2초짜리 영상 출력)
        if total_narration == 0:
            logger.warning(f"[script] {name} 빈 나레이션 → skip")
            continue
        # Reject if narration is below 60% of source: LLM summarized instead of excerpting
        #
        # 원래 조건에 total_chars > 200 이 붙어 있어서, 스크립트가 200자 이하면
        # 이 가드가 통째로 꺼졌다. 길이 예산을 조인 뒤로는 거의 모든 스크립트가
        # 160자 언저리라 사실상 상시 무방비 상태였다. 하한을 40자로 낮춘다.
        if total_narration < total_chars * 0.60 and total_chars > 40:
            logger.warning(
                f"[script] {name} narration compressed: {total_narration}자 "
                f"({total_narration * 100 // max(total_chars, 1)}% of {total_chars}자) < 60% → skip"
            )
            continue
        logger.info(f"[script] ✅ {len(result)} scenes from {name} (narration={total_narration}자)")
        result = _enrich_keywords(result, topic)
        return result

    logger.warning("[script] all scene APIs failed or narration compressed → local split")
    return _split_script_locally(script or topic, topic, n_scenes, duration_sec)


# ============================================================================
# Parallel race helper
# ============================================================================

_CJK_HANJA = re.compile(r"[\u4e00-\u9fff]")
_LATIN_LOWER = re.compile(r"\b[a-z]{3,}\b")


def _korean_quality(text: str) -> tuple[bool, str]:
    """한국어 나레이션으로 내보낼 수 있는 품질인지 검사.

    병렬 레이스는 '가장 먼저 도착한' 응답을 채택할 뿐 품질을 보지 않는다. 그래서
    빠르지만 한국어가 불안정한 모델이 늘 이기고, 실측으로 '오늘我们将은', '무钉 결합법',
    'intricate한' 같은 중국어/영어 혼입이 그대로 나레이션이 됐다. TTS가 이런 글자를
    읽지 못해 영상 품질이 바로 깨지므로, 레이스 단계에서 걸러낸다.
    """
    if not text:
        return False, "빈 응답"
    hanja = len(_CJK_HANJA.findall(text))
    if hanja > 2:
        return False, f"한자 {hanja}자 혼입"
    # AI·GPS·TV 같은 대문자 약어는 한국어 나레이션에서 정상이므로 통과시키고,
    # 'intricate한' 처럼 소문자로 섞여 들어오는 영단어만 혼입으로 본다.
    lower = len(_LATIN_LOWER.findall(text))
    per100 = lower / max(len(text) / 100.0, 1.0)
    if per100 > 1.0:
        return False, f"영단어 {lower}개(100자당 {per100:.1f}) 혼입"
    return True, ""


async def _parallel_race(
    tasks: List[tuple[str, Any]],
    min_len: int = 20,
    timeout: float = None,
) -> Optional[str]:
    """모든 task를 병렬 실행. 조건 만족하는 첫 결과 반환."""
    if not tasks:
        return None

    race_timeout = timeout or (LLM_TIMEOUT + 10)

    # (name, coro) 목록에서 태스크 생성
    named_tasks: List[tuple[str, asyncio.Task]] = []
    for name, coro in tasks:
        t = asyncio.create_task(coro)
        named_tasks.append((name, t))

    all_tasks = [t for _, t in named_tasks]
    result: Optional[str] = None
    fallback: Optional[str] = None   # 품질 탈락했지만 템플릿보다는 나은 차선책

    # as_completed: 완료 순서대로 처리
    for coro in asyncio.as_completed(all_tasks, timeout=race_timeout):
        try:
            text = await coro
            if text and len(text) < min_len:
                logger.info(
                    f"[script] 길이 미달로 탈락: {len(text)}자 < 하한 {min_len}자")
            if text and len(text) >= min_len:
                _ok, _why = _korean_quality(text)
                if not _ok:
                    # 전부 탈락하면 템플릿으로 추락하므로, 가장 긴 탈락본을 차선책으로 남긴다.
                    if not fallback or len(text) > len(fallback):
                        fallback = text
                    logger.warning(f"[script] 품질 탈락({_why}) → 다음 후보 대기")
                    continue
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

    if result is None and fallback:
        logger.warning(f"[script] 모든 후보가 품질 탈락 → 차선책 사용({len(fallback)}자). 템플릿보다는 낫다.")
        return fallback
    return result


# ============================================================================
# Task builders
# ============================================================================

def _build_text_tasks(prompt: str, max_tokens: int = 1500) -> List[tuple[str, Any]]:
    """스크립트 텍스트 생성 병렬 태스크 목록."""
    tasks = []
    _sys = "당신은 한국어 전문 유튜브 스크립트 작가입니다. 반드시 한국어로만 답변하세요. 중국어·일본어·터키어·베트남어 등 외국어 단어 절대 사용 금지."

    # 1. Groq (빠름, 한국어 양호)
    # 2026-06-17 Groq가 llama-3.3-70b-versatile / llama-3.1-8b-instant deprecate →
    # gpt-oss(120b/20b)로 교체 (실측 /v1/models 확인, 2026-09-18).
    if GROQ_API_KEY:
        # 1순위는 non-reasoning 모델. gpt-oss 계열은 reasoning 에 토큰을 다 쓰고
        # content 를 빈 문자열로 돌려주는 일이 잦아(실측) 뒤로 물렸다.
        tasks.append(("Groq-qwen", _llm_text_oai(
            "https://api.groq.com/openai/v1/chat/completions",
            GROQ_API_KEY,
            "qwen/qwen3.8-27b",
            prompt,
            system=_sys,
            max_tokens=max_tokens,
        )))
        tasks.append(("Groq-120b", _llm_text_oai(
            "https://api.groq.com/openai/v1/chat/completions",
            GROQ_API_KEY,
            "openai/gpt-oss-120b",
            prompt,
            system=_sys,
            max_tokens=max_tokens,
            # gpt-oss 는 reasoning 모델 — 사고과정에 토큰을 다 쓰고 content 를 비워
            # 보내는 일이 잦아서 사고 깊이를 낮춰 본문 토큰을 확보한다.
            extra={"reasoning_effort": "low"},
        )))
        # 20b는 긴 스크립트에 부적합 → 300s 이상이면 제외
        if max_tokens <= 2000:
            tasks.append(("Groq-20b", _llm_text_oai(
                "https://api.groq.com/openai/v1/chat/completions",
                GROQ_API_KEY,
                "openai/gpt-oss-20b",
                prompt,
                system=_sys,
                max_tokens=max_tokens,
                extra={"reasoning_effort": "low"},
            )))

    # 2. DeepSeek (한국어 최강)
    if DEEPSEEK_API_KEY:
        tasks.append(("DeepSeek", _llm_text_oai(
            "https://api.deepseek.com/v1/chat/completions",
            DEEPSEEK_API_KEY,
            "deepseek-chat",
            prompt,
            system=_sys,
            max_tokens=max_tokens,
        )))

    # 3. Gemini
    if GEMINI_API_KEY:
        tasks.append(("Gemini", _llm_text_gemini(prompt, max_tokens=max_tokens)))

    # 4. Cerebras
    if CEREBRAS_API_KEY:
        tasks.append(("Cerebras", _llm_text_oai(
            "https://api.cerebras.ai/v1/chat/completions",
            CEREBRAS_API_KEY,
            CEREBRAS_MODEL,
            prompt,
            system=_sys,
            max_tokens=max_tokens,
        )))

    # 5. ArliAI
    if ARLIAI_API_KEY:
        tasks.append(("ArliAI", _llm_text_oai(
            "https://api.arliai.com/v1/chat/completions",
            ARLIAI_API_KEY,
            ARLIAI_MODEL,
            prompt,
            system=_sys,
            max_tokens=max_tokens,
        )))

    # 6. OpenRouter (무료 모델)
    if OPENROUTER_API_KEY:
        tasks.append(("OpenRouter-llama", _llm_text_openrouter(
            prompt,
            "meta-llama/llama-3.3-70b-instruct:free",
            max_tokens=max_tokens,
        )))
        tasks.append(("OpenRouter-deepseek", _llm_text_openrouter(
            prompt,
            "deepseek/deepseek-chat-v3-0324:free",
            max_tokens=max_tokens,
        )))

    # 7. OpenAI (유료, 마지막)
    if OPENAI_API_KEY:
        tasks.append(("OpenAI", _llm_text_oai(
            "https://api.openai.com/v1/chat/completions",
            OPENAI_API_KEY,
            "gpt-4o-mini",
            prompt,
            system=_sys,
            max_tokens=max_tokens,
        )))

    # 8. Claude (마지막, 크레딧 아낌)
    if ANTHROPIC_API_KEY:
        tasks.append(("Claude", _llm_text_claude(prompt, max_tokens=max_tokens)))

    return tasks


def _scene_splitter_chain(prompt: str, n_scenes: int, max_tokens: int = 2000):
    """씬 분할용 Sequential generator. JSON 파싱 필요 = sequential이 안전."""
    if GROQ_API_KEY:
        yield "Groq-120b",  _call_groq_scenes(prompt, n_scenes, "openai/gpt-oss-120b", max_tokens)
        yield "Groq-20b",   _call_groq_scenes(prompt, n_scenes, "openai/gpt-oss-20b", max_tokens)
    if DEEPSEEK_API_KEY:
        yield "DeepSeek",   _call_oai_scenes("https://api.deepseek.com/v1/chat/completions", DEEPSEEK_API_KEY, "deepseek-chat", prompt, n_scenes, max_tokens=max_tokens)
    if GEMINI_API_KEY:
        yield "Gemini",     _call_gemini_scenes(prompt, n_scenes, max_tokens)
    if CEREBRAS_API_KEY:
        yield "Cerebras",   _call_oai_scenes("https://api.cerebras.ai/v1/chat/completions", CEREBRAS_API_KEY, CEREBRAS_MODEL, prompt, n_scenes, max_tokens=max_tokens)
    if ARLIAI_API_KEY:
        yield "ArliAI",     _call_oai_scenes("https://api.arliai.com/v1/chat/completions", ARLIAI_API_KEY, ARLIAI_MODEL, prompt, n_scenes, max_tokens=max_tokens)
    if OPENROUTER_API_KEY:
        yield "OpenRouter", _call_oai_scenes(
            "https://openrouter.ai/api/v1/chat/completions",
            OPENROUTER_API_KEY, "meta-llama/llama-3.3-70b-instruct:free",
            prompt, n_scenes, max_tokens=max_tokens,
            extra_headers={"HTTP-Referer": "https://longform.spacek.io", "X-Title": "LongForm Factory"},
        )
    if OPENAI_API_KEY:
        yield "OpenAI",     _call_oai_scenes("https://api.openai.com/v1/chat/completions", OPENAI_API_KEY, "gpt-4o-mini", prompt, n_scenes, max_tokens=max_tokens)
    if ANTHROPIC_API_KEY:
        yield "Claude",     _call_claude_scenes(prompt, n_scenes, max_tokens)


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
    extra: Optional[dict] = None,
) -> str:
    """OpenAI 호환 chat completions 호출.

    gpt-oss 계열은 'reasoning 모델'이라 사고과정에 토큰을 먼저 쓰고 최종 답변을
    content 에 담는다. reasoning 이 길어지면 content 가 빈 문자열로 돌아오는데,
    예전 코드는 그걸 그대로 반환해서 '200 OK 인데 결과는 빈값' -> 전체 폴백이라는
    조용한 실패를 냈다. 그래서 (a) 호출부에서 reasoning_effort 를 낮추고,
    (b) content 가 비면 reasoning 텍스트라도 건지고, (c) 빈 응답을 경고로 남긴다.
    """
    cancel.check_active("_llm_text_oai")
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    payload = {
        "model": model,
        "max_tokens": max_tokens,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": prompt},
        ],
    }
    if extra:
        payload.update(extra)
    try:
        async with httpx.AsyncClient(timeout=LLM_TIMEOUT) as c:
            resp = await c.post(url, headers=headers, json=payload)
            if resp.status_code == 200:
                msg = (resp.json().get("choices") or [{}])[0].get("message") or {}
                text = (msg.get("content") or "").strip()
                if not text:
                    text = (msg.get("reasoning") or "").strip()
                    if text:
                        logger.warning(f"[llm] {model}: content 비어서 reasoning 사용 ({len(text)}자)")
                if not text:
                    logger.warning(f"[llm] {model}: 200 OK 인데 본문이 비어 있음 (reasoning 토큰 소진 가능성)")
                return text
            logger.warning(f"[llm] {model} HTTP {resp.status_code}: {resp.text[:80]}")
    except Exception as e:
        logger.warning(f"[llm] {model} 예외: {type(e).__name__}: {str(e)[:100]}")
    return ""


async def _llm_text_openrouter(prompt: str, model: str, max_tokens: int = 1500) -> str:
    cancel.check_active("_llm_text_openrouter")
    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://longform.spacek.io",
        "X-Title": "LongForm Factory",
    }
    payload = {
        "model": model,
        "max_tokens": max_tokens,
        "messages": [
            {"role": "system", "content": "반드시 한국어로만 답변하세요. 외국어 단어 절대 금지."},
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
        logger.warning(f"[llm] OpenRouter/{model} 예외: {type(e).__name__}: {str(e)[:100]}")
    return ""


async def _llm_text_gemini(prompt: str, max_tokens: int = 1500) -> str:
    cancel.check_active("_llm_text_gemini")
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"
    payload = {
        "contents": [{"parts": [{"text": "반드시 한국어로만 답변하세요. 외국어 단어 절대 금지.\n\n" + prompt}]}],
        "generationConfig": {"maxOutputTokens": max_tokens},
    }
    try:
        async with httpx.AsyncClient(timeout=LLM_TIMEOUT) as c:
            resp = await c.post(url, headers={"Content-Type": "application/json"}, json=payload)
            if resp.status_code == 200:
                # 200 이어도 parts 가 없을 수 있다(실측: KeyError 'parts').
                # MAX_TOKENS 로 잘렸거나 안전필터에 걸리면 content 가 비거나
                # finishReason 만 온다. 체인이 다음 모델로 넘어가도록 ""를 준다.
                data = resp.json()
                cands = data.get("candidates") or []
                if not cands:
                    logger.warning(f"[llm] Gemini 200 인데 candidates 없음: "
                                   f"{str(data)[:140]}")
                    return ""
                c0 = cands[0]
                parts = ((c0.get("content") or {}).get("parts")) or []
                text = "".join(p.get("text", "") for p in parts
                               if isinstance(p, dict)).strip()
                if not text:
                    logger.warning(
                        f"[llm] Gemini 200 인데 text 없음 "
                        f"(finishReason={c0.get('finishReason')})")
                return text
            logger.warning(f"[llm] Gemini HTTP {resp.status_code}: {resp.text[:80]}")
    except Exception as e:
        logger.warning(f"[llm] Gemini 예외: {type(e).__name__}: {str(e)[:100]}")
    return ""


async def _llm_text_claude(prompt: str, max_tokens: int = 1500) -> str:
    cancel.check_active("_llm_text_claude")
    payload = {
        "model": ANTHROPIC_MODEL,
        "max_tokens": max_tokens,
        # thinking 을 끄지 않으면 max_tokens 를 사고과정이 다 먹고 text 블록이
        # 아예 오지 않는다(실측: content=[{'type':'thinking','thinking':''}]).
        # 나레이션 작성·교정에는 사고과정 출력이 필요 없다.
        "thinking": {"type": "disabled"},
        "system": "당신은 한국어 유튜브 스크립트 전문가입니다. 반드시 한국어로만 답변하세요. 중국어·일본어·터키어·베트남어 등 외국어 단어 절대 사용 금지.",
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
                # content 는 블록 배열이고 thinking / tool_use 블록이 앞에 올 수 있다.
                # 예전처럼 [0]["text"] 를 바로 읽으면 KeyError 로 죽어(실측) 조용히
                # 탈락했으므로, type=="text" 블록만 모아서 이어붙인다.
                blocks = resp.json().get("content") or []
                parts = [
                    b.get("text", "")
                    for b in blocks
                    if isinstance(b, dict) and b.get("type") == "text"
                ]
                text = "\n".join(p for p in parts if p).strip()
                if not text:
                    logger.warning(f"[llm] Claude 200 OK 인데 text 블록 없음: {str(blocks)[:120]}")
                return text
            logger.warning(f"[llm] Claude HTTP {resp.status_code}: {resp.text[:80]}")
    except Exception as e:
        logger.warning(f"[llm] Claude 예외: {type(e).__name__}: {str(e)[:100]}")
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
    max_tokens: int = 2000,
) -> Optional[List[Scene]]:
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    if extra_headers:
        headers.update(extra_headers)
    payload = {
        "model": model,
        "max_tokens": max_tokens,
        "messages": [
            {"role": "system", "content": "반드시 JSON 배열만 반환. 마크다운 코드블록 절대 금지. 한국어 나레이션. 나레이션은 원문 그대로 발췌 (요약 금지)."},
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


async def _call_groq_scenes(prompt: str, n_scenes: int, model: str = "openai/gpt-oss-120b", max_tokens: int = 2000) -> Optional[List[Scene]]:
    return await _call_oai_scenes(
        "https://api.groq.com/openai/v1/chat/completions",
        GROQ_API_KEY, model, prompt, n_scenes, max_tokens=max_tokens,
    )


async def _call_gemini_scenes(prompt: str, n_scenes: int, max_tokens: int = 2000) -> Optional[List[Scene]]:
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"
    payload = {
        "contents": [{"parts": [{"text": "반드시 JSON 배열만 반환. 마크다운 코드블록 절대 금지. 나레이션 요약 금지.\n\n" + prompt}]}],
        "generationConfig": {"maxOutputTokens": max_tokens},
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


async def _call_claude_scenes(prompt: str, n_scenes: int, max_tokens: int = 2000) -> Optional[List[Scene]]:
    payload = {
        "model": ANTHROPIC_MODEL,
        "max_tokens": max_tokens,
        # thinking 을 끄지 않으면 max_tokens 를 사고과정이 다 먹고 text 블록이
        # 아예 오지 않는다(실측: content=[{'type':'thinking','thinking':''}]).
        # 나레이션 작성·교정에는 사고과정 출력이 필요 없다.
        "thinking": {"type": "disabled"},
        "system": "반드시 JSON 배열만 반환. 마크다운 코드블록 절대 금지. 한국어 나레이션. 나레이션은 원문 그대로 발췌 (요약·압축 금지).",
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
            # Always recalculate duration from narration length — never trust LLM math
            # LLMs consistently produce wrong values (e.g. 217s for 243-char narration)
            narration_text = s.get("narration", "")
            s["duration_seconds"] = max(round(len(narration_text) / 5.56, 1), 3.0)
            valid.append(Scene(**s))
        return valid if valid else None
    except Exception as e:
        logger.warning(f"[script] JSON parse failed: {e} | raw={raw[:120]}")
    return None


_HANGUL_KW_RE = re.compile(r"[가-힣]")

# 한글 키워드를 '버리지 않고 옮기기' 위한 용어집.
#
# 한글을 그냥 지우면 '전기차 충전 winter' 가 'winter' 가 되고,
# 전부 한글이면 'korean culture' 로 떨어져 주제와 무관한 화면이 나온다(실측).
# 자주 나오는 명사만이라도 옮겨두면 검색어가 살아남는다. 없는 말은 지워지되
# 다른 낱말이 남아 있으면 그쪽으로 검색된다.
_KO_EN_GLOSSARY = {
    # 이동수단·에너지
    "전기차": "electric car", "자동차": "car", "배터리": "battery",
    "충전": "charging", "주행": "driving", "주행거리": "driving range",
    "히터": "car heater", "엔진": "engine", "타이어": "tire",
    "태양광": "solar panel", "풍력": "wind turbine", "전력": "electricity",
    # 제조·산업
    "스마트팩토리": "smart factory", "공장": "factory", "제조": "manufacturing",
    "자동화": "automation", "생산": "production line", "품질": "quality control",
    "로봇": "industrial robot", "설비": "industrial equipment",
    "물류": "logistics warehouse", "중소기업": "small business office",
    # 일상·공간
    "퇴근": "commute evening", "저녁": "evening", "아침": "morning",
    "집": "home interior", "거실": "living room", "주방": "kitchen",
    "카페": "cafe", "사무실": "office", "책상": "desk workspace",
    "스마트폰": "smartphone", "노트북": "laptop", "텔레비전": "television",
    "휴식": "relaxing", "청소": "cleaning home",
    # 여행·자연
    "여행": "travel", "제주도": "jeju island", "오름": "volcanic hill",
    "트레킹": "hiking trail", "등산": "mountain hiking", "단풍": "autumn foliage",
    "바다": "ocean", "해변": "beach", "숙소": "hotel room", "공항": "airport",
    # 문화·미디어
    "한류": "korean wave", "케이팝": "kpop", "드라마": "tv drama",
    "공연": "concert stage", "관광객": "tourists", "축제": "festival crowd",
    "화장품": "cosmetics", "뷰티": "beauty products",
    # 음식
    "라면": "instant noodles", "음식": "food", "요리": "cooking",
    "수출": "shipping export", "시장": "market",
    # 기타 자주 쓰는 말
    "겨울": "winter", "여름": "summer", "봄": "spring", "가을": "autumn",
    "온도": "temperature", "날씨": "weather", "가격": "price tag",
    "회의": "business meeting", "상담": "consulting meeting",
    "정부": "government building", "지원": "support",
}


def _translate_ko_token(tok: str) -> str:
    """한글 낱말 하나를 용어집으로 옮긴다. 모르면 빈 문자열."""
    if tok in _KO_EN_GLOSSARY:
        return _KO_EN_GLOSSARY[tok]
    # 조사가 붙었거나 합성어인 경우: 긴 표제어부터 부분일치
    for ko, en in sorted(_KO_EN_GLOSSARY.items(), key=lambda kv: -len(kv[0])):
        if len(ko) >= 2 and ko in tok:
            return en
    return ""


def _sanitize_keyword(keyword: str, narration: str, topic: str) -> str:
    """Replace LLM placeholder keywords with real English search terms.

    LLMs sometimes copy the instruction text ("영어 1-3단어", "한글 N단어")
    verbatim as the keyword value, causing Pexels to download the same
    useless footage for every scene.  This function detects and replaces
    those placeholder strings with topic-derived English keywords.
    """
    kw = (keyword or "").strip()

    # 한글이 섞인 키워드는 그대로 스톡 API 로 나가면 결과가 0건이거나 엉뚱하다.
    # (실측: query="korean 전기차 culture" → 약국 간판·거리 영상이 들어왔다)
    # 영어 낱말이 남아 있으면 그걸 쓰고, 하나도 없을 때만 주제에서 새로 뽑는다.
    # '전부 버리고 korean culture' 로 가면 주제와 무관한 화면이 되기 때문이다.
    if kw and _HANGUL_KW_RE.search(kw):
        parts = []
        for w in re.split(r"\s+", kw):
            if not w:
                continue
            if _HANGUL_KW_RE.search(w):
                en = _translate_ko_token(w)
                if en:
                    parts.append(en)
            else:
                parts.append(w)
        # 중복 낱말 제거(용어집이 같은 영어를 두 번 내놓는 경우)
        seen, out = set(), []
        for token in " ".join(parts).split():
            if token.lower() not in seen:
                seen.add(token.lower())
                out.append(token)
        translated = " ".join(out[:5]).strip()
        if translated:
            logger.warning(f"[keyword] 한글 혼입 '{kw}' → '{translated}'")
            return translated
        derived = _topic_to_keyword(topic, "") or _topic_to_keyword(narration[:60], "korean culture")
        logger.warning(f"[keyword] 옮길 수 없는 한글 '{kw}' → 주제 기반 '{derived}'")
        return derived

    if not kw or _PLACEHOLDER_KW.search(kw):
        # Derive keyword from narration text (first 50 chars) or topic
        source = narration[:50] if narration else topic
        replacement = _topic_to_keyword(source, _topic_to_keyword(topic, "korean culture"))
        logger.warning(
            f"[keyword] placeholder detected '{kw}' → '{replacement}'"
        )
        return replacement
    return kw


# 같은 영어 낱말이 전혀 다른 그림을 부르는 경우가 있다.
# 실측: '전기차 배터리' 영상에 분해된 스마트폰 기판이 들어왔다. 'battery' 만으로는
# 휴대폰 배터리와 구분이 안 되기 때문이다. 주제에 해당 맥락이 있으면 검색어에
# 한정어를 덧붙여 엉뚱한 소재가 1등으로 올라오는 것을 줄인다.
_CONTEXT_QUALIFIERS = [
    (("전기차", "ev ", "electric car", "electric vehicle"), "battery", "ev charging station"),
    (("스마트팩토리", "제조", "공장"), "automation", "industrial factory"),
    (("한류", "케이팝", "k-pop", "kpop"), "culture", "korea"),
    (("우주", "위성", "로켓"), "space", "spacecraft"),
]


def _apply_context(keyword: str, topic: str) -> str:
    """주제 맥락에 맞는 한정어를 키워드에 덧붙인다(이미 있으면 그대로)."""
    kl, tl = keyword.lower(), topic.lower()
    for topic_keys, trigger, add in _CONTEXT_QUALIFIERS:
        if trigger in kl and any(k in tl for k in topic_keys):
            if add.split()[0] not in kl:
                return f"{keyword} {add}"
    return keyword


def _enrich_keywords(scenes: List[Scene], topic: str) -> List[Scene]:
    """Sanitize and enrich scene keywords after LLM result parsing.

    1. Replace placeholder values (e.g. "영어 1-3단어") with real English terms.
    2. Replace overly generic fallback keywords with topic-derived terms.
    3. Inject topic subject noun into every keyword if not already present.
       e.g. topic="고양이" subject="cat", keyword="morning light" → "cat morning light"
       This ensures stock footage is visually relevant to the video topic.
    """
    generic = {"nature landscape", "city skyline", "people working",
               "technology innovation", "healthy lifestyle", "korean culture"}
    subject = _extract_topic_subject(topic)

    for s in scenes:
        # Step 1: sanitize placeholder / pure-Korean keywords
        s.keyword = _sanitize_keyword(s.keyword, s.narration or "", topic)
        # Step 2: enrich generic fallbacks
        if s.keyword in generic:
            topic_en = _topic_to_keyword(topic, s.keyword)
            s.keyword = topic_en
        # Step 3: inject subject noun if missing from keyword
        if subject and subject.lower() not in s.keyword.lower():
            s.keyword = f"{subject} {s.keyword}"
            logger.debug(f"[keyword] subject injected: '{subject}' → '{s.keyword}'")
        # Step 4: 중의적인 낱말에 맥락 한정어 추가 (battery → ev charging station)
        s.keyword = _apply_context(s.keyword, topic)
    return scenes


def _kw_hit(key: str, text: str) -> bool:
    """키워드가 주제에 '낱말로' 등장하는지. 한 글자 키만 경계를 따진다."""
    if len(key) >= 2:
        return key in text
    return re.search(rf"(?<![가-힣]){re.escape(key)}(?![가-힣])", text) is not None


def _topic_to_keyword(topic: str, fallback: str) -> str:
    """한국어 주제에서 Pexels 검색용 영어 키워드 생성."""
    mapping = {
        # Animals
        "고양이": "cat",
        "강아지": "dog",
        "개": "dog",
        "토끼": "rabbit",
        "새": "bird",
        "물고기": "fish",
        "곰": "bear",
        "사자": "lion",
        "호랑이": "tiger",
        "코끼리": "elephant",
        "펭귄": "penguin",
        "여우": "fox",
        "늑대": "wolf",
        "말": "horse",
        "소": "cow",
        "돼지": "pig",
        "개구리": "frog",
        "거북이": "turtle",
        "뱀": "snake",
        "앵무새": "parrot",
        # Food
        "비빔밥": "korean bibimbap food",
        "한식": "korean traditional food",
        "요리": "cooking kitchen",
        "커피": "coffee",
        "케이크": "cake dessert",
        "라면": "ramen noodles",
        "피자": "pizza",
        "초콜릿": "chocolate",
        # Lifestyle / People
        "건강": "healthy lifestyle",
        "운동": "exercise fitness",
        "요가": "yoga meditation",
        "명상": "meditation calm",
        "여행": "travel landscape",
        "캠핑": "camping outdoor",
        "독서": "reading books",
        "음악": "music performance",
        "영화": "cinema movie",
        "게임": "gaming",
        "패션": "fashion style",
        "육아": "parenting child",
        "결혼": "wedding",
        # Knowledge / Culture
        "한국": "korea culture",
        "역사": "history ancient",
        "기술": "technology innovation",
        "과학": "science research",
        "교육": "education learning",
        "경제": "economy business",
        "환경": "environment nature",
        "우주": "space cosmos",
        "철학": "philosophy thinking",
        "심리": "psychology mind",
        # Nature
        "산": "mountain nature",
        "바다": "ocean sea",
        "강": "river water",
        "숲": "forest trees",
        "꽃": "flowers bloom",
        "하늘": "sky clouds",
    }
    # 부분 문자열로 그냥 찾으면 한 글자 키가 지뢰가 된다.
    # 실측 사고: '중소 제조기업' 의 '소' 가 걸려 스마트팩토리 대신 소(cow)
    # 영상을 검색했다. '건강'→강(river), '생산'→산(mountain), '개발'→개(dog),
    # '정말'→말(horse) 도 같은 방식으로 터진다.
    #
    # 두 가지로 막는다.
    #   1) 긴 키부터 본다 — '건강' 이 '강' 보다 먼저 걸리게.
    #   2) 한 글자 키는 앞뒤가 한글이 아닐 때만 인정한다 (독립된 낱말일 때만).
    topic_lower = topic.lower()
    for ko, en in sorted(mapping.items(), key=lambda kv: -len(kv[0])):
        if _kw_hit(ko, topic_lower):
            return en
    return fallback or "korean culture"


def _extract_topic_subject(topic: str) -> str:
    """Extract a short English subject noun from the topic for keyword injection.

    Returns single word like 'cat', 'dog', 'cooking' that should appear
    in every scene keyword to ensure visually relevant stock footage is found.
    Returns empty string if no subject can be extracted.
    """
    # Use the same mapping but return just the FIRST word (the main noun)
    mapping = {
        "고양이": "cat", "강아지": "dog", "개": "dog", "토끼": "rabbit",
        "새": "bird", "물고기": "fish", "곰": "bear", "사자": "lion",
        "호랑이": "tiger", "코끼리": "elephant", "펭귄": "penguin",
        "여우": "fox", "늑대": "wolf", "말": "horse", "소": "cow",
        "돼지": "pig", "개구리": "frog", "거북이": "turtle",
        "뱀": "snake", "앵무새": "parrot",
        "커피": "coffee", "케이크": "cake", "라면": "ramen",
        "피자": "pizza", "초콜릿": "chocolate",
        "요리": "cooking", "운동": "exercise", "요가": "yoga",
        "음악": "music", "영화": "cinema", "게임": "gaming",
        "패션": "fashion", "육아": "parenting", "결혼": "wedding",
        "우주": "space", "꽃": "flowers",
    }
    # _topic_to_keyword 와 같은 함정이 여기에도 있었다. 이쪽이 더 위험한데,
    # 여기서 뽑힌 단어는 '모든 씬 키워드 앞에 강제로 붙기' 때문이다.
    # 실측: '중소 제조기업의 스마트팩토리' → 'cow' 가 뽑혀 네 씬 전부
    # 'cow smart factory ...' 로 검색됐다. 긴 키 우선 + 한 글자는 낱말 경계.
    topic_lower = topic.lower()
    for ko, en in sorted(mapping.items(), key=lambda kv: -len(kv[0])):
        if _kw_hit(ko, topic_lower):
            return en
    return ""


def _generate_template_script(topic: str, duration_sec: int, tone: str) -> str:
    """Generate template-based Korean narration script when all LLM APIs fail.

    Target: duration_sec * 7.0 chars (measured TTS rate: ~7.0 chars/sec at -5% rate)
    """
    # Rich multi-paragraph base templates per tone
    templates = {
        "friendly": f"""안녕하세요! 오늘은 '{topic}'에 대해 함께 알아보겠습니다.
{topic}은 우리 일상에서 매우 중요한 역할을 하고 있습니다. 많은 사람들이 {topic}에 관심을 가지고 있지만, 정작 그 본질을 깊이 이해하는 경우는 드물죠.
오늘은 {topic}의 기본 개념부터 시작해서, 핵심 원리, 실제 사례, 그리고 앞으로의 전망까지 단계적으로 살펴보겠습니다.
먼저 {topic}이 왜 중요한지부터 짚어볼게요. {topic}은 단순히 이론적인 개념이 아니라, 우리의 실생활과 밀접하게 연결되어 있습니다. 일상에서 {topic}을 올바르게 이해하고 활용하면 삶의 질이 크게 향상될 수 있습니다.
다음으로 {topic}의 역사적 배경을 살펴보겠습니다. {topic}은 오랜 시간에 걸쳐 발전해 왔으며, 각 시대마다 새로운 의미와 가치를 부여받았습니다. 과거의 사례를 통해 현재를 이해하고, 미래를 준비할 수 있습니다.
{topic}의 핵심 특징을 정리하면, 첫째로 접근성이 뛰어나다는 점입니다. 누구나 쉽게 시작할 수 있고, 전문 지식 없이도 기본적인 이해가 가능합니다. 둘째로 응용 범위가 매우 넓습니다. 다양한 분야에서 {topic}의 원리를 적용할 수 있습니다. 셋째로 지속적인 발전이 이루어지고 있다는 점입니다.
실제 사례를 통해 {topic}을 이해해 봅시다. 전문가들은 {topic}을 활용하여 놀라운 성과를 거두고 있습니다. 이러한 성공 사례들은 우리에게 많은 영감을 줍니다.
{topic}을 처음 접하는 분들을 위한 조언도 드리겠습니다. 처음부터 완벽할 필요는 없습니다. 작은 것부터 차근차근 시작하는 것이 중요합니다. {topic}에 대한 기초 지식을 쌓고, 꾸준히 연습하다 보면 어느새 전문가 수준에 도달할 수 있습니다.
오늘 영상이 도움이 되셨다면 좋아요와 구독 부탁드립니다! 앞으로도 유익한 내용으로 찾아뵙겠습니다.""",
        "professional": f"""'{topic}'에 대한 심층 분석을 시작하겠습니다.
{topic}은 현대 사회에서 핵심적인 위치를 차지하고 있으며, 다양한 분야에 걸쳐 광범위한 영향을 미치고 있습니다.
전문가들의 연구에 따르면, {topic}의 핵심 구성 요소는 크게 세 가지로 분류할 수 있습니다. 첫째는 기반 이론, 둘째는 실용적 응용, 셋째는 지속 가능한 발전 방향입니다.
{topic}의 기본 원리를 분석하면, 체계적인 접근 방식의 중요성을 확인할 수 있습니다. 이론과 실제를 균형 있게 통합하는 것이 성공적인 {topic} 활용의 핵심입니다.
실제 사례 분석을 통해 {topic}이 어떻게 현실 세계에 적용되는지 살펴보겠습니다. 국내외 선진 사례들은 {topic}의 무한한 가능성을 보여줍니다.
{topic}과 관련된 최신 트렌드를 살펴보면, 디지털화와 글로벌화의 흐름 속에서 {topic}의 중요성이 더욱 부각되고 있음을 알 수 있습니다. 이러한 변화에 능동적으로 대응하는 것이 필요합니다.
향후 {topic}의 발전 방향은 더욱 혁신적이 될 것으로 전망됩니다. 기술 발전과 사회적 요구의 변화에 따라 {topic}은 새로운 패러다임으로 진화할 것입니다.
결론적으로, {topic}에 대한 이해와 전략적 활용은 현대 사회에서 경쟁력을 확보하는 데 필수적입니다. 지속적인 학습과 실천을 통해 {topic}의 역량을 강화하시기 바랍니다.""",
        "neutral": f"""오늘은 '{topic}'에 대해 알아보겠습니다.
{topic}은 다양한 측면에서 살펴볼 수 있는 흥미로운 주제입니다. 오늘 영상에서는 기초부터 심화 내용까지 체계적으로 설명해 드리겠습니다.
첫 번째로 {topic}의 기본 개념을 살펴보겠습니다. {topic}이란 무엇인지, 왜 중요한지, 어떤 상황에서 활용되는지에 대해 명확하게 정의해 보겠습니다.
두 번째로 {topic}의 주요 특징과 구성 요소를 분석하겠습니다. 각 요소가 어떤 역할을 하는지, 서로 어떻게 연결되어 있는지 파악하면 전체적인 이해가 훨씬 쉬워집니다.
세 번째로 {topic}의 실제 적용 사례를 살펴보겠습니다. 이론과 실제의 연결 고리를 이해하는 것이 실용적인 지식 습득에 매우 중요합니다.
네 번째로 {topic}을 효과적으로 활용하기 위한 실용적인 팁을 공유하겠습니다. 누구나 쉽게 따라할 수 있는 방법론과 단계별 접근법을 소개합니다.
다섯 번째로 {topic}에서 자주 발생하는 오해와 실수를 짚어보겠습니다. 이를 미리 알고 대비하면 시행착오를 크게 줄일 수 있습니다.
마지막으로 {topic}의 미래 전망에 대해 논의하겠습니다. 앞으로 {topic}이 어떻게 발전하고, 우리 삶에 어떤 영향을 미칠지 전문가 의견을 바탕으로 전망해 보겠습니다.
오늘 영상이 {topic}을 이해하는 데 도움이 되셨길 바랍니다.""",
    }
    base = templates.get(tone, templates["neutral"])
    # Expand to match duration: 7.0 chars/sec (measured TTS rate on ko-KR-SunHiNeural at -5%)
    target = int(duration_sec * 5.6)
    expansion_phrases = [
        f"{topic}은 끊임없이 변화하고 발전하는 분야입니다. 새로운 연구와 사례가 축적될수록 {topic}에 대한 이해가 더욱 깊어지고 있습니다.",
        f"실제로 {topic}을 경험한 많은 사람들은 처음에는 어렵게 느껴지지만, 기초를 충분히 익히고 나면 훨씬 수월해진다고 말합니다.",
        f"{topic}과 관련된 최신 연구 결과에 따르면, 꾸준한 실천과 반복 학습이 가장 효과적인 방법으로 꼽히고 있습니다.",
        f"전문가들은 {topic}의 핵심을 이해하기 위해 다각도에서 접근하는 것을 권장합니다. 단순히 표면적인 지식에 그치지 않고 깊이 있는 탐구가 필요합니다.",
        f"{topic}을 더 잘 이해하기 위해서는 관련 분야에 대한 폭넓은 지식도 함께 쌓는 것이 좋습니다. 다양한 관점에서 바라볼 때 더 풍부한 인사이트를 얻을 수 있습니다.",
        f"많은 이들이 {topic}에 대해 궁금해하는 질문 중 하나는 어디서부터 시작해야 하는가입니다. 답은 간단합니다. 바로 지금, 여기서부터 시작하면 됩니다.",
    ]
    idx = 0
    while len(base) < target:
        base += f"\n{expansion_phrases[idx % len(expansion_phrases)]}"
        idx += 1
    return base


def _split_script_locally(
    script: str,
    topic: str,
    n_scenes: int,
    duration_sec: int,
) -> List[Scene]:
    """Split script into scenes by character count (not sentence count).

    Char-count split ensures each scene gets ~equal narration length,
    which maps correctly to TTS audio duration.
    """
    # Ensure script is long enough: target 7.0 chars/sec
    target_chars = int(duration_sec * 5.6)
    if len(script) < target_chars * 0.5:
        template = _generate_template_script(topic, duration_sec, "neutral")
        script = template

    # Split by characters: divide total chars into n_scenes equal chunks
    total = len(script)
    chunk_size = max(1, total // n_scenes)
    scene_texts: List[str] = []

    for i in range(n_scenes):
        start = i * chunk_size
        end = start + chunk_size if i < n_scenes - 1 else total
        chunk = script[start:end].strip()
        # Avoid cutting mid-word: extend to next space if possible
        if end < total and not script[end - 1].isspace():
            next_space = script.find(' ', end)
            if 0 < next_space < end + 50:
                chunk = script[start:next_space].strip()
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
            duration_seconds=max(round(len(scene_texts[i]) / 5.56, 1), 3.0),
        )
        for i in range(n_scenes)
    ]
