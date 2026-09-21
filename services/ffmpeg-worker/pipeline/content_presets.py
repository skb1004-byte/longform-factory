# -*- coding: utf-8 -*-
"""콘텐츠 카테고리 프리셋 — 같은 파이프라인으로 다른 장르를 만들기 위한 설정.

기존에는 tone 이 프롬프트에 "톤: neutral" 이라는 문자열로 꽂히기만 했다.
그래서 교육 영상이든 브이로그든 여행 영상이든 결과물의 말투·호흡·화면 전환이
전부 같은 다큐 톤으로 나왔다. 카테고리마다 실제로 달라야 하는 것은 네 가지다.

  structure  나레이션의 뼈대 (도입을 어떻게 열고 무엇으로 닫는지)
  voice      말투와 문장 길이
  pace       초당 글자수. 같은 30초라도 쇼츠는 빽빽하고 교육은 여유 있다.
  style      기본 비주얼 스타일 (사용자가 따로 고르면 그쪽이 우선)

pace 는 목표 글자수 계산에 직접 쓰이므로 영상 길이에 그대로 반영된다.
"""
from __future__ import annotations
from typing import Dict, Any, Optional

# ko-KR-SunHiNeural(-5%) 실측 평균. 예전 상수 5.56 은 6% 과속이라
# 30초 요청이 34초로 나왔다(실측 176자 / 33.46초 = 5.26).
TTS_CHARS_PER_SEC_DEFAULT = 5.26

CONTENT_PRESETS: Dict[str, Dict[str, Any]] = {
    "education": {
        "label": "교육·강의",
        "style": "infographic",
        "pace": 4.9,          # 이해할 시간을 준다. 가장 여유 있는 호흡.
        "bgm_volume": 0.04,
        "max_pause": 1.0,
        "scene_sec": (6.0, 10.0),
        "structure": (
            "- 도입: 학습자가 흔히 착각하는 지점이나 실패하는 순간을 먼저 제시.\n"
            "- 전개: 개념 → 왜 그런지 → 구체적 예시 순으로 한 번에 하나씩. "
            "앞 문장을 이해해야 다음 문장이 성립하도록 계단식으로 쌓으세요.\n"
            "- 마무리: 오늘 기억할 한 가지를 한 문장으로."
        ),
        "voice": (
            "차분하고 또박또박. 전문용어를 쓰면 반드시 바로 뒤에 쉬운 말로 풀어주세요. "
            "'~입니다', '~합니다' 체를 기본으로 하되 딱딱하지 않게."
        ),
    },
    "vlog": {
        "label": "브이로그",
        "style": "cinematic",
        "pace": 5.5,
        "bgm_volume": 0.10,
        "max_pause": 0.55,
        "scene_sec": (3.5, 6.0),
        "structure": (
            "- 도입: 그날의 한 장면을 현재형으로 툭 던지며 시작.\n"
            "- 전개: 시간 순으로 흘러가되, 느낀 점을 짧게 끼워 넣으세요. "
            "정보 전달이 아니라 '같이 있는 느낌'이 목적입니다.\n"
            "- 마무리: 담백한 한 마디. 교훈으로 정리하지 마세요."
        ),
        "voice": (
            "혼잣말에 가까운 편한 구어체. '~했어요', '~더라고요', '~거든요'. "
            "문장을 짧게 끊고, 완벽한 문장이 아니어도 괜찮습니다."
        ),
    },
    "documentary": {
        "label": "뉴스·다큐",
        "style": "news",
        "pace": 5.2,
        "bgm_volume": 0.05,
        "max_pause": 0.8,
        "scene_sec": (5.0, 9.0),
        "structure": (
            "- 도입: 확인된 사실이나 수치 하나로 시작. 추측으로 열지 마세요.\n"
            "- 전개: 무엇이 일어났는지 → 왜 그런지 → 그래서 무엇이 달라지는지. "
            "출처가 있는 사실만 쓰고, 근거가 없으면 단정하지 말고 빼세요.\n"
            "- 마무리: 해석을 덧붙이지 말고 사실로 닫으세요."
        ),
        "voice": (
            "절제된 서술체. 감탄사와 과장 형용사 금지. 숫자·시점·장소를 명시하세요."
        ),
    },
    "shorts": {
        "label": "쇼츠·릴스",
        "style": "cinematic",
        "pace": 6.0,          # 가장 빠르다. 첫 2초에 붙잡지 못하면 이탈한다.
        "bgm_volume": 0.12,
        "max_pause": 0.5,
        "scene_sec": (2.5, 4.5),
        "structure": (
            "- 첫 문장에서 결론이나 가장 놀라운 사실을 먼저 던지세요. 뜸 들이기 금지.\n"
            "- 전개: 한 문장 = 한 정보. 군더더기를 모두 쳐내세요.\n"
            "- 마무리: 여운이나 반전 한 줄로 끝. 요약하지 마세요."
        ),
        "voice": (
            "짧고 강한 구어체. 한 문장 25자 이내를 지키세요. 접속사를 쓰지 말고 붙이세요."
        ),
    },
    "corporate": {
        "label": "기업·홍보",
        "style": "minimal",
        "pace": 5.1,
        "bgm_volume": 0.06,
        "max_pause": 0.7,
        "scene_sec": (4.0, 7.0),
        "structure": (
            "- 도입: 고객이 겪는 문제를 먼저. 회사 소개로 시작하지 마세요.\n"
            "- 전개: 문제 → 해결 방식 → 그것이 만든 구체적 결과. "
            "검증되지 않은 수치나 '업계 최고' 같은 표현은 쓰지 마세요.\n"
            "- 마무리: 다음 행동 하나를 분명하게."
        ),
        "voice": (
            "신뢰감 있는 존댓말. 과장 없이 단정하게. 자화자찬 대신 사실로 말하세요."
        ),
    },
    "travel": {
        "label": "여행·라이프",
        "style": "cinematic",
        "pace": 5.3,
        "bgm_volume": 0.09,
        "max_pause": 0.8,
        "scene_sec": (4.0, 7.0),
        "structure": (
            "- 도입: 그 장소에 도착한 순간의 감각(소리·냄새·온도)으로 시작.\n"
            "- 전개: 장면을 그리듯 묘사하되, 실제로 쓸모 있는 정보"
            "(가는 법, 시간대, 비용 감각)를 자연스럽게 섞으세요.\n"
            "- 마무리: 다시 가고 싶은 이유 한 줄."
        ),
        "voice": (
            "따뜻하고 여유 있는 구어체. 감각을 묘사하는 동사를 쓰세요. "
            "'아름답다', '멋지다' 같은 뭉뚱그린 형용사 대신 구체적으로."
        ),
    },
}

# 사용자가 한글로 넣거나 기존 tone 값을 넣어도 받아준다.
_ALIASES = {
    # 대시보드가 실제로 보내는 키 (GENRE_PRESETS 의 키가 그대로 tone 으로 온다).
    # 이걸 빠뜨려서 UI 에서 장르를 골라도 프리셋이 발동하지 않았다.
    "edu": "education", "vlog": "vlog", "news": "documentary",
    "short": "shorts", "corp": "corporate", "travel": "travel",
    "professional_documentary": "documentary",
    "교육": "education", "강의": "education", "교육·강의": "education",
    "educational": "education", "lecture": "education",
    "브이로그": "vlog", "일상": "vlog", "vlogging": "vlog",
    "뉴스": "documentary", "다큐": "documentary", "뉴스·다큐": "documentary",
    "news": "documentary", "doc": "documentary", "neutral": "documentary",
    "쇼츠": "shorts", "릴스": "shorts", "쇼츠·릴스": "shorts", "reels": "shorts",
    "기업": "corporate", "홍보": "corporate", "기업·홍보": "corporate",
    "business": "corporate", "promo": "corporate",
    "여행": "travel", "라이프": "travel", "여행·라이프": "travel",
    "lifestyle": "travel",
}


def resolve_content(tone: str) -> Optional[str]:
    """tone 문자열을 카테고리 키로 정규화. 모르는 값이면 None."""
    if not tone:
        return None
    k = tone.strip().lower()
    if k in CONTENT_PRESETS:
        return k
    return _ALIASES.get(k) or _ALIASES.get(tone.strip())


def _get_content_raw(tone: str) -> Optional[Dict[str, Any]]:
    # [2026-09-20] 호출부가 tone('차분한','친근한')을 넘기는 곳이 여럿이라
    # 콘텐츠 프리셋 조회가 늘 실패했다. 그 결과 BGM 볼륨도, 목표 글자수도
    # 프리셋 값이 아니라 기본값이 쓰였다(실측: 30초 요청 → 24.7초, -18%).
    # 잡 시작 때 set_content_type() 으로 실제 콘텐츠 종류를 넣어두고,
    # tone 으로 못 찾으면 그걸로 되돌아간다.
    key = resolve_content(tone)
    return CONTENT_PRESETS.get(key) if key else None


def chars_per_sec(tone: str) -> float:
    """목표 글자수 계산에 쓰는 초당 글자수.

    카테고리와 무관하게 TTS 실측 속도를 쓴다. 한때 preset 의 pace 를 그대로
    곱했는데, 그건 '글자를 얼마나 빽빽하게 쓸까'라는 편집 의도였지 음성이
    빨라진다는 뜻이 아니었다.

    [2026-09-20] 5.26 은 '쉼 압축 이전'의 속도였다. 파이프라인이 Step 2.5 에서
    문장 사이 쉼을 max_pause 까지 줄이므로, 완성본은 그만큼 더 짧아진다.
    쉼을 많이 줄이는 프리셋일수록 같은 글자수가 더 짧은 영상이 된다.

      실측  155자 / 24.67초 = 6.28자/초  (shorts, max_pause=0.28)
            114자 / 17.37초 = 6.56자/초  (shorts, max_pause=0.28)

    5.26 을 그대로 쓰면 30초 요청에 24.7초가 나왔다(-18%).
    max_pause 로 보정한다: 쉼을 거의 안 줄이면 5.26 에 가깝고,
    많이 줄이면 6.3 쪽으로 올라간다.
    """
    base = TTS_CHARS_PER_SEC_DEFAULT          # 쉼 압축 없을 때
    c = get_content(tone)
    if not c:
        return base
    mp = float(c.get("max_pause") or 1.0)
    # mp=1.0 → 5.26(그대로), mp=0.28 → 6.3
    # 기울기는 실측으로 잡는다.
    #   176자 / 24.5초 = 7.18자/초  (max_pause=0.28 일 때)
    #   기준점 5.26자/초            (쉼을 거의 안 줄이는 max_pause=1.0)
    #   → (7.18-5.26)/(1.0-0.28) = 2.67
    # 1.45 를 쓰던 때는 30초 요청에 24.5초가 나왔다(-18%).
    adj = base + (1.0 - min(mp, 1.0)) * 2.67
    return round(min(max(adj, base), 6.6), 2)


def density_hint(tone: str) -> str:
    """장르별 정보 밀도를 프롬프트 문장으로 돌려준다(길이가 아니라 문체에 반영)."""
    c = get_content(tone)
    if not c:
        return ""
    pace = float(c["pace"])
    if pace >= 5.8:
        # '빽빽하게 눌러 담으라'고만 했더니 모델이 통계 목록을 뱉었다.
        # 실측: "2013년 도쿄 12만 명! 2021년 1억 4천만 시간! ..." 처럼 느낌표
        # 다섯 개짜리 숫자 나열이 나왔고, 근거가 없어 수치는 전부 지어낸 것이었다.
        # 밀도는 '군더더기 제거'이지 '숫자 나열'이 아니라는 걸 못박는다.
        return ("군더더기를 걷어내되 숫자를 나열하지 마세요. "
                "한 문장이 다음 문장을 부르도록 하나의 흐름으로 이어가고, "
                "확인되지 않은 수치·연도·인명은 아예 쓰지 마세요. "
                "느낌표는 전체에서 한 번 이하.")
    if pace <= 5.0:
        return "서두르지 말고 여백을 두세요. 한 번에 하나씩, 이해할 시간을 주는 속도로."
    return "적당한 밀도로. 정보와 호흡을 반반씩."


def max_pause(tone: str) -> float:
    """문장 사이 멈춤 상한(초).

    Edge TTS 는 문장마다 1.4~1.6초를 쉰다. 쇼츠에서는 그 여백이 '멈춘 화면'으로
    느껴지고, 교육 영상에서는 이해할 틈이 된다. 장르마다 다르게 잘라낸다.
    0 이면 손대지 않는다.
    """
    c = get_content(tone)
    return float(c.get("max_pause", 0.8)) if c else 0.8


def default_style(tone: str) -> str:
    """사용자가 이미지 스타일을 고르지 않았을 때 쓸 기본값."""
    c = get_content(tone)
    return str(c["style"]) if c else ""

_JOB_CONTENT_TYPE = ""


def set_content_type(content_type: str) -> None:
    """이 잡의 콘텐츠 종류(shorts/longform/edu/news...). 프리셋 조회의 기준."""
    global _JOB_CONTENT_TYPE
    _JOB_CONTENT_TYPE = (content_type or "").strip()


def get_content_type() -> str:
    return _JOB_CONTENT_TYPE


def get_content(tone: str = ""):
    """프리셋 조회. tone 으로 못 찾으면 잡 콘텐츠 종류로 되돌아간다."""
    c = _get_content_raw(tone)
    if c:
        return c
    if _JOB_CONTENT_TYPE:
        return _get_content_raw(_JOB_CONTENT_TYPE)
    return None
