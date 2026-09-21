# -*- coding: utf-8 -*-
"""Scene-to-image prompt builder: keyword/narration → FLUX-optimized English prompt.

Supports Korean food, ingredients, and general topic keywords via _CONTEXT_MAP.
Split from ai_image.py to keep each file under 300 lines.
"""
from __future__ import annotations
import re
from pipeline.style_presets import get_preset

# Scene context enrichment: keyword/narration fragment → descriptive subject hint
# Supports both English keywords and Korean narration terms
_CONTEXT_MAP: dict[str, str] = {
    # --- English topic keywords ---
    "labor":      "workers in orange uniforms facing authority figures in suits",
    "union":      "union workers protesting, raised fists, solidarity banner",
    "economy":    "economic charts, coins, currency symbols, business setting",
    "finance":    "stock market graphs, financial district, money flow",
    "politics":   "government building, officials at podium, policy documents",
    "law":        "courtroom, gavel, legal books, justice scales",
    "technology": "futuristic devices, glowing screens, innovation lab",
    "business":   "modern office meeting room, executives, corporate environment",
    "people":     "diverse group of people, community gathering, crowd",
    "nature":     "beautiful landscape, lush green scenery, outdoors",
    "city":       "urban cityscape, skyscrapers, busy street scene",
    "success":    "trophy, celebration, triumphant characters, achievement",
    "crisis":     "character looking worried, storm clouds, tense atmosphere",
    "solution":   "lightbulb moment, problem solved, happy characters",
    "health":     "medical setting, doctor, healthcare, wellness",
    "food":       "delicious meal, restaurant, kitchen, cooking",
    "education":  "classroom, students learning, books, school building",
    "travel":     "airplane, passport, world map, adventure",
    "family":     "happy family, home interior, warmth, togetherness",
    "sport":      "athletes competing, stadium, victory, training",
    "art":        "artist painting, gallery, colorful canvas, creative studio",
    "music":      "musician playing, concert stage, musical notes, instruments",
    "science":    "laboratory, researcher, microscope, scientific equipment",
    "history":    "ancient artifacts, historical monument, vintage setting",
    "culture":    "traditional ceremony, cultural festival, heritage items",
    # --- Korean food & ingredients ---
    "간장":       "soy sauce bottle, dark brown liquid, Korean condiment jar, fermented sauce",
    "된장":       "doenjang miso paste, fermented soybean, traditional Korean jar",
    "고추장":     "gochujang red pepper paste jar, spicy Korean condiment",
    "고추":       "Korean red chili pepper, spicy ingredient, vibrant red color",
    "마늘":       "garlic cloves, Korean cooking ingredient, kitchen counter",
    "생강":       "ginger root, Korean spice, fresh ingredient on cutting board",
    "대파":       "green onion, scallion, Korean vegetable, fresh herb",
    "양파":       "onion sliced, Korean cooking, fresh vegetable",
    "배추":       "napa cabbage, Korean vegetable, leafy greens",
    "단무지":     "daikon radish, Korean white radish, pickled vegetable",
    "무채":       "shredded radish, Korean side dish, white vegetable",
    "김치":       "kimchi jar, fermented cabbage, Korean traditional food, red color",
    "나물":       "Korean namul seasoned greens, vegetable side dish, bowl",
    "비빔밥":     "bibimbap Korean rice bowl, colorful toppings, stone pot",
    "불고기":     "bulgogi grilled marinated beef, Korean BBQ, sizzling grill",
    "삼겹살":     "samgyeopsal pork belly slices, Korean BBQ grill, smoke",
    "갈비":       "Korean galbi ribs, grilled meat, BBQ restaurant",
    "국밥":       "Korean gukbap soup with rice, steaming bowl, comfort food",
    "된장찌개":   "doenjang jjigae soybean paste stew, Korean tofu soup, clay pot",
    "김치찌개":   "kimchi jjigae stew, bubbling red soup, Korean comfort food",
    "순두부":     "sundubu soft tofu, silky texture, Korean soup bowl",
    "삼계탕":     "samgyetang ginseng chicken soup, medicinal Korean dish",
    "떡":         "rice cake tteok, Korean traditional dessert, colorful",
    "떡볶이":     "tteokbokki spicy rice cakes, street food, red sauce",
    "순대":       "sundae Korean blood sausage, street food, steam",
    "잡채":       "japchae glass noodles, Korean stir fry, colorful vegetables",
    "냉면":       "naengmyeon cold noodles, chilled bowl, Korean summer dish",
    "육개장":     "yukgaejang spicy beef soup, Korean stew, hearty meal",
    "해장국":     "haejanguk hangover soup, Korean broth, morning meal",
    "보쌈":       "bossam pork wraps, Korean lettuce wrap, fermented kimchi",
    "족발":       "jokbal braised pork trotters, Korean dish, soy sauce glaze",
    "치킨":       "Korean fried chicken, crispy golden, sauce coating",
    "라면":       "ramen instant noodle, Korean spicy broth, boiling pot",
    "김밥":       "gimbap seaweed rice roll, Korean picnic food, colorful filling",
    "샌드위치":   "sandwich, bread, fresh ingredients, meal",
    "빵":         "bread loaf, Korean bakery, fresh baked goods",
    "케이크":     "cake slice, celebration, sweet dessert, colorful frosting",
    "커피":       "coffee cup, Korean cafe, aroma, latte art",
    "차":         "tea cup, Korean traditional tea, steam, warmth",
    "막걸리":     "makgeolli rice wine, white milky Korean drink, bowl cup",
    "소주":       "soju glass, Korean spirit, clear bottle, drinking",
    "맥주":       "beer glass, Korean pub, refreshing drink",
    "양념":       "Korean seasoning, spices, marinade, flavor blend",
    "반찬":       "Korean banchan side dishes, small bowls, table spread",
    "발효":       "fermentation jars, traditional Korean storage, clay pots",
    "식재료":     "Korean cooking ingredients, fresh produce, kitchen",
    "요리":       "Korean cooking, kitchen preparation, chef, ingredients",
    "음식":       "Korean food spread, delicious meal, table setting",
    "시장":       "Korean traditional market, street food stalls, bustling",
    "주방":       "kitchen, cooking space, Korean home meal prep",
    # --- Korean general topics ---
    "회사":       "office building, business meeting, corporate Korea",
    "직장":       "workplace, Korean office, employees working",
    "돈":         "money, Korean won bills, financial, economy",
    "집":         "Korean home interior, cozy house, family living",
    "학교":       "Korean school building, students, classroom learning",
    "병원":       "Korean hospital, doctor and patient, medical care",
    "여행":       "travel destination, Korean tourism, sightseeing",
    "자연":       "Korean nature scenery, mountains, lush green landscape",
    "역사":       "Korean historical site, traditional architecture, heritage",
    "문화":       "Korean cultural festival, hanbok, traditional ceremony",
    "음악":       "Korean music performance, K-pop stage, concert",
    "미술":       "Korean art gallery, artwork, creative painting, exhibition",
    "스포츠":     "Korean sports, athletes competing, stadium crowd",
    "건강":       "health wellness, Korean medical, exercise, fitness",
    "환경":       "Korean environment, nature conservation, green energy",
    "기술":       "Korean technology, high-tech devices, innovation",
    "경제":       "Korean economy, financial district Seoul, business charts",
    "사회":       "Korean society, community people, social gathering",
    "정치":       "Korean government building, policy, official meeting",
}


_ASCII_KEY_RE = re.compile(r"[a-z0-9 \-]+")

# 한국 소재인데 일본·중국풍으로 그려지는 문제를 막는 단서.
#
# 실측: 'hanbok' 을 그리게 했더니 옷깃 여밈과 소매가 기모노에 가까웠다.
# SDXL 은 동아시아 복식을 뭉뚱그리는 경향이 있어서, 무엇이 한국적인지
# 구체적으로 적고 무엇이 아닌지를 네거티브로 못박아야 한다.
_KOREA_TOKENS = (
    "korea", "korean", "kpop", "k-pop", "hanbok", "seoul", "jeju",
    "busan", "gimpo", "hangul", "kimchi", "bibimbap",
)
_KOREA_CUES = (
    "authentic Korean setting, Korean people, "
    "hangul signage, modern South Korea"
)
_HANBOK_CUES = (
    "authentic Korean hanbok with jeogori jacket and full chima skirt, "
    "high empire waistline, wide flowing skirt, goreum ribbon tie"
)
_KOREA_NEGATIVE = (
    "japanese kimono, obi sash, yukata, chinese hanfu, qipao, "
    "japanese signage, chinese characters, kanji, geisha"
)

# 낱말만으로는 화면에 무엇이 있어야 할지 모호한 피사체를 풀어 쓴다.
# 실측: 'kpop channel broadcast studio' 로 카메라와 조명만 있는 빈 스튜디오가
# 나왔다. 케이팝이라는 말이 화면에 아무것도 만들어내지 못한 것이다.
_SUBJECT_EXPANSIONS = {
    "kpop": "K-pop idol group performing on a bright concert stage with LED screens",
    "k-pop": "K-pop idol group performing on a bright concert stage with LED screens",
    "broadcast studio": "TV broadcast studio with cameras facing a lit performance stage",
    "smart factory": "modern automated factory floor with robotic arms and conveyor lines",
    "ev charging station": "electric car plugged into a charging station",
    "korean wave": "crowd of international fans enjoying Korean pop culture",

    # 한국 고유 대상은 이름만 줘서는 안 된다.
    #
    # SDXL 은 'dol hareubang' 을 모른다. 'jeju stone statue' 라고만 주면
    # 자기가 아는 석상 — 그리스·로마 토가 조각이나 서양인 수염 흉상 — 을
    # 그린다(실측: 12컷 중 5컷이 서양 조각상). 이름 대신 '무엇처럼 생겼는지'를
    # 적어야 한다. 생김새를 적으면 모델이 모르는 대상도 그릴 수 있다.
    # 생김새를 최대한 구체적으로 적는다.
    #
    # negative prompt 로 'beard, greek statue' 를 막으려 했더니 오히려 그리스
    # 대리석 조각이 늘었다(실측: 5컷). 확산모델에는 negative 가 그 개념을
    # 되레 끌어오는 현상(Inducing Effect)이 있고, SDXL 은 negative 에 덜
    # 반응한다. 아닌 것을 적는 대신 맞는 것을 빽빽하게 적는 쪽이 통한다.
    "hareubang": ("squat stout stone figure carved from grey porous volcanic "
                  "basalt, tall cylindrical mushroom-cap hat, large bulging "
                  "oval eyes, broad flat nose, small closed mouth, smooth "
                  "rounded bare cheeks, plain undecorated surface, both hands "
                  "resting flat on a round belly, simple primitive folk "
                  "carving, single figure standing upright outdoors, "
                  "Jeju Island Korea"),
    "dol hareubang": ("squat volcanic basalt grandfather statue, bulging round eyes, "
                      "wide flat nose, mushroom-shaped tall hat, hands on belly, "
                      "weathered grey porous rock, Jeju Island Korea"),
    "hanok": ("traditional Korean wooden house with curved tiled roof, "
              "paper sliding doors, wooden pillars, stone courtyard"),
    "jangseung": ("tall carved wooden Korean village guardian pole with a "
                  "grimacing painted face, standing at a village entrance"),
    "seokguram": ("stone Buddha seated inside a domed granite grotto, Korea"),
    "haenyeo": ("Korean woman free diver in a black wetsuit surfacing beside "
                "an orange float buoy, rocky volcanic coast, no scuba tank"),
    "kimchi": ("napa cabbage coated in red chili paste, traditional Korean "
               "earthenware jars, hands mixing in a wide bowl"),
    "onggi": "large dark brown Korean earthenware fermentation jars in a row outdoors",
    "hanbok": ("Korean traditional dress with high-waisted long skirt and "
               "short jacket with curved sleeves"),
}


# 한국어 주제어 → _SUBJECT_EXPANSIONS 키.
#
# 씬 키워드는 LLM 이 영어로 만드는데, 고유명사를 로마자로 적어줄 때도 있고
# ('jeju dol hareubang carving') 일반어로 풀어버릴 때도 있다
# ('jeju stone statues street'). 후자면 생김새 묘사가 붙지 않아 SDXL 이
# 그리스 조각상을 그린다. 그래서 한국어 주제에서도 직접 잡는다.
_KO_SUBJECT_KEYS = {
    "돌하르방": "hareubang",
    "하르방": "hareubang",
    "한옥": "hanok",
    "장승": "jangseung",
    "석굴암": "seokguram",
    "해녀": "haenyeo",
    "김치": "kimchi",
    "옹기": "onggi",
    "항아리": "onggi",
    "한복": "hanbok",
}

_TOPIC_HINT = ""


def set_topic_hint(korean_topic: str) -> None:
    """이 잡의 한국어 주제. 영어 키워드가 고유명사를 흘렸을 때 대신 쓴다."""
    global _TOPIC_HINT
    _TOPIC_HINT = (korean_topic or "").strip()


# 한국어 주제에 나오는 지명. 스톡 커버리지 판정의 핵심어로 쓴다.
_KO_PLACE_KEYS = {
    "제주": "jeju", "서울": "seoul", "부산": "busan", "경주": "gyeongju",
    "전주": "jeonju", "강원": "gangwon", "안동": "andong", "속초": "sokcho",
    "한라산": "hallasan", "설악산": "seoraksan", "남산": "namsan",
}


def topic_terms_from_korean(korean_topic: str) -> list:
    """한국어 주제에서 영어 검색 기준어를 직접 뽑는다.

    기준어를 씬 키워드에서만 모으면, LLM 이 고유명사를 빼는 순간 주제가
    사라진다. 실측: '제주도 돌하르방의 유래' 인데 기준어가 ['statue','stone']
    이 됐고, 둘 다 스톡에 흔해 커버리지 8개로 판정되어 스톡에 머물렀다.
    그 결과 돌하르방 대신 아무 석상이 나왔다.

    주제 문자열에서 직접 뽑으면 LLM 이 무엇을 쓰든 흔들리지 않는다.
    """
    t = korean_topic or ""
    out = []
    for ko, en in _KO_SUBJECT_KEYS.items():
        if ko in t and en not in out:
            out.append(en)
    for ko, en in _KO_PLACE_KEYS.items():
        if ko in t and en not in out:
            out.append(en)
    return out


def _topic_expansion() -> str:
    """한국어 주제에 고유 대상이 있으면 그 생김새 묘사를 돌려준다."""
    if not _TOPIC_HINT:
        return ""
    for ko, key in _KO_SUBJECT_KEYS.items():
        if ko in _TOPIC_HINT:
            return _SUBJECT_EXPANSIONS.get(key, "")
    return ""


def _expand_subject(subject: str) -> str:
    """모호한 낱말을 '화면에 보일 것'으로 풀어 쓴다(낱말 경계 일치)."""
    out = subject
    low = subject.lower()
    for key, expansion in sorted(_SUBJECT_EXPANSIONS.items(), key=lambda kv: -len(kv[0])):
        pattern = r"(?<![a-z0-9])" + re.escape(key) + r"(?![a-z0-9])"
        if re.search(pattern, low):
            return f"{out}, {expansion}"
    # 주제 묘사는 폴백이 아니라 '항상' 붙인다.
    #
    # 30초 단일 주제 영상인데 씬 나레이션이 '사람들'을 말하면 키워드가
    # 그쪽으로 새고, 돌하르방 영상에 한복 입은 여성이 4컷 들어갔다(실측).
    # 씬마다 주제 피사체를 다시 못박아야 영상 전체가 한 주제로 묶인다.
    topic_exp = _topic_expansion()
    if topic_exp:
        return f"{out}, {topic_exp}"
    return out


def _korea_extras(text: str) -> tuple[str, str]:
    """한국 소재면 (추가 프롬프트, 추가 네거티브) 를 돌려준다."""
    low = (text or "").lower()
    hit = any(re.search(r"(?<![a-z0-9])" + re.escape(k) + r"(?![a-z0-9])", low)
              for k in _KOREA_TOKENS)
    if not hit:
        return "", ""
    extra = _KOREA_CUES
    if "hanbok" in low or "traditional" in low:
        extra = f"{extra}, {_HANBOK_CUES}"
    return extra, _KOREA_NEGATIVE


def scan_context_hints(text: str) -> list[str]:
    """Scan text for all matching context hints (Korean and English).

    Short keys (1-2 chars) require word-boundary match to avoid false positives.
    Long keys (3+ chars) use substring match.
    Returns a list of unique hints (up to 2) for richer prompts.
    """
    if not text:
        return []
    text_lower = text.lower()
    found: list[str] = []
    seen: set[str] = set()
    # 긴 표제어부터 본다. 'smart factory' 가 'factory' 보다 먼저 걸리도록.
    for key, hint in sorted(_CONTEXT_MAP.items(), key=lambda kv: -len(kv[0])):
        if _ASCII_KEY_RE.fullmatch(key) or len(key) == 1:
            # 영어 키와 한 글자 한글 키는 '낱말'로 등장할 때만 인정한다.
            #
            # 예전에는 3글자 이상이면 부분일치였다. 그래서 'smart tv' 안의 'art' 가
            # 걸려 프롬프트에 "artist painting, gallery, colorful canvas" 가 붙었고,
            # 케이팝 방송 장면 대신 풍경화가 걸린 갤러리가 그려졌다(실측).
            # 'car'→scarf, 'age'→village, 'eat'→theater 도 같은 방식으로 터진다.
            pattern = r'(?<![가-힣a-zA-Z])' + re.escape(key) + r'(?![가-힣a-zA-Z])'
            matched = bool(re.search(pattern, text_lower))
        else:
            matched = key in text_lower
        if matched and hint not in seen:
            found.append(hint)
            seen.add(hint)
            if len(found) >= 2:
                break
    return found


# 모든 AI 이미지에 공통으로 붙는 구도 지시.
_COMPOSITION = (
    "a single moment in one continuous scene, "
    "one clear main subject filling most of the frame, "
    "vertical 9:16 composition, natural depth of field"
)


def build_prompt(scene, style: str = "cartoon") -> str:
    """Build AI image prompt for the given style preset.

    Strategy: keyword → _CONTEXT_MAP enrichment → style prefix → final prompt.
    Falls back to narration excerpt when no keyword or map match found.
    """
    preset = get_preset(style)
    prefix = preset.get("prompt_prefix", "")

    keyword = (scene.keyword or "").strip().lower()
    narration = (scene.narration or "").strip()

    keyword_hints = scan_context_hints(keyword)
    narration_hints = scan_context_hints(narration)

    all_hints: list[str] = []
    seen_hints: set[str] = set()
    for h in keyword_hints + narration_hints:
        if h not in seen_hints:
            all_hints.append(h)
            seen_hints.add(h)

    if keyword:
        parts = [keyword] + all_hints[:2]
        subject = ", ".join(parts)
    elif narration:
        if all_hints:
            subject = ", ".join(all_hints[:2])
        else:
            subject = narration[:60].strip()
    else:
        subject = "abstract concept illustration"

    # 키워드만 던지면 SDXL 이 낱말을 문자 그대로 배치한다.
    # 실측: 'smart tv kpop broadcast' → 케이팝이 아니라 풍경 사진이 걸린
    # TV 여러 대(갤러리 벽)를 그렸다. '한 장면, 주 피사체 하나'라는 구도를
    # 명시해야 낱말 나열이 아니라 장면이 나온다.
    subject = _expand_subject(subject)
    korea_extra, _ = _korea_extras(f"{keyword} {narration}")
    parts = [subject]
    if korea_extra:
        parts.append(korea_extra)
    parts.append(_COMPOSITION)
    body = ", ".join(parts)
    return f"{prefix}, {body}" if prefix else body


def build_negative(scene, style: str = "cartoon") -> str:
    """스타일 기본 네거티브에 장면별 추가 금지어를 얹는다."""
    base = get_preset(style).get("negative_prompt", "")
    _, extra = _korea_extras(f"{scene.keyword or ''} {scene.narration or ''}")
    return f"{base}, {extra}" if extra else base


def build_cartoon_prompt(scene) -> str:
    """Backward-compatible alias for build_prompt(scene, 'cartoon')."""
    return build_prompt(scene, "cartoon")
