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
    for key, hint in _CONTEXT_MAP.items():
        if len(key) == 1:
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

    return f"{prefix}, {subject}" if prefix else subject


def build_cartoon_prompt(scene) -> str:
    """Backward-compatible alias for build_prompt(scene, 'cartoon')."""
    return build_prompt(scene, "cartoon")
