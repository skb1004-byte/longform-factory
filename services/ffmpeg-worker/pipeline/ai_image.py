# -*- coding: utf-8 -*-
"""AI image generation: WaveSpeed FLUX (primary) + DALL-E 3 (fallback)."""
from __future__ import annotations
import asyncio
import logging
import subprocess
from pathlib import Path

import httpx

from config import WAVESPEED_API_KEY, OPENAI_API_KEY
from pipeline.style_presets import get_preset

logger = logging.getLogger(__name__)

WAVESPEED_URL = "https://api.wavespeed.ai/api/v2/wavespeed-ai/flux-dev"
DALLE_URL = "https://api.openai.com/v1/images/generations"

_DEFAULT_NEGATIVE = (
    "realistic photo, 3d render, watermark, text, blurry, dark, "
    "violent, nsfw, low quality, deformed"
)

# Scene context enrichment: keyword/narration fragment → descriptive subject hint
# Supports both English keywords and Korean narration terms
_CONTEXT_MAP = {
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


def _scan_context_hints(text: str) -> list[str]:
    """Scan text for all matching context hints (Korean and English).

    Short keys (1-2 chars) require word-boundary match to avoid false positives
    (e.g. '무' matching inside '무대', '파' matching inside '파악').
    Long keys (3+ chars) use substring match.

    Returns a list of unique hints (up to 2) for richer prompts.
    """
    if not text:
        return []
    import re
    text_lower = text.lower()
    found: list[str] = []
    seen: set[str] = set()
    for key, hint in _CONTEXT_MAP.items():
        if len(key) == 1:
            # Single char: require word boundary (prevents false positives like 무→무대)
            pattern = r'(?<![가-힣a-zA-Z])' + re.escape(key) + r'(?![가-힣a-zA-Z])'
            matched = bool(re.search(pattern, text_lower))
        else:
            # 2+ chars: simple substring match (Korean 2-char words are specific enough)
            matched = key in text_lower
        if matched and hint not in seen:
            found.append(hint)
            seen.add(hint)
            if len(found) >= 2:
                break
    return found


def build_prompt(scene, style: str = "cartoon") -> str:
    """Build AI image prompt for the given style preset.

    Strategy (v17.7.4 — narration-visual matching):
    1. Check keyword against _CONTEXT_MAP for English enrichment
    2. Check narration against _CONTEXT_MAP for Korean ingredient/topic hints
    3. Combine keyword + narration hints for a specific, accurate prompt
    4. Fallback to narration[:60] when keyword is empty
    """
    preset = get_preset(style)
    prefix = preset.get("prompt_prefix", "")

    keyword = (scene.keyword or "").strip().lower()
    narration = (scene.narration or "").strip()

    # Collect hints from keyword (English) and narration (Korean/English)
    keyword_hints = _scan_context_hints(keyword)
    narration_hints = _scan_context_hints(narration)

    # Merge: keyword hints first, then unique narration hints
    all_hints: list[str] = []
    seen_hints: set[str] = set()
    for h in keyword_hints + narration_hints:
        if h not in seen_hints:
            all_hints.append(h)
            seen_hints.add(h)

    if keyword:
        # Build subject from keyword + all enrichment hints
        parts = [keyword] + all_hints[:2]
        subject = ", ".join(parts)
    elif narration:
        # No keyword: prefer English hints over raw Korean narration text
        if all_hints:
            # Use English hints as primary subject (FLUX handles English better)
            subject = ", ".join(all_hints[:2])
        else:
            # No hints found: use narration excerpt (may contain Korean, FLUX best-effort)
            subject = narration[:60].strip()
    else:
        subject = "abstract concept illustration"

    return f"{prefix}, {subject}" if prefix else subject


def build_cartoon_prompt(scene) -> str:
    """Backward-compatible alias for build_prompt(scene, 'cartoon')."""
    return build_prompt(scene, "cartoon")


async def generate_ai_image_wavespeed(
    prompt: str,
    output_path: Path,
    size: str = "768x1344",
    negative_prompt: str = "",
) -> bool:
    """Generate image via WaveSpeed FLUX. Returns True on success."""
    if not WAVESPEED_API_KEY:
        logger.warning("[ai_image] WAVESPEED_API_KEY missing")
        return False

    headers = {
        "Authorization": f"Bearer {WAVESPEED_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "prompt": prompt,
        "negative_prompt": negative_prompt or _DEFAULT_NEGATIVE,
        "size": size,
        "num_inference_steps": 28,
        "guidance_scale": 3.5,
        "num_outputs": 1,
    }

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(WAVESPEED_URL, headers=headers, json=payload)
            resp.raise_for_status()
            data = resp.json()

        task_data = data.get("data", {})
        poll_url = (
            task_data.get("urls", {}).get("get")
            or f"https://api.wavespeed.ai/api/v2/predictions/{task_data.get('id', '')}/result"
        )
        if not task_data.get("id"):
            logger.warning(f"[ai_image] WaveSpeed: no task_id: {data}")
            return False

        logger.info(f"[ai_image] WaveSpeed task: {task_data['id']} size={size}")

        for _ in range(50):  # max 150s
            await asyncio.sleep(3)
            async with httpx.AsyncClient(timeout=15.0) as client:
                pr = await client.get(poll_url, headers=headers)
                pr.raise_for_status()
                pd = pr.json().get("data", {})

            status = pd.get("status", "")
            if status in ("completed", "succeeded"):
                outputs = pd.get("outputs", [])
                if outputs:
                    return await _download_image(outputs[0], output_path)
                logger.warning(f"[ai_image] WaveSpeed: {status} but no outputs")
                return False
            elif status in ("failed", "cancelled"):
                logger.warning(f"[ai_image] WaveSpeed {status}: {pd.get('error', '')}")
                return False

        logger.warning("[ai_image] WaveSpeed: polling timeout 150s")
        return False

    except Exception as e:
        logger.warning(f"[ai_image] WaveSpeed error: {e}")
        return False


async def generate_ai_image_dalle(
    prompt: str,
    output_path: Path,
    size: str = "1024x1792",
) -> bool:
    """Generate image via OpenAI DALL-E 3 (fallback)."""
    if not OPENAI_API_KEY:
        logger.warning("[ai_image] OPENAI_API_KEY missing")
        return False

    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": "dall-e-3",
        "prompt": prompt,
        "n": 1,
        "size": size,
        "quality": "standard",
        "response_format": "url",
    }
    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(DALLE_URL, headers=headers, json=payload)
            resp.raise_for_status()
            data = resp.json()
        return await _download_image(data["data"][0]["url"], output_path)
    except Exception as e:
        logger.warning(f"[ai_image] DALL-E error: {e}")
        return False


async def _download_image(url: str, output_path: Path) -> bool:
    """Download image from URL to output_path."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.get(url)
            resp.raise_for_status()
        output_path.write_bytes(resp.content)
        kb = len(resp.content) // 1024
        logger.info(f"[ai_image] saved {output_path.name} ({kb}KB)")
        return output_path.stat().st_size > 1024
    except Exception as e:
        logger.warning(f"[ai_image] download error: {e}")
        return False


def image_to_video(image_path: Path, output_path: Path, duration: float) -> bool:
    """Convert static image to looping video with Ken Burns zoom effect."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists() and output_path.stat().st_size > 4096:
        return True

    dur = max(duration, 3.0)
    n_frames = int(dur * 30)

    vf = (
        f"scale=1920:1920:force_original_aspect_ratio=increase,"
        f"crop=1080:1920,"
        f"zoompan=z='min(zoom+0.0015,1.3)':d={n_frames}"
        f":x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)':s=1080x1920:fps=30,"
        f"unsharp=5:5:0.5,eq=brightness=0.02:contrast=1.1:saturation=1.3"
    )
    cmd = [
        "ffmpeg", "-y",
        "-loop", "1", "-i", str(image_path),
        "-vf", vf,
        "-t", str(dur),
        "-c:v", "libx264", "-preset", "fast", "-crf", "23",
        "-pix_fmt", "yuv420p", "-movflags", "+faststart", "-an",
        str(output_path),
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120.0)
        ok = result.returncode == 0 and output_path.exists() and output_path.stat().st_size > 4096
        if ok:
            logger.info(f"[ai_image] image→video OK: {output_path.name}")
        else:
            logger.warning(f"[ai_image] image→video failed: {result.stderr[-200:]}")
        return ok
    except Exception as e:
        logger.error(f"[ai_image] image→video error: {e}")
        return False
