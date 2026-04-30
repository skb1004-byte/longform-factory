"""Video thumbnail generation using ffmpeg + Pillow."""
from __future__ import annotations
import subprocess
import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

FONT_PATH = "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc"
FALLBACK_FONT_PATH = "/usr/share/fonts/opentype/noto/NotoSansCJKkr-Regular.otf"


def _load_font(size: int):
    """Load TrueType font with fallback."""
    from PIL import ImageFont

    for font_path in [FONT_PATH, FALLBACK_FONT_PATH]:
        try:
            return ImageFont.truetype(font_path, size)
        except Exception:
            continue
    return ImageFont.load_default()


def generate_thumbnail(
    video_path: Path,
    output_path: Path,
    title: str = "",
    timestamp: str = "3",
) -> bool:
    """Extract frame at timestamp + overlay title text.

    Args:
        video_path: Input video file
        output_path: Output thumbnail image path
        title: Text overlay (optional)
        timestamp: Frame extraction timestamp in seconds

    Returns:
        True if successful, False otherwise
    """
    frame_path = output_path.parent / f"{output_path.stem}_frame.jpg"

    # extract frame
    cmd = [
        "ffmpeg",
        "-ss",
        timestamp,
        "-i",
        str(video_path),
        "-vframes",
        "1",
        "-q:v",
        "2",
        "-y",
        str(frame_path),
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, timeout=30)
        if result.returncode != 0 or not frame_path.exists():
            logger.warning("[thumbnail] frame extraction failed")
            return False
    except Exception as e:
        logger.error(f"[thumbnail] ffmpeg error: {e}")
        return False

    if not title:
        frame_path.rename(output_path)
        logger.info(f"[thumbnail] extracted → {output_path.name}")
        return True

    # add title overlay
    try:
        from PIL import Image, ImageDraw

        img = Image.open(frame_path).convert("RGB")
        w, h = img.size
        draw = ImageDraw.Draw(img, "RGBA")

        # bottom gradient overlay (dark semi-transparent bar)
        overlay = Image.new("RGBA", (w, h // 3), (0, 0, 0, 0))
        overlay_draw = ImageDraw.Draw(overlay)
        for y in range(h // 3):
            alpha = int(180 * (y / (h // 3)))
            overlay_draw.rectangle([0, y, w, y + 1], fill=(0, 0, 0, alpha))
        img.paste(overlay, (0, h - h // 3), overlay)

        # load font
        font = _load_font(48)

        # title text (truncate if too long)
        text = title[:30] + ("..." if len(title) > 30 else "")
        draw = ImageDraw.Draw(img)
        text_y = h - h // 4
        # shadow
        draw.text((w // 2 + 2, text_y + 2), text, font=font, fill=(0, 0, 0, 180), anchor="mt")
        # white text
        draw.text((w // 2, text_y), text, font=font, fill=(255, 255, 255), anchor="mt")

        img.save(str(output_path), "JPEG", quality=90)
        frame_path.unlink(missing_ok=True)
        logger.info(f"[thumbnail] generated: {output_path.name}")
        return True

    except Exception as e:
        logger.error(f"[thumbnail] Pillow error: {e}")
        # fallback: use frame without overlay
        try:
            frame_path.rename(output_path)
            logger.info(f"[thumbnail] fallback (no overlay): {output_path.name}")
            return True
        except Exception:
            return False
