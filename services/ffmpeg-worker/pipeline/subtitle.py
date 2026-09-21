"""ASS subtitle generation and video burn-in."""
from __future__ import annotations
import difflib
import os
import shutil
import json
import re
import subprocess
import logging
import time
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

from config import SUBTITLE_KARAOKE

FONTS_DIR = "/usr/share/fonts/opentype/noto"
FONTS_DIR_TRUETYPE = "/usr/share/fonts/truetype/noto"

# BorderStyle=1 outline (not opaque box), white+thick-black-outline
ASS_STYLE = (
    "Style: Default,Noto Sans CJK KR,{font_size},&H00FFFFFF,&H000000FF,"
    "&H00000000,&H80000000,-1,0,0,0,100,100,0,0,1,3,1,2,10,10,{margin_v},1"
)


def _find_fonts_dir() -> str:
    """Return usable fonts directory."""
    import os
    for d in [FONTS_DIR, FONTS_DIR_TRUETYPE, "/usr/share/fonts", "/usr/local/share/fonts"]:
        if os.path.isdir(d):
            return d
    return FONTS_DIR


def compute_subtitle_style(resolution: str = "1920x1080") -> tuple[int, int]:
    """Return (font_size, margin_v) for given resolution."""
    try:
        w_str, h_str = resolution.lower().split("x")
        width, height = int(w_str), int(h_str)
    except Exception:
        width, height = 1920, 1080

    is_vertical = height > width
    if is_vertical:
        # 유튜브 쇼츠는 하단 약 25%가 좋아요/댓글/공유 버튼 영역이라 그 위로 올려야 한다.
        # 폰트는 1080x1920 기준 60~75px 권장 구간의 상단(75px)을 쓴다.
        # 7%(75px)는 폰에서 읽기에 작다. 쇼츠는 화면 폭의 8.5% 정도가
        # 기본값에 가깝다(1080 기준 92px). 글자가 커진 만큼 줄바꿈 기준도 줄인다.
        font_size = max(80, int(width * 0.085))
        margin_v = max(360, int(height * 0.22))
    else:
        font_size = max(40, int(height * 0.060))
        margin_v = max(55, int(height * 0.065))

    return font_size, margin_v


def _ass_escape(text: str) -> str:
    """ASS Dialogue 본문에 그대로 넣으면 안 되는 문자를 무해하게 바꾼다.

    중괄호는 ASS 의 override 블록 시작/끝이라 나레이션에 '{' 가 들어가면 그 구간
    자막이 통째로 사라진다. 역슬래시도 태그 접두사라 같은 문제를 낸다.
    """
    return (
        text.replace("\\\\", "＼")
            .replace("{", "｛")
            .replace("}", "｝")
    )


def _split_korean_line(text: str, max_chars: int = 15) -> str:
    """Split at space nearest midpoint (eojeol boundary). Fallback: midpoint cut."""
    if len(text) <= max_chars:
        return text
    mid = len(text) // 2
    # Find space closest to midpoint — search left then right
    left = text.rfind(" ", 0, mid + 1)
    right = text.find(" ", mid)
    if left < 0 and right < 0:
        # No spaces (pure Korean): split at midpoint
        return text[:mid] + "\\N" + text[mid:]
    elif left < 0:
        sp = right
    elif right < 0:
        sp = left
    else:
        sp = left if (mid - left) <= (right - mid) else right
    return text[:sp] + "\\N" + text[sp + 1:]


def _merge_short_segments(segments: list, min_dur: float = 0.5) -> list:
    """Merge segments < min_dur with next segment to prevent subtitle flicker."""
    if not segments:
        return segments
    merged: list = []
    i = 0
    while i < len(segments):
        seg = dict(segments[i])
        dur = seg.get("end", 0) - seg.get("start", 0)
        if dur < min_dur and i + 1 < len(segments):
            nxt = segments[i + 1]
            seg["text"] = (seg.get("text", "").strip() + " " + nxt.get("text", "").strip()).strip()
            seg["end"] = nxt.get("end", seg["end"])
            i += 2
        else:
            i += 1
        merged.append(seg)
    return merged


_FMT_STYLE = (
    "Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, "
    "OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, "
    "ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, "
    "Alignment, MarginL, MarginR, MarginV, Encoding"
)
_FMT_EVENT = "Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text"


def _ass_header(w: int, h: int, font_size: int, margin_v: int) -> list[str]:
    """Build ASS file header lines."""
    return [
        "[Script Info]", "ScriptType: v4.00+",
        f"PlayResX: {w}", f"PlayResY: {h}", "",
        "[V4+ Styles]", _FMT_STYLE,
        ASS_STYLE.format(font_size=font_size, margin_v=margin_v), "",
        "[Events]", _FMT_EVENT,
    ]


def _sec_to_ass(sec: float, lead: float = 0.0) -> str:
    """Convert seconds to ASS timestamp h:mm:ss.cc"""
    sec = max(0.0, sec - lead)
    h_ = int(sec // 3600)
    m_ = int((sec % 3600) // 60)
    s_ = sec % 60
    return f"{h_:d}:{m_:02d}:{s_:05.2f}"


def _strip_map(text: str) -> tuple[str, list[int]]:
    """怨듬갚??類 臾몄옄?닿낵, 洹?媛?湲?먭? ?먮Ц 紐?踰덉㎏??붿?????묓몴."""
    out, idx = [], []
    for i, ch in enumerate(text):
        if not ch.isspace():
            out.append(ch)
            idx.append(i)
    return "".join(out), idx


def align_segments_to_original(segments: list, original: str, text_key: str = "text") -> int:
    """Whisper ?멸렇癒쇳듃??'湲??瑜??먮낯 ?섎젅?댁뀡??湲?먮줈 諛붽씔?? (??대컢? 洹몃?濡?

    ?먮쭑? TTS ?뚯꽦??Whisper 濡?諛쏆븘?곸? 寃곌낵瑜??곕뒗?? ?곕━???대? 寃?좊? 嫄곗튇
    ?뺥솗???먮낯 ?섎젅?댁뀡??媛뽮퀬 ?덈떎. ?ㅼ륫?쇰줈 Whisper ??'議곗꽑 500?꾩궗???ъ쓽 二쇱슂',
    '?꾧뎄??援щ굹 ?쎄쾶' 泥섎읆 ?뚯젅??寃뱀퀜 諛쏆븘?곷뒗 ?ㅼ씤?앹쓣 ?덈떎. 洹몃옒????대컢留?    Whisper ?먯꽌 痍⑦븯怨?湲?먮뒗 ?먮낯?쇰줈 諛붽퓭 ?쇱슫??

    臾몄옄 ?⑥쐞 ?뺣젹???ш쾶 ?닿툔?섎㈃(?좎궗??0.5 誘몃쭔) ?먮?吏 ?딄퀬 ?먮옒 ?꾩궗蹂몄쓣 ?대떎.
    諛섑솚媛? ?ㅼ젣濡?援먯껜???멸렇癒쇳듃 ??
    """
    texts = [(s.get(text_key) or "").strip() for s in segments]
    joined = "".join(texts)
    if not joined or not original:
        return 0

    w_norm, _ = _strip_map(joined)
    o_norm, o_idx = _strip_map(original)
    if not w_norm or not o_norm:
        return 0

    matcher = difflib.SequenceMatcher(None, w_norm, o_norm, autojunk=False)
    ratio = matcher.quick_ratio()
    if ratio < 0.5:
        logger.warning(f"[subtitle] 원본-전사 유사도 낮음({ratio:.2f}) → 전사본 그대로 사용")
        return 0

    # w_norm ?꾩튂 -> o_norm ?꾩튂 ??묓몴 (留ㅼ묶 援ш컙? ?뺥솗?? ?ъ씠 援ш컙? 鍮꾨? 諛곕텇)
    w2o = [None] * (len(w_norm) + 1)
    for a, b, size in matcher.get_matching_blocks():
        for k in range(size):
            w2o[a + k] = b + k
    # 마지막 지점을 무조건 원본 끝으로 붙이면, TTS 가 길이 제한으로 읽지 않은 뒷부분까지
    # 마지막 자막 한 줄이 통째로 흡수한다(실측). 마지막 매칭 블록의 끝까지만 인정한다.
    _blocks = [b for b in matcher.get_matching_blocks() if b.size > 0]
    w2o[len(w_norm)] = (_blocks[-1].b + _blocks[-1].size) if _blocks else len(o_norm)
    last = 0
    for i in range(len(w2o)):
        if w2o[i] is None:
            w2o[i] = last
        else:
            last = w2o[i]

    # 媛??멸렇癒쇳듃媛 joined ?먯꽌 李⑥??섎뒗 援ш컙??怨듬갚 ?쒓굅 湲곗??쇰줈 ?섏궛
    bounds, cursor = [], 0
    for txt in texts:
        n = len(_strip_map(txt)[0])
        bounds.append((cursor, cursor + n))
        cursor += n

    replaced = 0
    prev_end = 0      # 앞 세그먼트가 이미 가져간 원본 위치
    for seg, (ws, we) in zip(segments, bounds):
        if we <= ws:
            continue
        os_, oe = w2o[ws], w2o[min(we, len(w_norm))]
        if oe <= os_:
            continue
        start_char = o_idx[os_]
        end_char = o_idx[oe - 1] + 1 if oe - 1 < len(o_idx) else len(original)

        # 공백을 뺀 인덱스로 자르다 보니 조각 경계가 원본의 띄어쓰기와 어긋날 수 있다
        # (실측: '밤새 쉼 없이' -> '밤 새 쉼 없이'). 앞뒤로 가장 가까운 공백/문장
        # 경계까지 밀어 단어가 중간에 잘리지 않게 한다.
        while start_char > 0 and not original[start_char - 1].isspace():
            start_char -= 1
        while end_char < len(original) and not original[end_char].isspace():
            end_char += 1

        # 경계 확장이 세그먼트마다 독립적으로 일어나기 때문에, 앞 세그먼트가
        # 오른쪽으로 늘리고 뒤 세그먼트가 왼쪽으로 늘리면 같은 낱말을 둘 다
        # 가져간다. 실측 사고: 원본 '또한 0도' → 자막 '또한 또한 0도',
        # '보고되었어요' → '보고 보고되었어요'. 커서로 겹침을 막는다.
        if start_char < prev_end:
            start_char = prev_end
            while start_char < len(original) and original[start_char].isspace():
                start_char += 1
        if end_char <= start_char:
            continue

        piece = original[start_char:end_char].strip()
        prev_end = end_char
        # 원본 조각이 전사본보다 지나치게 길면 정렬이 어긋난 것 — 원문을 지킨다.
        _orig_len = len((seg.get(text_key) or "").strip())
        if piece and _orig_len and len(piece) > _orig_len * 2.5:
            logger.warning(
                f"[subtitle] 정렬 이상(전사 {_orig_len}자 → 원본 {len(piece)}자) → 해당 세그먼트 유지"
            )
            continue
        if piece and piece != (seg.get(text_key) or "").strip():
            seg[text_key] = piece
            replaced += 1
    if replaced:
        logger.info(f"[subtitle] 원본 대조 교정: {replaced}/{len(segments)}개 세그먼트 (유사도 {ratio:.2f})")
    return replaced


# 카라오케(단어별 하이라이트) 스타일.
# PrimaryColour = 이미 읽은 단어, SecondaryColour = 아직 안 읽은 단어.
# 리서치 권고: 흰색→노랑 대비가 가장 잘 읽히고(흰색→연회색은 대비 부족),
# 한 번에 4~6단어만 띄워야 하이라이트 스윕을 눈이 따라갈 수 있다.
ASS_STYLE_KARAOKE = (
    "Style: Default,Noto Sans CJK KR,{font_size},&H0000D7FF,&H00FFFFFF,"
    "&H00000000,&H80000000,-1,0,0,0,100,100,0,0,1,3,1,2,10,10,{margin_v},1"
)

KARAOKE_WORDS_PER_LINE = 5      # 4~6 권장 구간의 가운데
KARAOKE_MAX_GAP_SEC = 1.2       # 이보다 벌어지면 다른 줄로 끊는다


def _chunk_words(words: list, per_line: int = KARAOKE_WORDS_PER_LINE) -> list:
    """단어 목록을 한 줄에 띄울 묶음으로 나눈다.

    개수만으로 자르면 문장 중간의 긴 침묵을 가로질러 한 줄이 되어버리므로,
    단어 사이가 KARAOKE_MAX_GAP_SEC 이상 벌어지면 거기서도 끊는다.
    """
    chunks, cur = [], []
    for w in words:
        if cur:
            gap = float(w.get("start", 0)) - float(cur[-1].get("end", 0))
            if len(cur) >= per_line or gap > KARAOKE_MAX_GAP_SEC:
                chunks.append(cur)
                cur = []
        cur.append(w)
    if cur:
        chunks.append(cur)
    return chunks


# 한 줄을 다음 줄 시작 전까지 최대 몇 초나 붙들어 둘지. 길게 잡으면 말이
# 끝난 뒤에도 자막이 남아 어색하고, 짧으면 쉼 구간이 비어 깜빡인다.
_HOLD_MAX_SEC = 1.6
_TAIL_HOLD_SEC = 0.8


def create_ass_karaoke(
    words: list,
    output_path: Path,
    resolution: str = "1920x1080",
    lead_sec: float = 0.05,
    original_text: Optional[str] = None,
) -> bool:
    """단어별 하이라이트(카라오케) ASS 자막을 만든다.

    Whisper 의 word 타임스탬프({word,start,end})를 ASS 의 \\k 태그로 옮긴다.
    \\k 값의 단위는 센티초(1/100초)다. 단어 사이에 실제 공백 시간이 있으면
    그만큼 빈 \\k 를 넣어야 하이라이트가 앞질러 가지 않는다.
    """
    words = [w for w in (words or []) if (w.get("word") or "").strip()]
    if not words:
        return False

    # 문장 자막에만 걸어놨던 원본 대조 교정을 여기에도 적용한다.
    # 실측 누락 사례: 원본은 '밤하늘을 물들이거든요' 인데 자막은 '밤만을',
    # '쉼 없이'->'심없이', '빛 뒤에도'->'비트에도' 로 나갔다. 타이밍은 Whisper 것을
    # 쓰되 글자는 검토를 마친 원본으로 바꿔 끼운다.
    if original_text:
        align_segments_to_original(words, original_text, text_key="word")

    font_size, margin_v = compute_subtitle_style(resolution)
    w_px, h_px = (int(x) for x in resolution.lower().split("x"))
    lines = [
        "[Script Info]", "ScriptType: v4.00+",
        f"PlayResX: {w_px}", f"PlayResY: {h_px}", "",
        "[V4+ Styles]", _FMT_STYLE,
        ASS_STYLE_KARAOKE.format(font_size=font_size, margin_v=margin_v), "",
        "[Events]", _FMT_EVENT,
    ]

    # 줄을 먼저 다 만들고 나서 표시 종료 시각을 늘린다.
    #
    # 원래는 각 줄이 '마지막 단어가 끝나는 순간' 사라졌다. 문장과 문장 사이
    # 쉼에는 아무 자막도 안 떠서, 34초 영상에서 9초(26%)가 무자막이었다(실측).
    # 자막은 다음 줄이 시작될 때까지 남아 있어야 읽을 시간이 생긴다.
    blocks = []
    for chunk in _chunk_words(words):
        start = float(chunk[0].get("start", 0))
        end = float(chunk[-1].get("end", start + 1))
        if end <= start:
            continue
        blocks.append((chunk, start, end))

    count = 0
    for bi, (chunk, start, end) in enumerate(blocks):
        next_start = blocks[bi + 1][1] if bi + 1 < len(blocks) else None
        if next_start is not None:
            # 다음 줄 직전까지 유지하되, 너무 오래 붙들지는 않는다.
            end = max(end, min(next_start - 0.05, end + _HOLD_MAX_SEC))
        else:
            end = end + _TAIL_HOLD_SEC      # 마지막 줄은 여운을 준다
        parts, cursor = [], start
        for w in chunk:
            ws = float(w.get("start", cursor))
            we = float(w.get("end", ws))
            # 단어 앞의 침묵을 빈 카라오케로 채워 하이라이트가 앞서가지 않게 한다.
            pause_cs = int(round(max(0.0, ws - cursor) * 100))
            if pause_cs > 0:
                parts.append(f"{{\\k{pause_cs}}}")
            dur_cs = max(1, int(round((we - ws) * 100)))
            text = _ass_escape((w.get("word") or "").strip())
            parts.append(f"{{\\k{dur_cs}}}{text} ")
            cursor = we
        lines.append(
            f"Dialogue: 0,{_sec_to_ass(start, lead_sec)},{_sec_to_ass(end, lead_sec)},"
            f"Default,,0,0,0,,{''.join(parts).rstrip()}"
        )
        count += 1

    try:
        output_path.write_text("\n".join(lines), encoding="utf-8")
        logger.info(f"[subtitle] ✅ 카라오케 ASS: {len(words)}단어 → {count}줄 ({output_path.name})")
        return count > 0
    except Exception as e:
        logger.error(f"[subtitle] 카라오케 ASS 쓰기 실패: {e}")
        return False


def create_ass_from_timestamps(
    ts_path: Path,
    output_path: Path,
    resolution: str = "1920x1080",
    lead_sec: float = 0.05,
    original_text: Optional[str] = None,
) -> bool:
    """Generate ASS from Whisper timestamps.json.

    lead_sec: shift all timestamps earlier by this many seconds (perception pre-roll).
    Kept small (0.05s) to preserve audio-subtitle sync.
    """
    if not ts_path or not ts_path.exists():
        return False
    try:
        data = json.loads(ts_path.read_text(encoding="utf-8"))
    except Exception as e:
        logger.error(f"[subtitle] cannot read timestamps: {e}")
        return False

    segments = data.get("segments") or []
    if not segments:
        logger.warning("[subtitle] no segments in timestamps")
        return False
    # NOTE: _merge_short_segments() intentionally NOT called here.
    # Merging causes next-segment text to appear before that phrase is spoken.

    # 寃?좊? 留덉튇 ?먮낯 ?섎젅?댁뀡???덉쑝硫??꾩궗 ?ㅼ씤?앹쓣 ?먮낯 湲?먮줈 諛붾줈?〓뒗??
    if original_text:
        align_segments_to_original(segments, original_text)

    font_size, margin_v = compute_subtitle_style(resolution)
    w, h = (int(x) for x in resolution.lower().split("x"))
    lines = _ass_header(w, h, font_size, margin_v)

    for seg in segments:
        start = seg.get("start", 0)
        end = seg.get("end", start + 2)
        text = seg.get("text", "").strip().replace("\n", " ")
        if not text:
            continue
        text = _split_korean_line(_ass_escape(text))
        lines.append(
            f"Dialogue: 0,{_sec_to_ass(start, lead_sec)},{_sec_to_ass(end, lead_sec)},"
            f"Default,,0,0,0,,{text}"
        )

    try:
        output_path.write_text("\n".join(lines), encoding="utf-8")
        logger.info(f"[subtitle] ASS from timestamps: {len(segments)} segs → {output_path.name}")
        return True
    except Exception as e:
        logger.error(f"[subtitle] ASS write failed: {e}")
        return False


def create_ass_from_narration(
    narration: str,
    tts_duration: float,
    output_path: Path,
    resolution: str = "1920x1080",
) -> bool:
    """Fallback: generate ASS by distributing narration text across TTS duration.

    Used when Whisper timestamps are unavailable. Splits at sentence boundaries
    and assigns timing proportional to character count.
    """
    if not narration or tts_duration <= 0:
        return False

    # Split at sentence-ending punctuation
    phrases = re.split(r"(?<=[.!?。])\s+", narration.strip())
    segments_text: list[str] = []
    for phrase in phrases:
        phrase = phrase.strip()
        if not phrase:
            continue
        if len(phrase) > 24:
            # Further split at commas for very long phrases
            parts = re.split(r"(?<=[,，])\s*", phrase)
            segments_text.extend(p.strip() for p in parts if p.strip())
        else:
            segments_text.append(phrase)

    if not segments_text:
        return False

    total_chars = sum(len(s) for s in segments_text)
    lead_in = 0.3
    usable = max(tts_duration - lead_in, 1.0)

    timestamp_segs: list[dict] = []
    cur = lead_in
    for text in segments_text:
        ratio = len(text) / total_chars if total_chars > 0 else 1 / len(segments_text)
        dur = usable * ratio
        timestamp_segs.append({"start": cur, "end": cur + dur, "text": text})
        cur += dur

    font_size, margin_v = compute_subtitle_style(resolution)
    w, h = (int(x) for x in resolution.lower().split("x"))
    lines = _ass_header(w, h, font_size, margin_v)

    for seg in timestamp_segs:
        text = _split_korean_line(seg["text"])
        lines.append(
            f"Dialogue: 0,{_sec_to_ass(seg['start'])},{_sec_to_ass(seg['end'])},"
            f"Default,,0,0,0,,{text}"
        )

    try:
        output_path.write_text("\n".join(lines), encoding="utf-8")
        logger.info(f"[subtitle] narration ASS: {len(timestamp_segs)} segs → {output_path.name}")
        return True
    except Exception as e:
        logger.error(f"[subtitle] narration ASS write failed: {e}")
        return False


def burn_subtitles(
    input_video: Path,
    ass_path: Path,
    output_video: Path,
    resolution: str = "1920x1080",
) -> bool:
    """Burn ASS subtitles into video."""
    if not ass_path.exists():
        return False
    subtitle_filter = f"ass={ass_path}:fontsdir={_find_fonts_dir()}"

    # 영상 끝 페이드아웃은 길이가 확정된 이 단계에서만 걸 수 있다.
    # (렌더 단계에서 걸면 뒤에 붙는 tpad=clone 패딩이 '검은 마지막 프레임'을
    #  복제해 끝에 완전 검은 구간이 생긴다 — 실측 1.25초.)
    _tail = 0.5
    try:
        _p = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
             "-of", "csv=p=0", str(input_video)],
            capture_output=True, text=True, timeout=20)
        _dur = float(_p.stdout.strip())
    except Exception:
        _dur = 0.0
    if _dur > _tail * 3:
        subtitle_filter += f",fade=t=out:st={_dur - _tail:.2f}:d={_tail:.2f}"
        logger.info(f"끝 페이드아웃: {_dur - _tail:.2f}s~{_dur:.2f}s")
    # 인코딩 프리셋: preset=medium + crf=15 는 12분짜리 1080p 를 5분 안에 끝낼 수 없어
    # 매번 timeout -> '자막 없는 원본 복사' 로 떨어졌다. 프로젝트 표준(veryfast/23)에 맞추고
    # 타임아웃도 영상 길이에 비례시킨다 (최소 10분, 길이의 6배).
    duration = 0.0
    try:
        probe = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
             "-of", "csv=p=0", str(input_video)],
            capture_output=True, text=True, timeout=30,
        )
        raw = probe.stdout.strip()
        if raw and raw not in ("N/A", ""):
            duration = float(raw)
    except Exception:
        pass
    burn_timeout = max(600.0, duration * 6.0)
    cmd = [
        "ffmpeg", "-i", str(input_video),
        "-vf", subtitle_filter,
        "-c:v", "libx264", "-preset", "veryfast", "-crf", "20",
        # faststart 가 없으면 moov 아톰이 파일 끝에 남는다. 실측: 21.8MB 영상의
        # moov 가 21,843,779 바이트 지점 → 브라우저가 파일 전체를 받아야 첫 프레임이
        # 뜬다(미리보기가 계속 로딩 스피너였던 원인). 믹싱 단계에서 붙여둔
        # faststart 를 이 재인코딩이 되돌리고 있었다.
        "-movflags", "+faststart",
        "-pix_fmt", "yuv420p",
        "-c:a", "copy", "-y", str(output_video),
    ]
    logger.info(f"[subtitle] burn-in 시작: {duration:.0f}s 영상, timeout={burn_timeout:.0f}s")
    try:
        _t0 = time.time()
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=burn_timeout)
        logger.info(f"[subtitle] burn-in ffmpeg 종료 rc={result.returncode} ({time.time()-_t0:.0f}s)")
        if result.returncode == 0 and output_video.exists():
            logger.info(f"[subtitle] burn-in OK → {output_video.name}")
            return True
        logger.warning(f"[subtitle] burn-in failed: {result.stderr[-200:]}")
        return False
    except Exception as e:
        logger.error(f"[subtitle] burn-in error: {e}")
        return False


def add_subtitles_to_video(
    input_video: Path,
    ts_path: Optional[Path],
    output_video: Path,
    resolution: str = "1920x1080",
    narration: Optional[str] = None,
    tts_duration: Optional[float] = None,
) -> bool:
    """Full subtitle pipeline: timestamps (or narration fallback) → ASS → burn-in.

    Priority:
      1. Whisper timestamps.json (best quality)
      2. narration + tts_duration (char-proportional fallback)
      3. No subtitles — copy video as-is
    """
    import shutil

    ass_path = output_video.parent / f"{output_video.stem}_sub.ass"
    ass_ok = False

    # 1순위: 단어별 하이라이트(카라오케). Whisper 가 word 타임스탬프를 준 경우에만 가능하다.
    if SUBTITLE_KARAOKE and ts_path and ts_path.exists():
        try:
            _data = json.loads(ts_path.read_text(encoding="utf-8"))
            _words = _data.get("words") or []
            # 개수만 보면 안 된다. Whisper 가 한국어 word 타임스탬프를 듬성듬성 주면
            # (실측: 40초 영상에 52단어, 중간에 4초씩 비는 구간) 카라오케 자막이 화면을
            # 절반도 못 채운다. 같은 조건에서 문장 자막은 훨씬 촘촘했다.
            # 그래서 '단어가 실제로 덮는 시간'이 음성 길이의 절반은 되는지 본다.
            _covered = sum(
                max(0.0, float(w.get("end", 0)) - float(w.get("start", 0)))
                for w in _words
            )
            _total = float(_data.get("duration") or 0)
            _ratio = (_covered / _total) if _total > 0 else 0.0
            if len(_words) >= 8 and _ratio >= 0.5:
                ass_ok = create_ass_karaoke(_words, ass_path, resolution, original_text=narration)
            elif len(_words) >= 8:
                logger.info(
                    f"[subtitle] word 커버리지 부족({_ratio:.0%} < 50%) → 문장 자막이 더 촘촘하므로 전환"
                )
            else:
                logger.info(f"[subtitle] word 타임스탬프 부족({len(_words)}개) → 문장 자막으로 진행")
        except Exception as e:
            logger.warning(f"[subtitle] 카라오케 생성 실패({e}) → 문장 자막으로 진행")

    # 2순위: 문장 단위 자막 (+ 원본 대조 교정)
    if not ass_ok and ts_path and ts_path.exists():
        ass_ok = create_ass_from_timestamps(ts_path, ass_path, resolution, original_text=narration)

    # Fallback: generate from narration text
    if not ass_ok and narration and tts_duration:
        logger.info("[subtitle] Whisper timestamps unavailable — using narration fallback")
        ass_ok = create_ass_from_narration(narration, tts_duration, ass_path, resolution)

    if not ass_ok:
        logger.info("[subtitle] no subtitle source — copying video as-is")
        shutil.copy2(input_video, output_video)
        return True

    if not burn_subtitles(input_video, ass_path, output_video, resolution):
        logger.warning("[subtitle] burn-in failed — output without subtitles")
        shutil.copy2(input_video, output_video)

    return True
