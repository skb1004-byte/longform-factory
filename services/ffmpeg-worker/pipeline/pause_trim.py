# -*- coding: utf-8 -*-
"""TTS 문장 사이 멈춤을 장르에 맞게 줄이고 타임스탬프를 함께 당긴다."""
from __future__ import annotations
import io
import json
import logging
import re
import subprocess
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

_SILENCE_THRESHOLD = "-35dB"


def _probe_duration(path: Path) -> Optional[float]:
    try:
        r = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
             "-of", "default=nw=1:nk=1", str(path)],
            capture_output=True, text=True, timeout=20)
        raw = r.stdout.strip()
        return float(raw) if raw and raw not in ("N/A", "") else None
    except Exception:
        return None


def _detect_silences(path: Path, min_pause: float) -> list:
    """(시작, 끝) 무음 구간 목록."""
    try:
        r = subprocess.run(
            ["ffmpeg", "-i", str(path), "-af",
             f"silencedetect=noise={_SILENCE_THRESHOLD}:d={min_pause}", "-f", "null", "-"],
            capture_output=True, text=True, timeout=120)
    except Exception as e:
        logger.warning(f"[tts] 무음 검출 실패: {type(e).__name__} {e}")
        return []
    out = (r.stderr or "") + (r.stdout or "")
    starts = [float(x) for x in re.findall(r"silence_start: ([-\d.]+)", out)]
    ends = [float(x) for x in re.findall(r"silence_end: ([\d.]+)", out)]
    if len(ends) < len(starts):
        d = _probe_duration(path)
        if d:
            ends.append(d)
    return list(zip(starts, ends))


def compress_pauses(mp3_path: Path, ts_path: Optional[Path], max_pause: float) -> bool:
    """문장 사이 긴 멈춤을 max_pause 로 줄이고 타임스탬프를 같은 양만큼 당긴다.

    Edge TTS 는 문장마다 1.4~1.6초를 쉰다. 실측: 31.3초 나레이션에서 8곳,
    합계 11.8초(38%)가 무음이었다. 다큐라면 그 여백이 어울리지만 쇼츠에서는
    화면이 멈춘 것처럼 느껴진다.

    자막과 씬 길이가 모두 이 타임스탬프에 묶여 있으므로, 오디오만 줄이면
    전부 어긋난다. 잘라낸 구간을 정확히 알고 있어야 같은 양을 타임스탬프에서
    빼줄 수 있어서, silenceremove 같은 한 줄짜리 필터 대신 직접 구간을 계산한다.

    실패하면 원본을 그대로 두고 False 를 돌려주므로 파이프라인은 멈추지 않는다.
    """
    if max_pause <= 0 or not mp3_path.exists():
        return False
    dur = _probe_duration(mp3_path)
    if not dur or dur <= 0:
        return False

    sil = _detect_silences(mp3_path, max_pause)
    cuts = []
    for s, e in sil:
        keep_until = s + max_pause
        if e - keep_until > 0.05:
            cuts.append((round(keep_until, 3), round(e, 3)))
    if not cuts:
        logger.info("[tts] 줄일 멈춤 없음")
        return False
    removed = sum(e - s for s, e in cuts)

    keep, prev = [], 0.0
    for cs, ce in cuts:
        if cs > prev:
            keep.append((prev, cs))
        prev = ce
    if prev < dur:
        keep.append((prev, dur))
    if len(keep) < 2:
        return False

    n = len(keep)
    fc = ["[0:a]asplit=%d%s;" % (n, "".join(f"[s{i}]" for i in range(n)))]
    for i, (a, b) in enumerate(keep):
        fc.append(f"[s{i}]atrim=start={a}:end={b},asetpts=PTS-STARTPTS[t{i}];")
    fc.append("".join(f"[t{i}]" for i in range(n)) + f"concat=n={n}:v=0:a=1[out]")

    tmp_out = mp3_path.with_name(mp3_path.stem + "_trim.mp3")
    try:
        r = subprocess.run(
            ["ffmpeg", "-v", "error", "-i", str(mp3_path), "-filter_complex", "".join(fc),
             "-map", "[out]", "-c:a", "libmp3lame", "-q:a", "2", str(tmp_out), "-y"],
            capture_output=True, text=True, timeout=180)
    except Exception as e:
        logger.warning(f"[tts] 멈춤 압축 실패: {type(e).__name__} {e}")
        return False
    if r.returncode != 0 or not tmp_out.exists() or tmp_out.stat().st_size < 1024:
        logger.warning(f"[tts] 멈춤 압축 ffmpeg 오류: {r.stderr[-200:]}")
        tmp_out.unlink(missing_ok=True)
        return False

    new_dur = _probe_duration(tmp_out) or (dur - removed)

    def remap(t: float) -> float:
        off = 0.0
        for cs, ce in cuts:
            if t >= ce:
                off += ce - cs
            elif t > cs:
                off += t - cs
        return round(max(0.0, t - off), 3)

    if ts_path and ts_path.exists():
        try:
            d = json.load(io.open(ts_path, encoding="utf-8"))
            for seg in (d.get("segments") or []):
                seg["start"], seg["end"] = remap(seg.get("start", 0)), remap(seg.get("end", 0))
                for w in (seg.get("words") or []):
                    w["start"], w["end"] = remap(w.get("start", 0)), remap(w.get("end", 0))
            d["duration"] = new_dur
            json.dump(d, io.open(ts_path, "w", encoding="utf-8"), ensure_ascii=False)
        except Exception as e:
            # 타임스탬프를 못 고치면 오디오도 되돌린다 — 싱크가 어긋나는 게 더 나쁘다.
            logger.warning(f"[tts] 타임스탬프 재매핑 실패 → 원본 유지: {type(e).__name__} {e}")
            tmp_out.unlink(missing_ok=True)
            return False

    tmp_out.replace(mp3_path)
    logger.info(
        f"[tts] 멈춤 압축: {dur:.1f}s → {new_dur:.1f}s "
        f"(무음 {len(cuts)}곳에서 {removed:.1f}s 제거, 상한 {max_pause}s)"
    )
    return True