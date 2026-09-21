# -*- coding: utf-8 -*-
"""업로드 전 기술 QC 게이트.

완성본이 유튜브에 올라가기 전에 '컨테이너 모양'과 '렌더 사고'를 잡는다.
내용 품질은 보지 않는다 — 판단이 필요한 것은 사람이 본다.

체크 항목과 임계값의 출처
  1. 화면비   : Shorts 는 세로여야 하고 w/h <= 0.58 이어야 Shorts 피드에 들어간다.
                (1080x1920 = 0.5625). 4:5(0.8) 같은 '세로긴 하지만 부족한' 영상이
                업로드는 성공하면서 Shorts 로 분류되지 않는 사고를 잡는다.
  2. 길이     : Shorts 15~180초. 오디오 합성이 중간에 잘리면 크래시 없이 그냥
                짧은 영상이 나오기 때문에 길이로만 잡힌다.
  3. 픽셀포맷 : 유튜브 인제스트는 yuv420p. yuv444p 등은 기기에 따라 색이 틀어진다.
  4. 검은화면 : blackdetect 합계가 전체의 8% 초과면 불합격.
                (17.61.0 실측: 29초에 4.0초 = 13.8% -> 이 게이트면 걸렸을 것)
  5. 무음     : silencedetect 합계가 전체의 12% 초과면 불합격.
  6. 라우드니스: 유튜브는 -14 LUFS 로 정규화한다. -16 ~ -12 범위를 벗어나면 경고.

fail-closed 원칙: 필터가 실패하면 '문제 없음'이 아니라 '검사 실패'로 처리한다.
검사가 깨진 채 통과시키면 게이트가 없는 것과 같다.

참고
  https://dev.to/morinaga/four-pre-upload-video-checks-i-run-with-ffprobe-before-any-youtube-video-goes-live-423h
  https://ayosec.github.io/ffmpeg-filters-docs/8.0/Filters/Video/blackdetect.html
"""

from __future__ import annotations

import json
import logging
import re
import subprocess
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

SHORT_MIN_SEC = 15.0
SHORT_MAX_SEC = 180.0
LONG_MIN_SEC = 60.0
LONG_MAX_SEC = 3600.0
SHORT_MAX_RATIO = 0.58          # w/h. 1080x1920 = 0.5625
BLACK_RATIO_MAX = 0.08
SILENCE_RATIO_MAX = 0.12
LUFS_MIN, LUFS_MAX = -16.0, -12.0
OK_PIX_FMT = {"yuv420p", "yuvj420p"}


def _run(cmd: list, timeout: float = 120.0) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)


def _probe(path: Path) -> dict:
    r = _run(["ffprobe", "-v", "error", "-show_streams", "-show_format",
              "-of", "json", str(path)], timeout=60)
    if r.returncode != 0:
        raise ValueError(f"ffprobe 실패: {r.stderr[-300:]}")
    return json.loads(r.stdout)


def _detect_seconds(path: Path, vf: Optional[str], af: Optional[str],
                    pattern: str) -> float:
    """필터가 보고한 구간 길이의 합(초). 필터가 실패하면 예외를 던진다(fail-closed)."""
    cmd = ["ffmpeg", "-hide_banner", "-nostats", "-i", str(path)]
    if vf:
        cmd += ["-vf", vf, "-an"]
    if af:
        cmd += ["-af", af, "-vn"]
    cmd += ["-f", "null", "-"]
    r = _run(cmd, timeout=300)
    if r.returncode != 0:
        raise ValueError(f"필터 실행 실패({vf or af}): {r.stderr[-300:]}")
    return sum(float(x) for x in re.findall(pattern, r.stderr))


def _measure_lufs(path: Path) -> Optional[float]:
    r = _run(["ffmpeg", "-hide_banner", "-nostats", "-i", str(path),
              "-af", "loudnorm=I=-14:TP=-1.5:LRA=11:print_format=json",
              "-f", "null", "-"], timeout=300)
    m = re.findall(r'"input_i"\s*:\s*"(-?[\d.]+)"', r.stderr)
    return float(m[-1]) if m else None


def qc_media(path: Path, mode: str = "short") -> dict:
    """완성본을 검사한다.

    반환: {"ok": bool, "errors": [...], "warnings": [...], "facts": {...}}
    예외는 던지지 않는다 — 검사 자체가 실패하면 errors 에 담아 불합격으로 만든다.
    """
    path = Path(path)
    errors: list = []
    warnings: list = []
    facts: dict = {}

    if not path.exists() or path.stat().st_size < 4096:
        return {"ok": False, "errors": [f"파일 없음/손상: {path}"],
                "warnings": [], "facts": {}}

    dur = 0.0
    try:
        data = _probe(path)
        vs = [s for s in data.get("streams", []) if s.get("codec_type") == "video"]
        aud = [s for s in data.get("streams", []) if s.get("codec_type") == "audio"]
        if not vs:
            errors.append("비디오 스트림 없음")
        else:
            w, h = int(vs[0]["width"]), int(vs[0]["height"])
            pix = vs[0].get("pix_fmt")
            facts.update(width=w, height=h, pix_fmt=pix,
                         vcodec=vs[0].get("codec_name"))
            ratio = w / max(h, 1)
            if mode == "short":
                if h <= w or ratio > SHORT_MAX_RATIO:
                    errors.append(
                        f"Shorts 는 세로 9:16 이어야 한다 (현재 {w}x{h}, w/h={ratio:.3f})")
            elif mode == "long" and w <= h:
                errors.append(f"롱폼은 가로여야 한다 (현재 {w}x{h})")
            if pix not in OK_PIX_FMT:
                errors.append(f"유튜브가 싫어하는 픽셀 포맷: {pix}")

        dur = float(data.get("format", {}).get("duration") or 0)
        facts["duration"] = round(dur, 2)
        lo, hi = ((SHORT_MIN_SEC, SHORT_MAX_SEC) if mode == "short"
                  else (LONG_MIN_SEC, LONG_MAX_SEC))
        if not (lo <= dur <= hi):
            errors.append(f"길이가 범위를 벗어남: {dur:.1f}s (허용 {lo:.0f}~{hi:.0f}s)")

        if not aud:
            errors.append("오디오 스트림 없음")
        else:
            facts["acodec"] = aud[0].get("codec_name")
    except Exception as e:
        errors.append(f"컨테이너 검사 실패: {e}")

    if dur > 0:
        try:
            black = _detect_seconds(
                path, "blackdetect=d=0.15:pix_th=0.12", None,
                r"black_duration:\s*([\d.]+)")
            facts["black_sec"] = round(black, 2)
            facts["black_ratio"] = round(black / dur, 4)
            if black / dur > BLACK_RATIO_MAX:
                errors.append(
                    f"검은 화면이 너무 많다: {black:.1f}s / {dur:.1f}s "
                    f"({black / dur * 100:.1f}% > {BLACK_RATIO_MAX * 100:.0f}%)")
        except Exception as e:
            errors.append(f"검은화면 검사 실패: {e}")

        try:
            sil = _detect_seconds(
                path, None, "silencedetect=n=-45dB:d=0.4",
                r"silence_duration:\s*([\d.]+)")
            facts["silence_sec"] = round(sil, 2)
            facts["silence_ratio"] = round(sil / dur, 4)
            if sil / dur > SILENCE_RATIO_MAX:
                errors.append(
                    f"무음이 너무 많다: {sil:.1f}s / {dur:.1f}s "
                    f"({sil / dur * 100:.1f}% > {SILENCE_RATIO_MAX * 100:.0f}%)")
        except Exception as e:
            errors.append(f"무음 검사 실패: {e}")

    try:
        lufs = _measure_lufs(path)
        if lufs is not None:
            facts["lufs"] = round(lufs, 1)
            if not (LUFS_MIN <= lufs <= LUFS_MAX):
                warnings.append(
                    f"라우드니스가 유튜브 기준(-14 LUFS)에서 벗어남: {lufs:.1f} LUFS")
    except Exception as e:
        warnings.append(f"라우드니스 측정 실패: {e}")

    ok = not errors
    lvl = logger.info if ok else logger.error
    lvl(f"[QC] {path.name} {'합격' if ok else '불합격'} :: {facts}")
    for m in errors:
        logger.error(f"[QC] X {m}")
    for m in warnings:
        logger.warning(f"[QC] ! {m}")
    return {"ok": ok, "errors": errors, "warnings": warnings, "facts": facts}