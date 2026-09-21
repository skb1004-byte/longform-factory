# -*- coding: utf-8 -*-
"""
LongForm Factory - Audio Mixing Pipeline

TTS narration + BGM ducking with loudnorm, fallback to simple amix.
"""

from __future__ import annotations
import subprocess
import logging
import re
import math
import random
from pathlib import Path
from typing import Optional

from config import (
    VIDEO_PRESET, BGM_DIR, BGM_VOLUME_DURING_VOICE, BGM_VOLUME_DEFAULT,
    AUDIO_LOUDNESS_TARGET,
)

logger = logging.getLogger(__name__)


def _get_duration(path: Path) -> Optional[float]:
    """Query audio/video duration via ffprobe."""
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
             "-of", "csv=p=0", str(path)],
            capture_output=True, text=True, timeout=10
        )
        raw = result.stdout.strip()
        if raw and raw not in ("N/A", ""):
            return float(raw)
    except Exception:
        pass
    return None


def get_random_bgm() -> Optional[Path]:
    """Select random BGM file from /data/bgm/."""
    try:
        bgm_files = list(BGM_DIR.glob("*.mp3")) + list(BGM_DIR.glob("*.wav"))
        if bgm_files:
            selected = random.choice(bgm_files)
            logger.info(f"selected BGM: {selected.name}")
            return selected
    except Exception:
        pass
    logger.warning("no BGM files available")
    return None


BGM_MIN_USABLE_LUFS = -45.0   # 이보다 조용한 파일은 '음악'이 아니라 잡음이다
BGM_TARGET_FLOOR = -45.0
BGM_TARGET_CEIL = -22.0


def _measure_lufs(path: Path) -> Optional[float]:
    """파일의 통합 라우드니스(LUFS). 측정 실패하면 None."""
    try:
        r = subprocess.run(
            ["ffmpeg", "-hide_banner", "-nostats", "-i", str(path),
             "-af", "loudnorm=I=-14:TP=-1.5:LRA=11:print_format=json",
             "-f", "null", "-"],
            capture_output=True, text=True, timeout=180)
    except Exception:
        return None
    m = re.findall(r'"input_i"\s*:\s*"(-?[\d.]+)"', r.stderr)
    return float(m[-1]) if m else None


def _bgm_target_lufs(bgm_volume: float) -> float:
    """기존 preset 의 volume 배율(0.04~0.12)을 '절대 목표 라우드니스'로 환산한다.

    예전에는 volume={배율} 로 원본을 그냥 곱했다. 그러면 결과 음량이 원본 파일의
    음량에 통째로 끌려간다. 실제로 /data/bgm 의 파일이 -53.7 LUFS(피크 -38dB)
    였고, 거기에 0.12 를 또 곱하니 -72 LUFS — 아예 안 들렸다. 그 결과 문장
    사이가 전부 '완전한 무음'이 되어 완성본의 무음 비율이 17~38% 였다.

    배율을 dB 로 바꿔 나레이션 목표(-14 LUFS) 아래로 내리는 절대 목표를 만든다.
    그러면 어떤 BGM 파일을 넣어도 최종 음량이 같다.
    """
    v = max(min(float(bgm_volume or 0.06), 1.0), 0.005)
    target = AUDIO_LOUDNESS_TARGET + 20.0 * math.log10(v)
    return max(BGM_TARGET_FLOOR, min(BGM_TARGET_CEIL, target))


def _bgm_is_usable(path: Path) -> bool:
    """거의 무음인 파일을 30dB 끌어올리면 음악이 아니라 히스가 된다. 그럴 바엔 끈다."""
    lufs = _measure_lufs(path)
    if lufs is None:
        logger.warning(f"BGM 라우드니스 측정 실패 → BGM 사용 보류: {path.name}")
        return False
    if lufs < BGM_MIN_USABLE_LUFS:
        logger.error(
            f"BGM 이 사실상 무음이다: {path.name} ({lufs:.1f} LUFS < "
            f"{BGM_MIN_USABLE_LUFS:.0f}). 정규화하면 잡음만 커지므로 BGM 없이 간다. "
            f"정상적인 음원(-14~-20 LUFS)을 /data/bgm 에 넣어야 한다.")
        return False
    logger.info(f"BGM OK: {path.name} ({lufs:.1f} LUFS)")
    return True


def _run_ffmpeg(cmd: list, timeout: float = 300.0) -> bool:
    """Execute ffmpeg command, log stderr on error."""
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        if result.returncode != 0:
            logger.error(f"ffmpeg error: {result.stderr[-300:]}")
            return False
        return True
    except subprocess.TimeoutExpired:
        logger.error(f"ffmpeg timeout ({timeout}s)")
        return False
    except Exception as e:
        logger.error(f"ffmpeg exception: {e}")
        return False


def _calc_output_duration(
    tts_path: Optional[Path],
    video_path: Path
) -> list:
    """
    Calculate -t trim argument for output video.

    Decision order:
    1. video_dur > tts_dur * 5: demuxer produced bogus timestamps, trust TTS.
    2. tts_dur < video_dur * 0.3: TTS encoding failure, use video_dur.
    3. Normal: use tts_dur + 0.5s tail buffer.
    """
    if not tts_path or not tts_path.exists():
        return []
    tts_dur = _get_duration(tts_path)
    if not tts_dur or tts_dur <= 0:
        return []
    video_dur = _get_duration(video_path)
    if video_dur and video_dur > 0:
        if video_dur > tts_dur * 5.0:
            # Concat demuxer stream-copy produced inflated timestamps — trust TTS
            effective_dur = tts_dur
            logger.warning(
                f"video ({video_dur:.1f}s) >> TTS ({tts_dur:.1f}s) by >5x "
                f"— demuxer timestamp anomaly, using TTS"
            )
        elif tts_dur < video_dur * 0.3:
            # TTS suspiciously short — likely encoding failure, use video
            effective_dur = video_dur
            logger.warning(
                f"TTS ({tts_dur:.2f}s) << video ({video_dur:.2f}s) "
                f"— using video duration"
            )
        else:
            effective_dur = tts_dur
            logger.info(f"TTS trim: {tts_dur:.2f}s + 0.5s")
    else:
        effective_dur = tts_dur
        logger.info(f"TTS trim: {tts_dur:.2f}s + 0.5s")
    return ["-t", str(round(effective_dur + 0.5, 2))]


def _video_pad_args(video_path: Path, tts_path: Optional[Path]) -> tuple:
    """영상이 나레이션보다 짧을 때 마지막 프레임을 복제해 길이를 맞춘다.

    실측된 사고: TTS 25.92초인데 조립된 영상이 24.53초였고, -shortest 가
    '더 짧은 쪽'인 영상을 따라가면서 나레이션 끝 1.39초를 잘라냈다. 마지막
    문장이 말하다 끊긴 채로 출력된 것이다.

    오디오를 자르는 대신 영상을 늘리는 쪽이 맞다. 말이 끊기는 것은 시청자가
    바로 알아채지만, 마지막 장면이 0.5초 더 머무는 것은 알아채지 못한다.

    반환: (비디오 필터 인자, 비디오 코덱 인자). 패딩이 불필요하면 스트림 카피.
    """
    if not tts_path or not tts_path.exists():
        return [], ["-c:v", "copy"]
    tts_dur = _get_duration(tts_path)
    video_dur = _get_duration(video_path)
    if not tts_dur or not video_dur or tts_dur <= 0 or video_dur <= 0:
        return [], ["-c:v", "copy"]
    # 5배 넘게 벌어지면 데뮤서 타임스탬프 이상이므로 손대지 않는다.
    if video_dur > tts_dur * 5.0 or tts_dur > video_dur * 5.0:
        return [], ["-c:v", "copy"]
    gap = tts_dur - video_dur
    if gap <= 0.15:
        return [], ["-c:v", "copy"]
    pad = round(gap + 0.4, 2)
    logger.info(
        f"나레이션({tts_dur:.2f}s)이 영상({video_dur:.2f}s)보다 길다 "
        f"→ 마지막 프레임 {pad:.2f}s 연장 (오디오를 자르지 않는다)"
    )
    return (["-vf", f"tpad=stop_mode=clone:stop_duration={pad}"],
            ["-c:v", "libx264", "-preset", "veryfast", "-crf", "20",
             "-pix_fmt", "yuv420p"])


def mix_audio(
    video_path: Path,
    tts_path: Optional[Path],
    bgm_path: Optional[Path],
    bgm_volume: float,
    output_path: Path
) -> bool:
    """
    Mix TTS narration + BGM with loudnorm. If loudnorm fails, fallback to simple amix.

    Args:
        video_path: input video (video stream)
        tts_path: TTS audio file (narration)
        bgm_path: background music file
        bgm_volume: BGM volume factor (0.0-1.0)
        output_path: output video with mixed audio

    Returns:
        True if successful
    """
    has_tts = tts_path and tts_path.exists() and tts_path.stat().st_size > 1024
    has_bgm = bool(bgm_path and bgm_path.exists()
                   and bgm_path.stat().st_size > 8192
                   and _bgm_is_usable(bgm_path))

    # Calculate trim: guard against abnormally short TTS
    tts_trim_args = _calc_output_duration(tts_path if has_tts else None, video_path)
    # 영상이 짧으면 오디오를 자르는 대신 영상을 늘린다.
    pad_vf, vcodec = _video_pad_args(video_path, tts_path if has_tts else None)
    # -t 로 길이를 명시한 경우 -shortest 는 해로울 뿐이다(짧은 쪽을 따라가 오디오를 자른다).
    shortest = [] if tts_trim_args else ["-shortest"]

    if has_tts and has_bgm:
        # TTS + BGM mix with ducking
        # loudnorm 을 TTS 스트림에만 걸면 뒤이은 amix 가 입력 수로 나누며
        # 약 6dB 를 깎아버린다(실측: -14 목표에 결과 -20.2 LUFS). 정규화는
        # 반드시 믹싱이 끝난 뒤 최종 버스에 걸어야 하고, amix 의 자동 감쇠도
        # normalize=0 으로 꺼야 한다.
        # 트랙마다 먼저 정규화하고 → 사이드체인으로 BGM 을 눌러주고 → 섞은 뒤 리미터.
        #
        # 바뀐 점 3가지
        #  (1) BGM 을 volume 배율로 곱하지 않고 loudnorm 절대 목표로 맞춘다.
        #      원본 파일 음량에 결과가 끌려가던 문제(-53.7 LUFS 파일 → 안 들림)를 없앤다.
        #  (2) preset 의 bgm_volume 을 실제로 쓴다. 예전에는 인자를 받아놓고
        #      BGM_VOLUME_DURING_VOICE 상수를 써서 preset 튜닝이 전부 무시됐다.
        #  (3) sidechaincompress 로 말할 때만 BGM 을 눌러 명료도를 지킨다.
        #      (수동 볼륨 자동화 대신 DAW 가 하는 방식)
        _bgm_lufs = _bgm_target_lufs(bgm_volume)
        logger.info(f"BGM 목표 라우드니스: {_bgm_lufs:.1f} LUFS (volume={bgm_volume})")
        filter_complex = (
            f"[1:a]aformat=sample_rates=48000:channel_layouts=stereo,"
            f"loudnorm=I={AUDIO_LOUDNESS_TARGET}:TP=-1.5:LRA=11,asplit=2[tts][tsc];"
            f"[2:a]aformat=sample_rates=48000:channel_layouts=stereo,"
            f"loudnorm=I={_bgm_lufs:.1f}:TP=-2:LRA=11[bgm];"
            f"[bgm][tsc]sidechaincompress=threshold=0.03:ratio=6:"
            f"attack=20:release=400[bgduck];"
            f"[tts][bgduck]amix=inputs=2:duration=first:dropout_transition=2:"
            f"normalize=0[mix];"
            f"[mix]alimiter=limit=0.891[aout]"
        )
        cmd = [
            "ffmpeg", "-i", str(video_path),
            "-i", str(tts_path), "-i", str(bgm_path),
            "-filter_complex", filter_complex,
            "-map", "0:v", "-map", "[aout]",
            *pad_vf, *tts_trim_args,
            *vcodec, "-c:a", "aac", "-ac", "2", "-b:a", "256k",
            *shortest, "-y", str(output_path)
        ]
    elif has_tts:
        # TTS only with loudnorm
        filter_complex = f"[1:a]loudnorm=I={AUDIO_LOUDNESS_TARGET}:TP=-1.5:LRA=11[aout]"
        cmd = [
            "ffmpeg", "-i", str(video_path), "-i", str(tts_path),
            "-filter_complex", filter_complex,
            "-map", "0:v", "-map", "[aout]",
            *pad_vf, *tts_trim_args,
            *vcodec, "-c:a", "aac", "-ac", "2", "-b:a", "256k",
            *shortest, "-y", str(output_path)
        ]
    elif has_bgm:
        # BGM only
        cmd = [
            "ffmpeg", "-i", str(video_path), "-i", str(bgm_path),
            "-filter_complex", f"[1:a]volume={bgm_volume}[aout]",
            "-map", "0:v", "-map", "[aout]",
            "-c:v", "copy", "-c:a", "aac", "-ac", "2", "-b:a", "256k",
            "-y", str(output_path)
        ]
    else:
        # No audio: copy video only
        cmd = ["ffmpeg", "-i", str(video_path), "-c", "copy", "-y", str(output_path)]

    success = _run_ffmpeg(cmd)
    if not success and has_tts:
        # Fallback: simple amix without loudnorm
        logger.warning("loudnorm failed, trying simple amix fallback")
        simple_cmd = [
            "ffmpeg", "-i", str(video_path), "-i", str(tts_path),
            "-map", "0:v", "-map", "1:a",
            *pad_vf, *tts_trim_args,
            *vcodec, "-c:a", "aac", "-ac", "2", "-b:a", "256k",
            *shortest, "-y", str(output_path)
        ]
        return _run_ffmpeg(simple_cmd)
    return success
