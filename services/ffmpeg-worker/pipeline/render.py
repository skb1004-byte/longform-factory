# -*- coding: utf-8 -*-
"""
LongForm Factory - Video Rendering Pipeline

Ken Burns clip generation, clip normalization, xfade concat with fallback,
and duration validation.
"""

from __future__ import annotations
import subprocess
import logging

from pipeline import cancel
import shutil
from pathlib import Path
from typing import Optional, List

from config import (
    BLUR_PAD_ENABLED,
    VIDEO_PRESET, VIDEO_CRF, TMP_DIR, OUTPUT_DIR,
    SCENE_HEAD_PAD_SEC, SCENE_TAIL_PAD_SEC, get_resolution
)

logger = logging.getLogger(__name__)

# Ken Burns zoom-pan presets: 6 variations for temporal rhythm
#
# 떨림(judder) 대책: zoompan 은 크롭 원점을 '소스 정수 픽셀'로 반올림하기 때문에,
# 소스가 출력과 같은 해상도면 프레임당 1픽셀 미만의 느린 이동이 '멈췄다 튀었다'를
# 반복하며 떨린다. 그래서 아래 필터를 적용하기 직전에 소스를 4배로 키운다
# (render 체인의 scale={W*4}:{H*4}) -> 반올림 오차가 출력 기준 0.25픽셀이 되어 사라진다.
# 팬 이동량은 iw/ih 비율식으로 쓰므로 업스케일 배율과 무관하게 속도가 유지된다.
#
# [2026-09-20 수정] zoompan 의 d 는 '입력 1프레임을 d 프레임으로 늘리는' 옵션이다.
# 예전엔 d=서브클립전체프레임 이라서 모든 서브클립이 '첫 프레임 한 장의 정지화면'이 됐다.
# 소재 영상의 움직임이 통째로 버려졌고, 첫 프레임이 어두운 소재(페이드인 시작 등)는
# 3초 내내 어두웠다(실측 22.2 vs 정상 42.1). 동영상 소재는 d=1 로 프레임마다 처리한다.
# 정지 이미지 소재만 d=전체프레임 이 맞다.
KB_PRESETS = [
    "zoompan=z='min(zoom+{kb_speed},1.06)':x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)':d={d}:s={W}x{H}:fps=30",
    "zoompan=z='if(lte(on,0),1.5,max(zoom-{kb_speed},1.0))':x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)':d={d}:s={W}x{H}:fps=30",
    "zoompan=z='1.3':x='min(on*iw*{pan_x},iw*0.25)':y='ih/2-(ih/zoom/2)':d={d}:s={W}x{H}:fps=30",
    "zoompan=z='1.3':x='max(iw*0.25-on*iw*{pan_x},0)':y='ih/2-(ih/zoom/2)':d={d}:s={W}x{H}:fps=30",
    "zoompan=z='min(zoom+{kb_speed},1.25)':x='iw/2-(iw/zoom/2)':y='min(on*ih*{pan_y},ih*0.18)':d={d}:s={W}x{H}:fps=30",
    "zoompan=z='min(zoom+{kb_speed_hi},1.25)':x='max(iw*0.1-on*iw*{pan_x6},0)':y='ih-ih/zoom':d={d}:s={W}x{H}:fps=30",
]

# Template presets: saturation, contrast, vignette
TEMPLATE_CONFIGS = {
    "info":   {"saturation": 1.25, "contrast": 1.10, "vignette": "PI/5"},
    "news":   {"saturation": 1.05, "contrast": 1.15, "vignette": "PI/6"},
    "edu":    {"saturation": 1.15, "contrast": 1.08, "vignette": "PI/5"},
    "ad":     {"saturation": 1.40, "contrast": 1.22, "vignette": "PI/4"},
    "story":  {"saturation": 1.20, "contrast": 1.15, "vignette": "PI/4"},
}


# 블러 패딩에서 전경이 차지할 최소 세로 비율.
# 0.62 = 화면의 62%를 실제 그림이 채우고 위아래 19%씩만 흐린 배경.
_FG_FILL_RATIO = 0.62


def _probe_size(path: str) -> Optional[tuple]:
    """소재의 실제 해상도를 읽는다. 실패하면 None."""
    try:
        r = subprocess.run(
            ["ffprobe", "-v", "error", "-select_streams", "v:0",
             "-show_entries", "stream=width,height", "-of", "csv=p=0:s=x", str(path)],
            capture_output=True, text=True, timeout=10,
        )
        raw = (r.stdout or "").strip().split("\n")[0]
        if "x" in raw:
            w, h = raw.split("x")[:2]
            return int(w), int(h)
    except Exception:
        pass
    return None


def _needs_blur_pad(src: str, out_w: int, out_h: int, threshold: float = 0.25) -> bool:
    """크롭하면 화면을 너무 많이 잘라내야 하는지 판단.

    가로 소재를 세로로 크롭하면 좌우 70% 이상이 날아가 피사체가 통째로 사라진다.
    가로세로비가 threshold 이상 차이나면 크롭 대신 블러 패딩을 쓴다.
    """
    size = _probe_size(src)
    if not size:
        return False
    sw, sh = size
    if sw <= 0 or sh <= 0:
        return False
    src_ar = sw / sh
    out_ar = out_w / out_h
    diff = abs(src_ar - out_ar) / max(out_ar, 0.001)
    return diff >= threshold


def get_video_duration(video_path: Path) -> Optional[float]:
    """Query video duration via ffprobe. Returns None if N/A or missing."""
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
             "-of", "csv=p=0", str(video_path)],
            capture_output=True, text=True, timeout=10
        )
        raw = result.stdout.strip()
        if raw and raw not in ("N/A", ""):
            return float(raw)
    except Exception:
        pass
    return None


def _is_valid_clip(clip_path: Path) -> bool:
    """Validate clip: exists, >4KB, has valid video stream."""
    try:
        if not clip_path.exists() or clip_path.stat().st_size < 4096:
            return False
        result = subprocess.run(
            ["ffprobe", "-v", "error", "-select_streams", "v:0",
             "-show_entries", "stream=codec_name", "-of", "default=nw=1:nk=1",
             str(clip_path)],
            capture_output=True, text=True, timeout=20
        )
        return result.returncode == 0 and bool(result.stdout.strip())
    except Exception:
        return False


def normalize_clip(clip_path: Path, timeout: float = 45.0) -> Path:
    """Re-encode clip to fix Duration:N/A (concat filter incompatibility)."""
    dur = get_video_duration(clip_path)
    if dur is not None and dur > 0:
        return clip_path
    norm_path = clip_path.with_name(clip_path.stem + "_norm.mp4")
    if norm_path.exists():
        return norm_path
    cmd = [
        "ffmpeg", "-i", str(clip_path), "-c:v", "libx264", "-preset", "ultrafast",
        "-crf", "18", "-movflags", "+faststart", "-an", "-y", str(norm_path)]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if result.returncode == 0:
        logger.debug(f"normalize_clip OK: {clip_path.name}")
        return norm_path
    logger.warning(f"normalize_clip failed: {clip_path.name}")
    return clip_path


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


def xfade_batch(clip_paths: list, output: Path, transition: str = "fade") -> bool:
    """Concatenate clips via concat filter (handles Duration:N/A). Fallback to demuxer."""
    original_n = len(clip_paths)
    clip_paths = [cp for cp in clip_paths if _is_valid_clip(cp)]
    dropped = original_n - len(clip_paths)
    if dropped:
        logger.warning(f"xfade_batch: dropped {dropped} invalid clips ({len(clip_paths)} remain)")

    if len(clip_paths) == 0:
        logger.error("xfade_batch: no valid clips")
        return False
    if len(clip_paths) == 1:
        shutil.copy(str(clip_paths[0]), str(output))
        return True

    # Method 1: normalize Duration:N/A clips, then concat filter
    clip_paths = [normalize_clip(cp) for cp in clip_paths]
    inputs = []
    for cp in clip_paths:
        inputs += ["-i", str(cp)]

    n = len(clip_paths)
    # Normalize PTS, concat video
    vparts = "".join(f"[{i}:v:0]setpts=PTS-STARTPTS,format=yuv420p,setsar=1:1[v{i}];" for i in range(n))
    vconcat = "".join(f"[v{i}]" for i in range(n))
    fg = f"{vparts}{vconcat}concat=n={n}:v=1:a=0[vout]"

    cmd = ["ffmpeg"] + inputs + [
        "-filter_complex", fg,
        "-map", "[vout]",
        "-c:v", "libx264", "-preset", VIDEO_PRESET, "-crf", str(VIDEO_CRF),
        "-movflags", "+faststart", "-filter_threads", "1", "-y", str(output)
    ]
    timeout = max(300.0, n * 30)
    if _run_ffmpeg(cmd, timeout=timeout):
        logger.info(f"xfade_batch concat OK: {n} clips -> {output.name}")
        return True

    # Method 2: demuxer concat fallback (stream copy)
    logger.warning("concat filter failed, trying demuxer fallback")
    clip_paths = [cp for cp in clip_paths if _is_valid_clip(cp)]
    if not clip_paths:
        logger.error("demuxer fallback: no valid clips")
        return False
    concat_txt = output.parent / f"_concat_{output.stem}.txt"
    with open(concat_txt, "w") as f:
        for cp in clip_paths:
            f.write(f"file '{cp}'\n")
    cmd2 = ["ffmpeg", "-f", "concat", "-safe", "0", "-i", str(concat_txt),
            "-c:v", "libx264", "-preset", "veryfast", "-crf", "23",
            "-movflags", "+faststart", "-y", str(output)]
    return _run_ffmpeg(cmd2, timeout=timeout)


def prepare_clips_for_longform(
    scenes: list,
    video_type: str = "longform",
    output_dir: Optional[Path] = None,
    W: Optional[int] = None,
    H: Optional[int] = None,
) -> List[Path]:
    """
    Generate Ken Burns clips from scenes.

    Each scene becomes N sub-clips (3-4s each) with Ken Burns effects,
    color grading, fades. Then merged per-scene and trimmed to scene duration.

    W/H가 주어지면 그대로 사용한다 (대시보드 해상도 선택 반영). 없으면 기존처럼
    video_type만으로 1080p 기준 해상도를 계산한다 (하위 호환).
    """
    if output_dir is None:
        output_dir = OUTPUT_DIR / "clips"
    output_dir.mkdir(parents=True, exist_ok=True)

    if not W or not H:
        W, H = get_resolution(video_type)
    template = TEMPLATE_CONFIGS.get("info", TEMPLATE_CONFIGS["info"])
    clips = []
    kb_counter = 0

    for scene_idx, scene in enumerate(scenes):
        cancel.check_active(f"렌더 씬{scene_idx + 1}")
        if not hasattr(scene, 'asset_url') or not scene.asset_url:
            logger.warning(f"scene {scene_idx}: no asset_url, skipping")
            continue

        scene_dur = max(getattr(scene, 'duration_seconds', None) or 5.0, 1.5)

        # Probe source duration
        src_dur = get_video_duration(Path(scene.asset_url))
        if src_dur is None:
            src_dur = scene_dur * 3
            needs_loop = True
        else:
            needs_loop = src_dur < scene_dur * 0.95

        # Calculate sub-clips (3s each, max 8)
        SUB_DUR = 3.0
        n_subs = min(8, max(1, int(scene_dur / SUB_DUR)))
        logger.info(f"scene {scene_idx}: {scene_dur:.1f}s -> {n_subs} sub-clips (src={src_dur:.1f}s, loop={needs_loop})")

        scene_clips = []
        base_sub_dur = scene_dur / n_subs

        for sub_i in range(n_subs):
            cancel.check_active(f"렌더 서브클립 {scene_idx + 1}-{sub_i + 1}")
            if sub_i == n_subs - 1:
                sub_dur = max(scene_dur - base_sub_dur * (n_subs - 1), 1.0)
            else:
                sub_dur = base_sub_dur

            fps_d = max(int(sub_dur * 30), 30)
            kb_speed = round(0.0008 * (4.0 / max(sub_dur, 4.0)), 5)
            kb_speed_hi = round(0.001 * (4.0 / max(sub_dur, 4.0)), 5)
            # 팬은 전체 프레임의 90% 지점에서 목표치에 닿도록 프레임당 비율로 환산.
            _span = max(fps_d * 0.9, 1.0)
            pan_x = round(0.25 / _span, 7)
            pan_y = round(0.18 / _span, 7)
            pan_x6 = round(0.10 / _span, 7)

            _preset_tpl = KB_PRESETS[kb_counter % len(KB_PRESETS)]
            kb_counter += 1

            # [2026-09-20] 예전엔 서브클립마다 fade in/out 을 걸었다. 컷 경계마다
            # 0.35s 블랙아웃 + 0.15s 블랙인 = 0.5s 씩 화면이 검게 깜빡였다(실측:
            # 30초 영상에 어두운 구간 3~4개, 최장 4.0s). 컷 편집은 하드컷이 정상이고
            # 페이드는 영상 전체의 맨 앞/맨 뒤에만 있어야 한다.
            # 끝 페이드아웃은 여기서 걸면 안 된다. 나레이션이 영상보다 길면
            # mix_audio 가 tpad=clone 으로 마지막 프레임을 복제해 길이를 맞추는데,
            # 그 마지막 프레임이 이미 검게 페이드된 상태라 끝에 1.25초짜리
            # 완전 검은 화면이 붙었다(실측: 22.75~24.0s YAVG 22 고정).
            # 끝 페이드는 길이가 확정된 뒤 자막 번인 단계에서 건다(burn_subtitles).
            _fades = ""
            if scene_idx == 0 and sub_i == 0:
                _fades = f"fade=t=in:st=0:d={SCENE_HEAD_PAD_SEC:.2f},"

            # Sweep crop x-position across sub-clips (left→center→right) so that
            # for landscape (16:9) sources converted to portrait (9:16), different
            # horizontal regions are captured per sub-clip rather than always
            # using the default center crop (which can miss off-center subjects).
            crop_x_ratio = sub_i / max(n_subs - 1, 1) if n_subs > 1 else 0.5
            crop_x_expr = f"(iw-{W})*{crop_x_ratio:.3f}"

            # 소재 선택: 서브클립마다 서로 다른 영상이 준비돼 있으면 그걸 쓴다.
            # (블러 판정이 이 값을 쓰므로 반드시 먼저 정해져야 한다)
            asset_list = getattr(scene, "asset_urls", None) or []
            src_asset = (asset_list[sub_i % len(asset_list)]
                         if asset_list else scene.asset_url)

            # 소재 비율이 출력 비율과 크게 다르면(가로 소재 → 세로 영상 등) 크롭하면
            # 피사체가 잘려 나간다. 참고 채널(이슈쏙쏙)이 쓰는 방식대로, 원본을 통째로
            # 가운데 넣고 남는 좌우/상하는 같은 화면을 흐리게 깔아 채운다.
            if BLUR_PAD_ENABLED and _needs_blur_pad(src_asset, W, H):
                # 전경을 '그냥 맞춰 넣기'만 하면 16:9 소재가 9:16 화면에서
                # 세로의 32% 밖에 못 채운다(실측). 나머지는 전부 흐린 배경이라
                # 화면이 텅 비어 보인다. 폭은 꽉 채우고 높이는 최소 _FG_FILL_RATIO
                # 만큼 확보하도록 키운 뒤 좌우를 잘라낸다. 크롭은 생기지만
                # 피사체가 화면을 채우는 쪽이 훨씬 낫다.
                fg_h = max(2, int(H * _FG_FILL_RATIO) // 2 * 2)
                base = (
                    f"split=2[bg][fg];"
                    f"[bg]scale={W}:{H}:force_original_aspect_ratio=increase,"
                    f"crop={W}:{H},boxblur=luma_radius=40:luma_power=2,"
                    f"eq=brightness=-0.06:saturation=0.75[bgb];"
                    f"[fg]scale={W}:{fg_h}:force_original_aspect_ratio=increase,"
                    f"crop={W}:{fg_h}[fgs];"
                    f"[bgb][fgs]overlay=(W-w)/2:(H-h)/2,"
                )
            else:
                base = (
                    f"scale={W}:{H}:force_original_aspect_ratio=increase,"
                    f"crop={W}:{H}:{crop_x_expr}:(ih-{H})/2,"
                )

            # 소재 종류에 따라 zoompan 동작이 완전히 달라진다.
            #  - 동영상: d=1 (프레임마다 1:1). 소재의 실제 움직임이 살아난다.
            #  - 정지이미지: d=전체프레임 (한 장을 늘려 켄번즈). -loop 1 과 짝.
            _is_still = str(src_asset).lower().endswith(
                (".jpg", ".jpeg", ".png", ".webp", ".bmp"))
            kb_filter = (_preset_tpl
                         .replace("{d}", str(fps_d) if _is_still else "1")
                         .replace("{kb_speed}", str(kb_speed))
                         .replace("{kb_speed_hi}", str(kb_speed_hi))
                         .replace("{pan_x}", str(pan_x))
                         .replace("{pan_y}", str(pan_y))
                         .replace("{pan_x6}", str(pan_x6))
                         .replace("{W}", str(W))
                         .replace("{H}", str(H)))
            # d=1 은 프레임마다 업스케일이 돌아 4배는 비용이 +40%인데 화질 차이가 없었다(실측).
            # 2배면 반올림오차 0.5px 이고, 실제 움직임이 있으면 떨림은 보이지 않는다.
            _up = 4 if _is_still else 2
            vf = (
                base +
                f"scale={W * _up}:{H * _up}:flags=bicubic,"   # zoompan 떨림 제거
                + ("" if _is_still else "fps=30,")            # 팬 속도를 fps 와 무관하게
                + f"{kb_filter},"
                + _fades
                + f"unsharp=lx=5:ly=5:la=1.2:cx=3:cy=3:ca=0.6,"
                f"eq=brightness=0.03:contrast={template['contrast']}:saturation={template['saturation']}:gamma=0.93,"
                f"curves=preset=increase_contrast,"
                f"colorbalance=rs=.05:gs=-.02:bs=-.03:rm=.02:gm=0:bm=-.02:rh=-.02:gh=.02:bh=.05,"
                f"vignette={template['vignette']},"
                f"format=yuv420p"
            )

            clip_output = output_dir / f"clip_{scene_idx}_{sub_i}.mp4"


            # 길이·루프 판단은 '실제로 넣을 그 소재' 기준이어야 한다.
            # 예전엔 scene.asset_url(대표 소재) 길이로 계산해 놓고 다른 소재를 넣어서,
            # 소재마다 길이가 다르면 엉뚱한 -ss 로 빈 화면이 나올 수 있었다.
            this_dur = get_video_duration(Path(src_asset)) or src_dur
            this_needs_loop = this_dur is not None and this_dur < sub_dur + 0.3

            # 같은 소재를 두 번 이상 쓸 때는 매번 다른 구간을 잘라 쓴다.
            #
            # 예전엔 '소재가 2개 이상이면 각자 처음부터'였다. 그런데 스톡에
            # 주제에 맞는 영상이 거의 없는 주제(예: 돌하르방)에서는 소재를
            # 2~3개밖에 못 모으고, 그걸 6~8컷에 돌려 쓴다. 매번 같은 시작점이라
            # 완성본에서 똑같은 화면이 7번 나왔다(실측: 12컷 중 7컷이 같은 폭포).
            #
            # 소재 수가 컷 수보다 적으면 '몇 번째 재사용인지'를 계산해
            # 그만큼 뒤에서 시작한다. d=1 로 실제 영상이 흐르므로 같은 소재라도
            # 다른 구간은 다른 장면으로 보인다.
            seek_start = 0.0
            _n_assets = max(len(asset_list), 1)
            _reuse_idx = sub_i // _n_assets                    # 0,1,2...
            _reuse_cnt = -(-n_subs // _n_assets)               # 올림
            if (_reuse_cnt > 1 and this_dur
                    and this_dur > sub_dur + 1.0):
                _span = max(this_dur - sub_dur - 0.3, 0.0)
                seek_start = round(
                    _span * _reuse_idx / max(_reuse_cnt - 1, 1), 2)
                if _reuse_idx:
                    logger.info(
                        f"  소재 재사용 {_reuse_idx + 1}/{_reuse_cnt} → "
                        f"{seek_start:.1f}s 지점부터 ({Path(src_asset).name})")

            cmd = ["ffmpeg"]
            if _is_still:
                cmd += ["-loop", "1", "-framerate", "30"]
            elif this_needs_loop:
                cmd += ["-stream_loop", "-1"]
            cmd += [
                "-ss", str(seek_start),
                "-i", src_asset,
                "-t", str(sub_dur),
                "-vf", vf,
                "-c:v", "libx264", "-preset", VIDEO_PRESET, "-crf", str(VIDEO_CRF),
                "-pix_fmt", "yuv420p",   # 구형 플레이어/모바일 호환
                "-movflags", "+faststart", "-an", "-y", str(clip_output)
            ]

            clip_timeout = max(60.0, sub_dur * 20)
            if _run_ffmpeg(cmd, timeout=clip_timeout) and clip_output.exists() and clip_output.stat().st_size >= 4096:
                scene_clips.append(clip_output)
                logger.info(f"  sub-clip OK: {sub_i}/{n_subs} ({sub_dur:.1f}s)")
            else:
                logger.warning(f"  sub-clip failed: {scene_idx}_{sub_i}")

        # Merge sub-clips per scene
        if len(scene_clips) == 1:
            scene_merged = output_dir / f"scene_{scene_idx}_merged.mp4"
            shutil.copy(str(scene_clips[0]), str(scene_merged))
            merged_ok = True
        elif len(scene_clips) > 1:
            scene_merged = output_dir / f"scene_{scene_idx}_merged.mp4"
            merged_ok = xfade_batch(scene_clips, scene_merged)
        else:
            logger.warning(f"scene {scene_idx}: no sub-clips generated")
            continue

        if merged_ok and scene_merged.exists() and scene_merged.stat().st_size > 4096:
            # Trim to exact scene_dur
            scene_final = output_dir / f"scene_{scene_idx}_final.mp4"
            trim_cmd = [
                "ffmpeg", "-i", str(scene_merged), "-t", str(round(scene_dur, 3)),
                "-c:v", "libx264", "-preset", VIDEO_PRESET, "-crf", str(VIDEO_CRF),
                "-movflags", "+faststart", "-an", "-y", str(scene_final)
            ]
            trim_timeout = max(60.0, scene_dur * 5)
            if _run_ffmpeg(trim_cmd, timeout=trim_timeout) and scene_final.exists():
                clips.append(scene_final)
                logger.info(f"scene {scene_idx} OK: trimmed to {scene_dur:.2f}s")
            else:
                logger.warning(f"scene {scene_idx} trim failed")
        else:
            logger.warning(f"scene {scene_idx} merge failed")

    logger.info(f"total clips: {len(clips)}")
    return clips

