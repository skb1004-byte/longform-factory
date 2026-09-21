# -*- coding: utf-8 -*-
"""BGM 폴더 점검.

/data/bgm 의 음원이 실제로 쓸 만한지 검사한다.

왜 필요한가: 파이프라인은 믹싱할 때 BGM 을 loudnorm 으로 목표 음량에 맞춘다.
그런데 원본이 거의 무음이면(실측: auto_bgm_economy.mp3 = -53.7 LUFS, 피크 -38dB)
끌어올려도 음악이 아니라 잡음이 커진다. 그래서 믹싱 단계에 -45 LUFS 미만 차단
가드를 뒀고, 그 결과 BGM 없이 나가면서 완성본 무음 비율이 17~38% 가 됐다.

음원을 넣기 전에 이 검사를 돌리면 '넣었는데 왜 소리가 안 나지' 를 겪지 않는다.

  docker exec lf2_ffmpeg python /app/pipeline/bgm_check.py
"""

from __future__ import annotations

import glob
import os
import re
import subprocess
import sys

GOOD_MIN, GOOD_MAX = -23.0, -9.0      # 일반적인 음악 마스터 범위
USABLE_MIN = -45.0                     # 믹싱 가드와 같은 기준


def measure(path: str) -> dict:
    out = {"path": path, "size_kb": os.path.getsize(path) // 1024}
    r = subprocess.run(
        ["ffprobe", "-v", "error", "-show_entries",
         "format=duration,bit_rate", "-of", "csv=p=0", path],
        capture_output=True, text=True, timeout=30)
    parts = (r.stdout.strip() or ",").split(",")
    try:
        out["duration"] = float(parts[0])
    except Exception:
        out["duration"] = 0.0
    try:
        out["kbps"] = int(parts[1]) // 1000
    except Exception:
        out["kbps"] = 0

    r2 = subprocess.run(
        ["ffmpeg", "-hide_banner", "-nostats", "-i", path,
         "-af", "loudnorm=I=-14:TP=-1.5:LRA=11:print_format=json",
         "-f", "null", "-"],
        capture_output=True, text=True, timeout=300)
    m = re.findall(r'"input_i"\s*:\s*"(-?[\d.]+)"', r2.stderr)
    out["lufs"] = float(m[-1]) if m else None
    m2 = re.findall(r'"input_tp"\s*:\s*"(-?[\d.]+)"', r2.stderr)
    out["peak_db"] = float(m2[-1]) if m2 else None
    return out


def verdict(d: dict) -> tuple:
    if d["lufs"] is None:
        return "측정불가", "파일이 손상됐거나 오디오 스트림이 없다"
    if d["lufs"] < USABLE_MIN:
        return "사용불가", (f"{d['lufs']:.1f} LUFS — 사실상 무음. 끌어올리면 "
                         f"잡음만 커지므로 파이프라인이 자동으로 건너뛴다")
    if d["lufs"] < GOOD_MIN:
        return "조용함", f"{d['lufs']:.1f} LUFS — 쓸 수는 있으나 원본이 작다"
    if d["lufs"] > GOOD_MAX:
        return "너무큼", f"{d['lufs']:.1f} LUFS — 나레이션을 덮을 수 있다"
    if d["duration"] < 30:
        return "짧음", f"{d['duration']:.0f}초 — 30초 이상 권장"
    return "정상", f"{d['lufs']:.1f} LUFS"


def main(folder: str = "/data/bgm") -> int:
    files = sorted(
        f for f in glob.glob(os.path.join(folder, "*"))
        if f.lower().endswith((".mp3", ".wav", ".m4a", ".ogg", ".flac"))
    )
    if not files:
        print(f"{folder} 에 음원이 없습니다.")
        print("정상 음원(-23 ~ -9 LUFS, 30초 이상)을 넣어주세요.")
        return 1

    ok = 0
    print(f"{'파일':<34} {'판정':<8} {'길이':>7} {'음량':>9} {'피크':>8} {'비트레이트':>8}")
    print("-" * 82)
    for f in files:
        d = measure(f)
        v, why = verdict(d)
        if v == "정상":
            ok += 1
        lufs = f"{d['lufs']:.1f}" if d["lufs"] is not None else "?"
        peak = f"{d['peak_db']:.1f}" if d["peak_db"] is not None else "?"
        print(f"{os.path.basename(f)[:33]:<34} {v:<8} "
              f"{d['duration']:>6.0f}s {lufs:>8} {peak:>7} {d['kbps']:>6}k")
        if v != "정상":
            print(f"    └ {why}")

    print("-" * 82)
    print(f"쓸 수 있는 음원: {ok} / {len(files)}")
    if ok == 0:
        print("\n지금 상태로는 모든 영상이 BGM 없이 나갑니다.")
        print("→ 완성본 무음 비율이 12% 를 넘어 QC 불합격이 계속됩니다.")
    return 0 if ok else 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "/data/bgm"))