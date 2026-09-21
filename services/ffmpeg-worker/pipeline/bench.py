# -*- coding: utf-8 -*-
"""고정 스크립트 벤치 — 코드 변경의 효과만 보기 위한 도구.

왜 필요한가
-----------
파이프라인을 통째로 돌려 화면을 비교하면, 매 실행마다 LLM 이 스크립트를
새로 쓰기 때문에 씬 키워드가 달라지고 화면도 달라진다. 그 상태로 "좋아졌다,
나빠졌다"를 판단하면 코드 변경의 효과가 아니라 난수를 보게 된다.
실제로 v89~v96 비교가 그 함정에 빠졌다.

이 도구는 스크립트·나레이션을 고정해 두고 Step 4(소재) 이후만 다시 돌린다.
입력이 같으므로 결과 차이는 코드 변경에서만 온다.

쓰는 법
-------
  # 1) 잘 나온 잡에서 픽스처를 뜬다 (한 번만)
  python /app/pipeline/bench.py capture <원본_job_id> <픽스처이름>

  # 2) 코드를 고칠 때마다 같은 픽스처로 돌린다
  python /app/pipeline/bench.py run <픽스처이름> <라벨>

  # 3) 결과 비교
  python /app/pipeline/bench.py report
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

JOBS = Path("/data/jobs")
TMP = Path("/data/tmp")
OUT = Path("/data/output")
# 픽스처·결과는 반드시 '마운트된' 경로에 둔다.
# /data/bench 는 컨테이너 내부라 재빌드하면 통째로 날아간다(실측: 한 번 잃음).
# /data/jobs 는 E 드라이브에 마운트돼 있어 이미지 교체와 무관하게 남는다.
FIX = Path("/data/jobs/_bench/fixtures")
RES = Path("/data/jobs/_bench/results")
API = "http://localhost:8002"
KEY = os.getenv("LF_API_KEY", "longform-2026-secret")


def _run(cmd, timeout=120):
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)


# ── 픽스처 ────────────────────────────────────────────────────────────
def capture(job_id: str, name: str) -> int:
    """돌아간 잡에서 스크립트·나레이션을 떠서 픽스처로 저장한다."""
    src_scenes = JOBS / job_id / "scenes.json"
    src_mp3 = TMP / f"{job_id}.mp3"
    src_ts = TMP / f"{job_id}_timestamps.json"
    missing = [str(p) for p in (src_scenes, src_mp3, src_ts) if not p.exists()]
    if missing:
        print("없는 파일:", missing)
        return 1
    d = FIX / name
    d.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src_scenes, d / "scenes.json")
    shutil.copy2(src_mp3, d / "narration.mp3")
    shutil.copy2(src_ts, d / "timestamps.json")

    scenes = json.loads((d / "scenes.json").read_text(encoding="utf-8"))
    meta = {
        "source_job": job_id,
        "n_scenes": len(scenes),
        "narration_chars": sum(len(s.get("narration") or "") for s in scenes),
        "keywords": [s.get("keyword") for s in scenes],
    }
    (d / "meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2),
                                 encoding="utf-8")
    print(f"픽스처 '{name}' 저장: {meta['n_scenes']}씬 "
          f"{meta['narration_chars']}자")
    for k in meta["keywords"]:
        print(f"   - {k}")
    return 0


# ── 실행 ──────────────────────────────────────────────────────────────
def _seed_job(name: str, job_id: str, topic: str) -> None:
    """픽스처를 잡 디렉터리에 심고 Step 1~3 을 '완료'로 표시한다."""
    d = FIX / name
    jd = JOBS / job_id
    jd.mkdir(parents=True, exist_ok=True)
    shutil.copy2(d / "scenes.json", jd / "scenes.json")
    shutil.copy2(d / "narration.mp3", TMP / f"{job_id}.mp3")
    shutil.copy2(d / "timestamps.json", TMP / f"{job_id}_timestamps.json")
    # state.json 포맷은 {"stages": {단계: {"completed_at":..., "payload":{...}}}}
    # payload 를 한 겹 감싸지 않으면 has() 는 통과해도 payload 를 읽는 쪽이 깨진다.
    from datetime import datetime
    now = datetime.utcnow().isoformat() + "Z"
    n = len(json.loads((d / "scenes.json").read_text(encoding="utf-8")))
    state = {
        "job_id": job_id,
        "created_at": now,
        "stages": {
            "scenes_loaded": {"completed_at": now, "payload": {"count": n}},
            "tts_done": {"completed_at": now,
                         "payload": {"mp3": str(TMP / f"{job_id}.mp3")}},
        },
    }
    (jd / "state.json").write_text(json.dumps(state, ensure_ascii=False, indent=2),
                                   encoding="utf-8")


def run(name: str, label: str, topic: str = "", video_type: str = "shorts") -> int:
    d = FIX / name
    if not (d / "scenes.json").exists():
        print(f"픽스처 '{name}' 가 없습니다. 먼저 capture 하세요.")
        return 1
    meta = json.loads((d / "meta.json").read_text(encoding="utf-8"))
    topic = topic or meta.get("topic") or "벤치"
    job_id = f"bench_{name}_{label}"
    # 이전 실행 흔적 제거 — 캐시가 남으면 비교가 무의미해진다
    shutil.rmtree(JOBS / job_id, ignore_errors=True)
    shutil.rmtree(TMP / job_id, ignore_errors=True)
    _seed_job(name, job_id, topic)

    body = json.dumps({
        "job_id": job_id, "topic": topic, "video_type": video_type,
        "duration_sec": 30, "tone": "차분한", "image_mode": "stock", "style": "",
    }).encode()
    req = urllib.request.Request(
        f"{API}/video/auto", data=body,
        headers={"Content-Type": "application/json", "X-LF-API-Key": KEY})
    t0 = time.time()
    with urllib.request.urlopen(req, timeout=60) as r:
        print("제출:", json.load(r).get("status"), job_id)

    last = ""
    while time.time() - t0 < 1800:
        time.sleep(10)
        try:
            with urllib.request.urlopen(
                    urllib.request.Request(f"{API}/video/{job_id}/status",
                                           headers={"X-LF-API-Key": KEY}),
                    timeout=20) as r:
                st = json.load(r)
        except Exception:
            continue
        if st.get("step") != last:
            last = st.get("step")
            print(f"   {int(time.time()-t0):>4}s  {last}")
        if st.get("status") in ("completed", "failed"):
            break
    elapsed = time.time() - t0
    print(f"소요 {elapsed:.0f}초")
    return measure(job_id, name, label, elapsed)


# ── 측정 ──────────────────────────────────────────────────────────────
def _find_output(job_id: str):
    for sub in ("shorts", "longform"):
        for p in (OUT / sub).glob(f"*{job_id[-8:]}*.mp4"):
            return p
    hits = sorted(OUT.rglob("*.mp4"), key=lambda p: p.stat().st_mtime)
    return hits[-1] if hits else None


def measure(job_id: str, name: str, label: str, elapsed: float = 0.0) -> int:
    p = _find_output(job_id)
    if not p:
        print("완성본을 찾지 못했습니다.")
        return 2
    RES.mkdir(parents=True, exist_ok=True)

    r = _run(["ffprobe", "-v", "error", "-show_entries",
              "format=duration", "-of", "csv=p=0", str(p)])
    dur = float(r.stdout.strip() or 0)

    def _sum(pattern, args):
        rr = _run(["ffmpeg", "-hide_banner", "-nostats", "-i", str(p)]
                  + args + ["-f", "null", "-"], timeout=300)
        return sum(float(x) for x in re.findall(pattern, rr.stderr))

    black = _sum(r"black_duration:\s*([\d.]+)",
                 ["-vf", "blackdetect=d=0.15:pix_th=0.12", "-an"])
    sil = _sum(r"silence_duration:\s*([\d.]+)",
               ["-af", "silencedetect=n=-45dB:d=0.4", "-vn"])
    rr = _run(["ffmpeg", "-hide_banner", "-nostats", "-i", str(p), "-af",
               "loudnorm=I=-14:TP=-1.5:LRA=11:print_format=json",
               "-f", "null", "-"], timeout=300)
    m = re.findall(r'"input_i"\s*:\s*"(-?[\d.]+)"', rr.stderr)
    lufs = float(m[-1]) if m else None

    # 화면이 얼마나 바뀌는가 — 같은 소재 반복을 잡는 지표
    rr2 = _run(["ffmpeg", "-i", str(p), "-vf",
                "fps=2,signalstats,metadata=print:key=lavfi.signalstats.YDIF",
                "-f", "null", "-"], timeout=300)
    yd = [float(x) for x in re.findall(r"YDIF=([\d.]+)", rr2.stderr)]

    assets = list((JOBS / job_id / "assets").glob("*")) if (JOBS / job_id / "assets").exists() else []
    n_assets = len({a.name for a in assets if a.suffix.lower() in (".mp4", ".png", ".jpg")})

    sheet = RES / f"{name}__{label}.png"
    _run(["ffmpeg", "-v", "error", "-i", str(p), "-vf",
          f"fps=1/{max(dur/12,1):.2f},scale=190:-2,tile=6x2",
          "-frames:v", "1", "-y", str(sheet)], timeout=180)

    rec = {
        "fixture": name, "label": label, "file": p.name,
        "elapsed_sec": round(elapsed),
        "duration": round(dur, 2),
        "black_pct": round(black / dur * 100, 2) if dur else None,
        "silence_pct": round(sil / dur * 100, 2) if dur else None,
        "lufs": lufs,
        "motion_ydif": round(sum(yd) / len(yd), 2) if yd else None,
        "n_assets": n_assets,
        "sheet": sheet.name,
    }
    (RES / f"{name}__{label}.json").write_text(
        json.dumps(rec, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(rec, ensure_ascii=False, indent=2))
    return 0


def report() -> int:
    RES.mkdir(parents=True, exist_ok=True)
    recs = []
    for f in sorted(RES.glob("*.json")):
        try:
            recs.append(json.loads(f.read_text(encoding="utf-8")))
        except Exception:
            pass
    if not recs:
        print("결과가 없습니다.")
        return 1
    hdr = f"{'픽스처':<12}{'라벨':<14}{'길이':>7}{'검은%':>7}{'무음%':>7}{'LUFS':>7}{'움직임':>7}{'소재':>5}{'초':>6}"
    print(hdr); print("-" * len(hdr))
    for r in recs:
        print(f"{r['fixture'][:11]:<12}{r['label'][:13]:<14}"
              f"{r['duration']:>7.1f}{r['black_pct']:>7.2f}{r['silence_pct']:>7.2f}"
              f"{(r['lufs'] or 0):>7.1f}{(r['motion_ydif'] or 0):>7.2f}"
              f"{r['n_assets']:>5}{r['elapsed_sec']:>6}")
    # 눈으로 비교할 HTML
    rows = "".join(
        f"<h3>{r['fixture']} / {r['label']} — {r['duration']}s, "
        f"검은 {r['black_pct']}%, 무음 {r['silence_pct']}%, "
        f"움직임 {r['motion_ydif']}, 소재 {r['n_assets']}개</h3>"
        f"<img src='bench/{r['sheet']}'>" for r in recs)
    html = ("<!doctype html><meta charset=utf-8><title>bench</title>"
            "<style>body{background:#111;color:#eee;font-family:system-ui;padding:10px}"
            "img{max-width:100%;border:1px solid #444;display:block;margin-bottom:14px}"
            "h3{font-size:12px;margin:0 0 4px}</style>" + rows)
    Path("/data/output/bench_report.html").write_text(html, encoding="utf-8")
    print("\nHTML: /data/output/bench_report.html  (static 으로 복사해 볼 것)")
    return 0


if __name__ == "__main__":
    a = sys.argv[1:]
    if not a:
        print(__doc__); sys.exit(1)
    cmd = a[0]
    if cmd == "capture" and len(a) >= 3:
        sys.exit(capture(a[1], a[2]))
    if cmd == "run" and len(a) >= 3:
        sys.exit(run(a[1], a[2], topic=(a[3] if len(a) > 3 else "")))
    if cmd == "measure" and len(a) >= 4:
        sys.exit(measure(a[1], a[2], a[3]))
    if cmd == "report":
        sys.exit(report())
    print(__doc__)
    sys.exit(1)