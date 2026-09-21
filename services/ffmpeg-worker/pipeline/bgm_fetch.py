# -*- coding: utf-8 -*-
"""Jamendo 에서 BGM 을 수급한다.

왜 라이선스를 이렇게 까다롭게 거르는가
---------------------------------------
Jamendo 는 크리에이티브 커먼즈 음원 창고다. 곡마다 조건이 다르고, 그중에는
상업적 이용이 금지된 것(NonCommercial)과 변형이 금지된 것(NoDerivatives)이
섞여 있다. 수익 창출하는 유튜브 영상에 NC 곡을 깔면 라이선스 위반이다.
영상에 음악을 깔고 나레이션과 믹싱하는 것은 '변형'으로 볼 여지가 있어
ND 곡도 뺀다. API 파라미터로 원천 차단한다:

    ccnc=false   NonCommercial 제외 → 상업적 이용 가능한 곡만
    ccnd=false   NoDerivatives 제외 → 믹싱 가능한 곡만

그래도 대부분은 CC-BY 다. CC-BY 는 **출처를 반드시 표시해야 한다**.
표시하지 않으면 라이선스가 소멸하고 그냥 무단 사용이 된다. 그래서 이 모듈은
음원만 받고 끝내지 않고, 곡마다 아티스트·곡명·라이선스 URL 을 모아
attribution.md 에 적는다. 그 내용을 영상 설명란에 넣어야 한다.

한 곡이라도 이 표에서 빠지면 그 곡은 쓰면 안 된다.

쓰는 법
-------
    docker exec -e JAMENDO_CLIENT_ID=xxxx lf2_ffmpeg \
        python /app/pipeline/bgm_fetch.py --limit 20
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import urllib.parse
import urllib.request
from pathlib import Path

API = "https://api.jamendo.com/v3.0/tracks/"
BGM_DIR = Path(os.getenv("BGM_DIR", "/data/bgm"))
ATTRIB = BGM_DIR / "attribution.md"

# bgm_check.py 와 같은 기준
USABLE_MIN_LUFS = -45.0
GOOD_MIN, GOOD_MAX = -23.0, -9.0


def classify_license(url: str) -> tuple:
    """라이선스 URL 로 '써도 되는지' 판정. (판정, 짧은이름, 사유)

    ccnc/ccnd 파라미터만으로는 부족하다. 실측으로 확인한 구멍:
      - CC BY-SA (ShareAlike) 가 그대로 통과한다. SA 는 카피레프트라
        이 음악을 넣은 영상 자체를 같은 조건으로 공개해야 한다는 해석이 가능하다.
        수익 창출하는 채널에 쓰기에는 위험하다.
      - Art Libre(LAL) 도 같은 성격의 카피레프트다.
      - license_ccurl 이 빈 곡이 섞여 온다. 조건을 확인할 수 없으면 쓰면 안 된다.

    그래서 '무엇을 뺄지'가 아니라 '무엇만 쓸지'로 뒤집는다.
    허용: CC0 / Public Domain / CC BY (SA·NC·ND 가 붙지 않은 순수 BY)
    """
    u = (url or "").strip().lower()
    if not u:
        return False, "?", "라이선스 정보 없음 — 조건을 확인할 수 없다"
    if "artlibre" in u or "/lal" in u:
        return False, "Art Libre", "카피레프트 — 영상까지 같은 조건이 될 수 있다"
    if "publicdomain" in u or "/zero/" in u or "/cc0" in u:
        return True, "CC0", ""
    if "/licenses/by-sa" in u:
        return False, "CC BY-SA", "ShareAlike(카피레프트) — 수익 채널에 위험"
    if "/licenses/by-nc" in u:
        return False, "CC BY-NC", "상업적 이용 금지"
    if "/licenses/by-nd" in u:
        return False, "CC BY-ND", "변형 금지 — 믹싱 불가"
    if "/licenses/by/" in u:
        return True, "CC BY", ""
    return False, u.rstrip("/").split("/")[-2] if "/" in u else u, "알 수 없는 라이선스"


def _safe_name(s: str) -> str:
    s = re.sub(r"[^\w\s가-힣-]", "", s).strip()
    return re.sub(r"\s+", "_", s)[:60] or "track"


def search(client_id: str, limit: int, tags: str, order: str) -> list:
    params = {
        "client_id": client_id,
        "format": "json",
        "limit": str(limit),
        "order": order,
        "include": "musicinfo+licenses",
        "audioformat": "mp32",
        "vocalinstrumental": "instrumental",   # 보컬은 나레이션과 겹친다
        "durationbetween": "60_600",           # 너무 짧으면 반복이 티 난다
        "ccnc": "false",                       # 상업적 이용 금지 곡 제외
        "ccnd": "false",                       # 변형 금지 곡 제외
    }
    if tags:
        params["fuzzytags"] = tags
    url = API + "?" + urllib.parse.urlencode(params)
    with urllib.request.urlopen(url, timeout=40) as r:
        data = json.load(r)
    head = data.get("headers", {})
    if head.get("status") != "success":
        raise RuntimeError(f"Jamendo API 오류: {head.get('error_message') or head}")
    return data.get("results", [])


def search_many(client_id: str, limit: int, tags_csv: str, order: str) -> list:
    """태그를 하나씩 따로 검색해 합친다.

    fuzzytags 에 'ambient+cinematic' 처럼 여러 개를 주면 Jamendo 는 AND 로
    해석한다. 두 태그를 모두 단 곡은 거의 없어서 실측 결과가 0건이었다.
    태그별로 따로 받아 합치는 편이 실제로 곡을 얻는다.
    """
    tags = [t.strip() for t in re.split(r"[+,]", tags_csv or "") if t.strip()]
    if not tags:
        return search(client_id, limit, "", order)
    per = max(3, -(-limit // len(tags)))     # 올림 나눗셈
    seen, out = set(), []
    for t in tags:
        try:
            got = search(client_id, per, t, order)
        except Exception as e:
            print(f"  태그 '{t}' 검색 실패: {e}")
            continue
        print(f"  태그 '{t}': {len(got)}곡")
        for tr in got:
            if tr.get("id") in seen:
                continue
            seen.add(tr.get("id"))
            out.append(tr)
        if len(out) >= limit:
            break
    return out[:limit]


def measure_lufs(path: Path):
    r = subprocess.run(
        ["ffmpeg", "-hide_banner", "-nostats", "-i", str(path),
         "-af", "loudnorm=I=-14:TP=-1.5:LRA=11:print_format=json",
         "-f", "null", "-"],
        capture_output=True, text=True, timeout=300)
    m = re.findall(r'"input_i"\s*:\s*"(-?[\d.]+)"', r.stderr)
    return float(m[-1]) if m else None


def download(track: dict) -> Path | None:
    url = track.get("audiodownload") or track.get("audio")
    if not url:
        return None
    name = f"jam_{track['id']}_{_safe_name(track.get('name', ''))}.mp3"
    dest = BGM_DIR / name
    if dest.exists() and dest.stat().st_size > 8192:
        return dest
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "longform-factory/1.0"})
        with urllib.request.urlopen(req, timeout=120) as r, open(dest, "wb") as f:
            f.write(r.read())
    except Exception as e:
        print(f"    다운로드 실패: {e}")
        return None
    if dest.stat().st_size < 8192:
        dest.unlink(missing_ok=True)
        return None
    return dest


def write_attribution(rows: list) -> None:
    """CC-BY 는 출처 표시가 의무다. 빠뜨리면 그냥 무단 사용이 된다."""
    lines = [
        "# BGM 출처 표시",
        "",
        "아래 내용을 이 음원을 쓴 영상의 **설명란에 그대로** 넣어야 합니다.",
        "CC-BY 는 출처를 밝히는 조건으로 무료입니다 — 밝히지 않으면 조건 위반입니다.",
        "",
        "| 파일 | 곡 | 아티스트 | 라이선스 | 곡 주소 |",
        "|---|---|---|---|---|",
    ]
    for r in rows:
        lines.append(
            f"| `{r['file']}` | {r['name']} | {r['artist']} | "
            f"[{r['license_short']}]({r['license_url']}) | {r['share_url']} |")
    lines += ["", "## 설명란 붙여넣기용", "```"]
    for r in rows:
        lines.append(f"{r['name']} by {r['artist']} — {r['license_short']} — {r['share_url']}")
    lines.append("```")
    ATTRIB.write_text("\n".join(lines), encoding="utf-8")
    print(f"\n출처 표시를 {ATTRIB} 에 적었습니다. 영상 설명란에 꼭 넣으세요.")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=20)
    ap.add_argument("--tags", default="ambient,cinematic,calm,documentary,inspiring")
    ap.add_argument("--order", default="popularity_total")
    ap.add_argument("--dry-run", action="store_true", help="받지 않고 목록만 본다")
    a = ap.parse_args()

    cid = os.getenv("JAMENDO_CLIENT_ID", "").strip()
    if not cid:
        print("JAMENDO_CLIENT_ID 가 없습니다.")
        print("https://devportal.jamendo.com 에서 앱을 만들면 client_id 가 나옵니다.")
        return 1

    BGM_DIR.mkdir(parents=True, exist_ok=True)
    print(f"검색: tags={a.tags} limit={a.limit} (상업이용 가능 + 변형 가능 + 보컬 없음)")
    try:
        tracks = search_many(cid, a.limit, a.tags, a.order)
    except Exception as e:
        print(f"검색 실패: {e}")
        return 1
    print(f"후보 {len(tracks)}곡\n")

    rows, kept = [], 0
    for t in tracks:
        lic = (t.get("license_ccurl") or "").strip()
        title = t.get("name", "?")
        artist = t.get("artist_name", "?")
        allowed, license_short, why = classify_license(lic)

        if not allowed:
            print(f"  제외 [{license_short}] {title} — {why}")
            continue

        if a.dry_run:
            print(f"  [dry] 사용가능 [{license_short}] {title} / {artist}")
            continue

        print(f"  받는 중: {title} / {artist}")
        p = download(t)
        if not p:
            continue
        lufs = measure_lufs(p)
        if lufs is None or lufs < USABLE_MIN_LUFS:
            print(f"    버림 — 음량 {lufs} LUFS (사실상 무음)")
            p.unlink(missing_ok=True)
            continue
        flag = "" if GOOD_MIN <= lufs <= GOOD_MAX else "  (음량이 표준 범위 밖)"
        print(f"    OK {lufs:.1f} LUFS{flag}")
        kept += 1
        rows.append({
            "file": p.name, "name": title, "artist": artist,
            "license_short": license_short,
            "license_url": lic or "https://creativecommons.org/",
            "share_url": t.get("shareurl", ""),
        })

    if rows:
        write_attribution(rows)
    print(f"\n확보한 음원: {kept}곡")
    return 0 if (kept or a.dry_run) else 2


if __name__ == "__main__":
    sys.exit(main())