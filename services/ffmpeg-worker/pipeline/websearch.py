# -*- coding: utf-8 -*-
"""Google 뉴스 RSS 웹 검색 — 나레이션 스크립트의 사실 근거(grounding) 확보용.

설계 근거 (2026-09 실측):
  · news.google.com/search (HTML) 은 거의 즉시 429, /rss/search 는 200 → RSS만 쓴다.
  · RSS 링크(news.google.com/rss/articles/CBMi...)에는 목적지가 들어 있지 않다.
    2024년 말까지 통하던 base64-protobuf 오프라인 디코딩은 죽었다(실측 30건 중 0건).
    Google 내부 batchexecute RPC(Fbv4je)를 거쳐야 실제 언론사 URL이 나온다.
  · 기사마다 RPC를 따로 쏘면 429가 난다 → 서명(sig/ts)만 기사별로 받고
    디코딩은 전체를 한 번의 POST로 묶는다. (N+1 요청 → N+1회가 아니라 N+1개 중 1개만 RPC)
  · 동의 화면에 걸리면 서명이 없는 페이지가 돌아온다 → CONSENT 쿠키로 1회 재시도.
  · 외부 패키지(gnews-decoder 등)는 신생·미검증이라 의존하지 않고 기법만 직접 구현했다.

원칙: 어떤 단계에서 실패하든 파이프라인을 막지 않는다.
      본문 URL을 못 풀면 제목·언론사·날짜만으로도 실제 출처 근거가 되므로 그 수준으로 강등한다.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from typing import Optional
import xml.etree.ElementTree as ET

import httpx

logger = logging.getLogger(__name__)

GNEWS_RSS = "https://news.google.com/rss/search"
GNEWS_ARTICLE = "https://news.google.com/rss/articles/{aid}"
GNEWS_BATCH = "https://news.google.com/_/DotsSplashUi/data/batchexecute"

_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
       "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
_HEADERS = {"User-Agent": _UA, "Accept-Language": "ko-KR,ko;q=0.9"}
# '동의 결정이 기록된' 상태를 흉내내는 쿠키. 동의 화면에 막혔을 때만 재시도에 쓴다.
_CONSENT_HEADERS = dict(_HEADERS, Cookie="CONSENT=YES+cb.20220419-08-p0.ko+FX+111")

# 시의성 주제 판별용. 이게 걸리면 검색이 큰 값을 하고, 안 걸리면 지연만 늘린다.
_TIME_MARKERS = (
    "오늘", "어제", "이번 주", "이번주", "이달", "최근", "최신", "요즘", "근황",
    "현재", "올해", "뉴스", "속보", "실시간", "화제", "이슈", "동향", "트렌드",
    "전망", "순위", "랭킹", "발표", "출시", "논란",
    "2024", "2025", "2026", "2027",
)

_CACHE: dict[str, tuple[float, list[dict]]] = {}
_CACHE_TTL = 900.0        # 15분. 같은 주제를 연달아 돌릴 때 Google을 두 번 때리지 않는다.
_SIG_CONCURRENCY = 8      # 서명 요청 동시성. 올리면 429 위험.


def needs_search(topic: str) -> bool:
    """시의성 주제인지 판정. auto 모드에서 검색 여부를 가른다."""
    return any(m in topic for m in _TIME_MARKERS)


# 검색어에서 빼야 하는 말들. '오늘자 뉴스'를 그대로 검색하면 Google 이
# 본문에 '오늘자'가 들어간 2018년 기사를 물어온다(실측). 시점은 검색어가 아니라
# when: 연산자로 표현해야 한다.
_QUERY_NOISE = (
    "오늘자", "오늘", "어제", "이번 주", "이번주", "이달", "최신", "최근", "요즘",
    "현재", "실시간", "속보", "뉴스", "소식", "기사", "근황",
    "에 대해", "에 대한", "에 관해", "관련해서", "관련", "알려줘", "만들어줘",
    "정리해줘", "설명해줘", "찾아줘", "검색해서", "서치해서", "디테일하게",
    "만들어줘", "만들어", "해줘", "해봐", "보여줘", "작성해줘", "영상", "스크립트",
)
# 앞 단어가 깎여나가며 홀로 남는 조사·서술어. 검색어에 있으면 결과를 망친다.
_DANGLING = {
    "를", "을", "이", "가", "은", "는", "의", "에", "로", "와", "과", "도", "만",
    "에서", "에게", "부터", "까지", "으로", "라는", "이라는", "대해", "대한", "관한",
}
_RECENT_MARKERS = ("오늘", "어제", "이번 주", "이번주", "최신", "최근", "요즘",
                   "실시간", "속보", "근황", "현재")


def _clean_query(topic: str) -> tuple[str, Optional[str]]:
    """주제를 검색어와 기간 연산자로 분리한다.

    반환: (검색어, when 값 또는 None)
    """
    recent = any(m in topic for m in _RECENT_MARKERS)
    q = topic
    for noise in _QUERY_NOISE:
        q = q.replace(noise, " ")
    q = re.sub(r"[\"'?!.,:;~\-_/|]+", " ", q)
    q = re.sub(r"\s+", " ", q).strip()
    # '뉴스를' 에서 '뉴스'만 빼면 '를' 이 남아 검색어를 오염시킨다.
    q = " ".join(w for w in q.split() if w not in _DANGLING)
    if len(q) < 2:                     # 전부 깎여나갔으면 원문을 쓴다
        q = re.sub(r"\s+", " ", topic).strip()
    return q, ("7d" if recent else None)


def _query_tokens(q: str) -> list:
    """검색어에서 의미 있는 낱말만 뽑는다(2글자 이상, when: 연산자 제외)."""
    q = re.sub(r"when:\S+", " ", q)
    return [w for w in re.split(r"[^0-9A-Za-z가-힣]+", q) if len(w) >= 2]


def _relevant_items(items: list, tokens: list) -> list:
    """제목에 검색어 낱말이 실제로 들어 있는 기사만 남긴다.

    always 모드에서는 추상적이거나 허구인 주제까지 검색하게 되는데, Google 은
    무엇을 물어도 기사 100건을 돌려준다. 그대로 근거로 넣으면 주제와 상관없는
    뉴스가 '사실'로 프롬프트에 박힌다. 근거가 없는 것보다 '틀린 근거'가 훨씬
    나쁘므로, 제목이 검색어와 겹치지 않으면 버린다.
    """
    if not tokens:
        return items
    keep = []
    for x in items:
        title = (x.get("title") or "")
        if any(tok in title for tok in tokens):
            keep.append(x)
    return keep


def _aid_from(url: str) -> Optional[str]:
    m = re.search(r"/(?:rss/)?articles/([A-Za-z0-9_\-]+)", url or "")
    return m.group(1) if m else None


def _unescape(u: str) -> str:
    r"""batchexecute 응답은 JSON 안에 JSON이라 이스케이프가 이중으로 걸려 있다.

    \u003d 만 따로 치환하면 앞의 잉여 백슬래시가 남아 '?key\=123' 같은 깨진 URL이
    나온다(실측). 이중 → 단일 → \uXXXX 일반 복원 → 잔여 백슬래시 제거 순서로 푼다.
    URL 에는 백슬래시가 올 수 없으므로 마지막 제거는 안전하다.
    """
    u = u.replace("\\\\", "\\").replace("\\/", "/")
    u = re.sub(r"\\u([0-9a-fA-F]{4})", lambda m: chr(int(m.group(1), 16)), u)
    return u.replace("\\", "")


async def _fetch_signature(client: httpx.AsyncClient, aid: str, sem: asyncio.Semaphore):
    """기사 페이지에서 디코딩에 필요한 data-n-a-sg / data-n-a-ts 를 뽑는다."""
    async with sem:
        for headers in (_HEADERS, _CONSENT_HEADERS):
            try:
                r = await client.get(GNEWS_ARTICLE.format(aid=aid),
                                     params={"oc": 5}, headers=headers,
                                     follow_redirects=True)
            except Exception as e:
                logger.debug(f"[websearch] 서명 요청 실패 {aid[:18]}: {type(e).__name__}")
                return None
            if "consent.google.com" in str(r.url):
                continue                      # 동의 화면 → 쿠키 붙여 재시도
            sg = re.search(r'data-n-a-sg="([^"]+)"', r.text)
            ts = re.search(r'data-n-a-ts="([^"]+)"', r.text)
            if sg and ts:
                return (aid, int(ts.group(1)), sg.group(1))
        return None


def _envelope(aid: str, ts: int, sg: str, idx: int) -> list:
    inner = json.dumps([
        "garturlreq",
        [["ko", "KR", ["FINANCE_TOP_INDICES", "WEB_TEST_1_0_0"],
          None, None, 1, 1, "KR:ko", None, None, None, None, None, None, None, 0],
         "ko", "KR", 1, [2, 4, 8], 1, 1, None, 0, 0, None, 0],
        aid, ts, sg,
    ])
    return ["Fbv4je", inner, None, str(idx)]


async def _batch_decode(client: httpx.AsyncClient, sigs: list) -> dict:
    """서명 묶음을 한 번의 POST로 보내 {aid: 실제URL} 을 받는다."""
    if not sigs:
        return {}
    payload = [_envelope(a, t, s, i + 1) for i, (a, t, s) in enumerate(sigs)]
    try:
        r = await client.post(
            GNEWS_BATCH,
            headers={**_HEADERS,
                     "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8"},
            data={"f.req": json.dumps([payload])},
        )
    except Exception as e:
        logger.warning(f"[websearch] batchexecute 실패: {type(e).__name__} {e}")
        return {}
    if r.status_code != 200:
        logger.warning(f"[websearch] batchexecute HTTP {r.status_code}")
        return {}

    by_idx: dict[int, str] = {}
    for part in r.text.split('"Fbv4je"')[1:]:
        mu = re.search(r'(https?:(?:\\?/){2}.*?)\\"', part)
        mi = re.search(r'null,null,null,"(\d+)"', part)
        if mu and mi:
            by_idx[int(mi.group(1))] = _unescape(mu.group(1))
    return {sigs[i - 1][0]: u for i, u in by_idx.items() if 1 <= i <= len(sigs)}


async def search_news(query: str, limit: int = 6, ceid: str = "KR:ko",
                      timeout: float = 25.0) -> list[dict]:
    """주제로 뉴스를 검색해 [{title, publisher, published, url}] 을 돌려준다.

    url 은 실제 언론사 주소. 디코딩에 실패한 항목은 url=None 이지만 제목·언론사·
    날짜는 살아 있으므로 그것만으로도 근거로 쓸 수 있다.
    """
    query, when = _clean_query(query)
    if when:
        query = f"{query} when:{when}"
    key = f"{query}|{limit}|{ceid}"
    hit = _CACHE.get(key)
    if hit and (time.time() - hit[0]) < _CACHE_TTL:
        logger.info(f"[websearch] 캐시 적중: {query[:30]}")
        return hit[1]

    lang, country = (ceid.split(":")[1], ceid.split(":")[0]) if ":" in ceid else ("ko", "KR")
    t0 = time.time()
    try:
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            r = await client.get(GNEWS_RSS, headers=_HEADERS,
                                 params={"q": query, "hl": lang, "gl": country, "ceid": ceid})
            if r.status_code != 200:
                logger.warning(f"[websearch] RSS HTTP {r.status_code}")
                return []
            try:
                items = ET.fromstring(r.text).findall(".//item")
            except ET.ParseError as e:
                logger.warning(f"[websearch] RSS 파싱 실패: {e}")
                return []

            results: list[dict] = []
            for it in items[:limit]:
                title = (it.findtext("title") or "").strip()
                src = it.find("source")
                results.append({
                    "title": title,
                    "publisher": (src.text if src is not None else "") or "",
                    "published": (it.findtext("pubDate") or "").strip(),
                    "google_url": (it.findtext("link") or "").strip(),
                    "url": None,
                })
            if not results:
                logger.info(f"[websearch] 검색 결과 없음: {query[:40]}")
                return []

            # 제목이 검색어와 겹치는 기사만 남긴다. 2건 미만이면 이 주제에
            # 관한 보도가 없다고 보고 근거 없이 진행한다(무관한 기사보다 낫다).
            _tok = _query_tokens(query)
            _rel = _relevant_items(results, _tok)
            if len(_rel) < 2:
                logger.info(
                    f"[websearch] '{query[:30]}' 관련 기사 부족"
                    f"({len(_rel)}/{len(results)}건 일치) → 근거로 쓰지 않음"
                )
                _CACHE[key] = (time.time(), [])
                return []
            if len(_rel) < len(results):
                logger.info(f"[websearch] 무관한 기사 {len(results) - len(_rel)}건 제외")
            results = _rel

            aids = [(_aid_from(x["google_url"]), x) for x in results]
            valid = [(a, x) for a, x in aids if a]
            sem = asyncio.Semaphore(_SIG_CONCURRENCY)
            sigs_raw = await asyncio.gather(
                *[_fetch_signature(client, a, sem) for a, _ in valid],
                return_exceptions=True,
            )
            sigs = [s for s in sigs_raw if isinstance(s, tuple)]
            resolved = await _batch_decode(client, sigs)

        for aid, item in valid:
            item["url"] = resolved.get(aid)
    except Exception as e:
        logger.warning(f"[websearch] 검색 오류: {type(e).__name__} {e}")
        return []

    ok = sum(1 for x in results if x["url"])
    logger.info(f"[websearch] '{query[:34]}' {len(results)}건 / URL해석 {ok}건 "
                f"({time.time() - t0:.1f}s)")
    _CACHE[key] = (time.time(), results)
    return results


def format_headlines(items: list[dict], limit: int = 8) -> str:
    """URL 본문을 못 얻었을 때 쓰는 최소 근거 — 제목·언론사·날짜는 모두 실제 출처다."""
    lines = []
    for x in items[:limit]:
        if not x.get("title"):
            continue
        meta = " / ".join(p for p in (x.get("publisher"), (x.get("published") or "")[:16]) if p)
        lines.append(f"- {x['title']}" + (f"  ({meta})" if meta else ""))
    if not lines:
        return ""
    return "[웹 검색 헤드라인 — 실제 보도된 사실]\n" + "\n".join(lines)