# -*- coding: utf-8 -*-
"""잡 취소.

왜 필요한가
-----------
파이프라인은 한 번에 한 잡만 돈다(_CURRENT_JOB). 그래서 잡 하나가 오래
걸리거나 잘못 들어가면 사용자는 끝날 때까지 아무것도 할 수 없다.
소재 수집만 6분이 걸리기도 하고, 주제를 잘못 넣었다는 걸 30초 만에
알아차려도 되돌릴 방법이 없었다.

어떻게 동작하나
---------------
강제 종료(kill)는 하지 않는다. ffmpeg 가 파일을 쓰는 중에 끊으면 깨진
중간 파일이 남고, 다음 실행이 그걸 캐시로 오인한다.
대신 '취소 요청' 표시를 남기고, 파이프라인이 단계 경계마다 확인해서
스스로 빠져나온다. 단계 사이에서 멈추므로 파일 상태가 항상 온전하다.

반응 시간은 그 단계가 끝나는 데 달렸다. 렌더 중이면 서브클립 하나(3~8초),
소재 수집 중이면 소재 하나(5~20초) 안에 멈춘다.
"""

from __future__ import annotations

import logging
from typing import Set

logger = logging.getLogger(__name__)

_CANCELLED: Set[str] = set()


class JobCancelled(BaseException):
    """사용자가 중지를 눌렀다. 실패가 아니라 정상적인 종료 경로.

    Exception 이 아니라 BaseException 을 상속하는 이유:
    파이프라인 곳곳에 `except Exception` 으로 넓게 받아 로그만 찍고 넘어가는
    자리가 많다(소재 하나 실패해도 전체는 계속 가야 하므로 그게 맞다).
    JobCancelled 가 Exception 이면 그 중 아무 데나 하나에 걸려 조용히
    삼켜지고, 사용자는 중지를 눌렀는데 영상이 계속 만들어진다.
    asyncio.CancelledError 가 BaseException 인 것과 같은 이유다.
    """


_MAX_KEEP = 50


def request_cancel(job_id: str) -> None:
    if not job_id:
        return
    # 돌지 않는 잡에 취소를 걸면 그 표시는 지워 줄 사람이 없다(clear 는 잡이
    # 시작하거나 끝날 때만 불린다). 그대로 두면 계속 쌓이므로 상한을 둔다.
    if len(_CANCELLED) >= _MAX_KEEP:
        _CANCELLED.clear()
        logger.info("[cancel] 오래된 취소 표시 정리")
    _CANCELLED.add(job_id)
    logger.warning(f"[cancel] 취소 요청: {job_id}")


def clear(job_id: str) -> None:
    """잡이 끝났거나 새로 시작할 때 표시를 지운다."""
    _CANCELLED.discard(job_id)


def is_cancelled(job_id: str) -> bool:
    return job_id in _CANCELLED


def check(job_id: str, where: str = "") -> None:
    """단계 경계에서 부른다. 취소 요청이 있으면 예외로 빠져나간다."""
    if job_id in _CANCELLED:
        logger.warning(f"[cancel] {job_id} 중지 — {where or '단계 경계'}")
        raise JobCancelled(where or job_id)


def pending() -> list:
    return sorted(_CANCELLED)

# ── 현재 활성 잡 ──────────────────────────────────────────────
# 소재 수집 루프나 렌더 루프까지 job_id 를 인자로 끌고 내려가면 시그니처가
# 지저분해진다. 파이프라인은 어차피 한 번에 하나만 돌기 때문에(_CURRENT_JOB),
# '지금 도는 잡'을 여기 적어 두고 깊은 곳에서는 인자 없이 확인한다.

_ACTIVE: str = ""


def set_active(job_id: str) -> None:
    global _ACTIVE
    _ACTIVE = job_id or ""


def check_active(where: str = "") -> None:
    if _ACTIVE:
        check(_ACTIVE, where)

async def race(coro, where: str = "", poll: float = 1.0):
    """긴 단일 await 를 취소 요청과 경주시킨다.

    체크(check/check_active)는 '작업과 작업 사이'에서만 걸린다. 그런데
    TTS 합성처럼 한 번의 호출이 통째로 1분 넘게 걸리는 자리가 있다.
    거기서는 중지를 눌러도 그 호출이 끝날 때까지 아무 일도 안 일어난다.

    이 함수는 1초마다 취소 표시를 확인하면서 코루틴을 기다리다가,
    취소가 걸리면 해당 작업을 cancel 하고 JobCancelled 를 던진다.
    """
    import asyncio
    task = asyncio.ensure_future(coro)
    while True:
        done, _pending = await asyncio.wait({task}, timeout=poll)
        if task in done:
            return task.result()
        if _ACTIVE and _ACTIVE in _CANCELLED:
            task.cancel()
            try:
                await task
            except BaseException:
                pass
            logger.warning(f"[cancel] {_ACTIVE} 중지 — {where or 'race'} (진행 중 호출 중단)")
            raise JobCancelled(where or _ACTIVE)
