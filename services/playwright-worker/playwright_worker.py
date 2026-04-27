#!/usr/bin/env python3
"""playwright_worker.py v3.0 — Auto-login + Storage State"""
import asyncio, json, os, logging, base64
from pathlib import Path
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("pw_worker")

BASE      = Path("E:/longform_factory/v2")
QUEUE_DIR = BASE / "jobs" / "pw_queue"
STATE_DIR = BASE / "jobs" / "pw_sessions"
STATE_DIR.mkdir(parents=True, exist_ok=True)
QUEUE_DIR.mkdir(parents=True, exist_ok=True)

MAX_PARALLEL  = int(os.getenv("PW_MAX_PARALLEL", "3"))
POLL_INTERVAL = 5.0

SITE_CONFIGS = {
    "wavespeed": {
        "url": "https://wavespeed.ai",
        "email": os.getenv("WAVESPEED_EMAIL", ""),
        "password": os.getenv("WAVESPEED_PASS", ""),
        "session_file": STATE_DIR / "wavespeed_session.json",
        "check_sel": "header .avatar, [data-testid='user-avatar'], .user-menu",
    },
    "deevid": {
        "url": "https://deevid.ai",
        "email": os.getenv("DEEVID_EMAIL", ""),
        "password": os.getenv("DEEVID_PASS", ""),
        "session_file": STATE_DIR / "deevid_session.json",
        "check_sel": ".user-profile, .logout-btn",
    },
    "hailuo": {
        "url": "https://hailuoai.video",
        "email": os.getenv("HAILUO_EMAIL", ""),
        "password": os.getenv("HAILUO_PASS", ""),
        "session_file": STATE_DIR / "hailuo_session.json",
        "check_sel": ".user-info, .avatar-wrap",
    },
}

PW_SITE_ORDER = os.getenv("PW_SITE_ORDER", "wavespeed,deevid,hailuo").split(",")


async def is_logged_in(page, sel):
    try:
        await page.wait_for_selector(sel, timeout=5000)
        return True
    except PWTimeout:
        return False


async def do_email_login(page, login_url, email, password, check_sel):
    await page.goto(login_url, wait_until="domcontentloaded", timeout=30000)
    # 이메일 입력 필드 찾기
    for sel in ["input[type='email']", "input[name='email']", "#email"]:
        try:
            await page.wait_for_selector(sel, timeout=5000)
            await page.fill(sel, email)
            break
        except PWTimeout:
            continue
    # 비밀번호 입력
    for sel in ["input[type='password']", "#password"]:
        try:
            await page.wait_for_selector(sel, timeout=3000)
            await page.fill(sel, password)
            break
        except PWTimeout:
            continue
    # 제출
    for sel in ["button[type='submit']", ".login-btn", "button:has-text('Sign in')"]:
        try:
            btn = page.locator(sel).first
            if await btn.count() > 0:
                await btn.click()
                break
        except Exception:
            continue
    await page.wait_for_load_state("networkidle", timeout=20000)
    return await is_logged_in(page, check_sel)


async def ensure_logged_in(browser, site_name):
    cfg = SITE_CONFIGS[site_name]
    session_file = cfg["session_file"]
    email = cfg["email"]
    password = cfg["password"]

    if not email or not password:
        log.warning(f"[{site_name}] no credentials (set {site_name.upper()}_EMAIL / _PASS)")
        return None

    # 기존 세션 복원
    if session_file.exists():
        log.info(f"[{site_name}] restoring session...")
        try:
            ctx = await browser.new_context(storage_state=str(session_file))
            page = await ctx.new_page()
            await page.goto(cfg["url"], wait_until="domcontentloaded", timeout=30000)
            if await is_logged_in(page, cfg["check_sel"]):
                log.info(f"[{site_name}] session OK")
                await page.close()
                return ctx
            await ctx.close()
        except Exception as e:
            log.warning(f"[{site_name}] session restore failed: {e}")

    # 새 로그인
    log.info(f"[{site_name}] logging in...")
    ctx = await browser.new_context()
    page = await ctx.new_page()
    success = await do_email_login(page, cfg["url"], email, password, cfg["check_sel"])
    await page.close()
    if success:
        await ctx.storage_state(path=str(session_file))
        log.info(f"[{site_name}] session saved")
        return ctx
    await ctx.close()
    return None


async def _download_url(url, output_path):
    import aiohttp
    try:
        async with aiohttp.ClientSession() as sess:
            async with sess.get(url, timeout=aiohttp.ClientTimeout(total=120)) as resp:
                if resp.status == 200:
                    output_path.write_bytes(await resp.read())
                    return output_path.stat().st_size > 4096
    except Exception as e:
        log.warning(f"[dl] {e}")
    return False


async def generate_wavespeed(ctx, prompt, duration, output_path):
    page = await ctx.new_page()
    try:
        await page.goto("https://wavespeed.ai/video-generator", timeout=30000)
        ta = page.locator("textarea").first
        await ta.fill(prompt[:480])
        dur_in = page.locator("input[type='number']").first
        if await dur_in.count() > 0:
            await dur_in.fill(str(min(duration, 10)))
        await page.locator("button[type='submit']").first.click()
        await page.wait_for_selector("a[href*='.mp4'], video[src]", timeout=300000)
        url = await page.locator("a[href*='.mp4']").first.get_attribute("href")
        if not url:
            url = await page.locator("video[src]").first.get_attribute("src")
        await page.close()
        return await _download_url(url, output_path) if url else False
    except Exception as e:
        log.warning(f"[WS-web] {e}")
        try: await page.close()
        except: pass
        return False


GENERATE_FUNCS = {"wavespeed": generate_wavespeed}


async def process_request(req_file, site_contexts, sem):
    done = req_file.with_suffix(".done")
    fail = req_file.with_suffix(".fail")
    async with sem:
        try:
            req = json.loads(req_file.read_text(encoding="utf-8"))
            prompt   = req.get("prompt", "cinematic footage")
            duration = int(req.get("duration", 5))
            out_path = Path(req.get("output_path", ""))
            for site in PW_SITE_ORDER:
                ctx = site_contexts.get(site)
                fn  = GENERATE_FUNCS.get(site)
                if not ctx or not fn:
                    continue
                out = out_path if out_path.suffix else out_path.parent / f"{out_path.stem}_{site}.mp4"
                if await fn(ctx, prompt, duration, out):
                    done.write_text(json.dumps({"path": str(out), "site": site}), encoding="utf-8")
                    req_file.unlink(missing_ok=True)
                    log.info(f"[OK] {req_file.stem} via {site}")
                    return
            fail.write_text(json.dumps({"error": "all sites failed"}), encoding="utf-8")
            req_file.unlink(missing_ok=True)
        except Exception as e:
            log.error(f"[ERR] {req_file.stem}: {e}")
            fail.write_text(json.dumps({"error": str(e)}), encoding="utf-8")
            try: req_file.unlink(missing_ok=True)
            except: pass


async def main():
    log.info("=== Playwright Worker v3.0 (Auto-Login) ===")
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=False,
            args=["--disable-blink-features=AutomationControlled", "--no-sandbox"])
        contexts = {}
        for site in PW_SITE_ORDER:
            if site in SITE_CONFIGS:
                contexts[site] = await ensure_logged_in(browser, site)
                log.info(f"  {site}: {'logged in' if contexts[site] else 'skip'}")
        sem = asyncio.Semaphore(MAX_PARALLEL)
        while True:
            for rf in sorted(QUEUE_DIR.glob("*.json")):
                if not rf.with_suffix(".done").exists() and not rf.with_suffix(".fail").exists():
                    asyncio.create_task(process_request(rf, contexts, sem))
            await asyncio.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    asyncio.run(main())
