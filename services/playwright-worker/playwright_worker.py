#!/usr/bin/env python3
"""playwright_worker.py v4.0 — Auto-login + Storage State + Grok Imagine Video"""
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
    # [v4.0] Grok Imagine Video — x.com/i/grok
    "grok": {
        "url": "https://x.com/i/grok",
        "email": os.getenv("GROK_EMAIL", os.getenv("CHATGPT_EMAIL", "")),
        "password": os.getenv("GROK_PASS", os.getenv("CHATGPT_PASS", "")),
        "session_file": STATE_DIR / "grok_session.json",
        "check_sel": "[data-testid='grok-input'], textarea[placeholder*='Grok'], .r-1adg3ll",
    },
}

PW_SITE_ORDER = os.getenv("PW_SITE_ORDER", "grok,wavespeed,deevid,hailuo").split(",")


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


# ─── Grok (x.com) 전용 로그인 ───────────────────────────────
async def login_grok_site(page, email, password, check_sel):
    """x.com 계정으로 Grok 로그인 (Twitter/X 2단계 입력 플로우)."""
    try:
        await page.goto("https://x.com/i/grok", wait_until="domcontentloaded", timeout=30000)
        if await is_logged_in(page, check_sel):
            return True
        # 로그인 페이지로
        await page.goto("https://x.com/login", wait_until="domcontentloaded", timeout=20000)
        # 1단계: 이메일/유저네임
        await page.wait_for_selector(
            "input[name='text'], input[autocomplete='username'], input[type='email']",
            timeout=15000
        )
        await page.fill(
            "input[name='text'], input[autocomplete='username'], input[type='email']",
            email
        )
        await page.keyboard.press("Enter")
        await page.wait_for_timeout(1500)
        # 전화번호/사용자명 추가 확인창 (스킵)
        try:
            extra = page.locator("input[data-testid='ocfEnterTextTextInput']")
            if await extra.count() > 0:
                await extra.fill(email.split("@")[0])
                await page.keyboard.press("Enter")
                await page.wait_for_timeout(1000)
        except Exception:
            pass
        # 2단계: 비밀번호
        await page.wait_for_selector(
            "input[name='password'], input[type='password']",
            timeout=10000
        )
        await page.fill("input[name='password'], input[type='password']", password)
        await page.keyboard.press("Enter")
        await page.wait_for_load_state("networkidle", timeout=30000)
        # Grok 페이지로 이동
        await page.goto("https://x.com/i/grok", wait_until="domcontentloaded", timeout=20000)
        logged = await is_logged_in(page, check_sel)
        log.info(f"[Grok] login: {logged}")
        return logged
    except Exception as e:
        log.warning(f"[Grok] login error: {e}")
        return False


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
    if site_name == "grok":
        success = await login_grok_site(page, email, password, cfg["check_sel"])
    else:
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


# ─── Grok Imagine Video 생성 ─────────────────────────────────
async def generate_grok(ctx, prompt, duration, output_path):
    """
    x.com/i/grok 에서 /imagine video 명령으로 영상 생성.
    네트워크 응답 인터셉트로 MP4 URL 캡처 → 다운로드.
    """
    page = await ctx.new_page()
    captured_urls = []

    async def _intercept(response):
        url = response.url
        ct  = response.headers.get("content-type", "")
        if "video" in ct or url.endswith(".mp4") or "video" in url.lower():
            captured_urls.append(url)
            log.info(f"[Grok-V] 네트워크 캡처: {url[:80]}")

    page.on("response", _intercept)

    try:
        await page.goto("https://x.com/i/grok", wait_until="domcontentloaded", timeout=30000)
        # 입력창 찾기
        input_el = page.locator(
            "[data-testid='grok-input'], "
            "div[contenteditable='true'][aria-label], "
            "textarea[placeholder]"
        ).first
        await input_el.wait_for(timeout=15000)
        await input_el.click()

        # Imagine Video 명령
        video_prompt = f"/imagine video {prompt[:280]}, cinematic 4K footage"
        await page.keyboard.type(video_prompt, delay=30)
        await page.keyboard.press("Enter")
        log.info(f"[Grok-V] 요청: {video_prompt[:80]}...")

        # 영상 요소 또는 다운로드 버튼 대기 (최대 3분)
        try:
            await page.wait_for_selector(
                "video, [data-testid='videoPlayer'], "
                "a[href*='.mp4'], button[aria-label*='download']",
                timeout=180000
            )
        except PWTimeout:
            log.warning("[Grok-V] 영상 요소 타임아웃")

        await page.wait_for_timeout(2000)

        # 1순위: 네트워크 인터셉트된 URL
        if captured_urls:
            video_url = captured_urls[-1]
            log.info(f"[Grok-V] 인터셉트 URL 사용: {video_url[:80]}")
            await page.close()
            return await _download_url(video_url, output_path)

        # 2순위: DOM에서 video src 추출
        video_url = await page.evaluate("""() => {
            const srcs = [];
            document.querySelectorAll('video').forEach(v => {
                if (v.src && v.src.startsWith('http')) srcs.push(v.src);
                if (v.currentSrc && v.currentSrc.startsWith('http')) srcs.push(v.currentSrc);
            });
            document.querySelectorAll('source').forEach(s => {
                if (s.src && s.src.startsWith('http')) srcs.push(s.src);
            });
            document.querySelectorAll('a[href]').forEach(a => {
                if (a.href.includes('.mp4')) srcs.push(a.href);
            });
            return srcs[0] || '';
        }""")

        await page.close()
        if video_url:
            log.info(f"[Grok-V] DOM URL 사용: {video_url[:80]}")
            return await _download_url(video_url, output_path)

        log.warning("[Grok-V] 영상 URL 추출 실패")
        return False

    except Exception as e:
        log.warning(f"[Grok-V] 오류: {e}")
        try: await page.close()
        except: pass
        return False


GENERATE_FUNCS = {
    "grok":       generate_grok,
    "wavespeed":  generate_wavespeed,
}


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
    log.info("=== Playwright Worker v4.0 (Grok + Auto-Login) ===")
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
