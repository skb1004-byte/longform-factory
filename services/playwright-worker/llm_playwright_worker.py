#!/usr/bin/env python3
"""
llm_playwright_worker.py v2.0
ChatGPT / Gemini / Claude 웹 자동 로그인 + LLM 쿼리 처리
Storage state 기반 세션 저장/복원. 만료 시 자동 재로그인.

사용법:
  환경변수 설정:
    CHATGPT_EMAIL / CHATGPT_PASS
    GEMINI_EMAIL  / GEMINI_PASS
    CLAUDE_EMAIL  / CLAUDE_PASS
  실행: python llm_playwright_worker.py
"""
import asyncio, json, os, logging
from pathlib import Path
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("llm_pw")

BASE      = Path("E:/longform_factory/v2")
QUEUE_DIR = BASE / "jobs" / "llm_pw_queue"
STATE_DIR = BASE / "jobs" / "pw_sessions"
STATE_DIR.mkdir(parents=True, exist_ok=True)
QUEUE_DIR.mkdir(parents=True, exist_ok=True)

POLL_INTERVAL = 5.0

LLM_SITES = {
    "chatgpt": {
        "url": "https://chatgpt.com",
        "email": os.getenv("CHATGPT_EMAIL", ""),
        "password": os.getenv("CHATGPT_PASS", ""),
        "session_file": STATE_DIR / "chatgpt_session.json",
        "check_sel": "[data-testid=send-button], .composer-btn, nav",
    },
    "gemini": {
        "url": "https://gemini.google.com",
        "email": os.getenv("GEMINI_EMAIL", ""),
        "password": os.getenv("GEMINI_PASS", ""),
        "session_file": STATE_DIR / "gemini_session.json",
        "check_sel": "[aria-label=Send], rich-textarea, .input-area-container",
    },
    "claude": {
        "url": "https://claude.ai",
        "email": os.getenv("CLAUDE_EMAIL", ""),
        "password": os.getenv("CLAUDE_PASS", ""),
        "session_file": STATE_DIR / "claude_session.json",
        "check_sel": "fieldset, [aria-label=Send], div[contenteditable=true]",
    },
}

LLM_PRIORITY = os.getenv("LLM_PW_ORDER", "chatgpt,gemini,claude").split(",")


async def is_logged_in(page, sel):
    try:
        await page.wait_for_selector(sel, timeout=6000)
        return True
    except PWTimeout:
        return False


# ─── ChatGPT ────────────────────────────────────────────────
async def login_chatgpt(page, email, password):
    try:
        await page.goto("https://chatgpt.com", wait_until="domcontentloaded", timeout=30000)
        if await is_logged_in(page, "[data-testid=send-button]"):
            return True
        try:
            await page.click("a[href*=log-in], button:has-text('Log in')", timeout=5000)
            await page.wait_for_load_state("domcontentloaded", timeout=15000)
        except Exception:
            await page.goto("https://auth.openai.com/log-in", timeout=20000)
        await page.wait_for_selector("input[name=email], #email-input", timeout=15000)
        await page.fill("input[name=email], #email-input", email)
        await page.click("button[type=submit]")
        await page.wait_for_selector("input[type=password]", timeout=10000)
        await page.fill("input[type=password]", password)
        await page.click("button[type=submit]")
        await page.wait_for_load_state("networkidle", timeout=30000)
        logged = await is_logged_in(page, "[data-testid=send-button], nav")
        log.info(f"[ChatGPT] login: {logged}")
        return logged
    except Exception as e:
        log.warning(f"[ChatGPT] login error: {e}")
        return False


async def query_chatgpt(ctx, prompt):
    page = await ctx.new_page()
    try:
        await page.goto("https://chatgpt.com", wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_selector("#prompt-textarea", timeout=15000)
        await page.fill("#prompt-textarea", prompt)
        await page.keyboard.press("Enter")
        await page.wait_for_selector("[data-message-author-role=assistant]", timeout=60000)
        await page.wait_for_function("() => !document.querySelector('.result-streaming')", timeout=120000)
        result = await page.locator("[data-message-author-role=assistant] .markdown").last.inner_text()
        await page.close()
        return result.strip()
    except Exception as e:
        log.warning(f"[ChatGPT] query error: {e}")
        try: await page.close()
        except: pass
        return None


# ─── Gemini ─────────────────────────────────────────────────
async def login_gemini(page, email, password):
    try:
        await page.goto("https://gemini.google.com", wait_until="domcontentloaded", timeout=30000)
        if await is_logged_in(page, "[aria-label=Send], rich-textarea"):
            return True
        await page.goto("https://accounts.google.com/signin", timeout=20000)
        await page.wait_for_selector("input[type=email]", timeout=10000)
        await page.fill("input[type=email]", email)
        await page.click("#identifierNext")
        await page.wait_for_selector("input[type=password]", timeout=10000)
        await page.fill("input[type=password]", password)
        await page.click("#passwordNext")
        await page.wait_for_load_state("networkidle", timeout=30000)
        await page.goto("https://gemini.google.com", wait_until="domcontentloaded", timeout=30000)
        logged = await is_logged_in(page, "[aria-label=Send], rich-textarea")
        log.info(f"[Gemini] login: {logged}")
        return logged
    except Exception as e:
        log.warning(f"[Gemini] login error: {e}")
        return False


async def query_gemini(ctx, prompt):
    page = await ctx.new_page()
    try:
        await page.goto("https://gemini.google.com", wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_selector("rich-textarea, .input-area-container", timeout=15000)
        input_el = page.locator("rich-textarea p, .input-area").first
        await input_el.click()
        await input_el.fill(prompt)
        await page.keyboard.press("Enter")
        await page.wait_for_selector("model-response, .response-content", timeout=60000)
        await page.wait_for_function("() => !document.querySelector('.loading-indicator')", timeout=120000)
        result = await page.locator("model-response, .response-content").last.inner_text()
        await page.close()
        return result.strip()
    except Exception as e:
        log.warning(f"[Gemini] query error: {e}")
        try: await page.close()
        except: pass
        return None


# ─── Claude ─────────────────────────────────────────────────
async def login_claude(page, email, password):
    try:
        await page.goto("https://claude.ai", wait_until="domcontentloaded", timeout=30000)
        if await is_logged_in(page, "fieldset, div[contenteditable=true]"):
            return True
        await page.goto("https://claude.ai/login", timeout=20000)
        await page.wait_for_selector("input[type=email]", timeout=10000)
        await page.fill("input[type=email]", email)
        await page.click("button[type=submit]")
        await page.wait_for_selector("input[type=password]", timeout=10000)
        await page.fill("input[type=password]", password)
        await page.click("button[type=submit]")
        await page.wait_for_load_state("networkidle", timeout=30000)
        logged = await is_logged_in(page, "fieldset, div[contenteditable=true]")
        log.info(f"[Claude] login: {logged}")
        return logged
    except Exception as e:
        log.warning(f"[Claude] login error: {e}")
        return False


async def query_claude(ctx, prompt):
    page = await ctx.new_page()
    try:
        await page.goto("https://claude.ai/new", wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_selector("div[contenteditable=true], fieldset", timeout=15000)
        input_el = page.locator("div[contenteditable=true]").first
        await input_el.click()
        await input_el.fill(prompt)
        await page.keyboard.press("Enter")
        await page.wait_for_selector(".font-claude-message", timeout=60000)
        await page.wait_for_function("() => !document.querySelector('.stop-button')", timeout=120000)
        result = await page.locator(".font-claude-message").last.inner_text()
        await page.close()
        return result.strip()
    except Exception as e:
        log.warning(f"[Claude] query error: {e}")
        try: await page.close()
        except: pass
        return None


LOGIN_FUNCS = {"chatgpt": login_chatgpt, "gemini": login_gemini, "claude": login_claude}
QUERY_FUNCS = {"chatgpt": query_chatgpt, "gemini": query_gemini,  "claude": query_claude}


# ─── 세션 관리 ───────────────────────────────────────────────
async def ensure_logged_in(browser, site_name):
    """세션 파일 복원 시도 → 만료면 재로그인 → 저장"""
    cfg = LLM_SITES[site_name]
    session_file = cfg["session_file"]
    email, password = cfg["email"], cfg["password"]

    if not email or not password:
        log.warning(f"[{site_name}] no credentials — set {site_name.upper()}_EMAIL / _PASS")
        return None

    # 기존 세션 복원 시도
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
            log.info(f"[{site_name}] session expired, re-login")
            await ctx.close()
        except Exception as e:
            log.warning(f"[{site_name}] restore failed: {e}")

    # 새 로그인
    log.info(f"[{site_name}] fresh login...")
    ctx = await browser.new_context()
    page = await ctx.new_page()
    fn = LOGIN_FUNCS[site_name]
    success = await fn(page, email, password)
    await page.close()
    if success:
        await ctx.storage_state(path=str(session_file))
        log.info(f"[{site_name}] session saved: {session_file}")
        return ctx
    await ctx.close()
    return None


# ─── 요청 처리 ───────────────────────────────────────────────
async def process_request(req_file, site_contexts):
    done = req_file.with_suffix(".done")
    fail = req_file.with_suffix(".fail")
    try:
        req = json.loads(req_file.read_text(encoding="utf-8"))
        prompt    = req.get("prompt", "")
        site_pref = req.get("site", "").lower()
        out_path  = req.get("output_path", "")

        order = ([site_pref] + [s for s in LLM_PRIORITY if s != site_pref]) if site_pref else LLM_PRIORITY

        for site in order:
            ctx = site_contexts.get(site)
            fn  = QUERY_FUNCS.get(site)
            if not ctx or not fn:
                continue
            result = await fn(ctx, prompt)
            if result:
                payload = {"result": result, "site": site}
                if out_path:
                    Path(out_path).write_text(result, encoding="utf-8")
                done.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
                req_file.unlink(missing_ok=True)
                log.info(f"[OK] {req_file.stem} via {site} ({len(result)} chars)")
                return

        fail.write_text(json.dumps({"error": "all LLMs failed"}), encoding="utf-8")
        req_file.unlink(missing_ok=True)
    except Exception as e:
        log.error(f"[ERR] {req_file.stem}: {e}")
        fail.write_text(json.dumps({"error": str(e)}), encoding="utf-8")
        try: req_file.unlink(missing_ok=True)
        except: pass


async def main():
    log.info("=== LLM Playwright Worker v2.0 (Auto-Login) ===")
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=False,
            args=["--disable-blink-features=AutomationControlled", "--no-sandbox"]
        )
        contexts = {}
        for site in LLM_PRIORITY:
            contexts[site] = await ensure_logged_in(browser, site)
            log.info(f"  {site}: {'ready' if contexts[site] else 'skip'}")

        log.info(f"Queue: {QUEUE_DIR}")
        while True:
            for rf in sorted(QUEUE_DIR.glob("*.json")):
                if not rf.with_suffix(".done").exists() and not rf.with_suffix(".fail").exists():
                    asyncio.create_task(process_request(rf, contexts))
            await asyncio.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    asyncio.run(main())
