"""Explore Baidu Baike star map page structure via Playwright."""
import json
import time
from playwright.sync_api import sync_playwright

URL = (
    "https://baike.baidu.com/starmap/view?"
    "nodeId=a243ebc279f9d72066d6c70d"
    "&lemmaTitle=%E8%94%A1%E5%A5%87"
    "&lemmaId=10748789"
    "&starMapFrom=lemma_starMap"
    "&fromModule=lemma_starMap"
    "&isAdForbidden=1"
)

captured_apis = []

def handle_response(response):
    url = response.url
    if any(kw in url for kw in ["starmap", "star_map", "starMap", "graph", "node", "relation"]):
        try:
            body = response.json()
            captured_apis.append({"url": url, "status": response.status, "body": body})
            print(f"\n★ API: {url[:120]}")
            print(f"  Status: {response.status}")
            print(f"  Body keys: {list(body.keys()) if isinstance(body, dict) else type(body)}")
        except Exception:
            captured_apis.append({"url": url, "status": response.status, "body": "non-json"})
            print(f"\n★ API (non-json): {url[:120]}")


with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    ctx = browser.new_context(
        user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        viewport={"width": 1920, "height": 1080},
        locale="zh-CN",
    )
    page = ctx.new_page()
    page.on("response", handle_response)

    print(f"Loading: {URL[:80]}...")
    page.goto(URL, wait_until="networkidle", timeout=30000)
    time.sleep(3)

    # Also capture all XHR/fetch URLs
    print("\n\n=== Page title ===")
    print(page.title())

    # Get page text content to understand structure
    print("\n\n=== Page text (first 3000 chars) ===")
    text = page.inner_text("body")
    print(text[:3000])

    # Look for clickable nodes
    print("\n\n=== Looking for star map nodes/links ===")
    # Try common selectors for star map
    for sel in [
        "[class*='node']", "[class*='star']", "[class*='graph']",
        "[class*='item']", "[class*='person']", "[class*='card']",
        "a[href*='starmap']", "a[href*='item']",
        "svg text", "canvas",
    ]:
        elements = page.query_selector_all(sel)
        if elements:
            print(f"\n  Selector '{sel}': {len(elements)} elements")
            for el in elements[:5]:
                try:
                    txt = el.inner_text()[:100] if el.is_visible() else "(hidden)"
                    print(f"    - {txt}")
                except:
                    print(f"    - (no text)")

    # Save all captured API data
    with open("temp/starmap_apis.json", "w", encoding="utf-8") as f:
        json.dump(captured_apis, f, ensure_ascii=False, indent=2)
    print(f"\n\nSaved {len(captured_apis)} API responses to temp/starmap_apis.json")

    # Take screenshot
    page.screenshot(path="temp/starmap_screenshot.png", full_page=True)
    print("Screenshot saved to temp/starmap_screenshot.png")

    browser.close()
