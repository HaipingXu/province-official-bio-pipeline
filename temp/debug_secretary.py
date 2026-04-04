"""
Debug: Check ALL star maps on a known secretary's page.
Try different group values for getstarmapmountlist API.
Also check if star maps appear in the HTML but not in the API response.
"""
import json
import re
import time
from urllib.parse import quote
from playwright.sync_api import sync_playwright

mount_responses = []
all_api_responses = []

def handle_response(response):
    url = response.url
    if "starmap" in url.lower():
        try:
            body = response.json()
            all_api_responses.append({"url": url, "body": body})
            if "getstarmapmountlist" in url:
                mount_responses.append({"url": url, "body": body})
        except:
            all_api_responses.append({"url": url, "body": "non-json"})

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    ctx = browser.new_context(
        user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        viewport={"width": 1920, "height": 1080},
        locale="zh-CN",
    )
    page = ctx.new_page()
    page.on("response", handle_response)

    # Test with 袁家军 (重庆书记) - the ONE case where we found secretary starmap
    print("=== 袁家军 (重庆市委书记) - KNOWN to have secretary starmap ===")
    page.goto("https://baike.baidu.com/item/%E8%A2%81%E5%AE%B6%E5%86%9B", wait_until="networkidle", timeout=30000)
    time.sleep(3)

    print(f"\ngetstarmapmountlist responses: {len(mount_responses)}")
    for resp in mount_responses:
        print(f"\n  URL: {resp['url'][:200]}")
        body = resp['body']
        if body.get('errno') == 0:
            names = [s['name'] for s in body.get('list', [])]
            print(f"  Star maps ({len(names)}): {names}")

    # Check ALL swiper slides
    slides = page.query_selector_all(".swiper-slide")
    print(f"\nSwiper slides: {len(slides)}")
    for i, slide in enumerate(slides):
        text = slide.inner_text()[:80]
        print(f"  Slide {i}: {text}")

    # Now try 尹力 (北京市委书记)
    print("\n\n=== 尹力 (北京市委书记) ===")
    mount_responses.clear()
    all_api_responses.clear()
    page.goto("https://baike.baidu.com/item/%E5%B0%B9%E5%8A%9B/56802", wait_until="networkidle", timeout=30000)
    time.sleep(3)

    print(f"\ngetstarmapmountlist responses: {len(mount_responses)}")
    for resp in mount_responses:
        body = resp['body']
        if body.get('errno') == 0:
            names = [s['name'] for s in body.get('list', [])]
            print(f"  Star maps ({len(names)}): {names}")

    slides = page.query_selector_all(".swiper-slide")
    print(f"\nSwiper slides: {len(slides)}")
    for i, slide in enumerate(slides):
        text = slide.inner_text()[:80]
        print(f"  Slide {i}: {text}")

    # Try to find if there are more star maps by calling API with different group values
    print("\n\n=== Try different group values for 尹力 ===")
    for group in range(1, 20):
        try:
            resp = page.evaluate(f"""
                async () => {{
                    const r = await fetch("https://baike.baidu.com/starmap/api/getstarmapmountlist?lemmaId=56802&lemmaRn=4&sign=test&timestamp={int(time.time())}&group={group}");
                    return await r.json();
                }}
            """)
            if resp.get('errno') == 0 and resp.get('list'):
                names = [s['name'] for s in resp['list']]
                print(f"  group={group}: {names}")
            elif resp.get('errno') != 0:
                pass  # skip errors
        except:
            pass

    # Try 黄坤明 (广东省委书记)
    print("\n\n=== 黄坤明 (广东省委书记) ===")
    mount_responses.clear()
    page.goto("https://baike.baidu.com/item/%E9%BB%84%E5%9D%A4%E6%98%8E", wait_until="networkidle", timeout=30000)
    time.sleep(3)

    for resp in mount_responses:
        body = resp['body']
        if body.get('errno') == 0:
            names = [s['name'] for s in body.get('list', [])]
            print(f"  Star maps ({len(names)}): {names}")

    slides = page.query_selector_all(".swiper-slide")
    print(f"\nSwiper slides: {len(slides)}")
    for i, slide in enumerate(slides):
        text = slide.inner_text()[:80]
        print(f"  Slide {i}: {text}")

    # Check the embedded starMapMount JSON
    html = page.content()
    match = re.search(r'"starmapIds"\s*:\s*\[([^\]]+)\]', html)
    if match:
        print(f"\nstarmapIds: [{match.group(1)}]")

    # Try getstarmapmountlist with different parameters
    print("\n\n=== Try getstarmapmountlist with higher lemmaRn ===")
    for rn in [4, 8, 20, 50]:
        try:
            resp = page.evaluate(f"""
                async () => {{
                    const r = await fetch("https://baike.baidu.com/starmap/api/getstarmapmountlist?lemmaId=56802&lemmaRn={rn}&sign=test&timestamp={int(time.time())}&group=10");
                    return await r.json();
                }}
            """)
            if resp.get('errno') == 0:
                names = [s['name'] for s in resp.get('list', [])]
                print(f"  lemmaRn={rn}: {len(names)} star maps")
                for n in names:
                    if '书记' in n or '历任' in n:
                        print(f"    ★ {n}")
        except Exception as e:
            print(f"  lemmaRn={rn}: error {e}")

    browser.close()
