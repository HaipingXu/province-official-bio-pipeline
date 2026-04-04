"""
Extract star map data from Baike pages.
Key finding: starMapMount JSON is embedded in page HTML with starmapIds.
The collectinfo API uses encodeRelId (hex), but pages show numeric starmapIds.
Let's explore both ID formats and the star map sections.
"""
import json
import re
import time
from playwright.sync_api import sync_playwright

captured_apis = []

def handle_response(response):
    url = response.url
    if "starmap" in url.lower() or "star_map" in url.lower():
        try:
            body = response.json()
            captured_apis.append({"url": url, "body": body})
        except:
            pass

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    ctx = browser.new_context(
        user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        viewport={"width": 1920, "height": 1080},
        locale="zh-CN",
    )
    page = ctx.new_page()
    page.on("response", handle_response)

    # Visit 蔡奇's page and extract full starMapMount data
    print("=== Visit 蔡奇's Baike page ===")
    page.goto("https://baike.baidu.com/item/%E8%94%A1%E5%A5%87/10748789",
              wait_until="networkidle", timeout=30000)
    time.sleep(2)

    # Extract starMapMount JSON from page
    html = page.content()

    # Find the full starMapMount data
    match = re.search(r'"starMapMount"\s*:\s*(\{.*?\})\s*[,}]', html)
    if match:
        print(f"starMapMount: {match.group(1)[:500]}")

    # Try broader pattern
    match = re.search(r'"starMapMount":\{"data":\{[^}]*\}[^}]*\}', html)
    if match:
        print(f"\nFull starMapMount block: {match.group(0)[:500]}")

    # Find ALL starmapIds mentions
    ids_match = re.findall(r'"starmapIds"\s*:\s*\[([^\]]+)\]', html)
    if ids_match:
        for m in ids_match:
            print(f"\nstarmapIds: [{m}]")

    # Find star map container and extract visible text/links
    print("\n\n=== Star map container content ===")
    containers = page.query_selector_all("[class*='starMap']")
    print(f"Found {len(containers)} starMap containers")
    for i, c in enumerate(containers[:3]):
        try:
            text = c.inner_text()[:300]
            inner_html = c.inner_html()[:500]
            print(f"\nContainer {i}:")
            print(f"  Text: {text}")
            # Look for data attributes or links
            links = c.query_selector_all("a")
            for link in links[:5]:
                href = link.get_attribute("href") or ""
                txt = link.inner_text()[:50]
                print(f"  Link: {txt} -> {href}")
        except Exception as e:
            print(f"  Error: {e}")

    # Now let's try extracting the star map section more broadly
    print("\n\n=== Looking for swiper items (star map cards) ===")
    swiper_items = page.query_selector_all(".swiper-slide")
    print(f"Found {len(swiper_items)} swiper slides")
    for i, item in enumerate(swiper_items[:10]):
        try:
            text = item.inner_text()[:200]
            # Look for href
            a_tag = item.query_selector("a")
            href = a_tag.get_attribute("href") if a_tag else "no link"
            print(f"\n  Slide {i}: {text[:100]}")
            print(f"  Link: {href}")
        except Exception as e:
            print(f"  Slide {i}: Error {e}")

    # Try to find star map API that's called when the page loads
    print(f"\n\n=== Captured {len(captured_apis)} starmap APIs ===")
    for api in captured_apis:
        print(f"\nURL: {api['url'][:150]}")
        body = api['body']
        if isinstance(body, dict):
            print(f"Keys: {list(body.keys())}")
            if 'data' in body and isinstance(body['data'], dict):
                print(f"Data keys: {list(body['data'].keys())}")
                if 'list' in body['data']:
                    items = body['data']['list']
                    print(f"List items: {len(items)}")
                    for item in items[:3]:
                        print(f"  - {item.get('title', item.get('lemmaTitle', '?'))}: {json.dumps(item, ensure_ascii=False)[:200]}")

    # Now try the starmap API with numeric IDs found
    print("\n\n=== Try collectinfo API with known numeric IDs ===")
    numeric_ids = [967390, 1270153, 1284685, 1225351, 600204, 1218520, 555967, 319307]

    # These might be different API format - try starmap/api/detail or similar
    for sid in numeric_ids[:3]:
        url = f"https://baike.baidu.com/starmap/api/collectinfo?lemmaId=10748789&encodeRelId={sid}&pn=1&rn=50&productId=1"
        try:
            resp = page.evaluate(f"""
                async () => {{
                    const r = await fetch("{url}");
                    return await r.json();
                }}
            """)
            print(f"\nID {sid}: errno={resp.get('errno')}")
            if resp.get('data') and resp['data'].get('list'):
                print(f"  List items: {len(resp['data']['list'])}")
                for item in resp['data']['list'][:2]:
                    print(f"    {item.get('lemmaTitle', '?')}: {item.get('desc', '?')}")
        except Exception as e:
            print(f"\nID {sid}: Error {e}")

    # Try a different API pattern: starmapview with numeric ID
    print("\n\n=== Try starmapview API ===")
    for sid in numeric_ids[:3]:
        url = f"https://baike.baidu.com/starmap/api/starmapview?flag=add&starmapid={sid}"
        try:
            resp = page.evaluate(f"""
                async () => {{
                    const r = await fetch("{url}");
                    return await r.json();
                }}
            """)
            print(f"\nstarmapview {sid}: {json.dumps(resp, ensure_ascii=False)[:200]}")
        except Exception as e:
            print(f"\nstarmapview {sid}: Error {e}")

    # NEW: Try to find the actual star map API that returns the graph
    # The page view URL uses nodeId which is hex - let's try fetching the star map view page
    # and see what APIs it calls internally
    print("\n\n=== Navigate to the star map view page for nodeId ===")
    page2 = ctx.new_page()
    captured_apis.clear()
    page2.on("response", handle_response)

    page2.goto(
        "https://baike.baidu.com/starmap/view?nodeId=a243ebc279f9d72066d6c70d&lemmaTitle=%E8%94%A1%E5%A5%87&lemmaId=10748789",
        wait_until="networkidle", timeout=30000
    )
    time.sleep(2)

    # Look for related star maps (other provinces, secretaries, etc.)
    related_links = page2.query_selector_all("a[href*='starmap']")
    print(f"Related starmap links on view page: {len(related_links)}")
    for link in related_links[:10]:
        href = link.get_attribute("href") or ""
        text = link.inner_text()[:80]
        print(f"  {text} -> {href}")

    # Check if there's a sidebar or navigation with other star maps
    all_links = page2.query_selector_all("a")
    for link in all_links:
        href = link.get_attribute("href") or ""
        if "starmap" in href:
            text = link.inner_text()[:80]
            print(f"  Starmap link: {text} -> {href}")

    print(f"\n\n=== APIs from star map view page: {len(captured_apis)} ===")
    for api in captured_apis:
        print(f"URL: {api['url'][:200]}")
        if isinstance(api['body'], dict):
            print(f"  Body: {json.dumps(api['body'], ensure_ascii=False)[:300]}")

    browser.close()
