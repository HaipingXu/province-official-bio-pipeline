"""
Find star map nodeIds for provincial governors and secretaries.

Strategy:
1. Visit a known provincial official's Baike page
2. Find star map links on that page (they contain nodeId)
3. The star map link pattern shows "XX省历任省长" / "XX省历任省委书记"
"""
import json
import re
import time
import random
from playwright.sync_api import sync_playwright

# We'll start from the star map we already have (北京市历任市长)
# and look for "related" star maps, or explore from official pages

# First, let's check what the "getrelatedlist" API returns for Beijing mayor list
# Then, let's visit individual official pages to find star map links

# Known entry points to explore
SEED_URLS = [
    # Beijing mayors (we already know this nodeId)
    "https://baike.baidu.com/starmap/view?nodeId=a243ebc279f9d72066d6c70d&lemmaTitle=%E8%94%A1%E5%A5%87&lemmaId=10748789",
]

captured_starmap_links = []
captured_apis = []

def handle_response(response):
    url = response.url
    if "starmap/api" in url or "collectinfo" in url or "getrelatedlist" in url:
        try:
            body = response.json()
            captured_apis.append({"url": url, "body": body})
        except:
            pass

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    ctx = browser.new_context(
        user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        viewport={"width": 1920, "height": 1080},
        locale="zh-CN",
    )
    page = ctx.new_page()
    page.on("response", handle_response)

    # Step 1: Visit a provincial governor's Baike page to find star map links
    # Let's try 王正伟 (宁夏省长) or pick a current provincial governor
    # Better: visit 蔡奇's page since we know he appears in Beijing mayor star map

    print("=== Step 1: Visit 蔡奇's Baike page for star map links ===")
    page.goto("https://baike.baidu.com/item/%E8%94%A1%E5%A5%87/10748789",
              wait_until="networkidle", timeout=30000)
    time.sleep(2)

    # Find all starmap links on the page
    links = page.query_selector_all("a[href*='starmap']")
    print(f"Found {len(links)} starmap links")
    for link in links:
        href = link.get_attribute("href")
        text = link.inner_text()[:100]
        print(f"  {text} -> {href}")
        captured_starmap_links.append({"text": text, "href": href})

    # Also look for starmap data in page source
    html = page.content()
    starmap_matches = re.findall(r'starmap[^"]*nodeId=([a-f0-9]+)[^"]*', html)
    print(f"\nFound {len(starmap_matches)} nodeIds in HTML: {starmap_matches[:10]}")

    # Find all starmap-related data attributes or embedded JSON
    starmap_json = re.findall(r'"starMap[^}]*}', html)
    print(f"\nStarmap JSON fragments: {len(starmap_json)}")
    for frag in starmap_json[:5]:
        print(f"  {frag[:200]}")

    # Step 2: Now let's try a provincial governor page
    print("\n\n=== Step 2: Visit 许昆林's page (江苏省长) ===")
    time.sleep(2)
    page.goto("https://baike.baidu.com/item/%E8%AE%B8%E6%98%86%E6%9E%97",
              wait_until="networkidle", timeout=30000)
    time.sleep(2)

    links = page.query_selector_all("a[href*='starmap']")
    print(f"Found {len(links)} starmap links")
    for link in links:
        href = link.get_attribute("href")
        text = link.inner_text()[:100]
        print(f"  {text} -> {href}")
        captured_starmap_links.append({"text": text, "href": href})

    # Step 3: Try searching for "历任省长" star maps
    print("\n\n=== Step 3: Visit 信长星 (安徽 then 江苏 then 上海 party sec) ===")
    time.sleep(2)
    page.goto("https://baike.baidu.com/item/%E4%BF%A1%E9%95%BF%E6%98%9F",
              wait_until="networkidle", timeout=30000)
    time.sleep(2)

    links = page.query_selector_all("a[href*='starmap']")
    print(f"Found {len(links)} starmap links")
    for link in links:
        href = link.get_attribute("href")
        text = link.inner_text()[:100]
        print(f"  {text} -> {href}")
        captured_starmap_links.append({"text": text, "href": href})

    # Step 4: Visit 韩俊's page (安徽省长)
    print("\n\n=== Step 4: Visit 韩俊 (安徽省长) ===")
    time.sleep(2)
    page.goto("https://baike.baidu.com/item/%E9%9F%A9%E4%BF%8A/65858",
              wait_until="networkidle", timeout=30000)
    time.sleep(2)

    links = page.query_selector_all("a[href*='starmap']")
    print(f"Found {len(links)} starmap links")
    for link in links:
        href = link.get_attribute("href")
        text = link.inner_text()[:100]
        print(f"  {text} -> {href}")
        captured_starmap_links.append({"text": text, "href": href})

    # Save results
    with open("temp/starmap_links.json", "w", encoding="utf-8") as f:
        json.dump(captured_starmap_links, f, ensure_ascii=False, indent=2)

    with open("temp/starmap_related_apis.json", "w", encoding="utf-8") as f:
        json.dump(captured_apis, f, ensure_ascii=False, indent=2)

    print(f"\n\nTotal starmap links found: {len(captured_starmap_links)}")
    print("Saved to temp/starmap_links.json")

    browser.close()
