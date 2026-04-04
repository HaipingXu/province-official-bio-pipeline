"""
Capture the full getstarmapmountlist API response from provincial officials' pages.
This API returns the list of star maps for each person, including titles and nodeIds.
"""
import json
import re
import time
from playwright.sync_api import sync_playwright

# Current provincial governors/secretaries to seed our search
# We just need one person per province to find the star map
SEED_OFFICIALS = [
    {"name": "蔡奇", "lemmaId": 10748789},  # Beijing (already known)
]

all_mount_responses = []

def handle_response(response):
    url = response.url
    if "getstarmapmountlist" in url:
        try:
            body = response.json()
            all_mount_responses.append({"url": url, "body": body})
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

    # Visit 蔡奇's page and capture full mount list
    print("=== Capturing getstarmapmountlist from 蔡奇's page ===")
    page.goto("https://baike.baidu.com/item/%E8%94%A1%E5%A5%87/10748789",
              wait_until="networkidle", timeout=30000)
    time.sleep(3)

    # Print full response
    for resp in all_mount_responses:
        print(f"\nURL: {resp['url']}")
        body = resp['body']
        print(json.dumps(body, ensure_ascii=False, indent=2)[:5000])

    # Now try to extract the nodeId for "北京市历任市长" from the swiper
    # The swiper slides are mapped to starmapIds, let's see if the mount list has nodeIds
    print("\n\n=== Examining swiper slides for nodeIds/links ===")

    # Let's click on each swiper item and see what happens
    slides = page.query_selector_all(".swiper-slide")
    print(f"Found {len(slides)} slides")

    for i, slide in enumerate(slides):
        title_el = slide.query_selector("[class*='title']")
        title_text = ""
        if title_el:
            title_text = title_el.inner_text()[:100]
        else:
            title_text = slide.inner_text()[:60]
        print(f"\n  Slide {i}: {title_text}")

        # Check for a link that goes to starmap view
        link = slide.query_selector("a[href*='starmap']")
        if link:
            href = link.get_attribute("href")
            print(f"    Link: {href}")

        # Check for data attributes
        all_attrs = slide.evaluate("el => Array.from(el.attributes).map(a => a.name + '=' + a.value)")
        data_attrs = [a for a in all_attrs if 'data' in a.lower() or 'id' in a.lower() or 'node' in a.lower()]
        if data_attrs:
            print(f"    Attrs: {data_attrs}")

    # Let's check the actual link when clicking the title
    print("\n\n=== Click on '北京市历任市长' slide title ===")
    for slide in slides:
        text = slide.inner_text()[:60]
        if "北京市历任市长" in text:
            # Find the title link
            title_link = slide.query_selector("a")
            if title_link:
                href = title_link.get_attribute("href")
                print(f"  Title link href: {href}")

            # Try clicking the title area
            title_area = slide.query_selector("[class*='title'], [class*='name'], [class*='header']")
            if title_area:
                print(f"  Title area text: {title_area.inner_text()[:60]}")
                # Check all child links
                for a in slide.query_selector_all("a"):
                    h = a.get_attribute("href") or ""
                    t = a.inner_text()[:40]
                    if h:
                        print(f"    a: {t} -> {h}")
            break

    # Let's also look at the inner HTML of the star map section
    print("\n\n=== Star map section inner HTML ===")
    starmap_section = page.query_selector("[class*='starMapContainer']")
    if starmap_section:
        html = starmap_section.inner_html()
        # Extract nodeId patterns
        node_ids = re.findall(r'nodeId[=:]([a-f0-9]+)', html)
        star_node_ids = re.findall(r'starNodeId[=:]([a-f0-9]+)', html)
        encode_ids = re.findall(r'encodeRelId[=:]([a-f0-9]+)', html)
        starmap_ids = re.findall(r'starmapId[=:]([a-f0-9]+)', html)
        print(f"  nodeId: {node_ids[:5]}")
        print(f"  starNodeId: {star_node_ids[:5]}")
        print(f"  encodeRelId: {encode_ids[:5]}")
        print(f"  starmapId: {starmap_ids[:5]}")

        # Also look in href attributes
        hrefs = re.findall(r'href="([^"]*starmap[^"]*)"', html)
        print(f"\n  Starmap hrefs in section:")
        for h in hrefs[:10]:
            print(f"    {h}")

        # Look for the title-to-nodeId mapping
        # Try finding in JavaScript data
        script_data = re.findall(r'data-[a-z-]+="([^"]*)"', html)
        print(f"\n  Data attributes: {script_data[:10]}")

    browser.close()
