"""
Phase 0: Scrape Baidu Baike star maps for provincial governors and party secretaries.

Strategy:
1. Visit seed officials' Baike pages to discover star map nodeIds
2. Filter for "历任省长/省委书记/主席" patterns
3. Use collectinfo API to get full official lists with names, terms, lemmaIds
4. Save results to data/provincial_starmaps.json

API endpoints:
- getstarmapmountlist: returns star maps associated with a lemma (person)
- collectinfo?encodeRelId=NODE_ID&pn=1&rn=50: returns officials in a star map
"""

import json
import re
import time
import random
from pathlib import Path
from urllib.parse import quote

# ── Constants ────────────────────────────────────────────────────────────────

DATA_DIR = Path("data")
TEMP_DIR = Path("temp")
DATA_DIR.mkdir(exist_ok=True)
TEMP_DIR.mkdir(exist_ok=True)

OUTPUT_FILE = DATA_DIR / "provincial_starmaps.json"
PROGRESS_FILE = TEMP_DIR / "starmap_progress.json"

# Patterns that indicate provincial governor/secretary star maps
GOVERNOR_PATTERNS = [
    r"历任.*省长",
    r"历任.*主席",      # 自治区: 历任XX自治区主席
    r"历任.*市长",       # 直辖市: 历任XX市市长
]
SECRETARY_PATTERNS = [
    r"历任.*省委书记",
    r"历任.*市委书记",   # 直辖市
    r"历任.*自治区.*书记",
    r"历任.*党委书记",
]

# All 31 provinces/municipalities/autonomous regions
PROVINCES = [
    "北京", "天津", "河北", "山西", "内蒙古",
    "辽宁", "吉林", "黑龙江",
    "上海", "江苏", "浙江", "安徽", "福建", "江西", "山东",
    "河南", "湖北", "湖南", "广东", "广西", "海南",
    "重庆", "四川", "贵州", "云南", "西藏",
    "陕西", "甘肃", "青海", "宁夏", "新疆",
]

# Seed officials: governors + secretaries per province to discover star maps
# Format: (name, lemmaId_or_None, province, role_hint)
# We need both governor and secretary seeds since they appear in different star maps
SEED_OFFICIALS = [
    # ── 直辖市 ──
    ("殷勇", 18433971, "北京", "governor"),
    ("尹力", None, "北京", "secretary"),
    ("张工", None, "天津", "governor"),
    ("陈敏尔", None, "天津", "secretary"),
    # ── 省 ──
    ("王正谱", None, "河北", "governor"),
    ("倪岳峰", None, "河北", "secretary"),
    ("金湘军", None, "山西", "governor"),
    ("唐登杰", None, "山西", "secretary"),
    ("王莉霞", None, "内蒙古", "governor"),
    ("孙绍骋", None, "内蒙古", "secretary"),
    ("李乐成", None, "辽宁", "governor"),
    ("郝鹏", None, "辽宁", "secretary"),
    ("胡玉亭", None, "吉林", "governor"),
    ("景俊海", None, "吉林", "secretary"),
    ("梁惠玲", None, "黑龙江", "governor"),
    ("许勤", None, "黑龙江", "secretary"),
    ("龚正", None, "上海", "governor"),
    ("陈吉宁", None, "上海", "secretary"),
    ("许昆林", None, "江苏", "governor"),
    ("信长星", None, "江苏", "secretary"),
    ("王浩", None, "浙江", "governor"),
    ("易炼红", None, "浙江", "secretary"),
    ("王清宪", None, "安徽", "governor"),
    ("韩俊", None, "安徽", "secretary"),
    ("赵龙", None, "福建", "governor"),
    ("周祖翼", None, "福建", "secretary"),
    ("叶建春", None, "江西", "governor"),
    ("尹弘", None, "江西", "secretary"),
    ("周乃翔", None, "山东", "governor"),
    ("林武", None, "山东", "secretary"),
    ("王凯", None, "河南", "governor"),
    ("楼阳生", None, "河南", "secretary"),
    ("王忠林", None, "湖北", "governor"),
    ("王蒙徽", None, "湖北", "secretary"),
    ("毛伟明", None, "湖南", "governor"),
    ("沈晓明", None, "湖南", "secretary"),
    ("许达哲", None, "湖南", "governor"),  # fallback: former governor
    ("王伟中", None, "广东", "governor"),
    ("黄坤明", None, "广东", "secretary"),
    ("蓝天立", None, "广西", "governor"),
    ("刘宁", None, "广西", "secretary"),
    ("刘小明", None, "海南", "governor"),
    ("冯飞", None, "海南", "secretary"),
    ("胡衡华", None, "重庆", "governor"),
    ("袁家军", None, "重庆", "secretary"),
    ("施小琳", None, "四川", "governor"),
    ("王晓晖", None, "四川", "secretary"),
    ("李炳军", None, "贵州", "governor"),
    ("徐麟", None, "贵州", "secretary"),
    ("王予波", None, "云南", "governor"),
    ("王宁", None, "云南", "secretary"),
    ("严金海", None, "西藏", "governor"),
    ("王君正", None, "西藏", "secretary"),
    ("赵刚", None, "陕西", "governor"),
    ("赵一德", None, "陕西", "secretary"),
    ("任振鹤", None, "甘肃", "governor"),
    ("胡昌升", None, "甘肃", "secretary"),
    ("吴晓军", None, "青海", "governor"),
    ("陈刚", None, "青海", "secretary"),
    ("张雨浦", None, "宁夏", "governor"),
    ("梁言顺", None, "宁夏", "secretary"),
    ("艾尔肯·吐尼亚孜", None, "新疆", "governor"),
    ("马兴瑞", None, "新疆", "secretary"),
]


def _is_governor_starmap(name: str) -> bool:
    """Check if a star map name matches governor patterns."""
    for pat in GOVERNOR_PATTERNS:
        if re.search(pat, name):
            return True
    return False


def _is_secretary_starmap(name: str) -> bool:
    """Check if a star map name matches secretary patterns."""
    for pat in SECRETARY_PATTERNS:
        if re.search(pat, name):
            return True
    return False


def _province_in_name(name: str, province: str) -> bool:
    """Check if the star map name is related to a specific province."""
    return province in name


def scrape_all_starmaps():
    """Main scraper: visit seed officials, discover star maps, fetch full lists."""
    from playwright.sync_api import sync_playwright

    # Load progress if exists
    discovered = {}  # province -> {governor: {...}, secretary: {...}}
    if PROGRESS_FILE.exists():
        discovered = json.loads(PROGRESS_FILE.read_text(encoding="utf-8"))
        print(f"Loaded progress: {len(discovered)} provinces already discovered")

    # Track all discovered star maps (for debugging)
    all_starmaps = []
    mount_responses = []

    def handle_response(response):
        url = response.url
        if "getstarmapmountlist" in url:
            try:
                body = response.json()
                mount_responses.append(body)
            except Exception:
                pass

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080},
            locale="zh-CN",
        )
        page = ctx.new_page()
        page.on("response", handle_response)

        # ── Phase 1: Discover star map nodeIds from seed officials ────────────

        print("=" * 60)
        print("Phase 1: Discovering star map nodeIds from seed officials")
        print("=" * 60)

        for i, (name, lemma_id, province, role_hint) in enumerate(SEED_OFFICIALS):
            # Skip if we already have both governor and secretary for this province
            if province in discovered:
                prov_data = discovered[province]
                if prov_data.get("governor") and prov_data.get("secretary"):
                    print(f"  [{i+1}/{len(SEED_OFFICIALS)}] {province} ({name}) — already complete, skip")
                    continue

            print(f"\n  [{i+1}/{len(SEED_OFFICIALS)}] {province}: visiting {name}'s page...")

            # Build URL
            if lemma_id:
                url = f"https://baike.baidu.com/item/{quote(name)}/{lemma_id}"
            else:
                url = f"https://baike.baidu.com/item/{quote(name)}"

            mount_responses.clear()
            try:
                page.goto(url, wait_until="networkidle", timeout=25000)
                time.sleep(2)
            except Exception as e:
                print(f"    [ERROR] Page load failed: {e}")
                continue

            # Check mount list responses
            if not mount_responses:
                # Try waiting a bit more
                time.sleep(2)

            for resp in mount_responses:
                if resp.get("errno") != 0:
                    continue
                star_list = resp.get("list", [])
                for star in star_list:
                    sm_name = star.get("name", "")
                    sm_encode_id = star.get("encodeId", "")
                    sm_count = star.get("lemmaCnt", 0)

                    all_starmaps.append({
                        "name": sm_name,
                        "encodeId": sm_encode_id,
                        "count": sm_count,
                        "source_official": name,
                        "source_province": province,
                    })

                    # Check if this is a provincial governor/secretary star map
                    if not _province_in_name(sm_name, province):
                        continue

                    if province not in discovered:
                        discovered[province] = {}

                    # Get a sample lemmaId from the star map's items (needed for collectinfo API)
                    sample_lemma_id = 0
                    sm_items = star.get("list", [])
                    if sm_items:
                        sample_lemma_id = sm_items[0].get("lemmaId", 0)

                    if _is_governor_starmap(sm_name) and "governor" not in discovered[province]:
                        discovered[province]["governor"] = {
                            "starmap_name": sm_name,
                            "encodeId": sm_encode_id,
                            "count": sm_count,
                            "sample_lemmaId": sample_lemma_id,
                        }
                        print(f"    ★ Governor: {sm_name} (nodeId={sm_encode_id}, {sm_count} entries)")

                    if _is_secretary_starmap(sm_name) and "secretary" not in discovered[province]:
                        discovered[province]["secretary"] = {
                            "starmap_name": sm_name,
                            "encodeId": sm_encode_id,
                            "count": sm_count,
                            "sample_lemmaId": sample_lemma_id,
                        }
                        print(f"    ★ Secretary: {sm_name} (nodeId={sm_encode_id}, {sm_count} entries)")

            # Save progress after each official
            PROGRESS_FILE.write_text(json.dumps(discovered, ensure_ascii=False, indent=2), encoding="utf-8")

            # Rate limiting
            delay = random.uniform(2, 4)
            time.sleep(delay)

        # Report phase 1 results
        print(f"\n{'=' * 60}")
        print(f"Phase 1 complete: {len(discovered)}/{len(PROVINCES)} provinces discovered")
        found_gov = sum(1 for v in discovered.values() if v.get("governor"))
        found_sec = sum(1 for v in discovered.values() if v.get("secretary"))
        print(f"  Governors: {found_gov}, Secretaries: {found_sec}")

        missing = [p for p in PROVINCES if p not in discovered or
                   not discovered[p].get("governor") or not discovered[p].get("secretary")]
        if missing:
            print(f"  Missing: {missing}")

        # Save all discovered star maps for reference
        (TEMP_DIR / "all_discovered_starmaps.json").write_text(
            json.dumps(all_starmaps, ensure_ascii=False, indent=2), encoding="utf-8"
        )

        # ── Phase 2: Fetch full official lists via collectinfo API ────────────

        print(f"\n{'=' * 60}")
        print("Phase 2: Fetching full official lists via collectinfo API")
        print(f"{'=' * 60}")

        results = {}  # province -> {governor_list: [...], secretary_list: [...]}

        for province, prov_data in discovered.items():
            results[province] = {"province": province}

            for role_key in ["governor", "secretary"]:
                if role_key not in prov_data:
                    print(f"  {province}: no {role_key} star map found")
                    continue

                sm = prov_data[role_key]
                encode_id = sm["encodeId"]
                sm_name = sm["starmap_name"]
                sample_lid = sm.get("sample_lemmaId", 1)
                if sample_lid < 1:
                    sample_lid = 1

                print(f"  {province} {role_key}: fetching {sm_name}...")

                # Use page.evaluate to call the API (paginated, max 50 per page)
                try:
                    all_officials = []
                    pn = 1
                    while True:
                        api_url = (
                            f"https://baike.baidu.com/starmap/api/collectinfo"
                            f"?lemmaId={sample_lid}&encodeRelId={encode_id}&pn={pn}&rn=50&productId=1"
                        )
                        resp = page.evaluate(f"""
                            async () => {{
                                const r = await fetch("{api_url}");
                                return await r.json();
                            }}
                        """)

                        if resp.get("errno") != 0:
                            if pn == 1:
                                print(f"    [ERROR] API error: {resp.get('errmsg', 'unknown')}")
                            break

                        batch = resp.get("data", {}).get("list", [])
                        if not batch:
                            break
                        all_officials.extend(batch)
                        if len(batch) < 50:
                            break
                        pn += 1

                    print(f"    Got {len(all_officials)} officials")

                    role_list = []
                    for off in all_officials:
                        entry = {
                            "name": off.get("lemmaTitle", ""),
                            "term": off.get("desc", ""),
                            "lemmaId": off.get("lemmaId"),
                            "summary": off.get("summary", ""),
                            "lemmaDesc": off.get("lemmaDesc", ""),
                            "baike_url": f"https://baike.baidu.com/item/{quote(off.get('lemmaTitle', ''))}/{off.get('lemmaId', '')}",
                        }
                        role_list.append(entry)

                    results[province][f"{role_key}_starmap"] = sm_name
                    results[province][f"{role_key}_nodeId"] = encode_id
                    results[province][f"{role_key}_list"] = role_list

                except Exception as e:
                    print(f"    [ERROR] Fetch failed: {e}")

                time.sleep(random.uniform(0.5, 1.5))

        browser.close()

    # ── Save final results ────────────────────────────────────────────────────

    OUTPUT_FILE.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n{'=' * 60}")
    print(f"Results saved to {OUTPUT_FILE}")

    # Summary
    total_gov = sum(len(v.get("governor_list", [])) for v in results.values())
    total_sec = sum(len(v.get("secretary_list", [])) for v in results.values())
    print(f"Total governors: {total_gov}")
    print(f"Total secretaries: {total_sec}")

    return results


if __name__ == "__main__":
    scrape_all_starmaps()
