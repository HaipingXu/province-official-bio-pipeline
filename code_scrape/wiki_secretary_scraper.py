"""
Scrape provincial party secretary lists from Wikipedia.

Source: 中国共产党XX省委员会 / XX市委员会 / XX自治区委员会 pages
Section: 历届书记 / 历届组成人员

Extracts: name, term (dates), role title changes
Saves to: data/wiki_secretaries.json
"""

import json
import re
import time
import random
from pathlib import Path
from urllib.parse import quote

import requests
from bs4 import BeautifulSoup

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)
OUTPUT_FILE = DATA_DIR / "wiki_secretaries.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8",
}

# Wikipedia page titles for each province's CPC committee
# Pattern: "中国共产党" + full_name + "委员会"
PROVINCE_WIKI_PAGES = {
    "北京": "中国共产党北京市委员会",
    "天津": "中国共产党天津市委员会",
    "河北": "中国共产党河北省委员会",
    "山西": "中国共产党山西省委员会",
    "内蒙古": "中国共产党内蒙古自治区委员会",
    "辽宁": "中国共产党辽宁省委员会",
    "吉林": "中国共产党吉林省委员会",
    "黑龙江": "中国共产党黑龙江省委员会",
    "上海": "中国共产党上海市委员会",
    "江苏": "中国共产党江苏省委员会",
    "浙江": "中国共产党浙江省委员会",
    "安徽": "中国共产党安徽省委员会",
    "福建": "中国共产党福建省委员会",
    "江西": "中国共产党江西省委员会",
    "山东": "中国共产党山东省委员会",
    "河南": "中国共产党河南省委员会",
    "湖北": "中国共产党湖北省委员会",
    "湖南": "中国共产党湖南省委员会",
    "广东": "中国共产党广东省委员会",
    "广西": "中国共产党广西壮族自治区委员会",
    "海南": "中国共产党海南省委员会",
    "重庆": "中国共产党重庆市委员会",
    "四川": "中国共产党四川省委员会",
    "贵州": "中国共产党贵州省委员会",
    "云南": "中国共产党云南省委员会",
    "西藏": "中国共产党西藏自治区委员会",
    "陕西": "中国共产党陕西省委员会",
    "甘肃": "中国共产党甘肃省委员会",
    "青海": "中国共产党青海省委员会",
    "宁夏": "中国共产党宁夏回族自治区委员会",
    "新疆": "中国共产党新疆维吾尔自治区委员会",
}


def fetch_wiki_page(title: str) -> str | None:
    """Fetch a Wikipedia page by title. Returns HTML or None."""
    url = f"https://zh.wikipedia.org/wiki/{quote(title)}"
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code == 200:
            return r.text
        print(f"    [WARN] HTTP {r.status_code} for {title}")
    except Exception as e:
        print(f"    [ERROR] {e}")
    return None


def extract_secretary_section(html: str) -> str:
    """
    Extract the secretary section text from a Wikipedia CPC committee page.

    Section headings vary by province:
    - "历届书记" (广东)
    - "历届市委书记（市委第一书记）" (北京)
    - "历任书记" (河北)
    - "历届省委书记" (四川)
    - Sometimes data is directly under "历届组成人员"

    Strategy: find any h3/h4 under "历届组成人员" that contains "书记"
    """
    soup = BeautifulSoup(html, "html.parser")
    mw = soup.find("div", class_="mw-parser-output")
    if not mw:
        return ""

    all_text = mw.get_text(separator="\n")
    lines = all_text.split("\n")

    # Strategy 1: Find heading with "书记" pattern
    secretary_heading_patterns = [
        "历届书记", "历任书记", "历届省委书记", "历届市委书记",
        "历届自治区党委书记", "历任省委书记", "历任市委书记",
    ]

    result_lines = []
    in_section = False
    # Stop words: next major section
    stop_patterns = [
        "历届省委", "历届市委", "历届自治区委",
        "参考文献", "外部链接", "参见",
    ]

    for line in lines:
        line = line.strip()
        if not line:
            continue

        # Check if we should start collecting
        if not in_section:
            for pat in secretary_heading_patterns:
                if pat in line:
                    in_section = True
                    break
            if in_section:
                continue

        if in_section:
            # Check stop conditions
            should_stop = False
            for stop in stop_patterns:
                if stop in line and "书记" not in line:
                    should_stop = True
                    break
            if should_stop:
                break

            # Skip [编辑] markers
            if line in ["[编辑]", "[", "编辑", "]"]:
                continue
            result_lines.append(line)

    # Strategy 2: If nothing found, try extracting from "历届组成人员" section
    if not result_lines:
        in_parent = False
        for line in lines:
            line = line.strip()
            if not line:
                continue
            if "历届组成人员" in line:
                in_parent = True
                continue
            if in_parent:
                if any(kw in line for kw in ["参考文献", "外部链接", "参见"]):
                    break
                if line in ["[编辑]", "[", "编辑", "]"]:
                    continue
                result_lines.append(line)

    return "\n".join(result_lines)


def parse_secretaries(section_text: str, province: str) -> list[dict]:
    """
    Parse secretary list from the section text.

    Patterns:
    1. Name + (date range) on separate or same lines
    2. Section headers like "中共XX省委第一书记", "中共XX省委书记"
    3. Names may have spaces (like 陶　铸)

    Returns list of {name, term, role_title, raw_text}
    """
    lines = section_text.split("\n")
    secretaries = []
    current_role = ""

    # Regex patterns
    # Role title headers
    role_pattern = re.compile(
        r"中共.*(?:第一书记|书记|组长)|"
        r"中国共产党.*(?:书记)"
    )
    # Name followed by date in parentheses
    # e.g. "林若（1985年7月 — 1991年1月）"
    name_date_pattern = re.compile(
        r"^([^\（\(（]+?)\s*[\（\(（](.+?)[\）\)）]\s*$"
    )
    # Standalone date pattern (sometimes on its own line after name)
    date_pattern = re.compile(
        r"[\（\(（]?\s*(\d{4})\s*年?\s*[\d月]*\s*[—\-－至]\s*[\d{4}年月日]*\s*[\）\)）]?"
    )
    # Name with inline role like "（第一，1949年...）"
    name_role_date = re.compile(
        r"^([^\（\(（]+?)\s*[\（\(（](.*?[，,]\s*\d{4}.*?)[\）\)）]\s*$"
    )
    # Clean name (remove full-width spaces, digits like superscripts)
    clean_name_re = re.compile(r"[\u3000\s\d]")

    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if not line:
            i += 1
            continue

        # Check if it's a role title header
        if role_pattern.search(line):
            current_role = line
            i += 1
            continue

        # Try name + date pattern
        m = name_date_pattern.match(line)
        if m:
            name = clean_name_re.sub("", m.group(1)).strip()
            term_info = m.group(2).strip()
            if name and len(name) >= 2:
                secretaries.append({
                    "name": name,
                    "term": term_info,
                    "role_title": current_role,
                    "raw_text": line,
                })
            i += 1
            continue

        # Try name + role + date pattern
        m = name_role_date.match(line)
        if m:
            name = clean_name_re.sub("", m.group(1)).strip()
            term_info = m.group(2).strip()
            if name and len(name) >= 2:
                secretaries.append({
                    "name": name,
                    "term": term_info,
                    "role_title": current_role,
                    "raw_text": line,
                })
            i += 1
            continue

        # Check if this is just a name and the next line is a date
        if i + 1 < len(lines):
            next_line = lines[i + 1].strip()
            if date_pattern.match(next_line):
                name = clean_name_re.sub("", line).strip()
                if name and len(name) >= 2 and len(name) <= 8:
                    secretaries.append({
                        "name": name,
                        "term": next_line.strip("（）()"),
                        "role_title": current_role,
                        "raw_text": f"{line} {next_line}",
                    })
                    i += 2
                    continue

        i += 1

    return secretaries


def filter_post_1949_secretaries(secretaries: list[dict]) -> list[dict]:
    """
    Filter to only include PRC-era secretaries (post-1949).
    Also deduplicate by (name, term).
    """
    prc_era = []
    seen = set()

    for sec in secretaries:
        term = sec["term"]
        # Extract start year
        year_match = re.search(r"(\d{4})", term)
        if year_match:
            year = int(year_match.group(1))
            if year < 1949:
                continue

        key = (sec["name"], sec["term"])
        if key not in seen:
            seen.add(key)
            prc_era.append(sec)

    return prc_era


def scrape_all_provinces():
    """Main function: scrape all provinces' secretary lists from Wikipedia."""
    results = {}

    for province, wiki_title in PROVINCE_WIKI_PAGES.items():
        print(f"\n  {province}: fetching {wiki_title}...")

        html = fetch_wiki_page(wiki_title)
        if not html:
            print(f"    [SKIP] Page not found")
            results[province] = {"province": province, "secretary_list": [], "error": "page not found"}
            continue

        section_text = extract_secretary_section(html)
        if not section_text:
            print(f"    [WARN] No 历届书记 section found")
            results[province] = {"province": province, "secretary_list": [], "error": "section not found"}
            continue

        secretaries = parse_secretaries(section_text, province)
        secretaries = filter_post_1949_secretaries(secretaries)

        print(f"    Found {len(secretaries)} secretaries")
        for sec in secretaries[-5:]:  # show last 5 (most recent)
            print(f"      {sec['name']:8s} {sec['term'][:40]}")

        results[province] = {
            "province": province,
            "wiki_page": wiki_title,
            "wiki_url": f"https://zh.wikipedia.org/wiki/{quote(wiki_title)}",
            "secretary_list": secretaries,
            "raw_section": section_text,
        }

        time.sleep(random.uniform(0.5, 1.5))

    # Save results
    # Remove raw_section from saved file (too bulky)
    save_data = {}
    for prov, data in results.items():
        save_data[prov] = {k: v for k, v in data.items() if k != "raw_section"}

    OUTPUT_FILE.write_text(json.dumps(save_data, ensure_ascii=False, indent=2), encoding="utf-8")

    # Also save raw sections for debugging
    raw_file = Path("temp") / "wiki_secretary_raw_sections.json"
    raw_data = {prov: data.get("raw_section", "") for prov, data in results.items()}
    raw_file.write_text(json.dumps(raw_data, ensure_ascii=False, indent=2), encoding="utf-8")

    # Summary
    print(f"\n{'=' * 60}")
    print(f"Results saved to {OUTPUT_FILE}")
    total = sum(len(d.get("secretary_list", [])) for d in save_data.values())
    found = sum(1 for d in save_data.values() if d.get("secretary_list"))
    print(f"Provinces with data: {found}/{len(PROVINCE_WIKI_PAGES)}")
    print(f"Total secretaries: {total}")

    return results


if __name__ == "__main__":
    scrape_all_provinces()
