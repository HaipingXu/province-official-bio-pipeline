"""
v2: Parse Wikipedia committee pages for secretary lists.
Targets the clean "历届书记" bullet-point sections, not the full committee tables.
"""
import json
import re
import urllib.request
import urllib.parse
from pathlib import Path

DATA_DIR = Path("data")
TEMP_DIR = Path("temp")
TEMP_DIR.mkdir(exist_ok=True)

# All 31 provinces with their Wikipedia page titles
# Use the best available page (standalone list > committee page)
WIKI_PAGES = {
    "北京": "中国共产党北京市委员会",
    "天津": "中共天津市委书记列表",
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
    "河南": "河南省省委书记列表",
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

# Alternative pages to try if primary fails
ALT_PAGES = {
    "天津": "中国共产党天津市委员会",
    "河南": "中国共产党河南省委员会",
}


def fetch_wikitext(title: str) -> str:
    """Fetch wikitext via Wikipedia API."""
    api_url = "https://zh.wikipedia.org/w/api.php"
    params = {
        "action": "parse",
        "page": title,
        "prop": "wikitext",
        "format": "json",
        "redirects": "1",
    }
    url = f"{api_url}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (research) wiki-scraper/2.0"
    })
    try:
        resp = urllib.request.urlopen(req, timeout=30)
        data = json.loads(resp.read().decode("utf-8"))
        if "parse" in data:
            return data["parse"]["wikitext"]["*"]
    except Exception as e:
        print(f"  Error fetching '{title}': {e}")
    return ""


def parse_name_from_wikilink(text: str) -> str:
    """Extract clean name from [[Name|Display]] or [[Name]]."""
    m = re.search(r'\[\[([^\]|]+?)(?:\|([^\]]+))?\]\]', text)
    if m:
        display = m.group(2) if m.group(2) else m.group(1)
        # Remove disambiguation like "尹力 (1962年)" → "尹力"
        display = re.sub(r'\s*\(.*?\)', '', display)
        # Remove full-width spaces used for formatting
        display = display.replace('\u3000', '').strip()
        return display
    return ""


def extract_secretary_list(wikitext: str, province: str) -> list:
    """
    Extract secretary list from the dedicated section.
    Targets bullet-point lists like:
      * [[彭真]]（1948年12月—1966年5月）
    """
    lines = wikitext.split("\n")
    secretaries = []

    # Phase 1: Find the secretary section header
    secretary_section_start = -1
    secretary_section_depth = 0

    # Patterns for the secretary list header
    header_patterns = [
        r"历届.*书记.*第一书记",
        r"历届.*第一书记.*书记",
        r"历任.*省委书记",
        r"历任.*市委书记",
        r"历任.*区委书记",
        r"历届.*书记",
        r"历任书记",
        r"书记.*列表",
    ]

    for i, line in enumerate(lines):
        header_match = re.match(r'^(={2,4})\s*(.+?)\s*\1', line)
        if header_match:
            depth = len(header_match.group(1))
            title = header_match.group(2).strip()

            for pat in header_patterns:
                if re.search(pat, title):
                    secretary_section_start = i
                    secretary_section_depth = depth
                    break

    if secretary_section_start == -1:
        print(f"  ⚠ No secretary section header found for {province}")
        # Fallback: try to find bullet points with secretary-like content anywhere
        return extract_secretary_fallback(wikitext, province)

    # Phase 2: Extract bullet-point entries from the section
    for i in range(secretary_section_start + 1, len(lines)):
        line = lines[i].strip()

        # Check if we've left the section (hit a same-or-higher-level header)
        header_match = re.match(r'^(={2,4})\s*(.+?)\s*\1', line)
        if header_match:
            depth = len(header_match.group(1))
            if depth <= secretary_section_depth:
                break
            continue

        # Skip empty lines
        if not line:
            continue

        # Match bullet-point entries: * [[Name]]（dates）
        if not line.startswith("*"):
            continue

        # Extract name(s) and dates
        names_in_line = re.findall(r'\[\[([^\]|]+?)(?:\|([^\]]+))?\]\]', line)
        date_match = re.search(
            r'[（(]?\s*(\d{4}年\d{0,2}月?\d{0,2}日?)\s*[—–\-]\s*(\d{4}年\d{0,2}月?\d{0,2}日?|至今|现任|)\s*[）)]?',
            line
        )

        for name_tuple in names_in_line:
            raw_name = name_tuple[1] if name_tuple[1] else name_tuple[0]
            name = re.sub(r'\s*\(.*?\)', '', raw_name).replace('\u3000', '').strip()

            # Skip non-person entries
            if len(name) > 5 or len(name) < 2:
                continue
            if any(skip in name for skip in ["共产党", "委员会", "省", "市", "自治区"]):
                continue

            term = ""
            if date_match:
                start, end = date_match.group(1), date_match.group(2)
                term = f"{start}—{end}" if end else f"{start}—"

            # Detect role
            role = "第一书记" if "第一书记" in line else "书记"

            # Check for notes (e.g., "两人同时担任", "代理")
            note = None
            if "同时担任" in line:
                note = "同时担任"
            if "代理" in line or "代" in line:
                note = "代理"

            secretaries.append({
                "name": name,
                "term": term,
                "role_title": role,
                "note": note,
                "raw_line": line[:100],
            })

    return secretaries


def extract_secretary_fallback(wikitext: str, province: str) -> list:
    """Fallback: parse table-based secretary lists (for standalone list pages like 河南)."""
    secretaries = []
    lines = wikitext.split("\n")

    in_table = False
    current_cells = []

    for line in lines:
        stripped = line.strip()

        if stripped.startswith("{|"):
            in_table = True
            current_cells = []
            continue
        if stripped == "|}":
            in_table = False
            continue
        if stripped == "|-":
            # Process accumulated cells
            if current_cells:
                row_text = " || ".join(current_cells)
                # Look for 书记 or 第一书记 in the row
                if re.search(r'(第一)?书记', row_text):
                    names = re.findall(r'\[\[([^\]|]+?)(?:\|([^\]]+))?\]\]', row_text)
                    dates = re.findall(r'(\d{4}年\d{0,2}月?\d{0,2}日?)', row_text)

                    for name_tuple in names:
                        raw_name = name_tuple[1] if name_tuple[1] else name_tuple[0]
                        name = re.sub(r'\s*\(.*?\)', '', raw_name).replace('\u3000', '').strip()
                        if 2 <= len(name) <= 5 and not any(s in name for s in ["共产党", "委员会"]):
                            term = ""
                            if len(dates) >= 2:
                                term = f"{dates[0]}—{dates[1]}"
                            elif len(dates) == 1:
                                term = f"{dates[0]}—"
                            secretaries.append({
                                "name": name,
                                "term": term,
                                "role_title": "第一书记" if "第一书记" in row_text else "书记",
                                "note": None,
                            })
            current_cells = []
            continue

        if in_table and (stripped.startswith("|") or stripped.startswith("!")):
            # Split by || for multi-cell rows
            cells = re.split(r'\|\|', stripped.lstrip("|!"))
            current_cells.extend(cells)

    return secretaries


def main():
    results = {}
    all_wiki_secretaries = {}

    for province, page_title in WIKI_PAGES.items():
        print(f"\n{'='*50}")
        print(f"{province} → {page_title}")

        wikitext = fetch_wikitext(page_title)
        if not wikitext and province in ALT_PAGES:
            page_title = ALT_PAGES[province]
            print(f"  Trying alt: {page_title}")
            wikitext = fetch_wikitext(page_title)

        if not wikitext:
            print(f"  ✗ Failed to fetch")
            results[province] = {"count": 0, "secretaries": []}
            continue

        # Save wikitext for debugging
        (TEMP_DIR / f"wiki_{province}_wikitext.txt").write_text(wikitext, encoding="utf-8")

        secs = extract_secretary_list(wikitext, province)

        # Deduplicate by name (keep first occurrence with better data)
        seen = {}
        unique = []
        for s in secs:
            if s["name"] not in seen:
                seen[s["name"]] = s
                unique.append(s)
            else:
                # Update if this entry has a term and the existing one doesn't
                if s.get("term") and not seen[s["name"]].get("term"):
                    seen[s["name"]].update(s)

        results[province] = {"count": len(unique), "secretaries": unique}

        # Store clean version
        all_wiki_secretaries[province] = {
            "province": province,
            "source": "wikipedia_v2",
            "secretary_list": [
                {"name": s["name"], "term": s["term"], "role_title": s.get("role_title", ""), "note": s.get("note")}
                for s in unique
            ],
        }

        print(f"  ✓ Found {len(unique)} secretaries:")
        for s in unique:
            print(f"    {s['name']:8s} {s.get('term', ''):30s} {s.get('note', '') or ''}")

    # Save all wiki results
    output = DATA_DIR / "wiki_secretaries_v2.json"
    with open(output, "w", encoding="utf-8") as f:
        json.dump(all_wiki_secretaries, f, ensure_ascii=False, indent=2)
    print(f"\nSaved wiki v2 data to {output}")

    # Comparison with current data
    print(f"\n{'='*60}")
    print("COMPARISON: Current data vs Wikipedia v2")
    print(f"{'='*60}")

    with open(DATA_DIR / "wiki_secretaries_clean.json", encoding="utf-8") as f:
        current = json.load(f)

    for province in WIKI_PAGES:
        wiki_names = [s["name"] for s in results.get(province, {}).get("secretaries", [])]
        curr_names = [s["name"] for s in current.get(province, {}).get("secretary_list", [])]

        wiki_set = set(wiki_names)
        curr_set = set(curr_names)
        only_curr = curr_set - wiki_set
        only_wiki = wiki_set - curr_set
        common = curr_set & wiki_set

        icon = "✓" if len(wiki_names) > 0 else "✗"
        print(f"{icon} {province:6s}: wiki={len(wiki_names):3d}, current={len(curr_names):3d}, common={len(common):3d}", end="")
        if only_curr:
            print(f"  | only_current: {', '.join(sorted(only_curr)[:5])}", end="")
        if only_wiki:
            print(f"  | only_wiki: {', '.join(sorted(only_wiki)[:5])}", end="")
        print()


if __name__ == "__main__":
    main()
