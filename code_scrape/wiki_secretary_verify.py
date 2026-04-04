"""
Fetch Wikipedia committee pages for 12 provinces and extract secretary lists.
Compare with manual supplement data for verification.
"""
import json
import re
import urllib.request
import urllib.parse
from html.parser import HTMLParser
from pathlib import Path

DATA_DIR = Path("data")

# The 12 provinces that were supplemented manually
PROVINCES_TO_VERIFY = {
    "北京": "中国共产党北京市委员会",
    "天津": "中共天津市委书记列表",  # standalone list page
    "山西": "中国共产党山西省委员会",
    "湖南": "中国共产党湖南省委员会",
    "浙江": "中国共产党浙江省委员会",
    "四川": "中国共产党四川省委员会",
    "云南": "中国共产党云南省委员会",
    "西藏": "中国共产党西藏自治区委员会",
    "吉林": "中国共产党吉林省委员会",
    "安徽": "中国共产党安徽省委员会",
    "河南": "河南省省委书记列表",  # standalone list page
    "宁夏": "中国共产党宁夏回族自治区委员会",
}

# Also try these alternative pages if primary fails
ALTERNATIVE_PAGES = {
    "天津": "中国共产党天津市委员会",
    "河南": "中国共产党河南省委员会",
    "安徽": "历任安徽省党政机关正职领导列表",
}


def fetch_wiki_html(title: str) -> str:
    """Fetch Wikipedia page HTML via API for cleaner parsing."""
    # Use Wikipedia API to get parsed HTML
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
        "User-Agent": "Mozilla/5.0 (research project) wiki-scraper/1.0"
    })
    try:
        resp = urllib.request.urlopen(req)
        data = json.loads(resp.read().decode("utf-8"))
        if "parse" in data:
            return data["parse"]["wikitext"]["*"]
        else:
            print(f"  API error for '{title}': {data.get('error', {}).get('info', 'unknown')}")
            return ""
    except Exception as e:
        print(f"  Fetch error for '{title}': {e}")
        return ""


def extract_secretaries_from_wikitext(wikitext: str, province: str) -> list:
    """Extract secretary names and terms from wikitext."""
    secretaries = []

    # Strategy 1: Look for tables with secretary data
    # Wikipedia tables often have format: || [[Name]] || dates || role

    # Strategy 2: Look for list items with secretary entries
    # Common patterns in wikitext:
    # * [[彭真]]（1949年—1966年5月）
    # | [[彭真]] || 1949年 || 1966年5月 || 书记/第一书记

    lines = wikitext.split("\n")

    # Track which section we're in
    in_secretary_section = False
    section_depth = 0

    # Section header patterns for secretary lists
    secretary_section_patterns = [
        r"历届书记", r"历任书记", r"历届.*第一书记",
        r"书记.*列表", r"历届组成人员", r"领导人",
        r"历届.*领导", r"书记",
    ]

    # Patterns to exit secretary section
    exit_patterns = [
        r"^={2,3}\s*(副书记|常委|委员|秘书长|纪检|组织|宣传|统战|政法|参考|注释|参见|外部)",
    ]

    for i, line in enumerate(lines):
        # Check for section headers
        header_match = re.match(r'^(={2,4})\s*(.+?)\s*\1', line)
        if header_match:
            depth = len(header_match.group(1))
            title = header_match.group(2)

            # Check if entering secretary section
            for pat in secretary_section_patterns:
                if re.search(pat, title):
                    in_secretary_section = True
                    section_depth = depth
                    break

            # Check if exiting secretary section
            if in_secretary_section and depth <= section_depth:
                for pat in exit_patterns:
                    if re.search(pat, line):
                        in_secretary_section = False
                        break
            continue

        if not in_secretary_section:
            # Also try to find secretary data outside sections (for list pages)
            # List pages may have tables directly
            pass

        # Extract names and dates from various formats
        # Format 1: Table rows: | [[Name]] || dates
        # Format 2: List items: * [[Name]]（dates）
        # Format 3: Inline: [[Name]]，dates

        # Find all wiki-linked names
        name_matches = re.findall(r'\[\[([^\]|]+?)(?:\|([^\]]+))?\]\]', line)

        # Find date patterns
        date_pattern = r'(\d{4}年\d{0,2}月?\d{0,2}日?)\s*[—–\-至]\s*(\d{4}年\d{0,2}月?\d{0,2}日?|至今|现任)?'
        date_matches = re.findall(date_pattern, line)

        # Also look for role markers
        has_secretary_role = bool(re.search(r'(第一)?书记', line))

        if name_matches and (date_matches or has_secretary_role) and in_secretary_section:
            for name_tuple in name_matches:
                name = name_tuple[1] if name_tuple[1] else name_tuple[0]
                # Skip non-person entries
                if any(skip in name for skip in ["中国共产党", "委员会", "省", "市", "区", "县"]):
                    continue
                # Skip if name is too long (likely not a person)
                if len(name) > 5:
                    continue

                term = ""
                if date_matches:
                    start, end = date_matches[0]
                    term = f"{start}—{end}" if end else f"{start}—"

                role = "第一书记" if "第一书记" in line else "书记"

                secretaries.append({
                    "name": name,
                    "role": role,
                    "term": term,
                    "line_num": i,
                })

    # If section-based extraction failed, try full-page table extraction
    if not secretaries:
        secretaries = extract_from_tables(wikitext, province)

    return secretaries


def extract_from_tables(wikitext: str, province: str) -> list:
    """Extract from wiki table format."""
    secretaries = []

    # Find table rows with names and dates
    # Wiki table format: |-\n| data || data || data
    in_table = False
    current_row = []

    for line in wikitext.split("\n"):
        if line.strip().startswith("{|"):
            in_table = True
            continue
        if line.strip() == "|}":
            in_table = False
            continue
        if line.strip() == "|-":
            # Process previous row
            if current_row:
                row_text = " ".join(current_row)
                # Look for name + dates in this row
                names = re.findall(r'\[\[([^\]|]+?)(?:\|([^\]]+))?\]\]', row_text)
                dates = re.findall(r'(\d{4}年\d{0,2}月?\d{0,2}日?)', row_text)
                has_shuji = "书记" in row_text

                for name_tuple in names:
                    name = name_tuple[1] if name_tuple[1] else name_tuple[0]
                    if len(name) <= 5 and not any(skip in name for skip in ["共产党", "委员会"]):
                        term = ""
                        if len(dates) >= 2:
                            term = f"{dates[0]}—{dates[1]}"
                        elif len(dates) == 1:
                            term = f"{dates[0]}—"

                        if has_shuji or dates:
                            secretaries.append({
                                "name": name,
                                "role": "第一书记" if "第一书记" in row_text else "书记",
                                "term": term,
                            })
            current_row = []
            continue

        if in_table and (line.startswith("|") or line.startswith("!")):
            current_row.append(line)

    return secretaries


def main():
    results = {}

    for province, page_title in PROVINCES_TO_VERIFY.items():
        print(f"\n{'='*60}")
        print(f"Fetching: {province} ({page_title})")

        wikitext = fetch_wiki_html(page_title)
        if not wikitext:
            # Try alternative
            alt = ALTERNATIVE_PAGES.get(province)
            if alt:
                print(f"  Trying alternative: {alt}")
                wikitext = fetch_wiki_html(alt)

        if not wikitext:
            print(f"  ✗ Failed to fetch")
            results[province] = {"status": "fetch_failed", "secretaries": []}
            continue

        print(f"  Wikitext length: {len(wikitext)}")

        # Save raw wikitext for debugging
        Path("temp").mkdir(exist_ok=True)
        Path(f"temp/wiki_{province}_wikitext.txt").write_text(wikitext, encoding="utf-8")

        secretaries = extract_secretaries_from_wikitext(wikitext, province)

        # Deduplicate by name
        seen = set()
        unique = []
        for s in secretaries:
            if s["name"] not in seen:
                seen.add(s["name"])
                unique.append(s)

        results[province] = {
            "status": "ok",
            "count": len(unique),
            "secretaries": unique,
        }

        print(f"  Found {len(unique)} secretaries:")
        for s in unique:
            print(f"    {s['name']:8s} {s.get('term', '')}")

    # Save results
    output_path = DATA_DIR / "wiki_secretary_verification.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"\nSaved to {output_path}")

    # Compare with manual supplement
    print(f"\n{'='*60}")
    print("COMPARISON: Manual supplement vs Wikipedia")
    print(f"{'='*60}")

    with open(DATA_DIR / "wiki_secretaries_clean.json", encoding="utf-8") as f:
        manual = json.load(f)

    for province in PROVINCES_TO_VERIFY:
        wiki_names = [s["name"] for s in results.get(province, {}).get("secretaries", [])]
        manual_names = [s["name"] for s in manual.get(province, {}).get("secretary_list", [])]

        only_manual = set(manual_names) - set(wiki_names)
        only_wiki = set(wiki_names) - set(manual_names)
        common = set(manual_names) & set(wiki_names)

        status = "✓" if not only_manual and not only_wiki else "⚠"
        print(f"\n{status} {province}: manual={len(manual_names)}, wiki={len(wiki_names)}, common={len(common)}")
        if only_manual:
            print(f"  Only in manual: {', '.join(only_manual)}")
        if only_wiki:
            print(f"  Only in wiki:   {', '.join(only_wiki)}")


if __name__ == "__main__":
    main()
