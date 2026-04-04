"""
v3: Smart parser for Wikipedia committee pages.
Handles two formats:
1. Clean bullet-point sections (=== 历届书记 ===)
2. Per-term blocks (; 第X届) with role labels (第一书记/书记)
"""
import json
import re
import urllib.request
import urllib.parse
from pathlib import Path

DATA_DIR = Path("data")
TEMP_DIR = Path("temp")

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

ALT_PAGES = {
    "天津": "中国共产党天津市委员会",
    "河南": "中国共产党河南省委员会",
}


def fetch_wikitext(title: str) -> str:
    api_url = "https://zh.wikipedia.org/w/api.php"
    params = {
        "action": "parse", "page": title,
        "prop": "wikitext", "format": "json", "redirects": "1",
    }
    url = f"{api_url}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (research) wiki-scraper/3.0"
    })
    try:
        resp = urllib.request.urlopen(req, timeout=30)
        data = json.loads(resp.read().decode("utf-8"))
        if "parse" in data:
            return data["parse"]["wikitext"]["*"]
    except Exception as e:
        print(f"  Error: {e}")
    return ""


def clean_wikitext(text: str) -> str:
    """Strip wiki templates that break date parsing."""
    # {{0}} renders as nothing/zero-width space
    text = re.sub(r'\{\{0\}\}', '', text)
    # Other common templates: {{snd}}, {{spnd}}, {{0}}
    text = re.sub(r'\{\{snd\}\}', '—', text)
    text = re.sub(r'\{\{spnd\}\}', '—', text)
    return text


def clean_name(raw: str) -> str:
    """Clean wiki name: remove disambiguation, formatting."""
    name = re.sub(r'\s*\(.*?\)', '', raw)  # "尹力 (1962年)" → "尹力"
    name = name.replace('\u3000', '').strip()  # full-width space
    name = re.sub(r'·', '·', name)  # normalize middle dot
    return name


def extract_names_from_text(text: str) -> list:
    """Extract all wiki-linked person names from text."""
    matches = re.findall(r'\[\[([^\]|]+?)(?:\|([^\]]+))?\]\]', text)
    names = []
    for m in matches:
        raw = m[1] if m[1] else m[0]
        name = clean_name(raw)
        if 2 <= len(name) <= 6 and not any(s in name for s in ["共产党", "委员", "省", "市", "区", "县", "中央", "政治"]):
            names.append(name)
    return names


def extract_dates_from_text(text: str) -> tuple:
    """Extract start-end date pair from text, ignoring dates inside [[wiki links]]."""
    # Strip wiki link internals to avoid picking up disambiguation years like "(1913年)"
    cleaned = re.sub(r'\[\[[^\]]*\]\]', '', text)
    m = re.search(r'(\d{4}年\d{0,2}月?\d{0,2}日?)\s*[—–\-－一]\s*(\d{4}年\d{0,2}月?\d{0,2}日?|至今|现任|)', cleaned)
    if m:
        return m.group(1), m.group(2) or ""
    # Single date
    m = re.search(r'(\d{4}年\d{0,2}月?\d{0,2}日?)', cleaned)
    if m:
        return m.group(1), ""
    return "", ""


def get_committee_section(wikitext: str) -> str:
    """Extract the 历届组成人员 / 组成人员 section."""
    # Find section start
    patterns = [r"==\s*历届组成人员\s*==", r"==\s*组成人员\s*==", r"==\s*历届领导\s*=="]
    start = -1
    for pat in patterns:
        m = re.search(pat, wikitext)
        if m:
            start = m.start()
            break
    if start == -1:
        return ""

    # Find section end (next == level header)
    rest = wikitext[start:]
    lines = rest.split("\n")
    end_line = len(lines)
    for i, line in enumerate(lines[1:], 1):
        if re.match(r'^==\s+[^=]', line):
            end_line = i
            break

    return "\n".join(lines[:end_line])


def parse_clean_secretary_section(wikitext: str, province: str) -> list:
    """
    Parse provinces with a dedicated === 历届书记 === bullet section.
    Returns list of secretaries or empty list if section not found.
    """
    lines = wikitext.split("\n")
    secretaries = []
    in_section = False
    section_depth = 0

    header_patterns = [
        r"历届.*书记.*第一书记", r"历届.*第一书记.*书记",
        r"历任.*省委书记", r"历任.*市委书记", r"历任.*区委书记",
        r"历届.*书记$", r"历任书记",
    ]

    for i, line in enumerate(lines):
        header_match = re.match(r'^(={2,4})\s*(.+?)\s*\1', line)
        if header_match:
            depth = len(header_match.group(1))
            title = header_match.group(2).strip()

            if in_section and depth <= section_depth:
                break  # Left the section

            for pat in header_patterns:
                if re.search(pat, title):
                    in_section = True
                    section_depth = depth
                    break
            continue

        if not in_section or not line.strip().startswith("*"):
            continue

        # Parse bullet entry
        names = extract_names_from_text(line)
        start_date, end_date = extract_dates_from_text(line)
        term = f"{start_date}—{end_date}" if start_date else ""

        note = None
        if "同时担任" in line:
            note = "同时担任"

        for name in names:
            secretaries.append({
                "name": name, "term": term,
                "role_title": "第一书记" if "第一书记" in line else "书记",
                "note": note,
            })

    return secretaries


def parse_per_term_blocks(section_text: str, province: str) -> list:
    """
    Parse per-term blocks like:
    ; 第五届省委（1985年7月一1991年2月）
    *书记：[[李立功]]
    *第一书记：[[某某]]（1985年—1991年）

    Extracts ONLY 第一书记 or 书记 (not 副书记, 常委, etc.)
    """
    secretaries = []
    lines = section_text.split("\n")

    current_届_dates = ("", "")

    for line in lines:
        stripped = line.strip()

        # Detect 届 header: ; 第X届（dates）or ; 早期省委书记
        if stripped.startswith(";"):
            # Extract dates from header
            m = re.search(r'(\d{4}年\d{0,2}月?\d{0,2}日?)\s*[—–\-至一]\s*(\d{4}年\d{0,2}月?\d{0,2}日?|)', stripped)
            if m:
                current_届_dates = (m.group(1), m.group(2) or "")
            else:
                current_届_dates = ("", "")

            # Check for "早期省委书记" header
            if re.search(r'早期.*(书记|省委)', stripped):
                continue

            continue

        # Match role-labeled lines: *第一书记/书记：[[Name]]
        # Must be 第一书记 or standalone 书记 (not 副书记, 常务书记, 候补书记)
        role_match = re.match(
            r'\*\s*(第一书记|书\s*记)\s*[：:]\s*(.*)',
            stripped
        )

        if not role_match:
            # Also match: * 书记：（line starting with 书记）
            role_match = re.match(
                r'\*\s*(第一书记|书　记)\s*[：:]\s*(.*)',
                stripped
            )

        if role_match:
            role = role_match.group(1).replace('\u3000', '').replace(' ', '')
            rest = role_match.group(2)

            # Handle "副书记" false positive: skip if line has 副 before 书记
            if "副书记" in stripped and "第一书记" not in stripped:
                continue

            # Extract all names and their individual dates
            # Pattern: [[Name]]（dates）、[[Name2]]（dates）
            segments = re.split(r'[、，,]', rest)

            for seg in segments:
                names = extract_names_from_text(seg)
                seg_start, seg_end = extract_dates_from_text(seg)

                for name in names:
                    if seg_start:
                        term = f"{seg_start}—{seg_end}"
                    elif current_届_dates[0]:
                        term = f"{current_届_dates[0]}—{current_届_dates[1]}"
                    else:
                        term = ""

                    secretaries.append({
                        "name": name, "term": term,
                        "role_title": role, "note": None,
                    })

        # Also handle lines that are just names under "早期省委书记"
        # These look like: * [[程子华]]
        if stripped.startswith("*") and not role_match:
            # Only capture if it's a simple "* [[Name]]" without role label
            # This handles the "早期省委书记" entries
            if re.match(r'^\*\s*\[\[', stripped) and "：" not in stripped and ":" not in stripped:
                names = extract_names_from_text(stripped)
                start_d, end_d = extract_dates_from_text(stripped)
                for name in names:
                    # Only add if we're right after "早期省委书记" header
                    # Check context by looking for preceding ; header
                    pass  # Skip these ambiguous entries

    return secretaries


def parse_list_page_table(wikitext: str, province: str) -> list:
    """Parse standalone list pages with table format (河南, 天津)."""
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
            # Process last row
            if current_cells:
                _process_table_row(current_cells, secretaries)
            in_table = False
            current_cells = []
            continue
        if stripped == "|-":
            if current_cells:
                _process_table_row(current_cells, secretaries)
            current_cells = []
            continue

        if in_table and (stripped.startswith("|") or stripped.startswith("!")):
            current_cells.append(stripped)

    return secretaries


def _process_table_row(cells, secretaries):
    row_text = " || ".join(cells)
    # Must have 书记 role
    if not re.search(r'(第一)?书记', row_text):
        return

    names = extract_names_from_text(row_text)
    dates = re.findall(r'(\d{4}年\d{0,2}月?\d{0,2}日?)', row_text)

    for name in names:
        term = ""
        if len(dates) >= 2:
            term = f"{dates[0]}—{dates[1]}"
        elif len(dates) == 1:
            term = f"{dates[0]}—"

        secretaries.append({
            "name": name, "term": term,
            "role_title": "第一书记" if "第一书记" in row_text else "书记",
            "note": None,
        })


def parse_province(wikitext: str, province: str) -> list:
    """Main dispatcher: try all parsing strategies."""
    # Pre-clean wikitext
    wikitext = clean_wikitext(wikitext)

    # Strategy 1: Clean === 历届书记 === section (best)
    secs = parse_clean_secretary_section(wikitext, province)
    if len(secs) >= 5:
        return secs

    # Strategy 2: Per-term blocks in 历届组成人员
    section = get_committee_section(wikitext)
    if section:
        secs2 = parse_per_term_blocks(section, province)
        if len(secs2) > len(secs):
            secs = secs2

    # Strategy 3: Table format (for standalone list pages)
    if len(secs) < 5:
        secs3 = parse_list_page_table(wikitext, province)
        if len(secs3) > len(secs):
            secs = secs3

    return secs


def deduplicate(secretaries: list) -> list:
    """Deduplicate by name, keeping best entry."""
    seen = {}
    result = []
    for s in secretaries:
        name = s["name"]
        if name not in seen:
            seen[name] = s
            result.append(s)
        else:
            # Prefer entry with term dates
            if s.get("term") and not seen[name].get("term"):
                idx = result.index(seen[name])
                result[idx] = s
                seen[name] = s
    return result


def main():
    all_results = {}

    for province, page_title in WIKI_PAGES.items():
        print(f"\n{province} → {page_title}")

        wikitext = fetch_wikitext(page_title)
        if not wikitext and province in ALT_PAGES:
            print(f"  Trying alt: {ALT_PAGES[province]}")
            wikitext = fetch_wikitext(ALT_PAGES[province])

        if not wikitext:
            print(f"  ✗ Failed")
            all_results[province] = {"province": province, "source": "wikipedia_v3", "secretary_list": []}
            continue

        (TEMP_DIR / f"wiki_{province}_wikitext.txt").write_text(wikitext, encoding="utf-8")

        secs = parse_province(wikitext, province)
        secs = deduplicate(secs)

        # Clean output format
        clean_list = [
            {"name": s["name"], "term": s["term"], "role_title": s.get("role_title", ""), "note": s.get("note")}
            for s in secs
        ]

        all_results[province] = {
            "province": province,
            "source": "wikipedia_v3",
            "secretary_list": clean_list,
        }

        print(f"  ✓ {len(secs)} secretaries")
        for s in secs:
            print(f"    {s['name']:10s} {s.get('term', '')}")

    # Save
    out = DATA_DIR / "wiki_secretaries_v3.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2)
    print(f"\nSaved: {out}")

    # Summary comparison
    print(f"\n{'='*70}")
    print(f"{'Province':8s} {'Wiki_v3':>8s} {'Manual':>8s} {'Common':>8s} Status")
    print(f"{'='*70}")

    with open(DATA_DIR / "wiki_secretaries_clean.json", encoding="utf-8") as f:
        manual = json.load(f)

    total_wiki = 0
    total_manual = 0
    wiki_better = 0
    manual_better = 0

    for province in WIKI_PAGES:
        w_names = [s["name"] for s in all_results.get(province, {}).get("secretary_list", [])]
        m_names = [s["name"] for s in manual.get(province, {}).get("secretary_list", [])]
        common = set(w_names) & set(m_names)

        wc = len(w_names)
        mc = len(m_names)
        total_wiki += wc
        total_manual += mc

        if wc >= mc and wc > 0:
            status = "wiki ≥"
            wiki_better += 1
        elif mc > wc and mc > 0:
            status = "manual >"
            manual_better += 1
        else:
            status = "both empty"

        only_m = set(m_names) - set(w_names)
        print(f"{province:8s} {wc:8d} {mc:8d} {len(common):8d} {status}", end="")
        if only_m and wc > 0:
            print(f"  (manual extra: {', '.join(list(only_m)[:3])})", end="")
        print()

    print(f"{'='*70}")
    print(f"{'Total':8s} {total_wiki:8d} {total_manual:8d}")
    print(f"Wiki better: {wiki_better}, Manual better: {manual_better}")


if __name__ == "__main__":
    main()
