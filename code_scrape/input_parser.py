"""
Phase 0 (v2): Manual Officials List Parser

Reads data/{city}_officials.txt — the authoritative name list maintained by the researcher.
Optionally supplements with Wikipedia list pages when available.

Format of txt file:
  城市：深圳
  省份：广东
  起始年份：2010
  维基列表_市长：https://zh.wikipedia.org/...  (optional)

  [市长]
  许勤, 2010.06-2017.01
  覃伟中（代）, 2021.04-2021.05

  [市委书记]
  王荣, 2010.04-2015.03
"""

import re
from pathlib import Path
from typing import Optional

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

try:
    from bs4 import BeautifulSoup
    HAS_BS4 = True
except ImportError:
    HAS_BS4 = False

DATA_DIR = Path(__file__).parent / "data"


# ── Parsing ────────────────────────────────────────────────────────────────────

def parse_date(s: str) -> str:
    """Normalise a date token to YYYY.MM or 至今."""
    s = s.strip()
    if s in ("至今", "present", "现任"):
        return "至今"
    # YYYY.MM
    if re.match(r"^\d{4}\.\d{2}$", s):
        return s
    # YYYY.M → zero-pad
    m = re.match(r"^(\d{4})\.(\d{1,2})$", s)
    if m:
        return f"{m.group(1)}.{int(m.group(2)):02d}"
    # YYYY only
    if re.match(r"^\d{4}$", s):
        return f"{s}.00"
    return s


def parse_entry(line: str) -> Optional[dict]:
    """
    Parse one official entry line.
    Examples:
      许勤, 2010.06-2017.01
      覃伟中（代）, 2021.04-2021.05
      孟凡利, 2022.04-至今
    Returns dict or None if unparseable.
    """
    line = line.strip()
    if not line or line.startswith("#"):
        return None

    # Split on comma: "姓名[（代）], 起始-终止"
    parts = line.split(",", 1)
    if len(parts) != 2:
        return None

    raw_name = parts[0].strip()
    date_part = parts[1].strip()

    # Detect acting flag
    acting = "（代）" in raw_name or "（代理）" in raw_name
    name = raw_name.replace("（代）", "").replace("（代理）", "").strip()

    # Parse dates
    if "-" in date_part:
        halves = date_part.split("-", 1)
        start = parse_date(halves[0])
        end = parse_date(halves[1])
    else:
        start = parse_date(date_part)
        end = "至今"

    return {
        "name": name,
        "acting": acting,
        "start": start,
        "end": end,
    }


def parse_officials_txt(city: str) -> dict:
    """
    Load and parse data/{city}_officials.txt.
    Returns:
      {
        "city": "深圳", "province": "广东", "start_year": 2010,
        "wiki_url_mayors": "https://...",   # optional
        "wiki_url_secretaries": "https://...", # optional
        "mayors": [{"name":..., "acting":bool, "start":..., "end":...}],
        "secretaries": [...],
        "all_officials": [{"name":..., "role":..., "start":..., "end":..., "needs_check":bool}]
      }
    """
    txt_path = DATA_DIR / f"{city}_officials.txt"
    if not txt_path.exists():
        raise FileNotFoundError(
            f"Name list not found: {txt_path}\n"
            f"Please create it manually following the format in data/深圳_officials.txt"
        )

    meta = {}
    mayors: list[dict] = []
    secretaries: list[dict] = []
    current_section = None

    for raw_line in txt_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()

        # Skip blank and comments
        if not line or line.startswith("#"):
            continue

        # Meta fields
        if "：" in line and not line.startswith("["):
            key, _, val = line.partition("：")
            key = key.strip()
            val = val.strip()
            if key == "城市":
                meta["city"] = val
            elif key == "省份":
                meta["province"] = val
            elif key == "起始年份":
                meta["start_year"] = int(val)
            elif key == "维基列表_市长":
                meta["wiki_url_mayors"] = val
            elif key == "维基列表_书记":
                meta["wiki_url_secretaries"] = val
            continue

        # Section headers (city-level + province-level + autonomous region)
        if line in ("[市长]", "[省长]", "[自治区主席]", "[主席]"):
            current_section = "mayors"
            continue
        if line in ("[市委书记]", "[书记]", "[党委书记]", "[省委书记]",
                     "[自治区党委书记]"):
            current_section = "secretaries"
            continue

        # Data lines
        if current_section:
            entry = parse_entry(line)
            if entry:
                if current_section == "mayors":
                    mayors.append(entry)
                else:
                    secretaries.append(entry)

    city_name = meta.get("city", city)
    province  = meta.get("province", "")
    # Default: if start_year not specified, keep all entries (use 1900)
    start_year = meta.get("start_year", 1900)

    # Filter by start_year
    def after_start(entry):
        try:
            y = int(entry["start"].split(".")[0])
            return y >= start_year
        except Exception:
            return True

    mayors     = [e for e in mayors     if after_start(e)]
    secretaries = [e for e in secretaries if after_start(e)]

    # Detect province-level vs city-level by checking if city field is absent
    is_province_level = "city" not in meta

    # Role labels depend on level
    mayor_role = "省长" if is_province_level else "市长"
    acting_mayor_role = "代省长" if is_province_level else "代市长"
    secretary_role = "省委书记" if is_province_level else "市委书记"

    # Build unified list (deduped by name)
    seen: dict[str, dict] = {}
    for entry in mayors:
        name = entry["name"]
        role = acting_mayor_role if entry["acting"] else mayor_role
        if name not in seen:
            seen[name] = {"name": name, "role": role,
                          "start": entry["start"], "end": entry["end"],
                          "needs_check": False}
        else:
            seen[name]["role"] = seen[name]["role"] + f"/{mayor_role}"

    for entry in secretaries:
        name = entry["name"]
        if name not in seen:
            seen[name] = {"name": name, "role": secretary_role,
                          "start": entry["start"], "end": entry["end"],
                          "needs_check": False}
        else:
            existing = seen[name]["role"]
            if "书记" not in existing:
                seen[name]["role"] = existing + f"/{secretary_role}"

    all_officials = sorted(seen.values(), key=lambda x: x.get("start", ""))

    print(f"✓ Loaded name list from {txt_path.name}")
    print(f"  市长: {len(mayors)} entries, 市委书记: {len(secretaries)} entries")
    print(f"  Unique officials: {len(all_officials)}")

    return {
        "city": city_name,
        "province": province,
        "start_year": start_year,
        "wiki_url_mayors":     meta.get("wiki_url_mayors", ""),
        "wiki_url_secretaries": meta.get("wiki_url_secretaries", ""),
        "mayors": mayors,
        "secretaries": secretaries,
        "all_officials": all_officials,
    }


# ── Wikipedia list page scraper (Phase 0 supplement) ──────────────────────────

def scrape_wiki_list(url: str, role: str) -> list[dict]:
    """
    Scrape a Wikipedia list page (e.g. 深圳市市长列表) to extract names + tenures.
    Returns list of {name, start, end} dicts.
    Used ONLY for verification — the txt file is authoritative.
    """
    if not url or not HAS_REQUESTS or not HAS_BS4:
        return []
    try:
        resp = requests.get(
            url,
            headers={"User-Agent": "Mozilla/5.0", "Accept-Language": "zh-CN"},
            timeout=10,
        )
        if resp.status_code != 200:
            return []
        soup = BeautifulSoup(resp.text, "html.parser")

        officials = []
        # Wikipedia list pages usually have wikitable
        for table in soup.find_all("table", class_=re.compile("wikitable")):
            for row in table.find_all("tr")[1:]:  # skip header
                cells = row.find_all(["td", "th"])
                if len(cells) < 2:
                    continue
                texts = [c.get_text(strip=True) for c in cells]
                # Heuristic: find cell with 2-4 Chinese chars as name
                name = None
                for t in texts:
                    if re.match(r"^[\u4e00-\u9fff]{2,4}$", t):
                        name = t
                        break
                if name:
                    officials.append({"name": name, "role": role, "source": "wikipedia"})

        return officials
    except Exception as e:
        print(f"  [WARN] Wikipedia scrape failed: {e}")
        return []


def compare_with_wiki(parsed: dict) -> list[str]:
    """
    Compare manual txt list against Wikipedia list page (if URL provided).
    Returns list of warning messages for names that appear in one but not the other.
    """
    warnings = []

    txt_mayor_names = {e["name"] for e in parsed["mayors"]}
    wiki_url = parsed.get("wiki_url_mayors", "")
    if wiki_url:
        wiki_mayors = scrape_wiki_list(wiki_url, "市长")
        wiki_names = {e["name"] for e in wiki_mayors}
        in_wiki_not_txt = wiki_names - txt_mayor_names
        in_txt_not_wiki = txt_mayor_names - wiki_names
        for n in in_wiki_not_txt:
            warnings.append(f"⚠ [市长] '{n}' 在维基百科列表中但不在手动名单里")
        for n in in_txt_not_wiki:
            warnings.append(f"  [市长] '{n}' 在手动名单但维基无记录（可能是近期/代理）")

    return warnings


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse, json
    parser = argparse.ArgumentParser(description="Parse city officials name list")
    parser.add_argument("--city", default="深圳")
    parser.add_argument("--compare-wiki", action="store_true",
                        help="Cross-check against Wikipedia list page")
    args = parser.parse_args()

    data = parse_officials_txt(args.city)

    if args.compare_wiki:
        warnings = compare_with_wiki(data)
        if warnings:
            print("\n名单对比警告：")
            for w in warnings:
                print(f"  {w}")
        else:
            print("\n✓ 名单与维基百科一致，无差异")

    print("\n最终名单：")
    for o in data["all_officials"]:
        print(f"  {o['name']} ({o['role']}) {o['start']}→{o['end']}")

    print(f"\n市长列表：")
    for e in data["mayors"]:
        flag = "（代）" if e["acting"] else ""
        print(f"  {e['name']}{flag}  {e['start']}→{e['end']}")

    print(f"\n书记列表：")
    for e in data["secretaries"]:
        print(f"  {e['name']}  {e['start']}→{e['end']}")
