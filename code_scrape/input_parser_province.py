"""
Phase 0 (Province): Provincial Officials List Parser

Reads data/{province}_officials.txt — the authoritative name list.

Format of txt file:
  # XX省主官名单
  省份：安徽省

  [省长]
  曾希圣, 1952.08-1955.03
  王清宪, 2021.02-至今

  [省委书记]
  曾希圣, 1952.01-1962.02
  梁言顺, 2024.06-至今

Section headers supported:
  Governors: [省长] [市长] [主席]
  Secretaries: [省委书记] [市委书记] [自治区党委书记] [书记] [党委书记]
"""

import re
from pathlib import Path
from typing import Optional

DATA_DIR = Path(__file__).parent / "data"

# Section header patterns
GOVERNOR_HEADERS = {"[省长]", "[市长]", "[主席]"}
SECRETARY_HEADERS = {"[省委书记]", "[市委书记]", "[自治区党委书记]", "[书记]", "[党委书记]"}


# ── Parsing ────────────────────────────────────────────────────────────────────

def parse_date(s: str) -> str:
    """Normalise a date token to YYYY.MM or 至今."""
    s = s.strip()
    if s in ("至今", "present", "现任", ""):
        return "至今"
    # YYYY.MM already
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
      曾希圣, 1952.08-1955.03
      王清宪, 2021.02-至今
      覃伟中（代）, 2021.04-2021.05
    Returns dict or None if unparseable.
    """
    line = line.strip()
    if not line or line.startswith("#"):
        return None

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


def parse_province_officials_txt(province: str, data_dir: Path | None = None) -> dict:
    """
    Load and parse data/{province}_officials.txt.

    Args:
        province: Province short name (e.g. "安徽", "北京", "内蒙古")
        data_dir: Override data directory (e.g. DATA_DIR / "1990")

    Returns:
      {
        "province": "安徽省",
        "province_short": "安徽",
        "gov_title": "省长",        # section header used
        "sec_title": "省委书记",     # section header used
        "governors": [{"name":..., "acting":bool, "start":..., "end":...}],
        "secretaries": [...],
        "all_officials": [{"name":..., "role":..., "start":..., "end":..., "needs_check":bool}]
      }
    """
    _dir = data_dir or DATA_DIR
    txt_path = _dir / f"{province}_officials.txt"
    if not txt_path.exists():
        raise FileNotFoundError(
            f"Name list not found: {txt_path}\n"
            f"Please create it (use generate_province_lists.py)"
        )

    meta = {}
    governors: list[dict] = []
    secretaries: list[dict] = []
    current_section = None
    gov_title = "省长"
    sec_title = "省委书记"

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
            if key == "省份":
                meta["province"] = val
            elif key == "起始年份":
                meta["start_year"] = int(val)
            continue

        # Section headers
        if line in GOVERNOR_HEADERS:
            current_section = "governors"
            gov_title = line[1:-1]  # strip brackets
            continue
        if line in SECRETARY_HEADERS:
            current_section = "secretaries"
            sec_title = line[1:-1]
            continue

        # Data lines
        if current_section:
            entry = parse_entry(line)
            if entry:
                if current_section == "governors":
                    governors.append(entry)
                else:
                    secretaries.append(entry)

    province_full = meta.get("province", province)
    start_year = meta.get("start_year", None)

    # Optionally filter by start_year
    if start_year:
        def after_start(entry):
            try:
                y = int(entry["start"].split(".")[0])
                return y >= start_year
            except Exception:
                return True
        governors = [e for e in governors if after_start(e)]
        secretaries = [e for e in secretaries if after_start(e)]

    # Build unified list (deduped by name)
    seen: dict[str, dict] = {}
    for entry in governors:
        name = entry["name"]
        role = f"代{gov_title}" if entry["acting"] else gov_title
        if name not in seen:
            seen[name] = {
                "name": name,
                "role": role,
                "start": entry["start"],
                "end": entry["end"],
                "needs_check": False,
            }
        else:
            existing = seen[name]["role"]
            if gov_title not in existing:
                seen[name]["role"] = f"{existing}/{gov_title}"

    for entry in secretaries:
        name = entry["name"]
        if name not in seen:
            seen[name] = {
                "name": name,
                "role": sec_title,
                "start": entry["start"],
                "end": entry["end"],
                "needs_check": False,
            }
        else:
            existing = seen[name]["role"]
            if "书记" not in existing:
                seen[name]["role"] = f"{existing}/{sec_title}"

    all_officials = sorted(seen.values(), key=lambda x: x.get("start", ""))

    print(f"✓ Loaded {txt_path.name}: {gov_title} {len(governors)}人, {sec_title} {len(secretaries)}人, 去重后 {len(all_officials)}人")

    return {
        "province": province_full,
        "province_short": province,
        "gov_title": gov_title,
        "sec_title": sec_title,
        "start_year": start_year,
        "governors": governors,
        "secretaries": secretaries,
        "all_officials": all_officials,
    }


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Parse province officials name list")
    parser.add_argument("--province", default="安徽", help="Province short name")
    args = parser.parse_args()

    data = parse_province_officials_txt(args.province)

    print(f"\n{data['gov_title']}列表：")
    for e in data["governors"]:
        flag = "（代）" if e["acting"] else ""
        print(f"  {e['name']}{flag}  {e['start']}→{e['end']}")

    print(f"\n{data['sec_title']}列表：")
    for e in data["secretaries"]:
        print(f"  {e['name']}  {e['start']}→{e['end']}")

    print(f"\n去重名单：")
    for o in data["all_officials"]:
        print(f"  {o['name']} ({o['role']}) {o['start']}→{o['end']}")
