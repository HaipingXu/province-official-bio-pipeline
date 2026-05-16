"""
Build province-year panel Excel from 1990/ officials txt files.

Version 1: Longest tenure in year; if can't determine → last one
Version 2: Who was in office on July 1; if can't determine → last one
"""

import re
from pathlib import Path
import pandas as pd

DATA_DIR = Path("/Users/xuhaiping/Desktop/Workflow省级官员/data/1990")
OUTPUT_DIR = Path("/Users/xuhaiping/Desktop/Workflow省级官员/output")

GOVERNOR_HEADERS = {"[省长]", "[市长]", "[主席]"}
SECRETARY_HEADERS = {"[省委书记]", "[市委书记]", "[自治区党委书记]"}

START_YEAR = 1990
END_YEAR = 2025


def parse_date(s: str):
    """Return (year, month). Month 0 = unknown. '至今' = (9999, 12)."""
    s = s.strip()
    if s == "至今":
        return (9999, 12)
    m = re.match(r"(\d{4})\.(\d{2})$", s)
    if m:
        return (int(m.group(1)), int(m.group(2)))
    m = re.match(r"(\d{4})$", s)
    if m:
        return (int(m.group(1)), 0)
    return None


def parse_file(filepath: Path) -> dict:
    text = filepath.read_text(encoding="utf-8")

    m = re.search(r"省份：(.+)", text)
    province = m.group(1).strip() if m else filepath.stem.replace("_officials", "")

    result = {"province": province, "governor": [], "secretary": []}
    current_section = None
    order = 0

    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or line.startswith("省份") or line.startswith("起始"):
            continue

        if line in GOVERNOR_HEADERS:
            current_section = "governor"
            continue
        if line in SECRETARY_HEADERS:
            current_section = "secretary"
            continue
        if current_section is None:
            continue

        # Parse: "姓名（注）, YYYY.MM-YYYY.MM"
        if "," not in line:
            continue
        comma = line.index(",")
        name = line[:comma].strip()
        tenure = line[comma + 1:].strip()

        # Match dates (YYYY.MM or 至今) around a dash
        dm = re.match(r"(\d{4}\.\d{2}|至今)-(\d{4}\.\d{2}|至今)$", tenure)
        if not dm:
            continue
        start = parse_date(dm.group(1))
        end = parse_date(dm.group(2))
        if start and end:
            result[current_section].append(
                {"name": name, "start": start, "end": end, "order": order}
            )
            order += 1

    return result


def active_in(officials: list, year: int) -> list:
    return [o for o in officials if o["start"][0] <= year <= o["end"][0]]


def last_one(officials: list, year: int):
    """Return the last official active in year (latest start date; tie → highest file order)."""
    active = active_in(officials, year)
    if not active:
        return None
    # Sort ascending by (start_year, start_month_or_high, order); take last
    def key(o):
        sy, sm = o["start"]
        return (sy, sm if sm > 0 else 13, o["order"])
    return sorted(active, key=key)[-1]


def months_in_year(o: dict, year: int):
    """Return (months, known). known=False means can't determine exactly."""
    sy, sm = o["start"]
    ey, em = o["end"]

    if sy > year or ey < year:
        return 0, True

    if sy < year and ey > year:
        return 12, True

    if sy == year and ey > year:
        if sm > 0:
            return 12 - sm + 1, True
        return None, False  # Unknown start month

    if sy < year and ey == year:
        if em > 0:
            return em, True
        return None, False  # Unknown end month

    # sy == year == ey
    if sm > 0 and em > 0:
        return max(em - sm + 1, 1), True
    return None, False


def on_july_first(o: dict, year: int):
    """Return True / False / None (ambiguous) for July 1 of year."""
    sy, sm = o["start"]
    ey, em = o["end"]

    if sy > year or ey < year:
        return False

    # Was started before/on July 1?
    if sy < year:
        after_start = True
    elif sm == 0 or sm == 7:
        after_start = None  # Month unknown or exactly July — ambiguous
    elif sm <= 6:
        after_start = True
    else:
        after_start = False  # sm >= 8, started after July 1

    # Was still in office on July 1?
    if ey > year:
        before_end = True
    elif em == 0 or em == 7:
        before_end = None
    elif em >= 8:
        before_end = True
    else:
        before_end = False  # em <= 6, left before July 1

    if after_start is False or before_end is False:
        return False
    if after_start is True and before_end is True:
        return True
    return None


def pick_v1(officials: list, year: int):
    """Version 1: longest tenure; fallback to last one."""
    active = active_in(officials, year)
    if not active:
        return None
    if len(active) == 1:
        return active[0]

    scored = [(o, *months_in_year(o, year)) for o in active]
    known = [(o, m) for o, m, k in scored if k and m is not None]

    if known:
        best_months = max(m for _, m in known)
        best = [o for o, m in known if m == best_months]
        unknown_officials = [o for o, m, k in scored if not k]
        if len(best) == 1 and not unknown_officials:
            return best[0]

    return last_one(officials, year)


def pick_v2(officials: list, year: int):
    """Version 2: July 1 rule; fallback to last one."""
    active = active_in(officials, year)
    if not active:
        return None
    if len(active) == 1:
        return active[0]

    on_july = [o for o in active if on_july_first(o, year) is True]
    if len(on_july) == 1:
        return on_july[0]

    return last_one(officials, year)


def build_panel():
    provinces = []
    for fp in sorted(DATA_DIR.glob("*_officials.txt")):
        provinces.append(parse_file(fp))

    rows_v1, rows_v2 = [], []
    for data in provinces:
        prov = data["province"]
        for year in range(START_YEAR, END_YEAR + 1):
            gov1 = pick_v1(data["governor"], year)
            sec1 = pick_v1(data["secretary"], year)
            gov2 = pick_v2(data["governor"], year)
            sec2 = pick_v2(data["secretary"], year)
            rows_v1.append(
                {
                    "省份": prov,
                    "年份": year,
                    "省长/市长/主席": gov1["name"] if gov1 else "",
                    "省委/市委书记": sec1["name"] if sec1 else "",
                }
            )
            rows_v2.append(
                {
                    "省份": prov,
                    "年份": year,
                    "省长/市长/主席": gov2["name"] if gov2 else "",
                    "省委/市委书记": sec2["name"] if sec2 else "",
                }
            )

    df1 = pd.DataFrame(rows_v1)
    df2 = pd.DataFrame(rows_v2)

    OUTPUT_DIR.mkdir(exist_ok=True)
    out_path = OUTPUT_DIR / "province_year_panel.xlsx"
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df1.to_excel(writer, sheet_name="V1_最长任职", index=False)
        df2.to_excel(writer, sheet_name="V2_七月一日", index=False)

    print(f"Written: {out_path}")
    print(f"Provinces: {len(provinces)}, Years: {START_YEAR}-{END_YEAR}")
    print(f"Total rows per sheet: {len(df1)}")


if __name__ == "__main__":
    build_panel()
