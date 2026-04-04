"""
Generate per-province official list files from provincial_officials_merged.json.
Output format matches the existing city-level format (data/{province}_officials.txt).
"""
import json
import re
from pathlib import Path

DATA_DIR = Path("data")

with open(DATA_DIR / "provincial_officials_merged.json", encoding="utf-8") as f:
    merged = json.load(f)

# Province metadata
PROVINCE_META = {
    "北京": {"fullname": "北京市", "type": "直辖市", "gov_title": "市长", "sec_title": "市委书记"},
    "天津": {"fullname": "天津市", "type": "直辖市", "gov_title": "市长", "sec_title": "市委书记"},
    "河北": {"fullname": "河北省", "type": "省", "gov_title": "省长", "sec_title": "省委书记"},
    "山西": {"fullname": "山西省", "type": "省", "gov_title": "省长", "sec_title": "省委书记"},
    "内蒙古": {"fullname": "内蒙古自治区", "type": "自治区", "gov_title": "主席", "sec_title": "自治区党委书记"},
    "辽宁": {"fullname": "辽宁省", "type": "省", "gov_title": "省长", "sec_title": "省委书记"},
    "吉林": {"fullname": "吉林省", "type": "省", "gov_title": "省长", "sec_title": "省委书记"},
    "黑龙江": {"fullname": "黑龙江省", "type": "省", "gov_title": "省长", "sec_title": "省委书记"},
    "上海": {"fullname": "上海市", "type": "直辖市", "gov_title": "市长", "sec_title": "市委书记"},
    "江苏": {"fullname": "江苏省", "type": "省", "gov_title": "省长", "sec_title": "省委书记"},
    "浙江": {"fullname": "浙江省", "type": "省", "gov_title": "省长", "sec_title": "省委书记"},
    "安徽": {"fullname": "安徽省", "type": "省", "gov_title": "省长", "sec_title": "省委书记"},
    "福建": {"fullname": "福建省", "type": "省", "gov_title": "省长", "sec_title": "省委书记"},
    "江西": {"fullname": "江西省", "type": "省", "gov_title": "省长", "sec_title": "省委书记"},
    "山东": {"fullname": "山东省", "type": "省", "gov_title": "省长", "sec_title": "省委书记"},
    "河南": {"fullname": "河南省", "type": "省", "gov_title": "省长", "sec_title": "省委书记"},
    "湖北": {"fullname": "湖北省", "type": "省", "gov_title": "省长", "sec_title": "省委书记"},
    "湖南": {"fullname": "湖南省", "type": "省", "gov_title": "省长", "sec_title": "省委书记"},
    "广东": {"fullname": "广东省", "type": "省", "gov_title": "省长", "sec_title": "省委书记"},
    "广西": {"fullname": "广西壮族自治区", "type": "自治区", "gov_title": "主席", "sec_title": "自治区党委书记"},
    "海南": {"fullname": "海南省", "type": "省", "gov_title": "省长", "sec_title": "省委书记"},
    "重庆": {"fullname": "重庆市", "type": "直辖市", "gov_title": "市长", "sec_title": "市委书记"},
    "四川": {"fullname": "四川省", "type": "省", "gov_title": "省长", "sec_title": "省委书记"},
    "贵州": {"fullname": "贵州省", "type": "省", "gov_title": "省长", "sec_title": "省委书记"},
    "云南": {"fullname": "云南省", "type": "省", "gov_title": "省长", "sec_title": "省委书记"},
    "西藏": {"fullname": "西藏自治区", "type": "自治区", "gov_title": "主席", "sec_title": "自治区党委书记"},
    "陕西": {"fullname": "陕西省", "type": "省", "gov_title": "省长", "sec_title": "省委书记"},
    "甘肃": {"fullname": "甘肃省", "type": "省", "gov_title": "省长", "sec_title": "省委书记"},
    "青海": {"fullname": "青海省", "type": "省", "gov_title": "省长", "sec_title": "省委书记"},
    "宁夏": {"fullname": "宁夏回族自治区", "type": "自治区", "gov_title": "主席", "sec_title": "自治区党委书记"},
    "新疆": {"fullname": "新疆维吾尔自治区", "type": "自治区", "gov_title": "主席", "sec_title": "自治区党委书记"},
}


def normalize_term(term_raw: str) -> tuple:
    """
    Normalize various date formats to (start, end) in YYYY.MM format.
    Input examples:
      "任期：2017年-2022年"  (Baidu star map)
      "2007年3月—2012年11月" (Wikipedia)
      "1985年—1991年"        (manual)
      "2022年10月—"          (current)
    """
    if not term_raw:
        return ("", "")

    # Clean up
    term = term_raw.replace("任期：", "").replace("任期:", "").strip()

    # Extract start date
    start_match = re.search(r'(\d{4})年(\d{1,2})?月?(\d{1,2})?日?', term)
    if not start_match:
        return ("", "")

    start_year = start_match.group(1)
    start_month = start_match.group(2) or "00"
    start = f"{start_year}.{int(start_month):02d}"

    # Extract end date (everything after separator)
    sep_pos = -1
    for sep in ["—", "–", "-", "－"]:
        idx = term.find(sep, start_match.end())
        if idx >= 0:
            sep_pos = idx
            break

    if sep_pos < 0:
        # No separator — single date, assume still current or unknown end
        return (start, "")

    rest = term[sep_pos + 1:].strip()

    if not rest or rest in ("至今", "现任", ""):
        return (start, "至今")

    end_match = re.search(r'(\d{4})年(\d{1,2})?月?(\d{1,2})?日?', rest)
    if end_match:
        end_year = end_match.group(1)
        end_month = end_match.group(2) or "00"
        end = f"{end_year}.{int(end_month):02d}"
        return (start, end)

    return (start, "至今")


def clean_date(d: str) -> str:
    """Normalize date: ensure YYYY.MM format, fix trailing dots."""
    if not d or d == "至今":
        return d
    d = d.strip().rstrip(".")
    # "1949" → "1949.00"
    if re.match(r'^\d{4}$', d):
        return f"{d}.00"
    # "1949.1" → "1949.01"
    m = re.match(r'^(\d{4})\.(\d{1,2})$', d)
    if m:
        return f"{m.group(1)}.{int(m.group(2)):02d}"
    # Already good format
    return d


def format_entry(name: str, start: str, end: str) -> str:
    """Format as: 姓名, YYYY.MM-YYYY.MM"""
    start = clean_date(start)
    end = clean_date(end)
    if not start:
        return f"{name}, ?-?"
    if not end:
        return f"{name}, {start}-?"
    return f"{name}, {start}-{end}"


def generate_province_file(province: str, data: dict, meta: dict):
    """Generate one province's officials.txt file."""
    lines = []
    fullname = meta["fullname"]
    gov_title = meta["gov_title"]
    sec_title = meta["sec_title"]

    # Header
    lines.append(f"# {fullname}主官名单")
    lines.append("# 格式：姓名, 起始时间-终止时间")
    lines.append("# 时间格式：YYYY.MM（不确定月份写 YYYY.00，在任写 至今）")
    lines.append(f"# 数据来源：{gov_title}=百度百科星图, {sec_title}=Wikipedia+百科+手工补充")
    lines.append("")
    lines.append(f"省份：{fullname}")
    lines.append("")

    # Governor section
    gov_list = data.get("governor_list", [])
    lines.append(f"[{gov_title}]")

    # Sort governors chronologically (by start date, oldest first)
    gov_entries = []
    for g in gov_list:
        # Governors have pre-parsed start/end from merge script
        start = g.get("start", "")
        end = g.get("end", "")
        # Normalize: "2021.02" → "2021.02", "2011" → "2011.00"
        if start and "." not in start:
            start = f"{start}.00"
        if end and end != "至今" and "." not in end:
            end = f"{end}.00"
        if not start:
            # Fallback to term_raw parsing
            term_raw = g.get("term_raw", "")
            start, end = normalize_term(term_raw)
        gov_entries.append((g["name"], start, end, g.get("baike_url", "")))

    # Sort by start date
    gov_entries.sort(key=lambda x: x[1] if x[1] else "9999")

    for name, start, end, url in gov_entries:
        lines.append(format_entry(name, start, end))

    lines.append("")

    # Secretary section
    sec_list = data.get("secretary_list", [])
    lines.append(f"[{sec_title}]")

    sec_entries = []
    for s in sec_list:
        term_raw = s.get("term_raw", s.get("term", ""))
        start, end = normalize_term(term_raw)
        sec_entries.append((s["name"], start, end))

    # Sort by start date
    sec_entries.sort(key=lambda x: x[1] if x[1] else "9999")

    for name, start, end in sec_entries:
        lines.append(format_entry(name, start, end))

    lines.append("")

    # Write file
    filename = DATA_DIR / f"{province}_officials.txt"
    filename.write_text("\n".join(lines), encoding="utf-8")
    return len(gov_entries), len(sec_entries)


def main():
    total_files = 0
    total_gov = 0
    total_sec = 0

    print(f"{'Province':8s} {'Gov':>5s} {'Sec':>5s} File")
    print("=" * 50)

    for province in [
        "北京", "天津", "河北", "山西", "内蒙古", "辽宁", "吉林", "黑龙江",
        "上海", "江苏", "浙江", "安徽", "福建", "江西", "山东", "河南",
        "湖北", "湖南", "广东", "广西", "海南", "重庆", "四川", "贵州",
        "云南", "西藏", "陕西", "甘肃", "青海", "宁夏", "新疆",
    ]:
        data = merged.get(province, {})
        meta = PROVINCE_META.get(province, {})
        if not data or not meta:
            print(f"{province:8s}   SKIP — no data")
            continue

        ng, ns = generate_province_file(province, data, meta)
        total_files += 1
        total_gov += ng
        total_sec += ns
        print(f"{province:8s} {ng:5d} {ns:5d} data/{province}_officials.txt")

    print("=" * 50)
    print(f"{'Total':8s} {total_gov:5d} {total_sec:5d} {total_files} files")


if __name__ == "__main__":
    main()
