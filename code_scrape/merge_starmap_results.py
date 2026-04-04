"""
Merge Baidu Baike star map data (governors) with Wikipedia secretary data.
Output: data/provincial_officials_merged.json

Also generates data/{province}_officials.txt for the pipeline.
"""
import json
import re
from pathlib import Path
from urllib.parse import quote

DATA_DIR = Path("data")

# Load sources
with open(DATA_DIR / "provincial_starmaps.json", encoding="utf-8") as f:
    baidu_data = json.load(f)

with open(DATA_DIR / "wiki_secretaries_clean.json", encoding="utf-8") as f:
    wiki_data = json.load(f)

# Also load the Baidu secretary data (only 重庆 has it)
baidu_sec = {}
for prov, d in baidu_data.items():
    if d.get("secretary_list"):
        baidu_sec[prov] = d["secretary_list"]

PROVINCES = [
    "北京", "天津", "河北", "山西", "内蒙古",
    "辽宁", "吉林", "黑龙江",
    "上海", "江苏", "浙江", "安徽", "福建", "江西", "山东",
    "河南", "湖北", "湖南", "广东", "广西", "海南",
    "重庆", "四川", "贵州", "云南", "西藏",
    "陕西", "甘肃", "青海", "宁夏", "新疆",
]

PROVINCE_FULLNAMES = {
    "北京": "北京市", "天津": "天津市", "河北": "河北省", "山西": "山西省",
    "内蒙古": "内蒙古自治区", "辽宁": "辽宁省", "吉林": "吉林省", "黑龙江": "黑龙江省",
    "上海": "上海市", "江苏": "江苏省", "浙江": "浙江省", "安徽": "安徽省",
    "福建": "福建省", "江西": "江西省", "山东": "山东省", "河南": "河南省",
    "湖北": "湖北省", "湖南": "湖南省", "广东": "广东省", "广西": "广西壮族自治区",
    "海南": "海南省", "重庆": "重庆市", "四川": "四川省", "贵州": "贵州省",
    "云南": "云南省", "西藏": "西藏自治区", "陕西": "陕西省", "甘肃": "甘肃省",
    "青海": "青海省", "宁夏": "宁夏回族自治区", "新疆": "新疆维吾尔自治区",
}


def normalize_term(term: str) -> tuple[str, str]:
    """Extract start/end from term string. Returns (start, end).

    Handles Baidu star map format with acting (代) dates:
      "任期：2021.10-2022.01（代）-"  → start=2021.10, end=至今
      "任期：2021.10-2022.01（代）-2023.05" → start=2021.10, end=2023.05
      "任期：2017.04-2021.10" → start=2017.04, end=2021.10
      "任期：2022年-" → start=2022, end=至今
    """
    term = term.replace("年", ".").replace("月", "").replace("日", "")
    term = term.replace("—", "-").replace("－", "-")

    # Remove "任期：" prefix
    term = re.sub(r"^任期[：:]?\s*", "", term)

    # Replace "文革" / "文化大革命" markers with approximate year 1966
    term = re.sub(r'"?文化大革命"?[初前]期', "1966", term)
    term = re.sub(r'"?文革"?[初前]期', "1966", term)
    term = re.sub(r'"?文化大革命"?', "1966", term)
    term = re.sub(r'"?文革"?', "1966", term)

    # Find all date-like tokens
    dates = re.findall(r"(\d{4}[\.\d]*)", term)
    if not dates:
        return "", ""

    start = dates[0]

    # Check if term ends with trailing dash (= still serving)
    # Pattern: "（代）- " or "（代）-$" or just "-$" or "- $"
    stripped = term.rstrip()
    ends_with_dash = stripped.endswith("-") or stripped.endswith("- ")

    if len(dates) == 1:
        # Only one date: "2022-" or "2022."
        return start, "至今"

    if len(dates) == 2:
        if ends_with_dash:
            # "2021.10-2022.01（代）-" → acting date, still serving
            return start, "至今"
        else:
            # "2017.04-2021.10" → normal range
            return start, dates[1]

    if len(dates) >= 3:
        # "2021.10-2022.01（代）-2023.05" → start, skip acting, use last date as end
        if ends_with_dash:
            return start, "至今"
        else:
            return start, dates[-1]

    return start, "至今"


merged = {}

for prov in PROVINCES:
    entry = {
        "province": prov,
        "province_fullname": PROVINCE_FULLNAMES[prov],
    }

    # Governor data (from Baidu star map - authoritative)
    bd = baidu_data.get(prov, {})
    gov_list = bd.get("governor_list", [])
    entry["governor_count"] = len(gov_list)
    entry["governor_source"] = "baidu_starmap"
    entry["governor_starmap"] = bd.get("governor_starmap", "")
    entry["governor_nodeId"] = bd.get("governor_nodeId", "")
    entry["governor_list"] = []

    for g in gov_list:
        start, end = normalize_term(g.get("term", ""))
        entry["governor_list"].append({
            "name": g["name"],
            "term_raw": g.get("term", ""),
            "start": start,
            "end": end,
            "lemmaId": g.get("lemmaId"),
            "baike_url": g.get("baike_url", ""),
            "summary": g.get("summary", ""),
        })

    # Secretary data (prefer Baidu if available, else Wikipedia)
    sec_list = []
    sec_source = "none"

    if prov in baidu_sec and len(baidu_sec[prov]) >= 5:
        sec_source = "baidu_starmap"
        for s in baidu_sec[prov]:
            start, end = normalize_term(s.get("term", ""))
            sec_list.append({
                "name": s["name"],
                "term_raw": s.get("term", ""),
                "start": start,
                "end": end,
                "lemmaId": s.get("lemmaId"),
                "baike_url": s.get("baike_url", ""),
                "summary": s.get("summary", ""),
            })
    elif prov in wiki_data:
        wiki_secs = wiki_data[prov].get("secretary_list", [])
        if wiki_secs:
            sec_source = "wikipedia"
            for s in wiki_secs:
                start, end = normalize_term(s.get("term", ""))
                sec_list.append({
                    "name": s["name"],
                    "term_raw": s.get("term", ""),
                    "start": start,
                    "end": end,
                    "source": "wikipedia",
                })

    entry["secretary_count"] = len(sec_list)
    entry["secretary_source"] = sec_source
    entry["secretary_list"] = sec_list

    # Quality flags
    entry["governor_complete"] = len(gov_list) > 0
    entry["secretary_complete"] = len(sec_list) >= 5
    entry["needs_manual_review"] = not entry["secretary_complete"]

    merged[prov] = entry

# Save merged result
(DATA_DIR / "provincial_officials_merged.json").write_text(
    json.dumps(merged, ensure_ascii=False, indent=2), encoding="utf-8"
)

# Print summary
print("=" * 70)
print(f"{'Province':8s} | {'Gov':>4s} | {'Sec':>4s} | {'Sec Source':12s} | {'Status':10s}")
print("-" * 70)

total_gov = 0
total_sec = 0
complete = 0

for prov in PROVINCES:
    e = merged[prov]
    gc = e["governor_count"]
    sc = e["secretary_count"]
    total_gov += gc
    total_sec += sc
    src = e["secretary_source"]
    status = "✓ Complete" if not e["needs_manual_review"] else "⚠ Need Sec"
    if e["governor_complete"] and e["secretary_complete"]:
        complete += 1
    print(f"{prov:8s} | {gc:4d} | {sc:4d} | {src:12s} | {status}")

print("-" * 70)
print(f"{'Total':8s} | {total_gov:4d} | {total_sec:4d} | {'':12s} | {complete}/31 complete")
print(f"\nSaved to: data/provincial_officials_merged.json")
