"""
Phase 4: Post-processing

Computes derived columns:
  - 是否当过市长 (person-level): ever served as mayor/acting mayor
  - 是否当过书记 (person-level): ever served as party secretary
  - 该条是市长 (per-row): this row IS the Shenzhen mayor tenure
  - 该条是书记 (per-row): this row IS the Shenzhen party secretary tenure
  - 备注栏: add [需人工核查] from verification report

Also flattens DeepSeek JSON results into flat row dicts ready for Excel.
"""

import json
import re
from pathlib import Path

from config import (
    COLUMNS, LOGS_DIR, ORG_TAGS,
)


def is_bureau_or_below(position: str, unit: str, org_tag: str) -> int:
    """
    Determine if a position is at 厅局级 or below.
    Returns 1 if bureau-level or below, 0 if 副省级 or above.

    Logic:
    - If position contains 副省级 or above keywords → 0
    - Study rows (本科生/研究生/进修学员) → 1 (by convention)
    - If position is 市长/书记 of a 副省级 city → 0 (Shenzhen is 副省级)
    - If position contains 厅长/局长/处长 etc. → 1
    """
    position = str(position)
    unit = str(unit)

    # Study rows are by definition below 副省级
    study_keywords = ["本科生", "研究生", "进修学员", "实习生", "博士生", "硕士生"]
    if any(kw in position for kw in study_keywords):
        return 1

    # Check for clear 副省级+ indicators
    for kw in VICE_PROVINCIAL_KEYWORDS:
        if kw in position:
            # Special case: 副市长 in non-副省级 city → bureau level
            # For our data (Shenzhen = 副省级), 市长/副市长 → 副省级 = 0
            return 0

    # If the unit is a 副省级 city government and position is 市长 → 0
    vice_provincial_cities = ["深圳", "广州", "宁波", "青岛", "大连", "厦门",
                               "成都", "武汉", "南京", "杭州", "沈阳", "西安",
                               "哈尔滨", "长春", "济南"]
    for city in vice_provincial_cities:
        if city in unit and any(kw in position for kw in ["市长", "书记", "常委"]):
            return 0

    # Bureau level or below keywords
    for kw in BUREAU_LEVEL_KEYWORDS:
        if kw in position:
            return 1

    # Study/training org tags
    study_tags = ["教育部直属高校", "地方属高校", "国外高校", "中小学/职业院校"]
    if org_tag in study_tags:
        return 1

    # Default: if we can't determine, assume bureau level (1)
    return 1


def is_shenzhen_mayor_row(position: str, unit: str, city: str = "深圳") -> int:
    """
    Return 1 if this row represents serving as Shenzhen mayor or acting mayor.
    Excludes: 副市长, 常务副市长, 第一副市长 (these are deputy mayors, not mayor).
    """
    position = position.strip()
    # Must contain 市长 or 代市长 but NOT 副市长 variants
    is_mayor = (
        ("市长" in position or "代市长" in position)
        and "副市长" not in position
        and "常务副" not in position
        and "第一副" not in position
    )
    if not is_mayor:
        return 0
    # Check city context: either city in unit, or unit is about 市政府/市委
    city_in_unit = (city in unit or f"{city}市" in unit
                    or "市政府" in unit or "市人民政府" in unit)
    return 1 if city_in_unit else 0


def is_shenzhen_secretary_row(position: str, unit: str, city: str = "深圳") -> int:
    """Return 1 if this row represents serving as Shenzhen party secretary."""
    # Position contains secretary keyword AND unit is Shenzhen party committee
    sec_keywords = ["市委书记", "委书记"]
    for kw in sec_keywords:
        if kw in position:
            if city in unit or f"{city}市" in unit or "市委" in unit:
                return 1
    # Also handle: position="书记" AND unit contains city's party committee
    if position.strip() in ("书记",) and ("市委" in unit) and (city in unit or f"{city}市" in unit):
        return 1
    return 0


def get_verification_flags(verification_reports: list[dict]) -> dict[str, str]:
    """
    Build name → flag_string mapping from verification reports.
    Returns dict of {name: "[需人工核查]"} for non-PASS officials.
    """
    flags = {}
    for report in verification_reports:
        name = report.get("official_name", "")
        verdict = report.get("summary", {}).get("verdict", "PASS")
        if verdict != "PASS":
            disc = report.get("summary", {}).get("total_discrepancies", 0)
            flags[name] = f"[需人工核查:DeepSeek/Qwen差异{disc}处,{verdict}]"
    return flags


def flatten_results(
    deepseek_results: list[dict],
    verification_reports: list[dict],
    city: str,
    province: str,
) -> list[dict]:
    """
    Flatten structured JSON results into flat row dicts matching COLUMNS schema.
    Computes all derived columns.
    """
    verif_flags = get_verification_flags(verification_reports)
    all_rows = []

    for result in deepseek_results:
        bio = result.get("bio", {})
        episodes = result.get("episodes", [])
        meta = result.get("_meta", {})

        name = bio.get("姓名") or meta.get("name", "")
        if not name:
            continue

        # Determine year of focal Shenzhen position
        shenzhen_start_year = 0
        for ep in episodes:
            pos = str(ep.get("职务", ""))
            unit = str(ep.get("供职单位", ""))
            if city in unit or f"{city}市" in unit:
                if any(kw in pos for kw in ["市长", "代市长", "书记"]):
                    try:
                        yr = int(str(ep.get("起始时间", "0")).split(".")[0])
                        if shenzhen_start_year == 0 or yr < shenzhen_start_year:
                            shenzhen_start_year = yr
                    except Exception:
                        pass

        # Pre-compute per-row Z and AA flags (needed for M and N)
        row_data = []
        for ep in episodes:
            pos = str(ep.get("职务", ""))
            unit = str(ep.get("供职单位", ""))
            row_data.append({
                "ep": ep,
                "is_mayor_row": is_shenzhen_mayor_row(pos, unit, city),
                "is_sec_row": is_shenzhen_secretary_row(pos, unit, city),
            })

        ever_mayor = int(any(r["is_mayor_row"] for r in row_data))
        ever_sec = int(any(r["is_sec_row"] for r in row_data))

        # Get verification flag for this person
        verif_note = verif_flags.get(name, "")

        for i, rd in enumerate(row_data):
            ep = rd["ep"]
            pos = str(ep.get("职务", ""))
            unit = str(ep.get("供职单位", ""))
            org_tag = str(ep.get("组织标签", ""))

            row = {
                "年份": shenzhen_start_year if shenzhen_start_year else "",
                "省份": f"{province}省" if not province.endswith("省") else province,
                "姓名": name,
                "出生年份": bio.get("出生年份", ""),
                "籍贯": bio.get("籍贯", ""),
                "少数民族": bio.get("少数民族", ""),
                "女性": bio.get("女性", ""),
                "全日制本科": bio.get("全日制本科", ""),
                "升迁": bio.get("升迁", ""),
                "本省提拔": bio.get("本省提拔", ""),
                "本省学习": bio.get("本省学习", ""),
                "是否当过市长": ever_mayor,
                "是否当过书记": ever_sec,
                "经历序号": ep.get("经历序号", i + 1),
                "起始时间": ep.get("起始时间", ""),
                "终止时间": ep.get("终止时间", ""),
                "组织标签": org_tag,
                "供职单位": unit,
                "职务": pos,
                "任职地": ep.get("任职地", ""),
                "中央/地方": ep.get("中央/地方", ""),
                "": "",  # empty column W
                "是否落马": ep.get("是否落马", "否"),
                "备注栏": verif_note if (verif_note and i == 0) else ep.get("备注栏", ""),
                "该条是市长": rd["is_mayor_row"],
                "该条是书记": rd["is_sec_row"],
            }
            all_rows.append(row)

    return all_rows


def run_postprocess(
    deepseek_results_path: Path,
    verification_report_path: Path,
    city: str,
    province: str,
) -> list[dict]:
    """Load results and verification reports, return flat rows ready for Excel."""
    print(f"\n=== Phase 4: Post-processing ===")

    with open(deepseek_results_path, encoding="utf-8") as f:
        ds_results = json.load(f)

    verif_reports = []
    if verification_report_path.exists():
        with open(verification_report_path, encoding="utf-8") as f:
            verif_reports = json.load(f)

    rows = flatten_results(ds_results, verif_reports, city, province)

    print(f"  Total rows: {len(rows)}")
    print(f"  Officials: {len(ds_results)}")
    print(f"  Verification reports: {len(verif_reports)}")

    mayor_rows = sum(1 for r in rows if r.get("该条是市长") == 1)
    sec_rows = sum(1 for r in rows if r.get("该条是书记") == 1)
    flagged = sum(1 for r in rows if "[需人工核查" in str(r.get("备注栏", "")))
    print(f"  Mayor rows (Z=1): {mayor_rows}")
    print(f"  Secretary rows (AA=1): {sec_rows}")
    print(f"  Flagged rows: {flagged}")

    return rows
