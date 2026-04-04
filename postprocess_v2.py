"""
Phase 4 (v2): Post-processing

Takes the merged step1 + step2 results and:
1. Normalises 任职地（省）and 任职地（市）to full official names
2. Validates 组织标签 against allowed list
3. Computes per-row flags: 该条是省长, 该条是省委书记
4. Computes per-person flags: 是否当过省长, 是否当过省委书记
5. Assembles the final flat row list in COLUMNS order (A→AC)
   Each row = one episode; person-level fields repeated on every row.
6. Optionally applies battle judge decisions to override disputed fields.

Input  : logs/deepseek_step1_results.json
         logs/deepseek_step2_labels.json
         logs/judge_decisions.json (optional — battle results)
Output : logs/final_rows.json  — list of dicts keyed by COLUMNS names
"""

import json
import logging
import re
from pathlib import Path
from typing import Any

from config import (
    LOGS_DIR,
    ORG_TAGS,
    PROVINCE_NORMALIZE,
    CITY_NORMALIZE,
    COLUMNS,
    get_highest_rank,
)
from text_preprocessor import preprocess_official

logger = logging.getLogger(__name__)
LOGS_DIR.mkdir(parents=True, exist_ok=True)


# ── Party committee name normalisation ─────────────────────────────────────────

def _normalize_party_unit(name: str) -> str:
    """Normalize party committee names: '深圳市委' → '中共深圳市委'."""
    if not name:
        return name
    if name.startswith("中共"):
        return name
    # Match patterns: X省委, X市委, X区委, X县委, X自治州委, X自治区委
    if re.search(r"(?:省|市|区|县|自治州|自治区)委", name):
        return "中共" + name
    return name


# ── Place-name normalisation ───────────────────────────────────────────────────

def normalise_province(raw: str) -> str:
    """Normalise a province string to the full official name."""
    if not raw:
        return ""
    raw = raw.strip()
    if raw in PROVINCE_NORMALIZE.values():
        return raw
    if raw in PROVINCE_NORMALIZE:
        return PROVINCE_NORMALIZE[raw]
    for short, full in PROVINCE_NORMALIZE.items():
        if raw.startswith(short) or full.startswith(raw):
            return full
    return raw


def normalise_city(raw: str) -> str:
    """Normalise a city string to the full official name."""
    if not raw:
        return ""
    raw = raw.strip()
    # Already ends with common admin suffixes?
    if re.search(r"[市区县州盟]$", raw):
        return raw
    if raw in CITY_NORMALIZE:
        return CITY_NORMALIZE[raw]
    for short, full in CITY_NORMALIZE.items():
        if raw.startswith(short):
            return full
    return raw


# ── Row-level flag helpers ─────────────────────────────────────────────────────

def _strip_admin_suffix(name: str) -> str:
    """Remove trailing admin-type suffixes: 省/市/州/盟/地区/自治区 etc."""
    # Province-level suffixes first (longer patterns)
    name = re.sub(r"(壮族|回族|维吾尔)?自治区$", "", name)
    name = re.sub(r"特别行政区$", "", name)
    name = re.sub(r"省$", "", name)
    # City-level suffixes
    return re.sub(r"[市州盟]$", "", name).rstrip("地区")


def is_mayor_row(position: str, unit: str, location_city: str, target_city: str) -> int:
    """
    Return 1 if this episode is a focal-city mayor / acting mayor tenure.
    Excludes 副市长, 常务副市长.

    Args:
        position: 职务 field
        unit: 供职单位 field
        location_city: 任职地（市）field
        target_city: the focal city (e.g. '深圳' or '延边')
    """
    if not position:
        return 0

    # Check target city match via unit or location
    target_short = _strip_admin_suffix(target_city)
    context = (unit or "") + (location_city or "")
    if context and target_short not in context:
        return 0

    # Exclude deputy / executive deputy
    if "副市长" in position or "常务副" in position:
        return 0
    if re.search(r"(^|[\s、，,])(?:代)?市长", position):
        return 1
    return 0


def is_secretary_row(position: str, unit: str, location_city: str, target_city: str) -> int:
    """
    Return 1 if this episode is a focal-city party secretary tenure.

    Args:
        position: 职务 field
        unit: 供职单位 field
        location_city: 任职地（市）field
        target_city: the focal city
    """
    if not position:
        return 0

    target_short = _strip_admin_suffix(target_city)
    context = (unit or "") + (location_city or "")
    if context and target_short not in context:
        return 0

    if "副书记" in position or "副市委" in position:
        return 0
    # Match "市委书记" or standalone "书记" when unit contains 市委
    if re.search(r"(^|[\s、，,])市委书记", position):
        return 1
    # If position is just "书记" and unit mentions the target city's 市委
    if re.search(r"(^|[\s、，,])书记$", position) and "市委" in (unit or ""):
        return 1
    return 0


def is_governor_row(position: str, unit: str, location_prov: str, target_province: str) -> int:
    """
    Return 1 if this episode is a focal-province governor / acting governor tenure.
    Matches: 省长, 代省长, 主席, 代主席 (for autonomous regions).
    Excludes: 副省长, 副主席.
    """
    if not position:
        return 0

    target_short = _strip_admin_suffix(target_province)
    context = (unit or "") + (location_prov or "")
    if context and target_short not in context:
        return 0

    # Exclude deputy
    if "副省长" in position or "副主席" in position or "常务副" in position:
        return 0
    # Match 省长 / 代省长
    if re.search(r"(^|[\s、，,])(?:代)?省长", position):
        return 1
    # Match 主席 / 代主席 (autonomous regions: 自治区主席)
    if re.search(r"(^|[\s、，,])(?:代)?主席", position):
        # Make sure it's a government chairman, not CPPCC etc.
        ctx = (unit or "") + position
        if "政协" in ctx or "人大" in ctx:
            return 0
        return 1
    # Match 市长 for direct-administered municipalities (北京/上海/天津/重庆)
    if target_short in ("北京", "上海", "天津", "重庆"):
        if "副市长" not in position and re.search(r"(^|[\s、，,])(?:代)?市长", position):
            return 1
    return 0


def is_prov_secretary_row(position: str, unit: str, location_prov: str, target_province: str) -> int:
    """
    Return 1 if this episode is a focal-province party secretary tenure.
    Matches: 省委书记, 自治区党委书记, 市委书记 (for municipalities).
    Excludes: 副书记.
    """
    if not position:
        return 0

    target_short = _strip_admin_suffix(target_province)
    context = (unit or "") + (location_prov or "")
    if context and target_short not in context:
        return 0

    if "副书记" in position:
        return 0
    # Match 省委书记
    if re.search(r"(^|[\s、，,])省委书记", position):
        return 1
    # Match 自治区党委书记
    if re.search(r"(^|[\s、，,])自治区党委书记", position):
        return 1
    # Match 市委书记 for direct-administered municipalities
    if target_short in ("北京", "上海", "天津", "重庆"):
        if re.search(r"(^|[\s、，,])市委书记", position):
            return 1
    # Position is "书记" and unit mentions target's 省委 / 自治区党委 / 市委(直辖市)
    if re.search(r"(^|[\s、，,])书记$", position):
        u = unit or ""
        if "省委" in u or "自治区党委" in u:
            return 1
        if target_short in ("北京", "上海", "天津", "重庆") and "市委" in u:
            return 1
    return 0


def _is_province_mode(city: str) -> bool:
    """Detect if we are in province mode (city param is actually a province name)."""
    # Province names: 31 provinces/municipalities/autonomous regions
    province_names = {
        "北京", "天津", "河北", "山西", "内蒙古",
        "辽宁", "吉林", "黑龙江",
        "上海", "江苏", "浙江", "安徽", "福建", "江西", "山东",
        "河南", "湖北", "湖南", "广东", "广西", "海南",
        "重庆", "四川", "贵州", "云南", "西藏",
        "陕西", "甘肃", "青海", "宁夏", "新疆",
    }
    short = _strip_admin_suffix(city)
    return short in province_names


# ── Source-line reference helper ───────────────────────────────────────────────

def _build_source_ref(
    ep: dict, idx: int, career_lines_map: dict[int, str] | None
) -> str:
    """Build 原文引用 field: 'L01: 1978.10-1982.07 北京工业学院...'"""
    # If episode already has a non-Lxx 原文引用, keep it
    existing = ep.get("原文引用", "")
    if existing and not re.match(r"^L\d+$", existing):
        return existing

    sl = ep.get("source_line", idx + 1)
    label = f"L{sl:02d}"

    if career_lines_map and sl in career_lines_map:
        raw_text = career_lines_map[sl]
        return f"{label}: {raw_text}"
    return label


# ── Episode flattening ─────────────────────────────────────────────────────────

def flatten_person(
    step1: dict,
    step2: dict,
    city: str,
    province: str,
    start_year: int,
    label_overrides: dict | None = None,
    episode_disputes: dict | None = None,
    career_lines_map: dict[int, str] | None = None,
    rank_map: dict[int, str] | None = None,
    low_conf_parts: list[str] | None = None,
) -> list[dict]:
    """
    Merge step1 episode data + step2 labels + metadata into a list of row dicts.
    Each dict uses the keys from COLUMNS (Chinese column names).
    """
    episodes: list[dict] = step1.get("episodes", [])
    if low_conf_parts is None:
        low_conf_parts = []

    # In v4, raw_bio comes from step2; fallback to step1 for backward compat
    raw_bio: dict = step2.get("raw_bio", step1.get("raw_bio", {}))

    name: str = raw_bio.get("姓名", step1.get("_meta", {}).get("name", ""))
    birth_year: int = raw_bio.get("出生年份", 0)
    birthplace: str = raw_bio.get("籍贯", "")
    birthplace_city: str = raw_bio.get("籍贯（市）", "")
    minority: int = raw_bio.get("少数民族", 0)
    female: int = raw_bio.get("女性", 0)
    bachelors: int = raw_bio.get("全日制本科", -1)
    # In v4, 是否落马/落马原因 come from step2
    fell: str = step2.get("是否落马", raw_bio.get("是否落马", "否"))
    fell_reason_global: str = step2.get("落马原因", raw_bio.get("落马原因", ""))

    # Analytical labels from step2, with judge overrides
    if label_overrides is None:
        label_overrides = {}
    if episode_disputes is None:
        episode_disputes = {}

    label_dispute_parts: list[str] = []

    def _get_label(field: str) -> Any:
        override_key = f"{name}||{field}"
        override = label_overrides.get(override_key)
        if override:
            source = override.get("source", "")
            if source == "qw":
                return override.get("value", -1)
            elif source == "corrected":
                val = override.get("value", -1)
                logger.info(f"  {name}.{field}: 裁判自行修正 → {val}")
                return val
            elif source == "disputed":
                ds_val = step2.get(field, -1)
                label_dispute_parts.append(f"{field}: DS={ds_val}, 两者均存疑")
                return ds_val  # default to DS but flag it
        return step2.get(field, -1)

    promoted_mayor: Any = _get_label("升迁_省长")
    promoted_sec: Any = _get_label("升迁_省委书记")
    prov_promoted: Any = _get_label("本省提拔")
    prov_study: Any = _get_label("本省学习")

    # Detect province mode: if city is actually a province name, use governor/prov-secretary matching
    _prov_mode = _is_province_mode(city)

    if _prov_mode:
        # Province mode: match 省长/主席 and 省委书记/自治区党委书记
        row_is_mayor = [
            is_governor_row(
                ep.get("职务", ""),
                ep.get("供职单位", ""),
                ep.get("任职地（省）", ""),
                city,
            )
            for ep in episodes
        ]
        row_is_sec = [
            is_prov_secretary_row(
                ep.get("职务", ""),
                ep.get("供职单位", ""),
                ep.get("任职地（省）", ""),
                city,
            )
            for ep in episodes
        ]
    else:
        # City mode: match 市长 and 市委书记
        row_is_mayor = [
            is_mayor_row(
                ep.get("职务", ""),
                ep.get("供职单位", ""),
                ep.get("任职地（市）", ""),
                city,
            )
            for ep in episodes
        ]
        row_is_sec = [
            is_secretary_row(
                ep.get("职务", ""),
                ep.get("供职单位", ""),
                ep.get("任职地（市）", ""),
                city,
            )
            for ep in episodes
        ]

    ever_mayor: int = 1 if any(row_is_mayor) else 0
    ever_sec: int = 1 if any(row_is_sec) else 0

    # Determine focal 年份: earliest year this person started as mayor or secretary
    focal_year = start_year
    for ep, is_m, is_s in zip(episodes, row_is_mayor, row_is_sec):
        if is_m or is_s:
            t = ep.get("起始时间", "")
            y = _parse_year(t)
            if y and y >= start_year:
                focal_year = y
                break

    # Build per-episode rank from step3 data
    if rank_map is None:
        rank_map = {}
    # Build source_line → highest rank map (当时的最高行政级别)
    # Group episodes by source_line, find highest rank within each group
    sl_rank_groups: dict[int, list[str]] = {}
    for idx_tmp, ep_tmp in enumerate(episodes):
        sl = ep_tmp.get("source_line", idx_tmp + 1)
        rank_val = rank_map.get(idx_tmp + 1, "")
        sl_rank_groups.setdefault(sl, []).append(rank_val)
    sl_highest_rank: dict[int, str] = {
        sl: get_highest_rank(ranks) for sl, ranks in sl_rank_groups.items()
    }

    rows = []
    for idx, ep in enumerate(episodes):
        # Skip study/training episodes — they are not work experience
        # (学习进修 + legacy "学校" with study-like 职务)
        pos_tag_raw = ep.get("标志位", "")
        if pos_tag_raw == "学习进修":
            continue
        if pos_tag_raw == "学校":
            job = ep.get("职务", "")
            if any(kw in job for kw in ("本科生", "研究生", "进修学员", "访问学者", "留学")):
                continue

        # Normalise location
        prov_raw = ep.get("任职地（省）", ep.get("任职地", ""))
        city_raw = ep.get("任职地（市）", "")
        prov_norm = normalise_province(prov_raw)
        city_norm = normalise_city(city_raw)

        # Validate org tag
        org_tag = ep.get("组织标签", "")
        if org_tag and org_tag not in ORG_TAGS:
            org_tag = f"[无效标签]{org_tag}"

        # Per-row flags
        ab_mayor = row_is_mayor[idx]
        ac_sec = row_is_sec[idx]

        # Fell details (v4: from step2 global, not per-episode)
        is_fell = "是" if "是" in str(fell) else "否"
        fell_reason = fell_reason_global or ep.get("落马原因", "")

        # Normalize party committee names (e.g. "深圳市委" → "中共深圳市委")
        unit = _normalize_party_unit(ep.get("供职单位", ""))
        pos = ep.get("职务", "")
        ep_dispute_key = f"{unit}||{pos}"
        ep_dispute_list = episode_disputes.get(ep_dispute_key, [])
        # On the first row, also include label-level disputes and low confidence flags
        dispute_text = ""
        if idx == 0 and label_dispute_parts:
            dispute_text = "; ".join(label_dispute_parts)
        if idx == 0 and low_conf_parts:
            conf_text = " ".join(low_conf_parts)
            dispute_text = f"{dispute_text}; {conf_text}" if dispute_text else conf_text
        if ep_dispute_list:
            ep_dispute_text = "; ".join(ep_dispute_list)
            dispute_text = f"{dispute_text}; {ep_dispute_text}" if dispute_text else ep_dispute_text

        # Validate position tag (标志位)
        pos_tag = ep.get("标志位", "无")
        from config import POSITION_TAGS
        if pos_tag and pos_tag not in POSITION_TAGS:
            pos_tag = f"[无效标志位]{pos_tag}"

        highest_rank = sl_highest_rank.get(ep.get("source_line", idx + 1), "")
        per_rank = rank_map.get(idx + 1, "")
        row = {
            # --- Person-level (A-Q) ---
            "年份":           focal_year,
            "省份":           province,
            "城市":           city,
            "姓名":           name,
            "出生年份":       birth_year,
            "籍贯":           birthplace,
            "籍贯（市）":     birthplace_city,
            "少数民族":       minority,
            "女性":           female,
            "全日制本科":     bachelors,
            "升迁_省长":      promoted_mayor,
            "升迁_省委书记":  promoted_sec,
            "本省提拔":       prov_promoted,
            "本省学习":       prov_study,
            "是否当过省长":   ever_mayor,
            "是否当过省委书记": ever_sec,
            "最终行政级别":   highest_rank if highest_rank else "无",
            # --- Per-row (R-AK) ---
            "经历序号":       ep.get("经历序号", idx + 1),
            "起始时间":       ep.get("起始时间", ""),
            "终止时间":       ep.get("终止时间", ""),
            "组织标签":       org_tag,
            "标志位":         pos_tag,
            "该条行政级别":   per_rank if per_rank else "无",
            "供职单位":       unit,
            "职务":           pos,
            "原文引用":       _build_source_ref(ep, idx, career_lines_map),
            "争议未解决":     dispute_text,
            "裁判理由":       "",  # filled later by battle postprocess
            "任职地（省）":   prov_norm,
            "任职地（市）":   city_norm,
            "中央/地方":      ep.get("中央/地方", ""),
            "":               "",   # spacer column
            "是否落马":       is_fell,
            "落马原因":       fell_reason,
            "备注栏":        ep.get("备注栏", ""),
            "该条是省长":     ab_mayor,
            "该条是省委书记": ac_sec,
        }
        rows.append(row)

    # Re-number 经历序号 after filtering out study episodes
    for i, row in enumerate(rows):
        row["经历序号"] = i + 1

    return rows


def _parse_year(time_str: str) -> int | None:
    """Extract 4-digit year from YYYY.MM string."""
    if not time_str:
        return None
    m = re.match(r"(\d{4})", str(time_str))
    return int(m.group(1)) if m else None


# ── Battle result integration ─────────────────────────────────────────────────

def _load_judge_decisions(judge_path: Path) -> dict[str, dict]:
    """
    Load judge_decisions.json — the full cache of all judge calls.
    Returns the raw cache dict keyed by cache_key strings.
    """
    if not judge_path.exists():
        return {}
    try:
        return json.loads(judge_path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _build_label_overrides(
    judge_cache: dict, qw_step2_by_name: dict
) -> dict[str, dict]:
    """
    Build label override lookup from judge decisions.
    Returns dict mapping "name||field" → {"value": int, "verdict": str}
    """
    overrides: dict[str, dict] = {}
    for key, decision in judge_cache.items():
        parts = key.split("||")
        if len(parts) < 3 or parts[1] != "label":
            continue
        name, field = parts[0], parts[2]
        verdict = decision.get("verdict", "")
        override_key = f"{name}||{field}"

        if verdict == "采纳DS":
            overrides[override_key] = {"verdict": verdict, "source": "ds"}
        elif verdict == "采纳QW":
            qw_data = qw_step2_by_name.get(name, {})
            qw_val = qw_data.get(field, -1)
            overrides[override_key] = {"verdict": verdict, "source": "qw", "value": qw_val}
        elif verdict == "自行修正":
            corrected = decision.get("correct_value", "")
            if corrected not in ("", None):
                # Try to parse as int (labels are 0/1/-1)
                try:
                    corrected_val = int(corrected)
                except (ValueError, TypeError):
                    corrected_val = corrected
                overrides[override_key] = {"verdict": verdict, "source": "corrected", "value": corrected_val}
            else:
                overrides[override_key] = {"verdict": verdict, "source": "disputed"}
        elif verdict == "两者均存疑":
            overrides[override_key] = {"verdict": verdict, "source": "disputed"}
    return overrides


def _build_episode_disputes(
    judge_cache: dict, name: str
) -> dict[str, list[str]]:
    """
    For a given official, find all episode-level disputes that are unresolved
    (verdict == "两者均存疑").
    Returns dict mapping "unit||pos" → list of dispute descriptions.
    """
    disputes: dict[str, list[str]] = {}
    for key, decision in judge_cache.items():
        parts = key.split("||")
        # Handle both old format "name||ep||unit||pos||field"
        # and new format "name||ep_batch||unit||pos||field"
        if len(parts) < 5 or parts[0] != name or parts[1] not in ("ep", "ep_batch"):
            continue
        verdict = decision.get("verdict", "")
        if verdict != "两者均存疑":
            continue
        unit_pos_key = f"{parts[2]}||{parts[3]}"
        field = parts[4]
        reason = decision.get("reason", "")
        desc = f"{field}: {reason}" if reason else field
        disputes.setdefault(unit_pos_key, []).append(desc)
    return disputes


# ── Source-line group overrides ────────────────────────────────────────────────

def _get_sl_group_overrides(judge_cache: dict, name: str) -> dict[int, dict]:
    """
    Find source_line group verdicts for an official.
    Returns {line_num: {"adopt": "DS"/"VF", "episodes": [...]}} for lines where
    the judge made a group decision. If the judge provided corrected episodes,
    those are used directly instead of swapping DS↔VF.
    """
    overrides: dict[int, dict] = {}
    for key, decision in judge_cache.items():
        # Key format: "name||sl_group||line_num"
        parts = key.split("||")
        if len(parts) == 3 and parts[0] == name and parts[1] == "sl_group":
            try:
                ln = int(parts[2])
                overrides[ln] = {
                    "adopt": decision.get("adopt", "DS"),
                    "episodes": decision.get("episodes", []),  # judge-corrected episodes
                }
            except (ValueError, TypeError):
                continue
    return overrides


def _apply_sl_group_overrides(
    ds_episodes: list[dict],
    vf_episodes: list[dict],
    overrides: dict[int, dict],
    name: str,
) -> list[dict]:
    """
    Apply group-level judge verdicts to episodes.
    Priority: judge-provided episodes > VF episodes > DS episodes.
    The judge can output corrected episodes with fixed org tags, naming, etc.
    """
    from collections import defaultdict

    # Group VF episodes by source_line
    vf_by_sl: dict[int, list[dict]] = defaultdict(list)
    for ep in vf_episodes:
        sl = ep.get("source_line", 0)
        vf_by_sl[sl].append(ep)

    # Build new episode list
    result: list[dict] = []
    seen_sl_replaced: set[int] = set()

    for ep in ds_episodes:
        sl = ep.get("source_line", 0)
        override = overrides.get(sl)

        if override is None:
            result.append(ep)
            continue

        if sl in seen_sl_replaced:
            # Already replaced this source_line, skip remaining DS episodes
            continue

        seen_sl_replaced.add(sl)
        adopt = override["adopt"]
        judge_eps = override.get("episodes", [])

        # Priority 1: Use judge-corrected episodes if available
        if judge_eps:
            # Ensure each episode has source_line set
            for je in judge_eps:
                je.setdefault("source_line", sl)
            result.extend(judge_eps)
            ds_count = len([e for e in ds_episodes if e.get("source_line", 0) == sl])
            logger.info(f"  {name} L{sl:02d}: 裁判修正版 ({len(judge_eps)}条, 基于{adopt}, 替换DS的{ds_count}条)")
        elif adopt == "VF":
            # Priority 2: Use VF episodes
            vf_eps = vf_by_sl.get(sl, [])
            if vf_eps:
                result.extend(vf_eps)
                ds_count = len([e for e in ds_episodes if e.get("source_line", 0) == sl])
                logger.info(f"  {name} L{sl:02d}: 采纳VF ({len(vf_eps)}条替换DS的{ds_count}条)")
            else:
                result.append(ep)
        else:
            # adopt == "DS", keep DS episodes
            result.append(ep)

    # Re-number 经历序号
    for i, ep in enumerate(result):
        ep["经历序号"] = i + 1

    return result


# ── Main entry ─────────────────────────────────────────────────────────────────

def run_postprocess(
    city: str,
    province: str,
    start_year: int,
    step1_path: Path | None = None,
    step2_path: Path | None = None,
    output_path: Path | None = None,
    logs_dir: Path | None = None,
    officials_dir: Path | None = None,
) -> list[dict]:
    """
    Load step1 + step2 JSON files, flatten, and return list of row dicts.
    Also writes logs/final_rows.json.
    """
    logger.info("=== Phase 4 (v2): 后处理 ===")

    _logs = logs_dir or LOGS_DIR

    if step1_path is None:
        step1_path = _logs / "deepseek_step1_results.json"
    if step2_path is None:
        step2_path = _logs / "deepseek_step2_labels.json"
    if output_path is None:
        output_path = _logs / "final_rows.json"

    if not step1_path.exists():
        logger.error(f"Step1 结果文件不存在: {step1_path}")
        return []

    step1_all: list[dict] = json.loads(step1_path.read_text(encoding="utf-8"))
    step2_all: list[dict] = []
    if step2_path.exists():
        step2_all = json.loads(step2_path.read_text(encoding="utf-8"))

    # Build name → step2 dict for quick lookup (DS step2)
    step2_by_name: dict[str, dict] = {
        item.get("_meta", {}).get("name", ""): item
        for item in step2_all
    }

    # Load verification step2 data for judge overrides that say "采纳QW"
    qw_step2_path = _logs / "verify_step2_labels.json"
    qw_step2_by_name: dict[str, dict] = {}
    if qw_step2_path.exists():
        try:
            qw_step2_all = json.loads(qw_step2_path.read_text(encoding="utf-8"))
            qw_step2_by_name = {
                item.get("_meta", {}).get("name", ""): item
                for item in qw_step2_all
            }
        except Exception:
            pass

    # Load step3 rank data (DS)
    ds_step3_path = _logs / "deepseek_step3_rank.json"
    ds_step3_by_name: dict[str, dict] = {}
    if ds_step3_path.exists():
        try:
            ds_step3_all = json.loads(ds_step3_path.read_text(encoding="utf-8"))
            ds_step3_by_name = {
                item.get("_meta", {}).get("name", ""): item
                for item in ds_step3_all
            }
        except Exception:
            pass

    # Load verification step1 data for source_line group overrides
    vf_step1_path = _logs / "verify_step1_results.json"
    vf_step1_by_name: dict[str, dict] = {}
    if vf_step1_path.exists():
        try:
            vf_step1_all = json.loads(vf_step1_path.read_text(encoding="utf-8"))
            vf_step1_by_name = {
                item.get("_meta", {}).get("name", ""): item
                for item in vf_step1_all
            }
        except Exception:
            pass

    # Load judge decisions and build overrides
    judge_cache = _load_judge_decisions(_logs / "judge_decisions.json")
    label_overrides = _build_label_overrides(judge_cache, qw_step2_by_name)

    all_rows: list[dict] = []
    for item in step1_all:
        official_name = item.get("_meta", {}).get("name", "")
        step1_data = item
        step2_data = step2_by_name.get(official_name, {})

        if not step1_data.get("episodes"):
            logger.warning(f"{official_name}: step1 无 episodes，跳过")
            continue

        # Apply source_line group verdicts: replace DS episodes with VF episodes
        # when judge says "采纳VF" for a source_line group
        episodes = step1_data.get("episodes", [])
        vf_data = vf_step1_by_name.get(official_name, {})
        vf_episodes = vf_data.get("episodes", [])

        sl_group_overrides = _get_sl_group_overrides(judge_cache, official_name)
        if sl_group_overrides:
            episodes = _apply_sl_group_overrides(
                episodes, vf_episodes, sl_group_overrides, official_name
            )
            step1_data = {**step1_data, "episodes": episodes}

        # Log label overrides
        for field in ["升迁_省长", "升迁_省委书记", "本省提拔", "本省学习"]:
            override_key = f"{official_name}||{field}"
            if override_key in label_overrides:
                ov = label_overrides[override_key]
                logger.info(f"  {official_name}.{field}: 裁判覆盖 → {ov['verdict']}")

        # Build episode-level dispute lookup for this official
        ep_disputes = _build_episode_disputes(judge_cache, official_name)

        # Collect low-confidence judge decisions for this official (threshold 90)
        low_conf_parts: list[str] = []
        for key, decision in judge_cache.items():
            if not key.startswith(f"{official_name}||"):
                continue
            conf = decision.get("confidence")
            if conf is not None:
                try:
                    if int(conf) < 90:
                        reason = decision.get("reason", "")
                        low_conf_parts.append(f"[信心:{conf}] {reason}")
                except (ValueError, TypeError):
                    pass

        # Build source_line → raw_text map from preprocessed biography
        career_lines_map: dict[int, str] = {}
        preprocessed = preprocess_official(official_name, officials_dir=officials_dir)
        if preprocessed and preprocessed.get("career_lines"):
            for cl in preprocessed["career_lines"]:
                career_lines_map[cl["line_num"]] = cl["raw_text"]

        # Build rank map from step3 data (episode_idx → final_rank)
        s3_data = ds_step3_by_name.get(official_name, {})
        ep_rank_map: dict[int, str] = {}
        for r in s3_data.get("ranks", []):
            ep_rank_map[r.get("episode_idx", 0)] = r.get("final_rank", "")

        rows = flatten_person(
            step1=step1_data,
            step2=step2_data,
            city=city,
            province=province,
            start_year=start_year,
            label_overrides=label_overrides,
            episode_disputes=ep_disputes,
            career_lines_map=career_lines_map,
            rank_map=ep_rank_map,
            low_conf_parts=low_conf_parts,
        )
        all_rows.extend(rows)
        logger.info(f"  ✓ {official_name}: {len(rows)} 行")

    # Write output
    output_path.write_text(
        json.dumps(all_rows, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    logger.info(f"总行数: {len(all_rows)}")
    logger.info(f"已保存: {output_path.name}")

    return all_rows
