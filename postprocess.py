"""
Phase 4 (v3): Post-processing

Takes merged_episodes.json + step judge decisions and:
1. Applies field-level judge overrides from step1
2. Injects rank from step2 judge results
3. Applies label overrides from step3 judge results
4. Normalises province/city names
5. Validates org tags and position tags
6. Flattens each person's episodes into rows (COLUMNS order)

Input  : logs/merged_episodes.json
         logs/step1_judge_decisions.json
         logs/step2_judge_decisions.json
         logs/step3_judge_decisions.json
         logs/llm1_step2_rank.json
         logs/llm1_step3_labels.json
         logs/llm2_step3_labels.json
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
    DIRECT_MUNICIPALITIES,
    PROVINCE_NAMES,
    EP_CHECK_FIELDS,
    POSITION_TAGS,
    get_highest_rank,
)
from text_preprocessor import preprocess_official
from utils import normalize_org_name, load_json_cache

logger = logging.getLogger(__name__)
LOGS_DIR.mkdir(parents=True, exist_ok=True)


# ── Place-name normalisation ───────────────────────────────────────────────────

def normalise_province(raw: str) -> str:
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
    if not raw:
        return ""
    raw = raw.strip()
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
    name = re.sub(r"(壮族|回族|维吾尔)?自治区$", "", name)
    name = re.sub(r"特别行政区$", "", name)
    name = re.sub(r"省$", "", name)
    return re.sub(r"[市州盟]$", "", name).rstrip("地区")


def _match_position(
    position: str, unit: str, location: str, target: str,
    *, role: str,
) -> int:
    if not position:
        return 0
    target_short = _strip_admin_suffix(target)
    context = (unit or "") + (location or "")
    if context and target_short not in context:
        return 0
    is_municipality = target_short in DIRECT_MUNICIPALITIES

    if role == "governor":
        if "副省长" in position or "副主席" in position or "常务副" in position:
            return 0
        if re.search(r"(^|[\s、，,])(?:代)?省长", position):
            return 1
        if re.search(r"(^|[\s、，,])(?:代)?主席", position):
            ctx = (unit or "") + position
            if "政协" in ctx or "人大" in ctx:
                return 0
            return 1
        if is_municipality:
            if "副市长" not in position and re.search(r"(^|[\s、，,])(?:代)?市长", position):
                return 1
    elif role == "secretary":
        if "副书记" in position:
            return 0
        if re.search(r"(^|[\s、，,])省委书记", position):
            return 1
        if re.search(r"(^|[\s、，,])自治区党委书记", position):
            return 1
        if is_municipality and re.search(r"(^|[\s、，,])市委书记", position):
            return 1
        if re.search(r"(^|[\s、，,])书记$", position):
            u = unit or ""
            if "省委" in u or "自治区党委" in u:
                return 1
            if is_municipality and "市委" in u:
                return 1
    elif role == "mayor":
        if "副市长" in position or "常务副" in position:
            return 0
        if re.search(r"(^|[\s、，,])(?:代)?市长", position):
            return 1
    elif role == "city_secretary":
        if "副书记" in position or "副市委" in position:
            return 0
        if re.search(r"(^|[\s、，,])市委书记", position):
            return 1
        if re.search(r"(^|[\s、，,])书记$", position) and "市委" in (unit or ""):
            return 1
    return 0


def is_mayor_row(position, unit, location_city, target_city):
    return _match_position(position, unit, location_city, target_city, role="mayor")

def is_secretary_row(position, unit, location_city, target_city):
    return _match_position(position, unit, location_city, target_city, role="city_secretary")

def is_governor_row(position, unit, location_prov, target_province):
    return _match_position(position, unit, location_prov, target_province, role="governor")

def is_prov_secretary_row(position, unit, location_prov, target_province):
    return _match_position(position, unit, location_prov, target_province, role="secretary")


def _is_province_mode(city: str) -> bool:
    return _strip_admin_suffix(city) in PROVINCE_NAMES


# ── Source-line reference helper ───────────────────────────────────────────────

def _build_source_ref(ep: dict, idx: int, career_lines_map: dict[int, str] | None) -> str:
    existing = ep.get("原文引用", "")
    if existing and not re.match(r"^L\d+$", existing):
        return existing
    sl = ep.get("source_line", idx + 1)
    label = f"L{sl:02d}"
    if career_lines_map and sl in career_lines_map:
        return f"{label}: {career_lines_map[sl]}"
    return label


# ── Dispute text helpers ──────────────────────────────────────────────────────

def _build_dispute_text(
    idx, ep, episode_disputes, label_dispute_parts,
    low_conf_parts, sl_conf_parts,
    ep_idx_conf_parts: dict[int, list[str]] | None = None,
) -> str:
    """Build dispute/confidence text for a single row.

    Routing rules:
    - label/bio disputes (low_conf_parts) → first row only (idx==0)
    - sl_group disputes (sl_conf_parts) → all rows sharing the same source_line
    - ep_batch disputes (ep_idx_conf_parts) → only the matched episode row
    - rank disputes (ep_idx_conf_parts) → only the matched episode row
    - episode_disputes (verdict==两者均存疑) → matched by unit||pos
    """
    unit = normalize_org_name(ep.get("供职单位", ""))
    pos = ep.get("职务", "")
    ep_dispute_key = f"{unit}||{pos}"
    ep_dispute_list = episode_disputes.get(ep_dispute_key, [])

    dispute_text = ""
    # label/bio → first row only
    if idx == 0 and label_dispute_parts:
        dispute_text = "; ".join(label_dispute_parts)
    if idx == 0 and low_conf_parts:
        conf_text = " ".join(low_conf_parts)
        dispute_text = f"{dispute_text}; {conf_text}" if dispute_text else conf_text
    # sl_group → all rows of that source_line
    ep_sl = ep.get("source_line", idx + 1)
    ep_conf_notes = sl_conf_parts.get(ep_sl, [])
    if ep_conf_notes:
        conf_text = " ".join(ep_conf_notes)
        dispute_text = f"{dispute_text}; {conf_text}" if dispute_text else conf_text
    # ep_batch / rank → only this specific episode
    orig_idx = ep.get("经历序号", idx + 1)
    if ep_idx_conf_parts:
        per_ep_notes = ep_idx_conf_parts.get(orig_idx, [])
        if per_ep_notes:
            conf_text = " ".join(per_ep_notes)
            dispute_text = f"{dispute_text}; {conf_text}" if dispute_text else conf_text
    # episode_disputes (verdict==两者均存疑) → matched by unit||pos
    if ep_dispute_list:
        ep_dispute_text = "; ".join(ep_dispute_list)
        dispute_text = f"{dispute_text}; {ep_dispute_text}" if dispute_text else ep_dispute_text
    return dispute_text


# ── Episode flattening ─────────────────────────────────────────────────────────

def flatten_person(
    step1: dict, step2: dict,
    city: str, province: str, start_year: int,
    label_overrides: dict | None = None,
    episode_disputes: dict | None = None,
    career_lines_map: dict[int, str] | None = None,
    rank_map: dict[int, str] | None = None,
    low_conf_parts: list[str] | None = None,
    sl_conf_parts: dict[int, list[str]] | None = None,
    ep_idx_conf_parts: dict[int, list[str]] | None = None,
) -> list[dict]:
    episodes: list[dict] = step1.get("episodes", [])
    if low_conf_parts is None:
        low_conf_parts = []
    if sl_conf_parts is None:
        sl_conf_parts = {}

    if ep_idx_conf_parts is None:
        ep_idx_conf_parts = {}

    raw_bio: dict = step2.get("raw_bio", step1.get("raw_bio", {}))
    name: str = raw_bio.get("姓名", step1.get("_meta", {}).get("name", ""))
    birth_year: int = raw_bio.get("出生年份", 0)
    birthplace: str = raw_bio.get("籍贯", "")
    birthplace_city: str = raw_bio.get("籍贯（市）", "")
    minority: int = raw_bio.get("少数民族", 0)
    female: int = raw_bio.get("女性", 0)
    bachelors: int = raw_bio.get("全日制本科", -1)
    fell: str = step2.get("是否落马", raw_bio.get("是否落马", "否"))
    fell_reason_global: str = step2.get("落马原因", raw_bio.get("落马原因", ""))

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
            if source == "llm2":
                return override.get("value", -1)
            elif source == "corrected":
                val = override.get("value", -1)
                logger.info(f"  {name}.{field}: 裁判自行修正 → {val}")
                return val
            elif source == "disputed":
                ds_val = step2.get(field, -1)
                label_dispute_parts.append(f"{field}: LLM1={ds_val}, 两者均存疑")
                return ds_val
        return step2.get(field, -1)

    promoted_mayor: Any = _get_label("升迁_省长")
    promoted_sec: Any = _get_label("升迁_省委书记")
    prov_promoted: Any = _get_label("本省提拔")
    prov_study: Any = _get_label("本省学习")

    _prov_mode = _is_province_mode(city)

    if _prov_mode:
        row_is_mayor = [
            is_governor_row(ep.get("职务", ""), ep.get("供职单位", ""),
                           ep.get("任职地（省）", ""), city)
            for ep in episodes
        ]
        row_is_sec = [
            is_prov_secretary_row(ep.get("职务", ""), ep.get("供职单位", ""),
                                  ep.get("任职地（省）", ""), city)
            for ep in episodes
        ]
    else:
        row_is_mayor = [
            is_mayor_row(ep.get("职务", ""), ep.get("供职单位", ""),
                        ep.get("任职地（市）", ""), city)
            for ep in episodes
        ]
        row_is_sec = [
            is_secretary_row(ep.get("职务", ""), ep.get("供职单位", ""),
                            ep.get("任职地（市）", ""), city)
            for ep in episodes
        ]

    focal_year = start_year
    for ep, is_m, is_s in zip(episodes, row_is_mayor, row_is_sec):
        if is_m or is_s:
            t = ep.get("起始时间", "")
            y = _parse_year(t)
            if y and y >= start_year:
                focal_year = y
                break

    if rank_map is None:
        rank_map = {}
    sl_rank_groups: dict[int, list[str]] = {}
    for idx_tmp, ep_tmp in enumerate(episodes):
        sl = ep_tmp.get("source_line", idx_tmp + 1)
        rank_val = ep_tmp.get("行政级别") or rank_map.get(idx_tmp + 1, "")
        sl_rank_groups.setdefault(sl, []).append(rank_val)
    sl_highest_rank: dict[int, str] = {
        sl: get_highest_rank(ranks) for sl, ranks in sl_rank_groups.items()
    }

    rows = []
    for idx, ep in enumerate(episodes):
        pos_tag_raw = ep.get("标志位", "")
        if pos_tag_raw == "学习进修":
            continue
        if pos_tag_raw == "学校":
            job = ep.get("职务", "")
            if any(kw in job for kw in ("本科生", "研究生", "进修学员", "访问学者", "留学")):
                continue

        prov_raw = ep.get("任职地（省）", ep.get("任职地", ""))
        city_raw = ep.get("任职地（市）", "")
        prov_norm = normalise_province(prov_raw)
        city_norm = normalise_city(city_raw)

        org_tag = ep.get("组织标签", "")
        if org_tag and org_tag not in ORG_TAGS:
            org_tag = f"[无效标签]{org_tag}"

        is_fell = "是" if "是" in str(fell) else "否"
        fell_reason = fell_reason_global or ep.get("落马原因", "")

        unit = normalize_org_name(ep.get("供职单位", ""))
        pos = ep.get("职务", "")
        dispute_text = _build_dispute_text(
            idx, ep, episode_disputes, label_dispute_parts,
            low_conf_parts, sl_conf_parts, ep_idx_conf_parts,
        )

        pos_tag = ep.get("标志位", "无")
        if pos_tag and pos_tag not in POSITION_TAGS:
            pos_tag = f"[无效标志位]{pos_tag}"

        highest_rank = sl_highest_rank.get(ep.get("source_line", idx + 1), "")
        per_rank = ep.get("行政级别") or rank_map.get(idx + 1, "")
        row = {
            "年份":           focal_year,
            "省份":           province,
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
            "最终行政级别":   highest_rank if highest_rank else "无",
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
            "任职地（省）":   prov_norm,
            "任职地（市）":   city_norm,
            "中央/地方":      ep.get("中央/地方", ""),
            "是否落马":       is_fell,
            "落马原因":       fell_reason,
            "备注栏":        ep.get("备注栏", ""),
        }
        rows.append(row)

    for i, row in enumerate(rows):
        row["经历序号"] = i + 1

    return rows


def _parse_year(time_str: str) -> int | None:
    if not time_str:
        return None
    m = re.match(r"(\d{4})", str(time_str))
    return int(m.group(1)) if m else None


# ── Judge integration helpers ────────────────────────────────────────────────

def _load_judge_decisions(judge_path: Path) -> dict[str, dict]:
    if not judge_path.exists():
        return {}
    try:
        return json.loads(judge_path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _build_label_overrides(
    judge_cache: dict, llm2_step3_by_name: dict
) -> dict[str, dict]:
    overrides: dict[str, dict] = {}
    for key, decision in judge_cache.items():
        parts = key.split("||")
        if len(parts) < 3 or parts[1] != "label":
            continue
        name, field = parts[0], parts[2]
        verdict = decision.get("verdict", "")
        override_key = f"{name}||{field}"

        if verdict in ("采纳LLM1", "采纳DS"):
            overrides[override_key] = {"verdict": verdict, "source": "llm1"}
        elif verdict in ("采纳LLM2", "采纳QW"):
            qw_data = llm2_step3_by_name.get(name, {})
            qw_val = qw_data.get(field, -1)
            overrides[override_key] = {"verdict": verdict, "source": "llm2", "value": qw_val}
        elif verdict in ("自行修正", "两者均存疑"):
            corrected = decision.get("correct_value", "")
            if corrected not in ("", None):
                try:
                    corrected_val = int(corrected)
                except (ValueError, TypeError):
                    corrected_val = corrected
                overrides[override_key] = {"verdict": verdict, "source": "corrected", "value": corrected_val}
            else:
                overrides[override_key] = {"verdict": verdict, "source": "disputed"}
    return overrides


def _build_episode_disputes(judge_cache: dict, name: str) -> dict[str, list[str]]:
    disputes: dict[str, list[str]] = {}
    for key, decision in judge_cache.items():
        parts = key.split("||")
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


# ── Field-level judge overrides (step1) ──────────────────────────────────────

_EP_CHECK_FIELDS = EP_CHECK_FIELDS


def _apply_ep_field_overrides(
    episodes: list[dict],
    llm2_step1_by_name: dict,
    judge_cache: dict,
    name: str,
) -> list[dict]:
    """Apply step1 field-level judge verdicts to merged episodes."""
    # Get LLM2 episodes for "采纳LLM2" overrides
    vf_data = llm2_step1_by_name.get(name, {})
    vf_episodes = vf_data.get("episodes", [])

    vf_by_key: dict[tuple, dict] = {}
    for ep in vf_episodes:
        k = (ep.get("source_line", 0), ep.get("供职单位", ""), ep.get("职务", ""))
        vf_by_key[k] = ep

    result: list[dict] = []

    for ep in episodes:
        ep = dict(ep)
        sl = ep.get("source_line", 0)
        ep_key = (
            f"{name}||ep_batch"
            f"||sl{sl}"
            f"||{ep.get('供职单位', '')}"
            f"||{ep.get('职务', '')}"
            f"||{ep.get('起始时间', '')}"
        )

        vf_ep = vf_by_key.get((sl, ep.get("供职单位", ""), ep.get("职务", "")), {})

        for field in _EP_CHECK_FIELDS:
            if field == "行政级别":
                continue  # rank handled by step2 judge
            cache_key = f"{ep_key}||{field}"
            decision = judge_cache.get(cache_key)
            if decision is None:
                continue
            verdict = decision.get("verdict", "")
            if verdict in ("采纳LLM2", "采纳QW"):
                vf_val = vf_ep.get(field)
                if vf_val is not None:
                    ep[field] = vf_val
            elif verdict in ("自行修正", "两者均存疑"):
                correct_val = decision.get("correct_value", "")
                if correct_val:
                    ep[field] = correct_val

        result.append(ep)

    return result


# ── Rank resolution from step2 judge ─────────────────────────────────────────

def _resolve_rank(
    name: str,
    llm1_rank_by_name: dict,
    llm2_rank_by_name: dict,
    step2_judge_cache: dict,
) -> dict[int, str]:
    """Build episode_idx → final_rank map, applying step2 judge overrides."""
    llm1_data = llm1_rank_by_name.get(name, {})
    llm1_ranks = {r.get("episode_idx", 0): r.get("final_rank", "") for r in llm1_data.get("ranks", [])}

    llm2_data = llm2_rank_by_name.get(name, {})
    llm2_ranks = {r.get("episode_idx", 0): r.get("final_rank", "") for r in llm2_data.get("ranks", [])}

    result = dict(llm1_ranks)

    for key, decision in step2_judge_cache.items():
        parts = key.split("||")
        if len(parts) != 3 or parts[0] != name or parts[1] != "rank":
            continue
        try:
            ep_idx = int(parts[2])
        except ValueError:
            continue
        verdict = decision.get("verdict", "")
        if verdict in ("采纳LLM2", "采纳QW"):
            result[ep_idx] = llm2_ranks.get(ep_idx, result.get(ep_idx, ""))
        elif verdict in ("自行修正", "两者均存疑"):
            correct_val = decision.get("correct_value", "")
            if correct_val:
                result[ep_idx] = correct_val

    return result


# ── Main entry ─────────────────────────────────────────────────────────────────

def run_postprocess(
    city: str,
    province: str,
    start_year: int,
    output_path: Path | None = None,
    logs_dir: Path | None = None,
    officials_dir: Path | None = None,
) -> list[dict]:
    """Load merged episodes + judge decisions, flatten, return row dicts."""
    logger.info("=== Phase 4 (v3): 后处理 ===")

    _logs = logs_dir or LOGS_DIR
    if output_path is None:
        output_path = _logs / "final_rows.json"

    # Load merged episodes (canonical after step1 judging)
    merged_path = _logs / "merged_episodes.json"
    merged_all = load_json_cache(merged_path)
    if not merged_all:
        logger.error(f"merged_episodes.json 不存在或为空: {merged_path}")
        return []

    # Load step judge decisions
    step1_judge = _load_judge_decisions(_logs / "step1_judge_decisions.json")
    step2_judge = _load_judge_decisions(_logs / "step2_judge_decisions.json")
    step3_judge = _load_judge_decisions(_logs / "step3_judge_decisions.json")

    # Load LLM1/LLM2 step2 (rank) for resolving rank
    llm1_rank_by_name = load_json_cache(_logs / "llm1_step2_rank.json")
    llm2_rank_by_name = load_json_cache(_logs / "llm2_step2_rank.json")

    # Load LLM1/LLM2 step3 (labels) for label values
    llm1_labels_by_name = load_json_cache(_logs / "llm1_step3_labels.json")
    llm2_labels_by_name = load_json_cache(_logs / "llm2_step3_labels.json")

    # Load LLM2 step1 for field-level overrides (采纳LLM2)
    llm2_step1_by_name = load_json_cache(_logs / "llm2_step1_results.json")

    # Build label overrides from step3 judge
    label_overrides = _build_label_overrides(step3_judge, llm2_labels_by_name)

    all_rows: list[dict] = []
    for name, merged_data in merged_all.items():
        episodes = merged_data.get("episodes", [])
        if not episodes:
            logger.warning(f"{name}: 无 episodes，跳过")
            continue

        step3_data = llm1_labels_by_name.get(name, {})

        # Apply step1 field-level judge overrides
        episodes = _apply_ep_field_overrides(
            episodes, llm2_step1_by_name, step1_judge, name
        )

        # Resolve rank from step2 judge
        rank_map = _resolve_rank(name, llm1_rank_by_name, llm2_rank_by_name, step2_judge)

        # Inject rank into episodes
        for ep in episodes:
            idx = ep.get("经历序号", 0)
            ep.setdefault("行政级别", rank_map.get(idx, ""))

        # Build episode-level dispute lookup
        ep_disputes = _build_episode_disputes(step1_judge, name)

        # Build confidence notes, routed by dispute type:
        #   step1 sl_group   → sl_conf_parts (all rows of that source_line)
        #   step1 ep_batch   → ep_idx_conf_parts (matched to specific episode row)
        #   step2 rank       → ep_idx_conf_parts (matched to specific episode row)
        #   step3 label/bio  → low_conf_parts (first row only)
        sl_conf_parts: dict[int, list[str]] = {}
        ep_idx_conf_parts: dict[int, list[str]] = {}
        low_conf_parts: list[str] = []
        for jcache in [step1_judge, step2_judge, step3_judge]:
            for key, decision in jcache.items():
                if not key.startswith(f"{name}||"):
                    continue
                parts_k = key.split("||")
                kind = parts_k[1] if len(parts_k) >= 2 else ""

                # Determine dispute type from cache key
                is_step1_ep = kind == "ep_batch"
                is_step1_sl = kind == "sl_group"
                is_step2 = kind == "rank"
                is_step3 = kind in ("label", "bio")

                if decision.get("judge_model") == "blocked":
                    reason = decision.get("reason", "内容安全拦截")
                    note = f"[裁判被拦截] {reason}"
                elif decision.get("confidence") is not None:
                    conf = decision["confidence"]
                    try:
                        if int(conf) >= 90:
                            continue
                    except (ValueError, TypeError):
                        continue
                    reason = decision.get("reason", "")
                    note = f"[信心:{conf}] {reason}"
                else:
                    continue

                # Route note to the appropriate container
                if is_step1_sl:
                    # sl_group → all rows of that source_line
                    if len(parts_k) >= 3:
                        try:
                            sl = int(parts_k[2])
                            sl_conf_parts.setdefault(sl, []).append(note)
                        except ValueError:
                            low_conf_parts.append(note)
                    else:
                        low_conf_parts.append(note)
                elif is_step1_ep:
                    # ep_batch → match to specific episode by (sl, unit, pos, time)
                    # key: name||ep_batch||sl{N}||{unit}||{pos}||{time}||{field}
                    if len(parts_k) >= 6:
                        try:
                            sl = int(parts_k[2][2:]) if parts_k[2].startswith("sl") else -1
                        except ValueError:
                            sl = -1
                        key_unit = parts_k[3]
                        key_pos = parts_k[4]
                        key_time = parts_k[5]
                        # Find matching episode index (1-based 经历序号)
                        matched = False
                        for ei, ep_tmp in enumerate(episodes, 1):
                            if (ep_tmp.get("source_line", 0) == sl and
                                ep_tmp.get("供职单位", "") == key_unit and
                                ep_tmp.get("职务", "") == key_pos and
                                ep_tmp.get("起始时间", "") == key_time):
                                ep_idx_conf_parts.setdefault(ei, []).append(note)
                                matched = True
                                break
                        if not matched:
                            # Fallback: try matching by sl + unit + pos only
                            for ei, ep_tmp in enumerate(episodes, 1):
                                if (ep_tmp.get("source_line", 0) == sl and
                                    ep_tmp.get("供职单位", "") == key_unit and
                                    ep_tmp.get("职务", "") == key_pos):
                                    ep_idx_conf_parts.setdefault(ei, []).append(note)
                                    matched = True
                                    break
                        if not matched:
                            # Last fallback: route to all rows of that source_line
                            if sl > 0:
                                sl_conf_parts.setdefault(sl, []).append(note)
                            else:
                                low_conf_parts.append(note)
                    else:
                        low_conf_parts.append(note)
                elif is_step2:
                    # rank → specific episode row by episode_idx
                    # key: name||rank||{ep_idx}
                    if len(parts_k) >= 3:
                        try:
                            ep_idx = int(parts_k[2])
                            ep_idx_conf_parts.setdefault(ep_idx, []).append(note)
                        except ValueError:
                            low_conf_parts.append(note)
                    else:
                        low_conf_parts.append(note)
                elif is_step3:
                    # label/bio → first row only
                    low_conf_parts.append(note)
                else:
                    low_conf_parts.append(note)

        # Build source_line → raw_text map
        career_lines_map: dict[int, str] = {}
        preprocessed = preprocess_official(name, officials_dir=officials_dir)
        if preprocessed and preprocessed.get("career_lines"):
            for cl in preprocessed["career_lines"]:
                career_lines_map[cl["line_num"]] = cl["raw_text"]

        step1_data = {"episodes": episodes, "_meta": {"name": name}}
        rows = flatten_person(
            step1=step1_data,
            step2=step3_data,
            city=city,
            province=province,
            start_year=start_year,
            label_overrides=label_overrides,
            episode_disputes=ep_disputes,
            career_lines_map=career_lines_map,
            rank_map=rank_map,
            low_conf_parts=low_conf_parts,
            sl_conf_parts=sl_conf_parts,
            ep_idx_conf_parts=ep_idx_conf_parts,
        )
        all_rows.extend(rows)
        logger.info(f"  ✓ {name}: {len(rows)} 行")

    output_path.write_text(
        json.dumps(all_rows, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    logger.info(f"总行数: {len(all_rows)}")
    logger.info(f"已保存: {output_path.name}")

    return all_rows
