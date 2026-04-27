"""
Phase 4 (v9): Post-processing.

Reads merged_episodes.json (post step2 judge) + step{1..4}_judge_decisions.json
and produces final_rows.json with the v9 column ordering, including per-step
judge confidence/reason columns judge1con / judge2con / judge3con / judge4con.

Threshold: decisions with confidence < JUDGE_CONF_THRESHOLD (85) flow into
"争议未解决"; ALL confidence values are surfaced in judge{1..4}con regardless.
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


# ── Confidence / reason rendering ─────────────────────────────────────────────

def _format_decision(decision: dict | None) -> str:
    """Render a judge decision into '[信心:NN] 理由' form. Empty if no decision."""
    if not decision:
        return ""
    conf = decision.get("confidence")
    reason = decision.get("reason", "")
    judge_model = decision.get("judge_model", "")
    if judge_model == "blocked":
        return f"[裁判被拦截] {reason}"
    if conf is None or conf == "":
        return ""
    try:
        conf_int = int(conf)
    except (ValueError, TypeError):
        return ""
    return f"[信心:{conf_int}] {reason}".strip()


# ── Build per-row judge confidence buckets ───────────────────────────────────

def _build_judge_buckets_for_person(
    name: str, episodes: list[dict],
    step1_judge: dict, step2_judge: dict,
    step3_judge: dict, step4_judge: dict,
) -> dict:
    """Return per-row judge confidence/reason strings.

    Returns dict with keys:
      judge1_per_row[ep_idx] -> str  (step1 ep_batch + sl_group, routed to ep)
      judge2_per_row[ep_idx] -> str  (step2 classify, routed to ep)
      judge3_per_row[ep_idx] -> str  (step3 rank, routed to ep)
      judge4_person          -> str  (step4 label/bio, single string for first row)
    """
    judge1_per_row: dict[int, list[str]] = {}
    judge2_per_row: dict[int, list[str]] = {}
    judge3_per_row: dict[int, list[str]] = {}
    judge4_parts: list[str] = []

    # Index episodes by (sl, unit, pos[, time])
    ep_index_by_sl: dict[int, list[tuple[int, dict]]] = {}
    for i, ep in enumerate(episodes, 1):
        ep_index_by_sl.setdefault(ep.get("source_line", 0), []).append((i, ep))

    def _match_ep_index(sl: int, unit: str, pos: str, time: str = "") -> int | None:
        candidates = ep_index_by_sl.get(sl, [])
        for ei, ep in candidates:
            if (ep.get("供职单位", "") == unit and
                ep.get("职务", "") == pos and
                (not time or ep.get("起始时间", "") == time)):
                return ei
        for ei, ep in candidates:
            if ep.get("供职单位", "") == unit and ep.get("职务", "") == pos:
                return ei
        return None

    # ---- Step1 ----
    for key, decision in step1_judge.items():
        if not key.startswith(f"{name}||"):
            continue
        parts = key.split("||")
        kind = parts[1] if len(parts) >= 2 else ""
        text = _format_decision(decision)

        if kind == "sl_group" and len(parts) >= 3:
            try:
                sl = int(parts[2])
            except ValueError:
                continue
            if not text:
                continue
            for ei, _ in ep_index_by_sl.get(sl, []):
                judge1_per_row.setdefault(ei, []).append(text)
        elif kind == "ep_batch" and len(parts) >= 7:
            try:
                sl = int(parts[2][2:]) if parts[2].startswith("sl") else -1
            except ValueError:
                sl = -1
            unit = parts[3]
            pos = parts[4]
            time = parts[5]
            field = parts[6]
            ei = _match_ep_index(sl, unit, pos, time)
            if not text:
                continue
            text_with_field = f"{field}{text}" if field else text
            if ei is not None:
                judge1_per_row.setdefault(ei, []).append(text_with_field)
            elif sl > 0:
                for ej, _ in ep_index_by_sl.get(sl, []):
                    judge1_per_row.setdefault(ej, []).append(text_with_field)

    # ---- Step2 ----
    for key, decision in step2_judge.items():
        if not key.startswith(f"{name}||classify||"):
            continue
        parts = key.split("||")
        if len(parts) < 4:
            continue
        try:
            ep_idx = int(parts[2])
        except ValueError:
            continue
        field = parts[3]
        text = _format_decision(decision)
        if not text:
            continue
        judge2_per_row.setdefault(ep_idx, []).append(f"{field}{text}")

    # ---- Step3 ----
    for key, decision in step3_judge.items():
        if not key.startswith(f"{name}||rank||"):
            continue
        parts = key.split("||")
        if len(parts) < 3:
            continue
        try:
            ep_idx = int(parts[2])
        except ValueError:
            continue
        text = _format_decision(decision)
        if not text:
            continue
        judge3_per_row.setdefault(ep_idx, []).append(text)

    # ---- Step4 ----
    for key, decision in step4_judge.items():
        if not key.startswith(f"{name}||"):
            continue
        parts = key.split("||")
        kind = parts[1] if len(parts) >= 2 else ""
        if kind != "label":
            continue
        if len(parts) < 3:
            continue
        field = parts[2]
        text = _format_decision(decision)
        if not text:
            continue
        judge4_parts.append(f"{field}{text}")

    return {
        "judge1_per_row":  {k: " | ".join(v) for k, v in judge1_per_row.items()},
        "judge2_per_row":  {k: " | ".join(v) for k, v in judge2_per_row.items()},
        "judge3_per_row":  {k: " | ".join(v) for k, v in judge3_per_row.items()},
        "judge4_person":    " | ".join(judge4_parts),
    }


# ── Episode flattening ─────────────────────────────────────────────────────────

def flatten_person(
    episodes: list[dict],
    step4_data: dict,
    city: str, province: str, start_year: int,
    label_overrides: dict | None = None,
    career_lines_map: dict[int, str] | None = None,
    rank_map: dict[int, str] | None = None,
    judge_buckets: dict | None = None,
) -> list[dict]:
    raw_bio: dict = step4_data.get("raw_bio", {})
    name: str = raw_bio.get("姓名", "")
    birth_year: int = raw_bio.get("出生年份", 0)
    birthplace: str = raw_bio.get("籍贯", "")
    birthplace_city: str = raw_bio.get("籍贯（市）", "")
    minority: int = raw_bio.get("少数民族", 0)
    female: int = raw_bio.get("女性", 0)
    bachelors: int = raw_bio.get("全日制本科", -1)
    fell: str = step4_data.get("是否落马", "否")
    fell_reason_global: str = step4_data.get("落马原因", "")

    if label_overrides is None:
        label_overrides = {}
    if judge_buckets is None:
        judge_buckets = {
            "judge1_per_row": {}, "judge2_per_row": {}, "judge3_per_row": {},
            "judge4_person": "",
        }

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
                # judge4con already records the "两者均存疑" verdict + reason
                return step4_data.get(field, -1)
        return step4_data.get(field, -1)

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

    # judge4 confidence goes to first row only
    judge4_first_row = judge_buckets.get("judge4_person", "")

    rows = []
    for idx, ep in enumerate(episodes):
        pos_tag_raw = ep.get("标志位", "")
        if pos_tag_raw == "学习进修":
            continue
        if pos_tag_raw == "学校":
            job = ep.get("职务", "")
            if any(kw in job for kw in ("本科生", "研究生", "进修学员", "访问学者", "留学")):
                continue

        ep_idx = ep.get("经历序号", idx + 1)
        ep_sl = ep.get("source_line", idx + 1)

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

        pos_tag = ep.get("标志位", "无")
        if pos_tag and pos_tag not in POSITION_TAGS:
            pos_tag = f"[无效标志位]{pos_tag}"

        highest_rank = sl_highest_rank.get(ep_sl, "")
        per_rank = ep.get("行政级别") or rank_map.get(idx + 1, "")
        if not per_rank:
            per_rank = "无"

        # Per-row judgeNcon strings (all confidences, threshold-independent)
        j1 = judge_buckets.get("judge1_per_row", {}).get(ep_idx, "")
        j2 = judge_buckets.get("judge2_per_row", {}).get(ep_idx, "")
        j3 = judge_buckets.get("judge3_per_row", {}).get(ep_idx, "")
        j4 = judge4_first_row if idx == 0 else ""

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
            "judge4con":      j4,
            "最终行政级别":   highest_rank if highest_rank else "无",
            "经历序号":       ep_idx,
            "起始时间":       ep.get("起始时间", ""),
            "终止时间":       ep.get("终止时间", ""),
            "供职单位":       unit,
            "职务":           pos,
            "judge1con":      j1,
            "组织标签":       org_tag,
            "标志位":         pos_tag,
            "任职地（省）":   prov_norm,
            "任职地（市）":   city_norm,
            "中央/地方":      ep.get("中央/地方", ""),
            "judge2con":      j2,
            "该条行政级别":   per_rank,
            "judge3con":      j3,
            "原文引用":       _build_source_ref(ep, idx, career_lines_map),
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
    judge_cache: dict, llm2_step4_by_name: dict
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
            qw_data = llm2_step4_by_name.get(name, {})
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


# ── Rank resolution from step3 judge ────────────────────────────────────────

def _resolve_rank(
    name: str,
    llm1_rank_by_name: dict,
    llm2_rank_by_name: dict,
    step3_judge_cache: dict,
) -> dict[int, str]:
    """Build episode_idx → final_rank map, applying step3 judge overrides."""
    llm1_data = llm1_rank_by_name.get(name, {})
    llm1_ranks = {r.get("episode_idx", 0): r.get("final_rank", "") for r in llm1_data.get("ranks", [])}

    llm2_data = llm2_rank_by_name.get(name, {})
    llm2_ranks = {r.get("episode_idx", 0): r.get("final_rank", "") for r in llm2_data.get("ranks", [])}

    result = dict(llm1_ranks)

    for key, decision in step3_judge_cache.items():
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
    """Load merged episodes + 4 judge decisions, flatten, return row dicts."""
    logger.info("=== Phase 4 (v9): 后处理 ===")

    _logs = logs_dir or LOGS_DIR
    if output_path is None:
        output_path = _logs / "final_rows.json"

    merged_path = _logs / "merged_episodes.json"
    merged_all = load_json_cache(merged_path)
    if not merged_all:
        logger.error(f"merged_episodes.json 不存在或为空: {merged_path}")
        return []

    step1_judge = _load_judge_decisions(_logs / "step1_judge_decisions.json")
    step2_judge = _load_judge_decisions(_logs / "step2_judge_decisions.json")
    step3_judge = _load_judge_decisions(_logs / "step3_judge_decisions.json")
    step4_judge = _load_judge_decisions(_logs / "step4_judge_decisions.json")

    llm1_rank_by_name = load_json_cache(_logs / "llm1_step3_rank.json")
    llm2_rank_by_name = load_json_cache(_logs / "llm2_step3_rank.json")

    llm1_labels_by_name = load_json_cache(_logs / "llm1_step4_labels.json")
    llm2_labels_by_name = load_json_cache(_logs / "llm2_step4_labels.json")

    label_overrides = _build_label_overrides(step4_judge, llm2_labels_by_name)

    all_rows: list[dict] = []
    for name, merged_data in merged_all.items():
        episodes = merged_data.get("episodes", [])
        if not episodes:
            logger.warning(f"{name}: 无 episodes，跳过")
            continue

        step4_data = llm1_labels_by_name.get(name, {})

        rank_map = _resolve_rank(name, llm1_rank_by_name, llm2_rank_by_name, step3_judge)

        for ep in episodes:
            idx = ep.get("经历序号", 0)
            ep.setdefault("行政级别", rank_map.get(idx, ""))

        judge_buckets = _build_judge_buckets_for_person(
            name, episodes,
            step1_judge, step2_judge, step3_judge, step4_judge,
        )

        career_lines_map: dict[int, str] = {}
        preprocessed = preprocess_official(name, officials_dir=officials_dir)
        if preprocessed and preprocessed.get("career_lines"):
            for cl in preprocessed["career_lines"]:
                career_lines_map[cl["line_num"]] = cl["raw_text"]

        rows = flatten_person(
            episodes=episodes,
            step4_data=step4_data,
            city=city,
            province=province,
            start_year=start_year,
            label_overrides=label_overrides,
            career_lines_map=career_lines_map,
            rank_map=rank_map,
            judge_buckets=judge_buckets,
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
