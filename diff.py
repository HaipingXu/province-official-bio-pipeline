"""
Diff functions for comparing LLM1 vs LLM2 extraction results.

Helpers for source-line-based episode matching and field comparison.
Entry points (one per step):
  diff_step1(logs_dir)  → step1_diff_report.json
  diff_step2(logs_dir)  → step2_diff_report.json
  diff_step3(logs_dir)  → step3_diff_report.json
"""

import json
import logging
from collections import defaultdict
from pathlib import Path

from config import DATE_DISCREPANCY_YEARS
from utils import normalize_org_name, to_float_date, load_json_cache

logger = logging.getLogger(__name__)


# ── Verdict determination (shared across steps) ──────────────────────────────

def compute_verdict(high_count: int, medium_count: int) -> str:
    """Determine verdict from HIGH/MEDIUM diff counts."""
    if high_count >= 2 or (high_count >= 1 and medium_count >= 2):
        return "MAJOR_CONFLICT"
    elif high_count >= 1 or medium_count >= 2:
        return "NEEDS_REVIEW"
    return "PASS"


# ── Source-line based matching ────────────────────────────────────────────────

def group_by_source_line(episodes: list[dict]) -> dict[int, list[dict]]:
    """Group episodes by source_line number."""
    groups: dict[int, list[dict]] = defaultdict(list)
    for i, ep in enumerate(episodes):
        sl = ep.get("source_line", i + 1)  # fallback to position
        groups[sl].append(ep)
    return dict(groups)


def diff_episode_groups(ds_group: list[dict], vf_group: list[dict],
                        source_line: int) -> list[dict]:
    """Compare two groups of episodes from the same source_line."""
    diffs = []

    if len(ds_group) == len(vf_group):
        ds_sorted = sorted(ds_group, key=lambda e: e.get("供职单位", ""))
        vf_sorted = sorted(vf_group, key=lambda e: e.get("供职单位", ""))
        for ep_ds, ep_vf in zip(ds_sorted, vf_sorted):
            diffs += _diff_single_pair(ep_ds, ep_vf, source_line)
    else:
        ds_desc = "; ".join(f"{e.get('供职单位', '')} {e.get('职务', '')}" for e in ds_group)
        vf_desc = "; ".join(f"{e.get('供职单位', '')} {e.get('职务', '')}" for e in vf_group)
        diffs.append({
            "scope": "episode_split",
            "field": "拆分方式",
            "llm1_value": f"{len(ds_group)}条: {ds_desc}",
            "llm2_value": f"{len(vf_group)}条: {vf_desc}",
            "level": "MEDIUM",
            "source_line": source_line,
        })
        ds_sorted = sorted(ds_group, key=lambda e: e.get("供职单位", ""))
        vf_sorted = sorted(vf_group, key=lambda e: e.get("供职单位", ""))
        for i in range(min(len(ds_sorted), len(vf_sorted))):
            diffs += _diff_single_pair(ds_sorted[i], vf_sorted[i], source_line)

    return diffs


def _diff_single_pair(ep_ds: dict, ep_vf: dict, source_line: int) -> list[dict]:
    """Compare two individual episodes from the same source line."""
    diffs = []
    for field in ["起始时间", "终止时间", "组织标签", "供职单位", "职务",
                  "任职地（省）", "任职地（市）", "中央/地方"]:
        v_ds = str(ep_ds.get(field, ""))
        v_vf = str(ep_vf.get(field, ""))
        if v_ds == v_vf:
            continue

        if field == "供职单位":
            if normalize_org_name(v_ds) == normalize_org_name(v_vf):
                continue

        if field in ("起始时间", "终止时间"):
            fd_ds = to_float_date(v_ds)
            fd_vf = to_float_date(v_vf)
            if fd_ds is None or fd_vf is None:
                # One or both values are empty — flag as MEDIUM for review
                level = "MEDIUM"
            else:
                diff_yr = abs(fd_ds - fd_vf)
                if diff_yr <= DATE_DISCREPANCY_YEARS:
                    continue
                level = "HIGH" if diff_yr > 2 else "MEDIUM"
        else:
            level = "MEDIUM"

        diffs.append({
            "scope": "episode_field",
            "field": field,
            "llm1_value": v_ds,
            "llm2_value": v_vf,
            "level": level,
            "source_line": source_line,
            "供职单位": ep_ds.get("供职单位", ""),
        })

    return diffs


def diff_bio_fields(bio_ds: dict, bio_vf: dict) -> list[dict]:
    """Compare raw_bio fields between LLM1 step3 and LLM2 step3."""
    diffs = []
    for field in ["出生年份", "籍贯", "少数民族", "女性", "全日制本科"]:
        v_ds = bio_ds.get(field)
        v_vf = bio_vf.get(field)
        if v_ds is None or v_vf is None:
            continue
        if v_ds != v_vf:
            level = "HIGH" if field in ["出生年份", "少数民族", "女性"] else "MEDIUM"
            diffs.append({"scope": "bio", "field": field,
                          "llm1_value": v_ds, "llm2_value": v_vf, "level": level})
    return diffs


def diff_label_fields(lbl_ds: dict, lbl_vf: dict) -> list[dict]:
    """Compare step3 label fields."""
    diffs = []
    for field in ["升迁_省长", "升迁_省委书记", "本省提拔", "本省学习"]:
        v_ds = lbl_ds.get(field)
        v_vf = lbl_vf.get(field)
        if v_ds is None or v_vf is None:
            continue
        if v_ds != v_vf:
            diffs.append({
                "scope": "label", "field": field,
                "llm1_value": v_ds, "ds_reason": lbl_ds.get(field + "依据", ""),
                "llm2_value": v_vf, "qw_reason": lbl_vf.get(field + "依据", ""),
                "level": "HIGH",
            })
    return diffs


def diff_corruption(ds_s3: dict, vf_s3: dict) -> list[dict]:
    """Compare corruption fields from step3."""
    diffs = []
    ds_luoma = ds_s3.get("是否落马", "")
    vf_luoma = vf_s3.get("是否落马", "")
    if ds_luoma and vf_luoma and ds_luoma != vf_luoma:
        diffs.append({
            "scope": "corruption", "field": "是否落马",
            "llm1_value": ds_luoma, "llm2_value": vf_luoma,
            "level": "HIGH",
        })
    return diffs


def diff_all_episodes(eps_ds: list[dict], eps_vf: list[dict]) -> list[dict]:
    """Diff episodes using source_line grouping."""
    ds_groups = group_by_source_line(eps_ds)
    vf_groups = group_by_source_line(eps_vf)
    all_lines = sorted(set(ds_groups) | set(vf_groups))

    diffs = []
    for line_num in all_lines:
        ds_g = ds_groups.get(line_num, [])
        vf_g = vf_groups.get(line_num, [])

        if not ds_g and vf_g:
            desc = "; ".join(f"{e.get('供职单位', '')} {e.get('职务', '')}" for e in vf_g)
            diffs.append({
                "scope": "episode_missing", "field": "LLM1缺失",
                "llm1_value": "（无）", "llm2_value": desc,
                "level": "MEDIUM", "source_line": line_num,
            })
        elif ds_g and not vf_g:
            desc = "; ".join(f"{e.get('供职单位', '')} {e.get('职务', '')}" for e in ds_g)
            diffs.append({
                "scope": "episode_missing", "field": "LLM2缺失",
                "llm1_value": desc, "llm2_value": "（无）",
                "level": "MEDIUM", "source_line": line_num,
            })
        else:
            diffs += diff_episode_groups(ds_g, vf_g, line_num)

    return diffs


# ── Diff: Step 1 (episode structure) ────────────────────────────────────────

def diff_step1(logs_dir: Path) -> Path:
    """Diff LLM1 vs LLM2 step1 episode extraction. Saves step1_diff_report.json."""
    logger.info("=== Step1 Diff: LLM1 vs LLM2 episodes ===")

    llm1_cache = load_json_cache(logs_dir / "llm1_step1_results.json")
    llm2_cache = load_json_cache(logs_dir / "llm2_step1_results.json")

    all_diffs: list[dict] = []

    for name, ds_s1 in llm1_cache.items():
        vf_s1 = llm2_cache.get(name)
        if not vf_s1:
            continue

        person_diffs = diff_all_episodes(
            ds_s1.get("episodes", []), vf_s1.get("episodes", [])
        )

        high_count = sum(1 for d in person_diffs if d["level"] == "HIGH")
        medium_count = sum(1 for d in person_diffs if d["level"] == "MEDIUM")
        verdict = compute_verdict(high_count, medium_count)

        all_diffs.append({
            "official_name": name,
            "verdict": verdict,
            "high_count": high_count,
            "medium_count": medium_count,
            "diffs": person_diffs,
            "llm1_step1": ds_s1,
            "llm2_step1": vf_s1,
        })
        logger.info(f"  [{name}] {verdict} ({high_count}H {medium_count}M, {len(person_diffs)} diffs)")

    output_path = logs_dir / "step1_diff_report.json"
    output_path.write_text(json.dumps(all_diffs, ensure_ascii=False, indent=2), encoding="utf-8")

    total = len(all_diffs)
    passes = sum(1 for d in all_diffs if d["verdict"] == "PASS")
    reviews = sum(1 for d in all_diffs if d["verdict"] == "NEEDS_REVIEW")
    conflicts = sum(1 for d in all_diffs if d["verdict"] == "MAJOR_CONFLICT")
    logger.info(f"Step1 Diff 完成: PASS={passes}, NEEDS_REVIEW={reviews}, MAJOR_CONFLICT={conflicts} / {total}")

    return output_path


# ── Diff: Step 2 (rank) ─────────────────────────────────────────────────────

def diff_step2(logs_dir: Path) -> Path:
    """Diff LLM1 vs LLM2 step2 rank results. Saves step2_diff_report.json."""
    logger.info("=== Step2 Diff: LLM1 vs LLM2 rank ===")

    llm1_cache = load_json_cache(logs_dir / "llm1_step2_rank.json")
    llm2_cache = load_json_cache(logs_dir / "llm2_step2_rank.json")
    merged_episodes = load_json_cache(logs_dir / "merged_episodes.json")

    all_diffs: list[dict] = []

    for name, ds_s2 in llm1_cache.items():
        vf_s2 = llm2_cache.get(name)
        if not vf_s2:
            continue

        ds_ranks = {r.get("episode_idx", 0): r.get("final_rank", "") for r in ds_s2.get("ranks", [])}
        vf_ranks = {r.get("episode_idx", 0): r.get("final_rank", "") for r in vf_s2.get("ranks", [])}

        # Get merged episodes for context (unit + position)
        ep_data = merged_episodes.get(name, {})
        episodes = ep_data.get("episodes", [])
        ep_map = {ep.get("经历序号", i+1): ep for i, ep in enumerate(episodes)}

        person_diffs = []
        all_indices = sorted(set(ds_ranks) | set(vf_ranks))
        for idx in all_indices:
            ds_rank = ds_ranks.get(idx, "")
            vf_rank = vf_ranks.get(idx, "")
            if ds_rank != vf_rank:
                ep = ep_map.get(idx, {})
                person_diffs.append({
                    "scope": "rank",
                    "field": "行政级别",
                    "episode_idx": idx,
                    "供职单位": ep.get("供职单位", ""),
                    "职务": ep.get("职务", ""),
                    "llm1_value": ds_rank,
                    "llm2_value": vf_rank,
                    "level": "MEDIUM",
                })

        high_count = sum(1 for d in person_diffs if d["level"] == "HIGH")
        medium_count = sum(1 for d in person_diffs if d["level"] == "MEDIUM")
        verdict = "PASS" if not person_diffs else "NEEDS_REVIEW"

        all_diffs.append({
            "official_name": name,
            "verdict": verdict,
            "high_count": high_count,
            "medium_count": medium_count,
            "diffs": person_diffs,
            "llm1_step2": ds_s2,
            "llm2_step2": vf_s2,
        })
        if person_diffs:
            logger.info(f"  [{name}] {len(person_diffs)} rank diffs")

    output_path = logs_dir / "step2_diff_report.json"
    output_path.write_text(json.dumps(all_diffs, ensure_ascii=False, indent=2), encoding="utf-8")

    total = len(all_diffs)
    n_with_diffs = sum(1 for d in all_diffs if d["diffs"])
    logger.info(f"Step2 Diff 完成: {n_with_diffs} 人有 rank 差异 / {total}")

    return output_path


# ── Diff: Step 3 (labels + bio + corruption) ────────────────────────────────

def diff_step3(logs_dir: Path) -> Path:
    """Diff LLM1 vs LLM2 step3 label/bio results. Saves step3_diff_report.json."""
    logger.info("=== Step3 Diff: LLM1 vs LLM2 labels ===")

    llm1_cache = load_json_cache(logs_dir / "llm1_step3_labels.json")
    llm2_cache = load_json_cache(logs_dir / "llm2_step3_labels.json")

    all_diffs: list[dict] = []

    for name, ds_s3 in llm1_cache.items():
        vf_s3 = llm2_cache.get(name)
        if not vf_s3:
            continue

        person_diffs: list[dict] = []
        person_diffs += diff_bio_fields(
            ds_s3.get("raw_bio", {}), vf_s3.get("raw_bio", {})
        )
        person_diffs += diff_label_fields(ds_s3, vf_s3)
        person_diffs += diff_corruption(ds_s3, vf_s3)

        high_count = sum(1 for d in person_diffs if d["level"] == "HIGH")
        medium_count = sum(1 for d in person_diffs if d["level"] == "MEDIUM")
        verdict = compute_verdict(high_count, medium_count)

        all_diffs.append({
            "official_name": name,
            "verdict": verdict,
            "high_count": high_count,
            "medium_count": medium_count,
            "diffs": person_diffs,
            "llm1_step3": ds_s3,
            "llm2_step3": vf_s3,
        })
        if person_diffs:
            logger.info(f"  [{name}] {verdict} ({high_count}H {medium_count}M, {len(person_diffs)} diffs)")

    output_path = logs_dir / "step3_diff_report.json"
    output_path.write_text(json.dumps(all_diffs, ensure_ascii=False, indent=2), encoding="utf-8")

    total = len(all_diffs)
    passes = sum(1 for d in all_diffs if d["verdict"] == "PASS")
    logger.info(f"Step3 Diff 完成: PASS={passes} / {total}")

    return output_path
