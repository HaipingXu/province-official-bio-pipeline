"""
metrics.py — Accuracy metrics for eval framework.

Functions:
  compare_step2(gold_eps, pred_cls) → field-level accuracy dict
  compare_step3(gold_eps, pred_ranks) → accuracy dict
  compare_step4(gold_person, pred_labels) → accuracy dict
  compare_step1(gold_eps, pred_eps) → accuracy dict (fuzzy matching)
  summary_table(results) → pandas DataFrame
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

# ── Field definitions ────────────────────────────────────────────────────────

STEP2_FIELDS = ["组织标签", "标志位", "任职地（省）", "任职地（市）", "中央/地方"]
STEP3_FIELDS = ["该条行政级别"]
STEP4_FIELDS = ["升迁_省长", "升迁_省委书记", "本省提拔", "本省学习"]
STEP1_FIELDS = ["供职单位", "职务", "起始时间", "终止时间"]


# ── Canonical form aliases for 标志位 ────────────────────────────────────────
# The prompt (step2_classify.md line 144) defines canonical forms.
# Both gold and pred are normalized to canonical before comparison.
# Key issue: gold sometimes has '省委常委（其他）' but prompt canonical is '省常委（其他）'.
_FLAG_ALIASES: dict[str, str] = {
    # '省委常委（其他）' in gold is non-canonical; prompt says '省常委（其他）'
    "省委常委（其他）": "省常委（其他）",
    "省委常委(其他)":  "省常委（其他）",
}


def _norm(v: Any) -> str:
    """Normalize a value to string for comparison.

    Applies:
      1. Strip whitespace
      2. Full-width brackets for half-width closing bracket (（X) → （X）)
      3. Canonical label aliases (e.g. 省常委 → 省委常委)
    """
    if v is None:
        return ""
    s = str(v).strip()
    # Normalize half-width closing bracket after full-width opening bracket
    # e.g. '市常委（其他)' → '市常委（其他）'
    s = s.replace("（其他)", "（其他）")
    s = s.replace("（常委)", "（常委）")
    s = s.replace("（非常委)", "（非常委）")
    s = s.replace("（省长)", "（省长）")
    s = s.replace("（非省长)", "（非省长）")
    s = s.replace("（市长)", "（市长）")
    s = s.replace("（非市长)", "（非市长）")
    # Apply alias mapping
    return _FLAG_ALIASES.get(s, s)


# ── Step2 comparison ──────────────────────────────────────────────────────────

def compare_step2(
    gold_eps: list[dict],
    pred_cls: list[dict],
    official_name: str = "",
) -> dict[str, Any]:
    """
    Compare model step2 output vs gold.

    gold_eps: list of gold episodes (each has 组织标签, 标志位, etc.)
    pred_cls: model's "classifications" array (each has episode_idx + step2 fields)

    Returns:
      {
        "n_episodes": int,
        "fields": {
            "组织标签": {"correct": int, "total": int, "accuracy": float},
            ...
        },
        "all_fields_correct": {"correct": int, "total": int, "accuracy": float},
        "errors": [{"episode_idx": int, "field": str, "gold": str, "pred": str}]
      }
    """
    # Build lookup: episode_idx → gold ep
    gold_by_idx = {ep["episode_idx"]: ep for ep in gold_eps}
    pred_by_idx = {c.get("episode_idx", i+1): c for i, c in enumerate(pred_cls)}

    # Align on gold episode_idxs
    indices = sorted(gold_by_idx.keys())

    field_stats: dict[str, dict] = {f: {"correct": 0, "total": 0} for f in STEP2_FIELDS}
    all_correct_count = 0
    errors: list[dict] = []

    for idx in indices:
        gold_ep = gold_by_idx[idx]
        pred_ep = pred_by_idx.get(idx, {})

        ep_all_correct = True
        for f in STEP2_FIELDS:
            g = _norm(gold_ep.get(f))
            p = _norm(pred_ep.get(f))
            field_stats[f]["total"] += 1
            if g == p:
                field_stats[f]["correct"] += 1
            else:
                ep_all_correct = False
                errors.append({
                    "official": official_name,
                    "episode_idx": idx,
                    "field": f,
                    "gold": g,
                    "pred": p,
                })

        if ep_all_correct:
            all_correct_count += 1

    # Compute accuracy per field
    fields_result = {}
    for f, s in field_stats.items():
        acc = s["correct"] / s["total"] if s["total"] > 0 else 0.0
        fields_result[f] = {
            "correct": s["correct"],
            "total": s["total"],
            "accuracy": round(acc, 4),
        }

    all_acc = all_correct_count / len(indices) if indices else 0.0

    return {
        "n_episodes": len(indices),
        "fields": fields_result,
        "all_fields_correct": {
            "correct": all_correct_count,
            "total": len(indices),
            "accuracy": round(all_acc, 4),
        },
        "errors": errors,
    }


# ── Step3 comparison ──────────────────────────────────────────────────────────

def compare_step3(
    gold_eps: list[dict],
    pred_ranks: list[dict],
    official_name: str = "",
) -> dict[str, Any]:
    """
    Compare model step3 output vs gold.

    pred_ranks: model's "ranks" array (each has episode_idx + final_rank)
    """
    gold_by_idx = {ep["episode_idx"]: ep for ep in gold_eps}
    pred_by_idx = {r.get("episode_idx", i+1): r for i, r in enumerate(pred_ranks)}
    indices = sorted(gold_by_idx.keys())

    correct = 0
    errors: list[dict] = []

    for idx in indices:
        gold_ep = gold_by_idx[idx]
        pred_r = pred_by_idx.get(idx, {})
        g = _norm(gold_ep.get("该条行政级别"))
        p = _norm(pred_r.get("final_rank"))
        if g == p:
            correct += 1
        else:
            errors.append({
                "official": official_name,
                "episode_idx": idx,
                "field": "该条行政级别",
                "gold": g,
                "pred": p,
            })

    total = len(indices)
    acc = correct / total if total > 0 else 0.0

    return {
        "n_episodes": total,
        "fields": {
            "该条行政级别": {
                "correct": correct,
                "total": total,
                "accuracy": round(acc, 4),
            }
        },
        "all_fields_correct": {
            "correct": correct,
            "total": total,
            "accuracy": round(acc, 4),
        },
        "errors": errors,
    }


# ── Step4 comparison ──────────────────────────────────────────────────────────

def compare_step4(
    gold_person: dict,
    pred_labels: dict,
    official_name: str = "",
) -> dict[str, Any]:
    """
    Compare model step4 bio/label output vs gold (person-level).

    gold_person: dict with 升迁_省长, 升迁_省委书记, 本省提拔, 本省学习
    pred_labels: model output with same fields
    """
    correct = 0
    total = 0
    errors: list[dict] = []

    for f in STEP4_FIELDS:
        g_raw = gold_person.get(f)
        p_raw = pred_labels.get(f)

        # Skip fields where gold is missing (ambiguous / not applicable)
        if g_raw is None or _norm(g_raw) == "":
            continue

        try:
            g = int(float(str(g_raw)))
        except (ValueError, TypeError):
            # Gold is a non-numeric string (e.g., ""), skip
            continue

        try:
            p = int(float(str(p_raw)))
        except (ValueError, TypeError):
            p = -999  # sentinel for parse failure

        total += 1
        if g == p:
            correct += 1
        else:
            errors.append({
                "official": official_name,
                "field": f,
                "gold": g,
                "pred": p_raw,
            })

    acc = correct / total if total > 0 else 0.0

    return {
        "n_fields": total,
        "correct": correct,
        "accuracy": round(acc, 4),
        "errors": errors,
    }


# ── Step1 comparison (fuzzy episode matching) ─────────────────────────────────

def _jaccard(a: str, b: str) -> float:
    """Simple character-level Jaccard similarity."""
    sa, sb = set(a), set(b)
    if not sa and not sb:
        return 1.0
    return len(sa & sb) / len(sa | sb)


def _match_episodes(gold_eps: list[dict], pred_eps: list[dict]) -> list[tuple[dict, dict | None]]:
    """
    Match predicted episodes to gold episodes by greedy best-match on 供职单位.
    Returns [(gold_ep, matched_pred_ep or None), ...]
    """
    pred_remaining = list(pred_eps)
    matched: list[tuple[dict, dict | None]] = []

    for g_ep in gold_eps:
        g_unit = _norm(g_ep.get("供职单位", ""))
        g_pos  = _norm(g_ep.get("职务", ""))

        if not pred_remaining:
            matched.append((g_ep, None))
            continue

        # Find best match by 供职单位 jaccard
        best_idx = 0
        best_score = -1.0
        for i, p_ep in enumerate(pred_remaining):
            p_unit = _norm(p_ep.get("供职单位", ""))
            score = _jaccard(g_unit, p_unit)
            if score > best_score:
                best_score = score
                best_idx = i

        if best_score >= 0.3:  # minimum similarity threshold
            matched.append((g_ep, pred_remaining.pop(best_idx)))
        else:
            matched.append((g_ep, None))

    return matched


def compare_step1(
    gold_eps: list[dict],
    pred_eps: list[dict],
    official_name: str = "",
) -> dict[str, Any]:
    """
    Compare model step1 output vs gold (with fuzzy matching).

    pred_eps: model's "episodes" array
    """
    matched = _match_episodes(gold_eps, pred_eps)

    # Count: how many gold episodes were matched at all
    matched_count = sum(1 for _, p in matched if p is not None)

    field_stats: dict[str, dict] = {f: {"correct": 0, "total": 0} for f in STEP1_FIELDS}
    errors: list[dict] = []

    for g_ep, p_ep in matched:
        idx = g_ep.get("episode_idx", "?")
        for f in STEP1_FIELDS:
            g = _norm(g_ep.get(f))
            p = _norm(p_ep.get(f)) if p_ep else ""
            field_stats[f]["total"] += 1
            if g == p:
                field_stats[f]["correct"] += 1
            else:
                errors.append({
                    "official": official_name,
                    "episode_idx": idx,
                    "field": f,
                    "gold": g,
                    "pred": p,
                })

    total = len(gold_eps)
    fields_result = {}
    for f, s in field_stats.items():
        acc = s["correct"] / s["total"] if s["total"] > 0 else 0.0
        fields_result[f] = {
            "correct": s["correct"],
            "total": s["total"],
            "accuracy": round(acc, 4),
        }

    # Precision / Recall / F1 on episode count
    pred_total = len(pred_eps)
    recall = matched_count / total if total > 0 else 0.0
    precision = matched_count / pred_total if pred_total > 0 else 0.0
    f1 = (2 * precision * recall / (precision + recall)
          if precision + recall > 0 else 0.0)

    return {
        "n_gold_episodes": total,
        "n_pred_episodes": pred_total,
        "matched_episodes": matched_count,
        "episode_recall": round(recall, 4),
        "episode_precision": round(precision, 4),
        "episode_f1": round(f1, 4),
        "fields": fields_result,
        "errors": errors,
    }


# ── Aggregate across officials ────────────────────────────────────────────────

def aggregate_results(
    per_official: dict[str, dict],
    fields: list[str],
) -> dict[str, Any]:
    """
    Aggregate per-official field metrics across all officials.

    per_official: {official_name: {fields: {field_name: {correct, total, accuracy}}}}
    """
    agg: dict[str, dict] = {f: {"correct": 0, "total": 0} for f in fields}

    for name, result in per_official.items():
        for f in fields:
            fs = result.get("fields", {}).get(f, {})
            agg[f]["correct"] += fs.get("correct", 0)
            agg[f]["total"]   += fs.get("total", 0)

    result_agg = {}
    for f, s in agg.items():
        acc = s["correct"] / s["total"] if s["total"] > 0 else 0.0
        result_agg[f] = {
            "correct": s["correct"],
            "total": s["total"],
            "accuracy": round(acc, 4),
        }

    return result_agg


def aggregate_step4(per_official: dict[str, dict]) -> dict[str, Any]:
    """Aggregate step4 person-level results across officials."""
    total_correct = sum(r.get("correct", 0) for r in per_official.values())
    total_n = sum(r.get("n_fields", 0) for r in per_official.values())
    acc = total_correct / total_n if total_n > 0 else 0.0
    return {
        "correct": total_correct,
        "total": total_n,
        "accuracy": round(acc, 4),
    }
