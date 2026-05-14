"""
report.py — Generate comparison reports from saved eval results.

Usage:
  uv run eval/report.py --mode 2
  uv run eval/report.py --mode 2 --format latex
  uv run eval/report.py --mode 2 --format csv
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

RESULTS_DIR = Path(__file__).parent / "results"

STEP2_FIELDS = ["组织标签", "标志位", "任职地（省）", "任职地（市）", "中央/地方"]
STEP3_FIELDS = ["该条行政级别"]
STEP4_FIELDS = ["升迁_省长", "升迁_省委书记", "本省提拔", "本省学习"]


def load_all_results(mode: int) -> list[dict]:
    """Load all saved result JSONs for given mode."""
    pattern = f"mode{mode}_*.json"
    results = []
    for p in sorted(RESULTS_DIR.glob(pattern)):
        try:
            results.append(json.loads(p.read_text(encoding="utf-8")))
        except Exception as e:
            print(f"[warn] Cannot load {p.name}: {e}", file=sys.stderr)
    return results


def print_report(mode: int = 2, fmt: str = "text") -> None:
    results = load_all_results(mode)
    if not results:
        print(f"No results found in {RESULTS_DIR} for mode {mode}")
        return

    if mode == 2:
        _report_mode2(results, fmt)
    else:
        _report_mode1(results, fmt)


def _report_mode2(results: list[dict], fmt: str) -> None:
    """Mode 2: step2+3 accuracy table."""
    fields = STEP2_FIELDS + STEP3_FIELDS
    field_abbrev = {
        "组织标签": "Org",
        "标志位": "Flag",
        "任职地（省）": "Prov",
        "任职地（市）": "City",
        "中央/地方": "C/L",
        "该条行政级别": "Rank",
    }

    rows = []
    for r in results:
        if "error" in r and "agg_step2" not in r:
            continue
        # Skip models with 0% on all fields (API unavailable)
        agg2 = r.get("agg_step2", {})
        if agg2 and all(v.get("accuracy", 0) == 0 for v in agg2.values()):
            continue
        agg3 = r.get("agg_step3", {})
        accs = {}
        for f in STEP2_FIELDS:
            accs[f] = agg2.get(f, {}).get("accuracy", 0.0)
        for f in STEP3_FIELDS:
            accs[f] = agg3.get(f, {}).get("accuracy", 0.0)
        # Episode-level all-correct
        total_correct = 0
        total_ep = 0
        for name, ep_r in r.get("per_official_step2", {}).items():
            if isinstance(ep_r, dict) and "all_fields_correct" in ep_r:
                total_correct += ep_r["all_fields_correct"]["correct"]
                total_ep += ep_r["all_fields_correct"]["total"]
        all_acc = total_correct / total_ep if total_ep > 0 else 0.0

        rows.append({
            "model": r.get("model", r.get("model_id", "?")),
            "accs": accs,
            "all_fields": all_acc,
            "elapsed": r.get("elapsed_s", 0),
            "n_ep": total_ep,
        })

    # Sort by average accuracy
    rows.sort(key=lambda x: -sum(x["accs"].values()) / max(len(x["accs"]), 1))

    if fmt == "text":
        abbrevs = [field_abbrev.get(f, f[:6]) for f in fields]
        header = f"{'Model':<22}" + "".join(f"{a:>7}" for a in abbrevs) + f"{'All':>7}{'Time':>7}"
        print("\n" + "Mode 2 Evaluation Results (step2+3)" )
        print("=" * len(header))
        print(header)
        print("-" * len(header))
        for row in rows:
            line = f"{row['model']:<22}"
            for f in fields:
                line += f"{row['accs'][f]:>7.1%}"
            line += f"{row['all_fields']:>7.1%}"
            line += f"{row['elapsed']:>6.0f}s"
            print(line)
        print("-" * len(header))
        print(f"\nN episodes evaluated: {rows[0]['n_ep'] if rows else '?'}")
        print(f"\nErrors breakdown (top models):")
        _print_top_errors(results, mode=2)

    elif fmt == "csv":
        import csv, io
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(["Model"] + [field_abbrev[f] for f in fields] + ["All_Correct%", "Elapsed_s"])
        for row in rows:
            writer.writerow(
                [row["model"]]
                + [f"{row['accs'][f]*100:.1f}" for f in fields]
                + [f"{row['all_fields']*100:.1f}", f"{row['elapsed']:.0f}"]
            )
        print(buf.getvalue())

    elif fmt == "latex":
        abbrevs = [field_abbrev.get(f, f) for f in fields]
        print(r"\begin{tabular}{l" + "r" * (len(fields) + 2) + "}")
        print(r"\toprule")
        print("Model & " + " & ".join(abbrevs) + r" & All & Time\\" )
        print(r"\midrule")
        for row in rows:
            vals = " & ".join(f"{row['accs'][f]*100:.1f}" for f in fields)
            print(f"{row['model']} & {vals} & {row['all_fields']*100:.1f} & {row['elapsed']:.0f}s \\\\")
        print(r"\bottomrule")
        print(r"\end{tabular}")


def _report_mode1(results: list[dict], fmt: str) -> None:
    """Mode 1: step1+4 accuracy table."""
    rows = []
    for r in results:
        if "error" in r and "agg_step4" not in r:
            continue
        agg4 = r.get("agg_step4", {})
        step1_data = r.get("per_official_step1", {})
        f1s = [v.get("episode_f1", 0.0) for v in step1_data.values()
               if isinstance(v, dict) and "episode_f1" in v]
        avg_f1 = sum(f1s) / len(f1s) if f1s else 0.0
        recalls = [v.get("episode_recall", 0.0) for v in step1_data.values()
                   if isinstance(v, dict) and "episode_recall" in v]
        avg_recall = sum(recalls) / len(recalls) if recalls else 0.0

        rows.append({
            "model": r.get("model", "?"),
            "step4_acc": agg4.get("accuracy", 0.0),
            "step1_f1": avg_f1,
            "step1_recall": avg_recall,
            "elapsed": r.get("elapsed_s", 0),
        })

    rows.sort(key=lambda x: -(x["step4_acc"] + x["step1_f1"]) / 2)

    if fmt == "text":
        header = f"{'Model':<22}{'Step4 Acc':>10}{'Step1 Recall':>13}{'Step1 F1':>10}{'Time':>8}"
        print("\nMode 1 Evaluation Results (step1+4)")
        print("=" * len(header))
        print(header)
        print("-" * len(header))
        for row in rows:
            print(f"{row['model']:<22}"
                  f"{row['step4_acc']:>10.1%}"
                  f"{row['step1_recall']:>13.1%}"
                  f"{row['step1_f1']:>10.1%}"
                  f"{row['elapsed']:>7.0f}s")


def _print_top_errors(results: list[dict], mode: int, top_n: int = 5) -> None:
    """Print most common error types."""
    from collections import Counter
    error_counts: Counter = Counter()
    for r in results:
        for err in r.get("errors", []):
            f = err.get("field", "?")
            g = err.get("gold", "?")
            p = err.get("pred", "?")
            error_counts[(f, g, p)] += 1

    print(f"  Top {top_n} errors across all models:")
    for (f, g, p), cnt in error_counts.most_common(top_n):
        print(f"    [{f}] gold={g!r} pred={p!r}  × {cnt}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", type=int, default=2)
    parser.add_argument("--format", type=str, default="text",
                        choices=["text", "csv", "latex"])
    args = parser.parse_args()
    print_report(args.mode, args.format)
