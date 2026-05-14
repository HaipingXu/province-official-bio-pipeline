"""
run_eval.py — Main eval orchestrator.

Modes:
  Mode 2 (default): Given gold step1 episodes → each model does step2+3
  Mode 1: Raw bio text → each model does step1+4

Usage:
  cd /Users/xuhaiping/Desktop/Workflow省级官员
  uv run eval/run_eval.py --mode 2                        # all models, step2+3
  uv run eval/run_eval.py --mode 2 --model gpt-5.4        # single model
  uv run eval/run_eval.py --mode 1                        # raw text → step1+4
  uv run eval/run_eval.py --mode 2 --model "gpt-5.4,claude-sonnet-4-6"
  uv run eval/run_eval.py --report                        # print report from saved results

Results are saved to: eval/results/{mode}_{model_display_name}.json
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")

import httpx
from openai import OpenAI

from eval.gold import load_gold, load_preprocessed, get_step1_from_gold
from eval.models import get_all_models, get_model_subset, EvalModel
from eval.metrics import (
    compare_step2, compare_step3, compare_step4, compare_step1,
    aggregate_results, aggregate_step4, STEP2_FIELDS, STEP3_FIELDS, STEP1_FIELDS,
)

logger = logging.getLogger(__name__)

RESULTS_DIR = Path(__file__).parent / "results"
RESULTS_DIR.mkdir(exist_ok=True)

# ── Prompt loading ─────────────────────────────────────────────────────────────

def load_prompt(name: str) -> str:
    p = Path(__file__).parent.parent / "prompts" / f"{name}.md"
    return p.read_text(encoding="utf-8").strip()


_PROMPT_CACHE: dict[str, str] = {}

def _prompt(name: str) -> str:
    if name not in _PROMPT_CACHE:
        _PROMPT_CACHE[name] = load_prompt(name)
    return _PROMPT_CACHE[name]


# ── Reference injection (for step2/step3) ─────────────────────────────────────

def _load_ref(name: str) -> str:
    p = Path(__file__).parent.parent / "prompts" / f"{name}.md"
    return p.read_text(encoding="utf-8").strip() if p.exists() else ""


_REF_UNI = _load_ref("ref_university_rank")
_REF_SOE = _load_ref("ref_soe_rank")

_UNI_KWS = ["大学", "学院", "党校", "研究生", "本科", "硕士", "博士", "进修",
             "留学", "教授", "副教授", "讲师", "校长", "高校"]
_SOE_KWS = ["集团", "总公司", "公司", "银行", "保险", "证券", "石油", "石化",
             "电力", "航天", "航空", "兵器", "船舶", "铁路", "钢铁", "国资委", "央企", "国企"]


def _detect_refs(text: str) -> str:
    extras = []
    if any(kw in text for kw in _UNI_KWS):
        extras.append(f"\n\n---\n\n## 附录：高校与党校判定参考\n\n{_REF_UNI}")
    if any(kw in text for kw in _SOE_KWS):
        extras.append(f"\n\n---\n\n## 附录：国有企业判定参考\n\n{_REF_SOE}")
    return "".join(extras)


# ── LLM call (thin wrapper) ───────────────────────────────────────────────────

_LLM_TIMEOUT = httpx.Timeout(connect=15.0, read=180.0, write=30.0, pool=60.0)


def _chat(
    model: EvalModel,
    system: str,
    user: str,
    max_retries: int = 3,
) -> str:
    """Call LLM with retry logic."""
    client = model.client()
    # Kimi-K2.5 only allows temperature=1; other models use 0.1
    temp = 1.0 if "kimi" in model.provider or model.notes == "temperature must be 1" else 0.1
    kwargs: dict = {
        "model": model.model_id,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "temperature": temp,
        "seed": 42,
    }
    if model.max_tokens:
        kwargs["max_tokens"] = model.max_tokens
    if model.extra_body:
        kwargs["extra_body"] = model.extra_body
    if model.response_format:
        kwargs["response_format"] = model.response_format

    for attempt in range(max_retries + 1):
        try:
            resp = client.chat.completions.create(**kwargs)
            content = resp.choices[0].message.content
            if not content:
                raise ValueError("Empty response from LLM")
            return content
        except Exception as e:
            s = str(e).lower()
            is_rate = "429" in s or "rate" in s or "throttl" in s
            if attempt < max_retries:
                wait = (30 * (3 ** attempt)) if is_rate else (5 * (attempt + 1))
                wait = min(wait, 180)
                logger.warning(
                    f"[retry {attempt+1}/{max_retries}] {model.display_name} "
                    f"err={str(e)[:80]} wait={wait:.0f}s"
                )
                time.sleep(wait)
            else:
                raise


def _extract_json(text: str) -> dict:
    """Parse JSON from LLM response, stripping markdown fences."""
    import re, json
    text = re.sub(r"^```(?:json)?\s*", "", text.strip())
    text = re.sub(r"\s*```$", "", text.strip())
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    m = re.search(r"\{.*\}", text, re.DOTALL)
    if m:
        try:
            return json.loads(m.group(0))
        except Exception:
            pass
    raise ValueError(f"Cannot parse JSON from: {text[:200]}")


# ── Mode 2: step2 classify ────────────────────────────────────────────────────

def _build_step2_call(
    model: EvalModel,
    name: str,
    episodes: list[dict],
    offset: int = 0,
) -> list[dict]:
    """Call step2 API for a (sub-)batch of episodes. Returns classifications list."""
    ep_lines = []
    ep_text_for_ref = ""
    for i, ep in enumerate(episodes, 1 + offset):
        sl = ep.get("source_line", ep.get("episode_idx", i))
        line = (f"  #{i}: source_line={sl}  "
                f"起始={ep.get('起始时间','')}  "
                f"终止={ep.get('终止时间','')}  "
                f"供职单位={ep.get('供职单位','')}  "
                f"职务={ep.get('职务','')}")
        ep_lines.append(line)
        ep_text_for_ref += f" {ep.get('供职单位','')} {ep.get('职务','')}"

    ep_text = "\n".join(ep_lines)
    ref_extra = _detect_refs(ep_text_for_ref)
    system = _prompt("step2_classify") + ref_extra
    user = (
        f"官员：{name}\n\n"
        f"=== Episodes (Step1 已固定) ===\n{ep_text}\n\n"
        "请对每条 episode 输出 episode_idx + 组织标签 + 标志位 + 任职地（省）+ 任职地（市）+ 中央/地方，纯JSON。"
    )
    raw = _chat(model, system, user)
    result = _extract_json(raw)
    return result.get("classifications", [])


def run_step2_for_official(
    model: EvalModel,
    name: str,
    gold_episodes: list[dict],
) -> list[dict]:
    """Run step2 classification for one official with one model.

    If model.max_ep_per_call > 0 and n_episodes exceeds it, splits into chunks.
    """
    n = len(gold_episodes)
    chunk = model.max_ep_per_call
    if chunk > 0 and n > chunk:
        # Split into batches and merge
        classifications = []
        for start in range(0, n, chunk):
            batch = gold_episodes[start:start + chunk]
            logger.info(f"    [batch] {model.display_name} {name} ep {start+1}-{start+len(batch)}/{n}")
            classifications.extend(_build_step2_call(model, name, batch, offset=start))
        return classifications
    return _build_step2_call(model, name, gold_episodes, offset=0)


# ── Mode 2: step3 rank ────────────────────────────────────────────────────────

def _build_step3_call(
    model: EvalModel,
    name: str,
    episodes: list[dict],
    offset: int = 0,
) -> list[dict]:
    """Call step3 API for a (sub-)batch of episodes. Returns ranks list."""
    ep_lines = []
    ep_text_for_ref = ""
    for i, ep in enumerate(episodes, 1 + offset):
        line = (f"  {i}. 供职单位: {ep.get('供职单位','')}  "
                f"职务: {ep.get('职务','')}")
        ep_lines.append(line)
        ep_text_for_ref += f" {ep.get('供职单位','')} {ep.get('职务','')}"

    ep_text = "\n".join(ep_lines)
    ref_extra = _detect_refs(ep_text_for_ref)
    system = _prompt("step3_rank") + ref_extra
    user = (
        f"以下是某官员的 {len(episodes)} 段职务经历（序号 {1+offset}–{offset+len(episodes)}，仅凭职务/单位名称判断级别）：\n{ep_text}\n\n"
        "请对每段经历判断行政级别，输出纯JSON。"
    )
    raw = _chat(model, system, user)
    result = _extract_json(raw)
    return result.get("ranks", [])


def run_step3_for_official(
    model: EvalModel,
    name: str,
    gold_episodes: list[dict],
) -> list[dict]:
    """Run step3 rank determination for one official with one model.

    If model.max_ep_per_call > 0 and n_episodes exceeds it, splits into chunks.
    """
    n = len(gold_episodes)
    chunk = model.max_ep_per_call
    if chunk > 0 and n > chunk:
        ranks = []
        for start in range(0, n, chunk):
            batch = gold_episodes[start:start + chunk]
            logger.info(f"    [batch] {model.display_name} {name} rank ep {start+1}-{start+len(batch)}/{n}")
            ranks.extend(_build_step3_call(model, name, batch, offset=start))
        return ranks
    return _build_step3_call(model, name, gold_episodes, offset=0)


# ── Mode 1: step1 extract ─────────────────────────────────────────────────────

def run_step1_for_official(
    model: EvalModel,
    name: str,
    preprocessed: dict,
) -> list[dict]:
    """Run step1 extraction for one official with one model."""
    from text_preprocessor import format_career_lines_for_llm
    career_text = format_career_lines_for_llm(preprocessed["career_lines"])
    total_lines = preprocessed["total_lines"]

    system = _prompt("step1_extraction")
    user = (
        f"官员：{name}\n\n"
        f"=== 编号履历行（共{total_lines}行）===\n{career_text}\n\n"
        "请将每行转化为最小事实条目，仅输出 source_line / 起始时间 / 终止时间 / 供职单位 / 职务 五个字段，纯JSON。"
    )

    raw = _chat(model, system, user)
    result = _extract_json(raw)
    episodes = result.get("episodes", [])
    return episodes


# ── Mode 1: step4 label ───────────────────────────────────────────────────────

def run_step4_for_official(
    model: EvalModel,
    name: str,
    preprocessed: dict,
    province: str,
    official_role: str,
) -> dict:
    """Run step4 bio labeling for one official with one model."""
    from text_preprocessor import format_career_lines_for_llm
    career_text = format_career_lines_for_llm(preprocessed["career_lines"])

    bio_summary = preprocessed.get("bio_summary", "")
    corruption_text = preprocessed.get("corruption_text", "")

    system = _prompt("step4_labeling")
    user = (
        f"官员：{name}，目标省份：{province}，职务：{official_role}\n\n"
        f"=== 完整履历文本 ===\n{career_text}\n\n"
        f"=== 人物简介 ===\n{bio_summary[:1000]}\n\n"
        f"=== 落马相关文本 ===\n{corruption_text[:500] if corruption_text else '（无）'}"
    )

    raw = _chat(model, system, user)
    result = _extract_json(raw)
    return result


# ── Eval Mode 2: step2 + step3 for all officials ──────────────────────────────

def eval_mode2_one_model(
    model: EvalModel,
    gold: dict[str, dict],
    verbose: bool = True,
) -> dict:
    """
    Run Mode 2 (step2+3) evaluation for one model across all officials.
    Returns per-official and aggregated metrics.
    """
    gold_step1 = get_step1_from_gold(gold)
    names = list(gold.keys())
    logger.info(f"[Mode2] {model.display_name} — testing {len(names)} officials")

    per_official_step2: dict[str, dict] = {}
    per_official_step3: dict[str, dict] = {}
    errors_all: list[dict] = []
    t0 = time.time()

    for name in names:
        gold_eps = gold[name]["episodes"]
        step1_eps = gold_step1[name]

        # Step2
        try:
            pred_cls = run_step2_for_official(model, name, step1_eps)
            r2 = compare_step2(gold_eps, pred_cls, name)
            per_official_step2[name] = r2
            errors_all.extend(r2["errors"])
            if verbose:
                n_ep = r2["n_episodes"]
                all_acc = r2["all_fields_correct"]["accuracy"]
                logger.info(f"  [{model.display_name}] {name}: step2 ep={n_ep} all_correct={all_acc:.1%}")
        except Exception as e:
            logger.error(f"  [{model.display_name}] {name} step2 FAILED: {e}")
            per_official_step2[name] = {"error": str(e)}

        # Step3
        try:
            pred_ranks = run_step3_for_official(model, name, step1_eps)
            r3 = compare_step3(gold_eps, pred_ranks, name)
            per_official_step3[name] = r3
            errors_all.extend(r3["errors"])
            if verbose:
                acc = r3["all_fields_correct"]["accuracy"]
                logger.info(f"  [{model.display_name}] {name}: step3 acc={acc:.1%}")
        except Exception as e:
            logger.error(f"  [{model.display_name}] {name} step3 FAILED: {e}")
            per_official_step3[name] = {"error": str(e)}

    elapsed = time.time() - t0

    # Aggregate
    agg_step2 = aggregate_results(per_official_step2, STEP2_FIELDS)
    agg_step3 = aggregate_results(per_official_step3, STEP3_FIELDS)

    result = {
        "model": model.display_name,
        "model_id": model.model_id,
        "mode": 2,
        "elapsed_s": round(elapsed, 1),
        "per_official_step2": per_official_step2,
        "per_official_step3": per_official_step3,
        "agg_step2": agg_step2,
        "agg_step3": agg_step3,
        "errors": errors_all,
    }

    # Print summary
    logger.info(f"\n{'='*60}")
    logger.info(f"[Mode2 SUMMARY] {model.display_name} ({elapsed:.0f}s)")
    logger.info(f"  Step2 accuracy by field:")
    for f, s in agg_step2.items():
        logger.info(f"    {f:15s}: {s['accuracy']:.1%}  ({s['correct']}/{s['total']})")
    logger.info(f"  Step3 该条行政级别: {agg_step3.get('该条行政级别', {}).get('accuracy', 0):.1%}")

    return result


# ── Eval Mode 1: step1 + step4 for all officials ──────────────────────────────

def eval_mode1_one_model(
    model: EvalModel,
    gold: dict[str, dict],
    preprocessed: dict[str, dict],
    verbose: bool = True,
) -> dict:
    """
    Run Mode 1 (step1+4) evaluation for one model across all officials.
    """
    names = list(gold.keys())
    logger.info(f"[Mode1] {model.display_name} — testing {len(names)} officials")

    per_official_step1: dict[str, dict] = {}
    per_official_step4: dict[str, dict] = {}
    errors_all: list[dict] = []
    t0 = time.time()

    for name in names:
        gold_eps = gold[name]["episodes"]
        gold_person = gold[name]["person"]
        pp = preprocessed.get(name, {})
        if not pp:
            logger.warning(f"  {name}: no preprocessed text, skipping")
            continue

        # Step1
        try:
            pred_eps = run_step1_for_official(model, name, pp)
            r1 = compare_step1(gold_eps, pred_eps, name)
            per_official_step1[name] = r1
            errors_all.extend(r1["errors"])
            if verbose:
                logger.info(f"  [{model.display_name}] {name}: step1 "
                            f"recall={r1['episode_recall']:.1%} "
                            f"f1={r1['episode_f1']:.1%}")
        except Exception as e:
            logger.error(f"  [{model.display_name}] {name} step1 FAILED: {e}")
            per_official_step1[name] = {"error": str(e)}

        # Step4
        # Determine province/role from test5 data
        _OFFICIAL_ROLES = {
            "王兆国": ("福建省", "省长"),
            "习近平": ("福建省", "省长"),
            "赵龙":   ("福建省", "省长"),
            "孙春兰": ("福建省", "省委书记"),
            "周祖翼": ("福建省", "省委书记"),
        }
        province, official_role = _OFFICIAL_ROLES.get(name, ("福建省", "省级官员"))
        try:
            pred_labels = run_step4_for_official(model, name, pp, province, official_role)
            r4 = compare_step4(gold_person, pred_labels, name)
            per_official_step4[name] = r4
            errors_all.extend(r4["errors"])
            if verbose:
                logger.info(f"  [{model.display_name}] {name}: step4 acc={r4['accuracy']:.1%}")
        except Exception as e:
            logger.error(f"  [{model.display_name}] {name} step4 FAILED: {e}")
            per_official_step4[name] = {"error": str(e)}

    elapsed = time.time() - t0
    agg_step4 = aggregate_step4(per_official_step4)

    result = {
        "model": model.display_name,
        "model_id": model.model_id,
        "mode": 1,
        "elapsed_s": round(elapsed, 1),
        "per_official_step1": per_official_step1,
        "per_official_step4": per_official_step4,
        "agg_step4": agg_step4,
        "errors": errors_all,
    }

    logger.info(f"\n{'='*60}")
    logger.info(f"[Mode1 SUMMARY] {model.display_name} ({elapsed:.0f}s)")
    logger.info(f"  Step4 acc: {agg_step4['accuracy']:.1%} ({agg_step4['correct']}/{agg_step4['total']})")

    return result


# ── Main orchestrator ─────────────────────────────────────────────────────────

def run_all_models_concurrent(
    models: list[EvalModel],
    mode: int,
    gold: dict[str, dict],
    preprocessed: dict[str, dict] | None = None,
    max_workers: int = 6,
) -> list[dict]:
    """Run all models concurrently (model-level parallelism)."""
    results = []

    def _run_one(model: EvalModel) -> dict:
        result_path = RESULTS_DIR / f"mode{mode}_{model.display_name.replace('/', '-')}.json"
        if result_path.exists():
            logger.info(f"[skip] {model.display_name}: cached result at {result_path.name}")
            return json.loads(result_path.read_text(encoding="utf-8"))

        try:
            if mode == 2:
                res = eval_mode2_one_model(model, gold)
            else:
                res = eval_mode1_one_model(model, gold, preprocessed or {})

            result_path.write_text(
                json.dumps(res, ensure_ascii=False, indent=2),
                encoding="utf-8"
            )
            logger.info(f"[saved] {result_path.name}")
            return res
        except Exception as e:
            logger.error(f"[FAIL] {model.display_name}: {e}")
            return {"model": model.display_name, "error": str(e)}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_run_one, m): m for m in models}
        for fut in as_completed(futures):
            model = futures[fut]
            try:
                result = fut.result()
                results.append(result)
            except Exception as e:
                logger.error(f"[FUTURE ERROR] {model.display_name}: {e}")

    return results


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Eval framework for provincial official biography pipeline")
    parser.add_argument("--mode", type=int, choices=[1, 2], default=2,
                        help="1=raw text→step1+4, 2=given step1→step2+3")
    parser.add_argument("--model", type=str, default="",
                        help="Comma-separated model display_names or model_ids to test")
    parser.add_argument("--workers", type=int, default=6,
                        help="Max concurrent models")
    parser.add_argument("--report", action="store_true",
                        help="Only print report from saved results, no new API calls")
    parser.add_argument("--force", action="store_true",
                        help="Ignore cached results, re-run all models")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
        ],
    )

    # Load gold data
    logger.info("Loading ground truth from test5_officials.xlsx...")
    gold = load_gold()
    logger.info(f"  Officials: {list(gold.keys())}")
    total_eps = sum(len(r["episodes"]) for r in gold.values())
    logger.info(f"  Total gold episodes: {total_eps}")

    if args.report:
        from eval.report import print_report
        print_report(args.mode)
        return

    # Load preprocessed texts for Mode 1
    preprocessed = None
    if args.mode == 1:
        logger.info("Loading preprocessed texts for Mode 1...")
        preprocessed = load_preprocessed()

    # Select models
    if args.model:
        model_names = [m.strip() for m in args.model.split(",")]
        models = get_model_subset(model_names)
        if not models:
            # Fall back to looking up by partial match
            all_m = get_all_models()
            models = [m for m in all_m
                      if any(n.lower() in m.model_id.lower() or n.lower() in m.display_name.lower()
                             for n in model_names)]
        if not models:
            logger.error(f"No models found matching: {args.model}")
            sys.exit(1)
    else:
        models = get_all_models()

    if args.force:
        # Delete cached results
        for m in models:
            result_path = RESULTS_DIR / f"mode{args.mode}_{m.display_name.replace('/', '-')}.json"
            if result_path.exists():
                result_path.unlink()
                logger.info(f"  [force] Deleted cache: {result_path.name}")

    logger.info(f"\nRunning Mode {args.mode} eval with {len(models)} models:")
    for m in models:
        logger.info(f"  {m.display_name} ({m.model_id})")

    # Run eval
    t_start = time.time()
    results = run_all_models_concurrent(
        models, args.mode, gold, preprocessed, max_workers=args.workers
    )
    elapsed = time.time() - t_start

    # Print summary report
    logger.info(f"\n{'='*70}")
    logger.info(f"EVAL MODE {args.mode} COMPLETE — {len(results)} models, {elapsed:.0f}s total")
    logger.info("="*70)

    if args.mode == 2:
        _print_mode2_summary(results)
    else:
        _print_mode1_summary(results)


def _print_mode2_summary(results: list[dict]) -> None:
    """Print Mode 2 summary table."""
    FIELDS_ORDER = ["组织标签", "标志位", "任职地（省）", "任职地（市）", "中央/地方", "该条行政级别"]
    HEADER = f"{'Model':<22}" + "".join(f"{f:>8}" for f in FIELDS_ORDER) + f"{'Time':>8}"
    print("\n" + HEADER)
    print("-" * len(HEADER))

    for r in sorted(results, key=lambda x: x.get("model", "")):
        if "error" in r and "agg_step2" not in r:
            print(f"{r['model']:<22} ERROR: {r['error']}")
            continue
        agg2 = r.get("agg_step2", {})
        agg3 = r.get("agg_step3", {})
        row = f"{r['model']:<22}"
        for f in FIELDS_ORDER[:5]:
            acc = agg2.get(f, {}).get("accuracy", 0.0)
            row += f"{acc:>8.1%}"
        # Step3
        acc3 = agg3.get("该条行政级别", {}).get("accuracy", 0.0)
        row += f"{acc3:>8.1%}"
        row += f"{r.get('elapsed_s', 0):>8.0f}s"
        print(row)


def _print_mode1_summary(results: list[dict]) -> None:
    """Print Mode 1 summary table."""
    HEADER = f"{'Model':<22}{'Step4 Acc':>10}{'Step1 F1':>10}{'Time':>8}"
    print("\n" + HEADER)
    print("-" * len(HEADER))

    for r in sorted(results, key=lambda x: x.get("model", "")):
        if "error" in r and "agg_step4" not in r:
            print(f"{r['model']:<22} ERROR: {r['error']}")
            continue
        agg4 = r.get("agg_step4", {})
        acc4 = agg4.get("accuracy", 0.0)
        # Compute avg step1 F1
        step1_results = r.get("per_official_step1", {})
        f1_scores = [v.get("episode_f1", 0.0) for v in step1_results.values()
                     if isinstance(v, dict) and "episode_f1" in v]
        avg_f1 = sum(f1_scores) / len(f1_scores) if f1_scores else 0.0
        print(f"{r['model']:<22}{acc4:>10.1%}{avg_f1:>10.1%}{r.get('elapsed_s', 0):>8.0f}s")


if __name__ == "__main__":
    main()
