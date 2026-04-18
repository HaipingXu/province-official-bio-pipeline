"""
Main Orchestrator — Province Level

Runs the full pipeline for province-level officials (省长/主席 + 省委书记):
  Phase 0 : Parse province officials list
  Phase 0.5: Bio files (scrape or existing)
  Phase 1 : Step1 extraction (LLM1+LLM2 parallel) → diff → judge → merged_episodes.json
  Phase 2 : Step2 rank (LLM1+LLM2 parallel on merged episodes) → diff → judge
  Phase 3 : Step3 labels (LLM1+LLM2 parallel on merged episodes) → diff → judge
  Phase 4 : Post-processing / flat rows (postprocess.py)
  Phase 5 : Excel export (export.py)

Usage:
  python main_province.py --province 安徽
  python main_province.py --province 北京 --start 2000
  python main_province.py --province 安徽 --official 王清宪
  python main_province.py --batch
  python main_province.py --province 安徽 --force
"""

import argparse
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Optional


class PipelineError(RuntimeError):
    """Fatal pipeline error — replaces sys.exit(1) for testability."""
    pass

from config import (
    DATA_DIR, LLM1_MAX_WORKERS, LLM2_MAX_WORKERS, JUDGE_MAX_WORKERS,
    LLM1_API_KEY, LLM1_API_KEYS, LLM1_BASE_URL, LLM1_MODEL,
    LLM2_API_KEY, LLM2_API_KEYS, LLM2_BASE_URL, LLM2_MODEL,
    JUDGE_API_KEY, JUDGE_API_KEYS, JUDGE_BASE_URL, JUDGE_MODEL,
    LOGS_DIR, OFFICIALS_DIR, OUTPUT_DIR, PROJECT_ROOT,
    PROVINCE_NAMES,
    setup_logging, validate_api_keys,
)
from utils import TOKENS, RoundRobinClientPool, LLMConfig

# All 31 provinces (ordered list from config.PROVINCE_NAMES set)
ALL_PROVINCES = sorted(PROVINCE_NAMES, key=lambda p: [
    "北京", "天津", "河北", "山西", "内蒙古",
    "辽宁", "吉林", "黑龙江",
    "上海", "江苏", "浙江", "安徽", "福建", "江西", "山东",
    "河南", "湖北", "湖南", "广东", "广西", "海南",
    "重庆", "四川", "贵州", "云南", "西藏",
    "陕西", "甘肃", "青海", "宁夏", "新疆",
].index(p))


# ── Per-province directory setup ──────────────────────────────────────────────

def get_province_dirs(province: str) -> dict:
    """Return province-specific directory paths, creating them if needed."""
    dirs = {
        "officials": OFFICIALS_DIR / province,
        "logs": LOGS_DIR / province,
        "output": OUTPUT_DIR / province,
    }
    for d in dirs.values():
        d.mkdir(parents=True, exist_ok=True)
    return dirs


# ── Banner ────────────────────────────────────────────────────────────────────

def print_banner(province: str, province_full: str, start_year: int | None):
    print("=" * 60)
    print("  省级官员履历数据库构建系统  v1")
    start_str = f"起始年份：{start_year}" if start_year else "全部年份"
    print(f"  省份：{province_full} | {start_str}")
    print("=" * 60)


# ── Phase 0: officials list ──────────────────────────────────────────────────

def load_officials(
    province: str,
    single_official: str = "",
    start_year: int | None = None,
    data_subdir: str = "",
) -> dict:
    """Load the province officials list from data/[subdir/]{province}_officials.txt."""
    from input_parser_province import parse_province_officials_txt

    if data_subdir:
        txt_path = DATA_DIR / data_subdir / f"{province}_officials.txt"
    else:
        txt_path = DATA_DIR / f"{province}_officials.txt"
    if not txt_path.exists():
        raise PipelineError(f"名单文件不存在: {txt_path}  请运行 python generate_province_lists.py 生成")

    parsed = parse_province_officials_txt(province, data_dir=txt_path.parent)

    # Override start_year from CLI if provided (keep end >= start_year)
    if start_year:
        def active_after(entry):
            end = entry.get("end", "至今")
            if end in ("至今", "present", "现任", ""):
                return True
            try:
                y = int(end.split(".")[0])
                return y >= start_year
            except Exception:
                return True
        parsed["all_officials"] = [o for o in parsed["all_officials"] if active_after(o)]
        parsed["governors"] = [o for o in parsed["governors"] if active_after(o)]
        parsed["secretaries"] = [o for o in parsed["secretaries"] if active_after(o)]

    if single_official:
        all_offs = [o for o in parsed["all_officials"] if o["name"] == single_official]
        if not all_offs:
            raise PipelineError(f"名单中未找到官员: {single_official}")
        parsed["all_officials"] = all_offs
        print(f"\n[单官员模式] {single_official}")
    else:
        gov_title = parsed["gov_title"]
        sec_title = parsed["sec_title"]
        print(f"\n✓ 名单加载完成: {len(parsed['all_officials'])} 人")
        print(f"  {gov_title}：{len(parsed['governors'])}人  {sec_title}：{len(parsed['secretaries'])}人")

    return parsed


# ── Phase 0.5: scraping ─────────────────────────────────────────────────────

def run_scraping(officials: list[dict], province: str, force: bool = False,
                 max_workers: int = 2,
                 playwright_first: bool = False) -> list[Path]:
    """Scrape biographies and return list of saved biography files."""
    from code_scrape.bio_scraper_v2 import scrape_all

    prov_dir = OFFICIALS_DIR / province
    prov_dir.mkdir(parents=True, exist_ok=True)

    result = scrape_all(
        officials, province, force=force, max_workers=max_workers,
        output_dir=prov_dir, playwright_first=playwright_first,
    )
    existing = list(prov_dir.glob("*_biography.txt"))

    if result.get("fail_count", 0) > 0:
        print(f"\n  ⚠ {result['fail_count']} 人爬取失败")
        print(f"  请手动保存到 officials/{province}/{{姓名}}_biography.txt")

    if not existing:
        raise PipelineError(f"无可用履历文本 ({prov_dir})")

    return existing


# ── LLM config builders ────────────────────────────────────────────────────

def _build_llm1_config() -> LLMConfig:
    pool = RoundRobinClientPool(LLM1_API_KEYS or [LLM1_API_KEY], LLM1_BASE_URL)
    return LLMConfig(pool=pool, model=LLM1_MODEL, max_retries=4, source_tag="llm1")


def _build_llm2_config() -> LLMConfig:
    pool = RoundRobinClientPool(LLM2_API_KEYS or [LLM2_API_KEY], LLM2_BASE_URL)
    return LLMConfig(pool=pool, model=LLM2_MODEL, max_retries=1, source_tag="llm2")


def _build_judge_pool() -> tuple[RoundRobinClientPool, str]:
    """Build a RoundRobinClientPool + model string for the judge LLM."""
    judge_keys = JUDGE_API_KEYS if JUDGE_API_KEYS else ([JUDGE_API_KEY] if JUDGE_API_KEY else [])
    pool = RoundRobinClientPool(judge_keys, JUDGE_BASE_URL)
    return pool, JUDGE_MODEL


# ── Interleaved extract-diff-judge phases ──────────────────────────────────

def _run_phase_step1(officials, province, province_full, dirs, force):
    """Phase 1: Step1 extraction → diff → judge → merged_episodes.json"""
    from extraction import run_step1
    from diff import diff_step1
    from judge import judge_step1

    prov_logs = dirs["logs"]
    officials_dir = dirs["officials"]
    cfg1 = _build_llm1_config()
    cfg2 = _build_llm2_config()

    print(f"\n★ Phase 1: Step1 提取 (LLM1 + LLM2 并行)")
    with ThreadPoolExecutor(max_workers=2) as pool:
        f1 = pool.submit(
            run_step1, officials, province, province_full,
            prov_logs / "llm1_step1_results.json", cfg1,
            force=force, officials_dir=officials_dir,
        )
        f2 = pool.submit(
            run_step1, officials, province, province_full,
            prov_logs / "llm2_step1_results.json", cfg2,
            force=force, officials_dir=officials_dir,
        )
        f1.result()
        f2.result()

    diff_step1(prov_logs)
    judge_pool, judge_model = _build_judge_pool()
    judge_step1(prov_logs, officials_dir=officials_dir, force=force,
                max_workers=JUDGE_MAX_WORKERS, pool=judge_pool, model=judge_model)


def _run_phase_step2(officials, province, dirs, force):
    """Phase 2: Step2 rank → diff → judge"""
    from extraction import run_step2
    from diff import diff_step2
    from judge import judge_step2

    prov_logs = dirs["logs"]
    merged_path = prov_logs / "merged_episodes.json"

    if not merged_path.exists():
        print(f"\n⚠ 跳过 Phase 2: merged_episodes.json 不存在")
        return

    cfg1 = _build_llm1_config()
    cfg2 = _build_llm2_config()

    print(f"\n★ Phase 2: Step2 级别判断 (LLM1 + LLM2 并行)")
    with ThreadPoolExecutor(max_workers=2) as pool:
        f1 = pool.submit(
            run_step2, officials, merged_path,
            prov_logs / "llm1_step2_rank.json", cfg1,
            force=force,
        )
        f2 = pool.submit(
            run_step2, officials, merged_path,
            prov_logs / "llm2_step2_rank.json", cfg2,
            force=force,
        )
        f1.result()
        f2.result()

    diff_step2(prov_logs)
    judge_pool, judge_model = _build_judge_pool()
    judge_step2(prov_logs, force=force, max_workers=JUDGE_MAX_WORKERS,
                pool=judge_pool, model=judge_model)


def _run_phase_step3(officials, province, province_full, dirs, force):
    """Phase 3: Step3 labels → diff → judge"""
    from extraction import run_step3
    from diff import diff_step3
    from judge import judge_step3

    prov_logs = dirs["logs"]
    merged_path = prov_logs / "merged_episodes.json"
    officials_dir = dirs["officials"]

    if not merged_path.exists():
        print(f"\n⚠ 跳过 Phase 3: merged_episodes.json 不存在")
        return

    cfg1 = _build_llm1_config()
    cfg2 = _build_llm2_config()

    print(f"\n★ Phase 3: Step3 标签 (LLM1 + LLM2 并行)")
    with ThreadPoolExecutor(max_workers=2) as pool:
        f1 = pool.submit(
            run_step3, officials, merged_path,
            province, province_full,
            prov_logs / "llm1_step3_labels.json", cfg1,
            force=force, officials_dir=officials_dir,
        )
        f2 = pool.submit(
            run_step3, officials, merged_path,
            province, province_full,
            prov_logs / "llm2_step3_labels.json", cfg2,
            force=force, officials_dir=officials_dir,
        )
        f1.result()
        f2.result()

    diff_step3(prov_logs)
    judge_pool, judge_model = _build_judge_pool()
    judge_step3(prov_logs, force=force, max_workers=JUDGE_MAX_WORKERS,
                pool=judge_pool, model=judge_model)


# ── Phase 4: Post-processing ────────────────────────────────────────────────

def run_postprocess(province: str, province_full: str, start_year: int | None) -> list[dict]:
    from postprocess import run_postprocess as _pp

    prov_logs = LOGS_DIR / province
    return _pp(
        city=province,
        province=province_full,
        start_year=start_year or 1949,
        output_path=prov_logs / "final_rows.json",
        logs_dir=prov_logs,
        officials_dir=OFFICIALS_DIR / province,
    )


# ── Phase 5: Export ──────────────────────────────────────────────────────────

def run_export(province: str, province_full: str, data_subdir: str = "") -> dict:
    from export import run_export as _exp

    if data_subdir:
        txt_path = DATA_DIR / data_subdir / f"{province}_officials.txt"
    else:
        txt_path = DATA_DIR / f"{province}_officials.txt"

    return _exp(
        province=province,
        final_rows_path=LOGS_DIR / province / "final_rows.json",
        output_dir=OUTPUT_DIR / province,
        officials_txt_path=txt_path,
    )


# ── Single-province pipeline ────────────────────────────────────────────────

def run_province_pipeline(
    province: str,
    start_year: int | None = None,
    single_official: str = "",
    skip_scrape: bool = True,
    skip_extract: bool = False,
    skip_battle: bool = False,
    force: bool = False,
    playwright_first: bool = False,
    data_subdir: str = "",
):
    t0 = time.time()
    phase_log: list[dict] = []

    def _phase_end(label: str, t_start: float, tok_before: dict) -> None:
        elapsed = time.time() - t_start
        tok_after = TOKENS.snapshot()
        delta = TOKENS.delta(tok_before, tok_after)
        phase_log.append({"phase": label, "elapsed": elapsed, "tokens": delta})
        tok_str = TOKENS.summary_str(delta)
        print(f"  ⏱  {label}: {elapsed:.1f}s | {tok_str}")

    # ── Phase 0 ──
    t_ph = time.time(); tok_ph = TOKENS.snapshot()
    parsed = load_officials(province, single_official=single_official, start_year=start_year,
                            data_subdir=data_subdir)
    officials = parsed["all_officials"]
    province_full = parsed["province"]
    gov_title = parsed["gov_title"]
    sec_title = parsed["sec_title"]
    _phase_end("Phase 0 (Load)", t_ph, tok_ph)

    print_banner(province, province_full, start_year)
    dirs = get_province_dirs(province)

    # ── Phase 0.5: Bio files ──
    t_ph = time.time(); tok_ph = TOKENS.snapshot()
    if skip_scrape:
        bio_files = list(dirs["officials"].glob("*_biography.txt"))
        if not bio_files:
            bio_files = list(OFFICIALS_DIR.glob("*_biography.txt"))
        print(f"\n[Phase 0.5] 使用现有文本: {len(bio_files)} 个 biography 文件")
        if not bio_files:
            raise PipelineError("无可用履历文件，请将 biography.txt 文件放入: officials/{}/".format(province))
    else:
        bio_files = run_scraping(officials, province, force=force,
                                 max_workers=2,
                                 playwright_first=playwright_first)
    _phase_end("Phase 0.5 (Bio files)", t_ph, tok_ph)

    if not skip_extract:
        # ── Phase 1: Step1 extraction → diff → judge ──
        t_ph = time.time(); tok_ph = TOKENS.snapshot()
        _run_phase_step1(officials, province, province_full, dirs, force)
        _phase_end("Phase 1 (Step1 extract+diff+judge)", t_ph, tok_ph)

        if not skip_battle:
            # ── Phase 2: Step2 rank → diff → judge ──
            t_ph = time.time(); tok_ph = TOKENS.snapshot()
            _run_phase_step2(officials, province, dirs, force)
            _phase_end("Phase 2 (Step2 rank+diff+judge)", t_ph, tok_ph)

            # ── Phase 3: Step3 labels → diff → judge ──
            t_ph = time.time(); tok_ph = TOKENS.snapshot()
            _run_phase_step3(officials, province, province_full, dirs, force)
            _phase_end("Phase 3 (Step3 labels+diff+judge)", t_ph, tok_ph)
        else:
            print(f"\n[跳过] Phase 2-3: Battle/Judge")
    else:
        print(f"\n[跳过] Phase 1-3: 使用现有结果")

    # ── Phase 4 ──
    t_ph = time.time(); tok_ph = TOKENS.snapshot()
    rows = run_postprocess(province, province_full, start_year)
    if not rows:
        raise PipelineError("后处理结果为空")
    _phase_end("Phase 4 (Postprocess)", t_ph, tok_ph)

    # ── Phase 5 ──
    t_ph = time.time(); tok_ph = TOKENS.snapshot()
    export_result = run_export(province, province_full, data_subdir=data_subdir)
    _phase_end("Phase 5 (Export)", t_ph, tok_ph)

    # ── Summary ──
    elapsed = time.time() - t0
    total_snap = TOKENS.snapshot()
    print("\n" + "=" * 60)
    print(f"  Pipeline 完成 — {province_full}")
    print(f"  总耗时: {elapsed:.1f}s")
    print(f"  官员数: {len(officials)}")
    print(f"  总行数: {len(rows)}")
    print(f"  {gov_title}: {len(parsed['governors'])}人")
    print(f"  {sec_title}: {len(parsed['secretaries'])}人")
    print("\n  ── 各阶段用时与Token ──")
    for ph in phase_log:
        tok_str = TOKENS.summary_str(ph["tokens"]) if ph["tokens"] else "0 calls"
        print(f"  {ph['phase']:<40} {ph['elapsed']:6.1f}s | {tok_str}")
    print("\n  ── Token总计 ──")
    for model, v in total_snap.items():
        short = model.split("/")[-1][:40]
        print(f"  {short:<40} in={v['input']:>8,} out={v['output']:>8,} calls={v['calls']:>4}")
    print("\n  输出文件:")
    for label, path in (export_result or {}).items():
        print(f"    [{label}] {Path(str(path)).name}")
    print("=" * 60)

    return export_result


# ── Batch mode: all 31 provinces ─────────────────────────────────────────────

def run_batch(
    provinces: list[str] | None = None,
    start_year: int | None = None,
    skip_scrape: bool = True,
    skip_extract: bool = False,
    skip_battle: bool = False,
    force: bool = False,
    data_subdir: str = "1990",
):
    """Run pipeline for multiple provinces sequentially."""
    provinces = provinces or ALL_PROVINCES
    t0 = time.time()

    print("=" * 60)
    print(f"  省级官员批量处理 — {len(provinces)} 个省份")
    print("=" * 60)

    results = {}
    failed = []

    for i, prov in enumerate(provinces, 1):
        print(f"\n{'─' * 60}")
        print(f"  [{i}/{len(provinces)}] {prov}")
        print(f"{'─' * 60}")

        try:
            result = run_province_pipeline(
                province=prov,
                start_year=start_year,
                skip_scrape=skip_scrape,
                skip_extract=skip_extract,
                skip_battle=skip_battle,
                force=force,
                data_subdir=data_subdir,
            )
            results[prov] = result
        except PipelineError as e:
            print(f"\n✗ {prov} 失败: {e}")
            failed.append((prov, str(e)))
        except Exception as e:
            print(f"\n✗ {prov} 失败: {e}")
            failed.append((prov, str(e)))

    # ── Batch summary ──
    elapsed = time.time() - t0
    print("\n" + "=" * 60)
    print(f"  批量处理完成")
    print(f"  成功: {len(results)}/{len(provinces)} 省份")
    if failed:
        print(f"  失败: {len(failed)} 省份")
        for prov, err in failed:
            print(f"    ✗ {prov}: {err}")
    print(f"  总耗时: {elapsed:.1f}s ({elapsed/60:.1f}min)")
    print("=" * 60)

    return results


# ── CLI ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="省级官员履历数据库构建系统",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python main_province.py --province 安徽            # 单省份
  python main_province.py --province 北京 --start 2000  # 限定起始年份
  python main_province.py --official 王清宪 --province 安徽  # 单官员
  python main_province.py --batch                      # 全部31省
  python main_province.py --batch --provinces 安徽,河北,广东  # 指定多省
  python main_province.py --province 安徽 --skip-scrape    # 跳过爬取
  python main_province.py --province 安徽 --force          # 强制重跑
        """,
    )
    parser.add_argument("--province", default="", help="省份简称（如 安徽、北京）")
    parser.add_argument("--provinces", default="", help="逗号分隔的多省份列表（batch模式）")
    parser.add_argument("--batch", action="store_true", help="批量处理所有31省")
    parser.add_argument("--start", type=int, default=None, help="起始年份（可选，不填=全部）")
    parser.add_argument("--official", default="", help="单官员姓名（调试用）")
    parser.add_argument("--scrape", action="store_true",
                        help="启用爬虫（默认关闭：pipeline 使用现有 biography 文本）")
    parser.add_argument("--skip-extract", action="store_true", help="跳过LLM提取（Phase 1-3全部跳过）")
    parser.add_argument("--skip-battle", action="store_true", help="跳过diff+judge（仅运行LLM1+LLM2 step1提取）")
    parser.add_argument("--force", action="store_true", help="强制重新处理")
    parser.add_argument("--playwright-first", action="store_true", help="Playwright为首选爬取方式（百度百科专用）")
    parser.add_argument("--data-subdir", default="1990", help="数据子目录（默认 1990）→ 从 data/1990/ 读取名单；传空字符串则用 data/ 根目录")
    args = parser.parse_args()

    # Validate: must specify either --province or --batch
    if not args.province and not args.batch and not args.provinces:
        parser.print_help()
        print("\n✗ 请指定 --province 或 --batch")
        sys.exit(1)

    # Init
    setup_logging()
    skip_all_llm = args.skip_extract
    if not skip_all_llm:
        validate_api_keys(require_judge=not args.skip_battle)

    if args.batch or args.provinces:
        provinces = None
        if args.provinces:
            provinces = [p.strip() for p in args.provinces.split(",") if p.strip()]
        run_batch(
            provinces=provinces,
            start_year=args.start,
            skip_scrape=not args.scrape,
            skip_extract=args.skip_extract,
            skip_battle=args.skip_battle,
            force=args.force,
            data_subdir=args.data_subdir,
        )
    else:
        run_province_pipeline(
            province=args.province,
            start_year=args.start,
            single_official=args.official,
            skip_scrape=not args.scrape,
            skip_extract=args.skip_extract,
            skip_battle=args.skip_battle,
            force=args.force,
            playwright_first=args.playwright_first,
            data_subdir=args.data_subdir,
        )
