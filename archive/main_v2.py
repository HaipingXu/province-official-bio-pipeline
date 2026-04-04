"""
Main Orchestrator — v2

Runs the full v2 pipeline:
  Phase 0 : Parse manual officials list (input_parser.py)
  Phase 1 : Scrape biographies — 3-layer strategy (bio_scraper_v2.py)
  Phase 2 : DeepSeek two-step extraction (api_processor_v2.py)
  Phase 3a: Qwen independent verification + diff report (verifier_v2.py)
  Phase 3b: Battle table + Kimi K2.5 judge (battle_generator.py)
  Phase 4 : Post-processing / flat rows (postprocess_v2.py)
  Phase 5 : Excel export — all / mayors / secretaries (export_v2.py)

Usage:
  python main_v2.py --city 深圳 --province 广东 --start 2010
  python main_v2.py --official 许勤          # single official
  python main_v2.py --skip-scrape            # reuse existing biography files
  python main_v2.py --skip-extract           # reuse existing LLM results
  python main_v2.py --skip-battle            # skip judge / battle table
  python main_v2.py --force                  # reprocess everything
"""

import argparse
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from config import DATA_DIR, LOGS_DIR, OFFICIALS_DIR, OUTPUT_DIR, setup_logging, validate_api_keys

# Ensure all directories exist
for _d in [LOGS_DIR, OFFICIALS_DIR, OUTPUT_DIR, DATA_DIR]:
    _d.mkdir(parents=True, exist_ok=True)


# ── Banner ─────────────────────────────────────────────────────────────────────

def print_banner(city: str, province: str, start_year: int):
    print("=" * 60)
    print("  城市官员履历数据库构建系统  v2")
    print(f"  城市：{province}{city}市 | 起始年份：{start_year}")
    print("=" * 60)


# ── Phase 0: officials list ────────────────────────────────────────────────────

def load_officials(
    city: str,
    single_official: str = "",
    check_wiki: bool = False,
) -> dict:
    """
    Load the manual officials list from data/{city}_officials.txt.
    Returns the full parsed dict (mayors, secretaries, all_officials, etc.).
    """
    from input_parser import parse_officials_txt, compare_with_wiki

    txt_path = DATA_DIR / f"{city}_officials.txt"
    if not txt_path.exists():
        print(f"\n✗ 名单文件不存在: {txt_path}")
        print(f"  请创建 data/{city}_officials.txt（参考 data/深圳_officials.txt）")
        sys.exit(1)

    parsed = parse_officials_txt(city)

    if single_official:
        # Filter to just this one person
        all_offs = [o for o in parsed["all_officials"] if o["name"] == single_official]
        if not all_offs:
            print(f"\n✗ 名单中未找到官员: {single_official}")
            sys.exit(1)
        parsed["all_officials"] = all_offs
        print(f"\n[单官员模式] {single_official}")
    else:
        print(f"\n✓ 名单加载完成: {len(parsed['all_officials'])} 人")
        mayors_n = len(parsed.get("mayors", []))
        secs_n   = len(parsed.get("secretaries", []))
        print(f"  市长：{mayors_n}人  书记：{secs_n}人")

    if check_wiki and not single_official:
        # FIX 4b: compare_with_wiki returns list[str], not dict
        warnings = compare_with_wiki(parsed)
        for w in warnings:
            print(f"  {w}")

    return parsed


# ── Phase 1: scraping ──────────────────────────────────────────────────────────

def run_scraping(officials: list[dict], city: str, force: bool = False,
                 max_workers: int = 2, province: str = "") -> list[Path]:
    """Scrape biographies and return list of saved biography files."""
    from bio_scraper_v2 import scrape_all

    result = scrape_all(officials, city, force=force, max_workers=max_workers,
                        province=province)
    existing = list(OFFICIALS_DIR.glob("*_biography.txt"))

    if result.get("fail_count", 0) > 0:
        print(f"\n  ⚠ {result['fail_count']} 人爬取失败")
        print("  请手动从百度百科复制全文并保存到 officials/{姓名}_biography.txt")

    if not existing:
        print("✗ 无可用履历文本，请手动提供 officials/*.txt 文件")
        sys.exit(1)

    return existing


# ── Phase 2: DeepSeek extraction ──────────────────────────────────────────────

def run_extraction(officials: list[dict], city: str, province: str,
                   force: bool = False, max_workers: int = 3) -> tuple[Path, Path]:
    """Run DeepSeek three-step extraction. Returns (step1_path, step2_path)."""
    from api_processor_v2 import process_all_officials

    step1_path = LOGS_DIR / "deepseek_step1_results.json"
    step2_path = LOGS_DIR / "deepseek_step2_labels.json"
    step3_path = LOGS_DIR / "deepseek_step3_rank.json"

    process_all_officials(
        city=city,
        province=province,
        officials_meta=officials,
        step1_path=step1_path,
        step2_path=step2_path,
        step3_path=step3_path,
        force=force,
        max_workers=max_workers,
    )
    return step1_path, step2_path


# ── Phase 3a: Qwen verification + diff ────────────────────────────────────────

def run_verification(officials: list[dict], city: str, province: str,
                     force: bool = False, max_workers: int = 3) -> Path:
    """Run Qwen verification and produce diff_report.json. Returns diff path."""
    from verifier_v2 import run_verification

    step1_path = LOGS_DIR / "deepseek_step1_results.json"
    step2_path = LOGS_DIR / "deepseek_step2_labels.json"
    diff_path  = LOGS_DIR / "diff_report.json"

    run_verification(
        ds_step1_path=step1_path,
        ds_step2_path=step2_path,
        diff_output_path=diff_path,
        city=city,
        province=province,
        force=force,
        max_workers=max_workers,
    )
    return diff_path


def run_qwen_extraction_only(officials: list[dict], city: str, province: str,
                              force: bool = False, max_workers: int = 3) -> None:
    """Run Qwen extraction only (no diff). Used in parallel mode."""
    from verifier_v2 import run_qwen_extraction
    run_qwen_extraction(
        officials_meta=officials,
        city=city,
        province=province,
        force=force,
        max_workers=max_workers,
    )


def run_diff_only() -> Path:
    """Run diff between existing DS and Qwen results."""
    from verifier_v2 import run_diff_only

    step1_path = LOGS_DIR / "deepseek_step1_results.json"
    step2_path = LOGS_DIR / "deepseek_step2_labels.json"
    diff_path  = LOGS_DIR / "diff_report.json"

    run_diff_only(
        ds_step1_path=step1_path,
        ds_step2_path=step2_path,
        diff_output_path=diff_path,
    )
    return diff_path


# ── Phase 3b: Battle table + judge ────────────────────────────────────────────

def run_battle(city: str, diff_path: Path, force: bool = False,
               max_workers: int = 3) -> dict:
    """Generate battle.xlsx with Kimi K2.5 judge verdicts."""
    from battle_generator import run_battle as _run_battle

    return _run_battle(
        diff_report_path=diff_path,
        output_dir=OUTPUT_DIR,
        city=city,
        force=force,
        max_workers=max_workers,
    )


# ── Phase 4: Post-processing ───────────────────────────────────────────────────

def run_postprocess(city: str, province: str, start_year: int) -> list[dict]:
    from postprocess_v2 import run_postprocess as _pp

    return _pp(
        city=city,
        province=province,
        start_year=start_year,
    )


# ── Phase 5: Export ───────────────────────────────────────────────────────────

def run_export(city: str, province: str) -> dict:
    from export_v2 import run_export as _exp

    return _exp(city=city, province=province)


# ── Full pipeline ──────────────────────────────────────────────────────────────

def run_pipeline(
    city: str,
    province: str,
    start_year: int,
    single_official: str = "",
    skip_scrape: bool = False,
    skip_extract: bool = False,
    skip_verify: bool = False,
    skip_battle: bool = False,
    check_wiki: bool = False,
    force: bool = False,
    workers: int = 3,
):
    t0 = time.time()
    print_banner(city, province, start_year)

    # ── Phase 0 ────────────────────────────────────────────────────────────────
    parsed = load_officials(city, single_official=single_official, check_wiki=check_wiki)
    officials = parsed["all_officials"]

    # ── Phase 1 ────────────────────────────────────────────────────────────────
    if skip_scrape:
        bio_files = list(OFFICIALS_DIR.glob("*_biography.txt"))
        print(f"\n[跳过] Phase 1: 使用现有 {len(bio_files)} 个履历文件")
        if not bio_files:
            print("✗ 无可用履历文件，请先运行不带 --skip-scrape 的命令")
            sys.exit(1)
    else:
        scrape_workers = min(workers, 2)  # cap scraping at 2 for anti-bot
        bio_files = run_scraping(officials, city, force=force,
                                 max_workers=scrape_workers, province=province)

    # ── Phase 2 + Phase 3a (parallel when both enabled) ─────────────────────
    step1_path = LOGS_DIR / "deepseek_step1_results.json"
    step2_path = LOGS_DIR / "deepseek_step2_labels.json"
    diff_path = LOGS_DIR / "diff_report.json"

    both_enabled = not skip_extract and not skip_verify

    if both_enabled:
        # DS extraction + Qwen extraction run simultaneously in separate threads
        # Then diff computed after both complete
        print(f"\n★ Phase 2 (DS) + Phase 3a (Qwen) 并行模式")
        t_parallel = time.time()

        with ThreadPoolExecutor(max_workers=2) as parallel_pool:
            ds_future = parallel_pool.submit(
                run_extraction, officials, city, province, force, workers
            )
            qw_future = parallel_pool.submit(
                run_qwen_extraction_only, officials, city, province, force, workers
            )

            # Wait for both to complete
            step1_path, step2_path = ds_future.result()
            qw_future.result()

        # Both done — now compute diff (if both produced results)
        if step1_path.exists() and (LOGS_DIR / "verify_step1_results.json").exists():
            diff_path = run_diff_only()
        else:
            missing = []
            if not step1_path.exists():
                missing.append("DS step1")
            if not (LOGS_DIR / "verify_step1_results.json").exists():
                missing.append("Qwen step1")
            print(f"  ⚠ 跳过 Diff: {', '.join(missing)} 结果文件缺失")

        elapsed_parallel = time.time() - t_parallel
        print(f"  ★ DS+Qwen 并行 + Diff 总耗时: {elapsed_parallel:.1f}s")
    else:
        if skip_extract:
            print(f"\n[跳过] Phase 2: 使用现有 DeepSeek 结果")
            if not step1_path.exists():
                print("✗ deepseek_step1_results.json 不存在，请先运行提取")
                sys.exit(1)
        else:
            step1_path, step2_path = run_extraction(
                officials, city, province, force=force, max_workers=workers
            )

        if skip_verify:
            print(f"\n[跳过] Phase 3a: Qwen 核查")
        else:
            diff_path = run_verification(officials, city, province, force=force, max_workers=workers)

    # ── Phase 3b ───────────────────────────────────────────────────────────────
    if skip_battle or not diff_path.exists():
        if skip_battle:
            print(f"\n[跳过] Phase 3b: Battle 表生成")
        else:
            print(f"\n[跳过] Phase 3b: diff_report.json 不存在")
    else:
        battle_result = run_battle(city, diff_path, force=force, max_workers=workers)

    # ── Phase 4 ────────────────────────────────────────────────────────────────
    rows = run_postprocess(city, province, start_year)
    if not rows:
        print("✗ 后处理结果为空，退出")
        sys.exit(1)

    # ── Phase 5 ────────────────────────────────────────────────────────────────
    export_result = run_export(city, province)

    # ── Summary ────────────────────────────────────────────────────────────────
    elapsed = time.time() - t0
    print("\n" + "=" * 60)
    print(f"  Pipeline v2 完成 — {city}市")
    print(f"  总耗时: {elapsed:.1f}s")
    print(f"  官员数: {len(officials)}")
    print(f"  总行数: {len(rows)}")
    print("  输出文件:")
    for label, path in (export_result or {}).items():
        print(f"    [{label}] {Path(str(path)).name}")
    battle_path = OUTPUT_DIR / f"{city}_battle.xlsx"
    if battle_path.exists():
        print(f"    [battle] {battle_path.name}")
    print("=" * 60)

    return export_result


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="城市官员履历数据库构建系统 v2",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python main_v2.py --city 深圳 --province 广东 --start 2010
  python main_v2.py --official 许勤              # 单官员测试
  python main_v2.py --skip-scrape               # 跳过爬虫，重用文本
  python main_v2.py --skip-extract --skip-verify  # 仅重新导出
  python main_v2.py --force                      # 强制全量重跑
        """,
    )
    parser.add_argument("--city",     default="深圳",  help="城市名（不含'市'）")
    parser.add_argument("--province", default="广东",  help="所属省份（不含'省'）")
    parser.add_argument("--start",    type=int, default=2010, help="起始年份")
    parser.add_argument("--official", default="",      help="单官员姓名（调试用）")
    parser.add_argument("--skip-scrape",   action="store_true", help="跳过爬取")
    parser.add_argument("--skip-extract",  action="store_true", help="跳过DeepSeek提取")
    parser.add_argument("--skip-verify",   action="store_true", help="跳过Qwen核查")
    parser.add_argument("--skip-battle",   action="store_true", help="跳过Battle表生成")
    parser.add_argument("--check-wiki",    action="store_true", help="与维基百科名单交叉核查")
    parser.add_argument("--force",         action="store_true", help="强制重新处理所有官员")
    parser.add_argument("--workers",       type=int, default=5, help="并发 worker 数（默认5，爬取最多2）")
    args = parser.parse_args()

    # Initialise logging and validate API keys
    setup_logging()
    skip_all_llm = args.skip_extract and args.skip_verify and args.skip_battle
    if not skip_all_llm:
        validate_api_keys(require_judge=not args.skip_battle)

    run_pipeline(
        city=args.city,
        province=args.province,
        start_year=args.start,
        single_official=args.official,
        skip_scrape=args.skip_scrape,
        skip_extract=args.skip_extract,
        skip_verify=args.skip_verify,
        skip_battle=args.skip_battle,
        check_wiki=args.check_wiki,
        force=args.force,
        workers=args.workers,
    )
