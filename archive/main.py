"""
Main Orchestrator

Runs the full pipeline:
  Phase 0: Collect official list (list_scraper.py)
  Phase 1: Scrape biographies (bio_scraper.py)
  Phase 2: DeepSeek extraction (api_processor.py)
  Phase 3: Qwen verification (verifier.py)
  Phase 4: Post-processing (postprocess.py)
  Phase 5: Excel export (export.py)

Usage:
  python main.py --city 深圳 --province 广东 --start 2010
  python main.py --city 深圳 --official 许勤   # single official demo
  python main.py --skip-scrape --skip-verify   # offline re-run
"""

import argparse
import json
import sys
import time
from pathlib import Path

from config import LOGS_DIR, OFFICIALS_DIR, OUTPUT_DIR

# Ensure directories exist
for d in [LOGS_DIR, OFFICIALS_DIR, OUTPUT_DIR]:
    d.mkdir(parents=True, exist_ok=True)


def print_banner(city: str, start_year: int):
    print("=" * 60)
    print(f"  城市官员履历数据库构建系统")
    print(f"  城市：{city}市 | 起始年份：{start_year}")
    print("=" * 60)


def load_or_build_officials_list(
    city: str,
    start_year: int,
    force: bool = False,
    single_official: str = "",
) -> list[dict]:
    """Load existing officials list or build it fresh."""
    list_path = LOGS_DIR / "officials_list.json"

    if single_official:
        print(f"\n[Single official mode] Processing: {single_official}")
        return [{"name": single_official, "role": "未知", "start_year": 0, "needs_check": False}]

    if list_path.exists() and not force:
        with open(list_path, encoding="utf-8") as f:
            officials = json.load(f)
        print(f"\n✓ Loaded existing officials list ({len(officials)} entries)")
        flagged = sum(1 for o in officials if o.get("needs_check"))
        if flagged:
            print(f"  ⚠ {flagged} officials flagged for manual check")
        return officials

    # Build fresh list
    from list_scraper import collect_officials
    return collect_officials(city, start_year)


def run_pipeline(
    city: str,
    province: str,
    start_year: int,
    single_official: str = "",
    skip_list: bool = False,
    skip_scrape: bool = False,
    skip_extract: bool = False,
    skip_verify: bool = False,
    force: bool = False,
):
    start_time = time.time()
    print_banner(city, start_year)

    # --- Phase 0: Get officials list ---
    officials = load_or_build_officials_list(city, start_year, force, single_official)
    if not officials:
        print("✗ No officials found. Exiting.")
        sys.exit(1)

    # --- Phase 1: Scrape biographies ---
    if not skip_scrape:
        from bio_scraper import scrape_all
        scrape_result = scrape_all(officials, city, force)
        existing_files = list(OFFICIALS_DIR.glob("*_biography.txt"))
        if scrape_result["fail_count"] > 0:
            print(f"\n  ⚠ {scrape_result['fail_count']} scrape failures.")
            print(f"  手动处理：从浏览器打开百度百科，复制全文，保存到 officials/{{姓名}}_biography.txt")
        if not existing_files:
            print("✗ No biographies available. Please manually provide biography files.")
            sys.exit(1)
    else:
        print("\n[SKIP] Phase 1: Biography scraping (--skip-scrape)")
        existing_files = list(OFFICIALS_DIR.glob("*_biography.txt"))
        print(f"  Using {len(existing_files)} existing biography files")

    # --- Phase 2: DeepSeek Extraction ---
    ds_results_path = LOGS_DIR / "deepseek_results.json"
    if not skip_extract:
        from api_processor import process_all_officials
        extract_result = process_all_officials(
            city=city,
            province=province,
            officials_meta=officials,
            output_path=ds_results_path,
            force=force,
        )
    else:
        print("\n[SKIP] Phase 2: DeepSeek extraction (--skip-extract)")
        if not ds_results_path.exists():
            print("✗ No DeepSeek results found. Run without --skip-extract first.")
            sys.exit(1)

    # --- Phase 3: Qwen Verification ---
    verif_path = LOGS_DIR / "verification_report.json"
    if not skip_verify and ds_results_path.exists():
        from verifier import run_verification
        verif_result = run_verification(
            deepseek_results_path=ds_results_path,
            output_path=verif_path,
            city=city,
            province=province,
            force=force,
        )
    else:
        if skip_verify:
            print("\n[SKIP] Phase 3: Qwen verification (--skip-verify)")
        else:
            print("\n[SKIP] Phase 3: No DeepSeek results to verify")

    # --- Phase 4 & 5: Post-process + Export ---
    if ds_results_path.exists():
        from postprocess import run_postprocess
        from export import export

        rows = run_postprocess(
            deepseek_results_path=ds_results_path,
            verification_report_path=verif_path,
            city=city,
            province=province,
        )
        export_result = export(rows, city, province)
    else:
        print("\n✗ No extraction results to export.")
        sys.exit(1)

    # --- Summary ---
    elapsed = time.time() - start_time
    print("\n" + "=" * 60)
    print(f"  Pipeline Complete — {city}市")
    print(f"  总耗时: {elapsed:.1f}s")
    print(f"  官员数: {len(officials)}")
    print(f"  总行数: {export_result['total_rows']}")
    print("  输出文件:")
    for key, path in export_result["paths"].items():
        print(f"    {Path(path).name}")
    print("=" * 60)

    return export_result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="城市官员履历数据库构建系统",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python main.py --city 深圳 --province 广东 --start 2010
  python main.py --city 深圳 --official 许勤           # 单官员测试
  python main.py --skip-scrape --skip-verify          # 仅重新导出
        """,
    )
    parser.add_argument("--city", default="深圳", help="城市名（中文，不含'市'）")
    parser.add_argument("--province", default="广东", help="所属省份")
    parser.add_argument("--start", type=int, default=2010, help="起始年份")
    parser.add_argument("--official", default="", help="单官员测试（按姓名）")
    parser.add_argument("--skip-scrape", action="store_true", help="跳过爬取步骤")
    parser.add_argument("--skip-extract", action="store_true", help="跳过DeepSeek提取")
    parser.add_argument("--skip-verify", action="store_true", help="跳过Qwen核查")
    parser.add_argument("--force", action="store_true", help="强制重新处理所有官员")
    args = parser.parse_args()

    run_pipeline(
        city=args.city,
        province=args.province,
        start_year=args.start,
        single_official=args.official,
        skip_scrape=args.skip_scrape,
        skip_extract=args.skip_extract,
        skip_verify=args.skip_verify,
        force=args.force,
    )
