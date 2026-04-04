"""
Main Orchestrator — Province Level

Runs the full pipeline for province-level officials (省长/主席 + 省委书记):
  Phase 0 : Parse province officials list (input_parser_province.py)
  Phase 1 : Scrape biographies — 2-layer strategy (bio_scraper_v2.py)
  Phase 2 : DeepSeek three-step extraction (api_processor_v2.py)
  Phase 3a: Doubao independent verification + diff report (verifier_v2.py)
  Phase 3b: Battle table + Kimi K2.5 judge (battle_generator.py)
  Phase 4 : Post-processing / flat rows (postprocess_v2.py)
  Phase 5 : Excel export (export_v2.py)

Usage:
  # Single province
  python main_province.py --province 安徽
  python main_province.py --province 北京 --start 2000

  # Single official (debug)
  python main_province.py --province 安徽 --official 王清宪

  # Batch all 31 provinces
  python main_province.py --batch

  # Skip options (same as city version)
  python main_province.py --province 安徽 --skip-scrape
  python main_province.py --province 安徽 --skip-extract
  python main_province.py --province 安徽 --skip-battle
  python main_province.py --province 安徽 --force

  # Batch with concurrency
  python main_province.py --batch --workers 8
"""

import argparse
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from config import (
    DATA_DIR, LOGS_DIR, OFFICIALS_DIR, OUTPUT_DIR, PROJECT_ROOT,
    setup_logging, validate_api_keys,
)

# All 31 provinces
ALL_PROVINCES = [
    "北京", "天津", "河北", "山西", "内蒙古",
    "辽宁", "吉林", "黑龙江",
    "上海", "江苏", "浙江", "安徽", "福建", "江西", "山东",
    "河南", "湖北", "湖南", "广东", "广西", "海南",
    "重庆", "四川", "贵州", "云南", "西藏",
    "陕西", "甘肃", "青海", "宁夏", "新疆",
]


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
        print(f"\n✗ 名单文件不存在: {txt_path}")
        print(f"  请运行 python generate_province_lists.py 生成")
        sys.exit(1)

    parsed = parse_province_officials_txt(province, data_dir=txt_path.parent)

    # Override start_year from CLI if provided
    if start_year:
        def after_start(entry):
            try:
                y = int(entry["start"].split(".")[0])
                return y >= start_year
            except Exception:
                return True
        parsed["all_officials"] = [o for o in parsed["all_officials"] if after_start(o)]
        parsed["governors"] = [o for o in parsed["governors"] if after_start(o)]
        parsed["secretaries"] = [o for o in parsed["secretaries"] if after_start(o)]

    if single_official:
        all_offs = [o for o in parsed["all_officials"] if o["name"] == single_official]
        if not all_offs:
            print(f"\n✗ 名单中未找到官员: {single_official}")
            sys.exit(1)
        parsed["all_officials"] = all_offs
        print(f"\n[单官员模式] {single_official}")
    else:
        gov_title = parsed["gov_title"]
        sec_title = parsed["sec_title"]
        print(f"\n✓ 名单加载完成: {len(parsed['all_officials'])} 人")
        print(f"  {gov_title}：{len(parsed['governors'])}人  {sec_title}：{len(parsed['secretaries'])}人")

    return parsed


# ── Phase 1: scraping ────────────────────────────────────────────────────────

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
        print(f"✗ 无可用履历文本 ({prov_dir})")
        sys.exit(1)

    return existing


# ── Phase 2: DeepSeek extraction ─────────────────────────────────────────────

def run_extraction(officials: list[dict], province: str, province_full: str,
                   force: bool = False, max_workers: int = 3) -> tuple[Path, Path]:
    """Run DeepSeek three-step extraction."""
    from api_processor_v2 import process_all_officials

    prov_logs = LOGS_DIR / province
    prov_logs.mkdir(parents=True, exist_ok=True)

    step1_path = prov_logs / "deepseek_step1_results.json"
    step2_path = prov_logs / "deepseek_step2_labels.json"
    step3_path = prov_logs / "deepseek_step3_rank.json"

    process_all_officials(
        city=province,          # reuse city param for province
        province=province_full,
        officials_meta=officials,
        step1_path=step1_path,
        step2_path=step2_path,
        step3_path=step3_path,
        force=force,
        max_workers=max_workers,
        officials_dir=OFFICIALS_DIR / province,
    )
    return step1_path, step2_path


# ── Phase 3a: Verification + diff ───────────────────────────────────────────

def run_verification(officials: list[dict], province: str, province_full: str,
                     force: bool = False, max_workers: int = 3) -> Path:
    """Run Doubao/Qwen verification and produce diff_report.json."""
    from verifier_v2 import run_verification

    prov_logs = LOGS_DIR / province
    step1_path = prov_logs / "deepseek_step1_results.json"
    step2_path = prov_logs / "deepseek_step2_labels.json"
    diff_path = prov_logs / "diff_report.json"

    run_verification(
        ds_step1_path=step1_path,
        ds_step2_path=step2_path,
        diff_output_path=diff_path,
        city=province,
        province=province_full,
        force=force,
        max_workers=max_workers,
        officials_dir=OFFICIALS_DIR / province,
        logs_dir=prov_logs,
    )
    return diff_path


def run_qwen_extraction_only(officials: list[dict], province: str, province_full: str,
                              force: bool = False, max_workers: int = 3) -> None:
    """Run verification extraction only (no diff). For parallel mode."""
    from verifier_v2 import run_qwen_extraction
    run_qwen_extraction(
        officials_meta=officials,
        city=province,
        province=province_full,
        force=force,
        max_workers=max_workers,
        officials_dir=OFFICIALS_DIR / province,
        logs_dir=LOGS_DIR / province,
    )


def run_diff_only(province: str) -> Path:
    """Run diff between existing DS and verification results."""
    from verifier_v2 import run_diff_only

    prov_logs = LOGS_DIR / province
    step1_path = prov_logs / "deepseek_step1_results.json"
    step2_path = prov_logs / "deepseek_step2_labels.json"
    diff_path = prov_logs / "diff_report.json"

    run_diff_only(
        ds_step1_path=step1_path,
        ds_step2_path=step2_path,
        diff_output_path=diff_path,
        logs_dir=prov_logs,
    )
    return diff_path


# ── Phase 3b: Battle table + judge ──────────────────────────────────────────

def run_battle(province: str, diff_path: Path, force: bool = False,
               max_workers: int = 3) -> dict:
    """Generate battle.xlsx with Kimi K2.5 judge verdicts."""
    from battle_generator import run_battle as _run_battle

    prov_output = OUTPUT_DIR / province
    prov_output.mkdir(parents=True, exist_ok=True)

    return _run_battle(
        diff_report_path=diff_path,
        output_dir=prov_output,
        city=province,
        force=force,
        max_workers=max_workers,
        officials_dir=OFFICIALS_DIR / province,
        logs_dir=LOGS_DIR / province,
    )


# ── Phase 4: Post-processing ────────────────────────────────────────────────

def run_postprocess(province: str, province_full: str, start_year: int | None) -> list[dict]:
    from postprocess_v2 import run_postprocess as _pp

    prov_logs = LOGS_DIR / province
    return _pp(
        city=province,
        province=province_full,
        start_year=start_year or 1949,
        step1_path=prov_logs / "deepseek_step1_results.json",
        step2_path=prov_logs / "deepseek_step2_labels.json",
        output_path=prov_logs / "final_rows.json",
        logs_dir=prov_logs,
        officials_dir=OFFICIALS_DIR / province,
    )


# ── Phase 5: Export ──────────────────────────────────────────────────────────

def run_export(province: str, province_full: str) -> dict:
    from export_v2 import run_export as _exp

    return _exp(
        city=province,
        province=province_full,
        final_rows_path=LOGS_DIR / province / "final_rows.json",
        output_dir=OUTPUT_DIR / province,
    )


# ── Single-province pipeline ────────────────────────────────────────────────

def run_province_pipeline(
    province: str,
    start_year: int | None = None,
    single_official: str = "",
    skip_scrape: bool = False,
    skip_extract: bool = False,
    skip_verify: bool = False,
    skip_battle: bool = False,
    force: bool = False,
    workers: int = 5,
    playwright_first: bool = False,
    data_subdir: str = "",
):
    t0 = time.time()

    # ── Phase 0 ──
    parsed = load_officials(province, single_official=single_official, start_year=start_year,
                            data_subdir=data_subdir)
    officials = parsed["all_officials"]
    province_full = parsed["province"]
    gov_title = parsed["gov_title"]
    sec_title = parsed["sec_title"]

    print_banner(province, province_full, start_year)

    # Setup per-province directories
    dirs = get_province_dirs(province)

    # ── Phase 1 ──
    if skip_scrape:
        bio_files = list(dirs["officials"].glob("*_biography.txt"))
        # Also check flat officials/ for backward compat
        if not bio_files:
            bio_files = list(OFFICIALS_DIR.glob("*_biography.txt"))
        print(f"\n[跳过] Phase 1: 使用现有 {len(bio_files)} 个履历文件")
        if not bio_files:
            print("✗ 无可用履历文件，请先运行不带 --skip-scrape 的命令")
            sys.exit(1)
    else:
        scrape_workers = min(workers, 2)
        bio_files = run_scraping(officials, province, force=force, max_workers=scrape_workers,
                                 playwright_first=playwright_first)

    # ── Phase 2 + Phase 3a (parallel when both enabled) ──
    prov_logs = dirs["logs"]
    step1_path = prov_logs / "deepseek_step1_results.json"
    step2_path = prov_logs / "deepseek_step2_labels.json"
    diff_path = prov_logs / "diff_report.json"

    both_enabled = not skip_extract and not skip_verify

    if both_enabled:
        print(f"\n★ Phase 2 (DS) + Phase 3a (Verify) 并行模式")
        t_parallel = time.time()

        with ThreadPoolExecutor(max_workers=2) as parallel_pool:
            ds_future = parallel_pool.submit(
                run_extraction, officials, province, province_full, force, workers
            )
            vf_future = parallel_pool.submit(
                run_qwen_extraction_only, officials, province, province_full, force, workers
            )
            step1_path, step2_path = ds_future.result()
            vf_future.result()

        if step1_path.exists() and (prov_logs / "verify_step1_results.json").exists():
            diff_path = run_diff_only(province)
        else:
            print(f"  ⚠ 跳过 Diff: 结果文件缺失")

        print(f"  ★ DS+Verify 并行 + Diff 总耗时: {time.time() - t_parallel:.1f}s")
    else:
        if skip_extract:
            print(f"\n[跳过] Phase 2: 使用现有 DeepSeek 结果")
        else:
            step1_path, step2_path = run_extraction(
                officials, province, province_full, force=force, max_workers=workers
            )

        if skip_verify:
            print(f"\n[跳过] Phase 3a: 核查")
        else:
            diff_path = run_verification(
                officials, province, province_full, force=force, max_workers=workers
            )

    # ── Phase 3b ──
    if skip_battle or not diff_path.exists():
        print(f"\n[跳过] Phase 3b: Battle 表")
    else:
        battle_result = run_battle(province, diff_path, force=force, max_workers=workers)

    # ── Phase 4 ──
    rows = run_postprocess(province, province_full, start_year)
    if not rows:
        print("✗ 后处理结果为空，退出")
        sys.exit(1)

    # ── Phase 5 ──
    export_result = run_export(province, province_full)

    # ── Summary ──
    elapsed = time.time() - t0
    print("\n" + "=" * 60)
    print(f"  Pipeline 完成 — {province_full}")
    print(f"  总耗时: {elapsed:.1f}s")
    print(f"  官员数: {len(officials)}")
    print(f"  总行数: {len(rows)}")
    print(f"  {gov_title}: {len(parsed['governors'])}人")
    print(f"  {sec_title}: {len(parsed['secretaries'])}人")
    print("  输出文件:")
    for label, path in (export_result or {}).items():
        print(f"    [{label}] {Path(str(path)).name}")
    print("=" * 60)

    return export_result


# ── Batch mode: all 31 provinces ─────────────────────────────────────────────

def run_batch(
    provinces: list[str] | None = None,
    start_year: int | None = None,
    skip_scrape: bool = False,
    skip_extract: bool = False,
    skip_verify: bool = False,
    skip_battle: bool = False,
    force: bool = False,
    workers: int = 5,
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
                skip_verify=skip_verify,
                skip_battle=skip_battle,
                force=force,
                workers=workers,
            )
            results[prov] = result
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
    parser.add_argument("--skip-scrape", action="store_true", help="跳过爬取")
    parser.add_argument("--skip-extract", action="store_true", help="跳过DeepSeek提取")
    parser.add_argument("--skip-verify", action="store_true", help="跳过核查")
    parser.add_argument("--skip-battle", action="store_true", help="跳过Battle表")
    parser.add_argument("--force", action="store_true", help="强制重新处理")
    parser.add_argument("--workers", type=int, default=5, help="并发worker数（默认5）")
    parser.add_argument("--playwright-first", action="store_true", help="Playwright为首选爬取方式（百度百科专用）")
    parser.add_argument("--data-subdir", default="", help="数据子目录（如 1990）→ 从 data/1990/ 读取名单")
    args = parser.parse_args()

    # Validate: must specify either --province or --batch
    if not args.province and not args.batch and not args.provinces:
        parser.print_help()
        print("\n✗ 请指定 --province 或 --batch")
        sys.exit(1)

    # Init
    setup_logging()
    skip_all_llm = args.skip_extract and args.skip_verify and args.skip_battle
    if not skip_all_llm:
        validate_api_keys(require_judge=not args.skip_battle)

    if args.batch or args.provinces:
        # Batch mode
        provinces = None  # all 31
        if args.provinces:
            provinces = [p.strip() for p in args.provinces.split(",") if p.strip()]
        run_batch(
            provinces=provinces,
            start_year=args.start,
            skip_scrape=args.skip_scrape,
            skip_extract=args.skip_extract,
            skip_verify=args.skip_verify,
            skip_battle=args.skip_battle,
            force=args.force,
            workers=args.workers,
        )
    else:
        # Single province mode
        run_province_pipeline(
            province=args.province,
            start_year=args.start,
            single_official=args.official,
            skip_scrape=args.skip_scrape,
            skip_extract=args.skip_extract,
            skip_verify=args.skip_verify,
            skip_battle=args.skip_battle,
            force=args.force,
            workers=args.workers,
            playwright_first=args.playwright_first,
            data_subdir=args.data_subdir,
        )
