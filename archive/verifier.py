"""
Phase 3: Qwen3-max Verification

Independently extracts career data using Qwen, then compares with
DeepSeek results. Flags discrepancies for human review.

System prompt also loaded from bio-extraction.md (same single source of truth).
Verification logic loaded from bio-verification.md.
"""

import argparse
import json
import re
import time
from pathlib import Path

from openai import OpenAI

from config import (
    QWEN_API_KEY, QWEN_BASE_URL, QWEN_MODEL,
    OFFICIALS_DIR, LOGS_DIR, SKILLS_DIR,
    DATE_DISCREPANCY_YEARS, EPISODE_COUNT_DIFF,
)
from api_processor import load_skill_prompt, extract_json_from_response

LOGS_DIR.mkdir(parents=True, exist_ok=True)


def qwen_extract_official(
    client: OpenAI,
    system_prompt: str,
    name: str,
    bio_text: str,
    city: str,
    province: str,
) -> dict | None:
    """Extract career data for one official using Qwen."""
    user_prompt = f"""请根据以下百度百科内容，整理{name}的完整履历。
该官员曾担任{city}市市长和/或市委书记。

=== 百度百科内容 ===
{bio_text}

请严格按照系统提示中的格式输出JSON，不要任何额外解释。"""

    for attempt in range(2):
        try:
            response = client.chat.completions.create(
                model=QWEN_MODEL,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=0.1,
                max_tokens=8000,
                extra_body={"enable_thinking": False},  # disable qwen thinking mode for speed
            )
            raw_text = response.choices[0].message.content
            result = extract_json_from_response(raw_text)
            result["_meta"] = {"name": name, "source": "qwen", "model": QWEN_MODEL}
            return result
        except Exception as e:
            if attempt == 0:
                time.sleep(3)
            else:
                print(f"    [FAIL Qwen] {name}: {e}")
                return None


def parse_date_to_float(date_str: str) -> float:
    """Convert YYYY.MM string to float for comparison. Returns -1 on error."""
    try:
        parts = str(date_str).strip().split(".")
        year = int(parts[0])
        month = int(parts[1]) if len(parts) > 1 and parts[1] != "00" else 0
        return year + month / 12.0
    except Exception:
        return -1.0


def compare_dates(d1: str, d2: str) -> float:
    """Return absolute year difference between two date strings."""
    f1 = parse_date_to_float(d1)
    f2 = parse_date_to_float(d2)
    if f1 < 0 or f2 < 0:
        return 0.0
    return abs(f1 - f2)


def compare_bio_fields(bio_a: dict, bio_b: dict) -> list[dict]:
    """Compare biographical fields between two extractions."""
    discrepancies = []
    fields_to_check = ["出生年份", "籍贯", "少数民族", "女性", "全日制本科"]

    for field in fields_to_check:
        val_a = bio_a.get(field)
        val_b = bio_b.get(field)
        if val_a != val_b and val_a is not None and val_b is not None:
            level = "HIGH" if field in ["出生年份", "少数民族", "女性"] else "MEDIUM"
            discrepancies.append({
                "field": field,
                "source_a_value": val_a,
                "source_b_value": val_b,
                "level": level,
                "note": f"{field}不一致",
            })
    return discrepancies


def match_episodes(episodes_a: list[dict], episodes_b: list[dict]) -> list[tuple]:
    """
    Match episodes between two extractions by 供职单位+职务 similarity.
    Returns list of (ep_a, ep_b) matched pairs. Unmatched have None.
    """
    matched = []
    used_b = set()

    for ep_a in episodes_a:
        unit_a = ep_a.get("供职单位", "")
        pos_a = ep_a.get("职务", "")
        best_match = None
        best_score = 0

        for j, ep_b in enumerate(episodes_b):
            if j in used_b:
                continue
            unit_b = ep_b.get("供职单位", "")
            pos_b = ep_b.get("职务", "")
            # Simple similarity: character overlap
            unit_sim = len(set(unit_a) & set(unit_b)) / max(len(set(unit_a) | set(unit_b)), 1)
            pos_sim = len(set(pos_a) & set(pos_b)) / max(len(set(pos_a) | set(pos_b)), 1)
            score = unit_sim * 0.6 + pos_sim * 0.4
            if score > best_score and score > 0.3:
                best_score = score
                best_match = j

        if best_match is not None:
            matched.append((ep_a, episodes_b[best_match]))
            used_b.add(best_match)
        else:
            matched.append((ep_a, None))

    # Add unmatched episodes from b
    for j, ep_b in enumerate(episodes_b):
        if j not in used_b:
            matched.append((None, ep_b))

    return matched


def compare_episodes(episodes_a: list[dict], episodes_b: list[dict]) -> tuple[list[dict], dict]:
    """Compare episodes and return (discrepancies, missing_episodes_report)."""
    discrepancies = []
    in_a_not_b = []
    in_b_not_a = []

    count_diff = abs(len(episodes_a) - len(episodes_b))
    if count_diff > EPISODE_COUNT_DIFF:
        discrepancies.append({
            "field": "经历条数",
            "source_a_value": len(episodes_a),
            "source_b_value": len(episodes_b),
            "level": "HIGH" if count_diff > 4 else "MEDIUM",
            "note": f"经历条数相差{count_diff}条",
        })

    pairs = match_episodes(episodes_a, episodes_b)
    for ep_a, ep_b in pairs:
        if ep_a is None:
            in_b_not_a.append(ep_b)
            continue
        if ep_b is None:
            in_a_not_b.append(ep_a)
            continue

        seq = ep_a.get("经历序号", "?")
        # Check start date
        date_diff = compare_dates(
            str(ep_a.get("起始时间", "")),
            str(ep_b.get("起始时间", ""))
        )
        if date_diff > DATE_DISCREPANCY_YEARS:
            discrepancies.append({
                "episode_seq": seq,
                "供职单位": ep_a.get("供职单位", ""),
                "field": "起始时间",
                "source_a_value": ep_a.get("起始时间"),
                "source_b_value": ep_b.get("起始时间"),
                "level": "HIGH" if date_diff > 2 else "MEDIUM",
                "note": f"起始时间相差{date_diff:.1f}年",
            })

        # Check org tag
        tag_a = ep_a.get("组织标签", "")
        tag_b = ep_b.get("组织标签", "")
        if tag_a != tag_b:
            discrepancies.append({
                "episode_seq": seq,
                "供职单位": ep_a.get("供职单位", ""),
                "field": "组织标签",
                "source_a_value": tag_a,
                "source_b_value": tag_b,
                "level": "MEDIUM",
                "note": "组织标签存在分歧",
            })

    return discrepancies, {"in_a_not_b": in_a_not_b, "in_b_not_a": in_b_not_a}


def determine_verdict(bio_disc: list, ep_disc: list) -> str:
    all_disc = bio_disc + ep_disc
    high_count = sum(1 for d in all_disc if d.get("level") == "HIGH")
    medium_count = sum(1 for d in all_disc if d.get("level") == "MEDIUM")

    if high_count >= 2 or (high_count >= 1 and medium_count >= 2):
        return "MAJOR_CONFLICT"
    elif high_count >= 1 or medium_count >= 2:
        return "NEEDS_REVIEW"
    else:
        return "PASS"


def verify_official(name: str, result_a: dict, result_b: dict) -> dict:
    """Compare DeepSeek vs Qwen results for one official."""
    bio_a = result_a.get("bio", {})
    bio_b = result_b.get("bio", {})
    eps_a = result_a.get("episodes", [])
    eps_b = result_b.get("episodes", [])

    bio_disc = compare_bio_fields(bio_a, bio_b)
    ep_disc, missing = compare_episodes(eps_a, eps_b)

    all_disc = bio_disc + ep_disc
    high_count = sum(1 for d in all_disc if d.get("level") == "HIGH")
    medium_count = sum(1 for d in all_disc if d.get("level") == "MEDIUM")
    verdict = determine_verdict(bio_disc, ep_disc)

    return {
        "official_name": name,
        "summary": {
            "total_discrepancies": len(all_disc),
            "high_count": high_count,
            "medium_count": medium_count,
            "low_count": 0,
            "verdict": verdict,
        },
        "bio_discrepancies": bio_disc,
        "episode_discrepancies": ep_disc,
        "missing_episodes": missing,
        "final_recommendation": (
            f"{'⚠ 建议人工核查' if verdict != 'PASS' else '✓ 质量良好'}：使用DeepSeek版本作为默认；"
            f"发现{len(all_disc)}处差异（HIGH:{high_count}, MEDIUM:{medium_count}）"
        ),
    }


def run_verification(
    deepseek_results_path: Path,
    output_path: Path,
    city: str,
    province: str,
    force: bool = False,
) -> dict:
    """Run Qwen verification against all DeepSeek results."""
    print(f"\n=== Phase 3: Qwen Verification ===")

    # Load DeepSeek results
    with open(deepseek_results_path, encoding="utf-8") as f:
        ds_results = json.load(f)

    # Build name → result map
    ds_map: dict[str, dict] = {}
    for r in ds_results:
        name = r.get("bio", {}).get("姓名") or r.get("_meta", {}).get("name", "")
        if name:
            ds_map[name] = r

    # Load existing verification results
    existing_verif: dict[str, dict] = {}
    if output_path.exists() and not force:
        try:
            with open(output_path, encoding="utf-8") as f:
                existing_list = json.load(f)
                for v in existing_list:
                    existing_verif[v.get("official_name", "")] = v
        except Exception:
            pass

    # Load Qwen results cache
    qwen_cache_path = LOGS_DIR / "qwen_results.json"
    qwen_cache: dict[str, dict] = {}
    if qwen_cache_path.exists():
        with open(qwen_cache_path, encoding="utf-8") as f:
            cached = json.load(f)
            for r in cached:
                name = r.get("bio", {}).get("姓名") or r.get("_meta", {}).get("name", "")
                if name:
                    qwen_cache[name] = r

    client = OpenAI(api_key=QWEN_API_KEY, base_url=QWEN_BASE_URL)
    system_prompt = load_skill_prompt("bio-extraction")

    officials_files = {p.stem.replace("_biography", ""): p
                       for p in OFFICIALS_DIR.glob("*_biography.txt")}

    verification_reports = list(existing_verif.values())
    verified_names = set(existing_verif.keys())
    qwen_new_results = []

    for i, (name, ds_result) in enumerate(ds_map.items()):
        if name in verified_names and not force:
            print(f"  [{i+1}] SKIP {name} (already verified)")
            continue

        print(f"  [{i+1}/{len(ds_map)}] Verifying: {name}")

        # Get Qwen result (from cache or fresh extraction)
        if name in qwen_cache and not force:
            qw_result = qwen_cache[name]
            print(f"    Using cached Qwen result")
        else:
            bio_path = officials_files.get(name)
            if not bio_path:
                print(f"    SKIP — no biography file")
                continue
            bio_text = bio_path.read_text(encoding="utf-8")
            qw_result = qwen_extract_official(client, system_prompt, name, bio_text, city, province)
            if qw_result:
                qwen_cache[name] = qw_result
                qwen_new_results.append(qw_result)
            time.sleep(1.5)

        if qw_result:
            report = verify_official(name, ds_result, qw_result)
            verification_reports.append(report)
            verified_names.add(name)

            verdict = report["summary"]["verdict"]
            disc_count = report["summary"]["total_discrepancies"]
            print(f"    {verdict}: {disc_count} discrepancies")

    # Save Qwen results cache
    all_qwen = list(qwen_cache.values()) + qwen_new_results
    with open(qwen_cache_path, "w", encoding="utf-8") as f:
        # Deduplicate by name
        seen = set()
        unique = []
        for r in all_qwen:
            name = r.get("bio", {}).get("姓名") or r.get("_meta", {}).get("name", "")
            if name not in seen:
                seen.add(name)
                unique.append(r)
        json.dump(unique, f, ensure_ascii=False, indent=2)

    # Save verification report
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(verification_reports, f, ensure_ascii=False, indent=2)

    # Print summary
    total = len(verification_reports)
    passes = sum(1 for r in verification_reports if r["summary"]["verdict"] == "PASS")
    reviews = sum(1 for r in verification_reports if r["summary"]["verdict"] == "NEEDS_REVIEW")
    conflicts = sum(1 for r in verification_reports if r["summary"]["verdict"] == "MAJOR_CONFLICT")

    print(f"\n✓ Verification complete:")
    print(f"  PASS: {passes}/{total}")
    print(f"  NEEDS_REVIEW: {reviews}/{total}")
    print(f"  MAJOR_CONFLICT: {conflicts}/{total}")
    print(f"  Report saved to {output_path}")

    return {"reports": verification_reports, "pass": passes, "review": reviews, "conflict": conflicts}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Verify DeepSeek results with Qwen")
    parser.add_argument("--primary", default="logs/deepseek_results.json")
    parser.add_argument("--output", default="logs/verification_report.json")
    parser.add_argument("--city", default="深圳")
    parser.add_argument("--province", default="广东")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    run_verification(
        deepseek_results_path=Path(args.primary),
        output_path=Path(args.output),
        city=args.city,
        province=args.province,
        force=args.force,
    )
