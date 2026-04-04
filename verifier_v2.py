"""
Phase 3a (v4): Verification LLM Independent Extraction + Source-Line Diff

Uses the same two-step extraction as DeepSeek, but with a different LLM
(Doubao > GLM-5 > Qwen). Then compares using source_line grouping.

Key v4 change: matching is by source_line number (from preprocessed career lines),
not by fuzzy text similarity. Episodes from the same source line are grouped together.

Saves:
  logs/verify_step1_results.json
  logs/verify_step2_labels.json
  logs/diff_report.json   — field-level diff with source values
"""

import json
import logging
import re
import threading
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from openai import OpenAI

from config import (
    QWEN_API_KEY, QWEN_API_KEYS, QWEN_BASE_URL, QWEN_MODEL,
    LOGS_DIR, DATE_DISCREPANCY_YEARS,
    DEFAULT_WORKERS,
)
from text_preprocessor import preprocess_official, format_career_lines_for_llm
from utils import extract_json, load_prompt, llm_chat, RoundRobinClientPool

# Try Doubao > GLM-5 > Qwen as verification model
try:
    from config import DOUBAO_API_KEY, DOUBAO_API_KEYS, DOUBAO_BASE_URL, DOUBAO_MODEL
    _USE_DOUBAO = bool(DOUBAO_API_KEY)
except ImportError:
    _USE_DOUBAO = False

try:
    from config import GLM_API_KEY, GLM_API_KEYS, GLM_BASE_URL, GLM_MODEL
    _USE_GLM = bool(GLM_API_KEY) and not _USE_DOUBAO
except ImportError:
    _USE_GLM = False

logger = logging.getLogger(__name__)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

# Import reference detection from api_processor_v2
from api_processor_v2 import _detect_refs


# ── Verification model selection ─────────────────────────────────────────────

def _get_verify_model() -> str:
    if _USE_DOUBAO:
        return DOUBAO_MODEL
    if _USE_GLM:
        return GLM_MODEL
    return QWEN_MODEL


def _get_verify_extra_body() -> dict | None:
    if _USE_DOUBAO or _USE_GLM:
        return None
    return {"enable_thinking": False}


def _get_verify_source_tag() -> str:
    if _USE_DOUBAO:
        return "doubao"
    return "glm" if _USE_GLM else "qwen"


def _resolve_verify_backend() -> tuple[str, list[str], str]:
    if _USE_DOUBAO:
        return "Doubao", DOUBAO_API_KEYS or [DOUBAO_API_KEY], DOUBAO_BASE_URL
    if _USE_GLM:
        return "GLM-5", GLM_API_KEYS or [GLM_API_KEY], GLM_BASE_URL
    return "Qwen", QWEN_API_KEYS or [QWEN_API_KEY], QWEN_BASE_URL


# ── Step 1: Extract episodes from numbered career lines ──────────────────────

def verify_step1(client, sys_prompt, name, preprocessed, city, province, role) -> dict | None:
    """Extract structured episodes using verification LLM."""
    model = _get_verify_model()
    source = _get_verify_source_tag()
    career_text = format_career_lines_for_llm(preprocessed["career_lines"])

    # Inject reference supplements when relevant
    ref_extra = _detect_refs(career_text)
    effective_sys = sys_prompt + ref_extra if ref_extra else sys_prompt

    location = f"{province}{city}市" if city else province
    user_prompt = (
        f"官员：{name}，{location}{role}\n\n"
        f"=== 编号履历行（共{preprocessed['total_lines']}行）===\n"
        f"{career_text}\n\n"
        "请将每行转化为结构化 episode，输出纯JSON。"
    )
    try:
        raw = llm_chat(client, model, effective_sys, user_prompt,
                       max_retries=1, extra_body=_get_verify_extra_body())
        result = extract_json(raw)
        if "episodes" not in result:
            raise ValueError("Missing 'episodes'")
        result["_meta"] = {
            "name": name, "source": f"{source}_step1",
            "total_source_lines": preprocessed["total_lines"],
        }
        return result
    except Exception as e:
        logger.error(f"[FAIL {source} step1] {name}: {e}")
        return None


# ── Step 2: Bio info + labels + corruption ───────────────────────────────────

def verify_step2(client, sys_prompt, name, city, province, role,
                 episodes, bio_summary="", corruption_text="") -> dict | None:
    """Extract raw_bio + labels + corruption using verification LLM."""
    model = _get_verify_model()
    source = _get_verify_source_tag()
    episodes_json = json.dumps(episodes, ensure_ascii=False, indent=2)

    user_prompt = (
        f"官员：{name}\n目标城市：{city}\n目标省份：{province}\n职务：{role}\n\n"
        f"=== 人物简介 ===\n{bio_summary}\n\n"
        f"=== 完整履历（共{len(episodes)}条）===\n{episodes_json}\n\n"
    )
    if corruption_text:
        user_prompt += f"=== 落马相关信息 ===\n{corruption_text}\n\n"
    user_prompt += "请输出 raw_bio + 三个标签 + 是否落马/落马原因 的纯JSON。"

    try:
        raw = llm_chat(client, model, sys_prompt, user_prompt,
                       max_tokens=2000, max_retries=1,
                       extra_body=_get_verify_extra_body())
        result = extract_json(raw)
        result["_meta"] = {"name": name, "source": f"{source}_step2"}
        return result
    except Exception as e:
        logger.error(f"[FAIL {source} step2] {name}: {e}")
        return None


# ── Step 3: Administrative rank determination ────────────────────────────────

def verify_step3(client, sys_prompt, name, episodes) -> dict | None:
    """Determine administrative rank using verification LLM (batch call)."""
    model = _get_verify_model()
    source = _get_verify_source_tag()

    if not episodes:
        return None

    ep_lines = []
    for i, ep in enumerate(episodes, 1):
        unit = ep.get("供职单位", "")
        pos = ep.get("职务", "")
        ep_lines.append(f"  {i}. 供职单位: {unit}  职务: {pos}")
    ep_text = "\n".join(ep_lines)

    # Inject reference supplements when relevant
    ref_extra = _detect_refs(ep_text)
    effective_sys = sys_prompt + ref_extra if ref_extra else sys_prompt

    user_prompt = (
        f"官员：{name}\n\n"
        f"以下是该官员的全部 {len(episodes)} 段职务经历：\n{ep_text}\n\n"
        "请对每段经历判断行政级别，输出纯JSON。"
    )

    try:
        raw = llm_chat(client, model, effective_sys, user_prompt,
                       max_retries=1, extra_body=_get_verify_extra_body())
        result = extract_json(raw)
        ranks = result.get("ranks", [])
        if not isinstance(ranks, list):
            raise ValueError("'ranks' must be a list")
        result["_meta"] = {"name": name, "source": f"{source}_step3"}
        return result
    except Exception as e:
        logger.error(f"[FAIL {source} step3] {name}: {e}")
        return None


# ── Source-line based matching ────────────────────────────────────────────────

def _normalize_party_committee(name: str) -> str:
    """Normalize party committee names: '深圳市委' → '中共深圳市委'."""
    if not name:
        return name
    if name.startswith("中共"):
        return name
    if re.search(r"(?:省|市|区|县|自治州|自治区)委", name):
        return "中共" + name
    return name


def _normalize_org_name(name: str) -> str:
    """Standardize common government/party organization name variants for comparison.

    Handles:
    - Party committee: 深圳市委 → 中共深圳市委
    - Full committee name: 中共XX省委员会 → 中共XX省委
    - Government: XX省人民政府 → XX省政府, XX市人民政府 → XX市政府
    - People's congress: XX省人民代表大会常务委员会 → XX省人大常委会
    - CPPCC: 中国人民政治协商会议XX省委员会 → XX省政协
    - Whitespace cleanup
    """
    if not name:
        return name
    # 1. Party committee prefix
    name = _normalize_party_committee(name)
    # 2. Full committee → short: 中共XX省委员会 → 中共XX省委
    name = re.sub(r"中共(.+?)委员会$", r"中共\1委", name)
    # 3. Government: XX省人民政府 → XX省政府
    name = re.sub(r"(.+?(?:省|自治区))人民政府", r"\1政府", name)
    # 4. Government: XX市/州/县/区人民政府 → XX市/州/县/区政府
    name = re.sub(r"(.+?(?:市|州|县|区|盟|旗))人民政府", r"\1政府", name)
    # 5. People's congress: 人民代表大会常务委员会 → 人大常委会
    name = re.sub(r"人民代表大会常务委员会", "人大常委会", name)
    # 6. People's congress: 人民代表大会 → 人大 (standalone)
    name = re.sub(r"人民代表大会", "人大", name)
    # 7. CPPCC: 中国人民政治协商会议XX省委员会 → XX省政协
    name = re.sub(r"中国人民政治协商会议(.+?)委员会", r"\1政协", name)
    # 8. Strip whitespace and fullwidth spaces
    name = name.strip().replace("\u3000", "").replace(" ", "")
    return name


def _to_float_date(s: str) -> float:
    try:
        parts = str(s).split(".")
        yr = int(parts[0])
        mo = int(parts[1]) if len(parts) > 1 and parts[1] not in ("00", "") else 0
        return yr + mo / 12.0
    except Exception:
        return -1.0


def group_by_source_line(episodes: list[dict]) -> dict[int, list[dict]]:
    """Group episodes by source_line number."""
    groups: dict[int, list[dict]] = defaultdict(list)
    for i, ep in enumerate(episodes):
        sl = ep.get("source_line", i + 1)  # fallback to position
        groups[sl].append(ep)
    return dict(groups)


def diff_episode_groups(ds_group: list[dict], vf_group: list[dict],
                        source_line: int) -> list[dict]:
    """
    Compare two groups of episodes from the same source_line.
    Within a group, try to match by unit+position similarity.
    """
    diffs = []

    # Simple case: same number of episodes
    if len(ds_group) == len(vf_group):
        # Sort by unit name for stable comparison
        ds_sorted = sorted(ds_group, key=lambda e: e.get("供职单位", ""))
        vf_sorted = sorted(vf_group, key=lambda e: e.get("供职单位", ""))
        for ep_ds, ep_vf in zip(ds_sorted, vf_sorted):
            diffs += _diff_single_pair(ep_ds, ep_vf, source_line)
    elif len(ds_group) != len(vf_group):
        # Different split count — flag as structural difference
        ds_desc = "; ".join(f"{e.get('供职单位', '')} {e.get('职务', '')}" for e in ds_group)
        vf_desc = "; ".join(f"{e.get('供职单位', '')} {e.get('职务', '')}" for e in vf_group)
        diffs.append({
            "scope": "episode_split",
            "field": "拆分方式",
            "ds_value": f"{len(ds_group)}条: {ds_desc}",
            "qw_value": f"{len(vf_group)}条: {vf_desc}",
            "level": "MEDIUM",
            "source_line": source_line,
        })
        # Still compare overlapping entries
        for i in range(min(len(ds_group), len(vf_group))):
            ds_sorted = sorted(ds_group, key=lambda e: e.get("供职单位", ""))
            vf_sorted = sorted(vf_group, key=lambda e: e.get("供职单位", ""))
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

        # Normalize organization names (party committee, government, NPC, CPPCC, etc.)
        if field == "供职单位":
            if _normalize_org_name(v_ds) == _normalize_org_name(v_vf):
                continue

        # Date threshold
        if field in ("起始时间", "终止时间"):
            diff_yr = abs(_to_float_date(v_ds) - _to_float_date(v_vf))
            if diff_yr <= DATE_DISCREPANCY_YEARS:
                continue
            level = "HIGH" if diff_yr > 2 else "MEDIUM"
        else:
            level = "MEDIUM"

        diffs.append({
            "scope": "episode_field",
            "field": field,
            "ds_value": v_ds,
            "qw_value": v_vf,
            "level": level,
            "source_line": source_line,
            "供职单位": ep_ds.get("供职单位", ""),
        })

    return diffs


def diff_bio_fields(bio_ds: dict, bio_vf: dict) -> list[dict]:
    """Compare raw_bio fields between DS step2 and verification step2."""
    diffs = []
    for field in ["出生年份", "籍贯", "少数民族", "女性", "全日制本科"]:
        v_ds = bio_ds.get(field)
        v_vf = bio_vf.get(field)
        if v_ds is None or v_vf is None:
            continue
        if v_ds != v_vf:
            level = "HIGH" if field in ["出生年份", "少数民族", "女性"] else "MEDIUM"
            diffs.append({"scope": "bio", "field": field,
                          "ds_value": v_ds, "qw_value": v_vf, "level": level})
    return diffs


def diff_label_fields(lbl_ds: dict, lbl_vf: dict) -> list[dict]:
    """Compare step2 label fields."""
    diffs = []
    for field in ["升迁_省长", "升迁_省委书记", "本省提拔", "本省学习"]:
        v_ds = lbl_ds.get(field)
        v_vf = lbl_vf.get(field)
        if v_ds is None or v_vf is None:
            continue
        if v_ds != v_vf:
            diffs.append({
                "scope": "label", "field": field,
                "ds_value": v_ds, "ds_reason": lbl_ds.get(field + "依据", ""),
                "qw_value": v_vf, "qw_reason": lbl_vf.get(field + "依据", ""),
                "level": "HIGH",
            })
    return diffs


def diff_corruption(ds_s2: dict, vf_s2: dict) -> list[dict]:
    """Compare corruption fields from step2."""
    diffs = []
    ds_luoma = ds_s2.get("是否落马", "")
    vf_luoma = vf_s2.get("是否落马", "")
    if ds_luoma and vf_luoma and ds_luoma != vf_luoma:
        diffs.append({
            "scope": "corruption", "field": "是否落马",
            "ds_value": ds_luoma, "qw_value": vf_luoma,
            "level": "HIGH",
        })
    return diffs


def diff_all_episodes(eps_ds: list[dict], eps_vf: list[dict]) -> list[dict]:
    """
    Diff episodes using source_line grouping.
    Episodes from the same source line are compared together.
    """
    ds_groups = group_by_source_line(eps_ds)
    vf_groups = group_by_source_line(eps_vf)
    all_lines = sorted(set(ds_groups) | set(vf_groups))

    diffs = []
    for line_num in all_lines:
        ds_g = ds_groups.get(line_num, [])
        vf_g = vf_groups.get(line_num, [])

        if not ds_g and vf_g:
            # Verification has episodes DS missed
            desc = "; ".join(f"{e.get('供职单位', '')} {e.get('职务', '')}" for e in vf_g)
            diffs.append({
                "scope": "episode_missing", "field": "DS缺失",
                "ds_value": "（无）", "qw_value": desc,
                "level": "MEDIUM", "source_line": line_num,
            })
        elif ds_g and not vf_g:
            # DS has episodes verification missed
            desc = "; ".join(f"{e.get('供职单位', '')} {e.get('职务', '')}" for e in ds_g)
            diffs.append({
                "scope": "episode_missing", "field": "验证缺失",
                "ds_value": desc, "qw_value": "（无）",
                "level": "MEDIUM", "source_line": line_num,
            })
        else:
            diffs += diff_episode_groups(ds_g, vf_g, line_num)

    return diffs


# ── Extraction-only (for parallel mode) ──────────────────────────────────────

def run_qwen_extraction(
    officials_meta: list[dict],
    city: str,
    province: str,
    force: bool = False,
    max_workers: int = DEFAULT_WORKERS,
    officials_dir: Path | None = None,
    logs_dir: Path | None = None,
) -> dict:
    """Run verification LLM step1 + step2 + step3 extraction independently (no diff)."""
    verify_name, api_keys, base_url = _resolve_verify_backend()

    print(f"\n=== Phase 3a-extract: {verify_name} 独立提取（编号行模式）===")
    print(f"  模型: {_get_verify_model()}")
    print(f"  并发 workers: {max_workers}（{verify_name} keys: {len(api_keys)}）")

    pool = RoundRobinClientPool(api_keys, base_url)
    sys_step1 = load_prompt("step1_extraction")
    sys_step2 = load_prompt("step2_labeling")
    sys_step3 = load_prompt("step3_rank")

    # Preprocess all biographies
    names = [o["name"] for o in officials_meta]
    from text_preprocessor import preprocess_all
    preprocessed = preprocess_all(names, officials_dir=officials_dir)

    _logs = logs_dir or LOGS_DIR
    vf1_path = _logs / "verify_step1_results.json"
    vf2_path = _logs / "verify_step2_labels.json"
    vf3_path = _logs / "verify_step3_rank.json"

    def load_cache(path: Path) -> dict[str, dict]:
        if path.exists() and not force:
            try:
                return {r["_meta"]["name"]: r for r in json.loads(path.read_text(encoding="utf-8"))}
            except Exception:
                pass
        return {}

    vf1_cache = load_cache(vf1_path)
    vf2_cache = load_cache(vf2_path)
    vf3_cache = load_cache(vf3_path)

    lock = threading.Lock()

    def _save_cache(path: Path, cache: dict) -> None:
        path.write_text(
            json.dumps(list(cache.values()), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _extract_one(official: dict) -> None:
        name = official["name"]
        official_role = official.get("role", "省长/省委书记")
        pp = preprocessed.get(name)
        if not pp:
            print(f"  SKIP {name} — 无预处理数据")
            return

        print(f"\n  [{name}] {verify_name} 提取...")

        # ─ Step 1 ────────────────────────────────────────────────────────
        with lock:
            cached_s1 = vf1_cache.get(name) if not force else None
        if cached_s1:
            vf_s1 = cached_s1
            print(f"    step1: 使用缓存")
        else:
            vf_s1 = verify_step1(pool.next_client(), sys_step1, name, pp,
                                  city, province, official_role)
            if vf_s1:
                with lock:
                    vf1_cache[name] = vf_s1
                    _save_cache(vf1_path, vf1_cache)

        if not vf_s1:
            return

        # ─ Step 2 ────────────────────────────────────────────────────────
        s2_skip = False
        with lock:
            cached_s2 = vf2_cache.get(name) if not force else None
        if cached_s2:
            print(f"    step2: 使用缓存")
            s2_skip = True
        else:
            vf_s2 = verify_step2(pool.next_client(), sys_step2, name, city, province,
                                  official_role, vf_s1.get("episodes", []),
                                  bio_summary=pp.get("bio_summary", ""),
                                  corruption_text=pp.get("corruption_text", ""))
            if vf_s2:
                with lock:
                    vf2_cache[name] = vf_s2
                    _save_cache(vf2_path, vf2_cache)

        # ─ Step 3: Rank ──────────────────────────────────────────────────
        with lock:
            cached_s3 = vf3_cache.get(name) if not force else None
        if cached_s3:
            print(f"    step3: 使用缓存")
            return
        vf_s3 = verify_step3(pool.next_client(), sys_step3, name,
                              vf_s1.get("episodes", []))
        if vf_s3:
            with lock:
                vf3_cache[name] = vf_s3
                _save_cache(vf3_path, vf3_cache)
            n_ranks = len(vf_s3.get("ranks", []))
            print(f"    ✓ step3 {name}: {n_ranks} 条级别判断")

    if max_workers <= 1:
        for official in officials_meta:
            _extract_one(official)
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(_extract_one, o) for o in officials_meta]
            for f in as_completed(futures):
                try:
                    f.result()
                except Exception as e:
                    logger.error(f"[verify_extract error] {e}")

    print(f"\n✓ {verify_name} 提取完成: {len(vf1_cache)} step1, {len(vf2_cache)} step2, {len(vf3_cache)} step3")
    return {"vf1_cache": vf1_cache, "vf2_cache": vf2_cache, "vf3_cache": vf3_cache}


# ── Diff-only (after both DS and verification extractions are done) ──────────

def run_diff_only(
    ds_step1_path: Path,
    ds_step2_path: Path,
    diff_output_path: Path,
    logs_dir: Path | None = None,
) -> dict:
    """Compute diff between DS and verification results using source_line grouping."""
    print(f"\n=== Phase 3a-diff: DS vs {_get_verify_source_tag().upper()} 差异对比（source_line 分组）===")

    _logs = logs_dir or LOGS_DIR

    ds_step1 = {r["_meta"]["name"]: r for r in json.loads(ds_step1_path.read_text(encoding="utf-8"))}
    ds_step2 = {r["_meta"]["name"]: r for r in json.loads(ds_step2_path.read_text(encoding="utf-8"))} \
        if ds_step2_path.exists() else {}

    vf1_path = _logs / "verify_step1_results.json"
    vf2_path = _logs / "verify_step2_labels.json"

    # Load step3 rank data
    ds_step3_path = _logs / "deepseek_step3_rank.json"
    vf3_path = _logs / "verify_step3_rank.json"

    def load_cache(path: Path) -> dict[str, dict]:
        if path.exists():
            try:
                return {r["_meta"]["name"]: r for r in json.loads(path.read_text(encoding="utf-8"))}
            except Exception:
                pass
        return {}

    vf1_cache = load_cache(vf1_path)
    vf2_cache = load_cache(vf2_path)
    ds_step3 = load_cache(ds_step3_path)
    vf3_cache = load_cache(vf3_path)

    all_diffs: list[dict] = []

    for name, ds_s1 in ds_step1.items():
        vf_s1 = vf1_cache.get(name)
        if not vf_s1:
            continue

        vf_s2 = vf2_cache.get(name, {})
        ds_s2_data = ds_step2.get(name, {})

        person_diffs: list[dict] = []

        # Bio diffs (from step2)
        if ds_s2_data and vf_s2:
            person_diffs += diff_bio_fields(
                ds_s2_data.get("raw_bio", {}), vf_s2.get("raw_bio", {})
            )
            person_diffs += diff_label_fields(ds_s2_data, vf_s2)
            person_diffs += diff_corruption(ds_s2_data, vf_s2)

        # Episode diffs (source_line grouping)
        person_diffs += diff_all_episodes(
            ds_s1.get("episodes", []), vf_s1.get("episodes", [])
        )

        high_count = sum(1 for d in person_diffs if d["level"] == "HIGH")
        medium_count = sum(1 for d in person_diffs if d["level"] == "MEDIUM")
        if high_count >= 2 or (high_count >= 1 and medium_count >= 2):
            verdict = "MAJOR_CONFLICT"
        elif high_count >= 1 or medium_count >= 2:
            verdict = "NEEDS_REVIEW"
        else:
            verdict = "PASS"

        all_diffs.append({
            "official_name": name,
            "verdict": verdict,
            "high_count": high_count,
            "medium_count": medium_count,
            "diffs": person_diffs,
            "ds_step1": ds_s1,
            "vf_step1": vf_s1,
            "ds_step2": ds_s2_data,
            "vf_step2": vf_s2,
            "ds_step3": ds_step3.get(name, {}),
            "vf_step3": vf3_cache.get(name, {}),
        })
        print(f"  [{name}] {verdict} ({high_count}H {medium_count}M, {len(person_diffs)} diffs)")

    diff_output_path.write_text(json.dumps(all_diffs, ensure_ascii=False, indent=2), encoding="utf-8")

    total = len(all_diffs)
    passes = sum(1 for d in all_diffs if d["verdict"] == "PASS")
    reviews = sum(1 for d in all_diffs if d["verdict"] == "NEEDS_REVIEW")
    conflicts = sum(1 for d in all_diffs if d["verdict"] == "MAJOR_CONFLICT")
    print(f"\n✓ Diff 完成: PASS={passes}, NEEDS_REVIEW={reviews}, MAJOR_CONFLICT={conflicts} / {total}")

    return {"diffs": all_diffs, "pass": passes, "review": reviews, "conflict": conflicts}


# ── Full verification run (sequential: extract + diff in one pass) ───────────

def run_verification(
    ds_step1_path: Path,
    ds_step2_path: Path,
    diff_output_path: Path,
    city: str,
    province: str,
    force: bool = False,
    max_workers: int = DEFAULT_WORKERS,
    officials_dir: Path | None = None,
    logs_dir: Path | None = None,
) -> dict:
    """Sequential mode: verification extraction + diff in one pass."""
    verify_name, api_keys, base_url = _resolve_verify_backend()

    print(f"\n=== Phase 3 (v4): {verify_name} 独立提取 + Diff（source_line 模式）===")
    print(f"  模型: {_get_verify_model()}")
    print(f"  并发 workers: {max_workers}（{verify_name} keys: {len(api_keys)}）")

    pool = RoundRobinClientPool(api_keys, base_url)
    sys_step1 = load_prompt("step1_extraction")
    sys_step2 = load_prompt("step2_labeling")

    ds_step1 = {r["_meta"]["name"]: r for r in json.loads(ds_step1_path.read_text(encoding="utf-8"))}
    ds_step2 = {r["_meta"]["name"]: r for r in json.loads(ds_step2_path.read_text(encoding="utf-8"))} \
        if ds_step2_path.exists() else {}

    # Preprocess biographies
    names = list(ds_step1.keys())
    from text_preprocessor import preprocess_all
    preprocessed = preprocess_all(names, officials_dir=officials_dir)

    _logs = logs_dir or LOGS_DIR
    vf1_path = _logs / "verify_step1_results.json"
    vf2_path = _logs / "verify_step2_labels.json"

    def load_cache(path: Path) -> dict[str, dict]:
        if path.exists() and not force:
            try:
                return {r["_meta"]["name"]: r for r in json.loads(path.read_text(encoding="utf-8"))}
            except Exception:
                pass
        return {}

    vf1_cache = load_cache(vf1_path)
    vf2_cache = load_cache(vf2_path)

    lock = threading.Lock()
    all_diffs: list[dict] = []

    def _save_cache(path: Path, cache: dict) -> None:
        path.write_text(
            json.dumps(list(cache.values()), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _verify_one(name: str, ds_s1: dict) -> dict | None:
        pp = preprocessed.get(name)
        if not pp:
            print(f"  [{name}] SKIP — 无预处理数据")
            return None

        official_role = ds_s1.get("_meta", {}).get("official_role", "省长/省委书记")
        print(f"\n  [{name}] {verify_name} 提取...")

        # ─ Step 1 ────────────────────────────────────────────────────────
        with lock:
            cached_s1 = vf1_cache.get(name) if not force else None
        if cached_s1:
            vf_s1 = cached_s1
            print(f"    step1: 使用缓存")
        else:
            vf_s1 = verify_step1(pool.next_client(), sys_step1, name, pp,
                                  city, province, official_role)
            if vf_s1:
                with lock:
                    vf1_cache[name] = vf_s1
                    _save_cache(vf1_path, vf1_cache)

        if not vf_s1:
            return None

        # ─ Step 2 ────────────────────────────────────────────────────────
        with lock:
            cached_s2 = vf2_cache.get(name) if not force else None
        if cached_s2:
            vf_s2 = cached_s2
            print(f"    step2: 使用缓存")
        else:
            vf_s2 = verify_step2(pool.next_client(), sys_step2, name, city, province,
                                  official_role, vf_s1.get("episodes", []),
                                  bio_summary=pp.get("bio_summary", ""),
                                  corruption_text=pp.get("corruption_text", ""))
            if vf_s2:
                with lock:
                    vf2_cache[name] = vf_s2
                    _save_cache(vf2_path, vf2_cache)

        # ─ Diff ──────────────────────────────────────────────────────────
        person_diffs: list[dict] = []
        ds_s2_data = ds_step2.get(name, {})

        if ds_s2_data and vf_s2:
            person_diffs += diff_bio_fields(
                ds_s2_data.get("raw_bio", {}), (vf_s2 or {}).get("raw_bio", {})
            )
            person_diffs += diff_label_fields(ds_s2_data, vf_s2 or {})
            person_diffs += diff_corruption(ds_s2_data, vf_s2 or {})

        person_diffs += diff_all_episodes(
            ds_s1.get("episodes", []), vf_s1.get("episodes", [])
        )

        high_count = sum(1 for d in person_diffs if d["level"] == "HIGH")
        medium_count = sum(1 for d in person_diffs if d["level"] == "MEDIUM")
        if high_count >= 2 or (high_count >= 1 and medium_count >= 2):
            verdict = "MAJOR_CONFLICT"
        elif high_count >= 1 or medium_count >= 2:
            verdict = "NEEDS_REVIEW"
        else:
            verdict = "PASS"

        result = {
            "official_name": name,
            "verdict": verdict,
            "high_count": high_count,
            "medium_count": medium_count,
            "diffs": person_diffs,
            "ds_step1": ds_s1,
            "vf_step1": vf_s1,
            "ds_step2": ds_s2_data,
            "vf_step2": vf_s2 or {},
        }
        print(f"    verdict: {verdict} ({high_count}H {medium_count}M, {len(person_diffs)} diffs)")
        return result

    items = list(ds_step1.items())

    if max_workers <= 1:
        for name, ds_s1 in items:
            result = _verify_one(name, ds_s1)
            if result:
                all_diffs.append(result)
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(_verify_one, name, ds_s1): name for name, ds_s1 in items}
            for f in as_completed(futures):
                try:
                    result = f.result()
                    if result:
                        all_diffs.append(result)
                except Exception as e:
                    logger.error(f"[verify_one error] {futures[f]}: {e}")

    diff_output_path.write_text(json.dumps(all_diffs, ensure_ascii=False, indent=2), encoding="utf-8")

    total = len(all_diffs)
    passes = sum(1 for d in all_diffs if d["verdict"] == "PASS")
    reviews = sum(1 for d in all_diffs if d["verdict"] == "NEEDS_REVIEW")
    conflicts = sum(1 for d in all_diffs if d["verdict"] == "MAJOR_CONFLICT")
    print(f"\n✓ Diff 完成: PASS={passes}, NEEDS_REVIEW={reviews}, MAJOR_CONFLICT={conflicts} / {total}")

    return {"diffs": all_diffs, "pass": passes, "review": reviews, "conflict": conflicts}
