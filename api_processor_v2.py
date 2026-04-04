"""
Phase 2 (v4): Two-Step DeepSeek Extraction with Preprocessed Text

Step 1: Extract structured episodes from numbered career lines.
        System prompt from prompts/step1_extraction.md
Step 2: Extract raw_bio + labels + corruption from episodes + bio_summary.
        System prompt from prompts/step2_labeling.md

Saves:
  logs/deepseek_step1_results.json  — career episodes with source_line
  logs/deepseek_step2_labels.json   — bio info + labels + corruption
"""

import json
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from openai import OpenAI

from config import (
    DEEPSEEK_API_KEY, DEEPSEEK_API_KEYS, DEEPSEEK_BASE_URL, DEEPSEEK_MODEL,
    LOGS_DIR, DEFAULT_WORKERS, DS_MAX_WORKERS,
)
from text_preprocessor import preprocess_official, format_career_lines_for_llm
from utils import extract_json, load_prompt, llm_chat, RoundRobinClientPool

logger = logging.getLogger(__name__)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

# ── Reference supplements (loaded once, injected per-official as needed) ──────

_REF_UNI = load_prompt("ref_university_rank") if (Path(__file__).parent / "prompts" / "ref_university_rank.md").exists() else ""
_REF_SOE = load_prompt("ref_soe_rank") if (Path(__file__).parent / "prompts" / "ref_soe_rank.md").exists() else ""

# Keywords that trigger injecting the reference supplement
_UNI_KWS = [
    "大学", "学院", "党校", "研究生", "本科", "硕士", "博士", "进修",
    "留学", "访问学者", "教授", "副教授", "讲师", "校长", "副校长",
    "研究员", "助理研究员", "高校", "学习",
]
_SOE_KWS = [
    "集团", "总公司", "公司", "银行", "保险", "证券", "基金",
    "石油", "石化", "电网", "电力", "航天", "航空", "兵器",
    "船舶", "核工业", "铁路", "中车", "中建", "中铁", "中交",
    "钢铁", "宝武", "中铝", "中粮", "中化", "招商局", "华能",
    "大唐", "华电", "国电", "中投", "光大", "中信", "工厂",
    "国资委", "央企", "国企",
]


def _detect_refs(career_text: str) -> str:
    """Detect if career text involves universities or SOEs; return extra prompt."""
    extras = []
    if any(kw in career_text for kw in _UNI_KWS):
        extras.append(f"\n\n---\n\n## 附录：高校与党校判定参考\n\n{_REF_UNI}")
    if any(kw in career_text for kw in _SOE_KWS):
        extras.append(f"\n\n---\n\n## 附录：国有企业判定参考\n\n{_REF_SOE}")
    return "".join(extras)


# ── Step 1: Career episode extraction from numbered lines ─────────────────────

def step1_extract(
    client: OpenAI,
    system_prompt: str,
    name: str,
    preprocessed: dict,
    city: str,
    province: str,
    official_role: str,
    max_retries: int = 4,
) -> dict | None:
    """Extract structured episodes from preprocessed career lines."""
    career_text = format_career_lines_for_llm(preprocessed["career_lines"])

    # Inject reference supplements when relevant
    ref_extra = _detect_refs(career_text)
    effective_sys = system_prompt + ref_extra if ref_extra else system_prompt

    location = f"{province}{city}市" if city else province
    user_prompt = (
        f"官员：{name}，{location}{official_role}\n\n"
        f"=== 编号履历行（共{preprocessed['total_lines']}行）===\n"
        f"{career_text}\n\n"
        "请将每行转化为结构化 episode，输出纯JSON。"
    )

    try:
        raw = llm_chat(client, DEEPSEEK_MODEL, effective_sys, user_prompt,
                       max_retries=max_retries)
        result = extract_json(raw)
        if "episodes" not in result:
            raise ValueError("Missing 'episodes'")
        if not isinstance(result["episodes"], list):
            raise ValueError("'episodes' must be a list")
        # Validate source_line presence
        for ep in result["episodes"]:
            if "source_line" not in ep:
                logger.warning(f"[step1] {name}: episode missing source_line, inferring from position")
        result["_meta"] = {
            "name": name, "city": city, "province": province,
            "official_role": official_role, "source": "deepseek_step1",
            "total_source_lines": preprocessed["total_lines"],
        }
        return result
    except Exception as e:
        logger.error(f"[FAIL step1] {name}: {e}")
        return None


# ── Step 2: Bio info + labels + corruption ───────────────────────────────────

def step2_label(
    client: OpenAI,
    system_prompt: str,
    name: str,
    city: str,
    province: str,
    official_role: str,
    episodes: list[dict],
    bio_summary: str = "",
    corruption_text: str = "",
    max_retries: int = 4,
) -> dict | None:
    """Extract raw_bio + labels + corruption from episodes and bio context."""
    episodes_json = json.dumps(episodes, ensure_ascii=False, indent=2)

    user_prompt = (
        f"官员：{name}\n"
        f"目标城市：{city}\n"
        f"目标省份：{province}\n"
        f"职务：{official_role}\n\n"
        f"=== 人物简介 ===\n{bio_summary}\n\n"
        f"=== 完整履历（共{len(episodes)}条）===\n{episodes_json}\n\n"
    )
    if corruption_text:
        user_prompt += f"=== 落马相关信息 ===\n{corruption_text}\n\n"

    user_prompt += "请输出 raw_bio + 三个标签 + 是否落马/落马原因 的纯JSON。"

    try:
        raw = llm_chat(client, DEEPSEEK_MODEL, system_prompt, user_prompt,
                       max_retries=max_retries)
        result = extract_json(raw)
        # Validate required fields
        if "raw_bio" not in result:
            raise ValueError("Missing 'raw_bio'")
        required_labels = {"升迁_省长", "升迁_省委书记", "本省提拔", "本省提拔依据", "本省学习", "本省学习依据"}
        missing = required_labels - set(result.keys())
        if missing:
            raise ValueError(f"Missing keys: {missing}")
        result["_meta"] = {
            "name": name, "city": city, "province": province, "source": "deepseek_step2",
        }
        return result
    except Exception as e:
        logger.error(f"[FAIL step2] {name}: {e}")
        return None


# ── Step 3: Administrative rank determination ────────────────────────────────

def step3_rank(
    client: OpenAI,
    system_prompt: str,
    name: str,
    episodes: list[dict],
    max_retries: int = 4,
) -> dict | None:
    """Determine administrative rank for each episode (batch call)."""
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
    effective_sys = system_prompt + ref_extra if ref_extra else system_prompt

    user_prompt = (
        f"官员：{name}\n\n"
        f"以下是该官员的全部 {len(episodes)} 段职务经历：\n{ep_text}\n\n"
        "请对每段经历判断行政级别，输出纯JSON。"
    )

    try:
        raw = llm_chat(client, DEEPSEEK_MODEL, effective_sys, user_prompt,
                       max_retries=max_retries)
        result = extract_json(raw)
        ranks = result.get("ranks", [])
        if not isinstance(ranks, list):
            raise ValueError("'ranks' must be a list")
        result["_meta"] = {"name": name, "source": "deepseek_step3"}
        return result
    except Exception as e:
        logger.error(f"[FAIL step3] {name}: {e}")
        return None


# ── Orchestrator ───────────────────────────────────────────────────────────────

def process_all_officials(
    city: str,
    province: str,
    officials_meta: list[dict],
    step1_path: Path,
    step2_path: Path,
    step3_path: Path | None = None,
    force: bool = False,
    max_workers: int = DEFAULT_WORKERS,
    officials_dir: Path | None = None,
) -> dict:
    max_workers = min(max_workers, DS_MAX_WORKERS)
    client_pool = RoundRobinClientPool(DEEPSEEK_API_KEYS or [DEEPSEEK_API_KEY], DEEPSEEK_BASE_URL)
    sys_step1 = load_prompt("step1_extraction")
    sys_step2 = load_prompt("step2_labeling")
    sys_step3 = load_prompt("step3_rank")
    print(f"  Loaded step1 ({len(sys_step1)}), step2 ({len(sys_step2)}), step3 ({len(sys_step3)}) chars")
    print(f"  DeepSeek API keys: {client_pool.size}")

    if step3_path is None:
        step3_path = LOGS_DIR / "deepseek_step3_rank.json"

    # Preprocess all biographies
    names = [o["name"] for o in officials_meta]
    from text_preprocessor import preprocess_all
    preprocessed = preprocess_all(names, officials_dir=officials_dir)
    print(f"  预处理完成: {len(preprocessed)}/{len(names)} 人")

    # Load existing results
    def load_existing(path: Path) -> dict[str, dict]:
        if path.exists() and not force:
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                return {r.get("_meta", {}).get("name", ""): r for r in data if r.get("_meta", {}).get("name")}
            except Exception:
                pass
        return {}

    existing_s1 = load_existing(step1_path)
    existing_s2 = load_existing(step2_path)
    existing_s3 = load_existing(step3_path)

    print(f"\n=== Phase 2 (v5.3): DeepSeek 三步提取（编号行模式）===")
    print(f"  已处理(step1): {len(existing_s1)}, (step2): {len(existing_s2)}, (step3): {len(existing_s3)}")
    print(f"  并发 workers: {max_workers}")

    lock = threading.Lock()

    def _save_cache(path: Path, cache: dict) -> None:
        path.write_text(
            json.dumps(list(cache.values()), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _process_one(official: dict) -> None:
        name = official["name"]
        official_role = official.get("role", "省长/省委书记")

        pp = preprocessed.get(name)
        if not pp:
            print(f"  SKIP {name} — 无预处理数据")
            return

        # ─ Step 1 ─────────────────────────────────────────────────────────────
        with lock:
            if name in existing_s1 and not force:
                s1_result = existing_s1[name]
                print(f"  SKIP step1 {name}")
            else:
                s1_result = None

        if s1_result is None:
            print(f"  Step1 提取: {name} ({pp['total_lines']} 行)...")
            s1_result = step1_extract(
                client_pool.next_client(), sys_step1, name, pp,
                city, province, official_role
            )
            if s1_result:
                with lock:
                    existing_s1[name] = s1_result
                    _save_cache(step1_path, existing_s1)
                n_ep = len(s1_result.get("episodes", []))
                print(f"    ✓ step1 {name}: {n_ep} 条 episode")
            else:
                return

        # ─ Step 2 ─────────────────────────────────────────────────────────────
        s2_skip = False
        with lock:
            if name in existing_s2 and not force:
                print(f"  SKIP step2 {name}")
                s2_skip = True

        if not s2_skip:
            episodes = s1_result.get("episodes", [])
            print(f"  Step2 标签: {name} ({len(episodes)} 条 episode)...")
            s2_result = step2_label(
                client_pool.next_client(), sys_step2, name, city, province,
                official_role, episodes,
                bio_summary=pp.get("bio_summary", ""),
                corruption_text=pp.get("corruption_text", ""),
            )
            if s2_result:
                with lock:
                    existing_s2[name] = s2_result
                    _save_cache(step2_path, existing_s2)
                print(f"    ✓ step2 {name}: 升迁_省长={s2_result.get('升迁_省长')}, "
                      f"升迁_省委书记={s2_result.get('升迁_省委书记')}, "
                      f"本省提拔={s2_result.get('本省提拔')}, "
                      f"落马={'是' if s2_result.get('是否落马') == '是' else '否'}")

        # ─ Step 3: Rank ──────────────────────────────────────────────────────
        with lock:
            if name in existing_s3 and not force:
                print(f"  SKIP step3 {name}")
                return

        episodes = s1_result.get("episodes", [])
        print(f"  Step3 级别: {name} ({len(episodes)} 条)...")
        s3_result = step3_rank(
            client_pool.next_client(), sys_step3, name, episodes
        )
        if s3_result:
            with lock:
                existing_s3[name] = s3_result
                _save_cache(step3_path, existing_s3)
            n_ranks = len(s3_result.get("ranks", []))
            print(f"    ✓ step3 {name}: {n_ranks} 条级别判断")

    if max_workers <= 1:
        for official in officials_meta:
            _process_one(official)
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = [pool.submit(_process_one, o) for o in officials_meta]
            for f in as_completed(futures):
                try:
                    f.result()
                except Exception as e:
                    logger.error(f"[process_one error] {e}")

    results_s1 = list(existing_s1.values())
    results_s2 = list(existing_s2.values())
    results_s3 = list(existing_s3.values())
    print(f"\n✓ Phase 2 完成: {len(results_s1)} step1, {len(results_s2)} step2, {len(results_s3)} step3")
    return {"step1": results_s1, "step2": results_s2, "step3": results_s3}
