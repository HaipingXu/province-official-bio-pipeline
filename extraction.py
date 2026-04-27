"""
Unified LLM extraction module parameterized by LLMConfig.

v9 pipeline (4 steps):
  step1: 起始时间, 终止时间, 供职单位, 职务         (no ref injection)
  step2: 组织标签, 标志位, 任职地（省/市）, 中央/地方  (inject SOE/uni refs)
  step3: 行政级别                                  (inject SOE/uni refs)
  step4: raw_bio + 升迁/本省提拔/本省学习 + 落马      (no ref injection)
"""

import json
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from typing import Callable

from config import DEFAULT_WORKERS
from text_preprocessor import format_career_lines_for_llm, preprocess_all
from utils import (
    extract_json,
    load_prompt,
    llm_chat,
    LLMConfig,
    load_json_cache,
    save_json_cache,
)

logger = logging.getLogger(__name__)

# ── Reference supplements (loaded once, injected per-official as needed) ──────

_REF_UNI = (
    load_prompt("ref_university_rank")
    if (Path(__file__).parent / "prompts" / "ref_university_rank.md").exists()
    else ""
)
_REF_SOE = (
    load_prompt("ref_soe_rank")
    if (Path(__file__).parent / "prompts" / "ref_soe_rank.md").exists()
    else ""
)

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


# ── Step 1: Basic episode extraction (time / unit / position only) ───────────


def step1_extract(
    cfg: LLMConfig,
    system_prompt: str,
    name: str,
    preprocessed: dict,
    city: str,
    province: str,
    official_role: str,
) -> dict | None:
    """Extract minimal episodes (source_line, 起止时间, 供职单位, 职务)."""
    career_text = format_career_lines_for_llm(preprocessed["career_lines"])

    location = f"{province}{city}市" if city else province
    user_prompt = (
        f"官员：{name}，{location}{official_role}\n\n"
        f"=== 编号履历行（共{preprocessed['total_lines']}行）===\n"
        f"{career_text}\n\n"
        "请将每行转化为最小事实条目，仅输出 source_line / 起始时间 / 终止时间 / 供职单位 / 职务 五个字段，纯JSON。"
    )

    try:
        raw = llm_chat(
            cfg.pool.next_client(), cfg.model, system_prompt, user_prompt,
            max_retries=cfg.max_retries, extra_body=cfg.extra_body,
            max_tokens=cfg.max_tokens,
        )
        result = extract_json(raw)
        if "episodes" not in result:
            raise ValueError("Missing 'episodes'")
        if not isinstance(result["episodes"], list):
            raise ValueError("'episodes' must be a list")
        for ep in result["episodes"]:
            if "source_line" not in ep:
                logger.warning(
                    f"[step1] {name}: episode missing source_line, inferring from position"
                )
        result["_meta"] = {
            "name": name, "city": city, "province": province,
            "official_role": official_role,
            "source": f"{cfg.source_tag}_step1",
            "total_source_lines": preprocessed["total_lines"],
        }
        return result
    except Exception as e:
        logger.error(f"[FAIL step1] {name}: {e}")
        return None


# ── Step 2: Classification (org tag, position tag, location, central/local) ──


def step2_classify(
    cfg: LLMConfig,
    system_prompt: str,
    name: str,
    episodes: list[dict],
) -> dict | None:
    """Classify each episode (organization tag, position tag, location)."""
    if not episodes:
        return None

    ep_lines = []
    for i, ep in enumerate(episodes, 1):
        sl = ep.get("source_line", i)
        st = ep.get("起始时间", "")
        et = ep.get("终止时间", "")
        unit = ep.get("供职单位", "")
        pos = ep.get("职务", "")
        ep_lines.append(
            f"  #{i}: source_line={sl}  起始={st}  终止={et}  供职单位={unit}  职务={pos}"
        )
    ep_text = "\n".join(ep_lines)

    ref_extra = _detect_refs(ep_text)
    effective_sys = system_prompt + ref_extra if ref_extra else system_prompt

    user_prompt = (
        f"官员：{name}\n\n"
        f"=== Episodes (Step1 已固定) ===\n{ep_text}\n\n"
        "请对每条 episode 输出 episode_idx + 组织标签 + 标志位 + 任职地（省）+ 任职地（市）+ 中央/地方，纯JSON。"
    )

    try:
        raw = llm_chat(
            cfg.pool.next_client(), cfg.model, effective_sys, user_prompt,
            max_retries=cfg.max_retries, extra_body=cfg.extra_body,
            max_tokens=cfg.max_tokens,
        )
        result = extract_json(raw)
        cls = result.get("classifications", [])
        if not isinstance(cls, list):
            raise ValueError("'classifications' must be a list")
        result["_meta"] = {"name": name, "source": f"{cfg.source_tag}_step2"}
        return result
    except Exception as e:
        logger.error(f"[FAIL step2] {name}: {e}")
        return None


# ── Step 3: Administrative rank determination ────────────────────────────────


def step3_rank(
    cfg: LLMConfig,
    system_prompt: str,
    name: str,
    episodes: list[dict],
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

    ref_extra = _detect_refs(ep_text)
    effective_sys = system_prompt + ref_extra if ref_extra else system_prompt

    user_prompt = (
        f"官员：{name}\n\n"
        f"以下是该官员的全部 {len(episodes)} 段职务经历：\n{ep_text}\n\n"
        "请对每段经历判断行政级别，输出纯JSON。"
        "无法定级的早期/秘书/干部条目请填 \"难以判断\"。"
    )

    try:
        raw = llm_chat(
            cfg.pool.next_client(), cfg.model, effective_sys, user_prompt,
            max_retries=cfg.max_retries, extra_body=cfg.extra_body,
            max_tokens=cfg.max_tokens,
        )
        result = extract_json(raw)
        ranks = result.get("ranks", [])
        if not isinstance(ranks, list):
            raise ValueError("'ranks' must be a list")
        result["_meta"] = {"name": name, "source": f"{cfg.source_tag}_step3"}
        return result
    except Exception as e:
        logger.error(f"[FAIL step3] {name}: {e}")
        return None


# ── Step 4: Bio info + labels + corruption ───────────────────────────────────


def step4_label(
    cfg: LLMConfig,
    system_prompt: str,
    name: str,
    city: str,
    province: str,
    official_role: str,
    episodes: list[dict],
    bio_summary: str = "",
    corruption_text: str = "",
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
        raw = llm_chat(
            cfg.pool.next_client(), cfg.model, system_prompt, user_prompt,
            max_retries=cfg.max_retries, extra_body=cfg.extra_body,
            max_tokens=cfg.max_tokens,
        )
        result = extract_json(raw)
        if "raw_bio" not in result:
            raise ValueError("Missing 'raw_bio'")
        required_labels = {
            "升迁_省长", "升迁_省委书记",
            "本省提拔", "本省提拔依据",
            "本省学习", "本省学习依据",
        }
        missing = required_labels - set(result.keys())
        if missing:
            raise ValueError(f"Missing keys: {missing}")
        result["_meta"] = {
            "name": name, "city": city, "province": province,
            "source": f"{cfg.source_tag}_step4",
        }
        return result
    except Exception as e:
        logger.error(f"[FAIL step4] {name}: {e}")
        return None


# ── Generic runner ──────────────────────────────────────────────────────────


def _run_step(
    step_name: str,
    officials_meta: list[dict],
    output_path: Path,
    cfg: LLMConfig,
    process_fn: Callable[[dict, dict, threading.Lock], None],
    existing: dict,
    force: bool = False,
) -> dict:
    """Generic runner: load cache, fan out process_fn, save per-official."""
    logger.info(
        f"[{cfg.source_tag}] {step_name}: model={cfg.model}, "
        f"keys={cfg.pool.size}, workers={DEFAULT_WORKERS}"
    )
    logger.info(f"[{cfg.source_tag}] Cached {step_name}: {len(existing)}")

    lock = threading.Lock()

    def _safe_process(official: dict) -> None:
        name = official["name"]
        with lock:
            if name in existing and not force:
                return
            existing[name] = None
        try:
            process_fn(official, existing, lock)
        except Exception:
            with lock:
                if existing.get(name) is None:
                    del existing[name]
            raise

    if DEFAULT_WORKERS <= 1:
        for official in officials_meta:
            _safe_process(official)
    else:
        with ThreadPoolExecutor(max_workers=DEFAULT_WORKERS) as pool:
            futures = [pool.submit(_safe_process, o) for o in officials_meta]
            for f in as_completed(futures):
                try:
                    f.result()
                except Exception as e:
                    logger.error(f"[{cfg.source_tag} {step_name} error] {e}")

    for k in [k for k, v in existing.items() if v is None]:
        del existing[k]

    logger.info(f"[{cfg.source_tag}] {step_name} done: {len(existing)} officials")
    return existing


# ── Runner: Step 1 ─────────────────────────────────────────────────────────


def run_step1(
    officials_meta: list[dict],
    city: str,
    province: str,
    output_path: Path,
    cfg: LLMConfig,
    force: bool = False,
    officials_dir: Path | None = None,
) -> dict:
    """Run step1 minimal extraction. Returns {name: result_dict}."""
    sys_step1 = load_prompt("step1_extraction")

    names = [o["name"] for o in officials_meta]
    preprocessed = preprocess_all(
        names, officials_dir=officials_dir, logs_dir=output_path.parent,
    )
    logger.info(f"[{cfg.source_tag}] Preprocessed: {len(preprocessed)}/{len(names)}")

    existing = load_json_cache(output_path, force)

    def _process(official: dict, existing: dict, lock: threading.Lock) -> None:
        name = official["name"]
        official_role = official.get("role", "省长/省委书记")
        pp = preprocessed.get(name)
        if not pp:
            logger.info(f"[{cfg.source_tag}] SKIP {name} -- no preprocessed data")
            with lock:
                if existing.get(name) is None:
                    del existing[name]
            return

        logger.info(f"[{cfg.source_tag}] Step1: {name} ({pp['total_lines']} lines)")
        result = step1_extract(cfg, sys_step1, name, pp, city, province, official_role)
        if result:
            with lock:
                existing[name] = result
                save_json_cache(output_path, existing)
            n_ep = len(result.get("episodes", []))
            logger.info(f"[{cfg.source_tag}] step1 {name}: {n_ep} episodes")

    return _run_step("step1", officials_meta, output_path, cfg, _process, existing, force)


# ── Runner: Step 2 ─────────────────────────────────────────────────────────


def run_step2(
    officials_meta: list[dict],
    merged_episodes_path: Path,
    output_path: Path,
    cfg: LLMConfig,
    force: bool = False,
) -> dict:
    """Run step2 (classification) on step1-merged episodes."""
    sys_step2 = load_prompt("step2_classify")
    merged_episodes = load_json_cache(merged_episodes_path)
    existing = load_json_cache(output_path, force)
    logger.info(
        f"[{cfg.source_tag}] step1-merged episodes: {len(merged_episodes)}, "
        f"cached step2: {len(existing)}"
    )

    def _process(official: dict, existing: dict, lock: threading.Lock) -> None:
        name = official["name"]
        ep_data = merged_episodes.get(name, {})
        episodes = ep_data.get("episodes", [])
        if not episodes:
            logger.info(f"[{cfg.source_tag}] SKIP step2 {name} -- no merged step1 episodes")
            with lock:
                if existing.get(name) is None:
                    del existing[name]
            return

        logger.info(f"[{cfg.source_tag}] Step2: {name} ({len(episodes)} episodes)")
        result = step2_classify(cfg, sys_step2, name, episodes)
        if result:
            with lock:
                existing[name] = result
                save_json_cache(output_path, existing)
            n = len(result.get("classifications", []))
            logger.info(f"[{cfg.source_tag}] step2 {name}: {n} classifications")

    return _run_step("step2", officials_meta, output_path, cfg, _process, existing, force)


# ── Runner: Step 3 ─────────────────────────────────────────────────────────


def run_step3(
    officials_meta: list[dict],
    merged_episodes_path: Path,
    output_path: Path,
    cfg: LLMConfig,
    force: bool = False,
) -> dict:
    """Run step3 (rank) on full merged episodes (post step2)."""
    sys_step3 = load_prompt("step3_rank")
    merged_episodes = load_json_cache(merged_episodes_path)
    existing = load_json_cache(output_path, force)
    logger.info(
        f"[{cfg.source_tag}] merged episodes: {len(merged_episodes)}, "
        f"cached step3: {len(existing)}"
    )

    def _process(official: dict, existing: dict, lock: threading.Lock) -> None:
        name = official["name"]
        ep_data = merged_episodes.get(name, {})
        episodes = ep_data.get("episodes", [])
        if not episodes:
            logger.info(f"[{cfg.source_tag}] SKIP step3 {name} -- no merged episodes")
            with lock:
                if existing.get(name) is None:
                    del existing[name]
            return

        logger.info(f"[{cfg.source_tag}] Step3: {name} ({len(episodes)} episodes)")
        result = step3_rank(cfg, sys_step3, name, episodes)
        if result:
            with lock:
                existing[name] = result
                save_json_cache(output_path, existing)
            n_ranks = len(result.get("ranks", []))
            logger.info(f"[{cfg.source_tag}] step3 {name}: {n_ranks} ranks")

    return _run_step("step3", officials_meta, output_path, cfg, _process, existing, force)


# ── Runner: Step 4 ─────────────────────────────────────────────────────────


def run_step4(
    officials_meta: list[dict],
    merged_episodes_path: Path,
    city: str,
    province: str,
    output_path: Path,
    cfg: LLMConfig,
    force: bool = False,
    officials_dir: Path | None = None,
) -> dict:
    """Run step4 (labels) on full merged episodes."""
    sys_step4 = load_prompt("step4_labeling")
    merged_episodes = load_json_cache(merged_episodes_path)
    existing = load_json_cache(output_path, force)
    logger.info(
        f"[{cfg.source_tag}] merged episodes: {len(merged_episodes)}, "
        f"cached step4: {len(existing)}"
    )

    names = [o["name"] for o in officials_meta]
    preprocessed = preprocess_all(
        names, officials_dir=officials_dir, logs_dir=output_path.parent,
    )

    def _process(official: dict, existing: dict, lock: threading.Lock) -> None:
        name = official["name"]
        official_role = official.get("role", "省长/省委书记")
        ep_data = merged_episodes.get(name, {})
        episodes = ep_data.get("episodes", [])
        if not episodes:
            logger.info(f"[{cfg.source_tag}] SKIP step4 {name} -- no merged episodes")
            with lock:
                if existing.get(name) is None:
                    del existing[name]
            return

        pp = preprocessed.get(name, {})
        logger.info(f"[{cfg.source_tag}] Step4: {name} ({len(episodes)} episodes)")
        result = step4_label(
            cfg, sys_step4, name, city, province, official_role, episodes,
            bio_summary=pp.get("bio_summary", ""),
            corruption_text=pp.get("corruption_text", ""),
        )
        if result:
            with lock:
                existing[name] = result
                save_json_cache(output_path, existing)
            logger.info(
                f"[{cfg.source_tag}] step4 {name}: "
                f"升迁_省长={result.get('升迁_省长')}, "
                f"升迁_省委书记={result.get('升迁_省委书记')}, "
                f"本省提拔={result.get('本省提拔')}, "
                f"落马={'是' if result.get('是否落马') == '是' else '否'}"
            )

    return _run_step("step4", officials_meta, output_path, cfg, _process, existing, force)
