"""
Merged episodes builder: resolve sl_group overrides and produce final episode list.
"""

import copy
import logging
from collections import defaultdict

logger = logging.getLogger(__name__)


def _get_sl_group_overrides(judge_cache: dict, name: str) -> dict[int, dict]:
    overrides: dict[int, dict] = {}
    for key, decision in judge_cache.items():
        parts = key.split("||")
        if len(parts) == 3 and parts[0] == name and parts[1] == "sl_group":
            try:
                ln = int(parts[2])
                overrides[ln] = {
                    "adopt": decision.get("adopt", "LLM1"),
                    "episodes": decision.get("episodes", []),
                }
            except (ValueError, TypeError):
                continue
    return overrides


def _apply_sl_group_overrides(
    ds_episodes: list[dict], vf_episodes: list[dict],
    overrides: dict[int, dict], name: str,
) -> list[dict]:
    vf_by_sl: dict[int, list[dict]] = defaultdict(list)
    for ep in vf_episodes:
        vf_by_sl[ep.get("source_line", 0)].append(ep)

    result: list[dict] = []
    seen_sl: set[int] = set()  # 追踪已处理的 override source_line

    for ep in ds_episodes:
        sl = ep.get("source_line", 0)
        override = overrides.get(sl)

        if override is None:
            result.append(ep)
            continue

        if sl in seen_sl:
            continue

        seen_sl.add(sl)
        adopt = override["adopt"]
        judge_eps = override.get("episodes", [])

        if judge_eps:
            for je in judge_eps:
                je.setdefault("source_line", sl)
            result.extend(judge_eps)
            ds_count = len([e for e in ds_episodes if e.get("source_line", 0) == sl])
            logger.info(f"  {name} L{sl:02d}: 裁判修正版 ({len(judge_eps)}条, 基于{adopt}, 替换LLM1的{ds_count}条)")
        elif adopt in ("LLM2", "VF"):
            vf_eps = vf_by_sl.get(sl, [])
            if vf_eps:
                result.extend(vf_eps)
            else:
                result.append(ep)
        else:
            result.append(ep)

    # C1 修复：补充 LLM2 独有、judge 已裁定但 LLM1 中不存在的 source_line
    unseen_overrides = {sl: ov for sl, ov in overrides.items() if sl not in seen_sl}
    if unseen_overrides:
        for sl in sorted(unseen_overrides.keys()):
            override = unseen_overrides[sl]
            adopt = override["adopt"]
            judge_eps = override.get("episodes", [])

            if judge_eps:
                for je in judge_eps:
                    je.setdefault("source_line", sl)
                result.extend(judge_eps)
                logger.info(f"  {name} L{sl:02d}: LLM2独有，裁判修正版 ({len(judge_eps)}条)")
            elif adopt in ("LLM2", "VF"):
                vf_eps = vf_by_sl.get(sl, [])
                if vf_eps:
                    result.extend(vf_eps)
                    logger.info(f"  {name} L{sl:02d}: LLM2独有，采纳LLM2 ({len(vf_eps)}条)")

    # 按 source_line 排序后重新编序号
    result.sort(key=lambda ep: ep.get("source_line", 0))

    for i, ep in enumerate(result):
        ep["经历序号"] = i + 1

    return result


def build_merged_episodes(
    official_name: str,
    ds_step1_item: dict,
    vf_step1_item: dict,
    judge_cache: dict,
) -> list[dict]:
    """Build post-争议-解决 episode list for one official."""
    # C2 修复：深拷贝避免污染原始数据
    episodes = copy.deepcopy(ds_step1_item.get("episodes", []))
    vf_episodes = copy.deepcopy(vf_step1_item.get("episodes", [])) if vf_step1_item else []
    overrides = _get_sl_group_overrides(judge_cache, official_name)
    if overrides:
        episodes = _apply_sl_group_overrides(
            episodes, vf_episodes, overrides, official_name
        )
    else:
        for i, ep in enumerate(episodes):
            ep["经历序号"] = i + 1
    return episodes
