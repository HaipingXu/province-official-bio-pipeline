"""
Merged episodes builder (v9 — 2-stage).

Stage A — `build_merged_episodes_step1()`:
    Resolve step1 disputes (sl_group + step1 field overrides for time/unit/pos)
    → produces merged_episodes_step1.json.
    Episodes contain only step1 fields.

Stage B — `build_merged_episodes_full()`:
    Layer step2 classification onto merged_step1 episodes, applying step2 judge
    overrides → produces merged_episodes.json.
    Episodes contain step1 + step2 fields, ready for step3 (rank) and step4.
"""

import copy
import logging
from collections import defaultdict

from config import STEP1_EPISODE_FIELDS, STEP2_EPISODE_FIELDS

logger = logging.getLogger(__name__)


# ── Stage A: step1 sl_group + field overrides ───────────────────────────────

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
    seen_sl: set[int] = set()

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

    result.sort(key=lambda ep: ep.get("source_line", 0))

    for i, ep in enumerate(result):
        ep["经历序号"] = i + 1

    return result


def _apply_step1_field_overrides(
    episodes: list[dict],
    vf_step1_data: dict,
    judge_cache: dict,
    name: str,
    skip_sls: set[int] | None = None,
) -> list[dict]:
    """Apply step1 ep_batch field-level verdicts to merged step1 episodes.

    Episodes whose source_line is in skip_sls have already been handled by
    sl_group overrides and should not be modified here.
    """
    vf_episodes = vf_step1_data.get("episodes", [])
    vf_by_key: dict[tuple, dict] = {}
    for ep in vf_episodes:
        k = (ep.get("source_line", 0), ep.get("供职单位", ""), ep.get("职务", ""))
        vf_by_key[k] = ep

    result: list[dict] = []
    for ep in episodes:
        ep = dict(ep)
        sl = ep.get("source_line", 0)

        # sl already handled by sl_group; skip field-level patch
        if skip_sls and sl in skip_sls:
            result.append(ep)
            continue

        ep_key_prefix = (
            f"{name}||ep_batch"
            f"||sl{sl}"
            f"||{ep.get('供职单位', '')}"
            f"||{ep.get('职务', '')}"
            f"||{ep.get('起始时间', '')}"
        )
        vf_ep = vf_by_key.get((sl, ep.get("供职单位", ""), ep.get("职务", "")), {})

        for field in STEP1_EPISODE_FIELDS:
            cache_key = f"{ep_key_prefix}||{field}"
            decision = judge_cache.get(cache_key)
            if decision is None:
                continue
            verdict = decision.get("verdict", "")
            if verdict in ("采纳LLM2", "采纳QW"):
                vf_val = vf_ep.get(field)
                if vf_val is not None:
                    ep[field] = vf_val
            elif verdict in ("自行修正", "两者均存疑"):
                correct_val = decision.get("correct_value", "")
                if correct_val:
                    ep[field] = correct_val

        result.append(ep)
    return result


def build_merged_episodes_step1(
    official_name: str,
    ds_step1_item: dict,
    vf_step1_item: dict,
    step1_judge_cache: dict,
) -> list[dict]:
    """Stage A: merge step1 disputes → episodes with only step1 fields."""
    episodes = copy.deepcopy(ds_step1_item.get("episodes", []))
    vf_episodes_full = copy.deepcopy(vf_step1_item) if vf_step1_item else {"episodes": []}
    vf_episodes = vf_episodes_full.get("episodes", []) if isinstance(vf_episodes_full, dict) else []

    overrides = _get_sl_group_overrides(step1_judge_cache, official_name)
    if overrides:
        episodes = _apply_sl_group_overrides(
            episodes, vf_episodes, overrides, official_name
        )
    else:
        for i, ep in enumerate(episodes):
            ep["经历序号"] = i + 1

    # Apply step1 field-level overrides (ep_batch), skipping sl_group-handled lines
    vf_data = vf_episodes_full if isinstance(vf_episodes_full, dict) else {"episodes": vf_episodes}
    episodes = _apply_step1_field_overrides(
        episodes, vf_data, step1_judge_cache, official_name,
        skip_sls=set(overrides.keys()) if overrides else None,
    )

    # Strip non-step1 fields to keep merged_step1 clean
    cleaned: list[dict] = []
    for ep in episodes:
        cep = {f: ep.get(f, "") for f in STEP1_EPISODE_FIELDS}
        cep["source_line"] = ep.get("source_line", 0)
        cep["经历序号"] = ep.get("经历序号", 0)
        cleaned.append(cep)
    return cleaned


# ── Stage B: layer step2 classifications onto merged_step1 ──────────────────


def _classifications_to_map(
    cls_data: dict,
) -> dict[int, dict]:
    out: dict[int, dict] = {}
    for c in cls_data.get("classifications", []):
        idx = c.get("episode_idx", 0)
        out[idx] = c
    return out


def build_merged_episodes_full(
    official_name: str,
    merged_step1_episodes: list[dict],
    ds_step2_item: dict,
    vf_step2_item: dict,
    step2_judge_cache: dict,
) -> list[dict]:
    """Stage B: layer step2 classifications onto step1-merged episodes.

    Default to LLM1 classification, then apply step2 judge overrides per (idx, field).
    """
    ds_map = _classifications_to_map(ds_step2_item or {})
    vf_map = _classifications_to_map(vf_step2_item or {})

    result: list[dict] = []
    for ep in merged_step1_episodes:
        ep = dict(ep)
        idx = ep.get("经历序号", 0)
        ds_c = ds_map.get(idx, {})
        vf_c = vf_map.get(idx, {})

        for field in STEP2_EPISODE_FIELDS:
            # Prefer LLM1; fall back to LLM2 when LLM1 missing (e.g. content
            # moderation rejection on one side).
            val = ds_c.get(field, "")
            if not val:
                val = vf_c.get(field, "")
            ep[field] = val

        # Apply step2 judge overrides
        for field in STEP2_EPISODE_FIELDS:
            cache_key = f"{official_name}||classify||{idx}||{field}"
            decision = step2_judge_cache.get(cache_key)
            if decision is None:
                continue
            verdict = decision.get("verdict", "")
            if verdict in ("采纳LLM2", "采纳QW"):
                vf_val = vf_c.get(field)
                if vf_val:
                    ep[field] = vf_val
            elif verdict in ("自行修正", "两者均存疑"):
                correct_val = decision.get("correct_value", "")
                if correct_val:
                    ep[field] = correct_val

        result.append(ep)

    return result
