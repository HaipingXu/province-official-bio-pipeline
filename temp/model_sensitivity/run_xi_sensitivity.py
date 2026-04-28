"""Run a live sensitivity comparison for Xi Jinping using real Zhejiang pipeline logs."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import (  # noqa: E402
    DEEPSEEK_API_KEY,
    DEEPSEEK_BASE_URL,
    DOUBAO_API_KEY,
    DOUBAO_BASE_URL,
    QWEN_API_KEY,
    QWEN_BASE_URL,
)
from extraction import _detect_refs  # noqa: E402
from text_preprocessor import format_career_lines_for_llm  # noqa: E402
from utils import RoundRobinClientPool, extract_json, llm_chat, load_json_cache, load_prompt  # noqa: E402


OFFICIAL_NAME = "习近平"
CITY = ""
PROVINCE = "浙江省"
ROLE = "省长/省委书记"
STEP_MAX_TOKENS = {
    "step1": 3000,
    "step2": 2500,
    "step3": 1200,
    "step4": 1800,
}
TARGET_SOURCE_LINES = {13, 14, 15, 16, 17}
LOGS_DIR = ROOT / "logs" / "浙江"
REPORT_PATH = Path(__file__).resolve().parent / "xi_model_sensitivity_report.json"

MODELS = [
    {
        "label": "deepseek-v4-pro",
        "base_url": DEEPSEEK_BASE_URL,
        "api_key": DEEPSEEK_API_KEY,
        "model": "deepseek-v4-pro",
        "extra_body": {"thinking": {"type": "disabled"}},
    },
    {
        "label": "deepseek-v4-flash",
        "base_url": DEEPSEEK_BASE_URL,
        "api_key": DEEPSEEK_API_KEY,
        "model": "deepseek-v4-flash",
        "extra_body": {"thinking": {"type": "disabled"}},
    },
    {
        "label": "doubao",
        "base_url": DOUBAO_BASE_URL,
        "api_key": DOUBAO_API_KEY,
        "model": "doubao-seed-2-0-pro-260215",
        "extra_body": None,
    },
    {
        "label": "qwen",
        "base_url": QWEN_BASE_URL,
        "api_key": QWEN_API_KEY,
        "model": "qwen3.6-plus",
        "extra_body": None,
    },
]


def _load_preprocessed() -> dict:
    data = json.loads((LOGS_DIR / "preprocessed_texts.json").read_text(encoding="utf-8"))
    return data[OFFICIAL_NAME]


def _load_samples() -> dict:
    preprocessed = _load_preprocessed()
    merged_step1 = load_json_cache(LOGS_DIR / "merged_episodes_step1.json")
    merged_full = load_json_cache(LOGS_DIR / "merged_episodes.json")
    focused_preprocessed = dict(preprocessed)
    focused_preprocessed["career_lines"] = [
        row for row in preprocessed["career_lines"]
        if row.get("line_num") in TARGET_SOURCE_LINES
    ]
    focused_preprocessed["total_lines"] = len(focused_preprocessed["career_lines"])

    focused_step2_episodes = [
        ep for ep in merged_step1[OFFICIAL_NAME]["episodes"]
        if ep.get("source_line") in TARGET_SOURCE_LINES
    ]
    focused_step34_episodes = [
        ep for ep in merged_full[OFFICIAL_NAME]["episodes"]
        if ep.get("source_line") in TARGET_SOURCE_LINES
    ]
    return {
        "preprocessed": focused_preprocessed,
        "step2_episodes": focused_step2_episodes,
        "step34_episodes": focused_step34_episodes,
    }


def _build_step1_payload(preprocessed: dict) -> tuple[str, str]:
    system_prompt = load_prompt("step1_extraction")
    career_text = format_career_lines_for_llm(preprocessed["career_lines"])
    location = f"{PROVINCE}{CITY}市" if CITY else PROVINCE
    user_prompt = (
        f"官员：{OFFICIAL_NAME}，{location}{ROLE}\n\n"
        f"=== 编号履历行（共{preprocessed['total_lines']}行）===\n"
        f"{career_text}\n\n"
        "请将每行转化为最小事实条目，仅输出 source_line / 起始时间 / 终止时间 / 供职单位 / 职务 五个字段，纯JSON。"
    )
    return system_prompt, user_prompt


def _build_step2_payload(episodes: list[dict]) -> tuple[str, str]:
    system_prompt = load_prompt("step2_classify")
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
        f"官员：{OFFICIAL_NAME}\n\n"
        f"=== Episodes (Step1 已固定) ===\n{ep_text}\n\n"
        "请对每条 episode 输出 episode_idx + 组织标签 + 标志位 + 任职地（省）+ 任职地（市）+ 中央/地方，纯JSON。"
    )
    return effective_sys, user_prompt


def _build_step3_payload(episodes: list[dict]) -> tuple[str, str]:
    system_prompt = load_prompt("step3_rank")
    ep_lines = []
    for i, ep in enumerate(episodes, 1):
        unit = ep.get("供职单位", "")
        pos = ep.get("职务", "")
        ep_lines.append(f"  {i}. 供职单位: {unit}  职务: {pos}")
    ep_text = "\n".join(ep_lines)
    ref_extra = _detect_refs(ep_text)
    effective_sys = system_prompt + ref_extra if ref_extra else system_prompt
    user_prompt = (
        f"官员：{OFFICIAL_NAME}\n\n"
        f"以下是该官员的全部 {len(episodes)} 段职务经历：\n{ep_text}\n\n"
        "请对每段经历判断行政级别，输出纯JSON。"
        "无法定级的早期/秘书/干部条目请填 \"难以判断\"。"
    )
    return effective_sys, user_prompt


def _build_step4_payload(preprocessed: dict, episodes: list[dict]) -> tuple[str, str]:
    system_prompt = load_prompt("step4_labeling")
    episodes_json = json.dumps(episodes, ensure_ascii=False, indent=2)
    user_prompt = (
        f"官员：{OFFICIAL_NAME}\n"
        f"目标省份：{PROVINCE}\n"
        f"职务：{ROLE}\n\n"
        f"=== 人物简介 ===\n{preprocessed.get('bio_summary', '')}\n\n"
        f"=== 完整履历（共{len(episodes)}条）===\n{episodes_json}\n\n"
    )
    if CITY:
        user_prompt = (
            f"官员：{OFFICIAL_NAME}\n"
            f"目标城市：{CITY}\n"
            f"目标省份：{PROVINCE}\n"
            f"职务：{ROLE}\n\n"
            f"=== 人物简介 ===\n{preprocessed.get('bio_summary', '')}\n\n"
            f"=== 完整履历（共{len(episodes)}条）===\n{episodes_json}\n\n"
        )
    corruption_text = preprocessed.get("corruption_text", "")
    if corruption_text:
        user_prompt += f"=== 落马相关信息 ===\n{corruption_text}\n\n"
    user_prompt += "请输出 raw_bio + 三个标签 + 是否落马/落马原因 的纯JSON。"
    return system_prompt, user_prompt


def _validate(step_name: str, result: dict) -> None:
    if step_name == "step1":
        if "episodes" not in result:
            raise ValueError("Missing 'episodes'")
        if not isinstance(result["episodes"], list):
            raise ValueError("'episodes' must be a list")
        return

    if step_name == "step2":
        if not isinstance(result.get("classifications", []), list):
            raise ValueError("'classifications' must be a list")
        return

    if step_name == "step3":
        if not isinstance(result.get("ranks", []), list):
            raise ValueError("'ranks' must be a list")
        return

    if step_name == "step4":
        if "raw_bio" not in result:
            raise ValueError("Missing 'raw_bio'")
        required = {
            "升迁_省长", "升迁_省委书记",
            "本省提拔", "本省提拔依据",
            "本省学习", "本省学习依据",
        }
        missing = required - set(result.keys())
        if missing:
            raise ValueError(f"Missing keys: {missing}")
        return

    raise ValueError(f"Unknown step: {step_name}")


def _classify_error(exc: Exception) -> tuple[str, bool]:
    text = str(exc)
    lowered = text.lower()
    blocked_markers = [
        "content exists risk",
        "datainspectionfailed",
        "inappropriate content",
        "content policy",
        "safety",
        "sensitive",
        "risk control",
    ]
    if any(marker in lowered for marker in blocked_markers):
        return "blocked", True
    if "cannot parse json" in lowered:
        return "parse_error", False
    if "missing 'episodes'" in lowered or "must be a list" in lowered or "missing keys:" in lowered:
        return "schema_error", False
    return "api_error", False


def _run_one(model_spec: dict, step_name: str, system_prompt: str, user_prompt: str) -> dict:
    pool = RoundRobinClientPool([model_spec["api_key"]], model_spec["base_url"])
    try:
        raw = llm_chat(
            pool.next_client(),
            model_spec["model"],
            system_prompt,
            user_prompt,
            temperature=0.1,
            max_tokens=STEP_MAX_TOKENS[step_name],
            max_retries=1,
            extra_body=model_spec.get("extra_body"),
            seed=42,
            stream=True,
        )
        parsed = extract_json(raw)
        _validate(step_name, parsed)
        return {
            "model": model_spec["label"],
            "model_name": model_spec["model"],
            "step": step_name,
            "status": "success",
            "content_sensitive": False,
            "response_chars": len(raw),
            "preview": raw[:160],
        }
    except Exception as exc:
        status, content_sensitive = _classify_error(exc)
        return {
            "model": model_spec["label"],
            "model_name": model_spec["model"],
            "step": step_name,
            "status": status,
            "content_sensitive": content_sensitive,
            "error": str(exc),
        }


def _write_report(results: list[dict]) -> None:
    report = {
        "official": OFFICIAL_NAME,
        "source_logs": str(LOGS_DIR.relative_to(ROOT)),
        "focused_source_lines": sorted(TARGET_SOURCE_LINES),
        "results": results,
        "summary": _summarise(results),
    }
    REPORT_PATH.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")


def _summarise(results: list[dict]) -> dict:
    summary: dict[str, dict] = {}
    for item in results:
        slot = summary.setdefault(item["model"], {
            "success": 0,
            "blocked": 0,
            "other_failures": 0,
            "steps": {},
        })
        slot["steps"][item["step"]] = item["status"]
        if item["status"] == "success":
            slot["success"] += 1
        elif item["status"] == "blocked":
            slot["blocked"] += 1
        else:
            slot["other_failures"] += 1
    return summary


def _load_existing_results() -> list[dict]:
    if not REPORT_PATH.exists():
        return []
    try:
        report = json.loads(REPORT_PATH.read_text(encoding="utf-8"))
        results = report.get("results", [])
        return results if isinstance(results, list) else []
    except Exception:
        return []


def _merge_results(existing: list[dict], incoming: list[dict]) -> list[dict]:
    merged: dict[tuple[str, str], dict] = {}
    for item in existing + incoming:
        merged[(item["model"], item["step"])] = item
    ordered = sorted(merged.values(), key=lambda item: (item["model"], item["step"]))
    return ordered


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", choices=["all"] + [spec["label"] for spec in MODELS], default="all")
    parser.add_argument("--step", choices=["all", "step1", "step2", "step3", "step4"], default="all")
    parser.add_argument("--reset", action="store_true")
    args = parser.parse_args()

    samples = _load_samples()
    payloads = {
        "step1": _build_step1_payload(samples["preprocessed"]),
        "step2": _build_step2_payload(samples["step2_episodes"]),
        "step3": _build_step3_payload(samples["step34_episodes"]),
        "step4": _build_step4_payload(samples["preprocessed"], samples["step34_episodes"]),
    }

    selected_models = MODELS if args.model == "all" else [
        spec for spec in MODELS if spec["label"] == args.model
    ]
    missing = [spec["label"] for spec in selected_models if not spec["api_key"]]
    if missing:
        print(f"Missing API keys for: {', '.join(missing)}")
        return 2
    selected_steps = ("step1", "step2", "step3", "step4") if args.step == "all" else (args.step,)

    existing_results = [] if args.reset else _load_existing_results()
    new_results = []
    for model_spec in selected_models:
        for step_name in selected_steps:
            print(f"[{model_spec['label']}] {step_name} ...", flush=True)
            system_prompt, user_prompt = payloads[step_name]
            result = _run_one(model_spec, step_name, system_prompt, user_prompt)
            new_results.append(result)
            _write_report(_merge_results(existing_results, new_results))
            print(f"  -> {result['status']}", flush=True)

    report = json.loads(REPORT_PATH.read_text(encoding="utf-8"))
    print(f"report saved to {REPORT_PATH}")
    print(json.dumps(report["summary"], ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())