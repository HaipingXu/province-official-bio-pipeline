"""
models.py — Model registry for eval framework.

All models are configured with OpenAI-compatible APIs.
Models can be tested via BLTCY proxy or direct provider APIs.
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass, field
from pathlib import Path

# Add parent to path so we can import from the main pipeline
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")

from openai import OpenAI
import httpx

_LLM_TIMEOUT = httpx.Timeout(connect=15.0, read=180.0, write=30.0, pool=60.0)
_LLM_TIMEOUT_SLOW = httpx.Timeout(connect=15.0, read=600.0, write=30.0, pool=60.0)


@dataclass
class EvalModel:
    """Configuration for a single model in eval."""
    model_id: str           # Model identifier (passed to API)
    display_name: str       # Short label for reports
    base_url: str           # API base URL
    api_key: str            # API key
    provider: str           # "bltcy" | "deepseek" | "dashscope" | "kimi"
    extra_body: dict | None = None   # Provider-specific extra params
    max_tokens: int | None = None
    response_format: dict | None = None
    notes: str = ""         # E.g. "reasoning model"
    slow: bool = False      # Use extended 600s read timeout (for large-batch slow models)
    max_ep_per_call: int = 0  # If > 0, split calls with >N episodes into chunks (0 = no limit)

    def client(self) -> OpenAI:
        timeout = _LLM_TIMEOUT_SLOW if self.slow else _LLM_TIMEOUT
        return OpenAI(
            api_key=self.api_key,
            base_url=self.base_url,
            timeout=timeout,
        )


def _k(env_var: str) -> str:
    """Get first key from comma-separated env var, empty string if missing."""
    raw = os.environ.get(env_var, "")
    keys = [k.strip() for k in raw.split(",") if k.strip()]
    return keys[0] if keys else ""


# ── Provider base URLs ────────────────────────────────────────────────────────

BLTCY_URL         = "https://api.bltcy.ai/v1"
CLARIONTECH_URL   = "http://token.clariontech.top/v1"
DEEPSEEK_URL      = "https://api.deepseek.com"
DASHSCOPE_URL     = "https://dashscope.aliyuncs.com/compatible-mode/v1"
KIMI_URL          = "https://api.moonshot.cn/v1"
VOLCENGINE_URL    = "https://ark.cn-beijing.volces.com/api/v3"

# ── Model registry ────────────────────────────────────────────────────────────

def get_all_models() -> list[EvalModel]:
    """Return list of all models to test."""
    bltcy_key   = _k("BLTCY_API_KEYS")
    ct_key      = _k("CLARIONTECH_API_KEYS")
    ds_key      = _k("DEEPSEEK_API_KEY")
    qs_key      = _k("QWEN_API_KEY")
    kimi_key    = _k("KIMI_API_KEY")

    models = []

    # ── OpenAI / GPT (via BLTCY) ─────────────────────────────────────────────
    if bltcy_key:
        models += [
            EvalModel("gpt-5",          "GPT-5",          BLTCY_URL, bltcy_key, "bltcy"),
            EvalModel("gpt-5.4",        "GPT-5.4",        BLTCY_URL, bltcy_key, "bltcy"),
            EvalModel("gpt-5.4-mini",   "GPT-5.4-mini",   BLTCY_URL, bltcy_key, "bltcy"),
            # EvalModel("gpt-5.3",      "GPT-5.3",        BLTCY_URL, bltcy_key, "bltcy"),
        ]

    # ── Claude (via BLTCY) ────────────────────────────────────────────────────
    if bltcy_key:
        models += [
            EvalModel("claude-sonnet-4-6",  "Claude-Sonnet-4.6", BLTCY_URL, bltcy_key, "bltcy"),
            EvalModel("claude-opus-4-6",    "Claude-Opus-4.6",   BLTCY_URL, bltcy_key, "bltcy"),
        ]

    # ── Gemini (via BLTCY) ────────────────────────────────────────────────────
    # Note: gemini-2.0-pro/flash unavailable on BLTCY as of 2026-05
    if bltcy_key:
        models += [
            EvalModel("gemini-2.5-pro",   "Gemini-2.5-Pro",   BLTCY_URL, bltcy_key, "bltcy"),
            EvalModel("gemini-2.5-flash", "Gemini-2.5-Flash", BLTCY_URL, bltcy_key, "bltcy"),
            # EvalModel("gemini-2.0-pro",   "Gemini-2.0-Pro",   BLTCY_URL, bltcy_key, "bltcy"),
            # EvalModel("gemini-2.0-flash", "Gemini-2.0-Flash", BLTCY_URL, bltcy_key, "bltcy"),
        ]

    # ── DeepSeek (direct API — more reliable than BLTCY for DeepSeek) ─────────
    if ds_key:
        models += [
            EvalModel("deepseek-v4-flash", "DS-V4-Flash", DEEPSEEK_URL, ds_key, "deepseek"),
            EvalModel("deepseek-v4-pro",   "DS-V4-Pro",   DEEPSEEK_URL, ds_key, "deepseek",
                      extra_body={"cache_ttl": 3600}),
        ]

    # ── Qwen (DashScope) ──────────────────────────────────────────────────────
    if qs_key:
        models += [
            EvalModel("qwen3.5-plus", "Qwen3.5-Plus", DASHSCOPE_URL, qs_key, "dashscope",
                      extra_body={"enable_thinking": False}),
        ]

    # ── Clariontech (GPT-5.2, Claude Haiku/Opus) ─────────────────────────────
    if ct_key:
        models += [
            EvalModel("gpt-5.2",                    "GPT-5.2",         CLARIONTECH_URL, ct_key, "clariontech"),
            EvalModel("claude-haiku-4-5-20251001",  "Claude-Haiku-4.5", CLARIONTECH_URL, ct_key, "clariontech"),
            EvalModel("claude-opus-4-6",            "Claude-Opus-4.6-CT", CLARIONTECH_URL, ct_key, "clariontech"),
        ]

    # ── Kimi (optional extra) ─────────────────────────────────────────────────
    # Kimi-K2.5 requires temperature=1; API has ~5-min gateway timeout for large batches.
    # max_ep_per_call=40 splits large officials (e.g., 习近平 68ep) into ≤40-ep chunks.
    if kimi_key:
        models += [
            EvalModel("kimi-k2.5", "Kimi-K2.5", KIMI_URL, kimi_key, "kimi",
                      notes="temperature must be 1", slow=True, max_ep_per_call=20),
        ]

    return models


def get_model_subset(names: list[str]) -> list[EvalModel]:
    """Return only models whose display_name or model_id matches any name in list."""
    all_m = get_all_models()
    name_set = set(n.lower() for n in names)
    return [m for m in all_m
            if m.model_id.lower() in name_set or m.display_name.lower() in name_set]


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    models = get_all_models()
    print(f"Total eval models: {len(models)}")
    for m in models:
        key_preview = m.api_key[:8] + "..." if m.api_key else "(MISSING)"
        print(f"  [{m.provider:10s}] {m.display_name:20s} → {m.model_id} (key={key_preview})")
