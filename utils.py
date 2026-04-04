"""
Shared utility functions for the pipeline.

Centralises JSON extraction, skill prompt loading, and LLM API calls
to eliminate duplication across api_processor_v2, verifier_v2, and battle_generator.
"""

import json
import logging
import random
import re
import threading
import time
from pathlib import Path

import httpx
from openai import OpenAI

from config import SKILLS_DIR, PROJECT_ROOT

# Longer timeouts for LLM APIs that generate long responses
_LLM_TIMEOUT = httpx.Timeout(
    connect=15.0,   # connection establishment
    read=120.0,     # reading response body (long generation)
    write=30.0,     # sending request
    pool=30.0,      # waiting for connection from pool
)

logger = logging.getLogger(__name__)


# ── JSON extraction from LLM output ──────────────────────────────────────────

def extract_json(text: str) -> dict:
    """
    Clean up LLM output (strip ```json fences) and parse as JSON dict.
    Falls back to regex extraction if direct parse fails.
    """
    text = re.sub(r"^```(?:json)?\s*", "", text.strip())
    text = re.sub(r"\s*```$", "", text.strip())
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    # Try extracting the first JSON object
    m = re.search(r"\{.*\}", text, re.DOTALL)
    if m:
        return json.loads(m.group(0))
    raise ValueError(f"Cannot parse JSON from LLM output: {text[:200]}")


# ── Skill prompt loader ──────────────────────────────────────────────────────

def load_skill_prompt(skill_name: str) -> str:
    """Read .claude/skills/{skill_name}.md, strip YAML frontmatter."""
    path = SKILLS_DIR / f"{skill_name}.md"
    if not path.exists():
        raise FileNotFoundError(f"Skill prompt not found: {path}")
    content = path.read_text(encoding="utf-8")
    return re.sub(r"^---.*?---\s*", "", content, flags=re.DOTALL).strip()


def load_prompt(prompt_name: str) -> str:
    """Read prompts/{prompt_name}.md (no YAML frontmatter stripping needed)."""
    path = PROJECT_ROOT / "prompts" / f"{prompt_name}.md"
    if not path.exists():
        raise FileNotFoundError(f"Prompt file not found: {path}")
    return path.read_text(encoding="utf-8").strip()


# ── Prefix cache stats logging ───────────────────────────────────────────────

def _log_cache_stats(usage, model: str) -> None:
    """Log DeepSeek prefix cache hit/miss stats (if available)."""
    hit = getattr(usage, "prompt_cache_hit_tokens", None)
    miss = getattr(usage, "prompt_cache_miss_tokens", None)
    if hit is not None and miss is not None:
        total = hit + miss
        rate = (hit / total * 100) if total > 0 else 0
        logger.debug(f"[Cache] {model}: hit={hit} miss={miss} rate={rate:.0f}%")
    # Also check dict-style access (some SDKs)
    elif isinstance(usage, dict):
        hit = usage.get("prompt_cache_hit_tokens")
        miss = usage.get("prompt_cache_miss_tokens")
        if hit is not None and miss is not None:
            total = hit + miss
            rate = (hit / total * 100) if total > 0 else 0
            logger.debug(f"[Cache] {model}: hit={hit} miss={miss} rate={rate:.0f}%")


# ── Generic LLM chat call with retry + 429 backoff ──────────────────────────

def llm_chat(
    client,
    model: str,
    system: str,
    user: str,
    temperature: float = 0.1,
    max_tokens: int = 8000,
    max_retries: int = 2,
    extra_body: dict | None = None,
    seed: int | None = 42,
    stream: bool = True,
) -> str:
    """
    Call an OpenAI-compatible chat API with automatic retry and 429 backoff.

    Args:
        seed: Fixed seed for reproducibility. Set to None to disable.
              Note: Kimi K2.5 (reasoning model) may ignore seed.
        stream: Use streaming mode (keeps connection alive, prevents server-side
                timeouts during long generation). Default True.

    Returns the raw text content of the assistant's response.
    Raises on final failure after all retries.
    """
    for attempt in range(max_retries + 1):
        try:
            kwargs = {
                "model": model,
                "messages": [
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ],
                "temperature": temperature,
                "max_tokens": max_tokens,
                "stream": stream,
            }
            if seed is not None:
                kwargs["seed"] = seed
            if extra_body:
                kwargs["extra_body"] = extra_body

            if stream:
                # Streaming: collect chunks to avoid server-side connection timeout
                chunks = []
                stream_resp = client.chat.completions.create(**kwargs)
                for chunk in stream_resp:
                    if chunk.choices and chunk.choices[0].delta.content:
                        chunks.append(chunk.choices[0].delta.content)
                    # Log prefix cache hit stats from final chunk (DeepSeek feature)
                    if chunk.usage:
                        _log_cache_stats(chunk.usage, model)
                return "".join(chunks)
            else:
                resp = client.chat.completions.create(**kwargs)
                usage = resp.usage
                if usage:
                    _log_cache_stats(usage, model)
                return resp.choices[0].message.content
        except Exception as e:
            err_str = str(e).lower()
            if attempt < max_retries:
                # Exponential backoff with jitter to prevent thundering herd
                if "429" in err_str or "rate" in err_str:
                    base_wait = 10 * (attempt + 1)
                    jitter = random.uniform(0, base_wait * 0.5)
                    wait = base_wait + jitter
                    logger.warning(f"[Rate limit] attempt {attempt+1}, wait {wait:.1f}s: {e}")
                else:
                    base_wait = 3 * (attempt + 1)
                    jitter = random.uniform(0, base_wait * 0.5)
                    wait = base_wait + jitter
                    logger.warning(f"[Retry {attempt+1}] {e} — wait {wait:.1f}s")
                time.sleep(wait)
            else:
                raise


# ── Round-robin client pool for multi-key concurrency ────────────────────────

class RoundRobinClientPool:
    """
    Thread-safe pool of OpenAI clients, one per API key.
    Each call to `next_client()` returns the next client in round-robin order.
    """

    def __init__(self, api_keys: list[str], base_url: str):
        if not api_keys:
            raise ValueError("RoundRobinClientPool requires at least 1 API key")
        self._clients = [
            OpenAI(api_key=key, base_url=base_url, timeout=_LLM_TIMEOUT)
            for key in api_keys
        ]
        self._index = 0
        self._lock = threading.Lock()
        logger.info(f"RoundRobinClientPool: {len(self._clients)} clients for {base_url}")

    def next_client(self) -> OpenAI:
        """Return the next client in round-robin order (thread-safe)."""
        with self._lock:
            client = self._clients[self._index % len(self._clients)]
            self._index += 1
            return client

    @property
    def size(self) -> int:
        return len(self._clients)

    @property
    def first_client(self) -> OpenAI:
        """Return the first client (for single-client backward compatibility)."""
        return self._clients[0]


class SmoothRateLimiter:
    """
    Thread-safe sliding-window rate limiter for RPM smoothing.
    Ensures requests are evenly distributed across time windows.
    """

    def __init__(self, rpm_limit: int = 500, tpm_limit: int = 3_000_000):
        self._rpm_limit = rpm_limit
        self._tpm_limit = tpm_limit
        self._interval = 60.0 / rpm_limit  # seconds between requests
        self._lock = threading.Lock()
        self._last_request_time = 0.0
        self._token_window: list[tuple[float, int]] = []  # (timestamp, token_count)
        self._window_seconds = 60.0

    def acquire(self, estimated_tokens: int = 0) -> None:
        """Block until it's safe to make the next request (RPM + TPM)."""
        with self._lock:
            now = time.monotonic()

            # RPM smoothing: ensure minimum interval between requests
            elapsed = now - self._last_request_time
            if elapsed < self._interval:
                wait = self._interval - elapsed
                time.sleep(wait)
                now = time.monotonic()

            # TPM check: sliding window of token usage
            if estimated_tokens > 0 and self._tpm_limit > 0:
                cutoff = now - self._window_seconds
                self._token_window = [
                    (t, c) for t, c in self._token_window if t > cutoff
                ]
                current_tokens = sum(c for _, c in self._token_window)
                if current_tokens + estimated_tokens > self._tpm_limit:
                    # Wait until oldest entries expire
                    if self._token_window:
                        oldest = self._token_window[0][0]
                        wait = oldest + self._window_seconds - now + 0.1
                        if wait > 0:
                            time.sleep(wait)
                            now = time.monotonic()
                            cutoff = now - self._window_seconds
                            self._token_window = [
                                (t, c) for t, c in self._token_window if t > cutoff
                            ]
                self._token_window.append((now, estimated_tokens))

            self._last_request_time = now

    def report_tokens(self, actual_tokens: int) -> None:
        """Update token window with actual usage (call after response)."""
        with self._lock:
            now = time.monotonic()
            # Replace the last estimated entry with actual
            if self._token_window:
                self._token_window[-1] = (now, actual_tokens)
