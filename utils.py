"""
Shared utility functions for the pipeline.

Centralises JSON extraction, skill prompt loading, and LLM API calls
to eliminate duplication across extraction, diff, judge, and postprocess modules.
"""

import json
import logging
import os
import random
import re
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path

import httpx
from openai import OpenAI, AuthenticationError, BadRequestError

from config import (
    SKILLS_DIR, PROJECT_ROOT, LOGS_DIR, RANK_LEVELS,
    LLM_429_BACKOFF_BASE, LLM_429_BACKOFF_FACTOR, LLM_429_BACKOFF_CAP,
    LLM_GENERIC_BACKOFF_BASE,
)

# Longer timeouts for LLM APIs that generate long responses
_LLM_TIMEOUT = httpx.Timeout(
    connect=15.0,   # connection establishment
    read=180.0,     # reading response body (long generation)
    write=30.0,     # sending request
    pool=60.0,      # waiting for connection from pool
)

logger = logging.getLogger(__name__)


# ── Global token counter (thread-safe) ───────────────────────────────────────

class TokenCounter:
    """Accumulate token usage across all llm_chat calls (thread-safe)."""

    def __init__(self):
        self._lock = threading.Lock()
        self._data: dict[str, dict[str, int]] = {}  # model → {input, output, calls}

    def add(self, model: str, input_tokens: int, output_tokens: int) -> None:
        with self._lock:
            if model not in self._data:
                self._data[model] = {"input": 0, "output": 0, "calls": 0}
            self._data[model]["input"] += input_tokens
            self._data[model]["output"] += output_tokens
            self._data[model]["calls"] += 1

    def snapshot(self) -> dict[str, dict[str, int]]:
        """Return a deep copy of current totals."""
        with self._lock:
            return {m: dict(v) for m, v in self._data.items()}

    def delta(self, before: dict, after: dict) -> dict[str, dict[str, int]]:
        """Compute per-model deltas between two snapshots."""
        result = {}
        all_models = set(before) | set(after)
        for m in all_models:
            b = before.get(m, {"input": 0, "output": 0, "calls": 0})
            a = after.get(m, {"input": 0, "output": 0, "calls": 0})
            d = {
                "input": a["input"] - b["input"],
                "output": a["output"] - b["output"],
                "calls": a["calls"] - b["calls"],
            }
            if d["calls"] > 0:
                result[m] = d
        return result

    def summary_str(self, delta: dict) -> str:
        """Format delta dict as compact string."""
        if not delta:
            return "0 calls"
        parts = []
        for model, d in delta.items():
            short = model.split("/")[-1][:30]
            parts.append(
                f"{short}: {d['calls']}次 "
                f"in={d['input']:,} out={d['output']:,} "
                f"total={d['input']+d['output']:,}"
            )
        return " | ".join(parts)


TOKENS = TokenCounter()


# ── JSON extraction from LLM output ──────────────────────────────────────────

def extract_json(text: str) -> dict:
    """
    Clean up LLM output (strip ```json fences) and parse as JSON dict.
    Falls back to json_repair for malformed LLM output (missing commas, etc.).
    """
    text = re.sub(r"^```(?:json)?\s*", "", text.strip())
    text = re.sub(r"\s*```$", "", text.strip())
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    # Try extracting the first JSON object block, then repair
    m = re.search(r"\{.*\}", text, re.DOTALL)
    candidate = m.group(0) if m else text
    try:
        from json_repair import repair_json
        repaired = repair_json(candidate, return_objects=True)
        if isinstance(repaired, dict):
            # P8: surface the fact that json_repair masked a parse error so we
            # can spot prompts that consistently produce malformed JSON.
            logger.warning(
                f"[JSON repair] fallback used; head={text[:120]!r}"
            )
            return repaired
    except Exception:
        pass
    # Last resort: strict parse of extracted block (raises if still broken)
    if m:
        return json.loads(candidate)
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
    """Log DeepSeek prefix cache hit/miss stats and accumulate global token counts."""
    # Accumulate token counts
    if hasattr(usage, "prompt_tokens") and usage.prompt_tokens is not None:
        inp = usage.prompt_tokens or 0
        out = getattr(usage, "completion_tokens", None) or 0
        if inp > 0 or out > 0:
            TOKENS.add(model, inp, out)
    elif isinstance(usage, dict):
        inp = usage.get("prompt_tokens", 0) or 0
        out = usage.get("completion_tokens", 0) or 0
        if inp > 0 or out > 0:
            TOKENS.add(model, inp, out)
    # DeepSeek fallback: derive prompt_tokens from cache fields if standard fields missing
    else:
        hit = getattr(usage, "prompt_cache_hit_tokens", None)
        miss = getattr(usage, "prompt_cache_miss_tokens", None)
        if hit is not None and miss is not None:
            TOKENS.add(model, hit + miss, 0)

    # Cache hit/miss logging
    hit = getattr(usage, "prompt_cache_hit_tokens", None)
    miss = getattr(usage, "prompt_cache_miss_tokens", None)
    if hit is not None and miss is not None:
        total = hit + miss
        rate = (hit / total * 100) if total > 0 else 0
        logger.debug(f"[Cache] {model}: hit={hit} miss={miss} rate={rate:.0f}%")
    elif isinstance(usage, dict):
        hit = usage.get("prompt_cache_hit_tokens")
        miss = usage.get("prompt_cache_miss_tokens")
        if hit is not None and miss is not None:
            total = hit + miss
            rate = (hit / total * 100) if total > 0 else 0
            logger.debug(f"[Cache] {model}: hit={hit} miss={miss} rate={rate:.0f}%")


# ── Generic LLM chat call with retry + 429 backoff ──────────────────────────


def _parse_retry_after(exc: Exception) -> float | None:
    """Extract Retry-After header from an OpenAI/httpx exception, if present.

    Most providers (DashScope, DeepSeek, Volcengine, Kimi) return a numeric
    Retry-After header on 429. We honour it instead of guessing.
    Returns None if header missing or unparseable.
    """
    resp = getattr(exc, "response", None)
    if resp is None:
        return None
    headers = getattr(resp, "headers", None)
    if headers is None:
        return None
    try:
        ra = headers.get("retry-after") or headers.get("Retry-After")
    except Exception:
        return None
    if not ra:
        return None
    try:
        return float(ra)
    except (TypeError, ValueError):
        # HTTP-date format is rare for these APIs — give up
        return None


def _is_rate_limited(exc: Exception) -> bool:
    s = str(exc).lower()
    if "429" in s or "rate" in s or "throttl" in s or "too many requests" in s:
        return True
    status = getattr(getattr(exc, "response", None), "status_code", None)
    return status == 429


# Vendor-specific 400 errors that are deterministic (re-issuing the same prompt
# produces the same rejection). Retrying just wastes minutes per occurrence.
# Examples:
#   - Qwen / DashScope: "<400> InternalError.Algo.DataInspectionFailed"
#                       "Output data may contain inappropriate content"
#   - DeepSeek / Volcengine / Doubao: "Content Exists Risk", "content_filter"
#   - OpenAI-style:     "content_policy_violation"
_NON_RETRYABLE_PATTERNS = (
    "datainspectionfailed",
    "data_inspection_failed",
    "internalerror.algo",
    "inappropriate content",
    "content exists risk",
    "content_filter",
    "content_policy_violation",
    "sensitive_words",
    "risk_content",
)


def _is_non_retryable_vendor_error(exc: Exception) -> bool:
    """Detect deterministic vendor 400s (content moderation / data inspection).

    These responses are not transient — retrying the identical request will
    always produce the same rejection, so we should fail fast and let the
    failure tracker record it.
    """
    s = str(exc).lower()
    return any(p in s for p in _NON_RETRYABLE_PATTERNS)


def _compute_backoff(exc: Exception, attempt: int) -> tuple[float, str]:
    """Return (wait_seconds, label) for given exception + zero-based attempt."""
    if _is_rate_limited(exc):
        ra = _parse_retry_after(exc)
        if ra is not None and ra > 0:
            wait = min(ra + random.uniform(0, 2.0), LLM_429_BACKOFF_CAP)
            return wait, f"rate(Retry-After={ra:.1f}s)"
        # Exponential: 10s, 30s, 90s, ... capped at LLM_429_BACKOFF_CAP
        base = LLM_429_BACKOFF_BASE * (LLM_429_BACKOFF_FACTOR ** attempt)
        base = min(base, LLM_429_BACKOFF_CAP)
        wait = base + random.uniform(0, base * 0.3)
        return wait, "rate(exp)"
    # Generic 5xx / network: linear with jitter
    base = LLM_GENERIC_BACKOFF_BASE * (attempt + 1)
    return base + random.uniform(0, base * 0.5), "generic"


def llm_chat(
    client_or_pool,
    model: str,
    system: str,
    user: str,
    temperature: float = 0.1,
    max_tokens: int | None = None,
    max_retries: int = 3,
    extra_body: dict | None = None,
    seed: int | None = 42,
    stream: bool = False,
    response_format: dict | None = None,
    safety_fallback_pool=None,
    safety_fallback_model: str = "",            # kept for backward compat (single model)
    safety_fallback_models: list[str] | None = None,  # cascade (takes priority)
) -> str:
    """Call an OpenAI-compatible chat API with retry, 429 backoff, and an
    optional provider-wide concurrency cap.

    Args:
        client_or_pool: Either a single OpenAI client OR a RoundRobinClientPool.
            When a pool is passed, each attempt:
              - acquires the pool's semaphore for the duration of the API call
                (provider-wide concurrency cap, see RoundRobinClientPool),
              - picks the next client round-robin (so a retry rotates keys).
        seed: Fixed seed for reproducibility. Set to None to disable.
              On retry the seed is offset by `attempt` to escape sticky bad
              outputs (Kimi K2.5 / reasoning models may ignore seed entirely).
        stream: Streaming mode keeps the HTTP connection alive during long
                generation (avoids server-side idle timeouts).
        max_retries: Number of *additional* attempts after the first failure.
                     Default 3 → up to 4 total attempts.

    Returns the raw text content of the assistant's response.
    Raises on final failure after all retries.
    """
    is_pool = isinstance(client_or_pool, RoundRobinClientPool)

    for attempt in range(max_retries + 1):
        # Pick client per-attempt so retries rotate to a different API key.
        client = (
            client_or_pool.next_client() if is_pool else client_or_pool
        )
        # On retry, perturb seed to escape deterministic bad outputs.
        attempt_seed = (
            seed + attempt if (seed is not None and attempt > 0) else seed
        )
        kwargs = {
            "model": model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "temperature": temperature,
            "stream": stream,
        }
        if max_tokens is not None:
            kwargs["max_tokens"] = max_tokens
        if attempt_seed is not None:
            kwargs["seed"] = attempt_seed
        if extra_body:
            kwargs["extra_body"] = extra_body
        if response_format is not None:
            kwargs["response_format"] = response_format
        if stream:
            kwargs["stream_options"] = {"include_usage": True}

        # Provider-wide concurrency cap: only held during the actual API call,
        # released during sleep/backoff so other workers can take the slot.
        sem = getattr(client_or_pool, "_sem", None) if is_pool else None
        try:
            if sem is not None:
                sem.acquire()
            try:
                if stream:
                    chunks = []
                    stream_resp = client.chat.completions.create(**kwargs)
                    for chunk in stream_resp:
                        if chunk.choices and chunk.choices[0].delta.content:
                            chunks.append(chunk.choices[0].delta.content)
                        if chunk.usage:
                            _log_cache_stats(chunk.usage, model)
                    result = "".join(chunks)
                    if not result:
                        raise ValueError(
                            f"LLM returned empty response (streaming, model={model})"
                        )
                    return result
                else:
                    resp = client.chat.completions.create(**kwargs)
                    usage = resp.usage
                    if usage:
                        _log_cache_stats(usage, model)
                    result = resp.choices[0].message.content
                    if not result:
                        raise ValueError(
                            f"LLM returned empty response (non-streaming, model={model})"
                        )
                    return result
            finally:
                if sem is not None:
                    sem.release()
        except (AuthenticationError, BadRequestError):
            # Non-retryable: bad credentials or malformed request
            raise
        except Exception as e:
            # Vendor-specific deterministic 400 (content moderation, data
            # inspection) — fail fast, don't waste retries.
            if _is_non_retryable_vendor_error(e):
                # Build ordered fallback list: cascade list takes priority over single model.
                _fallback_list: list[str] = list(
                    safety_fallback_models
                    if safety_fallback_models is not None
                    else ([safety_fallback_model] if safety_fallback_model else [])
                )
                if safety_fallback_pool and _fallback_list:
                    first_fb = _fallback_list[0]
                    rest_fb  = _fallback_list[1:]
                    tail_str = f" → {' → '.join(rest_fb)}" if rest_fb else ""
                    logger.warning(
                        f"[SafetyFallback] {model} → {first_fb}{tail_str} "
                        f"| err={str(e)[:120]}"
                    )
                    return llm_chat(
                        safety_fallback_pool, first_fb,
                        system, user,
                        temperature=temperature, max_tokens=max_tokens,
                        max_retries=max_retries, seed=seed, stream=stream,
                        # no extra_body/response_format: avoid provider-specific params
                        safety_fallback_pool=safety_fallback_pool if rest_fb else None,
                        safety_fallback_models=rest_fb if rest_fb else None,
                    )
                logger.error(
                    f"[NonRetryable vendor] model={model} err={str(e)[:200]}"
                )
                raise
            if attempt < max_retries:
                wait, label = _compute_backoff(e, attempt)
                logger.warning(
                    f"[Retry {attempt+1}/{max_retries}] {label} | "
                    f"wait {wait:.1f}s | model={model} | err={str(e)[:160]}"
                )
                time.sleep(wait)
            else:
                logger.error(
                    f"[GiveUp after {max_retries+1} attempts] model={model} "
                    f"err={str(e)[:200]}"
                )
                raise


# ── Round-robin client pool for multi-key concurrency ────────────────────────

class RoundRobinClientPool:
    """
    Thread-safe pool of OpenAI clients, one per API key.
    Each call to `next_client()` returns the next client in round-robin order.

    Optional `max_concurrency` installs a provider-wide BoundedSemaphore so
    `llm_chat(pool=...)` can cap simultaneous in-flight requests regardless of
    how many phases run in parallel (see Phase A: Step1 ∥ Step4 both → LLM1).
    """

    def __init__(
        self,
        api_keys: list[str],
        base_url: str,
        max_concurrency: int | None = None,
    ):
        if not api_keys:
            raise ValueError("RoundRobinClientPool requires at least 1 API key")
        self._clients = [
            OpenAI(api_key=key, base_url=base_url, timeout=_LLM_TIMEOUT)
            for key in api_keys
        ]
        self._index = 0
        self._lock = threading.Lock()
        # `_sem` is read by llm_chat via `getattr(..., "_sem", None)`; None ⇒ no cap.
        self._sem: threading.BoundedSemaphore | None = (
            threading.BoundedSemaphore(max_concurrency)
            if max_concurrency and max_concurrency > 0
            else None
        )
        cap_str = f", concurrency≤{max_concurrency}" if self._sem else ""
        logger.info(
            f"RoundRobinClientPool: {len(self._clients)} clients for {base_url}{cap_str}"
        )

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


@dataclass(frozen=True)
class LLMConfig:
    """Configuration for an LLM backend (extraction or verification)."""
    pool: RoundRobinClientPool
    model: str
    max_retries: int = 4
    extra_body: dict | None = None
    max_tokens: int | None = None
    source_tag: str = ""  # e.g. "llm1", "doubao"
    response_format: dict | None = None  # e.g. {"type": "json_object"}
    safety_fallback_pool: RoundRobinClientPool | None = None  # fallback when content moderation triggers
    safety_fallback_model: str = ""                          # compat: single fallback model
    safety_fallback_models: tuple[str, ...] = ()             # cascade: tried in order


# ── Shared data helpers ─────────────────────────────────────────────────────

def to_float_date(s: str) -> float | None:
    """Parse YYYY.MM string to float (e.g. '1978.10' → 1978.833).

    Returns None for empty/blank strings, -1.0 on parse failure.
    """
    s_str = str(s).strip()
    if not s_str or s_str in ("None", "nan"):
        return None
    try:
        parts = s_str.split(".")
        yr = int(parts[0])
        mo = int(parts[1]) if len(parts) > 1 and parts[1] not in ("00", "") else 0
        return yr + mo / 12.0
    except Exception:
        return -1.0


def normalize_org_name(name: str) -> str:
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
    if not name.startswith("中共") and re.search(r"(?:省|市|区|县|自治州|自治区)委", name):
        name = "中共" + name
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


def load_json_cache(path: Path, force: bool = False) -> dict[str, dict]:
    """Load a JSON cache file into {name: record} dict.

    Expects a JSON list where each item has _meta.name.
    Skips list entries that are None (in-flight placeholders that were saved
    before the run finished) or otherwise lack _meta.name.
    Returns empty dict if file missing, force=True, or parse failure.
    """
    if force or not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        result: dict[str, dict] = {}
        for r in data:
            if not isinstance(r, dict):
                continue
            name = r.get("_meta", {}).get("name", "")
            if name:
                result[name] = r
        return result
    except Exception:
        return {}


def save_json_cache(path: Path, cache: dict) -> None:
    """Save a {name: record} cache dict as JSON list (atomic write).

    Filters out None placeholders (used for concurrent dedup) before serializing.
    """
    import tempfile
    path.parent.mkdir(parents=True, exist_ok=True)
    serializable = [v for v in cache.values() if v is not None]
    content = json.dumps(serializable, ensure_ascii=False, indent=2)
    fd, tmp = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(content)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def get_highest_rank(ranks: list[str]) -> str:
    """Return the highest rank from a list of rank strings.

    Uses RANK_LEVELS ordering from config (正国级 highest → 副科级 lowest).
    """
    best_idx = len(RANK_LEVELS)  # worse than any valid rank
    for r in ranks:
        r = r.strip()
        if r in RANK_LEVELS:
            idx = RANK_LEVELS.index(r)
            if idx < best_idx:
                best_idx = idx
    return RANK_LEVELS[best_idx] if best_idx < len(RANK_LEVELS) else ""
