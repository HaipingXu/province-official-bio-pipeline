"""Thread-safe failure tracking + reporting for the LLM pipeline.

Centralises all `(scope, source, step, name, error)` failure records so the
pipeline can print a unified summary and persist them to
`logs/{省}/api_failures.json` for postmortem.

Used by:
  - extraction._run_step    (LLM1/LLM2 extract failures)
  - judge._run_judge_tasks  (judge call failures)

Lifecycle:
  - `FAILURES.reset()` is called at the start of each province pipeline.
  - `FAILURES.record(...)` is called from worker threads on exception.
  - `FAILURES.print_summary()` + `write_report(path)` at end of pipeline.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from collections import Counter
from pathlib import Path

logger = logging.getLogger(__name__)


class FailureTracker:
    """Thread-safe in-memory failure log."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._items: list[dict] = []

    def record(
        self,
        *,
        scope: str,
        name: str,
        step: str,
        error: str | Exception,
        source: str = "",
        attempt: int = 0,
        extra: dict | None = None,
    ) -> None:
        """Record a single failure.

        Args:
            scope:   "extract" | "judge" | "preprocess" | "diff"
            name:    Official name or judge cache key.
            step:    "step1" | "step2" | "step3" | "step4" | etc.
            source:  "llm1" | "llm2" | "judge" | ""
            attempt: Final retry attempt number that gave up.
            error:   Exception or error message.
            extra:   Optional dict of extra metadata.
        """
        entry: dict = {
            "ts": time.strftime("%Y-%m-%d %H:%M:%S"),
            "scope": scope,
            "name": name,
            "step": step,
            "source": source,
            "attempt": attempt,
            "error": str(error)[:500],
        }
        if extra:
            entry["extra"] = extra
        with self._lock:
            self._items.append(entry)
        # Best-effort log so failure shows up in pipeline.log immediately
        logger.warning(
            f"[FAIL][{scope}][{source}/{step}] {name}: {str(error)[:160]}"
        )

    def snapshot(self) -> list[dict]:
        with self._lock:
            return list(self._items)

    def count(self) -> int:
        with self._lock:
            return len(self._items)

    def reset(self) -> None:
        """Clear all records — call at start of each province in batch mode."""
        with self._lock:
            self._items.clear()

    def write_report(self, path: Path) -> Path | None:
        """Write JSON report. Returns path if any items, else None."""
        items = self.snapshot()
        if not items:
            return None
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(items, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return path

    def summary_lines(self, sample_n: int = 5, top_n: int = 10) -> list[str]:
        """Return a list of printable summary lines."""
        items = self.snapshot()
        if not items:
            return ["✓ 无失败记录"]

        by_scope: Counter = Counter(i.get("scope", "") for i in items)
        by_src_step: Counter = Counter(
            (i.get("source", ""), i.get("step", "")) for i in items
        )

        lines: list[str] = [f"⚠ 共 {len(items)} 条失败记录"]
        lines.append("  按 scope:")
        for scope, n in sorted(by_scope.items(), key=lambda x: -x[1]):
            lines.append(f"    {(scope or '(无)'):<10} {n}")
        lines.append(f"  按 source/step (top {top_n}):")
        for (src, step), n in by_src_step.most_common(top_n):
            lines.append(
                f"    {(src or '(无)'):<8} {(step or '(无)'):<8} {n}"
            )
        if sample_n > 0:
            lines.append(f"  示例（前 {sample_n} 条）:")
            for it in items[:sample_n]:
                lines.append(
                    f"    [{it.get('source','')}/{it.get('step','')}] "
                    f"{it.get('name','')}: {it.get('error','')[:120]}"
                )
        return lines

    def print_summary(self, sample_n: int = 5, top_n: int = 10) -> None:
        for line in self.summary_lines(sample_n=sample_n, top_n=top_n):
            print(line)


# Module-level singleton (mirrors `utils.TOKENS` style)
FAILURES = FailureTracker()
