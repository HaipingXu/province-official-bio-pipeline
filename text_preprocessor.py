"""
Phase 0.5: Pre-process raw biography text into structured components.

Parses Baidu Baike biography files into:
  - bio_summary: name, birth, ethnicity, origin (from 人物简介 + 基本信息)
  - career_lines: numbered career timeline entries (from 履历段落)
  - extra_text: honors, party positions, post-career news

落马 detection is handled entirely by Step 2 LLM (是否落马 field).
"""

import logging
import re
from pathlib import Path

from config import OFFICIALS_DIR

logger = logging.getLogger(__name__)


# ── Career line patterns ─────────────────────────────────────────────────────

# Shared dash pattern (covers half-width, en-dash, em-dash, fullwidth, double em-dash, wave tilde)
_DASH = r'(?:——|[-–—－～])'

# Pattern 1: "1978.10-1982.07 description" (dot-date range; month may be 1 or 2 digits)
_PAT_DOT = re.compile(
    r'^(\d{4})[.](\d{1,2})(?:[.]\d{1,2})?\s*' + _DASH + r'\s*(\d{4})[.](\d{1,2})(?:[.]\d{1,2})?\s*(.+)'
)

# Pattern 2: "1978.10- description" (dot-date, open/ongoing; month may be 1 or 2 digits)
_PAT_DOT_OPEN = re.compile(
    r'^(\d{4})[.](\d{1,2})(?:[.]\d{1,2})?\s*' + _DASH + r'\s*(.+)'
)

# Pattern 3: "1975—1978年 description" (year range, optional 年, any dash or 至 separator)
_PAT_DASH_NIAN = re.compile(
    r'^(\d{4})年?\s*(?:——|[-–—－～]|至)\s*(\d{4})年?\s*(.+)'
)

# Pattern 7: "2013- 现任职位" (year + dash, open/ongoing, no month)
# \s* allows no space (e.g. "2022-中央政治局委员"); _PAT_DASH_NIAN is checked first
# so year-ranges like "2013-2017年" are already consumed before reaching here.
_PAT_YEAR_OPEN = re.compile(
    r'^(\d{4})\s*' + _DASH + r'\s*(.+)'
)

# Pattern 8: "1982月09月，description" (scraper artifact: 年 replaced by 月)
_PAT_NIAN_AS_YUE = re.compile(
    r'^(\d{4})月(\d{1,2})月[，,\s]\s*(.+)'
)

# Pattern 4: "2025年9月 description" (single date, end inferred from next line)
_PAT_SINGLE_DATE = re.compile(
    r'^(\d{4})年(\d{1,2})月[\d日，,起]*\s*(.+)'
)

# Pattern 5: "从1978年..." or "1950年底..." (year-only, no .MM or 年MM月 format)
_PAT_NIAN_ONLY = re.compile(
    r'^从?(\d{4})年(?!\d{1,2}月).+'
)

# Pattern 6: "1973.02，description" or "2023.03 description" (dot-date + comma/space separator)
_PAT_DOT_COMMA = re.compile(
    r'^(\d{4})[.](\d{1,2})(?:[.]\d{1,2})?[，,\s]\s*(.+)'
)

# Party/honor summary line (not a career entry)
_HONOR_KW = re.compile(
    r'^(中共(十|第)|第十|中国共产党.+委员会|全国人大代表|'
    r'全国政协委员|省.*代表|市.*代表|.*党代会代表)'
)


def _is_career_line(line: str) -> bool:
    """Check if a line matches any career timeline pattern."""
    line = line.strip()
    if not line:
        return False
    if _is_honor_line(line):
        return False
    if _PAT_DOT.match(line):
        return True
    if _PAT_DOT_OPEN.match(line):
        return True
    if _PAT_DASH_NIAN.match(line):
        return True
    if _PAT_SINGLE_DATE.match(line):
        return True
    if _PAT_NIAN_ONLY.match(line):
        return True
    if _PAT_DOT_COMMA.match(line):
        return True
    if _PAT_YEAR_OPEN.match(line):
        return True
    if _PAT_NIAN_AS_YUE.match(line):
        return True
    return False


def _is_honor_line(line: str) -> bool:
    """Check if line is a party/honor summary (not career)."""
    return bool(_HONOR_KW.match(line.strip()))


def _extract_start_ym(raw_text: str) -> tuple[int, int] | None:
    """Extract (year, month) from the start of a career line. Returns None if unparseable."""
    m = _PAT_DOT.match(raw_text)
    if m:
        return _validate_month(int(m.group(1)), int(m.group(2)))
    m = _PAT_DOT_OPEN.match(raw_text)
    if m:
        return _validate_month(int(m.group(1)), int(m.group(2)))
    m = _PAT_DASH_NIAN.match(raw_text)
    if m:
        return int(m.group(1)), 0
    m = _PAT_SINGLE_DATE.match(raw_text)
    if m:
        return _validate_month(int(m.group(1)), int(m.group(2)))
    m = _PAT_NIAN_ONLY.match(raw_text)
    if m:
        return int(m.group(1)), 0
    m = _PAT_DOT_COMMA.match(raw_text)
    if m:
        return _validate_month(int(m.group(1)), int(m.group(2)))
    m = _PAT_YEAR_OPEN.match(raw_text)
    if m:
        return int(m.group(1)), 0
    m = _PAT_NIAN_AS_YUE.match(raw_text)
    if m:
        return _validate_month(int(m.group(1)), int(m.group(2)))
    return None


def _validate_month(year: int, month: int) -> tuple[int, int]:
    """Validate month is in 1-12 range; warn and return (year, 0) if invalid."""
    if 1 <= month <= 12:
        return year, month
    logger.warning(f"无效月份 {month}（年份 {year}），置为 0")
    return year, 0


def _has_explicit_end(raw_text: str) -> bool:
    """Return True if the line already contains an explicit end date."""
    return bool(_PAT_DOT.match(raw_text) or _PAT_DASH_NIAN.match(raw_text))


def _infer_end_dates(career_lines: list[dict]) -> list[dict]:
    """
    For single-date lines (no explicit end), set inferred_end = next line's start.
    Lines with explicit end dates are unchanged.
    """
    for i, entry in enumerate(career_lines):
        raw = entry["raw_text"]
        if _has_explicit_end(raw):
            continue

        inferred = None
        for j in range(i + 1, len(career_lines)):
            ym = _extract_start_ym(career_lines[j]["raw_text"])
            if ym:
                y, mo = ym
                inferred = f"{y}年{mo:02d}月" if mo else f"{y}年"
                break
        entry["inferred_end"] = inferred

    return career_lines


def preprocess_biography(bio_text: str, name: str = "") -> dict:
    """
    Parse raw biography text into structured components.

    Returns:
        {
            "name": str,
            "bio_summary": str,       # 人物简介 + 基本信息 text
            "career_lines": [          # numbered career entries
                {"line_num": 1, "raw_text": "...", "inferred_end": "1986年08月" | None},
                ...
            ],
            "extra_text": str,         # honors, party positions
            "corruption_text": str,    # kept for backward compat, always ""
            "total_lines": int,
        }
    """
    sections = _split_sections(bio_text)

    # Extract bio summary
    bio_summary = ""
    if sections.get("人物简介"):
        bio_summary += sections["人物简介"].strip()
    if sections.get("基本信息"):
        bio_summary += "\n" + sections["基本信息"].strip()

    # Parse career lines
    career_lines = []
    extra_lines = []

    raw_career = sections.get("履历段落", "")
    if raw_career:
        for line in raw_career.split("\n"):
            line = line.strip()
            if not line:
                continue
            if _is_career_line(line):
                career_lines.append({
                    "line_num": len(career_lines) + 1,
                    "raw_text": line,
                    "inferred_end": None,
                    "undated": False,
                })
            elif _is_honor_line(line):
                extra_lines.append(line)
            else:
                # Undated narrative — append to previous entry as elaboration
                if career_lines:
                    career_lines[-1]["raw_text"] += "　" + line
                else:
                    extra_lines.append(line)

    # Infer end dates for single-date lines (skip undated blocks)
    career_lines = _infer_end_dates(career_lines)

    if not career_lines:
        logger.warning(f"  {name}: 未提取到任何履历行")

    result = {
        "name": name,
        "bio_summary": bio_summary.strip(),
        "career_lines": career_lines,
        "extra_text": "\n".join(extra_lines).strip(),
        "corruption_text": "",   # kept for backward compat; LLM handles 落马 in Step 2
        "total_lines": len(career_lines),
    }

    logger.info(f"  预处理 {name}: {len(career_lines)} 条履历行, 简介 {len(bio_summary)}字")

    return result


def _split_sections(text: str) -> dict[str, str]:
    """Split biography text by === section headers ===."""
    sections: dict[str, str] = {}
    current_section = "_header"
    current_lines: list[str] = []

    for line in text.split("\n"):
        m = re.match(r'^===\s*(.+?)\s*===$', line.strip())
        if m:
            if current_lines:
                sections[current_section] = "\n".join(current_lines)
            current_section = m.group(1)
            current_lines = []
        else:
            current_lines.append(line)

    if current_lines:
        sections[current_section] = "\n".join(current_lines)

    return sections


def format_career_lines_for_llm(career_lines: list[dict]) -> str:
    """
    Format career lines as numbered entries for LLM input.
    Single-date lines with an inferred end show: "YYYY年MM月-[推断:YYYY年MM月] ..."

    Output:
        L01: 1978.10-1982.07 北京工业学院光学工程系...
        L07: 1983年05月-[推断:1986年08月] 任宁波市委书记。
        ...
    """
    lines = []
    for entry in career_lines:
        num = entry["line_num"]
        raw = entry["raw_text"]
        inferred = entry.get("inferred_end")

        if inferred and not _has_explicit_end(raw):
            lines.append(f"L{num:02d}: {raw}  [至{inferred}止]")
        else:
            lines.append(f"L{num:02d}: {raw}")
    return "\n".join(lines)


def preprocess_official(name: str, officials_dir: Path | None = None) -> dict | None:
    """
    Load and preprocess a single official's biography file.

    Returns preprocessed dict or None if file not found.
    """
    if officials_dir is None:
        officials_dir = OFFICIALS_DIR

    bio_path = officials_dir / f"{name}_biography.txt"
    if not bio_path.exists():
        logger.warning(f"  ✗ 传记文件不存在: {bio_path}")
        return None

    bio_text = bio_path.read_text(encoding="utf-8")
    return preprocess_biography(bio_text, name=name)


def preprocess_all(names: list[str], officials_dir: Path | None = None,
                   logs_dir: Path | None = None) -> dict[str, dict]:
    """
    Preprocess all officials' biographies.
    Saves results to logs/preprocessed_texts.json for traceability.

    Returns {name: preprocessed_dict}.
    """
    import json
    from config import LOGS_DIR

    _logs = logs_dir or LOGS_DIR

    results = {}
    for name in names:
        result = preprocess_official(name, officials_dir)
        if result:
            results[name] = result

    if results:
        _logs.mkdir(parents=True, exist_ok=True)
        log_path = _logs / "preprocessed_texts.json"
        log_path.write_text(
            json.dumps(results, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    return results
