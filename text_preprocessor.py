"""
Phase 0.5: Pre-process raw biography text into structured components.

Parses Baidu Baike biography files into:
  - bio_summary: name, birth, ethnicity, origin (from 人物简介 + 基本信息)
  - career_lines: numbered career timeline entries (from 履历段落)
  - extra_text: honors, party positions, post-career news
  - corruption_text: corruption-related paragraphs (审查调查, 落马, 判决)

This structured output replaces sending raw full text to LLMs.
"""

import logging
import re
from pathlib import Path

from config import OFFICIALS_DIR

logger = logging.getLogger(__name__)


# ── Career line patterns ─────────────────────────────────────────────────────

# Pattern 1: "1978.10-1982.07 description"
_PAT_DOT = re.compile(
    r'^(\d{4})[.\.](\d{2})\s*[-–—]\s*(\d{4})[.\.](\d{2})\s+(.+)'
)

# Pattern 2: "1978.10- description" (ongoing, no end date)
_PAT_DOT_OPEN = re.compile(
    r'^(\d{4})[.\.](\d{2})\s*[-–—]\s*(.+)'
)

# Pattern 3: "1975—1978年 description" (Chinese dash, optional 年 on both sides)
_PAT_DASH_NIAN = re.compile(
    r'^(\d{4})年?\s*[—\-–]\s*(\d{4})年?\s*(.+)'
)

# Pattern 4: "2025年9月 description" (single date event)
_PAT_SINGLE_DATE = re.compile(
    r'^(\d{4})年(\d{1,2})月[\d日，,]*\s*(.+)'
)

# Corruption keywords — only unambiguous legal proceedings, NOT institution names.
# Rationale: 中纪委/国家监委 can be a career posting (e.g., 中央纪委驻X单位纪检组长).
# The LLM handles 落马 detection in Step 2 via the 是否落马 field.
_CORRUPTION_KW = re.compile(
    r'审查调查|严重违纪|违纪违法|开除党籍|开除公职|双开|'
    r'受贿罪|贪污罪|滥用职权|判处有期|立案调查|被逮捕|移送检察'
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
    if _PAT_DOT.match(line):
        return True
    if _PAT_DOT_OPEN.match(line):
        return True
    if _PAT_DASH_NIAN.match(line):
        return True
    return False


def _is_corruption_paragraph(text: str) -> bool:
    """Check if text is primarily about corruption/legal proceedings."""
    return bool(_CORRUPTION_KW.search(text))


def _is_honor_line(line: str) -> bool:
    """Check if line is a party/honor summary (not career)."""
    return bool(_HONOR_KW.match(line.strip()))


def preprocess_biography(bio_text: str, name: str = "") -> dict:
    """
    Parse raw biography text into structured components.

    Returns:
        {
            "name": str,
            "bio_summary": str,       # 人物简介 + 基本信息 text
            "career_lines": [          # numbered career entries
                {"line_num": 1, "raw_text": "1978.10-1982.07 北京工业学院..."},
                ...
            ],
            "extra_text": str,         # honors, party positions
            "corruption_text": str,    # corruption-related paragraphs
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
    corruption_parts = []

    raw_career = sections.get("履历段落", "")
    if raw_career:
        for line in raw_career.split("\n"):
            line = line.strip()
            if not line:
                continue

            if _is_corruption_paragraph(line):
                corruption_parts.append(line)
            elif _is_career_line(line):
                career_lines.append({
                    "line_num": len(career_lines) + 1,
                    "raw_text": line,
                })
            elif _is_honor_line(line):
                extra_lines.append(line)
            elif _PAT_SINGLE_DATE.match(line):
                extra_lines.append(line)
            else:
                extra_lines.append(line)

    # Also scan remaining sections for corruption
    for section_name, section_text in sections.items():
        if section_name in ("人物简介", "基本信息", "履历段落", "词条标题"):
            continue
        for para in section_text.split("\n"):
            para = para.strip()
            if para and _is_corruption_paragraph(para):
                corruption_parts.append(para)

    # Check if bio_summary contains corruption info (like 陈如桂)
    if bio_summary and _is_corruption_paragraph(bio_summary):
        # Extract corruption sentences from summary
        sentences = re.split(r'[。；]', bio_summary)
        clean_summary = []
        for sent in sentences:
            if _is_corruption_paragraph(sent):
                corruption_parts.append(sent.strip())
            else:
                clean_summary.append(sent)
        # Don't modify bio_summary — keep original, corruption_text is supplementary

    result = {
        "name": name,
        "bio_summary": bio_summary.strip(),
        "career_lines": career_lines,
        "extra_text": "\n".join(extra_lines).strip(),
        "corruption_text": "\n".join(corruption_parts).strip(),
        "total_lines": len(career_lines),
    }

    logger.info(f"  预处理 {name}: {len(career_lines)} 条履历行, "
                f"简介 {len(bio_summary)}字, "
                f"{'有' if corruption_parts else '无'}落马信息")

    return result


def _split_sections(text: str) -> dict[str, str]:
    """Split biography text by === section headers ===."""
    sections: dict[str, str] = {}
    current_section = "_header"
    current_lines: list[str] = []

    for line in text.split("\n"):
        m = re.match(r'^===\s*(.+?)\s*===$', line.strip())
        if m:
            # Save previous section
            if current_lines:
                sections[current_section] = "\n".join(current_lines)
            current_section = m.group(1)
            current_lines = []
        else:
            current_lines.append(line)

    # Save last section
    if current_lines:
        sections[current_section] = "\n".join(current_lines)

    return sections


def format_career_lines_for_llm(career_lines: list[dict]) -> str:
    """
    Format career lines as numbered entries for LLM input.

    Output:
        L01: 1978.10-1982.07 北京工业学院光学工程系...
        L02: 1982.07-1984.09 兵器工业部第五五九厂...
        ...
    """
    lines = []
    for entry in career_lines:
        num = entry["line_num"]
        lines.append(f"L{num:02d}: {entry['raw_text']}")
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

    # Save preprocessed results as process log
    if results:
        _logs.mkdir(parents=True, exist_ok=True)
        log_path = _logs / "preprocessed_texts.json"
        log_path.write_text(
            json.dumps(results, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    return results
