"""
Post-process Wikipedia raw section text to extract only first secretaries / secretaries.
Filter out deputy secretaries, standing committee members, etc.
"""
import json
import re
from pathlib import Path

with open("temp/wiki_secretary_raw_sections.json", encoding="utf-8") as f:
    raw_data = json.load(f)

# Regex: Name（date_range）
# Name: 2-5 Chinese chars (may have middle dot for ethnic names)
# Date: YYYY年M月—YYYY年M月 or YYYY年M月—
entry_pattern = re.compile(
    r"^([\u4e00-\u9fff·]{2,8})\s*"          # Name (2-8 Chinese chars, may include ·)
    r"[\（\(]"                                 # Opening paren
    r"(.*?\d{4}.*?)"                           # Date content (must contain a year)
    r"[\）\)]"                                 # Closing paren
    r"(.*)$"                                   # Optional trailing text
)

# Committee breakdown markers (stop collecting "top" entries when we see these)
committee_break = re.compile(
    r"^中国共产党.*委员会[\（\(]|"
    r"^第[一二三四五六七八九十\d]+届.*省委|"
    r"^第[一二三四五六七八九十\d]+届.*市委|"
    r"^第[一二三四五六七八九十\d]+届.*自治区|"
    r"^书记[：:]$|"
    r"^副书记[：:]$|"
    r"^常[务委]|"
    r"^自治区第"
)

# Labels that indicate the following names are secretaries
secretary_label = re.compile(
    r"^(?:第一)?书记[：:]$|"
    r"^书记：$|"
    r"^第一书记[：:]$|"
    r"省委第一书记|"
    r"市委第一书记|"
    r"自治区.*第一书记|"
    r"中共.*第一书记$|"
    r"中共.*省委书记$|"
    r"中共.*市委书记$|"
    r"中共.*委员会书记$"
)

# Labels that indicate NON-secretary entries (skip these)
skip_label = re.compile(
    r"副书记|常委|秘书长|组织部|宣传部|纪委|政法委|统战部|常务副"
)


def extract_clean_secretaries(raw_text: str, province: str) -> list[dict]:
    """Extract only first secretary / secretary entries from raw text."""
    if not raw_text:
        return []

    lines = raw_text.split("\n")
    results = []
    seen_names = set()

    # Phase 1: Extract "top" entries (before committee breakdown)
    # These are typically the most recent secretaries listed at the top
    in_top = True
    in_secretary_section = False  # Under a "书记：" label
    skip_mode = False  # Under a "副书记：" label
    current_role = ""

    for i, line in enumerate(lines):
        line = line.strip()
        if not line:
            continue

        # Skip common noise
        if line in ["[编辑]", "[", "编辑", "]", "主条目："]:
            continue

        # Check for committee breakdown marker
        if committee_break.match(line):
            in_top = False
            skip_mode = False
            in_secretary_section = False
            # But check if next content is a secretary label
            continue

        # Check for secretary label
        if secretary_label.search(line):
            in_secretary_section = True
            skip_mode = False
            current_role = line.rstrip("：:")
            continue

        # Check for skip label
        if skip_label.search(line) and "：" in line:
            skip_mode = True
            in_secretary_section = False
            continue

        # Skip "代理书记" labels
        if "代理" in line and "书记" in line and "：" in line:
            skip_mode = True
            in_secretary_section = False
            continue

        # Try to extract an entry
        m = entry_pattern.match(line)
        if m:
            name = m.group(1).replace("\u3000", "")  # Remove full-width spaces
            term = m.group(2).strip()
            note = m.group(3).strip().lstrip("，,、")

            # Only include if we're in a valid context
            if in_top or in_secretary_section:
                if name not in seen_names and not skip_mode:
                    results.append({
                        "name": name,
                        "term": term,
                        "role_title": current_role,
                        "note": note if note else None,
                    })
                    seen_names.add(name)
            continue

        # Handle case where name is on one line and date on the next
        if i + 1 < len(lines):
            next_line = lines[i + 1].strip()
            if (len(line) >= 2 and len(line) <= 8 and
                    re.match(r"^[\u4e00-\u9fff·\u3000]+$", line) and
                    re.match(r"[\（\(].*\d{4}.*[\）\)]", next_line)):
                name = line.replace("\u3000", "")
                term_match = re.match(r"[\（\(](.*?)[\）\)]", next_line)
                if term_match and (in_top or in_secretary_section) and not skip_mode:
                    term = term_match.group(1)
                    if name not in seen_names:
                        results.append({
                            "name": name,
                            "term": term,
                            "role_title": current_role,
                            "note": None,
                        })
                        seen_names.add(name)

    return results


# Process all provinces
clean_results = {}
for prov, raw_text in raw_data.items():
    secs = extract_clean_secretaries(raw_text, prov)
    clean_results[prov] = secs
    if secs:
        recent = [f"{s['name']}({s['term'][:20]})" for s in secs[-3:]]
        print(f"✓ {prov:6s}: {len(secs):2d} secretaries | recent: {', '.join(recent)}")
    else:
        print(f"  {prov:6s}:  0 secretaries")

# Save
output = {}
for prov, secs in clean_results.items():
    output[prov] = {
        "province": prov,
        "source": "wikipedia",
        "secretary_list": secs,
    }

Path("data/wiki_secretaries_clean.json").write_text(
    json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8"
)

total = sum(len(v) for v in clean_results.values())
found = sum(1 for v in clean_results.values() if v)
print(f"\nTotal: {total} secretaries across {found} provinces")
