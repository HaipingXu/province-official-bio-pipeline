"""
Smart merge: for each province, pick the best secretary data source.

Strategy:
- Wikipedia clean bullet-point section → best (precise dates, authoritative)
- Manual supplement → fallback (approximate dates, web search)
- Wikipedia per-term blocks → noisy, use only to supplement missing entries

Output: data/wiki_secretaries_best.json (replaces wiki_secretaries_clean.json)
"""
import json
from pathlib import Path

DATA_DIR = Path("data")

# Load all sources
with open(DATA_DIR / "wiki_secretaries_clean.json", encoding="utf-8") as f:
    manual = json.load(f)  # Manual supplement + original wiki scrape

with open(DATA_DIR / "wiki_secretaries_v3.json", encoding="utf-8") as f:
    wiki_v3 = json.load(f)  # v3 Wikipedia parse

# Provinces where Wikipedia v3 has a CLEAN bullet-point section (verified manually)
# These have precise dates and are authoritative
WIKI_CLEAN = {
    "北京", "上海", "安徽", "广东", "广西", "贵州", "甘肃",
}

# Provinces where Wikipedia v3 parsed per-term blocks with good coverage
# Use wiki but supplement with manual for early-period gaps
WIKI_PARTIAL_GOOD = {
    "重庆", "青海",
}

# Provinces where wiki v3 is too noisy or incomplete → use manual
MANUAL_PREFERRED = {
    "天津", "河北", "山西", "内蒙古", "辽宁", "吉林", "黑龙江",
    "江苏", "浙江", "福建", "江西", "山东", "河南",
    "海南", "四川", "云南", "西藏", "陕西", "宁夏",
    "湖南", "湖北", "新疆",
}


def is_valid_term(term: str) -> bool:
    """Check if a term string looks like a legitimate political term (not a birth year)."""
    if not term or "年" not in term:
        return False
    import re
    # Must have a start year >= 1949 and contain a dash/separator
    m = re.search(r'(\d{4})年', term)
    if m:
        year = int(m.group(1))
        if year < 1945 or year > 2030:
            return False
    # Must have start—end pattern (not just a single year)
    if "—" in term or "–" in term or "-" in term:
        return True
    # Single date with 月 is ok (e.g., "2022年11月—")
    if "月" in term:
        return True
    return False


def upgrade_dates_only(wiki_list: list, manual_list: list) -> list:
    """Use manual as base, only upgrade dates from wiki for matching names. No new entries."""
    wiki_by_name = {s["name"]: s for s in wiki_list}

    result = []
    for s in manual_list:
        entry = dict(s)
        if s["name"] in wiki_by_name:
            wiki_term = wiki_by_name[s["name"]].get("term", "")
            if is_valid_term(wiki_term):
                entry["term"] = wiki_term
        result.append(entry)
    return result


def merge_lists(wiki_list: list, manual_list: list) -> list:
    """Merge: manual base + wiki date upgrades + wiki-only entries (for PARTIAL_GOOD)."""
    wiki_by_name = {s["name"]: s for s in wiki_list}

    result = []
    seen = set()
    # Manual entries first, with wiki date upgrades
    for s in manual_list:
        entry = dict(s)
        if s["name"] in wiki_by_name:
            wiki_term = wiki_by_name[s["name"]].get("term", "")
            if is_valid_term(wiki_term):
                entry["term"] = wiki_term
        result.append(entry)
        seen.add(s["name"])

    # Add wiki-only entries
    for s in wiki_list:
        if s["name"] not in seen:
            result.append(dict(s))
            seen.add(s["name"])

    return result


def main():
    best = {}

    print(f"{'Province':8s} {'Source':12s} {'Count':>6s} Notes")
    print("=" * 60)

    for province in [
        "北京", "天津", "河北", "山西", "内蒙古", "辽宁", "吉林", "黑龙江",
        "上海", "江苏", "浙江", "安徽", "福建", "江西", "山东", "河南",
        "湖北", "湖南", "广东", "广西", "海南", "重庆", "四川", "贵州",
        "云南", "西藏", "陕西", "甘肃", "青海", "宁夏", "新疆",
    ]:
        wiki_secs = wiki_v3.get(province, {}).get("secretary_list", [])
        manual_secs = manual.get(province, {}).get("secretary_list", [])

        if province in WIKI_CLEAN:
            # Pure Wikipedia (clean bullet section)
            chosen = wiki_secs
            source = "wikipedia"
            notes = "clean bullet-point section"
        elif province in WIKI_PARTIAL_GOOD:
            # Merge: manual base + wiki date upgrades
            chosen = merge_lists(wiki_secs, manual_secs)
            source = "merged"
            notes = f"wiki={len(wiki_secs)}, manual={len(manual_secs)}"
        else:
            # Manual preferred — only upgrade dates, never add wiki-only entries
            if wiki_secs:
                matched = sum(1 for w in wiki_secs if w["name"] in {s["name"] for s in manual_secs})
                chosen = upgrade_dates_only(wiki_secs, manual_secs)
                source = "manual+wiki"
                notes = f"manual base, wiki dates for {matched} names"
            else:
                chosen = manual_secs
                source = "manual"
                notes = "manual only"

        # Clean output format
        clean_list = []
        for s in chosen:
            clean_list.append({
                "name": s["name"],
                "term": s.get("term", ""),
                "role_title": s.get("role_title", ""),
                "note": s.get("note"),
            })

        best[province] = {
            "province": province,
            "source": source,
            "secretary_list": clean_list,
        }

        print(f"{province:8s} {source:12s} {len(clean_list):6d} {notes}")

    # Save
    out = DATA_DIR / "wiki_secretaries_best.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump(best, f, ensure_ascii=False, indent=2)

    total = sum(len(v["secretary_list"]) for v in best.values())
    print(f"\nTotal: {total} secretaries across 31 provinces")
    print(f"Saved: {out}")

    # Also overwrite wiki_secretaries_clean.json as the canonical source
    with open(DATA_DIR / "wiki_secretaries_clean.json", "w", encoding="utf-8") as f:
        json.dump(best, f, ensure_ascii=False, indent=2)
    print(f"Updated: {DATA_DIR / 'wiki_secretaries_clean.json'}")


if __name__ == "__main__":
    main()
