"""
Phase 0: Official List Collector

Gathers a complete, deduplicated list of city-level officials (mayors + party
secretaries) for a given city since a given year using three independent sources:
  1. Baidu Baike list page for "XX历任市长"
  2. Baidu Baike list page for "XX历任市委书记"
  3. LLM cross-check (DeepSeek + Qwen independently)

Saves result to logs/officials_list.json
"""

import argparse
import json
import random
import re
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from openai import OpenAI

from config import (
    DEEPSEEK_API_KEY, DEEPSEEK_BASE_URL, DEEPSEEK_MODEL,
    QWEN_API_KEY, QWEN_BASE_URL, QWEN_MODEL,
    LOGS_DIR, USER_AGENTS, SCRAPE_DELAY_MIN, SCRAPE_DELAY_MAX,
)

LOGS_DIR.mkdir(parents=True, exist_ok=True)


def get_headers() -> dict:
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate",
        "Referer": "https://www.baidu.com/",
        "Connection": "keep-alive",
    }


def scrape_baike_list_page(query: str, role: str, city: str, start_year: int) -> list[dict]:
    """
    Search Baidu Baike for a list page (历任市长/历任书记) and extract names.
    Returns list of {name, role, source, confidence} dicts.
    """
    officials = []

    # Try direct item URL first
    search_terms = [
        f"{city}市历任{role}",
        f"{city}历任{role}",
        f"{city}市{role}名单",
    ]

    for term in search_terms:
        url = f"https://baike.baidu.com/item/{requests.utils.quote(term)}"
        try:
            time.sleep(random.uniform(SCRAPE_DELAY_MIN, SCRAPE_DELAY_MAX))
            resp = requests.get(url, headers=get_headers(), timeout=15, allow_redirects=True)
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, "html.parser")
                text = soup.get_text(separator="\n")
                # Extract names that look like Chinese names (2-4 chars)
                # Look for table rows with years >= start_year
                names = extract_names_from_text(text, role, start_year)
                for name in names:
                    officials.append({
                        "name": name,
                        "role": role,
                        "source": f"baike_list_{term}",
                        "confidence": "medium",
                        "needs_check": False,
                    })
                if officials:
                    break
        except Exception as e:
            print(f"  [WARN] Failed to scrape list page for '{term}': {e}")
            continue

    return officials


def extract_names_from_text(text: str, role: str, start_year: int) -> list[str]:
    """Extract Chinese official names from Baidu Baike text."""
    names = []
    lines = text.split("\n")

    # Pattern: year + name combinations
    year_name_pattern = re.compile(
        r"(19|20)\d{2}.*?([^\d\s，。、]{2,4})"
    )

    for line in lines:
        line = line.strip()
        if not line:
            continue
        # Look for lines with years >= start_year
        year_match = re.search(r"(19|20)(\d{2})", line)
        if year_match:
            year = int(year_match.group(0))
            if year >= start_year:
                # Extract 2-4 char Chinese names
                # Names typically follow years or role keywords
                name_pattern = re.compile(r"[\u4e00-\u9fff]{2,4}")
                candidates = name_pattern.findall(line)
                for c in candidates:
                    # Filter common non-name characters
                    if c not in {"历任", "市长", "书记", "委员", "主任", "常委",
                                  "政府", "人民", "中共", "深圳", "广东", "北京",
                                  "上海", "以来", "任期", "担任", "至今"}:
                        if 2 <= len(c) <= 4:
                            names.append(c)

    # Deduplicate while preserving order
    seen = set()
    unique = []
    for n in names:
        if n not in seen:
            seen.add(n)
            unique.append(n)
    return unique


def llm_get_officials(client: OpenAI, model: str, city: str,
                       start_year: int, provider: str) -> list[dict]:
    """
    Ask LLM to produce a complete list of officials for a city since start_year.
    Returns list of {name, role, start_year, end_year} dicts.
    """
    prompt = f"""请列出{city}市自{start_year}年以来的所有市长（含代市长）和市委书记名单。
要求：
1. 只列出实际任职的官员，不要列出临时主持工作的人（除非是代市长）
2. 包含完整任期（起始年份到终止年份，若仍在任写"至今"）
3. 格式为JSON数组，每项包含：name（姓名）, role（市长/代市长/市委书记）, start_year（年份数字）, end_year（年份数字或"至今"）
4. 按任职时间排序（最早的在前）
5. 若对某人不确定，在needs_check字段写true

只输出JSON数组，不要任何解释文字。示例格式：
[
  {{"name": "张三", "role": "市长", "start_year": 2010, "end_year": 2015, "needs_check": false}},
  {{"name": "李四", "role": "市委书记", "start_year": 2011, "end_year": 2017, "needs_check": false}}
]"""

    try:
        response = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1,
            max_tokens=2000,
        )
        text = response.choices[0].message.content.strip()
        # Strip markdown code blocks if present
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
        data = json.loads(text)
        officials = []
        for item in data:
            officials.append({
                "name": item.get("name", "").strip(),
                "role": item.get("role", "未知"),
                "start_year": item.get("start_year", 0),
                "end_year": item.get("end_year", "至今"),
                "source": f"llm_{provider}",
                "confidence": "low" if item.get("needs_check") else "medium",
                "needs_check": item.get("needs_check", False),
            })
        return [o for o in officials if o["name"]]
    except Exception as e:
        print(f"  [WARN] LLM {provider} failed: {e}")
        return []


def merge_and_deduplicate(
    source_a: list[dict],
    source_b: list[dict],
    source_llm_ds: list[dict],
    source_llm_qw: list[dict],
) -> list[dict]:
    """
    Merge all four sources. Names appearing in only one source get needs_check=True.
    """
    all_names: dict[str, dict] = {}  # name -> merged entry

    def add(entry: dict):
        name = entry["name"]
        if not name:
            return
        if name not in all_names:
            all_names[name] = {
                "name": name,
                "role": entry.get("role", "未知"),
                "start_year": entry.get("start_year", 0),
                "end_year": entry.get("end_year", "至今"),
                "sources": [],
                "needs_check": False,
            }
        all_names[name]["sources"].append(entry.get("source", "unknown"))
        if entry.get("needs_check"):
            all_names[name]["needs_check"] = True

    for e in source_a:
        add(e)
    for e in source_b:
        add(e)
    for e in source_llm_ds:
        add(e)
    for e in source_llm_qw:
        add(e)

    result = list(all_names.values())
    # Flag names appearing in only 1 source
    for entry in result:
        if len(set(entry["sources"])) == 1:
            entry["needs_check"] = True
            print(f"  [FLAG] '{entry['name']}' found in only 1 source → marked needs_check")

    # Sort by start_year
    result.sort(key=lambda x: x.get("start_year", 9999))
    return result


def collect_officials(city: str, start_year: int) -> list[dict]:
    """Main function: collect officials from all sources and merge."""
    print(f"\n=== Phase 0: Collecting officials for {city} since {start_year} ===")

    deepseek_client = OpenAI(api_key=DEEPSEEK_API_KEY, base_url=DEEPSEEK_BASE_URL)
    qwen_client = OpenAI(api_key=QWEN_API_KEY, base_url=QWEN_BASE_URL)

    # Source A: Baidu Baike list page for mayors
    print("[1/4] Scraping Baidu Baike (市长 list)...")
    source_a = scrape_baike_list_page("历任市长", "市长", city, start_year)
    print(f"  → Found {len(source_a)} entries from Baike mayor list")

    # Source B: Baidu Baike list page for party secretaries
    print("[2/4] Scraping Baidu Baike (市委书记 list)...")
    source_b = scrape_baike_list_page("历任市委书记", "市委书记", city, start_year)
    print(f"  → Found {len(source_b)} entries from Baike secretary list")

    # Source C: DeepSeek LLM cross-check
    print("[3/4] Querying DeepSeek for official list...")
    time.sleep(random.uniform(1, 2))
    source_llm_ds = llm_get_officials(deepseek_client, DEEPSEEK_MODEL, city, start_year, "deepseek")
    print(f"  → Found {len(source_llm_ds)} entries from DeepSeek")

    # Source D: Qwen LLM cross-check
    print("[4/4] Querying Qwen for official list...")
    time.sleep(random.uniform(1, 2))
    source_llm_qw = llm_get_officials(qwen_client, QWEN_MODEL, city, start_year, "qwen")
    print(f"  → Found {len(source_llm_qw)} entries from Qwen")

    # Merge all sources
    merged = merge_and_deduplicate(source_a, source_b, source_llm_ds, source_llm_qw)

    # Save result
    output_path = LOGS_DIR / "officials_list.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)

    print(f"\n✓ Officials list saved to {output_path}")
    print(f"  Total: {len(merged)} officials")
    flagged = sum(1 for e in merged if e["needs_check"])
    if flagged:
        print(f"  ⚠ {flagged} officials flagged for manual check (single source)")

    return merged


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Collect city official list")
    parser.add_argument("--city", default="深圳", help="City name (Chinese, no 市)")
    parser.add_argument("--start", type=int, default=2010, help="Start year")
    args = parser.parse_args()

    officials = collect_officials(args.city, args.start)
    print("\nFinal list:")
    for o in officials:
        flag = "⚠" if o["needs_check"] else "✓"
        print(f"  {flag} {o['name']} ({o['role']}) {o.get('start_year','?')}–{o.get('end_year','?')}")
