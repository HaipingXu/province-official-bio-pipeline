"""
LLM Biography Fallback

When Baidu Baike scraping fails (403/blocked), this module generates
a biography document by asking DeepSeek to recall its training knowledge
about a Chinese official.

The output is saved to officials/{name}_biography.txt in the same format
as the scraper would produce, and downstream processing is identical.

Note: LLM-recalled biographies may be less accurate than scraped pages.
All such files are tagged with [来源:LLM回忆] for transparency.
Verification step (Qwen) will catch discrepancies.
"""

import json
import time
from pathlib import Path

from openai import OpenAI

from config import (
    DEEPSEEK_API_KEY, DEEPSEEK_BASE_URL, DEEPSEEK_MODEL,
    OFFICIALS_DIR, LOGS_DIR,
)

OFFICIALS_DIR.mkdir(parents=True, exist_ok=True)


def fetch_bio_from_llm(name: str, city: str, province: str, role: str = "") -> str:
    """
    Ask DeepSeek to provide known biographical information about an official.
    Returns biography text string.
    """
    client = OpenAI(api_key=DEEPSEEK_API_KEY, base_url=DEEPSEEK_BASE_URL)

    prompt = f"""请根据你的知识，提供关于{name}的详细人物简介和完整履历。
{name}曾担任{city}市{role if role else "主要领导职务"}。

请提供以下信息（仅提供你确定的信息，不确定的不要编造）：
1. 基本信息：出生年份、籍贯、民族、性别
2. 学历教育经历（全日制本科、研究生等）
3. 完整工作履历（从参加工作到当前/离任，包括每个职位的大致时间）
4. 在深圳任职情况（具体职务和任期）
5. 离任后去向（是否升迁）
6. 是否有腐败问题被查处

请尽量详细，按时间顺序列出所有履历。来源：百度百科及公开报道。"""

    for attempt in range(2):
        try:
            response = client.chat.completions.create(
                model=DEEPSEEK_MODEL,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3,
                max_tokens=3000,
            )
            return response.choices[0].message.content
        except Exception as e:
            if attempt == 0:
                time.sleep(3)
            else:
                raise e


def fetch_all_missing(officials: list[dict], city: str, province: str) -> dict:
    """
    For officials without biography files, fetch from LLM.
    Returns {success_count, fail_count}.
    """
    missing = [
        o for o in officials
        if not (OFFICIALS_DIR / f"{o['name']}_biography.txt").exists()
    ]

    if not missing:
        print("  All biography files already exist.")
        return {"success_count": 0, "fail_count": 0}

    print(f"\n=== LLM Biography Fallback: {len(missing)} officials ===")
    success = 0
    fail = 0

    for i, official in enumerate(missing):
        name = official["name"]
        role = official.get("role", "")
        print(f"  [{i+1}/{len(missing)}] Fetching LLM bio for: {name}...")

        try:
            bio_text = fetch_bio_from_llm(name, city, province, role)
            output_path = OFFICIALS_DIR / f"{name}_biography.txt"
            output_path.write_text(
                f"官员：{name}\n来源：LLM回忆（DeepSeek训练数据）\n\n{bio_text}",
                encoding="utf-8",
            )
            print(f"    ✓ Saved {len(bio_text)} chars [来源:LLM回忆]")
            success += 1
            time.sleep(1.5)
        except Exception as e:
            print(f"    ✗ Failed: {e}")
            fail += 1

    print(f"\n✓ LLM fallback complete: {success}/{len(missing)} obtained")
    return {"success_count": success, "fail_count": fail}


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--city", default="深圳")
    parser.add_argument("--province", default="广东")
    parser.add_argument("--names", default="logs/officials_list.json")
    parser.add_argument("--official", default="", help="Single official name")
    args = parser.parse_args()

    if args.official:
        text = fetch_bio_from_llm(args.official, args.city, args.province)
        path = OFFICIALS_DIR / f"{args.official}_biography.txt"
        path.write_text(f"官员：{args.official}\n来源：LLM回忆\n\n{text}", encoding="utf-8")
        print(f"Saved to {path}")
        print(text[:500])
    else:
        with open(args.names, encoding="utf-8") as f:
            officials = json.load(f)
        fetch_all_missing(officials, args.city, args.province)
