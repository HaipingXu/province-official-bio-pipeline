"""
Phase 2: DeepSeek API Extractor

Reads biography text files from officials/ directory,
sends each to DeepSeek API with the bio-extraction skill prompt,
and saves structured JSON results to logs/deepseek_results.json.

The system prompt is loaded from .claude/skills/bio-extraction.md
(strip YAML frontmatter, use body as system prompt).
"""

import argparse
import json
import re
import time
from pathlib import Path

from openai import OpenAI

from config import (
    DEEPSEEK_API_KEY, DEEPSEEK_BASE_URL, DEEPSEEK_MODEL,
    OFFICIALS_DIR, LOGS_DIR, SKILLS_DIR,
)

LOGS_DIR.mkdir(parents=True, exist_ok=True)


def load_skill_prompt(skill_name: str) -> str:
    """
    Read .claude/skills/{skill_name}.md, strip YAML frontmatter, return body.
    This is the single source of truth for extraction rules.
    """
    path = SKILLS_DIR / f"{skill_name}.md"
    if not path.exists():
        raise FileNotFoundError(f"Skill file not found: {path}")
    content = path.read_text(encoding="utf-8")
    # Strip --- frontmatter --- block
    body = re.sub(r"^---.*?---\s*", "", content, flags=re.DOTALL)
    return body.strip()


def extract_json_from_response(text: str) -> dict:
    """
    Parse JSON from LLM response, handling markdown code blocks.
    Returns dict with 'bio' and 'episodes' keys.
    """
    # Remove markdown code blocks
    text = re.sub(r"^```(?:json)?\s*", "", text.strip())
    text = re.sub(r"\s*```$", "", text.strip())

    # Try direct parse
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Try to find JSON object in the text
    json_match = re.search(r"\{.*\}", text, re.DOTALL)
    if json_match:
        try:
            return json.loads(json_match.group(0))
        except json.JSONDecodeError:
            pass

    raise ValueError(f"Could not parse JSON from response: {text[:200]}...")


def process_official(
    client: OpenAI,
    model: str,
    system_prompt: str,
    name: str,
    bio_text: str,
    city: str,
    province: str,
    official_meta: dict,
    max_retries: int = 2,
) -> dict | None:
    """
    Send one official's biography to DeepSeek for structured extraction.
    Returns parsed result dict or None on failure.
    """
    user_prompt = f"""请根据以下百度百科内容，整理{name}的完整履历。
该官员曾担任{city}市市长和/或市委书记。

=== 百度百科内容 ===
{bio_text}

=== 额外背景信息 ===
- 姓名：{name}
- 所属省份：{province}省
- 曾任城市：{city}市

请严格按照系统提示中的格式输出JSON，不要任何额外解释。"""

    for attempt in range(max_retries + 1):
        try:
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=0.1,
                max_tokens=8000,
            )
            raw_text = response.choices[0].message.content
            result = extract_json_from_response(raw_text)

            # Validate structure
            if "bio" not in result or "episodes" not in result:
                raise ValueError("Response missing 'bio' or 'episodes' key")
            if not isinstance(result["episodes"], list):
                raise ValueError("'episodes' must be a list")

            # Enrich with metadata
            result["_meta"] = {
                "name": name,
                "city": city,
                "province": province,
                "official_role": official_meta.get("role", ""),
                "source": "deepseek",
                "model": model,
            }
            return result

        except Exception as e:
            if attempt < max_retries:
                wait = 3 * (attempt + 1)
                print(f"    [RETRY {attempt+1}] {e} — waiting {wait}s")
                time.sleep(wait)
            else:
                print(f"    [FAIL] {name}: {e}")
                return None


def process_all_officials(
    city: str,
    province: str,
    officials_meta: list[dict],
    output_path: Path,
    force: bool = False,
) -> dict:
    """
    Process all officials: load bio text, call DeepSeek, save results.
    Supports incremental updates (skips already-processed officials if not force).
    """
    client = OpenAI(api_key=DEEPSEEK_API_KEY, base_url=DEEPSEEK_BASE_URL)
    system_prompt = load_skill_prompt("bio-extraction")
    print(f"  Loaded system prompt from bio-extraction.md ({len(system_prompt)} chars)")

    # Load existing results if any
    existing_results: dict[str, dict] = {}
    if output_path.exists() and not force:
        with open(output_path, encoding="utf-8") as f:
            existing_list = json.load(f)
            for r in existing_list:
                if "bio" in r:
                    existing_results[r["bio"].get("姓名", "")] = r

    officials_files = {p.stem.replace("_biography", ""): p
                       for p in OFFICIALS_DIR.glob("*_biography.txt")}

    results = list(existing_results.values())
    processed_names = set(existing_results.keys())

    print(f"\n=== Phase 2: DeepSeek extraction ===")
    print(f"  Officials with bio files: {len(officials_files)}")
    print(f"  Already processed: {len(processed_names)}")

    for i, official in enumerate(officials_meta):
        name = official["name"]
        if name in processed_names and not force:
            print(f"  [{i+1}] SKIP {name} (already processed)")
            continue

        bio_path = officials_files.get(name)
        if not bio_path:
            print(f"  [{i+1}] SKIP {name} (no biography file)")
            continue

        bio_text = bio_path.read_text(encoding="utf-8")
        print(f"  [{i+1}/{len(officials_meta)}] Processing: {name} ({len(bio_text)} chars)...")

        result = process_official(
            client=client,
            model=DEEPSEEK_MODEL,
            system_prompt=system_prompt,
            name=name,
            bio_text=bio_text,
            city=city,
            province=province,
            official_meta=official,
        )

        if result:
            results.append(result)
            processed_names.add(name)
            # Save incrementally
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(results, f, ensure_ascii=False, indent=2)
            total_episodes = sum(len(r.get("episodes", [])) for r in results)
            print(f"    ✓ {len(result['episodes'])} episodes extracted (total: {total_episodes})")
        else:
            print(f"    ✗ Failed to extract {name}")

        # Rate limiting between API calls
        time.sleep(1.5)

    print(f"\n✓ Extraction complete: {len(results)}/{len(officials_meta)} officials processed")
    return {"results": results, "output_path": str(output_path)}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract official career data with DeepSeek")
    parser.add_argument("--city", default="深圳")
    parser.add_argument("--province", default="广东")
    parser.add_argument("--names", default="logs/officials_list.json")
    parser.add_argument("--output", default="logs/deepseek_results.json")
    parser.add_argument("--force", action="store_true", help="Re-process all officials")
    parser.add_argument("--official", default="", help="Process single official by name")
    args = parser.parse_args()

    with open(args.names, encoding="utf-8") as f:
        officials_meta = json.load(f)

    if args.official:
        officials_meta = [o for o in officials_meta if o["name"] == args.official]
        if not officials_meta:
            officials_meta = [{"name": args.official, "role": "未知"}]

    process_all_officials(
        city=args.city,
        province=args.province,
        officials_meta=officials_meta,
        output_path=Path(args.output),
        force=args.force,
    )
