"""
Phase 1: Baidu Baike Biography Scraper

Scraping strategy (in order of preference):
  1. curl_cffi with Chrome TLS fingerprint impersonation (primary — bypasses 403)
  2. requests with session warm-up (fallback)

Saves raw text to officials/{name}_biography.txt
Manual fallback: drop a {name}_biography.txt file in officials/ to bypass scraping.
"""

import argparse
import json
import random
import re
import time
from pathlib import Path

from bs4 import BeautifulSoup

from config import (
    LOGS_DIR, OFFICIALS_DIR, USER_AGENTS,
    SCRAPE_DELAY_MIN, SCRAPE_DELAY_MAX,
)

OFFICIALS_DIR.mkdir(parents=True, exist_ok=True)

# Try to import curl_cffi; fall back to requests
try:
    from curl_cffi import requests as cffi_requests
    HAS_CFFI = True
except ImportError:
    import requests as cffi_requests  # type: ignore
    HAS_CFFI = False

import requests as std_requests  # standard requests as fallback


def build_baike_urls(name: str, city: str = "") -> list[tuple[str, bool]]:
    """
    Return list of (url, is_mobile) tuples to try, most likely to succeed first.
    """
    base = f"https://baike.baidu.com/item/{std_requests.utils.quote(name)}"
    mobile = f"https://m.baike.baidu.com/item/{std_requests.utils.quote(name)}"
    urls = [(base, False), (mobile, True)]
    if city:
        alt = f"https://baike.baidu.com/item/{std_requests.utils.quote(name + '（' + city + '市官员）')}"
        urls.append((alt, False))
    return urls


def cffi_get(url: str, mobile: bool = False) -> tuple[int, str]:
    """Fetch URL using curl_cffi with Chrome fingerprint impersonation."""
    ua = random.choice(USER_AGENTS)
    r = cffi_requests.get(
        url,
        impersonate="chrome120" if HAS_CFFI else None,
        headers={
            "User-Agent": ua,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8",
            "Referer": "https://www.baidu.com/",
            "Connection": "keep-alive",
        },
        timeout=20,
    )
    return r.status_code, r.text


def extract_biography_text(html: str, name: str) -> str:
    """
    Extract meaningful biographical text from Baidu Baike HTML.
    Captures: basic info box, career section paragraphs, and any
    lines containing career keywords.
    """
    soup = BeautifulSoup(html, "html.parser")

    for tag in soup(["script", "style", "nav", "header", "footer", "iframe"]):
        tag.decompose()

    sections = []

    # ── 1. Page title (contains role info) ─────────────────────────────────
    title_tag = soup.find("title")
    if title_tag and name in title_tag.text:
        sections.append(f"=== 词条标题 ===\n{title_tag.text.strip()}")

    # ── 2. Basic info box ────────────────────────────────────────────────────
    for cls_pat in [
        re.compile(r"basic-info|baseInfo|lemmaWgt-basicInfo"),
        re.compile(r"summary-wrapper|lemmaSummary"),
    ]:
        box = soup.find("div", class_=cls_pat)
        if box:
            sections.append(f"=== 基本信息 ===\n{box.get_text(separator=' | ', strip=True)}")
            break

    # ── 3. Article body (sections + paragraphs) ──────────────────────────────
    article = None
    for cls_pat in [
        re.compile(r"J-lemma-content|lemma-summary"),
        re.compile(r"content-wrapper"),
        re.compile(r"lemmaSummary"),
    ]:
        article = soup.find("div", class_=cls_pat)
        if article:
            break

    if article:
        current_section = "=== 人物简介 ==="
        section_texts: list[str] = []
        for elem in article.find_all(["h1", "h2", "h3", "h4", "p", "li"]):
            if elem.name in ["h1", "h2", "h3", "h4"]:
                if section_texts:
                    sections.append(f"{current_section}\n" + "\n".join(section_texts))
                    section_texts = []
                current_section = f"=== {elem.get_text(strip=True)} ==="
            else:
                text = elem.get_text(strip=True)
                if text and len(text) > 5:
                    section_texts.append(text)
        if section_texts:
            sections.append(f"{current_section}\n" + "\n".join(section_texts))

    # ── 4. Fallback: keyword-filtered full text ───────────────────────────────
    if not sections or sum(len(s) for s in sections) < 300:
        full_text = soup.get_text(separator="\n", strip=True)
        career_kws = ["市长", "书记", "省委", "市委", "人民政府", "任职", "历任",
                      "担任", "出生", "籍贯", "学历", "大学", "毕业", "年至", "年—",
                      "年－", "月－", "月—", "月-", "月至",
                      "国家计委", "发改委", "国家发展改革", "部委", "司长", "处长",
                      "副司长", "副处长", "厅长", "局长", "副省长", "省长", "部长",
                      name]
        # Also match lines starting with a 4-digit year (career timeline entries)
        year_pattern = re.compile(r"^\d{4}年")
        relevant = [
            line for line in full_text.split("\n")
            if (any(kw in line for kw in career_kws) or year_pattern.match(line))
            and len(line) > 10
        ]
        sections.append("=== 全文提取 ===\n" + "\n".join(relevant[:400]))

    return "\n\n".join(sections)


CITY_OFFICIAL_KEYWORDS = ["市长", "市委书记", "党委书记", "省委书记", "省长", "副省长",
                          "政治局", "国务院", "书记处", "中央委员", "人大常委"]


def is_correct_person(text: str, name: str, city: str) -> bool:
    """
    Heuristic check: does the extracted text describe a government official
    who served in the target city?
    Returns True if confident, False if likely the wrong person.
    """
    if not city:
        return True  # can't check without city
    # Must mention the city somewhere
    city_variants = [city, city + "市"]
    has_city = any(v in text for v in city_variants)
    # Must mention at least one government role keyword
    has_role = any(kw in text for kw in CITY_OFFICIAL_KEYWORDS)
    return has_city and has_role


def scrape_official(name: str, city: str = "", force: bool = False) -> tuple[bool, str]:
    """
    Scrape Baidu Baike for one official's biography.
    Returns (success, text_or_error).
    Skips if file already exists unless force=True.
    """
    output_path = OFFICIALS_DIR / f"{name}_biography.txt"

    if output_path.exists() and not force:
        size = output_path.stat().st_size
        print(f"  [SKIP] {name} — already exists ({size} bytes)")
        return True, output_path.read_text(encoding="utf-8")

    method = "curl_cffi (Chrome impersonation)" if HAS_CFFI else "requests (fallback)"
    print(f"  Using: {method}")

    urls = build_baike_urls(name, city)
    last_error = ""

    for i, (url, is_mobile) in enumerate(urls):
        delay = random.uniform(SCRAPE_DELAY_MIN, SCRAPE_DELAY_MAX)
        print(f"  [{i+1}/{len(urls)}] {'[mobile] ' if is_mobile else ''}Fetching... (delay {delay:.1f}s)")
        time.sleep(delay)

        try:
            status, html = cffi_get(url, is_mobile)

            if status == 200:
                if name in html and len(html) > 5000:
                    text = extract_biography_text(html, name)
                    if len(text) < 150:
                        last_error = "Extracted text too short — possibly wrong page"
                        continue

                    if not is_correct_person(text, name, city):
                        last_error = f"身份不符：页面未同时提及{city}和政务职务，可能是同名他人"
                        print(f"  [WARN] {last_error}")
                        continue

                    output_path.write_text(
                        f"官员：{name}\n来源：{url}\n\n{text}",
                        encoding="utf-8",
                    )
                    print(f"  ✓ Saved {len(text)} chars → {output_path.name}")
                    return True, text

                elif name not in html:
                    last_error = "官员姓名未在页面中找到（可能是同名词条或跳转）"

                else:
                    last_error = f"Page too short ({len(html)} chars)"

            elif status == 403:
                last_error = "403 Forbidden"
                print(f"  [WARN] 403 — backing off 10s")
                time.sleep(10)

            elif status == 404:
                last_error = "404 Not Found"

            else:
                last_error = f"HTTP {status}"

        except Exception as e:
            last_error = str(e)
            print(f"  [ERROR] {e}")

    if "身份不符" in last_error:
        print(f"  ✗ 所有URL均未通过身份验证: {last_error}")
        print(f"  💡 手动处理：从浏览器找到正确的{city}官员词条 → 复制全文 → 保存到 officials/{name}_biography.txt")
    else:
        print(f"  ✗ Failed: {last_error}")
        print(f"  💡 手动处理：从浏览器打开百度百科 → 复制全文 → 保存到 officials/{name}_biography.txt")
    return False, last_error


def scrape_all(officials: list[dict], city: str = "", force: bool = False) -> dict:
    """Scrape biographies for all officials."""
    existing = sum(
        1 for o in officials
        if (OFFICIALS_DIR / f"{o['name']}_biography.txt").exists() and not force
    )
    if existing:
        print(f"  [{existing} files already exist — will skip]")

    print(f"\n=== Phase 1: Scraping {len(officials)} official biographies ===")
    if HAS_CFFI:
        print("  Strategy: curl_cffi (Chrome TLS impersonation)")
    else:
        print("  Strategy: requests (install curl_cffi for better results)")

    success, failures = 0, []

    for i, official in enumerate(officials):
        name = official["name"]
        print(f"\n[{i+1}/{len(officials)}] {name}")
        ok, result = scrape_official(name, city, force)
        if ok:
            success += 1
        else:
            failures.append({"name": name, "error": result})

    if failures:
        fail_path = LOGS_DIR / "scrape_failures.txt"
        with open(fail_path, "w", encoding="utf-8") as f:
            f.write("以下官员爬取失败，请手动复制百度百科内容到 officials/{姓名}_biography.txt\n\n")
            for fl in failures:
                f.write(f"{fl['name']}: {fl['error']}\n")
        print(f"\n⚠ {len(failures)} failures logged to {fail_path}")

    print(f"\n✓ Phase 1 complete: {success}/{len(officials)} obtained")
    return {"success_count": success, "fail_count": len(failures), "failures": failures}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape Baidu Baike biographies")
    parser.add_argument("--names", default="logs/officials_list.json")
    parser.add_argument("--city", default="深圳")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--official", default="")
    args = parser.parse_args()

    if args.official:
        ok, result = scrape_official(args.official, args.city, args.force)
        if ok:
            print("\n--- Preview (first 800 chars) ---")
            print(result[:800])
    else:
        with open(args.names, encoding="utf-8") as f:
            officials = json.load(f)
        scrape_all(officials, args.city, args.force)
