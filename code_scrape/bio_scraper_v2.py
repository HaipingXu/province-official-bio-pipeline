"""
Phase 1 (v2): Three-Layer Biography Scraper

Layer 1: curl_cffi + improved HTML parsing (fast, ~2-3s)
Layer 2: Playwright + stealth fallback (JS-rendered, ~8-15s)
Layer 3: Chinese Wikipedia supplement (always appended if available)

Saves to officials/{name}_biography.txt
"""

import random
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests as std_requests
from bs4 import BeautifulSoup

from config import (
    LOGS_DIR, OFFICIALS_DIR, USER_AGENTS,
    SCRAPE_DELAY_MIN, SCRAPE_DELAY_MAX,
    SCRAPE_WORKERS,
)

OFFICIALS_DIR.mkdir(parents=True, exist_ok=True)

# ── Dependency detection ───────────────────────────────────────────────────────

try:
    from curl_cffi import requests as cffi_requests
    HAS_CFFI = True
except ImportError:
    cffi_requests = std_requests  # type: ignore
    HAS_CFFI = False

try:
    from playwright.sync_api import sync_playwright
    HAS_PLAYWRIGHT = True
except ImportError:
    HAS_PLAYWRIGHT = False

try:
    from playwright_stealth import stealth_sync
    HAS_STEALTH = True
except ImportError:
    HAS_STEALTH = False

CAREER_KEYWORDS = [
    "市长", "书记", "省委", "市委", "人民政府", "任职", "历任",
    "担任", "出生", "籍贯", "学历", "大学", "毕业", "年至", "年—",
    "年－", "月至", "国务院", "发改委", "部委", "司长", "处长",
    "副司长", "厅长", "局长", "副省长", "省长", "部长",
]
YEAR_LINE = re.compile(r"^\d{4}[年.．]")
CITY_OFFICIAL_ROLE_KWS = [
    "市长", "市委书记", "党委书记", "省委书记", "省长", "副省长",
    "政治局", "国务院", "书记处", "中央委员", "人大常委",
]

# ── 消歧义页面关键词（用于匹配政治人物义项）──────────────────────────────────
DISAMBIG_POLITICAL_KWS = [
    "省长", "省委", "书记", "市长", "市委", "政治局", "国务院",
    "人大", "政协", "中央委员", "部长", "副部长", "厅长", "主席",
    "自治区", "直辖市", "关工委", "组织部", "宣传部", "纪委",
]


# ── Disambiguation page detection & resolution ────────────────────────────────

def detect_disambiguation(html: str) -> list[dict]:
    """
    Detect if HTML is a Baidu Baike disambiguation page.
    Returns list of {'href': str, 'desc': str} for each item, or [] if not disambiguation.

    Disambiguation pages contain:
    - Text "多义词" or "请在下列义项中选择浏览"
    - Links with ?fromModule=disambiguation
    """
    if "多义词" not in html and "义项" not in html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    items = []

    # Method 1: Find links with disambiguation module parameter
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "fromModule=disambiguation" in href or "disambiguation" in href:
            desc = a.get_text(strip=True)
            if desc and len(desc) > 2:
                items.append({"href": href, "desc": desc})

    # Method 2: Look for polysemant list structure (class-based)
    if not items:
        for a in soup.find_all("a", href=True):
            href = a["href"]
            # Pattern: /item/名字/数字ID
            if re.match(r"/item/.+/\d+", href) and a.get_text(strip=True):
                desc = a.get_text(strip=True)
                if len(desc) > 2:
                    items.append({"href": href, "desc": desc})

    return items


def resolve_disambiguation(items: list[dict], name: str,
                           province: str = "", role: str = "") -> str | None:
    """
    Pick the correct disambiguation item for a political official.
    Uses province/role context for higher precision when available.
    Returns full URL or None.
    """
    if not items:
        return None

    # Score each item by political keyword matches + context
    scored = []
    for item in items:
        desc = item["desc"]
        score = sum(1 for kw in DISAMBIG_POLITICAL_KWS if kw in desc)
        # Province context boost (strong signal)
        if province and province in desc:
            score += 5
        # Role context boost
        if role and role in desc:
            score += 3
        scored.append((score, item))

    # Sort by score descending
    scored.sort(key=lambda x: -x[0])

    best_score, best_item = scored[0]
    if best_score > 0:
        href = best_item["href"]
        if not href.startswith("http"):
            href = f"https://baike.baidu.com{href}"
        # Remove disambiguation module param for clean URL
        href = re.sub(r'\?fromModule=disambiguation', '', href)
        ctx = f", province={province}" if province else ""
        print(f"    [消歧义] 选择: {best_item['desc']} (匹配{best_score}个关键词{ctx})")
        return href

    # No political match → take first item as fallback
    href = items[0]["href"]
    if not href.startswith("http"):
        href = f"https://baike.baidu.com{href}"
    href = re.sub(r'\?fromModule=disambiguation', '', href)
    print(f"    [消歧义] 无政治关键词匹配，选择第一项: {items[0]['desc']}")
    return href


# ── Layer 1: curl_cffi ─────────────────────────────────────────────────────────

def cffi_get(url: str) -> tuple[int, str]:
    ua = random.choice(USER_AGENTS)
    r = cffi_requests.get(
        url,
        impersonate="chrome120" if HAS_CFFI else None,
        headers={
            "User-Agent": ua,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8",
            "Referer": "https://www.baidu.com/",
        },
        timeout=20,
    )
    return r.status_code, r.text


# ── Layer 2: Playwright ────────────────────────────────────────────────────────

# ── Playwright browser reuse ──────────────────────────────────────────────────

_pw_instance = None  # Playwright context manager
_pw_browser = None   # Reusable browser instance


def _get_browser():
    """Get or create a shared Playwright browser instance."""
    global _pw_instance, _pw_browser
    if _pw_browser is not None:
        return _pw_browser
    if not HAS_PLAYWRIGHT:
        return None
    _pw_instance = sync_playwright().start()
    _pw_browser = _pw_instance.chromium.launch(
        headless=True,
        args=["--disable-blink-features=AutomationControlled",
              "--no-sandbox", "--disable-dev-shm-usage"],
    )
    return _pw_browser


def close_browser():
    """Close the shared browser instance. Call after scrape_all() finishes."""
    global _pw_instance, _pw_browser
    if _pw_browser:
        _pw_browser.close()
        _pw_browser = None
    if _pw_instance:
        _pw_instance.stop()
        _pw_instance = None


def playwright_get(url: str) -> tuple[int, str]:
    """Full JS rendering via Playwright (reuses browser). Returns (status, html)."""
    browser = _get_browser()
    if browser is None:
        return 0, ""
    try:
        ctx = browser.new_context(
            user_agent=random.choice(USER_AGENTS),
            viewport={"width": 1920, "height": 1080},
            locale="zh-CN",
        )
        page = ctx.new_page()
        if HAS_STEALTH:
            stealth_sync(page)

        # Fast load: domcontentloaded first, then wait for content selector
        page.goto(url, wait_until="domcontentloaded", timeout=15000)

        # Wait for the main content area (Baidu Baike specific selectors)
        for sel in ["div.J-lemma-content", "div[class*='lemmaContent']",
                     "div[class*='content']", "div[class*='lemma']", "article", "main"]:
            try:
                page.wait_for_selector(sel, timeout=5000)
                break
            except Exception:
                pass

        html = page.content()
        ctx.close()
        return 200, html
    except Exception as e:
        print(f"    [Playwright error] {e}")
        return 0, ""


# ── Layer 3: Wikipedia supplement ─────────────────────────────────────────────

def scrape_wikipedia_bio(name: str) -> str:
    """Fetch individual Wikipedia biography page."""
    # Try name directly, then with disambiguation hint
    urls = [
        f"https://zh.wikipedia.org/wiki/{std_requests.utils.quote(name)}",
        f"https://zh.wikipedia.org/wiki/{std_requests.utils.quote(name + '（官员）')}",
    ]
    for url in urls:
        try:
            resp = std_requests.get(
                url,
                headers={"User-Agent": "Mozilla/5.0", "Accept-Language": "zh-CN,zh;q=0.9"},
                timeout=10,
            )
            if resp.status_code != 200:
                continue
            soup = BeautifulSoup(resp.text, "html.parser")

            # Disambiguation page? skip
            if soup.find("div", id="disambig") or "消歧义" in soup.title.text if soup.title else False:
                continue

            content = soup.find("div", id="mw-content-text")
            if not content:
                continue

            text = content.get_text(separator="\n", strip=True)
            # Only return if it looks like an official's page
            if any(kw in text for kw in CITY_OFFICIAL_ROLE_KWS):
                return text[:6000]  # cap to avoid overwhelming the LLM
        except Exception:
            pass
    return ""


# ── HTML parsing (improved, class-name-agnostic) ──────────────────────────────

def _match_class(cls_list: list[str] | None, *keywords: str) -> bool:
    """Check if any CSS class contains any of the given keywords (case-insensitive)."""
    if not cls_list:
        return False
    joined = " ".join(cls_list).lower()
    return any(kw in joined for kw in keywords)


def extract_biography_text_v2(html: str, name: str) -> str:
    """
    Extract biography text from Baidu Baike HTML.

    Baidu Baike uses CSS-module hashed class names (e.g. lemmaSummary_RPxMM,
    basicInfo_o5Omt, para_iF7OU). We match by class name PREFIX (before the hash).

    Strategy:
    1. Title tag
    2. lemmaSummary → intro paragraph
    3. basicInfo → structured info box (birth, education, etc.)
    4. J-lemma-content → main content area with para divs
    5. Career paragraphs by content heuristics (fallback)
    6. Tables with tenure/time headers
    7. Plain-text fallback (year-line filter)
    """
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "nav", "header", "footer", "iframe", "aside"]):
        tag.decompose()

    # Remove reference section (lemmaReference) to avoid capturing citations
    for ref in soup.find_all("div", class_=lambda c: _match_class(
            c if isinstance(c, list) else [c] if c else [], "lemmaref", "reference")):
        ref.decompose()

    sections: list[str] = []

    # ── 1. Title ───────────────────────────────────────────────────────────────
    title = soup.find("title")
    if title and name in title.text:
        sections.append(f"=== 词条标题 ===\n{title.text.strip()}")

    # ── 2. lemmaSummary → intro paragraph ──────────────────────────────────────
    summary = soup.find("div", class_=lambda c: _match_class(
        c if isinstance(c, list) else [c] if c else [], "lemmasummary"))
    if summary:
        text = summary.get_text(strip=True)
        # Strip citation markers like [1][2]
        text = re.sub(r"\[\d+(?:-\d+)?\]", "", text)
        if len(text) > 20:
            sections.append(f"=== 人物简介 ===\n{text}")

    # ── 3. basicInfo → structured info box ─────────────────────────────────────
    basic = soup.find("div", class_=lambda c: _match_class(
        c if isinstance(c, list) else [c] if c else [], "basicinfo"))
    if not basic:
        basic = soup.find("div", class_=lambda c: _match_class(
            c if isinstance(c, list) else [c] if c else [], "j-basic-info"))
    if basic:
        text = basic.get_text(separator=" | ", strip=True)
        if len(text) > 20:
            sections.append(f"=== 基本信息 ===\n{text[:1000]}")

    # ── 4. J-lemma-content → main content paragraphs ───────────────────────────
    content = soup.find("div", class_="J-lemma-content")
    if content:
        para_lines: list[str] = []
        for div in content.find_all("div", class_=lambda c: _match_class(
                c if isinstance(c, list) else [c] if c else [], "para")):
            text = div.get_text(strip=True)
            # Strip citation markers
            text = re.sub(r"\[\d+(?:-\d+)?\]", "", text)
            if len(text) >= 10:
                para_lines.append(text)
        if para_lines:
            sections.append("=== 履历段落 ===\n" + "\n".join(para_lines[:300]))

    # ── 5. Career paragraphs by content heuristics (fallback) ──────────────────
    if not any("履历段落" in s for s in sections):
        career_lines: list[str] = []
        for elem in soup.find_all(["p", "li", "td", "dd"]):
            text = elem.get_text(strip=True)
            if len(text) < 10:
                continue
            has_year = bool(re.search(r"(19|20)\d{2}", text))
            has_kw = any(kw in text for kw in CAREER_KEYWORDS)
            if has_year or has_kw:
                career_lines.append(text)
        if career_lines:
            sections.append("=== 履历段落 ===\n" + "\n".join(career_lines[:300]))

    # ── 6. Tables with tenure/time headers ─────────────────────────────────────
    for table in soup.find_all("table"):
        header_text = ""
        for th in table.find_all("th"):
            header_text += th.get_text(strip=True)
        if any(kw in header_text for kw in ["时间", "任职", "起止", "年份", "职务"]):
            sections.append("=== 履历表格 ===\n" + table.get_text(separator="\t", strip=True)[:2000])

    # ── 7. Plain-text fallback (year-line filter) ──────────────────────────────
    if not sections or sum(len(s) for s in sections) < 400:
        full = soup.get_text(separator="\n", strip=True)
        relevant = [
            ln for ln in full.split("\n")
            if (any(kw in ln for kw in CAREER_KEYWORDS) or YEAR_LINE.match(ln))
            and len(ln) > 10
        ]
        sections.append("=== 全文关键词提取 ===\n" + "\n".join(relevant[:400]))

    return "\n\n".join(sections)


# ── Identity check ─────────────────────────────────────────────────────────────

def is_correct_person(text: str, name: str, city: str = "",
                      province: str = "") -> bool:
    """Check if scraped text belongs to the correct political official.

    Supports both city-level (city param) and province-level (province param).
    Returns True if location + role keywords both match, or if no location given.
    """
    if not city and not province:
        return True
    # Location check: province or city
    has_location = False
    if province:
        # Strip trailing "省"/"自治区"/"市" for flexible matching
        prov_short = re.sub(r"(省|自治区|回族自治区|维吾尔自治区|壮族自治区|特别行政区|市)$", "", province)
        has_location = province in text or prov_short in text
    if city and not has_location:
        has_location = city in text or (city + "市") in text
    has_role = any(kw in text for kw in CITY_OFFICIAL_ROLE_KWS)
    return has_location and has_role


# ── Quality scoring ───────────────────────────────────────────────────────────

def quality_score(text: str, name: str) -> int:
    """Score scraped biography quality (0-100). Used to decide auto-retry."""
    score = 0
    if "=== 履历段落 ===" in text:
        score += 40
    if "=== 人物简介 ===" in text:
        score += 20
    if "=== 基本信息 ===" in text:
        score += 15
    # Year-line density (proxy for career detail)
    year_lines = len(re.findall(r"\d{4}[年.．]", text))
    score += min(year_lines * 2, 25)
    return score


QUALITY_THRESHOLD = 60  # Below this → auto-retry with search engine URL


# ── Search engine fallback ───────────────────────────────────────────────────

def search_baike_url(name: str, province: str = "", city: str = "") -> str | None:
    """Use Baidu search to find the correct Baike item-ID URL.

    Searches for: "{name} {province/city} site:baike.baidu.com"
    Returns the first matching baike URL with item-ID, or None.
    """
    location = province or city
    query = f"{name} {location} site:baike.baidu.com" if location else f"{name} site:baike.baidu.com"
    search_url = f"https://www.baidu.com/s?wd={std_requests.utils.quote(query)}"

    try:
        status, html = cffi_get(search_url)
        if status != 200:
            print(f"    [搜索] 百度搜索失败: status={status}")
            return None
        # Extract baike URLs with item-ID (e.g., baike.baidu.com/item/袁家军/944417)
        matches = re.findall(r"baike\.baidu\.com/item/[^\"&\s<>]+/\d+", html)
        if matches:
            url = f"https://{matches[0]}"
            print(f"    [搜索] 找到精确URL: {url}")
            return url
        # Fallback: any baike URL
        matches = re.findall(r"baike\.baidu\.com/item/[^\"&\s<>]+", html)
        if matches:
            url = f"https://{matches[0]}"
            print(f"    [搜索] 找到URL (无item-ID): {url}")
            return url
    except Exception as e:
        print(f"    [搜索] 搜索引擎错误: {e}")
    return None


# ── Main scrape function ───────────────────────────────────────────────────────

def build_baike_urls(name: str, city: str = "", province: str = "") -> list[str]:
    q = std_requests.utils.quote
    urls = [
        f"https://baike.baidu.com/item/{q(name)}",
        f"https://m.baike.baidu.com/item/{q(name)}",
    ]
    if city:
        urls.append(f"https://baike.baidu.com/item/{q(name + '（' + city + '市官员）')}")
    if province:
        urls.append(f"https://baike.baidu.com/item/{q(name + '（' + province + '官员）')}")
    return urls


def _try_scrape_url(url: str, name: str, use_playwright: bool = True) -> tuple[str, str, str]:
    """Try scraping a single URL. Returns (text, html, layer_used) or ("", "", "")."""
    if use_playwright and HAS_PLAYWRIGHT:
        status, html = playwright_get(url)
        if status == 200 and len(html) > 5000:
            return extract_biography_text_v2(html, name), html, "playwright"
    # curl_cffi
    try:
        status, html = cffi_get(url)
        if status == 200 and len(html) > 5000:
            return extract_biography_text_v2(html, name), html, "curl_cffi"
        if status == 403:
            print(f"    [WARN] 403 TLS封锁，退避 10s")
            time.sleep(10)
    except Exception as e:
        print(f"    [WARN] curl_cffi error: {e}")
    return "", "", ""


def scrape_official(name: str, city: str = "", force: bool = False,
                    min_chars: int = 500,
                    output_dir: Path | None = None,
                    playwright_first: bool = False,
                    province: str = "") -> tuple[bool, str]:
    """
    Multi-layer scrape for one official (Baidu Baike).

    Layers (in order):
      1. Primary scraper (Playwright or curl_cffi based on playwright_first)
      2. Fallback scraper (the other one)
      3. Search engine URL resolution (if quality < threshold or identity mismatch)
      4. Quality scoring + auto-retry

    Args:
        province: Province name for identity check & disambiguation (省级模式).
    Returns (success, combined_text).
    """
    _dir = output_dir or OFFICIALS_DIR
    output_path = _dir / f"{name}_biography.txt"

    if output_path.exists() and not force:
        size = output_path.stat().st_size
        print(f"  [SKIP] {name} — already exists ({size} bytes)")
        return True, output_path.read_text(encoding="utf-8")

    print(f"  Scraping: {name}" + (f" ({province})" if province else ""))
    combined_text = ""
    layer_used = ""

    urls = build_baike_urls(name, city, province)

    # ── Phase A: Primary + Fallback scraping ──────────────────────────────────
    for url in urls:
        delay = random.uniform(SCRAPE_DELAY_MIN, SCRAPE_DELAY_MAX)
        time.sleep(delay)

        # Try primary layer
        text, html, layer = _try_scrape_url(url, name, use_playwright=playwright_first)

        # If primary failed, try the other layer
        if len(text) < min_chars:
            time.sleep(random.uniform(1, 3))
            text, html, layer = _try_scrape_url(url, name, use_playwright=not playwright_first)

        if len(text) < min_chars or not html:
            continue

        # ── 消歧义检测 ──
        disambig_items = detect_disambiguation(html)
        if disambig_items:
            resolved_url = resolve_disambiguation(disambig_items, name,
                                                  province=province)
            if resolved_url:
                time.sleep(random.uniform(SCRAPE_DELAY_MIN, SCRAPE_DELAY_MAX))
                text, html, layer = _try_scrape_url(resolved_url, name,
                                                     use_playwright=playwright_first)
                if len(text) < min_chars:
                    text, html, layer = _try_scrape_url(resolved_url, name,
                                                         use_playwright=not playwright_first)
                if len(text) < min_chars:
                    continue
            else:
                continue

        # ── 身份校验 ──
        if is_correct_person(text, name, city=city, province=province):
            combined_text = text
            layer_used = layer
            print(f"  ✓ {layer}: {len(text)} 字符 (质量={quality_score(text, name)})")
            break
        else:
            print(f"  [WARN] {layer}: 身份验证未通过，尝试下一URL")

    # ── Phase B: Quality check → search engine retry ─────────────────────────
    q_score = quality_score(combined_text, name) if combined_text else 0
    needs_retry = (
        len(combined_text) < min_chars
        or q_score < QUALITY_THRESHOLD
        or (combined_text and not is_correct_person(combined_text, name,
                                                     city=city, province=province))
    )

    if needs_retry:
        reason = "质量不足" if q_score < QUALITY_THRESHOLD else "文本不足或身份不匹配"
        print(f"  → {reason} (质量={q_score})，尝试搜索引擎获取精确URL...")
        time.sleep(random.uniform(2, 5))
        search_url = search_baike_url(name, province=province, city=city)
        if search_url:
            time.sleep(random.uniform(SCRAPE_DELAY_MIN, SCRAPE_DELAY_MAX))
            text, html, layer = _try_scrape_url(search_url, name,
                                                 use_playwright=playwright_first)
            if len(text) < min_chars:
                text, html, layer = _try_scrape_url(search_url, name,
                                                     use_playwright=not playwright_first)
            if len(text) >= min_chars:
                new_score = quality_score(text, name)
                if new_score > q_score or is_correct_person(text, name,
                                                              city=city, province=province):
                    combined_text = text
                    layer_used = f"{layer}+搜索"
                    q_score = new_score
                    print(f"  ✓ 搜索引擎重试成功: {len(text)} 字符 (质量={q_score})")

    # ── Result ─────────────────────────────────────────────────────────────────
    if not combined_text or len(combined_text.strip()) < 200:
        print(f"  ✗ 所有层均失败")
        print(f"  💡 手动处理：从浏览器复制百度百科内容 → officials/{name}_biography.txt")
        return False, "all layers failed"

    full_text = f"官员：{name}\n层次：{layer_used or '手动'}\n\n{combined_text}"
    output_path.write_text(full_text, encoding="utf-8")
    print(f"  ✓ 保存: {output_path.name} ({len(full_text)} 字符总计, 质量={q_score})")
    return True, combined_text


def scrape_all(officials: list[dict], city: str = "", force: bool = False,
               max_workers: int = SCRAPE_WORKERS,
               output_dir: Path | None = None,
               playwright_first: bool = False,
               province: str = "") -> dict:
    print(f"\n=== Phase 1 (v2): 百度百科爬取 {len(officials)} 位官员 ===")
    caps = []
    if HAS_CFFI:       caps.append("curl_cffi")
    if HAS_PLAYWRIGHT: caps.append("Playwright")
    print(f"  可用层次: {' → '.join(caps) or '无（需手动）'}")
    print(f"  并发 workers: {max_workers}")
    if province:
        print(f"  省份上下文: {province}")

    success, failures = 0, []

    if max_workers <= 1:
        # Serial mode
        for i, official in enumerate(officials):
            name = official["name"]
            print(f"\n[{i+1}/{len(officials)}] {name}")
            ok, result = scrape_official(name, city, force, output_dir=output_dir,
                                         playwright_first=playwright_first,
                                         province=province)
            if ok:
                success += 1
            else:
                failures.append({"name": name, "error": result})
    else:
        # Concurrent mode
        futures_map = {}
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            for i, official in enumerate(officials):
                name = official["name"]
                future = pool.submit(scrape_official, name, city, force,
                                     output_dir=output_dir,
                                     playwright_first=playwright_first,
                                     province=province)
                futures_map[future] = (i, name)

            for future in as_completed(futures_map):
                idx, name = futures_map[future]
                try:
                    ok, result = future.result()
                    if ok:
                        success += 1
                        print(f"  ✓ [{idx+1}/{len(officials)}] {name} 完成")
                    else:
                        failures.append({"name": name, "error": result})
                        print(f"  ✗ [{idx+1}/{len(officials)}] {name} 失败")
                except Exception as e:
                    failures.append({"name": name, "error": str(e)})
                    print(f"  ✗ [{idx+1}/{len(officials)}] {name} 异常: {e}")

    if failures:
        fail_path = LOGS_DIR / "scrape_failures.txt"
        fail_path.write_text(
            "以下官员爬取失败，请手动保存百度百科内容到 officials/{姓名}_biography.txt\n\n"
            + "\n".join(f["name"] for f in failures),
            encoding="utf-8",
        )
        print(f"\n⚠ {len(failures)} 失败 → {fail_path}")

    # Close shared browser if it was used
    close_browser()

    print(f"\n✓ Phase 1 完成: {success}/{len(officials)} 成功")
    return {"success_count": success, "fail_count": len(failures), "failures": failures}


if __name__ == "__main__":
    import argparse, json
    parser = argparse.ArgumentParser()
    parser.add_argument("--official", default="")
    parser.add_argument("--city", default="深圳")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--list", default="logs/officials_list.json")
    args = parser.parse_args()

    if args.official:
        ok, text = scrape_official(args.official, args.city, args.force)
        if ok:
            print(f"\n--- Preview (first 600 chars) ---\n{text[:600]}")
    else:
        with open(args.list, encoding="utf-8") as f:
            officials = json.load(f)
        scrape_all(officials, args.city, args.force)
