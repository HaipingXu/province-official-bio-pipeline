"""
Try to find provincial secretary star maps via additional seed officials.
Strategy: visit former provincial secretaries who might have the star map linked.
"""
import json
import re
import time
import random
from urllib.parse import quote
from playwright.sync_api import sync_playwright

SECRETARY_PATTERNS = [
    r"历任.*省委书记",
    r"历任.*市委书记",
    r"历任.*自治区.*书记",
    r"历任.*党委书记",
]

GOVERNOR_PATTERNS = [
    r"历任.*省长",
    r"历任.*主席",
    r"历任.*市长",
]

# Try well-known former provincial secretaries with longer histories
EXTRA_SEEDS = [
    # 北京
    ("李希", None, "北京"),
    ("郭金龙", None, "北京"),
    # 天津
    ("李鸿忠", None, "天津"),
    # 河北
    ("张庆伟", None, "河北"),
    # 山西
    ("林武", None, "山西"),
    # 内蒙古
    ("石泰峰", None, "内蒙古"),
    # 辽宁
    ("张国清", None, "辽宁"),
    # 吉林
    ("巴音朝鲁", None, "吉林"),
    # 黑龙江
    ("张庆伟", None, "黑龙江"),
    # 上海
    ("韩正", None, "上海"),
    ("李强", 10810185, "上海"),
    # 江苏
    ("娄勤俭", None, "江苏"),
    # 浙江
    ("车俊", None, "浙江"),
    # 安徽
    ("李锦斌", None, "安徽"),
    # 福建
    ("于伟国", None, "福建"),
    # 江西
    ("刘奇", None, "江西"),
    # 山东
    ("刘家义", None, "山东"),
    # 河南
    ("王国生", None, "河南"),
    # 湖北
    ("应勇", None, "湖北"),
    # 湖南
    ("张庆伟", None, "湖南"),
    # 广东
    ("李希", None, "广东"),
    # 广西
    ("鹿心社", None, "广西"),
    # 海南
    ("沈晓明", None, "海南"),
    # 四川
    ("彭清华", None, "四川"),
    # 贵州
    ("陈敏尔", None, "贵州"),
    # 云南
    ("陈豪", None, "云南"),
    # 西藏
    ("吴英杰", None, "西藏"),
    # 陕西
    ("胡和平", None, "陕西"),
    # 甘肃
    ("林铎", None, "甘肃"),
    # 青海
    ("王建军", None, "青海"),
    # 宁夏
    ("石泰峰", None, "宁夏"),
    # 新疆
    ("陈全国", None, "新疆"),
]

discovered_sec = {}
all_starmaps = []
mount_responses = []

def handle_response(response):
    url = response.url
    if "getstarmapmountlist" in url:
        try:
            body = response.json()
            mount_responses.append(body)
        except:
            pass

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    ctx = browser.new_context(
        user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        viewport={"width": 1920, "height": 1080},
        locale="zh-CN",
    )
    page = ctx.new_page()
    page.on("response", handle_response)

    for i, (name, lemma_id, province) in enumerate(EXTRA_SEEDS):
        if province in discovered_sec:
            print(f"  [{i+1}/{len(EXTRA_SEEDS)}] {province} ({name}) — already found, skip")
            continue

        print(f"  [{i+1}/{len(EXTRA_SEEDS)}] {province}: visiting {name}...")

        if lemma_id:
            url = f"https://baike.baidu.com/item/{quote(name)}/{lemma_id}"
        else:
            url = f"https://baike.baidu.com/item/{quote(name)}"

        mount_responses.clear()
        try:
            page.goto(url, wait_until="networkidle", timeout=25000)
            time.sleep(2)
        except Exception as e:
            print(f"    [ERROR] {e}")
            continue

        for resp in mount_responses:
            if resp.get("errno") != 0:
                continue
            for star in resp.get("list", []):
                sm_name = star.get("name", "")
                sm_encode_id = star.get("encodeId", "")
                sm_count = star.get("lemmaCnt", 0)
                sm_items = star.get("list", [])
                sample_lid = sm_items[0].get("lemmaId", 1) if sm_items else 1

                all_starmaps.append({
                    "name": sm_name,
                    "encodeId": sm_encode_id,
                    "count": sm_count,
                    "source": f"{name}({province})",
                })

                if province not in sm_name:
                    continue

                for pat in SECRETARY_PATTERNS:
                    if re.search(pat, sm_name):
                        discovered_sec[province] = {
                            "starmap_name": sm_name,
                            "encodeId": sm_encode_id,
                            "count": sm_count,
                            "sample_lemmaId": sample_lid,
                        }
                        print(f"    ★ Secretary: {sm_name} (nodeId={sm_encode_id}, {sm_count} entries)")
                        break

        time.sleep(random.uniform(2, 3))

    browser.close()

# Show results
print(f"\n\n=== Secretary star maps found: {len(discovered_sec)}/{31} ===")
for prov, data in sorted(discovered_sec.items()):
    print(f"  {prov}: {data['starmap_name']} ({data['count']} entries)")

# Show all star maps with 书记 in the name
print(f"\n=== All star maps with 书记 ===")
for item in all_starmaps:
    if "书记" in item["name"] and "历任" in item["name"]:
        print(f"  {item['source']:20s} | {item['name']} | {item['encodeId']}")

# Save for merging
with open("temp/secretary_starmaps.json", "w", encoding="utf-8") as f:
    json.dump(discovered_sec, f, ensure_ascii=False, indent=2)
