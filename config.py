"""
Project configuration.
API keys are loaded from environment variables or .env file.
"""

import logging
import os
import sys
from pathlib import Path
from typing import Optional

# Load .env if present
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# --- API Configuration ---

def _load_keys(env_var: str) -> tuple[list[str], str]:
    """Load comma-separated API keys from an environment variable.
    Returns (list_of_keys, first_key_or_empty_string)."""
    raw = os.environ.get(env_var, "")
    keys = [k.strip() for k in raw.split(",") if k.strip()]
    return keys, (keys[0] if keys else "")

# Single key (backward-compatible) — first key from comma-separated list
DEEPSEEK_API_KEYS, DEEPSEEK_API_KEY = _load_keys("DEEPSEEK_API_KEY")
DEEPSEEK_BASE_URL = "https://api.deepseek.com"
DEEPSEEK_MODEL = "deepseek-v4-flash"  # DeepSeek V4 Flash (kept for reference)

QWEN_API_KEYS, QWEN_API_KEY = _load_keys("QWEN_API_KEY")
QWEN_BASE_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1"
QWEN_MODEL = "qwen3.5-plus"  # Qwen3.5-plus (verifier fallback)

# --- LLM1: Extractor (Qwen3.6-plus via DashScope) ---
LLM1_API_KEY = QWEN_API_KEY
LLM1_API_KEYS = QWEN_API_KEYS
LLM1_BASE_URL = QWEN_BASE_URL
LLM1_MODEL = "qwen3.6-plus"

KIMI_API_KEYS, KIMI_API_KEY = _load_keys("KIMI_API_KEY")
KIMI_BASE_URL = "https://api.moonshot.cn/v1"
KIMI_MODEL = "kimi-k2.5"  # Kimi K2.5 judge

# --- Doubao (verification LLM — preferred over GLM/Qwen) ---
DOUBAO_API_KEYS, DOUBAO_API_KEY = _load_keys("DOUBAO_API_KEY")
DOUBAO_BASE_URL = "https://ark.cn-beijing.volces.com/api/v3"
DOUBAO_MODEL = "doubao-seed-2-0-pro-260215"

# --- LLM2: Verifier (Doubao-seed-2.0-pro — primary; GLM/Qwen as fallback) ---
LLM2_API_KEY = DOUBAO_API_KEY
LLM2_API_KEYS = DOUBAO_API_KEYS
LLM2_BASE_URL = DOUBAO_BASE_URL
LLM2_MODEL = DOUBAO_MODEL

# --- Judge: DeepSeek-V4-Pro (current default judge model) ---
JUDGE_API_KEY = DEEPSEEK_API_KEY
JUDGE_API_KEYS = DEEPSEEK_API_KEYS
JUDGE_BASE_URL = DEEPSEEK_BASE_URL
JUDGE_MODEL = "deepseek-v4-pro"

# --- GLM-5 (verification LLM — fallback after Doubao) ---
GLM_API_KEYS, GLM_API_KEY = _load_keys("GLM_API_KEY")
GLM_BASE_URL = "https://api.siliconflow.cn/v1"
GLM_MODEL = "Pro/zai-org/GLM-5"

# Optional: kept for reference, no longer primary judge
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
ANTHROPIC_MODEL = "claude-sonnet-4-5"

# --- Paths ---
PROJECT_ROOT = Path(__file__).parent
OFFICIALS_DIR = PROJECT_ROOT / "officials"
OUTPUT_DIR = PROJECT_ROOT / "output"
LOGS_DIR = PROJECT_ROOT / "logs"
DATA_DIR = PROJECT_ROOT / "data"
SKILLS_DIR = PROJECT_ROOT / ".claude" / "skills"

# --- Output Column Names — v9 schema (4-step pipeline, with per-step judge cols) ---
# Pipeline:
#   step1: 起始时间, 终止时间, 供职单位, 职务   (judge1)
#   step2: 组织标签, 标志位, 任职地（省/市）, 中央/地方   (judge2)
#   step3: 该条行政级别                          (judge3)
#   step4: raw_bio + 升迁/本省提拔/本省学习 + 落马  (judge4)
COLUMNS = [
    # --- Person-level ---
    "年份",
    "省份",
    "姓名",
    "出生年份",
    "籍贯",
    "籍贯（市）",
    "少数民族",
    "女性",
    "全日制本科",
    "升迁_省长",
    "升迁_省委书记",
    "本省提拔",
    "本省学习",
    "judge4con",      # confidence + reason for step4 label/bio judges
    "最终行政级别",
    # --- Per-row ---
    "经历序号",
    # --- Step1 fields + judge1con ---
    "起始时间",
    "终止时间",
    "供职单位",
    "职务",
    "judge1con",
    # --- Step2 fields + judge2con ---
    "组织标签",
    "标志位",
    "任职地（省）",
    "任职地（市）",
    "中央/地方",
    "judge2con",
    # --- Step3 field + judge3con ---
    "该条行政级别",
    "judge3con",
    # --- 引用 ---
    "原文引用",
    # --- 落马（来自 step4，落马字段不再单独输出 judge col；归入 judge4con）---
    "是否落马",
    "落马原因",
    "备注栏",
]

# --- 31+1 Organizational Tags ---
ORG_TAGS = [
    "党中央机关",
    "地方党委",
    "国务院组成部门",
    "地方政府",
    "直属机构/部委管理的国家局",
    "全国人大机关",
    "地方人大",
    "全国政协机关",
    "地方政协",
    "最高法院/最高检察院",
    "地方法院/检察院",
    "中央军委机关",
    "战区/军种/部队",
    "国资委央企",
    "中央金融企业",
    "省属国企",
    "市属/县属国企",
    "共青团中央",
    "地方团委",
    "民主党派中央机关",
    "地方民主党派组织",
    "全国性人民团体",
    "地方性人民团体/行业协会",
    "教育部直属高校",
    "部属高校（非教育部）",
    "地方属高校",
    "国外高校",
    "中小学/职业院校",
    "科研院所（中央）",        # 部委直属科研院所/事业单位（如水利水电科学研究院）
    "科研院所（地方）",        # 省/市属科研院所/事业单位（如广州市建筑科学研究所、省农科院）
    "村/社区\"两委\"",
    "乡镇站所",
    "其他",
]

# --- Position-Type Tags (标志位, 23 categories) ---
POSITION_TAGS = [
    "市委书记",
    "市长",
    "市委副书记（市长）",
    "市委副书记（非市长）",
    "副市长（常委）",
    "副市长（非常委）",
    "市常委（其他）",
    "市组织部长",
    "省委书记",
    "省长",
    "省委副书记（省长）",
    "省委副书记（非省长）",
    "副省长（常委）",
    "副省长（非常委）",
    "省常委（其他）",
    "省组织部长",
    "省组织部副部长",
    "军队",
    "学习进修",       # v5.3: was "学校", split into 学习进修 + 高校/党校任职
    "高校/党校任职",   # v5.3 NEW: teaching/admin at university or party school
    "学校",           # backward compat: old data may still have this
    "秘书",
    "政治局常委",
    "政治局委员",
    "无",
]

# --- Administrative Rank Levels (highest to lowest) ---
RANK_LEVELS = [
    "正国级", "副国级",
    "正部级", "副部级",
    "正厅级", "副厅级",
    "正处级", "副处级",
    "正科级", "副科级",
]

# Sentinel for early-career/secretary/cadre rows where rank cannot be inferred.
# Treated as "无层级" by get_highest_rank (does not participate in person-level max).
RANK_HARD_TO_JUDGE = "难以判断"


def get_highest_rank(ranks: list[str]) -> str:
    """Return the highest rank from a list of rank strings.

    DEPRECATED: Use utils.get_highest_rank instead. Kept for backward compatibility.
    """
    best_idx = len(RANK_LEVELS)
    for r in ranks:
        r = r.strip()
        if r in RANK_LEVELS:
            idx = RANK_LEVELS.index(r)
            if idx < best_idx:
                best_idx = idx
    return RANK_LEVELS[best_idx] if best_idx < len(RANK_LEVELS) else ""


# --- Position Level Classification ---
# Positions at 厅局级 (bureau level) or below → 厅局级及以下 = 1
BUREAU_LEVEL_KEYWORDS = [
    "局长", "副局长", "处长", "副处长", "科长", "副科长",
    "厅长", "副厅长", "局党组", "处级", "厅级",
    "所长", "副所长", "主任", "副主任",  # context-dependent
    "研究生", "本科生", "进修学员", "实习生",  # study rows
]
# Positions at 副省级 or above → 厅局级及以下 = 0
VICE_PROVINCIAL_KEYWORDS = [
    "省长", "副省长", "省委书记", "省委副书记",
    "市长", "副市长", "市委书记", "市委副书记",  # for 副省级 cities
    "部长", "副部长", "部党组书记",
    "省委常委", "市委常委",
    "政治局", "国务院", "总书记", "主席", "副主席",
    "副国级", "国级",
]

# --- Province name normalization (LLM may output shorthand) ---
PROVINCE_NORMALIZE: dict[str, str] = {
    # Direct full-name → unchanged (identity mappings not needed)
    # Shorthand → full official name
    "北京": "北京市", "上海": "上海市", "天津": "天津市", "重庆": "重庆市",
    "广东": "广东省", "广州": "广东省",  # 广州 used by mistake
    "浙江": "浙江省", "江苏": "江苏省", "山东": "山东省", "四川": "四川省",
    "湖北": "湖北省", "湖南": "湖南省", "河南": "河南省", "河北": "河北省",
    "安徽": "安徽省", "福建": "福建省", "江西": "江西省", "辽宁": "辽宁省",
    "吉林": "吉林省", "黑龙江": "黑龙江省", "陕西": "陕西省", "山西": "山西省",
    "云南": "云南省", "贵州": "贵州省", "甘肃": "甘肃省", "海南": "海南省",
    "宁夏": "宁夏回族自治区", "西藏": "西藏自治区", "新疆": "新疆维吾尔自治区",
    "内蒙古": "内蒙古自治区", "广西": "广西壮族自治区",
    "香港": "香港特别行政区", "澳门": "澳门特别行政区",
    # Special/central postings
    "国外": "国外", "中央": "中央",
}

# City name normalization (common shorthands → full names)
CITY_NORMALIZE: dict[str, str] = {
    "深圳": "深圳市", "广州": "广州市", "北京": "北京市", "上海": "上海市",
    "天津": "天津市", "重庆": "重庆市", "成都": "成都市", "杭州": "杭州市",
    "南京": "南京市", "武汉": "武汉市", "西安": "西安市", "长沙": "长沙市",
    "郑州": "郑州市", "济南": "济南市", "沈阳": "沈阳市", "哈尔滨": "哈尔滨市",
}

# --- Direct-administered Municipalities ---
DIRECT_MUNICIPALITIES = {"北京", "上海", "天津", "重庆"}

# --- 31 Province Short Names ---
PROVINCE_NAMES = {
    "北京", "天津", "河北", "山西", "内蒙古",
    "辽宁", "吉林", "黑龙江",
    "上海", "江苏", "浙江", "安徽", "福建", "江西", "山东",
    "河南", "湖北", "湖南", "广东", "广西", "海南",
    "重庆", "四川", "贵州", "云南", "西藏",
    "陕西", "甘肃", "青海", "宁夏", "新疆",
}

# --- Episode Fields (shared across verifier, battle, postprocess) ---
# v9: split episode fields by pipeline step.
STEP1_EPISODE_FIELDS = [
    "起始时间", "终止时间", "供职单位", "职务",
]
STEP2_EPISODE_FIELDS = [
    "组织标签", "标志位",
    "任职地（省）", "任职地（市）", "中央/地方",
]
EPISODE_FIELDS = STEP1_EPISODE_FIELDS + STEP2_EPISODE_FIELDS

EP_CHECK_FIELDS = EPISODE_FIELDS + ["行政级别"]

# --- Judge confidence threshold ---
# Decisions with confidence below this value flow into "争议未解决".
# All confidence scores (regardless of value) are surfaced in the
# per-step judgeNcon columns.
JUDGE_CONF_THRESHOLD = 85

# --- Scraping ---
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]
SCRAPE_DELAY_MIN = 2.0
SCRAPE_DELAY_MAX = 5.0

# --- Verification Thresholds ---
DATE_DISCREPANCY_YEARS = 1   # flag if dates differ by more than this
EPISODE_COUNT_DIFF = 2       # flag if episode count differs by more than this

# --- Concurrency ---
# These control max parallel API calls per pipeline phase.
# Higher values improve throughput but risk rate-limiting (429 errors).
DEFAULT_WORKERS = 100      # Default API call parallelism
LLM1_MAX_WORKERS = 100     # Extractor (LLM1/Qwen) — high: DashScope is generous
LLM2_MAX_WORKERS = 100     # Verifier (LLM2/Doubao) — matches LLM1 for parallel extraction
JUDGE_MAX_WORKERS = 200    # Judge (DeepSeek Reasoner) — highest: judge calls are per-diff, many small requests
SCRAPE_WORKERS = 2         # Scraping parallelism (low to avoid anti-bot detection)


# --- Retry / Backoff (used by utils.llm_chat) ---
# All `max_retries=3` after consolidation; LLM1/LLM2/judge share defaults.
LLM1_MAX_RETRIES = 3
LLM2_MAX_RETRIES = 3
JUDGE_MAX_RETRIES = 3

# 429 backoff: wait = BASE * (FACTOR ** attempt) + jitter   →  10s, 30s, 90s, ...
LLM_429_BACKOFF_BASE = 10.0
LLM_429_BACKOFF_FACTOR = 3.0
LLM_429_BACKOFF_CAP = 180.0   # absolute ceiling per attempt

# Generic (5xx / network) backoff: wait = BASE * (attempt+1) + jitter
LLM_GENERIC_BACKOFF_BASE = 3.0


# --- Provider-wide concurrency caps (semaphores in RoundRobinClientPool) ---
# Hard ceiling on simultaneous in-flight requests per provider, regardless of
# how many phases run in parallel. Phase A runs Step1 ∥ Step4 (both hit LLM1),
# so total concurrency would otherwise be 2 × LLM1_MAX_WORKERS = 200.
LLM1_PROVIDER_CONCURRENCY = 200
LLM2_PROVIDER_CONCURRENCY = 200
JUDGE_PROVIDER_CONCURRENCY = 300


# --- Logging ---
def setup_logging(log_dir: Optional[Path] = None) -> None:
    """Configure logging: console + file output."""
    if log_dir is None:
        log_dir = LOGS_DIR
    log_dir.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_dir / "pipeline.log", encoding="utf-8"),
        ],
    )


# --- API Key Validation ---
def validate_api_keys(require_judge: bool = True) -> None:
    """Check that essential API keys are set. Exit if missing."""
    missing = []
    if not LLM1_API_KEY:
        missing.append("QWEN_API_KEY (LLM1 extractor)")
    # Verification LLM2: Doubao > GLM-5 > Qwen
    if not LLM2_API_KEY and not GLM_API_KEY and not QWEN_API_KEY:
        missing.append("DOUBAO_API_KEY or GLM_API_KEY or QWEN_API_KEY (LLM2 verifier)")
    if require_judge and not JUDGE_API_KEY:
        missing.append("DEEPSEEK_API_KEY (judge)")

    _log = logging.getLogger(__name__)
    if missing:
        _log.error(f"✗ 缺少 API 密钥: {', '.join(missing)}")
        _log.error("  请在 .env 文件中配置（参考 .env.example）")
        raise RuntimeError(f"缺少 API 密钥: {', '.join(missing)}")

    _log.info(f"  提取模型 LLM1: {LLM1_MODEL} via DashScope, {len(LLM1_API_KEYS)} keys")
    # Report which verification model is active
    if LLM2_API_KEY:
        _log.info(f"  验证模型 LLM2: {LLM2_MODEL} via Volcengine, {len(LLM2_API_KEYS)} keys")
    elif GLM_API_KEY:
        _log.info(f"  验证模型 LLM2: GLM-5 ({GLM_MODEL}) via SiliconFlow, {len(GLM_API_KEYS)} keys")
    else:
        _log.info(f"  验证模型 LLM2: Qwen ({QWEN_MODEL}) via DashScope, {len(QWEN_API_KEYS)} keys")
    _log.info(f"  裁判模型: {JUDGE_MODEL} via DeepSeek, {len(JUDGE_API_KEYS)} keys")
