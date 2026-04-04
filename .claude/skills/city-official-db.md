---
name: city-official-db
description: Orchestrates the full v3 pipeline to build a structured career history database for Chinese city-level officials (mayor + party secretary). Covers list parsing, biography scraping, 3-LLM extraction/verification/judging, and Excel export with concurrent processing.
triggers:
  - "run official database pipeline"
  - "build official database for"
  - "run city official pipeline"
  - "官员数据库"
  - "履历数据库"
version: "3.0"
---

# 城市官员履历数据库构建技能

## 功能描述

为指定城市构建自2010年（或指定年份）以来的市级主官（市长 + 市委书记）完整履历数据库。

## 使用方式

### 完整运行

```bash
python main_v2.py --city 深圳 --province 广东 --start 2010
```

参数说明：
- `--city`：城市名（中文，不含"市"），如：深圳、北京、上海
- `--province`：所属省份（如：广东、北京、上海），直辖市填城市名
- `--start`：起始年份，默认2010
- `--official`：指定单个官员测试，如 `--official 许勤`
- `--skip-scrape`：跳过爬取步骤（使用已有txt文件）
- `--skip-extract`：跳过DeepSeek提取步骤
- `--skip-verify`：跳过Qwen验证步骤
- `--skip-battle`：跳过Battle表+裁判
- `--check-wiki`：与维基百科名单交叉核查
- `--force`：强制全量重跑（忽略缓存）
- `--workers N`：并发worker数（默认3，爬取阶段最多2以防反爬）

### 分步运行

```bash
# Phase 0：名单已通过 data/{city}_officials.txt 人工维护

# Phase 1：爬取百度百科
python bio_scraper_v2.py --city 深圳 --list logs/officials_list_v2.json

# Phase 2：DeepSeek两步提取
python api_processor_v2.py --city 深圳 --province 广东

# Phase 3：Qwen核查 + Battle表（通过 main_v2.py 调用）
python main_v2.py --city 深圳 --province 广东 --skip-scrape --skip-extract

# 导出（Phase 4+5 通过 main_v2.py 调用）
python main_v2.py --city 深圳 --province 广东 --skip-scrape --skip-extract --skip-verify --skip-battle
```

## 输出文件

| 文件 | 说明 |
|------|------|
| `officials/{姓名}_biography.txt` | 原始百度百科文本 |
| `logs/deepseek_step1_results.json` | DeepSeek Step1 履历提取 |
| `logs/deepseek_step2_labels.json` | DeepSeek Step2 标签打标 |
| `logs/qwen_step1_results.json` | Qwen Step1 独立提取 |
| `logs/qwen_step2_labels.json` | Qwen Step2 独立打标 |
| `logs/diff_report.json` | DS vs QW 差异报告 |
| `logs/judge_decisions.json` | Kimi K2.5 裁判缓存 |
| `logs/final_rows.json` | 后处理扁平化行 |
| `output/{city}_officials.xlsx` | 所有官员全部履历（29列） |
| `output/{city}_mayors.xlsx` | 仅市长任职行 |
| `output/{city}_secretaries.xlsx` | 仅书记任职行 |
| `output/{city}_battle.xlsx` | DS/QW/裁判对比表 |

## 列说明（A–AC，共29列）

| 列 | 含义 | 类型 |
|----|------|------|
| A 年份 | 该官员在目标城市担任主官的起始年份 | 整数 |
| B 省份 | 城市所属省份 | 字符串 |
| C 城市 | 目标城市 | 字符串 |
| D 姓名 | 官员姓名 | 字符串 |
| E 出生年份 | 生年 | 整数 |
| F 籍贯 | 省份格式 | 字符串 |
| G 少数民族 | 1=少数民族，0=汉族 | 0/1 |
| H 女性 | 1=女，0=男 | 0/1 |
| I 全日制本科 | 1=有全日制本科学历 | 0/1 |
| J 升迁 | 离任后是否升迁至更高职务 | 0/1/-1 |
| K 本省提拔 | 是否由本省系统内部提拔 | 0/1/-1 |
| L 本省学习 | 任前是否在本省有脱产学习经历 | 0/1/-1 |
| M 是否当过市长 | 该人是否曾任目标城市市长 | 0/1 |
| N 是否当过书记 | 该人是否曾任目标城市市委书记 | 0/1 |
| O 经历序号 | 该人第几段经历 | 整数 |
| P 起始时间 | YYYY.MM | 字符串 |
| Q 终止时间 | YYYY.MM 或 至今 | 字符串 |
| R 组织标签 | 30选1 | 字符串 |
| S 供职单位 | 具体单位名称 | 字符串 |
| T 职务 | 具体职务 | 字符串 |
| U 任职地（省） | 完整省份名 | 字符串 |
| V 任职地（市） | 完整城市名 | 字符串 |
| W 中央/地方 | 中央/地方 | 字符串 |
| X | 空列 | |
| Y 是否落马 | 是/否 | 字符串 |
| Z 落马原因 | 判词 | 字符串 |
| AA 备注栏 | 含[需人工核查]标记 | 字符串 |
| AB 该条是市长 | 该行为目标城市市长任职行 | 0/1 |
| AC 该条是书记 | 该行为目标城市市委书记任职行 | 0/1 |

## 移植到新城市

1. 创建 `data/{city}_officials.txt`（参考 `data/深圳_officials.txt`）
2. 配置 `.env` 文件（3个 API 密钥：DEEPSEEK / QWEN / KIMI）
3. 运行：`python main_v2.py --city 广州 --province 广东 --start 2010`
4. 无需修改任何代码

## 注意事项

- 百度百科爬取含2–5秒随机延迟，爬取阶段最多2个并发worker
- API调用并发默认3个worker，可通过 `--workers N` 调整
- 串行模式 `--workers 1` 可用于调试
- API调用费用：DeepSeek约$0.001/千tokens，Qwen约¥0.04/千tokens
- 若官员百度百科词条不存在，会跳过并记录到日志
- 所有中间结果增量保存，线程安全（threading.Lock 保护）
