# 省级主官履历数据库 — 项目配置（v7）

## 项目简介

本项目是一个 agentic pipeline，用于构建中国省级领导人（省长 + 省委书记）的结构化履历数据库。

- **数据来源**：百度百科
- **主要 LLM**：DeepSeek-V3（三步提取：事实+标签+行政级别）
- **验证 LLM**：Doubao-seed-2-0-pro（独立三步提取 + diff 核查，via Volcengine）
- **裁判 LLM**：Kimi K2.5（争议字段裁判+信心评分0-100，信心<90标红+记录理由，后备 DeepSeek R1 思考模型）
- **裁判上下文**：裁判接收完整 step1/step2/step3 提取规则作为参考（含行政级别规则），行政级别争议亦进入裁判流程
- **并发处理**：ThreadPoolExecutor + 多 API key 轮询池（RoundRobinClientPool），默认 100 workers + SmoothRateLimiter（Kimi RPM=500, TPM=3M）
- **并行架构**：DS 提取 + Doubao 提取同时进行，完成后统一 diff（source_line 分组），再进入裁判阶段
- **文本预处理**：百度百科原文 → 结构化编号行（text_preprocessor.py），压缩 Step1 上下文；落马判断交由 LLM（Step2），不在预处理阶段误判中纪委工作人员
- **输出格式**：Excel（36列 A→AK，每行为一段履历）
- **爬虫模块**：`code_scrape/`（bio_scraper_v2.py + wiki/starmap 辅助爬虫）
- **GitHub**：https://github.com/HaipingXu/Workflow-.git（公开仓库）

## 快速开始

```bash
# 1. 配置API密钥
cp .env.example .env
# 编辑 .env，填入三个密钥：DEEPSEEK_API_KEY / DOUBAO_API_KEY / KIMI_API_KEY

# 2. 安装依赖（含 Playwright）
pip install -r requirements.txt
playwright install chromium        # 首次安装必须

# 3. 运行省级示例
python main_province.py --province 浙江
python main_province.py --province 浙江 --start 2000

# 4. 单官员测试
python main_province.py --province 浙江 --official 习近平

# 5. 批量所有省份
python main_province.py --batch

# 6. 常用跳过选项
python main_province.py --province 浙江 --skip-scrape   # 重用现有文本
python main_province.py --province 浙江 --skip-extract  # 重用现有LLM结果
python main_province.py --province 浙江 --skip-battle   # 跳过Battle表生成
python main_province.py --province 浙江 --force         # 强制全量重跑

# 7. 并发控制
python main_province.py --province 浙江 --workers 10    # 并发worker数（默认100）
python main_province.py --province 浙江 --workers 1     # 串行模式（调试用）
```

## 项目结构

```
.
├── CLAUDE.md                           # 本文件（项目配置）
├── prompts/
│   ├── step1_extraction.md             # ★ Step1提取prompt（编号行→episodes，含命名标准化规则）
│   ├── step2_labeling.md              # ★ Step2打标prompt（raw_bio+标签+落马）
│   ├── step3_rank.md                  # ★ Step3级别prompt（批量行政级别判断）
│   ├── ref_university_rank.md         # 高校/党校级别参考（按需注入）
│   └── ref_soe_rank.md               # 国企级别参考（按需注入）
├── .claude/
│   ├── skills/
│   │   └── city-official-db.md         # 管道使用说明
│   └── agents/
│       └── official-bio-agent.md       # 全流程编排agent
│
├── text_preprocessor.py                # Phase 0.5：百科文本→结构化编号行（落马由LLM判断）
├── utils.py                            # 共享工具函数（extract_json, llm_chat, load_prompt, SmoothRateLimiter）
├── config.py                           # API密钥、路径、列名、并发常量（Kimi RPM/TPM限速）
├── input_parser_province.py            # Phase 0：解析省级官员名单txt
├── input_parser.py                     # Phase 0（城市版，备用）
├── api_processor_v2.py                 # Phase 2：DeepSeek三步提取（编号行模式）
├── verifier_v2.py                      # Phase 3a：Doubao独立提取+source_line分组diff
├── battle_generator.py                 # Phase 3b：Battle表+Kimi K2.5裁判（含完整提取规则上下文）
├── postprocess_v2.py                   # Phase 4：后处理+扁平化
├── export_v2.py                        # Phase 5：三表Excel导出
├── main_province.py                    # ★ 主编排器（省级）
│
├── code_scrape/                        # 爬虫模块包
│   ├── __init__.py
│   ├── bio_scraper_v2.py               # Phase 1：百度百科两层爬取策略
│   ├── starmap_scraper.py              # 星图数据爬取
│   ├── wiki_secretary_v3.py            # 维基百科省委书记列表爬取
│   └── wiki_secretary_scraper.py       # 维基百科辅助爬虫
│
├── archive/                            # 归档旧版本
├── requirements.txt
├── .env.example
├── data/
│   └── {省份}_officials.txt            # ★ 人工维护的省级官员名单（单一信息源）
├── officials/                          # 爬取的百科文本（.gitignored）
├── output/                             # Excel输出（.gitignored）
├── logs/                               # JSON中间结果、核查报告（.gitignored）
└── docs/                               # 技术文档
```

## 核心设计原则

### 单一信息源（Single Source of Truth）

| 数据 | 权威来源 |
|------|---------|
| 提取规则（Step1）| `prompts/step1_extraction.md` |
| 打标规则（Step2）| `prompts/step2_labeling.md` |
| 级别规则（Step3）| `prompts/step3_rank.md` |
| 官员名单 | `data/{city}_officials.txt`（人工维护） |
| 列定义 | `config.py → COLUMNS` |

修改对应 `.md` 文件即可更新 DeepSeek/Doubao 行为，无需改动代码。

### 文本预处理 + 三步提取架构

**Phase 0.5（text_preprocessor.py）**：将百度百科原文解析为结构化组件：
- `bio_summary`：人物简介 + 基本信息
- `career_lines[]`：编号履历行（`L01: 1978.10-1982.07 description`）
- `corruption_text`：落马相关段落
- 预处理结果保存到 `logs/preprocessed_texts.json`

**三步提取**：
1. **Step 1（事实提取）**：仅接收编号行 → 输出 `episodes[]`，每条含 `source_line` 整数引用
2. **Step 2（分析打标）**：接收 episodes + bio_summary + corruption_text → 输出 `raw_bio + 标签 + 落马`
3. **Step 3（行政级别）**：接收所有 episodes 的职务+供职单位 → 批量输出每条的行政级别+推导逻辑【v5.3新增】

### 三LLM架构 + source_line 确定性匹配 + 分组裁判 + 信心评分

```
百度百科原文 → text_preprocessor → 编号行（L01-LNN）→ 保存 logs/preprocessed_texts.json
                                        ↓
DeepSeek（Step1+Step2+Step3）  ──┐  每条 episode 标注 source_line + 行政级别
                                 ├── 并发 50 workers
Doubao（Step1+Step2+Step3，独立）──┘  同样标注 source_line + 行政级别
       ↓
正则标准化（党委"中共"前缀、政府简称、人大简称、政协简称）
       ↓
diff（按 source_line 分组对齐，确定性匹配，含行政级别对比）
       ↓
Kimi K2.5 裁判（两级裁判 + 信心评分 0-100，100并发 + RPM500平滑）
  ├── 字段级：DS≠VF 的字段逐个裁判（同 source_line 数量相等时）
  └── 分组级：DS 和 VF 拆分数量不同时，整组裁判决定采用哪套
  → 输出 采纳DS | 采纳VF | 整行采纳DS/VF + confidence 0-100 + 判断理由
       ↓
postprocess：分组裁判 "采纳VF" → 替换 DS episodes，信心<90标红+理由写入争议列
       ↓
battle.xlsx（DS灰/VF蓝/裁判紫/Final绿，冲突单元格标红，信心<90标红）
```

### 分组裁判（v5 新增）

当 DS 和 VF 对同一原文行（source_line）提取出不同数量的 episodes 时：
- **不再逐条配对裁判**（会导致 "市长、党组书记" + "党组书记" 重复）
- 裁判一次看到双方的全部 episodes + 原文，决定采用哪套拆分方式
- 后处理阶段用裁判指定的一方替换另一方的 episodes
- 典型场景：DS 将 "市长、党组书记" 拆为两条，VF 合并为一条 → 裁判采纳 VF

### 两层爬取策略（百度百科）

| 层 | 方式 | 适用场景 | 成功率 |
|----|------|---------|--------|
| Layer 1 | curl_cffi Chrome TLS伪装 | 多数页面 | ~70% |
| Layer 2 | Playwright+stealth（无头Chrome）| JS渲染/Layer1文本不足 | ~92% |

## 输出列说明（A–AK，共36列）

**人级别**（每人一行固定值）：A=年份 | B=省份 | C=城市 | D=姓名 | E=出生年份 | F=籍贯(省) | G=籍贯(市) | H=少数民族(1/0) | I=女性(1/0) | J=全日制本科(1/0) | K=升迁_省长 | L=升迁_省委书记 | M=本省提拔 | N=本省学习 | O=是否当过省长 | P=是否当过省委书记 | Q=最终行政级别

**行级别**（每段经历一行）：R=经历序号 | S=起始时间(YYYY.MM) | T=终止时间 | U=组织标签(33选1) | V=标志位(23选1) | W=该条行政级别 | X=供职单位 | Y=职务 | Z=原文引用(Lxx) | AA=争议未解决 | AB=裁判理由 | AC=任职地(省) | AD=任职地(市) | AE=中央/地方 | AF=空列 | AG=是否落马 | AH=落马原因 | AI=备注栏 | AJ=该条是省长(1) | AK=该条是省委书记(1)

> 完整枚举见 `prompts/step1_extraction.md`；列顺序见 `config.py → COLUMNS`

## Prompts（LLM 指令，代码外置）

| Prompt | 文件 | 用途 |
|--------|------|------|
| Step1 提取 | `prompts/step1_extraction.md` | 编号行 → episodes（含 source_line） |
| Step2 打标 | `prompts/step2_labeling.md` | raw_bio + 标签 + 落马信息 |
| Step3 级别 | `prompts/step3_rank.md` | 批量行政级别判断（10级+推导逻辑）【v5.3新增】|
| 高校参考 | `prompts/ref_university_rank.md` | 高校/党校级别+央地属性判定参考（按需注入 Step1/Step3）|
| 国企参考 | `prompts/ref_soe_rank.md` | 国企级别+央地属性判定参考（按需注入 Step1/Step3）|

## 技能 & Agent

- **city-official-db** → `.claude/skills/city-official-db.md`（管道使用文档）
- **official-bio-agent** → `.claude/agents/official-bio-agent.md`（全流程编排）

## 官员名单格式（data/{省份}_officials.txt）

```
省份：浙江
起始年份：2000

[省长]
张三, 2000.01-2003.06
李四（代）, 2003.07-2004.01

[省委书记]
王五, 1999.06-2002.10
```
无需修改任何 `~/.claude/` 全局配置。
