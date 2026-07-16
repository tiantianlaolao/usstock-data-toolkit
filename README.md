# usstock-data-toolkit — 美股全市场数据本地化工具箱

一套经过**生产环境验证**的美股日线数据管线：S&P 1500 成分股（大中小盘约 1500 只）5 年 OHLCV 落 Parquet，每日全自动增量更新，外加一组数据质量诊断 / 污染清洗工具。

- 数据源为 **Nasdaq 公开 API + iShares 官网**，无需注册、无需 API key
- lock 防重入、备份 + 原子替换、行数护栏防写空——每一处护栏都是被真实事故逼出来的（见[踩坑实录](#踩坑实录)）
- 一条 cron 挂上后每天收盘后自动增量，单日更新约 11 分钟
- **本仓库只提供程序，不提供任何数据文件**——数据自己跑出来，归你自己

姊妹仓：[astock-data-toolkit](https://github.com/tiantianlaolao/astock-data-toolkit)（A 股版，行情 / 估值 / 财务 / 公告全家桶）

## ⚠️ 先读这个：美股数据源对"你的服务器在哪"极其敏感

这是美股数据管线和 A 股最大的不同。同一份代码，换个 IP 就是两种命运——**动手前先对号入座**：

| 你的机器 | yfinance (Yahoo) | Nasdaq API（本管线） |
|---|---|---|
| 美国云服务器（机房 IP） | ❌ **常态 429 限流**，不是偶发 | ✅ 直连稳定，实测 ~6 req/s 无限流 |
| 中国大陆 | ❌ Yahoo 对大陆 IP 直接 403 | ❌ 直连不通/不稳，需海外服务器或自备代理 |
| 家庭宽带（美国/海外住宅 IP） | ⚠️ 低频可用，批量拉取仍会触发限流 | ✅ 通常可用 |

我们的亲身经历：这套管线的前身是 yfinance 版，在美国云服务器上跑了一段时间后，2026 年 4 月的某天 cron 全面崩溃——**每一只票都 429**，三轮退避全部失败，按退避后的速度拉完 1500 只要 175 个小时。Yahoo 对数据中心 IP 段的限流是常态化的，且随时收紧，**不适合做无人值守管线的地基**。当天全量切换到 Nasdaq 公开 API（切换前做了逐列 schema 对比、20 只 × 30 日收盘价抽样 p99 diff = 0、端到端双跑验证），此后每日增量稳定至今。

结论：**跑这套管线的最佳姿势是一台美国云服务器**（最低配的轻量机就够，见[硬件参考](#硬件与耗时参考)）。

## 数据覆盖

| 项 | 内容 |
|---|---|
| 股池 | S&P 500 + S&P 400（中盘）+ S&P 600（小盘），约 1500 只，从 iShares 三只核心 ETF（IVV/IJH/IJR）的官方 holdings CSV 实时抓取 |
| 数据 | 日线 OHLCV（Nasdaq 官方口径，split-adjusted） |
| 深度 | 5 年滚动窗口 |
| 落地 | 单文件 long-format Parquet（snappy 压缩），约 195 万行 / 38 MB |
| 列 | `date` `ticker` `open` `high` `low` `close` `volume` |

## 这份数据能拿来做什么

一句话：**凡是"日线级、全市场横截面"的活，它都是现成地基。**

- **策略回测**——5 年 × 1500 只的 long-format Parquet，`polars`/`pandas` 一行读入，接 backtrader / vectorbt 或自己手写循环都顺。S&P 1500 覆盖大中小盘，比只测大盘股的回测更接近真实市场
- **每日选股扫描**——收盘后全池算指标（均线距离、量比、动量、突破形态……），几十 MB 数据全内存秒算。我们自己就在生产环境这么用：cron 更完数据，下游一个每日扫描器直接读这份 parquet 算 30 日均线和放量信号
- **横截面研究**——行业轮动、动量分布、涨跌家数、市场宽度这类"每天看全市场一眼"的统计，本地数据随便切，不用一次次打 API
- **数据分析练手 / 教学**——真实、体量友好（笔记本电脑无压力）、schema 干净的金融数据集不好找；配套的污染扫描工具本身就是一课"免费数据源的坑长什么样"
- **自建行情面板**——parquet 后面随便接个 Streamlit / Grafana，就是自己的离线看盘页

不适合做什么也说清楚：日线数据做不了盘中/高频，5 年窗口做不了跨牛熊长周期研究，1500 只成分股外的小票不在池内。

## 快速开始

环境：Python 3.9+，Linux / Windows 均可。

```bash
pip install -r requirements.txt

# 首跑：parquet 不存在时自动对每只票回填 5 年历史（约 20~30 分钟）
python update_us_data.py

# 常用参数
python update_us_data.py --dry-run            # 只拉取汇总，不写 parquet
python update_us_data.py --limit 20           # 只处理前 20 只（测试）
python update_us_data.py --force              # 清掉残留 lock 强制跑
```

数据默认写到 `~/us_data/`，可用环境变量 `US_DATA_DIR` 指定其它目录。

### 每日自动更新

```
# crontab —— 美东收盘后跑（示例为北京时间上午，= 美东前一日晚间收盘后）
0 11 * * * /path/to/venv/bin/python /path/to/update_us_data.py >> ~/us_data/cron.log 2>&1
```

每天正常输出长这样（可作健康基线）：`update` 约等于股池规模、`error=0`、`empty` 只剩几只常年拉不到的 class share。若 `error` 突增或 `update` 大幅低于股池规模，立刻查 `errors.json`。

### 环境变量

| 变量 | 默认 | 说明 |
|---|---|---|
| `US_DATA_DIR` | `~/us_data` | 数据主目录（parquet / 日志 / 状态文件都在这） |
| `US_MIN_ROWS` | `500000` | 合并后行数低于此值拒绝写入（防把好数据写坏） |

另可在 `$US_DATA_DIR/delisted_blacklist.json` 放一个 ticker 数组，与内置退市黑名单合并，无需改代码。

## 目录结构

```
usstock-data-toolkit/
├── update_us_data.py                  # 核心：股池抓取 + 全量/增量下载 + 原子合并写入
└── tools/                             # 数据质量工具箱
    ├── check_parquet_integrity.py     # 10 项完整性诊断（null/零值/年度行数矩阵/日期断层…）
    ├── scan_contamination.py          # 全池污染扫描：自动分类 A纯净/B尾部/C新股回填/D ticker复用/E混合
    ├── clean_halts.py                 # 通用清洗：尾部 volume=0 删除 + 中间停牌日前向填充
    ├── clean_delisted.py              # 清洗实录：按名单整只剔除退市 ticker（改表头名单即用）
    ├── clean_pre_listing.py           # 清洗实录：按 first_valid_date 截断上市前污染段（改表头即用）
    └── verify_delisted.py             # 退市核实：拿 yfinance 低频交叉验证疑似退市名单（可选装 yfinance）
```

`clean_delisted.py` / `clean_pre_listing.py` 是我们真实清洗操作的**实录脚本**——名单和截断日期写在文件头部常量里，是当时用 `scan_contamination.py` 扫出来、逐只人工核对后定的。用法：先跑扫描 → 人工核对输出的 CSV → 把你自己的名单填进表头 → 再执行。所有清洗脚本动手前都会先做带时间戳的整库备份。

## 工程护栏（无人值守跑数据的底线）

1. **lock 文件防重入**——cron 与手动跑不会互相踩；`--force` 可清残留锁
2. **写入三段式**——先 copy 出 `.bak`，新数据写 `.tmp`，最后 `os.replace` 原子换名。任何一步崩溃，主文件都完好
3. **行数护栏**——合并后行数 < `US_MIN_ROWS` 直接拒绝写入。数据源哪天抽风返回空，最多丢一天增量，不会把 5 年历史写没
4. **按 ticker 增量**——每只票记录 last_date，只拉缺的日期；重复行按 `(ticker, date)` 去重保留最新
5. **状态三件套**——每次运行落 `progress.json`（统计）/ `errors.json`（逐只错误）/ `last_update.txt`（OK/FAIL），监控脚本只看文件就够

## 踩坑实录

同类工具的文档不会告诉你这些，但每一条都会在你挂上 cron 的第二周找上门：

1. **yfinance 在云服务器上是常态 429，不是你的代码有 bug**。Yahoo 对机房 IP 段限流常态化且随时收紧，退避重试救不了批量场景（见开头章节）。本管线因此整体建在 Nasdaq 公开 API 上。
2. **Nasdaq historical 接口带 `timeframe` 参数时，窄日期范围直接返回 null**。全量拉 5 年时加 `&timeframe=y5` 一切正常；增量只拉 1 天时同一参数让 `tradesTable` 变 null，表现为"全池 empty"。去掉该参数、只用 `fromdate/todate` 即可。这个坑在首次增量运行才暴露，全量测试根本发现不了。
3. **Nasdaq 返回的数字全是字符串，且格式随场景变**：价格带 `$` 和千分位逗号（`"$1,234.56"`），成交量在盘中甚至带小数（`"459,023.674112"`）——`int()` 直接炸，必须 `int(float())`。缺失值是字符串 `"N/A"` 不是 null。
4. **Nasdaq 的成交量口径是 consolidated（全市场合并），比 Yahoo 的 primary exchange 口径系统性大约 10%**。两边混用会让所有基于量的指标漂移；从 Yahoo 迁移过来时阈值类参数要重新校准。
5. **Class share 的符号在每个环节都不一样**：iShares CSV 里是 `BRKB`，Nasdaq API 要 `BRK.B`，本管线统一存 `BRK-B`。不做三方映射就会出现同一只票三个名字各存一份。
6. **iShares 股池不会替你剔除退市股**，holdings CSV 里的僵尸 ticker 会让你每天白白重试。本管线内置退市黑名单 + JSON 外挂名单双层过滤。
7. **免费源的历史数据自带三类污染**（`scan_contamination.py` 就是为此而写）：新股上市前的回填假数据、ticker 被新公司复用后旧实体的死数据、停牌日的 volume=0 假行情。直接喂给回测会出鬼故事，先扫再洗。

## 硬件与耗时参考

- **机器**：最低配的美国轻量云服务器即可（实际生产环境 2C2G，数据全家 < 200 MB）
- **首跑全量**：约 20~30 分钟（单线程 ~6 req/s，请不要提速——会被限流）
- **每日增量**：约 11 分钟
- **磁盘**：parquet 约 38 MB，加备份预留 1 GB 绰绰有余

## 免责声明

- 本项目仅供**学习与研究**使用，请勿用于任何商业数据转售场景
- 所有数据版权归各数据源（Nasdaq、iShares/BlackRock 等）所有，使用时请遵守各数据源的服务条款，控制访问频率
- 本项目仅提供数据获取程序，不提供任何投资建议；数据仅反映历史，不预示未来

## License

[MIT](LICENSE)
