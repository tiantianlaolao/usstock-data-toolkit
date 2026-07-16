#!/usr/bin/env python3
"""全量摸底 — 分类识别污染 ticker.

分类:
A = 纯净: 从 parquet 开头就是正常交易
B = 后段污染: 最近还有 volume=0 / 价格冻结 (疑似停牌 or 退市, 但这 5016 是当前活跃池所以应该极少)
C = 前段污染-新股: 前段有 volume=0, 之后恢复正常 (IPO 前回填)
D = 前段污染-Ticker复用: 前段有长期价格冻结 + volume=0 (旧实体死数据)
E = 混合: 前后都有问题

判定逻辑:
- "冻结段" = 连续 ≥3 天 volume==0
- "价格冻结段" = 连续 ≥5 天 close 完全不变
- 找每个 ticker 的 first_valid_date = 最后一次"从冻结段跳到正常段"之后的那天
- 若 first_valid_date == 第一天 → A
- 若 first_valid_date 在后半段 (占比 > 50%) → D (旧实体占一半以上)
- 否则 → C
"""
import os
import polars as pl
from datetime import date
from pathlib import Path

DATA_DIR = Path(os.environ.get('US_DATA_DIR', str(Path.home() / 'us_data')))
PARQUET = str(DATA_DIR / 'us_full_market_5y.parquet')

print(f"Loading {PARQUET} ...")
df = pl.scan_parquet(PARQUET).collect()
print(f"rows={df.height:,} tickers={df['ticker'].n_unique()}")
print(f"date range {df['date'].min()} → {df['date'].max()}")

df = df.sort(['ticker', 'date'])

# 逐 ticker 扫描
print("\n扫描中...")
rows = []
global_start = df['date'].min()

tickers = df['ticker'].unique().sort().to_list()
for i, t in enumerate(tickers):
    if i % 500 == 0:
        print(f"  {i}/{len(tickers)}")
    sub = df.filter(pl.col('ticker') == t)
    if sub.height == 0:
        continue

    n = sub.height
    vols = sub['volume'].to_list()
    closes = sub['close'].to_list()
    dates = sub['date'].to_list()

    # 找 first_valid_index: 最后一段"健康段"的起点
    # 健康段定义: volume > 0 且 该天 close != 前一天 close (至少有价格变动) — 但初始点只看 volume>0
    # 为简化: 从尾巴倒着走, 找到最早的连续健康段起点
    # "不健康": volume == 0 OR (i>0 and close == prev_close and volume == 0)

    bad = [v == 0 for v in vols]
    # first_valid = 第一个 bad[i]=False 的位置, 但要求之后也基本都是好的
    # 更稳: 找最后一次 bad->good 的转折
    first_valid = 0
    for idx in range(n - 1, -1, -1):
        if bad[idx]:
            first_valid = idx + 1
            break
    # first_valid 现在是最后一个 bad 之后的位置; 若整条无 bad, first_valid=0

    # 前段 bad 段的统计
    pre_bad_count = sum(bad[:first_valid]) if first_valid > 0 else 0
    post_bad_count = sum(bad[first_valid:])  # 应=0
    total_bad = sum(bad)

    # 价格冻结检测: 在前段内, 有没有连续 ≥5 天 close 完全不变
    frozen_run = 0
    max_frozen = 0
    for idx in range(1, min(first_valid + 1, n)):
        if closes[idx] == closes[idx - 1]:
            frozen_run += 1
            if frozen_run > max_frozen:
                max_frozen = frozen_run
        else:
            frozen_run = 0

    first_valid_date = dates[first_valid] if first_valid < n else None
    valid_rows = n - first_valid

    # 分类
    # 含义: last_bad_idx = first_valid - 1, 即 bad 在序列中分布的最末位置
    # 如果 last_bad_idx < n-1, 说明 bad 之后还有正常交易; 否则 bad 发生在最末
    if total_bad == 0:
        category = 'A_clean'
    elif first_valid >= n:
        # 最后一天是 bad — 可能是单日停牌 or 最近退市
        if total_bad == 1:
            category = 'B_last_day_zero'
        else:
            category = 'B_tail_bad'
    elif pre_bad_count == 1 and total_bad == 1:
        # 只有一天 volume=0, 位于前段 — 单日停牌
        category = 'E_single_day_halt'
    elif max_frozen >= 20:
        category = 'D_ticker_reuse'  # 长期价格冻结 = 死数据
    elif pre_bad_count >= 5:
        category = 'C_pre_listing'  # 多天 bad 集中在前段 = IPO 回填
    else:
        category = 'E_scattered_halt'

    rows.append({
        'ticker': t,
        'total_rows': n,
        'total_bad': total_bad,
        'pre_bad': pre_bad_count,
        'max_frozen_run': max_frozen,
        'first_valid_date': first_valid_date,
        'valid_rows': valid_rows,
        'category': category,
    })

result = pl.DataFrame(rows)
print(f"\n结果: {result.height} 只 ticker")

print("\n[1] 分类统计")
stats = result.group_by('category').agg(pl.len().alias('n')).sort('category')
print(stats)

print("\n[2] 各类代表 (前 10)")
for cat in ['D_ticker_reuse', 'C_pre_listing', 'E_mixed', 'F_all_bad']:
    sub = result.filter(pl.col('category') == cat).sort('pre_bad', descending=True).head(10)
    if sub.height == 0:
        continue
    print(f"\n--- {cat} ---")
    print(sub)

print("\n[3] D 类 (Ticker 复用) 全列表 — 需要人工核对")
d_list = result.filter(pl.col('category') == 'D_ticker_reuse').sort('pre_bad', descending=True)
print(f"共 {d_list.height} 只")
if d_list.height > 0:
    print(d_list.head(30))
    # 输出到文件
    out_d = str(DATA_DIR / 'contamination_D_ticker_reuse.csv')
    d_list.write_csv(out_d)
    print(f"→ 已写 {out_d}")

print("\n[4] C 类 (新股前回填) 全列表")
c_list = result.filter(pl.col('category') == 'C_pre_listing').sort('pre_bad', descending=True)
print(f"共 {c_list.height} 只")
if c_list.height > 0:
    print(c_list.head(30))
    out_c = str(DATA_DIR / 'contamination_C_pre_listing.csv')
    c_list.write_csv(out_c)
    print(f"→ 已写 {out_c}")

print("\n[5] 完整结果")
out_full = str(DATA_DIR / 'contamination_full.csv')
result.write_csv(out_full)
print(f"→ 已写 {out_full}")

print("\n[6] SW/INDV/PECO 核对")
for t in ['SW', 'INDV', 'PECO']:
    r = result.filter(pl.col('ticker') == t)
    if r.height > 0:
        print(r)

print("\n完成")
