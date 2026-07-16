#!/usr/bin/env python3
"""Parquet 完整性诊断 — 每 ticker 每年数据完整度 + NaN/零值检测.

检查项:
1. 总体: 行数 / ticker 数 / 日期范围 / 每列 null 比例
2. 每年交易日数 (正常 ~252, 异常会明显)
3. 每 ticker 覆盖的年数 + 总行数分布
4. Top-N 缺数据最严重的 ticker (最可能出问题)
5. OHLCV 里有 null 的 ticker 列表
6. OHLCV 零值 / 负值检测 (close/open 不应为 0 或负)
7. 每年每 ticker 的行数矩阵 (找到哪个 ticker 哪一年缺口最大)
8. 相邻日期 gap > 10 天的 ticker (整月断层)
"""
import os
import polars as pl
from datetime import date
from pathlib import Path

DATA_DIR = Path(os.environ.get('US_DATA_DIR', str(Path.home() / 'us_data')))
PARQUET = str(DATA_DIR / 'us_full_market_5y.parquet')

print("=" * 70)
print(f"Parquet: {PARQUET}")
print("=" * 70)

lf = pl.scan_parquet(PARQUET)
df = lf.collect()

print(f"\n[1] 总体")
print(f"    rows    = {df.height:,}")
print(f"    tickers = {df['ticker'].n_unique()}")
print(f"    cols    = {df.columns}")
print(f"    date range = {df['date'].min()} → {df['date'].max()}")

print(f"\n[2] 每列 null 数 / 零值数 / 负值数")
for col in ['open', 'high', 'low', 'close', 'volume']:
    if col not in df.columns:
        continue
    s = df[col]
    null_n = s.null_count()
    zero_n = (s == 0).sum()
    neg_n = (s < 0).sum() if col != 'volume' else 0
    print(f"    {col:<7} null={null_n:>6} zero={zero_n:>6} negative={neg_n:>6}")

print(f"\n[3] 每年交易日数 (全市场合并, 看日期完整性)")
per_year_days = (
    df.select([pl.col('date'), pl.col('date').dt.year().alias('year')])
      .group_by('year')
      .agg(pl.col('date').n_unique().alias('trade_days'))
      .sort('year')
)
print(per_year_days)

print(f"\n[4] 每 ticker 的总行数分布 (分位数)")
per_ticker = (
    df.group_by('ticker')
      .agg([
          pl.len().alias('rows'),
          pl.col('date').min().alias('first'),
          pl.col('date').max().alias('last'),
      ])
)
print(f"    min  = {per_ticker['rows'].min()}")
print(f"    p10  = {per_ticker['rows'].quantile(0.1):.0f}")
print(f"    p50  = {per_ticker['rows'].quantile(0.5):.0f}")
print(f"    p90  = {per_ticker['rows'].quantile(0.9):.0f}")
print(f"    max  = {per_ticker['rows'].max()}")
print(f"    mean = {per_ticker['rows'].mean():.0f}")

print(f"\n[5] 行数最少的 20 个 ticker (可能是新上市 or 数据缺失)")
few = per_ticker.sort('rows').head(20)
print(few)

print(f"\n[6] OHLCV 有 null 的 ticker (若有)")
null_tickers = (
    df.filter(
        pl.col('open').is_null() |
        pl.col('high').is_null() |
        pl.col('low').is_null() |
        pl.col('close').is_null() |
        pl.col('volume').is_null()
    )
    .group_by('ticker').agg(pl.len().alias('null_rows'))
    .sort('null_rows', descending=True)
)
if null_tickers.height == 0:
    print("    ✅ 全部 ticker 无 null 值")
else:
    print(f"    ⚠️ {null_tickers.height} 只 ticker 含 null")
    print(null_tickers.head(20))

print(f"\n[7] close 为 0 或负值的 ticker (价格异常)")
bad_close = (
    df.filter((pl.col('close') <= 0))
      .group_by('ticker').agg(pl.len().alias('bad_rows'))
      .sort('bad_rows', descending=True)
)
if bad_close.height == 0:
    print("    ✅ 无 close <= 0")
else:
    print(f"    ⚠️ {bad_close.height} 只 ticker 含非法 close")
    print(bad_close.head(20))

print(f"\n[8] 每年 × ticker 行数矩阵 — 统计每年有多少 ticker 行数 < 200")
year_ticker = (
    df.with_columns(pl.col('date').dt.year().alias('year'))
      .group_by(['year', 'ticker'])
      .agg(pl.len().alias('rows'))
)
for y in sorted(year_ticker['year'].unique().to_list()):
    sub = year_ticker.filter(pl.col('year') == y)
    total_tickers = sub.height
    lt_200 = sub.filter(pl.col('rows') < 200).height
    lt_100 = sub.filter(pl.col('rows') < 100).height
    lt_20 = sub.filter(pl.col('rows') < 20).height
    median_rows = sub['rows'].median()
    print(f"    {y}: tickers={total_tickers:>5}  rows_median={median_rows:>4.0f}  <200: {lt_200:>4}  <100: {lt_100:>4}  <20: {lt_20:>4}")

print(f"\n[9] 每 ticker 连续日期 gap 检测 (找 > 10 个交易日的断层)")
gapped = (
    df.sort(['ticker', 'date'])
      .with_columns(
          (pl.col('date') - pl.col('date').shift(1)).over('ticker').alias('gap_days')
      )
      .filter(pl.col('gap_days').dt.total_days() > 14)
      .group_by('ticker')
      .agg([
          pl.len().alias('gap_events'),
          pl.col('gap_days').dt.total_days().max().alias('max_gap_days')
      ])
      .sort('max_gap_days', descending=True)
)
if gapped.height == 0:
    print("    ✅ 无 > 14 天的断层")
else:
    print(f"    ⚠️ {gapped.height} 只 ticker 含 gap")
    print(gapped.head(20))

print(f"\n[10] 样本抽查 AAPL / MSFT / NVDA 每年行数")
for sym in ['AAPL', 'MSFT', 'NVDA', 'SPY', 'TSLA']:
    sub = df.filter(pl.col('ticker') == sym)
    if sub.height == 0:
        print(f"    {sym}: 不存在")
        continue
    per_yr = (
        sub.with_columns(pl.col('date').dt.year().alias('y'))
           .group_by('y').agg(pl.len().alias('rows')).sort('y')
    )
    yr_strs = [f"{r['y']}:{r['rows']}" for r in per_yr.iter_rows(named=True)]
    print(f"    {sym}: {' '.join(yr_strs)}  (总 {sub.height})")

print("\n" + "=" * 70)
print("诊断完成")
print("=" * 70)
