#!/usr/bin/env python3
"""修复所有 volume=0 的行.

规则:
1. 尾部 volume=0 (连续从末端倒数) → 直接删除. 这类是"未收盘"或采集时机问题.
2. 中间 volume=0 → 用前一日 close 填充 O/H/L/C, volume 保持 0 作为停牌标记.

处理对所有 ticker 统一进行, 不维护白名单.
"""
import os
import polars as pl
import shutil
from datetime import datetime
from pathlib import Path

DATA_DIR = Path(os.environ.get('US_DATA_DIR', str(Path.home() / 'us_data')))
PARQUET = str(DATA_DIR / 'us_full_market_5y.parquet')

# 1. 备份
ts = datetime.now().strftime('%Y%m%d_%H%M%S')
backup = f"{PARQUET}.bak_halts_{ts}"
print(f"[1/5] 备份 → {backup}")
shutil.copy2(PARQUET, backup)

# 2. 读
print(f"\n[2/5] 读取")
df = pl.read_parquet(PARQUET).sort(['ticker', 'date'])
before_rows = df.height
before_zeros = (df['volume'] == 0).sum()
print(f"      rows={before_rows:,}  volume=0 rows={before_zeros}")

# 3. 标记每个 ticker 的"尾部 0 段" — 用 reverse cumsum 技巧
# 思路: 在每个 ticker 内, 从末端往前, 找到第一个 volume>0 的位置, 之后的(向末端)全是尾部 0
print(f"\n[3/5] 找尾部 volume=0 段 (这些直接删除)")
df = df.with_columns(
    pl.col('volume').cum_max(reverse=True).over('ticker').alias('_rev_cummax')
)
# _rev_cummax == 0 意味着从该点到末端, volume 从未 > 0 → 尾部 0 段
tail_mask = pl.col('_rev_cummax') == 0
tail_zero_df = df.filter(tail_mask)
print(f"      尾部 volume=0 rows: {tail_zero_df.height}")
if tail_zero_df.height > 0:
    stats = tail_zero_df.group_by('ticker').agg(pl.len().alias('n')).sort('n', descending=True)
    print(stats.head(20))

# 删尾部 0
df = df.filter(~tail_mask).drop('_rev_cummax')
after_tail_rows = df.height
print(f"      删除后 rows: {after_tail_rows:,}")

# 4. 对剩余 volume=0 行前向填充 OHLC
print(f"\n[4/5] 中间 volume=0 行前向填充 OHLC")
mid_zero_count = (df['volume'] == 0).sum()
print(f"      中间 volume=0 rows: {mid_zero_count}")

if mid_zero_count > 0:
    # 先按 ticker+date 排序, 然后在 ticker 内用 shift+when/then 替换
    # 用: 先创建 prev_close, 然后 when volume==0 → open/high/low/close = prev_close
    df = df.sort(['ticker', 'date']).with_columns(
        pl.col('close').shift(1).over('ticker').alias('_prev_close')
    )
    # 只有 volume=0 且 _prev_close 非空时才替换 (防止第一行异常)
    mask = (pl.col('volume') == 0) & pl.col('_prev_close').is_not_null()
    df = df.with_columns([
        pl.when(mask).then(pl.col('_prev_close')).otherwise(pl.col('open')).alias('open'),
        pl.when(mask).then(pl.col('_prev_close')).otherwise(pl.col('high')).alias('high'),
        pl.when(mask).then(pl.col('_prev_close')).otherwise(pl.col('low')).alias('low'),
        pl.when(mask).then(pl.col('_prev_close')).otherwise(pl.col('close')).alias('close'),
    ]).drop('_prev_close')

    # volume 保持 0 作为停牌标记
    still_zero = (df['volume'] == 0).sum()
    print(f"      填充后 volume=0 rows (应与之前相同, 只改 OHLC): {still_zero}")

# 5. 写
print(f"\n[5/5] 写回 parquet")
df = df.sort(['ticker', 'date'])
df.write_parquet(PARQUET, compression='snappy')

# 6. 校验
df_v = pl.read_parquet(PARQUET)
after_rows = df_v.height
after_zeros = (df_v['volume'] == 0).sum()
tickers_n = df_v['ticker'].n_unique()
print(f"\n校验: rows={after_rows:,}  tickers={tickers_n}  volume=0 rows={after_zeros}")
print(f"删除尾部 0 行: {before_rows - after_rows}")

# 抽查几只 E 类 ticker 的 OHLC 填充效果
print(f"\n抽查 E 类修复效果 (ACAD 2022-06-21 前后, HTZ 2021-11-10 前后)")
for t, cutoff in [('ACAD', '2022-06-21'), ('HTZ', '2021-11-10')]:
    from datetime import date
    cd = date.fromisoformat(cutoff)
    sub = df_v.filter((pl.col('ticker') == t) & (pl.col('date').is_between(cd.replace(day=max(1, cd.day-3)), cd))).sort('date')
    if sub.height > 0:
        print(f'\n  {t}:')
        for r in sub.iter_rows(named=True):
            print(f"    {r['date']}  O={r['open']:.2f} H={r['high']:.2f} L={r['low']:.2f} C={r['close']:.2f} V={int(r['volume'])}")

print(f"\n完成. 备份: {backup}")
