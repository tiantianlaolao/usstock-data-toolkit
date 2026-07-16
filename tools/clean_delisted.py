#!/usr/bin/env python3
"""清理 5 只已退市 ticker — MPW / FYBR / DVAX / HI / TGNA.

流程:
1. 备份 parquet → us_full_market_5y.parquet.bak_before_delisted_$(timestamp)
2. 读 parquet, 过滤掉这 5 只
3. 写新 parquet
4. 打印 before/after 对比
"""
import os
import polars as pl
import shutil
from datetime import datetime
from pathlib import Path

DATA_DIR = Path(os.environ.get('US_DATA_DIR', str(Path.home() / 'us_data')))
PARQUET = str(DATA_DIR / 'us_full_market_5y.parquet')
DELISTED = ['MPW', 'FYBR', 'DVAX', 'HI', 'TGNA']

# 1. 备份
ts = datetime.now().strftime('%Y%m%d_%H%M%S')
backup = f"{PARQUET}.bak_delisted_{ts}"
print(f"[1/4] 备份 parquet → {backup}")
shutil.copy2(PARQUET, backup)
print(f"      OK")

# 2. 读
print(f"\n[2/4] 读取 parquet")
df = pl.read_parquet(PARQUET)
before_rows = df.height
before_tickers = df['ticker'].n_unique()
print(f"      before: rows={before_rows:,} tickers={before_tickers}")

# 3. 过滤
print(f"\n[3/4] 过滤掉 {DELISTED}")
removed = df.filter(pl.col('ticker').is_in(DELISTED))
for t in DELISTED:
    n = removed.filter(pl.col('ticker') == t).height
    print(f"      {t}: {n} 行")
print(f"      共移除: {removed.height} 行")

df_clean = df.filter(~pl.col('ticker').is_in(DELISTED))
after_rows = df_clean.height
after_tickers = df_clean['ticker'].n_unique()
print(f"      after:  rows={after_rows:,} tickers={after_tickers}")

assert before_rows - after_rows == removed.height, "行数对不上!"
assert before_tickers - after_tickers == 5, "ticker 数对不上!"

# 4. 写
print(f"\n[4/4] 写回 parquet (snappy)")
df_clean.write_parquet(PARQUET, compression='snappy')

# 校验
df_verify = pl.read_parquet(PARQUET)
print(f"      verify: rows={df_verify.height:,} tickers={df_verify['ticker'].n_unique()}")
assert df_verify.height == after_rows
assert df_verify['ticker'].n_unique() == after_tickers

# 确认 5 只都被清掉
still_there = df_verify.filter(pl.col('ticker').is_in(DELISTED))
if still_there.height == 0:
    print(f"      ✅ 5 只退市 ticker 已全部清除")
else:
    print(f"      ❌ 仍有残留: {still_there['ticker'].unique().to_list()}")

print(f"\n完成. 备份路径: {backup}")
