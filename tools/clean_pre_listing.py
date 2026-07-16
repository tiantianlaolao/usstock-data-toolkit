#!/usr/bin/env python3
"""清洗 SW/INDV/PECO 前段污染 — 按 first_valid_date 截断.

SW:   保留 >= 2024-07-08 (删除 876 行)
INDV: 保留 >= 2023-06-02 (删除 602 行)
PECO: 保留 >= 2021-07-15 (删除 97  行)
"""
import os
import polars as pl
import shutil
from datetime import date, datetime
from pathlib import Path

DATA_DIR = Path(os.environ.get('US_DATA_DIR', str(Path.home() / 'us_data')))
PARQUET = str(DATA_DIR / 'us_full_market_5y.parquet')

CUT = {
    'SW':   date(2024, 7, 8),
    'INDV': date(2023, 6, 2),
    'PECO': date(2021, 7, 15),
}

# 1. 备份
ts = datetime.now().strftime('%Y%m%d_%H%M%S')
backup = f"{PARQUET}.bak_preclean_{ts}"
print(f"[1/4] 备份 parquet → {backup}")
shutil.copy2(PARQUET, backup)
print(f"      OK")

# 2. 读
print(f"\n[2/4] 读取 parquet")
df = pl.read_parquet(PARQUET)
before_rows = df.height
print(f"      before: rows={before_rows:,} tickers={df['ticker'].n_unique()}")

# 3. 截断每只
print(f"\n[3/4] 截断污染 ticker")
removed_total = 0
kept_total = 0

# 先把 3 只从主表分出来
mask = pl.col('ticker').is_in(list(CUT.keys()))
others = df.filter(~mask)

cleaned_parts = [others]
for t, cutoff in CUT.items():
    sub = df.filter(pl.col('ticker') == t)
    before_n = sub.height
    bad_rows = sub.filter(pl.col('date') < cutoff).height
    kept = sub.filter(pl.col('date') >= cutoff)
    kept_n = kept.height
    print(f"      {t}: {before_n} → {kept_n} (删 {bad_rows}, 保留 >= {cutoff})")
    removed_total += bad_rows
    kept_total += kept_n
    cleaned_parts.append(kept)

df_clean = pl.concat(cleaned_parts).sort(['ticker', 'date'])
after_rows = df_clean.height
print(f"      after:  rows={after_rows:,} tickers={df_clean['ticker'].n_unique()}")
assert before_rows - after_rows == removed_total

# 4. 写
print(f"\n[4/4] 写回 parquet")
df_clean.write_parquet(PARQUET, compression='snappy')

# 5. 校验
print(f"\n[校验] 3 只清洗后 volume=0 情况")
df_v = pl.read_parquet(PARQUET)
for t in CUT.keys():
    sub = df_v.filter(pl.col('ticker') == t)
    zeros = (sub['volume'] == 0).sum()
    first = sub['date'].min()
    last = sub['date'].max()
    print(f"      {t}: rows={sub.height}  first={first}  last={last}  zeros={zeros}")

print(f"\n完成. 备份: {backup}")
