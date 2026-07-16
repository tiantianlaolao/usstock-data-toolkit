#!/usr/bin/env python3
"""核实 7 只疑似退市 ticker 的真实状态.

对每只:
1. 调 yf.Ticker(t).info 拿 quoteType / currentPrice / marketState / regularMarketVolume
2. 重新拉最近 10 天 history(period=10d) 对比 parquet 里的尾部
3. 判定: 真退市(from info), 还是 parquet 过时, 还是 yfinance 数据源单日问题
"""
import os
import yfinance as yf
import polars as pl
from datetime import datetime, timedelta
from pathlib import Path

DATA_DIR = Path(os.environ.get('US_DATA_DIR', str(Path.home() / 'us_data')))
PARQUET = str(DATA_DIR / 'us_full_market_5y.parquet')
TICKERS = ['SEE', 'HOLX', 'TGNA', 'DVAX', 'HI', 'MPW', 'FYBR']

print("Loading parquet...")
df = pl.scan_parquet(PARQUET).collect()

for t in TICKERS:
    print(f"\n{'='*70}")
    print(f"{t}")
    print('='*70)

    # parquet 尾部
    sub = df.filter(pl.col('ticker') == t).sort('date').tail(3)
    print(f"[parquet 尾部]")
    for r in sub.iter_rows(named=True):
        print(f"  {r['date']}  O={r['open']:.2f} C={r['close']:.2f} V={int(r['volume'])}")

    # yfinance info
    try:
        tk = yf.Ticker(t)
        info = tk.info
        print(f"[yf.info]")
        print(f"  quoteType      = {info.get('quoteType')}")
        print(f"  shortName      = {info.get('shortName')}")
        print(f"  marketState    = {info.get('marketState')}")
        print(f"  currentPrice   = {info.get('currentPrice')}")
        print(f"  regularMarketPrice = {info.get('regularMarketPrice')}")
        print(f"  regularMarketVolume = {info.get('regularMarketVolume')}")
        print(f"  exchange       = {info.get('exchange')}")
    except Exception as e:
        print(f"[yf.info] ERROR: {e}")

    # 最近 15 天历史
    try:
        hist = tk.history(period='15d', auto_adjust=False)
        print(f"[yf.history 最近 15 天]")
        if hist.empty:
            print(f"  (空 — 可能已退市)")
        else:
            for idx, row in hist.tail(5).iterrows():
                d = idx.strftime('%Y-%m-%d')
                print(f"  {d}  O={row['Open']:.2f} C={row['Close']:.2f} V={int(row['Volume'])}")
    except Exception as e:
        print(f"[yf.history] ERROR: {e}")

print("\n核实完成")
