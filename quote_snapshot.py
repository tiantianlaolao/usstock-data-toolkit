#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
美股实时报价快照工具 (Nasdaq 官网公开接口)

能力与定位 (动手前先读清楚):
- 这是「轮询快照」, 不是行情推送流: 每查一只票发 2 个 HTTP 请求,
  拿到 现价 / 今日开盘价 / 昨收 / 盘中累计成交量 的一个瞬时切片
- 精度定位是「分钟级监控」, 做不了 tick / 秒级 / 高频
- 接口是 nasdaq.com 官网页面自用的公开接口, **不是官方开放 API 产品**:
  无文档、无使用协议、无稳定性承诺, 字段和鉴权随时可能变化。
  需要正式授权的数据服务请使用 Nasdaq Data Link
- 内置限速有硬下限 (QUOTE_SLEEP_FLOOR), 请不要绕过——高频滥用只会
  让 Nasdaq 收紧接口, 所有人一起失去它 (见 README「限速铁律」)
- 仅供学习研究, 不构成投资建议, 请勿用于数据转售

用法:
  python quote_snapshot.py AAPL MSFT BRK-B          # 指定 ticker
  python quote_snapshot.py --file mylist.txt        # 从文件读, 每行一只
  python quote_snapshot.py --pool --limit 20        # 从本地 parquet 股池取前 20 只
  python quote_snapshot.py AAPL --csv snap.csv      # 追加写 CSV (含时间戳, 可攒盘中序列)

与日线管线的关系: ticker 规范与 update_us_data.py 一致 (parquet 形式
'BRK-B', 自动转 API 形式 'BRK.B'); --pool 直接读同一份 parquet 的股池。
"""
import os
import sys
import io
import csv
import time
import datetime
import argparse
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests


def _utf8_stdio():
    """Windows 控制台默认 GBK, 作为脚本入口运行时切 UTF-8 (被 import 时不动 stdout)."""
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace', line_buffering=True)
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace', line_buffering=True)

# ================ 配置 ================
BASE    = Path(os.environ.get('US_DATA_DIR', str(Path.home() / 'us_data')))
PARQUET = BASE / 'us_full_market_5y.parquet'

HTTP_TIMEOUT = 10

# 限速: 每只票查完后 sleep。可用 US_QUOTE_SLEEP 调大 (更礼貌),
# 但存在硬下限——这不是可选项, 是对免费公开接口的基本尊重。
QUOTE_SLEEP_FLOOR = 0.15
QUOTE_SLEEP = max(QUOTE_SLEEP_FLOOR, float(os.environ.get('US_QUOTE_SLEEP', '0.25')))

# 并发上限: 接口单请求延迟 ~2-3s, 纯单线程扫全池要 2 小时, 因此允许小并发。
# 上限 8 与我们生产环境长期验证的水平一致, 请勿调大 (改这个数前先读 README「限速铁律」)。
MAX_WORKERS = 8
AVG_TICKER_SECONDS = 5.5   # 实测: 2 个请求各 ~2-3s + sleep, 用于耗时预估

# Nasdaq 接口必须带完整浏览器请求头 (缺 origin/referer 会被拒)
HEADERS = {
    'authority': 'api.nasdaq.com',
    'accept': 'application/json, text/plain, */*',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'origin': 'https://www.nasdaq.com',
    'referer': 'https://www.nasdaq.com/',
    'accept-language': 'en-US,en;q=0.9',
}

# 复用连接 (Session): 省掉每次请求的 TLS 握手, 单票耗时从 ~5s 降到 <1s。
# 注意这只是减少握手开销, 不是提高请求频率——限速 sleep 照旧生效。
_session = requests.Session()
_session.headers.update(HEADERS)


def _nasdaq_symbol(ticker):
    """parquet 的 'BRK-B' 在 Nasdaq API 里是 'BRK.B'."""
    return ticker.replace('-', '.') if '-' in ticker else ticker


def _clean_num(x):
    """Nasdaq 数字全是字符串且带 $ 和千分位逗号, 缺失是 'N/A'."""
    s = str(x).replace('$', '').replace(',', '').strip()
    if not s or s == 'N/A':
        return None
    try:
        return float(s)
    except ValueError:
        return None


def get_quote(ticker):
    """抓一只票的实时快照。

    返回 dict: {'last', 'open', 'prev_close', 'volume'} (open/prev_close 可能为 None,
    盘前或数据缺失时常见), 失败返回 None。
    """
    sym = _nasdaq_symbol(ticker)
    try:
        info_url = f'https://api.nasdaq.com/api/quote/{sym}/info?assetclass=stocks'
        r_info = _session.get(info_url, timeout=HTTP_TIMEOUT).json()
        sum_url = f'https://api.nasdaq.com/api/quote/{sym}/summary?assetclass=stocks'
        r_sum = _session.get(sum_url, timeout=HTTP_TIMEOUT).json()

        primary = (r_info.get('data') or {}).get('primaryData') or {}
        summary = (r_sum.get('data') or {}).get('summaryData') or {}
        if not primary or not summary:
            return None

        last = _clean_num(primary.get('lastSalePrice'))
        open_p = _clean_num((summary.get('OpenPrice') or {}).get('value'))
        prev_close = _clean_num((summary.get('PreviousClose') or {}).get('value'))

        # 盘中累计成交量: 盘中是实时计算值, 会带小数 ('459,023.674112'),
        # 必须 int(float()) 而不是 int()
        vol = _clean_num((summary.get('ShareVolume') or {}).get('value'))
        volume = int(vol) if vol is not None else None

        if last is None or last <= 0:
            return None
        return {'last': last, 'open': open_p, 'prev_close': prev_close, 'volume': volume}
    except Exception:
        return None


def fetch_snapshots(tickers, workers=4, progress_every=100):
    """并发抓一批快照, 返回 {ticker: quote_dict_or_None}。

    workers 会被钳制在 [1, MAX_WORKERS]; 每个线程内部仍执行 QUOTE_SLEEP 限速。
    """
    workers = max(1, min(int(workers), MAX_WORKERS))

    def _one(t):
        q = get_quote(t)
        time.sleep(QUOTE_SLEEP)
        return t, q

    out = {}
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [ex.submit(_one, t) for t in tickers]
        for i, fut in enumerate(as_completed(futures)):
            t, q = fut.result()
            out[t] = q
            if progress_every and (i + 1) % progress_every == 0:
                print(f'  ... {i + 1}/{len(tickers)}')
    return out


def estimate_minutes(n, workers):
    workers = max(1, min(int(workers), MAX_WORKERS))
    return n * AVG_TICKER_SECONDS / workers / 60


def _pct(a, b):
    """(a-b)/b 的百分数, 任一缺失返回 None."""
    if a is None or b is None or b == 0:
        return None
    return (a - b) / b * 100


def load_pool_tickers():
    """从本地日线 parquet 读股池 (与 update_us_data.py 同源)."""
    if not PARQUET.exists():
        print(f'✗ 找不到 {PARQUET}, 先跑 update_us_data.py 或改用位置参数指定 ticker')
        sys.exit(1)
    import pandas as pd
    tickers = sorted(pd.read_parquet(PARQUET, columns=['ticker'])['ticker'].unique())
    return tickers


def main():
    parser = argparse.ArgumentParser(description='美股实时报价快照 (Nasdaq 官网公开接口, 轮询非推送)')
    parser.add_argument('tickers', nargs='*', help='ticker 列表 (parquet 形式, 如 BRK-B)')
    parser.add_argument('--file', help='ticker 清单文件, 每行一只')
    parser.add_argument('--pool', action='store_true', help='用本地 parquet 的全部股池')
    parser.add_argument('--limit', type=int, default=0, help='只处理前 N 只')
    parser.add_argument('--csv', help='追加写入 CSV (带时间戳, 可攒盘中时间序列)')
    parser.add_argument('--workers', type=int, default=4,
                        help=f'并发线程数, 默认 4, 上限 {MAX_WORKERS} (请勿改代码放宽)')
    args = parser.parse_args()

    tickers = [t.upper() for t in args.tickers]
    if args.file:
        with open(args.file, encoding='utf-8') as f:
            tickers += [line.strip().upper() for line in f if line.strip() and not line.startswith('#')]
    if args.pool:
        tickers += load_pool_tickers()
    tickers = list(dict.fromkeys(tickers))  # 去重保序
    if args.limit > 0:
        tickers = tickers[:args.limit]
    if not tickers:
        parser.error('没有任何 ticker: 用位置参数 / --file / --pool 至少给一个')

    n = len(tickers)
    if n > 50:
        print(f'⚠ {n} 只票 × {args.workers} 并发, 预计约 {estimate_minutes(n, args.workers):.1f} 分钟 '
              f'(限速与并发上限是刻意的, 请勿放宽)')

    ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    quotes = fetch_snapshots(tickers, workers=args.workers)

    print(f'{"ticker":<8} {"现价":>10} {"今开":>10} {"昨收":>10} {"较昨收%":>8} {"较今开%":>8} {"盘中累计量":>14}')
    rows, fail = [], 0
    for t in tickers:
        q = quotes.get(t)
        if q is None:
            fail += 1
            print(f'{t:<8} {"-":>10} (快照失败)')
            continue
        chg_pc = _pct(q['last'], q['prev_close'])
        chg_op = _pct(q['last'], q['open'])
        print(f'{t:<8} {q["last"]:>10.2f} '
              f'{q["open"] if q["open"] is not None else float("nan"):>10.2f} '
              f'{q["prev_close"] if q["prev_close"] is not None else float("nan"):>10.2f} '
              f'{chg_pc if chg_pc is not None else float("nan"):>8.2f} '
              f'{chg_op if chg_op is not None else float("nan"):>8.2f} '
              f'{q["volume"] if q["volume"] is not None else 0:>14,}')
        rows.append({'snapshot_ts': ts, 'ticker': t, 'last': q['last'],
                     'open': q['open'], 'prev_close': q['prev_close'],
                     'pct_vs_prev_close': round(chg_pc, 4) if chg_pc is not None else None,
                     'pct_vs_open': round(chg_op, 4) if chg_op is not None else None,
                     'volume': q['volume']})

    print(f'\n完成: 成功 {len(rows)} / 失败 {fail} / 共 {n}  @ {ts}')

    if args.csv and rows:
        path = Path(args.csv)
        new_file = not path.exists()
        with open(path, 'a', newline='', encoding='utf-8') as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            if new_file:
                w.writeheader()
            w.writerows(rows)
        print(f'已追加 {len(rows)} 行 → {path}')

    return 0 if rows else 1


if __name__ == '__main__':
    _utf8_stdio()
    sys.exit(main())
