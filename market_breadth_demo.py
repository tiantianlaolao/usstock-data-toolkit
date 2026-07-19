#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
盘中市场宽度统计 demo —— 「历史基线 + 实时快照」组合方法的示例

⚠ 定位声明 (设计如此, 不是没做完):
- 本脚本只输出**市场级统计** (涨跌家数 / 均线上方比例 / 量比分布),
  不输出任何个股名单——它回答"今天市场什么脸色", 不回答"该买哪只票"
- 演示的是方法: 历史日线提供基线 (昨收 / 30 日均线 / 5 日均量),
  实时快照提供当下状态, 两者对比才能把"绝对值"变成"相对异常"
- 不构成任何投资建议; 数据仅反映当下, 不预示未来

前置: 先用 update_us_data.py 生成本地日线 parquet (基线全部来自它)。

用法:
  python market_breadth_demo.py --limit 50        # 先小样本试跑
  python market_breadth_demo.py                   # 全池 ~1500 只 (默认 4 并发约 30 分钟)
  python market_breadth_demo.py --csv breadth.csv # 每次运行追加一行统计, 攒盘中序列
  python market_breadth_demo.py --workers 8       # 并发上限 8 (约 15 分钟, 请勿改代码放宽)

关于量比读数 (重要): 分子是"盘中累计成交量", 开盘 30 分钟和收盘前的
读数天然差一个量级——量比只能与**同一时点**的历史读数纵向比较,
不存在一个全天通用的"放量阈值"。
"""
import os
import sys
import io
import csv
import time
import datetime
import argparse
from pathlib import Path

import pandas as pd

from quote_snapshot import fetch_snapshots, estimate_minutes, MAX_WORKERS

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace', line_buffering=True)
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace', line_buffering=True)

BASE    = Path(os.environ.get('US_DATA_DIR', str(Path.home() / 'us_data')))
PARQUET = BASE / 'us_full_market_5y.parquet'

MA_WINDOW    = 30   # 均线窗口 (交易日)
AVG_VOL_DAYS = 5    # 均量窗口 (交易日)


def load_baselines():
    """从日线 parquet 算每只票的基线: 昨收 / 30 日均线 / 5 日均量。

    行数不足 MA_WINDOW 的票 (新股) 直接剔除, 保证统计口径一致。
    """
    if not PARQUET.exists():
        print(f'✗ 找不到 {PARQUET}, 先跑 update_us_data.py 生成日线数据')
        sys.exit(1)
    df = pd.read_parquet(PARQUET, columns=['date', 'ticker', 'close', 'volume'])
    df = df.sort_values(['ticker', 'date'])
    tail = df.groupby('ticker').tail(MA_WINDOW)
    g = tail.groupby('ticker')

    base = pd.DataFrame({
        'prev_close': g['close'].last(),
        'ma': g['close'].mean(),
        'avg_vol': g['volume'].apply(lambda s: s.tail(AVG_VOL_DAYS).mean()),
        'n_days': g.size(),
    })
    before = len(base)
    base = base[base['n_days'] >= MA_WINDOW]
    dropped = before - len(base)
    print(f'基线就绪: {len(base)} 只 (剔除历史不足 {MA_WINDOW} 日的 {dropped} 只), '
          f'数据截至 {tail["date"].max()}')
    return base


def _quantiles(values, qs=(0.25, 0.5, 0.75)):
    s = pd.Series(values).dropna()
    if s.empty:
        return [float('nan')] * len(qs)
    return [s.quantile(q) for q in qs]


def main():
    parser = argparse.ArgumentParser(description='盘中市场宽度统计 (只出市场级数字, 不出个股名单)')
    parser.add_argument('--limit', type=int, default=0, help='只处理前 N 只 (试跑用; 注意样本有字母序偏差)')
    parser.add_argument('--csv', help='把本次统计追加一行到 CSV (攒盘中时间序列)')
    parser.add_argument('--workers', type=int, default=4,
                        help=f'并发线程数, 默认 4, 上限 {MAX_WORKERS} (请勿改代码放宽)')
    args = parser.parse_args()

    base = load_baselines()
    tickers = list(base.index)
    if args.limit > 0:
        tickers = tickers[:args.limit]
        print(f'⚠ --limit {args.limit}: 样本是字母序前 {len(tickers)} 只, 统计有偏, 只作试跑')

    n = len(tickers)
    print(f'开始轮询 {n} 只 × {args.workers} 并发, 预计约 {estimate_minutes(n, args.workers):.1f} 分钟 '
          f'(限速与并发上限是刻意的, 请勿放宽)')

    t0 = time.time()
    quotes = fetch_snapshots(tickers, workers=args.workers)

    up_pc = down_pc = flat_pc = 0          # 相对昨收
    up_op = down_op = 0                    # 相对今开
    above_ma = below_ma = 0
    chg_list, vr_list = [], []
    fail = 0

    for t in tickers:
        q = quotes.get(t)
        row = base.loc[t]
        if q is None:
            fail += 1
            continue
        last = q['last']
        prev = q['prev_close'] if q['prev_close'] else row['prev_close']
        if prev and prev > 0:
            chg = (last - prev) / prev * 100
            chg_list.append(chg)
            if chg > 0.05:
                up_pc += 1
            elif chg < -0.05:
                down_pc += 1
            else:
                flat_pc += 1
        if q['open'] and q['open'] > 0:
            if last > q['open']:
                up_op += 1
            elif last < q['open']:
                down_op += 1
        if row['ma'] > 0:
            if last >= row['ma']:
                above_ma += 1
            else:
                below_ma += 1
        if q['volume'] is not None and row['avg_vol'] > 0:
            vr_list.append(q['volume'] / row['avg_vol'])

    ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    ok = n - fail
    if ok == 0:
        print('✗ 全部快照失败: 大概率是网络/IP 问题 (Nasdaq 接口对大陆 IP 不通, 见 README)')
        return 1

    chg_p25, chg_p50, chg_p75 = _quantiles(chg_list)
    vr_p25, vr_p50, vr_p75 = _quantiles(vr_list)
    vr_ge1 = sum(1 for v in vr_list if v >= 1.0)

    print(f'\n=== 市场宽度快照 @ {ts} (本机时间) ===')
    print(f'样本: 请求 {n} 只 / 快照成功 {ok} / 失败 {fail}   用时 {(time.time() - t0) / 60:.1f} 分钟')
    print(f'相对昨收:   上涨 {up_pc} ({up_pc / ok * 100:.1f}%)  下跌 {down_pc}  平盘(±0.05%) {flat_pc}')
    print(f'涨跌幅分布: p25 {chg_p25:+.2f}%  中位 {chg_p50:+.2f}%  p75 {chg_p75:+.2f}%')
    print(f'相对今开:   上涨 {up_op}  下跌 {down_op}')
    print(f'{MA_WINDOW}日均线:  上方 {above_ma} ({above_ma / max(above_ma + below_ma, 1) * 100:.1f}%)  下方 {below_ma}')
    print(f'量比(盘中累计/{AVG_VOL_DAYS}日均量): p25 {vr_p25:.2f}  中位 {vr_p50:.2f}  p75 {vr_p75:.2f}   ≥1.0 有 {vr_ge1} 只')
    print(f'  ⚠ 量比随开盘时长增长, 只能与同一时点的历史读数纵向比较')

    if args.csv:
        path = Path(args.csv)
        new_file = not path.exists()
        with open(path, 'a', newline='', encoding='utf-8') as f:
            w = csv.writer(f)
            if new_file:
                w.writerow(['snapshot_ts', 'requested', 'ok', 'fail',
                            'up_vs_prev', 'down_vs_prev', 'flat_vs_prev',
                            'chg_p25', 'chg_p50', 'chg_p75',
                            'up_vs_open', 'down_vs_open',
                            'above_ma', 'below_ma',
                            'vr_p25', 'vr_p50', 'vr_p75', 'vr_ge1'])
            w.writerow([ts, n, ok, fail, up_pc, down_pc, flat_pc,
                        round(chg_p25, 4), round(chg_p50, 4), round(chg_p75, 4),
                        up_op, down_op, above_ma, below_ma,
                        round(vr_p25, 4), round(vr_p50, 4), round(vr_p75, 4), vr_ge1])
        print(f'已追加统计行 → {path}')

    return 0


if __name__ == '__main__':
    sys.exit(main())
