#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
美股日线数据更新器 v3 (Nasdaq 数据源)

架构:
- 数据源: Nasdaq /api/quote/{sym}/historical (美服直连, 无 429 限流)
- 存储:   us_full_market_5y.parquet (long format, 唯一源头, 既读又写)
- 增量:   groupby(ticker).max() 拿 last_dates → 只补下载新行
- 安全:   lock 文件 + .bak 备份 + .tmp 原子换 + 行数下限保护
- 调度:   cron 每日 08:00 CST (= 美东 20:00 前日, 市场收盘后)
- 节假日: Nasdaq 无新数据 → 静默走 empty 分支

与 v2 (yfinance) 的差别:
- 下载栈从 yfinance 换成 urllib + Nasdaq JSON API
- 移除 RATE_LIMIT_BACKOFF 60/120/240 (yfinance 特有的长退避)
- 保留 429 短退避 [10,20,30] (防御性, Nasdaq 实测不会触发)
- 移除 IPO 截断 (Nasdaq 本身不回传 IPO 前的行)
- 移除 yfinance 尾部 volume=0 清理 (Nasdaq 没这毛病)
- 保留中间 halt 前向填充 (防御层)
- CLASS_SHARE_MAP 用于 iShares → parquet 规范化 (保留)
- 新增 _nasdaq_symbol: parquet 'BRK-B' → API 'BRK.B'
"""
import os, sys, io, re, time, datetime, traceback, json, shutil, argparse
import urllib.request, urllib.error
from pathlib import Path
from io import StringIO

# 合法美股 ticker 正则
TICKER_RE = re.compile(r'^[A-Z][A-Z0-9]{0,4}(-[A-Z0-9]{1,3})?$')

# iShares holdings CSV 把 Class A/B 票写成无分隔符连写 (BRKB/BFB/MOGA/CWENA),
# parquet/universe 统一用 dash 形式 (BRK-B/BF-B/MOG-A/CWEN-A).
CLASS_SHARE_MAP = {
    'BRKA':  'BRK-A',  'BRKB':  'BRK-B',
    'BFA':   'BF-A',   'BFB':   'BF-B',
    'MOGA':  'MOG-A',  'MOGB':  'MOG-B',
    'CWENA': 'CWEN-A',
}

import requests
import pandas as pd

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace', line_buffering=True)
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace', line_buffering=True)

# ================ 配置 ================
BASE          = Path(os.environ.get('US_DATA_DIR', str(Path.home() / 'us_data')))
PARQUET       = BASE / 'us_full_market_5y.parquet'
PARQUET_TMP   = BASE / 'us_full_market_5y.parquet.tmp'
PARQUET_BAK   = BASE / 'us_full_market_5y.parquet.bak'
STATUS_FILE   = BASE / 'last_update.txt'
ERRORS_FILE   = BASE / 'errors.json'
PROGRESS_FILE = BASE / 'progress.json'
LOCK_FILE     = BASE / 'update.lock'

MIN_ROWS_FOR_SWAP   = int(os.environ.get('US_MIN_ROWS', '500000'))
MAX_ERROR_LOG_LINES = 20
HTTP_TIMEOUT        = 20
NASDAQ_SLEEP        = 0.15     # 每次请求后 sleep (~6 req/s, 实测无 429)
NASDAQ_RETRIES      = 3
NASDAQ_RETRY_WAIT   = [10, 20, 30]

BASE.mkdir(parents=True, exist_ok=True)

NASDAQ_HDR = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) Gecko/20100101 Firefox/120.0',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.9',
}


# ========== 退市 / 被收购 黑名单 ==========
DELISTED_BLACKLIST: set = {
    'MPW', 'FYBR', 'DVAX', 'HI', 'TGNA',   # 2026-04-13 清理
    'AHH', 'CARS', 'SXC', 'PLAY', 'MYGN', 'KREF',
    'INN', 'ELME', 'TWI', 'BLMN', 'ATGE', 'ANGI',
    'SEE', 'AL', 'HOLX', 'AXL', 'MODG', 'ALEX',
    'THS', 'DAY', 'CIVI', 'CADE', 'CMA', 'PCH',
    'GES', 'MMC', 'KAR',   # 2026-04-14 C1 清理
}
BLACKLIST_FILE = BASE / 'delisted_blacklist.json'


def _load_blacklist():
    """合并外部 JSON 黑名单 (可热更新)."""
    global DELISTED_BLACKLIST
    if BLACKLIST_FILE.exists():
        try:
            extra = json.loads(BLACKLIST_FILE.read_text(encoding='utf-8'))
            if isinstance(extra, list):
                DELISTED_BLACKLIST = set(DELISTED_BLACKLIST) | {str(x).upper() for x in extra}
                print(f'[us-data-update] 加载外部黑名单 {len(extra)} 只, 总计 {len(DELISTED_BLACKLIST)}')
        except Exception as e:
            print(f'[us-data-update] 读 blacklist 失败 (忽略): {e}')


def log(msg):
    ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f'{ts} [us-data-update] {msg}', flush=True)


# ================ 锁 ================
def acquire_lock():
    try:
        fd = os.open(str(LOCK_FILE), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        info = f'pid={os.getpid()}\nstart={datetime.datetime.now().isoformat()}\n'
        os.write(fd, info.encode())
        os.close(fd)
        return True
    except FileExistsError:
        return False


def release_lock():
    try:
        LOCK_FILE.unlink()
    except FileNotFoundError:
        pass


# ================ 股池 (iShares) ================
def get_us_stock_list(fallback_tickers=None):
    """抓 S&P 500 + 400 + 600 (iShares 官方 ETF holdings CSV)."""
    headers = {'User-Agent': 'Mozilla/5.0'}
    sources = [
        ('S&P 500', 'https://www.ishares.com/us/products/239726/ishares-core-sp-500-etf/1467271812596.ajax?fileType=csv&fileName=IVV_holdings&dataType=fund'),
        ('S&P 400', 'https://www.ishares.com/us/products/239763/ishares-core-sp-midcap-etf/1467271812596.ajax?fileType=csv&fileName=IJH_holdings&dataType=fund'),
        ('S&P 600', 'https://www.ishares.com/us/products/239774/ishares-core-sp-smallcap-etf/1467271812596.ajax?fileType=csv&fileName=IJR_holdings&dataType=fund'),
    ]
    all_tickers = []
    for name, url in sources:
        try:
            resp = requests.get(url, headers=headers, timeout=HTTP_TIMEOUT)
            resp.raise_for_status()
            df = pd.read_csv(StringIO(resp.text), skiprows=9)
            df = df[df['Asset Class'].astype(str).str.strip() == 'Equity']
            tickers = df['Ticker'].dropna().astype(str).str.strip().tolist()
            all_tickers.extend(tickers)
            log(f'{name}: +{len(tickers)}')
        except Exception as e:
            log(f'{name} 抓取失败: {e}')

    raw = {CLASS_SHARE_MAP.get(s, s)
           for s in (str(t).replace('.', '-').upper() for t in all_tickers)}

    if not raw and fallback_tickers:
        log('⚠ 股池抓取全部失败, 用 parquet 已有 ticker 兜底')
        raw = set(fallback_tickers)

    cleaned, dropped = [], []
    for t in sorted(raw):
        if TICKER_RE.match(t):
            cleaned.append(t)
        else:
            dropped.append(t)
    if dropped:
        log(f'过滤异常 ticker {len(dropped)} 个: {dropped[:10]}{"..." if len(dropped)>10 else ""}')

    if DELISTED_BLACKLIST:
        before_bl = len(cleaned)
        cleaned = [t for t in cleaned if t not in DELISTED_BLACKLIST]
        if before_bl - len(cleaned) > 0:
            log(f'过滤黑名单 ticker {before_bl - len(cleaned)} 个')

    return cleaned


# ================ Nasdaq 抓取 ================
def _nasdaq_symbol(ticker):
    """parquet 的 'BRK-B' 在 Nasdaq API 里是 'BRK.B'."""
    return ticker.replace('-', '.') if '-' in ticker else ticker


def _fetch_nasdaq(ticker, fromdate, todate):
    """调用 Nasdaq historical, 返回 list[dict] 原始行 (可能为空).
    429 按 [10,20,30] 秒退避重试 3 次, 其它异常最后一轮抛出."""
    sym = _nasdaq_symbol(ticker)
    url = (
        f'https://api.nasdaq.com/api/quote/{sym}/historical'
        f'?assetclass=stocks&fromdate={fromdate}&todate={todate}'
        f'&limit=9999'
    )
    last_err = None
    waits = [0] + NASDAQ_RETRY_WAIT
    for attempt, wait in enumerate(waits):
        if wait > 0:
            time.sleep(wait)
        try:
            req = urllib.request.Request(url, headers=NASDAQ_HDR)
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
                payload = json.loads(r.read().decode('utf-8', errors='replace'))
            return (payload.get('data') or {}).get('tradesTable', {}).get('rows') or []
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < NASDAQ_RETRIES:
                last_err = e
                continue
            raise
        except Exception as e:
            last_err = e
            if attempt < NASDAQ_RETRIES:
                continue
            raise
    if last_err:
        raise last_err
    return []


def _parse_row(ticker, r):
    """Nasdaq 历史行 → {date,ticker,open,high,low,close,volume} 或 None."""
    try:
        mm, dd, yy = r['date'].split('/')
        d = datetime.date(int(yy), int(mm), int(dd))
    except Exception:
        return None

    def _num(key):
        v = (r.get(key) or '').replace('$', '').replace(',', '')
        if not v or v == 'N/A':
            return None
        try:
            return float(v)
        except ValueError:
            return None

    vol_str = (r.get('volume') or '').replace(',', '')
    try:
        volume = float(vol_str) if vol_str and vol_str != 'N/A' else None
    except ValueError:
        volume = None

    o = _num('open'); h = _num('high'); l = _num('low'); c = _num('close')
    if None in (o, h, l, c) or volume is None:
        return None

    return {'date': d, 'ticker': ticker,
            'open': o, 'high': h, 'low': l, 'close': c, 'volume': volume}


def _sanitize_nasdaq(df, ticker):
    """防御: Nasdaq 理论上无 halt 零成交, 但保留中间 vol=0 前向填充."""
    if df is None or len(df) == 0:
        return df
    df = df.sort_values('date').reset_index(drop=True)
    zero_mask = df['volume'] == 0
    if zero_mask.any():
        prev_close = df['close'].shift(1)
        fill_mask = zero_mask & prev_close.notna()
        if fill_mask.any():
            for col in ['open', 'high', 'low', 'close']:
                df.loc[fill_mask, col] = prev_close[fill_mask]
            log(f'  {ticker}: 填充 {int(fill_mask.sum())} 行 halt (vol=0)')
    return df


# ================ 读存量 ================
def load_existing():
    if not PARQUET.exists():
        log('⚠ 首次运行, parquet 不存在, 将对每支拉 5 年历史')
        return None, {}
    df = pd.read_parquet(PARQUET)
    log(f'加载存量 parquet: rows={len(df):,} tickers={df["ticker"].nunique()}')
    last_dates = df.groupby('ticker')['date'].max().to_dict()
    return df, last_dates


# ================ 增量下载 ================
def download_incremental(tickers, last_dates):
    today = datetime.datetime.now().date()
    full_from = (today - datetime.timedelta(days=365 * 5 + 30)).isoformat()
    today_iso = today.isoformat()
    stats = {'skip': 0, 'update': 0, 'new_ticker': 0, 'empty': 0, 'error': 0}
    errors = {}
    new_frames = []
    err_logged = 0
    total = len(tickers)

    for i, ticker in enumerate(tickers):
        if ticker in DELISTED_BLACKLIST:
            stats['skip'] += 1
            continue

        last_date = last_dates.get(ticker)
        # 只有 parquet 里已经是今天的才跳过; 昨天的必须尝试补今天
        if last_date is not None and last_date >= today:
            stats['skip'] += 1
            continue

        if last_date is not None:
            fromdate = (last_date + datetime.timedelta(days=1)).isoformat()
        else:
            fromdate = full_from

        try:
            raw = _fetch_nasdaq(ticker, fromdate, today_iso)
            if not raw:
                stats['empty'] += 1
                continue

            parsed = [_parse_row(ticker, r) for r in raw]
            parsed = [p for p in parsed if p is not None]
            if not parsed:
                stats['empty'] += 1
                continue

            data = pd.DataFrame(parsed, columns=['date','ticker','open','high','low','close','volume'])
            data = _sanitize_nasdaq(data, ticker)

            if last_date is not None:
                data = data[data['date'] > last_date]
                if data.empty:
                    stats['empty'] += 1
                    continue
                stats['update'] += 1
            else:
                stats['new_ticker'] += 1

            new_frames.append(data)

        except Exception as e:
            stats['error'] += 1
            errors[ticker] = f'{type(e).__name__}: {e}'
            if err_logged < MAX_ERROR_LOG_LINES:
                log(f'  {ticker} 跳过: {type(e).__name__}: {e}')
                err_logged += 1

        time.sleep(NASDAQ_SLEEP)
        if (i + 1) % 200 == 0:
            log(f'  progress {i + 1}/{total}  {stats}')

    log(f'下载完成: {stats}')
    return new_frames, stats, errors


# ================ 合并 + 原子写 ================
def merge_and_write(existing, new_frames):
    if not new_frames:
        log('无新数据需要合并')
        if existing is not None:
            return len(existing), existing['ticker'].nunique()
        return 0, 0

    new_long = pd.concat(new_frames, ignore_index=True)
    new_long = new_long[['date', 'ticker', 'open', 'high', 'low', 'close', 'volume']]
    for c in ['open','high','low','close','volume']:
        new_long[c] = new_long[c].astype('float64')
    log(f'新增数据: {len(new_long):,} 行')

    if existing is None or existing.empty:
        combined = new_long
    else:
        combined = pd.concat([existing, new_long], ignore_index=True)
        before = len(combined)
        combined = combined.drop_duplicates(subset=['ticker', 'date'], keep='last')
        after = len(combined)
        if before != after:
            log(f'去重: {before - after} 条冲突')

    combined = combined.sort_values(['ticker', 'date']).reset_index(drop=True)

    if len(combined) < MIN_ROWS_FOR_SWAP:
        raise RuntimeError(
            f'合并后行数 {len(combined):,} < {MIN_ROWS_FOR_SWAP:,}, 拒绝写入 (保护生产数据)'
        )

    if PARQUET.exists():
        shutil.copy2(PARQUET, PARQUET_BAK)
        log(f'备份 → {PARQUET_BAK.name}')

    combined.to_parquet(PARQUET_TMP, engine='pyarrow', compression='snappy', index=False)
    os.replace(PARQUET_TMP, PARQUET)
    size_mb = PARQUET.stat().st_size / 1024 / 1024
    log(
        f'写入 {PARQUET.name}: {len(combined):,} 行 '
        f'{combined["ticker"].nunique()} tickers, {size_mb:.1f} MB'
    )

    return len(combined), combined['ticker'].nunique()


# ================ 状态持久化 ================
def write_progress(stats, n_rows, n_tickers, elapsed):
    try:
        PROGRESS_FILE.write_text(
            json.dumps(
                {
                    'last_run': datetime.datetime.now().isoformat(),
                    'elapsed_seconds': round(elapsed, 1),
                    'download_stats': stats,
                    'parquet_rows': n_rows,
                    'parquet_tickers': n_tickers,
                    'source': 'nasdaq',
                },
                indent=2,
                ensure_ascii=False,
            ),
            encoding='utf-8',
        )
    except Exception as e:
        log(f'写 progress.json 失败: {e}')


def write_errors(errors):
    try:
        payload = {
            'last_run': datetime.datetime.now().isoformat(),
            'count': len(errors),
            'errors': errors,
        }
        ERRORS_FILE.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False), encoding='utf-8'
        )
    except Exception as e:
        log(f'写 errors.json 失败: {e}')


def write_status(ok, detail):
    try:
        STATUS_FILE.write_text(
            f'timestamp={datetime.datetime.now().isoformat()}\n'
            f'status={"OK" if ok else "FAIL"}\n'
            f'detail={detail}\n',
            encoding='utf-8',
        )
    except Exception as e:
        log(f'写 status 失败: {e}')


# ================ 主流程 ================
def main():
    parser = argparse.ArgumentParser(description='美股日线数据更新器 (Nasdaq)')
    parser.add_argument('--limit', type=int, default=0, help='只处理前 N 支 (调试)')
    parser.add_argument('--force', action='store_true', help='忽略已存在的 lock 文件')
    parser.add_argument('--dry-run', action='store_true', help='不写 parquet, 只汇总')
    args = parser.parse_args()

    if not acquire_lock():
        if args.force:
            log('⚠ --force, 强制清除旧 lock 并重新获取')
            release_lock()
            if not acquire_lock():
                log('清除后依然获取 lock 失败, 退出')
                return 2
        else:
            log(f'另一个进程似乎正在运行 ({LOCK_FILE} 存在), 退出')
            return 2

    t0 = time.time()
    log(f'=== start === BASE={BASE} source=nasdaq')

    _load_blacklist()
    log(f'黑名单 ticker 数: {len(DELISTED_BLACKLIST)}')

    try:
        existing, last_dates = load_existing()

        tickers = get_us_stock_list(
            fallback_tickers=list(last_dates.keys()) if last_dates else None
        )
        log(f'股池规模: {len(tickers)}')

        if args.limit > 0:
            tickers = tickers[: args.limit]
            log(f'⚠ --limit {args.limit}, 仅处理前 {len(tickers)} 支')

        if not tickers:
            raise RuntimeError('空股池, 无法继续')

        new_frames, stats, errors = download_incremental(tickers, last_dates)
        write_errors(errors)

        if args.dry_run:
            new_count = sum(len(f) for f in new_frames)
            log(f'--dry-run: 跳过写入, 本次会新增 {new_count} 行')
            n_rows = len(existing) if existing is not None else 0
            n_tickers = len(last_dates)
        else:
            n_rows, n_tickers = merge_and_write(existing, new_frames)

        elapsed = time.time() - t0
        write_progress(stats, n_rows, n_tickers, elapsed)
        write_status(
            True,
            f'rows={n_rows} tickers={n_tickers} stats={stats} elapsed={elapsed:.1f}s',
        )
        log(f'=== done === {elapsed:.1f}s')
        return 0

    except Exception as e:
        log(f'=== FAIL === {type(e).__name__}: {e}')
        log(traceback.format_exc())
        write_status(False, f'{type(e).__name__}: {e}')
        return 1

    finally:
        release_lock()


if __name__ == '__main__':
    sys.exit(main())
