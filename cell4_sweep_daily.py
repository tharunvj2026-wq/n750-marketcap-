# ============================================
# CELL 4: DAILY SWEEP CANDLE SCANNER (AUTO-LATEST DATE)
# Always uses the most recent date available in cache
# ============================================

import os
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings
warnings.filterwarnings('ignore')

from cell1_marketcap import ALL_SYMBOLS, SYMBOL_TO_SEGMENT
from cell3_coil_analysis import send_telegram_message

CACHE_FILE = "nifty750_bhavcopy_cache.csv"

def get_latest_trading_date_from_cache():
    """Get the most recent date that exists in the cache"""
    if not os.path.exists(CACHE_FILE):
        return None
    df = pd.read_csv(CACHE_FILE, parse_dates=['DATE'])
    if df.empty:
        return None
    latest = df['DATE'].max().date()
    return latest

def get_previous_trading_date_from_cache(date):
    """Get previous trading date that exists in cache (not just calendar)"""
    if not os.path.exists(CACHE_FILE):
        return None
    df = pd.read_csv(CACHE_FILE, parse_dates=['DATE'])
    dates = sorted(df['DATE'].dt.date.unique())
    idx = dates.index(date)
    if idx > 0:
        return dates[idx-1]
    return None

def fetch_candle(symbol, target_date):
    """Fetch candle from cache for exact date"""
    try:
        if not os.path.exists(CACHE_FILE):
            return None
        df = pd.read_csv(CACHE_FILE, parse_dates=['DATE'])
        row = df[(df['SYMBOL'] == symbol) & (df['DATE'].dt.date == target_date)]
        if row.empty:
            return None
        return {
            'open': float(row['OPEN'].iloc[0]),
            'high': float(row['HIGH'].iloc[0]),
            'low': float(row['LOW'].iloc[0]),
            'close': float(row['CLOSE'].iloc[0])
        }
    except:
        return None

def is_sweep(current, prev):
    prev_body_low = min(prev['open'], prev['close'])
    prev_body_high = max(prev['open'], prev['close'])
    return (prev_body_low < current['open'] < prev_body_high and
            prev_body_low < current['close'] < prev_body_high and
            current['low'] < prev['low'])

def scan_symbol(symbol, target, prev):
    curr = fetch_candle(symbol, target)
    prev_c = fetch_candle(symbol, prev)
    if curr and prev_c and is_sweep(curr, prev_c):
        return {
            'symbol': symbol,
            'price': round(curr['close'], 2),
            'segment': SYMBOL_TO_SEGMENT.get(symbol, 'MID')
        }
    return None

def main():
    print("=" * 60)
    print("DAILY SWEEP SCANNER (Auto-latest date)")
    print("=" * 60)

    # Get latest date from cache
    target_date = get_latest_trading_date_from_cache()
    if target_date is None:
        print("❌ No cache data found. Run cell2_build_db.py first.")
        return

    prev_date = get_previous_trading_date_from_cache(target_date)
    if prev_date is None:
        print(f"❌ Only one date in cache: {target_date}. Need at least two dates for sweep.")
        return

    print(f"📅 Latest cached date: {target_date}")
    print(f"📅 Previous cached date: {prev_date}")
    print(f"🔍 Checking sweep condition from {prev_date} to {target_date}")

    # Scan in parallel
    results = []
    with ThreadPoolExecutor(max_workers=30) as ex:
        futures = {ex.submit(scan_symbol, sym, target_date, prev_date): sym for sym in ALL_SYMBOLS}
        for i, fut in enumerate(as_completed(futures)):
            if (i+1) % 100 == 0:
                print(f"Progress: {i+1}/{len(ALL_SYMBOLS)}", end="\r")
            r = fut.result()
            if r:
                results.append(r)

    print(f"\n✅ Found {len(results)} sweep candles")

    # Build Telegram message
    if not results:
        msg = f"🔍 SWEEP DAILY | {target_date.strftime('%d-%b-%Y')}\n━━━━━━━━━━━━━━━━━━━━━━━━━\n\n⚠️ NO SWEEP CANDLES FOUND"
    else:
        seg_map = {'LARGE': [], 'MID': [], 'SMALL': [], 'MICRO': []}
        for r in results:
            seg = r['segment']
            if seg in seg_map:
                seg_map[seg].append(r)
        msg = f"🎯 SWEEP DAILY | {target_date.strftime('%d-%b-%Y')}\n━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        for seg in ['LARGE', 'MID', 'SMALL', 'MICRO']:
            if seg_map[seg]:
                msg += f"\n*{seg}*\n"
                for r in seg_map[seg]:
                    msg += f"   {r['symbol']} ₹{r['price']}\n"
        msg += f"\n━━━━━━━━━━━━━━━━━━━━━━━━━\n📊 Total: {len(results)} stocks"

    send_telegram_message(msg)
    print("✅ Alert sent")

if __name__ == "__main__":
    main()