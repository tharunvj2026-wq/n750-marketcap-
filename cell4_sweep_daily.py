# ============================================
# CELL 4: DAILY SWEEP CANDLE SCANNER (FIXED ALERT)
# Condition: Open & Close inside previous body, Low < previous low
# ============================================

import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import warnings
warnings.filterwarnings('ignore')

# Import shared stock lists from cell1
from cell1_marketcap import ALL_SYMBOLS, SYMBOL_TO_SEGMENT

# Import Telegram functions from cell3
from cell3_coil_analysis import send_telegram_message

# ============================================
# CONFIGURATION
# ============================================

CACHE_FILE = "nifty750_bhavcopy_cache.csv"

# ============================================
# TRADING DAY HELPERS
# ============================================

def is_trading_day(date):
    return date.weekday() < 5

def get_previous_trading_day(date):
    d = date - timedelta(days=1)
    while not is_trading_day(d):
        d -= timedelta(days=1)
    return d

def get_last_n_trading_dates_including_today(n=2):
    dates = []
    d = datetime.now().date()
    while len(dates) < n:
        if is_trading_day(d):
            dates.append(d)
        d -= timedelta(days=1)
    return dates

# ============================================
# DATA FETCHING
# ============================================

def fetch_from_cache(symbol, target_date):
    try:
        if not os.path.exists(CACHE_FILE):
            return None
        df = pd.read_csv(CACHE_FILE, parse_dates=['DATE'])
        df = df[df['SYMBOL'] == symbol]
        df = df[df['DATE'].dt.date == target_date]
        if df.empty:
            return None
        row = df.iloc[0]
        return {
            'open': float(row['OPEN']),
            'high': float(row['HIGH']),
            'low': float(row['LOW']),
            'close': float(row['CLOSE'])
        }
    except Exception:
        return None

def fetch_from_yfinance(symbol, target_date):
    try:
        import yfinance as yf
        ticker = yf.Ticker(symbol + ".NS")
        start = target_date - timedelta(days=5)
        end = target_date + timedelta(days=1)
        data = ticker.history(start=start, end=end, interval="1d")
        if data.empty:
            return None
        data = data.reset_index()
        data['Date'] = pd.to_datetime(data['Date']).dt.date
        row = data[data['Date'] == target_date]
        if row.empty:
            return None
        return {
            'open': float(row['Open'].iloc[0]),
            'high': float(row['High'].iloc[0]),
            'low': float(row['Low'].iloc[0]),
            'close': float(row['Close'].iloc[0])
        }
    except Exception:
        return None

def fetch_data_for_date(symbol, target_date):
    data = fetch_from_cache(symbol, target_date)
    if data:
        return data
    return fetch_from_yfinance(symbol, target_date)

# ============================================
# SWEEP CONDITION
# ============================================

def is_sweep_candle(current, prev):
    prev_body_low = min(prev['open'], prev['close'])
    prev_body_high = max(prev['open'], prev['close'])
    open_inside = (current['open'] > prev_body_low) and (current['open'] < prev_body_high)
    close_inside = (current['close'] > prev_body_low) and (current['close'] < prev_body_high)
    low_swept = current['low'] < prev['low']
    return open_inside and close_inside and low_swept

def scan_symbol_for_date(symbol, target_date, prev_date):
    current = fetch_data_for_date(symbol, target_date)
    prev = fetch_data_for_date(symbol, prev_date)
    if current is None or prev is None:
        return None
    if is_sweep_candle(current, prev):
        return {
            'symbol': symbol,
            'price': round(current['close'], 2),
            'segment': SYMBOL_TO_SEGMENT.get(symbol, 'MID')
        }
    return None

# ============================================
# FIXED TELEGRAM ALERT FORMATTING
# ============================================

def format_alert_message(results, latest_date):
    """Format minimal Telegram alert message - FIXED"""
    
    if not results:
        return f"""🔍 SWEEP DAILY | {latest_date}
━━━━━━━━━━━━━━━━━━━━━━━━━

⚠️ NO SWEEP CANDLES FOUND

Condition: Open/Close inside previous body, Low < previous low

No stocks met the criteria."""
    
    # Group by segment
    segment_order = ['LARGE', 'MID', 'SMALL', 'MICRO']
    grouped = {seg: [] for seg in segment_order}
    
    for r in results:
        seg = r['segment']
        if seg in grouped:
            grouped[seg].append(r)
    
    # Build message
    message = f"""🎯 SWEEP DAILY | {latest_date}
━━━━━━━━━━━━━━━━━━━━━━━━━

"""
    
    for seg in segment_order:
        if grouped[seg]:
            message += f"\n*{seg}*\n"
            for r in grouped[seg][:15]:  # Max 15 per segment
                message += f"   {r['symbol']} ₹{r['price']}\n"
    
    message += f"\n━━━━━━━━━━━━━━━━━━━━━━━━━\n📊 Total: {len(results)} stocks"
    
    return message

# ============================================
# MAIN FUNCTION
# ============================================

def main():
    print("=" * 60)
    print("DAILY SWEEP CANDLE SCANNER")
    print("Condition: Open & Close inside previous body, Low < previous low")
    print("=" * 60)
    
    # Get last 2 trading dates
    trading_dates = get_last_n_trading_dates_including_today(2)
    trading_dates_desc = sorted(trading_dates, reverse=True)
    
    print(f"\n📅 Last 2 trading days: {[d.strftime('%Y-%m-%d') for d in trading_dates_desc]}")
    
    # Create date pairs (today->yesterday, yesterday->day before)
    date_pairs = []
    for date in trading_dates_desc:
        prev = get_previous_trading_day(date)
        date_pairs.append((date, prev))
        print(f"   {date.strftime('%Y-%m-%d')} → previous: {prev.strftime('%Y-%m-%d')}")
    
    # Prepare tasks
    tasks = [(sym, target, prev) for sym in ALL_SYMBOLS for (target, prev) in date_pairs]
    
    print(f"\n🔍 Scanning {len(ALL_SYMBOLS)} symbols...")
    
    # Collect results for the latest date only (first in desc list)
    latest_date = trading_dates_desc[0]
    results = []
    
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(scan_symbol_for_date, sym, target, prev): (sym, target) 
                   for (sym, target, prev) in tasks}
        
        completed = 0
        for future in as_completed(futures):
            sym, target = futures[future]
            res = future.result()
            completed += 1
            if completed % 100 == 0:
                print(f"   Progress: {completed}/{len(tasks)}", end="\r")
            if res and target == latest_date:  # Only keep latest date results
                results.append(res)
    
    print(f"\n   ✅ Scan complete. Found: {len(results)} stocks")
    
    # Show in console
    if results:
        print(f"\n📊 SWEEP CANDLES FOUND ({latest_date.strftime('%Y-%m-%d')}):")
        for seg in ['LARGE', 'MID', 'SMALL', 'MICRO']:
            seg_results = [r for r in results if r['segment'] == seg]
            if seg_results:
                print(f"\n   {seg}:")
                for r in seg_results[:5]:
                    print(f"      {r['symbol']} ₹{r['price']}")
    else:
        print("\n📊 No sweep candles found.")
    
    # Send Telegram alert
    print("\n📨 Sending Telegram alert...")
    message = format_alert_message(results, latest_date.strftime('%d-%b-%Y'))
    send_telegram_message(message)
    
    print(f"\n✅ Alert sent at {datetime.now().strftime('%H:%M:%S')}")
    print("=" * 60)
    
    return results

if __name__ == "__main__":
    main()