# ============================================
# CELL 5: WEEKLY SWEEP CANDLE SCANNER
# Condition: Week open/close inside previous week's body, week low < previous week low
# ============================================

import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
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

def get_last_completed_friday():
    """Get the last completed Friday (week ending date)"""
    today = datetime.now().date()
    days_since_friday = (today.weekday() - 4) % 7
    if days_since_friday == 0 and today.weekday() == 4:
        # Today is Friday, but before market close? Use previous Friday
        days_since_friday = 7
    last_friday = today - timedelta(days=days_since_friday if days_since_friday > 0 else 7)
    return last_friday

def is_trading_day(date):
    """Simple check - weekday"""
    return date.weekday() < 5

# ============================================
# DATA FETCHING FROM CACHE
# ============================================

def get_daily_data_from_cache(symbol):
    """Get all daily data for a symbol from cache"""
    try:
        if not os.path.exists(CACHE_FILE):
            return None
        
        df = pd.read_csv(CACHE_FILE, parse_dates=['DATE'])
        df = df[df['SYMBOL'] == symbol]
        df = df.sort_values('DATE')
        
        if df.empty or len(df) < 10:
            return None
        
        return df[['DATE', 'OPEN', 'HIGH', 'LOW', 'CLOSE']]
    except Exception as e:
        return None

def aggregate_to_weekly(daily_df, end_date):
    """Aggregate daily data to weekly candles (Monday to Friday)"""
    if daily_df is None or daily_df.empty:
        return None
    
    df = daily_df.copy()
    df['DATE'] = pd.to_datetime(df['DATE'])
    
    # Filter data up to end_date
    df = df[df['DATE'].dt.date <= end_date]
    
    if df.empty or len(df) < 5:
        return None
    
    # Add week start (Monday)
    df['week_start'] = df['DATE'] - pd.to_timedelta(df['DATE'].dt.weekday, unit='D')
    
    # Aggregate by week
    weekly = df.groupby('week_start').agg(
        week_open=('OPEN', 'first'),
        week_high=('HIGH', 'max'),
        week_low=('LOW', 'min'),
        week_close=('CLOSE', 'last')
    ).reset_index()
    
    # Add week end (Friday)
    weekly['week_end'] = weekly['week_start'] + pd.Timedelta(days=4)
    weekly['week_end_date'] = weekly['week_end'].dt.date
    
    # Sort by week_start
    weekly = weekly.sort_values('week_start')
    
    return weekly

# ============================================
# WEEKLY SWEEP CONDITION
# ============================================

def is_weekly_sweep(current_week, prev_week):
    """Check if current week is a sweep of previous week"""
    prev_body_low = min(prev_week['week_open'], prev_week['week_close'])
    prev_body_high = max(prev_week['week_open'], prev_week['week_close'])
    
    open_inside = (current_week['week_open'] > prev_body_low) and (current_week['week_open'] < prev_body_high)
    close_inside = (current_week['week_close'] > prev_body_low) and (current_week['week_close'] < prev_body_high)
    low_swept = current_week['week_low'] < prev_week['week_low']
    
    return open_inside and close_inside and low_swept

def analyze_symbol_weekly(symbol, end_date):
    """Analyze a single symbol for weekly sweep"""
    try:
        daily_df = get_daily_data_from_cache(symbol)
        if daily_df is None:
            return None
        
        weekly = aggregate_to_weekly(daily_df, end_date)
        if weekly is None or len(weekly) < 2:
            return None
        
        # Find weeks ending on or before end_date
        valid = weekly[weekly['week_end_date'] <= end_date].copy()
        if len(valid) < 2:
            return None
        
        current = valid.iloc[-1]
        prev = valid.iloc[-2]
        
        if is_weekly_sweep(current, prev):
            return {
                'symbol': symbol,
                'price': round(current['week_close'], 2),
                'segment': SYMBOL_TO_SEGMENT.get(symbol, 'MID'),
                'week_end': current['week_end_date'].strftime('%Y-%m-%d')
            }
    except Exception as e:
        pass
    
    return None

# ============================================
# TELEGRAM ALERT FORMATTING
# ============================================

def format_alert_message(results, week_ending):
    """Format minimal Telegram alert message"""
    
    if not results:
        return f"""🔍 SWEEP WEEKLY | {week_ending}
━━━━━━━━━━━━━━━━━━━━━━━━━

⚠️ NO WEEKLY SWEEP CANDLES FOUND

Condition: Week open/close inside previous week's body, week low < previous week low

No stocks met the criteria."""
    
    message = f"""🎯 SWEEP WEEKLY | {week_ending}
━━━━━━━━━━━━━━━━━━━━━━━━━

"""
    
    segment_order = ['LARGE', 'MID', 'SMALL', 'MICRO']
    segment_display = {'LARGE': 'LARGE', 'MID': 'MID', 'SMALL': 'SMALL', 'MICRO': 'MICRO'}
    
    # Group by segment
    grouped = {seg: [] for seg in segment_order}
    for r in results:
        seg = r['segment']
        if seg in grouped:
            grouped[seg].append(r)
    
    for seg in segment_order:
        if grouped[seg]:
            message += f"\n*{segment_display[seg]}*\n"
            for r in grouped[seg][:10]:  # Max 10 per segment
                message += f"   {r['symbol']} ₹{r['price']}\n"
    
    message += f"\n━━━━━━━━━━━━━━━━━━━━━━━━━\n📊 Total: {len(results)} stocks"
    
    return message

# ============================================
# MAIN FUNCTION
# ============================================

def main():
    print("=" * 60)
    print("WEEKLY SWEEP CANDLE SCANNER")
    print("Condition: Week open/close inside previous week's body, week low < previous week low")
    print("=" * 60)
    
    # Check if cache exists
    if not os.path.exists(CACHE_FILE):
        print("❌ Cache file not found. Run cell2_build_db.py first.")
        return None
    
    # Get last completed week
    week_ending = get_last_completed_friday()
    print(f"\n📅 Last completed week ending: {week_ending.strftime('%Y-%m-%d')}")
    
    # Load cache info
    df_cache = pd.read_csv(CACHE_FILE, parse_dates=['DATE'])
    print(f"📊 Cache date range: {df_cache['DATE'].min().date()} to {df_cache['DATE'].max().date()}")
    
    # Scan all symbols
    print(f"\n🔍 Scanning {len(ALL_SYMBOLS)} symbols for weekly sweep...")
    
    results = []
    total = len(ALL_SYMBOLS)
    
    for i, symbol in enumerate(ALL_SYMBOLS):
        if (i + 1) % 100 == 0:
            print(f"   Progress: {i+1}/{total} - Found: {len(results)}", end="\r")
        
        result = analyze_symbol_weekly(symbol, week_ending)
        if result:
            results.append(result)
    
    print(f"\n   ✅ Scan complete. Found: {len(results)} stocks")
    
    # Display results in console
    if results:
        print(f"\n📊 WEEKLY SWEEP CANDLES FOUND: {len(results)}")
        segment_order = ['LARGE', 'MID', 'SMALL', 'MICRO']
        for seg in segment_order:
            seg_results = [r for r in results if r['segment'] == seg]
            if seg_results:
                print(f"\n   {seg}:")
                for r in seg_results[:5]:
                    print(f"      {r['symbol']} ₹{r['price']}")
    else:
        print("\n📊 No weekly sweep candles found.")
    
    # Send Telegram alert
    print("\n📨 Sending Telegram alert...")
    message = format_alert_message(results, week_ending.strftime('%d-%b-%Y'))
    send_telegram_message(message)
    
    print(f"\n✅ Alert sent at {datetime.now().strftime('%H:%M:%S')}")
    print("=" * 60)
    
    return results

if __name__ == "__main__":
    main()