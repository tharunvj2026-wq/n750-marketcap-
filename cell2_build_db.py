# ============================================
# CELL 2: BUILD DATABASE FROM NSELIB WITH CACHE
# ============================================

import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import warnings
warnings.filterwarnings('ignore')

from cell1_marketcap import ALL_SYMBOLS

def fetch_bhavcopy_for_date(trade_date):
    """Fetch bhavcopy for a single date from nselib"""
    try:
        from nselib import capital_market
        date_str = trade_date.strftime('%d-%m-%Y')
        df = capital_market.bhav_copy_with_delivery(trade_date=date_str)
        
        if df is None or len(df) == 0:
            return None
        
        df.columns = df.columns.str.upper().str.strip()
        
        result = pd.DataFrame()
        result['SYMBOL'] = df['SYMBOL'].astype(str).str.strip().str.upper()
        result['SERIES'] = df['SERIES'].astype(str).str.strip().str.upper()
        result['CLOSE'] = pd.to_numeric(df['CLOSE_PRICE'], errors='coerce')
        result['VOLUME'] = pd.to_numeric(df['TTL_TRD_QNTY'], errors='coerce')
        result['DELIVERY'] = pd.to_numeric(df['DELIV_PER'], errors='coerce')
        result['HIGH'] = pd.to_numeric(df['HIGH_PRICE'], errors='coerce')
        result['LOW'] = pd.to_numeric(df['LOW_PRICE'], errors='coerce')
        result['OPEN'] = pd.to_numeric(df['OPEN_PRICE'], errors='coerce')
        result['DATE'] = trade_date
        
        result = result[result['SERIES'] == 'EQ']
        result = result[result['SYMBOL'].isin(ALL_SYMBOLS)]
        result = result[result['CLOSE'].notna()]
        
        return result[['SYMBOL', 'DATE', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME', 'DELIVERY']]
        
    except Exception as e:
        return None

def get_last_n_trading_dates(n=100):
    """Get last n trading dates"""
    from nselib import trading_holiday_calendar
    
    try:
        holidays_df = trading_holiday_calendar()
        holidays = set()
        if holidays_df is not None and len(holidays_df) > 0:
            date_col = 'TRADING_DATE' if 'TRADING_DATE' in holidays_df.columns else 'DATE'
            if date_col in holidays_df.columns:
                holidays = set(pd.to_datetime(holidays_df[date_col]).dt.date)
    except:
        holidays = set()
    
    trading_dates = []
    current_date = datetime.now().date()
    days_checked = 0
    
    while len(trading_dates) < n and days_checked < 200:
        days_checked += 1
        check_date = current_date - timedelta(days=days_checked)
        
        if check_date.weekday() >= 5:
            continue
        if check_date in holidays:
            continue
        
        trading_dates.append(check_date)
    
    return sorted(trading_dates)

def build_database_cache(cache_file="nifty750_bhavcopy_cache.csv", days=100):
    """Build local database cache from nselib"""
    
    if os.path.exists(cache_file):
        file_time = datetime.fromtimestamp(os.path.getmtime(cache_file))
        if file_time.date() == datetime.now().date():
            print(f"✅ Loading cached data from {cache_file}")
            df = pd.read_csv(cache_file, parse_dates=['DATE'])
            print(f"   Rows: {len(df):,} | Dates: {df['DATE'].min().date()} to {df['DATE'].max().date()}")
            return df
    
    print(f"📥 Fetching last {days} trading days from nselib...")
    trading_dates = get_last_n_trading_dates(days)
    print(f"📅 Dates to fetch: {len(trading_dates)}")
    
    all_data = []
    success_count = 0
    
    for i, trade_date in enumerate(trading_dates):
        print(f"\r   [{i+1}/{len(trading_dates)}] {trade_date.strftime('%Y-%m-%d')}...", end="")
        
        df = fetch_bhavcopy_for_date(trade_date)
        if df is not None and len(df) > 0:
            all_data.append(df)
            success_count += 1
        
        time.sleep(0.2)
    
    print(f"\n   ✅ Successfully fetched {success_count}/{len(trading_dates)} dates")
    
    if not all_data:
        print("❌ No data fetched")
        return None
    
    combined_df = pd.concat(all_data, ignore_index=True)
    combined_df = combined_df.sort_values(['DATE', 'SYMBOL'])
    combined_df = combined_df.drop_duplicates(subset=['SYMBOL', 'DATE'])
    
    combined_df.to_csv(cache_file, index=False)
    print(f"💾 Saved to {cache_file}")
    print(f"   Total rows: {len(combined_df):,}")
    print(f"   Unique symbols: {combined_df['SYMBOL'].nunique()}")
    print(f"   Date range: {combined_df['DATE'].min()} to {combined_df['DATE'].max()}")
    
    return combined_df

if __name__ == "__main__":
    print("=" * 60)
    print("BUILDING NIFTY 750 DATABASE FROM NSELIB")
    print("=" * 60)
    df = build_database_cache()