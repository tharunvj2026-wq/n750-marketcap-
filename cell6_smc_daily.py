# ============================================
# CELL 6: DAILY SMC SCANNER (YFINANCE + LOCAL DB CACHE)
# First run: fetches from yfinance, saves to SQLite
# Later runs: reads from DB (fast), only updates last 5 days
# ============================================

import os
import sqlite3
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

# Import yfinance
try:
    import yfinance as yf
except ImportError:
    import subprocess
    import sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "yfinance", "-q"])
    import yfinance as yf

# ============================================
# CONFIGURATION
# ============================================

DB_FILE = "smc_daily_cache.db"  # Local database file

# SMC Parameters
DISCOUNT_THRESHOLD = 0.35
SWING_LOW_WINDOW = 5
MSS_THRESHOLD = 0.015
VOLUME_MULTIPLIER = 2.0
ORDER_BLOCK_MIN_BULLISH = 3
ORDER_BLOCK_MAX_BULLISH = 5
ORDER_BLOCK_RALLY_PCT = 0.006
FVG_MIN_BULLISH = 3
FVG_MAX_BULLISH = 6

# ============================================
# DATABASE SETUP & MANAGEMENT
# ============================================

def init_db():
    """Initialize SQLite database"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS daily_data (
            symbol TEXT,
            date TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume INTEGER,
            last_updated TEXT,
            PRIMARY KEY (symbol, date)
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS metadata (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    ''')
    
    conn.commit()
    conn.close()

def get_last_update_date():
    """Get last time database was updated"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT value FROM metadata WHERE key = 'last_update_date'")
    row = cursor.fetchone()
    conn.close()
    
    if row:
        return datetime.strptime(row[0], '%Y-%m-%d').date()
    return None

def set_last_update_date(date):
    """Set last update date in metadata"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)", 
                   ('last_update_date', date.strftime('%Y-%m-%d')))
    conn.commit()
    conn.close()

def get_existing_dates(symbol):
    """Get dates already in DB for a symbol"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT date FROM daily_data WHERE symbol = ?", (symbol,))
    rows = cursor.fetchall()
    conn.close()
    return {row[0] for row in rows}

def save_to_db(symbol, data):
    """Save stock data to database"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    for i in range(len(data['close'])):
        cursor.execute('''
            INSERT OR REPLACE INTO daily_data 
            (symbol, date, open, high, low, close, volume, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            symbol,
            data['dates'][i].strftime('%Y-%m-%d') if isinstance(data['dates'][i], pd.Timestamp) else str(data['dates'][i]),
            data['open'][i],
            data['high'][i],
            data['low'][i],
            data['close'][i],
            data['volume'][i],
            now
        ))
    
    conn.commit()
    conn.close()

def load_from_db(symbol):
    """Load stock data from database"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT date, open, high, low, close, volume 
        FROM daily_data 
        WHERE symbol = ? 
        ORDER BY date ASC
    ''', (symbol,))
    
    rows = cursor.fetchall()
    conn.close()
    
    if not rows or len(rows) < 100:
        return None
    
    return {
        'open': [row[1] for row in rows],
        'high': [row[2] for row in rows],
        'low': [row[3] for row in rows],
        'close': [row[4] for row in rows],
        'volume': [row[5] for row in rows],
        'dates': [datetime.strptime(row[0], '%Y-%m-%d').date() for row in rows]
    }

def fetch_and_update_symbol(symbol, force_refresh=False):
    """Fetch missing data from yfinance and update DB"""
    existing_dates = get_existing_dates(symbol)
    
    if force_refresh or not existing_dates:
        # Fetch full 1-year data
        ticker = yf.Ticker(symbol + ".NS")
        data = ticker.history(period="1y", interval="1d")
        
        if data.empty or len(data) < 100:
            return None
        
        result = {
            'open': data['Open'].tolist(),
            'high': data['High'].tolist(),
            'low': data['Low'].tolist(),
            'close': data['Close'].tolist(),
            'volume': data['Volume'].tolist(),
            'dates': data.index.tolist()
        }
        
        save_to_db(symbol, result)
        return result
    
    # Check if we need recent updates (last 5 days)
    if existing_dates:
        latest_date = max(datetime.strptime(d, '%Y-%m-%d').date() for d in existing_dates)
        today = datetime.now().date()
        
        if (today - latest_date).days > 2:
            # Fetch only recent data
            start_date = latest_date - timedelta(days=5)
            ticker = yf.Ticker(symbol + ".NS")
            data = ticker.history(start=start_date, end=today + timedelta(days=1), interval="1d")
            
            if not data.empty:
                new_data = {
                    'open': data['Open'].tolist(),
                    'high': data['High'].tolist(),
                    'low': data['Low'].tolist(),
                    'close': data['Close'].tolist(),
                    'volume': data['Volume'].tolist(),
                    'dates': data.index.tolist()
                }
                save_to_db(symbol, new_data)
    
    return load_from_db(symbol)

# ============================================
# SMC ANALYSIS FUNCTIONS
# ============================================

def calculate_discount_zone(data):
    """Check if stock is 35% below 52-week high"""
    try:
        all_highs = data['high']
        current_price = data['close'][-1]
        year_high = max(all_highs)
        
        if year_high == 0:
            return False
        
        return (year_high - current_price) / year_high >= DISCOUNT_THRESHOLD
    except:
        return False

def is_swing_low(data, window=SWING_LOW_WINDOW):
    """Check if current price is at swing low"""
    try:
        current_low = data['low'][-1]
        lookback_lows = data['low'][-window-1:-1]
        
        if len(lookback_lows) < window:
            return False
        
        return current_low <= min(lookback_lows)
    except:
        return False

def detect_order_block(data):
    """Detect Order Block"""
    try:
        if len(data['close']) < 25:
            return False
        
        for i in range(len(data['close'])-6, max(len(data['close'])-25, 0), -1):
            if data['close'][i] < data['open'][i]:
                bullish_count = 0
                for j in range(i+1, min(i+6, len(data['close']))):
                    if data['close'][j] > data['open'][j]:
                        bullish_count += 1
                    else:
                        break
                
                if ORDER_BLOCK_MIN_BULLISH <= bullish_count <= ORDER_BLOCK_MAX_BULLISH:
                    first_bullish_close = data['close'][i+1]
                    last_bullish_close = data['close'][i+bullish_count]
                    rally_pct = (last_bullish_close - first_bullish_close) / first_bullish_close
                    
                    if rally_pct > ORDER_BLOCK_RALLY_PCT:
                        return True
        return False
    except:
        return False

def detect_market_structure_shift(data):
    """Detect Market Structure Shift"""
    try:
        if len(data['close']) < 10:
            return False
        
        current_price = data['close'][-1]
        recent_lows = data['low'][-10:]
        recent_low = min(recent_lows)
        
        return current_price > recent_low * (1 + MSS_THRESHOLD)
    except:
        return False

def detect_fair_value_gap(data):
    """Detect Fair Value Gap"""
    try:
        if len(data['close']) < 15:
            return False
        
        for i in range(len(data['close'])-7, max(len(data['close'])-15, 0), -1):
            bullish_count = 0
            end_index = min(i+6, len(data['close'])-1)
            
            for j in range(i, end_index):
                if data['close'][j] > data['open'][j]:
                    bullish_count += 1
                else:
                    break
            
            if FVG_MIN_BULLISH <= bullish_count <= FVG_MAX_BULLISH:
                next_candle_idx = i + bullish_count
                if next_candle_idx < len(data['close']):
                    if data['close'][next_candle_idx] < data['open'][next_candle_idx]:
                        return True
        return False
    except:
        return False

def check_volume_spike(data):
    """Check if volume is 2x average"""
    try:
        if len(data['volume']) < 10:
            return False
        
        current_volume = data['volume'][-1]
        recent_volumes = data['volume'][-10:]
        avg_volume = sum(recent_volumes) / len(recent_volumes)
        
        if avg_volume == 0:
            return False
        
        return current_volume >= avg_volume * VOLUME_MULTIPLIER
    except:
        return False

def analyze_symbol_smc(symbol, use_cache=True):
    """Complete SMC analysis using local DB cache"""
    try:
        # Load from DB (fast)
        data = load_from_db(symbol)
        
        # If not in DB or outdated, fetch from yfinance
        if data is None:
            print(f"\n   📥 First time: fetching {symbol}...")
            data = fetch_and_update_symbol(symbol, force_refresh=True)
        else:
            # Quick check if data is recent (update if needed)
            last_date = data['dates'][-1]
            today = datetime.now().date()
            if (today - last_date).days > 2:
                print(f"\n   🔄 Updating {symbol}...")
                data = fetch_and_update_symbol(symbol, force_refresh=False)
        
        if data is None:
            return None
        
        # Primary filters
        discount_zone = calculate_discount_zone(data)
        swing_low = is_swing_low(data)
        
        if not (discount_zone and swing_low):
            return None
        
        # Secondary signals
        order_block = detect_order_block(data)
        mss = detect_market_structure_shift(data)
        fvg = detect_fair_value_gap(data)
        volume_spike = check_volume_spike(data)
        
        signals = []
        if order_block: signals.append('OB')
        if fvg: signals.append('FVG')
        if mss: signals.append('MSS')
        if volume_spike: signals.append('VOL')
        
        return {
            'symbol': symbol,
            'price': round(data['close'][-1], 2),
            'segment': SYMBOL_TO_SEGMENT.get(symbol, 'MID'),
            'order_block': order_block,
            'mss': mss,
            'fvg': fvg,
            'volume_spike': volume_spike,
            'signal_count': len(signals),
            'signals': ', '.join(signals) if signals else 'None'
        }
    except Exception as e:
        return None

# ============================================
# TELEGRAM ALERT FORMATTING
# ============================================

def format_alert_message(results, trade_date):
    """Format minimal Telegram alert message"""
    
    if not results:
        return f"""🔍 SMC DAILY | {trade_date}
━━━━━━━━━━━━━━━━━━━━━━━━━

⚠️ NO STOCKS FOUND

Conditions not met:
• 35% below 52-week high
• At daily swing low

No stocks met the primary criteria."""
    
    results_sorted = sorted(results, key=lambda x: x['signal_count'], reverse=True)
    
    message = f"""🎯 SMC DAILY | {trade_date}
━━━━━━━━━━━━━━━━━━━━━━━━━

"""
    
    segment_order = ['LARGE', 'MID', 'SMALL', 'MICRO']
    segment_display = {'LARGE': 'LARGE', 'MID': 'MID', 'SMALL': 'SMALL', 'MICRO': 'MICRO'}
    
    grouped = {seg: [] for seg in segment_order}
    for r in results_sorted:
        seg = r['segment']
        if seg in grouped:
            grouped[seg].append(r)
    
    for seg in segment_order:
        if grouped[seg]:
            message += f"\n*{segment_display[seg]}*\n"
            for r in grouped[seg][:10]:
                if r['signal_count'] > 0:
                    message += f"   {r['symbol']} ₹{r['price']} [{r['signals']}]\n"
                else:
                    message += f"   {r['symbol']} ₹{r['price']}\n"
    
    multi_signal = len([r for r in results if r['signal_count'] >= 2])
    
    message += f"\n━━━━━━━━━━━━━━━━━━━━━━━━━\n📊 Total: {len(results)} stocks"
    if multi_signal > 0:
        message += f" | Multi-signal: {multi_signal}"
    
    return message

# ============================================
# MAIN FUNCTION
# ============================================

def main():
    print("=" * 60)
    print("DAILY SMC SCANNER (with Local Database Cache)")
    print("First run: fetches from yfinance (slow)")
    print("Later runs: reads from DB (fast, 1-2 minutes)")
    print("=" * 60)
    
    # Initialize database
    init_db()
    
    trade_date = datetime.now().date().strftime('%d-%b-%Y')
    print(f"\n📅 Analysis date: {trade_date}")
    
    last_update = get_last_update_date()
    if last_update:
        print(f"📦 DB last updated: {last_update}")
    
    # Scan all symbols
    print(f"\n🔍 Scanning {len(ALL_SYMBOLS)} symbols...")
    print("   (First run slow, subsequent runs fast)\n")
    
    results = []
    total = len(ALL_SYMBOLS)
    
    for i, symbol in enumerate(ALL_SYMBOLS):
        if (i + 1) % 50 == 0:
            print(f"   Progress: {i+1}/{total} - Found: {len(results)}", end="\r")
        
        result = analyze_symbol_smc(symbol)
        if result:
            results.append(result)
    
    # Update last run date
    set_last_update_date(datetime.now().date())
    
    print(f"\n   ✅ Scan complete. Found: {len(results)} stocks")
    
    # Display results
    if results:
        print(f"\n📊 SMC SETUPS FOUND: {len(results)}")
        segment_order = ['LARGE', 'MID', 'SMALL', 'MICRO']
        results_sorted = sorted(results, key=lambda x: x['signal_count'], reverse=True)
        
        for seg in segment_order:
            seg_results = [r for r in results_sorted if r['segment'] == seg]
            if seg_results:
                print(f"\n   {seg}:")
                for r in seg_results[:5]:
                    if r['signal_count'] > 0:
                        print(f"      {r['symbol']} ₹{r['price']} - [{r['signals']}]")
                    else:
                        print(f"      {r['symbol']} ₹{r['price']}")
        
        ob_count = len([r for r in results if r['order_block']])
        fvg_count = len([r for r in results if r['fvg']])
        mss_count = len([r for r in results if r['mss']])
        vol_count = len([r for r in results if r['volume_spike']])
        
        print(f"\n📊 SIGNAL SUMMARY:")
        print(f"   OB: {ob_count} | FVG: {fvg_count} | MSS: {mss_count} | VOL: {vol_count}")
    else:
        print("\n📊 No SMC setups found.")
    
    # Send Telegram alert
    print("\n📨 Sending Telegram alert...")
    message = format_alert_message(results, trade_date)
    send_telegram_message(message)
    
    print(f"\n✅ Alert sent at {datetime.now().strftime('%H:%M:%S')}")
    print("=" * 60)
    
    return results

if __name__ == "__main__":
    main()