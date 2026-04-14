# ============================================
# CELL 3+4 MERGED: COIL ANALYSIS + TELEGRAM ALERT (FIXED)
# Run this after cell2_build_db.py
# ============================================

import os
import numpy as np
import pandas as pd
import warnings
from datetime import datetime
import requests
from scipy import stats

warnings.filterwarnings('ignore')

from cell1_marketcap import SYMBOL_TO_SEGMENT

# ============================================
# CONFIGURATION - HIGH THRESHOLDS (REAL ACCUMULATION)
# ============================================

PRICE_LIMITS = {
    'LARGE': 4,
    'MID': 6,
    'SMALL': 7,
    'MICRO': 8
}

ACCUMULATION_THRESHOLDS = {
    'delivery_5d_min': 50,        # Delivery must be >50%
    'volume_spike_min': 1.3,      # Volume must be rising >1.3x
    'delivery_delta_min': 1.2,    # Delivery rising faster than volume
    'price_range_5d_max': 5,      # Price range <5% (coiled)
    'atr_contraction_max': 0.78   # ATR contracted >22%
}

# TELEGRAM CONFIGURATION
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_IDS = os.environ.get("TELEGRAM_CHAT_IDS", "")

# ============================================
# ANALYSIS FUNCTIONS
# ============================================

def calculate_delivery_delta(stock_df):
    """CORRECTED: Delivery Delta = (5d Del / 20d Del) / (5d Vol / 20d Vol)
    Only valid when volume is NOT falling"""
    if len(stock_df) < 40:
        return 0, 0, 0, 0, 0
    
    delivery_5d = stock_df['DELIVERY'].iloc[-5:].mean()
    delivery_20d = stock_df['DELIVERY'].iloc[-20:].mean()
    volume_5d = stock_df['VOLUME'].iloc[-5:].mean()
    volume_20d = stock_df['VOLUME'].iloc[-20:].mean()
    
    if delivery_20d == 0 or volume_20d == 0:
        return 0, delivery_5d, delivery_20d, volume_5d, volume_20d
    
    volume_ratio = volume_5d / volume_20d
    delivery_ratio = delivery_5d / delivery_20d
    
    # If volume is falling, delivery delta is invalid
    if volume_ratio < 0.8:
        return 0, delivery_5d, delivery_20d, volume_5d, volume_20d
    
    delivery_delta = delivery_ratio / volume_ratio
    return delivery_delta, delivery_5d, delivery_20d, volume_5d, volume_20d

def calculate_volume_spike(stock_df):
    """Volume spike: 5d avg volume vs 20d avg volume"""
    if len(stock_df) < 20:
        return 1.0
    volume_5d = stock_df['VOLUME'].iloc[-5:].mean()
    volume_20d = stock_df['VOLUME'].iloc[-20:].mean()
    return volume_5d / volume_20d if volume_20d > 0 else 1.0

def calculate_price_change(stock_df, segment):
    """20-day price change with segment limit"""
    if len(stock_df) < 21:
        return 0, False
    
    price_20d_ago = stock_df['CLOSE'].iloc[-21]
    current_price = stock_df['CLOSE'].iloc[-1]
    price_change = ((current_price - price_20d_ago) / price_20d_ago) * 100
    
    price_limit = PRICE_LIMITS.get(segment, 5)
    is_valid = abs(price_change) <= price_limit
    
    return price_change, is_valid

def calculate_price_range_5d(stock_df):
    """5-day price range as percentage"""
    if len(stock_df) < 5:
        return 100
    
    high_5d = stock_df['HIGH'].iloc[-5:].max()
    low_5d = stock_df['LOW'].iloc[-5:].min()
    current_price = stock_df['CLOSE'].iloc[-1]
    
    price_range = (high_5d - low_5d) / current_price * 100
    return price_range

def calculate_bb_percentile(stock_df):
    """Bollinger Band Width Percentile using available data"""
    if len(stock_df) < 50:
        return 100, 0
    
    sma = stock_df['CLOSE'].rolling(20).mean()
    std = stock_df['CLOSE'].rolling(20).std()
    bb_upper = sma + (std * 2)
    bb_lower = sma - (std * 2)
    bb_width = (bb_upper - bb_lower) / sma
    
    current_width = bb_width.iloc[-1]
    
    lookback = min(90, len(stock_df) - 20)
    if lookback < 30:
        return 50, current_width
    
    historical_widths = bb_width.iloc[-lookback:-1].dropna()
    
    if len(historical_widths) < 20:
        return 50, current_width
    
    percentile = stats.percentileofscore(historical_widths, current_width)
    return percentile, current_width

def calculate_atr_contraction(stock_df):
    """ATR Contraction Ratio: 5-day ATR / 20-day ATR"""
    if len(stock_df) < 25:
        return 1, 0, 0
    
    high_low = stock_df['HIGH'] - stock_df['LOW']
    high_close = abs(stock_df['HIGH'] - stock_df['CLOSE'].shift(1))
    low_close = abs(stock_df['LOW'] - stock_df['CLOSE'].shift(1))
    
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    atr_5d = tr.rolling(5).mean().iloc[-1]
    atr_20d = tr.rolling(20).mean().iloc[-1]
    
    contraction = atr_5d / atr_20d if atr_20d > 0 else 1
    return contraction, atr_5d, atr_20d

def analyze_symbol(stock_df, symbol, segment):
    """Complete analysis - ONLY returns TRUE accumulation stocks"""
    
    if stock_df is None or len(stock_df) < 50:
        return None
    
    # Calculate all metrics
    delivery_delta, d5, d20, v5, v20 = calculate_delivery_delta(stock_df)
    volume_spike = calculate_volume_spike(stock_df)
    price_change, price_valid = calculate_price_change(stock_df, segment)
    price_range = calculate_price_range_5d(stock_df)
    bb_percentile, bb_width = calculate_bb_percentile(stock_df)
    atr_contraction, atr_5d, atr_20d = calculate_atr_contraction(stock_df)
    
    # ============================================
    # HARD FILTERS - TRUE ACCUMULATION ONLY
    # ============================================
    
    # Filter 1: Price must be in range
    if not price_valid:
        return None
    
    # Filter 2: Delivery must be >50% (institutional)
    if d5 < ACCUMULATION_THRESHOLDS['delivery_5d_min']:
        return None
    
    # Filter 3: Volume must be rising (>1.3x)
    if volume_spike < ACCUMULATION_THRESHOLDS['volume_spike_min']:
        return None
    
    # Filter 4: Delivery Delta must show accumulation
    if delivery_delta < ACCUMULATION_THRESHOLDS['delivery_delta_min']:
        return None
    
    # Filter 5: Price range must be tight (<5%)
    if price_range > ACCUMULATION_THRESHOLDS['price_range_5d_max']:
        return None
    
    # Filter 6: ATR must be contracting
    if atr_contraction > ACCUMULATION_THRESHOLDS['atr_contraction_max']:
        return None
    
    # Calculate score (only for ranking)
    # Higher delivery delta = stronger accumulation
    coil_score = delivery_delta * 10
    
    high_5d = stock_df['HIGH'].iloc[-5:].max()
    low_5d = stock_df['LOW'].iloc[-5:].min()
    trigger_level = high_5d * 1.01
    invalidation_level = low_5d * 0.99
    
    return {
        'symbol': symbol,
        'segment': segment,
        'coil_score': round(coil_score, 2),
        'delivery_delta': round(delivery_delta, 2),
        'delivery_5d': round(d5, 1),
        'delivery_20d': round(d20, 1),
        'volume_spike': round(volume_spike, 2),
        'volume_5d': int(v5),
        'volume_20d': int(v20),
        'bb_percentile': round(bb_percentile, 1),
        'atr_contraction': round(atr_contraction, 2),
        'price_range_5d': round(price_range, 1),
        'price_change_pct': round(price_change, 1),
        'current_price': round(stock_df['CLOSE'].iloc[-1], 2),
        'trigger_level': round(trigger_level, 2),
        'invalidation_level': round(invalidation_level, 2)
    }

# ============================================
# TELEGRAM FUNCTIONS
# ============================================

def send_telegram_message(message):
    """Send message to all configured Telegram chats"""
    if not TELEGRAM_BOT_TOKEN:
        print("❌ TELEGRAM_BOT_TOKEN not configured")
        return False
    
    if not TELEGRAM_CHAT_IDS:
        print("❌ TELEGRAM_CHAT_IDS not configured")
        return False
    
    chat_ids = [cid.strip() for cid in TELEGRAM_CHAT_IDS.split(',') if cid.strip()]
    
    success_count = 0
    for chat_id in chat_ids:
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
            payload = {
                'chat_id': chat_id,
                'text': message,
                'parse_mode': 'Markdown'
            }
            response = requests.post(url, json=payload, timeout=30)
            if response.status_code == 200:
                success_count += 1
                print(f"✅ Message sent to {chat_id}")
        except Exception as e:
            print(f"❌ Error sending to {chat_id}: {e}")
    
    return success_count > 0

def format_alert_message(results_df, trade_date):
    """Format the alert message for Telegram"""
    
    if len(results_df) == 0:
        return f"""🚀 COIL-ANOMALY | {trade_date.strftime('%d-%b-%Y')}
━━━━━━━━━━━━━━━━━━━━━━━━━

⚠️ NO ACCUMULATION SIGNALS

Requirements not met:
• Delivery > 50%
• Volume Spike > 1.3x
• Price Range < 5%
• ATR Contraction < 0.78

Waiting for institutional participation."""
    
    top_stocks = results_df.head(5)
    
    message = f"""🚀 COIL-ANOMALY | {trade_date.strftime('%d-%b-%Y')}
━━━━━━━━━━━━━━━━━━━━━━━━━

🔥 TRUE ACCUMULATION SIGNALS

"""
    
    for idx, (_, row) in enumerate(top_stocks.iterrows()):
        atr_pct = (1 - row['atr_contraction']) * 100
        
        if row['bb_percentile'] < 10:
            bb_text = f"{row['bb_percentile']:.0f}th %ile (Extreme compression)"
        elif row['bb_percentile'] < 25:
            bb_text = f"{row['bb_percentile']:.0f}th %ile (Compressed)"
        else:
            bb_text = f"{row['bb_percentile']:.0f}th %ile"
        
        if idx == 0:
            # First stock - full details
            message += f"""*{row['symbol']}* - ₹{row['current_price']:.0f} - {row['segment']}CAP
   Delivery Delta: {row['delivery_delta']:.2f}x
   Delivery%: {row['delivery_5d']:.0f}% (5D) | {row['delivery_20d']:.0f}% (20D)
   Volume Spike: {row['volume_spike']:.1f}x (5D vs 20D)
   Price Range: {row['price_range_5d']:.1f}% (5D)
   BB Width: {bb_text}
   ATR: {row['atr_contraction']:.2f} (Contracted {atr_pct:.0f}%)
   20D Chg: {row['price_change_pct']:+.1f}%

"""
        else:
            # Remaining stocks - compact format
            message += f"""*{row['symbol']}* - ₹{row['current_price']:.0f} - {row['segment']}CAP
   DD:{row['delivery_delta']:.2f}x | Del:{row['delivery_5d']:.0f}/{row['delivery_20d']:.0f}% | Vol:{row['volume_spike']:.1f}x | Range:{row['price_range_5d']:.1f}% | ATR:{row['atr_contraction']:.2f} | Chg:{row['price_change_pct']:+.1f}%

"""
    
    return message

# ============================================
# MAIN EXECUTION
# ============================================

def main():
    cache_file = "nifty750_bhavcopy_cache.csv"
    
    if not os.path.exists(cache_file):
        print("❌ Cache file not found. Run cell2_build_db.py first.")
        return
    
    df = pd.read_csv(cache_file, parse_dates=['DATE'])
    print(f"✅ Loaded database: {len(df):,} rows")
    print(f"   Date range: {df['DATE'].min().date()} to {df['DATE'].max().date()}")
    
    print(f"\n📊 FILTERS (TRUE ACCUMULATION):")
    print(f"   • Delivery 5D avg > {ACCUMULATION_THRESHOLDS['delivery_5d_min']}%")
    print(f"   • Volume Spike > {ACCUMULATION_THRESHOLDS['volume_spike_min']}x")
    print(f"   • Delivery Delta > {ACCUMULATION_THRESHOLDS['delivery_delta_min']}x")
    print(f"   • 5D Price Range < {ACCUMULATION_THRESHOLDS['price_range_5d_max']}%")
    print(f"   • ATR Contraction < {ACCUMULATION_THRESHOLDS['atr_contraction_max']}")
    
    symbols = df['SYMBOL'].unique()
    print(f"\n📊 Analyzing {len(symbols)} stocks...")
    
    results = []
    for i, symbol in enumerate(symbols):
        print(f"\r   Progress: {i+1}/{len(symbols)} - {symbol:<15} - Found: {len(results)}", end="")
        
        stock_df = df[df['SYMBOL'] == symbol].copy().sort_values('DATE')
        segment = SYMBOL_TO_SEGMENT.get(symbol, 'MID')
        
        result = analyze_symbol(stock_df, symbol, segment)
        if result:
            results.append(result)
    
    print(f"\n\n✅ Analysis complete. Results: {len(results)} stocks")
    
    if len(results) > 0:
        results_df = pd.DataFrame(results)
        results_df = results_df.sort_values('delivery_delta', ascending=False)
        results_df.to_csv('coil_analysis_results.csv', index=False)
        print(f"💾 Results saved to 'coil_analysis_results.csv'")
    else:
        print("💾 No results to save")
        results_df = pd.DataFrame()
    
    # Send Telegram Alert
    trade_date = datetime.now().date()
    message = format_alert_message(results_df, trade_date)
    send_telegram_message(message)
    print(f"\n✅ Alert sent at {datetime.now().strftime('%H:%M:%S')}")

if __name__ == "__main__":
    main()