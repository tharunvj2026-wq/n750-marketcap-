# ============================================
# CELL 3: COIL-ANALYSIS FROM CACHED DATABASE
# ============================================

import numpy as np
import pandas as pd
import os
from scipy import stats
import warnings
warnings.filterwarnings('ignore')

from cell1_marketcap import SYMBOL_TO_SEGMENT

# ============================================
# CONFIGURATION
# ============================================

PRICE_LIMITS = {
    'LARGE': 4,
    'MID': 6,
    'SMALL': 7,
    'MICRO': 8
}

STEALTH_PARAMS = {
    'delivery_delta_min': 1.00,
    'delivery_5d_min': 50,
}

COMPRESSION_PARAMS = {
    'bb_percentile_max': 40,
    'atr_contraction_max': 0.85,
    'inside_day_streak_min': 2,
}

# ============================================
# ANALYSIS FUNCTIONS
# ============================================

def calculate_delivery_delta(stock_df):
    if len(stock_df) < 40:
        return 0, 0, 0
    
    delivery_5d = stock_df['DELIVERY'].iloc[-5:].mean()
    delivery_20d = stock_df['DELIVERY'].iloc[-20:].mean()
    volume_5d = stock_df['VOLUME'].iloc[-5:].mean()
    volume_20d = stock_df['VOLUME'].iloc[-20:].mean()
    
    if delivery_20d == 0 or volume_20d == 0:
        return 0, delivery_5d, delivery_20d
    
    delivery_delta = (delivery_5d / delivery_20d) / (volume_5d / volume_20d)
    return delivery_delta, delivery_5d, delivery_20d

def calculate_down_day_absorption(stock_df):
    down_days = stock_df[stock_df['CLOSE'] < stock_df['CLOSE'].shift(1)]
    up_days = stock_df[stock_df['CLOSE'] > stock_df['CLOSE'].shift(1)]
    
    down_delivery = down_days['DELIVERY'].mean() if len(down_days) > 0 else 0
    up_delivery = up_days['DELIVERY'].mean() if len(up_days) > 0 else 0
    
    absorption = down_delivery > up_delivery
    return absorption, down_delivery, up_delivery

def calculate_price_change(stock_df, segment):
    if len(stock_df) < 21:
        return 0, False
    
    price_20d_ago = stock_df['CLOSE'].iloc[-21]
    current_price = stock_df['CLOSE'].iloc[-1]
    price_change = ((current_price - price_20d_ago) / price_20d_ago) * 100
    
    price_limit = PRICE_LIMITS.get(segment, 5)
    is_valid = abs(price_change) <= price_limit
    
    return price_change, is_valid

def calculate_vwap_compression(stock_df):
    vwap_proxy = (stock_df['HIGH'] + stock_df['LOW'] + stock_df['CLOSE']) / 3
    current_vwap = vwap_proxy.iloc[-1]
    current_close = stock_df['CLOSE'].iloc[-1]
    
    distance = abs(current_close - current_vwap) / current_vwap
    avg_distance = ((vwap_proxy - stock_df['CLOSE']).abs() / vwap_proxy).iloc[-20:].mean()
    
    if avg_distance == 0:
        compression = 0
    else:
        compression = 1 - min(1, distance / avg_distance)
    
    return compression

def calculate_moc_ratio(stock_df):
    high_low_range = stock_df['HIGH'] - stock_df['LOW']
    close_position = (stock_df['CLOSE'] - stock_df['LOW']) / high_low_range.where(high_low_range > 0, 1)
    
    current_position = close_position.iloc[-1]
    avg_position = close_position.iloc[-20:].mean()
    
    if avg_position == 0:
        moc_ratio = 1
    else:
        moc_ratio = current_position / avg_position
    
    volume_factor = stock_df['VOLUME'].iloc[-1] / stock_df['VOLUME'].iloc[-20:].mean()
    moc_score = moc_ratio * min(2, volume_factor)
    
    return moc_score, current_position

def calculate_tick_imbalance(stock_df):
    up_volume = stock_df[stock_df['CLOSE'] > stock_df['CLOSE'].shift(1)]['VOLUME'].sum()
    down_volume = stock_df[stock_df['CLOSE'] < stock_df['CLOSE'].shift(1)]['VOLUME'].sum()
    total_volume = up_volume + down_volume
    
    if total_volume == 0:
        return 0
    
    imbalance = (up_volume - down_volume) / total_volume
    return imbalance

def calculate_bb_percentile(stock_df):
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

def calculate_inside_day_streak(stock_df):
    streak = 0
    for i in range(1, min(10, len(stock_df))):
        if (stock_df['HIGH'].iloc[-i] <= stock_df['HIGH'].iloc[-i-1] and 
            stock_df['LOW'].iloc[-i] >= stock_df['LOW'].iloc[-i-1]):
            streak += 1
        else:
            break
    return streak

def calculate_price_range_5d(stock_df):
    if len(stock_df) < 5:
        return 100
    
    high_5d = stock_df['HIGH'].iloc[-5:].max()
    low_5d = stock_df['LOW'].iloc[-5:].min()
    current_price = stock_df['CLOSE'].iloc[-1]
    
    price_range = (high_5d - low_5d) / current_price * 100
    return price_range

def analyze_symbol(stock_df, symbol, segment):
    if stock_df is None or len(stock_df) < 50:
        return None
    
    delivery_delta, d5, d20 = calculate_delivery_delta(stock_df)
    absorption, down_del, up_del = calculate_down_day_absorption(stock_df)
    price_change, price_valid = calculate_price_change(stock_df, segment)
    
    if not price_valid or delivery_delta < 0.5:
        return None
    
    if delivery_delta > 1.5:
        delta_score = 100
    elif delivery_delta > 1.1:
        delta_score = 50 + (delivery_delta - 1.1) / 0.4 * 50
    else:
        delta_score = 0
    
    absorption_score = 30 if absorption else 0
    level_score = 30 if d5 > 65 else (15 if d5 > 50 else 0)
    stealth_score = min(100, (delta_score * 0.5) + absorption_score + (level_score * 0.2))
    
    vwap_comp = calculate_vwap_compression(stock_df)
    moc_ratio, moc_position = calculate_moc_ratio(stock_df)
    tick_imb = calculate_tick_imbalance(stock_df)
    
    vwap_score = min(100, vwap_comp * 100)
    moc_score = min(100, (moc_ratio - 0.8) / 1.2 * 100) if moc_ratio > 0.8 else 0
    tick_score = max(0, min(100, tick_imb * 500)) if tick_imb > 0 else 0
    oft_score = (vwap_score * 0.35) + (moc_score * 0.30) + (tick_score * 0.35)
    
    bb_percentile, bb_width = calculate_bb_percentile(stock_df)
    atr_contraction, atr_5d, atr_20d = calculate_atr_contraction(stock_df)
    inside_streak = calculate_inside_day_streak(stock_df)
    price_range = calculate_price_range_5d(stock_df)
    
    if bb_percentile < 10:
        bb_score = 40
    elif bb_percentile < 20:
        bb_score = 30
    elif bb_percentile < 30:
        bb_score = 20
    else:
        bb_score = 0
    
    if atr_contraction < 0.6:
        atr_score = 35
    elif atr_contraction < 0.75:
        atr_score = 25
    elif atr_contraction < 0.85:
        atr_score = 15
    else:
        atr_score = 0
    
    if inside_streak >= 5:
        inside_score = 25
    elif inside_streak >= 3:
        inside_score = 15
    elif inside_streak >= 2:
        inside_score = 8
    else:
        inside_score = 0
    
    compression_score = min(100, bb_score + atr_score + inside_score)
    coil_score = (stealth_score * 0.35) + (oft_score * 0.35) + (compression_score * 0.30)
    
    tier1 = (delivery_delta > STEALTH_PARAMS['delivery_delta_min'] and
             oft_score > 65 and compression_score > 65 and
             atr_contraction < COMPRESSION_PARAMS['atr_contraction_max'] and
             bb_percentile < COMPRESSION_PARAMS['bb_percentile_max'])
    
    high_5d = stock_df['HIGH'].iloc[-5:].max()
    low_5d = stock_df['LOW'].iloc[-5:].min()
    trigger_level = high_5d * 1.01
    invalidation_level = low_5d * 0.99
    
    return {
        'symbol': symbol,
        'segment': segment,
        'coil_score': round(coil_score, 2),
        'stealth_score': round(stealth_score, 2),
        'oft_score': round(oft_score, 2),
        'compression_score': round(compression_score, 2),
        'delivery_delta': round(delivery_delta, 2),
        'delivery_5d': round(d5, 1),
        'vwap_compression': round(vwap_comp, 2),
        'moc_ratio': round(moc_ratio, 2),
        'tick_imbalance': round(tick_imb, 2),
        'bb_percentile': round(bb_percentile, 1),
        'atr_contraction': round(atr_contraction, 2),
        'inside_streak': inside_streak,
        'price_range_5d': round(price_range, 1),
        'price_change_pct': round(price_change, 1),
        'current_price': round(stock_df['CLOSE'].iloc[-1], 2),
        'trigger_level': round(trigger_level, 2),
        'invalidation_level': round(invalidation_level, 2),
        'tier1': tier1
    }

def run_analysis(cache_file="nifty750_bhavcopy_cache.csv"):
    if not os.path.exists(cache_file):
        print(f"❌ Cache file {cache_file} not found. Run cell2_build_db.py first.")
        return None
    
    df = pd.read_csv(cache_file, parse_dates=['DATE'])
    print(f"✅ Loaded database: {len(df):,} rows")
    print(f"   Date range: {df['DATE'].min().date()} to {df['DATE'].max().date()}")
    
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
        results_df = results_df.sort_values('coil_score', ascending=False)
        results_df.to_csv('coil_analysis_results.csv', index=False)
        print(f"💾 Results saved to 'coil_analysis_results.csv'")
        return results_df
    
    return None

if __name__ == "__main__":
    results = run_analysis()