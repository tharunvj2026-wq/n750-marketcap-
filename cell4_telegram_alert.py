# ============================================
# CELL 4: TELEGRAM ALERT FOR TOP SIGNALS
# ============================================

import os
import requests
import pandas as pd
from datetime import datetime

# ============================================
# CONFIGURATION - READ FROM GITHUB SECRETS
# ============================================

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_IDS = os.environ.get("TELEGRAM_CHAT_IDS", "")

def send_telegram_message(message):
    """Send message to all configured Telegram chats"""
    if not TELEGRAM_BOT_TOKEN:
        print("❌ TELEGRAM_BOT_TOKEN not configured")
        return False
    
    if not TELEGRAM_CHAT_IDS:
        print("❌ TELEGRAM_CHAT_IDS not configured")
        return False
    
    chat_ids = [cid.strip() for cid in TELEGRAM_CHAT_IDS.split(',') if cid.strip()]
    
    if not chat_ids:
        print("❌ No valid chat IDs found")
        return False
    
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
            else:
                print(f"❌ Failed to send to {chat_id}: {response.text}")
        except Exception as e:
            print(f"❌ Error sending to {chat_id}: {e}")
    
    return success_count > 0

def calculate_volume_spike(stock_df):
    """Calculate volume spike ratio (latest volume vs 20d avg)"""
    if len(stock_df) < 20:
        return 1.0
    latest_vol = stock_df['VOLUME'].iloc[-1]
    avg_vol_20d = stock_df['VOLUME'].iloc[-20:].mean()
    return latest_vol / avg_vol_20d if avg_vol_20d > 0 else 1.0

def format_alert_message(results_df, trade_date, df):
    """Format the alert message for Telegram"""
    
    top_delivery = results_df.nlargest(5, 'delivery_delta')
    
    message = f"""🚀 COIL-ANOMALY | {trade_date.strftime('%d-%b-%Y')}
━━━━━━━━━━━━━━━━━━━━━━━━━

🔥 STEALTH ACCUMULATION LEADERS

"""
    
    for idx, (_, row) in enumerate(top_delivery.iterrows()):
        # Get stock data for volume spike
        stock_df = df[df['SYMBOL'] == row['symbol']].copy().sort_values('DATE')
        volume_spike = calculate_volume_spike(stock_df)
        
        delivery_5d = row['delivery_5d']
        delivery_20d = row.get('delivery_20d', delivery_5d * 0.7)  # Fallback
        
        if idx == 0:
            # First stock - full details
            atr_pct = (1 - row['atr_contraction']) * 100
            
            if row['bb_percentile'] < 10:
                bb_text = f"{row['bb_percentile']:.0f}th %ile (Extreme compression)"
            elif row['bb_percentile'] < 25:
                bb_text = f"{row['bb_percentile']:.0f}th %ile (Compressed)"
            else:
                bb_text = f"{row['bb_percentile']:.0f}th %ile"
            
            message += f"""*{row['symbol']}* - ₹{row['current_price']:.0f} - {row['segment']}CAP
   Delivery Delta: {row['delivery_delta']:.2f}x
   Delivery%: {delivery_5d:.0f}% (5D avg) | {delivery_20d:.0f}% (20D avg)
   Volume Spike: {volume_spike:.1f}x (Latest vs 20D avg)
   BB Width: {bb_text}
   ATR: {row['atr_contraction']:.2f} (Contracted {atr_pct:.0f}%)
   20D Chg: {row['price_change_pct']:+.1f}%

"""
        else:
            # Remaining stocks - compact format
            if row['bb_percentile'] < 10:
                bb_short = f"{row['bb_percentile']:.0f}th(E)"
            elif row['bb_percentile'] < 25:
                bb_short = f"{row['bb_percentile']:.0f}th(C)"
            else:
                bb_short = f"{row['bb_percentile']:.0f}th"
            
            message += f"""*{row['symbol']}* - ₹{row['current_price']:.0f} - {row['segment']}CAP
   DD:{row['delivery_delta']:.2f}x | Del:{delivery_5d:.0f}/{delivery_20d:.0f}% | Vol:{volume_spike:.1f}x | BB:{bb_short} | ATR:{row['atr_contraction']:.2f} | Chg:{row['price_change_pct']:+.1f}%

"""
    
    return message

def send_daily_alert(df):
    """Main function to send daily alert"""
    
    # Load analysis results
    if not os.path.exists('coil_analysis_results.csv'):
        print("❌ No analysis results found. Run cell3_coil_analysis.py first.")
        return
    
    results_df = pd.read_csv('coil_analysis_results.csv')
    trade_date = datetime.now().date()
    
    # Format and send message
    message = format_alert_message(results_df, trade_date, df)
    send_telegram_message(message)
    
    print(f"\n✅ Daily alert sent at {datetime.now().strftime('%H:%M:%S')}")

if __name__ == "__main__":
    # Load database for volume calculation
    if os.path.exists('nifty750_bhavcopy_cache.csv'):
        df = pd.read_csv('nifty750_bhavcopy_cache.csv', parse_dates=['DATE'])
        send_daily_alert(df)
    else:
        print("❌ Database not found. Run cell2_build_db.py first.")