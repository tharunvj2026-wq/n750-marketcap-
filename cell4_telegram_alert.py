# ============================================
# CELL 4: TELEGRAM ALERT FOR TOP SIGNALS
# ============================================

import os
import requests
import pandas as pd
from datetime import datetime

# ============================================
# CONFIGURATION - SET YOUR TELEGRAM CREDENTIALS
# ============================================

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
TELEGRAM_CHAT_IDS = os.environ.get("TELEGRAM_CHAT_IDS", "YOUR_CHAT_ID_HERE").split(',')

def send_telegram_message(message):
    """Send message to all configured Telegram chats"""
    if not TELEGRAM_BOT_TOKEN or TELEGRAM_BOT_TOKEN == "YOUR_BOT_TOKEN_HERE":
        print("❌ Telegram bot token not configured")
        return False
    
    success_count = 0
    for chat_id in TELEGRAM_CHAT_IDS:
        if not chat_id or chat_id == "YOUR_CHAT_ID_HERE":
            continue
        
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
            payload = {
                'chat_id': chat_id.strip(),
                'text': message,
                'parse_mode': 'HTML'
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

def format_alert_message(results_df, trade_date):
    """Format the alert message for Telegram"""
    
    tier1 = results_df[results_df['tier1'] == True].head(5)
    tier2 = results_df[(results_df['coil_score'] >= 70) & (results_df['tier1'] == False)].head(10)
    
    message = f"""
🚀 <b>COIL-ANOMALY v3.0 - DAILY SCAN</b>
📅 {trade_date.strftime('%d-%b-%Y')}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
    
    if len(tier1) > 0:
        message += f"\n🔥 <b>TIER 1 SIGNALS (CEPS ≥ 85)</b>\n"
        for _, row in tier1.iterrows():
            message += f"\n<b>{row['symbol']}</b> | {row['segment']}\n"
            message += f"   CEPS: {row['coil_score']:.0f} | LTP: ₹{row['current_price']:.0f}\n"
            message += f"   Delivery Delta: {row['delivery_delta']:.2f}x\n"
            message += f"   BB %ile: {row['bb_percentile']:.0f}th | ATR: {row['atr_contraction']:.2f}\n"
            message += f"   Trigger: ₹{row['trigger_level']:.0f} | Invalidate: ₹{row['invalidation_level']:.0f}\n"
    else:
        message += f"\n⚠️ No Tier 1 signals found\n"
    
    if len(tier2) > 0:
        message += f"\n📊 <b>TIER 2 SIGNALS (CEPS 70-84)</b>\n"
        for _, row in tier2.iterrows():
            message += f"\n<b>{row['symbol']}</b> | {row['segment']}\n"
            message += f"   CEPS: {row['coil_score']:.0f} | LTP: ₹{row['current_price']:.0f}\n"
            message += f"   Delivery Delta: {row['delivery_delta']:.2f}x | BB: {row['bb_percentile']:.0f}th\n"
    
    # Top 5 by delivery delta (stealth accumulation)
    top_delivery = results_df.nlargest(5, 'delivery_delta')
    message += f"\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    message += f"<b>🔥 STEALTH ACCUMULATION LEADERS</b>\n"
    for _, row in top_delivery.iterrows():
        message += f"   {row['symbol']}: {row['delivery_delta']:.2f}x | {row['price_change_pct']:+.1f}%\n"
    
    message += f"\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    message += f"📊 Total signals: {len(results_df)}\n"
    message += f"<i>Invalidate on close below given levels</i>"
    
    return message

def send_daily_alert():
    """Main function to send daily alert"""
    
    # Load analysis results
    if not os.path.exists('coil_analysis_results.csv'):
        print("❌ No analysis results found. Run cell3_coil_analysis.py first.")
        return
    
    results_df = pd.read_csv('coil_analysis_results.csv')
    trade_date = datetime.now().date()
    
    # Format and send message
    message = format_alert_message(results_df, trade_date)
    send_telegram_message(message)
    
    print(f"\n✅ Daily alert sent at {datetime.now().strftime('%H:%M:%S')}")

if __name__ == "__main__":
    send_daily_alert()