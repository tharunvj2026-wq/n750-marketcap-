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
        print("   Add it to GitHub Secrets: Settings → Secrets → Actions")
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
    top_delivery = results_df.nlargest(5, 'delivery_delta')
    
    message = f"""
🚀 COIL-ANOMALY | {trade_date.strftime('%d-%b-%Y')}
━━━━━━━━━━━━━━━━━━━━━━━━━
"""
    
    if len(tier1) > 0:
        message += f"\n🔥 TIER 1 SIGNALS\n"
        for _, row in tier1.iterrows():
            atr_pct = (1 - row['atr_contraction']) * 100
            message += f"\n{row['symbol']} - ₹{row['current_price']:.0f}-{row['segment']}CAP"
            message += f"\n   Delivery Delta: {row['delivery_delta']:.2f}x"
            
            if row['bb_percentile'] < 10:
                bb_text = f"{row['bb_percentile']:.0f}th %ile (Extreme compression)"
            elif row['bb_percentile'] < 25:
                bb_text = f"{row['bb_percentile']:.0f}th %ile (Compressed)"
            else:
                bb_text = f"{row['bb_percentile']:.0f}th %ile"
            
            message += f"\n   BB Width: {bb_text}"
            message += f"\n   ATR: {row['atr_contraction']:.2f} (Contracted {atr_pct:.0f}%)"
            message += f"\n   20D Chg: {row['price_change_pct']:+.1f}%"
            message += f"\n   Trigger: ₹{row['trigger_level']:.0f} | Invalidate: ₹{row['invalidation_level']:.0f}"
    
    message += f"\n\n🔥 STEALTH ACCUMULATION LEADERS\n"
    
    for _, row in top_delivery.iterrows():
        atr_pct = (1 - row['atr_contraction']) * 100
        message += f"\n{row['symbol']} - ₹{row['current_price']:.0f}-{row['segment']}CAP"
        message += f"\n   Delivery Delta: {row['delivery_delta']:.2f}x"
        
        if row['bb_percentile'] < 10:
            bb_text = f"{row['bb_percentile']:.0f}th %ile (Extreme compression)"
        elif row['bb_percentile'] < 25:
            bb_text = f"{row['bb_percentile']:.0f}th %ile (Compressed)"
        else:
            bb_text = f"{row['bb_percentile']:.0f}th %ile"
        
        message += f"\n   BB Width: {bb_text}"
        message += f"\n   ATR: {row['atr_contraction']:.2f} (Contracted {atr_pct:.0f}%)"
        message += f"\n   20D Chg: {row['price_change_pct']:+.1f}%"
        message += f"\n   Trigger: ₹{row['trigger_level']:.0f} | Invalidate: ₹{row['invalidation_level']:.0f}"
    
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