# ============================================
# MAIN ENTRY POINT - RUN FULL PIPELINE
# ============================================

import os
import sys
from datetime import datetime

def is_trading_day():
    """Check if today is a trading day (Monday-Friday, not holiday)"""
    # Simple check - weekday
    today = datetime.now().weekday()
    return today < 5  # Monday=0, Friday=4

def main():
    print("=" * 60)
    print("COIL-ANOMALY v3.0 - NIFTY 750 ACCUMULATION SCREENER")
    print(f"Run time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    # Check if trading day
    if not is_trading_day():
        print("⏸️ Non-trading day. Exiting.")
        return
    
    # Step 1: Build/Update Database
    print("\n[STEP 1] Building database...")
    from cell2_build_db import build_database_cache
    df = build_database_cache()
    
    if df is None:
        print("❌ Database build failed")
        return
    
    # Step 2: Run Analysis
    print("\n[STEP 2] Running COIL analysis...")
    from cell3_coil_analysis import run_analysis
    results = run_analysis()
    
    if results is None or len(results) == 0:
        print("❌ Analysis failed or no results")
        return
    
    # Step 3: Send Telegram Alert
    print("\n[STEP 3] Sending Telegram alert...")
    from cell4_telegram_alert import send_daily_alert
    send_daily_alert()
    
    print("\n" + "=" * 60)
    print("✅ Pipeline complete")
    print("=" * 60)

if __name__ == "__main__":
    main()