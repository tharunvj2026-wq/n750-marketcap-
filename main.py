# ============================================
# MAIN ENTRY POINT - RUN FULL PIPELINE (FIXED)
# ============================================

import os
import sys
from datetime import datetime
import pandas as pd

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
    
    # Step 2: Run Analysis (cell3 already sends Telegram alert)
    print("\n[STEP 2] Running COIL analysis & sending alert...")
    from cell3_coil_analysis import main as run_coil_analysis
    run_coil_analysis()   # This sends Telegram and saves CSV
    
    print("\n" + "=" * 60)
    print("✅ Pipeline complete")
    print("=" * 60)

if __name__ == "__main__":
    main()