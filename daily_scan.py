# ============================================
# COMPLETE DAILY SCAN - RUN ALL ANALYSES
# ============================================

import os
import time
from datetime import datetime

def run_analysis(module_name):
    """Run a single analysis module"""
    print(f"\n{'='*60}")
    print(f"Running: {module_name}")
    print('='*60)
    
    try:
        if module_name == "cell2_build_db":
            from cell2_build_db import build_database_cache
            build_database_cache()
        elif module_name == "cell3_coil_analysis":
            from cell3_coil_analysis import main
            main()
        elif module_name == "cell4_sweep_daily":
            from cell4_sweep_daily import main
            main()
        elif module_name == "cell5_sweep_weekly":
            from cell5_sweep_weekly import main
            main()
        elif module_name == "cell6_smc_daily":
            from cell6_smc_daily import main
            main()
        print(f"✅ {module_name} completed")
        return True
    except Exception as e:
        print(f"❌ {module_name} failed: {e}")
        return False

def main():
    print("="*60)
    print(f"DAILY MARKET SCAN - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    # Weekend check
    if datetime.now().weekday() >= 5:
        print("Weekend - skipping scan")
        return
    
    # Run all analyses in order
    analyses = [
        "cell2_build_db",
        "cell3_coil_analysis", 
        "cell4_sweep_daily",
        "cell5_sweep_weekly",
        "cell6_smc_daily"
    ]
    
    for analysis in analyses:
        run_analysis(analysis)
    
    print("\n" + "="*60)
    print("SCAN COMPLETE")
    print("="*60)

if __name__ == "__main__":
    main()