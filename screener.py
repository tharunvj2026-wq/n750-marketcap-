"""
NIFTY 750 SMART MONEY SCREENER - STANDALONE (FIXED)
=====================================================
- Uses CSV cache instead of Parquet (no extra dependencies)
- Fetches last 55 days of data directly from NSE
- Saves cache locally for faster future runs
- 2-stage analysis: Accumulation (Days -55 to -8) + Pre-Breakout (Last 7 days)
"""

import os
import time
import requests
import pandas as pd
import numpy as np
import datetime as dt
from dateutil.relativedelta import relativedelta
import warnings
warnings.filterwarnings('ignore')

# ============================================
# TELEGRAM CONFIGURATION (from GitHub Secrets)
# ============================================

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
TELEGRAM_GROUP_ID = os.environ.get("TELEGRAM_GROUP_ID")

TELEGRAM_CHAT_IDS = []
if TELEGRAM_CHAT_ID:
    TELEGRAM_CHAT_IDS.append(TELEGRAM_CHAT_ID)
if TELEGRAM_GROUP_ID:
    TELEGRAM_CHAT_IDS.append(TELEGRAM_GROUP_ID)

# ============================================
# CONFIGURATION
# ============================================

CACHE_FILE = "nifty_55days_cache.csv"  # Changed to CSV (no extra deps)
TRADING_DAYS = 55
PRE_BREAKOUT_WINDOW = 7

# ============================================
# OPTIMAL PARAMETERS (2-Stage)
# ============================================

OPTIMAL_PARAMETERS = {
    'LARGE': {
        'price_limit': 5,
        'vol_surge': 1.3,
        'delivery_min': 60,
        'dryup_max': 0.65,
        'compression_max': 4.0,
        'near_high_min': 95.0,
        'expected_return': 5.15,
        'win_rate': 80.0
    },
    'MID': {
        'price_limit': 8,
        'vol_surge': 1.5,
        'delivery_min': 55,
        'dryup_max': 0.60,
        'compression_max': 5.0,
        'near_high_min': 94.0,
        'expected_return': 7.02,
        'win_rate': 90.9
    },
    'SMALL': {
        'price_limit': 10,
        'vol_surge': 1.8,
        'delivery_min': 50,
        'dryup_max': 0.55,
        'compression_max': 6.0,
        'near_high_min': 93.0,
        'expected_return': 8.46,
        'win_rate': 83.3
    },
    'MICRO': {
        'price_limit': 12,
        'vol_surge': 2.0,
        'delivery_min': 45,
        'dryup_max': 0.50,
        'compression_max': 7.0,
        'near_high_min': 92.0,
        'expected_return': 9.13,
        'win_rate': 88.9
    }
}

# ============================================
# NIFTY 750 SYMBOLS (Complete - Same as before)
# ============================================

LARGE_CAP_SYMBOLS = [
    'HDFCBANK', 'RELIANCE', 'ICICIBANK', 'TCS', 'INFY', 'AXISBANK', 'SBIN',
    'BHARTIARTL', 'LT', 'WIPRO', 'ETERNAL', 'ADANIPOWER', 'M&M', 'ADANIENSOL',
    'KOTAKBANK', 'BAJFINANCE', 'ADANIGREEN', 'BAJAJ-AUTO', 'ASIANPAINT', 'TMCV',
    'TITAN', 'LODHA', 'CUMMINSIND', 'MARUTI', 'HINDALCO', 'ADANIPORTS', 'INDIGO',
    'TATASTEEL', 'NTPC', 'ABB', 'ITC', 'HAL', 'EICHERMOT', 'TVSMOTOR', 'MAZDOCK',
    'HCLTECH', 'ULTRACEMCO', 'BEL', 'TRENT', 'HINDUNILVR', 'POWERGRID', 'MAXHEALTH',
    'ONGC', 'CHOLAFIN', 'TMPV', 'DLF', 'ADANIENT', 'APOLLOHOSP', 'BOSCHLTD',
    'MOTHERSON', 'MUTHOOTFIN', 'SBILIFE', 'TORNTPHARM', 'SOLARINDS', 'TECHM',
    'CANBK', 'JIOFIN', 'BANKBARODA', 'HDFCLIFE', 'CIPLA', 'VBL', 'UNIONBANK',
    'TATAPOWER', 'HDFCAMC', 'DRREDDY', 'RECLTD', 'HINDZINC', 'PIDILITIND', 'GAIL',
    'SIEMENS', 'IRFC', 'JSWSTEEL', 'DIVISLAB', 'UNITDSPR', 'JINDALSTEL', 'IOC',
    'PNB', 'DMART', 'LTM', 'BRITANNIA', 'SHREECEM', 'AMBUJACEM', 'CGPOWER',
    'GODREJCP', 'INDHOTEL', 'HYUNDAI', 'ENRIN', 'ZYDUSLIFE', 'BAJAJHLDNG',
    'TATACAP', 'PFC', 'VEDL', 'NATIONALUM', 'PERSISTENT', 'COALINDIA', 'SUNPHARMA',
    'GRASIM', 'NESTLEIND', 'TATACONSUM', 'PAGEIND', 'MRF', 'COLPAL'
]

MID_CAP_SYMBOLS = [
    'BSE', 'GROWW', 'NIACL', 'DIXON', 'POWERINDIA', 'MCX', 'WAAREEENER', 'ASHOKLEY',
    'COFORGE', 'GVT&D', 'HEROMOTOCO', 'COCHINSHIP', 'ATGL', 'IDEA', 'BHEL', 'BPCL',
    'GODREJPROP', 'POLYCAB', 'NAUKRI', 'PRESTIGE', 'SUZLON', 'ICICIAMC', 'LUPIN',
    'PAYTM', 'GODFRYPHLP', 'BHARATFORG', 'FEDERALBNK', 'LENSKART', 'SAIL',
    'POLICYBZR', 'MPHASIS', 'HINDPETRO', 'RVNL', 'INDUSTOWER', 'LICI', 'NMDC',
    'INDIANB', 'LAURUSLABS', 'NAM-INDIA', 'APARINDS', 'KALYANKJIL', 'SRF', 'VOLTAS',
    'AUBANK', 'ASTRAL', 'GMRAIRPORT', 'MARICO', 'PHOENIXLTD', 'MFSL', 'BDL',
    'ABCAPITAL', 'LTF', 'YESBANK', 'JUBLFOOD', 'GLENMARK', 'BANKINDIA', 'UPL', 'KEI',
    'NHPC', 'PREMIERENE', 'EXIDEIND', 'IDFCFIRSTB', 'COROMANDEL', 'INDUSINDBK',
    'AUROPHARMA', 'MAHABANK', 'RADICO', 'IREDA', 'GICRE', 'OIL', 'OFSS', 'KPITTECH',
    'JSWENERGY', 'BLUESTARCO', 'OBEROIRLTY', 'PETRONET', 'VMM', 'DABUR', 'SUPREMEIND',
    'HAVELLS', 'TATAELXSI', 'THERMAX', 'NYKAA', 'M&MFIN', 'CONCOR', 'MOTILALOFS',
    'APLAPOLLO', 'ITCHOTELS', 'AWL', 'LGEINDIA', 'ICICIGI', 'BIOCON', 'SUNDARMFIN',
    'IRCTC', 'LTTS', 'BAJAJHFL', 'MANKIND', 'LLOYDSME', 'FORTIS', 'TIINDIA', 'JSL',
    'TATACOMM', 'SCHAEFFLER', 'KPRMILL', 'BALKRISIND', 'LICHSGFIN', 'PATANJALI',
    'ALKEM', 'JKCEMENT', 'DALBHARAT', 'SBICARD', 'ICICIPRULI', 'HUDCO', 'TORNTPOWER',
    'APOLLOTYRE', 'PIIND', 'ESCORTS', 'NLCINDIA', 'LINDEINDIA', 'ANTHEM', 'SJVN',
    'BERGEPAINT', 'AIAENG', 'CRISIL', 'ACC', 'HONAUT', 'ABBOTINDIA', 'UBL', 'HDBFS',
    'ENDURANCE', 'AJANTPHARM', 'GODREJIND', 'AIIL', 'MEDANTA', 'GLAXO', 'IPCALAB',
    'BHARTIHEXA', '3MINDIA', 'FLUOROCHEM'
]

SMALL_CAP_SYMBOLS = [
    'OLAELEC', 'COHANCE', 'ATHERENERG', 'GRSE', 'HFCL', 'KAYNES', 'FORCEMOT',
    'ANGELONE', 'OLECTRA', 'CDSL', 'AMBER', 'NETWEB', 'DATAPATTNS', 'SAMMAANCAP',
    'SONACOMS', 'HINDCOPPER', 'MEESHO', 'POONAWALLA', 'PGEL', 'TEJASNET', 'ITI',
    'ZEEL', 'CHENNPETRO', 'RBLBANK', 'JPPOWER', 'BELRISE', 'EMMVEE', 'RPOWER',
    'ABREL', 'IGL', 'ACE', 'JBMA', 'GALLANTT', 'NAVINFLUOR', 'GMDCLTD', 'IDBI',
    'DEVYANI', 'REDINGTON', 'HBLENGINE', 'KFINTECH', 'KIMS', 'ONESOURCE', 'TARIL',
    'HSCL', 'SCI', 'ABDL', 'TATACHEM', 'BLS', 'IRB', 'SYNGENE', 'BANDHANBNK',
    'KARURVYSYA', 'MANAPPURAM', 'FIRSTCRY', 'ANANTRAJ', 'ZENTEC', 'DELHIVERY',
    'TTML', 'SYRMA', 'SAPPHIRE', 'CONCORDBIO', 'NBCC', 'IFCI', 'MRPL', 'ENGINERSIN',
    'ABFRL', 'PINELABS', 'HONASA', 'NEULANDLAB', 'KIRLOSENG', 'NATCOPHARM',
    'MINDACORP', 'CROMPTON', 'ACUTAAS', 'KEC', 'APTUS', 'JWL', 'JBCHEPHARM',
    'INOXWIND', 'ABSLAMC', 'CHOICEIN', 'IRCON', 'WOCKPHARMA', 'SBFC', 'LEMONTREE',
    'RAILTEL', 'HEG', 'IIFL', 'CAMS', 'ECLERX', 'GPIL', 'CARTRADE', 'NCC', 'JKTYRE',
    'NEWGEN', 'PARADEEP', 'IEX', 'ARE&M', 'LALPATHLAB', 'MGL', 'WELCORP',
    'INDIAMART', 'BRIGADE', 'JYOTICNC', 'DEEPAKNTR', 'ELECON', 'TITAGARH',
    'JMFINANCIL', 'FIVESTAR', 'EMCURE', 'JINDALSAW', 'GRANULES', 'BSOFT',
    'PIRAMALFIN', 'IKS', 'DEEPAKFERT', 'JAINREC', 'NSLNISP', 'TATATECH', 'GESHIP',
    'GRAPHITE', 'J&KBANK', 'GRAVITA', 'CYIENT', 'VTL', 'CEATLTD', 'AAVAS',
    'PNBHOUSING', 'PWL', 'GABRIEL', 'MANYAVAR', 'CHOLAHLDNG', 'AFFLE', 'PPLPHARMA',
    'FACT', 'BEML', 'ELGIEQUIP', 'AARTIIND', 'BLUEJET', 'ASTERDM', 'CARBORUNIV',
    'HOMEFIRST', 'RAMCOCEM', 'ANURAS', 'SAILIFE', 'TEGA', 'KAJARIACER', 'NAVA',
    'SIGNATURE', 'SAREGAMA', 'NH', 'NUVAMA', 'MSUMI', 'PCBL', 'UTIAMC', 'TRITURBINE',
    'AADHARHFC', 'GILLETTE', 'BALRAMCHIN', 'CREDITACC', 'CEMPRO', 'SCHNEIDER',
    'EMAMILTD', 'LATENTVIEW', 'FINCABLES', 'CRAFTSMAN', 'CASTROLIND', 'ACMESOLAR',
    'CCL', 'STARHEALTH', 'KPIL', 'SWANCORP', 'EIHOTEL', 'CLEAN', 'SARDAEN',
    'TECHNOE', 'GSPL', 'LTFOODS', 'MMTC', 'THELEELA', 'SOBHA', 'AEGISVOPAK',
    'AKZOINDIA', 'PVRINOX', 'ZYDUSWELL', 'RRKABEL', 'ZENSARTECH', 'SHYAMMETL',
    'SUNTV', 'CENTRALBK', 'TENNIND', 'CESC', 'RITES', 'AFCONS', 'RAINBOW', 'CGCL',
    'JSWCEMENT', 'TRIDENT', 'UCOBANK', 'CAPLIPOINT', 'CHAMBLFERT', 'CPPLUS', 'BBTC',
    'ATUL', 'JUBLPHARMA', 'FSL', 'JUBLINGREA', 'POLYMED', 'IOB', 'INTELLECT',
    'TIMKEN', 'MAPMYINDIA', 'EIDPARRY', 'RKFORGE', 'USHAMART', 'WHIRLPOOL',
    'SONATSOFTW', 'IGIL', 'BATAINDIA', 'GLAND', 'BLUEDART', 'BAYERCROP', 'NUVOCO',
    'TBOTEK', 'CANFINHOME', 'WELSPUNLIV', 'ERIS', 'SPLPETRO', 'TRAVELFOOD', 'DOMS',
    'INDIACEM', 'NIVABUPA', 'PFIZER', 'GODIGIT', 'CHALET', 'SUMICHEM', 'ASAHIINDIA',
    'ABLBL', 'CANHLIFE', 'VIJAYA', 'RHIM', 'INDGN', 'DCMSHRIRAM', 'BIKAJI'
]

MICRO_CAP_SYMBOLS = [
    'AVANTIFEED', 'MTARTECH', 'TDPOWERSYS', 'STLTECH', 'SHRIPISTON', 'VIKRAMSOLR',
    'CUPID', 'AEQUS', 'LUMAXTECH', 'QPOWER', 'HCC', 'INOXINDIA', 'SAATVIKGL',
    'SANSERA', 'POWERMECH', 'PFOCUS', 'TRANSRAILL', 'SHAILY', 'VOLTAMP',
    'TIMETECHNO', 'PCJEWELLER', 'SHAKTIPUMP', 'UJJIVANSFB', 'KTKBANK', 'KRN',
    'THANGAMAYL', 'RUBICON', 'YATHARTH', 'JAYNECOIND', 'AETHER', 'AVALON', 'WABAG',
    'VMART', 'PARAS', 'SKYGOLD', 'ZAGGLE', 'SOUTHBANK', 'GOKEX', 'EDELWEISS',
    'AURIONPRO', 'ATLANTAELE', 'SENCO', 'JAMNAAUTO', 'HAPPSTMNDS', 'DCBBANK',
    'RAIN', 'BLACKBUCK', 'EPL', 'ELLEN', 'WAAREERTL', 'WEBELSOLAR', 'BBOX',
    'BORORENEW', 'KSCL', 'PNGJL', 'OSWALPUMPS', 'LLOYDSENGG', 'SHARDACROP',
    'SWSOLAR', 'SPARC', 'KNRCON', 'AZAD', 'SMLMAH', 'DBREALTY', 'KPIGREEN',
    'LLOYDSENT', 'ELECTCAST', 'PRAJIND', 'GPPL', 'PRIVISCL', 'RCF', 'IXIGO',
    'RTNINDIA', 'GOKULAGRO', 'TANLA', 'ASTRAMICRO', 'DIACABS', 'SANDUMA',
    'AXISCADES', 'PARKHOSPS', 'BANCOINDIA', 'MOIL', 'STAR', 'MIDHANI', 'LOTUSDEV',
    'V2RETAIL', 'RTNPOWER', 'IMFA', 'PURVA', 'MANORAMA', 'THOMASCOOK', 'RENUKA',
    'INOXGREEN', 'BLUESTONE', 'HGINFRA', 'DYNAMATECH', 'TI', 'AVL', 'IIFLCAPS',
    'KSB', 'KRBL', 'BALUFORGE', 'GSFC', 'CRAMC', 'HERITGFOOD', 'JAIBALAJI', 'NFL',
    'NAZARA', 'VIYASH', 'ASHAPURMIN', 'ASHOKA', 'KITEX', 'RELIGARE', 'EIEL',
    'NETWORK18', 'KANSAINER', 'SHILPAMED', 'ATUL', 'CCAVENUE', 'GREAVESCOT',
    'PRUDENT', 'EQUITASBNK', 'JSLL', 'VGUARD', 'SKFINDIA', 'MAHSEAMLES', 'AKUMS',
    'GAEL', 'TARC', 'MEDPLUS', 'ARVINDFASN', 'ROUTE', 'TMB', 'FIEMIND',
    'INDIGOPNTS', 'RELAXO', 'JUSTDIAL', 'PRICOLLTD', 'KIRLOSBROS', 'PTC', 'VARROC',
    'STYL', 'PNCINFRA', 'SAMHI', 'FINPIPE', 'CERA', 'BIRLACORPN', 'CIGNITITEC',
    'ANUP', 'MASTEK', 'CSBBANK', 'JSFB', 'CMSINFO', 'ALOKINDS', 'RATEGAIN',
    'GMRP&UI', 'ICIL', 'BECTORFOOD', 'REFEX', 'HEMIPROP', 'RBA', 'INDIAGLYCO',
    'GRWRHITECH', 'WELENT', 'SUDARSCHEM', 'BAJAJELEC', 'OPTIEMUS', 'CIEINDIA',
    'RAYMONDLSL', 'IFBIND', 'JKLAKSHMI', 'FEDFINA', 'GNFC', 'GODREJAGRO', 'SKIPPER',
    'MARKSANS', 'IONEXCHANG', 'INDIASHLTR', 'NEOGEN', 'AARTIDRUGS', 'ARVIND',
    'UTLSOLAR', 'LXCHEM', 'EUREKAFORB', 'JKPAPER', 'ACI', 'MSTCLTD', 'BALAMINES',
    'ORKLAINDIA', 'AARTIPHARM', 'JYOTHYLAB', 'TRIVENI', 'DBL', 'SUNTECK',
    'METROPOLIS', 'AWFIS', 'VIPIND', 'EMIL', 'ASKAUTOLTD', 'THYROCARE', 'RALLIS',
    'PICCADIL', 'ALKYLAMINE', 'SHAREINDIA', 'VAIBHAVGBL', 'DATAMATICS', 'SURYAROSNI',
    'ALIVUS', 'CENTURYPLY', 'WESTLIFE', 'ADVENZYMES', 'SUBROS', 'CAMPUS', 'HCG',
    'WEWORK', 'PGIL', 'KIRLPNU', 'SFL', 'MAHSCOOTER', 'QUESS', 'ENTERO', 'NESCO',
    'CRIZAC', 'SAFARI', 'TVSSCS', 'SUPRIYA', 'TSFINV', 'CORONA', 'STYRENIX',
    'GHCL', 'CAPILLARY', 'ETHOSLTD', 'SMARTWORKS', 'ORIENTCEM', 'AHLUCONT',
    'REDTAPE', 'APLLTD', 'AGARWALEYE', 'TIPSMUSIC', 'SUDEEPPHRM', 'GMMPFAUDLR',
    'STARCEMENT', 'PRSMJOHNSN', 'SKFINDUS', 'WAKEFIT', 'JLHL'
]

# Create symbol to segment mapping
SYMBOL_TO_SEGMENT = {}
for s in LARGE_CAP_SYMBOLS: SYMBOL_TO_SEGMENT[s] = 'LARGE'
for s in MID_CAP_SYMBOLS: SYMBOL_TO_SEGMENT[s] = 'MID'
for s in SMALL_CAP_SYMBOLS: SYMBOL_TO_SEGMENT[s] = 'SMALL'
for s in MICRO_CAP_SYMBOLS: SYMBOL_TO_SEGMENT[s] = 'MICRO'

ALL_SYMBOLS = set(SYMBOL_TO_SEGMENT.keys())

print(f"✅ Loaded {len(ALL_SYMBOLS)} stocks")

# ============================================
# TELEGRAM FUNCTIONS
# ============================================

def send_telegram_message(message):
    """Send message to Telegram"""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_IDS:
        return False
    
    success = 0
    for chat_id in TELEGRAM_CHAT_IDS:
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
            payload = {'chat_id': chat_id, 'text': message, 'parse_mode': 'HTML'}
            response = requests.post(url, json=payload, timeout=30)
            if response.status_code == 200:
                success += 1
        except Exception as e:
            print(f"Error: {e}")
    
    return success > 0

# ============================================
# FETCH BHAVCOPY FOR A SINGLE DATE
# ============================================

def fetch_bhavcopy_for_date(date):
    """Fetch bhavcopy for a specific date"""
    try:
        from nselib import capital_market
        date_str = date.strftime('%d-%m-%Y')
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
        result['DATE'] = date
        
        # Filter EQ series and our symbols
        result = result[result['SERIES'] == 'EQ']
        result = result[result['SYMBOL'].isin(ALL_SYMBOLS)]
        result = result[result['CLOSE'].notna()]
        result = result[result['VOLUME'].notna()]
        
        return result[['SYMBOL', 'DATE', 'CLOSE', 'VOLUME', 'DELIVERY']]
        
    except Exception as e:
        return None

# ============================================
# GET LAST 55 TRADING DATES
# ============================================

def get_last_n_trading_dates(n=55):
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
    current_date = dt.datetime.now().date()
    days_checked = 0
    
    while len(trading_dates) < n and days_checked < 110:
        days_checked += 1
        check_date = current_date - relativedelta(days=days_checked)
        if check_date.weekday() >= 5 or check_date in holidays:
            continue
        trading_dates.append(check_date)
    
    return sorted(trading_dates)

# ============================================
# BUILD 55-DAY DATABASE (WITH CSV CACHE)
# ============================================

def build_database():
    """Fetch last 55 days of data, save to CSV cache"""
    
    print("\n📊 Building 55-day database...")
    
    # Check if cache exists and is recent
    if os.path.exists(CACHE_FILE):
        file_time = dt.datetime.fromtimestamp(os.path.getmtime(CACHE_FILE))
        if file_time.date() == dt.date.today():
            print(f"📁 Loading from cache: {CACHE_FILE}")
            df = pd.read_csv(CACHE_FILE, parse_dates=['DATE'])
            print(f"✅ Loaded {len(df)} rows from cache")
            return df
    
    # Fetch fresh data
    print("📡 Fetching fresh data from NSE...")
    trading_dates = get_last_n_trading_dates(TRADING_DAYS)
    print(f"📅 Trading dates: {len(trading_dates)}")
    print(f"   Range: {trading_dates[0]} to {trading_dates[-1]}")
    
    all_data = []
    for i, date in enumerate(trading_dates, 1):
        print(f"\r   [{i}/{len(trading_dates)}] {date.strftime('%d-%b-%Y')}...", end=" ")
        df = fetch_bhavcopy_for_date(date)
        if df is not None and len(df) > 0:
            all_data.append(df)
            print(f"✓ {len(df)}", end="")
        else:
            print(f"✗", end="")
        time.sleep(0.3)
    
    print()
    
    if not all_data:
        print("❌ No data fetched")
        return None
    
    combined_df = pd.concat(all_data, ignore_index=True)
    combined_df = combined_df.sort_values(['DATE', 'SYMBOL'])
    combined_df = combined_df.drop_duplicates(subset=['SYMBOL', 'DATE'])
    
    # Save to CSV cache
    combined_df.to_csv(CACHE_FILE, index=False)
    print(f"💾 Saved to cache: {CACHE_FILE}")
    print(f"   Total rows: {len(combined_df):,}")
    print(f"   Unique symbols: {combined_df['SYMBOL'].nunique()}")
    
    return combined_df

# ============================================
# TWO-STAGE SCREENING
# ============================================

def analyze_stock(stock_df, symbol, segment):
    """Analyze single stock with 2-stage logic"""
    
    params = OPTIMAL_PARAMETERS[segment]
    stock_df = stock_df.sort_values('DATE')
    
    if len(stock_df) < TRADING_DAYS:
        return None
    
    # Split data
    acc_data = stock_df.iloc[:-PRE_BREAKOUT_WINDOW]  # Days -55 to -8
    pb_data = stock_df.iloc[-PRE_BREAKOUT_WINDOW:]   # Last 7 days
    
    if len(acc_data) < 30:
        return None
    
    # ========== STAGE 1: ACCUMULATION ==========
    
    # Price change over 20 days
    if len(acc_data) >= 21:
        price_20d_ago = acc_data['CLOSE'].iloc[-21]
        current_price_acc = acc_data['CLOSE'].iloc[-1]
        price_change = ((current_price_acc - price_20d_ago) / price_20d_ago) * 100
    else:
        return None
    
    if abs(price_change) > params['price_limit']:
        return None
    
    # Volume surge
    if len(acc_data) >= 35:
        last_7_vol = acc_data['VOLUME'].iloc[-7:].mean()
        prev_28_vol = acc_data['VOLUME'].iloc[-35:-7].mean()
        vol_surge = last_7_vol / prev_28_vol if prev_28_vol > 0 else 1
    else:
        return None
    
    if vol_surge < params['vol_surge']:
        return None
    
    # Delivery rising
    if len(acc_data) >= 10:
        last_5_delivery = acc_data['DELIVERY'].iloc[-5:].mean()
        prev_5_delivery = acc_data['DELIVERY'].iloc[-10:-5].mean()
    else:
        return None
    
    if last_5_delivery <= prev_5_delivery:
        return None
    
    if last_5_delivery < params['delivery_min']:
        return None
    
    # ========== STAGE 2: PRE-BREAKOUT ==========
    
    # Volume dry-up
    if len(stock_df) >= 27:
        vol_5d = pb_data['VOLUME'].mean()
        vol_20d_history = stock_df['VOLUME'].iloc[-27:-7].mean()
        vol_dryup = vol_5d / vol_20d_history if vol_20d_history > 0 else 1
    else:
        return None
    
    if vol_dryup > params['dryup_max']:
        return None
    
    # Near breakout
    if len(stock_df) >= 27:
        high_20d = stock_df['CLOSE'].iloc[-27:-7].max()
        current_price = pb_data['CLOSE'].iloc[-1]
        near_high_pct = (current_price / high_20d) * 100 if high_20d > 0 else 0
    else:
        return None
    
    if near_high_pct < params['near_high_min']:
        return None
    
    # Tight range
    if len(stock_df) >= 10:
        high_10d = stock_df['CLOSE'].iloc[-10:].max()
        low_10d = stock_df['CLOSE'].iloc[-10:].min()
        range_pct = ((high_10d - low_10d) / current_price) * 100 if current_price > 0 else 0
    else:
        return None
    
    if range_pct > params['compression_max']:
        return None
    
    # Delivery holding
    current_delivery = pb_data['DELIVERY'].iloc[-1]
    
    # Calculate quality score
    score = 0
    if vol_surge >= params['vol_surge'] * 1.2: score += 1
    if vol_dryup <= params['dryup_max'] * 0.8: score += 1
    if near_high_pct >= 98: score += 1
    if range_pct <= 3: score += 1
    if current_delivery >= params['delivery_min'] + 10: score += 1
    
    return {
        'symbol': symbol,
        'segment': segment,
        'ltp': round(current_price, 2),
        'price_change': round(price_change, 1),
        'vol_surge': round(vol_surge, 2),
        'vol_dryup': round(vol_dryup, 2),
        'delivery_acc': round(last_5_delivery, 1),
        'delivery_cur': round(current_delivery, 1),
        'near_high': round(near_high_pct, 1),
        'range_pct': round(range_pct, 1),
        'score': score,
        'exp_return': params['expected_return'],
        'win_rate': params['win_rate']
    }

# ============================================
# PROCESS ALL STOCKS
# ============================================

def process_all_stocks(df):
    """Process all stocks and return signals"""
    
    print(f"\n🔍 Screening {df['SYMBOL'].nunique()} stocks...")
    
    all_signals = []
    symbols = df['SYMBOL'].unique()
    
    for i, symbol in enumerate(symbols):
        if (i + 1) % 100 == 0:
            print(f"   Progress: {i+1}/{len(symbols)}")
        
        segment = SYMBOL_TO_SEGMENT.get(symbol)
        if not segment:
            continue
        
        stock_df = df[df['SYMBOL'] == symbol].copy()
        result = analyze_stock(stock_df, symbol, segment)
        
        if result:
            all_signals.append(result)
    
    # Sort by score
    all_signals.sort(key=lambda x: x['score'], reverse=True)
    
    print(f"\n✅ Found {len(all_signals)} signals")
    
    # Breakdown
    for segment in ['LARGE', 'MID', 'SMALL', 'MICRO']:
        count = len([s for s in all_signals if s['segment'] == segment])
        print(f"   {segment}: {count}")
    
    return all_signals

# ============================================
# FORMAT REPORT MESSAGE
# ============================================

def format_report_message(signals, trade_date):
    """Format single message with top 5 per segment"""
    
    message = f"""
🚀 <b>NIFTY 750 SMART MONEY SCREENER</b>
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📅 Date: {trade_date.strftime('%d-%b-%Y')}
📊 Total Signals: {len(signals)}
📈 Analysis: 55 days | Accumulation + Pre-Breakout
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

"""
    
    for segment in ['LARGE', 'MID', 'SMALL', 'MICRO']:
        segment_signals = [s for s in signals if s['segment'] == segment][:5]
        
        if segment_signals:
            message += f"\n<b>🔥 {segment} CAP</b>\n"
            message += f"┌─────┬──────────────┬──────────┬────────┬────────┬────────┐\n"
            message += f"│ #   │ STOCK        │ LTP      │ SURGE  │ DRYUP  │ NEAR%  │\n"
            message += f"├─────┼──────────────┼──────────┼────────┼────────┼────────┤\n"
            
            for idx, s in enumerate(segment_signals, 1):
                message += f"│ {idx:<3} │ {s['symbol']:<12} │ ₹{s['ltp']:<8.2f} │ {s['vol_surge']:.1f}x    │ {s['vol_dryup']:.2f}x   │ {s['near_high']:.0f}%    │\n"
            
            message += f"└─────┴──────────────┴──────────┴────────┴────────┴────────┘\n"
    
    message += f"\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    message += f"📈 Expected 20-day Return: +5-9% | Win Rate: 80-90%\n"
    message += f"⏰ {dt.datetime.now().strftime('%d-%b-%Y %H:%M:%S')}\n"
    message += f"⚠️ Always use stop loss. Do your own research."
    
    return message

# ============================================
# MAIN FUNCTION
# ============================================

def main():
    print("=" * 60)
    print("🚀 NIFTY 750 SMART MONEY SCREENER")
    print("=" * 60)
    print("✅ Accumulation: Days -55 to -8")
    print("✅ Pre-Breakout: Last 7 days ONLY")
    print("=" * 60)
    print(f"Time: {dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    # Check Telegram
    if not TELEGRAM_BOT_TOKEN:
        print("❌ TELEGRAM_BOT_TOKEN not set!")
        return
    
    if not TELEGRAM_CHAT_IDS:
        print("❌ No Telegram chat IDs configured!")
        return
    
    print(f"✅ Telegram configured for {len(TELEGRAM_CHAT_IDS)} chat(s)")
    
    # Build database (55 days)
    df = build_database()
    
    if df is None or len(df) == 0:
        print("❌ Failed to build database")
        send_telegram_message("🔴 NIFTY Screener failed: Unable to fetch data")
        return
    
    # Get trade date (latest date in data)
    trade_date = df['DATE'].max()
    
    # Process all stocks
    signals = process_all_stocks(df)
    
    # Send report
    if signals:
        message = format_report_message(signals, trade_date)
        send_telegram_message(message)
        print(f"\n✅ Sent report with {len(signals)} signals")
    else:
        message = f"""
🔴 <b>NIFTY 750 SCREENER RESULTS</b>
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📅 Date: {trade_date.strftime('%d-%b-%Y')}
❌ <b>NO STOCKS FOUND</b>

No stocks passed both stages.
⏰ {dt.datetime.now().strftime('%d-%b-%Y %H:%M:%S')}
"""
        send_telegram_message(message)
        print("No signals found")
    
    print("\n" + "=" * 60)
    print("✅ SCREENER COMPLETE")
    print("=" * 60)

if __name__ == "__main__":
    main()
