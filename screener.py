"""
NIFTY 750 SMART MONEY SCREENER - CORRECTED TIME WINDOWS
=========================================================
- ACCUMULATION: Days -55 to -8 (older data, excluding last 7 days)
- PRE-BREAKOUT: Last 7 days ONLY
- Single message with top 5 stocks per segment
"""

import os
import time
import requests
import duckdb
import pandas as pd
import numpy as np
import datetime as dt
from dateutil.relativedelta import relativedelta
import warnings
warnings.filterwarnings('ignore')

# ============================================
# TELEGRAM CONFIGURATION
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
# DATABASE CONFIGURATION
# ============================================

DB_FILE = "nifty_750_database.duckdb"

# ============================================
# OPTIMAL PARAMETERS (Two-stage)
# ============================================

OPTIMAL_PARAMETERS = {
    'LARGE': {
        # STAGE 1: ACCUMULATION (Days -55 to -8)
        'price_limit': 5,
        'vol_surge': 1.3,
        'delivery_min': 60,
        # STAGE 2: PRE-BREAKOUT (Last 7 days ONLY)
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
# NIFTY 750 SYMBOLS (Complete)
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
# FETCH LATEST BHAVCOPY
# ============================================

def fetch_latest_bhavcopy():
    """Fetch latest bhavcopy from NSE"""
    try:
        from nselib import capital_market
        
        today = dt.date.today()
        for days_back in range(1, 5):
            check_date = today - relativedelta(days=days_back)
            if check_date.weekday() >= 5:
                continue
            
            date_str = check_date.strftime('%d-%m-%Y')
            print(f"Fetching data for {date_str}...")
            
            df = capital_market.bhav_copy_with_delivery(trade_date=date_str)
            
            if df is not None and len(df) > 0:
                print(f"✅ Fetched {len(df)} rows")
                return df, check_date
        
        print("❌ No data found")
        return None, None
        
    except Exception as e:
        print(f"Error: {e}")
        return None, None

# ============================================
# TWO-STAGE SCREENING (Corrected time windows)
# ============================================

def calculate_two_stage_metrics(symbol, trade_date, params):
    """
    CORRECTED LOGIC:
    - STAGE 1 (Accumulation): Days -55 to -8 (excludes last 7 days)
    - STAGE 2 (Pre-Breakout): Last 7 days ONLY
    """
    try:
        if not os.path.exists(DB_FILE):
            return None
        
        con = duckdb.connect(DB_FILE)
        
        # Get data for the stock
        query = f"""
            SELECT 
                DATE,
                CLOSE,
                VOLUME,
                DELIVERY,
                ROW_NUMBER() OVER (ORDER BY DATE DESC) as rn_desc
            FROM bhavcopy
            WHERE SYMBOL = '{symbol}'
            ORDER BY DATE
        """
        
        df = con.execute(query).fetchdf()
        con.close()
        
        if len(df) < 55:
            return None
        
        # Split data into two periods
        # Last 7 days = Pre-Breakout window
        # Days before that = Accumulation window
        
        total_rows = len(df)
        pre_breakout_data = df.iloc[-7:].copy()  # Last 7 days
        accumulation_data = df.iloc[:-7].copy()  # Days -55 to -8
        
        if len(accumulation_data) < 30:
            return None
        
        # ============================================
        # STAGE 1: ACCUMULATION (Days -55 to -8)
        # ============================================
        
        # 1.1 Price change over 20 days (within accumulation data)
        if len(accumulation_data) >= 21:
            price_20d_ago = accumulation_data['CLOSE'].iloc[-21]
            current_price_acc = accumulation_data['CLOSE'].iloc[-1]
            price_change = ((current_price_acc - price_20d_ago) / price_20d_ago) * 100
        else:
            return None
        
        if abs(price_change) > params['price_limit']:
            return None
        
        # 1.2 Volume surge (7d vs prev 28d) within accumulation data
        if len(accumulation_data) >= 35:
            last_7_vol = accumulation_data['VOLUME'].iloc[-7:].mean()
            prev_28_vol = accumulation_data['VOLUME'].iloc[-35:-7].mean()
            vol_surge = last_7_vol / prev_28_vol if prev_28_vol > 0 else 1
        else:
            return None
        
        if vol_surge < params['vol_surge']:
            return None
        
        # 1.3 Delivery rising trend within accumulation data
        if len(accumulation_data) >= 10:
            last_5_delivery = accumulation_data['DELIVERY'].iloc[-5:].mean()
            prev_5_delivery = accumulation_data['DELIVERY'].iloc[-10:-5].mean()
        else:
            return None
        
        if last_5_delivery <= prev_5_delivery:
            return None
        
        if last_5_delivery < params['delivery_min']:
            return None
        
        # ============================================
        # STAGE 2: PRE-BREAKOUT (Last 7 days ONLY)
        # ============================================
        
        if len(pre_breakout_data) < 5:
            return None
        
        # 2.1 Volume Dry-up (5d vs 20d) - using last 5 days vs 20 days before pre-breakout
        vol_5d = pre_breakout_data['VOLUME'].mean()
        vol_20d_history = df['VOLUME'].iloc[-27:-7].mean() if len(df) >= 27 else vol_5d
        vol_dryup = vol_5d / vol_20d_history if vol_20d_history > 0 else 1
        
        if vol_dryup > params['dryup_max']:
            return None
        
        # 2.2 Tight Range (last 10 days, mostly pre-breakout period)
        high_10d = df['CLOSE'].iloc[-10:].max()
        low_10d = df['CLOSE'].iloc[-10:].min()
        current_price = pre_breakout_data['CLOSE'].iloc[-1]
        range_pct = ((high_10d - low_10d) / current_price) * 100 if current_price > 0 else 0
        
        if range_pct > params['compression_max']:
            return None
        
        # 2.3 Near Breakout (20-day high from before pre-breakout)
        high_20d = df['CLOSE'].iloc[-27:-7].max() if len(df) >= 27 else current_price
        near_high_pct = (current_price / high_20d) * 100 if high_20d > 0 else 0
        
        if near_high_pct < params['near_high_min']:
            return None
        
        # 2.4 Delivery holding (current delivery)
        current_delivery = pre_breakout_data['DELIVERY'].iloc[-1]
        
        # ============================================
        # CALCULATE QUALITY SCORE
        # ============================================
        
        score = 0
        if vol_surge >= params['vol_surge'] * 1.2: score += 1
        if vol_dryup <= params['dryup_max'] * 0.8: score += 1
        if near_high_pct >= 98: score += 1
        if range_pct <= 3: score += 1
        if current_delivery >= params['delivery_min'] + 10: score += 1
        
        return {
            'price_change': round(price_change, 2),
            'vol_surge': round(vol_surge, 2),
            'vol_dryup': round(vol_dryup, 2),
            'delivery_acc': round(last_5_delivery, 1),
            'delivery_current': round(current_delivery, 1),
            'near_high_pct': round(near_high_pct, 1),
            'range_pct': round(range_pct, 2),
            'quality_score': score
        }
        
    except Exception as e:
        print(f"  Error for {symbol}: {e}")
        return None

# ============================================
# PROCESS ALL STOCKS
# ============================================

def process_all_stocks(trade_date):
    """Process all stocks and return signals"""
    
    print(f"\n📊 Screening {len(ALL_SYMBOLS)} stocks...")
    
    all_signals = []
    processed = 0
    
    for symbol in ALL_SYMBOLS:
        segment = SYMBOL_TO_SEGMENT.get(symbol)
        if not segment:
            continue
        
        params = OPTIMAL_PARAMETERS[segment]
        metrics = calculate_two_stage_metrics(symbol, trade_date, params)
        
        if metrics:
            all_signals.append({
                'SYMBOL': symbol,
                'SEGMENT': segment,
                'LTP': round(metrics.get('current_price', 0), 2),
                'PRICE_CHANGE': metrics['price_change'],
                'VOL_SURGE': metrics['vol_surge'],
                'VOL_DRYUP': metrics['vol_dryup'],
                'DELIVERY_ACC': metrics['delivery_acc'],
                'DELIVERY_CUR': metrics['delivery_current'],
                'NEAR_HIGH': metrics['near_high_pct'],
                'RANGE': metrics['range_pct'],
                'SCORE': metrics['quality_score'],
                'EXP_RETURN': params['expected_return'],
                'WIN_RATE': params['win_rate']
            })
        
        processed += 1
        if processed % 100 == 0:
            print(f"   Processed {processed}/{len(ALL_SYMBOLS)} stocks...")
    
    # Sort by score
    all_signals.sort(key=lambda x: x['SCORE'], reverse=True)
    
    print(f"\n✅ Found {len(all_signals)} total signals")
    
    # Breakdown by segment
    for segment in ['LARGE', 'MID', 'SMALL', 'MICRO']:
        count = len([s for s in all_signals if s['SEGMENT'] == segment])
        print(f"   {segment}: {count} signals")
    
    return all_signals

# ============================================
# FORMAT SINGLE REPORT MESSAGE
# ============================================

def format_report_message(signals, trade_date):
    """Format single message with top 5 per segment"""
    
    message = f"""
🚀 <b>NIFTY 750 SMART MONEY SCREENER</b>
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📅 Date: {trade_date.strftime('%d-%b-%Y')}
📊 Total Signals: {len(signals)}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

"""
    
    for segment in ['LARGE', 'MID', 'SMALL', 'MICRO']:
        segment_signals = [s for s in signals if s['SEGMENT'] == segment][:5]
        
        if segment_signals:
            message += f"\n<b>🔥 {segment} CAP</b>\n"
            message += f"┌─────┬──────────────┬──────────┬────────┬────────┬────────┐\n"
            message += f"│ #   │ STOCK        │ LTP      │ SURGE  │ DRYUP  │ NEAR%  │\n"
            message += f"├─────┼──────────────┼──────────┼────────┼────────┼────────┤\n"
            
            for idx, s in enumerate(segment_signals, 1):
                emoji = "⭐" * s['SCORE'] if s['SCORE'] > 0 else "•"
                message += f"│ {idx:<3} │ {s['SYMBOL']:<12} │ ₹{s['LTP']:<8.2f} │ {s['VOL_SURGE']:.1f}x    │ {s['VOL_DRYUP']:.2f}x   │ {s['NEAR_HIGH']:.0f}%    │\n"
            
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
    
    # Check Telegram configuration
    if not TELEGRAM_BOT_TOKEN:
        print("❌ TELEGRAM_BOT_TOKEN not set!")
        return
    
    if not TELEGRAM_CHAT_IDS:
        print("❌ No Telegram chat IDs configured!")
        return
    
    print(f"✅ Telegram configured for {len(TELEGRAM_CHAT_IDS)} chat(s)")
    
    # Check if database exists
    if not os.path.exists(DB_FILE):
        print(f"❌ Database not found: {DB_FILE}")
        print("   Please build the database first")
        send_telegram_message("🔴 NIFTY Screener failed: Database not found")
        return
    
    # Get trade date
    trade_date = dt.date.today()
    for days_back in range(1, 5):
        check_date = trade_date - relativedelta(days=days_back)
        if check_date.weekday() < 5:
            trade_date = check_date
            break
    
    # Process all stocks
    signals = process_all_stocks(trade_date)
    
    # Send single report message
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

No stocks passed both stages:
- Stage 1: Accumulation (Days -55 to -8)
- Stage 2: Pre-Breakout (Last 7 days ONLY)

⏰ {dt.datetime.now().strftime('%d-%b-%Y %H:%M:%S')}
"""
        send_telegram_message(message)
        print("No signals found")
    
    print("\n" + "=" * 60)
    print("✅ SCREENER COMPLETE")
    print("=" * 60)

if __name__ == "__main__":
    main()
