"""
NIFTY 750 SMART MONEY SCREENER - FINAL
========================================
- Accumulation: Days -55 to -8
- Pre-Breakout: Last 7 days ONLY
- No near high filter
- Clean report with TOP 5 stocks only
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
# FIXED PARAMETERS
# ============================================

FIXED_PARAMETERS = {
    'LARGE': {
        'price_limit': 3,
        'vol_surge': 1.1,
        'delivery_min': 65,
        'dryup_max': 0.60,
        'compression_max': 4.0
    },
    'MID': {
        'price_limit': 8,
        'vol_surge': 1.2,
        'delivery_min': 60,
        'dryup_max': 0.60,
        'compression_max': 5.0
    },
    'SMALL': {
        'price_limit': 7,
        'vol_surge': 2.2,
        'delivery_min': 55,
        'dryup_max': 0.55,
        'compression_max': 6.0
    },
    'MICRO': {
        'price_limit': 8,
        'vol_surge': 1.6,
        'delivery_min': 50,
        'dryup_max': 0.50,
        'compression_max': 7.0
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
# FETCH BHAVCOPY
# ============================================

def fetch_bhavcopy_for_date(date):
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
        result = result[result['SERIES'] == 'EQ']
        result = result[result['SYMBOL'].isin(ALL_SYMBOLS)]
        result = result[result['CLOSE'].notna()]
        return result[['SYMBOL', 'DATE', 'CLOSE', 'VOLUME', 'DELIVERY']]
    except Exception as e:
        return None

# ============================================
# GET LAST 55 TRADING DATES
# ============================================

def get_last_n_trading_dates(n=55):
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
    dates = []
    current = dt.date.today()
    days_checked = 0
    while len(dates) < n and days_checked < 110:
        days_checked += 1
        check = current - relativedelta(days=days_checked)
        if check.weekday() >= 5 or check in holidays:
            continue
        dates.append(check)
    return sorted(dates)

# ============================================
# BUILD DATABASE
# ============================================

CACHE_FILE = "nifty_55days_cache.csv"

def build_database():
    if os.path.exists(CACHE_FILE):
        file_time = dt.datetime.fromtimestamp(os.path.getmtime(CACHE_FILE))
        if file_time.date() == dt.date.today():
            df = pd.read_csv(CACHE_FILE, parse_dates=['DATE'])
            return df
    dates = get_last_n_trading_dates(55)
    all_data = []
    for date in dates:
        df = fetch_bhavcopy_for_date(date)
        if df is not None and len(df) > 0:
            all_data.append(df)
        time.sleep(0.3)
    if not all_data:
        return None
    combined = pd.concat(all_data, ignore_index=True)
    combined = combined.sort_values(['DATE', 'SYMBOL'])
    combined = combined.drop_duplicates(subset=['SYMBOL', 'DATE'])
    combined.to_csv(CACHE_FILE, index=False)
    return combined

# ============================================
# ANALYZE STOCK
# ============================================

def analyze_stock(stock_df, symbol, segment):
    params = FIXED_PARAMETERS[segment]
    stock_df = stock_df.sort_values('DATE')
    if len(stock_df) < 55:
        return None
    acc_data = stock_df.iloc[:-7]
    pb_data = stock_df.iloc[-7:]
    if len(acc_data) < 30:
        return None
    # Stage 1: Accumulation
    if len(acc_data) >= 21:
        price_20d_ago = acc_data['CLOSE'].iloc[-21]
        curr_acc = acc_data['CLOSE'].iloc[-1]
        price_change = ((curr_acc - price_20d_ago) / price_20d_ago) * 100
        if abs(price_change) > params['price_limit']:
            return None
    else:
        return None
    if len(acc_data) >= 35:
        last_7_vol = acc_data['VOLUME'].iloc[-7:].mean()
        prev_28_vol = acc_data['VOLUME'].iloc[-35:-7].mean()
        vol_surge = last_7_vol / prev_28_vol if prev_28_vol > 0 else 1
        if vol_surge < params['vol_surge']:
            return None
    else:
        return None
    if len(acc_data) >= 10:
        last_5_del = acc_data['DELIVERY'].iloc[-5:].mean()
        prev_5_del = acc_data['DELIVERY'].iloc[-10:-5].mean()
        if last_5_del <= prev_5_del:
            return None
        if last_5_del < params['delivery_min']:
            return None
    else:
        return None
    # Stage 2: Pre-Breakout
    if len(stock_df) >= 27:
        vol_5d = pb_data['VOLUME'].mean()
        vol_20d_hist = stock_df['VOLUME'].iloc[-27:-7].mean()
        vol_dryup = vol_5d / vol_20d_hist if vol_20d_hist > 0 else 1
        if vol_dryup > params['dryup_max']:
            return None
    else:
        return None
    if len(stock_df) >= 10:
        high_10d = stock_df['CLOSE'].iloc[-10:].max()
        low_10d = stock_df['CLOSE'].iloc[-10:].min()
        curr_price = pb_data['CLOSE'].iloc[-1]
        range_pct = ((high_10d - low_10d) / curr_price) * 100 if curr_price > 0 else 0
        if range_pct > params['compression_max']:
            return None
    else:
        return None
    return {
        'symbol': symbol,
        'segment': segment,
        'ltp': round(curr_price, 2),
        'vol_surge': round(vol_surge, 2),
        'delivery': round(last_5_del, 1),
        'vol_dryup': round(vol_dryup, 2),
        'range': round(range_pct, 1)
    }

# ============================================
# MAIN
# ============================================

def main():
    print("=" * 60)
    print("🚀 NIFTY 750 SMART MONEY SCREENER")
    print("=" * 60)
    print(f"Time: {dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_IDS:
        print("❌ Telegram not configured")
        return
    
    df = build_database()
    if df is None or len(df) == 0:
        send_telegram_message("🔴 NIFTY Screener failed: Unable to fetch data")
        return
    
    trade_date = df['DATE'].max()
    signals = []
    symbols = df['SYMBOL'].unique()
    
    for symbol in symbols:
        segment = SYMBOL_TO_SEGMENT.get(symbol)
        if not segment:
            continue
        stock_df = df[df['SYMBOL'] == symbol].copy()
        result = analyze_stock(stock_df, symbol, segment)
        if result:
            signals.append(result)
    
    signals.sort(key=lambda x: x['vol_surge'], reverse=True)
    top_signals = signals[:5]
    
    if top_signals:
        message = f"""
🚀 <b>NIFTY 750 PRE-BREAKOUT SIGNALS</b>
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📅 Date: {trade_date.strftime('%d-%b-%Y')}
📊 Total: {len(signals)} signals | Top 5 shown
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

"""
        for i, s in enumerate(top_signals, 1):
            message += f"""
<b>{i}. {s['symbol']}</b> | {s['segment']} CAP
   💰 LTP: ₹{s['ltp']:.2f}
   📈 Volume Surge: {s['vol_surge']:.1f}x
   📦 Delivery: {s['delivery']}%
   📉 Volume Dry-up: {s['vol_dryup']:.2f}x
   📐 Range: {s['range']:.1f}%
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
        send_telegram_message(message)
        print(f"✅ Sent {len(top_signals)} signals")
    else:
        send_telegram_message(f"🔴 No stocks passed filters on {trade_date.strftime('%d-%b-%Y')}")
        print("No signals found")
    
    print("=" * 60)
    print("✅ SCREENER COMPLETE")

if __name__ == "__main__":
    main()
