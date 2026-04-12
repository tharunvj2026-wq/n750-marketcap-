"""
NIFTY 750 ACCUMULATION FINDER
===============================
- 40 days of data
- Finds stocks with institutional accumulation
- Top 5 per market cap
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
# ACCUMULATION PARAMETERS
# ============================================

PARAMETERS = {
    'LARGE': {'price_limit': 4, 'vol_surge': 1.2, 'delivery_min': 50},
    'MID': {'price_limit': 6, 'vol_surge': 1.3, 'delivery_min': 45},
    'SMALL': {'price_limit': 7, 'vol_surge': 1.5, 'delivery_min': 43},
    'MICRO': {'price_limit': 8, 'vol_surge': 1.8, 'delivery_min': 42}
}

# ============================================
# NIFTY 750 SYMBOLS
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

# Create mapping
SYMBOL_TO_SEGMENT = {}
for s in LARGE_CAP_SYMBOLS: SYMBOL_TO_SEGMENT[s] = 'LARGE'
for s in MID_CAP_SYMBOLS: SYMBOL_TO_SEGMENT[s] = 'MID'
for s in SMALL_CAP_SYMBOLS: SYMBOL_TO_SEGMENT[s] = 'SMALL'
for s in MICRO_CAP_SYMBOLS: SYMBOL_TO_SEGMENT[s] = 'MICRO'

ALL_SYMBOLS = set(SYMBOL_TO_SEGMENT.keys())
print(f"✅ Loaded {len(ALL_SYMBOLS)} stocks")

# ============================================
# FETCH FUNCTIONS
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

def get_last_n_trading_dates(n=40):
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
    while len(dates) < n and days_checked < 90:
        days_checked += 1
        check = current - relativedelta(days=days_checked)
        if check.weekday() >= 5 or check in holidays:
            continue
        dates.append(check)
    return sorted(dates)

CACHE_FILE = "nifty_40days_cache.csv"

def build_database():
    if os.path.exists(CACHE_FILE):
        file_time = dt.datetime.fromtimestamp(os.path.getmtime(CACHE_FILE))
        if file_time.date() == dt.date.today():
            df = pd.read_csv(CACHE_FILE, parse_dates=['DATE'])
            return df
    dates = get_last_n_trading_dates(40)
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
# ACCUMULATION ANALYSIS
# ============================================

def analyze_accumulation(stock_df, symbol, segment):
    params = PARAMETERS[segment]
    stock_df = stock_df.sort_values('DATE')
    
    if len(stock_df) < 40:
        return None
    
    # Use all 40 days for accumulation
    acc_data = stock_df
    
    if len(acc_data) < 30:
        return None
    
    # Price change over 20 days
    if len(acc_data) >= 21:
        price_20d_ago = acc_data['CLOSE'].iloc[-21]
        current_price_acc = acc_data['CLOSE'].iloc[-1]
        price_change = ((current_price_acc - price_20d_ago) / price_20d_ago) * 100
        if abs(price_change) > params['price_limit']:
            return None
    else:
        return None
    
    # Volume surge (7d vs prev 28d)
    if len(acc_data) >= 35:
        last_7_vol = acc_data['VOLUME'].iloc[-7:].mean()
        prev_28_vol = acc_data['VOLUME'].iloc[-35:-7].mean()
        vol_surge = last_7_vol / prev_28_vol if prev_28_vol > 0 else 1
        if vol_surge < params['vol_surge']:
            return None
    else:
        return None
    
    # Delivery rising and minimum
    if len(acc_data) >= 10:
        last_5_delivery = acc_data['DELIVERY'].iloc[-5:].mean()
        prev_5_delivery = acc_data['DELIVERY'].iloc[-10:-5].mean()
        if last_5_delivery <= prev_5_delivery:
            return None
        if last_5_delivery < params['delivery_min']:
            return None
    else:
        return None
    
    current_price = stock_df['CLOSE'].iloc[-1]
    
    return {
        'symbol': symbol,
        'segment': segment,
        'ltp': round(current_price, 2),
        'price_change': round(price_change, 1),
        'vol_surge': round(vol_surge, 2),
        'delivery': round(last_5_delivery, 1)
    }

# ============================================
# TELEGRAM
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
# MAIN
# ============================================

def main():
    print("=" * 60)
    print("🚀 NIFTY 750 ACCUMULATION FINDER")
    print("=" * 60)
    
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_IDS:
        print("❌ Telegram not configured")
        return
    
    df = build_database()
    if df is None or len(df) == 0:
        send_telegram_message("🔴 Failed to fetch data")
        return
    
    trade_date = df['DATE'].max()
    
    # Collect signals
    all_signals = {seg: [] for seg in ['LARGE', 'MID', 'SMALL', 'MICRO']}
    symbols = df['SYMBOL'].unique()
    
    for symbol in symbols:
        segment = SYMBOL_TO_SEGMENT.get(symbol)
        if not segment:
            continue
        stock_df = df[df['SYMBOL'] == symbol].copy()
        result = analyze_accumulation(stock_df, symbol, segment)
        if result:
            all_signals[segment].append(result)
    
    # Sort by volume surge
    for seg in all_signals:
        all_signals[seg].sort(key=lambda x: x['vol_surge'], reverse=True)
    
    # Build message
    message = f"""
🚀 NIFTY 750 ACCUMULATION
📅 {trade_date.strftime('%d-%b-%Y')}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

"""
    total = 0
    for seg in ['LARGE', 'MID', 'SMALL', 'MICRO']:
        signals = all_signals[seg][:5]
        if signals:
            total += len(signals)
            message += f"\n🔥 {seg} CAP\n"
            for i, s in enumerate(signals, 1):
                message += f"{i}. {s['symbol']} | ₹{s['ltp']} | {s['price_change']:+.1f}% | {s['vol_surge']}x | {s['delivery']}%\n"
    
    if total == 0:
        message = f"🔴 No accumulation signals on {trade_date.strftime('%d-%b-%Y')}"
    
    send_telegram_message(message)
    print(f"✅ Sent {total} signals")

if __name__ == "__main__":
    main()
