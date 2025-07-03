"""
Data Acquisition Pipeline for Cryptocurrency Market Data

This script orchestrates the fetching of market, on-chain, and social data
from CoinGecko, DeFiLlama, and LunarCrush APIs.

It features a two-level caching system (local and Google Cloud Storage) to
minimize redundant API calls and speed up subsequent runs.

Author: John Swindell - Data Engineering Team
Date: Last Updated 7/2/2025
"""

import pandas as pd
import numpy as np
import os
import requests
import json
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from pycoingecko import CoinGeckoAPI
from dotenv import load_dotenv
from google.cloud import storage

# --- Config ---
print("1. Configuring script...")
START_DATE = '2022-01-01'
UNIVERSE_SIZE = 200
CANDIDATE_SIZE = 500
OUTPUT_FILE = '../crypto_market_data.parquet'
CACHE_DIR = '../cache'
UNIVERSE_CACHE_FILE = f'{CACHE_DIR}/universe_cache.json'
LLAMA_PROTOCOL_MAP_CACHE = f'{CACHE_DIR}/llama_protocol_map.json'
LLAMA_CHAIN_MAP_CACHE = f'{CACHE_DIR}/llama_chain_map.json'
os.makedirs(CACHE_DIR, exist_ok=True)
print("   -> Config complete.")

# --- API Client Initialization ---
print("\n2. Initializing API clients...")
load_dotenv()
CG_API_KEY = os.getenv("COINGECKO_PRO_API_KEY")
DL_API_KEY = os.getenv("DEFILLAMA_PRO_API_KEY")
LC_API_KEY = os.getenv("LUNARCRUSH_PRO_API_KEY")
llama_headers = {'api-key': DL_API_KEY}
lunarcrush_headers = {'Authorization': f'Bearer {LC_API_KEY}'}
if not CG_API_KEY or not DL_API_KEY or not LC_API_KEY:
    raise ValueError("API keys for CoinGecko, DeFiLlama, or LunarCrush not found. Please check your .env file.")
cg = CoinGeckoAPI(api_key=CG_API_KEY)
print("   -> API clients initialized successfully.")

# --- 3. Helper Functions & CachingManager Class ---
print("\n3. Defining helper functions...")

# --- Google Cache Helper Class ---
class GCSCachingManager:
    def __init__(self, project_id: str, bucket_name: str, local_cache_dir: str = 'cache', gcs_client=None):
        """Initializes the Caching Manager for GCS."""
        try:
            # This single line handles both live runs and testing
            self.client = storage.Client(project=project_id) if gcs_client is None else gcs_client
            self.bucket = self.client.bucket(bucket_name)
        except Exception as e:
            raise Exception(f"Failed to initialize GCS client. Ensure you have authenticated correctly. Error: {e}")

        self.local_cache_dir = local_cache_dir
        os.makedirs(self.local_cache_dir, exist_ok=True)
        print(f"-> CachingManager initialized for GCS bucket '{bucket_name}'.")

    def get(self, file_name: str):
        """Tries to get a file from the cache. First checks local, then GCS."""
        local_path = os.path.join(self.local_cache_dir, file_name)
        if os.path.exists(local_path):
            return self._load_from_local(local_path)

        blob = self.bucket.blob(file_name)
        if blob.exists():
            print(f"   -> Cache HIT from GCS for '{file_name}'. Downloading...")
            blob.download_to_filename(local_path)
            return self._load_from_local(local_path)

        return None  # Return None on a complete cache miss

    def set(self, file_name: str, data):
        """Saves data to the local cache and uploads it to GCS."""
        local_path = os.path.join(self.local_cache_dir, file_name)

        try:
            if isinstance(data, pd.DataFrame):
                data.to_parquet(local_path)
            else:
                with open(local_path, 'w') as f:
                    json.dump(data, f)

            print(f"   -> Saving '{file_name}' to GCS cache...")
            blob = self.bucket.blob(file_name)
            blob.upload_from_filename(local_path)
        except Exception as e:
            print(f"     WARNING: Failed to save or upload '{file_name}' to cache. Error: {e}")

    def _load_from_local(self, local_path: str):
        """Helper to load data from a local file based on its extension."""
        try:
            if local_path.endswith('.parquet'):
                return pd.read_parquet(local_path)
            elif local_path.endswith('.json'):
                with open(local_path, 'r') as f:
                    return json.load(f)
        except Exception as e:
            print(f"     WARNING: Could not load local cache file '{local_path}'. Error: {e}")
        return None


# --- Processing Functions  ---
def get_cg_market_data(coin_id: str, days: str = 'max') -> dict | None:
    """
    Fetches ALL necessary market data from CoinGecko using the correct endpoints.
    - Fetches full history for Price, MCap, and Volume.
    - Fetches daily OHLC for the maximum possible range.
    """
    full_data = {}
    max_retries = 3

    for attempt in range(max_retries):
        try:
            # Call 1: Get the full, multi-year history for price, mcap, and volume. This is our base.
            market_chart_data = cg.get_coin_market_chart_by_id(
                id=coin_id, vs_currency='usd', days='max', interval='daily'
            )
            full_data['price_mcap_vol'] = market_chart_data

            # Call 2: Get daily OHLC data. We fetch 'max' which gives us a substantial recent history.
            # We will align this with the main dataframe later.
            ohlc_data = cg.get_coin_ohlc_by_id(id=coin_id, vs_currency='usd', days='max')
            full_data['ohlc'] = ohlc_data

            return full_data

        except Exception as e:
            if '429' in str(e):
                wait = 61
                print(f"     RATE LIMIT HIT for '{coin_id}'. Waiting {wait}s...")
                time.sleep(wait)
            else:
                # This can happen if a coin doesn't have OHLC data available
                print(f"     INFO: Could not fetch complete CoinGecko data for '{coin_id}'. Error: {e}")
                # Try to return at least the market chart data if OHLC fails
                if 'price_mcap_vol' in full_data:
                    return full_data
                return None

    print(f"     FAILED to fetch complete data for '{coin_id}' after {max_retries} retries.")
    return None


def process_market_data_to_df(raw_data: dict, coin_id: str, ticker_map: dict) -> pd.DataFrame:
    """
    Correctly processes and merges the full history from CoinGecko.
    """
    if not raw_data or not raw_data.get('price_mcap_vol'):
        return pd.DataFrame()

    # Create the base DataFrame from the most reliable long-term data source
    price_data = raw_data['price_mcap_vol'].get('prices', [])
    volume_data = raw_data['price_mcap_vol'].get('total_volumes', [])
    mcap_data = raw_data['price_mcap_vol'].get('market_caps', [])

    if not price_data:
        return pd.DataFrame()

    # Create the base DataFrame with Close price, Volume, and Market Cap
    df_base = pd.DataFrame(price_data, columns=['timestamp', 'close'])
    df_vol = pd.DataFrame(volume_data, columns=['timestamp', 'volume'])
    df_mcap = pd.DataFrame(mcap_data, columns=['timestamp', 'market_cap'])

    df = pd.merge(df_base, df_vol, on='timestamp', how='left')
    df = pd.merge(df, df_mcap, on='timestamp', how='left')
    df['date'] = pd.to_datetime(df['timestamp'], unit='ms').dt.normalize()

    # Create the OHLC DataFrame
    ohlc_data = raw_data.get('ohlc', [])
    if ohlc_data:
        df_ohlc = pd.DataFrame(ohlc_data, columns=['timestamp', 'open', 'high', 'low', 'close_ohlc'])
        df_ohlc['date'] = pd.to_datetime(df_ohlc['timestamp'], unit='ms').dt.normalize()
        # Merge OHLC data onto the base DataFrame. 'close_ohlc' is redundant, so we drop it.
        df = pd.merge(df, df_ohlc[['date', 'open', 'high', 'low']], on='date', how='left')

    # Add identifiers and clean up
    df['coin_id'] = coin_id
    df['ticker'] = df['coin_id'].map(ticker_map)

    # Reorder columns for clarity
    final_cols = ['date', 'coin_id', 'ticker', 'open', 'high', 'low', 'close', 'volume', 'market_cap']
    existing_cols = [col for col in final_cols if col in df.columns]

    return df[existing_cols]


def _process_protocol_tvl_response(raw_data: dict) -> pd.DataFrame:
    """
    Parses complex TVL responses from 'chainTvls', with a fallback for simple 'tvl' structures.
    """
    chain_tvls = raw_data.get('chainTvls')

    # This handles simpler protocols that do not have the nested 'chainTvls' structure
    if not isinstance(chain_tvls, dict):
        tvl_data = raw_data.get('tvl', [])
        if not tvl_data: return pd.DataFrame()

        df = pd.DataFrame(tvl_data)
        df.rename(columns={'totalLiquidityUSD': 'protocol_tvl'}, inplace=True)
        df['date'] = pd.to_datetime(pd.to_numeric(df['date'], errors='coerce'), unit='s').dt.normalize()
        return df[['date', 'protocol_tvl']]

    # This handles complex, multi-chain protocols by aggregating TVL across all chains
    daily_total_tvl = {}
    for chain_data in chain_tvls.values():
        for item in chain_data.get('tvl', []):
            date = pd.to_datetime(item['date'], unit='s').normalize()
            tvl = item.get('totalLiquidityUSD', 0)
            daily_total_tvl[date] = daily_total_tvl.get(date, 0) + tvl

    if not daily_total_tvl:
        return pd.DataFrame()

    # Create the DataFrame from the aggregated daily totals
    df = pd.DataFrame(list(daily_total_tvl.items()), columns=['date', 'protocol_tvl'])

    # *** THE MISSING LINE: Return the DataFrame ***
    return df.sort_values('date')


def _process_dex_volume_response(raw_data: dict) -> pd.DataFrame:
    """
    Takes a raw /summary/dexs/{slug} response and correctly parses the
    'totalDataChart' key to extract DEX volume data.
    """
    chart_data = raw_data.get('totalDataChart')
    if not chart_data or not isinstance(chart_data, list):
        return pd.DataFrame()

    df = pd.DataFrame(chart_data, columns=['date', 'dex_volume'])
    if df.empty:
        return pd.DataFrame()

    df['date'] = pd.to_datetime(df['date'], unit='s', errors='coerce').dt.normalize()
    df.dropna(subset=['date'], inplace=True)
    df['dex_volume'] = pd.to_numeric(df['dex_volume'], errors='coerce')
    return df


def get_and_process_protocol_data(llama_slug: str, coin_id: str, ticker_map: dict, headers: dict) -> pd.DataFrame:
    """
    Fetches, processes, and merges TVL and DEX data using the new, robust parsers.
    """
    tvl_df, dex_df = pd.DataFrame(), pd.DataFrame()

    try:
        res_tvl = requests.get(f"https://api.llama.fi/protocol/{llama_slug}", headers=headers)
        if res_tvl.ok: tvl_df = _process_protocol_tvl_response(res_tvl.json())
    except Exception as e:
        print(f"     WARNING: Protocol TVL request for {llama_slug} failed. Error: {e}")

    try:
        res_dex = requests.get(f"https://api.llama.fi/summary/dexs/{llama_slug}", headers=headers)
        if res_dex.ok: dex_df = _process_dex_volume_response(res_dex.json())
    except Exception as e:
        print(f"     WARNING: DEX volume request for {llama_slug} failed. Error: {e}")

    if tvl_df.empty and dex_df.empty:
        return pd.DataFrame()

    if tvl_df.empty:
        merged_df = dex_df
        merged_df['protocol_tvl'] = np.nan
    elif dex_df.empty:
        merged_df = tvl_df
        merged_df['dex_volume'] = np.nan
    else:
        merged_df = pd.merge(tvl_df, dex_df, on='date', how='outer')

    merged_df['coin_id'] = coin_id
    merged_df['ticker'] = merged_df['coin_id'].map(ticker_map)
    return merged_df


def get_and_process_lunarcrush_data(ticker: str, coin_id: str, headers: dict) -> pd.DataFrame:
    """
    Fetches and processes historical daily metrics for a given ticker from LunarCrush.
    """
    if not ticker:
        return pd.DataFrame()
    try:
        # We specify bucket='day' to match our daily data frequency
        url = f"https://lunarcrush.com/api4/public/coins/{ticker}/time-series/v2?bucket=day&interval=3650d"
        res = requests.get(url, headers=headers)

        if not res.ok:
            print(f"     WARNING: LunarCrush request for {ticker} returned status {res.status_code}.")
            return pd.DataFrame()

        raw_data = res.json()
        time_series_data = raw_data.get('data', [])

        if not time_series_data:
            return pd.DataFrame()

        # Process the data into a DataFrame
        df = pd.DataFrame(time_series_data)

        # Rename columns for clarity and to avoid conflicts
        df.rename(columns={
            'time': 'date',
            'galaxy_score': 'lc_galaxy_score',
            'alt_rank': 'lc_alt_rank',
            'social_dominance': 'lc_social_dominance',
            'sentiment': 'lc_sentiment'
        }, inplace=True)

        df['date'] = pd.to_datetime(df['date'], unit='s').dt.normalize()
        df['coin_id'] = coin_id

        # Select only the columns we need for our feature store
        required_cols = ['date', 'coin_id', 'lc_galaxy_score', 'lc_alt_rank', 'lc_social_dominance', 'lc_sentiment']

        # Filter for only the columns that actually exist in the response
        existing_cols = [col for col in required_cols if col in df.columns]

        return df[existing_cols]

    except Exception as e:
        print(f"     WARNING: LunarCrush processing for {ticker} failed. Error: {e}")
        return pd.DataFrame()


def process_chain_tvl_to_df(raw_data: list, coin_id: str, ticker_map: dict):
    df = pd.DataFrame(raw_data)
    if 'totalLiquidityUSD' not in df.columns: return pd.DataFrame()
    df.rename(columns={'totalLiquidityUSD': 'chain_tvl'}, inplace=True)
    df['date'] = pd.to_datetime(pd.to_numeric(df['date'], errors='coerce'), unit='s').dt.normalize()
    df['coin_id'] = coin_id
    df['ticker'] = df['coin_id'].map(ticker_map)
    return df


# --- DeFi Llama Map Functions ---
def create_defillama_protocol_map(headers: dict, cacher: GCSCachingManager) -> dict:
    cache_key = 'maps/llama_protocol_map.json'
    cached_data = cacher.get(cache_key)
    if cached_data is not None: return cached_data

    print(f"   -> Cache MISS for '{cache_key}'. Fetching from live API...")
    try:
        res = requests.get("https://api.llama.fi/protocols", headers=headers)
        res.raise_for_status()
        protocol_map = {p['gecko_id']: p['slug'] for p in res.json() if p.get('gecko_id')}
        cacher.set(cache_key, protocol_map)
        return protocol_map
    except Exception as e:
        print(f"Could not create DeFi Llama protocol map. Error: {e}")
    return {}


def get_all_chains_map(headers: dict, cacher: GCSCachingManager) -> dict:
    cache_key = 'maps/llama_chain_map.json'
    cached_data = cacher.get(cache_key)
    if cached_data is not None: return cached_data

    print(f"   -> Cache MISS for '{cache_key}'. Fetching from live API...")
    try:
        res = requests.get("https://api.llama.fi/chains", headers=headers)
        res.raise_for_status()
        chain_map = {c['gecko_id']: c['name'] for c in res.json() if c.get('gecko_id')}
        cacher.set(cache_key, chain_map)
        return chain_map
    except Exception as e:
        print(f"Could not fetch DeFi Llama chains. Error: {e}")
    return {}


# --- Cache Aware Orchestration Function ---
def fetch_and_cache_coin_history(coin_id: str, ticker_map: dict, llama_protocol_map: dict, llama_chain_map: dict,
                                 all_headers: dict, cacher: GCSCachingManager) -> pd.DataFrame:
    """
    Orchestrates the fetching of all data for a single coin, utilizing the shared cache.
    If a cached version exists, it's returned. Otherwise, it fetches from live APIs and populates the cache.
    """
    cache_key = f"coin_history/{coin_id}.parquet"
    cached_df = cacher.get(cache_key)
    if cached_df is not None:
        print(f"  -> Cache HIT for '{coin_id}'. Loading from cache.")
        return cached_df

    print(f"  -> Cache MISS for '{coin_id}'. Fetching from live APIs...")
    ticker = ticker_map.get(coin_id, '').upper()

    # Step 1: Fetch CoinGecko Data
    cg_raw = get_cg_market_data(coin_id, days='max')
    final_coin_df = process_market_data_to_df(cg_raw, coin_id, ticker_map)
    if final_coin_df.empty: return pd.DataFrame()

    # Step 2: Fetch DeFi Llama Data
    chain_tvl_df = pd.DataFrame()
    if coin_id in llama_chain_map:
        llama_slug_for_chain = llama_chain_map.get(coin_id)
        try:
            res = requests.get(f"https://api.llama.fi/charts/{llama_slug_for_chain}", headers=all_headers['llama'])
            if res.ok: chain_tvl_df = process_chain_tvl_to_df(res.json(), coin_id, ticker_map)
        except Exception as e:
            print(f"     WARNING: Chain TVL request for {llama_slug_for_chain} failed. Error: {e}")

    protocol_data_df = pd.DataFrame()
    if coin_id in llama_protocol_map:
        llama_slug_for_protocol = llama_protocol_map.get(coin_id)
        if llama_slug_for_protocol:
            protocol_data_df = get_and_process_protocol_data(llama_slug_for_protocol, coin_id, ticker_map,
                                                             headers=all_headers['llama'])

    # Step 3: Fetch LunarCrush Data
    lunarcrush_df = get_and_process_lunarcrush_data(ticker, coin_id, headers=all_headers['lunarcrush'])

    # Step 4: Merge all dataframes together
    if not chain_tvl_df.empty:
        final_coin_df = pd.merge(final_coin_df, chain_tvl_df[['date', 'coin_id', 'chain_tvl']], on=['date', 'coin_id'],
                                 how='left')
    if not protocol_data_df.empty:
        data_columns = [col for col in ['protocol_tvl', 'dex_volume'] if col in protocol_data_df.columns]
        if data_columns:
            final_coin_df = pd.merge(final_coin_df, protocol_data_df[['date', 'coin_id'] + data_columns],
                                     on=['date', 'coin_id'], how='left')
    if not lunarcrush_df.empty:
        final_coin_df = pd.merge(final_coin_df, lunarcrush_df, on=['date', 'coin_id'], how='left')

    # Step 5: Save the final merged DataFrame to the cache for next time.
    if not final_coin_df.empty:
        cacher.set(cache_key, final_coin_df)

    return final_coin_df

print("✅ Helper functions defined and ready for caching.")

# if __name__ == '__main__':
#
#     # --- 4. Initialize Cache Manager ---
#     print("\n4. Initializing Cache Manager...")
#     cacher = GCSCachingManager(
#         project_id='your-gcp-project-id',      # TODO <--- IMPORTANT: SET TO GCP PROJECT ID
#         bucket_name='cryptonest-pipeline-cache' # TODO <--- IMPORTANT: SET THE BUCKET NAME FROM ARSLAN
#     )
# --- Dummy Cache Manager for Testing ---
if __name__ == '__main__':

    print("\n4. Initializing Dummy Cache Manager for a dry run...")


    class DummyCachingManager:
        def get(self, file_name): return None  # Always return None to simulate a cache miss

        def set(self, file_name, data): pass


    cacher = DummyCachingManager()
    # --- 5. Load Universe & Build Lookup Maps ---
    print("\n5. Loading universe and building lookup maps...")
    try:
        with open(UNIVERSE_CACHE_FILE, 'r') as f:
            point_in_time_universe = json.load(f)
        print(f"   -> Universe loaded successfully from local file '{UNIVERSE_CACHE_FILE}'.")
        candidate_markets = cg.get_coins_markets(vs_currency='usd', per_page=CANDIDATE_SIZE, page=1)
        print("   -> Candidate markets fetched for map creation.")
    except FileNotFoundError:
        raise SystemExit(f"FATAL: Local cache file '{UNIVERSE_CACHE_FILE}' not found. Please run universe construction first.")

    ticker_map = {coin['id']: coin['symbol'].upper() for coin in candidate_markets}
    
    # Bundle headers for easy passing
    all_headers = {'llama': llama_headers, 'lunarcrush': lunarcrush_headers}

    # Use the refactored, cache-aware map functions
    llama_protocol_map = create_defillama_protocol_map(headers=all_headers['llama'], cacher=cacher)
    llama_chain_map = get_all_chains_map(headers=all_headers['llama'], cacher=cacher)
    print("   -> All maps loaded and validated.")


    # --- 6. STAGE 1: Fetch All Coin Histories ---
    all_unique_coins = sorted(list(set(coin for coin_list in point_in_time_universe.values() for coin in coin_list)))
    print(f"\n--- STAGE 1: Fetching full history for {len(all_unique_coins)} unique assets ---")
    full_history_cache = {}
    for coin_id in all_unique_coins:
        # This function handles all caching, API calls, and merging logic
        coin_df = fetch_and_cache_coin_history(
            coin_id=coin_id,
            ticker_map=ticker_map,
            llama_protocol_map=llama_protocol_map,
            llama_chain_map=llama_chain_map,
            all_headers=all_headers,
            cacher=cacher
        )
        if not coin_df.empty:
            full_history_cache[coin_id] = coin_df
        # A smaller sleep is fine here since most calls will be cached after the first run
        time.sleep(0.1) 


    # --- 7. STAGE 2: Assemble Point-In-Time Dataset ---
    print(f"\n--- STAGE 2: Assembling point-in-time dataset from cached histories ---")
    all_period_data = []
    # Operates on the in-memory cache
    for period_str, coin_list in point_in_time_universe.items():
        period_date = pd.to_datetime(period_str)
        print(f"   -> Assembling data for period <= {period_date.date()}")
        for coin_id in coin_list:
            if coin_id in full_history_cache:
                full_coin_history = full_history_cache[coin_id]
                point_in_time_slice = full_coin_history[full_coin_history['date'] <= period_date].copy()
                all_period_data.append(point_in_time_slice)
    

    # --- 8. Final Combination & Save ---
    print("\n8. Combining, cleaning, and saving final dataset...")
    if all_period_data:
        master_df = pd.concat(all_period_data, ignore_index=True)
        print(f"   -> Filtering final dataset for dates after {START_DATE}...")
        master_df = master_df[master_df['date'] >= pd.to_datetime(START_DATE)].copy()
        # Final cleaning, deduplication, and saving logic
        master_df.drop_duplicates(subset=['coin_id', 'date'], keep='last', inplace=True)
        print("   -> Resolving different asset versions (e.g., bridged tokens)...")
        canonical_map = { 'binance-peg-weth': 'weth', 'wrapped-steth': 'staked-ether', 'wrapped-bitcoin': 'bitcoin' }
        master_df['canonical_id'] = master_df['coin_id'].map(canonical_map).fillna(master_df['coin_id'])
        master_df.drop_duplicates(subset=['canonical_id', 'date'], keep='last', inplace=True)
        master_df.sort_values(by=['canonical_id', 'date'], inplace=True)
        master_df.reset_index(drop=True, inplace=True)
        
        print("\n--- Inspecting Final DataFrame before saving ---")
        master_df.info(verbose=True)

        master_df.to_parquet(OUTPUT_FILE)
        print(f"\n✅ PROCESS COMPLETE! Final CLEAN dataset saved to '{OUTPUT_FILE}'.")
        print(f"     -> DataFrame shape: {master_df.shape}")
        print(f"     -> Contains data for {master_df['canonical_id'].nunique()} unique assets.")
    else:
        print("\n❌ PROCESS FAILED. No data was fetched or processed.")