"""
Universe Construction Script for research data pipeline

This script creates a point-in-time, survivorship-bias-free universe of crypto assets.
It fetches a large pool of candidate assets from CoinGecko, processes their historical
market cap data, and determines the top N assets for each month in the specified period.

The output is a single JSON file ('cache/universe_cache.json') which serves as the
foundational input for the main data acquisition pipeline, ensuring that all backtests
are run against a realistic and historically accurate set of assets.

Author: John Swindell
Date: 2025-07-01
"""

import pandas as pd
import os
import json
import time
from pycoingecko import CoinGeckoAPI
from dotenv import load_dotenv

# --- 1. Configuration ---
print("1. Configuring universe creation script...")
START_DATE = '2022-01-01'
CANDIDATE_SIZE = 2000  # How many top coins to consider as candidates
UNIVERSE_SIZE = 200  # The final size of our universe for each period
CACHE_DIR = '../../cache'
OUTPUT_FILE = os.path.join(CACHE_DIR, 'universe_cache.json')
os.makedirs(CACHE_DIR, exist_ok=True)
print("   -> Config complete.")

# --- 2. API Client Initialization ---
print("\n2. Initializing CoinGecko API client...")
load_dotenv()
CG_API_KEY = os.getenv("COINGECKO_PRO_API_KEY")
if not CG_API_KEY:
    raise ValueError("COINGECKO_PRO_API_KEY not found in .env file.")
cg = CoinGeckoAPI(api_key=CG_API_KEY)
print("   -> API client initialized.")


# --- 3. Helper Functions ---
def get_cg_market_data(coin_id: str) -> dict | None:
    """Fetches full daily market chart data from CoinGecko with rate-limit handling."""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            return cg.get_coin_market_chart_by_id(id=coin_id, vs_currency='usd', days='max', interval='daily')
        except Exception as e:
            if '429' in str(e):
                wait = 61
                print(f"     - RATE LIMIT HIT for '{coin_id}'. Waiting {wait}s...")
                time.sleep(wait)
            else:
                print(f"     - INFO: Could not fetch market chart for '{coin_id}'. Skipping.")
                return None
    print(f"     - FAILED to fetch '{coin_id}' after {max_retries} retries.")
    return None


def process_market_data_to_df(raw_data: dict, coin_id: str) -> pd.DataFrame:
    """Converts raw CoinGecko market data into a structured DataFrame with only necessary columns."""
    if not raw_data or not raw_data.get('market_caps'):
        return pd.DataFrame()

    df = pd.DataFrame(raw_data['market_caps'], columns=['timestamp', 'market_cap'])
    df['date'] = pd.to_datetime(df['timestamp'], unit='ms').dt.normalize()
    df['coin_id'] = coin_id
    return df[['date', 'coin_id', 'market_cap']]


# --- 4. Main Execution Block ---
if __name__ == '__main__':
    print(f"\n4. Fetching Top {CANDIDATE_SIZE} candidate assets...")
    try:
        candidate_ids = []
        for page_num in range(1, 9): # CoinGecko's max per_page is 250. 2000 / 250 = 8 pages.
            print(f"   -> Fetching page {page_num}/8...")
            candidate_markets_page = cg.get_coins_markets(
                vs_currency='usd',
                order='market_cap_desc',
                per_page=250,
                page=page_num
            )
            candidate_ids.extend([coin['id'] for coin in candidate_markets_page])
            time.sleep(1)  # Be respectful of API limits
        print(f"   -> Found {len(candidate_ids)} total candidates.")
    except Exception as e:
        raise SystemExit(f"FATAL: Could not fetch candidate assets from CoinGecko. Error: {e}")

    print("\n5. Fetching historical market cap data for all candidates...")
    all_histories = []
    for i, coin_id in enumerate(candidate_ids):
        print(f"   -> Processing {i + 1}/{len(candidate_ids)}: {coin_id}")
        raw_data = get_cg_market_data(coin_id)
        if raw_data:
            all_histories.append(process_market_data_to_df(raw_data, coin_id))
        time.sleep(1)  # Be respectful of the API rate limits

    if not all_histories:
        raise SystemExit("FATAL: No historical data could be fetched. Exiting.")

    print("\n6. Calculating point-in-time universe...")
    master_df = pd.concat(all_histories, ignore_index=True)
    master_df = master_df[master_df['date'] >= pd.to_datetime(START_DATE)]
    master_df.dropna(subset=['market_cap'], inplace=True)

    # Calculate monthly average market cap for ranking
    master_df['month'] = master_df['date'].dt.to_period('M')
    monthly_avg_mcap = master_df.groupby(['month', 'coin_id'])['market_cap'].mean().reset_index()

    # Rank coins within each month
    monthly_avg_mcap['rank'] = monthly_avg_mcap.groupby('month')['market_cap'].rank(method='first', ascending=False)

    # Filter for the top N coins for each month
    top_n_df = monthly_avg_mcap[monthly_avg_mcap['rank'] <= UNIVERSE_SIZE]

    # Construct the final universe dictionary
    point_in_time_universe = {}
    for month, group in top_n_df.groupby('month'):
        # Format the key as 'YYYY-MM-01'
        month_str = month.to_timestamp().strftime('%Y-%m-01')
        # Get the list of coin_ids for that month
        point_in_time_universe[month_str] = group['coin_id'].tolist()

    print(f"\n7. Saving final universe to '{OUTPUT_FILE}'...")
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(point_in_time_universe, f, indent=4)

    print("\n✅ Process Complete. Universe cache created successfully.")