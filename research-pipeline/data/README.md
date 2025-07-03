# Universe Creation Script

This script, `create_universe.py`, builds a point-in-time, **survivorship-bias-free** historical universe of crypto assets. Its primary purpose is to generate the `universe_cache.json` file, which serves as a foundational input for quantitative research and backtesting pipelines.

The script is designed to solve the critical problem of **survivorship bias**. A backtest performed only on today's top assets is fundamentally flawed because it ignores assets that were once prominent but have since failed or declined. By reconstructing the true investment universe as it existed in the past, this script ensures that models are tested under realistic historical market conditions.

## Prerequisites

Before running the script, ensure your environment is set up correctly.

### Conda Environment
You must have a Conda environment activated. The required packages are defined in the `environment.yml` file located in the main project directory.

To create and activate the environment, run:
```bash
# Create the environment from the YAML file
conda env create -f environment.yml

# Activate the environment
conda activate crypto-ml
```

### API Keys
The script requires a CoinGecko Pro API key to function.

1. Create a file named `.env` in the root directory of the project.
2. Add your API key to this file in the following format:
   ```
   COINGECKO_PRO_API_KEY="YOUR_API_KEY_HERE"
   ```

## Usage

The script is designed to be run directly from the command line.

```bash
python create_universe.py
```

> **Important Note**
> This is a potentially long-running and data-intensive process. It makes hundreds of API calls to CoinGecko, and a full run can take a significant amount of time, especially depending on your local development capabilities. It is designed to be run periodically (e.g., monthly or quarterly) to refresh the dataset, not on a daily basis.

## Configuration

The script's behavior can be customized by adjusting the parameters at the top of the `create_universe.py` file.

| Parameter        | Default Value      | Description                                                          |
| :--------------- | :----------------- | :------------------------------------------------------------------- |
| `START_DATE`     | `2022-01-01`       | The first month to include in the historical universe.               |
| `CANDIDATE_SIZE` | `500`              | The size of the initial pool of assets to consider from the API.     |
| `UNIVERSE_SIZE`  | `200`              | The final number of top assets to select for each month's universe.  |

## Methodology

The script follows a robust, multi-step process to generate the point-in-time universe:

1.  **Fetch Candidate Pool:** It begins by fetching a large list of candidate assets (the **Top 500** by default, controlled by `CANDIDATE_SIZE`) from the CoinGecko API. This creates a wide pool of potential universe members.

2.  **Acquire Historical Data:** It then fetches the complete daily market cap history for every candidate in the pool.

3.  **Group by Period:** The historical data is grouped into monthly periods.

4.  **Calculate Monthly Rank:** For each month, it calculates the **average market cap** for every asset. It then ranks all assets against each other based on this metric. Using a monthly average makes the ranking robust to short-term price volatility.

5.  **Select Top Universe:** The script filters the ranked list, keeping only the **Top 200** assets for each month (controlled by `UNIVERSE_SIZE`).

6.  **Construct Final Universe File:** The final output is a JSON object where each key is a month (formatted as `YYYY-MM-01`) and the value is a list of the CoinGecko `coin_id`s that constituted the top assets during that month.

## Output

The script generates a single file: `cache/universe_cache.json`. This file is a critical input for the main data pipeline.

**Example Structure:**
```json
{
    "2022-01-01": [
        "bitcoin",
        "ethereum",
        "binancecoin",
        "... (top 200 assets for this month)"
    ],
    "2022-02-01": [
        "bitcoin",
        "ethereum",
        "tether",
        "... (a potentially different top 200 for this month)"
    ]
}
```
