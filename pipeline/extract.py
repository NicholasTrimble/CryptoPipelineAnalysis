import requests
import time
from typing import List, Dict, Any
import pandas as pd

COINGECKO_BASE = "https://api.coingecko.com/api/v3"

def fetch_market_chart(coin_id: str, vs_currency: str = "usd", days: int = 30) -> Dict[str, Any]:
    """Fetch market chart data for a given coin from CoinGecko API."""
    url = f"{COINGECKO_BASE}/coins/{coin_id}/market_chart"
    params = {"vs_currency": vs_currency, "days": days}
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()

def market_chart_to_df(js: Dict[str, Any], coin: str) -> pd.DataFrame:
    """Convert CoinGecko market chart JSON into a DataFrame with one row per timestamp."""
    df = pd.DataFrame(js["prices"], columns=["timestamp", "price"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")  # Convert ms -> datetime
    df["coin"] = coin  # Ensure coin is correctly set for all rows

    # Optional: include market cap and total volume if available
    if "market_caps" in js:
        df["market_cap"] = [x[1] for x in js["market_caps"]]
    if "total_volumes" in js:
        df["total_volume"] = [x[1] for x in js["total_volumes"]]
    
    return df

def fetch_multiple_coins(coins: List[str], vs_currency: str = "usd", days: int = 30, pause: float = 1.2) -> pd.DataFrame:
    """
    Fetch market chart data for multiple coins and combine into a single DataFrame.
    Guarantees all rows have a valid 'coin' value and drops any rows with missing coins.
    """
    all_dfs = []
    for coin in coins:
        try:
            js = fetch_market_chart(coin, vs_currency, days)
            df = market_chart_to_df(js, coin)
            all_dfs.append(df)
            time.sleep(pause)  # Respect API rate limits
        except requests.HTTPError as e:
            print(f"Failed to fetch data for {coin}: {e}")

    # Combine all coins into a single DataFrame
    if all_dfs:
        combined = pd.concat(all_dfs, ignore_index=True)
        # Safety check: drop any rows where coin is None
        combined = combined.dropna(subset=["coin"])
        # Ensure timestamp is datetime
        combined["timestamp"] = pd.to_datetime(combined["timestamp"])
        return combined
    else:
        return pd.DataFrame(columns=["timestamp", "price", "coin", "market_cap", "total_volume"])
