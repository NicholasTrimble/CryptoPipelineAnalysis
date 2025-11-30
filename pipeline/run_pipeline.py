import requests
import time
from typing import List, Dict, Any
import pandas as pd
import sqlite3

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
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
    df["coin"] = coin  # Important: set coin for every row

    if "market_caps" in js:
        df["market_cap"] = [x[1] for x in js["market_caps"]]
    if "total_volumes" in js:
        df["total_volume"] = [x[1] for x in js["total_volumes"]]
    
    return df

def fetch_multiple_coins(coins: List[str], vs_currency: str = "usd", days: int = 30, pause: float = 1.2) -> pd.DataFrame:
    """
    Fetch market chart data for multiple coins and combine into a single DataFrame.
    Ensures every row has a valid 'coin' value.
    """
    all_dfs = []
    for coin in coins:
        try:
            js = fetch_market_chart(coin, vs_currency, days)
            df = market_chart_to_df(js, coin)
            all_dfs.append(df)
            time.sleep(pause)
        except requests.HTTPError as e:
            print(f"Failed to fetch data for {coin}: {e}")

    if all_dfs:
        combined = pd.concat(all_dfs, ignore_index=True)
        combined = combined.dropna(subset=["coin"])  # Safety check
        combined["timestamp"] = pd.to_datetime(combined["timestamp"])
        return combined
    else:
        # Return empty DataFrame with proper columns if nothing fetched
        return pd.DataFrame(columns=["timestamp", "price", "coin", "market_cap", "total_volume"])
