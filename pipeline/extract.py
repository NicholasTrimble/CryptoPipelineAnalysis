import requests
import time
from typing import List, Dict, Any

COINGECKO_BASE = "https://api.coingecko.com/api/v3"

def fetch_market_chart(coin_id: str, vs_currency: str = "usd", days: int = 30) -> Dict[str, Any]:
    """Fetch market chart data for a given coin from CoinGecko API."""
    url = f"{COINGECKO_BASE}/coins/{coin_id}/market_chart"
    params = {
        "vs_currency": vs_currency,
        "days": days
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()

def fetch_multiple_coins(coins: List[str], vs_currency: str = "usd", days: int = 30, pause: float = 1.2) -> Dict[str, Dict]:
    """Fetch market chart data for multiple coins."""
    all_data = {}
    for coin in coins:
        try:
            data = fetch_market_chart(coin, vs_currency, days)
            all_data[coin] = data
            time.sleep(1)  # To respect API rate limits
        except requests.HTTPError as e:
            print(f"Failed to fetch data for {coin}: {e}")
    return all_data