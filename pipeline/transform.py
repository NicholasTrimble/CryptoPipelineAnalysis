import pandas as pd
import numpy as np
from typing import Dict, Any

def market_chart_to_df(market_chart_json: Dict[str, Any], coin_id: str, freq: str = "1H") -> pd.DataFrame:
    """
    Convert CoinGecko market_chart JSON to tidy DataFrame:
    columns: ['timestamp','price','market_cap','total_volume','coin', plus derived features]
    - freq: frequency to asfreq/resample (default hourly).
    """
    prices = pd.DataFrame(market_chart_json.get("prices", []), columns=["timestamp_ms", "price"])
    market_caps = pd.DataFrame(market_chart_json.get("market_caps", []), columns=["timestamp_ms", "market_cap"])
    volumes = pd.DataFrame(market_chart_json.get("total_volumes", []), columns=["timestamp_ms", "total_volume"])

    df = prices.merge(market_caps, on="timestamp_ms", how="outer").merge(volumes, on="timestamp_ms", how="outer")
    df = df.drop_duplicates(subset=["timestamp_ms"]).sort_values("timestamp_ms")

    df["timestamp"] = pd.to_datetime(df["timestamp_ms"], unit="ms", utc=True)
    df = df.drop(columns=["timestamp_ms"])
    df["coin"] = coin_id

    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["market_cap"] = pd.to_numeric(df["market_cap"], errors="coerce")
    df["total_volume"] = pd.to_numeric(df["total_volume"], errors="coerce")

    # index by timestamp and reindex to fixed frequency
    df = df.set_index("timestamp").asfreq(freq)
    # fill price forward, but not volume/market_cap (we may want to sum/aggregate later)
    df["price"] = df["price"].ffill()

    # basic returns and log returns
    df["return_1h"] = df["price"].pct_change(periods=1)
    df["log_return_1h"] = np.log1p(df["return_1h"])

    # moving averages and rolling volatility (window sizes are in periods of freq - 24H = 24 if freq='1H')
    df["ma_24h"] = df["price"].rolling(window=24, min_periods=1).mean()
    df["volatility_24h"] = df["log_return_1h"].rolling(window=24, min_periods=1).std()

    # momentum: price / ma - 1
    df["momentum_24h"] = df["price"]/df["ma_24h"] - 1

    # reset index to make timestamp a column again
    df = df.reset_index()
    return df

def combine_coin_dfs(dfs: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    combined = pd.concat(dfs.values(), ignore_index=True, sort=False)
    return combined
