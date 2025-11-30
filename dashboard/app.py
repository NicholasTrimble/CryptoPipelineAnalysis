import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import plotly.express as px

DB_PATH = "data/crypto_data.sqlite"

@st.cache_data(ttl=300)
def load_data(db_path: str = DB_PATH):
    engine = create_engine(f"sqlite:///{db_path}", future=True)
    with engine.connect() as conn:
        df = pd.read_sql(
            text("SELECT * FROM crypto_prices"), 
            conn, 
            parse_dates=["timestamp", "ingested_at"]
        )
    return df

def main():
    st.set_page_config(layout="wide", page_title="CryptoDashboard")
    st.title("Cryptocurrency Market Dashboard")

    df = load_data()
    if df.empty:
        st.warning("No data available. Please run the data pipeline first.")
        return

    # Drop coins with None
    df = df.dropna(subset=["coin"])

    # Ensure timestamp is UTC-aware
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

    # Sort coins for selectbox
    coins = sorted(df["coin"].unique())
    selected_coin = st.sidebar.selectbox(
        "Select coin", 
        coins, 
        index=coins.index("bitcoin") if "bitcoin" in coins else 0
    )

    # Determine date range defaults based on actual DB data
    date_min = df["timestamp"].min().date()
    date_max = df["timestamp"].max().date()
    date_range = st.sidebar.date_input(
        "Select date range",
        [date_min, date_max],
        min_value=date_min,
        max_value=date_max
    )

    # Make start/end timestamps UTC-aware
    start = pd.to_datetime(date_range[0]).tz_localize("UTC")
    end = pd.to_datetime(date_range[1]).tz_localize("UTC") + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)

    # Filter for selected coin and date range
    mask = (df["coin"] == selected_coin) & (df["timestamp"] >= start) & (df["timestamp"] <= end)
    view = df.loc[mask].sort_values("timestamp").reset_index(drop=True)

    if view.empty:
        st.info("No data for the selected coin/date range.")
        return

    # Show latest metrics
    latest = view.iloc[-1]
    col1, col2, col3, col4 = st.columns(4)
    col1.metric(
        "Current Price (USD)", 
        f"${latest['price']:.2f}", 
        delta=f"{(latest['return_1h'] or 0)*100:.2f}%" if pd.notna(latest.get("return_1h")) else "N/A"
    )
    col2.metric("24h MA", f"${view['ma_24h'].iloc[-1]:.2f}")
    col3.metric("24h Volatility", f"{view['volatility_24h'].iloc[-1]:.4f}")
    col4.metric("24h Momentum", f"{view['momentum_24h'].iloc[-1]:.4f}")

    # Price chart with MA overlay
    fig_price = px.line(view, x="timestamp", y="price", title=f"{selected_coin} Price")
    fig_price.add_scatter(x=view["timestamp"], y=view["ma_24h"], mode="lines", name="MA 24h")
    st.plotly_chart(fig_price, use_container_width=True)

    # Volatility chart
    fig_vol = px.line(view, x="timestamp", y="volatility_24h", title="Rolling Volatility (24h)")
    st.plotly_chart(fig_vol, use_container_width=True)

    # Correlation matrix for selected date range
    corr_df = df[(df["timestamp"] >= start) & (df["timestamp"] <= end)].pivot_table(
        index="timestamp", columns="coin", values="log_return_1h"
    )
    if not corr_df.empty:
        corr = corr_df.corr()
        fig_corr = px.imshow(corr, text_auto=True, title="Return Correlation Between Coins")
        st.plotly_chart(fig_corr, use_container_width=True)
    else:
        st.info("Not enough data to compute correlation.")

    # Show recent data
    st.subheader("Recent Data (last 24 rows)")
    st.dataframe(view.tail(24).reset_index(drop=True))

    # Optional debug prints
    st.write("Filtered DataFrame head (debug):")
    st.write(view.head())
    st.write("Number of rows in filtered view:", len(view))

if __name__ == "__main__":
    main()
