"""Streamlit app for the dashboard"""

from typing import Union

import streamlit as st
import requests
import pandas as pd
import plotly.graph_objects as go


PRIMARY_URL = "http://127.0.0.1:8000"


def get_ticks_df(
    symbol: str, date_range: str, frequency: int = 1
) -> Union[pd.DataFrame, None]:
    """
    Fetches tick data for a specified symbol and date range and returns it as a DataFrame.

    Args:
        symbol (str): The symbol for which to fetch the tick data.
        date_range (str): The date range in the format "YYYY-MM-DD:YYYY-MM-DD".
        frequency (int, optional): The frequency of the tick data in seconds. Defaults to 1.

    Returns:
        Union[pd.DataFrame, None]: A DataFrame containing the tick data with columns
        'datetime', 'ltp', 'ltq', 'buy_price', 'buy_qty', 'sell_price', 'sell_qty', and
        'open_interest'. Returns None if there is an error in fetching the data.
    """

    rows = []
    header = None

    try:
        with requests.get(
            f"{PRIMARY_URL}/ticks?"
            + f"symbol={symbol}&date_range={date_range}&frequency={frequency}",
            timeout=3600,
            stream=True,
        ) as response:
            for row in response.iter_lines(decode_unicode=True):
                if not header:
                    header = row.lower().split(",")
                else:
                    rows.append(row.split(","))

        df = pd.DataFrame(
            rows,
            columns=header,
        )
        df = df.astype(
            {
                "ltp": float,
                "ltq": int,
                "buy_price": float,
                "buy_qty": int,
                "sell_price": float,
                "sell_qty": int,
                "open_interest": int,
            }
        )
        df["datetime"] = pd.to_datetime(df["datetime"], format="%Y-%m-%d %H:%M:%S")
    except requests.exceptions.ChunkedEncodingError:
        df = None

    return df


def get_bars_df(
    symbol: str, date_range: str, frequency: int = 1
) -> Union[pd.DataFrame, None]:
    """
    Fetches OHLCV (Open, High, Low, Close, Volume) data for a specified symbol and date range
    and returns it as a DataFrame.

    Args:
        symbol (str): The symbol for which to fetch the OHLCV data.
        date_range (str): The date range in the format "YYYY-MM-DD:YYYY-MM-DD".
        frequency (int, optional): The frequency of the OHLCV data in seconds. Defaults to 1.

    Returns:
        Union[pd.DataFrame, None]: A DataFrame containing the OHLCV data with columns such as
        'datetime', 'open', 'high', 'low', 'close', 'volume'. Returns None if there is an error
        in fetching the data.
    """

    rows = []

    try:
        with requests.get(
            f"{PRIMARY_URL}/ohlcv?"
            + f"symbol={symbol}&date_range={date_range}&frequency={frequency}",
            timeout=3600,
            stream=True,
        ) as response:
            for row in response.iter_lines(decode_unicode=True):
                rows.append(row.split(","))

        df = pd.DataFrame(
            rows[1:],
            columns=rows[0],
        )
        # df["datetime"] = pd.to_datetime(df["datetime"], format="%Y-%m-%d %H:%M:%S")
    except requests.exceptions.ChunkedEncodingError:
        df = None

    return df


def plot_ticks(tick_df: Union[pd.DataFrame, None]):
    """
    Plots tick data using Plotly and displays it in a Streamlit app.

    Args:
        tick_df (pd.DataFrame): A DataFrame containing tick data with at least
        the following columns: 'datetime' and 'ltp'.
    """

    if tick_df is None:
        return

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(x=tick_df["datetime"], y=tick_df["ltp"], name="Last Price")
    )

    st.plotly_chart(fig, theme="streamlit")


def plot_candlesticks(bars_df: Union[pd.DataFrame, None]):
    """
    Plots OHLCV data using Plotly and displays it in a Streamlit app.

    Args:
        bars_df (pd.DataFrame): A DataFrame containing OHLCV data with at least
        the following columns: 'datetime', 'open', 'high', 'low', and 'close'.
    """
    if bars_df is None:
        return

    fig = go.Figure()

    fig.add_trace(
        go.Candlestick(
            x=bars_df["datetime"],
            open=bars_df["open"],
            high=bars_df["high"],
            low=bars_df["low"],
            close=bars_df["close"],
        )
    )

    st.plotly_chart(fig, theme="streamlit")


def plot_volume(bars_df: Union[pd.DataFrame, None]):
    """
    Plots volume data using Plotly and displays it in a Streamlit app.

    Args:
        bars_df (pd.DataFrame): A DataFrame containing OHLCV data with at least
        the following columns: 'datetime' and 'volume'.
    """
    if bars_df is None:
        return

    fig = go.Figure()

    fig.add_trace(go.Bar(x=bars_df["datetime"], y=bars_df["volume"], name="Volume"))

    st.plotly_chart(fig, theme="streamlit")


def tick_tab_section():
    """
    Streamlit tab section for fetching and displaying tick data.

    This function will create a Streamlit tab section with input fields for
    symbol, start date, end date and frequency. Upon clicking the "Get Ticks"
    button, it will fetch the tick data, display it in the Streamlit app and
    plot the tick data using Plotly.
    """
    symbol = st.text_input("Enter Symbol:")
    start_date = st.date_input(label="Starting Date:", format="YYYY-MM-DD")
    end_date = st.date_input(label="End Date:", format="YYYY-MM-DD")
    freq = int(
        st.select_slider(
            label="Frequency Selector (in secs):",
            options=[1, 15, 30] + [m * 60 for m in (1, 2, 5, 10, 15, 30, 60)],
        )
    )

    if st.button(label="Get Ticks"):
        ticks_df = get_ticks_df(symbol, f"{start_date}:{end_date}", freq)
        st.write("### TICK DATA")
        st.dataframe(ticks_df)

        st.write("### TICK Data Chart")
        plot_ticks(ticks_df)


def order_form_section():
    """
    Streamlit form section for placing an order.

    This function creates a form in a Streamlit app where users can input
    a symbol, price, and quantity to place an order. Upon clicking the
    "Send Order" button, the order details are sent to the server via a
    POST request. If the order is successfully placed, a success message
    is displayed along with the list of placed orders. If the order fails,
    an error message is shown.
    """

    symbol = st.text_input("Symbol:")
    price = st.text_input("Enter Price (in float):")
    qty = st.text_input("Enter Quantity (int int):")

    if st.button("Send Order"):
        payload = {"symbol": symbol, "price": float(price), "qty": int(qty)}

        response = requests.post(
            f"{PRIMARY_URL}/place-order/",
            json=payload,
            timeout=3600,
        )

        if response.status_code != 200:
            st.error("ORDER FAILED!\n" + response.reason)
            return

        resp_msg = response.json()
        st.success(f"[RESPONSE MSG] {resp_msg['message']}")
        with st.expander("[ORDERS LIST]", icon=":material/receipt_long:"):
            st.dataframe(
                pd.DataFrame(
                    resp_msg["orders_list"],
                    columns=["timestamp", "symbol", "price", "qty"],
                ),
            )


def sanity_tab_section():
    """
    Streamlit tab section for running sanity checks on trading data.

    This function creates a Streamlit interface where users can input a trading
    date and run quality checks on the bhavcopy data for that date. When the
    "Run Checks" button is clicked, it sends a GET request to the server to
    perform quality checks. The results, including any mismatches in volume,
    high, and low values between the bhavcopy and database data, are displayed
    in the Streamlit app. Mismatches are shown in expandable sections with
    detailed DataFrames.
    """

    trade_date = st.date_input(label="Trading Date:", format="YYYY-MM-DD")

    if st.button("Run Checks"):
        response = requests.get(
            f"{PRIMARY_URL}/quality-checks?tdate={trade_date}", timeout=3600
        )

        if response.status_code != 200:
            st.error("ORDER FAILED!\n" + response.reason)
            return

        resp_msg = response.json()
        st.success(f"[DIFFERENCE B/w BHAV & DB] {resp_msg['shape_diff']}")

        if resp_msg["volume_mismatch"] is not None:
            with st.expander("VOLUME MISMATCH"):
                st.dataframe(
                    pd.DataFrame(
                        resp_msg["volume_mismatch"][1:],
                        columns=resp_msg["volume_mismatch"][0],
                    )
                )

        if resp_msg["high_mismatch"] is not None:
            with st.expander("HIGH MISMATCH"):
                st.dataframe(
                    pd.DataFrame(
                        resp_msg["high_mismatch"][1:],
                        columns=resp_msg["high_mismatch"][0],
                    )
                )

        if resp_msg["low_mismatch"] is not None:
            with st.expander("LOW MISMATCH"):
                st.dataframe(
                    pd.DataFrame(
                        resp_msg["low_mismatch"][1:],
                        columns=resp_msg["low_mismatch"][0],
                    )
                )


def candle_tab_section():
    """
    Streamlit tab section for fetching and displaying OHLCV candles.

    This function will create a Streamlit tab section with input fields for
    symbol, start date, end date and frequency. Upon clicking the "Get Candles"
    button, it will fetch the OHLCV candles, display it in the Streamlit app and
    plot the OHLCV data using Plotly.
    """
    symbol = st.text_input("Bars Symbol:")
    start_date = st.date_input(label="First Date:", format="YYYY-MM-DD")
    end_date = st.date_input(label="Last Date:", format="YYYY-MM-DD")
    freq = int(
        st.select_slider(
            label="Freq Selector (in secs):",
            options=[1, 15, 30] + [m * 60 for m in (1, 2, 5, 10, 15, 30, 60)],
        )
    )

    if st.button(label="Get Candles"):
        bars_df = get_bars_df(symbol, f"{start_date}:{end_date}", freq)
        st.write("### BARS DATA")
        st.dataframe(bars_df)

        st.write("### CANDLES PLOT")
        plot_candlesticks(bars_df)

        st.write("### VOLUME PLOT")
        plot_volume(bars_df)


if __name__ == "__main__":
    TICK_TAB, ORDER_FORM_TAB, SANITY, CANDLES = st.tabs(
        ["GET TICK DATA", "ORDER SIMULATOR", "BHAVCOPY CHECKS", "CANDLES"]
    )

    with TICK_TAB:
        tick_tab_section()

    with ORDER_FORM_TAB:
        order_form_section()

    with SANITY:
        sanity_tab_section()

    with CANDLES:
        candle_tab_section()
