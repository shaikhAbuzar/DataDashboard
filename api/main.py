"""The API Module"""

import os
import sys
from typing import Generator
from datetime import datetime

from pydantic import BaseModel

from fastapi import FastAPI
from fastapi.responses import StreamingResponse

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

from database.dbutils import PGUtils
from sanity.bhavcopy import run_bhav_checks

PLACED_ORDERS = []


class OrderMsg(BaseModel):
    """
    A Pydantic model for representing an order message.
    """

    symbol: str
    price: float
    qty: int


app = FastAPI(title="Tick Data API")
pgutils = PGUtils()


def stream_ticks(symbol: str, date_range: str, frequency: int = 1) -> Generator:
    """
    Stream the tick data for a given symbol and date range.

    Args:
        symbol (str): The symbol for which to fetch the tick data.
        date_range (str): The date range in the format "YYYY-MM-DD:YYYY-MM-DD".
        frequency (int, optional): The frequency of the tick data. Defaults to 1.

    Yields:
        Generator: A generator which yields a comma-separated string for each tick.
    """
    tick_data = pgutils.get_tick_data(
        symbol=symbol, date_range=date_range, frequency=frequency
    )
    # print(tick_data.shape)
    yield ",".join(tick_data.columns) + "\n"

    for tick in tick_data.itertuples():
        # print(tick)
        yield ",".join(map(str, tick[1:])) + "\n"


def stream_bars(symbol: str, date_range: str, frequency: int = 1) -> Generator:
    """
    Stream the OHLCV (Open, High, Low, Close, Volume) candles for a given symbol and date range.

    Args:
        symbol (str): The symbol for which to fetch the OHLCV candles.
        date_range (str): The date range in the format "YYYY-MM-DD:YYYY-MM-DD".
        frequency (int, optional): The frequency of the OHLCV candles. Defaults to 1.

    Yields:
        Generator: A generator which yields a comma-separated string for each candle.
    """
    candles = pgutils.get_ohlcv(
        symbol=symbol, date_range=date_range, frequency=frequency
    )

    yield ",".join(candles.columns) + "\n"

    for candle in candles.itertuples():
        yield ",".join(map(str, candle[1:])) + "\n"


@app.get("/ticks")
def get_ticks(symbol: str, date_range: str, frequency: int = 1) -> Generator:
    """
    Fetch the tick data for a given symbol and date range.

    Args:
        symbol (str): The symbol for which to fetch the tick data.
        date_range (str): The date range in the format "YYYY-MM-DD:YYYY-MM-DD".
        frequency (int, optional): The frequency of the tick data. Defaults to 1.

    Returns:
        StreamingResponse: A streaming response which yields a comma-separated string for each tick.
    """
    return StreamingResponse(
        stream_ticks(symbol, date_range, frequency), media_type="text/csv"
    )


@app.get("/ohlcv")
def get_ohlc(symbol: str, date_range: str, frequency: int = 1) -> Generator:
    """
    Fetch the OHLCV (Open, High, Low, Close, Volume) data for a given symbol and date range.

    Args:
        symbol (str): The symbol for which to fetch the OHLCV data.
        date_range (str): The date range in the format "YYYY-MM-DD:YYYY-MM-DD".
        frequency (int, optional): The frequency of the OHLCV data. Defaults to 1.

    Yields:
        Generator: A generator which yields a comma-separated string for each OHLCV candle.
    """
    return StreamingResponse(
        stream_bars(symbol, date_range, frequency), media_type="text/csv"
    )


@app.post("/place-order")
def place_order(ordermsg: OrderMsg) -> dict:
    """
    Place an order with the given symbol, price, and quantity.

    Args:
        symbol (str): The symbol for which to place the order.
        price (float): The price at which to place the order.
        qty (int): The quantity for the order.

    Returns:
        str: A string containing a success message with the order details.
    """
    symbol, price, qty = ordermsg.symbol, ordermsg.price, ordermsg.qty

    PLACED_ORDERS.append((datetime.now(), symbol, price, qty))

    return {
        "message": f"[SUCCESS] Symbol: {symbol}, Price: {price}, Quantity: {qty}s",
        "orders_list": PLACED_ORDERS,
    }


@app.get("/quality-checks")
def quality_checks(tdate: str) -> dict:
    """
    Run quality checks on the bhavcopy data for a given date.

    Args:
        tdate (str): The date in 'YYYY-MM-DD' format.

    Returns:
        dict: A dictionary containing the results of the quality checks.
    """
    results = run_bhav_checks(tdate)
    return results
