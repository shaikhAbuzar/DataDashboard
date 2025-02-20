"""Data Ingestion Module"""

import os
import sys
import zipfile
from pathlib import Path
from typing import Generator
from datetime import datetime

import pandas as pd

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

from database import dbutils


class Ingester:
    """
    Class for the data ingestion pipeline
    """

    def __init__(self):
        self.data_path = Path.cwd() / "data"
        self.tbt_data_path = self.data_path / "tbt"

        self.pgutils = dbutils.PGUtils()

    def unzip_tbt(self, tdate: str) -> Generator:
        """
        Generator to unzip TBT data from a given date and extract tick data

        Parameters
        ----------
        tdate : str
            Date in 'dd-mm-yyyy' format

        Yields
        ------
        pd.DataFrame
            Dataframe containing tick data for each ticker
        """
        zip_file = (
            self.tbt_data_path / f"STOCK_TICK_{''.join(tdate.split('-')[::-1])}.zip"
        )
        # print(f"[RUNNING] {zip_file}")

        if not os.path.exists(zip_file):
            print(f"File {zip_file} does not exist")
            return

        with zipfile.ZipFile(zip_file, "r") as zip_ref:
            for ticker_file in zip_ref.infolist():
                if not ticker_file.filename.endswith(".csv"):
                    continue

                # print(f"Extracting {ticker_file}")
                ticker_df = pd.read_csv(zip_ref.open(ticker_file))
                ticker_df["Datetime"] = pd.to_datetime(
                    ticker_df["Date"] + " " + ticker_df["Time"],
                    format="%d/%m/%Y %H:%M:%S",
                )
                ticker_df.drop(columns=["Date", "Time"], inplace=True)
                ticker_df["Ticker"] = ticker_df["Ticker"].str.replace(".NSE", "")

                yield ticker_df[
                    [
                        "Datetime",
                        "Ticker",
                        "LTP",
                        "BuyPrice",
                        "BuyQty",
                        "SellPrice",
                        "SellQty",
                        "LTQ",
                        "OpenInterest",
                    ]
                ]

    def ingest_tbt_data(self, tdate: str):
        """
        Ingests data from a given date from the TBT zip files and inserts it into the tbt table

        Parameters
        ----------
        tdate : str
            Date in 'dd-mm-yyyy' format
        """
        for tbt_df in self.unzip_tbt(tdate=tdate):
            self.pgutils.insert_df_to_tbt(tbt_df)


if __name__ == "__main__":
    trade_date = sys.argv[1]

    try:
        datetime.strptime(trade_date, "%Y-%m-%d")
    except ValueError:
        print("Invalid date format. Please provide date in 'YYYY-MM-DD' format")
        sys.exit(1)
    except TypeError:
        print("Invalid date type, please provide a valid date string")
        sys.exit(1)

    ingester = Ingester()
    ingester.ingest_tbt_data(trade_date)
