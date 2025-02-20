"""Module for performing sanity checks with bhavcopy"""

import os
import sys
import zipfile
from pathlib import Path
from datetime import datetime

import pandas as pd

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

from database.dbutils import PGUtils


BHAV_PATH = Path.cwd() / "data" / "bhavcopy"


def get_bhavcopy(tdate: str) -> pd.DataFrame:
    """
    Retrieves the bhavcopy data for the given date from the bhavcopy zip files.

    Parameters
    ----------
    tdate : str
        Date in 'YYYY-MM-DD' format

    Returns
    -------
    pd.DataFrame
        Dataframe containing the bhavcopy data
    """

    bhav_date = datetime.strptime(tdate, "%Y-%m-%d").strftime("%d%b%Y").upper()
    bhavcopy_zip_path = os.path.join(
        BHAV_PATH,
        f"EODSNAPSHOT_{bhav_date}bhav.csv.zip",
    )

    with zipfile.ZipFile(bhavcopy_zip_path, "r") as zip_ref:
        bhavcopy_file = zip_ref.filelist[0]
        df = pd.read_csv(zip_ref.open(bhavcopy_file))

    return df


def get_ohlcv(tdate: str) -> pd.DataFrame:
    """
    Retrieves the OHLCV (Open, High, Low, Close, Volume) data for all symbols for the given date
    from the database and returns it as a DataFrame.

    Parameters
    ----------
    tdate : str
        Date in 'YYYY-MM-DD' format

    Returns
    -------
    pd.DataFrame
        Dataframe containing the OHLCV data
    """
    pgutils = PGUtils()
    return pgutils.get_ohlcv(symbol="", date_range=tdate, frequency=86400)


def compare_data(bhav_df: pd.DataFrame, ohlcv_df: pd.DataFrame) -> dict:
    """
    Compares the bhavcopy data with the ohlcv data and returns a dictionary
    containing the differences.

    The dictionary contains the following keys:
    - shape_diff: The difference in the number of rows between the bhavcopy
      data and the ohlcv data.
    - volume_mismatch: A DataFrame containing the ticker, datetime, and volume
      columns for the rows where the volume in the bhavcopy data does not match
      the volume in the ohlcv data.
    - high_mismatch: A DataFrame containing the ticker, datetime, and high columns
      for the rows where the high in the bhavcopy data is less than the high in
      the ohlcv data.
    - low_mismatch: A DataFrame containing the ticker, datetime, and low columns
      for the rows where the low in the bhavcopy data is greater than the low in
      the ohlcv data.

    If there are no mismatches, the corresponding values in the dictionary will be
    None.

    Parameters
    ----------
    bhav_df : pd.DataFrame
        The bhavcopy data
    ohlcv_df : pd.DataFrame
        The ohlcv data

    Returns
    -------
    dict
        A dictionary containing the differences between the bhavcopy data and the
        ohlcv data
    """
    bhav_df.columns = bhav_df.columns.str.lower()
    bhav_df["datetime"] = pd.to_datetime(bhav_df["timestamp"], format="%d-%b-%Y")
    bhav_df["ticker"] = bhav_df[["symbol", "series"]].apply(
        lambda row: (
            row["symbol"]
            if row["series"] == "EQ"
            else row["symbol"] + "." + str(row["series"]).upper()[:2]
        ),
        axis=1,
    )
    bhav_df = bhav_df[
        ["datetime", "ticker", "open", "high", "low", "close", "tottrdqty"]
    ].rename(columns={"tottrdqty": "volume"})

    merged_df = pd.merge(
        bhav_df, ohlcv_df, on=["datetime", "ticker"], how="outer", suffixes=("_b", "_o")
    )

    volume_mismatch = merged_df[merged_df["volume_b"] != merged_df["volume_o"]]
    if not volume_mismatch.empty:
        volume_mismatch = [["datetime", "ticker", "volume_b", "volume_o"]] + list(
            [
                row[1:]
                for row in volume_mismatch[
                    ["datetime", "ticker", "volume_b", "volume_o"]
                ].itertuples()
            ]
        )
    else:
        volume_mismatch = None

    high_mismatch = merged_df[merged_df["high_b"] < merged_df["high_o"]]
    if not high_mismatch.empty:
        high_mismatch = [["datetime", "ticker", "high_b", "high_o"]] + list(
            [
                row[1:]
                for row in high_mismatch[
                    ["datetime", "ticker", "high_b", "high_o"]
                ].itertuples()
            ]
        )
    else:
        high_mismatch = None

    low_mismatch = merged_df[merged_df["low_b"] > merged_df["low_o"]]
    if not low_mismatch.empty:
        low_mismatch = [["datetime", "ticker", "low_b", "low_o"]] + list(
            [
                row[1:]
                for row in low_mismatch[
                    ["datetime", "ticker", "low_b", "low_o"]
                ].itertuples()
            ]
        )
    else:
        low_mismatch = None

    shape_diff = bhav_df.shape[0] - ohlcv_df.shape[0]

    return {
        "shape_diff": shape_diff,
        "volume_mismatch": volume_mismatch,
        "high_mismatch": high_mismatch,
        "low_mismatch": low_mismatch,
    }


def run_bhav_checks(tdate: str) -> dict:
    """Main Function to run all the checks"""

    bhav_df = get_bhavcopy(tdate)
    ohlcv_df = get_ohlcv(tdate)

    return compare_data(bhav_df, ohlcv_df)


if __name__ == "__main__":
    run_bhav_checks("2022-04-04")
