"DB Connectors Module"

from abc import ABC, abstractmethod
from datetime import datetime

import psycopg2
import psycopg2.extras
import pandas as pd


class DBInterface(ABC):
    """Interface for implementing Database connector"""

    @abstractmethod
    def __init__(self, host: str, port: str, user: str, password: str):
        self.connect_params = dict(host=host, port=port, user=user, password=password)

    @abstractmethod
    def _create_tbt_table(self):
        """Method for TBT Table creation"""

    @abstractmethod
    def insert_df_to_tbt(self, data_df: pd.DataFrame):
        """Method for Data Insertion in TBT Table"""

    @abstractmethod
    def get_tick_data(
        self, symbol: str, date_range: str, frequency: int = 1
    ) -> pd.DataFrame:
        """Method to query the tick data"""


class PGUtils(DBInterface):
    """Postgres DB Connector"""

    def __init__(
        self,
        host: str = "localhost",
        port: str = "5432",
        user: str = "postgres",
        password: str = "mysecretpassword",
    ):
        """
        Initializes the PGUtils class with connection parameters.

        Args:
            host (str):
                The hostname of the database server. Defaults to 'localhost'.
            port (str):
                The port number to connect to the database server. Defaults to '5432'.
            user (str):
                The username used to authenticate with the database. Defaults to 'postgres'.
            password (str):
                The password used to authenticate with the database. Defaults to 'mysecretpassword'.
        """

        self.connect_params = dict(host=host, port=port, user=user, password=password)
        self.conn = None
        self.cursor = None

        self._get_connection()
        self._create_tbt_table()

    def __del__(self):
        """
        Destructor for the class to close the connection when the object exceeds it's scope,
        this ensures no extra or unintended connections are open.
        """
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    def _get_connection(self):
        """
        Establishes a connection to the PostgreSQL database using the provided
        connection parameters and initializes a cursor for executing database
        operations.
        """

        self.conn = psycopg2.connect(**self.connect_params)
        self.cursor = self.conn.cursor()

    def _create_tbt_table(self):
        """
        Method for TBT Table creation

        Creates a table in the connected database with the specified columns
        if the table does not already exist.

        If the table is created, prints a success message.
        """
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS tbt (
                "ID"            SERIAL PRIMARY KEY,
                "Datetime"      TIMESTAMP NOT NULL,
                "Ticker"        VARCHAR(20) NOT NULL,
                "LTP"           DOUBLE PRECISION,
                "BuyPrice"      DOUBLE PRECISION,
                "BuyQty"        INTEGER,
                "SellPrice"     DOUBLE PRECISION,
                "SellQty"       INTEGER,
                "LTQ"           INTEGER,
                "OpenInterest"  INTEGER
            );
            """
        )
        self.conn.commit()
        # print("Table created successfully")

    def _extract_dates(self, date_range: str) -> tuple[str]:
        """
        Helper function to extract the start and end date from a given date range
        string.

        Parameters
        ----------
        date_range : str
            A string in the format 'start_date:end_date' or 'start_date:' or ':end_date',
            where start_date and end_date are in the format 'yyyy-mm-dd'.

        Returns
        -------
        tuple[str]
            A tuple containing the start and end date in the format 'yyyy-mm-dd'.
        """
        if date_range == "" or date_range is None:
            date_range = datetime.now().strftime("%Y-%m-%d")

        date_range_split = date_range.split(":")
        if len(date_range_split) == 1:
            start_date = end_date = date_range_split[0]
        else:
            start_date = date_range_split[0]
            end_date = date_range_split[1]

        return start_date, end_date

    def _query_ticks(self, symbol: str, date_range: str) -> pd.DataFrame:
        """
        Queries the 'tbt' table in the database for the given symbol and date range.

        Parameters
        ----------
        symbol : str
            The symbol for which to fetch the tick data.
        date_range : str
            The date range in the format 'start_date:end_date' or 'start_date:' or ':end_date',
            where start_date and end_date are in the format 'yyyy-mm-dd'.

        Returns
        -------
        pd.DataFrame
            A DataFrame containing the queried tick data with columns 'Datetime', 'Ticker',
            'LTP', 'LTQ', 'BuyPrice', 'BuyQty', 'SellPrice', 'SellQty', and 'OpenInterest'.
        """
        # print(f"Fetching data for {symbol} from {date_range}")

        start_date, end_date = self._extract_dates(date_range)
        symbol_condition = ""
        if symbol != "" and symbol is not None:
            symbol_condition = f"\nAND \"Ticker\" = '{symbol}'"

        self.cursor.execute(
            f"""
                SELECT
                    "Datetime",
                    "Ticker",
                    "LTP",
                    "LTQ",
                    "BuyPrice",
                    "BuyQty",
                    "SellPrice",
                    "SellQty",
                    "OpenInterest"
                FROM tbt
                WHERE
                    "Datetime"::date BETWEEN '{start_date}' AND '{end_date}'
                    {symbol_condition}
                ORDER BY "Datetime"
            """
        )
        rows = self.cursor.fetchall()
        return pd.DataFrame(rows, columns=[desc[0] for desc in self.cursor.description])

    def insert_df_to_tbt(self, data_df: pd.DataFrame):
        """
        Inserts data from a pandas DataFrame into the 'tbt' table in the database.

        Parameters
        ----------
        data_df : pd.DataFrame
            DataFrame containing the data to be inserted. The DataFrame should have
            columns corresponding to the 'tbt' table: "Datetime", "Ticker", "LTP",
            "BuyPrice", "BuyQty", "SellPrice", "SellQty", "LTQ", and "OpenInterest".

        Raises
        ------
        Exception
            If data insertion fails, an error message is printed and the transaction
            is rolled back.
        """

        data = list(data_df.itertuples(index=False, name=None))
        insertion_query = """
            INSERT INTO tbt ("Datetime", "Ticker", "LTP", "BuyPrice", "BuyQty", "SellPrice", "SellQty", "LTQ", "OpenInterest")
            VALUES %s;
        """
        self.conn.autocommit = True

        try:
            self._create_tbt_table()
            psycopg2.extras.execute_values(self.cursor, insertion_query, data)
            self.conn.commit()
            print("Data inserted successfully")
        except Exception as error:
            self.conn.rollback()
            print("Data insertion failed:", error)

    def get_tick_data(
        self, symbol: str, date_range: str, frequency: int = 1
    ) -> pd.DataFrame:
        """
        Retrieves tick data for a given symbol and date range from the 'tbt' table
        in the database and resamples it to the specified frequency.

        Parameters
        ----------
        symbol : str
            The symbol for which to fetch the tick data.
        date_range : str
            The date range in the format "YYYY-MM-DD:YYYY-MM-DD".
        frequency : int, optional
            The frequency of the tick data in seconds. Defaults to 1.

        Returns
        -------
        pd.DataFrame
            A DataFrame containing the resampled tick data with columns
            'datetime', 'ticker', 'ltp', 'ltq', 'buy_price', 'buy_qty',
            'sell_price', 'sell_qty', and 'open_interest'.
        """

        queried_data = self._query_ticks(symbol, date_range)
        queried_data.set_index("Datetime", inplace=True)

        resampled_data = (
            queried_data.resample(f"{frequency}s")
            .agg(
                ticker=pd.NamedAgg(column="Ticker", aggfunc="last"),
                ltp=pd.NamedAgg(column="LTP", aggfunc="last"),
                ltq=pd.NamedAgg(column="LTQ", aggfunc="sum"),
                buy_price=pd.NamedAgg(column="BuyPrice", aggfunc="last"),
                buy_qty=pd.NamedAgg(column="BuyQty", aggfunc="last"),
                sell_price=pd.NamedAgg(column="SellPrice", aggfunc="last"),
                sell_qty=pd.NamedAgg(column="SellQty", aggfunc="last"),
                open_interest=pd.NamedAgg(column="OpenInterest", aggfunc="last"),
            )
            .reset_index()
            .dropna()
        ).astype(
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
        resampled_data.columns = resampled_data.columns.str.lower()

        return resampled_data

    def get_ohlcv(
        self, symbol: str, date_range: str, frequency: int = 1
    ) -> pd.DataFrame:
        """
        Retrieves the OHLCV (Open, High, Low, Close, Volume) data for a given
        symbol and date range from the 'tbt' table in the database and resamples
        it to the specified frequency.

        Parameters
        ----------
        symbol : str
            The symbol for which to fetch the OHLCV data.
        date_range : str
            The date range in the format "YYYY-MM-DD:YYYY-MM-DD".
        frequency : int
            The frequency of the OHLCV data in seconds.

        Returns
        -------
        pd.DataFrame
            A DataFrame containing the resampled OHLCV data with columns
            'ticker', 'open', 'high', 'low', 'close', and 'volume'.
        """
        queried_data = self._query_ticks(symbol, date_range)
        # print(queried_data)
        queried_data.set_index("Datetime", inplace=True)

        candles = []
        for _, group_df in queried_data.groupby(["Ticker"]):
            resampled_df = (
                group_df.resample(f"{frequency}s")
                .agg(
                    ticker=pd.NamedAgg(column="Ticker", aggfunc="last"),
                    open=pd.NamedAgg(column="LTP", aggfunc="first"),
                    high=pd.NamedAgg(column="LTP", aggfunc="max"),
                    low=pd.NamedAgg(column="LTP", aggfunc="min"),
                    close=pd.NamedAgg(column="LTP", aggfunc="last"),
                    volume=pd.NamedAgg(column="LTQ", aggfunc="sum"),
                )
                .reset_index()
                .dropna()
                .astype(
                    {
                        "open": float,
                        "high": float,
                        "low": float,
                        "close": float,
                        "volume": int,
                    }
                )
            )

            candles.append(resampled_df)

        candles_df = pd.concat(candles).reset_index(drop=True)
        candles_df.columns = candles_df.columns.str.lower()

        return candles_df
