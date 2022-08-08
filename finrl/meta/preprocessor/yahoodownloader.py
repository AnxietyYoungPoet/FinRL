"""Contains methods and classes to collect data from
Yahoo Finance API
"""
from __future__ import annotations

import pandas as pd
import yfinance as yf
import os.path as osp

from finrl.config import DATA_SAVE_DIR


def fix_tick(tick):
    if tick.split('.')[-1] == 'XSHG':
        tick = tick.split('.')[0] + '.SS'
    if tick.split('.')[-1] == 'XSHE':
        tick = tick.split('.')[0] + '.SZ'
    return tick


class YahooDownloader:
    """Provides methods for retrieving daily stock data from
    Yahoo Finance API

    Attributes
    ----------
        start_date : str
            start date of the data (modified from neofinrl_config.py)
        end_date : str
            end date of the data (modified from neofinrl_config.py)
        ticker_list : list
            a list of stock tickers (modified from neofinrl_config.py)

    Methods
    -------
    fetch_data()
        Fetches data from yahoo API

    """

    def __init__(self, start_date: str, end_date: str, ticker_list: list, dataset=None):

        self.start_date = start_date
        self.end_date = end_date
        self.ticker_list = ticker_list
        self.dataset = dataset

    def fetch_data(self, proxy=None) -> pd.DataFrame:
        """Fetches data from Yahoo API
        Parameters
        ----------

        Returns
        -------
        `pd.DataFrame`
            7 columns: A date, open, high, low, close, volume and tick symbol
            for the specified stock ticker
        """
        # Download and save the data in a pandas DataFrame:
        if self.dataset is not None and osp.exists(osp.join(DATA_SAVE_DIR, self.dataset)):
            print('dataset already exists! Just load it')
            file_path = osp.join(DATA_SAVE_DIR, self.dataset)
            return pd.read_csv(file_path, index_col=0)
        data_df = pd.DataFrame()
        df_list = []
        for tic in self.ticker_list:
            temp_df = yf.download(
                fix_tick(tic), start=self.start_date, end=self.end_date, proxy=proxy
            )
            temp_df["tic"] = fix_tick(tic)
            df_list.append(temp_df)
        data_df = pd.concat(df_list)
        # reset the index, we want to use numbers as index instead of dates
        data_df = data_df.reset_index()
        try:
            # convert the column names to standardized names
            data_df.columns = [
                "date",
                "open",
                "high",
                "low",
                "close",
                "adjcp",
                "volume",
                "tic",
            ]
            # use adjusted close price instead of close price
            data_df["close"] = data_df["adjcp"]
            # drop the adjusted close price column
            data_df = data_df.drop(labels="adjcp", axis=1)
        except NotImplementedError:
            print("the features are not supported currently")
        # create day of the week column (monday = 0)
        data_df["day"] = data_df["date"].dt.dayofweek
        # convert date to standard string format, easy to filter
        data_df["date"] = data_df.date.apply(lambda x: x.strftime("%Y-%m-%d"))
        # drop missing data
        data_df = data_df.dropna()
        data_df = data_df.reset_index(drop=True)
        print("Shape of DataFrame: ", data_df.shape)
        # print("Display DataFrame: ", data_df.head())

        data_df = data_df.sort_values(by=["date", "tic"]).reset_index(drop=True)

        if self.dataset is not None:
            file_path = osp.join(DATA_SAVE_DIR, self.dataset)
            data_df.to_csv(file_path)
        return data_df

    def select_equal_rows_stock(self, df):
        df_check = df.tic.value_counts()
        df_check = pd.DataFrame(df_check).reset_index()
        df_check.columns = ["tic", "counts"]
        mean_df = df_check.counts.mean()
        equal_list = list(df.tic.value_counts() >= mean_df)
        names = df.tic.value_counts().index
        select_stocks_list = list(names[equal_list])
        df = df[df.tic.isin(select_stocks_list)]
        return df
