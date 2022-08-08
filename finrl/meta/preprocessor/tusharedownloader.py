"""Contains methods and classes to collect data from
tushare API
"""
from __future__ import annotations

import pandas as pd
import tushare as ts
from tqdm import tqdm
import os.path as osp

from finrl.config import DATA_SAVE_DIR


def date_format(date_string):
    if '-' in date_string:
        date_string = date_string.replace('-', '')
    return date_string


def fix_tick(tick):
    if tick.split('.')[-1] == 'XSHG':
        tick = tick.split('.')[0] + '.SH'
    if tick.split('.')[-1] == 'XSHE':
        tick = tick.split('.')[0] + '.SZ'
    return tick


class TushareDownloader:
    """Provides methods for retrieving daily stock data from
    tushare API
    Attributes
    ----------
        start_date : str
            start date of the data (modified from config.py)
        end_date : str
            end date of the data (modified from config.py)
        ticker_list : list
            a list of stock tickers (modified from config.py)
    Methods
    -------
    fetch_data()
        Fetches data from tushare API
    date：date
    Open: opening price
    High: the highest price
    Close: closing price
    Low: lowest price
    Volume: volume
    Price_change: price change
    P_change: fluctuation
    ma5: 5-day average price
    Ma10: 10 average daily price
    Ma20:20 average daily price
    V_ma5:5 daily average
    V_ma10:10 daily average
    V_ma20:20 daily average
    """

    def __init__(self, start_date: str, end_date: str, ticker_list: list, dataset=None):

        self.start_date = date_format(start_date)
        self.end_date = date_format(end_date)
        self.ticker_list = ticker_list
        self.dataset = dataset
        # self.pro = ts.pro_api()

    def fetch_data(self) -> pd.DataFrame:
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
        for tic in tqdm(self.ticker_list, total=len(self.ticker_list)):
            # print(tic, self.start_date, self.end_date)
            temp_df = ts.pro_bar(
                ts_code=fix_tick(tic), adj='qfq', start_date=self.start_date, end_date=self.end_date
            )
            temp_df["tic"] = fix_tick(tic)
            df_list.append(temp_df)
        data_df = pd.concat(df_list, ignore_index=True)
        # print(data_df.head())

        # create day of the week column (monday = 0)
        data_df = data_df.drop(
            [
                "pre_close",
                "change",
                "pct_chg",
                "amount",
                "ts_code"
                # "ma5",
                # "ma10",
                # "ma20",
                # "v_ma5",
                # "v_ma10",
                # "v_ma20",
            ],
            1,
        )
        data_df["day"] = pd.to_datetime(data_df["trade_date"]).dt.dayofweek
        # rank desc
        data_df = data_df.sort_index(axis=0, ascending=False)
        data_df = data_df.reset_index(drop=True)
        # convert date to standard string format, easy to filter
        data_df["date"] = pd.to_datetime(data_df["trade_date"])
        data_df["date"] = data_df.date.apply(lambda x: x.strftime("%Y-%m-%d"))
        data_df = data_df.drop(
            [
                "trade_date",
            ],
            1,
        )
        # drop missing data
        data_df = data_df.dropna()
        print("Shape of DataFrame: ", data_df.shape)
        # print("Display DataFrame: ", data_df.head())
        print(data_df)
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
