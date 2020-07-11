from config import AlphaVantage
import requests
from datetime import datetime
from pandas.tseries.offsets import MonthEnd
import pandas as pd


def get_ticker_tradingdays(source='av', ticker='IBM', start_date='1900-01-01', end_date='2100-12-31', api_key=None):
    if source == 'av':
        if not api_key:
            api_key = AlphaVantage.AV_KEY

        request = dict(url='https://www.alphavantage.co/query',
                       params={'function': 'TIME_SERIES_DAILY',
                               'symbol': ticker,
                               'outputsize': 'full',
                               'apikey': api_key})

        response = requests.get(**request)
        ticker_time_series = response.json()

        try:
            ticker_time_series['Meta Data']
        except KeyError as e:
            print(f'error: improper response object from alpha vantage for ticker_proxy = {ticker}')
            raise

        ticker_trading_dates = list(ticker_time_series['Time Series (Daily)'].keys())
        ticker_trading_dates = [datetime.strptime(d, '%Y-%m-%d') for d in ticker_trading_dates
                                if start_date <= d <= end_date]
        return ticker_trading_dates

    else:
        raise Exception(f'input parameter {source} is not supported')


def get_tradingday_monthends(source='av', ticker='IBM', start_date='1900-01-01', end_date='2100-12-31', api_key=None):
    # in theory, the ticker here could be the ETF ticker (counter: IBM only needs to be called once)
    # also could use 'BMonthEnd()' offset from pandas or market-calendar extension
    trading_days = get_ticker_tradingdays(source=source, ticker=ticker,
                                          start_date=start_date, end_date=end_date, api_key=api_key)
    trading_days_df = pd.DataFrame({'trading_day': trading_days,
                                    'month_end': [td + MonthEnd(0) for td in trading_days]})
    trading_days_df_me = trading_days_df.groupby('month_end').max().reset_index()

    # remove cases where month is not yet over
    trading_days_me = trading_days_df_me.sort_values(by='trading_day')['trading_day'].to_list()[:-1]

    return trading_days_me
