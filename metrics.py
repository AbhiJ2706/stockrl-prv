import numpy as np

from dateutil.parser import parse as date_parse

import logging

from read_data import fetch_data, rotate_tech_indicators, rotate_time_series


logger = logging.getLogger('server.sub')


@rotate_time_series
def get_historical_data(symbol, ts=None):
    historical_data, write = fetch_data(symbol, lambda: ts.get_daily(
        symbol=symbol, outputsize='full'), "historical_data")

    if historical_data is None:
        return

    historical_data.index = historical_data.index.map(
        lambda x: str(date_parse(x).date()))
    if write:
        historical_data.to_csv(f"data/{symbol.lower()}/historical_data.csv")

    return historical_data


@rotate_tech_indicators
def get_rsi(symbol, ti=None):
    rsi, write = fetch_data(symbol, lambda: ti.get_rsi(symbol=symbol, interval='daily',
                                                       time_period=14), "rsi")

    if rsi is None:
        return

    rsi.index = rsi.index.map(lambda x: str(date_parse(x).date()))
    if write:
        rsi.to_csv(f"data/{symbol.lower()}/rsi.csv")

    return rsi


@rotate_tech_indicators
def get_adx(symbol, ti=None):
    adx, write = fetch_data(symbol, lambda: ti.get_adx(symbol=symbol, interval='daily',
                                                       time_period=14), "adx")

    if adx is None:
        return

    adx.index = adx.index.map(lambda x: str(date_parse(x).date()))
    if write:
        adx.to_csv(f"data/{symbol.lower()}/adx.csv")

    return adx


@rotate_tech_indicators
def get_cci(symbol, ti=None):
    cci, write = fetch_data(symbol, lambda: ti.get_cci(symbol=symbol, interval='daily',
                                                       time_period=14), "cci")

    if cci is None:
        return

    cci.index = cci.index.map(lambda x: str(date_parse(x).date()))
    if write:
        cci.to_csv(f"data/{symbol.lower()}/cci.csv")

    return cci


# @rotate_time_series
# def get_sharpe_ratio(symbol, rfr=0.05, ts=None):

#     data, write = fetch_data(symbol, lambda: ts.get_daily(
#         symbol=symbol, outputsize='full'), "historical_data")['4. close']
#     daily_returns = data.pct_change().dropna()

#     excess_returns = daily_returns - rfr / 252
#     sharpe_ratio = excess_returns.mean() / excess_returns.std() * np.sqrt(252)

#     if write: sharpe_ratio.to_csv(f"data/{symbol.lower()}/sharpe_ratio.csv")

#     return sharpe_ratio


# # @rotate_time_series
# # def get_percentage_return(symbol, ts=None):

# #     data, write = fetch_data(symbol, lambda: ts.get_daily(
# #         symbol=symbol, outputsize='full'), "historical_data")['4. close']

# #     daily_returns = data.pct_change().dropna() * 100

# #     if write: daily_returns.to_csv(f"data/{symbol.lower()}/percent_return.csv")

# #     return daily_returns
