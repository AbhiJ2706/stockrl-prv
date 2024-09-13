import numpy as np

from dateutil.parser import parse as date_parse

import logging

from read_data import fetch_data

from TimeSeries import TimeSeries
from TechIndicators import TechIndicators


logger = logging.getLogger('server.sub')


TIME_SERIES = TimeSeries()
TECH_INDICATORS = TechIndicators()


def get_historical_data(symbol):
    global TIME_SERIES
    historical_data, write = fetch_data(symbol, lambda: TIME_SERIES.get_daily(symbol), "historical_data")

    if historical_data is None:
        return

    historical_data.index = historical_data.index.map(lambda x: str(date_parse(x).date()))
    if write:
        historical_data.to_csv(f"data/{symbol.lower()}/historical_data.csv")

    return historical_data


def get_rsi(symbol):
    global TECH_INDICATORS
    rsi, write = fetch_data(symbol, lambda: TECH_INDICATORS.get_rsi(symbol), "rsi")

    if rsi is None:
        return

    rsi.index = rsi.index.map(lambda x: str(date_parse(x).date()))
    if write:
        rsi.to_csv(f"data/{symbol.lower()}/rsi.csv")

    return rsi


def get_adx(symbol):
    global TECH_INDICATORS
    adx, write = fetch_data(symbol, lambda: TECH_INDICATORS.get_adx(symbol), "adx")

    if adx is None:
        return

    adx.index = adx.index.map(lambda x: str(date_parse(x).date()))
    if write:
        adx.to_csv(f"data/{symbol.lower()}/adx.csv")

    return adx


def get_cci(symbol):
    global TECH_INDICATORS
    cci, write = fetch_data(symbol, lambda: TECH_INDICATORS.get_cci(symbol), "cci")

    if cci is None:
        return

    cci.index = cci.index.map(lambda x: str(date_parse(x).date()))
    if write:
        cci.to_csv(f"data/{symbol.lower()}/cci.csv")

    return cci


def get_macd(symbol):
    global TECH_INDICATORS
    macd, write = fetch_data(symbol, lambda: TECH_INDICATORS.get_macd(symbol), "macd")

    if macd is None:
        return

    macd.index = macd.index.map(lambda x: str(date_parse(x).date()))
    if write:
        macd.to_csv(f"data/{symbol.lower()}/macd.csv")

    return macd


def get_macd_signal(symbol):
    global TECH_INDICATORS
    macd, write = fetch_data(symbol, lambda: TECH_INDICATORS.get_macd_signal(symbol), "macd_signal")

    if macd is None:
        return

    macd.index = macd.index.map(lambda x: str(date_parse(x).date()))
    if write:
        macd.to_csv(f"data/{symbol.lower()}/macd_signal.csv")

    return macd


def get_ema(symbol):
    global TECH_INDICATORS
    ema, write = fetch_data(symbol, lambda: TECH_INDICATORS.get_ema(symbol), "ema")

    if ema is None:
        return

    ema.index = ema.index.map(lambda x: str(date_parse(x).date()))
    if write:
        ema.to_csv(f"data/{symbol.lower()}/ema.csv")

    return ema


if __name__ == "__main__":
    get_historical_data("amzn")