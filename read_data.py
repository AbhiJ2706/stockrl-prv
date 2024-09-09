import os
import time

from alpha_vantage.timeseries import TimeSeries
from alpha_vantage.techindicators import TechIndicators

import pandas as pd

from datetime import datetime
from dateutil.parser import parse as date_parse

import logging


logger = logging.getLogger('server.sub')


API_KEYS = ["BDDPYHV1TYLC6RJ0", "6JYIGGZ6H082EIQF",
            "5H6V7NS9OBC9HM6N"]  # os.environ["ALPHAVANTAGE_KEY"]
TIME_SERIES = [TimeSeries(key=key, output_format='pandas') for key in API_KEYS]
TECH_INDICATORS = [TechIndicators(
    key=key, output_format='pandas') for key in API_KEYS]


def rotate_time_series(func):
    def wrapper_func(*args, **kwargs):
        global TIME_SERIES
        for ts in TIME_SERIES:
            kwargs["ts"] = ts
            data = func(*args, **kwargs)
            if data is not None:
                return data
    return wrapper_func


def rotate_tech_indicators(func):
    def wrapper_func(*args, **kwargs):
        global TECH_INDICATORS
        for ti in TECH_INDICATORS:
            kwargs["ti"] = ti
            data = func(*args, **kwargs)
            if data is not None:
                return data
    return wrapper_func


def try_fetch_csv(symbol, metric_name, reverse=False):
    global logger
    try:
        logger.debug(f"{metric_name} returned from csv")
        data = pd.read_csv(f"data/{symbol}/{metric_name}.csv")
        data.index = data["date"].apply(
            lambda x: str(date_parse(str(x)).date()))
        del data["date"]
        return data.iloc[::-1].iloc[-100:].iloc[::(1 if reverse else -1)]
    except Exception as e:
        logger.error(f"fetching {metric_name} from csv failed with error {e}")
        return None


def try_fetch_alphavantage(metric, metric_name, reverse=False):
    global logger
    try:
        data, _ = metric()
        logger.debug(f"{metric_name} returned from alphavantage")
        data.index = data.index.map(lambda x: str(
            date_parse(str(x)).date()))
        return data.iloc[::-1].iloc[-100:].iloc[::(1 if reverse else -1)]
    except Exception as e:
        logger.error(
            f"fetching {metric_name} from alphavantage failed with error {e}")
        return None


def fetch_data(symbol, metric, metric_name, reverse=False):
    global logger

    directory = symbol.lower()

    if os.path.isdir(f"data/{directory}"):
        if os.path.exists(f"data/{directory}/{metric_name}.csv"):
            modified_date = date_parse(time.ctime(
                os.path.getmtime(f"data/{directory}/{metric_name}.csv"))).date()

            time_now = datetime.now().date()

            if time_now > modified_date and not time_now.weekday() >= 5:
                logger.debug(
                    f"stock {symbol} is out-of-date ({time_now} > {modified_date})")
                return try_fetch_alphavantage(metric, metric_name, reverse), True

            logger.debug(f"stock {symbol} is up-to-date")
            return try_fetch_csv(symbol, metric_name, reverse), False

        logger.debug(f"metric {metric_name} is missing for stock {symbol}")
        return try_fetch_alphavantage(metric, metric_name, reverse), True

    logger.debug(f"directory data/{directory} does not exist")
    return try_fetch_alphavantage(metric, metric_name, reverse), True
