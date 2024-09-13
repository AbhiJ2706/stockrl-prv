import os

from dateutil.parser import parse as date_parse

import logging


logger = logging.getLogger('server.sub')


def try_fetch(metric, metric_name, reverse=False):
    global logger
    try:
        data = metric()
        data.index = data.index.map(lambda x: str(date_parse(str(x)).date()))
        return data.iloc[::-1].iloc[-100:].iloc[::(1 if reverse else -1)]
    except Exception as e:
        logger.error(f"fetching {metric_name} failed with error {e}")
        return None


def fetch_data(symbol, metric, metric_name, reverse=False):
    global logger

    directory = symbol.lower()

    if not os.path.exists("data"):
        os.makedirs("data")
    if not os.path.exists(f"data/{symbol.lower()}"):
        os.makedirs(f"data/{symbol.lower()}")

    logger.debug(f"directory data/{directory} does not exist")
    data = try_fetch(metric, metric_name, reverse)
    return data, True
