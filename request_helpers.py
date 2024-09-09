import finnhub

import json
import os
import re

from inspect import getmembers, isfunction
from dateutil.parser import parse as date_parse

import pandas as pd

import metrics

import logging


logger = logging.getLogger('server.sub')


def retrieve_and_store_data(func, symbol):

    if not os.path.exists("data"):
        os.makedirs("data")
    if not os.path.exists(f"data/{symbol.lower()}"):
        os.makedirs(f"data/{symbol.lower()}")

    func(symbol)


def retrieve_snapshot(symbol):
    if not os.path.exists("data"):
        os.makedirs("data")
    if not os.path.exists(f"data/{symbol.lower()}"):
        os.makedirs(f"data/{symbol.lower()}")

    logging.debug(f"beginning snapshot for {symbol}")

    snapshot = {}
    pattern = re.compile('[\W]+')

    for (metric_name, metric) in getmembers(metrics, isfunction):
        logging.debug(f"retrieving {metric_name}")

        if metric_name.startswith("get"):
            retrieve_and_store_data(metric, symbol)

            name = metric_name[4:]
            data = pd.read_csv(f"data/{symbol.lower()}/{name}.csv").iloc[0]
            snapshot[name] = json.loads(data.to_json())
            if isinstance(snapshot[name], dict):
                for (k, v) in snapshot[name].items():
                    snapshot[pattern.sub('', name + k).replace("_", "")] = v
                del snapshot[name]
            date = date_parse(data.values[0])
            snapshot["realdate"] = date if (snapshot.get(
                "realdate") and date < snapshot["realdate"]) or not snapshot.get("realdate") else snapshot["realdate"]
    
    snapshot["realdate"] = str(snapshot["realdate"].date())

    finnhub_client = finnhub.Client(
        api_key="crct279r01qkg0hdm2h0crct279r01qkg0hdm2hg")
    quote = finnhub_client.quote(symbol.upper())
    snapshot["current"] = quote["c"]
    snapshot["open"] = quote["o"]

    return [snapshot]
