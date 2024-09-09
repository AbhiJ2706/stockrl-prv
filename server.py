import finnhub

import json
import pandas as pd

from flask import Flask, jsonify, request

from metrics import get_adx, get_cci, get_historical_data, get_rsi
from request_helpers import retrieve_and_store_data, retrieve_snapshot


app = Flask(__name__)


def process(filename):
    return jsonify(list(json.loads(pd.read_csv(filename).to_json(orient="index")).values()))


@app.route("/stock/historical_data")
def historical_data():
    symbol = request.args.get('symbol')
    retrieve_and_store_data(get_historical_data, symbol)
    return process(f"data/{symbol.lower()}/historical_data.csv")


@app.route("/stock/rsi")
def rsi():
    symbol = request.args.get('symbol')
    retrieve_and_store_data(get_rsi, symbol)
    return process(f"data/{symbol.lower()}/rsi.csv")


@app.route("/stock/adx")
def adx():
    symbol = request.args.get('symbol')
    retrieve_and_store_data(get_adx, symbol)
    return process(f"data/{symbol.lower()}/adx.csv")


@app.route("/stock/cci")
def cci():
    symbol = request.args.get('symbol')
    retrieve_and_store_data(get_cci, symbol)
    return process(f"data/{symbol.lower()}/cci.csv")


@app.route("/quote")
def quote():
    symbol = request.args.get('symbol')
    finnhub_client = finnhub.Client(
        api_key="crct279r01qkg0hdm2h0crct279r01qkg0hdm2hg")
    return [finnhub_client.quote(symbol.upper())]


@app.route("/stock/snapshot")
def snapshot():
    symbol = request.args.get('symbol')
    snap = retrieve_snapshot(symbol)
    return jsonify(snap)


if __name__ == "__main__":
    import logging
    logging.basicConfig(filename='error.log', level=logging.DEBUG)
    logging.basicConfig(filename='error.log', level=logging.WARNING)
    logging.basicConfig(filename='error.log', level=logging.ERROR)

    app.run()
