from datetime import datetime
import finnhub

import json

from TechIndicators import TechIndicators

import logging

from TimeSeries import TimeSeries


logger = logging.getLogger('server.sub')


def finnhub_data(symbol):
    finnhub_client = finnhub.Client(
        api_key="crct279r01qkg0hdm2h0crct279r01qkg0hdm2hg")
    return finnhub_client.quote(symbol.upper())


def retrieve_snapshot(symbol):
    def harvest_data_from(metric, metric_name):
        try:
            data = metric().iloc[0]
        except:
            data = f"""{{ "{metric_name}": "no data" }}"""
        data = json.loads(data if isinstance(data, str) else data.to_json())
        return data.get(metric_name, data)

    logger.debug(f"beginning snapshot for {symbol}")

    snapshot = {}

    ti = TechIndicators()
    ts = TimeSeries()
    data = ts.get_daily(symbol).iloc[0]
    quote = finnhub_data(symbol)

    snapshot["adx"] = harvest_data_from(lambda: ti.get_adx(symbol, intraday=True, time_period=3), "ADX")
    snapshot["rsi"] = harvest_data_from(lambda: ti.get_rsi(symbol, intraday=True, time_period=4), "RSI")
    snapshot["macd"] = harvest_data_from(lambda: ti.get_macd(symbol, intraday=True, window_slow=20), "MACD")
    snapshot["macdsignal"] = harvest_data_from(lambda: ti.get_macd_signal(symbol, intraday=True, window_slow=20), "MACD_Signal")
    snapshot["date"] = str(datetime.now())
    snapshot["current"] = quote["c"]
    snapshot["open"] = quote["o"]
    snapshot["previousopen"] = data["1. open"]
    snapshot["previousclose"] = data["4. close"]
    snapshot["previoushigh"] = data["2. high"]
    snapshot["previouslow"] = data["3. low"]

    return [snapshot]


if __name__ == "__main__":
    print(retrieve_snapshot("amzn"))