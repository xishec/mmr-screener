import gzip
import itertools
import json
import math
import os
import string
import sys

import yaml
from rs_data import cfg
import screen_stocks
import pandas as pd
import datetime

DIR = os.path.dirname(os.path.abspath(__file__))
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)
pd.set_option('display.max_columns', None)

try:
    with open('../config.yaml', 'r') as stream:
        config = yaml.safe_load(stream)
except FileNotFoundError:
    config = None
except yaml.YAMLError as exc:
    print(exc)

MIN_PERCENTILE = cfg("MIN_PERCENTILE")
POS_COUNT_TARGET = cfg("POSITIONS_COUNT_TARGET")
REFERENCE_TICKER = cfg("REFERENCE_TICKER")
ALL_STOCKS = cfg("USE_ALL_LISTED_STOCKS")

TITLE_RANK = "Rank"
TITLE_TICKER = "Ticker"
TITLE_TICKERS = "Tickers"
TITLE_PERCENTILE = "Percentile"
TITLE_1M = "1 Month Ago"
TITLE_3M = "3 Months Ago"
TITLE_6M = "6 Months Ago"
TITLE_RS = "Relative Strength"

if not os.path.exists('../rs_stocks'):
    os.makedirs('../rs_stocks')


def relative_strength(closes: pd.Series, closes_ref: pd.Series):
    rs_stock = strength(closes)
    rs_ref = strength(closes_ref)
    rs = (1 + rs_stock) / (1 + rs_ref) * 100
    if math.isnan(rs):
        rs = 0.0
    else:
        rs = int(rs * 100) / 100
    return rs


def strength(closes: pd.Series):
    """Calculates the performance of the last year (most recent quarter is weighted double)"""
    try:
        quarters1 = quarters_perf(closes, 1)
        quarters2 = quarters_perf(closes, 2)
        quarters3 = quarters_perf(closes, 3)
        quarters4 = quarters_perf(closes, 4)
        return 0.4 * quarters1 + 0.2 * quarters2 + 0.2 * quarters3 + 0.2 * quarters4
    except:
        return 0


def quarters_perf(closes: pd.Series, n):
    length = min(len(closes), n * int(252 / 4))
    prices = closes.tail(length)
    pct_chg = prices.pct_change().dropna()
    perf_cum = (pct_chg + 1).cumprod() - 1
    return perf_cum.tail(1).item()


def rankings(PRICE_DATA, end_date):
    output_dir = os.path.join(os.path.dirname(DIR), 'rs_stocks')
    output_path = os.path.join(output_dir, f'rs_stocks_{end_date}.csv')
    original_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    for i in range(0, 11):
        try_date = original_date - datetime.timedelta(days=i)
        try_date_str = try_date.strftime("%Y-%m-%d")
        output_path = os.path.join(output_dir, f'rs_stocks_{try_date_str}.csv')
        if os.path.exists(output_path):
            print(f"rs_stocks file {output_path} already exists. Loading...")
            df = pd.read_csv(output_path)
            return [df]

    relative_strengths = []
    stock_rs = {}
    total = len(PRICE_DATA)
    closes_ref = pd.Series([candle["close"] for candle in PRICE_DATA[REFERENCE_TICKER]["candles"]])

    for i, (ticker, data) in enumerate(PRICE_DATA.items()):
        print(f"\rCalculating ranking for {ticker:>5}, {i + 1:>5} / {total:>5}, {(i + 1) / total * 100:>6.2f}% ",
              end="", flush=True)
        try:
            closes = pd.Series([candle["close"] for candle in data["candles"]], dtype=float)
            if len(closes) >= 6 * 20:
                rs = relative_strength(closes, closes_ref)
                month = 20
                tmp_percentile = 100
                rs1m = relative_strength(closes.head(-1 * month), closes_ref.head(-1 * month))
                rs3m = relative_strength(closes.head(-3 * month), closes_ref.head(-3 * month))
                rs6m = relative_strength(closes.head(-6 * month), closes_ref.head(-6 * month))

                if rs < 590:
                    relative_strengths.append((ticker, rs, tmp_percentile, rs1m, rs3m, rs6m))
                    stock_rs[ticker] = rs
        except KeyError:
            print(f'Ticker {ticker} has corrupted data.')

    df = pd.DataFrame(relative_strengths,
                      columns=[TITLE_TICKER, TITLE_RS, TITLE_PERCENTILE, TITLE_1M, TITLE_3M, TITLE_6M])
    df[TITLE_PERCENTILE] = pd.qcut(df[TITLE_RS], 100, labels=False, duplicates="drop")
    df[TITLE_1M] = pd.qcut(df[TITLE_1M], 100, labels=False, duplicates="drop")
    df[TITLE_3M] = pd.qcut(df[TITLE_3M], 100, labels=False, duplicates="drop")
    df[TITLE_6M] = pd.qcut(df[TITLE_6M], 100, labels=False, duplicates="drop")
    df = df.sort_values(([TITLE_RS]), ascending=False).reset_index(drop=True)
    df[TITLE_RANK] = df.index + 1

    df = df[df[TITLE_PERCENTILE] >= MIN_PERCENTILE]

    # Create a list of desired percentiles
    percentile_values = [98, 89, 69, 49, 29, 9, 1]

    # Create a dictionary to store the first value of df[TITLE_RS] for each percentile
    first_rs_values = {}

    # Iterate through the desired percentiles
    for percentile in percentile_values:
        matching_rows = df[df[TITLE_PERCENTILE] == percentile]
        if matching_rows.empty:
            print(f"No rows match the condition for percentile {percentile}.")
            continue

        first_row = matching_rows.iloc[0]
        rs_value = first_row[TITLE_RS]
        first_rs_values[percentile] = rs_value

    os.makedirs(output_dir, exist_ok=True)
    df.to_csv(output_path, index=False)

    return [df]


def find_closest_date(PRICE_DATA, target_date_str):
    target_ts = int(datetime.datetime.strptime(target_date_str, "%Y-%m-%d").timestamp())

    # find max candles
    max_candles_length = 0
    max_candles = None
    for data in PRICE_DATA.values():
        candles = data.get("candles", [])
        if len(candles) > max_candles_length:
            max_candles = candles

    def key_function(item):
        index, candle = item
        return abs(candle["datetime"] - target_ts)
    _, closest_candle = min(enumerate(max_candles), key=key_function)

    return closest_candle["datetime"]


def filter_price_data_by_index(price_data: dict, timestamp: int) -> dict:
    filtered_data = {}
    for ticker, data in price_data.items():
        candles = data.get("candles", [])
        # Filter candles with datetime less than or equal to timestamp
        filtered_candles = [candle for candle in candles if candle["datetime"] <= timestamp]
        # Copy the original data and update the candles list
        new_data = data.copy()
        new_data["candles"] = filtered_candles
        filtered_data[ticker] = new_data
    return filtered_data


def load_data():
    """Load price history data in parallel using ThreadPoolExecutor for faster processing."""
    import concurrent.futures

    def load_file(char):
        try:
            file_path = os.path.join(os.path.dirname(DIR), 'data_persist', f'{char}_price_history.json.gz')
            with gzip.open(file_path, 'rb') as f_in:
                return json.loads(f_in.read().decode('utf-8'))
        except Exception as e:
            print(f"Error loading file for '{char}': {e}")
            return {}

    PRICE_DATA = {}
    chars = list(string.ascii_lowercase)

    with concurrent.futures.ThreadPoolExecutor(max_workers=min(26, len(chars))) as executor:
        future_to_char = {executor.submit(load_file, char): char for char in chars}

        for i, future in enumerate(concurrent.futures.as_completed(future_to_char)):
            char = future_to_char[future]
            print(f"\rLoading in parallel: {(i + 1):3}/{len(chars)} files processed ({char.upper()})",
                  end="", flush=True)
            data = future.result()
            if data:
                PRICE_DATA.update(data)

    print(f"\nLoaded price data for {len(PRICE_DATA)} tickers\n")
    return PRICE_DATA


def main(PRICE_DATA=None, date_override=None):
    if PRICE_DATA is None:
        PRICE_DATA = load_data()

    date = datetime.date.today().strftime("%Y-%m-%d")
    # date = "2023-11-20"
    if date_override is not None:
        date = date_override
    if len(sys.argv) > 1:
        date = sys.argv[1]

    timestamp = find_closest_date(PRICE_DATA, date)
    filtered_price_date = filter_price_data_by_index(PRICE_DATA, timestamp)
    start_date = (datetime.datetime.fromtimestamp(filtered_price_date["A"]["candles"][0]["datetime"])
                  .strftime("%Y-%m-%d"))
    end_date = (datetime.datetime.fromtimestamp(filtered_price_date["A"]["candles"][-1]["datetime"])
                .strftime('%Y-%m-%d'))
    print(f"Considering data from {start_date} to {end_date}")

    rankings(filtered_price_date, end_date)
    screen_stocks.main(filtered_price_date, end_date)

    return filtered_price_date, end_date


if __name__ == "__main__":
    main()
