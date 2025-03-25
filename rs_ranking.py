import gzip
import json
import string

import pandas as pd
import os
import yaml
from rs_data import cfg
from functools import reduce
import datetime

DIR = os.path.dirname(os.path.realpath(__file__))

pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)
pd.set_option('display.max_columns', None)

try:
    with open('config.yaml', 'r') as stream:
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

if not os.path.exists('output'):
    os.makedirs('output')


def relative_strength(closes: pd.Series, closes_ref: pd.Series):
    rs_stock = strength(closes)
    rs_ref = strength(closes_ref)
    rs = (1 + rs_stock) / (1 + rs_ref) * 100
    rs = int(rs * 100) / 100  # round to 2 decimals
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


# Added with ChatGPT------
def generate_tradingview_csv(percentile_values, first_rs_values):
    lines = []  # Store the lines in a list

    # Get yesterday's date
    trading_days = 0
    yesterday = datetime.date.today() - datetime.timedelta(days=1)

    # Iterate through the desired percentiles in descending order
    for percentile in sorted(percentile_values):
        rs_value = first_rs_values[percentile]

        # Iterate five times for each percentile
        for _ in range(5):
            trading_date = yesterday - datetime.timedelta(days=trading_days)
            date_str = trading_date.strftime("%Y%m%dT")

            # Construct the CSV row
            csv_row = f"{date_str},0,1000,0,{rs_value},0\n"
            lines.append(csv_row)  # Add the line to the list

            # Increment the trading_days count
            trading_days += 1

    # Reverse the order of the lines and concatenate them
    reversed_lines = reversed(lines)
    csv_content = ''.join(reversed_lines)

    return csv_content


def rankings(PRICE_DATA):
    """Returns a dataframe with percentile rankings for relative strength"""
    relative_strengths = []
    ranks = []
    stock_rs = {}
    for ticker, data in PRICE_DATA.items():
        try:
            closes = list(map(lambda candle: candle["close"], data["candles"]))
            closes_ref = list(map(lambda candle: candle["close"], PRICE_DATA[REFERENCE_TICKER]["candles"]))
            if len(closes) >= 6 * 20:
                closes_series = pd.Series(closes)
                closes_ref_series = pd.Series(closes_ref)
                rs = relative_strength(closes_series, closes_ref_series)
                month = 20
                tmp_percentile = 100
                rs1m = relative_strength(closes_series.head(-1 * month), closes_ref_series.head(-1 * month))
                rs3m = relative_strength(closes_series.head(-3 * month), closes_ref_series.head(-3 * month))
                rs6m = relative_strength(closes_series.head(-6 * month), closes_ref_series.head(-6 * month))

                # if rs is too big assume there is faulty price data
                if rs < 590:
                    # stocks output
                    ranks.append(len(ranks) + 1)
                    relative_strengths.append(
                        (0, ticker, rs, tmp_percentile, rs1m,
                         rs3m, rs6m))
                    stock_rs[ticker] = rs

        except KeyError:
            print(f'Ticker {ticker} has corrupted data.')
    dfs = []
    suffix = ''

    # stocks
    df = pd.DataFrame(relative_strengths,
                      columns=[TITLE_RANK, TITLE_TICKER, TITLE_RS, TITLE_PERCENTILE, TITLE_1M, TITLE_3M, TITLE_6M])
    df[TITLE_PERCENTILE] = pd.qcut(df[TITLE_RS], 100, labels=False, duplicates="drop")
    df[TITLE_1M] = pd.qcut(df[TITLE_1M], 100, labels=False, duplicates="drop")
    df[TITLE_3M] = pd.qcut(df[TITLE_3M], 100, labels=False, duplicates="drop")
    df[TITLE_6M] = pd.qcut(df[TITLE_6M], 100, labels=False, duplicates="drop")
    df = df.sort_values(([TITLE_RS]), ascending=False)
    df[TITLE_RANK] = ranks
    out_tickers_count = 0
    for index, row in df.iterrows():
        if row[TITLE_PERCENTILE] >= MIN_PERCENTILE:
            out_tickers_count = out_tickers_count + 1
    df = df.head(out_tickers_count)

    # Add with ChatGPT------
    # Create a list of desired percentiles
    percentile_values = [98, 89, 69, 49, 29, 9, 1]

    # Create a dictionary to store the first value of df[TITLE_RS] for each percentile
    first_rs_values = {}

    # Iterate through the desired percentiles
    for percentile in percentile_values:
        # Check if there are any rows matching the condition
        matching_rows = df[df[TITLE_PERCENTILE] == percentile]
        if matching_rows.empty:
            print(f"No rows match the condition for percentile {percentile}.")
            continue

        # Find the first row in the DataFrame where TITLE_PERCENTILE matches the desired percentile
        first_row = df[df[TITLE_PERCENTILE] == percentile].iloc[0]

        # Get the value of df[TITLE_RS] for this row
        rs_value = first_row[TITLE_RS]

        # Store the rs_value in the dictionary with the percentile as the key
        first_rs_values[percentile] = rs_value

    # Generate the TradingView CSV content
    tradingview_csv_content = generate_tradingview_csv(percentile_values, first_rs_values)

    # Save the TradingView CSV content to a file
    with open(os.path.join(DIR, "output", "RSRATING.csv"), "w") as csv_file:
        csv_file.write(tradingview_csv_content)

    df.to_csv(os.path.join(DIR, "output", f'rs_stocks{suffix}.csv'), index=False)
    dfs.append(df)

    return dfs


def main():
    PRICE_DATA = {}
    for char in string.ascii_lowercase:
        file_path = f'data_persist/{char}_price_history.json.gz'
        try:
            with gzip.open(file_path, 'rb') as f_in:
                data = json.loads(f_in.read().decode('utf-8'))
                PRICE_DATA.update(data)
        except:
            print(f"File not found: {char}")

    ranks = rankings(PRICE_DATA)
    print(ranks[0])


if __name__ == "__main__":
    main()
