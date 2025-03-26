import gzip
import json
import os
import string
import datetime
import yfinance as yf
import csv
import time
import pandas as pd
from scripts import rs_ranking

DIR = os.path.dirname(os.path.realpath(__file__))
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)
pd.set_option('display.max_columns', None)


# Load the first half of rs_stocks.csv
def load_csv(end_date):
    path = os.path.join(os.path.dirname(DIR), 'output', f'rs_stocks_{end_date}.csv')
    with open(path, mode='r') as csv_file:
        csv_reader = csv.reader(csv_file)
        rows = list(csv_reader)
        header = rows[0]
        first_half_rows = rows[1:len(rows) // 5 + 1]
    return first_half_rows


def calculate_sma(prices, window):
    close_prices = [candle['close'] for candle in prices['candles']]
    if len(close_prices) < window:
        return -1
    return sum(close_prices[-window:]) / window


def calculate_average_volume(prices, window):
    volumes = [candle['volume'] for candle in prices['candles']]
    if len(volumes) < window:
        return -1
    return sum(volumes[-window:]) / window


def calculate_recent_high_volume(prices, window):
    volumes = [(candle['volume'], candle['datetime']) for candle in prices['candles']]
    if len(volumes) < window:
        return None, None
    recent_volumes = volumes[-window:]
    max_volume, max_date = max(recent_volumes, key=lambda x: x[0])
    return max_volume, max_date


def get_change_on_date(prices, target_date):
    for candle in prices['candles']:
        if candle['datetime'] == target_date:
            return (candle['close'] - candle['open']) / candle['close']
    return None


def get_market_cap(ticker_symbol):
    for _ in range(2):
        try:
            ticker = yf.Ticker(ticker_symbol)
            info = ticker.info
            market_cap = info.get("marketCap")
            if market_cap is None:
                return 0
            return market_cap
        except Exception:
            time.sleep(2)
    return 0


def screen(filtered_price_date, end_date):
    first_half_rows = load_csv(end_date)
    price_history = filtered_price_date
    results = []

    filtered_rows = [row for row in first_half_rows if row[1] in price_history]
    total = len(filtered_rows)
    for i, row in enumerate(filtered_rows):
        ticker = row[1]
        print(f"Screening stocks {ticker} \r{i + 1} / {total}, {(i + 1) / total * 100:.2f}% ", end="", flush=True)

        sma20 = calculate_sma(price_history[ticker], 22)
        sma200 = calculate_sma(price_history[ticker], 200)

        latest_close_price = price_history[ticker]["candles"][-1]["close"]
        recent_high_volume, date = calculate_recent_high_volume(price_history[ticker], 2)
        average_volume = calculate_average_volume(price_history[ticker], 100)
        change = get_change_on_date(price_history[ticker], date) * 100
        above_sma20 = (latest_close_price - sma20) * 100 / sma20
        above_sma200 = (latest_close_price - sma200) * 100 / sma200
        volume_change = (recent_high_volume - average_volume) * 100 / average_volume

        if latest_close_price > sma20 and latest_close_price > sma200 * 1.2:
            if change > 1 and recent_high_volume > average_volume * 2:
                marketCap = get_market_cap(ticker)
                market_cap_billion = marketCap / 1e9
                if 10e9 < marketCap < 200e9:
                    results.append(
                        (ticker,
                         f"{market_cap_billion:>6.2f}B",
                         f"{latest_close_price:>7.2f}$",
                         f"{above_sma20:>6.2f}%",
                         f"{above_sma200:>6.2f}%",
                         datetime.datetime.fromtimestamp(date).strftime('%Y-%m-%d'),
                         f"{change:>5.2f}%",
                         f"{volume_change:>6.2f}%"))
    print()

    # Write CSV file with defined column headers.
    df = pd.DataFrame(results,
                      columns=["Ticker", "Market Cap", "Close Price", "Above SMA20",
                               "Above SMA200", "Date", "Price Change", "Volume Change"])
    df = df.sort_values((["Ticker"]), ascending=True)

    output_path = os.path.join(os.path.dirname(DIR), 'output', f'screen_results_{end_date}.csv')
    df.to_csv(output_path, index=False)
    print(df)


def main(filtered_price_date=None, end_date=None):
    if filtered_price_date is not None and end_date is not None:
        screen(filtered_price_date, end_date)


if __name__ == "__main__":
    main()
