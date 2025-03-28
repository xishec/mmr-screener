import os
import datetime
import sys

import yfinance as yf
import csv
import time
import pandas as pd
import json

from scripts import rs_ranking

DIR = os.path.dirname(os.path.realpath(__file__))
OUTPUT_DIR = os.path.join(os.path.dirname(DIR), 'output')
CACHE_FILE = os.path.join(OUTPUT_DIR, 'market_cap_cache.json')

pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)
pd.set_option('display.max_columns', None)


# Load cache on module load
def load_market_cap_cache():
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r", encoding="utf8") as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading market cap cache: {e}")
    return {}


def save_market_cap_cache():
    try:
        with open(CACHE_FILE, "w", encoding="utf8") as f:
            json.dump(market_cap_cache, f)
    except Exception as e:
        print(f"Error saving market cap cache: {e}")


market_cap_cache = load_market_cap_cache()


def load_csv(end_date):
    output_dir = os.path.join(os.path.dirname(DIR), 'output')
    original_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    for i in range(0, 11):
        try_date = original_date - datetime.timedelta(days=i)
        try_date_str = try_date.strftime("%Y-%m-%d")
        path = os.path.join(os.path.dirname(DIR), 'output', f'rs_stocks_{try_date_str}.csv')
        if os.path.exists(path):
            with open(path, mode='r') as csv_file:
                csv_reader = csv.reader(csv_file)
                rows = list(csv_reader)
                first_half_rows = rows[1:len(rows) // 4 + 1]
            return first_half_rows
    return None


def calculate_sma(prices, window, shift_back=1):
    close_prices = [candle['close'] for candle in prices['candles']]
    if len(close_prices) < window:
        return None
    return sum(close_prices[-(window + shift_back):-shift_back]) / window


def find_last_max_price(prices, max_value):
    close_prices = [candle['close'] for candle in prices['candles']]
    for index, price in enumerate(reversed(close_prices)):
        if index == 0: continue
        if price >= max_value: return index
    return 3650


def find_last_max_volume(prices, max_value):
    volume = [candle['volume'] for candle in prices['candles']]
    for index, volume in enumerate(reversed(volume)):
        if index == 0: continue
        if volume >= max_value: return index
    return 3650


def find_avg_volume(prices, window):
    volumes = [candle['volume'] for candle in prices['candles']]
    if len(volumes) < window:
        return 0
    return sum(volumes[-window:]) / window


def get_change_on_date(prices, target_date):
    candles = prices['candles']
    for i, candle in enumerate(candles):
        if candle['datetime'] == target_date:
            this_close = candles[i]['close']
            last_close = candles[i - 1]['close']
            return (this_close - last_close) / last_close
    return None


def get_close_avg_movement_last_period(prices, period, shift_back=1):
    candles = prices['candles'][(-period + 1):]
    changes = [(candles[i]['close'] - candles[i - 1]['close']) / candles[i - 1]['close'] for i, _ in enumerate(candles)]
    sum_abs = sum(abs(change) for change in changes)
    return sum_abs / period


def get_close_max_movement_last_period(prices, period):
    candles = prices['candles'][(-period + 1):]
    changes = [(candles[i]['close'] - candles[i - 1]['close']) / candles[i - 1]['close'] for i, _ in enumerate(candles)]
    max_abs = max(abs(change) for change in changes)
    return max_abs


def get_market_cap_beta(ticker_symbol):
    global market_cap_cache
    # Check if market cap is already cached
    if ticker_symbol in market_cap_cache:
        market_cap = market_cap_cache[ticker_symbol]["market_cap"]
        beta = market_cap_cache[ticker_symbol]["beta"]
        return market_cap, beta

    for _ in range(2):
        try:
            ticker = yf.Ticker(ticker_symbol)
            info = ticker.info
            market_cap = info.get("marketCap")
            beta = info.get("beta")
            if market_cap is None:
                market_cap = 0
            market_cap_cache.setdefault(ticker_symbol, {})["market_cap"] = market_cap
            market_cap_cache[ticker_symbol]["beta"] = beta
            return market_cap, beta
        except Exception:
            time.sleep(1)
    return 0, 0


def screen(filtered_price_date, end_date):
    first_half_rows = load_csv(end_date)
    price_history = filtered_price_date
    results = []

    filtered_rows = [row for row in first_half_rows if row[0] in price_history]
    total = len(filtered_rows)
    for i, row in enumerate(filtered_rows):
        ticker = row[0]
        print(f"Screening stocks {ticker} \r{i + 1} / {total}, {(i + 1) / total * 100:.2f}% ", end="", flush=True)

        sma50 = calculate_sma(price_history[ticker], 50)
        sma150 = calculate_sma(price_history[ticker], 150)
        sma200 = calculate_sma(price_history[ticker], 200)
        sma200_22 = calculate_sma(price_history[ticker], 200, 22)

        latest_close_price = price_history[ticker]["candles"][-1]["close"]
        volume = price_history[ticker]["candles"][-1]["volume"]
        date = price_history[ticker]["candles"][-1]["datetime"]
        price_change = get_change_on_date(price_history[ticker], date) * 100

        last_max_price = find_last_max_price(price_history[ticker], latest_close_price)
        last_max_volume = find_last_max_volume(price_history[ticker], volume)

        close_sma50 = latest_close_price / sma50 if sma50 else 0
        close_sma150 = latest_close_price / sma150 if sma150 else 0
        close_sma200 = latest_close_price / sma200 if sma200 else 0
        sma50_sma150 = sma50 / sma150 if sma150 else 0
        sma50_sma200 = sma50 / sma200 if sma200 else 0
        sma150_sma200 = sma150 / sma200 if sma200 else 0
        trending_up = sma200 / sma200_22 if sma200_22 else 0

        score = 0
        if close_sma50 >= 1:
            score += 1
        if close_sma150 >= 1:
            score += 1
        if close_sma200 >= 1:
            score += 1
        if sma50_sma150 >= 1:
            score += 1
        if sma50_sma200 >= 1:
            score += 1
        if sma150_sma200 >= 1:
            score += 1
        if trending_up >= 1:
            score += 1

        avg_volume100 = find_avg_volume(price_history[ticker], 100)
        volume_volume100 = volume / avg_volume100 if avg_volume100 else 0
        avg_mov7 = get_close_avg_movement_last_period(price_history[ticker], 30) * 100
        max_mov7 = get_close_max_movement_last_period(price_history[ticker], 30) * 100

        is_breakout = price_change > 1 and last_max_price > 180 and max_mov7 <= 4 and volume_volume100 >= 0.5

        if score >= 7 and is_breakout:
            market_cap, beta = get_market_cap_beta(ticker)
            market_cap_billion = market_cap / 1e9
            if beta is not None and 5 < market_cap_billion < 200 and 1.4 >= beta >= 0.6:
                vix_ticker = "^VIX"
                vix = price_history[vix_ticker]["candles"][-1]["close"]
                vix_sma20 = calculate_sma(price_history[vix_ticker], 20)
                if vix < 20 and vix < vix_sma20:
                    date_string = datetime.datetime.fromtimestamp(date).strftime('%Y-%m-%d')
                    results.append(
                        (ticker, f"{market_cap_billion:>6.2f}B", date_string, f"{latest_close_price:>7.2f}$",
                         f"{price_change:>6.2f}", f"{volume_volume100:>6.2f}", last_max_price, last_max_volume,
                         f"{close_sma50:>6.2f}", f"{close_sma150:>6.2f}", f"{close_sma200:>6.2f}",
                         f"{sma50_sma150:>6.2f}", f"{sma50_sma200:>6.2f}", f"{sma150_sma200:>6.2f}",
                         f"{trending_up:>6.2f}", beta, f"{avg_mov7:>6.2f}", f"{max_mov7:>6.2f}",
                         "N/A", "N/A", "N/A", "N/A")),
    print()

    # Write CSV file with defined column headers.

    if len(results) == 0:
        print()
        return

    df = pd.DataFrame(results,
                      columns=["Ticker", "Market Cap", "Date", "Close Price",
                               "Price change", "Volume / Avg", "Last max price", "Last max volume",
                               "Close / SMA50", "Close / SMA150", "Close / SMA200",
                               "SMA50 / SMA150", "SMA50 / SMA200", "SMA150 / SMA200",
                               "Trending up", "Beta", "Avg mov7", "Max mov7",
                               "Sell Date", "Holding Duration", "Profit", "Max Profit"])
    df = df.sort_values((["Ticker"]), ascending=True)

    output_path = os.path.join(os.path.dirname(DIR), 'output', f'screen_results_{end_date}.csv')
    df.to_csv(output_path, index=False)
    if (len(df) > 0):
        print(df)
    else:
        print("No stocks found")
    print()
    save_market_cap_cache()


def main(filtered_price_date=None, end_date=None):
    if filtered_price_date is not None and end_date is not None:
        screen(filtered_price_date, end_date)


if __name__ == "__main__":
    main()
