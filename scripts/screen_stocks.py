import os
import datetime
import yfinance as yf
import csv
import time
import pandas as pd
import json

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


# Load the first half of rs_stocks.csv
def load_csv(end_date):
    path = os.path.join(os.path.dirname(DIR), 'output', f'rs_stocks_{end_date}.csv')
    with open(path, mode='r') as csv_file:
        csv_reader = csv.reader(csv_file)
        rows = list(csv_reader)
        first_half_rows = rows[1:len(rows) // 2 + 1]
    return first_half_rows


def calculate_sma(prices, window):
    close_prices = [candle['close'] for candle in prices['candles']]
    if len(close_prices) < window:
        return 0
    return sum(close_prices[-window:]) / window


def find_max_prices(prices, window):
    close_prices = [candle['close'] for candle in prices['candles']]
    if len(close_prices) < window:
        return 0
    return max(close_prices[-window:])


def find_max_volume(prices, window):
    volumes = [candle['volume'] for candle in prices['candles']]
    if len(volumes) < window:
        return 0
    return max(volumes[-window:])


def find_avg_volume(prices, window):
    volumes = [candle['volume'] for candle in prices['candles']]
    if len(volumes) < window:
        return 0
    return sum(volumes[-window:]) / window


def get_change_on_date(prices, target_date):
    for candle in prices['candles']:
        if candle['datetime'] == target_date:
            return (candle['close'] - candle['open']) / candle['close']
    return None


def get_market_cap(ticker_symbol):
    global market_cap_cache
    # Check if market cap is already cached
    if ticker_symbol in market_cap_cache:
        return market_cap_cache[ticker_symbol]

    for _ in range(2):
        try:
            ticker = yf.Ticker(ticker_symbol)
            info = ticker.info
            market_cap = info.get("marketCap")
            if market_cap is None:
                market_cap = 0
            market_cap_cache[ticker_symbol] = market_cap
            return market_cap
        except Exception:
            time.sleep(2)
    return 0


def screen(filtered_price_date, end_date):
    first_half_rows = load_csv(end_date)
    price_history = filtered_price_date
    results = []

    filtered_rows = [row for row in first_half_rows if row[0] in price_history]
    total = len(filtered_rows)
    for i, row in enumerate(filtered_rows):
        ticker = row[0]
        print(f"Screening stocks {ticker} \r{i + 1} / {total}, {(i + 1) / total * 100:.2f}% ", end="", flush=True)

        recent_max_close = find_max_prices(price_history[ticker], 30)
        recent_max_volume = find_max_volume(price_history[ticker], 30)

        sma20 = calculate_sma(price_history[ticker], 22)
        sma200 = calculate_sma(price_history[ticker], 200)
        latest_close_price = price_history[ticker]["candles"][-1]["close"]
        date = price_history[ticker]["candles"][-1]["datetime"]
        volume = price_history[ticker]["candles"][-1]["volume"]
        avg_volume100 = find_avg_volume(price_history[ticker], 100)
        change = get_change_on_date(price_history[ticker], date) * 100
        above_sma20 = (latest_close_price - sma20) * 100 / sma20 if sma20 else 0
        above_sma200 = (latest_close_price - sma200) * 100 / sma200 if sma200 else 0
        volume_change100 = (volume - avg_volume100) * 100 / avg_volume100 if avg_volume100 else 0

        if latest_close_price > sma20 and latest_close_price > sma200 and latest_close_price == recent_max_close:
            if 10 > change > 3 and volume_change100 > 200 and volume == recent_max_volume:
                market_cap_billion = get_market_cap(ticker) / 1e9
                if 10 < market_cap_billion < 200:
                    results.append(
                        (ticker,
                         f"{market_cap_billion:>6.2f}B",
                         f"{latest_close_price:>7.2f}$",
                         f"{above_sma20:>6.2f}%",
                         f"{above_sma200:>6.2f}%",
                         datetime.datetime.fromtimestamp(date).strftime('%Y-%m-%d'),
                         f"{change:>5.2f}%",
                         f"{volume_change100:>6.2f}%",
                         "N/A", "N/A", "N/A")),
    print()

    # Write CSV file with defined column headers.
    df = pd.DataFrame(results,
                      columns=["Ticker", "Market Cap", "Close Price", "Above SMA20",
                               "Above SMA200", "Date", "Latest Close Price Change", "Latest Volume Change avg100",
                               "Sell Date", "Holding Duration", "Profit"])
    df = df.sort_values((["Ticker"]), ascending=True)

    output_path = os.path.join(os.path.dirname(DIR), 'output', 'screen_results.csv')
    if not os.path.exists(output_path):
        df.to_csv(output_path, index=False)
    else:
        df.to_csv(output_path, index=False, mode='a', header=False)
    if(len(df)>0):
        print(df)
    else:
        print("No stocks found")
    save_market_cap_cache()


def main(filtered_price_date=None, end_date=None):
    if filtered_price_date is not None and end_date is not None:
        screen(filtered_price_date, end_date)


if __name__ == "__main__":
    main()

