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
                first_half_rows = rows[1:len(rows) // 2 + 1]
            return first_half_rows
    return None


def calculate_sma(prices, window, shift_back=0):
    close_prices = [candle['close'] for candle in prices['candles']]
    shift_back += 1
    if len(close_prices) < window:
        return 3650
    return sum(close_prices[-(window + shift_back):-shift_back]) / window


def find_last_max_price(prices, max_value):
    close_prices = [candle['close'] for candle in prices['candles']]
    for index, price in enumerate(reversed(close_prices)):
        if price >= max_value and index > 0:
            return index
    return 3650


def find_last_max_volume(prices, max_value):
    close_prices = [candle['volume'] for candle in prices['candles']]
    for index, price in enumerate(reversed(close_prices)):
        if price >= max_value and index > 0:
            return index
    return 0


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
        if 1.5 >= close_sma50 >= 1.45:
            score += 1
        if 1.9 >= close_sma150 >= 1.6:
            score += 1
        if 1.9 >= close_sma200 >= 1.7:
            score += 1
        if 1.2 >= sma50_sma150 >= 1.14:
            score += 1
        if 1.26 >= sma50_sma200 >= 1.18:
            score += 1
        if 1.06 >= sma150_sma200 >= 1.02:
            score += 1
        if 1.7 >= trending_up >= 1.03:
            score += 1

        avg_volume100 = find_avg_volume(price_history[ticker], 100)
        volume_volume100 = volume / avg_volume100 if avg_volume100 else 0
        is_breakout = 3 >= price_change >= 0 and 3 >= volume_volume100 >= 1

        if score > 5 and is_breakout and last_max_price > 365 and last_max_volume < 3:
            market_cap_billion = get_market_cap(ticker) / 1e9
            if 20 < market_cap_billion < 40:
                date_string = datetime.datetime.fromtimestamp(date).strftime('%Y-%m-%d')
                results.append(
                    (ticker, f"{market_cap_billion:>6.2f}B", date_string, f"{latest_close_price:>7.2f}$",
                     f"{price_change:>6.2f}", f"{volume_volume100:>6.2f}", last_max_price, last_max_volume,
                     f"{close_sma50:>6.2f}", f"{close_sma150:>6.2f}", f"{close_sma200:>6.2f}",
                     f"{sma50_sma150:>6.2f}", f"{sma50_sma200:>6.2f}", f"{sma150_sma200:>6.2f}",
                     f"{trending_up:>6.2f}", "N/A", "N/A", "N/A")),
    print()

    # Write CSV file with defined column headers.
    df = pd.DataFrame(results,
                      columns=["Ticker", "Market Cap", "Date", "Close Price",
                               "Price change", "Volume / Avg", "Last max price", "Last max volume",
                               "Close / SMA50", "Close / SMA150", "Close / SMA200",
                               "SMA50 / SMA150", "SMA50 / SMA200", "SMA150 / SMA200",
                               "Trending up", "Sell Date", "Holding Duration", "Profit"])
    df = df.sort_values((["Ticker"]), ascending=True)

    output_path = os.path.join(os.path.dirname(DIR), 'output', 'screen_results.csv')
    if not os.path.exists(output_path):
        df.to_csv(output_path, index=False)
    else:
        df.to_csv(output_path, index=False, mode='a', header=False)
    if (len(df) > 0):
        print(df)
    else:
        print("No stocks found")
    save_market_cap_cache()


def main(filtered_price_date=None, end_date=None):
    if filtered_price_date is not None and end_date is not None:
        screen(filtered_price_date, end_date)


if __name__ == "__main__":
    main()
