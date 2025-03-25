import gzip
import json
import string
from datetime import datetime
import yfinance as yf
import csv
import time


# Load the first half of rs_stocks.csv
def load_csv():
    with open('output/rs_stocks.csv', mode='r') as csv_file:
        csv_reader = csv.reader(csv_file)
        rows = list(csv_reader)
        header = rows[0]
        first_half_rows = rows[1:len(rows) // 5 + 1]
    return first_half_rows


# Load price history data
def load_price_history():
    price_history = {}
    for char in string.ascii_lowercase:
        file_path = f'data_persist/{char}_price_history.json.gz'
        try:
            with gzip.open(file_path, 'rb') as f_in:
                data = json.loads(f_in.read().decode('utf-8'))
                price_history.update(data)
        except:
            print(f"File not found: {char}")
    return price_history


def calculate_sma(prices, window):
    close_prices = [candle['close'] for candle in prices['candles']]
    if len(close_prices) < window:
        return None
    return sum(close_prices[-window:]) / window


def calculate_average_volume(prices, window):
    volumes = [candle['volume'] for candle in prices['candles']]
    if len(volumes) < window:
        return None
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
    try:
        ticker = yf.Ticker(ticker_symbol)
        info = ticker.info
        market_cap = info.get("marketCap")
        if market_cap is None:
            return 0
        return market_cap
    except Exception:
        time.sleep(2)
        try:
            ticker = yf.Ticker(ticker_symbol)
            info = ticker.info
            market_cap = info.get("marketCap")
            if market_cap is None:
                return 0
            return market_cap
        except Exception:
            return 0


def screen():
    first_half_rows = load_csv()
    price_history = load_price_history()
    results = []

    filtered_rows = [row for row in first_half_rows if row[1] in price_history]
    total = len(filtered_rows)
    for i, row in enumerate(filtered_rows):
        print(f"\r{i + 1} / {total}, {(i + 1) / total * 100:.2f}% ", end="", flush=True)
        ticker = row[1]

        sma20 = calculate_sma(price_history[ticker], 22)
        sma200 = calculate_sma(price_history[ticker], 200)

        if sma20 is None or sma200 is None:
            continue

        latest_close_price = price_history[ticker]["candles"][-1]["close"]
        recent_high_volume, date = calculate_recent_high_volume(price_history[ticker], 5)
        average_volume = calculate_average_volume(price_history[ticker], 100)
        change = get_change_on_date(price_history[ticker], date) * 100
        close_price_str = f"{latest_close_price:>7.2f}"
        above_sma20 = (latest_close_price - sma20) * 100 / sma20
        above_sma200 = (latest_close_price - sma200) * 100 / sma200
        date_str = datetime.fromtimestamp(date).strftime('%Y-%m-%d')
        volume_change = (recent_high_volume - average_volume) * 100 / average_volume

        if latest_close_price > sma20 and latest_close_price > sma200:
            if change > 1 and recent_high_volume > average_volume * 2:
                marketCap = get_market_cap(ticker)
                # print(f"{ticker} at {marketCap:.2f}B")
                market_cap_billion = marketCap / 1e9
                if 10e9 < marketCap < 100e9:
                    message = (
                        f"{ticker:<5} -> market cap is ${market_cap_billion:>6.2f}B, "
                        f"closed at ${close_price_str}, "
                        f"{above_sma20:>6.2f}% above sma20, "
                        f"{above_sma200:>6.2f}% above sma200, "
                        f"on {date_str}, "
                        f"it went up {change:>5.2f}% and volume is {volume_change:>6.2f}% higher than average volume"
                    )
                    results.append(message)

    print("\n")
    for result in sorted(results, key=lambda x: x[0].lower()):
        print(result)


def main():
    screen()


if __name__ == "__main__":
    main()
