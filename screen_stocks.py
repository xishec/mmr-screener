import csv
import gzip
import json
import string
from datetime import datetime

# Load the first half of rs_stocks.csv
with open('output/rs_stocks.csv', mode='r') as csv_file:
    csv_reader = csv.reader(csv_file)
    rows = list(csv_reader)
    header = rows[0]
    first_half_rows = rows[1:len(rows) // 5 + 1]

price_history = {}
for char in string.ascii_lowercase:
    file_path = f'data_persist/{char}_price_history.json.gz'
    try:
        with gzip.open(file_path, 'rb') as f_in:
            data = json.loads(f_in.read().decode('utf-8'))
            price_history.update(data)
    except:
        print(f"File not found: {char}")


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


filtered_rows = [row for row in first_half_rows if row[1] in price_history and 10e9 < float(row[6]) < 100e9]

for row in filtered_rows:
    ticker = row[1]
    marketCap = float(row[6])

    sma20 = calculate_sma(price_history[ticker], 22)
    sma200 = calculate_sma(price_history[ticker], 200)
    latest_close_price = price_history[ticker]["candles"][-1]["close"]

    recent_high_volume, date = calculate_recent_high_volume(price_history[ticker], 5)
    average_volume = calculate_average_volume(price_history[ticker], 100)

    if sma20 is None or sma200 is None:
        continue

    if latest_close_price > sma20 and latest_close_price > sma200:

        change = get_change_on_date(price_history[ticker], date) * 100

        if change > 1 and recent_high_volume > average_volume * 2:
            market_cap_billion = marketCap / 1e9
            close_price_str = f"{latest_close_price:>7.2f}"
            above_sma20 = (latest_close_price - sma20) * 100 / sma20
            above_sma200 = (latest_close_price - sma200) * 100 / sma200
            date_str = datetime.fromtimestamp(date).strftime('%Y-%m-%d')
            volume_change = (recent_high_volume - average_volume) * 100 / average_volume

            print(
                f"{ticker:<5} -> market cap is ${market_cap_billion:>6.2f}B, closed at ${close_price_str}, "
                f"{above_sma20:>6.2f}% above sma20, "
                f"{above_sma200:>6.2f}% above sma200, "
                f"on {date_str}, it went up ",
                f"{change:>5.2f}% and ",
                f"volume is {volume_change:>6.2f}% higher than average volume"
            )
