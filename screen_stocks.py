import csv
import json

# Load the first half of rs_stocks.csv
with open('output/rs_stocks.csv', mode='r') as csv_file:
    csv_reader = csv.reader(csv_file)
    rows = list(csv_reader)
    header = rows[0]
    first_half_rows = rows[1:len(rows) // 2 + 1]

# Load price_history.json
with open('data/price_history.json', mode='r') as json_file:
    price_history = json.load(json_file)


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


def calculate_recent_high(prices, window):
    highs = [candle['high'] for candle in prices['candles']]
    if len(highs) < window:
        return None
    return max(highs[-window:])


# Check for breakout with volume
for row in first_half_rows:
    ticker = row[1]
    marketCap = float(row[6])
    if ticker in price_history and 10e8 < marketCap < 100e8:

        marketCap = float(row[6])
        sma200 = calculate_sma(price_history[ticker], 200)
        latest_close_price = price_history[ticker]["candles"][-1]["close"]
        recent_close_high = calculate_recent_high(price_history[ticker], 20)
        latest_volume = price_history[ticker]['candles'][-1]['volume']
        average_volume = calculate_average_volume(price_history[ticker], 20)

        if ticker in price_history and 10e8 < marketCap < 100e8 and latest_close_price > sma200:
            print(
                f"{ticker}, marketCap and above sma200")

            if latest_close_price >= recent_close_high and latest_volume > average_volume * 1.5:
                print(
                    f"------- High and Volume")