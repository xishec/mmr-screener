import gzip
import json
import string
import datetime
from dateutil.relativedelta import relativedelta
import os
import csv

from scripts import rs_ranking


def calculate_sma(prices, window):
    close_prices = [candle['close'] for candle in prices['candles']]
    if len(close_prices) < window:
        return None
    return sum(close_prices[-window:]) / window


def screen_stocks(PRICE_DATA):
    # Set the starting date as 5 years ago from today
    current_date = datetime.date.today() - relativedelta(years=1)
    today = datetime.date.today()

    # Loop over each month from start to today
    while current_date <= today:
        # Format the date as "YYYY-MM-DD"
        date_str = current_date.strftime("%Y-%m-%d")
        print(f"Running rs_ranking.main for date: {date_str}")

        # Call the function with the date override argument
        rs_ranking.main(PRICE_DATA, date_str)

        # Increment the date by one month
        current_date += relativedelta(months=1)


import datetime


def check_stop_loss(start_timestamp, candles_dict):
    MA_PERIOD = 14  # Period for moving average calculation
    buy_timestamp = -1
    purchase_price = -1
    close_prices = []

    sorted_timestamps = sorted(candles_dict.keys(),
                               key=lambda ts: datetime.datetime.fromtimestamp(int(ts)))

    if not sorted_timestamps:
        return -1, -1, 0

    for ts in sorted_timestamps:
        current_close = candles_dict[ts]["close"]
        current_low = candles_dict[ts]["low"]

        close_prices.append(current_close)
        if len(close_prices) > MA_PERIOD:
            close_prices.pop(0)

        ts_int = int(ts)
        if ts_int < start_timestamp:
            continue

        # Initial purchase
        if purchase_price == -1:
            buy_timestamp = ts_int
            purchase_price = current_close
            continue

        ma_value = sum(close_prices) / MA_PERIOD
        if current_close < ma_value:
            return buy_timestamp, ts_int, current_close / purchase_price

    # If we reach the end without selling, calculate final profit/loss
    last_ts = sorted_timestamps[-1]
    return buy_timestamp, int(last_ts), candles_dict[last_ts]["close"] / purchase_price


def back_test(PRICE_DATA):
    results = {}
    output_dir = os.path.join("..", "output")
    for file_name in os.listdir(output_dir):
        if file_name.startswith("screen_results_2022-03") and file_name.endswith(".csv"):
            # Extract the date from the filename: everything between 'screen_results_' and '.csv'
            date_str = file_name[len("screen_results_"):-len(".csv")]
            file_path = os.path.join(output_dir, file_name)
            with open(file_path, mode="r", newline="") as csv_file:
                reader = csv.DictReader(csv_file)
                results[date_str] = list(reader)

    for date, screen_results in results.items():
        start_timestamp = int(datetime.datetime.strptime(date, "%Y-%m-%d").timestamp())
        for screen_result in screen_results:
            ticker = screen_result["Ticker"]
            candles = PRICE_DATA[ticker]["candles"]
            candles_dict = {str(candle["datetime"]): candle for candle in candles}
            buy_timestamp, sell_timestamp, profit = check_stop_loss(start_timestamp, candles_dict)
            held_days = (datetime.datetime.fromtimestamp(sell_timestamp)
                         - datetime.datetime.fromtimestamp(buy_timestamp)).days
            print(f"Held {ticker} for {held_days} days with profit {profit}")
    return results


def main():
    PRICE_DATA = rs_ranking.load_data()
    screen_stocks(PRICE_DATA)
    back_test(PRICE_DATA)


if __name__ == "__main__":
    main()
