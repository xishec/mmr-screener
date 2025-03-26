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
        current_date += relativedelta(months=3)


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
            return buy_timestamp, ts_int, (current_close - purchase_price) / purchase_price

    # If we reach the end without selling, calculate final profit/loss
    last_ts = sorted_timestamps[-1]
    return buy_timestamp, int(last_ts), (candles_dict[last_ts]["close"] - purchase_price) / purchase_price


def back_test(PRICE_DATA):
    DIR = os.path.dirname(os.path.realpath(__file__))
    output_dir = os.path.join(os.path.dirname(DIR), 'output')
    global_holding_days = []
    global_profits = []
    for file_name in os.listdir(output_dir):
        # Process all CSV files starting with 'screen_results_'
        if file_name.startswith("screen_results_") and file_name.endswith(".csv"):
            date_str = file_name[len("screen_results_"):-len(".csv")]
            file_path = os.path.join(output_dir, file_name)
            with open(file_path, mode="r", newline="") as csv_file:
                reader = csv.DictReader(csv_file)
                rows = list(reader)
            # Process each row to add Sell Date, Held Days and Profit
            start_timestamp = int(datetime.datetime.strptime(date_str, "%Y-%m-%d").timestamp())
            total_holding_days = 0
            total_profit = 0
            count = 0
            has_average = False
            for row in rows:
                ticker = row["Ticker"]
                if ticker == "AVERAGE":
                    has_average = True
                    continue
                candles = PRICE_DATA[ticker]["candles"]
                candles_dict = {str(candle["datetime"]): candle for candle in candles}
                buy_timestamp, sell_timestamp, profit = check_stop_loss(start_timestamp, candles_dict)
                sell_date = datetime.datetime.fromtimestamp(sell_timestamp).strftime("%Y-%m-%d")
                row["Sell Date"] = sell_date
                row["Profit"] = f"{profit * 100:.4f}%"
                holding_days = (datetime.datetime.fromtimestamp(sell_timestamp) - datetime.datetime.fromtimestamp(
                    buy_timestamp)).days
                row["Held Days"] = holding_days
                total_holding_days += holding_days
                total_profit += profit
                count += 1
                global_holding_days.append(holding_days)
                global_profits.append(profit)

            # Append a summary row for averages, if any rows processed
            if count > 0 and not has_average:
                avg_held = total_holding_days / count
                avg_profit = total_profit / count
                summary = {col: "" for col in rows[0].keys()}
                summary["Ticker"] = "AVERAGE"
                summary["Held Days"] = f"{avg_held:.2f}"
                summary["Profit"] = f"{avg_profit * 100:.4f}%"
                rows.append(summary)

            # Rewrite the CSV file with new columns
            header = list(rows[0].keys()) if rows else []
            with open(file_path, mode="w", newline="") as csv_out:
                writer = csv.DictWriter(csv_out, fieldnames=header)
                writer.writeheader()
                writer.writerows(rows)
    print(f"Global Average Holding Days: {sum(global_holding_days) / len(global_holding_days):.2f}")
    print(f"Global Average Profit: {sum(global_profits) / len(global_profits) * 100:.4f}%")

def main():
    PRICE_DATA = rs_ranking.load_data()
    screen_stocks(PRICE_DATA)
    back_test(PRICE_DATA)


if __name__ == "__main__":
    main()
