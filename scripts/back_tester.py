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
    today = datetime.datetime.strptime("2025-01-01", "%Y-%m-%d")
    current_date = today - relativedelta(years=3)

    # Loop over each month from start to today
    while current_date <= today:
        # Format the date as "YYYY-MM-DD"
        date_str = current_date.strftime("%Y-%m-%d")

        # Call the function with the date override argument
        rs_ranking.main(PRICE_DATA, date_str)

        # Increment the date by one month
        current_date += relativedelta(days=10)


import datetime


def check_stop_loss(start_timestamp, candles_dict):
    MA_PERIOD = 10  # Period for moving average calculation
    buy_timestamp = -1
    purchase_price = -1
    max_close = -1
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
            max_close = current_close
            continue

        # Update trailing stop loss: update max_close and check if current close dropped 7% below max
        max_close = max(max_close, current_close)
        if current_close < max_close * 0.93:
            return buy_timestamp, ts_int, (current_close - purchase_price) / purchase_price

        ma_value = sum(close_prices) / MA_PERIOD
        if current_close < ma_value:
            return buy_timestamp, ts_int, (current_close - purchase_price) / purchase_price

    # If we reach the end without selling, calculate final profit/loss
    last_ts = sorted_timestamps[-1]
    return buy_timestamp, int(last_ts), (candles_dict[last_ts]["close"] - purchase_price) / purchase_price


def back_test(PRICE_DATA):
    DIR = os.path.dirname(os.path.realpath(__file__))
    output_dir = os.path.join(os.path.dirname(DIR), 'output')
    file_path = os.path.join(output_dir, 'screen_results.csv')
    global_holding_days = []
    global_profits = []

    with open(file_path, mode="r", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        rows = list(reader)

    total_holding_days = 0
    total_profit = 0
    count = 0
    has_average = False
    for row in rows:
        if row["Ticker"] == "AVERAGE":
            has_average = True
            continue

        screening_date = row["Date"]
        start_timestamp = int(datetime.datetime.strptime(screening_date, "%Y-%m-%d").timestamp())
        candles = PRICE_DATA[row["Ticker"]]["candles"]
        candles_dict = {str(candle["datetime"]): candle for candle in candles}
        buy_timestamp, sell_timestamp, profit = check_stop_loss(start_timestamp, candles_dict)
        sell_date = datetime.datetime.fromtimestamp(sell_timestamp).strftime("%Y-%m-%d")
        row["Sell Date"] = sell_date
        row["Profit"] = f"{profit * 100:.4f}%"
        holding_days = (datetime.datetime.fromtimestamp(sell_timestamp) - datetime.datetime.fromtimestamp(
            buy_timestamp)).days
        row["Holding Duration"] = holding_days
        total_holding_days += holding_days
        total_profit += profit
        count += 1
        global_holding_days.append(holding_days)
        global_profits.append(profit)

    if count > 0 and not has_average:
        avg_held = total_holding_days / count
        avg_profit = total_profit / count
        r = sum(global_profits) / len(global_profits)
        d = sum(global_holding_days) / len(global_holding_days)
        n = 365 / d
        f = (1 + r) ** n
        annualized_return = (f - 1) * 100

        summary = {col: "" for col in rows[0].keys()}
        summary["Ticker"] = "AVERAGE"
        summary["Holding Duration"] = f"{avg_held:.2f}"
        summary["Profit"] = f"{avg_profit * 100:.4f}% ({annualized_return:.4f}%)"
        rows.append(summary)

    with open(file_path, mode="w", newline="") as csv_out:
        header = list(rows[0].keys()) if rows else []
        writer = csv.DictWriter(csv_out, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)

    if len(global_holding_days) > 0 and len(global_profits) > 0:
        r = sum(global_profits) / len(global_profits)
        d = sum(global_holding_days) / len(global_holding_days)
        n = 365 / d
        f = (1 + r) ** n
        annualized_return = (f - 1) * 100
        print(f"Global Average Profit: {r * 100:.4f}%")
        print(f"Global Average Holding Days: {d:.2f}")
        print(f"Annualized Return: {annualized_return:.4f}%")


def main():
    PRICE_DATA = rs_ranking.load_data()
    screen_stocks(PRICE_DATA)
    back_test(PRICE_DATA)


if __name__ == "__main__":
    main()

