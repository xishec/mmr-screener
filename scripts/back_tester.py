import gzip
import json
import string
import datetime
from dateutil.relativedelta import relativedelta
import os
import csv

from scripts import rs_ranking
from scripts.rs_ranking import find_closest_date

DIR = os.path.dirname(os.path.realpath(__file__))


def calculate_sma(prices, window):
    close_prices = [candle['close'] for candle in prices['candles']]
    if len(close_prices) < window:
        return None
    return sum(close_prices[-window:]) / window


def screen_stocks(PRICE_DATA):
    s2021 = datetime.datetime.strptime("2021-01-01", "%Y-%m-%d")
    s2022 = datetime.datetime.strptime("2022-01-01", "%Y-%m-%d")
    s2023 = datetime.datetime.strptime("2023-01-01", "%Y-%m-%d")
    s2024 = datetime.datetime.strptime("2024-01-01", "%Y-%m-%d")
    s2025 = datetime.datetime.strptime("2025-01-01", "%Y-%m-%d")
    today = datetime.datetime.today()

    # current_date = today - relativedelta(years=1)
    current_date = s2021
    end_date = today - relativedelta(days=30)

    # last_ts and timestamp make sure we only process each friday once
    last_ts = None
    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")
        timestamp = find_closest_date(PRICE_DATA, date_str)
        if timestamp != last_ts:
            filtered_price_date, date = rs_ranking.main(PRICE_DATA, date_str)
            back_test(PRICE_DATA, date)
            last_ts = timestamp

        current_date += relativedelta(days=15)


import datetime


def check_stop_loss(start_timestamp, candles_dict):
    MA_PERIOD = 50  # Period for moving average calculation
    buy_timestamp = -1
    purchase_price = -1
    max_close = -1
    min_close = -1
    close_prices = []

    sorted_timestamps = sorted(candles_dict.keys(),
                               key=lambda ts: datetime.datetime.fromtimestamp(int(ts)))

    if not sorted_timestamps:
        return -1, -1, 0, "Error"

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
            min_close = current_close
            continue

        if ts_int < start_timestamp:
            continue

        profit = (current_close - purchase_price) / purchase_price

        min_close = min(min_close, current_close)
        if profit > 0.4:
            min_profit = (min_close - purchase_price) / purchase_price
            return buy_timestamp, ts_int, profit, f"Gain, min: {min_profit * 100:.2f}%"

        # Update trailing stop loss: update max_close and check if current close dropped 7% below max
        max_close = max(max_close, current_close)
        if current_close < purchase_price * 0.8:
            max_profit = (max_close - purchase_price) / purchase_price
            return buy_timestamp, ts_int, profit, f"Loss, max: {max_profit * 100:.2f}%"

        # ma_value = sum(close_prices) / MA_PERIOD
        # if current_close < ma_value:
        #     max_profit = (max_close - purchase_price) / purchase_price
        #     return buy_timestamp, ts_int, profit, f"Less than MA, max: {max_profit * 100:.2f}%"

    # If we reach the end without selling, calculate final profit/loss
    last_ts = sorted_timestamps[-1]
    max_profit = (max_close - purchase_price) / purchase_price
    return (buy_timestamp, int(last_ts),
            (candles_dict[last_ts]["close"] - purchase_price) / purchase_price,
            f"End of data, max: {max_profit * 100:.2f}%")


def simulate():
    timeline = []  # new: timeline list to record simulation state per date

    output_dir = os.path.join(os.path.dirname(DIR), 'screen_results')
    file_path = os.path.join(output_dir, 'screen_results.csv')
    dates = {}
    with open(file_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            date_key = row.get("Date")
            if date_key:
                dates.setdefault(date_key, []).append(row)

    initial_cash = 100
    current_cash = initial_cash
    holdings = {}
    zero_budget_counter = 0
    trading_counter = 0

    for current_date, rows in dates.items():
        # Release holdings if sell date has passed
        for sell_date in list(holdings.keys()):
            if current_date > sell_date:
                tickers_dict_to_sell = holdings.pop(sell_date)
                for ticker, profit in tickers_dict_to_sell.items():
                    current_cash += profit
                    holding_value = sum(sum(profits.values()) for profits in holdings.values())
                    timeline.append({
                        "Date": current_date,
                        "Action": "Sell",
                        "Ticker": ticker,
                        "Current Cash": f"{current_cash:.2f}",
                        "Holding Value": f"{holding_value:.2f}",
                        "Portfolio Value": f"{current_cash + holding_value:.2f}"
                    })

        cash_per_share = current_cash * 1 / len(rows)
        trading_counter += 1
        for row in rows:
            ticker = row.get("Ticker")
            if cash_per_share == 0:
                zero_budget_counter += 1
                holding_value = sum(sum(profits.values()) for profits in holdings.values())
                timeline.append({
                    "Date": current_date,
                    "Action": "No Cash",
                    "Ticker": ticker,
                    "Current Cash": f"{current_cash:.2f}",
                    "Holding Value": f"{holding_value:.2f}",
                    "Portfolio Value": f"{current_cash + holding_value:.2f}"
                })
                continue

            current_cash -= cash_per_share
            percentage = float(row["Profit"].replace("%", ""))
            future_profit = cash_per_share * (1 + percentage / 100)
            sell_date = row.get("Sell Date")
            holdings.setdefault(sell_date, {})[ticker] = future_profit
            holding_value = sum(sum(profits.values()) for profits in holdings.values())
            timeline.append({
                "Date": current_date,
                "Action": "Buy",
                "Ticker": ticker,
                "Current Cash": f"{current_cash:.2f}",
                "Holding Value": f"{holding_value:.2f}",
                "Portfolio Value": f"{current_cash + holding_value:.2f}"
            })

    # Add remaining holdings to budget
    for profits in holdings.values():
        current_cash += sum(profits.values())

    percentage = (current_cash - initial_cash) / initial_cash * 100
    print(f"{percentage:.2f}%, {zero_budget_counter} zero budget days / {trading_counter} trading days")

    # New: write simulation timeline to CSV file with header and rows
    timeline_file = os.path.join(output_dir, 'screen_results_timeline.csv')
    with open(timeline_file, mode='w', newline='') as csvfile:
        fieldnames = ["Date", "Action", "Ticker", "Current Cash", "Holding Value", "Portfolio Value"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in timeline:
            writer.writerow(row)


def back_test(PRICE_DATA, end_date):
    output_dir = os.path.join(os.path.dirname(DIR), 'screen_results')
    file_path = os.path.join(output_dir, f'screen_results.csv')
    global_holding_days = []
    global_profits = []

    if not os.path.exists(file_path):
        return
    with open(file_path, mode="r", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        rows = list(reader)

    total_holding_days = 0
    total_profit = 0
    count = 0
    has_average = False
    index_to_pop = None
    for i, row in enumerate(rows):
        if row["Ticker"] == "AVERAGE":
            index_to_pop = i
            continue

        screening_date = row["Date"]
        if screening_date == "":
            continue
        start_timestamp = int(datetime.datetime.strptime(screening_date, "%Y-%m-%d").timestamp())
        candles = PRICE_DATA[row["Ticker"]]["candles"]
        candles_dict = {str(candle["datetime"]): candle for candle in candles}
        buy_timestamp, sell_timestamp, profit, sell_reason = check_stop_loss(start_timestamp, candles_dict)
        sell_date = datetime.datetime.fromtimestamp(sell_timestamp).strftime("%Y-%m-%d")
        row["Sell Date"] = sell_date
        row["Profit"] = f"{profit * 100:.4f}%"
        holding_days = (datetime.datetime.fromtimestamp(sell_timestamp) - datetime.datetime.fromtimestamp(
            buy_timestamp)).days
        row["Holding Duration"] = str(holding_days)
        row["Sell reason"] = sell_reason
        total_holding_days += holding_days
        total_profit += profit
        count += 1
        global_holding_days.append(holding_days)
        global_profits.append(profit)

    if index_to_pop: rows.pop(index_to_pop)

    if count > 0 and len(rows) > 0:
        avg_held = total_holding_days / count
        avg_profit = total_profit / count
        r = sum(global_profits) / len(global_profits)
        d = sum(global_holding_days) / len(global_holding_days)
        n = 365 / d if d > 0 else 0
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
        n = 365 / d if d > 0 else 0
        f = (1 + r) ** n
        annualized_return = (f - 1) * 100
        print(f"Global Average Profit: {r * 100:.4f}%")
        print(f"Global Average Holding Days: {d:.2f}")
        print(f"Annualized Return: {annualized_return:.4f}%")
        print("\n")


def main():
    PRICE_DATA = rs_ranking.load_data()
    file_path = os.path.join(os.path.dirname(DIR), 'screen_results', 'screen_results.csv')
    if os.path.exists(file_path):
        os.remove(file_path)
    screen_stocks(PRICE_DATA)
    # simulate()


if __name__ == "__main__":
    main()
