import os
import datetime
import sys
from dateutil.relativedelta import relativedelta
import yfinance as yf
import csv
import time
import pandas as pd
import json

from scripts import rs_ranking

DIR = os.path.dirname(os.path.realpath(__file__))
OUTPUT_DIR = os.path.join(os.path.dirname(DIR), 'data_persist')
CACHE_FILE = os.path.join(OUTPUT_DIR, '_market_cap_cache.json')

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
    original_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    for i in range(0, 11):
        try_date = original_date - datetime.timedelta(days=i)
        try_date_str = try_date.strftime("%Y-%m-%d")
        path = os.path.join(os.path.dirname(DIR), 'rs_stocks', f'rs_stocks_{try_date_str}.csv')
        if os.path.exists(path):
            with open(path, mode='r') as csv_file:
                csv_reader = csv.reader(csv_file)
                rows = list(csv_reader)
                first_half_rows = rows[:len(rows) // 2 + 1]
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


def find_last_last_max_price(prices, max_value):
    close_prices = [candle['close'] for candle in prices['candles']]
    for index, price in enumerate(reversed(close_prices)):
        if index == 0: continue
        if index == 1: continue
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
            if last_close == 0: return 0
            return (this_close - last_close) / last_close
    return None


def get_close_avg_movement_last_period(prices, period, shift_back=1):
    candles = prices['candles'][(-period + 1):]
    changes = []
    for i in range(1, len(candles)):
        prev_close = candles[i - 1]['close']
        if prev_close == 0:
            change = 0  # or use `continue` to skip this iteration
        else:
            change = (candles[i]['close'] - prev_close) / prev_close
        changes.append(change)
    sum_abs = sum(abs(change) for change in changes)
    return sum_abs / period


def get_close_max_movement_last_period(prices, period):
    candles = prices['candles'][(-period + 1):]
    changes = []
    for i in range(1, len(candles)):
        prev_close = candles[i - 1]['close']
        if prev_close == 0:
            change = 0  # or use `continue` to skip this iteration
        else:
            change = (candles[i]['close'] - prev_close) / prev_close
        changes.append(change)
    max_abs = max(abs(change) for change in changes)
    return max_abs


def get_market_cap_info(ticker_symbol):
    global market_cap_cache

    # # Check if market cap is already cached
    # if ticker_symbol in market_cap_cache:
    #     market_cap = market_cap_cache[ticker_symbol]["market_cap"]
    #     beta = market_cap_cache[ticker_symbol]["beta"]
    #     next_earning = market_cap_cache[ticker_symbol]["next_earning"]
    #     exchange = market_cap_cache[ticker_symbol]["exchange"]
    #     return market_cap, beta, next_earning, exchange

    for _ in range(2):
        try:
            ticker = yf.Ticker(ticker_symbol)
            info = ticker.info
            market_cap = info.get("marketCap")
            # beta = info.get("beta")
            # exchange = info.get("exchange")
            # currency = info.get("currency")
            # next_earning = ticker.calendar["Earnings Date"][0].strftime("%Y-%m-%d")
            if market_cap is None:
                market_cap = 0
            market_cap_cache.setdefault(ticker_symbol, {})["market_cap"] = market_cap
            return market_cap, info
        except Exception:
            if _ == 1:
                time.sleep(1)
            else:
                market_cap_cache.setdefault(ticker_symbol, {})["market_cap"] = 0
    return 0, None


def screen(PRICE_DATA, filtered_price_date, end_date, new_csv=False):
    first_half_rows = load_csv(end_date)
    price_history = filtered_price_date
    results = []

    filtered_rows = [row for row in first_half_rows if row[0] in price_history]
    total = len(filtered_rows)
    for i, row in enumerate(filtered_rows):
        vix_ticker = "^VIX"
        vix = price_history[vix_ticker]["candles"][-1]["close"]
        vix_sma20 = calculate_sma(price_history[vix_ticker], 10)
        # if vix > 22 or vix > vix_sma20:
        # if vix > 20:
        # print(f"VIX is too high: {vix}, (SMA20 {vix_sma20})\n")
        # return

        ticker = row[0]
        # print(f"Screening stocks {ticker} \r{i + 1} / {total}, {(i + 1) / total * 100:.2f}% ", end="", flush=True)

        sma10 = calculate_sma(price_history[ticker], 10)
        sma50 = calculate_sma(price_history[ticker], 50)
        sma150 = calculate_sma(price_history[ticker], 150)
        sma200 = calculate_sma(price_history[ticker], 200)
        sma200_22 = calculate_sma(price_history[ticker], 200, 22)

        if len(price_history[ticker]["candles"]) < 2:
            continue
        latest_close_price = price_history[ticker]["candles"][-1]["close"]
        yesterday_close_price = price_history[ticker]["candles"][-2]["close"]
        volume = price_history[ticker]["candles"][-1]["volume"]
        date = price_history[ticker]["candles"][-1]["datetime"]
        price_change = get_change_on_date(price_history[ticker], date) * 100

        last_max_price = find_last_max_price(price_history[ticker], latest_close_price)
        last_max_price_yesterday = find_last_last_max_price(price_history[ticker], yesterday_close_price)
        last_max_volume = find_last_max_volume(price_history[ticker], volume)

        close_sma10 = latest_close_price / sma10 if sma10 else 0
        close_sma50 = latest_close_price / sma50 if sma50 else 0
        close_sma150 = latest_close_price / sma150 if sma150 else 0
        close_sma200 = latest_close_price / sma200 if sma200 else 0
        sma50_sma150 = sma50 / sma150 if sma150 else 0
        sma50_sma200 = sma50 / sma200 if sma200 else 0
        sma150_sma200 = sma150 / sma200 if sma200 else 0
        trending_up = sma200 / sma200_22 if sma200_22 else 0

        mm_conditions = [
            close_sma50 > 1,
            close_sma150 > 1,
            close_sma200 > 1,
            sma50_sma150 > 1,
            sma50_sma200 > 1,
            sma150_sma200 > 1,
            trending_up > 1,
        ]

        mm_score = sum(1 for mm_condition in mm_conditions if mm_condition)

        avg_volume100 = find_avg_volume(price_history[ticker], 100)
        volume_volume100 = volume / avg_volume100 if avg_volume100 else 0
        max_mov5 = get_close_max_movement_last_period(price_history[ticker], 5) * 100
        max_mov100 = get_close_max_movement_last_period(price_history[ticker], 100) * 100

        # last_max_price > 90 -> A price like this must be at least 90 days ago -> soit at ATH, soit ATH since 90 days
        # 90 > last_max_price_yesterday -> we had yesterday's price within 90d -> I need at least one mini vcp loop
        is_breakout = 0 < price_change < 8 and last_max_price > 90 > last_max_price_yesterday > 2

        if mm_score >= 6 and is_breakout:
            market_cap, info = get_market_cap_info(ticker)
            beta = info.get("beta")
            exchange = info.get("exchange")
            currency = info.get("currency")
            summary =  info.get("longBusinessSummary")

            if market_cap == 0: continue
            market_cap_billion = market_cap / 1e9
            # google_sheet_condition = ((beta is None or (beta is not None and beta <= 1.1)) and 0.5 <= max_mov5 <= 9 and
            #                           max_mov100 <= 9 and 1.02 <= close_sma10)

            xc_conditions = [
                0 <= price_change <= 1,
                beta is None or beta <= 1.1,
                0.5 <= max_mov5 <= 9,
                max_mov100 <= 9,
                1.02 <= close_sma10,
                888 <= last_max_price,
                5 <= market_cap_billion < 50
            ]

            xc_score = sum(1 for xc_condition in xc_conditions if xc_condition)

            date_string = datetime.datetime.fromtimestamp(date).strftime('%Y-%m-%d')

            if xc_score >= 5 and 1 <= market_cap_billion < 200:
                results.append(
                    (ticker, f"{mm_score}/7 {xc_score}/7", date_string, exchange,
                     currency, summary, last_max_price, last_max_volume,
                     f"{close_sma50:>6.2f}", f"{close_sma150:>6.2f}", f"{close_sma200:>6.2f}",
                     f"{sma50_sma150:>6.2f}", f"{sma50_sma200:>6.2f}", f"{sma150_sma200:>6.2f}",
                     f"{trending_up:>6.2f}", beta, f"{max_mov5:>6.2f}", f"{max_mov100:>6.2f}",
                     f"{vix:>6.2f}", f"{vix_sma20:>6.2f}", f"{close_sma10:>6.2f}",
                     f"{mm_score}/7", f"{xc_score}/7", f"N/A", "N/A")),

    save_market_cap_cache()
    print("\n")
    if len(results) == 0:
        print("No stocks passed\n")
        results = [("No stocks paassed", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-",
                    "-", "-", "-", "-", "-", "-", "-")]

    df = pd.DataFrame(results,
                      columns=["Ticker", "Scores", "Date", "Exchange",
                               "Currency", "Summary", "Last max price", "Last max volume",
                               "Close / SMA50", "Close / SMA150", "Close / SMA200",
                               "SMA50 / SMA150", "SMA50 / SMA200", "SMA150 / SMA200",
                               "Trending up", "Beta", "Max mov5", "Max mov30",
                               "VIX", "VIX SMA20", "Close / SMA10",
                               "Sell Date", "Holding Duration", "Profit", "Sell reason"])
    df = df.sort_values((["Ticker"]), ascending=True)

    if new_csv:
        results_with_candles = {}
        for r in results:
            ticker = r[0]
            if ticker in PRICE_DATA:
                results_with_candles[ticker] = {
                    "price_data": PRICE_DATA[ticker],
                    "score": r[1],
                    "date": r[2],
                    "exchange": r[3],
                    "currency": r[4],
                    "summary": r[5],
                }
        json_path = os.path.join(os.path.dirname(DIR), 'screen_results/daily', f'screen_results_{end_date}.json')
        with open(json_path, 'w') as outfile:
            json.dump(results_with_candles, outfile)
    else:
        output_path = os.path.join(os.path.dirname(DIR), 'screen_results', f'screen_results.csv')
        header = not os.path.exists(output_path)
        df.to_csv(output_path, mode='a', index=False, header=header)

    print(df)
    print("\n")


def main(PRICE_DATA, filtered_price_date=None, end_date=None, new_csv=False):
    if filtered_price_date is not None and end_date is not None:
        screen(PRICE_DATA, filtered_price_date, end_date, new_csv)


if __name__ == "__main__":
    main()
