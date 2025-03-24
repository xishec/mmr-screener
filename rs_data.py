#!/usr/bin/env python
import gzip
import json
import time
import datetime as dt
import os
import requests
import yaml
import yfinance as yf
import pandas as pd
import dateutil.relativedelta
import numpy as np
import re
from ftplib import FTP
from io import StringIO

from datetime import date
from datetime import datetime

DIR = os.path.dirname(os.path.realpath(__file__))

# if not os.path.exists(os.path.join(DIR, 'data')):
#     os.makedirs(os.path.join(DIR, 'data'))
if not os.path.exists(os.path.join(DIR, 'tmp')):
    os.makedirs(os.path.join(DIR, 'tmp'))

try:
    with open(os.path.join(DIR, 'config_private.yaml'), 'r') as stream:
        private_config = yaml.safe_load(stream)
except FileNotFoundError:
    private_config = None
except yaml.YAMLError as exc:
    print(exc)

try:
    with open('config.yaml', 'r') as stream:
        config = yaml.safe_load(stream)
except FileNotFoundError:
    config = None
except yaml.YAMLError as exc:
    print(exc)


def cfg(key):
    try:
        return private_config[key]
    except:
        try:
            return config[key]
        except:
            return None


def read_json(json_file):
    with open(json_file, "r", encoding="utf-8") as fp:
        return json.load(fp)


# PRICE_DATA_FILE = os.path.join(DIR, "data", "price_history.json")
REFERENCE_TICKER = cfg("REFERENCE_TICKER")
ALL_STOCKS = cfg("USE_ALL_LISTED_STOCKS")
TICKER_INFO_FILE = os.path.join(DIR, "data_persist", "ticker_info.json")
TICKER_INFO_DICT = read_json(TICKER_INFO_FILE)
REF_TICKER = {"ticker": REFERENCE_TICKER,
              "sector": "--- Reference ---",
              "industry": "--- Reference ---",
              "marketCap": "--- Reference ---",
              "universe": "--- Reference ---"}

UNKNOWN = "unknown"


def get_resolved_securities():
    tickers = {REFERENCE_TICKER: REF_TICKER}
    if ALL_STOCKS:
        return get_tickers_from_nasdaq(tickers)


def exchange_from_symbol(symbol):
    if symbol == "Q":
        return "NASDAQ"
    if symbol == "A":
        return "NYSE MKT"
    if symbol == "N":
        return "NYSE"
    if symbol == "P":
        return "NYSE ARCA"
    if symbol == "Z":
        return "BATS"
    if symbol == "V":
        return "IEXG"
    return "n/a"


def get_tickers_from_nasdaq(tickers):
    filename = "nasdaqtraded.txt"
    ticker_column = 1
    etf_column = 5
    exchange_column = 3
    test_column = 7
    ftp = FTP('ftp.nasdaqtrader.com')
    ftp.login()
    ftp.cwd('SymbolDirectory')
    lines = StringIO()
    ftp.retrlines('RETR ' + filename, lambda x: lines.write(str(x) + '\n'))
    ftp.quit()
    lines.seek(0)
    results = lines.readlines()

    for entry in results:
        sec = {}
        values = entry.split('|')
        ticker = values[ticker_column]
        if ticker in TICKER_INFO_DICT and re.match(r'^[A-Z]+$', ticker) and values[etf_column] == "N" and values[
            test_column] == "N":
            sec["ticker"] = ticker
            sec["sector"] = UNKNOWN
            sec["industry"] = UNKNOWN
            sec["marketCap"] = UNKNOWN
            sec["universe"] = exchange_from_symbol(values[exchange_column])
            tickers[sec["ticker"]] = sec

    return tickers


SECURITIES = get_resolved_securities().values()


# SECURITIES = list(get_resolved_securities().values())[:120]


def write_to_file(dict, file):
    with open(file, "w", encoding='utf8') as fp:
        json.dump(dict, fp, ensure_ascii=False)


def write_price_history_file(tickers_dict):
    with gzip.open('data_persist/price_history.json.gz', 'wb') as f_out:
        json_str = json.dumps(tickers_dict)
        f_out.write(json_str.encode('utf-8'))


# def write_ticker_info_file(info_dict):
#     write_to_file(info_dict, TICKER_INFO_FILE)


def enrich_ticker_data(ticker_response, security):
    ticker_response["sector"] = security["sector"]
    ticker_response["industry"] = security["industry"]
    ticker_response["universe"] = security["universe"]


def print_data_progress(ticker, universe, idx, securities, error_text, elapsed_s, remaining_s):
    dt_ref = datetime.fromtimestamp(0)
    dt_e = datetime.fromtimestamp(elapsed_s)
    elapsed = dateutil.relativedelta.relativedelta(dt_e, dt_ref)
    if remaining_s and not np.isnan(remaining_s):
        dt_r = datetime.fromtimestamp(remaining_s)
        remaining = dateutil.relativedelta.relativedelta(dt_r, dt_ref)
        remaining_string = f'{remaining.hours}h {remaining.minutes}m {remaining.seconds}s'
    else:
        remaining_string = "?"
    print(
        f'{ticker} from {universe}{error_text} ({idx + 1} / {len(securities)}). Elapsed: {elapsed.hours}h {elapsed.minutes}m {elapsed.seconds}s. Remaining: {remaining_string}.')


def get_remaining_seconds(all_load_times, idx, len):
    load_time_ma = pd.Series(all_load_times).rolling(np.minimum(idx + 1, 25)).mean().tail(1).item()
    remaining_seconds = (len - idx) * load_time_ma
    return remaining_seconds


def escape_ticker(ticker):
    return ticker.replace(".", "-")


def get_info_from_dict(dict, key):
    value = dict[key] if key in dict else "n/a"
    # fix unicode
    # value = value.replace("\u2014", " ")
    return value


def load_ticker_info(ticker, info_dict):
    escaped_ticker = escape_ticker(ticker)
    info = yf.Ticker(escaped_ticker)
    try:
        ticker_info = {
            "info": {
                "industry": get_info_from_dict(info.info, "industry"),
                "sector": get_info_from_dict(info.info, "sector"),
                "marketCap": get_info_from_dict(info.info, "marketCap")
            }
        }
    except Exception:
        ticker_info = {
            "info": {
                "industry": "n/a",
                "sector": "n/a",
                "marketCap": "n/a"
            }
        }
    info_dict[ticker] = ticker_info


def get_yf_data(security, start_date, end_date):
    ticker_data = {}
    ticker = security["ticker"]
    escaped_ticker = escape_ticker(ticker)

    try:
        # Import the random user agent function
        # First check if we need to import it
        try:
            from user_agents import get_random_user_agent
        except ImportError:
            # If import fails, use a default method
            # Define the function inline if we can't import it
            def get_random_user_agent():
                import random
                default_agents = [
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36"
                ]
                return random.choice(default_agents)

        # Set a random user agent
        user_agent = get_random_user_agent()

        # Configure yfinance session with the random user agent
        session = requests.Session()
        session.headers['User-Agent'] = user_agent

        # Download data with auto_adjust=False (based on Reddit fix) and using our custom session
        df = yf.download(
            escaped_ticker,
            start=start_date,
            end=end_date,
            auto_adjust=False,
            progress=False,
            session=session
        )

        # Add a small delay to avoid rate limiting (adjust as needed)
        time.sleep(0.01)

        # Check if DataFrame is empty
        if df.empty:
            print(f"No data found for {ticker}, symbol may be delisted or incorrect")
            return None

        # Fix the MultiIndex columns by dropping the second level
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.droplevel(1)

        # Convert to dictionary and process as in the original simple function
        yahoo_response = df.to_dict()

        # Check if we have all required columns
        required_cols = ["Open", "Close", "Low", "High", "Volume"]
        if not all(col in yahoo_response for col in required_cols):
            print(f"Missing required columns for {ticker}. Available: {list(yahoo_response.keys())}")
            return None

        timestamps = list(yahoo_response["Open"].keys())
        timestamps = list(map(lambda timestamp: int(timestamp.timestamp()), timestamps))

        opens = list(yahoo_response["Open"].values())
        closes = list(yahoo_response["Close"].values())
        lows = list(yahoo_response["Low"].values())
        highs = list(yahoo_response["High"].values())
        volumes = list(yahoo_response["Volume"].values())

        candles = []
        for i in range(0, len(opens)):
            candle = {}
            candle["open"] = opens[i]
            candle["close"] = closes[i]
            candle["low"] = lows[i]
            candle["high"] = highs[i]
            candle["volume"] = volumes[i]
            candle["datetime"] = timestamps[i]
            candles.append(candle)

        ticker_data["candles"] = candles
        enrich_ticker_data(ticker_data, security)
        return ticker_data
    except Exception as e:
        print(f"Error downloading data for {ticker}: {str(e)}")
        # Check if it's a rate limit error and wait longer
        if "Too Many Requests" in str(e) or "Rate limit" in str(e):
            print(f"Rate limit hit for {ticker}, will retry later with longer delay")
            # You might want to implement a more sophisticated retry mechanism here

        # Extensive debug info
        if 'df' in locals() and not df.empty:
            print(f"DataFrame shape: {df.shape}")
            print(f"DataFrame columns: {df.columns}")
            print(f"DataFrame index: {type(df.index)}")
            try:
                # Try printing the first row in different formats to help debugging
                print(f"First row (head): {df.head(1)}")

                # Reset index and show columns
                df_reset = df.reset_index()
                print(f"Reset index columns: {df_reset.columns}")
                print(f"First row after reset: {df_reset.iloc[0]}")
            except Exception as inner_e:
                print(f"Error during debug: {str(inner_e)}")
        return None


def load_prices_from_yahoo(securities):
    print("*** Loading Stocks from Yahoo Finance ***")
    today = date.today()
    start = time.time()
    start_date = today - dt.timedelta(days=1 * 365 + 183)  # 183 = 6 months
    tickers_dict = {}
    load_times = []
    failed_tickers = []

    # Add retry mechanism
    max_retries = 2
    base_delay = 2  # seconds

    for idx, security in enumerate(securities):
        ticker = security["ticker"]
        retry_count = 0
        success = False

        if ticker != "SPY" and not ticker in TICKER_INFO_DICT:
            continue

        while retry_count < max_retries and not success:
            r_start = time.time()

            # If this is a retry, wait with exponential backoff
            if retry_count > 0:
                retry_delay = base_delay * (2 ** (retry_count - 1))  # Exponential backoff
                print(f"Retry {retry_count}/{max_retries} for {ticker}, waiting {retry_delay}s...")
                time.sleep(retry_delay)

            # Use the updated get_yf_data function
            ticker_data = get_yf_data(security, start_date, today)

            # If successful, mark as success and break retry loop
            if ticker_data is not None:
                success = True
            else:
                retry_count += 1

        # Handle failed downloads after all retries
        if not success:
            print(f"Failed to download {ticker} after {max_retries} retries")
            failed_tickers.append(ticker)
            continue

        # # Add industry info if available
        # if not ticker in TICKER_INFO_DICT:
        #     try:
        #         load_ticker_info(ticker, TICKER_INFO_DICT)
        #         write_ticker_info_file(TICKER_INFO_DICT)
        #     except Exception as e:
        #         print(f"Error loading ticker info for {ticker}: {str(e)}")

        # # Add industry data safely with error handling
        # try:
        #     ticker_data["industry"] = TICKER_INFO_DICT[ticker]["info"]["industry"]
        # except (KeyError, TypeError):
        #     # Set a default if industry info is missing
        #     ticker_data["industry"] = "Unknown"
        #     print(f"Warning: Could not find industry information for {ticker}")

        # Track timing and progress
        now = time.time()
        current_load_time = now - r_start
        load_times.append(current_load_time)
        remaining_seconds = get_remaining_seconds(load_times, idx, len(securities))
        print_data_progress(ticker, security["universe"], idx, securities, "", time.time() - start, remaining_seconds)

        # Add to results dictionary
        tickers_dict[ticker] = ticker_data

        # # Periodically save results to avoid losing everything if the process fails
        # if idx > 0 and idx % 100 == 0:
        #     print(f"Saving intermediate results after {idx} tickers...")
        #     write_price_history_file(tickers_dict)

    # Report on failures
    if failed_tickers:
        print(f"Failed for {len(failed_tickers)} tickers: {', '.join(failed_tickers[:10])}...")
        with open("failed_tickers.txt", "w") as f:
            f.write("\n".join(failed_tickers))
        print(f"Saved list of failed tickers to failed_tickers.txt")

    # Write results to file
    write_price_history_file(tickers_dict)

    return tickers_dict


def main():
    load_prices_from_yahoo(SECURITIES)
    # write_ticker_info_file(TICKER_INFO_DICT)


if __name__ == "__main__":
    main()
