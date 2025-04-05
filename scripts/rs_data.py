import gzip
import json
import sys
import time
import datetime as dt
import os
import requests
import yaml
import yfinance as yf
import pandas as pd
import dateutil.relativedelta
import numpy as np
from io import StringIO
from datetime import date
from datetime import datetime
from dateutil.relativedelta import relativedelta
from requests.exceptions import ConnectionError, HTTPError, Timeout

DIR = os.path.dirname(os.path.realpath(__file__))

try:
    with open(os.path.join(DIR, 'config_private.yaml'), 'r') as stream:
        private_config = yaml.safe_load(stream)
except FileNotFoundError:
    private_config = None
except yaml.YAMLError as exc:
    print(exc)

try:
    file_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                             'config.yaml')
    with open(file_path, 'r') as stream:
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


def get_tickers_from_nasdaq():
    print("*** Loading Stocks from Nasdaq ***")
    url = "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqtraded.txt"
    max_retries = 5
    backoff_factor = 1

    for attempt in range(max_retries):
        try:
            response = requests.get(url)
            response.raise_for_status()
            df = pd.read_csv(StringIO(response.text), delimiter='|')

            # Drop rows with missing 'Symbol' values and filter out rows with '$' in 'Symbol'
            filtered_symbols = df.dropna(subset=['Symbol'])
            filtered_symbols = filtered_symbols[~filtered_symbols['Symbol'].str.contains(r'\$')]
            filtered_symbols['Symbol'] = filtered_symbols['Symbol'].str.replace(".", "", regex=False)

            # Extract the 'Symbol' column as a list
            symbols_list = filtered_symbols['Symbol'].tolist()

            print(f"Retrieved {len(symbols_list)} symbols from NASDAQ")
            return symbols_list
        except (ConnectionError, HTTPError, Timeout) as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                sleep_time = backoff_factor * (2 ** attempt)
                print(f"Retrying in {sleep_time} seconds...")
                time.sleep(sleep_time)
            else:
                print("Max retries reached. Exiting.")
                raise


def write_to_file(dict, file):
    with open(file, "w", encoding='utf8') as fp:
        json.dump(dict, fp, ensure_ascii=False)


def write_price_history_file(char, tickers_dict):
    directory = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data_persist')
    os.makedirs(directory, exist_ok=True)
    path = os.path.join(directory, f'{char.lower()}_price_history.json.gz')
    with gzip.open(path, 'wb') as f_out:
        json_str = json.dumps(tickers_dict)
        f_out.write(json_str.encode('utf-8'))


def print_data_progress(ticker, idx, tickers, error_text, elapsed_s, remaining_s):
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
        f'{ticker} from {error_text} ({idx + 1} / {len(tickers)}). Elapsed: {elapsed.hours}h {elapsed.minutes}m {elapsed.seconds}s. Remaining: {remaining_string}.')


def get_remaining_seconds(all_load_times, idx, len):
    load_time_ma = pd.Series(all_load_times).rolling(np.minimum(idx + 1, 25)).mean().tail(1).item()
    remaining_seconds = (len - idx) * load_time_ma
    return remaining_seconds


def get_yf_data(ticker, start_date, end_date):
    ticker_data = {}

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
            ticker,
            start=start_date,
            end=end_date,
            auto_adjust=False,
            progress=False,
            session=session,
            rounding=True
        )

        # Add a small delay to avoid rate limiting (adjust as needed)
        time.sleep(0.1)

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
        timestamps = timestamps = [int((timestamp + relativedelta(hours=16)).timestamp())
                                   for timestamp in timestamps]

        opens = list(yahoo_response["Open"].values())
        closes = list(yahoo_response["Adj Close"].values())
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


def load_prices_from_yahoo(char):
    today = date.today()
    start = time.time()
    s2018 = datetime.strptime("2022-01-01", "%Y-%m-%d") - relativedelta(years=2)
    # start_date = today - dt.timedelta(days=5 * 365)
    start_date = s2018
    tickers_dict = {}
    load_times = []
    failed_tickers = []

    # Add retry mechanism
    max_retries = 2
    base_delay = 2  # seconds

    tickers = ([ticker for ticker in get_tickers_from_nasdaq()
                if (ticker.lower().startswith(char.lower()))]
               + ["SPY", "^VIX"])

    print("*** Loading Stocks from Yahoo Finance ***")
    for idx, ticker in enumerate(tickers):
        retry_count = 0
        success = False

        r_start = 0
        ticker_data = None
        while retry_count < max_retries and not success:
            r_start = time.time()

            # If this is a retry, wait with exponential backoff
            if retry_count > 0:
                retry_delay = base_delay * (2 ** (retry_count - 1))  # Exponential backoff
                print(f"Retry {retry_count}/{max_retries} for {ticker}, waiting {retry_delay}s...")
                time.sleep(retry_delay)

            # Use the updated get_yf_data function
            ticker_data = get_yf_data(ticker, start_date, today)

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

        # Track timing and progress
        now = time.time()
        current_load_time = now - r_start
        load_times.append(current_load_time)
        remaining_seconds = get_remaining_seconds(load_times, idx, len(tickers))
        print_data_progress(ticker, idx, tickers, "", time.time() - start, remaining_seconds)

        # Add to results dictionary
        tickers_dict[ticker] = ticker_data

    # Write results to file
    write_price_history_file(char, tickers_dict)

    return tickers_dict


def main():
    char = "v" if len(sys.argv) <= 1 else sys.argv[1]
    load_prices_from_yahoo(char)


if __name__ == "__main__":
    main()
