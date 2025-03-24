import pandas as pd
import yfinance as yf
import json
from time import sleep
import requests
from pathlib import Path
from io import StringIO  # Added this due to error
from yfinance.exceptions import YFException


def get_ticker_info(symbol, session):
    """Function to retrieve ticker information with retry logic"""
    tries = 1
    for attempt in range(tries):
        try:
            ticker = yf.Ticker(symbol, session=session)
            info = ticker.info
            sector = info.get('sector')
            industry = info.get('industry')
            marketCap = info.get('marketCap')

            if sector and industry and marketCap and sector != 'Unknown' and industry != 'Unknown' and marketCap != 'Unknown':
                return sector, industry, marketCap

            if attempt < tries - 1:
                print(f"Attempt {attempt + 1} failed for {symbol}, retrying in 2s...")
                sleep(2)

        except Exception as e:
            if attempt < tries - 1:
                print(f"Error for {symbol} (attempt {attempt + 1}): {e}, retrying in 2s...")
                sleep(2)
            else:
                print(f"Final failure for {symbol}: {e}")

    return None, None, None


def process_nasdaq_file():
    """Process NASDAQ symbols and update JSON file with sector/industry/marketCap data"""

    # Get NASDAQ symbols directly from URL
    url = "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqtraded.txt"
    result = {}

    # Define path for output file
    script_dir = Path(__file__).parent
    output_file = script_dir / 'ticker_info.json'

    # Load existing data if available
    # if output_file.exists():
    #     try:
    #         with open(output_file, 'r') as f:
    #             result = json.load(f)
    #         print(f"Loaded existing data with {len(result)} entries")
    #     except Exception as e:
    #         print(f"Error loading existing file: {e}")
    #         pass

    try:
        # Download and process NASDAQ data
        response = requests.get(url)
        response.raise_for_status()

        # Read data into DataFrame
        df = pd.read_csv(StringIO(response.text), delimiter='|')

        # # Get only the first 150 elements
        # df = df.head(150)

        print(f"Retrieved {len(df)} symbols from NASDAQ")

        session = requests.Session()

        for _, row in df.iterrows():
            symbol = row['Symbol']
            if symbol not in result:
                sector, industry, marketCap = get_ticker_info(symbol, session)

                if sector and industry and marketCap:
                    result[symbol] = {
                        "info": {
                            "industry": industry,
                            "sector": sector,
                            "marketCap": marketCap
                        }
                    }
                    print(f"Added: {symbol} - {sector}/{industry}/{marketCap}")

                    # Save after each successful addition
                    with open(output_file, 'w') as f:
                        json.dump(result, f, indent=2)
                # else:
                # print(f"Skipped (missing data after retries): {symbol}")

                sleep(0.5)  # Pause between symbols

    except Exception as e:
        print(f"Error processing NASDAQ data: {e}")
        raise

    print(f"Final dataset contains {len(result)} symbols")
    return result


if __name__ == "__main__":
    process_nasdaq_file()
