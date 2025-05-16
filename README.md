# mmr-screener 
mr-screener is a stock screener that uses the Yahoo Finance API to screen all US listed stocks.

## What does it do?
1. Every 7 days, [update_stock.py](https://github.com/xishec/mmr-screener/blob/8a633a1269cd965515b1fe0e4b53bdaec53fc42e/data_persist/update_stocks.py) will be executed to update the [stock list](https://github.com/xishec/mmr-screener/blob/19fd8121a2f22674a1a3c1666ea265d9f58a4de3/data_persist/ticker_info.json).It fetches all US listed stocks from the Nasdaq [website](
https://www.nasdaqtrader.com/dynamic/symdir/nasdaqtraded.txt). It includes stock from NASDAQ, NYSE MKT, NYSE, NYSE ARCA, BATS and IEXG.
2. Every night, [relative-strength.py](https://github.com/xishec/mmr-screener/blob/19fd8121a2f22674a1a3c1666ea265d9f58a4de3/relative-strength.py) will be executed to update the [RS Ratings](https://github.com/xishec/mmr-screener/blob/19fd8121a2f22674a1a3c1666ea265d9f58a4de3/output/rs_stocks.csv).
3. Every hour, [screen_stocks.py](https://github.com/xishec/mmr-screener/blob/8a633a1269cd965515b1fe0e4b53bdaec53fc42e/screen_stocks.py) will be executed to screen stocks based on some specific criteria, and update the list and email me. 
