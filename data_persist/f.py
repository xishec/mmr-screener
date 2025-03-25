# import gzip
#
# with open('price_history.json', 'rb') as f_in:
#     with gzip.open('price_history.json.gz', 'wb') as f_out:
#         f_out.writelines(f_in)
import json

with open('ticker_info.json', 'r') as file:
    ticker_info = json.load(file)

print(len(ticker_info))