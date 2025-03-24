import gzip

with open('price_history.json', 'rb') as f_in:
    with gzip.open('price_history.json.gz', 'wb') as f_out:
        f_out.writelines(f_in)
