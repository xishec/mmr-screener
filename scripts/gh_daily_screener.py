import datetime
import os

from dateutil.relativedelta import relativedelta

from scripts import rs_ranking

DIR = os.path.dirname(os.path.realpath(__file__))


def main():
    PRICE_DATA = rs_ranking.load_data()

    today = datetime.datetime.today()
    # today = datetime.datetime(2025, 7, 16)

    current_date = today - relativedelta(days=60)
    end_date = today

    last_ts = None
    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")
        timestamp = rs_ranking.find_closest_date(PRICE_DATA, date_str)
        if timestamp != last_ts:
            rs_ranking.main(PRICE_DATA, timestamp, new_csv=True)
            last_ts = timestamp

        current_date += relativedelta(days=1)


if __name__ == "__main__":
    main()
