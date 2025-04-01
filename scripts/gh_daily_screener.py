import datetime
import os

from scripts import rs_ranking

DIR = os.path.dirname(os.path.realpath(__file__))


def main():
    rs_ranking.main(new_csv=True)


if __name__ == "__main__":
    main()
