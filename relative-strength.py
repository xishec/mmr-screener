import rs_data
import rs_ranking
import sys


def main():
    rs_data.main()
    rs_ranking.main(skipEnter="true")


if __name__ == "__main__":
    main()
