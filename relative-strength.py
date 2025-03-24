import rs_data
import rs_ranking
import sys


def main():
    char = None if len(sys.argv) <= 1 else sys.argv[1]
    rs_data.main(char)
    rs_ranking.main(skipEnter="true")


if __name__ == "__main__":
    main()
