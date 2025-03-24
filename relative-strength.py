import string

import rs_data
import rs_ranking


def main():
    for char in string.ascii_lowercase:
        rs_data.main(char)
    rs_ranking.main(skipEnter="true")


if __name__ == "__main__":
    main()
