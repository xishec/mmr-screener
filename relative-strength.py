import rs_data
import rs_ranking
import sys
import os

COUNTER_FILE = 'execution_counter.txt'

def get_execution_count():
    if not os.path.exists(COUNTER_FILE):
        return 0
    with open(COUNTER_FILE, 'r') as file:
        return int(file.read().strip())

def increment_execution_count():
    count = get_execution_count() + 1
    if count > 26:
        count = 1
    with open(COUNTER_FILE, 'w') as file:
        file.write(str(count))
    return count

def main():
    char = None if len(sys.argv) <= 1 else sys.argv[1]
    rs_data.main(char)

    count = increment_execution_count()
    if count == 26:
        rs_ranking.main(skipEnter="true")

if __name__ == "__main__":
    main()
