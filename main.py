from tick_util import TickDataQuery

import sys
from datetime import datetime, time
import os

def is_trading_time(dt):
    trading_start = time(9, 30)
    trading_end = time(16, 0)
    return trading_start <= dt.time() <= trading_end

def main():
    if len(sys.argv) != 3:
        print(sys.argv)
        print("Usage: python main.py <start_time> <end_time>")
        sys.exit(1)

    start_time_str = sys.argv[1]
    end_time_str = sys.argv[2]

    try:
        start_time = datetime.strptime(start_time_str, "%Y-%m-%d-%H:%M:%S.%f")
        end_time = datetime.strptime(end_time_str, "%Y-%m-%d-%H:%M:%S.%f")
    except ValueError:
        print("Invalid time format. Use UTC format: YYYY-MM-DD-HH:MM:SS.sss")
        sys.exit(1)

    if start_time >= end_time:
        print("Error: start_time should be earlier than end_time")
        sys.exit(1)

    if not is_trading_time(start_time) or not is_trading_time(end_time):
        print("Error: Both start_time and end_time should be within trading hours (9:30 AM to 4:00 PM)")
        sys.exit(1)

    num_threads = os.cpu_count()
    query = TickDataQuery("tick_data.bin", "tick_data_index.pkl", num_threads)
    query.query_data(start_time, end_time)

if __name__ == "__main__":
    main()