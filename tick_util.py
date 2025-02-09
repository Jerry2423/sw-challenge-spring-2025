import os
import csv
import struct
import threading
from datetime import datetime
import queue
import pickle
import time


class TickDataProcessor:
    def __init__(self, data_dir, num_threads=1):
        self.csv_files = sorted(os.listdir(data_dir))
        # print(self.csv_files[:5])
        self.data_dir = data_dir
        self.num_threads = num_threads
        self.cleaned_data = []

    def _clean_data(self, row, prev_price=None):
        try:
            timestamp = datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S.%f")
            price = float(row[1])
            size = int(row[2])
        except ValueError:
            return None  

        if price <= 0 or size <= 0:
            return None

        if prev_price and abs(price - prev_price) / prev_price > 0.5:
            return None

        if not (9 <= timestamp.hour < 16):
            return None

        return timestamp, price, size

    def load_and_clean_csv(self, file_paths, result_queue, tid):
        cleaned_data = []
        prev_price = None
        for file_path in file_paths:
            with open(os.path.join(self.data_dir, file_path), mode="r", newline="", encoding="utf-8") as file:
                reader = csv.reader(file)
                for row in reader:
                    clean_row = self._clean_data(row, prev_price)
                    if clean_row:
                        cleaned_data.append(clean_row)
                        prev_price = clean_row[1]
        result_queue.put((tid, cleaned_data))

    def process_files(self):
        chunk_size = len(self.csv_files) // self.num_threads
        result_queue = queue.Queue()
        threads = []
        for i in range(self.num_threads):
            start = i * chunk_size
            end = (i + 1) * chunk_size if i != self.num_threads - \
                1 else len(self.csv_files)
            file_paths = self.csv_files[start:end]
            thread = threading.Thread(
                target=self.load_and_clean_csv, args=(file_paths, result_queue, i))
            threads.append(thread)
            thread.start()

        for t in threads:
            t.join()

        tmp_result = [None] * self.num_threads
        for i in range(self.num_threads):
            thread_data = result_queue.get()
            tmp_result[thread_data[0]] = thread_data[1]

        for data in tmp_result:
            if data:
                self.cleaned_data.extend(data)

        # print("All threads finished processing.")
        # print(f"Total records: {len(self.cleaned_data)}")
        # print(f"Sample data: {self.cleaned_data[:5]}")

    def write_data_to_binary(self, data, binary_file, index_file):
        index = {}  # Index: key is Unix timestamp (accurate to the minute), value is the offset of the first data item in that minute
        current_minute = None  # The current minute being processed

        with open(binary_file, "wb") as f:
            for time_stamp, price, volume in data:
                # Convert datetime.datetime to Unix timestamp (float)
                unix_time = time_stamp.timestamp()

                # Pack the data into binary format
                packed_data = struct.pack("ffi", unix_time, price, volume)

                # Record the current write position
                start_pos = f.tell()

                # Write to the binary file
                f.write(packed_data)

                # Group by minute, record the offset of the first data item in that minute
                minute_key = int(unix_time // 60) * 60  # Unix timestamp accurate to the minute
                if minute_key != current_minute:
                    index[minute_key] = start_pos  # Record the offset of the first data item in that minute
                    current_minute = minute_key
            
            # Append a placeholder data item
            if current_minute is not None:
                placeholder_time = current_minute + 60  # Next minute
                placeholder_data = struct.pack("ffi", placeholder_time, 0.0, 0)
                start_pos = f.tell()
                f.write(placeholder_data)
                index[placeholder_time] = start_pos


        # Save the index as a pickle file
        with open(index_file, "wb") as f:
            pickle.dump(index, f)


class TickDataQuery:
    def __init__(self, binary_file, index_file, num_threads=1):
        self.binary_file = binary_file
        self.index_file = index_file
        self.index = self.load_index()
        self.num_threads = num_threads
        self.result_lock = threading.Lock()

    def load_index(self):
        with open(self.index_file, "rb") as f:
            index = pickle.load(f)
        return index

    def process_range(self, start_offset, end_offset, result, tid, time_s, time_e):
        min_price = float("inf")
        max_price = 0.0
        total_volume = 0
        start_price = None
        end_price = None
        with open(self.binary_file, "rb") as f:
            f.seek(start_offset)
            cnt = 0
            while f.tell() < end_offset:
                packed_data = f.read(struct.calcsize("ffi"))
                if not packed_data:
                    break

                unix_time, price, volume = struct.unpack("ffi", packed_data)
                if unix_time < time_s.timestamp() or unix_time > time_e.timestamp():
                    continue
                if tid == 0 and cnt == 0:
                    start_price = price
                if tid == self.num_threads - 1:
                    end_price = price
                min_price = min(min_price, price)
                max_price = max(max_price, price)
                total_volume += volume
                cnt += 1
        
        with self.result_lock:
            result["min_price"] = min(result["min_price"], min_price)
            result["max_price"] = max(result["max_price"], max_price)
            result["total_volume"] += total_volume
            if start_price is not None:
                result["start_price"] = start_price
            if end_price is not None:
                result["end_price"] = end_price
        

    def query_data(self, start_time, end_time):
        start_key = int(start_time.timestamp() // 60) * 60
        end_key = int((end_time.timestamp() + 60 - 1) // 60) * 60
        offset_start = self.index.get(start_key)
        orfset_end = self.index.get(end_key)
        if offset_start is None or orfset_end is None:
            print("No data available for the specified time range.")
            return
        total_elements = (orfset_end - offset_start) // struct.calcsize("ffi")
        chunk_size = total_elements // self.num_threads

        threads = []
        result = {"min_price": float("inf"), "max_price": 0.0, "total_volume": 0, "start_price": None, "end_price": None}
        for i in range(self.num_threads):
            start = offset_start + i * chunk_size * struct.calcsize("ffi")
            end = offset_start + (i + 1) * chunk_size * struct.calcsize("ffi")
            if i == self.num_threads - 1:
                end = orfset_end
            thread = threading.Thread(target=self.process_range, args=(start, end, result, i, start_time, end_time))
            threads.append(thread)
            thread.start()

        for t in threads:
            t.join()
        
        print(result)
        with open("query_result.csv", "w", newline="") as csvfile:
            fieldnames = ["min_price", "max_price", "total_volume", "start_price", "end_price"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerow(result)
        print("Query result saved to query_result.csv")
        