from tick_util import TickDataProcessor
import os


data_dir = "./data"  

num_threads = os.cpu_count()

processor = TickDataProcessor(data_dir, num_threads=num_threads)

processor.process_files()


processor.write_data_to_binary(processor.cleaned_data, "tick_data.bin", "tick_data_index.pkl")
