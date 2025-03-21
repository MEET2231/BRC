import sys
from collections import defaultdict
from threading import Thread, Lock
import os
import math

global_stats = defaultdict(lambda: {"min": float('inf'), "max": float('-inf'), "sum": 0, "count": 0})
stats_lock = Lock()

def round_up(value, decimals=1):
    factor = 10 ** decimals
    return math.ceil(value * factor) / factor

def process_chunk(chunk):
    local_stats = defaultdict(lambda: {"min": float('inf'), "max": float('-inf'), "sum": 0, "count": 0})
    for line in chunk:
        city, temp = line.strip().split(";")
        temp = float(temp)

        if temp < local_stats[city]["min"]:
            local_stats[city]["min"] = temp
        if temp > local_stats[city]["max"]:
            local_stats[city]["max"] = temp
        local_stats[city]["sum"] += temp
        local_stats[city]["count"] += 1

    with stats_lock:
        for city, stats in local_stats.items():
            if stats["min"] < global_stats[city]["min"]:
                global_stats[city]["min"] = stats["min"]
            if stats["max"] > global_stats[city]["max"]:
                global_stats[city]["max"] = stats["max"]
            global_stats[city]["sum"] += stats["sum"]
            global_stats[city]["count"] += stats["count"]

def main():
    input_file = "testcase.txt"
    output_file = "output.txt"
    num_threads = os.cpu_count() or 4
    chunk_size = 1024 * 1024
    threads = []

    with open(input_file, "r") as file:
        while True:
            chunk = file.readlines(chunk_size)
            if not chunk:
                break

            thread = Thread(target=process_chunk, args=(chunk,))
            thread.start()
            threads.append(thread)

            if len(threads) >= num_threads:
                for t in threads:
                    t.join()
                threads = []

    for t in threads:
        t.join()

    sorted_cities = sorted(global_stats.keys())
    output = []
    for city in sorted_cities:
        stats = global_stats[city]
        mean_temp = stats["sum"] / stats["count"]
        output_line = f"{city}={round_up(stats['min'], 1)}/{round_up(mean_temp, 1)}/{round_up(stats['max'], 1)}"
        output.append(output_line)

    with open(output_file, "w") as file:
        file.write("\n".join(output))

if __name__ == "__main__":
    main()