import math
import mmap
import multiprocessing
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Dict, List, Tuple

def round_up(value: float) -> float:
    """Round up a value to one decimal place."""
    return math.ceil(value * 10) / 10

def initialize_city_data() -> List[float]:
    """Initialize city statistics: [min, max, total, count]."""
    return [math.inf, -math.inf, 0.0, 0]

def process_file_chunk(filename: str, start_offset: int, end_offset: int) -> Dict[bytes, List[float]]:
    """Process a chunk of the file and return city statistics."""
    city_data = defaultdict(initialize_city_data)  # Use a named function instead of lambda

    with open(filename, "rb") as file:
        with mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ) as memory_map:
            # Adjust start offset to the beginning of the next line
            if start_offset != 0:
                while start_offset < len(memory_map) and memory_map[start_offset] != ord('\n'):
                    start_offset += 1
                start_offset += 1

            # Adjust end offset to the end of the current line
            end = end_offset
            while end < len(memory_map) and memory_map[end] != ord('\n'):
                end += 1
            if end < len(memory_map):
                end += 1

            # Process the chunk
            chunk = memory_map[start_offset:end]

    for line in chunk.splitlines():
        if not line:
            continue

        city, _, score_str = line.partition(b';')
        if not score_str:
            continue

        try:
            score = float(score_str)
        except ValueError:
            continue

        stats = city_data[city]
        stats[0] = min(stats[0], score)
        stats[1] = max(stats[1], score)
        stats[2] += score
        stats[3] += 1

    return city_data

def merge_city_data(data_list: List[Dict[bytes, List[float]]]) -> Dict[bytes, List[float]]:
    """Merge city statistics from multiple chunks."""
    final_data = defaultdict(initialize_city_data)  # Use a named function instead of lambda

    for data in data_list:
        for city, stats in data.items():
            final_stats = final_data[city]
            final_stats[0] = min(final_stats[0], stats[0])
            final_stats[1] = max(final_stats[1], stats[1])
            final_stats[2] += stats[2]
            final_stats[3] += stats[3]

    return final_data

def main(input_filename: str = "testcase.txt", output_filename: str = "output.txt") -> None:
    """Main function to process the file and generate output."""
    with open(input_filename, "rb") as file:
        with mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ) as memory_map:
            file_size = len(memory_map)

    num_processes = multiprocessing.cpu_count() * 2
    chunk_size = file_size // num_processes
    chunks = [(input_filename, i * chunk_size, (i + 1) * chunk_size if i < num_processes - 1 else file_size)
              for i in range(num_processes)]

    with ProcessPoolExecutor(max_workers=num_processes) as executor:
        futures = [executor.submit(process_file_chunk, *chunk) for chunk in chunks]
        results = [future.result() for future in as_completed(futures)]

    final_data = merge_city_data(results)

    # Generate output
    output_lines = []
    for city in sorted(final_data.keys(), key=lambda c: c.decode()):
        min_score, max_score, total_score, count = final_data[city]
        average_score = round_up(total_score / count)
        output_lines.append(f"{city.decode()}={round_up(min_score):.1f}/{average_score:.1f}/{round_up(max_score):.1f}\n")

    with open(output_filename, "w") as file:
        file.writelines(output_lines)

if __name__ == "__main__":
    main()
