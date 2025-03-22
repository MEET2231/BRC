import math
import mmap
import multiprocessing
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

def round_up(value: float) -> float:
    """Round up a value to one decimal place."""
    return math.ceil(value * 10) / 10

def initialize_city_data() -> list:
    """Initialize city statistics: [min, max, total, count]."""
    return [math.inf, -math.inf, 0.0, 0]

def process_sub_chunk(sub_chunk: bytes) -> dict:
    """Process a sub-chunk of data and return city statistics."""
    city_data = defaultdict(initialize_city_data)
    for line in sub_chunk.split(b'\n'):
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

def process_file_chunk(filename: str, start_offset: int, end_offset: int) -> dict:
    """Process a file chunk and return city statistics."""
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

            # Extract the chunk
            chunk = memory_map[start_offset:end]

    # Split the chunk into sub-chunks for parallel processing
    sub_chunks = []
    previous = 0
    for _ in range(3):
        position = (len(chunk) * (_ + 1)) // 4
        while position < len(chunk) and chunk[position] != ord('\n'):
            position += 1
        sub_chunks.append(chunk[previous:position + 1])
        previous = position + 1
    sub_chunks.append(chunk[previous:])

    # Process sub-chunks in parallel using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(process_sub_chunk, sub_chunks))

    # Merge results from sub-chunks
    merged_data = defaultdict(initialize_city_data)
    for result in results:
        for city, stats in result.items():
            entry = merged_data[city]
            entry[0] = min(entry[0], stats[0])
            entry[1] = max(entry[1], stats[1])
            entry[2] += stats[2]
            entry[3] += stats[3]
    return merged_data

def merge_city_data(data_list: list) -> dict:
    """Merge city statistics from multiple chunks."""
    final_data = defaultdict(initialize_city_data)
    for data in data_list:
        for city, stats in data.items():
            entry = final_data[city]
            entry[0] = min(entry[0], stats[0])
            entry[1] = max(entry[1], stats[1])
            entry[2] += stats[2]
            entry[3] += stats[3]
    return final_data

def main(input_filename: str = "testcase.txt", output_filename: str = "output.txt") -> None:
    """Main function to process the file and generate output."""
    with open(input_filename, "rb") as file:
        with mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ) as memory_map:
            file_size = len(memory_map)

    # Divide the file into chunks for parallel processing
    num_processes = multiprocessing.cpu_count() * 2
    chunk_size = file_size // num_processes
    chunks = [(input_filename, i * chunk_size, (i + 1) * chunk_size if i < num_processes - 1 else file_size)
              for i in range(num_processes)]

    # Process chunks in parallel using ProcessPoolExecutor
    with ProcessPoolExecutor(max_workers=num_processes) as executor:
        results = list(executor.map(lambda args: process_file_chunk(*args), chunks))

    # Merge results from all chunks
    final_data = merge_city_data(results)

    # Generate output
    output_lines = []
    for city in sorted(final_data.keys(), key=lambda c: c.decode()):
        min_score, max_score, total_score, count = final_data[city]
        average_score = round_up(total_score / count)
        output_lines.append(f"{city.decode()}={round_up(min_score):.1f}/{average_score:.1f}/{round_up(max_score):.1f}\n")

    # Write output to file
    with open(output_filename, "w") as file:
        file.writelines(output_lines)

if __name__ == "__main__":
    main()
