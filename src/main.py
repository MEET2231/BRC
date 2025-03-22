import math
import mmap
import multiprocessing
from concurrent.futures import ThreadPoolExecutor
import threading

def round_inf(x):
    return math.ceil(x * 10) / 10

def process_lines(lines, data, lock, city_to_index):
    """Process a list of lines and update the shared data list."""
    local_data = []
    local_city_to_index = {}
    
    for line in lines:
        if not line:
            continue
        
        semicolon_pos = line.find(b';')
        if semicolon_pos == -1:
            continue
        
        city = line[:semicolon_pos]
        score_str = line[semicolon_pos+1:]
        
        try:
            score = float(score_str)
        except ValueError:
            continue
        
        # Get or create the index for the city
        if city not in local_city_to_index:
            local_city_to_index[city] = len(local_city_to_index)
            local_data.append([float('inf'), float('-inf'), 0.0, 0])
        index = local_city_to_index[city]
        
        # Update local_data
        entry = local_data[index]
        entry[0] = min(entry[0], score)
        entry[1] = max(entry[1], score)
        entry[2] += score
        entry[3] += 1
    
    # Merge local_data into the shared data list
    with lock:
        for city, local_index in local_city_to_index.items():
            if city not in city_to_index:
                city_to_index[city] = len(city_to_index)
                data.append([float('inf'), float('-inf'), 0.0, 0])
            global_index = city_to_index[city]
            
            entry = data[global_index]
            local_entry = local_data[local_index]
            entry[0] = min(entry[0], local_entry[0])
            entry[1] = max(entry[1], local_entry[1])
            entry[2] += local_entry[2]
            entry[3] += local_entry[3]

def process_chunk(filename, start_offset, end_offset):
    """Process a chunk of the file using threading."""
    data = []
    city_to_index = {}
    lock = threading.Lock()
    
    with open(filename, "rb") as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        size = len(mm)
        
        # Ensure the chunk starts at a line boundary
        if start_offset != 0:
            mm.seek(start_offset)
            if mm.read(1) != b'\n':
                while mm.tell() < size and mm.read(1) != b'\n':
                    pass
            start_offset = mm.tell()
        
        # Ensure the chunk ends at a line boundary
        mm.seek(end_offset)
        if mm.read(1) != b'\n':
            while mm.tell() < size and mm.read(1) != b'\n':
                pass
            end_offset = mm.tell()
        
        chunk = mm[start_offset:end_offset]
        mm.close()
    
    # Split the chunk into lines
    lines = chunk.split(b'\n')
    
    # Distribute lines among threads
    num_threads = 4  # Adjust based on your system
    lines_per_thread = (len(lines) + num_threads - 1) // num_threads
    thread_args = []
    
    for i in range(num_threads):
        start = i * lines_per_thread
        end = start + lines_per_thread
        thread_args.append((lines[start:end], data, lock, city_to_index))
    
    # Process lines using threading
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        executor.map(lambda args: process_lines(*args), thread_args)
    
    return data, city_to_index

def merge_data(data_list, city_to_index_list):
    """Merge results from all chunks."""
    final_data = []
    final_city_to_index = {}
    
    for data, city_to_index in zip(data_list, city_to_index_list):
        for city, index in city_to_index.items():
            if city not in final_city_to_index:
                final_city_to_index[city] = len(final_city_to_index)
                final_data.append([float('inf'), float('-inf'), 0.0, 0])
            final_index = final_city_to_index[city]
            
            entry = final_data[final_index]
            stats = data[index]
            entry[0] = min(entry[0], stats[0])
            entry[1] = max(entry[1], stats[1])
            entry[2] += stats[2]
            entry[3] += stats[3]
    
    return final_data, final_city_to_index

def main(input_file_name="testcase.txt", output_file_name="output.txt"):
    # Determine file size and split into chunks
    with open(input_file_name, "rb") as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        file_size = len(mm)
        mm.close()
    
    num_procs = multiprocessing.cpu_count() * 2
    chunk_size = file_size // num_procs
    chunks = [(i * chunk_size, (i + 1) * chunk_size if i < num_procs - 1 else file_size)
              for i in range(num_procs)]
    
    # Process chunks in parallel using multiprocessing
    with multiprocessing.Pool(num_procs) as pool:
        tasks = [(input_file_name, start, end) for start, end in chunks]
        results = pool.starmap(process_chunk, tasks)
    
    # Separate data and city_to_index from results
    data_list = [result[0] for result in results]
    city_to_index_list = [result[1] for result in results]
    
    # Merge results from all chunks
    final_data, final_city_to_index = merge_data(data_list, city_to_index_list)
    
    # Generate output
    out = []
    for city, index in sorted(final_city_to_index.items(), key=lambda x: x[0]):
        mn, mx, total, count = final_data[index]
        avg = round_inf(total / count)
        out.append(f"{city.decode()}={round_inf(mn):.1f}/{avg:.1f}/{round_inf(mx):.1f}\n")
    
    # Write output to file
    with open(output_file_name, "w") as f:
        f.writelines(out)

if _name_ == "_main_":
    main()
