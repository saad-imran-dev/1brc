import os
import multiprocessing as mp
import time

def get_file_chunks(
    filename: str,
    cpu_count: int = 8
) -> tuple:
    """Divide the file into chunks for processing"""
    cpu_count = min(os.cpu_count(), cpu_count)
    file_size = os.path.getsize(filename)
    chunk_size = file_size // cpu_count

    start_end = []

    with open(filename, "+rb") as f:
        def is_line_end(position):
            if position == 0:
                return True
            else:
                f.seek(position - 1)
                return f.read(1) == b"\n"
        
        def next_line(position):
            f.seek(position)
            f.readline()
            return f.tell()

        f.seek(0)
        chunk_start = 0

        while chunk_start < file_size:
            chunk_end = min(file_size, chunk_start + chunk_size)

            while not is_line_end(chunk_end):
                chunk_end -= 1

            if chunk_start == chunk_end:
                chunk_end = next_line(chunk_end) 
            
            start_end.append(
                (
                    filename,
                    chunk_start,
                    chunk_end
                )
            )

            chunk_start = chunk_end
    
    return cpu_count, start_end

def _process_file_chunk(
    filename: str,
    chunk_start: int,
    chunk_end: int,
    sep: str = ";"
) -> dict: 
    """Process chunk file in different processes"""
    result = {}

    with open(filename, "r", encoding="utf-8") as f:
        f.seek(chunk_start)

        while f.tell() < chunk_end:
            line = f.readline().strip().split(sep)
            location, measurement = line[0], float(line[1])

            if location not in result:
                result[location] = [
                    measurement,
                    measurement,
                    measurement,
                    1
                ]
            else:
                _result = result[location]
                if measurement < _result[0]:
                    _result[0] = measurement
                elif measurement > _result[1]:
                    _result[1] = measurement
                _result[2] += measurement
                _result[3] += 1
    
    return result

def printMeasurements(result: dict):
    """Print the measurements in sorted order"""
    result_keys = list(result.keys())
    result_keys.sort()

    print("{{", end="")
    for location in result_keys:
        measurements = result[location]
        print(
            f"{location}: {measurements[0]:.1f}/{measurements[2]/measurements[3] if measurements[3] != 0 else 0:.1f}/{measurements[1]:.1f}",
            end=", {" if location != result_keys[-1] else ""
        )
    print("}}", end="")

def process_file(
    file_chunks: list,
    cpu_count: int
):
    """Process data file"""
    start_time = time.time()

    with mp.Pool(cpu_count) as p:
        # process each chunk in a separate process 
        chunk_results = p.starmap(
            _process_file_chunk,
            file_chunks
        )
    
    # combine results from each process
    result = {}
    for chunk_result in chunk_results:
        for location, measurements in chunk_result.items():
            if location not in result:
                result[location] = measurements
            else:
                _result = result[location]
                if measurements[0] < _result[0]:
                    _result[0] = measurements[0]
                elif measurements[1] > _result[1]:
                    _result[1] = measurements[1]
                _result[2] += measurements[2]
                _result[3] += measurements[3]
    
    print(f"Time taken: {time.time() - start_time:.1f}s")
    printMeasurements(result)

if __name__ == "__main__":
    cpu_count , *file_chunks = get_file_chunks("measurements.txt")
    process_file(file_chunks[0], cpu_count)