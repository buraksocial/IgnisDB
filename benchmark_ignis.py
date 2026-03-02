import argparse
import asyncio
import time
import json
import statistics

async def benchmark(skip_import=False):
    host = '127.0.0.1'
    port = 6381
    # Updated path to match the renamed file
    snapshot_file = r"c:\Users\bur4k\Downloads\IgnisDB-main\ignis_snapshot.ignis"
    
    print(f"Connecting to IgnisDB at {host}:{port}...")
    try:
        reader, writer = await asyncio.open_connection(host, port)
    except ConnectionRefusedError:
        print("Error: IgnisDB server is not running!")
        return

    # 1. Import Data (Optional)
    if not skip_import:
        print(f"Importing snapshot: {snapshot_file}...")
        start_import = time.time()
        writer.write(f"IMPORT {snapshot_file}\r\n".encode())
        await writer.drain()
        
        # Wait for response (might take a bit for 400MB)
        # Import command returns a simple string, not RESP formatted usually in this custom server, 
        # but the server code returns "Imported X keys..."
        # Let's read a big chunk
        resp = await reader.read(4096)
        print(f"Import Response: {resp.decode(errors='replace').strip()}")
        print(f"Import Time: {time.time() - start_import:.2f}s")
    else:
        print("Skipping import step (presuming data is loaded via AOF/Snapshot)...")

    # 2. Benchmark Query
    key = "data_users:1"
    iterations = 10000
    latencies = []

    print(f"\nBenchmarking GET {key} ({iterations} iterations)...")
    
    # Warmup
    for _ in range(100):
        writer.write(f"GET {key}\r\n".encode())
        await writer.drain()
        await reader.read(65536)

    # Measurement
    start_total = time.time()
    for _ in range(iterations):
        t0 = time.perf_counter()
        writer.write(f"GET {key}\r\n".encode())
        await writer.drain()
        data = await reader.read(65536)
        t1 = time.perf_counter()
        latencies.append((t1 - t0) * 1000) # ms

        if _ == 0:
            print(f"First Response Sample: {data.decode(errors='replace')[:200]}...")

    total_time = time.time() - start_total
    avg_latency = statistics.mean(latencies)
    p95_latency = statistics.quantiles(latencies, n=20)[18] # 95th percentile
    p99_latency = statistics.quantiles(latencies, n=100)[98] # 99th percentile
    rps = iterations / total_time

    print(f"\nResults:")
    print(f"  Total Time: {total_time:.4f}s")
    print(f"  Throughput: {rps:,.0f} req/s")
    print(f"  Avg Latency: {avg_latency:.4f} ms")
    print(f"  p95 Latency: {p95_latency:.4f} ms")
    print(f"  p99 Latency: {p99_latency:.4f} ms")

    print(f"  p99 Latency: {p99_latency:.4f} ms")

    # 3. Benchmark Non-Existent Key
    key_ne = "data_users:9999999"
    print(f"\nBenchmarking GET {key_ne} (Not Found) ({iterations} iterations)...")
    latencies_ne = []
    
    start_total_ne = time.time()
    for _ in range(iterations):
        t0 = time.perf_counter()
        writer.write(f"GET {key_ne}\r\n".encode())
        await writer.drain()
        data = await reader.read(65536)
        t1 = time.perf_counter()
        latencies_ne.append((t1 - t0) * 1000)

        if _ == 0:
            print(f"First Response Sample (Should be nil): {data.decode(errors='replace').strip()}")

    total_time_ne = time.time() - start_total_ne
    avg_latency_ne = statistics.mean(latencies_ne)
    rps_ne = iterations / total_time_ne
    
    print(f"\nResults (Not Found):")
    print(f"  Total Time: {total_time_ne:.4f}s")
    print(f"  Throughput: {rps_ne:,.0f} req/s")
    print(f"  Avg Latency: {avg_latency_ne:.4f} ms")

    writer.close()
    await writer.wait_closed()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--skip-import', action='store_true', help='Skip the IMPORT command step')
    args = parser.parse_args()
    
    try:
        asyncio.run(benchmark(skip_import=args.skip_import))
    except KeyboardInterrupt:
        pass
