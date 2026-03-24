"""
================================================================================
WORKBOOK 08: PROCESSPOOLEXECUTOR FOR CPU-BOUND TASKS
================================================================================
Difficulty: Advanced
Topics: ProcessPoolExecutor, CPU-bound work, multiprocessing with asyncio, GIL

Prerequisites: Workbooks 01-07

Learning Objectives:
- Understand when to use ProcessPoolExecutor vs ThreadPoolExecutor
- Bypass the GIL for true parallelism
- Handle inter-process communication with asyncio
- Optimize CPU-bound workloads in async applications
================================================================================
"""

import asyncio
import time
import math
import os
from concurrent.futures import ProcessPoolExecutor
from typing import Any
import multiprocessing


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║                  PRIMER: PROCESSPOOLEXECUTOR WITH ASYNCIO                   ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
THE PROBLEM: PYTHON'S GIL (Global Interpreter Lock)
====================================================
Python has a lock (GIL) that allows only ONE thread to execute Python code
at a time. This means:

- ThreadPoolExecutor: Good for I/O-bound work (threads release GIL while waiting)
- ThreadPoolExecutor: BAD for CPU-bound work (threads fight for GIL)

For CPU-intensive work, you need SEPARATE PROCESSES, not threads!


THREADPOOLEXECUTOR vs PROCESSPOOLEXECUTOR:
==========================================
| Aspect              | ThreadPoolExecutor      | ProcessPoolExecutor          |
|---------------------|-------------------------|------------------------------|
| Memory              | Shared memory           | Separate memory per process  |
| GIL                 | Limited by GIL          | Bypasses GIL completely      |
| Best for            | I/O-bound (files, net)  | CPU-bound (math, processing) |
| Overhead            | Low (fast to create)    | High (process creation)      |
| Data passing        | Direct (shared refs)    | Serialized (pickle)          |
| Communication       | Easy                    | More complex                 |


WHEN TO USE WHICH:
==================
    ┌────────────────────────────────────────────────────────────────┐
    │                     What kind of work?                          │
    └───────────────────────────┬────────────────────────────────────┘
                                │
            ┌───────────────────┴───────────────────┐
            │                                       │
            ▼                                       ▼
    ┌───────────────┐                      ┌───────────────┐
    │   I/O-bound   │                      │   CPU-bound   │
    │ (waiting for  │                      │ (calculations,│
    │  network/disk)│                      │  processing)  │
    └───────┬───────┘                      └───────┬───────┘
            │                                       │
            ▼                                       ▼
    ┌───────────────┐                      ┌───────────────┐
    │ Use asyncio   │                      │    Use        │
    │ OR            │                      │ ProcessPool-  │
    │ ThreadPool-   │                      │ Executor      │
    │ Executor      │                      │               │
    └───────────────┘                      └───────────────┘


BASIC USAGE:
============
ProcessPoolExecutor works just like ThreadPoolExecutor with run_in_executor.
"""

# IMPORTANT: Functions for ProcessPoolExecutor MUST be at module level (picklable)
def _demo_cpu_heavy(n: int) -> dict:
    """CPU-intensive: sum of squares (must be TOP-LEVEL function!)"""
    result = sum(i * i for i in range(n))
    return {"n": n, "result": result, "pid": os.getpid()}


async def _demo_basic_processpool():
    """
    Run this to see ProcessPoolExecutor:
    >>> asyncio.run(_demo_basic_processpool())
    """
    print("\n--- DEMO: Basic ProcessPoolExecutor ---")

    loop = asyncio.get_event_loop()

    print(f"Main process PID: {os.getpid()}")

    with ProcessPoolExecutor(max_workers=2) as executor:
        # Run in a separate process
        result = await loop.run_in_executor(
            executor,
            _demo_cpu_heavy,
            100000
        )
        print(f"Result from PID {result['pid']}: {result['result']}")
        print(f"Different PID? {result['pid'] != os.getpid()}")


"""
PARALLEL CPU WORK:
==================
The real power: running CPU-intensive tasks in PARALLEL on multiple cores!
"""

async def _demo_parallel_cpu():
    """
    Run this to see parallel CPU work:
    >>> asyncio.run(_demo_parallel_cpu())
    """
    print("\n--- DEMO: Parallel CPU Work ---")

    loop = asyncio.get_event_loop()

    print("Running 4 CPU-heavy tasks on multiple cores...")
    start = time.perf_counter()

    with ProcessPoolExecutor(max_workers=4) as executor:
        results = await asyncio.gather(
            loop.run_in_executor(executor, _demo_cpu_heavy, 500000),
            loop.run_in_executor(executor, _demo_cpu_heavy, 500000),
            loop.run_in_executor(executor, _demo_cpu_heavy, 500000),
            loop.run_in_executor(executor, _demo_cpu_heavy, 500000),
        )

    elapsed = time.perf_counter() - start

    # Show which PIDs were used
    pids = [r["pid"] for r in results]
    print(f"PIDs used: {set(pids)}")
    print(f"Total time: {elapsed:.2f}s (parallel execution!)")


"""
IMPORTANT LIMITATIONS:
======================
1. Functions MUST be at module level (top of file, not nested)
2. Arguments and results MUST be picklable (serializable)
3. Higher overhead than threads (process creation is slow)
4. No shared memory by default


WHAT CAN'T BE PICKLED?
======================
- Lambda functions
- Nested functions (defined inside other functions)
- Open file handles
- Database connections
- Most objects with C extensions

    # BAD - lambda can't be pickled
    executor.submit(lambda x: x*2, 5)  # Will fail!

    # BAD - nested function can't be pickled
    def outer():
        def inner(x):
            return x * 2
        executor.submit(inner, 5)  # Will fail!

    # GOOD - module-level function
    def multiply(x):
        return x * 2
    executor.submit(multiply, 5)  # Works!


PROCESS INITIALIZATION:
=======================
Use initializer to set up each worker process (load models, connect to DB, etc.)
"""

# Module-level variable for worker processes
_worker_config = {}


def _demo_init_worker(config: dict):
    """Called once when each worker process starts"""
    global _worker_config
    _worker_config = config
    print(f"  Worker {os.getpid()} initialized with config: {config}")


def _demo_worker_task(task_id: int) -> dict:
    """Uses the initialized config"""
    return {"task_id": task_id, "pid": os.getpid(), "config": _worker_config}


async def _demo_worker_initialization():
    """
    Run this to see worker initialization:
    >>> asyncio.run(_demo_worker_initialization())
    """
    print("\n--- DEMO: Worker Initialization ---")

    loop = asyncio.get_event_loop()
    config = {"model": "v1", "threshold": 0.5}

    with ProcessPoolExecutor(
        max_workers=2,
        initializer=_demo_init_worker,  # Called once per worker
        initargs=(config,)               # Arguments for initializer
    ) as executor:
        results = await asyncio.gather(*[
            loop.run_in_executor(executor, _demo_worker_task, i)
            for i in range(4)
        ])

    for r in results:
        print(f"  Task {r['task_id']} on PID {r['pid']}: {r['config']}")


"""
COMPARISON: Sequential vs ThreadPool vs ProcessPool
====================================================
For CPU-bound work:
- Sequential: Slowest (one at a time)
- ThreadPool: Still slow (GIL limits to one thread)
- ProcessPool: Fast! (true parallelism)
"""

def _demo_cpu_task(iterations: int) -> int:
    """Simple CPU-bound task"""
    total = 0
    for i in range(iterations):
        total += i * i
    return total


async def _demo_compare_approaches():
    """
    Run this to compare approaches:
    >>> asyncio.run(_demo_compare_approaches())
    """
    print("\n--- DEMO: Comparing Approaches ---")

    from concurrent.futures import ThreadPoolExecutor

    loop = asyncio.get_event_loop()
    iterations = 2_000_000
    tasks = 4

    # Sequential
    print(f"\n1. Sequential ({tasks} tasks):")
    start = time.perf_counter()
    for _ in range(tasks):
        _demo_cpu_task(iterations)
    print(f"   Time: {time.perf_counter() - start:.2f}s")

    # ThreadPoolExecutor (limited by GIL)
    print(f"\n2. ThreadPoolExecutor ({tasks} tasks):")
    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=tasks) as executor:
        await asyncio.gather(*[
            loop.run_in_executor(executor, _demo_cpu_task, iterations)
            for _ in range(tasks)
        ])
    print(f"   Time: {time.perf_counter() - start:.2f}s")

    # ProcessPoolExecutor (bypasses GIL)
    print(f"\n3. ProcessPoolExecutor ({tasks} tasks):")
    start = time.perf_counter()
    with ProcessPoolExecutor(max_workers=tasks) as executor:
        await asyncio.gather(*[
            loop.run_in_executor(executor, _demo_cpu_task, iterations)
            for _ in range(tasks)
        ])
    print(f"   Time: {time.perf_counter() - start:.2f}s (should be fastest!)")


"""
QUICK REFERENCE:
================
| Pattern                    | Code                                          |
|----------------------------|-----------------------------------------------|
| Basic usage                | with ProcessPoolExecutor() as ex:             |
|                            |     await loop.run_in_executor(ex, fn, arg)   |
| With max workers           | ProcessPoolExecutor(max_workers=4)            |
| With initializer           | ProcessPoolExecutor(initializer=fn, initargs=())|
| Multiple concurrent        | await asyncio.gather(*[run_in_executor(...)])  |

RULES TO REMEMBER:
==================
1. Functions MUST be at module level (not nested/lambda)
2. All arguments and return values MUST be picklable
3. Use for CPU-bound work; use ThreadPool for I/O-bound
4. Consider overhead - don't use for tiny tasks
5. Number of workers typically = number of CPU cores

Now try the exercises below!
================================================================================
"""


# ==============================================================================
# QUESTION 1: Basic ProcessPoolExecutor Usage
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Explain the difference between ThreadPoolExecutor and ProcessPoolExecutor.
When would you use processes over threads in Python?"

REQUIREMENTS:
1. Create function `cpu_intensive_task(n: int) -> dict` (must be top-level, not nested!)
   - Calculate sum of squares from 1 to n (CPU-bound work)
   - Return {"n": n, "result": sum, "pid": os.getpid()}

2. Create async function `run_cpu_task_in_process(n: int) -> dict`
   - Use ProcessPoolExecutor with run_in_executor
   - Return the result from cpu_intensive_task

3. Create async function `run_multiple_cpu_tasks(numbers: list[int]) -> list[dict]`
   - Run all tasks concurrently using ProcessPoolExecutor
   - Return all results

EXPECTED BEHAVIOR:
>>> result = asyncio.run(run_cpu_task_in_process(1000))
>>> result["result"]
333833500  # sum of squares 1^2 + 2^2 + ... + 1000^2
>>> result["pid"] != os.getpid()  # Ran in different process
True

KEY INSIGHT:
- ProcessPoolExecutor bypasses the GIL for true parallelism
- Each task runs in a separate process with its own PID
- Good for CPU-bound work; overkill for I/O-bound work
"""

def cpu_intensive_task(n: int) -> dict:
    # YOUR CODE HERE
    # Note: This MUST be a top-level function for pickling
    pass


async def run_cpu_task_in_process(n: int) -> dict:
    # YOUR CODE HERE
    pass


async def run_multiple_cpu_tasks(numbers: list[int]) -> list[dict]:
    # YOUR CODE HERE
    pass


# ==============================================================================
# QUESTION 2: Prime Number Calculation (CPU-Bound)
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Implement a prime number checker that can process multiple numbers
in parallel using multiple CPU cores."

REQUIREMENTS:
1. Create function `is_prime(n: int) -> bool` (top-level)
   - Return True if n is prime, False otherwise
   - Use simple trial division (no external libraries)

2. Create function `find_primes_in_range(start: int, end: int) -> list[int]` (top-level)
   - Return list of all primes in range [start, end)

3. Create async function `parallel_prime_search(
    ranges: list[tuple[int, int]],
    max_workers: int = None
) -> dict`
   - Search for primes in all ranges using ProcessPoolExecutor
   - Return {
       "total_primes": count of all primes found,
       "primes_per_range": list of counts per range,
       "worker_count": number of workers used
     }

EXPECTED BEHAVIOR:
>>> result = asyncio.run(parallel_prime_search([(2, 100), (100, 200), (200, 300)]))
>>> result["total_primes"]
62  # 25 + 21 + 16 primes in each range
"""

def is_prime(n: int) -> bool:
    # YOUR CODE HERE
    pass


def find_primes_in_range(start: int, end: int) -> list[int]:
    # YOUR CODE HERE
    pass


async def parallel_prime_search(
    ranges: list[tuple[int, int]],
    max_workers: int = None
) -> dict:
    # YOUR CODE HERE
    pass


# ==============================================================================
# QUESTION 3: Image Processing Simulation
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Simulate image processing where each image requires CPU-intensive
transformation. This is a common use case for ProcessPoolExecutor."

REQUIREMENTS:
1. Create function `process_image(image_id: int, size: int) -> dict` (top-level)
   - Simulate CPU work: sum(i*i for i in range(size))
   - Return {"image_id": image_id, "processed_size": size, "checksum": sum}

2. Create async function `process_image_batch(
    images: list[tuple[int, int]],  # (image_id, size) pairs
    max_workers: int = 4
) -> list[dict]`
   - Process all images in parallel
   - Return list of results in order

3. Create async function `compare_sequential_vs_parallel(
    images: list[tuple[int, int]]
) -> dict`
   - Run processing sequentially (no executor)
   - Run processing in parallel (with ProcessPoolExecutor)
   - Return {"sequential_time": float, "parallel_time": float, "speedup": float}

EXPECTED BEHAVIOR:
>>> images = [(i, 100000) for i in range(4)]
>>> result = asyncio.run(compare_sequential_vs_parallel(images))
>>> result["speedup"] > 1.5  # Should see speedup on multi-core
True
"""

def process_image(image_id: int, size: int) -> dict:
    # YOUR CODE HERE
    pass


async def process_image_batch(
    images: list[tuple[int, int]],
    max_workers: int = 4
) -> list[dict]:
    # YOUR CODE HERE
    pass


async def compare_sequential_vs_parallel(images: list[tuple[int, int]]) -> dict:
    # YOUR CODE HERE
    pass


# ==============================================================================
# QUESTION 4: Map-Reduce Pattern with Processes
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Implement a map-reduce pattern using ProcessPoolExecutor.
Map operations run in parallel; reduce aggregates results."

REQUIREMENTS:
1. Create function `mapper(chunk: list[int]) -> dict` (top-level)
   - Calculate: sum, count, min, max for the chunk
   - Return {"sum": s, "count": c, "min": m, "max": x}

2. Create function `reducer(results: list[dict]) -> dict`
   - Combine all mapper results into final statistics
   - Return {"total_sum": s, "total_count": c, "global_min": m, "global_max": x, "average": avg}

3. Create async function `parallel_map_reduce(
    data: list[int],
    chunk_size: int = 1000
) -> dict`
   - Split data into chunks
   - Run mapper on each chunk in parallel (ProcessPoolExecutor)
   - Reduce results
   - Return final statistics

EXPECTED BEHAVIOR:
>>> data = list(range(10000))
>>> result = asyncio.run(parallel_map_reduce(data))
>>> result["total_count"]
10000
>>> result["global_min"]
0
>>> result["global_max"]
9999
"""

def mapper(chunk: list[int]) -> dict:
    # YOUR CODE HERE
    pass


def reducer(results: list[dict]) -> dict:
    # YOUR CODE HERE
    pass


async def parallel_map_reduce(data: list[int], chunk_size: int = 1000) -> dict:
    # YOUR CODE HERE
    pass


# ==============================================================================
# QUESTION 5: Hybrid I/O and CPU Pipeline
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Design a pipeline that fetches data asynchronously (I/O-bound),
processes it with multiple cores (CPU-bound), and saves results."

REQUIREMENTS:
1. Create async function `fetch_raw_data(data_id: int) -> list[int]`
   - Simulate async fetch (asyncio.sleep 0.01s)
   - Return list(range(data_id * 100, (data_id + 1) * 100))

2. Create function `heavy_computation(data: list[int]) -> int` (top-level)
   - CPU-intensive: sum of squares
   - Return the sum

3. Create async function `save_result(result_id: int, value: int) -> str`
   - Simulate async save (asyncio.sleep 0.01s)
   - Return f"saved_{result_id}_{value}"

4. Create async function `hybrid_pipeline(data_ids: list[int]) -> dict`
   - Fetch all data concurrently (async)
   - Process all data concurrently (ProcessPoolExecutor)
   - Save all results concurrently (async)
   - Return {
       "fetched": count,
       "processed": list of computation results,
       "saved": list of save confirmations
     }

EXPECTED BEHAVIOR:
>>> result = asyncio.run(hybrid_pipeline([0, 1, 2]))
>>> result["fetched"]
3
>>> len(result["processed"])
3
>>> all("saved" in s for s in result["saved"])
True
"""

async def fetch_raw_data(data_id: int) -> list[int]:
    # YOUR CODE HERE
    pass


def heavy_computation(data: list[int]) -> int:
    # YOUR CODE HERE
    pass


async def save_result(result_id: int, value: int) -> str:
    # YOUR CODE HERE
    pass


async def hybrid_pipeline(data_ids: list[int]) -> dict:
    # YOUR CODE HERE
    pass


# ==============================================================================
# QUESTION 6: Process Pool with Initialization
# ==============================================================================
"""
INTERVIEW CONTEXT:
"How do you initialize worker processes with shared state or configuration?
This is important for loading models or establishing connections."

REQUIREMENTS:
1. Create function `init_worker(shared_config: dict)` (top-level)
   - Store config in a global variable for the worker process
   - Print f"Worker {os.getpid()} initialized with {shared_config}"

2. Create function `worker_task(task_id: int) -> dict` (top-level)
   - Access the global config
   - Return {"task_id": task_id, "pid": os.getpid(), "config": global_config}

3. Create async function `run_with_init(
    task_count: int,
    config: dict,
    max_workers: int = 2
) -> list[dict]`
   - Create ProcessPoolExecutor with initializer
   - Run task_count tasks
   - Return all results

Note: Use a module-level global variable for the worker config

EXPECTED BEHAVIOR:
>>> result = asyncio.run(run_with_init(4, {"model": "v1"}, 2))
>>> all(r["config"]["model"] == "v1" for r in result)
True
>>> len(set(r["pid"] for r in result)) <= 2  # Max 2 workers
True
"""

# Global config for worker processes
_worker_config = {}

def init_worker(shared_config: dict):
    # YOUR CODE HERE
    pass


def worker_task(task_id: int) -> dict:
    # YOUR CODE HERE
    pass


async def run_with_init(
    task_count: int,
    config: dict,
    max_workers: int = 2
) -> list[dict]:
    # YOUR CODE HERE
    pass


# ==============================================================================
# QUESTION 7: Chunked Parallel Processing with Progress
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Process a large dataset in chunks with progress reporting.
Show how to communicate progress from worker processes."

REQUIREMENTS:
1. Create function `process_chunk(chunk_id: int, data: list[int]) -> dict` (top-level)
   - Process: sum all values
   - Return {"chunk_id": chunk_id, "sum": total, "size": len(data)}

2. Create async function `process_with_progress(
    data: list[int],
    chunk_size: int,
    progress_callback: callable  # async callback(completed, total)
) -> dict`
   - Split data into chunks
   - Process chunks in parallel
   - Call progress_callback after each chunk completes
   - Return {"total_sum": sum, "chunks_processed": count}

EXPECTED BEHAVIOR:
>>> progress = []
>>> async def track(done, total):
...     progress.append((done, total))
>>> data = list(range(1000))
>>> result = asyncio.run(process_with_progress(data, 200, track))
>>> len(progress)  # 5 chunks processed
5
>>> progress[-1]  # Final progress
(5, 5)
"""

def process_chunk(chunk_id: int, data: list[int]) -> dict:
    # YOUR CODE HERE
    pass


async def process_with_progress(
    data: list[int],
    chunk_size: int,
    progress_callback
) -> dict:
    # YOUR CODE HERE
    pass


# ==============================================================================
# QUESTION 8: Process Pool Task Queue Pattern
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Implement a task queue that distributes CPU-intensive work across
multiple processes with backpressure control."

REQUIREMENTS:
Implement class `ProcessTaskQueue`:
    def __init__(self, max_workers: int, max_pending: int):
        # ProcessPoolExecutor
        # Semaphore for backpressure

    async def submit(self, func: callable, *args) -> asyncio.Future:
        # Submit task with backpressure
        # Wait if too many pending

    async def map(self, func: callable, items: list) -> list:
        # Process all items, return results in order

    async def shutdown(self):
        # Clean shutdown

EXPECTED BEHAVIOR:
>>> async def test():
...     queue = ProcessTaskQueue(max_workers=2, max_pending=4)
...     results = await queue.map(cpu_intensive_task, [100, 200, 300, 400])
...     await queue.shutdown()
...     return len(results)
>>> asyncio.run(test())
4

KEY INSIGHT:
- Backpressure prevents memory exhaustion with large workloads
- Semaphore limits concurrent submissions
"""

class ProcessTaskQueue:
    def __init__(self, max_workers: int, max_pending: int):
        # YOUR CODE HERE
        pass

    async def submit(self, func, *args) -> asyncio.Future:
        # YOUR CODE HERE
        pass

    async def map(self, func, items: list) -> list:
        # YOUR CODE HERE
        pass

    async def shutdown(self):
        # YOUR CODE HERE
        pass


# ==============================================================================
# MAIN - Test Your Solutions
# ==============================================================================
async def main():
    print("=" * 60)
    print("WORKBOOK 08: Testing Your Solutions")
    print("=" * 60)

    # Test Q1
    print("\n[Q1] Testing ProcessPoolExecutor basics...")
    try:
        result = await run_cpu_task_in_process(1000)
        expected = sum(i*i for i in range(1, 1001))
        assert result["result"] == expected, f"Expected {expected}, got {result['result']}"
        assert result["pid"] != os.getpid(), "Should run in different process"

        results = await run_multiple_cpu_tasks([100, 200, 300])
        assert len(results) == 3, "Should have 3 results"
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Test Q2
    print("\n[Q2] Testing parallel prime search...")
    try:
        result = await parallel_prime_search([(2, 100), (100, 200)])
        assert result["total_primes"] == 46, f"Expected 46 primes, got {result['total_primes']}"
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Test Q3
    print("\n[Q3] Testing image processing simulation...")
    try:
        images = [(i, 10000) for i in range(4)]
        results = await process_image_batch(images)
        assert len(results) == 4, "Should have 4 results"
        assert all("checksum" in r for r in results), "Should have checksums"

        comparison = await compare_sequential_vs_parallel(images)
        print(f"    Sequential: {comparison['sequential_time']:.3f}s")
        print(f"    Parallel: {comparison['parallel_time']:.3f}s")
        print(f"    Speedup: {comparison['speedup']:.2f}x")
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Test Q4
    print("\n[Q4] Testing map-reduce pattern...")
    try:
        data = list(range(5000))
        result = await parallel_map_reduce(data, chunk_size=1000)
        assert result["total_count"] == 5000, f"Expected 5000, got {result['total_count']}"
        assert result["global_min"] == 0, "Min should be 0"
        assert result["global_max"] == 4999, "Max should be 4999"
        expected_avg = sum(data) / len(data)
        assert abs(result["average"] - expected_avg) < 0.01, "Average incorrect"
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Test Q5
    print("\n[Q5] Testing hybrid pipeline...")
    try:
        result = await hybrid_pipeline([0, 1, 2])
        assert result["fetched"] == 3, "Should fetch 3"
        assert len(result["processed"]) == 3, "Should process 3"
        assert all("saved" in s for s in result["saved"]), "Should save all"
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Test Q6
    print("\n[Q6] Testing process pool with initialization...")
    try:
        result = await run_with_init(4, {"model": "test_v1"}, 2)
        assert len(result) == 4, "Should have 4 results"
        assert all(r.get("config", {}).get("model") == "test_v1" for r in result), \
            "All workers should have config"
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Test Q7
    print("\n[Q7] Testing chunked processing with progress...")
    try:
        progress_log = []
        async def track(done, total):
            progress_log.append((done, total))

        data = list(range(500))
        result = await process_with_progress(data, 100, track)
        assert result["chunks_processed"] == 5, "Should have 5 chunks"
        assert len(progress_log) == 5, "Should have 5 progress updates"
        assert progress_log[-1] == (5, 5), f"Final progress should be (5,5), got {progress_log[-1]}"
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Test Q8
    print("\n[Q8] Testing ProcessTaskQueue...")
    try:
        queue = ProcessTaskQueue(max_workers=2, max_pending=4)
        results = await queue.map(cpu_intensive_task, [100, 200, 300, 400])
        await queue.shutdown()
        assert len(results) == 4, "Should have 4 results"
        assert all("result" in r for r in results), "All should have results"
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    print("\n" + "=" * 60)
    print("Workbook 08 Complete!")
    print("=" * 60)


if __name__ == "__main__":
    # Required for ProcessPoolExecutor on some platforms
    multiprocessing.freeze_support()
    asyncio.run(main())
