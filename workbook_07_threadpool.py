"""
================================================================================
WORKBOOK 07: THREADPOOLEXECUTOR WITH ASYNCIO
================================================================================
Difficulty: Intermediate-Advanced
Topics: run_in_executor, ThreadPoolExecutor, blocking I/O, thread safety

Prerequisites: Workbooks 01-06

Learning Objectives:
- Integrate blocking/synchronous code with asyncio
- Master loop.run_in_executor() for offloading blocking work
- Understand thread safety when mixing threads and asyncio
- Learn when to use threads vs pure async
================================================================================
"""

import asyncio
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any
import os


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║                   PRIMER: THREADPOOLEXECUTOR WITH ASYNCIO                   ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
THE PROBLEM:
============
Not all code is async! Many libraries are synchronous and BLOCK:
- File I/O (reading/writing files)
- Database drivers (some are sync-only)
- Legacy code/libraries
- CPU-bound operations (sort of - see Workbook 08)

If you call blocking code directly in async, you FREEZE the entire event loop!

    async def bad_example():
        time.sleep(5)  # BAD! Blocks the entire event loop for 5 seconds!
        # No other coroutines can run during this time


THE SOLUTION: run_in_executor()
================================
Offload blocking code to a thread pool. The event loop stays responsive
while threads handle the blocking work.

    async def good_example():
        loop = asyncio.get_event_loop()
        # Runs in a thread, doesn't block event loop
        await loop.run_in_executor(None, time.sleep, 5)


HOW IT WORKS:
=============
1. Your async code calls run_in_executor()
2. The blocking function runs in a SEPARATE THREAD
3. Event loop continues running other coroutines
4. When the thread finishes, you get the result back
5. Your async code continues

    ┌─────────────────────────────────────────────────┐
    │                  Event Loop                      │
    │  ┌─────────┐  ┌─────────┐  ┌─────────┐         │
    │  │ Coro A  │  │ Coro B  │  │ Coro C  │         │
    │  └────┬────┘  └─────────┘  └─────────┘         │
    │       │                                          │
    │       ▼ run_in_executor()                       │
    └───────┼─────────────────────────────────────────┘
            │
    ┌───────▼─────────────────────────────────────────┐
    │              ThreadPoolExecutor                   │
    │  ┌──────────┐ ┌──────────┐ ┌──────────┐        │
    │  │ Thread 1 │ │ Thread 2 │ │ Thread 3 │        │
    │  │(blocking)│ │  (idle)  │ │  (idle)  │        │
    │  └──────────┘ └──────────┘ └──────────┘        │
    └─────────────────────────────────────────────────┘
"""


def _demo_blocking_function(seconds: float) -> str:
    """A blocking function using time.sleep (NOT asyncio.sleep)"""
    print(f"  [Thread {threading.current_thread().name}] Starting blocking work...")
    time.sleep(seconds)  # This BLOCKS!
    print(f"  [Thread {threading.current_thread().name}] Blocking work done!")
    return f"Completed after {seconds}s"


async def _demo_run_in_executor():
    """
    Run this to see run_in_executor:
    >>> asyncio.run(_demo_run_in_executor())
    """
    print("\n--- DEMO: run_in_executor Basics ---")

    loop = asyncio.get_event_loop()

    print("\n1. Using default executor (None):")
    result = await loop.run_in_executor(
        None,                       # None = use default ThreadPoolExecutor
        _demo_blocking_function,    # The blocking function
        0.2                         # Arguments to the function
    )
    print(f"   Result: {result}")

    print("\n2. Using custom ThreadPoolExecutor:")
    with ThreadPoolExecutor(max_workers=3) as executor:
        result = await loop.run_in_executor(
            executor,                   # Custom executor
            _demo_blocking_function,
            0.1
        )
    print(f"   Result: {result}")


"""
CONCURRENT BLOCKING CALLS:
==========================
You can run multiple blocking operations concurrently using gather!
"""

async def _demo_concurrent_blocking():
    """
    Run this to see concurrent blocking:
    >>> asyncio.run(_demo_concurrent_blocking())
    """
    print("\n--- DEMO: Concurrent Blocking Calls ---")

    loop = asyncio.get_event_loop()

    print("Running 3 blocking operations (0.2s each)...")
    start = time.perf_counter()

    # All three run concurrently in different threads!
    results = await asyncio.gather(
        loop.run_in_executor(None, _demo_blocking_function, 0.2),
        loop.run_in_executor(None, _demo_blocking_function, 0.2),
        loop.run_in_executor(None, _demo_blocking_function, 0.2),
    )

    elapsed = time.perf_counter() - start
    print(f"\nTotal time: {elapsed:.2f}s (NOT 0.6s because they ran in parallel!)")


"""
MIXING ASYNC AND BLOCKING:
==========================
A common pattern: fetch data async, process it with blocking code.
"""

async def _demo_fetch_data(data_id: int) -> dict:
    """Simulates async API call"""
    await asyncio.sleep(0.1)
    return {"id": data_id, "values": list(range(1000))}


def _demo_process_data_sync(data: dict) -> int:
    """Simulates blocking CPU work"""
    time.sleep(0.1)  # Blocking!
    return sum(data["values"])


async def _demo_hybrid_pipeline():
    """
    Run this to see hybrid async/blocking pipeline:
    >>> asyncio.run(_demo_hybrid_pipeline())
    """
    print("\n--- DEMO: Hybrid Async/Blocking Pipeline ---")

    loop = asyncio.get_event_loop()

    # Step 1: Fetch data asynchronously
    print("Fetching data (async)...")
    data = await _demo_fetch_data(1)

    # Step 2: Process with blocking code in executor
    print("Processing data (blocking, in executor)...")
    result = await loop.run_in_executor(None, _demo_process_data_sync, data)

    print(f"Result: {result}")


"""
THREAD SAFETY WARNING:
======================
When mixing threads and asyncio, be careful about shared state!

UNSAFE:
    counter = 0
    def increment():
        global counter
        counter += 1  # Race condition!

SAFE:
    import threading
    lock = threading.Lock()
    counter = 0
    def increment():
        global counter
        with lock:
            counter += 1

ALSO SAFE (for async code):
    asyncio.Lock() - but only for coroutines, not threads!
"""


def _demo_thread_safe_increment(counter_dict: dict, lock: threading.Lock):
    """Thread-safe increment using threading.Lock"""
    with lock:
        counter_dict["value"] += 1


async def _demo_thread_safety():
    """
    Run this to see thread safety:
    >>> asyncio.run(_demo_thread_safety())
    """
    print("\n--- DEMO: Thread Safety ---")

    loop = asyncio.get_event_loop()
    counter = {"value": 0}
    lock = threading.Lock()  # Use threading.Lock for threads!

    # Run 100 concurrent increments
    await asyncio.gather(*[
        loop.run_in_executor(None, _demo_thread_safe_increment, counter, lock)
        for _ in range(100)
    ])

    print(f"Final counter: {counter['value']} (should be 100)")


"""
WHEN TO USE WHAT:
=================
| Scenario                          | Solution                              |
|-----------------------------------|---------------------------------------|
| Async library available           | Use async library directly            |
| Blocking I/O (file, legacy DB)    | ThreadPoolExecutor + run_in_executor  |
| CPU-bound work                    | ProcessPoolExecutor (see Workbook 08) |
| Quick sync operation              | Just call it (if < 1ms, OK to block)  |


QUICK REFERENCE:
================
| Pattern                           | Code                                      |
|-----------------------------------|-------------------------------------------|
| Default executor                  | await loop.run_in_executor(None, fn, arg) |
| Custom executor                   | await loop.run_in_executor(executor, fn)  |
| Multiple blocking ops             | await asyncio.gather(*[run_in_executor...]) |
| Get current loop                  | loop = asyncio.get_event_loop()           |
| Thread-safe lock                  | threading.Lock() (NOT asyncio.Lock!)      |

ASYNCIO.TO_THREAD (Python 3.9+):
================================
Simpler syntax for running in default executor:

    # Old way:
    result = await loop.run_in_executor(None, blocking_func, arg)

    # New way (Python 3.9+):
    result = await asyncio.to_thread(blocking_func, arg)

Now try the exercises below!
================================================================================
"""


# ==============================================================================
# QUESTION 1: Basic run_in_executor Usage
# ==============================================================================
"""
INTERVIEW CONTEXT:
"You have a legacy synchronous function that blocks. How do you integrate
it with asyncio without blocking the event loop?"

REQUIREMENTS:
1. Create SYNCHRONOUS function `blocking_io_operation(duration: float) -> str`
   - Use time.sleep (NOT asyncio.sleep!) to simulate blocking I/O
   - Return f"Completed after {duration}s"

2. Create async function `run_blocking_async(duration: float) -> str`
   - Use loop.run_in_executor to run blocking_io_operation
   - Return the result

3. Create async function `run_multiple_blocking(durations: list[float]) -> list[str]`
   - Run all blocking operations concurrently using executor
   - Return all results

EXPECTED BEHAVIOR:
>>> asyncio.run(run_blocking_async(0.1))
'Completed after 0.1s'

>>> start = time.perf_counter()
>>> asyncio.run(run_multiple_blocking([0.1, 0.1, 0.1]))
>>> elapsed = time.perf_counter() - start
>>> elapsed < 0.2  # Should run concurrently
True

KEY INSIGHT:
- run_in_executor offloads blocking code to a thread pool
- Event loop stays responsive while threads do blocking work
- Default executor is a ThreadPoolExecutor
"""

def blocking_io_operation(duration: float) -> str:
    # YOUR CODE HERE - use time.sleep, NOT asyncio.sleep
    pass


async def run_blocking_async(duration: float) -> str:
    # YOUR CODE HERE
    pass


async def run_multiple_blocking(durations: list[float]) -> list[str]:
    # YOUR CODE HERE
    pass


# ==============================================================================
# QUESTION 2: Custom ThreadPoolExecutor
# ==============================================================================
"""
INTERVIEW CONTEXT:
"How do you control the number of threads used for blocking operations?
What are the tradeoffs of different pool sizes?"

REQUIREMENTS:
1. Create SYNCHRONOUS function `cpu_light_blocking(task_id: int, duration: float) -> dict`
   - time.sleep for duration
   - Return {"task_id": task_id, "thread": threading.current_thread().name}

2. Create async function `run_with_custom_pool(
    task_count: int,
    max_workers: int,
    duration: float
) -> dict`
   - Create ThreadPoolExecutor with max_workers
   - Run task_count tasks concurrently
   - Return {
       "results": list of result dicts,
       "unique_threads": set of thread names used
     }

EXPECTED BEHAVIOR:
>>> result = asyncio.run(run_with_custom_pool(10, 3, 0.01))
>>> len(result["unique_threads"]) <= 3  # Only 3 threads
True
>>> len(result["results"])
10
"""

def cpu_light_blocking(task_id: int, duration: float) -> dict:
    # YOUR CODE HERE
    pass


async def run_with_custom_pool(
    task_count: int,
    max_workers: int,
    duration: float
) -> dict:
    # YOUR CODE HERE
    pass


# ==============================================================================
# QUESTION 3: File I/O with Executor
# ==============================================================================
"""
INTERVIEW CONTEXT:
"File operations are blocking. How do you read multiple files concurrently
without blocking the asyncio event loop?"

REQUIREMENTS:
1. Create SYNCHRONOUS function `read_file_sync(filepath: str) -> str`
   - Read and return file contents
   - Handle FileNotFoundError by returning "FILE_NOT_FOUND"

2. Create async function `read_files_async(filepaths: list[str]) -> list[str]`
   - Read all files concurrently using executor
   - Return list of contents in same order as filepaths

3. Create async function `write_and_read_test() -> dict`
   - Create a temp directory with 3 test files
   - Write "content_1", "content_2", "content_3" to them
   - Read them all back concurrently
   - Clean up
   - Return {"files_read": count, "contents": list of contents}

EXPECTED BEHAVIOR:
>>> result = asyncio.run(write_and_read_test())
>>> result["files_read"]
3
>>> "content_1" in result["contents"]
True
"""

import tempfile
import shutil

def read_file_sync(filepath: str) -> str:
    # YOUR CODE HERE
    pass


async def read_files_async(filepaths: list[str]) -> list[str]:
    # YOUR CODE HERE
    pass


async def write_and_read_test() -> dict:
    # YOUR CODE HERE
    pass


# ==============================================================================
# QUESTION 4: Mixing Async and Blocking Code
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Design a system that makes async HTTP calls AND processes files (blocking)
in the same workflow without blocking."

REQUIREMENTS:
1. Create async function `fetch_data_async(url_id: int, delay: float) -> dict`
   - Simulate async API call with asyncio.sleep
   - Return {"source": "api", "id": url_id}

2. Create SYNCHRONOUS function `process_data_sync(data: dict, processing_time: float) -> dict`
   - Simulate blocking processing with time.sleep
   - Return {"processed": True, **data}

3. Create async function `fetch_and_process(url_id: int) -> dict`
   - Fetch data asynchronously
   - Process data using executor (blocking)
   - Return final processed result

4. Create async function `batch_fetch_and_process(ids: list[int]) -> list[dict]`
   - Process all IDs concurrently
   - Return all results

EXPECTED BEHAVIOR:
>>> result = asyncio.run(fetch_and_process(1))
>>> result["source"]
'api'
>>> result["processed"]
True

>>> start = time.perf_counter()
>>> results = asyncio.run(batch_fetch_and_process([1, 2, 3, 4, 5]))
>>> elapsed = time.perf_counter() - start
>>> elapsed < 0.3  # Should be concurrent
True
"""

async def fetch_data_async(url_id: int, delay: float = 0.05) -> dict:
    # YOUR CODE HERE
    pass


def process_data_sync(data: dict, processing_time: float = 0.05) -> dict:
    # YOUR CODE HERE
    pass


async def fetch_and_process(url_id: int) -> dict:
    # YOUR CODE HERE
    pass


async def batch_fetch_and_process(ids: list[int]) -> list[dict]:
    # YOUR CODE HERE
    pass


# ==============================================================================
# QUESTION 5: Thread-Safe Communication
# ==============================================================================
"""
INTERVIEW CONTEXT:
"How do you safely communicate between executor threads and the asyncio
event loop? This is crucial for avoiding race conditions."

REQUIREMENTS:
1. Create class `ThreadSafeAccumulator`:
    def __init__(self):
        # Thread-safe storage for results

    def add_sync(self, value: int) -> None:
        # Thread-safe add (called from executor threads)

    async def add_async(self, value: int) -> None:
        # Async add (called from coroutines)

    def get_total(self) -> int:
        # Return current total

2. Create async function `test_thread_safety() -> dict`
   - Create accumulator
   - Run 100 blocking operations that each add 1 (using executor)
   - Run 100 async operations that each add 1
   - Wait for all to complete
   - Return {"total": final_total, "expected": 200}

EXPECTED BEHAVIOR:
>>> result = asyncio.run(test_thread_safety())
>>> result["total"]
200
>>> result["total"] == result["expected"]
True

KEY INSIGHT:
- Use threading.Lock for thread-safe operations
- asyncio.Lock is for coroutines, not threads!
"""

class ThreadSafeAccumulator:
    def __init__(self):
        # YOUR CODE HERE
        pass

    def add_sync(self, value: int) -> None:
        # YOUR CODE HERE
        pass

    async def add_async(self, value: int) -> None:
        # YOUR CODE HERE
        pass

    def get_total(self) -> int:
        # YOUR CODE HERE
        pass


async def test_thread_safety() -> dict:
    # YOUR CODE HERE
    pass


# ==============================================================================
# QUESTION 6: Executor Timeout Handling
# ==============================================================================
"""
INTERVIEW CONTEXT:
"How do you handle blocking operations that might hang? Implement
timeout handling for executor tasks."

REQUIREMENTS:
1. Create SYNCHRONOUS function `potentially_hanging_operation(hang: bool) -> str`
   - If hang: time.sleep(10) and return "never reached"
   - Otherwise: time.sleep(0.01) and return "success"

2. Create async function `run_with_timeout(hang: bool, timeout: float) -> dict`
   - Run the operation using executor
   - Use asyncio.wait_for for timeout
   - Return {"status": "success", "result": <result>} on success
   - Return {"status": "timeout", "result": None} on timeout

EXPECTED BEHAVIOR:
>>> asyncio.run(run_with_timeout(False, 1.0))
{'status': 'success', 'result': 'success'}
>>> asyncio.run(run_with_timeout(True, 0.1))
{'status': 'timeout', 'result': None}

WARNING:
- The thread may continue running after timeout!
- Python cannot forcibly kill threads
- Consider this when designing systems
"""

def potentially_hanging_operation(hang: bool) -> str:
    # YOUR CODE HERE
    pass


async def run_with_timeout(hang: bool, timeout: float) -> dict:
    # YOUR CODE HERE
    pass


# ==============================================================================
# QUESTION 7: Connection Pool Pattern
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Implement a connection pool that manages blocking database connections
within an async application."

REQUIREMENTS:
Implement class `AsyncConnectionPool`:
    def __init__(self, max_connections: int):
        # Use semaphore to limit concurrent connections
        # Create ThreadPoolExecutor

    async def acquire(self) -> "Connection":
        # Acquire semaphore
        # Return a connection context

    async def execute(self, query: str, delay: float = 0.01) -> str:
        # Execute a "query" (simulate with blocking sleep in executor)
        # Respect connection limit
        # Return f"Result: {query}"

    async def close(self):
        # Clean up

EXPECTED BEHAVIOR:
>>> async def test():
...     pool = AsyncConnectionPool(2)
...     # Run 5 queries with only 2 connections
...     results = await asyncio.gather(*[
...         pool.execute(f"query_{i}") for i in range(5)
...     ])
...     await pool.close()
...     return len(results)
>>> asyncio.run(test())
5
"""

class AsyncConnectionPool:
    def __init__(self, max_connections: int):
        # YOUR CODE HERE
        pass

    async def execute(self, query: str, delay: float = 0.01) -> str:
        # YOUR CODE HERE
        pass

    async def close(self):
        # YOUR CODE HERE
        pass


# ==============================================================================
# QUESTION 8: Hybrid Async/Sync Pipeline
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Design a data pipeline that efficiently mixes async I/O (network) with
blocking I/O (file processing) and CPU-light transformations."

REQUIREMENTS:
1. Create async function `fetch_batch(batch_id: int, size: int) -> list[dict]`
   - Simulate fetching `size` items with async sleep (0.02s)
   - Return [{"batch": batch_id, "item": i} for i in range(size)]

2. Create SYNCHRONOUS function `transform_item(item: dict) -> dict`
   - Blocking transformation (time.sleep 0.01s)
   - Return {**item, "transformed": True}

3. Create SYNCHRONOUS function `save_item(item: dict) -> str`
   - Blocking save (time.sleep 0.01s)
   - Return f"saved_{item['batch']}_{item['item']}"

4. Create async function `process_pipeline(
    batch_count: int,
    items_per_batch: int
) -> dict`
   - Fetch all batches concurrently (async)
   - Transform all items concurrently (executor)
   - Save all items concurrently (executor)
   - Return {
       "batches_fetched": count,
       "items_transformed": count,
       "items_saved": count
     }

EXPECTED BEHAVIOR:
>>> result = asyncio.run(process_pipeline(3, 4))
>>> result["batches_fetched"]
3
>>> result["items_transformed"]
12  # 3 batches * 4 items
>>> result["items_saved"]
12
"""

async def fetch_batch(batch_id: int, size: int) -> list[dict]:
    # YOUR CODE HERE
    pass


def transform_item(item: dict) -> dict:
    # YOUR CODE HERE
    pass


def save_item(item: dict) -> str:
    # YOUR CODE HERE
    pass


async def process_pipeline(batch_count: int, items_per_batch: int) -> dict:
    # YOUR CODE HERE
    pass


# ==============================================================================
# MAIN - Test Your Solutions
# ==============================================================================
async def main():
    print("=" * 60)
    print("WORKBOOK 07: Testing Your Solutions")
    print("=" * 60)

    # Test Q1
    print("\n[Q1] Testing run_in_executor basics...")
    try:
        result = await run_blocking_async(0.05)
        assert "Completed" in result, f"Unexpected result: {result}"

        start = time.perf_counter()
        results = await run_multiple_blocking([0.05, 0.05, 0.05])
        elapsed = time.perf_counter() - start
        assert len(results) == 3, "Should have 3 results"
        assert elapsed < 0.12, f"Should run concurrently, took {elapsed:.3f}s"
        print(f"    PASSED! (3 operations in {elapsed:.3f}s)")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Test Q2
    print("\n[Q2] Testing custom ThreadPoolExecutor...")
    try:
        result = await run_with_custom_pool(10, 3, 0.01)
        assert len(result["results"]) == 10, "Should have 10 results"
        assert len(result["unique_threads"]) <= 3, f"Should use <=3 threads, used {len(result['unique_threads'])}"
        print(f"    PASSED! (used threads: {result['unique_threads']})")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Test Q3
    print("\n[Q3] Testing file I/O with executor...")
    try:
        result = await write_and_read_test()
        assert result["files_read"] == 3, f"Expected 3 files, got {result['files_read']}"
        assert all("content" in c for c in result["contents"]), "Should have content"
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Test Q4
    print("\n[Q4] Testing mixed async/blocking pipeline...")
    try:
        result = await fetch_and_process(1)
        assert result["source"] == "api", "Should have source"
        assert result["processed"] == True, "Should be processed"

        start = time.perf_counter()
        results = await batch_fetch_and_process([1, 2, 3, 4, 5])
        elapsed = time.perf_counter() - start
        assert len(results) == 5, "Should have 5 results"
        print(f"    PASSED! (5 items in {elapsed:.3f}s)")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Test Q5
    print("\n[Q5] Testing thread safety...")
    try:
        result = await test_thread_safety()
        assert result["total"] == result["expected"], \
            f"Expected {result['expected']}, got {result['total']}"
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Test Q6
    print("\n[Q6] Testing executor timeout...")
    try:
        success = await run_with_timeout(False, 1.0)
        assert success["status"] == "success", "Should succeed"
        timeout = await run_with_timeout(True, 0.1)
        assert timeout["status"] == "timeout", "Should timeout"
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Test Q7
    print("\n[Q7] Testing AsyncConnectionPool...")
    try:
        pool = AsyncConnectionPool(2)
        start = time.perf_counter()
        results = await asyncio.gather(*[
            pool.execute(f"query_{i}", 0.02) for i in range(6)
        ])
        elapsed = time.perf_counter() - start
        await pool.close()
        assert len(results) == 6, "Should have 6 results"
        # With 2 connections and 6 queries at 0.02s each: minimum ~0.06s
        assert elapsed >= 0.05, f"Should respect connection limit, took {elapsed:.3f}s"
        print(f"    PASSED! (6 queries in {elapsed:.3f}s with 2 connections)")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Test Q8
    print("\n[Q8] Testing hybrid pipeline...")
    try:
        result = await process_pipeline(3, 4)
        assert result["batches_fetched"] == 3, f"Expected 3 batches, got {result['batches_fetched']}"
        assert result["items_transformed"] == 12, f"Expected 12 items, got {result['items_transformed']}"
        assert result["items_saved"] == 12, f"Expected 12 saved, got {result['items_saved']}"
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    print("\n" + "=" * 60)
    print("Workbook 07 Complete!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
