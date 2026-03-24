"""
================================================================================
WORKBOOK 05: ASYNC ITERATORS, GENERATORS, AND CONTEXT MANAGERS
================================================================================
Difficulty: Intermediate
Topics: async for, async with, __aiter__, __anext__, async generators

Prerequisites: Workbooks 01-04

Learning Objectives:
- Implement async iterators using __aiter__ and __anext__
- Master async generators with yield
- Create async context managers
- Understand when to use async iteration patterns
================================================================================
"""

import asyncio
from typing import Any, AsyncIterator, AsyncGenerator
from contextlib import asynccontextmanager


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║           PRIMER: ASYNC ITERATORS, GENERATORS & CONTEXT MANAGERS            ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
WHY ASYNC ITERATION?
====================
Regular iterators block while fetching each item. Async iterators allow
other coroutines to run while waiting for the next item. Perfect for:
- Streaming API responses
- Database cursors
- Real-time data feeds
- Paginated results


PART 1: ASYNC GENERATORS (The Easy Way)
=======================================
Just like regular generators, but with `async def` and `await` inside.

Regular generator:          Async generator:
    def gen():                  async def agen():
        yield 1                     await asyncio.sleep(0.1)
        yield 2                     yield 1
                                    await asyncio.sleep(0.1)
                                    yield 2

Iteration:
    for x in gen():             async for x in agen():
        print(x)                    print(x)
"""

async def _demo_async_generator():
    """
    Run this to see async generators:
    >>> asyncio.run(_demo_async_generator())
    """
    print("\n--- DEMO: Async Generator ---")

    # Define an async generator
    async def countdown(n: int):
        while n > 0:
            print(f"  (sleeping before yielding {n}...)")
            await asyncio.sleep(0.1)  # Can await inside!
            yield n
            n -= 1

    # Use with async for
    print("Counting down:")
    async for num in countdown(3):
        print(f"  Got: {num}")


"""
PART 2: ASYNC ITERATOR CLASS (The Manual Way)
==============================================
Implement __aiter__ and __anext__ methods for full control.

Protocol:
- __aiter__(self) -> returns the iterator (usually self)
- __anext__(self) -> async method that returns next value or raises StopAsyncIteration
"""

class DemoAsyncRange:
    """Async version of range() - demonstrates the protocol"""

    def __init__(self, start: int, stop: int):
        self.current = start
        self.stop = stop

    def __aiter__(self):
        return self  # Return the iterator

    async def __anext__(self) -> int:
        if self.current >= self.stop:
            raise StopAsyncIteration  # Signal end of iteration
        await asyncio.sleep(0.05)  # Simulate async work
        value = self.current
        self.current += 1
        return value


async def _demo_async_iterator_class():
    """
    Run this to see async iterator class:
    >>> asyncio.run(_demo_async_iterator_class())
    """
    print("\n--- DEMO: Async Iterator Class ---")

    print("Using DemoAsyncRange(0, 3):")
    async for num in DemoAsyncRange(0, 3):
        print(f"  Got: {num}")


"""
PART 3: ASYNC CONTEXT MANAGERS
==============================
For resources that need async setup/teardown (connections, files, etc.)

Protocol:
- __aenter__(self) -> async method, returns the resource
- __aexit__(self, exc_type, exc, tb) -> async method, cleanup

Use with: async with resource as r:
"""

class DemoAsyncConnection:
    """Simulates an async database connection"""

    def __init__(self, name: str):
        self.name = name
        self.connected = False

    async def __aenter__(self):
        print(f"  Connecting to {self.name}...")
        await asyncio.sleep(0.1)  # Async connection
        self.connected = True
        print(f"  Connected!")
        return self  # Return the resource

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        print(f"  Disconnecting from {self.name}...")
        await asyncio.sleep(0.05)  # Async cleanup
        self.connected = False
        print(f"  Disconnected!")
        return False  # Don't suppress exceptions


async def _demo_async_context_manager_class():
    """
    Run this to see async context manager:
    >>> asyncio.run(_demo_async_context_manager_class())
    """
    print("\n--- DEMO: Async Context Manager Class ---")

    async with DemoAsyncConnection("database") as conn:
        print(f"  Inside context, connected={conn.connected}")
        await asyncio.sleep(0.1)  # Do some work

    print("After context (auto-disconnected)")


"""
PART 4: @asynccontextmanager DECORATOR (The Easy Way)
=====================================================
Use contextlib.asynccontextmanager for simpler context managers.
"""

@asynccontextmanager
async def demo_timed_block(name: str):
    """Context manager that times an async block"""
    import time
    start = time.perf_counter()
    print(f"  Starting {name}...")
    try:
        yield start  # Value available as 'as' target
    finally:
        elapsed = time.perf_counter() - start
        print(f"  {name} took {elapsed:.3f}s")


async def _demo_asynccontextmanager():
    """
    Run this to see @asynccontextmanager:
    >>> asyncio.run(_demo_asynccontextmanager())
    """
    print("\n--- DEMO: @asynccontextmanager ---")

    async with demo_timed_block("my_operation") as start_time:
        await asyncio.sleep(0.1)
        print("  Doing work...")
        await asyncio.sleep(0.1)


"""
PART 5: PRACTICAL EXAMPLE - Paginated API Fetch
================================================
Async generators shine for paginated/streaming data.
"""

async def _demo_paginated_fetch():
    """
    Run this to see pagination example:
    >>> asyncio.run(_demo_paginated_fetch())
    """
    print("\n--- DEMO: Paginated API Fetch ---")

    async def fetch_pages(total_pages: int):
        for page in range(total_pages):
            print(f"  Fetching page {page}...")
            await asyncio.sleep(0.1)  # API call
            yield {"page": page, "items": [f"item_{page}_{i}" for i in range(3)]}

    # Process pages as they arrive (memory efficient!)
    all_items = []
    async for page_data in fetch_pages(3):
        print(f"  Processing page {page_data['page']}")
        all_items.extend(page_data["items"])

    print(f"  Total items: {len(all_items)}")


"""
COMPARISON TABLE:
=================
| Concept                 | Regular               | Async                       |
|-------------------------|-----------------------|-----------------------------|
| Generator function      | def gen(): yield x    | async def gen(): yield x    |
| Iteration               | for x in gen()        | async for x in gen()        |
| Iterator protocol       | __iter__, __next__    | __aiter__, __anext__        |
| Stop signal             | StopIteration         | StopAsyncIteration          |
| Context manager         | __enter__, __exit__   | __aenter__, __aexit__       |
| Context usage           | with resource as r    | async with resource as r    |
| Decorator               | @contextmanager       | @asynccontextmanager        |


QUICK REFERENCE:
================
| Pattern                          | Code                                    |
|----------------------------------|-----------------------------------------|
| Async generator                  | async def gen(): yield x                |
| Iterate async                    | async for item in gen(): ...            |
| Async iterator class             | __aiter__(self), async __anext__(self)  |
| End iteration                    | raise StopAsyncIteration                |
| Async context manager class      | async __aenter__, async __aexit__       |
| Async context decorator          | @asynccontextmanager + try/yield/finally|

Now try the exercises below!
================================================================================
"""


# ==============================================================================
# QUESTION 1: Basic Async Generator
# ==============================================================================
"""
INTERVIEW CONTEXT:
"What's the difference between a regular generator and an async generator?
Implement an async generator that yields values with delays."

REQUIREMENTS:
1. Create async generator `async_range(start: int, stop: int, delay: float) -> AsyncGenerator[int, None]`
   - Yield integers from start to stop (exclusive), like range()
   - Sleep for `delay` seconds between each yield
   - Use `async def` and `yield`

EXPECTED BEHAVIOR:
>>> async def test():
...     results = []
...     async for num in async_range(0, 5, 0.01):
...         results.append(num)
...     return results
>>> asyncio.run(test())
[0, 1, 2, 3, 4]

KEY INSIGHT:
- Async generators are defined with `async def` and use `yield`
- They're iterated with `async for`
- Useful for streaming data, pagination, real-time feeds
"""

async def async_range(start: int, stop: int, delay: float) -> AsyncGenerator[int, None]:
    # YOUR CODE HERE
    pass


# ==============================================================================
# QUESTION 2: Async Iterator Class
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Implement an async iterator from scratch using __aiter__ and __anext__.
This tests deep understanding of the async iteration protocol."

REQUIREMENTS:
Implement class `AsyncCountdown`:
    def __init__(self, start: int, delay: float):
        # Initialize countdown from `start` down to 0

    def __aiter__(self):
        # Return self

    async def __anext__(self) -> int:
        # Return next value, sleep `delay` between values
        # Raise StopAsyncIteration when done (after yielding 0)

EXPECTED BEHAVIOR:
>>> async def test():
...     results = []
...     async for num in AsyncCountdown(3, 0.01):
...         results.append(num)
...     return results
>>> asyncio.run(test())
[3, 2, 1, 0]

KEY INSIGHT:
- __aiter__ must return the iterator (usually self)
- __anext__ must be an async method
- Raise StopAsyncIteration to signal end of iteration
"""

class AsyncCountdown:
    def __init__(self, start: int, delay: float):
        # YOUR CODE HERE
        pass

    def __aiter__(self):
        # YOUR CODE HERE
        pass

    async def __anext__(self) -> int:
        # YOUR CODE HERE
        pass


# ==============================================================================
# QUESTION 3: Async Generator with External Data
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Implement an async generator that simulates paginated API responses.
This is common when fetching large datasets from web APIs."

REQUIREMENTS:
1. Create async generator `paginated_fetch(total_items: int, page_size: int) -> AsyncGenerator[list[dict], None]`
   - Simulate fetching pages of items
   - Each page contains up to `page_size` items
   - Items are dicts: {"id": i, "page": page_num}
   - Sleep 0.01s per page (simulating API latency)
   - Yield one page at a time

EXPECTED BEHAVIOR:
>>> async def test():
...     pages = []
...     async for page in paginated_fetch(5, 2):
...         pages.append(page)
...     return pages
>>> asyncio.run(test())
[
    [{'id': 0, 'page': 0}, {'id': 1, 'page': 0}],
    [{'id': 2, 'page': 1}, {'id': 3, 'page': 1}],
    [{'id': 4, 'page': 2}]
]

ALGORITHM TIE-IN:
- This is chunked iteration - O(n) items, O(n/k) yields
- Memory efficient: only one page in memory at a time
"""

async def paginated_fetch(total_items: int, page_size: int) -> AsyncGenerator[list[dict], None]:
    # YOUR CODE HERE
    pass


# ==============================================================================
# QUESTION 4: Basic Async Context Manager
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Implement an async context manager for managing resources that require
async setup and teardown, like database connections."

REQUIREMENTS:
Implement class `AsyncResource`:
    def __init__(self, name: str, setup_time: float, teardown_time: float):
        self.name = name
        self.setup_time = setup_time
        self.teardown_time = teardown_time
        self.is_open = False
        self.operations = []  # Track operations for testing

    async def __aenter__(self):
        # Sleep for setup_time, set is_open=True
        # Append "opened" to operations
        # Return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # Sleep for teardown_time, set is_open=False
        # Append "closed" to operations
        # Return False (don't suppress exceptions)

    async def do_work(self, work_id: int):
        # Append f"work_{work_id}" to operations
        # Sleep 0.01s
        # Return f"Completed {work_id}"

EXPECTED BEHAVIOR:
>>> async def test():
...     resource = AsyncResource("db", 0.01, 0.01)
...     async with resource as r:
...         await r.do_work(1)
...     return resource.operations
>>> asyncio.run(test())
['opened', 'work_1', 'closed']
"""

class AsyncResource:
    def __init__(self, name: str, setup_time: float, teardown_time: float):
        # YOUR CODE HERE
        pass

    async def __aenter__(self):
        # YOUR CODE HERE
        pass

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # YOUR CODE HERE
        pass

    async def do_work(self, work_id: int):
        # YOUR CODE HERE
        pass


# ==============================================================================
# QUESTION 5: Async Context Manager with @asynccontextmanager
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Use the @asynccontextmanager decorator to create a simpler async context manager.
This is often preferred over implementing __aenter__/__aexit__."

REQUIREMENTS:
1. Create async context manager `timed_operation(name: str)` using @asynccontextmanager
   - Print f"Starting {name}" on enter
   - Yield the start time (time.perf_counter())
   - Print f"Finished {name} in {elapsed:.3f}s" on exit

2. Create async function `test_timed_operation() -> float`
   - Use timed_operation("test") context manager
   - Sleep for 0.1 seconds inside the context
   - Return the elapsed time

EXPECTED BEHAVIOR:
>>> elapsed = asyncio.run(test_timed_operation())
Starting test
Finished test in 0.1XXs
>>> 0.09 < elapsed < 0.15
True
"""

import time

@asynccontextmanager
async def timed_operation(name: str):
    # YOUR CODE HERE
    pass


async def test_timed_operation() -> float:
    # YOUR CODE HERE
    pass


# ==============================================================================
# QUESTION 6: Async Generator with Cleanup
# ==============================================================================
"""
INTERVIEW CONTEXT:
"How do you ensure cleanup happens in an async generator, even if
iteration is stopped early? This is crucial for resource management."

REQUIREMENTS:
1. Create async generator `stream_with_cleanup(items: list, cleanup_log: list) -> AsyncGenerator[Any, None]`
   - Use try/finally to ensure cleanup
   - In try: yield each item with 0.01s delay
   - In finally: append "cleanup" to cleanup_log

2. Create async function `test_early_stop() -> list[str]`
   - Create cleanup_log = []
   - Create stream from stream_with_cleanup([1, 2, 3, 4, 5], cleanup_log)
   - Only consume first 2 items using `async for` with a break
   - Return cleanup_log

EXPECTED BEHAVIOR:
>>> asyncio.run(test_early_stop())
['cleanup']  # Cleanup runs even though we stopped early

KEY INSIGHT:
- async generators support try/finally for cleanup
- Cleanup runs when: iteration completes, break, or exception
"""

async def stream_with_cleanup(items: list, cleanup_log: list) -> AsyncGenerator[Any, None]:
    # YOUR CODE HERE
    pass


async def test_early_stop() -> list[str]:
    # YOUR CODE HERE
    pass


# ==============================================================================
# QUESTION 7: Combining Async Iterators
# ==============================================================================
"""
INTERVIEW CONTEXT:
"How would you merge multiple async streams into one? This is useful
for combining data from multiple real-time sources."

REQUIREMENTS:
1. Create async generator `stream_source(source_id: int, values: list[int], delay: float)`
   - For each value, sleep delay seconds then yield (source_id, value)

2. Create async function `merge_streams(sources: list[tuple[int, list[int], float]]) -> list[tuple[int, int]]`
   - sources is list of (source_id, values, delay)
   - Collect ALL values from ALL sources
   - Run sources concurrently (hint: use tasks and a shared collection)
   - Return list of all (source_id, value) tuples

EXPECTED BEHAVIOR:
>>> sources = [
...     (1, [10, 11], 0.02),
...     (2, [20, 21, 22], 0.01)
... ]
>>> result = asyncio.run(merge_streams(sources))
>>> len(result)
5  # Total of all values from all sources
>>> {r[0] for r in result}
{1, 2}  # Both sources represented

ALGORITHM TIE-IN:
- This is a merge operation across multiple async iterables
- Similar to merging sorted streams in merge-sort
"""

async def stream_source(source_id: int, values: list[int], delay: float) -> AsyncGenerator[tuple[int, int], None]:
    # YOUR CODE HERE
    pass


async def merge_streams(sources: list[tuple[int, list[int], float]]) -> list[tuple[int, int]]:
    # YOUR CODE HERE
    pass


# ==============================================================================
# QUESTION 8: Async Iterator with Buffer
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Implement a buffered async iterator that pre-fetches items for better
performance. This is a common optimization pattern."

REQUIREMENTS:
Implement class `BufferedAsyncIterator`:
    def __init__(self, source: AsyncIterator, buffer_size: int = 3):
        # source: the underlying async iterator
        # buffer_size: how many items to pre-fetch

    async def start(self):
        # Start the background task that fills the buffer
        # Use asyncio.Queue as the buffer

    def __aiter__(self):
        return self

    async def __anext__(self):
        # Get next item from buffer
        # Handle end of iteration

    async def close(self):
        # Clean up background task

EXPECTED BEHAVIOR:
>>> async def slow_source():
...     for i in range(5):
...         await asyncio.sleep(0.05)
...         yield i

>>> async def test():
...     buffered = BufferedAsyncIterator(slow_source(), buffer_size=2)
...     await buffered.start()
...     results = []
...     async for item in buffered:
...         results.append(item)
...     await buffered.close()
...     return results
>>> asyncio.run(test())
[0, 1, 2, 3, 4]

KEY INSIGHT:
- Buffer runs as background task, filling a queue
- Consumer reads from queue, potentially getting items without waiting
- Improves throughput when consumer processing time varies
"""

class BufferedAsyncIterator:
    def __init__(self, source: AsyncIterator, buffer_size: int = 3):
        # YOUR CODE HERE
        pass

    async def start(self):
        # YOUR CODE HERE
        pass

    def __aiter__(self):
        # YOUR CODE HERE
        pass

    async def __anext__(self):
        # YOUR CODE HERE
        pass

    async def close(self):
        # YOUR CODE HERE
        pass


# ==============================================================================
# MAIN - Test Your Solutions
# ==============================================================================
async def main():
    print("=" * 60)
    print("WORKBOOK 05: Testing Your Solutions")
    print("=" * 60)

    # Test Q1
    print("\n[Q1] Testing async_range()...")
    try:
        results = []
        async for num in async_range(0, 5, 0.01):
            results.append(num)
        assert results == [0, 1, 2, 3, 4], f"Expected [0,1,2,3,4], got {results}"
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Test Q2
    print("\n[Q2] Testing AsyncCountdown...")
    try:
        results = []
        async for num in AsyncCountdown(3, 0.01):
            results.append(num)
        assert results == [3, 2, 1, 0], f"Expected [3,2,1,0], got {results}"
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Test Q3
    print("\n[Q3] Testing paginated_fetch()...")
    try:
        pages = []
        async for page in paginated_fetch(5, 2):
            pages.append(page)
        assert len(pages) == 3, f"Expected 3 pages, got {len(pages)}"
        assert len(pages[0]) == 2, "First page should have 2 items"
        assert len(pages[2]) == 1, "Last page should have 1 item"
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Test Q4
    print("\n[Q4] Testing AsyncResource...")
    try:
        resource = AsyncResource("test_db", 0.01, 0.01)
        async with resource as r:
            assert r.is_open == True, "Should be open inside context"
            await r.do_work(1)
            await r.do_work(2)
        assert resource.is_open == False, "Should be closed after context"
        assert resource.operations == ["opened", "work_1", "work_2", "closed"], \
            f"Unexpected operations: {resource.operations}"
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Test Q5
    print("\n[Q5] Testing timed_operation()...")
    try:
        elapsed = await test_timed_operation()
        assert 0.08 < elapsed < 0.15, f"Expected ~0.1s, got {elapsed}"
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Test Q6
    print("\n[Q6] Testing stream_with_cleanup()...")
    try:
        result = await test_early_stop()
        assert "cleanup" in result, "Cleanup should run on early stop"
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Test Q7
    print("\n[Q7] Testing merge_streams()...")
    try:
        sources = [
            (1, [10, 11], 0.01),
            (2, [20, 21, 22], 0.01)
        ]
        result = await merge_streams(sources)
        assert len(result) == 5, f"Expected 5 items, got {len(result)}"
        source_ids = {r[0] for r in result}
        assert source_ids == {1, 2}, f"Should have both sources, got {source_ids}"
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Test Q8
    print("\n[Q8] Testing BufferedAsyncIterator...")
    try:
        async def slow_source():
            for i in range(5):
                await asyncio.sleep(0.02)
                yield i

        buffered = BufferedAsyncIterator(slow_source(), buffer_size=2)
        await buffered.start()
        results = []
        async for item in buffered:
            results.append(item)
        await buffered.close()
        assert results == [0, 1, 2, 3, 4], f"Expected [0,1,2,3,4], got {results}"
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    print("\n" + "=" * 60)
    print("Workbook 05 Complete!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
