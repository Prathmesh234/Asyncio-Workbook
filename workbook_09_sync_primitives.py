"""
================================================================================
WORKBOOK 09: SYNCHRONIZATION PRIMITIVES AND ASYNC DATA STRUCTURES
================================================================================
Difficulty: Advanced
Topics: Lock, Semaphore, Event, Condition, Barrier, async-safe data structures

Prerequisites: Workbooks 01-08

Learning Objectives:
- Master asyncio synchronization primitives
- Implement thread-safe async data structures
- Understand when and how to use each primitive
- Solve classic concurrency problems with async/await
================================================================================
"""

import asyncio
import time
import random
from typing import Any, Optional
from collections import deque


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║                 PRIMER: ASYNCIO SYNCHRONIZATION PRIMITIVES                  ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
WHY SYNCHRONIZATION?
====================
Even in single-threaded asyncio, you can have RACE CONDITIONS when:
- Multiple coroutines access shared data
- Operations are interrupted between read and write

Example race condition:
    counter = 0

    async def increment():
        global counter
        temp = counter      # Read: temp = 0
        await asyncio.sleep(0)  # Yield control!
        counter = temp + 1  # Write: counter = 1

    # Both read 0, both write 1, counter ends up as 1, not 2!
    await asyncio.gather(increment(), increment())


THE PRIMITIVES:
===============
asyncio provides several synchronization primitives:

┌─────────────────────────────────────────────────────────────────────────────┐
│ Primitive     │ Purpose                              │ Use Case             │
├─────────────────────────────────────────────────────────────────────────────┤
│ Lock          │ Mutual exclusion (one at a time)     │ Protecting shared    │
│               │                                       │ resources            │
├─────────────────────────────────────────────────────────────────────────────┤
│ Semaphore     │ Limit concurrent access (N at a time)│ Rate limiting,       │
│               │                                       │ connection pools     │
├─────────────────────────────────────────────────────────────────────────────┤
│ Event         │ Signal between coroutines            │ Startup/shutdown     │
│               │                                       │ coordination         │
├─────────────────────────────────────────────────────────────────────────────┤
│ Condition     │ Wait for condition + notify          │ Producer-consumer    │
│               │                                       │ with conditions      │
├─────────────────────────────────────────────────────────────────────────────┤
│ Barrier       │ Sync N coroutines at a point         │ Phased computation   │
└─────────────────────────────────────────────────────────────────────────────┘


1. ASYNCIO.LOCK - Mutual Exclusion
===================================
Only ONE coroutine can hold the lock at a time.
"""

async def _demo_lock():
    """
    Run this to see Lock:
    >>> asyncio.run(_demo_lock())
    """
    print("\n--- DEMO: asyncio.Lock ---")

    lock = asyncio.Lock()
    counter = {"value": 0}

    async def safe_increment(name: str):
        async with lock:  # Only one at a time!
            print(f"  {name} acquired lock")
            temp = counter["value"]
            await asyncio.sleep(0.01)  # Simulate work
            counter["value"] = temp + 1
            print(f"  {name} releasing lock")

    await asyncio.gather(
        safe_increment("A"),
        safe_increment("B"),
        safe_increment("C"),
    )
    print(f"Final counter: {counter['value']} (should be 3)")


"""
2. ASYNCIO.SEMAPHORE - Limited Concurrency
==========================================
Allow up to N coroutines to proceed simultaneously.
Perfect for rate limiting or resource pools.
"""

async def _demo_semaphore():
    """
    Run this to see Semaphore:
    >>> asyncio.run(_demo_semaphore())
    """
    print("\n--- DEMO: asyncio.Semaphore ---")

    semaphore = asyncio.Semaphore(2)  # Only 2 at a time

    async def limited_task(task_id: int):
        print(f"  Task {task_id} waiting for semaphore...")
        async with semaphore:
            print(f"  Task {task_id} acquired! (semaphore)")
            await asyncio.sleep(0.2)
            print(f"  Task {task_id} releasing")

    # Start 5 tasks, but only 2 run at a time
    await asyncio.gather(*[limited_task(i) for i in range(5)])


"""
BoundedSemaphore: Like Semaphore, but raises error if released too many times.
Use when you want to catch bugs from unbalanced acquire/release.
"""


"""
3. ASYNCIO.EVENT - Simple Signaling
===================================
One coroutine sets a flag, others wait for it.
Like a "go" signal.
"""

async def _demo_event():
    """
    Run this to see Event:
    >>> asyncio.run(_demo_event())
    """
    print("\n--- DEMO: asyncio.Event ---")

    event = asyncio.Event()

    async def waiter(name: str):
        print(f"  {name} waiting for event...")
        await event.wait()
        print(f"  {name} received event!")

    async def setter():
        print("  Setter: doing some work...")
        await asyncio.sleep(0.2)
        print("  Setter: setting event!")
        event.set()  # Wake up all waiters

    await asyncio.gather(
        waiter("A"),
        waiter("B"),
        waiter("C"),
        setter(),
    )


"""
4. ASYNCIO.CONDITION - Wait for Specific Conditions
===================================================
More powerful than Event: wait until a condition is true.
Combine a lock with notification.
"""

async def _demo_condition():
    """
    Run this to see Condition:
    >>> asyncio.run(_demo_condition())
    """
    print("\n--- DEMO: asyncio.Condition ---")

    condition = asyncio.Condition()
    data = []

    async def consumer():
        async with condition:
            while len(data) < 3:  # Wait until we have 3 items
                print("  Consumer: waiting for 3 items...")
                await condition.wait()
            print(f"  Consumer: got {data}")

    async def producer():
        for i in range(3):
            await asyncio.sleep(0.1)
            async with condition:
                data.append(i)
                print(f"  Producer: added {i}, notifying...")
                condition.notify_all()  # Wake up waiters

    await asyncio.gather(consumer(), producer())


"""
5. ASYNCIO.BARRIER - Synchronize N Coroutines
=============================================
All N coroutines must reach the barrier before any can proceed.
Useful for phased computation.
"""

async def _demo_barrier():
    """
    Run this to see Barrier:
    >>> asyncio.run(_demo_barrier())
    """
    print("\n--- DEMO: asyncio.Barrier ---")

    barrier = asyncio.Barrier(3)  # Wait for 3 coroutines

    async def worker(name: str, delay: float):
        print(f"  {name} starting (will take {delay}s)...")
        await asyncio.sleep(delay)
        print(f"  {name} reached barrier, waiting for others...")
        await barrier.wait()
        print(f"  {name} passed barrier!")

    await asyncio.gather(
        worker("Fast", 0.1),
        worker("Medium", 0.2),
        worker("Slow", 0.3),
    )


"""
WHEN TO USE WHAT:
=================
| Scenario                                  | Primitive    |
|-------------------------------------------|--------------|
| Protect shared resource                   | Lock         |
| Limit concurrent operations (e.g., 10)    | Semaphore    |
| Wait for initialization/shutdown          | Event        |
| Wait for specific condition               | Condition    |
| Sync multiple workers at checkpoints      | Barrier      |


IMPORTANT: asyncio vs threading primitives
==========================================
| asyncio                    | threading             |
|----------------------------|-----------------------|
| asyncio.Lock               | threading.Lock        |
| asyncio.Semaphore          | threading.Semaphore   |
| asyncio.Event              | threading.Event       |
| asyncio.Condition          | threading.Condition   |
| asyncio.Barrier            | threading.Barrier     |

NEVER mix them! asyncio primitives for coroutines, threading for threads.


QUICK REFERENCE:
================
| Primitive      | Create                    | Use                           |
|----------------|---------------------------|-------------------------------|
| Lock           | lock = asyncio.Lock()     | async with lock: ...          |
| Semaphore      | sem = asyncio.Semaphore(n)| async with sem: ...           |
| Event          | event = asyncio.Event()   | await event.wait() / set()    |
| Condition      | cond = asyncio.Condition()| async with cond: await cond.wait() |
| Barrier        | barrier = asyncio.Barrier(n)| await barrier.wait()        |

Now try the exercises below!
================================================================================
"""


# ==============================================================================
# QUESTION 1: asyncio.Lock - Mutual Exclusion
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Multiple coroutines need to update a shared counter. How do you prevent
race conditions using asyncio.Lock?"

REQUIREMENTS:
1. Create class `AsyncCounter`:
    def __init__(self):
        self.value = 0
        # Add lock

    async def increment(self, amount: int = 1) -> int:
        # Thread-safe increment using lock
        # Return new value

    async def decrement(self, amount: int = 1) -> int:
        # Thread-safe decrement using lock
        # Return new value

    async def get(self) -> int:
        # Thread-safe read
        # Return current value

2. Create async function `test_counter_safety(num_coroutines: int, ops_per_coroutine: int) -> dict`
   - Create counter
   - Run num_coroutines concurrent tasks, each doing ops_per_coroutine increments
   - Return {"final_value": counter.value, "expected": num_coroutines * ops_per_coroutine}

EXPECTED BEHAVIOR:
>>> result = asyncio.run(test_counter_safety(100, 100))
>>> result["final_value"] == result["expected"]
True  # 10000 without race conditions
"""

class AsyncCounter:
    def __init__(self):
        # YOUR CODE HERE
        pass

    async def increment(self, amount: int = 1) -> int:
        # YOUR CODE HERE
        pass

    async def decrement(self, amount: int = 1) -> int:
        # YOUR CODE HERE
        pass

    async def get(self) -> int:
        # YOUR CODE HERE
        pass


async def test_counter_safety(num_coroutines: int, ops_per_coroutine: int) -> dict:
    # YOUR CODE HERE
    pass


# ==============================================================================
# QUESTION 2: asyncio.Semaphore - Rate Limiting
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Implement rate limiting for API calls where only N concurrent requests
are allowed at any time."

REQUIREMENTS:
1. Create class `RateLimiter`:
    def __init__(self, max_concurrent: int):
        # Semaphore for limiting

    async def acquire(self):
        # Acquire permit

    async def release(self):
        # Release permit

    @asynccontextmanager
    async def limit(self):
        # Context manager for automatic acquire/release

2. Create async function `api_call(call_id: int, limiter: RateLimiter, log: list) -> dict`
   - Use limiter.limit() context
   - Append f"start_{call_id}" to log
   - Sleep 0.05s (simulating API call)
   - Append f"end_{call_id}" to log
   - Return {"call_id": call_id, "status": "success"}

3. Create async function `test_rate_limiting(total_calls: int, max_concurrent: int) -> dict`
   - Run total_calls API calls with max_concurrent limit
   - Track max concurrent calls observed
   - Return {"max_concurrent_observed": int, "total_completed": int}

EXPECTED BEHAVIOR:
>>> result = asyncio.run(test_rate_limiting(10, 3))
>>> result["max_concurrent_observed"] <= 3
True
>>> result["total_completed"]
10
"""

from contextlib import asynccontextmanager

class RateLimiter:
    def __init__(self, max_concurrent: int):
        # YOUR CODE HERE
        pass

    async def acquire(self):
        # YOUR CODE HERE
        pass

    async def release(self):
        # YOUR CODE HERE
        pass

    @asynccontextmanager
    async def limit(self):
        # YOUR CODE HERE
        pass


async def api_call(call_id: int, limiter: RateLimiter, log: list) -> dict:
    # YOUR CODE HERE
    pass


async def test_rate_limiting(total_calls: int, max_concurrent: int) -> dict:
    # YOUR CODE HERE
    pass


# ==============================================================================
# QUESTION 3: asyncio.Event - Signaling
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Implement a startup sequence where workers wait for initialization
to complete before starting their work."

REQUIREMENTS:
1. Create async function `initializer(event: asyncio.Event, delay: float, log: list) -> None`
   - Append "init_start" to log
   - Sleep for delay (simulating initialization)
   - Append "init_complete" to log
   - Set the event

2. Create async function `worker(worker_id: int, event: asyncio.Event, log: list) -> str`
   - Append f"worker_{worker_id}_waiting" to log
   - Wait for event
   - Append f"worker_{worker_id}_started" to log
   - Return f"worker_{worker_id}_done"

3. Create async function `test_startup_sequence(num_workers: int) -> list[str]`
   - Create event and log
   - Start workers (they should wait)
   - Start initializer
   - Wait for all to complete
   - Return log

EXPECTED BEHAVIOR:
>>> log = asyncio.run(test_startup_sequence(3))
>>> log.index("init_complete") < log.index("worker_0_started")
True  # Workers start only after init
"""

async def initializer(event: asyncio.Event, delay: float, log: list) -> None:
    # YOUR CODE HERE
    pass


async def worker_event(worker_id: int, event: asyncio.Event, log: list) -> str:
    # YOUR CODE HERE
    pass


async def test_startup_sequence(num_workers: int) -> list[str]:
    # YOUR CODE HERE
    pass


# ==============================================================================
# QUESTION 4: asyncio.Condition - Producer/Consumer with Conditions
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Implement a bounded buffer where producers wait when full and
consumers wait when empty, using Condition."

REQUIREMENTS:
Implement class `BoundedBuffer`:
    def __init__(self, capacity: int):
        self.capacity = capacity
        self.buffer = []
        # Add condition

    async def put(self, item: Any) -> None:
        # Wait while buffer is full
        # Add item
        # Notify waiting consumers

    async def get(self) -> Any:
        # Wait while buffer is empty
        # Remove and return item
        # Notify waiting producers

    def size(self) -> int:
        return len(self.buffer)

EXPECTED BEHAVIOR:
>>> async def test():
...     buf = BoundedBuffer(2)
...     await buf.put(1)
...     await buf.put(2)
...     assert buf.size() == 2
...     item = await buf.get()
...     assert item == 1
...     assert buf.size() == 1
>>> asyncio.run(test())
"""

class BoundedBuffer:
    def __init__(self, capacity: int):
        # YOUR CODE HERE
        pass

    async def put(self, item: Any) -> None:
        # YOUR CODE HERE
        pass

    async def get(self) -> Any:
        # YOUR CODE HERE
        pass

    def size(self) -> int:
        # YOUR CODE HERE
        pass


# ==============================================================================
# QUESTION 5: asyncio.Barrier - Synchronized Phases
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Implement a parallel computation where all workers must complete each
phase before any can proceed to the next phase."

REQUIREMENTS:
1. Create async function `phased_worker(
    worker_id: int,
    barriers: list[asyncio.Barrier],
    results: list
) -> None`
   - For each barrier (phase):
     * Do some "work" (sleep 0.01 * (worker_id + 1))
     * Append (worker_id, phase_num) to results
     * Wait at the barrier

2. Create async function `run_phased_computation(
    num_workers: int,
    num_phases: int
) -> list[tuple[int, int]]`
   - Create barriers for each phase
   - Run all workers
   - Return results list

EXPECTED BEHAVIOR:
>>> results = asyncio.run(run_phased_computation(3, 2))
>>> # All workers complete phase 0 before any start phase 1
>>> phase_0_workers = [r[0] for r in results if r[1] == 0]
>>> phase_1_workers = [r[0] for r in results if r[1] == 1]
>>> len(phase_0_workers) == len(phase_1_workers) == 3
True
"""

async def phased_worker(
    worker_id: int,
    barriers: list[asyncio.Barrier],
    results: list
) -> None:
    # YOUR CODE HERE
    pass


async def run_phased_computation(num_workers: int, num_phases: int) -> list[tuple[int, int]]:
    # YOUR CODE HERE
    pass


# ==============================================================================
# QUESTION 6: Async-Safe LRU Cache
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Implement an async-safe LRU cache that can handle concurrent access.
This is a common interview question combining data structures with concurrency."

REQUIREMENTS:
Implement class `AsyncLRUCache`:
    def __init__(self, capacity: int):
        self.capacity = capacity
        # Use OrderedDict or custom structure
        # Add lock for thread safety

    async def get(self, key: str) -> Optional[Any]:
        # Return value if exists (move to end for LRU)
        # Return None if not found

    async def put(self, key: str, value: Any) -> None:
        # Add/update item
        # Evict oldest if over capacity

    async def size(self) -> int:
        # Return current number of items

EXPECTED BEHAVIOR:
>>> async def test():
...     cache = AsyncLRUCache(2)
...     await cache.put("a", 1)
...     await cache.put("b", 2)
...     assert await cache.get("a") == 1
...     await cache.put("c", 3)  # Evicts "b" (LRU)
...     assert await cache.get("b") is None
...     assert await cache.get("c") == 3
>>> asyncio.run(test())

ALGORITHM TIE-IN:
- LRU Cache: O(1) get and put using OrderedDict
- Classic LeetCode problem (#146) with async twist
"""

from collections import OrderedDict

class AsyncLRUCache:
    def __init__(self, capacity: int):
        # YOUR CODE HERE
        pass

    async def get(self, key: str) -> Optional[Any]:
        # YOUR CODE HERE
        pass

    async def put(self, key: str, value: Any) -> None:
        # YOUR CODE HERE
        pass

    async def size(self) -> int:
        # YOUR CODE HERE
        pass


# ==============================================================================
# QUESTION 7: Dining Philosophers Problem
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Solve the classic Dining Philosophers problem using asyncio primitives.
This tests understanding of deadlock prevention."

REQUIREMENTS:
1. Create class `DiningPhilosophers`:
    def __init__(self, num_philosophers: int):
        # Create forks (locks) for each position
        # Track eating history

    async def philosopher(self, philosopher_id: int, meals: int) -> None:
        # Each philosopher:
        #   - Thinks (sleep random 0.01-0.03s)
        #   - Picks up forks (prevent deadlock!)
        #   - Eats (sleep 0.01s, record meal)
        #   - Puts down forks
        # Repeat for `meals` times

    async def run_dinner(self, meals_per_philosopher: int) -> dict:
        # Run all philosophers concurrently
        # Return {"total_meals": count, "meals_per_philosopher": dict}

DEADLOCK PREVENTION:
- Use resource ordering: always acquire lower-numbered fork first
- Or use an arbitrator lock

EXPECTED BEHAVIOR:
>>> dp = DiningPhilosophers(5)
>>> result = asyncio.run(dp.run_dinner(3))
>>> result["total_meals"]
15  # 5 philosophers * 3 meals each
"""

class DiningPhilosophers:
    def __init__(self, num_philosophers: int):
        # YOUR CODE HERE
        pass

    async def philosopher(self, philosopher_id: int, meals: int) -> None:
        # YOUR CODE HERE
        pass

    async def run_dinner(self, meals_per_philosopher: int) -> dict:
        # YOUR CODE HERE
        pass


# ==============================================================================
# QUESTION 8: Async Read-Write Lock
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Implement a read-write lock that allows multiple concurrent readers
but exclusive access for writers. This is important for database-like systems."

REQUIREMENTS:
Implement class `AsyncRWLock`:
    def __init__(self):
        # Internal state for tracking readers/writers

    @asynccontextmanager
    async def read_lock(self):
        # Multiple readers allowed
        # Block if writer is active or waiting

    @asynccontextmanager
    async def write_lock(self):
        # Exclusive access
        # Block if any readers or another writer active

2. Create async function `test_rw_lock() -> dict`
   - Create shared data and RW lock
   - Run multiple readers and writers concurrently
   - Verify data consistency
   - Return {"reads": count, "writes": count, "data_consistent": bool}

EXPECTED BEHAVIOR:
>>> result = asyncio.run(test_rw_lock())
>>> result["data_consistent"]
True
"""

class AsyncRWLock:
    def __init__(self):
        # YOUR CODE HERE
        pass

    @asynccontextmanager
    async def read_lock(self):
        # YOUR CODE HERE
        pass

    @asynccontextmanager
    async def write_lock(self):
        # YOUR CODE HERE
        pass


async def test_rw_lock() -> dict:
    # YOUR CODE HERE
    pass


# ==============================================================================
# MAIN - Test Your Solutions
# ==============================================================================
async def main():
    print("=" * 60)
    print("WORKBOOK 09: Testing Your Solutions")
    print("=" * 60)

    # Test Q1
    print("\n[Q1] Testing AsyncCounter...")
    try:
        result = await test_counter_safety(50, 100)
        assert result["final_value"] == result["expected"], \
            f"Race condition! Expected {result['expected']}, got {result['final_value']}"
        print(f"    PASSED! Final value: {result['final_value']}")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Test Q2
    print("\n[Q2] Testing RateLimiter...")
    try:
        result = await test_rate_limiting(10, 3)
        assert result["max_concurrent_observed"] <= 3, \
            f"Rate limit exceeded: {result['max_concurrent_observed']}"
        assert result["total_completed"] == 10, "Not all calls completed"
        print(f"    PASSED! Max concurrent: {result['max_concurrent_observed']}")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Test Q3
    print("\n[Q3] Testing Event-based startup sequence...")
    try:
        log = await test_startup_sequence(3)
        init_complete_idx = log.index("init_complete")
        for i in range(3):
            start_idx = log.index(f"worker_{i}_started")
            assert start_idx > init_complete_idx, \
                f"Worker {i} started before init complete"
        print("    PASSED! Workers correctly waited for initialization")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Test Q4
    print("\n[Q4] Testing BoundedBuffer...")
    try:
        buf = BoundedBuffer(3)

        async def producer():
            for i in range(5):
                await buf.put(i)

        async def consumer():
            results = []
            for _ in range(5):
                results.append(await buf.get())
            return results

        producer_task = asyncio.create_task(producer())
        consumer_task = asyncio.create_task(consumer())

        results = await consumer_task
        await producer_task

        assert results == [0, 1, 2, 3, 4], f"Expected [0,1,2,3,4], got {results}"
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Test Q5
    print("\n[Q5] Testing phased computation...")
    try:
        results = await run_phased_computation(3, 2)
        phase_0 = [r for r in results if r[1] == 0]
        phase_1 = [r for r in results if r[1] == 1]
        assert len(phase_0) == 3, "All workers should complete phase 0"
        assert len(phase_1) == 3, "All workers should complete phase 1"

        # Verify phase 0 completes before phase 1 starts
        last_phase_0_idx = max(results.index(r) for r in phase_0)
        first_phase_1_idx = min(results.index(r) for r in phase_1)
        assert last_phase_0_idx < first_phase_1_idx, "Phase 0 should complete before phase 1"
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Test Q6
    print("\n[Q6] Testing AsyncLRUCache...")
    try:
        cache = AsyncLRUCache(2)
        await cache.put("a", 1)
        await cache.put("b", 2)
        assert await cache.get("a") == 1, "Should find 'a'"
        await cache.put("c", 3)  # Should evict "b"
        assert await cache.get("b") is None, "'b' should be evicted"
        assert await cache.get("c") == 3, "Should find 'c'"
        assert await cache.get("a") == 1, "'a' should still exist"
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Test Q7
    print("\n[Q7] Testing Dining Philosophers...")
    try:
        dp = DiningPhilosophers(5)
        result = await asyncio.wait_for(dp.run_dinner(3), timeout=5.0)
        assert result["total_meals"] == 15, f"Expected 15 meals, got {result['total_meals']}"
        print(f"    PASSED! Total meals: {result['total_meals']}")
    except asyncio.TimeoutError:
        print("    FAILED: Deadlock detected (timeout)")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Test Q8
    print("\n[Q8] Testing AsyncRWLock...")
    try:
        result = await test_rw_lock()
        assert result["data_consistent"], "Data should be consistent"
        print(f"    PASSED! Reads: {result['reads']}, Writes: {result['writes']}")
    except Exception as e:
        print(f"    FAILED: {e}")

    print("\n" + "=" * 60)
    print("Workbook 09 Complete!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
