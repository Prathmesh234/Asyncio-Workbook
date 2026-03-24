"""
================================================================================
WORKBOOK 04: TIMEOUTS, WAIT, AND EXCEPTION HANDLING
================================================================================
Difficulty: Intermediate
Topics: asyncio.wait_for, asyncio.wait, asyncio.timeout, shield, exception groups

Prerequisites: Workbooks 01-03

Learning Objectives:
- Implement timeouts for async operations
- Understand asyncio.wait() vs asyncio.gather()
- Master exception handling in concurrent code
- Learn to shield operations from cancellation
================================================================================
"""

import asyncio
import time
from typing import Any


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║                    PRIMER: TIMEOUTS, WAIT, AND SHIELD                       ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
WHY TIMEOUTS MATTER:
====================
In production systems, operations can hang forever (network issues, deadlocks).
Timeouts prevent your application from getting stuck. asyncio provides several
timeout mechanisms for different use cases.


METHOD 1: asyncio.wait_for() - Timeout for a Single Coroutine
==============================================================
Wraps a single coroutine with a timeout. Raises TimeoutError if exceeded.
"""

async def _demo_slow_operation(delay: float) -> str:
    await asyncio.sleep(delay)
    return f"Completed after {delay}s"

async def _demo_wait_for():
    """
    Run this to see wait_for:
    >>> asyncio.run(_demo_wait_for())
    """
    print("\n--- DEMO: asyncio.wait_for ---")

    # Case 1: Operation completes in time
    print("\n1. Fast operation with generous timeout:")
    try:
        result = await asyncio.wait_for(
            _demo_slow_operation(0.1),
            timeout=1.0  # 1 second timeout
        )
        print(f"   Success: {result}")
    except asyncio.TimeoutError:
        print("   Timed out!")

    # Case 2: Operation exceeds timeout
    print("\n2. Slow operation with short timeout:")
    try:
        result = await asyncio.wait_for(
            _demo_slow_operation(2.0),  # Takes 2 seconds
            timeout=0.1  # Only wait 0.1 seconds
        )
        print(f"   Success: {result}")
    except asyncio.TimeoutError:
        print("   Timed out! (as expected)")


"""
METHOD 2: asyncio.timeout() - Context Manager (Python 3.11+)
============================================================
Wraps a BLOCK of code with a timeout. More flexible than wait_for.
"""

async def _demo_timeout_context():
    """
    Run this to see timeout context manager:
    >>> asyncio.run(_demo_timeout_context())
    """
    print("\n--- DEMO: asyncio.timeout() context manager ---")

    # Timeout for a block of operations
    print("\n1. Multiple operations under one timeout:")
    try:
        async with asyncio.timeout(0.5):  # Total budget: 0.5s
            result1 = await _demo_slow_operation(0.1)
            print(f"   Step 1: {result1}")
            result2 = await _demo_slow_operation(0.1)
            print(f"   Step 2: {result2}")
            result3 = await _demo_slow_operation(0.1)
            print(f"   Step 3: {result3}")
            print("   All steps completed!")
    except asyncio.TimeoutError:
        print("   Block timed out!")

    # Timeout exceeded
    print("\n2. Block exceeds timeout:")
    try:
        async with asyncio.timeout(0.15):
            await _demo_slow_operation(0.1)
            print("   Step 1 done")
            await _demo_slow_operation(0.1)  # This will be interrupted
            print("   Step 2 done")  # Never reached
    except asyncio.TimeoutError:
        print("   Timed out during step 2!")


"""
METHOD 3: asyncio.wait() - Fine-Grained Control Over Multiple Tasks
====================================================================
Unlike gather(), wait() gives you control over WHEN to stop waiting.

Return conditions:
- FIRST_COMPLETED: Return when ANY task finishes
- FIRST_EXCEPTION: Return when ANY task raises (or all complete)
- ALL_COMPLETED: Return when ALL tasks finish (default)
"""

async def _demo_racer(name: str, delay: float) -> str:
    await asyncio.sleep(delay)
    return f"{name} finished in {delay}s"

async def _demo_wait_modes():
    """
    Run this to see asyncio.wait modes:
    >>> asyncio.run(_demo_wait_modes())
    """
    print("\n--- DEMO: asyncio.wait() modes ---")

    # FIRST_COMPLETED - Race condition, fastest wins
    print("\n1. FIRST_COMPLETED (racing tasks):")
    tasks = [
        asyncio.create_task(_demo_racer("Fast", 0.1)),
        asyncio.create_task(_demo_racer("Medium", 0.2)),
        asyncio.create_task(_demo_racer("Slow", 0.3)),
    ]
    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    print(f"   Done: {[t.result() for t in done]}")
    print(f"   Still pending: {len(pending)} tasks")

    # Cancel pending tasks (important!)
    for task in pending:
        task.cancel()

    # ALL_COMPLETED - Wait for everything
    print("\n2. ALL_COMPLETED (wait for all):")
    tasks = [
        asyncio.create_task(_demo_racer("A", 0.1)),
        asyncio.create_task(_demo_racer("B", 0.1)),
    ]
    done, pending = await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED)
    print(f"   All done: {[t.result() for t in done]}")


"""
COMPARISON: gather() vs wait()
==============================
| Feature                    | gather()            | wait()                    |
|----------------------------|---------------------|---------------------------|
| Returns                    | List of results     | (done, pending) task sets |
| Order preserved            | Yes                 | No (sets)                 |
| Early return               | No                  | Yes (FIRST_COMPLETED)     |
| Exception handling         | return_exceptions   | Check task.exception()    |
| Use case                   | Get all results     | Racing, fine control      |


METHOD 4: asyncio.shield() - Protect from Cancellation
======================================================
Some operations (database commits, file writes) should NOT be cancelled.
shield() protects the inner operation from cancellation.
"""

async def _demo_critical_operation() -> str:
    print("   Critical operation started...")
    await asyncio.sleep(0.2)
    print("   Critical operation completed!")
    return "data saved"

async def _demo_shield():
    """
    Run this to see shield:
    >>> asyncio.run(_demo_shield())
    """
    print("\n--- DEMO: asyncio.shield() ---")

    print("\n1. WITHOUT shield (operation gets cancelled):")
    task = asyncio.create_task(_demo_critical_operation())
    await asyncio.sleep(0.05)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        print("   Operation was cancelled (BAD for critical ops!)")

    print("\n2. WITH shield (operation completes):")

    async def shielded_wrapper():
        try:
            return await asyncio.shield(_demo_critical_operation())
        except asyncio.CancelledError:
            print("   Outer task cancelled, but inner continues...")
            # Give inner operation time to complete
            await asyncio.sleep(0.2)

    task = asyncio.create_task(shielded_wrapper())
    await asyncio.sleep(0.05)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    print("   Critical operation was protected!")


"""
QUICK REFERENCE:
================
| Pattern                     | Code                                           |
|-----------------------------|------------------------------------------------|
| Timeout single operation    | await asyncio.wait_for(coro, timeout=5.0)      |
| Timeout code block          | async with asyncio.timeout(5.0): ...           |
| Race tasks (first wins)     | done, pending = await asyncio.wait(tasks,      |
|                             |     return_when=asyncio.FIRST_COMPLETED)       |
| Wait for any exception      | await asyncio.wait(..., return_when=FIRST_EXCEPTION) |
| Protect from cancellation   | await asyncio.shield(critical_operation())     |
| Handle timeout gracefully   | try: ... except asyncio.TimeoutError: ...      |

Now try the exercises below!
================================================================================
"""


# ==============================================================================
# QUESTION 1: Basic Timeout with wait_for
# ==============================================================================
"""
INTERVIEW CONTEXT:
"How do you prevent an async operation from hanging indefinitely?
Implement a timeout pattern using asyncio.wait_for."

REQUIREMENTS:
1. Create async function `slow_api_call(delay: float) -> str`
   - Sleep for `delay` seconds
   - Return "API response"

2. Create async function `fetch_with_timeout(delay: float, timeout: float) -> dict`
   - Use asyncio.wait_for to call slow_api_call with the given timeout
   - If successful: return {"status": "success", "data": <result>}
   - If TimeoutError: return {"status": "timeout", "data": None}

EXPECTED BEHAVIOR:
>>> asyncio.run(fetch_with_timeout(0.1, 1.0))  # Fast enough
{'status': 'success', 'data': 'API response'}
>>> asyncio.run(fetch_with_timeout(1.0, 0.1))  # Too slow
{'status': 'timeout', 'data': None}

KEY INSIGHT:
- wait_for raises TimeoutError if the coroutine doesn't complete in time
- The coroutine is CANCELLED when timeout occurs
"""

async def slow_api_call(delay: float) -> str:
    # YOUR CODE HERE
    pass


async def fetch_with_timeout(delay: float, timeout: float) -> dict:
    # YOUR CODE HERE
    pass


# ==============================================================================
# QUESTION 2: Context Manager Timeout (Python 3.11+)
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Python 3.11 introduced asyncio.timeout(). How does it differ from wait_for?
When would you use one over the other?"

REQUIREMENTS:
1. Create async function `multi_step_operation(steps: list[float]) -> list[str]`
   - For each step duration, sleep and append f"Step {i} done" to results
   - Return the list of results

2. Create async function `timed_multi_step(steps: list[float], timeout: float) -> dict`
   - Use asyncio.timeout() context manager (Python 3.11+)
   - Run multi_step_operation inside the context
   - Return {"completed": True, "results": <results>} on success
   - Return {"completed": False, "results": []} on timeout

EXPECTED BEHAVIOR:
>>> asyncio.run(timed_multi_step([0.01, 0.01, 0.01], 1.0))
{'completed': True, 'results': ['Step 0 done', 'Step 1 done', 'Step 2 done']}
>>> asyncio.run(timed_multi_step([0.1, 0.1, 0.1], 0.15))
{'completed': False, 'results': []}

KEY INSIGHT:
- timeout() is a context manager that can wrap multiple operations
- Useful when you want to limit total time for a code block
"""

async def multi_step_operation(steps: list[float]) -> list[str]:
    # YOUR CODE HERE
    pass


async def timed_multi_step(steps: list[float], timeout: float) -> dict:
    # YOUR CODE HERE
    pass


# ==============================================================================
# QUESTION 3: asyncio.wait with FIRST_COMPLETED
# ==============================================================================
"""
INTERVIEW CONTEXT:
"How would you race multiple async operations and use the first result?
This is useful for redundant requests or implementing 'fastest wins' patterns."

REQUIREMENTS:
1. Create async function `racer(racer_id: int, delay: float) -> dict`
   - Sleep for `delay` seconds
   - Return {"racer_id": racer_id, "delay": delay}

2. Create async function `race_to_finish(racers: list[tuple[int, float]]) -> dict`
   - racers is a list of (racer_id, delay) tuples
   - Create a task for each racer
   - Use asyncio.wait with return_when=FIRST_COMPLETED
   - Return the result of the first racer to complete
   - IMPORTANT: Cancel remaining tasks before returning

EXPECTED BEHAVIOR:
>>> asyncio.run(race_to_finish([(1, 0.1), (2, 0.05), (3, 0.15)]))
{'racer_id': 2, 'delay': 0.05}  # Racer 2 is fastest
"""

async def racer(racer_id: int, delay: float) -> dict:
    # YOUR CODE HERE
    pass


async def race_to_finish(racers: list[tuple[int, float]]) -> dict:
    # YOUR CODE HERE
    pass


# ==============================================================================
# QUESTION 4: asyncio.wait with FIRST_EXCEPTION
# ==============================================================================
"""
INTERVIEW CONTEXT:
"How can you detect when any task fails in a group, without waiting
for all tasks to complete?"

REQUIREMENTS:
1. Create async function `worker(worker_id: int, should_fail: bool, delay: float) -> str`
   - Sleep for `delay` seconds
   - If should_fail: raise ValueError(f"Worker {worker_id} failed")
   - Otherwise: return f"Worker {worker_id} done"

2. Create async function `run_until_failure(configs: list[tuple[int, bool, float]]) -> dict`
   - configs is list of (worker_id, should_fail, delay)
   - Use asyncio.wait with return_when=FIRST_EXCEPTION
   - Return {
       "done_count": number of completed tasks,
       "pending_count": number of pending tasks,
       "had_failure": True if any task raised an exception
     }
   - Cancel pending tasks before returning

EXPECTED BEHAVIOR:
>>> asyncio.run(run_until_failure([(1, False, 0.1), (2, True, 0.05), (3, False, 0.2)]))
{'done_count': 1, 'pending_count': 2, 'had_failure': True}
# Worker 2 fails first after 0.05s
"""

async def worker(worker_id: int, should_fail: bool, delay: float) -> str:
    # YOUR CODE HERE
    pass


async def run_until_failure(configs: list[tuple[int, bool, float]]) -> dict:
    # YOUR CODE HERE
    pass


# ==============================================================================
# QUESTION 5: Shielding Operations from Cancellation
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Some operations (like database commits) should not be cancelled midway.
How do you protect a critical section from cancellation?"

REQUIREMENTS:
1. Create a list to track: critical_log = []

2. Create async function `critical_operation(op_id: int, log: list) -> str`
   - Append f"Starting {op_id}" to log
   - Sleep for 0.1 seconds (simulating critical work)
   - Append f"Completed {op_id}" to log
   - Return f"Result {op_id}"

3. Create async function `protected_operation(op_id: int, log: list) -> str`
   - Use asyncio.shield() to protect critical_operation
   - Handle CancelledError: append f"Protected {op_id} from cancel" to log
   - Return the result

4. Create async function `test_shield() -> list[str]`
   - Create empty log
   - Create task from protected_operation(1, log)
   - Wait 0.01 seconds
   - Cancel the task
   - Wait 0.15 seconds (let shielded operation finish)
   - Return log

EXPECTED BEHAVIOR:
>>> result = asyncio.run(test_shield())
>>> "Starting 1" in result
True
>>> "Completed 1" in result  # Operation completes despite cancellation
True

KEY INSIGHT:
- shield() protects the inner coroutine from cancellation
- The outer coroutine still receives CancelledError
- Use for database transactions, critical writes, etc.
"""

async def critical_operation(op_id: int, log: list) -> str:
    # YOUR CODE HERE
    pass


async def protected_operation(op_id: int, log: list) -> str:
    # YOUR CODE HERE
    pass


async def test_shield() -> list[str]:
    # YOUR CODE HERE
    pass


# ==============================================================================
# QUESTION 6: Implementing Retry with Exponential Backoff
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Implement a retry mechanism with exponential backoff for flaky operations.
This is a common pattern in distributed systems."

REQUIREMENTS:
1. Create async function `flaky_operation(fail_count: int) -> str`
   - Keep track of how many times it's been called (use a mutable default or closure)
   - First `fail_count` calls: raise ConnectionError("Service unavailable")
   - After that: return "Success"

2. Create async function `retry_with_backoff(
    operation,
    max_retries: int = 3,
    base_delay: float = 0.01
) -> dict`
   - Try to call the operation
   - On failure: wait (base_delay * 2^attempt) seconds before retry
   - Track number of attempts
   - Return {"success": bool, "attempts": int, "result": <result or None>}

EXPECTED BEHAVIOR:
>>> counter = {"calls": 0}
>>> async def test_op():
...     counter["calls"] += 1
...     if counter["calls"] < 3:
...         raise ConnectionError("fail")
...     return "done"
>>> result = asyncio.run(retry_with_backoff(test_op, max_retries=5))
>>> result["success"]
True
>>> result["attempts"]
3

ALGORITHM TIE-IN:
- Exponential backoff: delay = base * 2^n
- Common in network programming to avoid thundering herd
"""

async def flaky_operation(fail_until: int, call_tracker: dict) -> str:
    # YOUR CODE HERE
    pass


async def retry_with_backoff(
    operation,
    max_retries: int = 3,
    base_delay: float = 0.01
) -> dict:
    # YOUR CODE HERE
    pass


# ==============================================================================
# QUESTION 7: Collecting Results with Partial Failures
# ==============================================================================
"""
INTERVIEW CONTEXT:
"When running multiple async operations, how do you handle scenarios
where some succeed and some fail, while still getting all available results?"

REQUIREMENTS:
1. Create async function `unreliable_fetch(item_id: int) -> dict`
   - Sleep for 0.01 seconds
   - If item_id % 3 == 0: raise ValueError(f"Cannot fetch {item_id}")
   - Otherwise: return {"id": item_id, "data": f"Data_{item_id}"}

2. Create async function `fetch_all_with_partial_failures(ids: list[int]) -> dict`
   - Fetch all items concurrently
   - Collect successes and failures separately
   - Return {
       "successful": [list of successful results],
       "failed": [list of {"id": item_id, "error": str(exception)}]
     }

EXPECTED BEHAVIOR:
>>> result = asyncio.run(fetch_all_with_partial_failures([1, 2, 3, 4, 5, 6]))
>>> len(result["successful"])
4  # Items 1, 2, 4, 5
>>> len(result["failed"])
2  # Items 3, 6
"""

async def unreliable_fetch(item_id: int) -> dict:
    # YOUR CODE HERE
    pass


async def fetch_all_with_partial_failures(ids: list[int]) -> dict:
    # YOUR CODE HERE
    pass


# ==============================================================================
# QUESTION 8: Implementing Deadline Pattern
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Implement a deadline pattern where multiple operations must all complete
within a total time budget, not individual timeouts."

REQUIREMENTS:
1. Create async function `timed_task(task_id: int, duration: float) -> dict`
   - Sleep for `duration` seconds
   - Return {"task_id": task_id, "duration": duration}

2. Create async function `run_with_deadline(
    tasks: list[tuple[int, float]],
    deadline: float
) -> dict`
   - tasks is list of (task_id, duration)
   - All tasks must complete within `deadline` seconds total
   - Return {
       "completed": [list of completed task results],
       "timed_out": [list of task_ids that didn't finish],
       "met_deadline": bool (True if all completed)
     }

EXPECTED BEHAVIOR:
>>> asyncio.run(run_with_deadline([(1, 0.05), (2, 0.03)], 0.1))
{'completed': [{'task_id': 1, ...}, {'task_id': 2, ...}], 'timed_out': [], 'met_deadline': True}

>>> asyncio.run(run_with_deadline([(1, 0.1), (2, 0.2)], 0.15))
# Task 2 won't finish in time
{'completed': [...], 'timed_out': [2], 'met_deadline': False}
"""

async def timed_task(task_id: int, duration: float) -> dict:
    # YOUR CODE HERE
    pass


async def run_with_deadline(
    tasks: list[tuple[int, float]],
    deadline: float
) -> dict:
    # YOUR CODE HERE
    pass


# ==============================================================================
# MAIN - Test Your Solutions
# ==============================================================================
async def main():
    print("=" * 60)
    print("WORKBOOK 04: Testing Your Solutions")
    print("=" * 60)

    # Test Q1
    print("\n[Q1] Testing fetch_with_timeout()...")
    try:
        success = await fetch_with_timeout(0.05, 1.0)
        assert success["status"] == "success", "Should succeed with long timeout"
        timeout = await fetch_with_timeout(1.0, 0.05)
        assert timeout["status"] == "timeout", "Should timeout with short timeout"
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Test Q2
    print("\n[Q2] Testing timed_multi_step()...")
    try:
        success = await timed_multi_step([0.01, 0.01, 0.01], 1.0)
        assert success["completed"] == True, "Should complete"
        assert len(success["results"]) == 3, "Should have 3 results"
        timeout = await timed_multi_step([0.1, 0.1, 0.1], 0.15)
        assert timeout["completed"] == False, "Should timeout"
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Test Q3
    print("\n[Q3] Testing race_to_finish()...")
    try:
        result = await race_to_finish([(1, 0.1), (2, 0.03), (3, 0.15)])
        assert result["racer_id"] == 2, f"Racer 2 should win, got {result}"
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Test Q4
    print("\n[Q4] Testing run_until_failure()...")
    try:
        result = await run_until_failure([
            (1, False, 0.1),
            (2, True, 0.03),
            (3, False, 0.2)
        ])
        assert result["had_failure"] == True, "Should detect failure"
        assert result["done_count"] >= 1, "At least one task should be done"
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Test Q5
    print("\n[Q5] Testing test_shield()...")
    try:
        result = await test_shield()
        assert "Starting 1" in result, "Should start the operation"
        assert "Completed 1" in result, "Should complete despite cancellation"
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Test Q6
    print("\n[Q6] Testing retry_with_backoff()...")
    try:
        call_tracker = {"calls": 0}

        async def test_op():
            call_tracker["calls"] += 1
            if call_tracker["calls"] < 3:
                raise ConnectionError("Simulated failure")
            return "Success"

        result = await retry_with_backoff(test_op, max_retries=5)
        assert result["success"] == True, "Should eventually succeed"
        assert result["attempts"] == 3, f"Should take 3 attempts, got {result['attempts']}"
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Test Q7
    print("\n[Q7] Testing fetch_all_with_partial_failures()...")
    try:
        result = await fetch_all_with_partial_failures([1, 2, 3, 4, 5, 6])
        assert len(result["successful"]) == 4, f"Expected 4 successes, got {len(result['successful'])}"
        assert len(result["failed"]) == 2, f"Expected 2 failures, got {len(result['failed'])}"
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Test Q8
    print("\n[Q8] Testing run_with_deadline()...")
    try:
        success = await run_with_deadline([(1, 0.03), (2, 0.02)], 0.1)
        assert success["met_deadline"] == True, "Should meet deadline"
        assert len(success["completed"]) == 2, "Both should complete"

        partial = await run_with_deadline([(1, 0.05), (2, 0.2)], 0.1)
        assert partial["met_deadline"] == False, "Should miss deadline"
        assert len(partial["timed_out"]) >= 1, "At least one should timeout"
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    print("\n" + "=" * 60)
    print("Workbook 04 Complete!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
