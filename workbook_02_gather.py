"""
================================================================================
WORKBOOK 02: RUNNING MULTIPLE COROUTINES & ASYNCIO.GATHER
================================================================================
Difficulty: Beginner
Topics: asyncio.gather, concurrent coroutines, return_exceptions, unpacking

Prerequisites: Workbook 01

Learning Objectives:
- Understand concurrent vs parallel execution
- Master asyncio.gather() for running multiple coroutines
- Learn how to handle results from multiple coroutines
- Understand the performance benefits of concurrent I/O
================================================================================
"""

import asyncio
import time
from typing import Any


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║                              PRIMER: asyncio.gather                         ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
WHAT IS asyncio.gather?
=======================
asyncio.gather() is the PRIMARY way to run multiple coroutines CONCURRENTLY
and collect all their results. Think of it as saying "start all these tasks
at once and give me all results when they're done."

SYNTAX:
-------
    results = await asyncio.gather(coro1, coro2, coro3, ...)
    # results is a list: [result1, result2, result3, ...]

KEY POINTS:
-----------
1. All coroutines START at the same time (concurrently)
2. gather() waits for ALL to complete before returning
3. Results are returned in the SAME ORDER as input (not completion order)
4. If any coroutine raises an exception, gather() raises it (by default)


DEMO 1: Basic Usage
-------------------
"""

async def _demo_fetch(name: str, delay: float) -> str:
    """Simulates fetching data with a delay"""
    print(f"  Starting fetch for {name}...")
    await asyncio.sleep(delay)
    print(f"  Finished fetch for {name}")
    return f"Data from {name}"

async def _demo_basic_gather():
    """
    Run this to see gather in action:
    >>> asyncio.run(_demo_basic_gather())
    """
    print("\n--- DEMO: Basic asyncio.gather ---")
    start = time.perf_counter()

    # All three fetches run CONCURRENTLY
    results = await asyncio.gather(
        _demo_fetch("API_1", 0.3),
        _demo_fetch("API_2", 0.2),
        _demo_fetch("API_3", 0.1),
    )

    elapsed = time.perf_counter() - start
    print(f"\nResults: {results}")
    print(f"Total time: {elapsed:.2f}s (NOT 0.6s because they ran concurrently!)")
    # Output order matches input order, even though API_3 finished first


"""
DEMO 2: Sequential vs Concurrent
---------------------------------
This is WHY gather matters - massive performance improvement for I/O operations.
"""

async def _demo_sequential_vs_concurrent():
    """
    Run this to see the performance difference:
    >>> asyncio.run(_demo_sequential_vs_concurrent())
    """
    print("\n--- DEMO: Sequential vs Concurrent ---")

    # SEQUENTIAL (BAD for independent operations)
    print("\n1. Sequential execution:")
    start = time.perf_counter()
    result1 = await _demo_fetch("A", 0.1)
    result2 = await _demo_fetch("B", 0.1)
    result3 = await _demo_fetch("C", 0.1)
    sequential_time = time.perf_counter() - start
    print(f"   Sequential time: {sequential_time:.2f}s")

    # CONCURRENT (GOOD - use gather!)
    print("\n2. Concurrent execution with gather:")
    start = time.perf_counter()
    results = await asyncio.gather(
        _demo_fetch("A", 0.1),
        _demo_fetch("B", 0.1),
        _demo_fetch("C", 0.1),
    )
    concurrent_time = time.perf_counter() - start
    print(f"   Concurrent time: {concurrent_time:.2f}s")
    print(f"   Speedup: {sequential_time/concurrent_time:.1f}x faster!")


"""
DEMO 3: Handling Exceptions
---------------------------
By default, gather raises the first exception encountered.
Use return_exceptions=True to get exceptions as return values instead.
"""

async def _demo_maybe_fail(name: str, should_fail: bool) -> str:
    await asyncio.sleep(0.1)
    if should_fail:
        raise ValueError(f"{name} failed!")
    return f"{name} succeeded"

async def _demo_exception_handling():
    """
    Run this to see exception handling:
    >>> asyncio.run(_demo_exception_handling())
    """
    print("\n--- DEMO: Exception Handling in gather ---")

    # Default behavior: raises exception
    print("\n1. Default behavior (raises exception):")
    try:
        results = await asyncio.gather(
            _demo_maybe_fail("Task1", False),
            _demo_maybe_fail("Task2", True),   # This will fail
            _demo_maybe_fail("Task3", False),
        )
    except ValueError as e:
        print(f"   Caught exception: {e}")

    # With return_exceptions=True: exceptions become return values
    print("\n2. With return_exceptions=True:")
    results = await asyncio.gather(
        _demo_maybe_fail("Task1", False),
        _demo_maybe_fail("Task2", True),   # This will fail
        _demo_maybe_fail("Task3", False),
        return_exceptions=True  # <-- Key parameter!
    )
    print(f"   Results: {results}")
    print("   Notice: Task2's exception is in the results list, not raised!")


"""
DEMO 4: Dynamic Number of Coroutines
------------------------------------
Often you need to gather a variable number of operations (e.g., from a list).
Use unpacking (*) or create a list of coroutines.
"""

async def _demo_dynamic_gather():
    """
    Run this to see dynamic gathering:
    >>> asyncio.run(_demo_dynamic_gather())
    """
    print("\n--- DEMO: Dynamic Number of Coroutines ---")

    urls = ["url_1", "url_2", "url_3", "url_4", "url_5"]

    # Method 1: List comprehension + unpacking
    results = await asyncio.gather(*[_demo_fetch(url, 0.1) for url in urls])
    print(f"Results: {results}")

    # Method 2: Create list of coroutines first
    coroutines = [_demo_fetch(url, 0.1) for url in urls]
    results = await asyncio.gather(*coroutines)


"""
QUICK REFERENCE:
================
| Pattern                          | Code                                      |
|----------------------------------|-------------------------------------------|
| Basic gather                     | await asyncio.gather(coro1, coro2)        |
| Get results                      | results = await asyncio.gather(...)       |
| Handle failures gracefully       | await asyncio.gather(..., return_exceptions=True) |
| Dynamic list of coroutines       | await asyncio.gather(*[coro for x in items]) |
| Named results                    | a, b, c = await asyncio.gather(c1, c2, c3) |

Now try the exercises below!
================================================================================
"""


# ==============================================================================
# QUESTION 1: Basic Gather Usage
# ==============================================================================
"""
INTERVIEW CONTEXT:
"How would you run multiple async operations concurrently and collect all results?
Demonstrate the basic usage of asyncio.gather."

REQUIREMENTS:
1. Create async function `fetch_user(user_id: int) -> dict`
   - Sleep for 0.1 seconds (simulating API call)
   - Return {"id": user_id, "name": f"User_{user_id}"}

2. Create async function `fetch_all_users(user_ids: list[int]) -> list[dict]`
   - Use asyncio.gather to fetch ALL users CONCURRENTLY
   - Return the list of user dicts in the same order as user_ids

EXPECTED BEHAVIOR:
>>> asyncio.run(fetch_all_users([1, 2, 3]))
[{'id': 1, 'name': 'User_1'}, {'id': 2, 'name': 'User_2'}, {'id': 3, 'name': 'User_3'}]

# IMPORTANT: This should take ~0.1s total, NOT 0.3s!

KEY INSIGHT:
- asyncio.gather runs coroutines concurrently
- All coroutines start "at the same time" (in async terms)
- Results are returned in the ORDER coroutines were passed, not completion order
"""

async def fetch_user(user_id):
    await asyncio.sleep(0.1)
    return {"id": user_id, "name": f"User_{user_id}"}

async def fetch_all_users(users_id):
    coro = [fetch_user(id) for id in users_id]
    results = await asyncio.gather(*coro)

    return results

# ==============================================================================
# QUESTION 2: Gather with Different Delays
# ==============================================================================
"""
INTERVIEW CONTEXT:
"If you have multiple API calls with different latencies, how does gather
handle them? Does it wait for all to complete?"

Yes, asyncio.gather waits for all the actions to be completed 



REQUIREMENTS:
1. Create async function `timed_operation(name: str, delay: float) -> tuple[str, float]`
   - Record start time
   - Sleep for `delay` seconds
   - Return (name, delay)

2. Create async function `run_mixed_operations() -> list[tuple[str, float]]`
   - Run these operations concurrently using gather:
     * timed_operation("fast", 0.05)
     * timed_operation("medium", 0.1)
     * timed_operation("slow", 0.15)
   - Return all results

EXPECTED BEHAVIOR:
>>> asyncio.run(run_mixed_operations())
[('fast', 0.05), ('medium', 0.1), ('slow', 0.15)]

# Total time should be ~0.15s (the slowest operation), NOT 0.30s

KEY INSIGHT:
- gather waits for ALL coroutines to complete
- Total time = time of SLOWEST operation
- Results maintain input order regardless of completion order
"""

async def timed_operation(name: str, delay):
    await asyncio.sleep(delay)
    return (name, delay)

async def run_mixed_operations():
    result = await asyncio.gather(
        timed_operation("fast", 0.05), 
        timed_operation("medium", 0.1), 
        timed_operation("slow", 0.15)
    )
    return result

# ==============================================================================
# QUESTION 3: Gather with Exception - Default Behavior
# ==============================================================================
"""
INTERVIEW CONTEXT:
"What happens when one coroutine in a gather fails? How does the default
behavior affect other coroutines?"

✅ True: gather() raises the exception immediately
❗ Important Detail: The other coroutines don't automatically stop - they may continue running in the background!

REQUIREMENTS:
1. Create async function `maybe_fail(value: int) -> int`
   - Sleep for 0.01 seconds
   - If value < 0, raise ValueError(f"Negative value: {value}")
   - Otherwise return value * 2

2. Create async function `gather_with_failures(values: list[int]) -> str`
   - Use asyncio.gather to run maybe_fail for all values
   - Use try/except to catch any ValueError
   - If exception occurs, return "FAILED: <exception message>"
   - If all succeed, return "SUCCESS: <list of results>"

EXPECTED BEHAVIOR:
>>> asyncio.run(gather_with_failures([1, 2, 3]))
'SUCCESS: [2, 4, 6]'
>>> asyncio.run(gather_with_failures([1, -2, 3]))
'FAILED: Negative value: -2'

KEY INSIGHT:
- By default, gather raises the FIRST exception it encounters
- When an exception is raised, the gather call fails
- Other coroutines may have already started running!
"""

async def maybe_fail(value):
    await asyncio.sleep(0.01)
    if value < 0:
        raise ValueError(f"Negative value: {value}")
    return value*2

async def gather_with_failures(values):
    try:
        result = await asyncio.gather(*[maybe_fail(value) for value in values])
    except ValueError as e:
        return f"FAILED: Negative value: {str(e)}"
    return f"SUCCESS: {result}"

# ==============================================================================
# QUESTION 4: Gather with return_exceptions=True
# ==============================================================================
"""
INTERVIEW CONTEXT:
"How can you run multiple operations and handle individual failures without
failing the entire batch? This is crucial for resilient systems."

REQUIREMENTS:
1. Create async function `resilient_fetch(item_id: int) -> dict | Exception`
   - Sleep for 0.01 seconds
   - If item_id is even, return {"id": item_id, "status": "success"}
   - If item_id is odd, raise ValueError(f"Cannot fetch odd id: {item_id}")

2. Create async function `fetch_batch_resilient(ids: list[int]) -> dict`
   - Use asyncio.gather with return_exceptions=True
   - Count successes and failures
   - Return {"successes": <count>, "failures": <count>, "results": <list>}

EXPECTED BEHAVIOR:
>>> result = asyncio.run(fetch_batch_resilient([1, 2, 3, 4]))
>>> result["successes"]
2
>>> result["failures"]
2

KEY INSIGHT:
- return_exceptions=True makes gather return exceptions as values
- This allows you to handle partial failures gracefully
- Common pattern in batch processing systems

#using isinstance(item, Exception) -> this basically checks the ValueError with ValueError remember these are types of classes that is why
"""

async def resilient_fetch(item_id: int) -> dict:
    await asyncio.sleep(0.01)
    if item_id % 2 != 0:
        raise ValueError
    return {"id": item_id, "status": "success"}


async def fetch_batch_resilient(ids: list[int]) -> dict:
    f_count = 0
    s_count = 0
    result = await asyncio.gather(*[resilient_fetch(id) for id in ids], return_exceptions=True)
    print(result)
    for item in result:
        if isinstance(item, Exception):  # ✅ Use isinstance()
            f_count += 1
        else:
            s_count += 1
    return {"successes": s_count,  "failures": f_count, "results": result}
    




# ==============================================================================
# QUESTION 5: Dynamic Gather (Variable Number of Coroutines)
# ==============================================================================
"""
INTERVIEW CONTEXT:
"How would you gather results from a dynamically generated list of coroutines?
For example, processing a variable-length list of URLs."

REQUIREMENTS:
1. Create async function `process_item(item: str) -> str`
   - Sleep for 0.01 seconds
   - Return item.upper()

2. Create async function `process_all_items(items: list[str]) -> list[str]`
   - Create a coroutine for EACH item in the list
   - Use gather to run them all concurrently
   - Return the results

EXPECTED BEHAVIOR:
>>> asyncio.run(process_all_items(["apple", "banana", "cherry"]))
['APPLE', 'BANANA', 'CHERRY']
>>> asyncio.run(process_all_items([]))  # Edge case: empty list
[]

ALGORITHM TIE-IN:
- This is the map-reduce pattern: map operation across items, gather results
- Time complexity: O(max_item_time) instead of O(sum_item_times)
"""

async def process_item(item):
    await asyncio.sleep(0.01)
    return item.upper()

async def process_all_items(items):
    result = await asyncio.gather( *[process_item(item) for item in items])
    return result


# ==============================================================================
# QUESTION 6: Nested Gather Operations
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Can you nest gather operations? For instance, fetching users and their
posts in a hierarchical structure."

REQUIREMENTS:
1. Create async function `fetch_post(post_id: int) -> dict`
   - Sleep for 0.01 seconds
   - Return {"post_id": post_id, "title": f"Post {post_id}"}

2. Create async function `fetch_user_with_posts(user_id: int, post_ids: list[int]) -> dict`
   - Fetch all posts concurrently using gather
   - Return {"user_id": user_id, "posts": <list of post dicts>}

3. Create async function `fetch_all_users_with_posts(user_data: list[tuple[int, list[int]]]) -> list[dict]`
   - user_data is a list of (user_id, [post_ids...]) tuples
   - Fetch ALL users with ALL their posts concurrently
   - Return list of user dicts with their posts

EXPECTED BEHAVIOR:
>>> data = [(1, [101, 102]), (2, [201])]
>>> result = asyncio.run(fetch_all_users_with_posts(data))
>>> len(result)
2
>>> result[0]["posts"][0]["post_id"]
101

KEY INSIGHT:
- Gather operations can be nested for hierarchical data
- All I/O across all levels happens concurrently
"""

async def fetch_post(post_id):
    await asyncio.sleep(0.01)
    return  {"post_id": post_id, "title": f"Post {post_id}"}

async def fetch_user_with_posts(user_id: int, post_ids: list[int]):
    posts = await asyncio.gather(*[fetch_post(pid) for pid in post_ids])
    return  {"user_id": user_id, "posts": posts}

async def fetch_all_users_with_posts(user_data: list[tuple[int, list[int]]]):
    #we have to return a list of user dicts with their posts 
    return_list = []
    user_dict = await asyncio.gather( *[fetch_user_with_posts(uid, pid) for uid, pid in user_data] )
    print(user_dict)
    return user_dict



# ==============================================================================
# QUESTION 7: Gather with Filtering Results
# ==============================================================================
"""
INTERVIEW CONTEXT:
"After gathering results, how would you filter and transform them?
This tests combining gather with data structure operations."

REQUIREMENTS:
1. Create async function `fetch_product(product_id: int) -> dict`
   - Sleep for 0.01 seconds
   - Return {"id": product_id, "price": product_id * 10, "in_stock": product_id % 2 == 0}

2. Create async function `get_available_products(product_ids: list[int]) -> list[dict]`
   - Fetch all products concurrently
   - Filter to only return products where in_stock is True
   - Sort by price ascending
   - Return the filtered, sorted list

EXPECTED BEHAVIOR:
>>> asyncio.run(get_available_products([1, 2, 3, 4, 5, 6]))
[{'id': 2, 'price': 20, 'in_stock': True}, {'id': 4, 'price': 40, 'in_stock': True}, {'id': 6, 'price': 60, 'in_stock': True}]

DATA STRUCTURE TIE-IN:
- Combining async I/O with filtering (O(n)) and sorting (O(n log n))
- Total time dominated by I/O, not computation
"""

async def fetch_product(product_id):
    await asyncio.sleep(0.01)
    return {"id": product_id, "price": product_id * 10, "in_stock": product_id % 2 == 0}

async def get_available_products(product_ids):
    products = await asyncio.gather(*[ fetch_product(id) for id in product_ids])
    return_list = []
    for product in products:
        if product["in_stock"]:
            return_list.append(product)
    return return_list
        

# ==============================================================================
# QUESTION 8: Gather vs Sequential - Performance Comparison
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Demonstrate the performance difference between sequential and concurrent
execution. When should you use each approach?"

REQUIREMENTS:
1. Create async function `slow_operation(delay: float = 0.05) -> str`
   - Sleep for delay seconds
   - Return "done"

2. Create async function `run_sequential(count: int, delay: float) -> float`
   - Run slow_operation `count` times SEQUENTIALLY
   - Return the total elapsed time

3. Create async function `run_concurrent(count: int, delay: float) -> float`
   - Run slow_operation `count` times CONCURRENTLY using gather
   - Return the total elapsed time

   #this is not concurrent 
        await asyncio.gather(slow_operations(delay))
        count -= 1
    end_time= time.perf_counter()
    return end_time - start_time

EXPECTED BEHAVIOR:
>>> seq_time = asyncio.run(run_sequential(5, 0.05))
>>> seq_time >= 0.25  # 5 * 0.05 = 0.25s minimum
True
>>> conc_time = asyncio.run(run_concurrent(5, 0.05))
>>> conc_time < 0.1  # Should be ~0.05s
True
>>> seq_time > conc_time * 4  # Sequential is much slower
True
"""

async def slow_operations(delay):
    await asyncio.sleep(delay)
    return "done"

async def run_sequential(count, delay):
    start_time = time.perf_counter()
    for _ in range(count):
        await slow_operations(delay)
    end_time = time.perf_counter()
    return end_time - start_time

async def run_concurrent(count, delay):
    start_time = time.perf_counter()
    await asyncio.gather(*[ slow_operations(delay) for _ in range(count)])
    end_time= time.perf_counter()
    return end_time - start_time


# ==============================================================================
# MAIN - Test Your Solutions
# ==============================================================================
async def main():
    print("=" * 60)
    print("WORKBOOK 02: Testing Your Solutions")
    print("=" * 60)

    # Test Q1
    print("\n[Q1] Testing fetch_all_users()...")
    try:
        start = time.perf_counter()
        result = await fetch_all_users([1, 2, 3, 4, 5])
        elapsed = time.perf_counter() - start
        assert len(result) == 5, f"Expected 5 users, got {len(result)}"
        assert result[0]["id"] == 1, "Order not preserved"
        assert elapsed < 0.2, f"Should run concurrently, took {elapsed:.2f}s"
        print(f"    PASSED! (took {elapsed:.3f}s)")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Test Q2
    print("\n[Q2] Testing run_mixed_operations()...")
    try:
        start = time.perf_counter()
        result = await run_mixed_operations()
        elapsed = time.perf_counter() - start
        assert len(result) == 3, "Should return 3 results"
        assert result[0][0] == "fast", "Order should be preserved"
        assert elapsed < 0.2, f"Should take ~0.15s, took {elapsed:.2f}s"
        print(f"    PASSED! (took {elapsed:.3f}s)")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Test Q3
    print("\n[Q3] Testing gather_with_failures()...")
    try:
        success = await gather_with_failures([1, 2, 3])
        assert "SUCCESS" in success, "Should succeed with positive values"
        failure = await gather_with_failures([1, -2, 3])
        assert "FAILED" in failure, "Should fail with negative value"
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Test Q4
    print("\n[Q4] Testing fetch_batch_resilient()...")
    try:
        result = await fetch_batch_resilient([1, 2, 3, 4, 5, 6])
        assert result["successes"] == 3, f"Expected 3 successes, got {result['successes']}"
        assert result["failures"] == 3, f"Expected 3 failures, got {result['failures']}"
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Test Q5
    print("\n[Q5] Testing process_all_items()...")
    try:
        result = await process_all_items(["hello", "world"])
        assert result == ["HELLO", "WORLD"], f"Expected uppercase, got {result}"
        empty = await process_all_items([])
        assert empty == [], "Empty list should return empty list"
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Test Q6
    print("\n[Q6] Testing fetch_all_users_with_posts()...")
    try:
        data = [(1, [101, 102]), (2, [201, 202, 203])]
        start = time.perf_counter()
        result = await fetch_all_users_with_posts(data)
        elapsed = time.perf_counter() - start
        assert len(result) == 2, "Should have 2 users"
        assert len(result[0]["posts"]) == 2, "User 1 should have 2 posts"
        assert len(result[1]["posts"]) == 3, "User 2 should have 3 posts"
        assert elapsed < 0.1, "Should run concurrently"
        print(f"    PASSED! (took {elapsed:.3f}s)")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Test Q7
    print("\n[Q7] Testing get_available_products()...")
    try:
        result = await get_available_products([1, 2, 3, 4, 5, 6])
        assert len(result) == 3, "Should have 3 in-stock products"
        assert all(p["in_stock"] for p in result), "All should be in stock"
        assert result[0]["price"] < result[1]["price"], "Should be sorted by price"
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Test Q8
    print("\n[Q8] Testing performance comparison...")
    try:
        seq_time = await run_sequential(5, 0.05)
        conc_time = await run_concurrent(5, 0.05)
        speedup = seq_time / conc_time
        assert seq_time > 0.24, f"Sequential should take >=0.25s, got {seq_time:.3f}s"
        assert conc_time < 0.15, f"Concurrent should take ~0.05s, got {conc_time:.3f}s"
        print(f"    PASSED! Sequential: {seq_time:.3f}s, Concurrent: {conc_time:.3f}s")
        print(f"    Speedup: {speedup:.1f}x")
    except Exception as e:
        print(f"    FAILED: {e}")

    print("\n" + "=" * 60)
    print("Workbook 02 Complete!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
