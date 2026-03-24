"""
================================================================================
WORKBOOK 01: ASYNC/AWAIT FUNDAMENTALS
================================================================================
Difficulty: Beginner
Topics: Coroutines, async/await syntax, asyncio.run, asyncio.sleep

Prerequisites: Basic Python knowledge

Learning Objectives:
- Understand what a coroutine is and how it differs from a regular function
- Master the async/await syntax
- Learn how asyncio.run() starts the event loop
- Understand cooperative multitasking with asyncio.sleep()
================================================================================
"""

import asyncio
from typing import Any


# ==============================================================================
# QUESTION 1: Your First Coroutine
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Can you explain the difference between a regular function and a coroutine?
Write a simple coroutine that returns a greeting message."
## A regular function executes right away after defining however a coroutine basically is a async function which when called first gets awaited. This coroutine can be thought of as a promise of some sort that when awaited will eventually run. 

REQUIREMENTS:
1. Create an async function named `greet` that takes a `name` parameter (str)
2. The function should return the string: "Hello, {name}!"
3. DO NOT use asyncio.sleep - this is about understanding basic syntax


EXPECTED BEHAVIOR:
>>> asyncio.run(greet("Alice"))
'Hello, Alice!'

KEY CONCEPTS TO UNDERSTAND:
- A coroutine is defined using `async def` instead of `def`
- Calling a coroutine returns a coroutine object, not the result
- You must `await` a coroutine or use asyncio.run() to execute it
"""

async def greet(name: str) -> str:
    return f"Hello, {name}!"

# ==============================================================================
# QUESTION 2: Understanding Coroutine Objects
# ==============================================================================
"""
INTERVIEW CONTEXT:
"What happens when you call an async function without awaiting it?
Demonstrate the difference between calling and awaiting a coroutine."
-
If you call an async function without awaiting it you just get a promise object rather than a result or executed function 

REQUIREMENTS:
1. Create a function `demonstrate_coroutine_object` (regular function, not async)
2. Inside it, call `greet("Test")` WITHOUT awaiting and store in variable `coro_obj`
3. Print the type of `coro_obj`
4. Return the coro_obj (we'll await it outside)

EXPECTED BEHAVIOR:
>>> coro = demonstrate_coroutine_object()
<class 'coroutine'>
>>> asyncio.run(coro)
'Hello, Test!'

WHY THIS MATTERS:
- Common interview question: "What's the bug in this code: result = my_async_func()"
- Understanding coroutine objects helps debug "coroutine was never awaited" warnings
"""

def demonstrate_coroutine_object():
    co_routine_object  = greet("Test")
    print(type(co_routine_object))
    return co_routine_object


# ==============================================================================
# QUESTION 3: Sequential Async Execution
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Write an async function that simulates fetching data from multiple sources
sequentially. Each fetch takes some time. Return the total time taken."

REQUIREMENTS:
1. Create async function `fetch_data_sequential(delays: list[float]) -> float`
2. For each delay in the list, simulate a fetch using `asyncio.sleep(delay)`
3. After each "fetch", print: "Fetched data after {delay}s"
4. Return the sum of all delays (representing total sequential time)

EXPECTED BEHAVIOR:
>>> asyncio.run(fetch_data_sequential([0.1, 0.2, 0.1]))
Fetched data after 0.1s
Fetched data after 0.2s
Fetched data after 0.1s
0.4

KEY INSIGHT:
- Sequential awaits block until each completes
- Total time = sum of all individual times
- This is NOT the optimal pattern for independent operations (we'll fix in Workbook 02)
"""

async def fetch_data_sequential(delays) -> float:
    total_time = 0.0
    for delay in delays:
        await asyncio.sleep(delay)
        print(f"Fetched data after {delay}s")
        total_time += delay
    return total_time


# ==============================================================================
# QUESTION 4: Async Function Return Values
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Implement an async function that processes a list of numbers.
For each number, 'process' it (simulate with sleep) and return doubled values."

REQUIREMENTS:
1. Create async function `process_numbers(numbers: list[int]) -> list[int]`
2. For each number, await asyncio.sleep(0.01) to simulate processing
3. Return a list with each number doubled

EXPECTED BEHAVIOR:
>>> asyncio.run(process_numbers([1, 2, 3, 4, 5]))
[2, 4, 6, 8, 10]

DATA STRUCTURE TIE-IN:
- This demonstrates that async functions can return complex data structures
- The async/await pattern doesn't change how return values work
"""
async def process_numbers(numbers):
    return_list = []
    for num in numbers:
        await asyncio.sleep(0.01)
        return_list.append(2*num)
    return return_list




# ==============================================================================
# QUESTION 5: Nested Coroutine Calls
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Show how async functions can call other async functions.
Implement a pipeline where data flows through multiple async stages."

REQUIREMENTS:
1. Create async function `stage_one(value: int) -> int`
   - Sleep for 0.01 seconds
   - Return value * 2

2. Create async function `stage_two(value: int) -> int`
   - Sleep for 0.01 seconds
   - Return value + 10

3. Create async function `pipeline(value: int) -> int`
   - Call stage_one with the input value
   - Pass stage_one's result to stage_two
   - Return stage_two's result

EXPECTED BEHAVIOR:
>>> asyncio.run(pipeline(5))
20  # (5 * 2) + 10 = 20

KEY INSIGHT:
- Async functions can await other async functions
- The call stack works similarly to regular functions
- Each await is a potential suspension point
"""
async def stage_one(value):
    await asyncio.sleep(0.01)
    return value*2

async def stage_two(value):
    await asyncio.sleep(0.01)
    return value  +10

async def pipeline(value):
    first_val = await stage_one(value)
    second_val = await stage_two(first_val)
    return second_val
# ==============================================================================
# QUESTION 6: Async in a Loop with State
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Implement an async counter that increments a value with a delay between
each increment. This tests understanding of state management in async code."

REQUIREMENTS:
1. Create async function `async_counter(target: int, delay: float) -> list[int]`
2. Start from 0 and increment until reaching `target` (inclusive)
3. Between each increment, sleep for `delay` seconds
4. Collect and return all values from 0 to target as a list

EXPECTED BEHAVIOR:
>>> asyncio.run(async_counter(5, 0.01))
[0, 1, 2, 3, 4, 5]

ALGORITHM TIE-IN:
- This is similar to generating a sequence with controlled timing
- Foundation for rate-limiting patterns used in production systems
"""
async def async_counter(target, delay):
    return_list = []
    for i in range(0, target+1):
        return_list.append(i)
        await asyncio.sleep(delay)
    return return_list


# ==============================================================================
# QUESTION 7: Conditional Async Execution
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Write an async function that conditionally performs different async operations
based on input. This tests branching logic in async code."

REQUIREMENTS:
1. Create async function `conditional_fetch(fetch_type: str) -> dict`
2. If fetch_type == "fast": sleep 0.01s, return {"type": "fast", "data": "quick result"}
3. If fetch_type == "slow": sleep 0.1s, return {"type": "slow", "data": "detailed result"}
4. If fetch_type is anything else: NO sleep, return {"type": "error", "data": None}

EXPECTED BEHAVIOR:
>>> asyncio.run(conditional_fetch("fast"))
{'type': 'fast', 'data': 'quick result'}
>>> asyncio.run(conditional_fetch("slow"))
{'type': 'slow', 'data': 'detailed result'}
>>> asyncio.run(conditional_fetch("invalid"))
{'type': 'error', 'data': None}
"""

async def conditional_fetch(fetch_type):
    if fetch_type == "fast":
        await asyncio.sleep(0.01)
        return {"type": "fast", "data": "quick result"}
    elif fetch_type == "slow":
        await asyncio.sleep(0.1)
        return {"type": "slow", "data": "detailed result"}
    else:
        return {"type": "error", "data": None}




# ==============================================================================
# QUESTION 8: Async with Exception Handling
# ==============================================================================
"""
INTERVIEW CONTEXT:
"How do you handle exceptions in async code? Write a function that
gracefully handles failures during async operations."

REQUIREMENTS:
1. Create async function `safe_fetch(should_fail: bool) -> dict`
2. If should_fail is True:
   - Sleep for 0.01s (simulating partial work)
   - Raise a ValueError with message "Fetch failed!"
3. If should_fail is False:
   - Sleep for 0.01s
   - Return {"status": "success", "data": "fetched data"}

4. Create async function `fetch_with_fallback(should_fail: bool) -> dict`
   - Try to call safe_fetch(should_fail)
   - If ValueError is raised, return {"status": "fallback", "data": "default data"}
   - Otherwise return the successful result

EXPECTED BEHAVIOR:
>>> asyncio.run(fetch_with_fallback(False))
{'status': 'success', 'data': 'fetched data'}
>>> asyncio.run(fetch_with_fallback(True))
{'status': 'fallback', 'data': 'default data'}

KEY INSIGHT:
- try/except works normally in async code
- Exceptions propagate up the await chain just like regular calls
"""

async def safe_fetch(should_fail):
    await asyncio.sleep(0.01)
    if should_fail:
        raise ValueError("Fetch failed!")
    return  {"status": "success", "data": "fetched data"}
        



async def fetch_with_fallback(should_fail: bool) -> dict:
    try:
        output = await safe_fetch(should_fail)
    except ValueError:
         return {"status": "fallback", "data": "default data"}
    return output



# ==============================================================================
# MAIN - Test Your Solutions
# ==============================================================================
async def main():
    print("=" * 60)
    print("WORKBOOK 01: Testing Your Solutions")
    print("=" * 60)

    # Test Q1
    print("\n[Q1] Testing greet()...")
    try:
        result = await greet("Alice")
        assert result == "Hello, Alice!", f"Expected 'Hello, Alice!', got '{result}'"
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Test Q2
    print("\n[Q2] Testing demonstrate_coroutine_object()...")
    try:
        import sys
        from io import StringIO
        old_stdout = sys.stdout
        sys.stdout = StringIO()
        coro = demonstrate_coroutine_object()
        
        output = sys.stdout.getvalue()
        sys.stdout = old_stdout
        assert "coroutine" in output.lower(), "Should print coroutine type"
        result = await coro
        assert result == "Hello, Test!", f"Coroutine should return greeting"
        print(coro)
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Test Q3
    print("\n[Q3] Testing fetch_data_sequential()...")
    try:
        result = await fetch_data_sequential([0.01, 0.02, 0.01])
        assert abs(result - 0.04) < 0.001, f"Expected ~0.04, got {result}"
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Test Q4
    print("\n[Q4] Testing process_numbers()...")
    try:
        result = await process_numbers([1, 2, 3, 4, 5])
        assert result == [2, 4, 6, 8, 10], f"Expected [2,4,6,8,10], got {result}"
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Test Q5
    print("\n[Q5] Testing pipeline()...")
    try:
        result = await pipeline(5)
        assert result == 20, f"Expected 20, got {result}"
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Test Q6
    print("\n[Q6] Testing async_counter()...")
    try:
        result = await async_counter(5, 0.001)
        assert result == [0, 1, 2, 3, 4, 5], f"Expected [0,1,2,3,4,5], got {result}"
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Test Q7
    print("\n[Q7] Testing conditional_fetch()...")
    try:
        fast = await conditional_fetch("fast")
        slow = await conditional_fetch("slow")
        error = await conditional_fetch("invalid")
        assert fast["type"] == "fast", "Fast fetch failed"
        assert slow["type"] == "slow", "Slow fetch failed"
        assert error["type"] == "error", "Error case failed"
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Test Q8
    print("\n[Q8] Testing fetch_with_fallback()...")
    try:
        success = await fetch_with_fallback(False)
        fallback = await fetch_with_fallback(True)
        assert success["status"] == "success", "Success case failed"
        assert fallback["status"] == "fallback", "Fallback case failed"
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    print("\n" + "=" * 60)
    print("Workbook 01 Complete!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
