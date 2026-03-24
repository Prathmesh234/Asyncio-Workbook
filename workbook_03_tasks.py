"""
================================================================================
WORKBOOK 03: CREATE_TASK AND TASK MANAGEMENT
================================================================================
Difficulty: Beginner-Intermediate
Topics: asyncio.create_task, Task objects, cancellation, task lifecycle

Prerequisites: Workbooks 01-02

Learning Objectives:
- Understand the difference between gather and create_task
- Master Task object lifecycle (pending, running, done, cancelled)
- Learn how to cancel tasks gracefully
- Understand fire-and-forget patterns vs awaited tasks
================================================================================
"""

import asyncio
import time
from typing import Any


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║                           PRIMER: asyncio.create_task                       ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
WHAT IS asyncio.create_task?
============================
create_task() schedules a coroutine to run "in the background" and returns
a Task object. Unlike gather(), it gives you a HANDLE to control the task.

gather just executes it while create_task gives the scheduled co routine which can be run 
create_task returns a task object

SYNTAX:
-------
    task = asyncio.create_task(my_coroutine())
    # task starts running IMMEDIATELY (doesn't wait for await)
    # ... do other stuff ...
    result = await task  # Get the result when you need it

    so there is task.done(), task.result(), task.concelled(), task.cancel() can cancel the task 
    task.add_done_callback(background_tasks.discard)


KEY DIFFERENCE FROM GATHER:
---------------------------
| asyncio.gather()                    | asyncio.create_task()              |
|-------------------------------------|-------------------------------------|
| Runs multiple coroutines together   | Schedules ONE coroutine             |
| Returns results directly            | Returns a Task object               |
| Limited control                     | Full control (cancel, check status) |
| Good for: "run all, get all"        | Good for: "start now, control later"|


DEMO 1: Basic create_task
-------------------------
"""

async def _demo_work(name: str, delay: float) -> str:
    print(f"  [{name}] Starting...")
    await asyncio.sleep(delay)
    print(f"  [{name}] Done!")
    return f"{name} result"

async def _demo_basic_create_task():
    """
    Run this to see create_task in action:
    >>> asyncio.run(_demo_basic_create_task())
    """
    print("\n--- DEMO: Basic create_task ---")

    # Task starts IMMEDIATELY when create_task is called
    print("Creating task...")
    task = asyncio.create_task(_demo_work("BackgroundJob", 0.2))

    print(f"Task created! Is it done? {task.done()}")  # False - still running

    # We can do other work while the task runs
    print("Doing other work...")
    await asyncio.sleep(0.1)
    print(f"Still running? {not task.done()}")

    # Now wait for the result
    result = await task
    print(f"Task result: {result}")
    print(f"Is it done now? {task.done()}")  # True


"""
DEMO 2: Task States and Lifecycle
---------------------------------
Tasks go through states: PENDING -> RUNNING -> DONE (or CANCELLED)
"""

async def _demo_task_lifecycle():
    """
    Run this to see task states:
    >>> asyncio.run(_demo_task_lifecycle())
    """
    print("\n--- DEMO: Task Lifecycle ---")

    async def slow_task():
        await asyncio.sleep(0.2)
        return "completed"

    task = asyncio.create_task(slow_task())

    # Check states at different times
    print(f"Immediately after create_task:")
    print(f"  task.done() = {task.done()}")        # False
    print(f"  task.cancelled() = {task.cancelled()}")  # False

    await asyncio.sleep(0.3)  # Wait for completion

    print(f"\nAfter task completes:")
    print(f"  task.done() = {task.done()}")        # True
    print(f"  task.result() = {task.result()}")    # "completed"


"""
DEMO 3: Task Cancellation
-------------------------
You can CANCEL a running task. The task receives CancelledError.
"""

async def _demo_cancellation():
    """
    Run this to see cancellation:
    >>> asyncio.run(_demo_cancellation())
    """
    print("\n--- DEMO: Task Cancellation ---")

    async def long_running():
        try:
            print("  Long task starting...")
            await asyncio.sleep(10)  # Would take 10 seconds
            print("  Long task finished!")  # Never reached
            return "done"
        except asyncio.CancelledError:
            print("  Long task was CANCELLED!")
            raise  # Always re-raise CancelledError!

    task = asyncio.create_task(long_running())
    await asyncio.sleep(0.1)  # Let it start

    print(f"Cancelling task...")
    task.cancel()  # Request cancellation

    try:
        await task  # This will raise CancelledError
    except asyncio.CancelledError:
        print(f"Task is now cancelled: {task.cancelled()}")


"""
DEMO 4: Fire-and-Forget Pattern
-------------------------------
Sometimes you want to start a task without waiting for it.
WARNING: Keep a reference or the task might be garbage collected!
"""

async def _demo_fire_and_forget():
    """
    Run this to see fire-and-forget:
    >>> asyncio.run(_demo_fire_and_forget())
    """
    print("\n--- DEMO: Fire-and-Forget ---")

    background_tasks = set()  # Keep references!

    async def background_job(job_id: int):
        await asyncio.sleep(0.1)
        print(f"  Background job {job_id} completed!")

    # Start tasks without awaiting
    for i in range(3):
        task = asyncio.create_task(background_job(i))
        background_tasks.add(task)
        task.add_done_callback(background_tasks.discard)  # Auto-cleanup

    print("Tasks started, doing other work...")
    await asyncio.sleep(0.2)  # Let background tasks complete
    print("All done!")


"""
DEMO 5: create_task vs gather (When to Use Which)
--------------------------------------------------
"""

async def _demo_create_task_vs_gather():
    """
    Run this to compare approaches:
    >>> asyncio.run(_demo_create_task_vs_gather())
    """
    print("\n--- DEMO: create_task vs gather ---")

    # USE GATHER when: you want all results together
    print("\n1. Using gather (simpler for 'run all, get all'):")
    results = await asyncio.gather(
        _demo_work("A", 0.1),
        _demo_work("B", 0.1),
    )
    print(f"   All results: {results}")

    # USE CREATE_TASK when: you need control or interleaving
    print("\n2. Using create_task (more control):")
    task_a = asyncio.create_task(_demo_work("A", 0.2))
    task_b = asyncio.create_task(_demo_work("B", 0.1))

    # Wait for B first (it's faster)
    result_b = await task_b
    print(f"   B finished first: {result_b}")

    # Then get A
    result_a = await task_a
    print(f"   A finished: {result_a}")


"""
TASK METHODS QUICK REFERENCE:
=============================
| Method/Property      | Description                                    |
|----------------------|------------------------------------------------|
| task.done()          | Returns True if task completed/cancelled/failed|
| task.cancelled()     | Returns True if task was cancelled             |
| task.result()        | Get return value (raises if not done)          |
| task.exception()     | Get exception if task failed                   |
| task.cancel()        | Request task cancellation                      |
| task.get_name()      | Get task name                                  |
| task.set_name(name)  | Set task name (useful for debugging)           |
| task.add_done_callback(fn) | Call fn when task completes              |

CREATING TASKS WITH NAMES (Python 3.8+):
    task = asyncio.create_task(coro(), name="my-task")

    await task pauses the current function, but doesn't freeze everything - other tasks keep running!
    task1 = asyncio.create_task(work1())  # Starts running immediately
task2 = asyncio.create_task(work2())  # Starts running immediately

# Both tasks are ALREADY RUNNING in the background

await task1  # Pauses THIS function until task1 completes
             # BUT task2 keeps running concurrently!

Now try the exercises below!
================================================================================
"""


# ==============================================================================
# QUESTION 1: Basic create_task Usage
# ==============================================================================
"""
INTERVIEW CONTEXT:
"What's the difference between calling a coroutine directly and using create_task?
When would you use create_task over gather?"

Calling a coroutine directly is basically that you want to use await along with it, like you call it right there however create_task runs the task in the background itself
create_task can be used over gather when two operations need to be decoupled  - lets say two operations are of different speeds, using gather is not effecient as you will be as slow as your slowest fetch 


REQUIREMENTS:
1. Create async function `background_job(job_id: int, duration: float) -> str`
   - Sleep for `duration` seconds
   - Return f"Job {job_id} completed"

2. Create async function `run_with_create_task() -> list[str]`
   - Create 3 tasks using asyncio.create_task():
     * background_job(1, 0.1)
     * background_job(2, 0.05)
     * background_job(3, 0.15)
   - Await all 3 tasks and collect results
   - Return results IN THE ORDER tasks were created (not completion order)

EXPECTED BEHAVIOR:
>>> asyncio.run(run_with_create_task())
['Job 1 completed', 'Job 2 completed', 'Job 3 completed']
# Total time should be ~0.15s (concurrent execution)

KEY INSIGHT:
- create_task schedules the coroutine immediately (doesn't wait for await)
- gather is syntactic sugar over create_task for simple cases
- create_task gives you a Task object for more control
"""
async def background_job(job_id, duration):
    await asyncio.sleep(duration)
    return f"Job {job_id} completed"

async def run_with_create_task():
    #we have to start 3 tasks 
    result = []
    task1 = asyncio.create_task(background_job(1,0.1))
    task2 = asyncio.create_task(background_job(2,0.05))
    task3 = asyncio.create_task(background_job(3,0.15))
    result = await asyncio.gather(task1, task2, task3)
    return result
# ==============================================================================
# QUESTION 2: Task State Inspection
# ==============================================================================
"""
INTERVIEW CONTEXT:
"How can you check if an async task has completed? Demonstrate inspecting
Task object states during execution."

REQUIREMENTS:
1. Create async function `long_running_task(delay: float) -> str`
   - Sleep for `delay` seconds
   - Return "completed"

2. Create async function `monitor_task_state(delay: float) -> dict`
   - Create a task from long_running_task(delay)
   - Immediately check: is task.done()? Store as "initially_done"
   - Wait for half the delay
   - Check again: is task.done()? Store as "midway_done"
   - Await the task to completion
   - Check final state: is task.done()? Store as "finally_done"
   - Return {"initially_done": bool, "midway_done": bool, "finally_done": bool}

EXPECTED BEHAVIOR:
>>> asyncio.run(monitor_task_state(0.1))
{'initially_done': False, 'midway_done': False, 'finally_done': True}
"""

async def long_running_task(delay):
    await asyncio.sleep(delay)
    return "completed"

async def monitor_task_state(delay):
    return_map = {}
    start_time = time.perf_counter()
    task = asyncio.create_task(long_running_task(2))
    return_map["initially_done"] = task.done()
    return_map["midway_done"] = task.done()
    await task
    return_map["finally_done"] = task.done()
    end_time = time.perf_counter()
    print(end_time-start_time)

    return return_map



# ==============================================================================
# QUESTION 3: Task Cancellation
# ==============================================================================
"""
INTERVIEW CONTEXT:
"How do you cancel a running task? What happens to the task when cancelled?
This is critical for implementing timeouts and graceful shutdowns."

We can cancel a task by using task = asyncio.create_task(func...) and then task.cancel()
A CancelledError exception is raised at the next await point inside the coroutine
The coroutine can catch this exception to perform cleanup
The coroutine must re-raise the CancelledError (critical!)
except asyncio.CancelledError:
        print("Task was CANCELLED! Cleaning up...")
        # Do cleanup here (close files, connections, etc.)
        raise  # MUST re-raise!

REQUIREMENTS:
1. Create async function `cancellable_task(task_id: int) -> str`
   - Sleep for 10 seconds (long enough to be cancelled)
   - Return f"Task {task_id} finished"

2. Create async function `cancel_after_delay(task_id: int, cancel_delay: float) -> dict`
   - Create a task from cancellable_task(task_id)
   - Wait for `cancel_delay` seconds
   - Cancel the task
   - Return {
       "was_cancelled": task.cancelled(),
       "is_done": task.done(),
       "task_id": task_id
     }

EXPECTED BEHAVIOR:
>>> result = asyncio.run(cancel_after_delay(1, 0.1))
>>> result["was_cancelled"]
True
>>> result["is_done"]
True

KEY INSIGHT:
- cancelled() returns True only after the task has been cancelled
- A cancelled task is also considered done()
- CancelledError is raised inside the coroutine
"""
async def cancellable_task(task_id: int):
    try:
        await asyncio.sleep(10)
        return f"Task {task_id} finished"
    except asyncio.CancelledError:
        raise


async def cancel_after_delay(task_id: int, cancel_delay: float):
    task = asyncio.create_task(cancellable_task(task_id))
    await asyncio.sleep(cancel_delay)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        print("Cancellation confirmed")
    return {"was_cancelled": task.cancelled(),
       "is_done": task.done(),
       "task_id": task_id}



# ==============================================================================
# QUESTION 4: Graceful Cancellation Handling
# ==============================================================================
"""
INTERVIEW CONTEXT:
"How should a task handle cancellation requests? Implement a task that
performs cleanup when cancelled."
to perform a cleanup we can use like a set in order to check for the current task execution

REQUIREMENTS:
1. Create a list to track cleanup: cleanup_log = []

2. Create async function `task_with_cleanup(task_id: int, cleanup_log: list) -> str`
   - Try to sleep for 10 seconds
   - If CancelledError is raised:
     * Append f"Task {task_id} cleaning up" to cleanup_log
     * Re-raise the CancelledError (important!)
   - If not cancelled, return f"Task {task_id} completed"

3. Create async function `test_graceful_cancellation() -> list[str]`
   - Create empty cleanup_log
   - Create task from task_with_cleanup(1, cleanup_log)
   - Wait 0.05 seconds
   - Cancel the task
   - Try to await the task, catching CancelledError
   - Return cleanup_log

EXPECTED BEHAVIOR:
>>> asyncio.run(test_graceful_cancellation())
['Task 1 cleaning up']

KEY INSIGHT:
- Always re-raise CancelledError after cleanup
- Swallowing CancelledError breaks task cancellation semantics
"""
async def task_with_cleanup(task_id, cleanup_log):
    try:
        await asyncio.sleep(10)
        return f"Task {task_id} completed"
        
    except asyncio.CancelledError:
        cleanup_log.append( f"Task {task_id} cleaning up")
        raise

async def test_graceful_cancellation():
    cleanup_log = []
    task = asyncio.create_task(task_with_cleanup(1, cleanup_log))
    await asyncio.sleep(0.05)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        print("Cancelled")
    return cleanup_log

# ==============================================================================
# QUESTION 5: Fire-and-Forget Pattern
# ==============================================================================
"""
INTERVIEW CONTEXT:
"How do you run a background task without waiting for its result?
What are the pitfalls of fire-and-forget?"

We can basically use a set in order to manage the different tasks. So once we create the task object we add it to the set 
the set makes sure we do not have duplicates 

This is the fire and forget method, and then we check for the tasks if they are done or not and if they are then we output the result 

REQUIREMENTS:
1. Create a list to track completions: completion_log = []

2. Create async function `fire_and_forget_task(task_id: int, delay: float, log: list) -> None`
   - Sleep for `delay` seconds
   - Append f"Task {task_id} done" to log

3. Create async function `run_fire_and_forget() -> list[str]`
   - Create empty completion_log
   - Create 3 tasks (DO NOT await them yet):
     * fire_and_forget_task(1, 0.05, completion_log)
     * fire_and_forget_task(2, 0.1, completion_log)
     * fire_and_forget_task(3, 0.03, completion_log)
   - Wait 0.15 seconds to let tasks complete
   - Return completion_log (should have 3 entries)

EXPECTED BEHAVIOR:
>>> result = asyncio.run(run_fire_and_forget())
>>> len(result)
3
>>> 'Task 3 done' in result  # Task 3 finishes first (shortest delay)
True

WARNING:
- If you don't keep a reference to the task, it may be garbage collected
- Always store tasks in a set/list if you need them to complete
"""

async def fire_and_forget_task(task_id, delay, log):
    await asyncio.sleep(delay)
    log.append(task_id)

async def run_fire_and_forget():
    comp_log = []
    task1 = asyncio.create_task(fire_and_forget_task(1, 0.05, comp_log))
    task2 = asyncio.create_task(fire_and_forget_task(2, 0.1, comp_log))
    task3 = asyncio.create_task(fire_and_forget_task(3, 0.03, comp_log))

    await asyncio.sleep(0.15)
    return comp_log


# ==============================================================================
# QUESTION 6: Task with Result and Exception
# ==============================================================================
"""
INTERVIEW CONTEXT:
"How do you get results or exceptions from a Task object?
When should you use task.result() vs awaiting the task?"
await task: Cooperatively waits for the task. Other tasks keep running while this coroutine is suspended. The event loop stays active.

task.result(): Gets the result only if already done. Raises InvalidStateError if not ready. No waiting happens - it's instant (either returns or crashes).

using callbacks when 

# Fire and forget - don't need result immediately
task = asyncio.create_task(send_email(user))
task.add_done_callback(log_email_sent)  # Just log when done

# Keep going immediately
return {"status": "email queued"}

REQUIREMENTS:
1. Create async function `maybe_failing_task(should_fail: bool) -> int`
   - Sleep for 0.01 seconds
   - If should_fail: raise ValueError("Task failed intentionally")
   - Otherwise: return 42

2. Create async function `get_task_outcome(should_fail: bool) -> dict`
   - Create task from maybe_failing_task(should_fail)
   - Await the task (use try/except)
   - Return dict with:
     * "done": task.done()
     * "cancelled": task.cancelled()
     * "has_exception": task.exception() is not None (only check if done and not cancelled)
     * "result": task.result() if no exception, else None

EXPECTED BEHAVIOR:
>>> asyncio.run(get_task_outcome(False))
{'done': True, 'cancelled': False, 'has_exception': False, 'result': 42}
>>> result = asyncio.run(get_task_outcome(True))
>>> result['has_exception']
True
>>> result['result'] is None
True
"""

async def maybe_failing_task(should_fail):
    await asyncio.sleep(0.01)
    if should_fail:
        raise ValueError("Task failed intentionally")
    return 42

async def get_task_outcome(should_fail):
    task = asyncio.create_task(maybe_failing_task(should_fail))

    try:
        result = await task
        return {'done': True, 'cancelled': False, 'has_exception': False, 'result': result}
    except ValueError:
        # Task failed (exception), not cancelled
        return {
            'done': task.done(),  # True - task is done
            'cancelled': task.cancelled(),  # False - not cancelled
            'has_exception': task.exception() is not None,  # True - has exception
            'result': None  # Return None for failed tasks
        }


        

# ==============================================================================
# QUESTION 7: Task Naming and Identification
# ==============================================================================
"""
INTERVIEW CONTEXT:
"In a system with many concurrent tasks, how do you identify and debug
specific tasks? Demonstrate task naming."

With many concurrent tasks we can basically use a set in order to keep track of all the tasks 
If a specific task fails or gets a value error kind we just check it with if this task is in the set and then diagnose it 
also if a task gets completed we simply remove it from the set 

We can also set the names of the tasks using name="ny-task" as one of the args 
also task.set_name("task") does the same and we can also do task.get_name() to get  the name 

REQUIREMENTS:
1. Create async function `named_worker(work_id: int) -> str`
   - Sleep for 0.01 seconds
   - Return f"Work {work_id} done"

2. Create async function `create_named_tasks(count: int) -> list[str]`
   - Create `count` tasks from named_worker
   - Name each task: "worker-{i}" (e.g., "worker-0", "worker-1")
   - Collect all task names using task.get_name()
   - Await all tasks
   - Return list of task names

EXPECTED BEHAVIOR:
>>> asyncio.run(create_named_tasks(3))
['worker-0', 'worker-1', 'worker-2']

KEY INSIGHT:
- Task names are helpful for logging and debugging
- In Python 3.8+, use asyncio.create_task(coro, name="my-task")
"""

async def named_worker(work_id):
    await asyncio.sleep(0.01)
    return  f"Work {work_id} done"

async def create_named_tasks(count):
    tasks = []
    for i in range(count):
        task = asyncio.create_task(named_worker(i), name=f"worker-{i}")
        tasks.append(task.get_name())
    
    return tasks


# ==============================================================================
# QUESTION 8: Task Group Pattern (Manual Implementation)
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Implement a task manager that tracks multiple tasks and provides
aggregate operations. This tests understanding of task lifecycle."

We have to implement a task manager which does the following 

1) tracking tasks 
2) submiting new tasks 
3) cancelling all tasks 
4) waiting for all tasks to complete (will have to account for cancelled and failed tasks too )
5) count the number of active tasks 

Okay to track all the active tasks we will def need a list 


REQUIREMENTS:
Implement the TaskManager class:

class TaskManager:
    def __init__(self):
        # Track active tasks

    async def submit(self, coro) -> asyncio.Task:
        # Create and register a task

    async def cancel_all(self) -> int:
        # Cancel all active tasks, return count of cancelled tasks

    async def wait_all(self) -> list[Any]:
        # Wait for all tasks to complete, return results (or None for failed/cancelled)

    def active_count(self) -> int:
        # Return number of tasks that are not done

EXPECTED BEHAVIOR:
>>> async def test():
...     tm = TaskManager()
...     await tm.submit(asyncio.sleep(0.1))
...     await tm.submit(asyncio.sleep(0.05))
...     assert tm.active_count() == 2
...     results = await tm.wait_all()
...     assert tm.active_count() == 0
>>> asyncio.run(test())

ALGORITHM TIE-IN:
- This is essentially managing a dynamic collection with lifecycle tracking
- Similar to thread pool management in multi-threaded systems
"""
class TaskManager:
    def __init__(self):
        self.task_list = []
    
    async def submit(self, coro) -> asyncio.Task:
        task = asyncio.create_task(coro)
        self.task_list.append(task)
        return task  # Must return the task
    
    async def cancel_all(self) -> int:
        count = 0
        for task in self.task_list:
            task.cancel()
            count += 1
        return count
    
    async def wait_all(self) -> list[Any]:
        # Use return_exceptions=True to handle failed/cancelled tasks gracefully
        results = await asyncio.gather(*self.task_list, return_exceptions=True)
        # Convert exceptions to None as per requirements
        final_results = []
        for r in results:
            if not isinstance(r, Exception):
                final_results.append(r)
            else:
                final_results.append(None)
        return final_results
    
    def active_count(self) -> int:
        # Count only tasks that are NOT done
        count = 0
        for task in self.task_list:
            if not task.done():
                count += 1
        return count




# ==============================================================================
# MAIN - Test Your Solutions
# ==============================================================================
async def main():
    print("=" * 60)
    print("WORKBOOK 03: Testing Your Solutions")
    print("=" * 60)

    # Test Q1
    print("\n[Q1] Testing run_with_create_task()...")
    try:
        start = time.perf_counter()
        result = await run_with_create_task()
        elapsed = time.perf_counter() - start
        assert len(result) == 3, f"Expected 3 results, got {len(result)}"
        assert "Job 1" in result[0], "Order should be preserved"
        assert elapsed < 0.2, f"Should run concurrently, took {elapsed:.2f}s"
        print(f"    PASSED! (took {elapsed:.3f}s)")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Test Q2
    print("\n[Q2] Testing monitor_task_state()...")
    try:
        result = await monitor_task_state(0.1)
        assert result["initially_done"] == False, "Should not be done initially"
        assert result["midway_done"] == False, "Should not be done midway"
        assert result["finally_done"] == True, "Should be done finally"
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Test Q3
    print("\n[Q3] Testing cancel_after_delay()...")
    try:
        result = await cancel_after_delay(1, 0.05)
        assert result["was_cancelled"] == True, "Task should be cancelled"
        assert result["is_done"] == True, "Cancelled task should be done"
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Test Q4
    print("\n[Q4] Testing test_graceful_cancellation()...")
    try:
        result = await test_graceful_cancellation()
        assert len(result) == 1, "Should have one cleanup entry"
        assert "cleaning up" in result[0], "Should contain cleanup message"
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Test Q5
    print("\n[Q5] Testing run_fire_and_forget()...")
    try:
        result = await run_fire_and_forget()
        assert len(result) == 3, f"Expected 3 completions, got {len(result)}"
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Test Q6
    print("\n[Q6] Testing get_task_outcome()...")
    try:
        success = await get_task_outcome(False)
        assert success["done"] == True, "Task should be done"
        assert success["result"] == 42, "Result should be 42"
        failure = await get_task_outcome(True)
        assert failure["has_exception"] == True, "Should have exception"
        assert failure["result"] is None, "Failed task should return None"
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Test Q7
    print("\n[Q7] Testing create_named_tasks()...")
    try:
        names = await create_named_tasks(3)
        assert names == ["worker-0", "worker-1", "worker-2"], f"Got {names}"
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Test Q8
    print("\n[Q8] Testing TaskManager...")
    try:
        tm = TaskManager()

        async def dummy_task(delay):
            await asyncio.sleep(delay)
            return delay

        await tm.submit(dummy_task(0.05))
        await tm.submit(dummy_task(0.03))
        await tm.submit(dummy_task(0.07))

        assert tm.active_count() == 3, f"Expected 3 active, got {tm.active_count()}"

        results = await tm.wait_all()
        assert tm.active_count() == 0, "All tasks should be done"
        assert len(results) == 3, "Should have 3 results"
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    print("\n" + "=" * 60)
    print("Workbook 03 Complete!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
