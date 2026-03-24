"""
================================================================================
WORKBOOK 06: ASYNCIO.QUEUE AND PRODUCER-CONSUMER PATTERNS
================================================================================
Difficulty: Intermediate
Topics: asyncio.Queue, LifoQueue, PriorityQueue, producer-consumer, backpressure

Prerequisites: Workbooks 01-05

Learning Objectives:
- Master asyncio.Queue for inter-coroutine communication
- Implement producer-consumer patterns
- Understand backpressure and flow control
- Learn different queue types and their use cases
================================================================================
"""

import asyncio
import time
import heapq
from dataclasses import dataclass, field
from typing import Any


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║                      PRIMER: ASYNCIO.QUEUE                                  ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
WHAT IS asyncio.Queue?
======================
asyncio.Queue is a coroutine-safe queue for passing data between coroutines.
It's the foundation of the producer-consumer pattern in async Python.

KEY DIFFERENCE from queue.Queue:
- queue.Queue: for threads (blocks the thread)
- asyncio.Queue: for coroutines (awaits, doesn't block event loop)


QUEUE TYPES:
============
| Type                    | Behavior                          | Use Case              |
|-------------------------|-----------------------------------|-----------------------|
| asyncio.Queue           | FIFO (First In, First Out)        | General messaging     |
| asyncio.LifoQueue       | LIFO (Last In, First Out/Stack)   | Depth-first tasks     |
| asyncio.PriorityQueue   | Lowest value first                | Priority scheduling   |


BASIC OPERATIONS:
=================
"""

async def _demo_basic_queue():
    """
    Run this to see basic queue operations:
    >>> asyncio.run(_demo_basic_queue())
    """
    print("\n--- DEMO: Basic Queue Operations ---")

    # Create a queue (optionally with max size)
    queue = asyncio.Queue(maxsize=3)  # maxsize=0 means unlimited

    # PUT items into queue
    print("\nPutting items:")
    await queue.put("item_1")  # Awaits if queue is full
    print(f"  Put item_1, size={queue.qsize()}")
    await queue.put("item_2")
    print(f"  Put item_2, size={queue.qsize()}")

    # Check queue state
    print(f"\nQueue state:")
    print(f"  Size: {queue.qsize()}")
    print(f"  Empty: {queue.empty()}")
    print(f"  Full: {queue.full()}")

    # GET items from queue
    print("\nGetting items:")
    item = await queue.get()  # Awaits if queue is empty
    print(f"  Got: {item}, size={queue.qsize()}")

    # Non-blocking variants
    queue.put_nowait("item_3")  # Raises QueueFull if full
    item = queue.get_nowait()  # Raises QueueEmpty if empty
    print(f"  Got (nowait): {item}")


"""
THE PRODUCER-CONSUMER PATTERN:
==============================
- Producers: Create work items, put them in queue
- Consumers: Take items from queue, process them
- Queue: Decouples producers from consumers

This pattern enables:
- Parallel processing
- Load balancing
- Backpressure handling
"""

async def _demo_producer_consumer():
    """
    Run this to see producer-consumer:
    >>> asyncio.run(_demo_producer_consumer())
    """
    print("\n--- DEMO: Producer-Consumer Pattern ---")

    queue = asyncio.Queue()

    async def producer(name: str, items: list):
        for item in items:
            await asyncio.sleep(0.1)  # Simulate work
            await queue.put(item)
            print(f"  {name} produced: {item}")
        await queue.put(None)  # Sentinel to signal "done"

    async def consumer(name: str):
        while True:
            item = await queue.get()
            if item is None:  # Check sentinel
                print(f"  {name} received stop signal")
                break
            print(f"  {name} consumed: {item}")
            await asyncio.sleep(0.15)  # Simulate processing

    # Run producer and consumer concurrently
    await asyncio.gather(
        producer("Producer", ["A", "B", "C"]),
        consumer("Consumer"),
    )


"""
BACKPRESSURE WITH BOUNDED QUEUES:
=================================
When producers are faster than consumers, use maxsize to create backpressure.
Producers will wait when queue is full.
"""

async def _demo_backpressure():
    """
    Run this to see backpressure:
    >>> asyncio.run(_demo_backpressure())
    """
    print("\n--- DEMO: Backpressure with Bounded Queue ---")

    queue = asyncio.Queue(maxsize=2)  # Only holds 2 items!

    async def fast_producer():
        for i in range(5):
            print(f"  Producer: putting {i}...")
            await queue.put(i)  # Will WAIT if queue full
            print(f"  Producer: put {i} (queue size: {queue.qsize()})")

    async def slow_consumer():
        for _ in range(5):
            await asyncio.sleep(0.2)  # Slow processing
            item = await queue.get()
            print(f"  Consumer: got {item}")

    await asyncio.gather(fast_producer(), slow_consumer())


"""
MULTIPLE CONSUMERS (Worker Pool):
=================================
Multiple consumers can process items concurrently.
Use task_done() and join() for coordination.
"""

async def _demo_worker_pool():
    """
    Run this to see worker pool:
    >>> asyncio.run(_demo_worker_pool())
    """
    print("\n--- DEMO: Worker Pool Pattern ---")

    queue = asyncio.Queue()

    async def worker(name: str):
        while True:
            item = await queue.get()
            try:
                print(f"  {name} processing: {item}")
                await asyncio.sleep(0.1)  # Simulate work
            finally:
                queue.task_done()  # Mark item as processed

    # Start workers
    workers = [
        asyncio.create_task(worker(f"Worker-{i}"))
        for i in range(3)
    ]

    # Add work
    for item in ["A", "B", "C", "D", "E", "F"]:
        await queue.put(item)

    # Wait for all items to be processed
    await queue.join()
    print("  All items processed!")

    # Cancel workers
    for w in workers:
        w.cancel()


"""
PRIORITY QUEUE:
===============
Items are retrieved in priority order (lowest first).
Wrap items in tuples: (priority, data)
"""

async def _demo_priority_queue():
    """
    Run this to see priority queue:
    >>> asyncio.run(_demo_priority_queue())
    """
    print("\n--- DEMO: Priority Queue ---")

    pq = asyncio.PriorityQueue()

    # Add items with priority (lower number = higher priority)
    await pq.put((3, "Low priority"))
    await pq.put((1, "HIGH PRIORITY"))
    await pq.put((2, "Medium priority"))

    # Items come out in priority order
    print("Getting items in priority order:")
    while not pq.empty():
        priority, item = await pq.get()
        print(f"  Priority {priority}: {item}")


"""
LIFO QUEUE (Stack):
===================
Last item in is first item out. Useful for depth-first processing.
"""

async def _demo_lifo_queue():
    """
    Run this to see LIFO queue:
    >>> asyncio.run(_demo_lifo_queue())
    """
    print("\n--- DEMO: LIFO Queue (Stack) ---")

    stack = asyncio.LifoQueue()

    # Add items
    for item in ["First", "Second", "Third"]:
        await stack.put(item)
        print(f"  Pushed: {item}")

    # Items come out in reverse order
    print("\nPopping items:")
    while not stack.empty():
        item = await stack.get()
        print(f"  Popped: {item}")


"""
QUICK REFERENCE:
================
| Operation              | Blocking (awaits)      | Non-blocking               |
|------------------------|------------------------|----------------------------|
| Add item               | await queue.put(item)  | queue.put_nowait(item)     |
| Get item               | await queue.get()      | queue.get_nowait()         |
| Check size             | queue.qsize()          | -                          |
| Check empty            | queue.empty()          | -                          |
| Check full             | queue.full()           | -                          |
| Mark task done         | queue.task_done()      | -                          |
| Wait for all tasks     | await queue.join()     | -                          |

COMMON PATTERNS:
================
| Pattern                 | Code                                              |
|-------------------------|---------------------------------------------------|
| Bounded queue           | asyncio.Queue(maxsize=10)                         |
| Sentinel for stop       | await queue.put(None) ... if item is None: break  |
| Worker pool             | queue.task_done() + await queue.join()            |
| Priority item           | await pq.put((priority, data))                    |

Now try the exercises below!
================================================================================
"""


# ==============================================================================
# QUESTION 1: Basic Queue Operations
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Explain the basic operations of asyncio.Queue and how it differs from
queue.Queue in the standard library."

REQUIREMENTS:
1. Create async function `basic_queue_operations() -> dict`
   - Create an asyncio.Queue with maxsize=3
   - Put items 1, 2, 3 into the queue
   - Check if queue is full
   - Get all 3 items and collect them
   - Check if queue is empty
   - Return {
       "items": [collected items in order],
       "was_full": bool,
       "is_empty": bool
     }

EXPECTED BEHAVIOR:
>>> asyncio.run(basic_queue_operations())
{'items': [1, 2, 3], 'was_full': True, 'is_empty': True}

KEY INSIGHT:
- asyncio.Queue is for coroutine-to-coroutine communication
- put() and get() are coroutines that can await
- queue.Queue blocks threads; asyncio.Queue yields control
"""

async def basic_queue_operations() -> dict:
    # YOUR CODE HERE
    pass


# ==============================================================================
# QUESTION 2: Simple Producer-Consumer
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Implement the classic producer-consumer pattern using asyncio.Queue.
The producer generates items; the consumer processes them."

REQUIREMENTS:
1. Create async function `producer(queue: asyncio.Queue, items: list, delay: float) -> int`
   - Put each item into the queue with `delay` between puts
   - Return the count of items produced

2. Create async function `consumer(queue: asyncio.Queue, processed: list, delay: float) -> int`
   - Get items from queue and append to `processed` list
   - Process each item by sleeping `delay` seconds
   - Stop when receiving None (sentinel value)
   - Return count of items processed (not counting None)

3. Create async function `run_producer_consumer(items: list) -> list`
   - Create a queue
   - Run producer and consumer concurrently
   - Producer should put all items + None (sentinel)
   - Return the processed items list

EXPECTED BEHAVIOR:
>>> asyncio.run(run_producer_consumer([1, 2, 3, 4, 5]))
[1, 2, 3, 4, 5]
"""

async def producer(queue: asyncio.Queue, items: list, delay: float) -> int:
    # YOUR CODE HERE
    pass


async def consumer(queue: asyncio.Queue, processed: list, delay: float) -> int:
    # YOUR CODE HERE
    pass


async def run_producer_consumer(items: list) -> list:
    # YOUR CODE HERE
    pass


# ==============================================================================
# QUESTION 3: Multiple Producers and Consumers
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Scale the producer-consumer pattern with multiple producers and consumers.
This tests understanding of coordination in concurrent systems."

REQUIREMENTS:
1. Create async function `multi_producer(
    producer_id: int,
    queue: asyncio.Queue,
    item_count: int,
    delay: float
) -> list`
   - Produce `item_count` items: (producer_id, item_num)
   - Return list of items produced

2. Create async function `multi_consumer(
    consumer_id: int,
    queue: asyncio.Queue,
    results: list,
    stop_event: asyncio.Event
) -> int`
   - Consume items until stop_event is set AND queue is empty
   - Append (consumer_id, item) to results
   - Return count of items consumed

3. Create async function `run_multi_pc(
    num_producers: int,
    num_consumers: int,
    items_per_producer: int
) -> dict`
   - Run multiple producers and consumers
   - Return {
       "total_produced": sum of all produced,
       "total_consumed": sum of all consumed,
       "results_count": len(results)
     }

EXPECTED BEHAVIOR:
>>> result = asyncio.run(run_multi_pc(2, 3, 5))
>>> result["total_produced"]
10  # 2 producers * 5 items each
>>> result["total_consumed"]
10  # All items consumed
"""

async def multi_producer(
    producer_id: int,
    queue: asyncio.Queue,
    item_count: int,
    delay: float
) -> list:
    # YOUR CODE HERE
    pass


async def multi_consumer(
    consumer_id: int,
    queue: asyncio.Queue,
    results: list,
    stop_event: asyncio.Event
) -> int:
    # YOUR CODE HERE
    pass


async def run_multi_pc(
    num_producers: int,
    num_consumers: int,
    items_per_producer: int
) -> dict:
    # YOUR CODE HERE
    pass


# ==============================================================================
# QUESTION 4: Priority Queue
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Implement a task scheduler using asyncio.PriorityQueue where high-priority
tasks are processed first."

REQUIREMENTS:
1. Create dataclass `PriorityTask`:
    priority: int  # Lower number = higher priority
    task_id: str
    payload: Any

    # Make it comparable for PriorityQueue
    def __lt__(self, other): ...

2. Create async function `priority_scheduler(tasks: list[PriorityTask]) -> list[str]`
   - Add all tasks to an asyncio.PriorityQueue
   - Process tasks in priority order
   - Return list of task_ids in the order they were processed

EXPECTED BEHAVIOR:
>>> tasks = [
...     PriorityTask(3, "low", {}),
...     PriorityTask(1, "high", {}),
...     PriorityTask(2, "medium", {}),
... ]
>>> asyncio.run(priority_scheduler(tasks))
['high', 'medium', 'low']

ALGORITHM TIE-IN:
- PriorityQueue uses a heap internally: O(log n) for put/get
- Classic scheduling algorithm implementation
"""

@dataclass(order=True)
class PriorityTask:
    priority: int
    task_id: str = field(compare=False)
    payload: Any = field(compare=False)


async def priority_scheduler(tasks: list[PriorityTask]) -> list[str]:
    # YOUR CODE HERE
    pass


# ==============================================================================
# QUESTION 5: LIFO Queue (Stack Behavior)
# ==============================================================================
"""
INTERVIEW CONTEXT:
"When would you use a LifoQueue instead of a regular Queue?
Implement a depth-first work processor using LifoQueue."

REQUIREMENTS:
1. Create async function `process_depth_first(items: list[int]) -> list[int]`
   - Add all items to an asyncio.LifoQueue
   - Process and collect items
   - Return items in the order processed (should be reverse of input)

2. Create async function `depth_first_tree_traversal(tree: dict) -> list[str]`
   - tree is a dict like: {"name": "root", "children": [{"name": "child1", ...}, ...]}
   - Traverse the tree depth-first using LifoQueue
   - Return list of names in traversal order

EXPECTED BEHAVIOR:
>>> asyncio.run(process_depth_first([1, 2, 3, 4, 5]))
[5, 4, 3, 2, 1]

>>> tree = {
...     "name": "A",
...     "children": [
...         {"name": "B", "children": []},
...         {"name": "C", "children": [{"name": "D", "children": []}]}
...     ]
... }
>>> asyncio.run(depth_first_tree_traversal(tree))
['A', 'C', 'D', 'B']  # Depth-first: A -> C -> D -> B
"""

async def process_depth_first(items: list[int]) -> list[int]:
    # YOUR CODE HERE
    pass


async def depth_first_tree_traversal(tree: dict) -> list[str]:
    # YOUR CODE HERE
    pass


# ==============================================================================
# QUESTION 6: Queue with Backpressure
# ==============================================================================
"""
INTERVIEW CONTEXT:
"How do you handle backpressure when producers are faster than consumers?
Implement a bounded queue with monitoring."

REQUIREMENTS:
1. Create async function `fast_producer(queue: asyncio.Queue, count: int, stats: dict) -> None`
   - Produce `count` items as fast as possible
   - Track in stats["blocked_count"] how many times put had to wait (queue was full)
   - Use put_nowait with try/except to detect full queue, then fall back to put

2. Create async function `slow_consumer(queue: asyncio.Queue, count: int, delay: float) -> list`
   - Consume `count` items with `delay` between each
   - Return list of consumed items

3. Create async function `measure_backpressure(
    queue_size: int,
    produce_count: int,
    consume_delay: float
) -> dict`
   - Create bounded queue with queue_size
   - Run fast producer and slow consumer
   - Return {
       "blocked_count": number of times producer was blocked,
       "consumed": list of consumed items
     }

EXPECTED BEHAVIOR:
>>> result = asyncio.run(measure_backpressure(2, 10, 0.02))
>>> result["blocked_count"] > 0  # Producer should have been blocked
True
>>> len(result["consumed"])
10
"""

async def fast_producer(queue: asyncio.Queue, count: int, stats: dict) -> None:
    # YOUR CODE HERE
    pass


async def slow_consumer(queue: asyncio.Queue, count: int, delay: float) -> list:
    # YOUR CODE HERE
    pass


async def measure_backpressure(
    queue_size: int,
    produce_count: int,
    consume_delay: float
) -> dict:
    # YOUR CODE HERE
    pass


# ==============================================================================
# QUESTION 7: Task Distribution with Work Stealing
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Implement a work distribution system where idle workers can 'steal' work
from others. This is a common pattern in parallel computing."

REQUIREMENTS:
1. Create class `WorkerPool`:
    def __init__(self, num_workers: int):
        # Each worker has its own queue
        # Track completed work

    async def submit(self, work_item: Any) -> None:
        # Add work to the shortest queue (load balancing)

    async def worker(self, worker_id: int, results: list) -> None:
        # Process own queue
        # If own queue empty, try to steal from longest queue
        # Append (worker_id, work_item) to results

    async def run_until_complete(self, work_items: list) -> list:
        # Submit all work, run workers, return results

EXPECTED BEHAVIOR:
>>> pool = WorkerPool(3)
>>> results = asyncio.run(pool.run_until_complete([1, 2, 3, 4, 5, 6, 7, 8, 9]))
>>> len(results)
9
>>> len(set(r[0] for r in results))  # Multiple workers participated
3

ALGORITHM TIE-IN:
- Work stealing: O(1) steal from largest queue
- Better load balancing than static distribution
"""

class WorkerPool:
    def __init__(self, num_workers: int):
        # YOUR CODE HERE
        pass

    async def submit(self, work_item: Any) -> None:
        # YOUR CODE HERE
        pass

    async def worker(self, worker_id: int, results: list, stop_event: asyncio.Event) -> None:
        # YOUR CODE HERE
        pass

    async def run_until_complete(self, work_items: list) -> list:
        # YOUR CODE HERE
        pass


# ==============================================================================
# QUESTION 8: Queue-based Pipeline
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Design a multi-stage processing pipeline where each stage is connected
by a queue. This is common in data processing systems."

REQUIREMENTS:
1. Create async function `stage_processor(
    name: str,
    input_queue: asyncio.Queue,
    output_queue: asyncio.Queue | None,
    transform: callable,
    results: list
) -> None`
   - Read from input_queue
   - Apply transform function
   - If output_queue exists, put result there
   - Append (name, result) to results
   - Stop on None sentinel (propagate it to output_queue)

2. Create async function `run_pipeline(data: list[int]) -> dict`
   - Create 3-stage pipeline:
     * Stage 1: multiply by 2
     * Stage 2: add 10
     * Stage 3: convert to string
   - Return {
       "final_outputs": list of final strings,
       "stage_results": all (stage_name, result) tuples
     }

EXPECTED BEHAVIOR:
>>> result = asyncio.run(run_pipeline([1, 2, 3]))
>>> result["final_outputs"]
['12', '14', '16']  # (1*2+10, 2*2+10, 3*2+10)
"""

async def stage_processor(
    name: str,
    input_queue: asyncio.Queue,
    output_queue: asyncio.Queue | None,
    transform: callable,
    results: list
) -> None:
    # YOUR CODE HERE
    pass


async def run_pipeline(data: list[int]) -> dict:
    # YOUR CODE HERE
    pass


# ==============================================================================
# MAIN - Test Your Solutions
# ==============================================================================
async def main():
    print("=" * 60)
    print("WORKBOOK 06: Testing Your Solutions")
    print("=" * 60)

    # Test Q1
    print("\n[Q1] Testing basic_queue_operations()...")
    try:
        result = await basic_queue_operations()
        assert result["items"] == [1, 2, 3], f"Expected [1,2,3], got {result['items']}"
        assert result["was_full"] == True, "Queue should have been full"
        assert result["is_empty"] == True, "Queue should be empty"
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Test Q2
    print("\n[Q2] Testing run_producer_consumer()...")
    try:
        result = await run_producer_consumer([1, 2, 3, 4, 5])
        assert result == [1, 2, 3, 4, 5], f"Expected [1,2,3,4,5], got {result}"
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Test Q3
    print("\n[Q3] Testing run_multi_pc()...")
    try:
        result = await run_multi_pc(2, 3, 5)
        assert result["total_produced"] == 10, f"Expected 10 produced, got {result['total_produced']}"
        assert result["total_consumed"] == 10, f"Expected 10 consumed, got {result['total_consumed']}"
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Test Q4
    print("\n[Q4] Testing priority_scheduler()...")
    try:
        tasks = [
            PriorityTask(3, "low", {}),
            PriorityTask(1, "high", {}),
            PriorityTask(2, "medium", {}),
        ]
        result = await priority_scheduler(tasks)
        assert result == ["high", "medium", "low"], f"Expected priority order, got {result}"
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Test Q5
    print("\n[Q5] Testing process_depth_first() and depth_first_tree_traversal()...")
    try:
        result = await process_depth_first([1, 2, 3, 4, 5])
        assert result == [5, 4, 3, 2, 1], f"Expected reverse order, got {result}"

        tree = {
            "name": "A",
            "children": [
                {"name": "B", "children": []},
                {"name": "C", "children": [{"name": "D", "children": []}]}
            ]
        }
        traversal = await depth_first_tree_traversal(tree)
        assert traversal[0] == "A", f"Should start with root, got {traversal}"
        assert len(traversal) == 4, f"Should visit all 4 nodes, got {len(traversal)}"
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Test Q6
    print("\n[Q6] Testing measure_backpressure()...")
    try:
        result = await measure_backpressure(2, 10, 0.01)
        assert result["blocked_count"] > 0, "Should have backpressure"
        assert len(result["consumed"]) == 10, "Should consume all items"
        print(f"    PASSED! (blocked {result['blocked_count']} times)")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Test Q7
    print("\n[Q7] Testing WorkerPool...")
    try:
        pool = WorkerPool(3)
        results = await pool.run_until_complete([1, 2, 3, 4, 5, 6, 7, 8, 9])
        assert len(results) == 9, f"Expected 9 results, got {len(results)}"
        workers_used = set(r[0] for r in results)
        assert len(workers_used) >= 2, f"Should use multiple workers, used {workers_used}"
        print(f"    PASSED! (workers used: {workers_used})")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Test Q8
    print("\n[Q8] Testing run_pipeline()...")
    try:
        result = await run_pipeline([1, 2, 3])
        assert result["final_outputs"] == ["12", "14", "16"], \
            f"Expected ['12','14','16'], got {result['final_outputs']}"
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    print("\n" + "=" * 60)
    print("Workbook 06 Complete!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
