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
            ##this is how we add to the queue - in python you know for deque we do appendLeft right
            await queue.put(item)
            print(f"  {name} produced: {item}")
        await queue.put(None)  # Sentinel to signal "done"

    async def consumer(name: str):
        ##while True because we keep processing it 
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

max_size to create backpressure
Producers have to wait 
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

tuples - Priority, data 
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

#    await q.join()  # waits here until both task_done() calls happen


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

async def worker(q):
    item = await q.get()
    try:
        await process(item)
    except Exception as e:
        # handle failure — log, retry, dead letter queue etc.
        await dead_letter_queue.put(item)  # send to separate failure queue
    finally:
        q.task_done()  # ALWAYS call in finally, success or failure
```
finally is always called regardless of the condition 
"""


# ==============================================================================
# QUESTION 1: Basic Queue Operations
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Explain the basic operations of asyncio.Queue and how it differs from
queue.Queue in the standard library."

asyncio.Queue is asynchronous queue, basically handling operations async when needed. 
So it gives a coroutine and we do not have for the task to finish. Also there are q.get.no_wait() operations where we can skip a heavy lifted process of getting from teh queue and directly get the value 


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
    q = asyncio.Queue(maxsize=3)
    await q.put(1)
    await q.put(2)
    await q.put(3)
    items = []
    was_full = not q.empty()
    if not q.empty():
        while not q.empty() :
            item = await q.get()
            items.append(item)
    return {
        "items": items,
        "was_full": was_full,
        "is_empty": q.empty()  # True now since drained
    }






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

processed_list = []
async def producer(queue, items, delay):
    for item in items:
        await asyncio.sleep(delay)
        await queue.put(item)
    await queue.put(None)
    return queue.qsize()

async def consumer(queue, processed, delay):
    while True:
        await asyncio.sleep(delay)
        item = await queue.get()
        if item is None:
            break
        processed_list.append(item)
    return len(processed_list)

async def run_producer_consumer(items):
    queue = asyncio.Queue()
    await asyncio.gather(
        consumer(queue, processed_list, 0.15), 
        producer(queue,items, 0.15)
    )
    return processed_list







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
    return_list = []
    for item in range(item_count):
        await asyncio.sleep(delay)
        return_list.append(item)
        await queue.put((producer_id, item))
    await queue.put((producer_id, None))
    return return_list


async def multi_consumer(
    consumer_id: int,
    queue: asyncio.Queue,
    results: list,
    stop_event: asyncio.Event
) -> int:
    while True:
        prod_id, item = await queue.get()
        if item is None and stop_event.set():
            break
        results.append((consumer_id, item))
    return len(results)


async def run_multi_pc(
    num_producers: int,
    num_consumers: int,
    items_per_producer: int
) -> dict:
    queue = asyncio.Queue()
    stop_event = asyncio.Event()
    results = []
    producers = [
        multi_producer(i, queue, items_per_producer, 0.01) for i in range(num_producers)
    ]
    consumers = [
        multi_consumer(i, queue, results, stop_event=stop_event) for i in range(num_consumers)
    ]
    await asyncio.gather(*producers)
    stop_event.set()
    await asyncio.gather(*consumers)
    return


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
    pqueue = asyncio.PriorityQueue()
    
    for task in tasks:
        await pqueue.put(task)
    
    return_list = []
    while not pqueue.empty():          # just drain until empty
        task = await pqueue.get()
        return_list.append(task.task_id)
    
    return return_list
    

    


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
Backpressure is an issue when the producer is producing faster than the consumer 


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

async def fast_producer(queue: asyncio.Queue, count, stats):
    stats["blocked_count"] = 0
    for c in range(count):
        try:
            queue.put_nowait(c)
        except asyncio.QueueFull:
            stats["blocked_count"] += 1
            ##we add it anyways at the end however we wait for some time 
            await queue.put(c)

async def slow_consumer(queue: asyncio.Queue, count, delay):
    return_list = []
    for _ in range(count):  # ✅ Consume exactly count items
        await asyncio.sleep(delay)  # ✅ Slow processing delay
        item = await queue.get()  # ✅ Blocking get - waits for item
        return_list.append(item)
    return return_list

async def measure_backpressure(queue_size, produce_count, consume_delay):
    queue = asyncio.Queue(maxsize=queue_size)
    stats = {}
    rtn_list = await asyncio.gather(
        fast_producer(queue, produce_count, stats ), 
        slow_consumer(queue, produce_count, consume_delay)
    )
    #[None, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]] - we will be getting both None and the list itself 
    print(rtn_list)
    return {"blocked_count":stats["blocked_count"],"consumed":rtn_list[1]}


# ==============================================================================
# QUESTION 7: Task Distribution with Work Stealing
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Implement a work distribution system where idle workers can 'steal' work
from others. This is a common pattern in parallel computing."
Ok this is interesting. We will 
1) Have a queue for each of the worker (a hashmap possibly worker_id: queue object?)
2) We can do something like a round robin. check size of each of the queue(can we do better here?)
3) check if own queue is not empty (means have to process), if true then process the items/ If not then steal from the longest queue?
How to steal? We will need proxy for the longest queue so something to keep track of the longest and the smallest queue
For now we will check the max size for each of the queue and just pop it/remove it from the queue's last message  

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
    def __init__(self, num_workers):
        self.worker_map = {}
        self.num_workers = num_workers
        for worker in range(self.num_workers):
            queue = asyncio.Queue()
            self.worker_map[worker] = queue
    
    async def submit(self, work_item):
        shortest_queue_size = float('inf')  # ✅ Start with infinity
        shortest_queue_id = 0
        
        for worker_id, queue in self.worker_map.items():
            size = queue.qsize()
            if size < shortest_queue_size:  # ✅ Correct comparison
                shortest_queue_size = size
                shortest_queue_id = worker_id
        
        await self.worker_map[shortest_queue_id].put(work_item)
    async def worker(self, worker_id: int, results: list, stop_event: asyncio.Event) -> None:
        while True:
            my_queue = self.worker_map[worker_id]
            
            # Try to get from own queue first
            if my_queue.qsize() > 0:
                work_item = await my_queue.get()
                results.append((worker_id, work_item))  # ✅ Tuple format
            else:
                # Try to steal from longest queue
                longest_queue_size = 0
                longest_queue_id = None
                
                for other_id, other_queue in self.worker_map.items():
                    if other_id != worker_id:  # Don't steal from self
                        size = other_queue.qsize()
                        if size > longest_queue_size:
                            longest_queue_size = size
                            longest_queue_id = other_id
                
                # If found work to steal
                if longest_queue_id is not None and longest_queue_size > 0:
                    try:
                        work_item = self.worker_map[longest_queue_id].get_nowait()
                        results.append((worker_id, work_item))  # ✅ Tuple format
                    except asyncio.QueueEmpty:
                        pass  # Someone else got it first
                
                # Check if we should stop
                if stop_event.is_set():
                    # Check if all queues are empty
                    if all(q.qsize() == 0 for q in self.worker_map.values()):
                        break
                
                # Small delay to avoid busy-waiting
                await asyncio.sleep(0.001)
    
    async def run_until_complete(self, work_items: list) -> list:
        ##we have also added the stop event to make sure we stop at some point  - signalling all the workers to stop 
        results = []
        stop_event = asyncio.Event()
        for item in work_items:
            await self.submit(item)
        
        #starting the workers 
        ##we then start all the workers along with a stop event 
        ##we create different tasks i.e different async tasks for workers to use 
        worker_tasks = [
            asyncio.create_task(self.worker(i, results, stop_event))
            for i in range(self.num_workers)
        ]
        await asyncio.sleep(0.1)
        stop_event.set()
        await asyncio.gather(*worker_tasks)
        return results

        
                




# ==============================================================================
# QUESTION 8: Queue-based Pipeline
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Design a multi-stage processing pipeline where each stage is connected
by a queue. This is common in data processing systems."

Okay this is not too bad but we have the input queue and the output queue



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
    while True:
        val = await input_queue.get()
        if val is None:
            ##making sure to check if the val is valid or not 
            if output_queue is not None:
                await output_queue.put(None)
            break
        transformed = transform(val)
        if output_queue is not None:
             await output_queue.put(transformed)
        results.append((name, transformed))

async def run_pipeline(data: list[int]) -> dict:
    ##we need 3 queues 
    q0_to_s1 = asyncio.Queue()  # Input → Stage 1
    q1_to_s2 = asyncio.Queue()  # Stage 1 → Stage 2
    q2_to_s3 = asyncio.Queue()  # Stage 2 → Stage 3
    
    results = []
    
    # Add initial data + sentinel
    for d in data:
        await q0_to_s1.put(d)
    ##sentinel value to stop the pipeline 
    await q0_to_s1.put(None)  # ✅ Sentinel to stop pipeline
    
    # Define transforms for each stage
    def multiply_by_2(x): return x * 2
    def add_10(x): return x + 10
    def to_string(x): return str(x)
    
    # Run all 3 stages CONCURRENTLY
    await asyncio.gather(
        ##these are 3 seperate pipelines 
        stage_processor("Stage1", q0_to_s1, q1_to_s2, multiply_by_2, results),
        stage_processor("Stage2", q1_to_s2, q2_to_s3, add_10, results),
        stage_processor("Stage3", q2_to_s3, None, to_string, results),  # ✅ No output queue for last stage
    )
    
    # Extract final outputs (only from Stage3)
    final_outputs = [val for name, val in results if name == "Stage3"]
    
    return {
        "final_outputs": final_outputs,
        "stage_results": results
    }


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

    # # Test Q3
    # print("\n[Q3] Testing run_multi_pc()...")
    # try:
    #     result = await run_multi_pc(2, 3, 5)
    #     assert result["total_produced"] == 10, f"Expected 10 produced, got {result['total_produced']}"
    #     assert result["total_consumed"] == 10, f"Expected 10 consumed, got {result['total_consumed']}"
    #     print("    PASSED!")
    # except Exception as e:
    #     print(f"    FAILED: {e}")

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
