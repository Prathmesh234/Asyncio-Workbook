"""
================================================================================
CODESIGNAL CONCURRENCY — WORKBOOK 04: BOUNDED BLOCKING QUEUE & PRODUCER/CONSUMER
================================================================================
Difficulty: Core (the pattern behind almost every real-world worker system)
Model:      Python `threading` (preemptive, multi-threaded) — NOT asyncio
Primitives: Lock, Condition, queue.Queue, sentinel shutdown

Maps to INTERVIEW.MD:
    Bounded Blocking Queue (LC 1188, item 5), log pipeline (item 29),
    pub-sub (item 31)

HOW TO USE THIS FILE
--------------------
1. Read each PRIMER block.
2. Read the `_demo_*` function, then RUN the file to watch it:
       python3 "CodeSignal-Concurrency/workbook_04_bounded_queue.py"
3. Each EXERCISE below is a SKELETON — signature + docstring + `# YOUR CODE
   HERE` + `raise NotImplementedError`. Implement it yourself. Nothing here is
   solved for you (unlike workbook 01).
4. The `_check_*` self-tests at the bottom print PASS/FAIL — or
   "[FAIL] ...: not implemented" until you fill in the body. Run the file
   after each attempt.

THE ONE IDEA
------------
A bounded queue is TWO waits glued to one piece of shared state:
  - a producer waits while the buffer is FULL,
  - a consumer waits while the buffer is EMPTY,
  - and both need a lock around the buffer itself so size-checks and
    push/pop don't race.
`queue.Queue` already IS this, correctly, with better error handling than
you'll write in twenty minutes. Know how to build it from scratch (interviewers
ask), but reach for the stdlib version whenever the problem doesn't explicitly
forbid it.
================================================================================
"""

import threading
import time
import queue
import collections


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  PRIMER 1: THE CONDITION-BASED BOUNDED BUFFER                                ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
The classic hand-rolled shape:

    class BoundedBuffer:
        def __init__(self, capacity):
            self._buf = collections.deque()
            self._capacity = capacity
            self._lock = threading.Lock()
            self._not_full = threading.Condition(self._lock)
            self._not_empty = threading.Condition(self._lock)

        def put(self, x):
            with self._not_full:
                while len(self._buf) >= self._capacity:
                    self._not_full.wait()          # block while full
                self._buf.append(x)
                self._not_empty.notify()           # wake ONE waiting consumer

        def get(self):
            with self._not_empty:
                while not self._buf:
                    self._not_empty.wait()          # block while empty
                x = self._buf.popleft()
                self._not_full.notify()             # wake ONE waiting producer
                return x

TWO Conditions sharing ONE lock, vs. ONE Condition with two predicates?
  - Two Conditions (`not_full`, `not_empty`) built on the SAME underlying lock
    (`Condition(lock)`) let you `notify()` only the waiters who could possibly
    proceed — a full-buffer producer never gets woken by another producer's
    put. Less "thundering herd," slightly more bookkeeping.
  - One Condition for everything is simpler to write correctly under pressure:
    every wait is `while not predicate(): cond.wait()`, and every mutation
    ends with `cond.notify_all()` (not `notify()` — you don't know which
    predicate the sleeping threads are waiting on, so wake them all and let
    each re-check its own `while`). Costs a few extra spurious wakeups; costs
    you nothing in correctness. **In a 20-minute interview, default to this.**

Either way, the `while` (never `if`) around `wait()` is non-negotiable — see
workbook 01 PRIMER 5 if you need the refresher on why.
"""


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  PRIMER 2: SENTINEL-BASED SHUTDOWN                                           ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
A worker loop that does `while True: item = q.get(); process(item)` never
exits on its own — there's no "no more work" signal baked into a queue. The
standard fix is a SENTINEL: a distinguished value (usually `None`, or a
private `_SHUTDOWN = object()` if `None` is a legal payload) that means
"stop, don't process this, just exit."

The detail people get wrong: with M consumer threads pulling from ONE shared
queue, you must enqueue **exactly one sentinel per consumer** (not one total).
A queue is FIFO-fair but NOT "broadcast" — a sentinel dequeued by consumer A
is gone; consumer B never sees it and hangs forever. So:

    for _ in range(num_consumers):
        q.put(SENTINEL)                 # one per consumer, not one total

Each consumer, on seeing the sentinel, exits its loop WITHOUT re-enqueueing
anything. Every consumer thread then joins cleanly — that's how you know
shutdown is "graceful" rather than "we killed threads mid-flight."

(If you don't know `num_consumers` in advance, `Event`-based shutdown — each
consumer periodically checks `stop_event.is_set()` with a `get(timeout=...)`
— is the alternative. Sentinels are simpler when the consumer count is fixed
up front, which is the common interview framing.)
"""


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  PRIMER 3: WHY `queue.Queue` IS USUALLY THE RIGHT ANSWER                     ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
`queue.Queue(maxsize=N)` is exactly PRIMER 1's Condition-based bounded buffer,
already written, already tested, with extras:
  - `put(item, block=True, timeout=None)` / `get(...)` — blocking with
    optional timeout, raising `queue.Full` / `queue.Empty` instead of hanging
    forever if you pass `block=False` or a timeout.
  - `task_done()` / `join()` — a second layer of coordination: producers can
    `q.join()` to block until every enqueued item has been marked
    `task_done()` by a consumer, independent of sentinel shutdown.
  - Internally thread-safe: you never touch a lock yourself.

DEFAULT RULE: unless the prompt says "implement a bounded queue using only
locks/conditions" (that's Exercise 1 below — LC 1188 asks exactly this),
reach for `queue.Queue`. Hand-rolling one when you didn't have to is wasted
interview time and more surface area for bugs.

--- DEMO: producer + N consumers over queue.Queue, sentinel shutdown ---
"""

def _demo_producer_consumer_stdlib_queue():
    """>>> _demo_producer_consumer_stdlib_queue()"""
    print("\n--- DEMO: queue.Queue producer/consumer with sentinel shutdown ---")
    n_items = 20
    num_consumers = 3
    q = queue.Queue(maxsize=5)          # bounded: producer blocks if full
    results = []
    results_lock = threading.Lock()

    def producer():
        for i in range(n_items):
            q.put(i)                    # blocks if the queue is full
        for _ in range(num_consumers):
            q.put(None)                 # one sentinel PER consumer

    def consumer(cid):
        processed = 0
        while True:
            item = q.get()
            if item is None:            # sentinel -> exit, don't re-enqueue
                break
            with results_lock:
                results.append(item)
            processed += 1
        # (each consumer prints its own tally so you can see work was shared)
        print(f"    consumer {cid} processed {processed} items")

    p = threading.Thread(target=producer)
    consumers = [threading.Thread(target=consumer, args=(i,)) for i in range(num_consumers)]
    p.start()
    for c in consumers:
        c.start()
    p.join()
    for c in consumers:
        c.join()

    ok = sorted(results) == list(range(n_items))
    print(f"  all {n_items} items processed exactly once, every consumer exited: {ok}")


# ══════════════════════════════════════════════════════════════════════════════
#  CHEAT SHEET
# ══════════════════════════════════════════════════════════════════════════════
"""
┌──────────────────────────────────┬───────────────────────────────────────────┐
│ Situation                        │ Reach for                                  │
├──────────────────────────────────┼───────────────────────────────────────────┤
│ "Producer/consumer", no other    │ queue.Queue(maxsize=N)                     │
│ constraint mentioned             │                                             │
│ "Implement a bounded queue from  │ Lock + Condition(s) (Exercise 1)           │
│ scratch / without queue.Queue"   │                                             │
│ "N consumers, need clean exit"   │ one sentinel per consumer (PRIMER 2)       │
│ "Every subscriber gets every     │ PubSub: one queue.Queue PER subscriber,    │
│ message published while it's     │ publish() fans a message out to all of    │
│ subscribed"                      │ them (Exercise 3)                          │
└──────────────────────────────────┴───────────────────────────────────────────┘
"""


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  EXERCISE 1 — Bounded Blocking Queue (LeetCode 1188)                        ║
# ║  primitives: Lock + Condition(s) — NOT queue.Queue                          ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
Implement `BoundedBlockingQueue(capacity)` from scratch:

    enqueue(element)  -> blocks while the queue is at capacity, then adds
                         element to the back.
    dequeue()         -> blocks while the queue is empty, then removes and
                         returns the front element.
    size()            -> current number of elements (non-blocking).

This is LC 1188. The point of the exercise is PRIMER 1 — you may NOT use
`queue.Queue` internally. Build it from `threading.Lock` / `threading.Condition`.

    >>> q = BoundedBlockingQueue(2)
    >>> q.enqueue(1); q.enqueue(2)      # queue is now full (size 2)
    >>> q.enqueue(3)                    # BLOCKS until something is dequeued
    >>> q.dequeue()                     # 1  (unblocks the enqueue(3) above)

--- YOUR IMPLEMENTATION ---
"""

class BoundedBlockingQueue:
    def __init__(self, capacity):
        """
        Set up the fixed-size buffer and whatever Lock/Condition(s) you need
        to block enqueue() while full and dequeue() while empty.
        """
        # YOUR CODE HERE
        raise NotImplementedError

    def enqueue(self, element):
        """Block while the queue is full; then append `element`."""
        # YOUR CODE HERE
        raise NotImplementedError

    def dequeue(self):
        """Block while the queue is empty; then pop and return the front."""
        # YOUR CODE HERE
        raise NotImplementedError

    def size(self):
        """Return the current number of elements. Must not block."""
        # YOUR CODE HERE
        raise NotImplementedError


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  EXERCISE 2 — Log Pipeline with Graceful Shutdown                           ║
# ║  primitives: queue.Queue (or your BoundedBlockingQueue) + sentinels          ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
Build `LogPipeline(num_workers, process)`:
  - `run(records)` feeds every item in `records` (1 producer, run on whatever
    thread calls `run` — you may spawn an internal producer thread or feed
    directly, your call) through `num_workers` consumer threads.
  - Each worker calls `process(record)` exactly once per record it receives.
  - Every record is processed EXACTLY ONCE across all workers combined (no
    drops, no duplicates) — the work should be shared, not repeated.
  - `run()` returns only once EVERY record has been processed and EVERY
    worker thread has exited cleanly (via sentinel shutdown — PRIMER 2 — not
    by killing threads or polling with `join(timeout=...)`).

You may use `queue.Queue` (recommended — PRIMER 3) or your own
`BoundedBlockingQueue` from Exercise 1 as the shared channel.

    >>> results = []
    >>> pipeline = LogPipeline(num_workers=4, process=lambda rec: results.append(rec))
    >>> pipeline.run(records=list(range(100)))
    >>> sorted(results) == list(range(100))
    True

--- YOUR IMPLEMENTATION ---
"""

class LogPipeline:
    def __init__(self, num_workers, process):
        """
        `process` is a callable invoked once per record (may be called
        concurrently from different worker threads — if `process` itself
        touches shared state, that's the caller's problem, not yours).
        """
        # YOUR CODE HERE
        raise NotImplementedError

    def run(self, records):
        """
        Push every record from `records` through `num_workers` worker
        threads, then shut every worker down via sentinels and block until
        all of them have exited.
        """
        # YOUR CODE HERE
        raise NotImplementedError


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  EXERCISE 3 — Pub/Sub with Multiple Subscribers                             ║
# ║  primitives: Lock (to guard the subscriber list) + one Queue per subscriber  ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
Build `PubSub`:

    subscribe() -> registers a new subscriber and returns a handle (e.g. a
                   `queue.Queue`) that will receive every message published
                   from this point forward.
    publish(msg) -> delivers `msg` to every CURRENTLY registered subscriber
                    (a subscriber that hasn't subscribed yet must NOT see
                    messages published before it subscribed).

`subscribe()` and `publish()` will be called from multiple threads
concurrently — the subscriber list itself needs a lock. Fan-out (handing the
message to N subscriber queues) does not need every subscriber to be woken by
the same Condition — the cleanest approach is one independent `queue.Queue`
per subscriber; `publish` just calls `.put(msg)` on each one it holds a
reference to under the lock.

    >>> ps = PubSub()
    >>> q1 = ps.subscribe()
    >>> ps.publish("hello")
    >>> q1.get()
    'hello'

--- YOUR IMPLEMENTATION ---
"""

class PubSub:
    def __init__(self):
        # YOUR CODE HERE
        raise NotImplementedError

    def subscribe(self):
        """Register a new subscriber; return a handle that supports `.get()`
        and will receive every message published from now on."""
        # YOUR CODE HERE
        raise NotImplementedError

    def publish(self, msg):
        """Deliver `msg` to every subscriber currently registered."""
        # YOUR CODE HERE
        raise NotImplementedError


# ══════════════════════════════════════════════════════════════════════════════
#  SELF-TESTS — run the file to grade YOUR implementations.
#  Every check prints "[FAIL] ...: not implemented" until you fill in the body.
# ══════════════════════════════════════════════════════════════════════════════

def _run(threads):
    for t in threads:
        t.start()
    for t in threads:
        t.join()


def _run_catching(funcs):
    """
    Run each zero-arg callable in its own thread, join all of them, then
    re-raise the FIRST exception any of them raised (in the calling thread).

    Exceptions raised inside a `threading.Thread` target do NOT propagate to
    whoever called `.join()` — they just get printed to stderr and the thread
    dies silently. Since our EXERCISE skeletons `raise NotImplementedError`,
    without this helper every `_check_*` below would hang or falsely pass
    instead of reporting "not implemented". This is the plumbing that makes
    the try/except pattern work across threads.
    """
    errors = []
    errors_lock = threading.Lock()

    def wrap(fn):
        def _inner():
            try:
                fn()
            except BaseException as e:
                with errors_lock:
                    errors.append(e)
        return _inner

    threads = [threading.Thread(target=wrap(fn)) for fn in funcs]
    _run(threads)
    if errors:
        raise errors[0]


def _check_bounded_blocking_queue():
    try:
        capacity = 2
        n_items = 20
        bq = BoundedBlockingQueue(capacity)
        produced = list(range(n_items))
        consumed = []
        consumed_lock = threading.Lock()
        max_size = {"v": 0}
        stats_lock = threading.Lock()

        def note_size():
            s = bq.size()
            with stats_lock:
                if s > max_size["v"]:
                    max_size["v"] = s

        def producer():
            for x in produced:
                bq.enqueue(x)
                note_size()             # sample right after a push (near-peak)

        def consumer():
            for _ in range(n_items):
                note_size()             # sample right before a pop (near-peak)
                x = bq.dequeue()
                with consumed_lock:
                    consumed.append(x)

        _run_catching([producer, consumer])

        fifo_ok = consumed == produced
        cap_ok = max_size["v"] <= capacity
        ok = fifo_ok and cap_ok
        print(f"  [{'PASS' if ok else 'FAIL'}] BoundedBlockingQueue -> "
              f"FIFO order {fifo_ok}, max observed size {max_size['v']} (capacity {capacity})")
        return ok
    except NotImplementedError:
        print("  [FAIL] BoundedBlockingQueue: not implemented")
        return False


def _check_log_pipeline():
    try:
        n_records = 200
        num_workers = 4
        results = []
        results_lock = threading.Lock()

        def process(rec):
            with results_lock:
                results.append(rec)

        pipeline = LogPipeline(num_workers=num_workers, process=process)
        pipeline.run(records=list(range(n_records)))

        ok = sorted(results) == list(range(n_records))
        print(f"  [{'PASS' if ok else 'FAIL'}] LogPipeline -> "
              f"{len(results)}/{n_records} records processed, each exactly once: {ok}")
        return ok
    except NotImplementedError:
        print("  [FAIL] LogPipeline: not implemented")
        return False


def _check_pubsub():
    try:
        ps = PubSub()

        q1 = ps.subscribe()
        ps.publish("m1")
        q2 = ps.subscribe()             # subscribes AFTER m1 -> must not see it
        ps.publish("m2")
        q3 = ps.subscribe()              # subscribes AFTER m1, m2
        ps.publish("m3")

        def drain(q, n):
            items = []
            try:
                for _ in range(n):
                    items.append(q.get(timeout=2))
            except queue.Empty:
                pass
            return items

        r1 = drain(q1, 3)               # expect m1, m2, m3
        r2 = drain(q2, 2)               # expect m2, m3
        r3 = drain(q3, 1)               # expect m3

        ok = r1 == ["m1", "m2", "m3"] and r2 == ["m2", "m3"] and r3 == ["m3"]
        print(f"  [{'PASS' if ok else 'FAIL'}] PubSub -> sub1={r1} sub2={r2} sub3={r3}")
        return ok
    except NotImplementedError:
        print("  [FAIL] PubSub: not implemented")
        return False


def _run_demos():
    _demo_producer_consumer_stdlib_queue()


def _run_checks():
    print("\n=== SELF-TESTS (fill in the EXERCISE skeletons above to turn these green) ===")
    results = [
        _check_bounded_blocking_queue(),
        _check_log_pipeline(),
        _check_pubsub(),
    ]
    print(f"\n  {sum(results)}/{len(results)} checks passed.")


if __name__ == "__main__":
    _run_demos()
    _run_checks()
    print("\nNext: workbook_05 — deadlock & resource ordering "
          "(Dining Philosophers, Traffic Light), YOUR turn to solve.")
