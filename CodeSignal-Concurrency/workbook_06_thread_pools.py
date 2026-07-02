"""
================================================================================
CODESIGNAL CONCURRENCY — WORKBOOK 06: THREAD POOLS & EXECUTORS
================================================================================
Difficulty: Core interview topic (thread pool from scratch is a FAANG classic)
Model:      Python `threading` + stdlib `concurrent.futures`
Primitives: queue.Queue (the work queue), threading.Event (shutdown signal),
            concurrent.futures.ThreadPoolExecutor (the batteries-included option)

Maps to INTERVIEW.MD:
    Thread pool from scratch (10), worker-pool shutdown/cancellation (33),
    web crawler stages 1->2 (18)

HOW TO USE THIS FILE
--------------------
1. Read each PRIMER block.
2. Run the file to watch the `_demo_*` functions (they use the stdlib
   ThreadPoolExecutor, the "already solved" option):
       python3 CodeSignal-Concurrency/workbook_06_thread_pools.py
3. Each EXERCISE below is a SKELETON — signature + docstring + hints, no
   reference solution. Implement the body yourself. (See WB01 if you want a
   fully-worked model of the *style* these checks expect.)
4. The `_check_*` self-tests at the bottom grade YOUR implementation and print
   PASS/FAIL. Until you implement something, its check FAILS loudly — but
   the file still runs start-to-finish every time. That's by design: a broken
   or unfinished pool must never hang the whole workbook.

THE ONE IDEA
------------
A thread pool trades "one OS thread per task" (unbounded, expensive to spin up
and tear down, no backpressure) for "N long-lived worker threads pulling tasks
off a shared queue.Queue" (bounded concurrency, threads are reused, and
backpressure falls out for free if you cap the queue). Everything below is
really just three questions: how does work get IN, how do results get OUT, and
how do workers know when to STOP.
================================================================================
"""

import queue
import threading
import time
import concurrent.futures



# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  PRIMER 0: WHY NOT JUST threading.Thread() PER TASK?                         ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
Spawning a brand-new OS thread for every incoming task looks simple and is a
trap under real load:

  - CREATION COST. Each `Thread()` asks the OS for a stack + kernel bookkeeping.
    Cheap once, ruinous at 10,000 tasks/sec.
  - UNBOUNDED CONCURRENCY. Nothing stops 50,000 threads existing at once if
    50,000 tasks arrive in a burst -> memory blow-up, brutal context-switch
    thrashing, and on some systems you'll flat out hit the OS thread limit and
    crash.
  - NO BACKPRESSURE. A producer that never blocks can outrun every consumer
    forever. There's no natural "slow down" signal.
  - NO REUSE. A thread that finishes its one task is just garbage; the next
    task pays the full creation cost again.

THE FIX: start a FIXED number of worker threads ONCE. Each one loops forever,
pulling `(fn, args)` off a single shared `queue.Queue()`. "Submitting work" now
means `q.put(...)`, not `Thread(...).start()`. You get bounded concurrency
(exactly N tasks run at a time), thread reuse (zero repeated spawn cost), and
if you cap the queue's `maxsize`, `put()` itself blocks when it's full —
backpressure for free.
"""

# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  PRIMER 1: GETTING RESULTS OUT — THE FUTURE HANDLE                           ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
Worker threads run detached from the caller, so `submit(fn)` can't just
return `fn`'s value — `fn` hasn't run yet when `submit()` returns control.
The standard trick is a small "future" object:

  1. `submit()` builds a future: a container with an empty value/exception slot
     and a `threading.Event` (unset).
  2. It packages `(fn, args, kwargs, future)` and puts it on the shared queue,
     then returns the future immediately — the caller is NOT blocked.
  3. A worker thread pops the tuple, calls `fn(*args, **kwargs)`, and stores
     the return value on the future — OR, if `fn` raised, stores the
     EXCEPTION instead (never swallow it silently).
  4. The worker sets the future's Event. `future.result(timeout=None)` is just
     `event.wait(timeout)` followed by "return the stored value, or raise the
     stored exception."

This is exactly what `concurrent.futures.Future` does internally — you're
about to hand-roll a tiny version of it in Exercise 1.
"""

# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  PRIMER 2: GRACEFUL SHUTDOWN — SENTINEL VS. EVENT+DRAIN                      ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
Two idioms. Pick based on whether you want an ORDERLY drain or an IMMEDIATE
cancel.

1. SENTINEL PER WORKER (orderly drain — what CPython's own executor does).
   Push ONE sentinel value (e.g. `None`, or a dedicated `_SHUTDOWN` object so it
   can never collide with real data) onto the queue for EACH worker. Every
   worker's loop is:
       item = q.get()
       if item is SENTINEL: break
       ...run item...
   Because each worker consumes exactly one sentinel, all N exit, and every
   task that was queued BEFORE the sentinels still runs to completion.
   `shutdown(wait=True)` then just joins the worker threads.

2. SHUTDOWN EVENT + DRAIN-OR-CANCEL (immediate cancellation). Flip a
   `threading.Event` (or a bool guarded by a lock) the instant `shutdown_now()`
   is called, so `submit()` starts REJECTING new work outright. Then drain
   whatever is STILL sitting in the queue — anything no worker has popped
   yet — and mark each one cancelled instead of running it. Anything a
   worker already popped keeps running to completion (you cannot force-kill a
   Python thread mid-task, and you shouldn't want to — it could leave
   shared state half-updated). Finally push sentinels to wake any worker
   blocked on an empty queue, and join.

EITHER WAY: always `join()` the worker threads before returning from shutdown.
A caller who gets control back should be able to assume no worker is still
touching shared state.
"""

# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  PRIMER 3: THE BATTERIES-INCLUDED OPTION — ThreadPoolExecutor                ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
`concurrent.futures.ThreadPoolExecutor` is exactly PRIMER 0-2 already built,
tested, and battle-hardened — reach for it in real code; build your own only
when asked to (interviews) or when you need behavior it doesn't offer.

  - `executor.submit(fn, *args)` -> returns a real `Future` immediately.
    Collect results OUT OF ORDER, as they finish, with
    `concurrent.futures.as_completed(futures)`.
  - `executor.map(fn, iterable)` -> returns results in INPUT order (it does the
    index-tracking from Exercise 3 for you).
  - Always use it as a context manager: `with ThreadPoolExecutor(...) as ex:`.
    `__exit__` calls `shutdown(wait=True)` for you — no leaked threads.

Know how to build one by hand (that's the rest of this workbook) AND know this
exists, so production code doesn't reinvent it.
"""


def _demo_executor_submit_as_completed():
    """>>> _demo_executor_submit_as_completed()"""
    print("\n--- DEMO: ThreadPoolExecutor.submit() + as_completed() (arrival order) ---")

    def work(i):
        time.sleep(0.05 * (5 - i))   # later i's finish FIRST
        return i * i

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
        future_to_i = {ex.submit(work, i): i for i in range(5)}
        arrival = []
        for fut in concurrent.futures.as_completed(future_to_i):
            i = future_to_i[fut]
            arrival.append((i, fut.result()))
    print(f"  arrival order (NOT input order): {arrival}")


def _demo_executor_map():
    """>>> _demo_executor_map()"""
    print("\n--- DEMO: ThreadPoolExecutor.map() (input order, always) ---")

    def work(i):
        time.sleep(0.05 * (5 - i))   # same skewed timing as above...
        return i * i

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
        results = list(ex.map(work, range(5)))   # ...but map() preserves order
    print(f"  map() results (input order preserved): {results}")



# ══════════════════════════════════════════════════════════════════════════════
#  CHEAT SHEET — hand-rolled pool vs. the stdlib option
# ══════════════════════════════════════════════════════════════════════════════
"""
  Need                    -> Reach for
  ----------------------------------------------------------------------------
  Production code         -> concurrent.futures.ThreadPoolExecutor
  Results, out of order   -> executor.submit(fn, x) + as_completed(futures)
  Results, input order    -> executor.map(fn, items)  (or your own index-tracking)
  "Build it yourself"     -> N workers + queue.Queue + a tiny Future  (Exercise 1)
  Orderly shutdown        -> one sentinel per worker, then join()
  Immediate cancel        -> shutdown Event + drain unstarted queue items (Exercise 2)
"""

# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  EXERCISE 1 — Thread Pool From Scratch   (INTERVIEW.MD #10)                  ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
Build a fixed-size thread pool with NO reference solution to copy — this is
the single most common "concurrency from scratch" interview question.

    >>> pool = ThreadPool(4)
    >>> f1 = pool.submit(lambda: 2 + 2)
    >>> f1.result()          # blocks until a worker runs the task
    4
    >>> pool.shutdown(wait=True)   # joins every worker thread

Requirements:
  - `ThreadPool(num_workers)` starts `num_workers` worker threads immediately.
  - `submit(fn, *args, **kwargs)` returns a Future-LIKE object (yours, not
    concurrent.futures.Future) whose `.result(timeout=None)` blocks until the
    task has run and then returns its value — or re-raises its exception.
    `submit()` itself must NOT block waiting for the task to finish.
  - `shutdown(wait=True)` stops the pool. If `wait` is True, block until every
    already-queued task has run and every worker thread has exited.

HINTS (see PRIMER 1 and PRIMER 2 — no code, just the shape):
  - A tiny `_Future`: one slot for the result, one for an exception, one
    `threading.Event`, set by whichever worker finishes the task.
  - Work items flow through one shared `queue.Queue()`.
  - Shutdown = push one sentinel per worker, each worker breaks its loop on
    seeing its sentinel, then `shutdown()` joins every worker thread.
"""



class ThreadPool:
    """Fixed-size pool of `num_workers` threads pulling work off a queue.Queue."""

    def __init__(self, num_workers):
        """
        Start `num_workers` daemon-or-not worker threads. Each should loop:
        pop a work item, run it, store the result/exception on its future,
        set the future's event, repeat -- until it sees a shutdown sentinel.
        """
        # YOUR CODE HERE
        raise NotImplementedError

    def submit(self, fn, *args, **kwargs):
        """
        Enqueue (fn, args, kwargs) for a worker to run and return a
        Future-like object immediately (do not block here). The returned
        object must expose `.result(timeout=None)`.
        """
        # YOUR CODE HERE
        raise NotImplementedError

    def shutdown(self, wait=True):
        """
        Stop accepting the idea of new work arriving forever: signal every
        worker to exit once its currently-queued work is drained (see PRIMER 2
        -- sentinel approach). If `wait`, block until all worker threads have
        actually terminated before returning.
        """
        # YOUR CODE HERE
        raise NotImplementedError


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  EXERCISE 2 — Worker Pool: Graceful Shutdown + Cancel  (#33)                 ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
A pool that supports an EMERGENCY stop, distinct from ThreadPool's orderly
`shutdown()`: `shutdown_now()` must stop taking new work immediately, let
whatever a worker is MID-TASK on finish normally, but CANCEL — never run —
anything still waiting in the queue. Track both counts.

    >>> pool = WorkerPool(4)
    >>> [pool.submit(job) for job in many_jobs]     # flood it
    >>> pool.shutdown_now()                         # called mid-stream
    >>> pool.completed_count + pool.cancelled_count == number_of_accepted_jobs
    True

Requirements:
  - `WorkerPool(num_workers)` starts `num_workers` worker threads.
  - `submit(fn, *args, **kwargs)` returns True if the job was accepted (queued
    or already running), False if the pool is already shutting down and the
    job was rejected outright (never queued at all).
  - `shutdown_now()`: (a) immediately stops accepting new submissions,
    (b) any job a worker has ALREADY popped off the queue keeps running to
    completion and increments `completed_count`, (c) any job still sitting in
    the queue, unstarted, is dropped and increments `cancelled_count` instead
    of running, (d) blocks until every worker thread has exited.
  - `completed_count` / `cancelled_count`: thread-safe counters, readable any
    time (including mid-flight).

HINTS (see PRIMER 2, "EVENT + DRAIN-OR-CANCEL" — no code, just the shape):
  - A `threading.Event` (or lock-guarded bool) flips the instant
    `shutdown_now()` runs; `submit()` checks it BEFORE putting anything on
    the queue.
  - `shutdown_now()` also needs to reach into the queue and drain whatever's
    left unstarted (e.g. loop `q.get_nowait()` until `queue.Empty`), marking
    each drained item cancelled -- then push sentinels so workers blocked on
    an empty queue wake up and exit instead of hanging forever.
  - Guard the two counters with a `Lock` (`+= 1` is not atomic — WB01 PRIMER 0).
"""



class WorkerPool:
    """Like ThreadPool, but with an emergency `shutdown_now()` + cancel counts."""

    def __init__(self, num_workers):
        """Start `num_workers` worker threads and zero out the two counters."""
        # YOUR CODE HERE
        raise NotImplementedError

    def submit(self, fn, *args, **kwargs):
        """
        Return True if accepted (queued for a worker to run), False if
        rejected because the pool is already shutting down.
        """
        # YOUR CODE HERE
        raise NotImplementedError

    def shutdown_now(self):
        """
        Stop accepting new submissions immediately. Let in-flight jobs (a
        worker already popped them) finish and count as completed. Cancel
        every job still queued and unstarted. Block until all worker threads
        have exited.
        """
        # YOUR CODE HERE
        raise NotImplementedError

    @property
    def completed_count(self):
        """Thread-safe read of how many jobs actually ran to completion."""
        # YOUR CODE HERE
        raise NotImplementedError

    @property
    def cancelled_count(self):
        """Thread-safe read of how many queued jobs were dropped, unrun."""
        # YOUR CODE HERE
        raise NotImplementedError


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  EXERCISE 3 — Parallel Map, Order-Preserving  (crawler #18)                  ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
The shape behind "stage 1 fetches URLs, stage 2 parses them concurrently, but
the final report must line up with the original URL list": apply `fn` to every
item using a BOUNDED number of worker threads, but return results in the SAME
ORDER as `items` — regardless of which task happens to finish first.

    >>> parallel_map(lambda x: x * x, [1, 2, 3, 4, 5], max_workers=3)
    [1, 4, 9, 16, 25]        # always in input order, even if item 5 finishes first

HINTS (no code, just the shape):
  - Easiest correct route: `concurrent.futures.ThreadPoolExecutor.map` already
    does exactly this — read PRIMER 3 again.
  - If you'd rather use your own `ThreadPool` from Exercise 1: submit every
    item WITH its index, collect futures in a list positionally, then build
    the result list by reading `results[i] = futures[i].result()` in order —
    the ORDER you submit/collect in doesn't have to match the order tasks
    finish in, only the order you read results back in.
"""



def parallel_map(fn, items, max_workers=4):
    """
    Apply `fn` to every element of `items` using at most `max_workers`
    concurrent workers. Return a list of results in the SAME ORDER as
    `items`, even though tasks may complete in a different order.
    """
    # YOUR CODE HERE
    raise NotImplementedError


# ══════════════════════════════════════════════════════════════════════════════
#  SELF-TESTS — run the file to grade YOUR implementations.
#  A watchdog (join(timeout=...)) guards every check so a hung/broken pool
#  can never hang this file -- it always prints results and exits.
# ══════════════════════════════════════════════════════════════════════════════

def _run_with_timeout(fn, timeout):
    """
    Run fn() on a background daemon thread; wait up to `timeout` seconds.
    Returns (completed, result). If fn raised, the exception is re-raised
    HERE (in the calling/checking thread) once we know it actually finished.
    If it timed out, returns (False, None) -- the leaked daemon thread cannot
    keep the process alive or block the rest of this file from running.
    """
    box = {}

    def _target():
        try:
            box["result"] = fn()
        except BaseException as exc:      # noqa: BLE001 -- must catch everything
            box["error"] = exc

    t = threading.Thread(target=_target, daemon=True)
    t.start()
    t.join(timeout)
    if t.is_alive():
        return False, None
    if "error" in box:
        raise box["error"]
    return True, box.get("result")


def _check_thread_pool_scratch():
    print("\n[Exercise 1] ThreadPool from scratch")
    try:
        def body():
            baseline = threading.active_count()
            pool = ThreadPool(4)
            n = 50
            seen_threads = set()
            seen_lock = threading.Lock()

            def task(i):
                with seen_lock:
                    seen_threads.add(threading.get_ident())
                time.sleep(0.01)
                return i * i

            futures = [pool.submit(task, i) for i in range(n)]
            results = [f.result(timeout=5) for f in futures]
            pool.shutdown(wait=True)
            after = threading.active_count()
            return results, seen_threads, baseline, after

        completed, payload = _run_with_timeout(body, timeout=10.0)
        if not completed:
            print("  [FAIL] timed out -- possible deadlock in submit()/result()/shutdown()")
            return False

        results, seen_threads, baseline, after = payload
        expected = [i * i for i in range(50)]
        results_ok = results == expected
        spread_ok = len(seen_threads) > 1
        joined_ok = after <= baseline
        ok = results_ok and spread_ok and joined_ok
        print(f"  [{'PASS' if ok else 'FAIL'}] results_correct={results_ok} "
              f"workers_used={len(seen_threads)} all_threads_joined={joined_ok}")
        return ok
    except NotImplementedError:
        print("  [FAIL] not implemented yet")
        return False
    except Exception as exc:
        print(f"  [FAIL] raised {type(exc).__name__}: {exc}")
        return False


def _check_worker_pool_cancel():
    print("\n[Exercise 2] WorkerPool graceful shutdown + cancellation")
    try:
        def body():
            baseline = threading.active_count()
            pool = WorkerPool(4)
            n = 200
            accepted = 0

            def job():
                time.sleep(0.01)

            for i in range(n):
                if pool.submit(job):
                    accepted += 1
                if i == n // 2:               # flood, then pull the plug mid-stream
                    pool.shutdown_now()

            after = threading.active_count()
            return accepted, pool.completed_count, pool.cancelled_count, baseline, after

        completed, payload = _run_with_timeout(body, timeout=10.0)
        if not completed:
            print("  [FAIL] timed out -- possible deadlock in submit()/shutdown_now()")
            return False

        accepted, done, cancelled, baseline, after = payload
        accounted_ok = (done + cancelled) == accepted
        joined_ok = after <= baseline
        ok = accounted_ok and joined_ok
        print(f"  [{'PASS' if ok else 'FAIL'}] accepted={accepted} completed={done} "
              f"cancelled={cancelled} (completed+cancelled==accepted: {accounted_ok}) "
              f"all_threads_joined={joined_ok}")
        return ok
    except NotImplementedError:
        print("  [FAIL] not implemented yet")
        return False
    except Exception as exc:
        print(f"  [FAIL] raised {type(exc).__name__}: {exc}")
        return False


def _check_parallel_map():
    print("\n[Exercise 3] parallel_map preserves input order")
    try:
        def body():
            n = 8

            def fn(i):
                # earlier items sleep LONGER, so a naive "collect as they
                # finish" implementation would scramble the order if it
                # weren't explicitly fixed up.
                time.sleep(0.02 * (n - i))
                return i * i

            return parallel_map(fn, list(range(n)), max_workers=4)

        completed, results = _run_with_timeout(body, timeout=10.0)
        if not completed:
            print("  [FAIL] timed out -- possible deadlock in parallel_map()")
            return False

        expected = [i * i for i in range(8)]
        ok = results == expected
        print(f"  [{'PASS' if ok else 'FAIL'}] got {results}  want {expected}")
        return ok
    except NotImplementedError:
        print("  [FAIL] not implemented yet")
        return False
    except Exception as exc:
        print(f"  [FAIL] raised {type(exc).__name__}: {exc}")
        return False


def _run_demos():
    _demo_executor_submit_as_completed()
    _demo_executor_map()


def _run_checks():
    print("\n=== SELF-TESTS (grade YOUR implementations; all FAIL until solved) ===")
    results = [
        _check_thread_pool_scratch(),
        _check_worker_pool_cancel(),
        _check_parallel_map(),
    ]
    print(f"\n  {sum(results)}/{len(results)} checks passed.")


if __name__ == "__main__":
    _run_demos()
    _run_checks()
    print("\nNext: workbook_07_rate_limiters.py -- token buckets & sliding windows.")

