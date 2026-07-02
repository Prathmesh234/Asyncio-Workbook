"""
================================================================================
CODESIGNAL CONCURRENCY — WORKBOOK 03: BUILD THE PRIMITIVES FROM SCRATCH
================================================================================
Difficulty: Advanced (this is the "implement X using only Y" interview format)
Model:      Python `threading` (preemptive, multi-threaded) — NOT asyncio
Primitives you get to REBUILD: Semaphore, Barrier, RWLock, Future, Once
Foundation ALLOWED:            threading.Lock + threading.Condition ONLY

Maps to INTERVIEW.MD:
    Semaphore-from-Condition (12), RW-lock (11), Barrier (13),
    Future (14), Once (17)

HOW TO USE THIS FILE
--------------------
1. Read PRIMER 0 first — it is the ONE pattern every exercise below reuses.
2. Read `_demo_countdown_latch`, then RUN the file to watch it:
       python3 "CodeSignal-Concurrency/workbook_03_build_primitives.py"
3. Each EXERCISE below is a SKELETON (signature + docstring + `# YOUR CODE
   HERE` + `raise NotImplementedError`). Unlike Workbook 01, there is NO
   reference solution here — build each primitive yourself using only
   threading.Lock and threading.Condition (no cheating with the real
   threading.Semaphore/Barrier under the hood).
4. The `_check_*` self-tests at the bottom print [PASS]/[FAIL]. Until you
   implement an exercise, its check prints `[FAIL] ...: not implemented` —
   that's expected. Getting every check to [PASS] is the goal.

WHY THIS MATTERS FOR THE INTERVIEW
-----------------------------------
"Use threading.Semaphore" is a one-liner. "Implement a Semaphore using only a
Lock" tests whether you understand what a Semaphore actually IS: a protected
integer plus a wait queue. Every primitive in the standard library boils down
to that shape. Once you've built one from scratch, you stop treating
Lock / Condition / Semaphore / Event / Barrier as unrelated black boxes — they
are all the same three ingredients: shared state, a lock, and a wait/notify
queue.
================================================================================
"""

import threading
import time


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  PRIMER 0: CONDITION = LOCK + A WAIT/NOTIFY QUEUE                             ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
`threading.Condition(lock=None)` is TWO things bolted together:
  1. a Lock (its own, or one you pass in) that you hold via `with cond:`
  2. a queue of threads parked in `cond.wait()`, woken by `cond.notify()`
     (wakes one waiter) or `cond.notify_all()` (wakes everyone)

`cond.wait()` is special: it ATOMICALLY (a) releases the lock and (b) parks
the thread, so nothing can sneak a state change in between "I checked the
predicate" and "I started waiting" (the classic lost-wakeup bug). When woken,
it re-acquires the lock before returning — you never have to re-lock by hand.

THE ONE PATTERN EVERY PRIMITIVE IN THIS FILE IS BUILT FROM:

    # a thread that must WAIT for some condition on shared state:
    with cond:
        while not predicate(state):     # ALWAYS while, never if (see WB01)
            cond.wait()
        ...mutate state now that the predicate holds...
        cond.notify()      # or notify_all() — wake whoever cares that state changed

Read each exercise below through this lens:

    Semaphore -> state is "permits available"        predicate: count > 0
    Barrier   -> state is "everyone has arrived"      predicate: arrived == parties
    RWLock    -> state is "no writer active/queued"   (readers' predicate)
                 state is "no readers/writer active"  (writers' predicate)
    Future    -> state is "resolved yet?"              predicate: done is True
    Once      -> state is "has fn run yet?"            predicate: done is True

If you can write that `while / wait / notify` skeleton from memory, you can
build every primitive below — they only differ in what `state` and
`predicate(state)` mean.
"""

def _demo_countdown_latch():
    """
    >>> _demo_countdown_latch()
    A CountDownLatch(n): N calls to count_down() must all happen before any
    thread blocked in wait() is released. This is the WHOLE pattern above in
    about 10 lines — study it before touching the exercises.
    """
    print("\n--- DEMO: a CountDownLatch built from Condition (the pattern in miniature) ---")

    class CountDownLatch:
        def __init__(self, count):
            self._count = count
            self._cond = threading.Condition()

        def count_down(self):
            with self._cond:
                self._count -= 1
                if self._count <= 0:
                    self._cond.notify_all()      # wake every waiter once we hit 0

        def wait(self):
            with self._cond:
                while self._count > 0:           # predicate: "done counting down?"
                    self._cond.wait()

    latch = CountDownLatch(3)
    order = []
    order_lock = threading.Lock()

    def waiter(i):
        latch.wait()
        with order_lock:
            order.append(f"waiter-{i}-released")

    def worker(i):
        time.sleep(0.02 * i)
        with order_lock:
            order.append(f"worker-{i}-done")
        latch.count_down()

    threads = [threading.Thread(target=waiter, args=(i,)) for i in range(2)]
    threads += [threading.Thread(target=worker, args=(i,)) for i in range(3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    print("  " + " | ".join(order))
    print("  (both waiters released only AFTER all 3 workers counted down)")


# ══════════════════════════════════════════════════════════════════════════════
#  BUILD MAP — what state + predicate does each exercise below need?
# ══════════════════════════════════════════════════════════════════════════════
"""
┌───────────┬───────────────────────────────┬─────────────────────────────────┐
│ Exercise   │ Shared state                  │ Predicate you wait on            │
├───────────┼───────────────────────────────┼─────────────────────────────────┤
│ Semaphore  │ count                         │ count > 0        (acquire)       │
│ Barrier    │ arrived, generation           │ arrived == parties (or new gen)  │
│ RWLock     │ readers, writer_active,       │ readers: no writer active/queued │
│            │ writers_waiting               │ writers: no readers/writer active│
│ Future     │ done, value/exc               │ done is True                     │
│ Once       │ done, cached result           │ done is True (double-checked)    │
└───────────┴───────────────────────────────┴─────────────────────────────────┘
"""


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  EXERCISE 1 — Semaphore from scratch   (INTERVIEW.MD #12)                    ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
Build a counting semaphore using ONLY threading.Lock + threading.Condition
(do NOT use threading.Semaphore/BoundedSemaphore internally).

    >>> sem = MySemaphore(2)
    >>> # up to 2 threads may hold it concurrently; a 3rd blocks in acquire()
    >>> # until one of the first 2 calls release()

acquire(): block WHILE the internal permit count == 0, then take one permit.
release(): give back one permit and wake a waiter.

Approach: one Condition. acquire() is the textbook
`with cond: while not predicate(): cond.wait()` shape where the predicate is
`count > 0`; on the way out, decrement count. release() takes the lock,
increments count, and calls notify() (one permit became available, one
waiter is enough).
"""

class MySemaphore:
    def __init__(self, initial: int):
        # YOUR CODE HERE
        raise NotImplementedError

    def acquire(self):
        # YOUR CODE HERE
        raise NotImplementedError

    def release(self):
        # YOUR CODE HERE
        raise NotImplementedError

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  EXERCISE 2 — Barrier from scratch   (INTERVIEW.MD #13)                      ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
Build a reusable barrier using ONLY Lock/Condition (do NOT use
threading.Barrier internally).

    >>> b = MyBarrier(3)
    >>> # each of 3 threads calls b.wait(); NONE returns until all 3 have
    >>> # called it, then all 3 are released together — and the barrier
    >>> # resets so it can be used again for a second "phase"

Approach: a Condition guarding a count of "arrived" threads (plus a
"generation" counter so a thread that arrives for phase 2 doesn't get
confused with phase 1's wakeup — this is what makes the barrier REUSABLE,
not just single-shot like the countdown latch in the primer).

    with cond:
        my_generation = generation
        arrived += 1
        if arrived == parties:
            arrived = 0                # reset for next round
            generation += 1            # advance so waiters know it's a NEW round
            cond.notify_all()
        else:
            while generation == my_generation:
                cond.wait()
"""

class MyBarrier:
    def __init__(self, parties: int):
        # YOUR CODE HERE
        raise NotImplementedError

    def wait(self):
        # YOUR CODE HERE
        raise NotImplementedError


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  EXERCISE 3 — Read/Write Lock with WRITER PRIORITY   (INTERVIEW.MD #11)      ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
Build a reader/writer lock: many readers may hold it AT THE SAME TIME, OR
exactly one writer holds it exclusively — never both. Critically, it must
give WRITERS PRIORITY: once a writer is waiting, NEW readers must block (even
though existing readers already in the critical section finish normally).
This is what prevents writer starvation under a steady stream of readers.

    >>> rw = RWLock()
    >>> rw.acquire_read(); ...; rw.release_read()      # many readers OK at once
    >>> rw.acquire_write(); ...; rw.release_write()    # exclusive, blocks readers

Approach: one Condition + a few counters:
    _readers          -- how many readers currently hold the lock
    _writer_active    -- bool, a writer currently holds the lock
    _writers_waiting  -- how many writers are queued (THE priority knob —
                         readers must check this, not just `_writer_active`)

acquire_read():  block WHILE `_writer_active or _writers_waiting > 0`,
                 then `_readers += 1`.
release_read():  `_readers -= 1`; notify_all() if it hit 0 (a writer may
                 now be able to proceed).
acquire_write(): `_writers_waiting += 1`; block WHILE
                 `_writer_active or _readers > 0`; then
                 `_writers_waiting -= 1; _writer_active = True`.
release_write(): `_writer_active = False`; notify_all() (readers AND writers
                 may now be able to proceed).
"""

class RWLock:
    def __init__(self):
        # YOUR CODE HERE
        raise NotImplementedError

    def acquire_read(self):
        # YOUR CODE HERE
        raise NotImplementedError

    def release_read(self):
        # YOUR CODE HERE
        raise NotImplementedError

    def acquire_write(self):
        # YOUR CODE HERE
        raise NotImplementedError

    def release_write(self):
        # YOUR CODE HERE
        raise NotImplementedError


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  EXERCISE 4 — Future / Promise   (INTERVIEW.MD #14)                          ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
A one-shot container for "a value that will exist later" — the primitive
behind thread-pool `submit()`, RPC clients, and (conceptually) `await`.

    >>> f = Future()
    >>> # thread A, eventually:  f.set_result(42)
    >>> f.result()               # thread B: blocks, then returns 42

set_result(value):    resolve successfully with `value` (exactly once —
                       calling it twice, or after set_exception, is a misuse
                       you don't need to guard against here).
set_exception(exc):   resolve with an exception object `exc`.
result(timeout=None): block until resolved, then return `value` OR re-raise
                       `exc`. If `timeout` elapses first, raise TimeoutError.

Approach: build on a threading.Event (or a Condition, if you'd rather
practice that instead) as the "done" signal. Store `_value` / `_exc` / a done
flag. set_result/set_exception stash the outcome and set the event; result()
waits on the event (respecting `timeout`), then either returns `_value` or
does `raise self._exc`.
"""

class Future:
    def __init__(self):
        # YOUR CODE HERE
        raise NotImplementedError

    def set_result(self, value):
        # YOUR CODE HERE
        raise NotImplementedError

    def set_exception(self, exc):
        # YOUR CODE HERE
        raise NotImplementedError

    def result(self, timeout=None):
        # YOUR CODE HERE
        raise NotImplementedError


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  EXERCISE 5 — Once / lazy one-time init   (INTERVIEW.MD #17)                 ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
Run a given zero-arg function EXACTLY ONCE no matter how many threads call
`do(fn)` concurrently — every caller (racing or arriving later) gets back the
SAME cached return value. Classic lazy-singleton / "init this exactly once"
primitive (Go's sync.Once, C++'s std::call_once).

    >>> once = Once()
    >>> once.do(expensive_init)   # thread A: runs expensive_init(), caches it
    >>> once.do(expensive_init)   # thread B (racing OR later): does NOT
    >>>                           # re-run it — returns the cached result

Approach: a `_done` flag, a cached `_result`, and a Lock. Classic
DOUBLE-CHECKED LOCKING:

    if not self._done:                  # fast unlocked PEEK (perf only)
        with self._lock:
            if not self._done:          # check AGAIN — someone may have
                                         # finished it while we waited for
                                         # the lock
                self._result = fn()
                self._done = True
    return self._result

The outer unlocked check is only a performance shortcut for the common case
(already initialized) — it must NEVER be the only check; the one taken while
holding the lock is what's actually load-bearing.
"""

class Once:
    def __init__(self):
        # YOUR CODE HERE
        raise NotImplementedError

    def do(self, fn):
        # YOUR CODE HERE
        raise NotImplementedError


# ══════════════════════════════════════════════════════════════════════════════
#  SELF-TESTS — run the file to grade YOUR implementations.
#  Unimplemented exercises print [FAIL] ...: not implemented — that's expected
#  until you fill them in.
# ══════════════════════════════════════════════════════════════════════════════

def _run_safe(fns):
    """
    Run each zero-arg callable in its own thread, join all of them, then
    re-raise the FIRST exception any of them hit (in the calling thread) so a
    `_check_*` can catch it with a plain try/except and print a clean [FAIL]
    instead of an unhandled-exception traceback leaking from a background
    thread.
    """
    errors = []
    err_lock = threading.Lock()

    def wrap(fn):
        try:
            fn()
        except Exception as e:
            with err_lock:
                errors.append(e)

    threads = [threading.Thread(target=wrap, args=(fn,)) for fn in fns]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    if errors:
        raise errors[0]


def _check_semaphore():
    try:
        sem = MySemaphore(2)
        active = {"n": 0, "max": 0}
        lock = threading.Lock()

        def worker():
            sem.acquire()
            with lock:
                active["n"] += 1
                active["max"] = max(active["max"], active["n"])
            time.sleep(0.05)
            with lock:
                active["n"] -= 1
            sem.release()

        _run_safe([worker for _ in range(6)])
        ok = active["max"] == 2
        print(f"  [{'PASS' if ok else 'FAIL'}] MySemaphore -> peak concurrent {active['max']} (want 2)")
        return ok
    except Exception as e:
        print(f"  [FAIL] MySemaphore: not implemented ({e})")
        return False


def _check_barrier():
    try:
        barrier = MyBarrier(3)
        log = []
        log_lock = threading.Lock()

        def worker(i):
            for phase in range(2):
                time.sleep(0.01 * i)
                with log_lock:
                    log.append(f"t{i}-arrive-p{phase}")
                barrier.wait()
                with log_lock:
                    log.append(f"t{i}-pass-p{phase}")

        _run_safe([lambda i=i: worker(i) for i in range(3)])

        ok = True
        for phase in range(2):
            arrive_idxs = [i for i, s in enumerate(log) if s.endswith(f"arrive-p{phase}")]
            pass_idxs = [i for i, s in enumerate(log) if s.endswith(f"pass-p{phase}")]
            ok &= len(arrive_idxs) == 3 and len(pass_idxs) == 3 and max(arrive_idxs) < min(pass_idxs)
        print(f"  [{'PASS' if ok else 'FAIL'}] MyBarrier -> both phases held the line for all 3 threads")
        return bool(ok)
    except Exception as e:
        print(f"  [FAIL] MyBarrier: not implemented ({e})")
        return False


def _check_rwlock():
    try:
        rw = RWLock()

        # (1) multiple readers concurrently
        active_r = {"n": 0, "max": 0}
        r_lock = threading.Lock()

        def reader():
            rw.acquire_read()
            with r_lock:
                active_r["n"] += 1
                active_r["max"] = max(active_r["max"], active_r["n"])
            time.sleep(0.05)
            with r_lock:
                active_r["n"] -= 1
            rw.release_read()

        _run_safe([reader for _ in range(5)])
        ok1 = active_r["max"] >= 3

        # (2) writers are mutually exclusive
        active_w = {"n": 0, "max": 0}
        w_lock = threading.Lock()

        def writer():
            rw.acquire_write()
            with w_lock:
                active_w["n"] += 1
                active_w["max"] = max(active_w["max"], active_w["n"])
            time.sleep(0.02)
            with w_lock:
                active_w["n"] -= 1
            rw.release_write()

        _run_safe([writer for _ in range(5)])
        ok2 = active_w["max"] == 1

        # (3) a WAITING writer blocks new readers (no writer starvation)
        events = []
        ev_lock = threading.Lock()

        def log(msg):
            with ev_lock:
                events.append(msg)

        def long_reader():
            rw.acquire_read()
            log("r1-acquired")
            time.sleep(0.15)
            log("r1-release")
            rw.release_read()

        def waiting_writer():
            time.sleep(0.02)           # ensure r1 has already acquired
            log("w-request")
            rw.acquire_write()
            log("w-acquired")
            time.sleep(0.02)
            log("w-release")
            rw.release_write()

        def late_reader():
            time.sleep(0.06)           # arrives after w-request, before r1 releases
            log("r2-request")
            rw.acquire_read()
            log("r2-acquired")
            rw.release_read()

        _run_safe([long_reader, waiting_writer, late_reader])
        ok3 = events.index("r2-acquired") > events.index("w-release")

        ok = ok1 and ok2 and ok3
        print(f"  [{'PASS' if ok else 'FAIL'}] RWLock -> concurrent readers max={active_r['max']} "
              f"(>=3), concurrent writers max={active_w['max']} (==1), "
              f"writer-priority held={ok3}")
        return ok
    except Exception as e:
        print(f"  [FAIL] RWLock: not implemented ({e})")
        return False


def _check_future():
    try:
        fut = Future()
        result_holder = {}

        def resolver():
            time.sleep(0.05)
            fut.set_result(42)

        def waiter():
            result_holder["value"] = fut.result()

        _run_safe([resolver, waiter])
        ok1 = result_holder.get("value") == 42

        fut2 = Future()
        exc_holder = {}

        def resolver_err():
            time.sleep(0.02)
            fut2.set_exception(ValueError("boom"))

        def waiter_err():
            try:
                fut2.result()
            except ValueError:
                exc_holder["caught"] = True

        _run_safe([resolver_err, waiter_err])
        ok2 = exc_holder.get("caught") is True

        ok = ok1 and ok2
        print(f"  [{'PASS' if ok else 'FAIL'}] Future -> result()={result_holder.get('value')} "
              f"(want 42), exception path {'ok' if ok2 else 'FAIL'}")
        return ok
    except Exception as e:
        print(f"  [FAIL] Future: not implemented ({e})")
        return False


def _check_once():
    try:
        once = Once()
        call_count = {"n": 0}
        count_lock = threading.Lock()
        results = []
        results_lock = threading.Lock()

        def slow_init():
            with count_lock:
                call_count["n"] += 1
            time.sleep(0.02)
            return "initialized"

        def caller():
            r = once.do(slow_init)
            with results_lock:
                results.append(r)

        _run_safe([caller for _ in range(20)])
        ok = call_count["n"] == 1 and len(results) == 20 and all(r == "initialized" for r in results)
        print(f"  [{'PASS' if ok else 'FAIL'}] Once -> fn ran {call_count['n']}x across 20 threads "
              f"(want 1), all 20 callers got the cached result: {len(results) == 20 and ok}")
        return ok
    except Exception as e:
        print(f"  [FAIL] Once: not implemented ({e})")
        return False


def _run_demos():
    _demo_countdown_latch()


def _run_checks():
    print("\n=== SELF-TESTS (grade your implementations — [FAIL]: not implemented is expected until you build them) ===")
    results = [
        _check_semaphore(),
        _check_barrier(),
        _check_rwlock(),
        _check_future(),
        _check_once(),
    ]
    print(f"\n  {sum(results)}/{len(results)} checks passed.")


if __name__ == "__main__":
    _run_demos()
    _run_checks()
    print("\nNext: workbook_04_bounded_queue.py — producer/consumer with a hand-built bounded queue.")
