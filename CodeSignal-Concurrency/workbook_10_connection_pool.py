"""
================================================================================
CODESIGNAL CONCURRENCY — WORKBOOK 10: CONNECTION POOL & GRACEFUL SHUTDOWN
================================================================================
Difficulty: Capstone (this is the LAST workbook in the series)
Model:      Python `threading` (preemptive, multi-threaded) — NOT asyncio
Primitives: Semaphore / queue.Queue as a pool, contextlib.contextmanager

Maps to INTERVIEW.MD:
    Connection pool (32), worker pool cancel / graceful shutdown (33)

HOW TO USE THIS FILE
--------------------
1. Read the PRIMER block.
2. Read `_demo_connection_pool()`, then RUN the file to watch it:
       python3 CodeSignal-Concurrency/workbook_10_connection_pool.py
3. Each EXERCISE is a skeleton: signature + docstring + `# YOUR CODE HERE`.
   Implement it yourself — there is no reference solution in this file (see
   workbook_01_primitives.py if you want a worked model of the *pattern*).
4. The `_check_*` self-tests at the bottom print PASS/FAIL against YOUR code.
   They are watchdog-protected: a broken pool that deadlocks will print
   [FAIL] instead of hanging the process forever.

THE ONE IDEA
------------
A connection pool is just a fixed set of expensive-to-create, reusable
resources (DB connections, sockets, worker handles) shared by many callers:
  - checkout   -> BLOCK if none are free (with an optional timeout — don't
                  wait forever in production code)
  - checkin    -> return it so someone else can use it, EVEN IF the caller
                  raised an exception while holding it (leak = pool starves)
  - shutdown   -> stop handing new ones out, drain what's outstanding, then
                  dispose everything exactly once

That "even if the caller raised" requirement is why the clean public API is a
context manager (`with pool.get() as conn: ...`), not bare acquire/release
calls the caller has to remember to pair with try/finally themselves.
================================================================================
"""

import contextlib
import queue
import threading
import time


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  PRIMER: THE SEMAPHORE+QUEUE PATTERN                                         ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
Two equivalent ways to build a bounded pool of N reusable objects:

  (a) Semaphore(N) + a plain list/deque protected by a Lock
      - acquire() = sem.acquire() [blocks if 0 permits] then pop an idle conn
      - release() = push the conn back, then sem.release()
      - the semaphore's permit count IS "how many idle connections exist"

  (b) queue.Queue(maxsize=N), pre-filled with N connections
      - acquire() = q.get(timeout=...)   [blocks if empty; raises queue.Empty
                    on timeout — the timeout contract comes almost for free]
      - release() = q.put(conn)
      - a Queue already bundles a Lock + two Conditions (not-empty / not-full)
        internally, so this is really (a) with the bookkeeping done for you.

Either is a correct, idiomatic answer in an interview. (b) is less code and
is what the demo below uses; mention (a) out loud so the interviewer knows you
understand a Queue *is* a semaphore-guarded structure, not magic.

WHY THE CONTEXT MANAGER MATTERS
--------------------------------
    conn = pool.acquire()
    do_something_that_might_raise(conn)   # <- if this throws, release() below
    pool.release(conn)                    #    never runs. Pool now has N-1
                                           #    usable connections forever.
`with pool.get() as conn:` fixes this the same way `with lock:` fixes
forgetting to unlock: acquire in `__enter__`, release in a `finally` inside
`__exit__` (or, easiest, write `get()` as a `@contextlib.contextmanager`
generator with `try/finally` around a single `yield conn`).

GRACEFUL SHUTDOWN
-----------------
close() has to do three things, in order:
  1. stop handing out new connections (reject/raise on further acquire()),
  2. wait for whatever is currently checked out to come back,
  3. dispose of every connection exactly once (call conn.close()).
Step 2 is the same "wait for a predicate, get notified" shape as everything
else in this series — a Condition, or simpler: block on the pool's own
container until you've collected all `size` connections back into it.
"""


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  FAKE CONNECTION — implemented, used by the demo and every check below       ║
# ╚════════════════════════════════════════════════════════════════════════════╝

class Connection:
    """A stand-in for a real DB/socket connection. Free to use as-is."""

    _id_lock = threading.Lock()
    _next_id = 1

    def __init__(self):
        with Connection._id_lock:
            self.id = Connection._next_id
            Connection._next_id += 1
        self.in_use = False        # instrumentation: pool should flip this
        self._close_calls = 0      # instrumentation: how many times close() ran

    def close(self):
        """Dispose of the connection. Real code would drop a socket here."""
        self._close_calls += 1

    @property
    def closed(self):
        return self._close_calls > 0

    def __repr__(self):
        return f"Conn#{self.id}(in_use={self.in_use}, closed={self.closed})"


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  DEMO — a minimal pool built directly on queue.Queue                         ║
# ╚════════════════════════════════════════════════════════════════════════════╝

def _demo_connection_pool():
    """Run to SEE checkout/checkin on a queue.Queue-backed pool.
    >>> _demo_connection_pool()
    """
    print("\n--- DEMO: queue.Queue as a connection pool ---")
    size = 3
    pool = queue.Queue(maxsize=size)
    for _ in range(size):
        pool.put(Connection())

    log = []
    log_lock = threading.Lock()

    def worker(i):
        conn = pool.get(timeout=2)          # BLOCKS if all 3 are checked out
        conn.in_use = True
        with log_lock:
            log.append(f"worker {i} checked out {conn}")
        time.sleep(0.03)                    # pretend to do work with it
        conn.in_use = False
        pool.put(conn)                      # check back in — always runs here
        with log_lock:
            log.append(f"worker {i} returned it")

    # 6 workers, only 3 connections -> some workers must wait their turn
    threads = [threading.Thread(target=worker, args=(i,)) for i in range(6)]
    for t in threads: t.start()
    for t in threads: t.join()

    for line in log:
        print(f"  {line}")
    print(f"  pool.qsize() = {pool.qsize()} (expected {size} — everyone checked back in)")


# ══════════════════════════════════════════════════════════════════════════════
#  EXERCISES — skeletons only. Implement them yourself.
# ══════════════════════════════════════════════════════════════════════════════

# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  EXERCISE 1 — ConnectionPool: acquire / release / get()                      ║
# ║  EXERCISE 2 — acquire(timeout): fail fast, don't hang forever                ║
# ║  EXERCISE 3 — close(): drain outstanding, dispose everything exactly once    ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
Build a general-purpose ConnectionPool. All three exercises live on this one
class because that's how it looks in real code — acquire/release/close are one
cohesive object, not three unrelated pieces.

    pool = ConnectionPool(factory=Connection, size=2)

    # low-level API
    conn = pool.acquire(timeout=1.0)   # blocks up to 1s; see contract below
    pool.release(conn)

    # preferred API — exception-safe
    with pool.get(timeout=1.0) as conn:
        do_stuff(conn)                 # even if this raises, conn comes back

    pool.close()                       # graceful shutdown (Exercise 3)

CONTRACT YOU MUST DECIDE AND HONOR (pick one, be consistent):
    (A) acquire(timeout) RAISES (e.g. queue.Empty, or a TimeoutError you
        define) when no connection frees up in time, or
    (B) acquire(timeout) RETURNS None on timeout.
Either is a legitimate interview answer as long as it's documented and never
just hangs past the requested timeout.

INVARIANTS the checks below will hammer on:
  - never more than `size` connections are checked out (in_use) at once
  - no two callers are ever handed the SAME Connection object simultaneously
  - every connection that goes out via acquire()/get() eventually comes back
    via release() (checked: after all workers finish, the pool is "full" again)
  - after close(): every connection factory() ever produced has had close()
    called on it EXACTLY once, and close() itself must return (not hang)
    even though other threads are still holding connections when it's called

HINTS
-----
  - queue.Queue(maxsize=size), pre-filled with `size` factory() connections in
    __init__, gets you acquire()/release() almost for free (see the PRIMER).
  - queue.Queue.get(timeout=...) raises queue.Empty on timeout — that can BE
    your acquire() timeout behavior (contract A above).
  - get() is a one-line-body @contextlib.contextmanager: acquire, `yield conn`
    inside a try, release() inside the matching finally.
  - close(): the simplest correct approach is "keep calling acquire() (no
    timeout, or a generous one) until you've collected all `size` connections
    back, then close() each one and mark the pool as closed so any further
    acquire() raises/refuses immediately." You already own every connection
    once you've pulled all `size` of them out of the queue — no other thread
    can be holding one you haven't accounted for.
  - Guard `closed`/shutdown state with a Lock — it's shared state read and
    written from multiple threads, same as everything else in this series.
"""

class ConnectionPool:
    """Fixed-size pool of reusable connections. See module docstring above
    for the full contract this class must satisfy."""

    def __init__(self, factory, size):
        """Pre-create `size` connections by calling factory() `size` times."""
        # YOUR CODE HERE
        raise NotImplementedError

    def acquire(self, timeout=None):
        """Return a free connection, blocking until one is available (up to
        `timeout` seconds if given). Document + implement your timeout
        contract here: raise on timeout, or return None — pick one."""
        # YOUR CODE HERE
        raise NotImplementedError

    def release(self, conn):
        """Return `conn` to the pool so another caller can acquire() it."""
        # YOUR CODE HERE
        raise NotImplementedError

    @contextlib.contextmanager
    def get(self, timeout=None):
        """Context manager: acquire a connection, GUARANTEE it is released
        even if the caller's `with` block raises. Usage:
            with pool.get(timeout=1) as conn:
                ...
        """
        # YOUR CODE HERE
        raise NotImplementedError

    def close(self):
        """Graceful shutdown:
          1. stop handing out new connections (further acquire() should
             raise or fail fast, not succeed and not hang),
          2. block until every outstanding connection has been release()d,
          3. call conn.close() on every connection exactly once.
        Must itself return promptly once all connections are back — it
        should NOT deadlock waiting on itself."""
        # YOUR CODE HERE
        raise NotImplementedError


# ══════════════════════════════════════════════════════════════════════════════
#  SELF-TESTS — run the file to grade YOUR implementation. Watchdog-protected:
#  a hung/deadlocked pool prints [FAIL], it does not hang the whole file.
# ══════════════════════════════════════════════════════════════════════════════

def _watchdog_call(fn, watchdog_timeout, *args, **kwargs):
    """Run fn(*args, **kwargs) on a daemon helper thread and join it with a
    timeout, so a deadlocked/hanging implementation can never hang the test
    suite itself. `watchdog_timeout` is how long WE wait for fn to return —
    separate from any `timeout=` kwarg you pass through to fn itself.
    Returns (finished, result, exception)."""
    box = {"result": None, "exc": None}

    def _run():
        try:
            box["result"] = fn(*args, **kwargs)
        except BaseException as e:            # noqa: BLE001 — capture anything
            box["exc"] = e

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    t.join(timeout=watchdog_timeout)
    return (not t.is_alive()), box["result"], box["exc"]


def _check_pool_basic():
    """E1: pool of 2, 5 concurrent workers, never more than 2 checked out,
    everyone eventually gets one, pool is full again at the end."""
    try:
        size = 2
        pool = ConnectionPool(factory=Connection, size=size)

        active = {"n": 0, "max": 0}
        active_lock = threading.Lock()
        results = []
        results_lock = threading.Lock()

        def worker(i):
            try:
                with pool.get(timeout=3) as conn:
                    ok_flag = conn.in_use is True
                    with active_lock:
                        active["n"] += 1
                        active["max"] = max(active["max"], active["n"])
                    time.sleep(0.05)
                    with active_lock:
                        active["n"] -= 1
                    with results_lock:
                        results.append(ok_flag)
            except Exception:
                with results_lock:
                    results.append(False)

        threads = [threading.Thread(target=worker, args=(i,), daemon=True) for i in range(5)]
        for t in threads: t.start()
        for t in threads: t.join(timeout=6)     # watchdog per worker
        hung = any(t.is_alive() for t in threads)

        # pool should be "full" again: we can pull `size` connections right
        # back out without blocking, and every original conn is accounted for.
        drained = []
        full_ok = True
        for _ in range(size):
            finished, conn, exc = _watchdog_call(pool.acquire, 2, timeout=1)
            if not finished or exc is not None or conn is None:
                full_ok = False
                break
            drained.append(conn)
        for c in drained:
            try:
                pool.release(c)
            except Exception:
                pass

        ok = (
            not hung
            and len(results) == 5
            and all(results)
            and active["max"] <= size
            and full_ok
        )
        print(f"  [{'PASS' if ok else 'FAIL'}] ConnectionPool basic checkout -> "
              f"peak concurrent {active['max']} (limit {size}), "
              f"{sum(results)}/5 workers ok, pool refilled={full_ok}, hung={hung}")
        return ok
    except Exception as e:
        print(f"  [FAIL] ConnectionPool basic checkout -> {type(e).__name__}: {e}")
        return False


def _check_acquire_timeout():
    """E2: exhaust the pool, then a further timed acquire must fail fast
    (raise or return None) within roughly the requested timeout window."""
    try:
        size = 2
        pool = ConnectionPool(factory=Connection, size=size)

        held = []
        for _ in range(size):
            finished, conn, exc = _watchdog_call(pool.acquire, 2, timeout=1)
            if not finished or exc is not None or conn is None:
                raise RuntimeError(f"could not exhaust pool for setup (exc={exc})")
            held.append(conn)

        requested_timeout = 0.3
        t0 = time.time()
        finished, conn, exc = _watchdog_call(pool.acquire, 2.0, timeout=requested_timeout)
        elapsed = time.time() - t0

        for c in held:
            try:
                pool.release(c)
            except Exception:
                pass

        timed_out_correctly = finished and (exc is not None or conn is None)
        fast_enough = elapsed < requested_timeout + 1.0     # generous slack

        ok = timed_out_correctly and fast_enough
        outcome = "raised" if exc is not None else ("returned None" if conn is None else "got a connection (WRONG)")
        print(f"  [{'PASS' if ok else 'FAIL'}] acquire(timeout) on exhausted pool -> "
              f"{outcome} in {elapsed:.2f}s (requested {requested_timeout}s), finished={finished}")
        return ok
    except Exception as e:
        print(f"  [FAIL] acquire(timeout) on exhausted pool -> {type(e).__name__}: {e}")
        return False


def _check_graceful_shutdown():
    """E3: check out some connections, close() from another thread, return
    them from yet another thread — close() must complete (watchdog) and every
    connection the pool ever created must have close() called exactly once."""
    try:
        size = 3
        created = []
        created_lock = threading.Lock()

        def factory():
            c = Connection()
            with created_lock:
                created.append(c)
            return c

        pool = ConnectionPool(factory=factory, size=size)

        f1, c1, e1 = _watchdog_call(pool.acquire, 2, timeout=1)
        f2, c2, e2 = _watchdog_call(pool.acquire, 2, timeout=1)
        if not (f1 and f2) or e1 is not None or e2 is not None or c1 is None or c2 is None:
            raise RuntimeError(f"setup: could not check out 2 connections (e1={e1}, e2={e2})")

        closer_done = {"v": False}

        def do_close():
            pool.close()
            closer_done["v"] = True

        closer = threading.Thread(target=do_close, daemon=True)
        closer.start()
        time.sleep(0.15)     # give close() a chance to start blocking on the outstanding conns
        waited_for_outstanding = not closer_done["v"]

        # return the outstanding connections from a third thread
        def return_them():
            pool.release(c1)
            time.sleep(0.03)
            pool.release(c2)

        returner = threading.Thread(target=return_them, daemon=True)
        returner.start()
        returner.join(timeout=3)

        closer.join(timeout=3)     # watchdog: close() must finish
        hung = closer.is_alive()

        with created_lock:
            all_closed_once = len(created) == size and all(c._close_calls == 1 for c in created)

        ok = (not hung) and closer_done["v"] and all_closed_once
        print(f"  [{'PASS' if ok else 'FAIL'}] graceful close() -> "
              f"waited_for_outstanding={waited_for_outstanding}, "
              f"{len(created)} conns created, all closed exactly once={all_closed_once}, hung={hung}")
        return ok
    except Exception as e:
        print(f"  [FAIL] graceful close() -> {type(e).__name__}: {e}")
        return False


def _run_demos():
    _demo_connection_pool()


def _run_checks():
    print("\n=== SELF-TESTS (grading YOUR ConnectionPool) ===")
    results = [
        _check_pool_basic(),
        _check_acquire_timeout(),
        _check_graceful_shutdown(),
    ]
    print(f"\n  {sum(results)}/{len(results)} checks passed.")


if __name__ == "__main__":
    _run_demos()
    _run_checks()
    print("\nThis was the last workbook in the series.")
    print("Next: go back to CodeSignal-Concurrency/README.md and re-run workbook_01 "
          "cold, or better — set a 30-45 min timer and do a mock round: pick 2-3 "
          "INTERVIEW.MD problems you haven't drilled and solve them without peeking.")
