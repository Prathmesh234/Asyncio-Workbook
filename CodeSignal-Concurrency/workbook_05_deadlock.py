"""
================================================================================
CODESIGNAL CONCURRENCY — WORKBOOK 05: DEADLOCK & RESOURCE ORDERING
================================================================================
Difficulty: Core interview topic (this is the one they ask you to TALK about)
Model:      Python `threading` (preemptive, multi-threaded) — NOT asyncio
Primitives: Lock (with timeout), Semaphore, and DISCIPLINE (not a new API)

Maps to INTERVIEW.MD:
    Dining Philosophers (1226), Traffic Light Controlled Intersection (1279)

HOW TO USE THIS FILE
--------------------
1. Read the PRIMER — the four Coffman conditions and the three practical fixes.
2. Run the file and WATCH a deadlock form and get detected (`_demo_deadlock_*`).
3. Solve the two EXERCISE skeletons yourself (`# YOUR CODE HERE`).
4. Run the file again — `_check_*` grades your implementations, with a
   watchdog so a broken (deadlocking) solution reports FAIL instead of
   hanging forever.

THE ONE IDEA
------------
Deadlock isn't a bug in a primitive — Locks and Semaphores work exactly as
documented. Deadlock is a bug in the ORDER multiple threads acquire MULTIPLE
resources. Fix the order (or bound the contention), and the primitives you
already know are enough.
================================================================================
"""

import threading
import time
from collections import deque


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  PRIMER 0: THE FOUR COFFMAN CONDITIONS                                       ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
A deadlock needs ALL FOUR of these true at once. Break any ONE and deadlock is
impossible. This is the vocabulary an interviewer expects you to use out loud:

  1. MUTUAL EXCLUSION  — a resource can be held by only one thread at a time.
                          (This is usually the whole POINT of the lock — you
                          rarely get to remove this one.)
  2. HOLD AND WAIT      — a thread holds resource A while blocked waiting for
                          resource B (instead of releasing A first).
  3. NO PREEMPTION      — the runtime can't forcibly take a resource back from
                          a thread that holds it; only the holder can release.
  4. CIRCULAR WAIT      — a cycle of threads T1 -> T2 -> ... -> T1 where each
                          Ti holds a resource that T(i+1) is waiting for.

Classic two-lock deadlock:
    Thread A:  acquire(lock_1)  ...  acquire(lock_2)
    Thread B:  acquire(lock_2)  ...  acquire(lock_1)
If A gets lock_1 and B gets lock_2 at "the same time", A now waits on lock_2
(held by B) and B waits on lock_1 (held by A). Neither ever proceeds. This is
condition 4 (circular wait) riding on 1-3, which were both "obviously fine"
Locks in isolation.

THREE PRACTICAL FIXES (pick based on the shape of the problem):
  A. GLOBAL LOCK ORDERING — assign every lock a fixed rank (e.g. id(), an index,
     a name) and require EVERY thread to acquire locks in ascending rank order,
     no matter which "logical" order the algorithm wants. Kills circular wait
     (condition 4) by construction — a cycle can't form if everyone walks the
     same one-way street.
  B. TRY-ACQUIRE WITH TIMEOUT/BACKOFF — `lock.acquire(timeout=...)`; if it
     fails, release whatever you already hold and retry later. Turns a
     deadlock into a detectable, recoverable failure instead of a permanent
     hang. Good when a global order is impractical (locks discovered at
     runtime, e.g. transferring between two accounts chosen by the caller).
  C. LIMIT CONCURRENT HOLDERS (admission control) — a Semaphore(N-1) gate in
     front of N threads that each need 2 of N shared resources guarantees at
     least one resource is always free, so hold-and-wait can never complete
     the cycle. This is the classic Dining Philosophers Semaphore(4) trick.
"""


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  DEMO 1 — WATCH a deadlock form, and DETECT it with acquire(timeout=...)     ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
Two threads each grab "their" lock first, sleep just long enough for the OTHER
thread to grab theirs too, then reach for the second lock. Without a timeout
this hangs forever. WITH a timeout, `acquire()` returns False and we can log
the deadlock and back off cleanly — this is fix B from the primer.
"""

def _demo_deadlock_detected():
    """>>> _demo_deadlock_detected()"""
    print("\n--- DEMO: two threads deadlock on two locks (detected via timeout) ---")
    lock_a = threading.Lock()
    lock_b = threading.Lock()
    log = deque()
    log_lock = threading.Lock()

    def record(msg):
        with log_lock:
            log.append(msg)

    def thread_1():
        with lock_a:
            record("T1: holds lock_a, wants lock_b")
            time.sleep(0.2)              # <- give T2 time to grab lock_b first
            got = lock_b.acquire(timeout=0.3)
            if got:
                record("T1: got lock_b (no contention this run)")
                lock_b.release()
            else:
                record("T1: TIMED OUT waiting for lock_b -> DEADLOCK, backing off")

    def thread_2():
        with lock_b:
            record("T2: holds lock_b, wants lock_a")
            time.sleep(0.2)              # <- give T1 time to grab lock_a first
            got = lock_a.acquire(timeout=0.3)
            if got:
                record("T2: got lock_a (no contention this run)")
                lock_a.release()
            else:
                record("T2: TIMED OUT waiting for lock_a -> DEADLOCK, backing off")

    t1 = threading.Thread(target=thread_1)
    t2 = threading.Thread(target=thread_2)
    t1.start(); t2.start()
    t1.join(); t2.join()
    for line in log:
        print(f"  {line}")
    print("  (both threads survived — the timeout turned a hang into a reported failure)")


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  DEMO 2 — THE FIX: global lock ordering                                      ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
Same two locks, same two threads wanting them in "opposite" logical order —
but now BOTH threads acquire in a fixed global order (lower `id()` first)
before doing any work. Circular wait becomes structurally impossible: whoever
gets the first (lower-ranked) lock is guaranteed the second one isn't stuck
behind someone waiting on the first. No timeout needed, no retries, no hang.
"""

def _demo_ordered_fix():
    """>>> _demo_ordered_fix()"""
    print("\n--- DEMO: same two locks, fixed acquisition order -> no deadlock ---")
    lock_a = threading.Lock()
    lock_b = threading.Lock()
    log = deque()
    log_lock = threading.Lock()

    def record(msg):
        with log_lock:
            log.append(msg)

    def acquire_in_order(wanted_first, wanted_second):
        """Return (first, second) sorted by a fixed global rank (id()),
        REGARDLESS of which one the caller logically wanted first."""
        return tuple(sorted((wanted_first, wanted_second), key=id))

    def worker(name, wanted_first, wanted_second):
        first, second = acquire_in_order(wanted_first, wanted_second)
        with first:
            record(f"{name}: acquired rank-1 lock")
            time.sleep(0.05)             # same widened window as the deadlock demo
            with second:
                record(f"{name}: acquired rank-2 lock -> critical section -> done")

    # T1 logically wants (a, b); T2 logically wants (b, a) — opposite "intent",
    # but acquire_in_order() forces both onto the SAME global order.
    t1 = threading.Thread(target=worker, args=("T1", lock_a, lock_b))
    t2 = threading.Thread(target=worker, args=("T2", lock_b, lock_a))
    t1.start(); t2.start()
    t1.join(); t2.join()
    for line in log:
        print(f"  {line}")
    print("  no timeout fired, no backoff needed — the order made the cycle impossible")


# ══════════════════════════════════════════════════════════════════════════════
#  CHEAT SHEET — deadlock fixes, at a glance
# ══════════════════════════════════════════════════════════════════════════════
"""
┌───────────────────────────┬────────────────────────────────────────────────┐
│ Fix                        │ Use when...                                    │
├───────────────────────────┼────────────────────────────────────────────────┤
│ Global lock ordering       │ The set of locks is known ahead of time; you   │
│                            │ can rank them (id, index, name) and enforce    │
│                            │ ascending acquisition everywhere.              │
│ Try-acquire + backoff      │ Locks/resources chosen at runtime and you      │
│                            │ can't fix a global order (e.g. transfer(A,B)   │
│                            │ where A/B are arbitrary accounts).             │
│ Admission control          │ N threads each need 2+ of N shared resources — │
│ (Semaphore(N-1))           │ cap concurrent claimants so one is always free.│
└───────────────────────────┴────────────────────────────────────────────────┘
Say all three out loud if asked "how do you prevent deadlock" — naming the
condition each fix breaks (circular wait, hold-and-wait) is what separates a
strong answer from "I'd add a timeout".
"""


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  EXERCISE 1 — Dining Philosophers (LeetCode 1226)                            ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
5 philosophers sit around a table with 5 forks between them. Philosopher `p`
needs BOTH the fork to their left and the fork to their right to eat; forks are
shared with neighbors. Fork numbering used by this workbook (and its checks):

    fork k sits between philosopher k and philosopher (k+1) % 5
    philosopher p's RIGHT fork = p
    philosopher p's LEFT  fork = (p - 1) % 5

wantsToEat(philosopher, pickLeftFork, pickRightFork, eat, putLeftFork,
putRightFork) is called concurrently (possibly many times) for each
philosopher 0..4. You must call the five callbacks, in order, such that:
  - a fork is held by at most one philosopher at any instant,
  - no philosopher starves (every call to wantsToEat eventually eats),
  - the system never deadlocks.

Naive "always pick left then right" deadlocks: if all 5 grab their left fork
simultaneously, every philosopher then waits forever for a right fork someone
else is holding — textbook circular wait.

HINT — pick ONE of these (all are standard, correct answers):
  (a) Resource ordering: always acquire the LOWER-numbered of your two forks
      first, release order doesn't matter. Kills circular wait directly.
  (b) Admission control: a Semaphore(4) gates entry to "may attempt to pick up
      forks" — with at most 4 of 5 philosophers trying at once, at least one
      seat's forks are always both free, so the wait cycle can't close.
  (c) Odd/even handedness: even-numbered philosophers pick right-then-left,
      odd-numbered pick left-then-right — breaks the symmetry that lets a
      cycle form.
A per-fork `threading.Lock` (5 of them) is the natural primitive either way.
"""

class DiningPhilosophers:
    def __init__(self):
        """
        Set up 5 fork locks (or whatever state your chosen fix needs — e.g. an
        admission-control Semaphore alongside the 5 fork locks).
        """
        # YOUR CODE HERE
        raise NotImplementedError

    def wantsToEat(self, philosopher, pickLeftFork, pickRightFork, eat,
                   putLeftFork, putRightFork):
        """
        philosopher: int in [0, 4].
        pickLeftFork, pickRightFork, eat, putLeftFork, putRightFork: zero-arg
        callables you must call, in the correct order, to model one full
        eating cycle for this philosopher. Must not deadlock, must not let two
        philosophers hold the same fork, must not starve anyone.
        """
        # YOUR CODE HERE
        raise NotImplementedError


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  EXERCISE 2 — Traffic Light Controlled Intersection (LeetCode 1279)          ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
Two roads (roadId 1 and 2) cross at one intersection; the light is green for
exactly one road at a time and starts green for road 1. carArrived(carId,
roadId, direction, turnGreen, crossCar) is invoked concurrently as cars show
up on either road:
  - if the car's road is already green: just call crossCar().
  - if the car's road is red: call turnGreen() FIRST (exactly once, to switch
    the light), THEN crossCar().
  - turnGreen() must be called ONLY when a switch is actually needed (never
    "just in case", never twice for the same switch) — minimize light flips.
  - crossCar() calls from cars on DIFFERENT roads must never overlap in time.
    (Multiple cars on the SAME green road may cross back-to-back or, in the
    real problem, even concurrently — but this workbook's simplest correct
    solution just serializes all crossings under one lock, which trivially
    satisfies both constraints. That's a perfectly good interview answer.)

This is a single shared piece of state (which road is green) guarded by a
single Lock — no risk of the multi-lock deadlock from the primer, but it's
the textbook example of condition 1 (mutual exclusion) done right: keep the
critical section (check-switch-cross) as one atomic unit.

HINT: one `threading.Lock` guarding "current green road"; inside `with lock:`
check whether roadId matches, call turnGreen() only on mismatch (and update
your stored green road), then call crossCar() before releasing.
"""

class TrafficLight:
    def __init__(self):
        """Light starts green for road 1. Set up your guard (a Lock is enough)."""
        # YOUR CODE HERE
        raise NotImplementedError

    def carArrived(self, carId, roadId, direction, turnGreen, crossCar):
        """
        carId: unique int id. roadId: 1 or 2. direction: unused by the logic
        (kept only because the original LeetCode signature has it).
        turnGreen, crossCar: zero-arg callables — call turnGreen() only when
        switching is required, always call crossCar() exactly once per call.
        """
        # YOUR CODE HERE
        raise NotImplementedError


# ══════════════════════════════════════════════════════════════════════════════
#  SELF-TESTS — run the file to grade your implementations.
#  Every check has a WATCHDOG: threads run as daemons and are joined with a
#  timeout, so a deadlocking/incorrect solution reports FAIL instead of
#  hanging the whole file.
# ══════════════════════════════════════════════════════════════════════════════

def _run_with_watchdog(threads, timeout):
    """Start all threads as daemons, join with a total time budget.
    Returns the list of threads still alive after the budget (non-empty ==
    something is stuck / deadlocked)."""
    for t in threads:
        t.daemon = True
        t.start()
    deadline = time.time() + timeout
    stuck = []
    for t in threads:
        remaining = max(0.0, deadline - time.time())
        t.join(remaining)
        if t.is_alive():
            stuck.append(t)
    return stuck


def _check_dining_philosophers():
    n_philosophers = 5
    attempts_per_philosopher = 4
    watchdog_timeout = 5.0

    try:
        dp = DiningPhilosophers()
    except Exception as e:
        print(f"  [FAIL] DiningPhilosophers.__init__ -> {type(e).__name__}: {e}")
        return False

    fork_owner = [None] * n_philosophers          # instrumentation, NOT the student's locks
    instrumentation_lock = threading.Lock()
    violations = []
    eaten_counts = [0] * n_philosophers
    errors = []

    def make_callbacks(p):
        left_fork = (p - 1) % n_philosophers
        right_fork = p

        def pick(fork_id):
            with instrumentation_lock:
                if fork_owner[fork_id] is not None:
                    violations.append(
                        f"fork {fork_id} double-held by {fork_owner[fork_id]} and {p}")
                fork_owner[fork_id] = p

        def put(fork_id):
            with instrumentation_lock:
                fork_owner[fork_id] = None

        pick_left = lambda: pick(left_fork)
        pick_right = lambda: pick(right_fork)
        put_left = lambda: put(left_fork)
        put_right = lambda: put(right_fork)

        def eat():
            eaten_counts[p] += 1

        return pick_left, pick_right, eat, put_left, put_right

    def philosopher_loop(p):
        try:
            for _ in range(attempts_per_philosopher):
                pick_left, pick_right, eat, put_left, put_right = make_callbacks(p)
                dp.wantsToEat(p, pick_left, pick_right, eat, put_left, put_right)
        except Exception as e:
            errors.append((p, e))

    threads = [threading.Thread(target=philosopher_loop, args=(p,))
               for p in range(n_philosophers)]
    stuck = _run_with_watchdog(threads, watchdog_timeout)

    if stuck:
        print(f"  [FAIL] DiningPhilosophers -> {len(stuck)} thread(s) still stuck after "
              f"{watchdog_timeout}s (DEADLOCK)")
        return False
    if errors:
        p, e = errors[0]
        print(f"  [FAIL] DiningPhilosophers -> philosopher {p} raised "
              f"{type(e).__name__}: {e}")
        return False
    if violations:
        print(f"  [FAIL] DiningPhilosophers -> fork safety violated: {violations[0]}")
        return False
    ok = all(c == attempts_per_philosopher for c in eaten_counts)
    print(f"  [{'PASS' if ok else 'FAIL'}] DiningPhilosophers -> "
          f"eaten counts {eaten_counts} (want {attempts_per_philosopher} each), "
          f"no fork double-held, no deadlock")
    return ok


def _check_traffic_light():
    watchdog_timeout = 5.0

    try:
        tl = TrafficLight()
    except Exception as e:
        print(f"  [FAIL] TrafficLight.__init__ -> {type(e).__name__}: {e}")
        return False

    # deterministic interleaving of roads (no `random` — std lib only per the series rule)
    road_pattern = [1, 2, 1, 1, 2, 2, 1, 2, 2, 1, 1, 2]
    n_cars = len(road_pattern)

    active_lock = threading.Lock()
    active_roads = {}                 # roadId -> count of cars currently crossing
    violation = {"hit": False, "detail": None}
    crossed = []
    crossed_lock = threading.Lock()
    errors = []

    def make_callbacks(car_id, road_id):
        def turnGreen():
            pass  # just a signal in this model; correctness is judged on crossCar overlap

        def crossCar():
            with active_lock:
                active_roads[road_id] = active_roads.get(road_id, 0) + 1
                other_roads = {r for r, cnt in active_roads.items() if cnt > 0} - {road_id}
                if other_roads:
                    violation["hit"] = True
                    violation["detail"] = (
                        f"car {car_id} (road {road_id}) crossed while road(s) "
                        f"{other_roads} also crossing")
            time.sleep(0.01)           # widen the window so a real race always shows
            with active_lock:
                active_roads[road_id] -= 1
            with crossed_lock:
                crossed.append(car_id)

        return turnGreen, crossCar

    def car_thread(car_id, road_id):
        try:
            turnGreen, crossCar = make_callbacks(car_id, road_id)
            tl.carArrived(car_id, road_id, 1, turnGreen, crossCar)
        except Exception as e:
            errors.append((car_id, e))

    threads = [threading.Thread(target=car_thread, args=(car_id, road_id))
               for car_id, road_id in enumerate(road_pattern)]
    stuck = _run_with_watchdog(threads, watchdog_timeout)

    if stuck:
        print(f"  [FAIL] TrafficLight -> {len(stuck)} thread(s) still stuck after "
              f"{watchdog_timeout}s (DEADLOCK)")
        return False
    if errors:
        car_id, e = errors[0]
        print(f"  [FAIL] TrafficLight -> car {car_id} raised {type(e).__name__}: {e}")
        return False
    if violation["hit"]:
        print(f"  [FAIL] TrafficLight -> {violation['detail']}")
        return False
    ok = sorted(crossed) == list(range(n_cars))
    print(f"  [{'PASS' if ok else 'FAIL'}] TrafficLight -> {len(crossed)}/{n_cars} cars "
          f"crossed exactly once, never two roads crossing at once")
    return ok


def _run_demos():
    _demo_deadlock_detected()
    _demo_ordered_fix()


def _run_checks():
    print("\n=== SELF-TESTS (solve the exercises above to make these PASS) ===")
    results = [
        _check_dining_philosophers(),
        _check_traffic_light(),
    ]
    print(f"\n  {sum(results)}/{len(results)} checks passed.")


if __name__ == "__main__":
    _run_demos()
    _run_checks()
    print("\nNext: workbook_06_pools.py — thread pools & executors, YOUR turn to solve.")
