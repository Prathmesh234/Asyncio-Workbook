"""
================================================================================
CODESIGNAL CONCURRENCY — WORKBOOK 01: THREADING PRIMITIVES  (FULLY WORKED)
================================================================================
Difficulty: Foundational (but the whole series builds on it)
Model:      Python `threading` (preemptive, multi-threaded) — NOT asyncio
Primitives: Lock, RLock, Semaphore, BoundedSemaphore, Event, Condition, Barrier

Maps to INTERVIEW.MD:
    Print in Order (1114), FooBar Alternately (1115),
    Print Zero Even Odd (1116), Building H2O (1117)
    ...and the primitives underlying items 10-17.

HOW TO USE THIS FILE
--------------------
1. Read each PRIMER block.
2. Read the `_demo_*` functions, then RUN the file to watch them:
       python "CodeSignal-Concurrency/workbook_01_primitives.py"
3. Each exercise has a REFERENCE SOLUTION (this workbook is the worked exemplar).
   Before reading it: cover the body, re-type it yourself, then run the checks.
4. The `_check_*` self-tests at the bottom print PASS/FAIL.

THE ONE IDEA
------------
A thread can be preempted between ANY two bytecodes — even between reading `x`
and writing `x + 1`. Everything below exists to make that safe:
  - protect shared state          -> Lock / RLock
  - limit how many run at once     -> Semaphore
  - make a thread WAIT for a signal-> Event / Condition / Barrier
Never busy-wait (`while not ready: pass`). Waiting is a primitive's job.
================================================================================
"""

import threading
import time
from collections import deque


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  PRIMER 0: THE RACE CONDITION YOU MUST BE ABLE TO EXPLAIN                    ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
`counter += 1` is THREE operations:
        tmp = counter        # LOAD
        tmp = tmp + 1        # ADD
        counter = tmp        # STORE
A thread can be preempted between any of them. Two threads both LOAD 0, both
STORE 1 -> one increment is lost. Over 100k iterations x N threads the final
value is almost never N*100k. The GIL does NOT save you: it can switch threads
between those three bytecodes.

RULE OF THUMB: `list.append(x)` and `dict[k] = v` are atomic (single C op).
`x += 1`, `x = x + 1`, check-then-act (`if k not in d: d[k]=...`) are NOT.
"""

def _demo_race_condition():
    """Run to SEE the lost updates. >>> _demo_race_condition()"""
    print("\n--- DEMO: race condition (unprotected counter) ---")
    n_threads, per = 8, 50
    expected = n_threads * per
    counter = {"v": 0}

    def bump():
        for _ in range(per):
            tmp = counter["v"]          # LOAD
            time.sleep(0.0001)          # <- widen the window so the race ALWAYS
                                        #    shows. The bug exists WITHOUT this
                                        #    sleep too; it's just rarer to catch.
            counter["v"] = tmp + 1      # STORE (may clobber another thread's write)

    threads = [threading.Thread(target=bump) for _ in range(n_threads)]
    for t in threads: t.start()
    for t in threads: t.join()
    lost = expected - counter["v"]
    print(f"  got {counter['v']} of expected {expected}  <- lost {lost} updates "
          f"({'THIS is the bug a Lock fixes' if lost else 'run again — timing-dependent'})")


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  PRIMER 1: LOCK — mutual exclusion (one thread in the critical section)      ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
A Lock has two states: locked / unlocked. `acquire()` blocks until it can take
the lock; `release()` frees it. ALWAYS use `with lock:` so it releases on
exception too. The block between acquire and release is the "critical section" —
keep it as small as possible (hold the lock only while touching shared state).
"""

def _demo_lock():
    """>>> _demo_lock()"""
    print("\n--- DEMO: Lock fixes the race (same widened window) ---")
    n_threads, per = 8, 50
    expected = n_threads * per
    counter = {"v": 0}
    lock = threading.Lock()

    def bump():
        for _ in range(per):
            with lock:                  # critical section: LOAD..STORE is now
                tmp = counter["v"]      # indivisible to other threads
                time.sleep(0.0001)      # even with the wide window...
                counter["v"] = tmp + 1
    threads = [threading.Thread(target=bump) for _ in range(n_threads)]
    for t in threads: t.start()
    for t in threads: t.join()
    print(f"  got {counter['v']} of expected {expected}  <- exact, every time")


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  PRIMER 2: RLOCK — reentrant lock (same thread can acquire it N times)       ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
A plain Lock DEADLOCKS if the SAME thread tries to acquire it twice (e.g. a
locked method calls another locked method on the same object). An RLock counts
how many times its owning thread acquired it and only frees on the matching
number of releases. Reach for RLock when a class's locked methods call each
other. (If they don't, prefer a plain Lock — it's cheaper and less forgiving of
mistakes, which is good.)
"""

def _demo_rlock():
    """>>> _demo_rlock()"""
    print("\n--- DEMO: RLock allows re-entry ---")

    class Account:
        def __init__(self):
            self._lock = threading.RLock()
            self._balance = 0
        def deposit(self, amt):
            with self._lock:
                self._balance += amt
        def deposit_two(self, a, b):
            with self._lock:            # already holds the lock...
                self.deposit(a)         # ...and deposit() acquires it AGAIN
                self.deposit(b)         # a plain Lock would hang here forever
        def balance(self):
            with self._lock:
                return self._balance

    acc = Account()
    acc.deposit_two(10, 5)
    print(f"  balance = {acc.balance()} (expected 15) — no deadlock")


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  PRIMER 3: SEMAPHORE — let at most N threads through at once                 ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
A Semaphore holds an integer permit count. acquire() decrements (blocking at 0);
release() increments. Semaphore(1) behaves like a Lock. Semaphore(0) is a
"gate" you open by releasing from another thread — this is how you make one
thread wait for another (used heavily in the ordering problems below).

BoundedSemaphore raises if you release() more than you acquired — a bug-catcher.
Use it for resource pools (N connections, N slots) where over-release is a bug.
"""

def _demo_semaphore():
    """>>> _demo_semaphore()"""
    print("\n--- DEMO: Semaphore caps concurrency at 2 ---")
    sem = threading.Semaphore(2)
    active = {"n": 0, "max": 0}
    active_lock = threading.Lock()

    def worker(i):
        with sem:                       # at most 2 inside at once
            with active_lock:
                active["n"] += 1
                active["max"] = max(active["max"], active["n"])
            time.sleep(0.05)
            with active_lock:
                active["n"] -= 1

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(6)]
    for t in threads: t.start()
    for t in threads: t.join()
    print(f"  peak concurrent = {active['max']} (expected 2)")


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  PRIMER 4: EVENT — a one-bit flag threads can WAIT on                        ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
An Event is a boolean. wait() blocks until it's set(); once set, wait() returns
immediately for everyone. clear() resets it. Perfect for "start signal",
"shutdown signal", or "step A finished, step B may go". Turn-taking (like FooBar)
uses a PAIR of Events that ping-pong: each thread waits on its own, then sets the
other's.
"""

def _demo_event():
    """>>> _demo_event()"""
    print("\n--- DEMO: Event as a start gate ---")
    start = threading.Event()
    order = []
    order_lock = threading.Lock()

    def racer(i):
        start.wait()                    # all block here...
        with order_lock:
            order.append(i)

    threads = [threading.Thread(target=racer, args=(i,)) for i in range(4)]
    for t in threads: t.start()
    time.sleep(0.05)
    print("  (all 4 threads parked on start.wait())")
    start.set()                         # ...released together
    for t in threads: t.join()
    print(f"  finished: {sorted(order)}")


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  PRIMER 5: CONDITION — wait for a PREDICATE, get notified when it changes    ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
A Condition bundles a lock with a wait/notify queue. The pattern is iron-clad:

    with cond:                          # hold the lock
        while not predicate():          # ALWAYS a while, never an if
            cond.wait()                 # atomically releases lock + sleeps;
                                        # re-acquires lock on wake
        ...consume/act on the state...

    with cond:                          # producer side
        ...change the state...
        cond.notify()      # wake ONE waiter   (or notify_all() to wake all)

WHY `while` NOT `if`:
  - spurious wakeups happen,
  - and another thread may grab the resource between your wake and your re-lock.
  Re-checking the predicate is the only correct thing. Memorize this.

Use Condition for anything shaped like "wait until the buffer is non-empty /
non-full" — i.e. bounded queues (Workbook 04).
"""

def _demo_condition():
    """>>> _demo_condition()  — a tiny bounded buffer (cap 2)."""
    print("\n--- DEMO: Condition (bounded buffer) ---")
    buf = deque()
    cap = 2
    cond = threading.Condition()
    produced, consumed = [], []

    def producer():
        for i in range(5):
            with cond:
                while len(buf) >= cap:
                    cond.wait()         # wait for space
                buf.append(i); produced.append(i)
                cond.notify()           # wake a consumer

    def consumer():
        for _ in range(5):
            with cond:
                while not buf:
                    cond.wait()         # wait for an item
                consumed.append(buf.popleft())
                cond.notify()           # wake the producer

    p = threading.Thread(target=producer); c = threading.Thread(target=consumer)
    p.start(); c.start(); p.join(); c.join()
    print(f"  produced {produced}  consumed {consumed}  (buffer never exceeded {cap})")


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  PRIMER 6: BARRIER — N threads meet at a line before any proceeds            ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
Barrier(N) blocks each caller of wait() until N of them have arrived, then
releases all N together and resets for the next round. Great for phased/lock-step
computation and for "assemble a group before continuing" (Building H2O groups
2 H + 1 O = 3 threads per water molecule).
"""

def _demo_barrier():
    """>>> _demo_barrier()"""
    print("\n--- DEMO: Barrier synchronizes 3 threads per phase ---")
    barrier = threading.Barrier(3)
    log = []
    log_lock = threading.Lock()

    def worker(i):
        for phase in range(2):
            time.sleep(0.01 * i)        # arrive at different times
            with log_lock:
                log.append(f"t{i}-arrive-p{phase}")
            barrier.wait()              # all 3 wait here
            with log_lock:
                log.append(f"t{i}-pass-p{phase}")

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(3)]
    for t in threads: t.start()
    for t in threads: t.join()
    # All three "arrive-p0" happen before any "pass-p0":
    arrive_p0 = [x for x in log if x.endswith("arrive-p0")]
    pass_p0   = [x for x in log if x.endswith("pass-p0")]
    print(f"  {len(arrive_p0)} arrived before {len(pass_p0)} passed phase 0 (barrier held the line)")


# ══════════════════════════════════════════════════════════════════════════════
#  CHEAT SHEET — which primitive do I reach for?
# ══════════════════════════════════════════════════════════════════════════════
"""
┌────────────────────┬──────────────────────────────────────────────────────────┐
│ Need               │ Primitive                                                 │
├────────────────────┼──────────────────────────────────────────────────────────┤
│ Protect shared var │ Lock  (RLock if locked methods call each other)           │
│ At most N at once   │ Semaphore(N)   /  BoundedSemaphore(N) for pools           │
│ One-shot signal     │ Event  (start / shutdown / "step done")                  │
│ Turn-taking         │ pair of Events, or Semaphores that release each other     │
│ Wait for predicate  │ Condition  (with `while not pred(): cond.wait()`)         │
│ Group of N rendezvous│ Barrier(N)                                               │
│ Producer/consumer   │ queue.Queue  (don't hand-roll unless asked) — Workbook 04 │
└────────────────────┴──────────────────────────────────────────────────────────┘
"""


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  EXERCISE 1 — Thread-safe counter          (primitive: Lock)                 ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
Implement a counter that N threads can increment concurrently and whose final
value is always exact. This is the "hello world" of thread safety.

    >>> c = Counter()
    >>> # 4 threads each call c.increment() 100_000 times
    >>> c.value == 400_000

--- REFERENCE SOLUTION (cover this, implement it yourself first) ---
"""

class Counter:
    def __init__(self):
        self._value = 0
        self._lock = threading.Lock()

    def increment(self):
        with self._lock:
            self._value += 1

    @property
    def value(self):
        with self._lock:
            return self._value


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  EXERCISE 2 — Print in Order  (LeetCode 1114)     (primitive: Event)         ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
Three threads call first(), second(), third() on the SAME Foo object, in an
ARBITRARY order. Guarantee the output prints "firstsecondthird" regardless.

Idea: gate second() behind an Event set by first(); gate third() behind an Event
set by second(). Each printX() argument is a callback that does the printing.

--- REFERENCE SOLUTION ---
"""

class Foo:
    def __init__(self):
        self._second_gate = threading.Event()
        self._third_gate = threading.Event()

    def first(self, printFirst):
        printFirst()
        self._second_gate.set()          # unblock second()

    def second(self, printSecond):
        self._second_gate.wait()         # wait for first()
        printSecond()
        self._third_gate.set()           # unblock third()

    def third(self, printThird):
        self._third_gate.wait()          # wait for second()
        printThird()


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  EXERCISE 3 — FooBar Alternately (LC 1115)   (primitive: two Events)         ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
Thread A calls foo() n times, thread B calls bar() n times. Output must be
"foobar" repeated n times — strictly alternating.

Idea: two Events ping-ponging. foo's turn starts SET (foo goes first); after
printing foo, set bar's turn; bar waits its turn, prints, sets foo's turn.
Remember to clear() your own event after waking so the next loop blocks again.

--- REFERENCE SOLUTION ---
"""

class FooBar:
    def __init__(self, n):
        self.n = n
        self._foo_turn = threading.Event()
        self._bar_turn = threading.Event()
        self._foo_turn.set()             # foo prints first

    def foo(self, printFoo):
        for _ in range(self.n):
            self._foo_turn.wait()
            self._foo_turn.clear()
            printFoo()
            self._bar_turn.set()

    def bar(self, printBar):
        for _ in range(self.n):
            self._bar_turn.wait()
            self._bar_turn.clear()
            printBar()
            self._foo_turn.set()


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  EXERCISE 4 — Print Zero Even Odd (LC 1116)   (primitive: three Semaphores)  ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
Three threads share a ZeroEvenOdd(n). zero() prints 0 n times; even() prints the
even numbers; odd() prints the odd numbers. Output must be "0102030405..." up to
n (e.g. n=5 -> "0102030405", wait — that's "01 02 03 04 05" => "0102030405").
So the sequence is 0,1,0,2,0,3,... a zero before every number.

Idea: three Semaphores acting as gates. zero starts open (permit=1); even/odd
start closed (0). After zero prints a 0 it opens odd (for odd i) or even (for
even i). After odd/even prints, it re-opens zero. Turn-passing via release().

--- REFERENCE SOLUTION ---
"""

class ZeroEvenOdd:
    def __init__(self, n):
        self.n = n
        self._zero = threading.Semaphore(1)   # open first
        self._even = threading.Semaphore(0)
        self._odd = threading.Semaphore(0)

    def zero(self, printNumber):
        for i in range(self.n):
            self._zero.acquire()
            printNumber(0)
            # decide who prints the next number
            if i % 2 == 0:                     # next number is odd (1,3,5,..)
                self._odd.release()
            else:                              # next number is even (2,4,..)
                self._even.release()

    def even(self, printNumber):
        for i in range(2, self.n + 1, 2):
            self._even.acquire()
            printNumber(i)
            self._zero.release()

    def odd(self, printNumber):
        for i in range(1, self.n + 1, 2):
            self._odd.acquire()
            printNumber(i)
            self._zero.release()


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  EXERCISE 5 — Building H2O (LC 1117)   (primitives: Semaphore + Barrier)     ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
Many threads call hydrogen() or oxygen(). They must bond into water molecules:
every group that passes the release-callbacks must be exactly 2 H and 1 O, and
one molecule must fully form before the next starts mixing.

Idea:
  - Semaphore(2) throttles hydrogens; Semaphore(1) throttles oxygen — so at most
    2 H and 1 O are "staged" at a time.
  - A Barrier(3) makes those exact 3 threads rendezvous before any calls its
    release fn — guaranteeing well-formed molecules. Release the throttles after.

--- REFERENCE SOLUTION ---
"""

class H2O:
    def __init__(self):
        self._h_sem = threading.Semaphore(2)
        self._o_sem = threading.Semaphore(1)
        self._barrier = threading.Barrier(3)

    def hydrogen(self, releaseHydrogen):
        self._h_sem.acquire()
        self._barrier.wait()             # wait for the full 2H+1O group
        releaseHydrogen()
        self._h_sem.release()

    def oxygen(self, releaseOxygen):
        self._o_sem.acquire()
        self._barrier.wait()
        releaseOxygen()
        self._o_sem.release()


# ══════════════════════════════════════════════════════════════════════════════
#  SELF-TESTS — run the file to grade the reference solutions (all should PASS).
#  When you re-type a solution yourself, these tell you if you got it right.
# ══════════════════════════════════════════════════════════════════════════════

def _run(threads):
    for t in threads: t.start()
    for t in threads: t.join()


def _check_counter():
    c = Counter()
    def bump():
        for _ in range(100_000): c.increment()
    _run([threading.Thread(target=bump) for _ in range(4)])
    ok = c.value == 400_000
    print(f"  [{'PASS' if ok else 'FAIL'}] Counter -> {c.value} (want 400000)")
    return ok


def _check_print_in_order():
    ok_all = True
    for order in ([0, 1, 2], [2, 1, 0], [1, 2, 0]):      # try nasty start orders
        foo = Foo()
        out = []
        calls = {
            0: lambda: foo.first(lambda: out.append("first")),
            1: lambda: foo.second(lambda: out.append("second")),
            2: lambda: foo.third(lambda: out.append("third")),
        }
        _run([threading.Thread(target=calls[i]) for i in order])
        ok = out == ["first", "second", "third"]
        ok_all &= ok
    print(f"  [{'PASS' if ok_all else 'FAIL'}] Print in Order across 3 start orders")
    return ok_all


def _check_foobar():
    n = 20
    fb = FooBar(n)
    out = []
    lock = threading.Lock()
    def emit(s):
        with lock: out.append(s)
    _run([
        threading.Thread(target=lambda: fb.foo(lambda: emit("foo"))),
        threading.Thread(target=lambda: fb.bar(lambda: emit("bar"))),
    ])
    ok = out == ["foo", "bar"] * n
    print(f"  [{'PASS' if ok else 'FAIL'}] FooBar -> {''.join(out[:8])}... ({len(out)} tokens)")
    return ok


def _check_zero_even_odd():
    n = 7
    zeo = ZeroEvenOdd(n)
    out = []
    lock = threading.Lock()
    def printNumber(x):
        with lock: out.append(str(x))
    _run([
        threading.Thread(target=lambda: zeo.zero(printNumber)),
        threading.Thread(target=lambda: zeo.even(printNumber)),
        threading.Thread(target=lambda: zeo.odd(printNumber)),
    ])
    expected = "".join("0" + str(i) for i in range(1, n + 1))
    got = "".join(out)
    ok = got == expected
    print(f"  [{'PASS' if ok else 'FAIL'}] ZeroEvenOdd -> {got} (want {expected})")
    return ok


def _check_h2o():
    # 3 waters = 6 H + 3 O, shuffled arrival
    h2o = H2O()
    out = []
    lock = threading.Lock()
    def rel(sym):
        with lock: out.append(sym)
    threads = []
    for _ in range(6):
        threads.append(threading.Thread(target=lambda: h2o.hydrogen(lambda: rel("H"))))
    for _ in range(3):
        threads.append(threading.Thread(target=lambda: h2o.oxygen(lambda: rel("O"))))
    # interleave the start order to stress the barrier
    threads = [threads[i] for i in (0, 6, 1, 2, 7, 3, 4, 8, 5)]
    _run(threads)
    # every consecutive group of 3 must contain exactly 2 H and 1 O
    ok = len(out) == 9 and all(
        out[i:i+3].count("H") == 2 and out[i:i+3].count("O") == 1
        for i in range(0, 9, 3)
    )
    print(f"  [{'PASS' if ok else 'FAIL'}] H2O -> {''.join(out)} (each group of 3 = 2H+1O)")
    return ok


def _run_demos():
    _demo_race_condition()
    _demo_lock()
    _demo_rlock()
    _demo_semaphore()
    _demo_event()
    _demo_condition()
    _demo_barrier()


def _run_checks():
    print("\n=== SELF-TESTS (reference solutions should all PASS) ===")
    results = [
        _check_counter(),
        _check_print_in_order(),
        _check_foobar(),
        _check_zero_even_odd(),
        _check_h2o(),
    ]
    print(f"\n  {sum(results)}/{len(results)} checks passed.")


if __name__ == "__main__":
    _run_demos()
    _run_checks()
    print("\nNext: workbook_02_ordering.py — more signaling problems, YOUR turn to solve.")
