"""
================================================================================
CODESIGNAL CONCURRENCY — WORKBOOK 02: ORDERING & SIGNALING  (practice)
================================================================================
Difficulty: Foundational -> intermediate (same primitives, new shapes)
Model:      Python `threading` (preemptive, multi-threaded) — NOT asyncio
Primitives: Event, Semaphore, Condition (Lock/Barrier show up again in WB05+)

Maps to INTERVIEW.MD:
    FizzBuzz Multithreaded (1195) — item 6
    ...plus two original ordering drills (round-robin, interleave) that stress
    the SAME two patterns from more angles than the canonical LeetCode set.

WB01 already fully solved Print in Order (1114), FooBar Alternately (1115),
Print Zero Even Odd (1116), and Building H2O (1117) — go re-read those if any
of "gate with a Semaphore(0)" or "ping-pong with a pair of Events" feels shaky.
This workbook does NOT re-solve those. It gives you THREE NEW problems built
from the exact same primitives, so you practice recognizing the pattern
yourself instead of reading it solved.

HOW TO USE THIS FILE
--------------------
1. Read the PRIMER blocks below — they are a fast recap, not a first lesson.
2. Run the file to watch the `_demo_*` functions:
       python3 "CodeSignal-Concurrency/workbook_02_ordering.py"
3. Each EXERCISE is a skeleton: a docstring with the problem + a hint (the
   PRIMITIVE to reach for, not the code), then `# YOUR CODE HERE` and a
   `raise NotImplementedError`. Delete the raise, write your solution.
4. The `_check_*` self-tests at the bottom will print [FAIL] until you
   implement each exercise correctly, then flip to [PASS]. They cannot hang —
   every check runs your threads as daemons with a time budget, so a buggy
   deadlock prints [FAIL] instead of freezing your terminal.

THE ONE IDEA (recap from WB01)
-------------------------------
Every "print things in a specific order across threads" problem reduces to:
  - a thread must be made to WAIT until it's its turn (never busy-wait), and
  - the thread whose turn just ended must SIGNAL whose turn is next.
Events and Semaphores(0) are gates: closed until someone else opens them.
Conditions generalize this to "wait until some predicate on shared state is
true," which scales better once you have more than 2-3 parties.
================================================================================
"""

import threading
import time


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  PRIMER 1 (RECAP): TURN-PASSING WITH A PAIR OF EVENTS                        ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
Two parties, strictly alternating: A goes, then B, then A, then B, ...
Give each party its OWN Event ("my turn"). Exactly one starts set(). Each side:
    my_turn.wait()      # block until it's my turn
    my_turn.clear()     # close my own gate again for next time
    ...do the work...
    other_turn.set()    # open the other side's gate

This is FooBar's pattern (WB01, LC 1115) — recapped here because Exercise 3
below (AlphaNum) is the exact same shape with a stop condition instead of an
infinite ping-pong.
"""

def _demo_event_pingpong():
    """>>> _demo_event_pingpong()"""
    print("\n--- RECAP: pair-of-Events ping-pong (turn-passing) ---")
    n = 3
    ping_turn = threading.Event()
    pong_turn = threading.Event()
    ping_turn.set()                      # ping goes first
    out = []

    def ping():
        for _ in range(n):
            ping_turn.wait()
            ping_turn.clear()
            out.append("ping")
            pong_turn.set()

    def pong():
        for _ in range(n):
            pong_turn.wait()
            pong_turn.clear()
            out.append("pong")
            ping_turn.set()

    t1 = threading.Thread(target=ping)
    t2 = threading.Thread(target=pong)
    t1.start(); t2.start()
    t1.join(); t2.join()
    print(f"  {''.join(out)}  (expected {'pingpong' * n})")


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  PRIMER 2 (RECAP): A GATE BUILT FROM Semaphore(0)                            ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
Semaphore(0) starts with zero permits, so acquire() on it always blocks — until
someone else calls release(). That makes it a one-shot (or repeatable) gate:
"thread B may not proceed past this line until thread A says so." Unlike Event,
a Semaphore gate can be opened exactly N times (release() N times lets N
acquire()s through), which is handy when you need to hand off a turn repeatedly
without an explicit clear()/set() dance. This is the backbone of ZeroEvenOdd
and H2O in WB01, and of FizzBuzz Multithreaded below.
"""

def _demo_semaphore_gate():
    """>>> _demo_semaphore_gate()"""
    print("\n--- RECAP: Semaphore(0) as a one-way gate ---")
    gate_b = threading.Semaphore(0)       # closed: b can't proceed until a opens it
    out = []

    def a():
        out.append("a-work")
        time.sleep(0.02)
        out.append("a-done")
        gate_b.release()                  # open the gate for b

    def b():
        gate_b.acquire()                  # blocks here until a() releases
        out.append("b-work-after-a")

    t1 = threading.Thread(target=a)
    t2 = threading.Thread(target=b)
    t2.start(); t1.start()                # start b FIRST — it still waits correctly
    t1.join(); t2.join()
    print(f"  order = {out}  (b always after a, regardless of start order)")


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  PRIMER 3 (NEW SHAPE): CONDITION-BASED ROUND-ROBIN AMONG N THREADS           ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
The Event/Semaphore ping-pong patterns above are naturally 2-party. Once you
have K > 2 parties taking turns in a fixed cycle, a pair (or array) of gates
still works, but a single Condition guarding one shared "whose turn" variable
is often cleaner: ONE lock, ONE wait/notify queue, one piece of shared state.

    with cond:
        while turn != my_id:
            cond.wait()          # not my turn yet — sleep
        ...do my turn...
        turn = (turn + 1) % k
        cond.notify_all()        # wake everyone; only the correct thread proceeds

`notify_all()` here is intentionally wasteful-but-simple: every waiter wakes,
re-checks its `while`, and all but one go straight back to sleep. That's the
whole point of `while` over `if` (see WB01 PRIMER 5) — it's what makes waking
the wrong threads harmless instead of a bug.

Exercise 1 and Exercise 2 below can BOTH be built this way (one Condition +
shared index), or with an array of Events/Semaphores (one gate per party) like
the primers above. Either is a legitimate interview answer — pick whichever
you can write bug-free under pressure.
"""

def _demo_condition_round_robin():
    """>>> _demo_condition_round_robin() — 3 threads take turns via one Condition."""
    print("\n--- DEMO: Condition-based round-robin among 3 threads ---")
    k, rounds = 3, 2
    cond = threading.Condition()
    state = {"turn": 0}
    out = []

    def worker(i):
        for _ in range(rounds):
            with cond:
                while state["turn"] != i:
                    cond.wait()
                out.append(i)
                state["turn"] = (state["turn"] + 1) % k
                cond.notify_all()

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(k)]
    for t in threads: t.start()
    for t in threads: t.join()
    expected = [i % k for i in range(k * rounds)]
    print(f"  order = {out}  (expected {expected})")


# ══════════════════════════════════════════════════════════════════════════════
#  GENERALIZING THE PATTERN — what changes per problem
# ══════════════════════════════════════════════════════════════════════════════
"""
┌───────────────────┬────────────────────────────────────────────────────────┐
│ Problem shape      │ What differs from the primers above                    │
├───────────────────┼────────────────────────────────────────────────────────┤
│ FizzBuzz (4 roles) │ 4 gates instead of 2-3; a "dispatcher" (number thread   │
│                    │ or shared index) decides WHICH ONE gate opens each i   │
│ Round-robin (k)    │ same as PRIMER 3, but k is a parameter, not fixed at 3 │
│ Alpha/Num interleave│ same as PRIMER 1, but bounded (n turns) not infinite  │
└───────────────────┴────────────────────────────────────────────────────────┘
"""


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  EXERCISE 1 — FizzBuzz Multithreaded  (LeetCode 1195)                        ║
# ║  primitive: 4 gates (Semaphores or Events) or a Condition + shared index      ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
Four threads share one FizzBuzz(n) instance and call, in some arbitrary start
order:
    fizz(printFizz)           call printFizz()      when i % 3 == 0 and i % 5 != 0
    buzz(printBuzz)           call printBuzz()      when i % 5 == 0 and i % 3 != 0
    fizzbuzz(printFizzBuzz)   call printFizzBuzz()  when i % 15 == 0
    number(printNumber)       call printNumber(i)   otherwise

Across ALL FOUR threads combined, exactly one of the four print* calls must
fire for each i = 1, 2, ..., n, IN ORDER. Two threads must never both act on
the same i, and no i may be skipped.

Hint: this is Print-in-Order / ZeroEvenOdd (WB01) generalized from 2-3 branches
to 4. Give each method its own gate (Semaphore(0) or Event), all closed. Track
the current i as shared state. On each step, decide which ONE gate is correct
for that i and open only that gate; whichever thread wakes prints, then
advances i and opens the next correct gate. A single Condition with
`while self._i > my_turn_marker: cond.wait()`-style predicates (or simply a
shared `self._i` guarded by the condition's lock, with each method checking
"is it my kind of number?" before consuming it) also works and avoids juggling
4 separate primitives.

    >>> fb = FizzBuzz(15)
    >>> # 4 threads call fb.fizz/.buzz/.fizzbuzz/.number concurrently
    >>> # combined output (in i-order) == "1 2 fizz 4 buzz fizz 7 8 fizz buzz
    >>> #                                  11 fizz 13 14 fizzbuzz"
"""

class FizzBuzz:
    def __init__(self, n):
        self.n = n
        # YOUR CODE HERE
        # e.g. self._cond = threading.Condition(); self._i = 1

    def fizz(self, printFizz):
        """Call printFizz() once for every i in 1..n where i % 3 == 0 and i % 5 != 0."""
        # YOUR CODE HERE
        raise NotImplementedError("FizzBuzz.fizz not implemented")

    def buzz(self, printBuzz):
        """Call printBuzz() once for every i in 1..n where i % 5 == 0 and i % 3 != 0."""
        # YOUR CODE HERE
        raise NotImplementedError("FizzBuzz.buzz not implemented")

    def fizzbuzz(self, printFizzBuzz):
        """Call printFizzBuzz() once for every i in 1..n where i % 15 == 0."""
        # YOUR CODE HERE
        raise NotImplementedError("FizzBuzz.fizzbuzz not implemented")

    def number(self, printNumber):
        """Call printNumber(i) once for every i in 1..n not covered above."""
        # YOUR CODE HERE
        raise NotImplementedError("FizzBuzz.number not implemented")


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  EXERCISE 2 — Round-Robin Printer                                            ║
# ║  primitive: array of k Events/Semaphores, or one Condition + shared turn     ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
k threads, with ids 0..k-1, each call `turn(i, printId)` on a shared
RoundRobin(k, rounds) instance. Collectively (across all k threads) they must
print their own id, in strict order 0, 1, 2, ..., k-1, 0, 1, ..., for exactly
`rounds` full cycles — k * rounds prints total, thread i's id appearing at
every position congruent to i (mod k).

Hint: generalize PRIMER 3. Either keep a shared `turn` counter guarded by one
Condition (`while turn % k != i: cond.wait()`), or allocate an array of k
gates `gates[0..k-1]` (Events or Semaphores(0)) where thread i waits on
`gates[i]` and, after printing, opens `gates[(i + 1) % k]`. Exactly one gate
(`gates[0]`) must start OPEN so thread 0 can go first on round 0.

    >>> rr = RoundRobin(k=3, rounds=2)
    >>> # threads 0, 1, 2 each call rr.turn(their_id, printId)
    >>> # combined output == [0, 1, 2, 0, 1, 2]
"""

class RoundRobin:
    def __init__(self, k, rounds):
        self.k = k
        self.rounds = rounds
        # YOUR CODE HERE

    def turn(self, i, printId):
        """
        Thread i calls this exactly once. It must internally loop `rounds`
        times, each iteration waiting for thread i's turn in the 0..k-1 cycle
        before calling printId() (no arguments — printId itself already knows
        which thread it belongs to).
        """
        # YOUR CODE HERE
        raise NotImplementedError("RoundRobin.turn not implemented")


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  EXERCISE 3 — Ordered Interleave (AlphaNum)                                  ║
# ║  primitive: two-Event ping-pong (bounded, not infinite)                      ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
Two threads share an AlphaNum(n) instance:
    alpha(printChar)   must call printChar(ch) for ch = 'a', 'b', 'c', ...
                       (the first n lowercase letters), in order
    digit(printNum)    must call printNum(i) for i = 1, 2, ..., n, in order

The combined, interleaved output must read "a1b2c3...": letter i is always
immediately followed by digit i+1, e.g. for n=3: "a1b2c3".

Hint: this is PRIMER 1's ping-pong, but (a) alpha always goes first each round
and (b) both sides stop after exactly n iterations instead of running forever.
Reuse the "my_turn.wait() / clear() / work / other_turn.set()" shape.

    >>> an = AlphaNum(3)
    >>> # one thread calls an.alpha(printChar), another calls an.digit(printNum)
    >>> # combined output == "a1b2c3"
"""

class AlphaNum:
    def __init__(self, n):
        self.n = n
        # YOUR CODE HERE

    def alpha(self, printChar):
        """Call printChar(ch) for the first self.n lowercase letters, in order."""
        # YOUR CODE HERE
        raise NotImplementedError("AlphaNum.alpha not implemented")

    def digit(self, printNum):
        """Call printNum(i) for i in 1..self.n, in order."""
        # YOUR CODE HERE
        raise NotImplementedError("AlphaNum.digit not implemented")


# ══════════════════════════════════════════════════════════════════════════════
#  SELF-TESTS — run the file to grade YOUR implementations.
#  Unsolved exercises print [FAIL]; correct ones flip to [PASS].
# ══════════════════════════════════════════════════════════════════════════════

def _start_join(fns, timeout=2.0):
    """
    Run each zero-arg callable in its own DAEMON thread, wait up to `timeout`
    seconds total, then return regardless of whether they finished.

    This exists so an unsolved (or buggily deadlocked) exercise can NEVER hang
    this file: daemon threads don't block process exit, and the time budget
    bounds how long a check waits. Exceptions raised inside a thread (e.g. the
    `raise NotImplementedError` in a skeleton) do NOT normally propagate to the
    caller, so we catch them ourselves and hand them back.

    Returns: (all_finished: bool, errors: list[Exception])
    """
    errors = []
    errors_lock = threading.Lock()

    def wrap(fn):
        try:
            fn()
        except Exception as e:
            with errors_lock:
                errors.append(e)

    threads = [threading.Thread(target=wrap, args=(fn,), daemon=True) for fn in fns]
    start = time.time()
    for t in threads:
        t.start()
    for t in threads:
        remaining = max(0.0, timeout - (time.time() - start))
        t.join(remaining)
    finished = all(not t.is_alive() for t in threads)
    return finished, errors


def _check_fizzbuzz():
    try:
        n = 15
        fb = FizzBuzz(n)
        out = []
        lock = threading.Lock()

        def emit(s):
            with lock:
                out.append(s)

        fns = [
            lambda: fb.fizz(lambda: emit("fizz")),
            lambda: fb.buzz(lambda: emit("buzz")),
            lambda: fb.fizzbuzz(lambda: emit("fizzbuzz")),
            lambda: fb.number(lambda x: emit(str(x))),
        ]
        finished, errors = _start_join(fns, timeout=2.0)
        if errors:
            raise errors[0]
        if not finished:
            raise TimeoutError("threads did not finish in time (deadlock?)")

        expected = []
        for i in range(1, n + 1):
            if i % 15 == 0:
                expected.append("fizzbuzz")
            elif i % 3 == 0:
                expected.append("fizz")
            elif i % 5 == 0:
                expected.append("buzz")
            else:
                expected.append(str(i))
        ok = out == expected
        print(f"  [{'PASS' if ok else 'FAIL'}] FizzBuzz Multithreaded -> {' '.join(out)}")
        return ok
    except Exception as e:
        print(f"  [FAIL] FizzBuzz Multithreaded: {e}")
        return False


def _check_round_robin():
    try:
        k, rounds = 4, 3
        rr = RoundRobin(k, rounds)
        out = []
        lock = threading.Lock()

        def emit(i):
            with lock:
                out.append(i)

        fns = [(lambda i=i: rr.turn(i, lambda: emit(i))) for i in range(k)]
        finished, errors = _start_join(fns, timeout=2.0)
        if errors:
            raise errors[0]
        if not finished:
            raise TimeoutError("threads did not finish in time (deadlock?)")

        expected = [i % k for i in range(k * rounds)]
        ok = out == expected
        print(f"  [{'PASS' if ok else 'FAIL'}] RoundRobin -> {out} (want {expected})")
        return ok
    except Exception as e:
        print(f"  [FAIL] RoundRobin: {e}")
        return False


def _check_alpha_num():
    try:
        n = 5
        an = AlphaNum(n)
        out = []
        lock = threading.Lock()

        def emit(s):
            with lock:
                out.append(s)

        fns = [
            lambda: an.alpha(lambda ch: emit(ch)),
            lambda: an.digit(lambda x: emit(str(x))),
        ]
        finished, errors = _start_join(fns, timeout=2.0)
        if errors:
            raise errors[0]
        if not finished:
            raise TimeoutError("threads did not finish in time (deadlock?)")

        expected = "".join(chr(ord("a") + i) + str(i + 1) for i in range(n))
        got = "".join(out)
        ok = got == expected
        print(f"  [{'PASS' if ok else 'FAIL'}] AlphaNum -> {got} (want {expected})")
        return ok
    except Exception as e:
        print(f"  [FAIL] AlphaNum: {e}")
        return False


def _run_demos():
    _demo_event_pingpong()
    _demo_semaphore_gate()
    _demo_condition_round_robin()


def _run_checks():
    print("\n=== SELF-TESTS (implement the exercises above to flip these to PASS) ===")
    results = [
        _check_fizzbuzz(),
        _check_round_robin(),
        _check_alpha_num(),
    ]
    print(f"\n  {sum(results)}/{len(results)} checks passed.")


if __name__ == "__main__":
    _run_demos()
    _run_checks()
    print("\nNext: workbook_03 — build the primitives themselves from scratch "
          "(Semaphore-from-Condition, Barrier, RW-lock, Future, Once).")
