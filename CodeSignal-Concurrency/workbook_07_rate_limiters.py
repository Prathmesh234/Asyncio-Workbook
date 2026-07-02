"""
================================================================================
CODESIGNAL CONCURRENCY — WORKBOOK 07: RATE LIMITERS (thread-safe)
================================================================================
Difficulty: Intermediate
Model:      Python `threading` (preemptive, multi-threaded) — NOT asyncio
Primitives: Lock (protecting shared counters / timestamp deques)

Maps to INTERVIEW.MD:
    Token bucket (15), Sliding window (16), Hit counter (26, LeetCode 362 style)

HOW TO USE THIS FILE
--------------------
1. Read the PRIMER block below.
2. Read the `_demo_*` functions, then RUN the file to watch them:
       python3 workbook_07_rate_limiters.py
3. Each exercise is a SKELETON — implement the body yourself (`# YOUR CODE HERE`).
   Unlike Workbook 01, there is no reference solution folded in; you write it.
4. The `_check_*` self-tests at the bottom print PASS/FAIL. They FAIL until you
   implement the class — that's expected and fine.

THE ONE IDEA
------------
A rate limiter's entire job is: "shared mutable state (a token count, or a
deque of timestamps) is read-modified-written by every caller, from possibly
many threads at once." Same race condition as workbook_01's counter demo —
two threads can both see "1 token left", both decide to allow, and you've
over-granted. EVERY method that touches that shared state needs a Lock around
its full read-modify-write, not just the write.

TOKEN BUCKET vs WINDOW COUNTING
--------------------------------
  TOKEN BUCKET: a bucket holds up to `capacity` tokens and refills continuously
  at `refill_rate` tokens/sec. Each allowed request consumes one token. Because
  tokens accumulate while idle (up to the cap), a client that's been quiet can
  BURST up to `capacity` requests instantly, then is throttled to the steady
  refill rate. This is the industry-standard shape (AWS, Stripe, nginx) because
  it tolerates bursts without letting sustained rate exceed the target.

  FIXED / SLIDING WINDOW: count events in a trailing time period and cap it.
  A *fixed* window (e.g. "reset every :00 of the minute") can double-allow at
  the boundary (a burst at 11:59:59 + a burst at 12:00:00 = 2x the rate in
  2 seconds). A *sliding* window fixes this by keeping actual timestamps in a
  deque and evicting anything older than `window_secs` on every call — it
  always looks at the last `window_secs` of real time, not a fixed clock tick.

  Token bucket = smooth budget that refills over time.
  Sliding window = hard cap on event COUNT in a moving time period.
  Both need a Lock: token bucket protects (tokens, last_refill_ts); sliding
  window protects the timestamp deque.
================================================================================
"""

import threading
import time
from collections import deque


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  DEMO — token bucket allows a burst, then throttles to the refill rate       ║
# ╚════════════════════════════════════════════════════════════════════════════╝

def _demo_token_bucket_burst_then_throttle():
    """>>> _demo_token_bucket_burst_then_throttle()

    A hand-rolled bucket (not the exercise class) with capacity=3, refilling
    1 token every 0.05s. Shows: 3 instant allows (the burst), then a 4th call
    right away is denied (bucket empty), then after waiting for a refill
    interval it's allowed again.
    """
    print("\n--- DEMO: token bucket — burst capacity, then throttled ---")
    capacity = 3
    refill_rate = 1 / 0.05          # 1 token per 0.05s
    lock = threading.Lock()
    state = {"tokens": float(capacity), "last": time.monotonic()}

    def allow():
        with lock:
            now = time.monotonic()
            elapsed = now - state["last"]
            state["tokens"] = min(capacity, state["tokens"] + elapsed * refill_rate)
            state["last"] = now
            if state["tokens"] >= 1:
                state["tokens"] -= 1
                return True
            return False

    # Burst: first `capacity` calls all succeed instantly (bucket starts full).
    burst = [allow() for _ in range(capacity)]
    print(f"  burst of {capacity} immediate calls -> {burst}  (all True: bucket was full)")

    # Bucket is now empty — an immediate extra call is denied.
    immediate_extra = allow()
    print(f"  1 more call with no wait          -> {immediate_extra}  (False: no tokens left)")

    # Wait for ~2 tokens worth of refill, then allow() should succeed again.
    time.sleep(0.11)
    refilled = allow()
    print(f"  after waiting for a refill window -> {refilled}  (True: tokens accrued over time)")


# ══════════════════════════════════════════════════════════════════════════════
#  CHEAT SHEET — token bucket vs sliding window vs hit counter
# ══════════════════════════════════════════════════════════════════════════════
"""
┌─────────────────────┬───────────────────────────────────────────────────────┐
│ Need                │ Shape                                                  │
├─────────────────────┼───────────────────────────────────────────────────────┤
│ Smooth burst + budget│ Token bucket: tokens += elapsed * rate, cap at capacity│
│ Hard cap on COUNT    │ Sliding window: deque of timestamps, evict > window    │
│   in a moving period │ old, len(deque) < max_requests to allow                │
│ "hits in last N secs"│ Same as sliding window, but reporting a COUNT, not a   │
│                      │ True/False decision (LeetCode 362 Design Hit Counter)  │
│ Every method above   │ ALL need a Lock — shared token count / timestamp deque│
│                      │ is a read-modify-write, exactly like workbook_01's     │
│                      │ counter race.                                         │
└─────────────────────┴───────────────────────────────────────────────────────┘
"""


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  EXERCISE 1 — TokenBucket                          (primitive: Lock)         ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
Implement a thread-safe token bucket rate limiter.

    TokenBucket(capacity, refill_rate_per_sec, now=time.monotonic)
        .allow() -> bool

- The bucket starts FULL (capacity tokens).
- allow() first refills lazily: compute elapsed time since the last refill
  (using the injected `now` callable, NOT time.monotonic() directly — this is
  what lets the self-test use a fake clock), add elapsed * refill_rate_per_sec
  tokens, capped at `capacity`. Then, if >= 1 token is available, consume one
  and return True; otherwise return False.
- The whole "read elapsed -> compute new token count -> decide -> consume"
  sequence must happen under a single Lock acquisition, or two threads can
  both see "1.0 tokens available" and both get an over-grant.

Why inject `now`? So tests can advance a FAKE clock deterministically instead
of sprinkling real time.sleep() calls — same trick you'll want for any
interview timing problem.

    >>> tb = TokenBucket(capacity=3, refill_rate_per_sec=10)
    >>> [tb.allow() for _ in range(3)]   # bucket starts full
    [True, True, True]
    >>> tb.allow()                        # empty now
    False
"""

class TokenBucket:
    def __init__(self, capacity, refill_rate_per_sec, now=time.monotonic):
        """
        capacity:            max (and starting) number of tokens.
        refill_rate_per_sec: tokens added per second of elapsed time.
        now:                 zero-arg callable returning the current time
                              (inject a fake clock in tests).
        """
        # YOUR CODE HERE
        raise NotImplementedError

    def allow(self) -> bool:
        """Refill lazily based on elapsed time, then consume one token if
        available. Return True if a token was consumed, else False."""
        # YOUR CODE HERE
        raise NotImplementedError


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  EXERCISE 2 — SlidingWindowLimiter                 (primitive: Lock)         ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
Implement a thread-safe sliding-window rate limiter.

    SlidingWindowLimiter(max_requests, window_secs, now=time.monotonic)
        .allow() -> bool

- Keep a deque of timestamps (using the injected `now`) of ALLOWED requests.
- On each allow() call: evict timestamps older than `window_secs` from the
  front of the deque (they're outside the trailing window and no longer
  count). If len(deque) < max_requests, append `now()` and return True.
  Otherwise return False.
- Evict-then-check-then-append must happen under one Lock acquisition.

This differs from a FIXED window (which resets counts at clock boundaries,
e.g. every wall-clock minute): here the window is always "the trailing
`window_secs` seconds ending now", so there's no double-allow at a boundary.

    >>> w = SlidingWindowLimiter(max_requests=2, window_secs=1.0)
    >>> w.allow(); w.allow()   # True, True  (2 allowed instantly)
    >>> w.allow()              # False (cap reached within the window)
"""

class SlidingWindowLimiter:
    def __init__(self, max_requests, window_secs, now=time.monotonic):
        """
        max_requests: max allowed requests in any trailing `window_secs`.
        window_secs:  length of the trailing window, in seconds.
        now:          zero-arg callable returning the current time.
        """
        # YOUR CODE HERE
        raise NotImplementedError

    def allow(self) -> bool:
        """Evict timestamps older than window_secs, then allow (and record)
        the request if under max_requests. Return True/False."""
        # YOUR CODE HERE
        raise NotImplementedError


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  EXERCISE 3 — HitCounter  (LeetCode 362, Design Hit Counter)  (Lock)         ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
Implement a thread-safe hit counter that reports hits in the trailing 300
seconds — same underlying data structure as Exercise 2 (a deque of
timestamps), but it reports a COUNT instead of an allow/deny decision, and it
never "denies" a hit (every call to hit() is recorded).

    HitCounter(now=time.monotonic)
        .hit()       -> None      # record a hit at the current time
        .get_hits()  -> int       # hits recorded in the last 300 seconds

- hit() appends `now()` to a deque.
- get_hits() evicts timestamps older than 300s from the front of the deque,
  then returns len(deque). (Note: get_hits() advances/mutates the same
  eviction state as hit() would — both need the Lock.)
- Use the injected `now` callable everywhere instead of time.monotonic()
  directly, so tests can drive a fake clock.

    >>> hc = HitCounter()
    >>> hc.hit(); hc.hit()
    >>> hc.get_hits()   # 2, both just happened
"""

WINDOW_SECS = 300

class HitCounter:
    def __init__(self, now=time.monotonic):
        """now: zero-arg callable returning the current time."""
        # YOUR CODE HERE
        raise NotImplementedError

    def hit(self) -> None:
        """Record a hit at the current time."""
        # YOUR CODE HERE
        raise NotImplementedError

    def get_hits(self) -> int:
        """Return the number of hits in the trailing WINDOW_SECS seconds,
        evicting stale timestamps first."""
        # YOUR CODE HERE
        raise NotImplementedError


# ══════════════════════════════════════════════════════════════════════════════
#  SELF-TESTS — run the file to grade YOUR implementations. FAIL is expected
#  until you fill in the classes above; checks are wrapped so a NotImplementedError
#  prints a clean [FAIL] instead of crashing the file.
# ══════════════════════════════════════════════════════════════════════════════

class _FakeClock:
    """A controllable clock: pass `.now` as the `now=` callable, then call
    `.advance(secs)` between actions to simulate time passing without
    real sleeps. Keeps checks fast and deterministic."""
    def __init__(self, start=0.0):
        self._t = start

    def now(self):
        return self._t

    def advance(self, secs):
        self._t += secs


def _run(threads):
    for t in threads: t.start()
    for t in threads: t.join()


def _check_token_bucket():
    try:
        clock = _FakeClock()
        capacity = 5
        tb = TokenBucket(capacity=capacity, refill_rate_per_sec=1.0, now=clock.now)

        # Starts full: `capacity` allows in a row, then denied.
        allows = [tb.allow() for _ in range(capacity)]
        extra = tb.allow()
        start_ok = allows == [True] * capacity and extra is False

        # Advance fake time by 3s at rate 1/sec -> 3 tokens refilled.
        clock.advance(3.0)
        refill_allows = [tb.allow() for _ in range(3)]
        refill_extra = tb.allow()
        refill_ok = refill_allows == [True, True, True] and refill_extra is False

        # Advance far past capacity -> refill caps at `capacity`, not unbounded.
        clock.advance(1000.0)
        cap_allows = [tb.allow() for _ in range(capacity)]
        cap_extra = tb.allow()
        cap_ok = cap_allows == [True] * capacity and cap_extra is False

        # Concurrency: hammer a fresh bucket with many threads, total allowed
        # must never exceed capacity (using the real clock, no advancing).
        real_tb = TokenBucket(capacity=10, refill_rate_per_sec=0.0)
        granted = {"n": 0}
        glock = threading.Lock()
        def hammer():
            for _ in range(50):
                if real_tb.allow():
                    with glock:
                        granted["n"] += 1
        _run([threading.Thread(target=hammer) for _ in range(8)])
        never_over_ok = granted["n"] <= 10

        ok = start_ok and refill_ok and cap_ok and never_over_ok
        print(f"  [{'PASS' if ok else 'FAIL'}] TokenBucket "
              f"(start={start_ok} refill={refill_ok} cap={cap_ok} "
              f"concurrency<=capacity={never_over_ok}, granted={granted['n']})")
        return ok
    except NotImplementedError:
        print("  [FAIL] TokenBucket -> not implemented yet")
        return False
    except Exception as e:
        print(f"  [FAIL] TokenBucket -> raised {e!r}")
        return False


def _check_sliding_window():
    try:
        clock = _FakeClock()
        max_requests, window = 3, 1.0
        w = SlidingWindowLimiter(max_requests=max_requests, window_secs=window, now=clock.now)

        allows = [w.allow() for _ in range(max_requests)]
        denied = w.allow()
        start_ok = allows == [True] * max_requests and denied is False

        # Advance halfway through the window: still full, still denied.
        clock.advance(window / 2)
        still_denied = w.allow()
        mid_ok = still_denied is False

        # Advance past the full window from the FIRST allowed timestamp:
        # all 3 original timestamps should now be evicted, freeing capacity.
        clock.advance(window)  # total advance now > window since t=0
        freed = [w.allow() for _ in range(max_requests)]
        freed_ok = freed == [True] * max_requests

        # Concurrency: hammer a fresh limiter with real clock; never over-grant
        # more than max_requests within the (long) window.
        real_w = SlidingWindowLimiter(max_requests=20, window_secs=10.0)
        granted = {"n": 0}
        glock = threading.Lock()
        def hammer():
            for _ in range(10):
                if real_w.allow():
                    with glock:
                        granted["n"] += 1
        _run([threading.Thread(target=hammer) for _ in range(8)])
        never_over_ok = granted["n"] <= 20

        ok = start_ok and mid_ok and freed_ok and never_over_ok
        print(f"  [{'PASS' if ok else 'FAIL'}] SlidingWindowLimiter "
              f"(start={start_ok} mid_denied={mid_ok} freed_after_window={freed_ok} "
              f"concurrency<=cap={never_over_ok}, granted={granted['n']})")
        return ok
    except NotImplementedError:
        print("  [FAIL] SlidingWindowLimiter -> not implemented yet")
        return False
    except Exception as e:
        print(f"  [FAIL] SlidingWindowLimiter -> raised {e!r}")
        return False


def _check_hit_counter():
    try:
        clock = _FakeClock()
        hc = HitCounter(now=clock.now)

        # 3 hits at t=0.
        hc.hit(); hc.hit(); hc.hit()
        at_zero_ok = hc.get_hits() == 3

        # Advance 100s, 2 more hits (now 5 total, all within 300s).
        clock.advance(100)
        hc.hit(); hc.hit()
        at_100_ok = hc.get_hits() == 5

        # Advance so the FIRST 3 hits (at t=0) are now 300+s old, others
        # (at t=100) are still within the trailing 300s window.
        clock.advance(201)   # now at t=301: hits at t=0 are 301s old (evict),
                              # hits at t=100 are 201s old (keep)
        trailing_ok = hc.get_hits() == 2

        # Advance past everything -> 0 hits remain.
        clock.advance(1000)
        empty_ok = hc.get_hits() == 0

        # Concurrency smoke test: many threads hitting concurrently, count
        # should equal total hits recorded (real clock, short window is fine
        # since WINDOW_SECS=300 easily covers a fast test).
        real_hc = HitCounter()
        def hammer():
            for _ in range(50):
                real_hc.hit()
        _run([threading.Thread(target=hammer) for _ in range(8)])
        concurrency_ok = real_hc.get_hits() == 400

        ok = at_zero_ok and at_100_ok and trailing_ok and empty_ok and concurrency_ok
        print(f"  [{'PASS' if ok else 'FAIL'}] HitCounter "
              f"(t0={at_zero_ok} t100={at_100_ok} trailing={trailing_ok} "
              f"empty={empty_ok} concurrency={concurrency_ok})")
        return ok
    except NotImplementedError:
        print("  [FAIL] HitCounter -> not implemented yet")
        return False
    except Exception as e:
        print(f"  [FAIL] HitCounter -> raised {e!r}")
        return False


def _run_demos():
    _demo_token_bucket_burst_then_throttle()


def _run_checks():
    print("\n=== SELF-TESTS (implement the exercises above to make these PASS) ===")
    results = [
        _check_token_bucket(),
        _check_sliding_window(),
        _check_hit_counter(),
    ]
    print(f"\n  {sum(results)}/{len(results)} checks passed.")


if __name__ == "__main__":
    _run_demos()
    _run_checks()
    print("\nNext: workbook_08_data_structures.py — thread-safe KV+TTL, LRU, striped LRU.")
