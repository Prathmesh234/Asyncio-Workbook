"""
================================================================================
CODESIGNAL CONCURRENCY — WORKBOOK 08: THREAD-SAFE DATA STRUCTURES
================================================================================
Difficulty: Core interview territory (this IS the "design a thread-safe X" round)
Model:      Python `threading` (preemptive, multi-threaded) — NOT asyncio
Primitives: Lock (mostly), the discipline of WHERE you put it

Maps to INTERVIEW.MD:
    "design a thread-safe cache", "thread-safe LRU cache", "thread-safe counter",
    "thread-safe singleton", lock striping / sharding for contention reduction.

HOW TO USE THIS FILE
--------------------
1. Read each PRIMER block.
2. Read the `_demo_*` functions, then RUN the file to watch them:
       python3 workbook_08_thread_safe_ds.py
3. Each EXERCISE below is a SKELETON — signature + docstring + hints. Implement
   it yourself (look for `# YOUR CODE HERE`). Do not skip to a solution; there
   isn't one here on purpose.
4. The `_check_*` self-tests at the bottom print PASS/FAIL against YOUR code.
   They are wrapped in try/except, so an unimplemented (`NotImplementedError`)
   exercise prints a clean [FAIL] instead of crashing the whole file.

THE ONE IDEA
------------
Every "design a thread-safe X" question reduces to the same recipe:
  1. Find the shared, mutable state (usually a dict / list / counter / OrderedDict).
  2. Wrap EVERY public method that reads-or-writes it in the SAME lock.
  3. Watch for CHECK-THEN-ACT races: `if k in d: ...` then mutate `d` a moment
     later is two operations — another thread can slip in between them. The fix
     is not "check, then lock, then act" — it's "lock, THEN check-and-act", all
     inside one critical section.
  4. Keep the critical section small: never sleep, do I/O, or call user code
     while holding the lock.
When ONE lock becomes a bottleneck (many threads serialize on it even though
they touch different keys), consider LOCK STRIPING — split below.
================================================================================
"""

import threading
import time
from collections import OrderedDict


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  PRIMER 0: CHECK-THEN-ACT — the race hiding inside "obviously safe" code      ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
    if key not in cache:        # CHECK
        cache[key] = compute()  # ACT

Between the CHECK and the ACT, another thread can run the same CHECK (also
sees "not in cache"), and now `compute()` runs twice, or worse, one thread's
write clobbers the other's. `dict.__setitem__` alone is atomic; the CHECK-then-
ACT *sequence* is not. The fix: hold ONE lock across both the check and the act.
"""

def _demo_check_then_act_race():
    """Run to SEE two threads both think they're the one initializing a key.
    >>> _demo_check_then_act_race()
    """
    print("\n--- DEMO: check-then-act race (unprotected) ---")
    cache = {}
    init_calls = {"n": 0}
    init_calls_lock = threading.Lock()

    def get_or_init(key):
        if key not in cache:                # CHECK
            time.sleep(0.001)                # widen the window so it ALWAYS shows
            with init_calls_lock:
                init_calls["n"] += 1
            cache[key] = f"value-for-{key}"  # ACT (may double-run)

    threads = [threading.Thread(target=get_or_init, args=("shared",)) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    print(f"  8 threads raced to init the SAME key -> compute() ran "
          f"{init_calls['n']} times (want 1 -> {'THIS is the bug' if init_calls['n'] > 1 else 'run again, timing-dependent'})")


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  PRIMER 1: LOCK STRIPING — shard the keyspace to cut contention              ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
A single global Lock guarding a whole dict means EVERY thread serializes on it,
even threads touching completely unrelated keys (`d["alice"]` blocks a thread
wanting `d["bob"]` for no real reason). Under high concurrency this lock becomes
the bottleneck.

LOCK STRIPING fixes this: split the keyspace into N independent "shards", each
with its OWN lock (and often its own sub-dict). `hash(key) % N` picks the shard.
Two threads hitting different shards can now run their critical sections truly
in parallel — no false contention.

Trade-offs (say these out loud in an interview):
  - Operations that need a GLOBAL view (size(), iterate-all, clear()) now need
    to take ALL shard locks (in a fixed, consistent order, to avoid deadlock)
    or accept an approximate/non-atomic answer.
  - More locks = more memory + more acquire/release overhead per op; only worth
    it when contention (not raw op cost) is the bottleneck.
  - Choose N based on expected thread count / core count, not key count.
"""

def _shard_for(key, num_shards):
    """The sharding function: same key ALWAYS maps to the same shard."""
    return hash(key) % num_shards


def _demo_lock_striping():
    """>>> _demo_lock_striping() — show the shard function + independent locks."""
    print("\n--- DEMO: lock striping concept (sharding function) ---")
    num_shards = 4
    shard_locks = [threading.Lock() for _ in range(num_shards)]
    shard_dicts = [{} for _ in range(num_shards)]

    keys = ["alice", "bob", "carol", "dave", "eve", "frank"]
    for k in keys:
        s = _shard_for(k, num_shards)
        with shard_locks[s]:
            shard_dicts[s][k] = f"data-{k}"
        print(f"  key={k!r:8} -> shard {s} (its own lock; unrelated shards never block it)")

    # Two threads hitting DIFFERENT shards can hold their locks AT THE SAME TIME —
    # that's the whole point. Prove it: both acquire simultaneously, no waiting.
    both_held = threading.Event()
    order = []
    order_lock = threading.Lock()

    def hold_shard(shard_idx, label):
        with shard_locks[shard_idx]:
            with order_lock:
                order.append(f"{label}-in")
            both_held.wait(timeout=0.2)   # wait for the other thread to also be in
            with order_lock:
                order.append(f"{label}-out")

    t_a = threading.Thread(target=hold_shard, args=(0, "A"))
    t_b = threading.Thread(target=hold_shard, args=(1, "B"))
    t_a.start(); t_b.start()
    time.sleep(0.05)
    both_held.set()                        # release both once we know both got in
    t_a.join(); t_b.join()
    concurrent = order[:2] in (["A-in", "B-in"], ["B-in", "A-in"])
    print(f"  shard 0 and shard 1 locks both held concurrently: {concurrent} "
          f"(order={order})")


# ══════════════════════════════════════════════════════════════════════════════
#  CHEAT SHEET — thread-safe data structure recipe
# ══════════════════════════════════════════════════════════════════════════════
"""
┌──────────────────────────────┬────────────────────────────────────────────────┐
│ Symptom                      │ Fix                                            │
├──────────────────────────────┼────────────────────────────────────────────────┤
│ `if k in d: ... d[k]=...`     │ one Lock around the WHOLE if/else, not just the │
│                               │ mutation                                       │
│ Need recency order + O(1) get│ OrderedDict + move_to_end() + one Lock          │
│ One lock is a hot spot        │ Lock striping: hash(key) % N shards, N locks   │
│ Need a global count/iterate  │ take ALL shard locks in a FIXED order, or accept│
│  across striped shards        │ an approximate answer                          │
│ Lazy one-time init shared by │ double-checked locking: check, lock, check again│
│  many threads (singleton)     │ before constructing                            │
└──────────────────────────────┴────────────────────────────────────────────────┘
"""


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  EXERCISE 1 — TTLCache: thread-safe KV store with expiry     (primitive: Lock)║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
Implement a thread-safe key/value store where each entry expires `ttl_secs`
after it was PUT. Expiry is LAZY: an expired entry isn't actively cleaned up by
a background thread — it's simply treated as absent the next time anyone
touches it (get, or a put that overwrites it).

    >>> c = TTLCache()
    >>> c.put("a", 1, ttl_secs=10)
    >>> c.get("a")            # -> 1   (not expired yet)
    >>> # ...10+ seconds pass...
    >>> c.get("a")            # -> None (expired, and lazily removed)

Constructor takes a `now` callable (defaults to `time.monotonic`) so tests can
inject a FAKE clock instead of sleeping — never use `time.sleep()` in the check
to simulate TTL expiry; advance a fake clock instead.

Requirements:
  - `put(key, value, ttl_secs)`: store value, record its expiry time.
  - `get(key)`: return the value if present AND not expired, else None. If
    expired, remove it from the store while you're at it (lazy cleanup).
  - Every public method's critical section must be under ONE Lock — no
    check-then-act gap between "is it expired" and "remove/return it".

# YOUR CODE HERE
"""

class TTLCache:
    def __init__(self, now=time.monotonic):
        """
        Args:
            now: zero-arg callable returning the current time (monotonic clock
                 by default; tests will pass a fake clock for determinism).
        """
        # YOUR CODE HERE
        raise NotImplementedError

    def put(self, key, value, ttl_secs):
        """Store `value` under `key`, expiring `ttl_secs` after this call."""
        # YOUR CODE HERE
        raise NotImplementedError

    def get(self, key):
        """Return the value for `key`, or None if missing or expired.
        If expired, lazily remove the entry.
        """
        # YOUR CODE HERE
        raise NotImplementedError


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  EXERCISE 2 — LRUCache: thread-safe, single lock       (primitives: Lock,    ║
# ║                                                          OrderedDict)        ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
The classic "design an LRU cache" (LeetCode 146), made thread-safe. Fixed
`capacity`. `get(key)` returns the value (or None if absent) and marks the key
as most-recently-used. `put(key, value)` inserts/updates and marks
most-recently-used; if this PUSHES the cache over capacity, evict the least-
recently-used key.

    >>> c = LRUCache(2)
    >>> c.put(1, "a"); c.put(2, "b")
    >>> c.get(1)              # -> "a"   (1 is now most-recently-used)
    >>> c.put(3, "c")          # over capacity -> evicts 2 (least recently used)
    >>> c.get(2)               # -> None  (evicted)

Idea: `collections.OrderedDict` tracks insertion/access order for you.
`move_to_end(key)` marks a key as most-recently-used; `popitem(last=False)`
pops the least-recently-used (the item at the front). ONE `threading.Lock`
guards the whole structure — every method's body is one critical section.

# YOUR CODE HERE
"""

class LRUCache:
    def __init__(self, capacity):
        # YOUR CODE HERE
        raise NotImplementedError

    def get(self, key):
        """Return the value for `key` (marking it MRU), or None if absent."""
        # YOUR CODE HERE
        raise NotImplementedError

    def put(self, key, value):
        """Insert/update `key` (marking it MRU); evict LRU key if over capacity."""
        # YOUR CODE HERE
        raise NotImplementedError

    def __len__(self):
        # YOUR CODE HERE
        raise NotImplementedError


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  EXERCISE 3 — StripedLRU: shard the LRU across N locks   (primitive: lock    ║
# ║                                                            striping)         ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
A single-lock LRUCache (Exercise 2) serializes ALL threads through one lock,
even ones touching unrelated keys. Reduce contention: partition the keyspace
into `num_shards` independent LRUCache instances, each with capacity
`capacity // num_shards` and each guarded by its OWN lock (reuse your
Exercise 2 LRUCache as each shard — don't reinvent eviction logic here).

    >>> s = StripedLRU(capacity=100, num_shards=4)   # 4 shards of 25 each
    >>> s.put("alice", 1)      # hashes into exactly one shard
    >>> s.get("alice")         # -> 1, only that shard's lock is touched

Use the SAME sharding idea as PRIMER 1 (`hash(key) % num_shards`) to route each
key to its shard deterministically. get/put on a key only ever touch that
key's shard — a thread working shard 0 never blocks a thread working shard 1.

# YOUR CODE HERE
"""

class StripedLRU:
    def __init__(self, capacity, num_shards):
        """
        Args:
            capacity: TOTAL capacity across all shards (split ~evenly).
            num_shards: number of independent LRUCache shards.
        """
        # YOUR CODE HERE
        raise NotImplementedError

    def _shard_index(self, key):
        """Route `key` to a shard index in [0, num_shards)."""
        # YOUR CODE HERE
        raise NotImplementedError

    def get(self, key):
        # YOUR CODE HERE
        raise NotImplementedError

    def put(self, key, value):
        # YOUR CODE HERE
        raise NotImplementedError


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║  EXERCISE 4 — AtomicCounter + lazy Singleton   (primitive: Lock, double-     ║
# ║                                                  checked locking)            ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
Part A: AtomicCounter — inc()/dec()/value under a lock. Trivial but everyone
should be able to write this cold in an interview.

Part B: Singleton — lazily construct exactly ONE instance, shared by however
many threads race to call `Singleton.instance()` first, even 20 of them at once.

Naive lazy init:
    if cls._instance is None:        # CHECK  (unlocked! racy)
        cls._instance = cls()        # ACT
...is the check-then-act bug from PRIMER 0. Naive FIX (lock every call) works
but pays lock overhead on every single call forever, even after the singleton
already exists.

DOUBLE-CHECKED LOCKING: check WITHOUT the lock first (fast path once
initialized); only if that check says "not yet built" do you take the lock and
check AGAIN (because another thread may have built it while you were waiting
for the lock) before constructing.

    if cls._instance is None:            # check 1 (no lock — fast path)
        with cls._lock:
            if cls._instance is None:    # check 2 (locked — safe path)
                cls._instance = cls()
    return cls._instance

# YOUR CODE HERE
"""

class AtomicCounter:
    def __init__(self, start=0):
        # YOUR CODE HERE
        raise NotImplementedError

    def inc(self, amount=1):
        # YOUR CODE HERE
        raise NotImplementedError

    def dec(self, amount=1):
        # YOUR CODE HERE
        raise NotImplementedError

    @property
    def value(self):
        # YOUR CODE HERE
        raise NotImplementedError


class Singleton:
    _instance = None
    _lock = threading.Lock()

    def __init__(self):
        # Side note for the check: real code would guard against accidental
        # direct construction, but that's not the point of this exercise.
        pass

    @classmethod
    def instance(cls):
        """Return the one shared instance, constructing it on first call.
        Must use double-checked locking (see PRIMER above) — safe under any
        number of concurrently racing threads.
        """
        # YOUR CODE HERE
        raise NotImplementedError


# ══════════════════════════════════════════════════════════════════════════════
#  SELF-TESTS — run the file to grade YOUR implementations.
# ══════════════════════════════════════════════════════════════════════════════

def _run(threads):
    for t in threads:
        t.start()
    for t in threads:
        t.join()


def _check_ttl_cache():
    try:
        clock = {"t": 0.0}
        cache = TTLCache(now=lambda: clock["t"])

        cache.put("a", 1, ttl_secs=5)
        ok1 = cache.get("a") == 1                 # not expired yet

        clock["t"] = 5.1                          # advance PAST ttl
        ok2 = cache.get("a") is None               # now expired -> None

        ok3 = cache.get("missing") is None         # never existed

        # concurrent puts/gets on many keys shouldn't corrupt state or crash
        clock["t"] = 0.0
        errors = []

        def hammer(i):
            try:
                for j in range(200):
                    key = f"k{(i + j) % 10}"
                    cache.put(key, i * 1000 + j, ttl_secs=1000)
                    cache.get(key)
            except Exception as e:
                errors.append(e)

        _run([threading.Thread(target=hammer, args=(i,)) for i in range(8)])
        ok4 = not errors

        ok = ok1 and ok2 and ok3 and ok4
        print(f"  [{'PASS' if ok else 'FAIL'}] TTLCache -> fresh={ok1} expired={ok2} "
              f"missing={ok3} concurrent_no_crash={ok4}")
        return ok
    except Exception as e:
        print(f"  [FAIL] TTLCache -> raised {type(e).__name__}: {e}")
        return False


def _check_lru_cache():
    try:
        # classic LC 146 trace
        c = LRUCache(2)
        c.put(1, "a")
        c.put(2, "b")
        trace = [c.get(1)]           # -> "a", 1 becomes MRU
        c.put(3, "c")                 # capacity 2 -> evicts 2 (LRU)
        trace.append(c.get(2))        # -> None (evicted)
        c.put(4, "d")                 # evicts 1 (LRU now, since 3 then 1 was used... )
        trace.append(c.get(1))        # -> None (evicted)
        trace.append(c.get(3))        # -> "c"
        trace.append(c.get(4))        # -> "d"
        expected = ["a", None, None, "c", "d"]
        ok_trace = trace == expected

        # hammer with threads: no crash, size stays <= capacity
        cap = 16
        lru = LRUCache(cap)
        errors = []

        def hammer(i):
            try:
                for j in range(300):
                    k = (i * 37 + j) % 50
                    lru.put(k, (i, j))
                    lru.get(k)
            except Exception as e:
                errors.append(e)

        _run([threading.Thread(target=hammer, args=(i,)) for i in range(8)])
        ok_concurrent = not errors and len(lru) <= cap

        ok = ok_trace and ok_concurrent
        print(f"  [{'PASS' if ok else 'FAIL'}] LRUCache -> trace={trace} "
              f"(want {expected}); concurrent_ok={ok_concurrent} len={len(lru)}<= {cap}")
        return ok
    except Exception as e:
        print(f"  [FAIL] LRUCache -> raised {type(e).__name__}: {e}")
        return False


def _check_striped_lru():
    try:
        num_shards = 4
        s = StripedLRU(capacity=40, num_shards=num_shards)

        # basic correctness: put then get round-trips, per-key routing is stable
        for i in range(20):
            s.put(f"key{i}", i)
        ok_roundtrip = all(s.get(f"key{i}") == i for i in range(20))

        # same key always routes to the same shard
        ok_stable = all(
            s._shard_index(f"key{i}") == s._shard_index(f"key{i}")
            for i in range(20)
        )

        # different keys landing in different shards can be accessed truly
        # concurrently -- prove two distinct shard indices exist among our keys
        # and instrument concurrent hold of two different shards' locks.
        shard_ids = {s._shard_index(f"key{i}") for i in range(20)}
        ok_multi_shard = len(shard_ids) >= 2

        # concurrency hammer: no crash
        errors = []

        def hammer(i):
            try:
                for j in range(200):
                    k = f"k{(i + j) % 20}"
                    s.put(k, i * 1000 + j)
                    s.get(k)
            except Exception as e:
                errors.append(e)

        _run([threading.Thread(target=hammer, args=(i,)) for i in range(8)])
        ok_concurrent = not errors

        ok = ok_roundtrip and ok_stable and ok_multi_shard and ok_concurrent
        print(f"  [{'PASS' if ok else 'FAIL'}] StripedLRU -> roundtrip={ok_roundtrip} "
              f"stable_routing={ok_stable} multi_shard={ok_multi_shard} "
              f"concurrent_ok={ok_concurrent} (shards used={sorted(shard_ids)})")
        return ok
    except Exception as e:
        print(f"  [FAIL] StripedLRU -> raised {type(e).__name__}: {e}")
        return False


def _check_atomic_counter_and_singleton():
    try:
        # AtomicCounter: exact under contention
        counter = AtomicCounter()

        def bump():
            for _ in range(10_000):
                counter.inc()
            for _ in range(3_000):
                counter.dec()

        _run([threading.Thread(target=bump) for _ in range(6)])
        expected = 6 * (10_000 - 3_000)
        ok_counter = counter.value == expected

        # Singleton: all 20 threads see the SAME object
        Singleton._instance = None   # reset for a clean test run
        seen = []
        seen_lock = threading.Lock()

        def grab():
            inst = Singleton.instance()
            with seen_lock:
                seen.append(inst)

        _run([threading.Thread(target=grab) for _ in range(20)])
        ok_singleton = len(seen) == 20 and all(x is seen[0] for x in seen)

        ok = ok_counter and ok_singleton
        print(f"  [{'PASS' if ok else 'FAIL'}] AtomicCounter -> {counter.value} "
              f"(want {expected}); Singleton identical across 20 threads: {ok_singleton}")
        return ok
    except Exception as e:
        print(f"  [FAIL] AtomicCounter/Singleton -> raised {type(e).__name__}: {e}")
        return False


def _run_demos():
    _demo_check_then_act_race()
    _demo_lock_striping()


def _run_checks():
    print("\n=== SELF-TESTS (implement the exercises above to make these PASS) ===")
    results = [
        _check_ttl_cache(),
        _check_lru_cache(),
        _check_striped_lru(),
        _check_atomic_counter_and_singleton(),
    ]
    print(f"\n  {sum(results)}/{len(results)} checks passed.")


if __name__ == "__main__":
    _run_demos()
    _run_checks()
    print("\nNext: workbook_09_... — apply this recipe to a producer/consumer pipeline.")
