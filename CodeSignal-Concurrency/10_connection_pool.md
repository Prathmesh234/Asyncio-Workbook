# Workbook 10 — Connection Pool

> **You write all the code.** Notes + tiny examples only. Practice problems are yours.

## The idea

A **connection pool** hands out a fixed number of reusable resources (say, 5 DB
connections). If all are in use, the next caller **waits** until one is returned.

Two must-haves:
- **block when empty** (with an optional timeout),
- **always return** the connection, even if the caller crashed → use a context manager.

## Example: a pool on top of `queue.Queue`

`queue.Queue` already gives you "block until an item is available."
```python
import queue, contextlib

class Pool:
    def __init__(self, make, size):
        self._q = queue.Queue()
        for _ in range(size):
            self._q.put(make())

    @contextlib.contextmanager
    def get(self, timeout=None):
        conn = self._q.get(timeout=timeout)   # waits if none free
        try:
            yield conn
        finally:
            self._q.put(conn)                  # always return it
```
```python
with pool.get() as conn:      # clean, exception-safe usage
    conn.query(...)
```

---

## Practice (you code these)

1. **Connection pool** — `acquire()` / `release()` + a `with pool.get():` helper.
   Never hand the same connection to two callers at once.
2. **Acquire timeout** — if nothing frees up in time, fail fast (raise or return None).
3. **Graceful shutdown** — `close()` stops handing out connections, waits for
   outstanding ones to come back, then closes each exactly once.

---

## 2 rules

1. Return the resource in a `finally` (or context manager) so a crash can't leak it.
2. Blocking checkout with a timeout beats a busy-loop checking "is one free yet?".

---

## 🎉 You finished the series

Go back to **Workbook 01** and re-implement the primitives from memory, then do
timed runs of the practice problems. On interview day, remember the two jobs:
**protect shared data, and make threads wait politely.**
