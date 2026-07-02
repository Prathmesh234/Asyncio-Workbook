# Workbook 06 — Thread Pools

> **You write all the code.** Notes + tiny examples only. Practice problems are yours.

## The idea

Spawning one thread per task is wasteful. A **thread pool** keeps a fixed number of
worker threads that pull tasks off a shared queue. This is *the* pattern for "do
these 1000 things, but only N at a time."

**In an interview, use `ThreadPoolExecutor` first** unless asked to build one.

## Example: `ThreadPoolExecutor`

```python
from concurrent.futures import ThreadPoolExecutor, as_completed

with ThreadPoolExecutor(max_workers=4) as pool:
    futures = [pool.submit(do_work, item) for item in items]
    for f in as_completed(futures):
        print(f.result())
```

A pool from scratch is just: a `queue.Queue` of tasks + N worker threads looping
`task = q.get(); task()`, plus a sentinel to shut them down (Workbook 04).

---

## Practice (you code these)

1. **Thread pool from scratch** — `submit(fn)` returns a handle with `.result()`;
   `shutdown()` joins all workers. *(Tool: `queue.Queue` + worker threads)*
2. **Graceful shutdown** — stop taking new work, let running jobs finish, cancel
   the ones still queued; report completed vs cancelled counts.
3. **Ordered parallel map** — run `fn` over a list concurrently but return results
   in the **original order**. *(Tool: `ThreadPoolExecutor` + track indexes)*

---

## 2 rules

1. Reuse a fixed pool of workers; don't spawn a thread per task.
2. Always have a shutdown path (sentinel or a `shutdown` flag) so workers can exit.

*Next: Workbook 07 — rate limiters.*
