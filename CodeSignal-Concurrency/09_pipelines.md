# Workbook 09 — Real Concurrent Pipelines

> **You write all the code.** Notes + tiny examples only. Practice problems are yours.
>
> ⭐ **This mirrors Anthropic's own example: "build a web crawler, then make it
> multi-threaded, then filter the data."** Practice this one hardest.

## The idea

Most real concurrency problems are the same shape:

1. a **queue of work** to do,
2. a **pool of workers** taking from it (Workbook 06),
3. a **shared "seen" set / results dict** protected by a `Lock`,
4. a way to know when you're **done** (queue drained).

## Example: fan out, collect results

```python
from concurrent.futures import ThreadPoolExecutor

with ThreadPoolExecutor(max_workers=5) as pool:
    results = list(pool.map(fetch, urls))   # runs fetches in parallel
```

For a crawler, guard the visited set so each URL is processed once:
```python
with lock:
    if url in seen:
        continue
    seen.add(url)
```

---

## Practice (you code these)

1. **Multi-threaded web crawler** — LeetCode 1242. Crawl all URLs on the same host,
   each visited once, using a worker pool + a thread-safe `seen` set.
2. **Parallel downloader with retries** — fetch many URLs concurrently; retry a few
   times on failure; return results (or None if it keeps failing).
3. **Scatter-gather with per-host limit** — fetch many URLs but never more than K at
   once **per host**. *(Tool: a dict of `Semaphore`s keyed by host)*
4. **DAG scheduler** — run tasks that depend on each other; a task starts only after
   its dependencies finish; run independent tasks in parallel.

---

## 2 rules

1. Protect the shared `seen`/`results` collection with a lock (or use a thread-safe queue).
2. Cap concurrency (pool size or `Semaphore`) — don't launch unbounded threads.

*Next: Workbook 10 — connection pools.*
