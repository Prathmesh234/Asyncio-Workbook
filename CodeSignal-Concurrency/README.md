# CodeSignal Concurrency Workbooks

Simple, fundamentals-first notes for a **threading** concurrency interview. Each
workbook is a short Markdown file: the idea, tiny examples, and practice problems
**you code yourself**.

> Why threading (not asyncio)? The classic interview problems here — Print in Order,
> FooBar, Dining Philosophers, Bounded Queue — use real threads. You already know
> asyncio; this fills the gap.

## The two jobs of concurrency (the whole series in one line)

1. **Protect shared data** — put it behind a `Lock`.
2. **Make threads wait politely** — use `Event` / `Condition` / `Semaphore` /
   `Barrier`, never a busy-loop.

## Order to study

| # | File | Topic |
|---|------|-------|
| 01 | `01_primitives.md` | **Start here** — Lock, Semaphore, Event, Condition, Barrier |
| 02 | `02_ordering.md` | Making threads take turns |
| 03 | `03_build_primitives.md` | Build the tools yourself (from `Condition`) |
| 04 | `04_bounded_queue.md` | Producer/consumer & bounded queue |
| 05 | `05_deadlock.md` | Deadlock and how to avoid it |
| 06 | `06_thread_pools.md` | Thread pools |
| 07 | `07_rate_limiters.md` | Rate limiters |
| 08 | `08_thread_safe_ds.md` | Thread-safe data structures |
| 09 | `09_pipelines.md` | ⭐ Real pipelines (crawler) — closest to the real interview |
| 10 | `10_connection_pool.md` | Connection pool |

## How to use

1. Read a workbook's Markdown (5–10 min).
2. Open your own `.py` file and **write the solutions** to that workbook's practice
   problems.
3. Test them, then move on.

The `workbook_NN_*.py` files are **optional answer keys / runnable examples** — only
peek if you're stuck. Try to write everything yourself first.

Everything uses the Python standard library only (`threading`, `queue`, `time`).
