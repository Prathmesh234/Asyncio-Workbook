# Workbook 04 — Producer / Consumer & Bounded Queue

> **You write all the code.** Notes + tiny examples only. Practice problems are yours.

## The idea

One thread makes work, another does it. They share a **queue**. If the queue has a
size limit (bounded), producers wait when it's full and consumers wait when it's
empty.

**In an interview, reach for `queue.Queue` first** — it's already thread-safe. Only
build your own with `Condition` if they specifically ask you to.

## Example: `queue.Queue` with a shutdown signal

A `None` "sentinel" tells the consumer to stop. **One sentinel per consumer.**
```python
import queue, threading

q = queue.Queue(maxsize=5)   # bounded: blocks producer when full

def producer():
    for item in range(10):
        q.put(item)          # blocks if full
    q.put(None)              # sentinel = "done"

def consumer():
    while True:
        item = q.get()       # blocks if empty
        if item is None:
            break
        handle(item)
```

---

## Practice (you code these)

1. **Bounded Blocking Queue** — LeetCode 1188. Build `enqueue` / `dequeue` / `size`
   yourself with `Lock` + `Condition` (no `queue.Queue`). Blocks when full/empty.
2. **Log pipeline with clean shutdown** — 1 producer, M workers, every item handled
   exactly once, every worker exits. *(Tool: one sentinel per worker)*
3. **Pub/Sub** — `subscribe()` returns a personal queue; `publish(msg)` delivers to
   all current subscribers.

---

## 2 rules

1. Default to `queue.Queue`; hand-roll only when asked.
2. Send **one sentinel per consumer** — a single `None` only stops one of them.

*Next: Workbook 05 — deadlock, and how to avoid it.*
