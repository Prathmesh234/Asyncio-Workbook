# Workbook 05 — Deadlock (and How to Avoid It)

> **You write all the code.** Notes + tiny examples only. Practice problems are yours.

## The idea

**Deadlock** = two threads each hold a lock the other needs, so both wait forever.

Classic setup: thread 1 grabs lock A then wants B; thread 2 grabs B then wants A.

## The fix: always grab locks in the same order

If everyone acquires locks in one agreed order, the circular wait can't happen.
```python
# both threads lock the LOWER-id lock first, then the higher one
first, second = sorted([lock_a, lock_b], key=id)
with first:
    with second:
        do_work()
```

Other fixes: `lock.acquire(timeout=...)` and back off, or limit how many threads
can reach for resources at once (a `Semaphore`).

---

## Practice (you code these)

1. **Dining Philosophers** — LeetCode 1226. 5 philosophers, 5 forks; no deadlock,
   no starvation.
   *(Tool: pick the lower-numbered fork first — OR let at most 4 reach at once)*
2. **Traffic Light** — LeetCode 1279. Two roads cross; only one road green at a
   time; a car crosses only on green.
   *(Tool: a single `Lock` guarding "which road is green")*

---

## 2 rules

1. When you hold more than one lock, acquire them in a **consistent order** everywhere.
2. Hold locks for as short a time as possible.

*Next: Workbook 06 — thread pools.*
