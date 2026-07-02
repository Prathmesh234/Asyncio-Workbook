# Workbook 01 — Threading Primitives (the basics)

> **You write all the code.** This is just notes + one tiny example per tool.
> Read top to bottom once, then try the practice problems yourself.

## The one thing to understand first

Threads can be **paused between any two lines** of your code. So `count += 1` is
dangerous — it's really *read, add, write*, and two threads can read the same old
value and both write the same new one. One update is lost.

**Fix: put shared data behind a lock.**

```python
import threading

count = 0
lock = threading.Lock()

def add_one():
    global count
    with lock:        # only one thread inside at a time
        count += 1    # now safe
```

That's the whole game: **protect shared data**, and **make threads wait politely**
(never a `while not ready: pass` busy-loop). The tools below do those two jobs.

---

## The 6 tools (one example each)

### 1. `Lock` — one thread at a time
Use it to protect any shared variable.
```python
with lock:
    balance += amount
```

### 2. `RLock` — a Lock the same thread can take twice
Only needed when a locked method calls another locked method on the same object.
Otherwise just use `Lock`.
```python
rlock = threading.RLock()   # re-entrant: same thread won't deadlock itself
```

### 3. `Semaphore(n)` — let at most *n* threads through
Great for "only 3 at a time." `Semaphore(0)` starts closed and opens when another
thread calls `.release()` — that's how one thread waits for another.
```python
sem = threading.Semaphore(3)
with sem:
    do_work()     # never more than 3 at once
```

### 4. `Event` — a simple on/off signal
One thread waits, another flips it on. Good for "go!" or "we're done."
```python
event = threading.Event()
event.wait()      # blocks until...
event.set()       # ...someone sets it (from another thread)
```

### 5. `Condition` — wait until something becomes true
The one pattern to memorize (note the **`while`**, never `if`):
```python
with cond:
    while not ready():     # re-check after every wake
        cond.wait()        # sleeps and releases the lock
    take_the_item()

with cond:                 # the other thread:
    make_ready()
    cond.notify()          # wake one waiter
```

### 6. `Barrier(n)` — everyone waits for the group
All *n* threads stop at the line until the last one arrives, then all go together.
```python
barrier = threading.Barrier(3)
barrier.wait()    # blocks until 3 threads reach here
```

---

## Quick reference

| I want to... | Use |
|---|---|
| Protect a shared variable | `Lock` |
| Allow only N at once | `Semaphore(N)` |
| Send a one-time "go" signal | `Event` |
| Wait until a condition is true | `Condition` |
| Make N threads meet up | `Barrier(N)` |

---

## Practice (you code these — start with #1)

Do them in order. Each one teaches one tool. Google the LeetCode number for the
full statement; the hint tells you which tool to reach for.

1. **Thread-safe counter** — 4 threads each `increment()` 100k times; final value
   must be exactly 400,000. *(Tool: `Lock`)*
2. **Print in Order** — LeetCode 1114. Three threads, force output `first second third`.
   *(Tool: `Event`)*
3. **FooBar** — LeetCode 1115. Two threads alternate to print `foobarfoobar...`.
   *(Tool: two `Event`s taking turns)*
4. **Print Zero Even Odd** — LeetCode 1116. Print `0102030405...`.
   *(Tool: three `Semaphore`s)*
5. **Building H2O** — LeetCode 1117. Group threads into 2 H + 1 O.
   *(Tool: `Semaphore` + `Barrier`)*

---

## 3 rules that prevent most bugs

1. Touch shared data **only** inside `with lock:`.
2. Never busy-wait — block on `Event` / `Condition` / `Semaphore` instead.
3. `Condition` waits always go in a `while`, never an `if`.

*Next: Workbook 02 — using these tools to make threads take turns.*
