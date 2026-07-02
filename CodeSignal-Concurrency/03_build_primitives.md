# Workbook 03 — Build the Tools Yourself

> **You write all the code.** Notes + tiny examples only. Practice problems are yours.

## The idea

Interviewers love "implement a Semaphore/Barrier yourself." The secret: almost
every tool is built from **`Condition`** (which is just a `Lock` + a wait/notify
queue). Learn one pattern and you can build them all.

## The one pattern

```python
cond = threading.Condition()

def wait_for_something():
    with cond:
        while not ready():   # always a while
            cond.wait()
        take_it()

def make_it_ready():
    with cond:
        change_state()
        cond.notify()        # or notify_all()
```

Everything below is a variation on "wait while a counter isn't right, notify when
it changes."

---

## Practice (you code these — in order)

1. **MySemaphore** — `acquire()` / `release()` using only `Lock` + `Condition`
   (no `threading.Semaphore`). Block in `acquire` while the count is 0.
2. **MyBarrier(n)** — `wait()` blocks until n threads arrive, then releases all and
   resets for reuse.
3. **Read-Write lock** — many readers OR one writer. Give **writers priority**:
   once a writer is waiting, new readers must wait too (so writers don't starve).
4. **Future / Promise** — `set_result(v)` / `result()` where `result()` blocks
   until the value is set, then returns it. *(Tool: `Event` or `Condition`)*
5. **Once** — run a setup function exactly once no matter how many threads call it
   (double-checked locking).

---

## 2 rules

1. Every `wait()` sits in a `while`, and every state change is followed by `notify`.
2. Use `notify_all()` when different waiters are waiting for different conditions.

*Next: Workbook 04 — producer/consumer and the bounded queue.*
