# Workbook 08 — Thread-Safe Data Structures

> **You write all the code.** Notes + tiny examples only. Practice problems are yours.

## The idea

To make a data structure thread-safe: **wrap every public method's body in a
`Lock`.** The main trap is *check-then-act* — `if key in cache: ...` then change it
— because another thread can slip in between the check and the act. Keep both under
one lock.

## Example: LRU cache (shape only)

`OrderedDict` remembers insertion order, so it's perfect for LRU.
```python
from collections import OrderedDict

class LRUCache:
    def get(self, key):
        with self.lock:
            if key not in self.data:
                return None
            self.data.move_to_end(key)   # mark as recently used
            return self.data[key]
```

**Lock striping** (advanced): if one lock is a bottleneck, split keys across N
locks (shard by `hash(key) % N`) so different keys don't block each other.

---

## Practice (you code these)

1. **TTL cache** — `put(key, val, ttl)`, `get(key)` returns None if expired.
   *(Tool: store an expiry time; check it on read. Inject a `now` clock for tests)*
2. **LRU cache** — LeetCode 146. `get` / `put` with capacity; evict least-recently-used.
3. **Striped LRU** — same, but shard keys across N locks to reduce contention.
4. **Atomic counter + singleton** — a counter safe under contention, and a
   `get_instance()` that returns the same object to all threads (double-checked locking).

---

## 2 rules

1. Put check-then-act sequences under **one** lock, not two separate locks.
2. `list.append` / `dict[k]=v` are atomic; `+=`, and "check then modify" are not.

*Next: Workbook 09 — putting it together in real pipelines.*
