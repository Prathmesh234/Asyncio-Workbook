# Workbook 07 — Rate Limiters

> **You write all the code.** Notes + tiny examples only. Practice problems are yours.

## The idea

A rate limiter answers "am I allowed to do this action right now?" without exceeding
a limit. Two common designs:

- **Token bucket** — you have a bucket of tokens that refills over time; each action
  spends one. Allows short bursts, then throttles.
- **Sliding window** — count actions in the last N seconds; allow only up to a cap.

Both share state across threads, so **every method needs a `Lock`**.

## Example: token bucket (shape only)

```python
class TokenBucket:
    def allow(self):
        with self.lock:
            self._refill()          # add tokens based on elapsed time
            if self.tokens >= 1:
                self.tokens -= 1
                return True
            return False
```

*Tip: pass a `now=time.monotonic` function in, so tests can use a fake clock.*

---

## Practice (you code these)

1. **Token bucket** — `allow()` returns True/False; refills lazily by elapsed time.
2. **Sliding window** — at most `max_requests` in any `window_secs`.
   *(Tool: a `deque` of timestamps; drop old ones)*
3. **Hit counter** — LeetCode 362 style. `hit()` records now; `get_hits()` returns
   hits in the last 300 seconds.

---

## 2 rules

1. Lock the whole check-and-update — reading tokens then spending them is one step.
2. Use a monotonic clock (`time.monotonic`), not wall-clock time.

*Next: Workbook 08 — thread-safe data structures.*
