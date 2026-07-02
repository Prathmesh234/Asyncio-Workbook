# Workbook 02 — Making Threads Take Turns

> **You write all the code.** Notes + tiny examples only. Practice problems are yours.

## The idea

Sometimes threads must run in a **specific order** ("A before B") or **alternate**
("A, B, A, B..."). You control the order with the signaling tools from Workbook 01:
`Event`, `Semaphore`, and `Condition`.

---

## Patterns (tiny examples)

### Two Events taking turns (alternate A/B)
Each thread waits its turn, does its bit, then hands the turn to the other.
```python
a_turn = threading.Event(); a_turn.set()   # A goes first
b_turn = threading.Event()

def a():
    a_turn.wait(); a_turn.clear()   # wait my turn, then reset it
    print("A")
    b_turn.set()                    # give the turn to B
```

### `Semaphore(0)` as a one-way gate (B waits for A)
```python
gate = threading.Semaphore(0)   # starts closed

def a(): do_a(); gate.release()  # open the gate
def b(): gate.acquire(); do_b()  # wait until A opens it
```

---

## Practice (you code these)

1. **FizzBuzz Multithreaded** — LeetCode 1195. Four threads (fizz/buzz/fizzbuzz/number)
   cooperate to print the FizzBuzz sequence 1..n in order.
   *(Tool: 4 gates coordinated by the number thread, or a `Condition` + shared index)*
2. **Round-robin printer** — k threads print their ids in order 0,1,...,k-1 repeatedly.
   *(Tool: an array of `Event`s; thread i wakes thread i+1)*
3. **Interleave** — one thread prints letters, one prints numbers → `a1b2c3...`.
   *(Tool: two `Event`s taking turns)*

---

## 2 rules

1. After you wake on an Event, `clear()` it so the next round blocks again.
2. Decide up front **who starts** (set one Event before the threads run).

*Next: Workbook 03 — build the tools themselves, to see how they work inside.*
