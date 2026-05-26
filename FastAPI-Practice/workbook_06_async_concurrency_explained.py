"""
================================================================================
WORKBOOK 06 (EXPLAINER): ASYNC, CONCURRENCY & RESILIENCE PATTERNS
================================================================================
Difficulty: Medium → Medium-Hard
Format: READ-ONLY. Fully implemented and heavily commented. Read the prose
        above each concept, then the code, then run it to watch the behavior
        get verified (including timing/concurrency assertions) in main().

Run:  uv run python FastAPI-Practice/workbook_06_async_concurrency_explained.py

Concepts covered:
  1. async vs sync routes; blocking the event loop; asyncio.to_thread
  2. Concurrent fan-out with asyncio.gather (+ return_exceptions)
  3. Deadlines with asyncio.wait_for -> HTTP 504
  4. BackgroundTasks (fire-and-forget) and its durability boundary
  5. Concurrency limiting with asyncio.Semaphore
  6. StreamingResponse for large / incremental payloads
================================================================================
"""

from __future__ import annotations

import asyncio
import time
from typing import AsyncIterator

from fastapi import BackgroundTasks, FastAPI, HTTPException
from fastapi.responses import StreamingResponse

app = FastAPI(title="Workbook 06 - Async & Concurrency (explainer)")


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║   CONCEPT 1: async vs sync, BLOCKING THE LOOP, and asyncio.to_thread        ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
FastAPI runs on a single-threaded event loop per worker. How your route is
declared decides where it runs:

  * `def route(): ...`        -> FastAPI runs it in a THREADPOOL. Safe for
                                 blocking code (sync DB driver, requests, heavy
                                 CPU) because it won't freeze the loop.
  * `async def route(): ...`  -> runs ON the event loop. You MUST only await
                                 non-blocking I/O here. A blocking call inside an
                                 async route (time.sleep, requests.get, a big
                                 CPU loop) stalls EVERY concurrent request on
                                 that worker.

        async def bad():   time.sleep(2)            # ❌ freezes the whole worker
        async def good():  await asyncio.sleep(2)   # ✅ yields; others proceed

THE ESCAPE HATCH
----------------
If you're in an async route but must call blocking code, push it to a thread so
the loop stays free:

        result = await asyncio.to_thread(blocking_function, arg1, arg2)

`to_thread` schedules the function on a worker thread and gives you an awaitable.
Below, `cpu_heavy` is deliberately blocking; the async route offloads it.
"""


def cpu_heavy(n: int) -> int:
    # A deliberately BLOCKING function (sum of squares). In an async route this
    # would hog the loop — so we run it via to_thread.
    total = 0
    for i in range(n):
        total += i * i
    return total


@app.get("/compute")
async def compute(n: int = 10_000) -> dict:
    # Offload the blocking work to a thread; the loop is free meanwhile.
    result = await asyncio.to_thread(cpu_heavy, n)
    return {"n": n, "result": result}


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║   CONCEPT 2: CONCURRENT FAN-OUT WITH asyncio.gather                         ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
The headline benefit of async: fire off many independent I/O calls AT ONCE
instead of one after another. `asyncio.gather` schedules them concurrently and
waits for all:

        results = await asyncio.gather(call_a(), call_b(), call_c())
        # wall time ~= the SLOWEST call, not the sum of all of them.

`return_exceptions=True` is the resilience knob: instead of the first failure
cancelling the whole gather, each slot gets either a result OR the exception
object, so one dead dependency doesn't sink the others. You then decide how to
report partial failure (here: "degraded").
"""


async def probe(service: str, delay: float, fail: bool = False) -> dict:
    await asyncio.sleep(delay)              # simulated network latency
    if fail:
        raise RuntimeError(f"{service} unreachable")
    return {"service": service, "status": "ok"}


@app.get("/health")
async def health() -> dict:
    targets = [
        ("auth", 0.1, False),
        ("billing", 0.1, True),     # this dependency is down
        ("inventory", 0.1, False),
    ]
    # All three probes run concurrently -> ~0.1s total, not ~0.3s.
    results = await asyncio.gather(
        *(probe(name, delay, fail) for name, delay, fail in targets),
        return_exceptions=True,     # don't let billing's failure kill the rest
    )
    services, all_ok = [], True
    for (name, _, _), res in zip(targets, results):
        if isinstance(res, Exception):
            all_ok = False
            services.append({"service": name, "status": "down"})
        else:
            services.append({"service": name, "status": "ok"})
    return {"overall": "ok" if all_ok else "degraded", "services": services}


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║   CONCEPT 3: DEADLINES WITH asyncio.wait_for -> 504                         ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
Never let an upstream call hang forever — it ties up a request slot and cascades
into timeouts upstream of YOU. Bound every external call with a deadline and map
the timeout to a meaningful HTTP status (504 Gateway Timeout):

        try:
            data = await asyncio.wait_for(call_upstream(), timeout=2.0)
        except asyncio.TimeoutError:
            raise HTTPException(504, "upstream timed out")

`wait_for` cancels the underlying task when the budget is exceeded, so the slow
work doesn't keep running in the background.
"""


@app.get("/report")
async def report(delay: float = 0.05) -> dict:
    try:
        # Budget: 0.2s. delay below that succeeds; above it -> TimeoutError.
        return await asyncio.wait_for(probe("report", delay), timeout=0.2)
    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="upstream timed out")


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║   CONCEPT 4: BackgroundTasks (FIRE-AND-FORGET) + ITS LIMITS                 ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
`BackgroundTasks` lets you respond NOW and do cheap follow-up work AFTER the
response is sent (audit log, cache warm, send an email):

        @app.post("/things")
        def make(bg: BackgroundTasks):
            bg.add_task(write_audit, "...")   # runs after the response
            return {"queued": True}

KNOW THE BOUNDARY (interviewers love this):
  * It runs in the SAME process, after the response. If the process crashes
    before/while it runs, the work is LOST. It is NOT durable and NOT retried.
  * For anything that must survive restarts or needs retries/scheduling, use a
    real task queue/broker (Celery, RQ, Arq, a cloud queue) instead.

So: BackgroundTasks = best-effort side effects; task queue = guaranteed work.
"""

_AUDIT: list[str] = []


def write_audit(line: str) -> None:
    _AUDIT.append(line)  # stands in for "append to a log / emit a metric"


@app.post("/actions/{name}")
async def do_action(name: str, bg: BackgroundTasks) -> dict:
    bg.add_task(write_audit, f"performed:{name}")  # scheduled, runs post-response
    return {"scheduled": True, "name": name}


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║   CONCEPT 5: CONCURRENCY LIMITING WITH asyncio.Semaphore                    ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
Fanning out is great until you flood a fragile upstream (or your own DB) with
500 simultaneous calls. An `asyncio.Semaphore(N)` caps how many coroutines run
the guarded section at once — the rest wait their turn:

        sem = asyncio.Semaphore(3)
        async def guarded():
            async with sem:          # at most 3 run this block concurrently
                await call_upstream()

Below, `/batch` launches 10 tasks but the semaphore (limit 3) ensures no more
than 3 are "in flight" at any instant. We record the peak concurrency so the
demo can prove the cap held.
"""

_concurrency = {"current": 0, "peak": 0}
_sem = asyncio.Semaphore(3)


async def limited_unit(i: int) -> int:
    async with _sem:                       # acquire a slot (max 3 at once)
        _concurrency["current"] += 1
        _concurrency["peak"] = max(_concurrency["peak"], _concurrency["current"])
        try:
            await asyncio.sleep(0.05)       # simulate the guarded upstream call
            return i
        finally:
            _concurrency["current"] -= 1    # release the slot


@app.get("/batch")
async def batch() -> dict:
    _concurrency["current"] = 0
    _concurrency["peak"] = 0
    # 10 units, but the semaphore admits at most 3 concurrently.
    results = await asyncio.gather(*(limited_unit(i) for i in range(10)))
    return {"done": len(results), "peak_concurrency": _concurrency["peak"]}


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║   CONCEPT 6: StreamingResponse — DON'T BUFFER THE WHOLE PAYLOAD             ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
For large exports or long-running output, you don't want to build the entire
body in memory before sending. `StreamingResponse` takes a (sync or async)
generator and flushes each chunk to the client as it's produced — constant
memory, and the client starts receiving immediately:

        async def rows():
            for r in query_in_batches():
                yield serialize(r)
        return StreamingResponse(rows(), media_type="application/x-ndjson")

This is also how you'd stream LLM tokens or server-sent events. Below we stream
N plain-text lines, yielding to the loop between chunks.
"""


async def line_generator(n: int) -> AsyncIterator[str]:
    for i in range(n):
        yield f"line-{i}\n"
        await asyncio.sleep(0)  # cooperatively yield so other tasks can run


@app.get("/export")
async def export(n: int = 5) -> StreamingResponse:
    return StreamingResponse(line_generator(n), media_type="text/plain")


# ==============================================================================
# DEMO / VERIFICATION
# ==============================================================================
def main() -> None:
    from fastapi.testclient import TestClient

    print("=" * 70)
    print("WORKBOOK 06 (EXPLAINER): verifying each concept")
    print("=" * 70)
    c = TestClient(app)

    # Concept 1 — offload blocking work
    print("\n[1] async route offloads blocking CPU via asyncio.to_thread")
    r = c.get("/compute?n=1000").json()
    print(f"      /compute?n=1000 -> {r}")
    assert r["result"] == sum(i * i for i in range(1000))
    print("      OK: blocking cpu_heavy ran off the loop, correct result returned")

    # Concept 2 — concurrent fan-out
    print("\n[2] gather fan-out (concurrent + partial failure)")
    t0 = time.perf_counter()
    body = c.get("/health").json()
    elapsed = time.perf_counter() - t0
    print(f"      /health -> {body}")
    print(f"      wall time: {elapsed:.3f}s (3 probes x 0.1s each)")
    assert body["overall"] == "degraded"
    statuses = {s["service"]: s["status"] for s in body["services"]}
    assert statuses == {"auth": "ok", "billing": "down", "inventory": "ok"}
    assert elapsed < 0.25, "probes should run concurrently (~0.1s), not ~0.3s"
    print("      OK: ran concurrently; billing failure isolated as 'down'")

    # Concept 3 — deadline -> 504
    print("\n[3] wait_for deadline -> 504")
    fast = c.get("/report?delay=0.05")
    slow = c.get("/report?delay=1.0")
    print(f"      delay=0.05 -> {fast.status_code}; delay=1.0 -> {slow.status_code}")
    assert fast.status_code == 200 and slow.status_code == 504
    print("      OK: under-budget succeeds; over-budget mapped to 504")

    # Concept 4 — BackgroundTasks
    print("\n[4] BackgroundTasks fire-and-forget")
    _AUDIT.clear()
    resp = c.post("/actions/scale-up")
    print(f"      POST /actions/scale-up -> {resp.json()}; audit log: {_AUDIT}")
    assert resp.json() == {"scheduled": True, "name": "scale-up"}
    assert "performed:scale-up" in _AUDIT, "task should have run after response"
    print("      OK: responded immediately; audit written by the background task")

    # Concept 5 — semaphore concurrency cap
    print("\n[5] Semaphore concurrency limiting")
    batch = c.get("/batch").json()
    print(f"      /batch -> {batch}")
    assert batch["done"] == 10
    assert batch["peak_concurrency"] <= 3, "semaphore(3) must cap in-flight at 3"
    print("      OK: 10 units ran but never more than 3 at once")

    # Concept 6 — streaming
    print("\n[6] StreamingResponse")
    stream = c.get("/export?n=3")
    print(f"      /export?n=3 body -> {stream.text!r}")
    assert stream.text == "line-0\nline-1\nline-2\n"
    print("      OK: body streamed chunk-by-chunk")

    print("\n" + "=" * 70)
    print("All concepts verified. ✔")
    print("=" * 70)


if __name__ == "__main__":
    main()
