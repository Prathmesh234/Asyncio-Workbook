"""
================================================================================
WORKBOOK 04: ERROR HANDLING & ASYNC BASICS
================================================================================
Difficulty: Medium
Topics: HTTPException (404/400), async def endpoints, asyncio.sleep,
        concurrent calls with asyncio.gather, a custom exception handler

Prerequisites: Workbooks 01-03 (and the asyncio gather workbook helps here).

Learning Objectives:
- Return correct error statuses by raising HTTPException
- Validate input in the handler and reject it with 400
- Write async endpoints and understand when async helps
- Run independent awaits concurrently with asyncio.gather
- Turn a domain exception into a clean JSON response with a handler
================================================================================
"""
from __future__ import annotations

from email import message

import asyncio

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║                    PRIMER: ERRORS & ASYNC IN FASTAPI                        ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
RAISING HTTP ERRORS
===================
To send an error response, RAISE HTTPException — don't return an error dict.
## this is pretty important  - always raise HTTPException

    raise HTTPException(status_code=404, detail="user not found")
    # -> response: 404  {"detail": "user not found"}

Common codes you'll use:
    400 Bad Request   - the client sent something invalid you caught yourself
    404 Not Found     - the resource doesn't exist
    409 Conflict      - duplicate / state conflict

(FastAPI already returns 422 automatically when a body/param fails Pydantic
validation — you only raise HTTPException for YOUR business rules.)


SYNC vs ASYNC ENDPOINTS
=======================
    @app.get("/a")
    def sync_route():           # runs in a threadpool — fine for blocking code
        ...

    @app.get("/b")
    async def async_route():    # runs on the event loop
        await asyncio.sleep(1)  # use AWAITABLE I/O here, never time.sleep()

Rule of thumb: if you `await` things (async DB, httpx, asyncio.sleep), use
`async def`. The big win of async is doing several awaits CONCURRENTLY.


CONCURRENCY WITH gather (your asyncio muscle memory)
====================================================
Two independent calls, done at the same time instead of one-after-another:

    a, b = await asyncio.gather(fetch_a(), fetch_b())
    #   total time ~= max(a_time, b_time), NOT a_time + b_time

If each fetch sleeps 0.1s, sequential awaits take ~0.2s but gather takes ~0.1s.


A CUSTOM EXCEPTION HANDLER
==========================
Map your own exception type to a consistent response shape, app-wide:

    class NotFound(Exception):
        def __init__(self, what): self.what = what

    @app.exception_handler(NotFound)
    async def handle_not_found(request, exc):
        return JSONResponse(status_code=404, content={"missing": exc.what})

Now any route can just `raise NotFound("user")` and get the same clean 404.
================================================================================
"""

app = FastAPI(title="Workbook 04 - Errors & Async")


# A fake user table (provided)
_USERS = {1: "Alice", 2: "Bob"}


# ==============================================================================
# QUESTION 1: Raise a 404
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Look up a user by id; if it doesn't exist, return a proper 404."

REQUIREMENTS:
Complete `get_user` (GET /users/{user_id}):
- If user_id not in _USERS -> raise HTTPException(404, detail="user not found").
- Otherwise return {"id": user_id, "name": _USERS[user_id]}.

EXPECTED BEHAVIOR:
GET /users/1    -> {"id": 1, "name": "Alice"}
GET /users/99   -> 404  {"detail": "user not found"}
"""


@app.get("/users/{user_id}")
def get_user(user_id: int) -> dict:
    if user_id not in _USERS:
        raise HTTPException(status_code=404, detail="user not found")
    return {"id": user_id, "name": _USERS[user_id]}


# ==============================================================================
# QUESTION 2: Validate Input -> 400
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Reject a bad business input yourself with a 400 (not everything is a Pydantic
type error)."

REQUIREMENTS:
Complete `divide` (GET /divide). Two int query params: `a` and `b`.
- If b == 0 -> raise HTTPException(400, detail="cannot divide by zero").
- Otherwise return {"result": a / b}.

EXPECTED BEHAVIOR:
GET /divide?a=10&b=2   -> {"result": 5.0}
GET /divide?a=10&b=0   -> 400  {"detail": "cannot divide by zero"}
"""


@app.get("/divide")
def divide(a: int, b: int) -> dict:
    if b == 0:
        raise HTTPException(status_code=400, detail="cannot divide by zero")
    return {"result": a / b}


# ==============================================================================
# QUESTION 3: An Async Endpoint
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Write an async endpoint that awaits some I/O (simulated with asyncio.sleep)."

REQUIREMENTS:
Complete `slow_ping` (GET /ping) as an ASYNC function:
- `await asyncio.sleep(0.05)` to simulate I/O.
- Return {"pong": True}.

EXPECTED BEHAVIOR:
GET /ping  -> {"pong": true}
"""


@app.get("/ping")
async def slow_ping() -> dict:
    await asyncio.sleep(0.05)
    return {"pong": True}


# ==============================================================================
# QUESTION 4: Concurrency with gather
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Fetch two independent things at once. Show you can run awaits concurrently."

A fake async fetch (provided):
"""


async def fetch_metric(name: str) -> int:
    await asyncio.sleep(0.1)        # pretend this is a network call
    return len(name)               # some "metric"


"""
REQUIREMENTS:
Complete `combined` (GET /combined) as an ASYNC function:
- Call fetch_metric("cpu") and fetch_metric("memory") CONCURRENTLY using
  asyncio.gather (NOT one await then the other).
- Return {"cpu": <result>, "memory": <result>}.

PERFORMANCE NOTE: each fetch sleeps 0.1s. Done concurrently the endpoint takes
~0.1s, not ~0.2s — the test asserts it finishes under 0.18s.

EXPECTED BEHAVIOR:
GET /combined  -> {"cpu": 3, "memory": 6}
"""


@app.get("/combined")
async def combined() -> dict:
    task1 = asyncio.create_task(fetch_metric("cpu"))
    task2 = asyncio.create_task(fetch_metric("memory"))
    cpu_res, memory_res = await asyncio.gather(task1, task2)
    return {"cpu": cpu_res, "memory": memory_res}


# ==============================================================================
# QUESTION 5: A Custom Exception Handler
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Give your domain errors a consistent JSON shape instead of ad-hoc responses."

A domain exception (provided):
"""


class OutOfStock(Exception):
    def __init__(self, sku: str) -> None:
        self.sku = sku


"""
REQUIREMENTS:
1. Complete the handler `handle_out_of_stock(request, exc)` (already registered)
   to return JSONResponse(status_code=409,
   content={"error": "out_of_stock", "sku": exc.sku}).
2. Complete `buy` (GET /buy/{sku}): if sku == "soldout", raise OutOfStock(sku);
   otherwise return {"bought": sku}.

EXPECTED BEHAVIOR:
GET /buy/widget   -> {"bought": "widget"}
GET /buy/soldout  -> 409  {"error": "out_of_stock", "sku": "soldout"}
"""


@app.exception_handler(OutOfStock)
async def handle_out_of_stock(request: Request, exc: OutOfStock) -> JSONResponse:
    return JSONResponse(
        status_code=409,
        content={"error": "out_of_stock", "sku": exc.sku}
    )


@app.get("/buy/{sku}")
def buy(sku: str) -> dict:
    if sku == "soldout":
        raise OutOfStock(sku)
    return {"bought": sku}


# ==============================================================================
# MAIN - Test Your Solutions
# ==============================================================================
def main() -> None:
    import time
    from fastapi.testclient import TestClient

    print("=" * 60)
    print("WORKBOOK 04: Testing Your Solutions")
    print("=" * 60)
    client = TestClient(app)

    # Q1
    print("\n[Q1] Raise a 404...")
    try:
        assert client.get("/users/1").json() == {"id": 1, "name": "Alice"}
        r = client.get("/users/99")
        assert r.status_code == 404 and r.json() == {"detail": "user not found"}
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Q2
    print("\n[Q2] Validate input -> 400...")
    try:
        assert client.get("/divide?a=10&b=2").json() == {"result": 5.0}
        r = client.get("/divide?a=10&b=0")
        assert r.status_code == 400 and r.json() == {"detail": "cannot divide by zero"}
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Q3
    print("\n[Q3] Async endpoint...")
    try:
        assert client.get("/ping").json() == {"pong": True}
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Q4
    print("\n[Q4] Concurrency with gather...")
    try:
        t0 = time.perf_counter()
        r = client.get("/combined")
        elapsed = time.perf_counter() - t0
        assert r.json() == {"cpu": 3, "memory": 6}, r.text
        assert elapsed < 0.18, f"not concurrent (took {elapsed:.3f}s)"
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Q5
    print("\n[Q5] Custom exception handler...")
    try:
        assert client.get("/buy/widget").json() == {"bought": "widget"}
        r = client.get("/buy/soldout")
        assert r.status_code == 409, r.text
        assert r.json() == {"error": "out_of_stock", "sku": "soldout"}
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    print("\n" + "=" * 60)
    print("Workbook 04 Complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
