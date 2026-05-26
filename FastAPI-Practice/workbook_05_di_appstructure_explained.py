"""
================================================================================
WORKBOOK 05 (EXPLAINER): DEPENDENCY INJECTION DEEP-DIVE, APP STRUCTURE & LIFESPAN
================================================================================
Difficulty: Medium → Medium-Hard
Format: READ-ONLY. Unlike workbooks 01-04, this file is FULLY IMPLEMENTED and
        heavily commented. Read the explanation above each concept, then the
        code, then run it to watch the behavior get verified in main().

Run:  uv run python FastAPI-Practice/workbook_05_di_appstructure_explained.py

Concepts covered:
  1. Sub-dependencies and the dependency tree
  2. Per-request dependency CACHING (and how to opt out)
  3. `yield` dependencies — setup/teardown (the DB-session pattern)
  4. Class-based dependencies that carry configuration/state
  5. `dependency_overrides` — swapping dependencies in tests
  6. `APIRouter` — splitting an app into modules + router-level dependencies
  7. `lifespan` + `app.state` — process-lifetime shared resources
================================================================================
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Annotated, AsyncIterator

from fastapi import APIRouter, Depends, FastAPI, Header, HTTPException


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║   CONCEPT 1 & 2: SUB-DEPENDENCIES + PER-REQUEST CACHING                     ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
A dependency is just a callable FastAPI runs for you. The important — and often
mis-stated in interviews — detail is what happens when dependencies DEPEND ON
each other.

THE DEPENDENCY TREE
-------------------
`get_connection` and `get_audit_log` both need a DB URL, so they each declare
`Depends(get_db_url)`. FastAPI builds the whole tree for a request:

        work() endpoint
        ├── Depends(get_connection) ──┐
        ├── Depends(get_audit_log) ───┼──> Depends(get_db_url)
        └── Depends(get_db_url) ──────┘

CACHING (the key rule)
----------------------
Within a SINGLE request, FastAPI calls each dependency AT MOST ONCE and reuses
the result everywhere it appears in the tree. So even though three places ask
for `get_db_url`, it runs exactly once per request. That's why dependencies are
cheap to compose — you don't pay for `get_settings` ten times.

Opt out with `Depends(fn, use_cache=False)` when you genuinely want a fresh
value each time (rare — e.g. a per-call nonce).
"""

# A counter so the demo can PROVE get_db_url runs once per request.
_db_url_calls = {"count": 0}


def get_db_url() -> str:
    _db_url_calls["count"] += 1
    return "postgres://demo/app"


def get_connection(url: Annotated[str, Depends(get_db_url)]) -> str:
    # In real life you'd open/borrow a pooled connection here.
    return f"conn({url})"


def get_audit_log(url: Annotated[str, Depends(get_db_url)]) -> str:
    return f"audit({url})"


app_cache = FastAPI()


@app_cache.get("/work")
def work(
    conn: Annotated[str, Depends(get_connection)],
    audit: Annotated[str, Depends(get_audit_log)],
    url: Annotated[str, Depends(get_db_url)],
) -> dict:
    # get_db_url appears 3x in this tree but runs ONCE — see db_url_calls == 1.
    return {"conn": conn, "audit": audit, "url": url,
            "db_url_calls": _db_url_calls["count"]}


@app_cache.get("/work-no-cache")
def work_no_cache(
    a: Annotated[str, Depends(get_db_url, use_cache=False)],
    b: Annotated[str, Depends(get_db_url, use_cache=False)],
) -> dict:
    # use_cache=False forces a fresh call each time -> 2 calls here.
    return {"db_url_calls": _db_url_calls["count"]}


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║   CONCEPT 3: `yield` DEPENDENCIES — SETUP / TEARDOWN                        ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
A dependency can `yield` a value instead of returning it. Code BEFORE the yield
runs as setup; code AFTER (best put in `finally`) runs as teardown once the
response is produced — even if the endpoint raised. This is FastAPI's analogue
of a context manager, and it's THE canonical database-session pattern:

    def get_session():
        session = SessionLocal()          # setup
        try:
            yield session                 # hand it to the endpoint
        finally:
            session.close()               # teardown — ALWAYS runs

The guarantee that teardown runs on error is what prevents leaked connections.
Below, even the endpoint that raises a 500 still gets its session closed.
"""

# Capture created sessions so the demo can assert they were all closed.
_created_sessions: list["Session"] = []


class Session:
    def __init__(self) -> None:
        self.closed = False
        self.queries: list[str] = []

    def execute(self, sql: str) -> str:
        self.queries.append(sql)
        return f"result({sql})"

    def close(self) -> None:
        self.closed = True


def get_session() -> AsyncIterator[Session]:  # sync generators work too
    session = Session()
    _created_sessions.append(session)
    try:
        yield session
    finally:
        session.close()  # runs whether the endpoint succeeded or raised


app_session = FastAPI()


@app_session.get("/query")
def run_query(session: Annotated[Session, Depends(get_session)]) -> dict:
    return {"result": session.execute("SELECT 1")}


@app_session.get("/boom")
def boom(session: Annotated[Session, Depends(get_session)]) -> dict:
    session.execute("SELECT 1")
    raise HTTPException(status_code=500, detail="kaboom")  # teardown still runs


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║   CONCEPT 4: CLASS-BASED DEPENDENCIES (carry config/state)                  ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
A class is callable, so it can BE a dependency in two ways:

  * Depends(SomeClass)          -> FastAPI calls SomeClass(...) using its
                                   __init__ params as request inputs.
  * Depends(instance)           -> the INSTANCE is called (__call__). This lets
                                   you bake configuration into the instance once
                                   and reuse it as a parametrized dependency.

Below, `Paginator(max_limit=50)` is constructed ONCE at import time with its
policy; each request calls `paginator(limit=..., offset=...)` and the instance
clamps `limit` to its configured maximum. Great for "the same dependency, but
configured differently in different parts of the app".
"""


class Paginator:
    def __init__(self, max_limit: int = 100) -> None:
        self.max_limit = max_limit  # configuration baked into the instance

    def __call__(self, limit: int = 10, offset: int = 0) -> dict:
        # limit/offset come from the query string; clamp to the policy.
        return {"limit": min(limit, self.max_limit), "offset": max(offset, 0)}


paginate = Paginator(max_limit=50)  # one configured instance, reused everywhere

app_cls = FastAPI()


@app_cls.get("/items")
def list_items(page: Annotated[dict, Depends(paginate)]) -> dict:
    # Request /items?limit=1000 -> limit is clamped to 50 by the instance.
    return page


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║   CONCEPT 5: dependency_overrides (TESTING)                                 ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
The single biggest reason to inject things (DB URL, settings, auth, clients) as
dependencies: in tests you can REPLACE any of them without monkeypatching.

    app.dependency_overrides[get_db_url] = lambda: "sqlite:///:memory:"
    ... run requests against a fake ...
    app.dependency_overrides.clear()   # always clean up

FastAPI looks up the override map first; if a dependency is present there, the
fake is used instead of the real callable — recursively, anywhere in the tree.
We reuse app_cache from Concept 1 to show get_db_url being swapped.
"""
# (Demonstrated in main() — no new routes needed.)


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║   CONCEPT 6: APIRouter — MODULAR APP STRUCTURE                              ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
Real apps don't put 80 routes in one file. `APIRouter` is a mini-app you define
in its own module and then mount onto the main app:

    # products/routes.py
    router = APIRouter(prefix="/v1/products", tags=["products"],
                       dependencies=[Depends(verify_key)])
    @router.get("")        -> serves GET /v1/products
    @router.get("/{id}")   -> serves GET /v1/products/{id}

    # main.py
    app.include_router(router)

Two things to note:
  * prefix/tags apply to every route in the router (tags group them in /docs).
  * `dependencies=[...]` on the router runs for EVERY route in it — perfect for
    "all product routes require an API key". These dependencies run for their
    side effects (auth check); their return value is discarded.
"""


def verify_key(x_api_key: Annotated[str | None, Header()] = None) -> None:
    # Router-level guard: runs before every route in the router below.
    if x_api_key != "secret-key":
        raise HTTPException(status_code=401, detail="missing/invalid api key")


products_router = APIRouter(
    prefix="/v1/products",
    tags=["products"],
    dependencies=[Depends(verify_key)],  # applied to all routes in this router
)

_PRODUCTS = {1: {"id": 1, "name": "widget"}, 2: {"id": 2, "name": "gadget"}}


@products_router.get("")
def list_products() -> list:
    return list(_PRODUCTS.values())


@products_router.get("/{product_id}")
def get_product(product_id: int) -> dict:
    if product_id not in _PRODUCTS:
        raise HTTPException(status_code=404, detail="product not found")
    return _PRODUCTS[product_id]


app_router = FastAPI()
app_router.include_router(products_router)  # mount the module's routes


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║   CONCEPT 7: lifespan + app.state — PROCESS-LIFETIME RESOURCES              ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
Some resources (an HTTP client, a connection pool, an ML model) should be
created ONCE when the process boots and closed when it shuts down — never per
request. `lifespan` is an async context manager that brackets the app's serving
period; store shared objects on `app.state`:

    @asynccontextmanager
    async def lifespan(app):
        app.state.client = httpx.AsyncClient(base_url="https://upstream")  # startup
        yield                                                              # serve
        await app.state.client.aclose()                                    # shutdown

    app = FastAPI(lifespan=lifespan)

Endpoints then reuse `app.state.client`. Below we use a tiny FAKE async client
so the file runs with no network; the real version is the httpx snippet above.
The TestClient context manager (`with TestClient(app) as c:`) is what triggers
startup and shutdown, so the demo can observe both.
"""


class FakeAsyncClient:
    """Stand-in for httpx.AsyncClient so this file needs no network."""

    def __init__(self) -> None:
        self.closed = False

    async def get(self, path: str) -> dict:
        return {"path": path, "status": 200}

    async def aclose(self) -> None:
        self.closed = True


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    app.state.client = FakeAsyncClient()  # ---- startup: build shared resource ----
    yield                                 # ---- app serves requests here ----
    await app.state.client.aclose()       # ---- shutdown: clean up ----


app_life = FastAPI(lifespan=lifespan)


@app_life.get("/proxy")
async def proxy() -> dict:
    # Reuse the ONE shared client for every request.
    return await app_life.state.client.get("/upstream")


# ==============================================================================
# DEMO / VERIFICATION — run to watch each concept's behavior get checked.
# ==============================================================================
def main() -> None:
    from fastapi.testclient import TestClient

    print("=" * 70)
    print("WORKBOOK 05 (EXPLAINER): verifying each concept")
    print("=" * 70)

    # Concept 1 & 2 — sub-dependencies + caching
    print("\n[1/2] Sub-dependencies + per-request caching")
    _db_url_calls["count"] = 0
    c = TestClient(app_cache)
    body = c.get("/work").json()
    print(f"      /work -> {body}")
    assert body["db_url_calls"] == 1, "get_db_url should be cached -> 1 call/request"
    _db_url_calls["count"] = 0
    body2 = c.get("/work-no-cache").json()
    print(f"      /work-no-cache -> {body2}  (use_cache=False forces re-runs)")
    assert body2["db_url_calls"] == 2, "use_cache=False should call it twice"
    print("      OK: cached once normally; twice with use_cache=False")

    # Concept 3 — yield dependency teardown (even on error)
    print("\n[3] yield dependency setup/teardown")
    _created_sessions.clear()
    cs = TestClient(app_session)
    ok = cs.get("/query")
    print(f"      /query -> {ok.json()}")
    boom = cs.get("/boom")
    print(f"      /boom  -> status {boom.status_code} (endpoint raised)")
    assert boom.status_code == 500
    assert all(s.closed for s in _created_sessions), "every session must be closed"
    print(f"      OK: {len(_created_sessions)} sessions created, all closed "
          "(teardown ran even on the 500)")

    # Concept 4 — class-based dependency clamps limit
    print("\n[4] Class-based dependency carrying config")
    ci = TestClient(app_cls)
    clamped = ci.get("/items?limit=1000&offset=-5").json()
    print(f"      /items?limit=1000&offset=-5 -> {clamped}")
    assert clamped == {"limit": 50, "offset": 0}, "should clamp to configured policy"
    print("      OK: Paginator(max_limit=50) clamped limit 1000 -> 50, offset -5 -> 0")

    # Concept 5 — dependency_overrides
    print("\n[5] dependency_overrides (testing)")
    app_cache.dependency_overrides[get_db_url] = lambda: "sqlite:///:memory:"
    overridden = c.get("/work").json()
    print(f"      /work with override -> url={overridden['url']!r}")
    assert "sqlite" in overridden["url"], "override should replace the real dep"
    app_cache.dependency_overrides.clear()  # always clean up
    print("      OK: get_db_url swapped for a fake, then cleared")

    # Concept 6 — APIRouter + router-level dependency
    print("\n[6] APIRouter + router-level dependency")
    cr = TestClient(app_router)
    no_key = cr.get("/v1/products")
    with_key = cr.get("/v1/products", headers={"x-api-key": "secret-key"})
    one = cr.get("/v1/products/1", headers={"x-api-key": "secret-key"})
    print(f"      no key       -> {no_key.status_code}")
    print(f"      with key     -> {with_key.status_code} {with_key.json()}")
    print(f"      /v1/products/1 -> {one.json()}")
    assert no_key.status_code == 401, "router dependency should block missing key"
    assert with_key.status_code == 200 and len(with_key.json()) == 2
    assert one.json()["name"] == "widget"
    print("      OK: prefix /v1/products applied; router dep guards every route")

    # Concept 7 — lifespan + app.state
    print("\n[7] lifespan + app.state shared resource")
    with TestClient(app_life) as cl:        # entering the block = startup
        captured = app_life.state.client
        proxied = cl.get("/proxy").json()
        print(f"      during serving: client exists, /proxy -> {proxied}")
        assert isinstance(captured, FakeAsyncClient) and captured.closed is False
    # exiting the block = shutdown
    assert captured.closed is True, "client must be closed on shutdown"
    print("      OK: client built at startup, reused per request, closed at shutdown")

    print("\n" + "=" * 70)
    print("All concepts verified. ✔")
    print("=" * 70)


if __name__ == "__main__":
    main()
