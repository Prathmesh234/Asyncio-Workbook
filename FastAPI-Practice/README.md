# FastAPI Practice Workbooks

4 progressive workbooks for a **backend / infrastructure** coding interview, in
the same fill-in-the-blanks style as the asyncio workbooks. Each file explains
the concepts up top (the `PRIMER`), then gives scaffolding with
`# YOUR CODE HERE` markers for you to implement.

## Workbook Progression

| Workbook | Difficulty | Topics |
|----------|------------|--------|
| 01 `routing_params_bodies` | Easy-Medium | App + routes, path params, query params (defaults/optional), Pydantic request bodies, status codes |
| 02 `pydantic_models` | Easy-Medium → Medium | Types & defaults, optional fields, `Field` constraints, nested models, a custom `field_validator`, `response_model` |
| 03 `dependencies_crud` | Medium | Dependency injection with `Depends`, a reusable pagination dependency, in-memory CRUD (create/list/read/delete), 404 handling |
| 04 `errors_async` | Medium | `HTTPException` (404/400), `async def` endpoints, `asyncio.gather` concurrency, a custom exception handler |

### Explainer workbooks (read-only — full code + commentary, nothing to fill in)

| Workbook | Difficulty | Topics |
|----------|------------|--------|
| 05 `di_appstructure_explained` | Medium → Medium-Hard | sub-dependencies + per-request caching, `yield` dependencies (teardown/session), class-based dependencies, `dependency_overrides`, `APIRouter` + router-level deps, `lifespan` + `app.state` |
| 06 `async_concurrency_explained` | Medium → Medium-Hard | blocking the event loop + `asyncio.to_thread`, `gather` fan-out (+ `return_exceptions`), `wait_for` deadlines → 504, `BackgroundTasks` + durability boundary, `asyncio.Semaphore` concurrency limiting, `StreamingResponse` |

Workbooks 01-04 are fill-in-the-blanks practice. Workbooks 05-06 are **fully
implemented and heavily commented** — read the prose above each concept, then
the code; run the file to watch each concept's behavior get verified (with
timing/concurrency assertions) in `main()`.

## Running a Workbook

Each workbook is **self-testing** — it spins up a `TestClient` in `main()` and
checks your implementation. No pytest needed.

```bash
# from the repo root
uv run python FastAPI-Practice/workbook_01_routing_params_bodies.py
```

Unimplemented questions print `FAILED: ...`; fill in the `# YOUR CODE HERE`
blocks until every question prints `PASSED!`.

## Dependencies

Tracked in the project's `pyproject.toml` (added via `uv add`): `fastapi`,
`uvicorn[standard]`, `httpx` (powers `TestClient`). Pydantic v2 comes with
FastAPI. Run anything with `uv run` and it uses the project environment.

## How to use these for interview prep

1. Read the `PRIMER` block at the top of the file — it frames each concept the
   way an interviewer would.
2. Implement one question at a time; re-run to see it flip to `PASSED!`.
3. Be ready to explain *why* (e.g. "when do you use `async def`?", "why raise
   `HTTPException` instead of returning an error dict?").

Suggested order: 01 → 02 → 03 → 04. You can also serve any workbook's `app`
under uvicorn and poke at the auto-generated docs at `/docs`:

```bash
uv run uvicorn FastAPI-Practice.workbook_03_dependencies_crud:app --reload
```
