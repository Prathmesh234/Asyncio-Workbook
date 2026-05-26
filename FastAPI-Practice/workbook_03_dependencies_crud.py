"""
================================================================================
WORKBOOK 03: DEPENDENCY INJECTION & IN-MEMORY CRUD
================================================================================
Difficulty: Medium
Topics: Dependency injection with Depends, reusable parameter dependencies,
        building a small CRUD resource over an in-memory store, 404 handling

Prerequisites: Workbooks 01-02.

Learning Objectives:
- Factor shared logic into a dependency and inject it with Depends
- Build a reusable "common query params" dependency (pagination)
- Implement Create / Read / List / Delete over a dict-backed store
- Return 404 when a resource doesn't exist
================================================================================
"""

from __future__ import annotations

from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel, Field


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║                   PRIMER: DEPENDENCIES & A CRUD RESOURCE                    ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
WHAT IS A DEPENDENCY?
=====================
Just a function FastAPI calls FOR you, passing the result into your endpoint.
You "declare a need"; FastAPI "satisfies" it.

    def common_params(limit: int = 10, offset: int = 0) -> dict:
        return {"limit": limit, "offset": offset}

    @app.get("/items")
    def list_items(params: Annotated[dict, Depends(common_params)]):
        # params is whatever common_params returned
        ...

WHY BOTHER? You write the limit/offset logic ONCE and reuse it across every
list endpoint, instead of repeating the same query params everywhere. (It also
makes endpoints easy to test — you can swap the dependency out.)

    @app.get("/a")  -> Depends(common_params) ┐
    @app.get("/b")  -> Depends(common_params) ┼─> one definition, reused
    @app.get("/c")  -> Depends(common_params) ┘


THE Annotated SHORTHAND
=======================
    Annotated[dict, Depends(common_params)]
means: "the type is dict, and it's produced by Depends(common_params)."
You can alias it to keep signatures tidy:
    CommonParams = Annotated[dict, Depends(common_params)]


CRUD OVER AN IN-MEMORY STORE
============================
"CRUD" = Create, Read, Update, Delete. Here the "database" is just a dict:

    _DB: dict[str, dict] = {}          # id -> record

    POST   /notes        -> create  (insert, return 201 + the record)
    GET    /notes        -> list    (return all, with pagination)
    GET    /notes/{id}   -> read    (return one, or 404 if missing)
    DELETE /notes/{id}   -> delete  (remove, or 404 if missing)

RETURNING 404
=============
When the id isn't there, don't return None — raise:
    raise HTTPException(status_code=404, detail="note not found")
FastAPI turns that into a proper 404 JSON response: {"detail": "note not found"}.
================================================================================
"""

app = FastAPI(title="Workbook 03 - DI & CRUD")


# ==============================================================================
# QUESTION 1: A Simple Dependency
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Move a small piece of shared logic into a dependency."

REQUIREMENTS:
Complete `api_version` so it returns the string "v1". Then complete the
`version` endpoint (GET /version) to return {"version": <the dependency value>}.
The dependency is already injected in the signature.

EXPECTED BEHAVIOR:
GET /version  ->  {"version": "v1"}
"""


def api_version() -> str:
    return "v1"


@app.get("/version")
def version(ver: Annotated[str, Depends(api_version)]) -> dict:
    return {"version": ver}


# ==============================================================================
# QUESTION 2: A Reusable Pagination Dependency
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Every list endpoint needs limit/offset. Define it once as a dependency."

REQUIREMENTS:
Complete `pagination` so it:
- Accepts query params `limit` (default 10) and `offset` (default 0).
- Returns the dict {"limit": limit, "offset": offset}.

(It's wired into the list endpoint in Q4 via the CommonParams alias below.)

EXPECTED BEHAVIOR (seen through GET /notes):
GET /notes?limit=2&offset=1   -> uses limit=2, offset=1
"""


def pagination(limit: int = 10, offset: int = 0) -> dict:
    return {"limit": limit, "offset": offset}


CommonParams = Annotated[dict, Depends(pagination)]


# ==============================================================================
# QUESTION 3: The Models + Store  (Create)
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Implement the create endpoint of a notes resource."

The store and models (store provided, one model to finish):
"""

_DB: dict[str, dict] = {}            # id -> {"id":..., "title":..., "body":...}
_NEXT_ID = {"value": 1}              # tiny auto-increment id source


class NoteCreate(BaseModel):
    title: str = Field(min_length=1)
    body: str = ""


"""
REQUIREMENTS:
Complete `create_note` (POST /notes, status_code=201):
1. Make a string id: `note_id = str(_NEXT_ID["value"])`, then increment
   _NEXT_ID["value"] by 1.
2. Build record = {"id": note_id, "title": body.title, "body": body.body}.
3. Store it: _DB[note_id] = record.
4. Return record.

EXPECTED BEHAVIOR:
POST /notes {"title":"hi","body":"there"}
   -> 201  {"id":"1","title":"hi","body":"there"}
"""


@app.post("/notes", status_code=201)
def create_note(body: NoteCreate) -> dict:
    note_id = str(_NEXT_ID["value"])
    _NEXT_ID["value"] += 1
    note = {"id": note_id, "title": body.title, "body": body.body}
    _DB[note_id] = note
    return note
    


# ==============================================================================
# QUESTION 4: List (with pagination) + Read one (with 404)
# ==============================================================================
"""
INTERVIEW CONTEXT:
"List all notes (respecting pagination) and fetch a single note by id."

REQUIREMENTS:
1. `list_notes` (GET /notes): inject CommonParams as `page`.
   - Take all records: `items = list(_DB.values())`.
   - Return the slice items[offset : offset + limit].

2. `get_note` (GET /notes/{note_id}):
   - If note_id not in _DB -> raise HTTPException(404, detail="note not found").
   - Otherwise return _DB[note_id].

EXPECTED BEHAVIOR:
GET /notes?limit=2&offset=0   -> first 2 notes
GET /notes/1                  -> the note with id "1"
GET /notes/9999               -> 404
"""


@app.get("/notes")
def list_notes(page: CommonParams) -> list:
    items = list(_DB.values())
    limit = page["limit"]
    offset = page["offset"]
    
    # 3. Return the sliced list of notes
    return items[offset : offset + limit]


@app.get("/notes/{note_id}")
def get_note(note_id: str) -> dict:
    if note_id not in _DB:
        raise HTTPException(status_code=404, detail="note not found")
    return _DB[note_id]


# ==============================================================================
# QUESTION 5: Delete (with 404)
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Delete a note by id; deleting something that doesn't exist is a 404."

REQUIREMENTS:
Complete `delete_note` (DELETE /notes/{note_id}):
- If note_id not in _DB -> raise HTTPException(404, detail="note not found").
- Otherwise remove it (`del _DB[note_id]`) and return {"deleted": note_id}.

EXPECTED BEHAVIOR:
DELETE /notes/1     -> {"deleted": "1"}
DELETE /notes/1     -> 404 (already gone)
"""


@app.delete("/notes/{note_id}")
def delete_note(note_id: str) -> dict:
    if note_id not in _DB:
        raise HTTPException(404, detail="note not found")
    del _DB[note_id]
    return {"deleted": note_id}
    

   


# ==============================================================================
# MAIN - Test Your Solutions
# ==============================================================================
def main() -> None:
    from fastapi.testclient import TestClient

    print("=" * 60)
    print("WORKBOOK 03: Testing Your Solutions")
    print("=" * 60)
    client = TestClient(app)

    # reset store between runs
    _DB.clear()
    _NEXT_ID["value"] = 1

    # Q1
    print("\n[Q1] Simple dependency...")
    try:
        assert client.get("/version").json() == {"version": "v1"}
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Q2
    print("\n[Q2] Pagination dependency...")
    try:
        assert pagination(limit=2, offset=1) == {"limit": 2, "offset": 1}
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Q3
    print("\n[Q3] Create note...")
    try:
        r = client.post("/notes", json={"title": "hi", "body": "there"})
        assert r.status_code == 201, r.text
        assert r.json() == {"id": "1", "title": "hi", "body": "there"}
        # a couple more for later questions
        client.post("/notes", json={"title": "two"})
        client.post("/notes", json={"title": "three"})
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Q4
    print("\n[Q4] List (paginated) + read one + 404...")
    try:
        all_notes = client.get("/notes").json()
        assert len(all_notes) == 3, all_notes
        page = client.get("/notes?limit=2&offset=0").json()
        assert len(page) == 2, page
        assert client.get("/notes/1").json()["title"] == "hi"
        assert client.get("/notes/9999").status_code == 404
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Q5
    print("\n[Q5] Delete + 404...")
    try:
        assert client.delete("/notes/1").json() == {"deleted": "1"}
        assert client.delete("/notes/1").status_code == 404
        assert client.get("/notes/1").status_code == 404
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    print("\n" + "=" * 60)
    print("Workbook 03 Complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
