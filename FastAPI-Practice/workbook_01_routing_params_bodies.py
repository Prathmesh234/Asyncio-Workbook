"""
================================================================================
WORKBOOK 01: FASTAPI FUNDAMENTALS — ROUTING, PARAMS & REQUEST BODIES
================================================================================
Difficulty: Easy-Medium
Topics: Creating an app, path operations (GET/POST/PUT), path parameters,
        query parameters (defaults + optional), request bodies with Pydantic,
        status codes

Prerequisites: Basic Python (functions, type hints, dicts).

Learning Objectives:
- Create a FastAPI app and register endpoints with decorators
- Read values from the URL path and the query string
- Accept a JSON request body via a Pydantic model
- Return the right HTTP status code
================================================================================
"""

from __future__ import annotations

from typing import Optional

from fastapi import FastAPI
from pydantic import BaseModel


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║                       PRIMER: HOW A FASTAPI ROUTE WORKS                     ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
THE SHAPE OF AN ENDPOINT
========================
    app = FastAPI()

    @app.get("/hello")          # HTTP method + URL path
    def hello():                # the "path operation function"
        return {"msg": "hi"}    # a dict -> FastAPI serializes it to JSON

The decorator (@app.get / @app.post / ...) tells FastAPI: "when this method
hits this path, call this function." Return a dict/list/Pydantic model and
FastAPI turns it into a JSON response automatically.


WHERE DOES INPUT COME FROM? Three places:
=========================================
1. PATH PARAMS — part of the URL, declared in the path AND the signature:
       @app.get("/users/{user_id}")
       def get_user(user_id: int):   # /users/7 -> user_id == 7 (auto-converted)

2. QUERY PARAMS — after the "?" in the URL. Any function arg NOT in the path
   becomes a query param. Give it a default to make it optional:
       @app.get("/search")
       def search(q: str, limit: int = 10):   # /search?q=cat&limit=5

3. REQUEST BODY — JSON sent with POST/PUT. Declare a Pydantic model arg:
       class Item(BaseModel):
           name: str
       @app.post("/items")
       def create(item: Item):   # body {"name": "x"} -> item.name == "x"

    URL:  /users/7 / search ? q=cat & limit=5      BODY (POST/PUT):
          ^^^^^^^^^         ^^^^^^^^^^^^^^^^         {"name": "x"}
          path param        query params            request body


STATUS CODES
============
Default success is 200. For "I created something" use 201:
       @app.post("/items", status_code=201)

You'll see all this live at http://127.0.0.1:8000/docs when you run the app.
================================================================================
"""

app = FastAPI(title="Workbook 01 - FastAPI Fundamentals")


# ==============================================================================
# QUESTION 1: Your First Endpoint
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Set up a FastAPI app with a simple root/health endpoint."

REQUIREMENTS:
Complete `root` so a GET to "/" returns the dict {"message": "ok"}.

EXPECTED BEHAVIOR:
GET /  ->  200  {"message": "ok"}
"""


@app.get("/")
def root() -> dict:
    return {"message": "ok"}


# ==============================================================================
# QUESTION 2: Path Parameter
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Read a value out of the URL path. FastAPI converts it to the declared type."

REQUIREMENTS:
Complete `square` (GET /square/{number}). `number` is an int path param.
Return {"number": number, "square": number * number}.

EXPECTED BEHAVIOR:
GET /square/5   ->  {"number": 5, "square": 25}
GET /square/abc ->  422  (FastAPI rejects non-int automatically — no code needed)
"""


@app.get("/square/{number}")
def square(number: int) -> dict:
    return {"number": number, "square": number ** 2}


# ==============================================================================
# QUESTION 3: Query Parameters with Defaults
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Accept optional settings via the query string with sensible defaults."

REQUIREMENTS:
Complete `greet` (GET /greet). Two query params:
- name: str, default "world"
- loud: bool, default False
Return {"greeting": "<text>"} where text is "Hello, <name>!" — and if loud is
True, uppercase the WHOLE greeting string.

EXPECTED BEHAVIOR:
GET /greet                       -> {"greeting": "Hello, world!"}
GET /greet?name=alice            -> {"greeting": "Hello, alice!"}
GET /greet?name=alice&loud=true  -> {"greeting": "HELLO, ALICE!"}
"""


@app.get("/greet")
def greet(name: str = "world", loud: bool = False) -> dict:
    if name and loud:
        return {"greeting": f"HELLO, {name.capitalize()}"}
    elif name:
        return {"greeting": f"HELLO, {name}"}
    return {"greeting": "Hello, world!"}



# ==============================================================================
# QUESTION 4: Optional Query Parameter
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Filter a list, but the filter is optional. No filter -> return everything."

A fixed list (provided):
"""

_FRUITS = ["apple", "apricot", "banana", "cherry", "avocado"]

"""
REQUIREMENTS:
Complete `search` (GET /search). One OPTIONAL query param:
- q: Optional[str], default None
Return {"results": [...]}:
- If q is None -> return ALL fruits.
- Otherwise -> return only fruits that START WITH q (case-insensitive).

EXPECTED BEHAVIOR:
GET /search      -> {"results": ["apple","apricot","banana","cherry","avocado"]}
GET /search?q=a  -> {"results": ["apple","apricot","avocado"]}
"""


@app.get("/search")
def search(q: Optional[str] = None) -> dict:
    return_list = []
    if q is None:
        return {"results": _FRUITS }
    for f in _FRUITS:
        if f.startswith(q):
            return_list.append(f)
    return {"results":  return_list }



# ==============================================================================
# QUESTION 5: A Request Body (Pydantic Model)
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Accept a JSON body. Declare its shape with a Pydantic model so FastAPI parses
and validates it for you."

REQUIREMENTS:
1. The `User` model is given below (name: str, age: int).
2. Complete `echo_user` (POST /users): accept a `user: User` body and return
   {"name": user.name, "age": user.age, "is_adult": user.age >= 18}.

EXPECTED BEHAVIOR:
POST /users  {"name": "Mia", "age": 30}
   -> {"name": "Mia", "age": 30, "is_adult": true}
POST /users  {"name": "Sam"}            -> 422 (age is required)
"""


class User(BaseModel):
    name: str
    age: int


@app.post("/users")
def echo_user(user: User) -> dict:
    return {"name": user.name, "age": user.age, "is_adult": True if user.age >= 18 else False}



# ==============================================================================
# QUESTION 6: Status Code + Path Param + Body Together
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Create-or-replace a resource: take an id from the path AND a body, and return
201 Created."

REQUIREMENTS:
Complete `put_user` (PUT /users/{user_id}, status_code=201):
- user_id: int (path param)
- user: User (body)
Return {"id": user_id, "name": user.name, "age": user.age}.
(The status_code=201 is already set in the decorator.)

EXPECTED BEHAVIOR:
PUT /users/7  {"name": "Mia", "age": 30}
   -> 201  {"id": 7, "name": "Mia", "age": 30}
"""

#if you want to add a status_code you can add that 
#@app.put("/users/{user_id}", status_code=201)
@app.put("/users/{user_id}", status_code=201)
def put_user(user_id: int, user: User) -> dict:
    return {"id": user_id, "name": user.name, "age": user.age}


# ==============================================================================
# MAIN - Test Your Solutions  (run: uv run python workbook_01_routing_params_bodies.py)
# ==============================================================================
def main() -> None:
    from fastapi.testclient import TestClient

    print("=" * 60)
    print("WORKBOOK 01: Testing Your Solutions")
    print("=" * 60)
    client = TestClient(app)

    # Q1
    print("\n[Q1] Root endpoint...")
    try:
        r = client.get("/")
        assert r.status_code == 200 and r.json() == {"message": "ok"}, r.text
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Q2
    print("\n[Q2] Path parameter...")
    try:
        assert client.get("/square/5").json() == {"number": 5, "square": 25}
        assert client.get("/square/abc").status_code == 422
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Q3
    print("\n[Q3] Query params with defaults...")
    try:
        assert client.get("/greet").json() == {"greeting": "Hello, world!"}
        assert client.get("/greet?name=alice").json() == {"greeting": "Hello, alice!"}
        assert client.get("/greet?name=alice&loud=true").json() == {"greeting": "HELLO, ALICE!"}
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Q4
    print("\n[Q4] Optional query param...")
    try:
        assert client.get("/search").json() == {"results": _FRUITS}
        assert client.get("/search?q=a").json() == {"results": ["apple", "apricot", "avocado"]}
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Q5
    print("\n[Q5] Request body (Pydantic)...")
    try:
        r = client.post("/users", json={"name": "Mia", "age": 30})
        assert r.json() == {"name": "Mia", "age": 30, "is_adult": True}, r.text
        assert client.post("/users", json={"name": "Sam"}).status_code == 422
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Q6
    print("\n[Q6] Status code + path + body...")
    try:
        r = client.put("/users/7", json={"name": "Mia", "age": 30})
        assert r.status_code == 201, r.text
        assert r.json() == {"id": 7, "name": "Mia", "age": 30}
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    print("\n" + "=" * 60)
    print("Workbook 01 Complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
