"""
================================================================================
WORKBOOK 02: PYDANTIC MODELS & VALIDATION
================================================================================
Difficulty: Easy-Medium → Medium
Topics: Pydantic v2 models, types & defaults, optional fields, Field
        constraints, nested models, a simple field validator, response_model

Prerequisites: Workbook 01.

Learning Objectives:
- Define typed models with required, optional, and defaulted fields
- Add declarative constraints with Field (min/max length, ge/le)
- Compose models (a model that contains another model / a list of models)
- Write one custom field_validator to normalize/validate a value
- Use response_model so the response only exposes the fields you choose
================================================================================
"""

from __future__ import annotations

from typing import Optional

from fastapi import FastAPI
from pydantic import BaseModel, Field, field_validator


# ╔════════════════════════════════════════════════════════════════════════════╗
# ║                     PRIMER: MODELING DATA WITH PYDANTIC                     ║
# ╚════════════════════════════════════════════════════════════════════════════╝
"""
A MODEL IS A TYPED, SELF-VALIDATING STRUCT
==========================================
    class Item(BaseModel):
        name: str                 # required
        price: float = 0.0        # optional, has a default
        note: Optional[str] = None  # optional, may be omitted/null

Create one from a dict and Pydantic CHECKS and CONVERTS the data:
    Item(name="x", price="3.5")   -> price becomes 3.5 (str coerced to float)
    Item(price=1)                 -> ValidationError: 'name' is required


DECLARATIVE CONSTRAINTS WITH Field
==================================
Instead of writing if-checks, declare the rule on the field:
    qty:   int = Field(ge=1, le=100)        # 1 <= qty <= 100
    title: str = Field(min_length=1, max_length=80)
    tags:  list[str] = Field(default_factory=list)   # mutable default!

Use default_factory (not `= []`) for mutable defaults so every instance gets
its own fresh list.


NESTED MODELS
=============
Fields can be other models, or lists of them:
    class Address(BaseModel):
        city: str
    class Person(BaseModel):
        name: str
        address: Address              # nested object
        nicknames: list[str] = []     # list of scalars


A CUSTOM VALIDATOR (when Field isn't enough)
============================================
    This is important
    A @field_validator("email)
    @classmethod
    def must_have_at(cls, v:str) -> str:
        if "@" .... 
    @field_validator("email")
    @classmethod
    def must_have_at(cls, v: str) -> str:
        if "@" not in v:
            raise ValueError("not an email")
        return v          # you can also RETURN a transformed value (normalize)

Raise ValueError inside a validator and FastAPI returns HTTP 422 with a clear
message pointing at the bad field.


response_model — SHAPE THE OUTPUT
=================================
A route can accept a rich model but RETURN a trimmed one. Declaring
`response_model=PublicView` filters the response to exactly that model's
fields — handy for hiding internal data (passwords, tokens):

    @app.post("/signup", response_model=UserPublic)
    def signup(body: UserCreate):     # body has a password...
        return {... including password ...}   # response_model strips it
================================================================================
"""

app = FastAPI(title="Workbook 02 - Pydantic Models")


# ==============================================================================
# QUESTION 1: Types, Defaults, and Optional Fields
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Model a product. Some fields are required, some have defaults, some are
optional."

REQUIREMENTS:
Complete the `Product` model with these fields:
- name:        str           (required)
- price:       float         (required)
- in_stock:    bool          (default True)
- description: Optional[str] (default None)

EXPECTED BEHAVIOR:
>>> Product(name="Pen", price=1.5)
Product(name='Pen', price=1.5, in_stock=True, description=None)
>>> Product(price=1.5)          # missing name
ValidationError
"""


class Product(BaseModel):
    name: str
    price: float
    in_stock: bool = True
    description: Optional[str] = None


# ==============================================================================
# QUESTION 2: Field Constraints
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Enforce limits declaratively instead of writing manual if-checks."

REQUIREMENTS:
Complete `OrderLine` using Field(...) constraints:
- sku:      str, min_length=1, max_length=20
- quantity: int, between 1 and 999 inclusive (ge / le)

EXPECTED BEHAVIOR:
>>> OrderLine(sku="ABC", quantity=3)      # ok
>>> OrderLine(sku="", quantity=3)         # ValidationError (sku too short)
>>> OrderLine(sku="ABC", quantity=0)      # ValidationError (quantity < 1)
"""

class OrderLine(BaseModel):
    sku: str = Field(min_length=1, max_length=20)
    quantity: int = Field(ge=1, le=999)


# ==============================================================================
# QUESTION 3: Nested Models + List of Models
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Model an order that contains multiple line items and a shipping address."

REQUIREMENTS:
1. `Address` is given (city: str, zip_code: str).
2. Complete `Order`:
   - order_id: str
   - address:  Address                      (nested model)
   - lines:    list[OrderLine]              (list of the Q2 model)
   - notes:    Optional[str] = None

Then complete `total_quantity(order)` (a plain helper) to return the SUM of
quantity across all lines.

EXPECTED BEHAVIOR:
>>> o = Order(order_id="o1", address={"city":"NYC","zip_code":"10001"},
...           lines=[{"sku":"A","quantity":2},{"sku":"B","quantity":5}])
>>> total_quantity(o)
7
"""


class Address(BaseModel):
    city: str
    zip_code: str


class Order(BaseModel):
   order_id: str
   address: Address
   lines: list[OrderLine]
   notes: Optional[str] = None


def total_quantity(order: Order) -> int:
    return sum(line.quantity for line in order.lines)
    


# ==============================================================================
# QUESTION 4: A Custom Field Validator (Normalize + Validate)
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Validate an email and normalize a username so storage is consistent."

REQUIREMENTS:
Complete `Account`:
- username: str  -> a field_validator that RETURNS it lowercased + stripped
                    (e.g. "  Alice " -> "alice").
- email:    str  -> a field_validator that raises ValueError if "@" not in it,
                    otherwise returns it unchanged.

EXPECTED BEHAVIOR:
>>> Account(username="  Alice ", email="a@b.com").username
'alice'
>>> Account(username="x", email="not-an-email")
ValidationError
"""


class Account(BaseModel):
    username: str
    email: str

    @field_validator("username")
    @classmethod
    def validate_username(cls, username):
        return username.strip().lower()
    @field_validator("email")
    @classmethod
    def validate_email(cls, email):
        if '@' not in email:
            raise ValueError("Not a valid email")
        return email



# ==============================================================================
# QUESTION 5: response_model (Hide Internal Fields)
# ==============================================================================
"""
INTERVIEW CONTEXT:
"Accept a signup with a password, but never echo the password back."

REQUIREMENTS:
1. `SignupRequest` is given (username, email, password).
2. Complete `AccountPublic` with EXACTLY two fields: username, email.
3. Complete `signup` (POST /signup, response_model=AccountPublic):
   - return a dict {"username":..., "email":..., "password":...}.
   - Because response_model=AccountPublic, the password must NOT appear in the
     response body.

EXPECTED BEHAVIOR:
POST /signup {"username":"mia","email":"m@x.com","password":"secret"}
   -> {"username":"mia","email":"m@x.com"}   (no password key)
"""


class SignupRequest(BaseModel):
    username: str
    email: str
    password: str


class AccountPublic(BaseModel):
    username: str
    email: str


@app.post("/signup", response_model=AccountPublic)
def signup(body: SignupRequest) -> dict:
    return body.model_dump()


# ==============================================================================
# MAIN - Test Your Solutions
# ==============================================================================
def main() -> None:
    from fastapi.testclient import TestClient
    from pydantic import ValidationError

    print("=" * 60)
    print("WORKBOOK 02: Testing Your Solutions")
    print("=" * 60)
    client = TestClient(app)

    # Q1
    print("\n[Q1] Types, defaults, optional...")
    try:
        p = Product(name="Pen", price=1.5)
        assert p.in_stock is True and p.description is None, p
        try:
            Product(price=1.5)
            raise AssertionError("missing name should fail")
        except ValidationError:
            pass
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Q2
    print("\n[Q2] Field constraints...")
    try:
        OrderLine(sku="ABC", quantity=3)
        for bad in (dict(sku="", quantity=3), dict(sku="ABC", quantity=0)):
            try:
                OrderLine(**bad)
                raise AssertionError(f"should reject {bad}")
            except ValidationError:
                pass
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Q3
    print("\n[Q3] Nested models + list...")
    try:
        o = Order(
            order_id="o1",
            address={"city": "NYC", "zip_code": "10001"},
            lines=[{"sku": "A", "quantity": 2}, {"sku": "B", "quantity": 5}],
        )
        assert isinstance(o.address, Address) and o.address.city == "NYC"
        assert total_quantity(o) == 7
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Q4
    print("\n[Q4] Custom field validators...")
    try:
        a = Account(username="  Alice ", email="a@b.com")
        assert a.username == "alice", a.username
        try:
            Account(username="x", email="not-an-email")
            raise AssertionError("bad email should fail")
        except ValidationError:
            pass
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    # Q5
    print("\n[Q5] response_model hides password...")
    try:
        r = client.post("/signup", json={"username": "mia", "email": "m@x.com",
                                         "password": "secret"})
        assert r.status_code == 200, r.text
        assert r.json() == {"username": "mia", "email": "m@x.com"}, r.json()
        print("    PASSED!")
    except Exception as e:
        print(f"    FAILED: {e}")

    print("\n" + "=" * 60)
    print("Workbook 02 Complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
