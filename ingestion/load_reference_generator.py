"""
Reference-table generator for the dvdrental database.

Generates realistic mutations across all reference tables to produce
CDC events for every Kafka topic in the pipeline:

  actor        – INSERT new actors, UPDATE names
  address      – INSERT new addresses
  city         – INSERT new cities
  customer     – INSERT new customers (with address), UPDATE email / active
  inventory    – INSERT new inventory items (film assigned to store)
  film_actor   – INSERT new cast associations
  film_category – UPDATE film category (reassign film to different genre)

Tables that are essentially static (country, language, store, staff, category)
are read-only in this generator but will still appear as Kafka snapshot events.

Usage:
    python3 generators/load_reference_generator.py

Optional env vars:
    ITERATIONS          stop after N mutations (default: run forever)
    SLEEP_MIN / SLEEP_MAX   sleep range between mutations (default 0.3 / 1.5 s)
    PGHOST / PGPORT / PGDATABASE / PGUSER / PGPASSWORD
"""
import os
import random
import string
import time

import psycopg2


# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------

def env_float(name: str, default: float) -> float:
    return float(os.getenv(name, default))


def env_int(name: str, default=None):
    value = os.getenv(name)
    return int(value) if value is not None else default


CONN = psycopg2.connect(
    host=os.getenv("PGHOST", "localhost"),
    port=os.getenv("PGPORT", "5432"),
    dbname=os.getenv("PGDATABASE", "demo"),
    user=os.getenv("PGUSER", "postgres"),
    password=os.getenv("PGPASSWORD", "postgres"),
)
CONN.autocommit = True
CURSOR = CONN.cursor()

ITERATIONS = env_int("ITERATIONS")
SLEEP_MIN = env_float("SLEEP_MIN", 0.3)
SLEEP_MAX = env_float("SLEEP_MAX", 1.5)

# ---------------------------------------------------------------------------
# Random-lookup helpers
# ---------------------------------------------------------------------------

def _one(query: str, *params):
    CURSOR.execute(query, params)
    row = CURSOR.fetchone()
    return row[0] if row else None


def random_film_id():
    return _one("SELECT film_id FROM film ORDER BY random() LIMIT 1")

def random_actor_id():
    return _one("SELECT actor_id FROM actor ORDER BY random() LIMIT 1")

def random_store_id():
    return _one("SELECT store_id FROM store ORDER BY random() LIMIT 1")

def random_city_id():
    return _one("SELECT city_id FROM city ORDER BY random() LIMIT 1")

def random_country_id():
    return _one("SELECT country_id FROM country ORDER BY random() LIMIT 1")

def random_category_id():
    return _one("SELECT category_id FROM category ORDER BY random() LIMIT 1")

def random_address_id():
    return _one("SELECT address_id FROM address ORDER BY random() LIMIT 1")

def uncast_film_category(film_id: int):
    """Return a category_id not currently assigned to this film."""
    return _one(
        """
        SELECT category_id FROM category
        WHERE category_id NOT IN (
            SELECT category_id FROM film_category WHERE film_id = %s
        )
        ORDER BY random() LIMIT 1
        """,
        film_id,
    )

def uncast_actor(film_id: int):
    """Return an actor_id not already in the cast of this film."""
    return _one(
        """
        SELECT actor_id FROM actor
        WHERE actor_id NOT IN (
            SELECT actor_id FROM film_actor WHERE film_id = %s
        )
        ORDER BY random() LIMIT 1
        """,
        film_id,
    )

# ---------------------------------------------------------------------------
# Random value builders
# ---------------------------------------------------------------------------

_FIRST_NAMES = [
    "Alice", "Bob", "Carol", "David", "Emma", "Frank", "Grace", "Henry",
    "Isla", "Jack", "Karen", "Liam", "Mia", "Noah", "Olivia", "Paul",
    "Quinn", "Rachel", "Sam", "Tara", "Uma", "Victor", "Wendy", "Xander",
    "Yara", "Zach",
]
_LAST_NAMES = [
    "Smith", "Jones", "Williams", "Brown", "Taylor", "Davies", "Evans",
    "Wilson", "Thomas", "Roberts", "Johnson", "Lewis", "Walker", "Robinson",
    "Wright", "Thompson", "White", "Hughes", "Edwards", "Green",
]
_STREETS = [
    "Main St", "Oak Ave", "Maple Dr", "Cedar Ln", "Elm St", "Pine Rd",
    "Lake View", "Park Blvd", "River Rd", "Hill Top",
]
_CITIES = [
    "Springfield", "Riverdale", "Lakewood", "Fairview", "Greenville",
    "Maplewood", "Oakdale", "Cedarburg", "Pinehill", "Elmwood",
]

def rand_name(pool):
    return random.choice(pool)

def rand_street():
    return f"{random.randint(1, 9999)} {random.choice(_STREETS)}"

def rand_phone():
    return "".join(random.choices(string.digits, k=10))

def rand_postal():
    return "".join(random.choices(string.digits, k=5))

def rand_email(first: str, last: str) -> str:
    suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=4))
    return f"{first.lower()}.{last.lower()}{suffix}@example.com"

# ---------------------------------------------------------------------------
# Mutation functions
# ---------------------------------------------------------------------------

def insert_actor() -> None:
    first = rand_name(_FIRST_NAMES)
    last = rand_name(_LAST_NAMES)
    CURSOR.execute(
        "INSERT INTO actor (first_name, last_name, last_update) VALUES (%s, %s, now()) RETURNING actor_id",
        (first, last),
    )
    actor_id = CURSOR.fetchone()[0]
    print(f"INSERT actor {actor_id} {first} {last}")


def update_actor() -> None:
    actor_id = random_actor_id()
    if actor_id is None:
        return
    new_last = rand_name(_LAST_NAMES)
    CURSOR.execute(
        "UPDATE actor SET last_name = %s, last_update = now() WHERE actor_id = %s",
        (new_last, actor_id),
    )
    print(f"UPDATE actor {actor_id} last_name={new_last}")


def insert_address() -> int | None:
    city_id = random_city_id()
    if city_id is None:
        return None
    street = rand_street()
    postal = rand_postal()
    phone = rand_phone()
    CURSOR.execute(
        """
        INSERT INTO address (address, district, city_id, postal_code, phone, last_update)
        VALUES (%s, %s, %s, %s, %s, now())
        RETURNING address_id
        """,
        (street, "District", city_id, postal, phone),
    )
    address_id = CURSOR.fetchone()[0]
    print(f"INSERT address {address_id} '{street}' city_id={city_id}")
    return address_id


def insert_city() -> None:
    country_id = random_country_id()
    if country_id is None:
        return
    city_name = random.choice(_CITIES) + f"-{random.randint(100, 999)}"
    CURSOR.execute(
        "INSERT INTO city (city, country_id, last_update) VALUES (%s, %s, now()) RETURNING city_id",
        (city_name, country_id),
    )
    city_id = CURSOR.fetchone()[0]
    print(f"INSERT city {city_id} '{city_name}' country_id={country_id}")


def insert_customer() -> None:
    store_id = random_store_id()
    if store_id is None:
        return
    # Create a fresh address for this customer
    address_id = insert_address()
    if address_id is None:
        return
    first = rand_name(_FIRST_NAMES)
    last = rand_name(_LAST_NAMES)
    email = rand_email(first, last)
    CURSOR.execute(
        """
        INSERT INTO customer
            (store_id, first_name, last_name, email, address_id,
             activebool, create_date, last_update, active)
        VALUES (%s, %s, %s, %s, %s, true, now(), now(), 1)
        RETURNING customer_id
        """,
        (store_id, first, last, email, address_id),
    )
    customer_id = CURSOR.fetchone()[0]
    print(f"INSERT customer {customer_id} {first} {last} <{email}>")


def update_customer() -> None:
    customer_id = _one("SELECT customer_id FROM customer ORDER BY random() LIMIT 1")
    if customer_id is None:
        return
    first = rand_name(_FIRST_NAMES)
    last = rand_name(_LAST_NAMES)
    email = rand_email(first, last)
    CURSOR.execute(
        "UPDATE customer SET email = %s, last_update = now() WHERE customer_id = %s",
        (email, customer_id),
    )
    print(f"UPDATE customer {customer_id} email={email}")


def insert_inventory() -> None:
    film_id = random_film_id()
    store_id = random_store_id()
    if not all([film_id, store_id]):
        return
    CURSOR.execute(
        "INSERT INTO inventory (film_id, store_id, last_update) VALUES (%s, %s, now()) RETURNING inventory_id",
        (film_id, store_id),
    )
    inventory_id = CURSOR.fetchone()[0]
    print(f"INSERT inventory {inventory_id} film_id={film_id} store_id={store_id}")


def insert_film_actor() -> None:
    film_id = random_film_id()
    if film_id is None:
        return
    actor_id = uncast_actor(film_id)
    if actor_id is None:
        return
    CURSOR.execute(
        "INSERT INTO film_actor (actor_id, film_id, last_update) VALUES (%s, %s, now())",
        (actor_id, film_id),
    )
    print(f"INSERT film_actor film_id={film_id} actor_id={actor_id}")


def update_film_category() -> None:
    film_id = random_film_id()
    if film_id is None:
        return
    new_category_id = uncast_film_category(film_id)
    if new_category_id is None:
        return
    CURSOR.execute(
        "UPDATE film_category SET category_id = %s, last_update = now() WHERE film_id = %s",
        (new_category_id, film_id),
    )
    print(f"UPDATE film_category film_id={film_id} category_id={new_category_id}")


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

ACTIONS = [
    ("insert_actor",         insert_actor,         0.10),
    ("update_actor",         update_actor,         0.08),
    ("insert_city",          insert_city,          0.05),
    ("insert_customer",      insert_customer,      0.15),
    ("update_customer",      update_customer,      0.12),
    ("insert_inventory",     insert_inventory,     0.20),
    ("insert_film_actor",    insert_film_actor,    0.15),
    ("update_film_category", update_film_category, 0.15),
]
_NAMES, _FUNCS, _WEIGHTS = zip(*ACTIONS)


def main() -> None:
    iteration = 0
    while ITERATIONS is None or iteration < ITERATIONS:
        action_fn = random.choices(_FUNCS, weights=_WEIGHTS, k=1)[0]
        try:
            action_fn()
        except psycopg2.Error as exc:
            print(f"SKIP ({exc.pgcode}): {exc.pgerror.strip() if exc.pgerror else exc}")
            CONN.rollback()
        iteration += 1
        time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))


if __name__ == "__main__":
    main()
