#!/usr/bin/env python3
"""
Bulk data loader for dvdrental PostgreSQL (local Docker).
Adds ~1000 customers, ~1000 films, and runs 10000+ DML events
(insert/update/delete on actor/film/rental/payment tables).

Usage:
    python ingestion/load_bulk_data.py

Environment variables (all optional, with local-docker defaults):
    PGHOST      — postgres host      (default: localhost)
    PGPORT      — postgres port      (default: 5432)
    PGDATABASE  — database name      (default: dvdrental)
    PGUSER      — postgres user      (default: postgres)
    PGPASSWORD  — postgres password  (default: postgres)
"""

import os
import random
import string
import time
from datetime import datetime, timedelta

import psycopg2
import psycopg2.extras

PGHOST = os.environ.get("PGHOST", "localhost")
PGPORT = int(os.environ.get("PGPORT", "5432"))
PGDATABASE = os.environ.get("PGDATABASE", "dvdrental")
PGUSER = os.environ.get("PGUSER", "postgres")
PGPASSWORD = os.environ.get("PGPASSWORD", "postgres")

FIRST_NAMES = [
    "Alice", "Bob", "Carol", "Dave", "Eve", "Frank", "Grace", "Hank",
    "Iris", "Jack", "Karen", "Leo", "Mia", "Nick", "Olivia", "Paul",
    "Quinn", "Rachel", "Sam", "Tina", "Uma", "Victor", "Wendy", "Xander",
    "Yara", "Zoe", "Aaron", "Beth", "Cole", "Diana", "Ethan", "Fiona",
    "George", "Hannah", "Ivan", "Julia", "Kevin", "Laura", "Mike", "Nancy",
]
LAST_NAMES = [
    "Smith", "Jones", "Williams", "Brown", "Davis", "Wilson", "Moore",
    "Taylor", "Anderson", "Thomas", "Jackson", "White", "Harris", "Martin",
    "Thompson", "Garcia", "Martinez", "Robinson", "Clark", "Rodriguez",
    "Lewis", "Lee", "Walker", "Hall", "Allen", "Young", "Hernandez", "King",
    "Wright", "Lopez", "Hill", "Scott", "Green", "Adams", "Baker", "Nelson",
    "Carter", "Mitchell", "Perez", "Roberts", "Turner", "Phillips", "Campbell",
]
ADJECTIVES = [
    "Dark", "Lost", "Wild", "Last", "First", "Final", "Silent", "Broken",
    "Golden", "Silver", "Iron", "Steel", "Crystal", "Shadow", "Burning",
    "Frozen", "Hidden", "Twisted", "Ancient", "Forgotten", "Rising",
    "Falling", "Eternal", "Sacred", "Cursed", "Blazing", "Stormy",
]
NOUNS = [
    "Hour", "Night", "Dawn", "Storm", "Fire", "Ocean", "River", "Mountain",
    "Road", "Door", "Gate", "Bridge", "Tower", "Palace", "Kingdom",
    "Empire", "Legend", "Mystery", "Journey", "Quest", "Battle", "War",
    "Peace", "Dream", "Vision", "Mirror", "Shadow", "Flame", "Wind",
]
RATINGS = ["G", "PG", "PG-13", "R", "NC-17"]
SPECIAL_FEATURES_OPTIONS = [
    "Trailers", "Commentaries", "Deleted Scenes", "Behind the Scenes"
]


def _conn():
    return psycopg2.connect(
        host=PGHOST,
        port=PGPORT,
        database=PGDATABASE,
        user=PGUSER,
        password=PGPASSWORD,
        connect_timeout=15,
    )


def rand_suffix(k=4):
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=k))


def load_customers(conn, count=1000):
    """Insert ~count new customers, reusing existing address_ids."""
    print(f"\n[customers] Loading {count} rows...")
    with conn.cursor() as cur:
        cur.execute("SELECT address_id FROM address")
        address_ids = [r[0] for r in cur.fetchall()]
        cur.execute("SELECT store_id FROM store")
        store_ids = [r[0] for r in cur.fetchall()]

    batch = []
    for i in range(count):
        first = random.choice(FIRST_NAMES)
        last = f"{random.choice(LAST_NAMES)}_{rand_suffix(3)}"
        email = f"{first.lower()}.{last.lower()}@example-{rand_suffix(4).lower()}.com"
        address_id = random.choice(address_ids)
        store_id = random.choice(store_ids)
        batch.append((store_id, first, last, email, address_id, True, 1))

    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(
            cur,
            """
            INSERT INTO customer (store_id, first_name, last_name, email, address_id, activebool, active)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            batch,
            page_size=200,
        )
    conn.commit()
    print(f"[customers] Done — inserted {count} rows.")


def load_films(conn, count=1000):
    """Insert ~count new films."""
    print(f"\n[films] Loading {count} rows...")
    with conn.cursor() as cur:
        cur.execute("SELECT language_id FROM language")
        language_ids = [r[0] for r in cur.fetchall()]

    batch = []
    for _ in range(count):
        adj = random.choice(ADJECTIVES)
        noun = random.choice(NOUNS)
        title = f"{adj} {noun} {rand_suffix(3)}".title()
        description = f"A {random.choice(['thrilling','dramatic','comedic','epic','mysterious'])} tale of {adj.lower()} {noun.lower()}s."
        release_year = random.randint(1990, 2024)
        language_id = random.choice(language_ids)
        rental_duration = random.randint(3, 7)
        rental_rate = round(random.uniform(0.99, 6.99), 2)
        length = random.randint(60, 180)
        replacement_cost = round(random.uniform(9.99, 29.99), 2)
        rating = random.choice(RATINGS)
        sf_count = random.randint(0, 3)
        special_features = random.sample(SPECIAL_FEATURES_OPTIONS, sf_count) if sf_count else None
        batch.append((
            title, description, release_year, language_id,
            rental_duration, rental_rate, length,
            replacement_cost, rating, special_features,
        ))

    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(
            cur,
            """
            INSERT INTO film
              (title, description, release_year, language_id,
               rental_duration, rental_rate, length,
               replacement_cost, rating, special_features)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            batch,
            page_size=200,
        )
    conn.commit()
    print(f"[films] Done — inserted {count} rows.")


# ---------- DML event helpers ----------

def _event_insert_actor(cur) -> dict:
    first = random.choice(FIRST_NAMES)
    last = f"{random.choice(LAST_NAMES)}_{rand_suffix(3)}"
    cur.execute(
        "INSERT INTO actor (first_name, last_name) VALUES (%s, %s) RETURNING actor_id",
        (first, last),
    )
    actor_id = cur.fetchone()[0]
    return {"op": "insert", "table": "actor", "actor_id": actor_id}


def _event_update_film(cur) -> dict:
    factor = random.uniform(0.8, 1.3)
    cur.execute(
        "UPDATE film SET rental_rate = ROUND((rental_rate * %s)::numeric, 2) "
        "WHERE film_id = (SELECT film_id FROM film ORDER BY random() LIMIT 1) "
        "RETURNING film_id, rental_rate",
        (factor,),
    )
    row = cur.fetchone()
    return {"op": "update", "table": "film", "film_id": row[0], "rental_rate": float(row[1])}


def _event_delete_actor(cur) -> dict:
    cur.execute(
        "DELETE FROM actor WHERE actor_id = ("
        "  SELECT a.actor_id FROM actor a "
        "  WHERE NOT EXISTS (SELECT 1 FROM film_actor fa WHERE fa.actor_id = a.actor_id) "
        "  ORDER BY a.last_update DESC LIMIT 1"
        ") RETURNING actor_id"
    )
    row = cur.fetchone()
    if row:
        return {"op": "delete", "table": "actor", "actor_id": row[0]}
    return _event_update_film(cur)


def _event_insert_rental(cur, customer_ids, inventory_ids, staff_ids) -> dict:
    customer_id = random.choice(customer_ids)
    inventory_id = random.choice(inventory_ids)
    staff_id = random.choice(staff_ids)
    rental_date = datetime.now() - timedelta(days=random.randint(0, 365))
    return_date = rental_date + timedelta(days=random.randint(1, 14)) if random.random() > 0.2 else None
    cur.execute(
        "INSERT INTO rental (rental_date, inventory_id, customer_id, return_date, staff_id) "
        "VALUES (%s, %s, %s, %s, %s) RETURNING rental_id",
        (rental_date, inventory_id, customer_id, return_date, staff_id),
    )
    return {"op": "insert", "table": "rental", "rental_id": cur.fetchone()[0]}


def _event_insert_payment(cur, customer_ids, staff_ids, rental_ids) -> dict:
    customer_id = random.choice(customer_ids)
    staff_id = random.choice(staff_ids)
    rental_id = random.choice(rental_ids)
    amount = round(random.uniform(0.99, 9.99), 2)
    payment_date = datetime.now() - timedelta(days=random.randint(0, 365))
    cur.execute(
        "INSERT INTO payment (customer_id, staff_id, rental_id, amount, payment_date) "
        "VALUES (%s, %s, %s, %s, %s) RETURNING payment_id",
        (customer_id, staff_id, rental_id, amount, payment_date),
    )
    return {"op": "insert", "table": "payment", "payment_id": cur.fetchone()[0]}


def _event_update_customer(cur, customer_ids) -> dict:
    customer_id = random.choice(customer_ids)
    active = random.choice([0, 1])
    cur.execute(
        "UPDATE customer SET active = %s WHERE customer_id = %s RETURNING customer_id",
        (active, customer_id),
    )
    return {"op": "update", "table": "customer", "customer_id": customer_id, "active": active}


def run_dml_events(conn, count=10000):
    """Run `count` random DML events, committing in batches."""
    print(f"\n[events] Running {count} DML events...")

    with conn.cursor() as cur:
        cur.execute("SELECT customer_id FROM customer")
        customer_ids = [r[0] for r in cur.fetchall()]
        cur.execute("SELECT inventory_id FROM inventory")
        inventory_ids = [r[0] for r in cur.fetchall()]
        cur.execute("SELECT staff_id FROM staff")
        staff_ids = [r[0] for r in cur.fetchall()]
        cur.execute("SELECT rental_id FROM rental ORDER BY random() LIMIT 5000")
        rental_ids = [r[0] for r in cur.fetchall()]

    ops = ["insert_actor", "update_film", "delete_actor",
           "insert_rental", "insert_payment", "update_customer"]
    weights = [15, 20, 5, 25, 25, 10]

    event_counts = {op: 0 for op in ops}
    batch_size = 500
    start = time.time()

    for i in range(1, count + 1):
        op = random.choices(ops, weights=weights, k=1)[0]
        try:
            with conn.cursor() as cur:
                if op == "insert_actor":
                    _event_insert_actor(cur)
                elif op == "update_film":
                    _event_update_film(cur)
                elif op == "delete_actor":
                    _event_delete_actor(cur)
                elif op == "insert_rental":
                    _event_insert_rental(cur, customer_ids, inventory_ids, staff_ids)
                elif op == "insert_payment":
                    _event_insert_payment(cur, customer_ids, staff_ids, rental_ids)
                elif op == "update_customer":
                    _event_update_customer(cur, customer_ids)
            event_counts[op] += 1
        except Exception as e:
            conn.rollback()
            print(f"  [warn] event {i} ({op}) failed: {e}")
            continue

        if i % batch_size == 0:
            conn.commit()
            elapsed = time.time() - start
            rate = i / elapsed
            print(f"  {i:>6}/{count}  ({rate:.0f} ev/s)  elapsed={elapsed:.1f}s")

    conn.commit()
    elapsed = time.time() - start
    print(f"\n[events] Done — {count} events in {elapsed:.1f}s ({count/elapsed:.0f} ev/s)")
    print(f"  Breakdown: {event_counts}")


def print_summary(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
              (SELECT COUNT(*) FROM customer) AS customers,
              (SELECT COUNT(*) FROM film)     AS films,
              (SELECT COUNT(*) FROM rental)   AS rentals,
              (SELECT COUNT(*) FROM payment)  AS payments,
              (SELECT COUNT(*) FROM actor)    AS actors
        """)
        row = cur.fetchone()
    print(f"\n{'='*50}")
    print(f"  customers : {row[0]:,}")
    print(f"  films     : {row[1]:,}")
    print(f"  rentals   : {row[2]:,}")
    print(f"  payments  : {row[3]:,}")
    print(f"  actors    : {row[4]:,}")
    print(f"{'='*50}")


if __name__ == "__main__":
    print(f"Connecting to {PGHOST}:{PGPORT}/{PGDATABASE} as {PGUSER}...")
    conn = _conn()
    print("Connected.")

    print_summary(conn)

    load_customers(conn, count=1000)
    load_films(conn, count=1000)
    run_dml_events(conn, count=10000)

    print("\nFinal state:")
    print_summary(conn)
    conn.close()
