"""
Rental + Payment generator for the dvdrental database.

Mutations:
  - INSERT rental  (new rental for a random customer + inventory item)
  - UPDATE rental  (set return_date, simulating a film return)
  - INSERT payment (payment for a completed rental)
"""
import os
import random
import time
from datetime import datetime, timedelta, timezone

import psycopg2


def env_float(name: str, default: float) -> float:
    return float(os.getenv(name, default))


def env_int(name: str, default: int | None = None) -> int | None:
    value = os.getenv(name)
    if value is None:
        return default
    return int(value)


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
SLEEP_MIN = env_float("SLEEP_MIN", 0.5)
SLEEP_MAX = env_float("SLEEP_MAX", 2.0)


def random_customer_id() -> int | None:
    CURSOR.execute("SELECT customer_id FROM customer ORDER BY random() LIMIT 1")
    row = CURSOR.fetchone()
    return row[0] if row else None


def random_inventory_id() -> int | None:
    CURSOR.execute("SELECT inventory_id FROM inventory ORDER BY random() LIMIT 1")
    row = CURSOR.fetchone()
    return row[0] if row else None


def random_staff_id() -> int | None:
    CURSOR.execute("SELECT staff_id FROM staff ORDER BY random() LIMIT 1")
    row = CURSOR.fetchone()
    return row[0] if row else None


def open_rental_id() -> int | None:
    """Pick a rental that has no return_date yet."""
    CURSOR.execute(
        "SELECT rental_id FROM rental WHERE return_date IS NULL ORDER BY random() LIMIT 1"
    )
    row = CURSOR.fetchone()
    return row[0] if row else None


def unreimbursed_rental_id() -> int | None:
    """Pick a returned rental that has no payment yet."""
    CURSOR.execute(
        """
        SELECT r.rental_id
        FROM rental r
        LEFT JOIN payment p ON p.rental_id = r.rental_id
        WHERE r.return_date IS NOT NULL AND p.payment_id IS NULL
        ORDER BY random()
        LIMIT 1
        """
    )
    row = CURSOR.fetchone()
    return row[0] if row else None


def insert_rental() -> None:
    customer_id = random_customer_id()
    inventory_id = random_inventory_id()
    staff_id = random_staff_id()
    if not all([customer_id, inventory_id, staff_id]):
        return
    rental_date = datetime.now(timezone.utc)
    CURSOR.execute(
        """
        INSERT INTO rental (rental_date, inventory_id, customer_id, staff_id, last_update)
        VALUES (%s, %s, %s, %s, now())
        RETURNING rental_id
        """,
        (rental_date, inventory_id, customer_id, staff_id),
    )
    rental_id = CURSOR.fetchone()[0]
    print(f"INSERT rental {rental_id} customer={customer_id} inventory={inventory_id}")


def return_rental() -> None:
    rental_id = open_rental_id()
    if rental_id is None:
        return
    return_date = datetime.now(timezone.utc) + timedelta(days=random.randint(1, 14))
    CURSOR.execute(
        "UPDATE rental SET return_date = %s, last_update = now() WHERE rental_id = %s",
        (return_date, rental_id),
    )
    print(f"UPDATE rental {rental_id} return_date set")


def insert_payment() -> None:
    rental_id = unreimbursed_rental_id()
    if rental_id is None:
        return
    CURSOR.execute("SELECT customer_id, staff_id FROM rental WHERE rental_id = %s", (rental_id,))
    row = CURSOR.fetchone()
    if row is None:
        return
    customer_id, staff_id = row
    amount = round(random.uniform(0.99, 9.99), 2)
    CURSOR.execute(
        """
        INSERT INTO payment (customer_id, staff_id, rental_id, amount, payment_date)
        VALUES (%s, %s, %s, %s, now())
        RETURNING payment_id
        """,
        (customer_id, staff_id, rental_id, amount),
    )
    payment_id = CURSOR.fetchone()[0]
    print(f"INSERT payment {payment_id} rental={rental_id} amount={amount}")


def main() -> None:
    iteration = 0
    while ITERATIONS is None or iteration < ITERATIONS:
        action = random.choices(
            ["insert_rental", "return_rental", "insert_payment"],
            weights=[0.4, 0.35, 0.25],
        )[0]

        if action == "insert_rental":
            insert_rental()
        elif action == "return_rental":
            return_rental()
        else:
            insert_payment()

        iteration += 1
        time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))


if __name__ == "__main__":
    main()
