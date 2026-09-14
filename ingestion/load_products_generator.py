"""
Film generator for the dvdrental database.

The dvdrental database ships with ~1000 films as seed data, so this generator
only performs UPDATE mutations to simulate catalogue changes (pricing, duration).
"""
import os
import random
import time

import psycopg2


RATINGS = ["G", "PG", "PG-13", "R", "NC-17"]


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
SLEEP_MIN = env_float("SLEEP_MIN", 1.0)
SLEEP_MAX = env_float("SLEEP_MAX", 3.0)


def random_film_id() -> int | None:
    CURSOR.execute("SELECT film_id FROM film ORDER BY random() LIMIT 1")
    row = CURSOR.fetchone()
    return row[0] if row else None


def update_film() -> None:
    film_id = random_film_id()
    if film_id is None:
        return
    rental_rate = round(random.uniform(0.99, 4.99), 2)
    rental_duration = random.randint(3, 7)
    replacement_cost = round(random.uniform(9.99, 29.99), 2)
    CURSOR.execute(
        """
        UPDATE film
        SET rental_rate = %s,
            rental_duration = %s,
            replacement_cost = %s,
            last_update = now()
        WHERE film_id = %s
        """,
        (rental_rate, rental_duration, replacement_cost, film_id),
    )
    print(f"UPDATE film {film_id} rental_rate={rental_rate} rental_duration={rental_duration}")


def main() -> None:
    # Wait for films to be loaded before running
    max_wait = 60
    wait_count = 0
    while wait_count < max_wait:
        CURSOR.execute("SELECT COUNT(*) FROM film")
        if CURSOR.fetchone()[0] > 0:
            break
        print(f"Waiting for dvdrental films to load... ({wait_count + 1}/{max_wait})")
        time.sleep(1)
        wait_count += 1
    else:
        print("Timeout waiting for films. Exiting.")
        return

    iteration = 0
    while ITERATIONS is None or iteration < ITERATIONS:
        update_film()
        iteration += 1
        time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))


if __name__ == "__main__":
    main()
