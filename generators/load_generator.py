import os
import random
import time

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


def pick_existing_id(table_name: str) -> int | None:
    CURSOR.execute(f"SELECT max(id) FROM {table_name}")
    max_id = CURSOR.fetchone()[0]

    if not max_id:
        return None

    candidate = random.randint(1, max_id)
    CURSOR.execute(
        f"SELECT id FROM {table_name} WHERE id >= %s ORDER BY id LIMIT 1",
        (candidate,),
    )
    row = CURSOR.fetchone()
    if row:
        return row[0]

    CURSOR.execute(f"SELECT id FROM {table_name} ORDER BY id LIMIT 1")
    row = CURSOR.fetchone()
    return row[0] if row else None


def get_product_id() -> int | None:
    """Get a random product_id from the products table."""
    CURSOR.execute("SELECT id FROM products ORDER BY random() LIMIT 1")
    row = CURSOR.fetchone()
    return row[0] if row else None


def insert_row() -> None:
    product_id = get_product_id()
    if product_id is None:
        print("No products available to create order")
        return
    
    CURSOR.execute(
        "INSERT INTO orders(product_id, price) VALUES(%s, %s) RETURNING id",
        (product_id, random.randint(5, 100)),
    )
    row_id = CURSOR.fetchone()[0]
    print(f"INSERT order {row_id} with product_id {product_id}")


def update_row() -> None:
    row_id = pick_existing_id("orders")
    if row_id is None:
        return

    CURSOR.execute(
        "UPDATE orders SET price=%s WHERE id=%s",
        (random.randint(5, 100), row_id),
    )
    print(f"UPDATE order {row_id}")


def delete_row() -> None:
    row_id = pick_existing_id("orders")
    if row_id is None:
        return

    CURSOR.execute("DELETE FROM orders WHERE id=%s", (row_id,))
    print(f"DELETE order {row_id}")


def main() -> None:
    # Wait for products to exist before creating orders
    max_wait = 30
    wait_count = 0
    while wait_count < max_wait:
        CURSOR.execute("SELECT COUNT(*) FROM products")
        if CURSOR.fetchone()[0] > 0:
            break
        print(f"Waiting for products to be created... ({wait_count + 1}/{max_wait})")
        time.sleep(1)
        wait_count += 1
    else:
        print("Timeout waiting for products. Exiting.")
        return

    iteration = 0
    while ITERATIONS is None or iteration < ITERATIONS:
        action = random.choices(
            ["insert", "update", "delete"],
            weights=[0.5, 0.35, 0.15],
        )[0]

        if action == "insert":
            insert_row()
        elif action == "update":
            update_row()
        else:
            delete_row()

        iteration += 1
        time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))


if __name__ == "__main__":
    main()
