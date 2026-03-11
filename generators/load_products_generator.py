import os
import random
import time

import psycopg2


PRODUCTS = ["beer", "bread", "wine", "cheese", "vodka"]
COLORS = ["brown", "white", "red", "green", "black"]


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


def insert_product() -> None:
    CURSOR.execute(
        """
        INSERT INTO products(product_name, weight, color)
        VALUES (%s, %s, %s)
        RETURNING id
        """,
        (
            random.choice(PRODUCTS),
            round(random.uniform(0.2, 2.0), 2),
            random.choice(COLORS),
        ),
    )
    product_id = CURSOR.fetchone()[0]
    print(f"INSERT product {product_id}")


def update_product() -> None:
    product_id = pick_existing_id("products")
    if product_id is None:
        return

    CURSOR.execute(
        """
        UPDATE products
        SET weight = %s,
            color = %s,
            updated_at = now()
        WHERE id = %s
        """,
        (
            round(random.uniform(0.2, 2.0), 2),
            random.choice(COLORS),
            product_id,
        ),
    )
    print(f"UPDATE product {product_id}")


def main() -> None:
    iteration = 0
    while ITERATIONS is None or iteration < ITERATIONS:
        action = random.choices(["insert", "update"], weights=[0.6, 0.4])[0]
        if action == "insert":
            insert_product()
        else:
            update_product()

        iteration += 1
        time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))


if __name__ == "__main__":
    main()
