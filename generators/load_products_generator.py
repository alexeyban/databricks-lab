import psycopg2
import random
import time

conn = psycopg2.connect(
    host="localhost",
    dbname="demo",
    user="postgres",
    password="postgres"
)

conn.autocommit = True
cur = conn.cursor()

products = ["beer","bread","wine","cheese","vodka"]
colors = ["brown","white","red","green","black"]

def insert_product():

    cur.execute(
        """
        INSERT INTO products(product_name, weight, color)
        VALUES (%s,%s,%s)
        RETURNING id
        """,
        (
            random.choice(products),
            round(random.uniform(0.2,2.0),2),
            random.choice(colors)
        )
    )

    product_id = cur.fetchone()[0]
    print(f"INSERT product {product_id}")

def update_product():

    cur.execute("SELECT id FROM products ORDER BY random() LIMIT 1")
    row = cur.fetchone()

    if row:

        product_id = row[0]

        cur.execute(
            """
            UPDATE products
            SET weight=%s,
                color=%s
            WHERE id=%s
            """,
            (
                round(random.uniform(0.2,2.0),2),
                random.choice(colors),
                product_id
            )
        )

        print(f"UPDATE product {product_id}")


while True:

    action = random.choices(
        ["insert","update"],
        weights=[0.6,0.4]
    )[0]

    if action == "insert":
        insert_product()
    else:
        update_product()

    time.sleep(random.uniform(1,3))