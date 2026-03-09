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

products = ["beer", "wine", "vodka", "bread", "cheese"]

def insert_row():
    cur.execute(
        "INSERT INTO orders(product,price) VALUES(%s,%s) RETURNING id",
        (random.choice(products), random.randint(5,100))
    )
    row_id = cur.fetchone()[0]
    print(f"INSERT id={row_id}")

def update_row():

    cur.execute("SELECT id FROM orders ORDER BY random() LIMIT 1")
    row = cur.fetchone()

    if row:
        id = row[0]

        cur.execute(
            "UPDATE orders SET price=%s WHERE id=%s",
            (random.randint(5,100), id)
        )

        print(f"UPDATE id={id}")

def delete_row():

    cur.execute("SELECT id FROM orders ORDER BY random() LIMIT 1")
    row = cur.fetchone()

    if row:
        id = row[0]

        cur.execute(
            "DELETE FROM orders WHERE id=%s",
            (id,)
        )

        print(f"DELETE id={id}")


while True:

    action = random.choices(
        ["insert","update","delete"],
        weights=[0.5,0.35,0.15]
    )[0]

    if action == "insert":
        insert_row()

    elif action == "update":
        update_row()

    elif action == "delete":
        delete_row()

    time.sleep(random.uniform(0.5,2))