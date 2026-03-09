CREATE TABLE if NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    product TEXT,
    price NUMERIC,
    created_at TIMESTAMP DEFAULT now()
);

CREATE PUBLICATION dbz_publication FOR ALL TABLES;