#!/usr/bin/env python3
"""Advanced schema features: views, sequences, foreign keys, check constraints.

Demonstrates the full range of DDL features supported by usqldb,
and how they appear in the PostgreSQL catalog system.
"""

from usqldb import USQLEngine


def main() -> None:
    engine = USQLEngine()

    # ---- Schema with multiple constraint types --------------------------

    engine.sql("""
        CREATE TABLE categories (
            id   SERIAL PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            slug TEXT NOT NULL UNIQUE
        )
    """)

    engine.sql("""
        CREATE TABLE products (
            id          SERIAL PRIMARY KEY,
            name        TEXT NOT NULL,
            category_id INTEGER REFERENCES categories(id),
            price       NUMERIC NOT NULL,
            weight_kg   REAL,
            sku         VARCHAR(20) UNIQUE,
            CHECK (price > 0)
        )
    """)

    engine.sql("""
        CREATE TABLE order_items (
            id         SERIAL PRIMARY KEY,
            product_id INTEGER REFERENCES products(id),
            quantity   INTEGER NOT NULL,
            unit_price NUMERIC NOT NULL,
            CHECK (quantity > 0),
            CHECK (unit_price >= 0)
        )
    """)

    # ---- Views ----------------------------------------------------------

    engine.sql("""
        CREATE VIEW product_catalog AS
        SELECT
            p.id,
            p.name,
            c.name AS category,
            p.price,
            p.sku
        FROM products p
        JOIN categories c ON p.category_id = c.id
    """)

    engine.sql("""
        CREATE VIEW recent_orders AS
        SELECT
            oi.id AS order_item_id,
            p.name AS product_name,
            oi.quantity,
            oi.unit_price
        FROM order_items oi
        JOIN products p ON oi.product_id = p.id
    """)

    # ---- Sequences ------------------------------------------------------

    engine.sql("CREATE SEQUENCE invoice_number_seq START 1000 INCREMENT 1")

    # ---- Populate data --------------------------------------------------

    engine.sql(
        "INSERT INTO categories (name, slug) VALUES ('Electronics', 'electronics')"
    )
    engine.sql("INSERT INTO categories (name, slug) VALUES ('Books', 'books')")
    engine.sql("INSERT INTO categories (name, slug) VALUES ('Clothing', 'clothing')")

    engine.sql(
        "INSERT INTO products (name, category_id, price, weight_kg, sku) "
        "VALUES ('Laptop', 1, 999.99, 2.1, 'ELEC-001')"
    )
    engine.sql(
        "INSERT INTO products (name, category_id, price, weight_kg, sku) "
        "VALUES ('Python Book', 2, 49.99, 0.8, 'BOOK-001')"
    )
    engine.sql(
        "INSERT INTO products (name, category_id, price, weight_kg, sku) "
        "VALUES ('T-Shirt', 3, 19.99, 0.2, 'CLTH-001')"
    )

    engine.sql(
        "INSERT INTO order_items (product_id, quantity, unit_price) VALUES (1, 2, 999.99)"
    )
    engine.sql(
        "INSERT INTO order_items (product_id, quantity, unit_price) VALUES (2, 5, 49.99)"
    )
    engine.sql(
        "INSERT INTO order_items (product_id, quantity, unit_price) VALUES (3, 10, 19.99)"
    )
    engine.sql(
        "INSERT INTO order_items (product_id, quantity, unit_price) VALUES (1, 1, 899.99)"
    )

    # ---- Query views ----------------------------------------------------

    print("=== Product catalog (view) ===")
    result = engine.sql("SELECT * FROM product_catalog ORDER BY price DESC")
    for row in result:
        print(
            f"  {row['name']:<15} {row['category']:<12} ${row['price']:>8.2f}  SKU={row['sku']}"
        )

    print("\n=== Recent orders (view) ===")
    result = engine.sql("SELECT * FROM recent_orders ORDER BY order_item_id")
    for row in result:
        print(
            f"  #{row['order_item_id']}  {row['product_name']:<15} "
            f"qty={row['quantity']}  ${row['unit_price']:>8.2f}"
        )

    # ---- Introspect constraints via pg_catalog --------------------------

    print("\n=== All constraints ===")
    result = engine.sql("""
        SELECT
            c.conname AS constraint_name,
            cl.relname AS table_name,
            c.contype AS type
        FROM pg_catalog.pg_constraint c
        JOIN pg_catalog.pg_class cl ON c.conrelid = cl.oid
        ORDER BY cl.relname, c.contype, c.conname
    """)
    type_labels = {"p": "PRIMARY KEY", "u": "UNIQUE", "f": "FOREIGN KEY", "c": "CHECK"}
    for row in result:
        label = type_labels.get(row["type"], row["type"])
        print(f"  {row['table_name']:<15} {label:<15} {row['constraint_name']}")

    # ---- Introspect views -----------------------------------------------

    print("\n=== Views in information_schema ===")
    result = engine.sql("""
        SELECT table_name, view_definition
        FROM information_schema.views
        WHERE table_schema = 'public'
        ORDER BY table_name
    """)
    for row in result:
        defn = (row["view_definition"] or "")[:70]
        print(f"  {row['table_name']:<20} {defn}...")

    # ---- Introspect sequences -------------------------------------------

    print("\n=== Sequences ===")
    result = engine.sql("""
        SELECT sequence_name, start_value, increment
        FROM information_schema.sequences
        WHERE sequence_schema = 'public'
        ORDER BY sequence_name
    """)
    for row in result:
        print(
            f"  {row['sequence_name']:<25} "
            f"START {row['start_value']}  "
            f"INCREMENT {row['increment']}"
        )


if __name__ == "__main__":
    main()
