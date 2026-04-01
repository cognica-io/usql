#!/usr/bin/env python3
"""Connecting to usqldb with psycopg (Python PostgreSQL client).

Starts an in-process pgwire server and demonstrates using psycopg
to perform DDL, DML, queries, and catalog introspection -- the same
way you would interact with a real PostgreSQL database.

Requirements:
    pip install psycopg
"""

import asyncio
import threading

import psycopg

from usqldb.net.pgwire import PGWireConfig, PGWireServer


def start_server() -> tuple[threading.Thread, int]:
    """Start a pgwire server in a background thread, return (thread, port)."""
    started = threading.Event()
    port_holder: list[int] = []

    def _run() -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        async def _start() -> None:
            config = PGWireConfig(host="127.0.0.1", port=0)
            server = PGWireServer(config)
            await server.start()
            port_holder.append(server.port)
            started.set()
            while True:
                await asyncio.sleep(0.5)

        try:
            loop.run_until_complete(_start())
        except asyncio.CancelledError:
            pass

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    started.wait(timeout=10)
    return thread, port_holder[0]


def main() -> None:
    _thread, port = start_server()
    conninfo = f"host=127.0.0.1 port={port} user=uqa dbname=uqa"
    print(f"Server running on port {port}\n")

    conn = psycopg.connect(conninfo, autocommit=True)

    # ---- DDL ------------------------------------------------------------

    conn.execute("""
        CREATE TABLE products (
            id    SERIAL PRIMARY KEY,
            name  TEXT NOT NULL,
            price REAL NOT NULL,
            stock INTEGER DEFAULT 0
        )
    """)
    print("Created table: products")

    # ---- INSERT ---------------------------------------------------------

    products = [
        ("Laptop", 999.99, 50),
        ("Mouse", 29.99, 200),
        ("Keyboard", 79.99, 150),
        ("Monitor", 449.99, 75),
        ("Headphones", 149.99, 120),
    ]
    for name, price, stock in products:
        conn.execute(
            "INSERT INTO products (name, price, stock) VALUES (%s, %s, %s)",
            [name, price, stock],
        )
    print(f"Inserted {len(products)} products\n")

    # ---- SELECT ---------------------------------------------------------

    print("=== Products (price > 100) ===")
    cur = conn.execute(
        "SELECT name, price, stock FROM products WHERE price > %s ORDER BY price DESC",
        [100],
    )
    for row in cur.fetchall():
        print(f"  {row[0]:<15} ${row[1]:>8.2f}  stock={row[2]}")

    # ---- Aggregation ----------------------------------------------------

    print("\n=== Summary ===")
    cur = conn.execute("""
        SELECT
            COUNT(*) AS total_products,
            SUM(stock) AS total_stock,
            AVG(price) AS avg_price,
            MIN(price) AS min_price,
            MAX(price) AS max_price
        FROM products
    """)
    row = cur.fetchone()
    print(f"  Products: {row[0]}")
    print(f"  Total stock: {row[1]}")
    print(f"  Avg price: ${row[2]:.2f}")
    print(f"  Price range: ${row[3]:.2f} - ${row[4]:.2f}")

    # ---- Catalog introspection via psycopg ------------------------------

    print("\n=== Column metadata (via information_schema) ===")
    cur = conn.execute("""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_name = 'products'
        ORDER BY ordinal_position
    """)
    for row in cur.fetchall():
        print(f"  {row[0]:<10} {row[1]:<15} nullable={row[2]}")

    # ---- Server version -------------------------------------------------

    cur = conn.execute("SHOW server_version")
    print(f"\nServer version: {cur.fetchone()[0]}")

    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
