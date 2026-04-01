#!/usr/bin/env python3
"""PostgreSQL 17 catalog introspection.

Demonstrates querying information_schema and pg_catalog views to
inspect database structure -- the same queries that tools like
psql, DBeaver, DataGrip, and SQLAlchemy use internally.
"""

from usqldb import USQLEngine


def main() -> None:
    engine = USQLEngine()

    # Create a sample schema.
    engine.sql("""
        CREATE TABLE authors (
            id   SERIAL PRIMARY KEY,
            name TEXT NOT NULL UNIQUE
        )
    """)
    engine.sql("""
        CREATE TABLE books (
            id        SERIAL PRIMARY KEY,
            title     TEXT NOT NULL,
            author_id INTEGER REFERENCES authors(id),
            isbn      VARCHAR(13) UNIQUE,
            price     NUMERIC,
            published DATE
        )
    """)
    engine.sql("""
        CREATE VIEW expensive_books AS
        SELECT b.title, b.price, a.name AS author
        FROM books b
        JOIN authors a ON b.author_id = a.id
        WHERE b.price > 30
    """)

    # ---- information_schema.tables --------------------------------------

    print("=== information_schema.tables ===")
    result = engine.sql("""
        SELECT table_name, table_type
        FROM information_schema.tables
        WHERE table_schema = 'public'
        ORDER BY table_name
    """)
    for row in result:
        print(f"  {row['table_name']:<20} {row['table_type']}")

    # ---- information_schema.columns -------------------------------------

    print("\n=== information_schema.columns (books) ===")
    result = engine.sql("""
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_name = 'books'
        ORDER BY ordinal_position
    """)
    for row in result:
        nullable = "NULL" if row["is_nullable"] == "YES" else "NOT NULL"
        default = f" DEFAULT {row['column_default']}" if row["column_default"] else ""
        print(f"  {row['column_name']:<15} {row['data_type']:<20} {nullable}{default}")

    # ---- information_schema.table_constraints ---------------------------

    print("\n=== information_schema.table_constraints ===")
    result = engine.sql("""
        SELECT constraint_name, table_name, constraint_type
        FROM information_schema.table_constraints
        WHERE table_schema = 'public'
        ORDER BY table_name, constraint_type
    """)
    for row in result:
        print(
            f"  {row['constraint_name']:<30} "
            f"{row['table_name']:<15} "
            f"{row['constraint_type']}"
        )

    # ---- information_schema.key_column_usage -----------------------------

    print("\n=== Foreign key details ===")
    result = engine.sql("""
        SELECT
            tc.constraint_name,
            kcu.column_name,
            ccu.table_name AS referenced_table,
            ccu.column_name AS referenced_column
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
        JOIN information_schema.constraint_column_usage ccu
            ON tc.constraint_name = ccu.constraint_name
        WHERE tc.constraint_type = 'FOREIGN KEY'
    """)
    for row in result:
        print(
            f"  {row['constraint_name']}: "
            f"{row['column_name']} -> "
            f"{row['referenced_table']}.{row['referenced_column']}"
        )

    # ---- pg_catalog.pg_class + pg_attribute + pg_type -------------------

    print("\n=== pg_catalog: columns with type OIDs ===")
    result = engine.sql("""
        SELECT
            c.relname AS table_name,
            a.attname AS column_name,
            t.typname AS type_name,
            a.atttypid AS type_oid,
            a.attnotnull AS not_null
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_attribute a ON c.oid = a.attrelid
        JOIN pg_catalog.pg_type t ON a.atttypid = t.oid
        WHERE c.relname = 'books' AND a.attnum > 0
        ORDER BY a.attnum
    """)
    for row in result:
        nn = "NOT NULL" if row["not_null"] else ""
        print(
            f"  {row['column_name']:<15} {row['type_name']:<15} "
            f"(OID {row['type_oid']}) {nn}"
        )

    # ---- pg_catalog.pg_indexes ------------------------------------------

    print("\n=== pg_catalog.pg_indexes ===")
    result = engine.sql("""
        SELECT schemaname, tablename, indexname
        FROM pg_catalog.pg_indexes
        WHERE schemaname = 'public'
        ORDER BY tablename, indexname
    """)
    for row in result:
        print(f"  {row['tablename']:<15} {row['indexname']}")

    # ---- pg_catalog.pg_views --------------------------------------------

    print("\n=== pg_catalog.pg_views ===")
    result = engine.sql("""
        SELECT viewname, definition
        FROM pg_catalog.pg_views
        WHERE schemaname = 'public'
    """)
    for row in result:
        defn = (row["definition"] or "")[:60]
        print(f"  {row['viewname']:<20} {defn}...")

    # ---- pg_catalog.pg_settings -----------------------------------------

    print("\n=== pg_catalog.pg_settings (sample) ===")
    result = engine.sql("""
        SELECT name, setting
        FROM pg_catalog.pg_settings
        WHERE name IN ('server_version', 'server_encoding', 'DateStyle',
                       'TimeZone', 'standard_conforming_strings')
        ORDER BY name
    """)
    for row in result:
        print(f"  {row['name']:<35} {row['setting']}")


if __name__ == "__main__":
    main()
