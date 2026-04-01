#!/usr/bin/env python3
"""Persistent storage with db_path.

Demonstrates using USQLEngine with a file-based database that survives
across process restarts.
"""

import os
import tempfile

from usqldb import USQLEngine

DB_PATH = os.path.join(tempfile.gettempdir(), "usqldb_example.db")


def create_and_populate() -> None:
    """First run: create schema and insert data."""
    engine = USQLEngine(db_path=DB_PATH)

    engine.sql("""
        CREATE TABLE IF NOT EXISTS notes (
            id      SERIAL PRIMARY KEY,
            title   TEXT NOT NULL,
            body    TEXT
        )
    """)

    engine.sql("INSERT INTO notes (title, body) VALUES ('Hello', 'First note')")
    engine.sql("INSERT INTO notes (title, body) VALUES ('TODO', 'Buy groceries')")
    engine.sql(
        "INSERT INTO notes (title, body) VALUES ('Idea', 'Build something cool')"
    )

    result = engine.sql("SELECT COUNT(*) AS cnt FROM notes")
    print(f"Created {result.rows[0]['cnt']} notes in {DB_PATH}")
    engine.close()


def read_back() -> None:
    """Second run: read data from the existing database."""
    engine = USQLEngine(db_path=DB_PATH)

    result = engine.sql("SELECT id, title, body FROM notes ORDER BY id")
    print("\nNotes from persistent storage:")
    for row in result:
        print(f"  [{row['id']}] {row['title']}: {row['body']}")

    engine.close()


def main() -> None:
    create_and_populate()
    read_back()

    # Cleanup
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
        print(f"\nCleaned up {DB_PATH}")


if __name__ == "__main__":
    main()
