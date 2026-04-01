#
# usqldb -- PostgreSQL 17-compatible catalog layer for UQA
#
# Copyright (c) 2023-2026 Cognica, Inc.
#

"""Shared fixtures for pgwire tests."""

from __future__ import annotations

import asyncio

import pytest

from usqldb.core.engine import USQLEngine


@pytest.fixture
def engine() -> USQLEngine:
    """Create a fresh in-memory USQLEngine."""
    return USQLEngine()


@pytest.fixture
def engine_with_data() -> USQLEngine:
    """USQLEngine with sample tables and data."""
    e = USQLEngine()
    e.sql(
        "CREATE TABLE users ("
        "  id SERIAL PRIMARY KEY,"
        "  name TEXT NOT NULL,"
        "  email VARCHAR(255),"
        "  score REAL,"
        "  active BOOLEAN DEFAULT TRUE"
        ")"
    )
    e.sql(
        "INSERT INTO users (name, email, score) VALUES ('Alice', 'alice@example.com', 95.5)"
    )
    e.sql(
        "INSERT INTO users (name, email, score) VALUES ('Bob', 'bob@example.com', 87.0)"
    )
    e.sql(
        "CREATE TABLE posts ("
        "  id SERIAL PRIMARY KEY,"
        "  user_id INTEGER,"
        "  title TEXT NOT NULL,"
        "  body TEXT"
        ")"
    )
    e.sql("INSERT INTO posts (user_id, title, body) VALUES (1, 'Hello', 'World')")
    return e


@pytest.fixture
def event_loop():
    """Create an event loop for async tests."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()
