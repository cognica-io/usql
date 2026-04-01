#
# usqldb -- PostgreSQL 17-compatible catalog layer for UQA
#
# Copyright (c) 2023-2026 Cognica, Inc.
#

"""PostgreSQL v3 wire protocol server for usqldb.

This module implements a full PostgreSQL 17-compatible wire protocol
server that enables standard PostgreSQL clients (psql, psycopg,
asyncpg, SQLAlchemy, JDBC, DBeaver, DataGrip, Django, etc.) to
connect to usqldb over TCP.

Quick start::

    import asyncio
    from usqldb.net.pgwire import PGWireServer, PGWireConfig

    config = PGWireConfig(host="0.0.0.0", port=5432, db_path="my.db")
    server = PGWireServer(config)
    asyncio.run(server.serve_forever())

With authentication::

    from usqldb.net.pgwire import PGWireServer, PGWireConfig, AuthMethod

    config = PGWireConfig(
        host="0.0.0.0",
        port=5432,
        auth_method=AuthMethod.SCRAM_SHA_256.value,
        credentials={"admin": "secret123"},
    )
    server = PGWireServer(config)
    asyncio.run(server.serve_forever())
"""

from usqldb.net.pgwire._auth import AuthMethod
from usqldb.net.pgwire._config import PGWireConfig
from usqldb.net.pgwire._server import PGWireServer

__all__ = ["AuthMethod", "PGWireConfig", "PGWireServer"]
