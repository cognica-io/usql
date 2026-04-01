#!/usr/bin/env python3
"""PGWire server with a shared engine across all connections.

By default, each connection gets its own USQLEngine instance.
This example demonstrates sharing a single engine so all connections
see the same data -- useful for multi-client scenarios.

Usage:
    python examples/08_pgwire_shared_engine.py

Then connect with multiple psql sessions:
    psql -h 127.0.0.1 -p 15432 -U uqa -d uqa
"""

import asyncio

from usqldb import USQLEngine
from usqldb.net.pgwire import PGWireConfig, PGWireServer


async def main() -> None:
    # Create a single shared engine.
    shared_engine = USQLEngine()

    # Pre-populate with sample data.
    shared_engine.sql("""
        CREATE TABLE messages (
            id   SERIAL PRIMARY KEY,
            text TEXT NOT NULL
        )
    """)
    shared_engine.sql("INSERT INTO messages (text) VALUES ('Hello from the server!')")

    config = PGWireConfig(
        host="127.0.0.1",
        port=15432,
        engine_factory=lambda: shared_engine,
    )

    server = PGWireServer(config)
    await server.start()
    print(f"Server listening on {config.host}:{server.port}")
    print("All connections share the same engine instance.")
    print()
    print("Try connecting with multiple psql sessions:")
    print("  psql -h 127.0.0.1 -p 15432 -U uqa -d uqa")
    print()
    print("Insert in one session, query in another -- data is shared.")
    print("Press Ctrl+C to stop.\n")

    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        pass
    finally:
        await server.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nServer stopped.")
