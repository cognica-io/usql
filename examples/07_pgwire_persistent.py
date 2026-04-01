#!/usr/bin/env python3
"""PGWire server with persistent storage.

Starts a pgwire server backed by a file-based database so data
survives server restarts.

Usage:
    python examples/07_pgwire_persistent.py

Then connect with:
    psql -h 127.0.0.1 -p 15432 -U uqa -d uqa
"""

import asyncio
import os
import tempfile

from usqldb.net.pgwire import PGWireConfig, PGWireServer

DB_PATH = os.path.join(tempfile.gettempdir(), "usqldb_server_example.db")


async def main() -> None:
    config = PGWireConfig(
        host="127.0.0.1",
        port=15432,
        db_path=DB_PATH,
    )

    server = PGWireServer(config)
    await server.start()
    print(f"Server listening on {config.host}:{server.port}")
    print(f"Database: {DB_PATH}")
    print()
    print("Connect with:  psql -h 127.0.0.1 -p 15432 -U uqa -d uqa")
    print()
    print("Data persists across server restarts.")
    print("Press Ctrl+C to stop.\n")

    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        pass
    finally:
        await server.stop()
        print(f"\nDatabase saved to {DB_PATH}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nServer stopped.")
