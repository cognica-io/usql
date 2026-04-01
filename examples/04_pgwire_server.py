#!/usr/bin/env python3
"""PGWire server: basic startup.

Starts a PostgreSQL-compatible TCP server that accepts connections
from any standard PostgreSQL client.

Usage:
    python examples/04_pgwire_server.py

Then connect with:
    psql -h 127.0.0.1 -p 15432 -U uqa -d uqa
"""

import asyncio

from usqldb.net.pgwire import PGWireConfig, PGWireServer


async def main() -> None:
    config = PGWireConfig(
        host="127.0.0.1",
        port=15432,
    )

    server = PGWireServer(config)
    await server.start()
    print(f"usqldb pgwire server listening on {config.host}:{server.port}")
    print("Connect with:  psql -h 127.0.0.1 -p 15432 -U uqa -d uqa")
    print("Press Ctrl+C to stop.\n")

    try:
        await asyncio.Event().wait()  # run forever
    except asyncio.CancelledError:
        pass
    finally:
        await server.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nServer stopped.")
