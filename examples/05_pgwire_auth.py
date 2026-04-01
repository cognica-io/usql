#!/usr/bin/env python3
"""PGWire server with authentication.

Demonstrates all four authentication methods supported by usqldb:
trust, cleartext password, MD5, and SCRAM-SHA-256.

Usage:
    python examples/05_pgwire_auth.py

Then connect with:
    psql -h 127.0.0.1 -p 15432 -U admin -d uqa
    (password: secret123)
"""

import asyncio

from usqldb.net.pgwire import AuthMethod, PGWireConfig, PGWireServer

# User credentials shared across examples.
CREDENTIALS = {
    "admin": "secret123",
    "reader": "readonly",
}


async def run_server(method: AuthMethod) -> None:
    config = PGWireConfig(
        host="127.0.0.1",
        port=15432,
        auth_method=method.value,
        credentials=CREDENTIALS,
    )

    server = PGWireServer(config)
    await server.start()
    print(f"Server listening on {config.host}:{server.port}")
    print(f"Auth method: {method.value}")
    print(f"Users: {', '.join(CREDENTIALS)}")
    print("Connect with:  psql -h 127.0.0.1 -p 15432 -U admin -d uqa")
    print("Press Ctrl+C to stop.\n")

    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        pass
    finally:
        await server.stop()


def main() -> None:
    print("Available authentication methods:")
    for m in AuthMethod:
        print(f"  - {m.value}")
    print()

    # Default to SCRAM-SHA-256 (PostgreSQL 17 default).
    method = AuthMethod.SCRAM_SHA_256
    print(f"Starting server with {method.value} authentication...\n")

    try:
        asyncio.run(run_server(method))
    except KeyboardInterrupt:
        print("\nServer stopped.")


if __name__ == "__main__":
    main()
