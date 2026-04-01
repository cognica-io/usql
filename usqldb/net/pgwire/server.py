#
# usqldb -- PostgreSQL 17-compatible catalog layer for UQA
#
# Copyright (c) 2023-2026 Cognica, Inc.
#

"""Command-line entry point for the usqldb pgwire server.

Usage::

    usqldb-server                        # in-memory, port 5432
    usqldb-server --port 15432           # custom port
    usqldb-server --db mydata.db         # persistent storage
    usqldb-server --auth scram-sha-256 \\
                  --user admin:secret    # with authentication
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys

from usqldb.net.pgwire._auth import AuthMethod
from usqldb.net.pgwire._config import PGWireConfig
from usqldb.net.pgwire._server import PGWireServer


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="usqldb-server",
        description="PostgreSQL 17-compatible wire protocol server for usqldb",
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="bind address (default: 127.0.0.1)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=5432,
        help="bind port (default: 5432)",
    )
    parser.add_argument(
        "--db",
        metavar="PATH",
        default=None,
        help="database file for persistent storage (default: in-memory)",
    )
    parser.add_argument(
        "--auth",
        choices=[m.value for m in AuthMethod],
        default="trust",
        help="authentication method (default: trust)",
    )
    parser.add_argument(
        "--user",
        metavar="NAME:PASSWORD",
        action="append",
        default=[],
        help="add a user credential (repeatable, e.g. --user alice:secret)",
    )
    parser.add_argument(
        "--max-connections",
        type=int,
        default=100,
        help="maximum concurrent connections (default: 100)",
    )
    parser.add_argument(
        "--ssl-cert",
        metavar="PATH",
        default=None,
        help="SSL certificate file (PEM)",
    )
    parser.add_argument(
        "--ssl-key",
        metavar="PATH",
        default=None,
        help="SSL private key file (PEM)",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="logging level (default: INFO)",
    )
    return parser.parse_args(argv)


def _build_credentials(user_args: list[str]) -> dict[str, str] | None:
    if not user_args:
        return None
    creds: dict[str, str] = {}
    for entry in user_args:
        if ":" not in entry:
            print(
                f"Invalid --user format: {entry!r} (expected NAME:PASSWORD)",
                file=sys.stderr,
            )
            sys.exit(1)
        name, password = entry.split(":", 1)
        creds[name] = password
    return creds


def main(argv: list[str] | None = None) -> None:
    """Entry point for the ``usqldb-server`` command."""
    args = _parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    credentials = _build_credentials(args.user)

    if args.auth != "trust" and not credentials:
        print(
            f"Authentication method {args.auth!r} requires at least one "
            f"--user NAME:PASSWORD",
            file=sys.stderr,
        )
        sys.exit(1)

    config = PGWireConfig(
        host=args.host,
        port=args.port,
        db_path=args.db,
        auth_method=args.auth,
        credentials=credentials,
        ssl_certfile=args.ssl_cert,
        ssl_keyfile=args.ssl_key,
        max_connections=args.max_connections,
    )

    server = PGWireServer(config)
    loop = asyncio.new_event_loop()

    def _shutdown() -> None:
        loop.call_soon_threadsafe(loop.stop)

    try:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, _shutdown)
    except RuntimeError:
        # Signal handlers only work in the main thread.
        pass

    db_desc = args.db if args.db else "(in-memory)"
    print(
        f"usqldb-server starting on {args.host}:{args.port} "
        f"[db={db_desc}, auth={args.auth}]"
    )

    try:
        loop.run_until_complete(server.start())
        actual_port = server.port
        if actual_port != args.port:
            print(f"Listening on port {actual_port}")
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        loop.run_until_complete(server.stop())
        loop.close()
        print("usqldb-server stopped")


if __name__ == "__main__":
    main()
