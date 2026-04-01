#
# usqldb -- PostgreSQL 17-compatible catalog layer for UQA
#
# Copyright (c) 2023-2026 Cognica, Inc.
#

"""Server configuration for the pgwire protocol server."""

from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable


@dataclasses.dataclass(frozen=True, slots=True)
class PGWireConfig:
    """Configuration for a :class:`PGWireServer` instance.

    Parameters
    ----------
    host
        Bind address for the TCP listener.
    port
        Bind port.  Use ``0`` for an OS-assigned ephemeral port.
    db_path
        Path passed to :class:`USQLEngine` for persistent storage.
        ``None`` creates an in-memory engine per connection.
    auth_method
        Authentication method name: ``"trust"``, ``"password"``,
        ``"md5"``, or ``"scram-sha-256"``.
    credentials
        Mapping of ``{username: password}`` for password-based auth.
    ssl_certfile
        Path to an SSL certificate file (PEM).  When set together with
        *ssl_keyfile*, the server accepts SSL connections.
    ssl_keyfile
        Path to an SSL private key file (PEM).
    max_connections
        Maximum number of concurrent client connections.
    engine_factory
        Optional callable that returns a :class:`USQLEngine` instance.
        When provided, this overrides *db_path*.
    """

    host: str = "127.0.0.1"
    port: int = 5432
    db_path: str | None = None
    auth_method: str = "trust"
    credentials: dict[str, str] | None = None
    ssl_certfile: str | None = None
    ssl_keyfile: str | None = None
    max_connections: int = 100
    engine_factory: Callable[[], Any] | None = None
