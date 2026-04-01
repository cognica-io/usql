#
# usqldb -- PostgreSQL 17-compatible catalog layer for UQA
#
# Copyright (c) 2023-2026 Cognica, Inc.
#

"""Asyncio-based PostgreSQL wire protocol server.

:class:`PGWireServer` manages the TCP listener, connection lifecycle,
and cancel-request routing.  Each accepted connection is handled by
a :class:`PGWireConnection` running as an independent asyncio task.

Usage::

    import asyncio
    from usqldb.net.pgwire import PGWireServer, PGWireConfig

    config = PGWireConfig(host="0.0.0.0", port=5432, db_path="my.db")
    server = PGWireServer(config)
    asyncio.run(server.serve_forever())
"""

from __future__ import annotations

import asyncio
import logging
import secrets
import ssl
from typing import TYPE_CHECKING

from usqldb.core.engine import USQLEngine
from usqldb.net.pgwire._connection import PGWireConnection

if TYPE_CHECKING:
    from usqldb.net.pgwire._config import PGWireConfig

logger = logging.getLogger("usqldb.pgwire")


class PGWireServer:
    """PostgreSQL wire protocol server.

    Listens for TCP connections and creates a :class:`PGWireConnection`
    for each one.  Supports cancel requests routed between connections.
    """

    def __init__(self, config: PGWireConfig) -> None:
        self._config = config
        self._server: asyncio.Server | None = None
        self._connections: dict[int, PGWireConnection] = {}
        self._tasks: set[asyncio.Task[None]] = set()
        self._next_pid = 1
        self._ssl_context: ssl.SSLContext | None = None

        if config.ssl_certfile and config.ssl_keyfile:
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            ctx.load_cert_chain(config.ssl_certfile, config.ssl_keyfile)
            self._ssl_context = ctx

    @property
    def port(self) -> int:
        """Return the actual listening port (useful when port=0)."""
        if self._server is not None:
            sockets = self._server.sockets
            if sockets:
                return sockets[0].getsockname()[1]
        return self._config.port

    @property
    def host(self) -> str:
        return self._config.host

    # ==================================================================
    # Lifecycle
    # ==================================================================

    async def start(self) -> None:
        """Start the TCP listener."""
        self._server = await asyncio.start_server(
            self._handle_client,
            host=self._config.host,
            port=self._config.port,
        )
        addrs = [s.getsockname() for s in self._server.sockets]
        logger.info("PGWire server listening on %s", addrs)

    async def stop(self) -> None:
        """Gracefully shut down the server and all connections."""
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()

        # Cancel all connection tasks.
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        self._connections.clear()
        logger.info("PGWire server stopped")

    async def serve_forever(self) -> None:
        """Start the server and run until interrupted."""
        await self.start()
        assert self._server is not None
        try:
            await self._server.serve_forever()
        except asyncio.CancelledError:
            pass
        finally:
            await self.stop()

    # ==================================================================
    # Connection handling
    # ==================================================================

    async def _handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        """Called for each new TCP connection."""
        # Check max connections.
        if len(self._connections) >= self._config.max_connections:
            logger.warning("Max connections reached, rejecting client")
            writer.close()
            await writer.wait_closed()
            return

        pid, secret = self._allocate_process_id()
        engine = self._create_engine()

        conn = PGWireConnection(
            reader=reader,
            writer=writer,
            engine=engine,
            auth_method=self._config.auth_method,
            credentials=self._config.credentials,
            process_id=pid,
            secret_key=secret,
            cancel_callback=self._cancel_query,
            ssl_context=self._ssl_context,
        )
        self._connections[pid] = conn

        task = asyncio.current_task()
        if task is not None:
            self._tasks.add(task)
            task.add_done_callback(lambda t: self._cleanup_connection(pid, t))

        try:
            await conn.run()
        finally:
            self._connections.pop(pid, None)
            if task is not None:
                self._tasks.discard(task)

    def _allocate_process_id(self) -> tuple[int, int]:
        """Allocate a unique (process_id, secret_key) pair."""
        pid = self._next_pid
        self._next_pid += 1
        secret = secrets.randbelow(2**31)
        return pid, secret

    def _create_engine(self) -> USQLEngine:
        """Create a USQLEngine instance for a new connection."""
        if self._config.engine_factory is not None:
            return self._config.engine_factory()
        return USQLEngine(db_path=self._config.db_path)

    def _cancel_query(self, process_id: int, secret_key: int) -> None:
        """Route a CancelRequest to the target connection."""
        conn = self._connections.get(process_id)
        if conn is not None and conn.secret_key == secret_key:
            conn.cancel()

    def _cleanup_connection(self, pid: int, task: asyncio.Task[None]) -> None:
        """Remove a finished connection from tracking."""
        self._connections.pop(pid, None)
        self._tasks.discard(task)
