#
# usqldb -- PostgreSQL 17-compatible catalog layer for UQA
#
# Copyright (c) 2023-2026 Cognica, Inc.
#

"""Frontend (client -> server) message dataclasses and shared types.

Every message that a PostgreSQL client can send is modelled as a frozen,
slotted dataclass.  Backend (server -> client) messages are not modelled
here -- they are encoded directly by :class:`MessageCodec` to avoid an
unnecessary intermediate representation.

The :class:`ColumnDescription` named tuple is used by both the codec
(for RowDescription encoding) and the query executor (for building
column metadata).
"""

from __future__ import annotations

import dataclasses
from typing import NamedTuple

# ======================================================================
# Shared types
# ======================================================================


class ColumnDescription(NamedTuple):
    """Metadata for a single column in a RowDescription message."""

    name: str
    table_oid: int
    column_number: int
    type_oid: int
    type_size: int
    type_modifier: int
    format_code: int


# ======================================================================
# Startup-phase messages (no type byte prefix)
# ======================================================================


@dataclasses.dataclass(frozen=True, slots=True)
class StartupMessage:
    """Initial connection handshake with protocol version and parameters."""

    protocol_version: int
    parameters: dict[str, str]


@dataclasses.dataclass(frozen=True, slots=True)
class SSLRequest:
    """Client requests SSL/TLS upgrade."""


@dataclasses.dataclass(frozen=True, slots=True)
class GSSENCRequest:
    """Client requests GSSAPI encryption."""


@dataclasses.dataclass(frozen=True, slots=True)
class CancelRequest:
    """Client requests cancellation of a running query."""

    process_id: int
    secret_key: int


# ======================================================================
# Authentication messages
# ======================================================================


@dataclasses.dataclass(frozen=True, slots=True)
class PasswordMessage:
    """Cleartext or MD5-hashed password from client."""

    password: str


@dataclasses.dataclass(frozen=True, slots=True)
class SASLInitialResponse:
    """First SASL message from client (mechanism selection + data)."""

    mechanism: str
    data: bytes


@dataclasses.dataclass(frozen=True, slots=True)
class SASLResponse:
    """Subsequent SASL message from client."""

    data: bytes


# ======================================================================
# Simple query protocol
# ======================================================================


@dataclasses.dataclass(frozen=True, slots=True)
class Query:
    """Simple query containing one or more SQL statements."""

    sql: str


# ======================================================================
# Extended query protocol
# ======================================================================


@dataclasses.dataclass(frozen=True, slots=True)
class Parse:
    """Create a prepared statement."""

    statement_name: str
    query: str
    param_type_oids: list[int]


@dataclasses.dataclass(frozen=True, slots=True)
class Bind:
    """Bind parameters to a prepared statement, creating a portal."""

    portal_name: str
    statement_name: str
    param_format_codes: list[int]
    param_values: list[bytes | None]
    result_format_codes: list[int]


@dataclasses.dataclass(frozen=True, slots=True)
class Describe:
    """Request description of a statement ('S') or portal ('P')."""

    kind: str  # 'S' for statement, 'P' for portal
    name: str


@dataclasses.dataclass(frozen=True, slots=True)
class Execute:
    """Execute a named portal."""

    portal_name: str
    max_rows: int


@dataclasses.dataclass(frozen=True, slots=True)
class Close:
    """Close a prepared statement ('S') or portal ('P')."""

    kind: str  # 'S' or 'P'
    name: str


@dataclasses.dataclass(frozen=True, slots=True)
class Sync:
    """End of an extended-query batch -- server must send ReadyForQuery."""


@dataclasses.dataclass(frozen=True, slots=True)
class Flush:
    """Request the server to flush its output buffer."""


# ======================================================================
# COPY protocol
# ======================================================================


@dataclasses.dataclass(frozen=True, slots=True)
class CopyData:
    """A chunk of COPY data from the client."""

    data: bytes


@dataclasses.dataclass(frozen=True, slots=True)
class CopyDone:
    """Client signals completion of COPY IN data."""


@dataclasses.dataclass(frozen=True, slots=True)
class CopyFail:
    """Client signals failure of COPY IN."""

    message: str


# ======================================================================
# Other
# ======================================================================


@dataclasses.dataclass(frozen=True, slots=True)
class Terminate:
    """Client requests graceful connection close."""


@dataclasses.dataclass(frozen=True, slots=True)
class FunctionCall:
    """Deprecated function call protocol (PostgreSQL 7.3+)."""

    function_oid: int
    arg_format_codes: list[int]
    arguments: list[bytes | None]
    result_format: int
