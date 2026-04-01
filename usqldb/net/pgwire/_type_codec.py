#
# usqldb -- PostgreSQL 17-compatible catalog layer for UQA
#
# Copyright (c) 2023-2026 Cognica, Inc.
#

"""Convert between Python values and PostgreSQL wire format.

The :class:`TypeCodec` provides static methods for encoding Python
values to PostgreSQL text or binary format, decoding wire bytes back
to Python values, and inferring PostgreSQL type OIDs from Python types.

Text format is the default for psql, psycopg2, JDBC.  Binary format
is used exclusively by asyncpg and optionally by psycopg3.

References:
    https://www.postgresql.org/docs/17/protocol-overview.html#PROTOCOL-FORMAT-CODES
"""

from __future__ import annotations

import struct
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from typing import Any
from uuid import UUID

from usqldb.pg_compat.oid import TYPE_LENGTHS, TYPE_OIDS

# PostgreSQL epoch: 2000-01-01 00:00:00 UTC
_PG_EPOCH = datetime(2000, 1, 1, tzinfo=UTC)
_PG_EPOCH_DATE = date(2000, 1, 1)
_PG_EPOCH_ORDINAL = _PG_EPOCH_DATE.toordinal()

# Microseconds per second/day
_USEC_PER_SEC = 1_000_000
_USEC_PER_DAY = 86_400_000_000


class TypeCodec:
    """Encode/decode values between Python types and PostgreSQL wire format."""

    # ==================================================================
    # Text format encoding (Python -> PG text bytes)
    # ==================================================================

    @staticmethod
    def encode_text(value: Any, type_oid: int = 0) -> bytes | None:
        """Encode a Python value as PostgreSQL text-format bytes.

        Returns ``None`` for SQL NULL.
        """
        if value is None:
            return None

        if isinstance(value, bool):
            return b"t" if value else b"f"

        if isinstance(value, int):
            return str(value).encode("ascii")

        if isinstance(value, float):
            if value != value:  # NaN
                return b"NaN"
            if value == float("inf"):
                return b"Infinity"
            if value == float("-inf"):
                return b"-Infinity"
            return repr(value).encode("ascii")

        if isinstance(value, Decimal):
            return str(value).encode("ascii")

        if isinstance(value, str):
            return value.encode("utf-8")

        if isinstance(value, bytes):
            return b"\\x" + value.hex().encode("ascii")

        if isinstance(value, datetime):
            if value.tzinfo is not None:
                return value.isoformat().encode("ascii")
            return value.isoformat().encode("ascii")

        if isinstance(value, date):
            return value.isoformat().encode("ascii")

        if isinstance(value, time):
            return value.isoformat().encode("ascii")

        if isinstance(value, timedelta):
            return _encode_interval_text(value)

        if isinstance(value, UUID):
            return str(value).encode("ascii")

        if isinstance(value, list):
            return _encode_array_text(value).encode("utf-8")

        # Fallback: use str()
        return str(value).encode("utf-8")

    # ==================================================================
    # Binary format encoding (Python -> PG binary bytes)
    # ==================================================================

    @staticmethod
    def encode_binary(value: Any, type_oid: int = 0) -> bytes | None:
        """Encode a Python value as PostgreSQL binary-format bytes.

        Returns ``None`` for SQL NULL.
        """
        if value is None:
            return None

        if isinstance(value, bool):
            return b"\x01" if value else b"\x00"

        if isinstance(value, int):
            if type_oid == TYPE_OIDS["smallint"]:
                return struct.pack("!h", value)
            if type_oid == TYPE_OIDS["bigint"]:
                return struct.pack("!q", value)
            # Default to int4 for integer
            if -(2**31) <= value <= 2**31 - 1:
                return struct.pack("!i", value)
            return struct.pack("!q", value)

        if isinstance(value, float):
            if type_oid == TYPE_OIDS.get("real", 700):
                return struct.pack("!f", value)
            return struct.pack("!d", value)

        if isinstance(value, str):
            return value.encode("utf-8")

        if isinstance(value, bytes):
            return value

        if isinstance(value, datetime):
            if value.tzinfo is not None:
                delta = value - _PG_EPOCH
            else:
                delta = value - _PG_EPOCH.replace(tzinfo=None)
            usec = int(delta.total_seconds() * _USEC_PER_SEC)
            return struct.pack("!q", usec)

        if isinstance(value, date):
            days = value.toordinal() - _PG_EPOCH_ORDINAL
            return struct.pack("!i", days)

        if isinstance(value, UUID):
            return value.bytes

        if isinstance(value, Decimal):
            # Fall back to text encoding for Decimal in binary mode.
            return str(value).encode("ascii")

        # Fallback: use text encoding
        return TypeCodec.encode_text(value, type_oid)

    # ==================================================================
    # Text format decoding (PG text bytes -> Python)
    # ==================================================================

    @staticmethod
    def decode_text(data: bytes, type_oid: int) -> Any:
        """Decode PostgreSQL text-format bytes to a Python value."""
        text = data.decode("utf-8")

        if type_oid == TYPE_OIDS["boolean"]:
            return text.lower() in ("t", "true", "1", "yes", "on")

        if type_oid in (
            TYPE_OIDS["integer"],
            TYPE_OIDS["smallint"],
            TYPE_OIDS["bigint"],
        ):
            return int(text)

        if type_oid in (TYPE_OIDS["real"], TYPE_OIDS["double precision"]):
            return float(text)

        if type_oid == TYPE_OIDS["numeric"]:
            return Decimal(text)

        if type_oid == TYPE_OIDS["uuid"]:
            return UUID(text)

        if type_oid == TYPE_OIDS["bytea"]:
            if text.startswith("\\x"):
                return bytes.fromhex(text[2:])
            return text.encode("utf-8")

        # text, varchar, name, json, jsonb, xml, etc.
        return text

    # ==================================================================
    # Binary format decoding (PG binary bytes -> Python)
    # ==================================================================

    @staticmethod
    def decode_binary(data: bytes, type_oid: int) -> Any:
        """Decode PostgreSQL binary-format bytes to a Python value."""
        if type_oid == TYPE_OIDS["boolean"]:
            return data[0] != 0

        if type_oid == TYPE_OIDS["smallint"]:
            return struct.unpack("!h", data)[0]

        if type_oid == TYPE_OIDS["integer"]:
            return struct.unpack("!i", data)[0]

        if type_oid == TYPE_OIDS["bigint"]:
            return struct.unpack("!q", data)[0]

        if type_oid == TYPE_OIDS["real"]:
            return struct.unpack("!f", data)[0]

        if type_oid == TYPE_OIDS["double precision"]:
            return struct.unpack("!d", data)[0]

        if type_oid == TYPE_OIDS["uuid"]:
            return UUID(bytes=data)

        if type_oid == TYPE_OIDS["bytea"]:
            return data

        if type_oid == TYPE_OIDS["date"]:
            days = struct.unpack("!i", data)[0]
            return date.fromordinal(_PG_EPOCH_ORDINAL + days)

        if type_oid in (TYPE_OIDS["timestamp"], TYPE_OIDS["timestamptz"]):
            usec = struct.unpack("!q", data)[0]
            return _PG_EPOCH + timedelta(microseconds=usec)

        if type_oid == TYPE_OIDS.get("oid", 26):
            return struct.unpack("!I", data)[0]

        # Default: treat as UTF-8 text
        return data.decode("utf-8")

    # ==================================================================
    # Type inference
    # ==================================================================

    @staticmethod
    def infer_type_oid(value: Any) -> int:
        """Infer the PostgreSQL type OID from a Python value."""
        if value is None:
            return TYPE_OIDS["text"]
        if isinstance(value, bool):
            return TYPE_OIDS["boolean"]
        if isinstance(value, int):
            if -(2**31) <= value <= 2**31 - 1:
                return TYPE_OIDS["integer"]
            return TYPE_OIDS["bigint"]
        if isinstance(value, float):
            return TYPE_OIDS["double precision"]
        if isinstance(value, Decimal):
            return TYPE_OIDS["numeric"]
        if isinstance(value, str):
            return TYPE_OIDS["text"]
        if isinstance(value, bytes):
            return TYPE_OIDS["bytea"]
        if isinstance(value, datetime):
            if value.tzinfo is not None:
                return TYPE_OIDS["timestamptz"]
            return TYPE_OIDS["timestamp"]
        if isinstance(value, date):
            return TYPE_OIDS["date"]
        if isinstance(value, time):
            return TYPE_OIDS["time"]
        if isinstance(value, UUID):
            return TYPE_OIDS["uuid"]
        return TYPE_OIDS["text"]

    @staticmethod
    def type_size(type_oid: int) -> int:
        """Return the wire type size for a type OID (-1 for variable)."""
        return TYPE_LENGTHS.get(type_oid, -1)


# ======================================================================
# Internal helpers
# ======================================================================


def _encode_interval_text(td: timedelta) -> bytes:
    """Encode a timedelta as a PostgreSQL interval text literal."""
    total_seconds = int(td.total_seconds())
    days = td.days
    hours, rem = divmod(abs(total_seconds) - abs(days) * 86400, 3600)
    minutes, seconds = divmod(rem, 60)

    parts: list[str] = []
    if days:
        parts.append(f"{days} day{'s' if abs(days) != 1 else ''}")
    time_str = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    if total_seconds < 0 and not days:
        time_str = "-" + time_str
    parts.append(time_str)
    return " ".join(parts).encode("ascii")


def _encode_array_text(values: list[Any]) -> str:
    """Encode a Python list as a PostgreSQL text array literal."""
    elements: list[str] = []
    for v in values:
        if v is None:
            elements.append("NULL")
        elif isinstance(v, str):
            escaped = v.replace("\\", "\\\\").replace('"', '\\"')
            elements.append(f'"{escaped}"')
        elif isinstance(v, bool):
            elements.append("t" if v else "f")
        elif isinstance(v, list):
            elements.append(_encode_array_text(v))
        else:
            elements.append(str(v))
    return "{" + ",".join(elements) + "}"
