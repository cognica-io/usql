#
# usqldb -- PostgreSQL 17-compatible catalog layer for UQA
#
# Copyright (c) 2023-2026 Cognica, Inc.
#

"""Low-level read/write buffers for PostgreSQL wire protocol messages.

ReadBuffer wraps a ``bytes`` object and provides sequential typed reads
(int16, int32, null-terminated string, etc.).

WriteBuffer accumulates payload bytes and finalises them into a complete
PostgreSQL wire message with the type byte and 4-byte length prefix.
"""

from __future__ import annotations

import struct


class ReadBuffer:
    """Sequential reader over a ``bytes`` payload.

    All multi-byte integers are read as big-endian (network byte order),
    matching the PostgreSQL wire protocol specification.
    """

    __slots__ = ("_data", "_pos")

    def __init__(self, data: bytes) -> None:
        self._data = data
        self._pos = 0

    @property
    def remaining(self) -> int:
        """Number of unread bytes."""
        return len(self._data) - self._pos

    def read_byte(self) -> int:
        """Read a single unsigned byte."""
        val = self._data[self._pos]
        self._pos += 1
        return val

    def read_int16(self) -> int:
        """Read a signed 16-bit big-endian integer."""
        val = struct.unpack_from("!h", self._data, self._pos)[0]
        self._pos += 2
        return val

    def read_uint16(self) -> int:
        """Read an unsigned 16-bit big-endian integer."""
        val = struct.unpack_from("!H", self._data, self._pos)[0]
        self._pos += 2
        return val

    def read_int32(self) -> int:
        """Read a signed 32-bit big-endian integer."""
        val = struct.unpack_from("!i", self._data, self._pos)[0]
        self._pos += 4
        return val

    def read_uint32(self) -> int:
        """Read an unsigned 32-bit big-endian integer."""
        val = struct.unpack_from("!I", self._data, self._pos)[0]
        self._pos += 4
        return val

    def read_string(self) -> str:
        """Read a null-terminated UTF-8 string."""
        end = self._data.index(0, self._pos)
        val = self._data[self._pos : end].decode("utf-8")
        self._pos = end + 1  # skip the null terminator
        return val

    def read_bytes(self, n: int) -> bytes:
        """Read exactly *n* raw bytes."""
        val = self._data[self._pos : self._pos + n]
        self._pos += n
        return val

    def read_remaining(self) -> bytes:
        """Read all remaining bytes."""
        val = self._data[self._pos :]
        self._pos = len(self._data)
        return val


class WriteBuffer:
    """Accumulates payload bytes for a single PostgreSQL wire message.

    After writing all fields, call :meth:`finish` to produce the
    complete message bytes (type byte + 4-byte length + payload).
    """

    __slots__ = ("_buf",)

    def __init__(self) -> None:
        self._buf = bytearray()

    def write_byte(self, val: int) -> None:
        """Append a single unsigned byte."""
        self._buf.append(val)

    def write_int16(self, val: int) -> None:
        """Append a signed 16-bit big-endian integer."""
        self._buf.extend(struct.pack("!h", val))

    def write_uint16(self, val: int) -> None:
        """Append an unsigned 16-bit big-endian integer."""
        self._buf.extend(struct.pack("!H", val))

    def write_int32(self, val: int) -> None:
        """Append a signed 32-bit big-endian integer."""
        self._buf.extend(struct.pack("!i", val))

    def write_uint32(self, val: int) -> None:
        """Append an unsigned 32-bit big-endian integer."""
        self._buf.extend(struct.pack("!I", val))

    def write_string(self, val: str) -> None:
        """Append a null-terminated UTF-8 string."""
        self._buf.extend(val.encode("utf-8"))
        self._buf.append(0)

    def write_bytes(self, val: bytes) -> None:
        """Append raw bytes (no length prefix, no terminator)."""
        self._buf.extend(val)

    def finish(self, msg_type: int) -> bytes:
        """Finalise the message with a type byte and length prefix.

        The length field includes itself (4 bytes) but not the type byte,
        matching the PostgreSQL wire protocol convention.
        """
        payload = bytes(self._buf)
        length = len(payload) + 4  # length includes itself
        return bytes([msg_type]) + struct.pack("!i", length) + payload

    def finish_no_type(self) -> bytes:
        """Finalise without a type byte (used for startup responses).

        The returned bytes are just the 4-byte length prefix followed
        by the payload.  This is used for messages that do not have a
        leading type byte, such as the server's SSL response.
        """
        payload = bytes(self._buf)
        length = len(payload) + 4
        return struct.pack("!i", length) + payload
