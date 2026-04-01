#
# usqldb -- PostgreSQL 17-compatible catalog layer for UQA
#
# Copyright (c) 2023-2026 Cognica, Inc.
#

"""Unit tests for ReadBuffer and WriteBuffer."""

from __future__ import annotations

import struct

from usqldb.net.pgwire._buffer import ReadBuffer, WriteBuffer


class TestReadBuffer:
    def test_read_byte(self):
        buf = ReadBuffer(b"\x42")
        assert buf.read_byte() == 0x42

    def test_read_int16(self):
        data = struct.pack("!h", -1234)
        buf = ReadBuffer(data)
        assert buf.read_int16() == -1234

    def test_read_uint16(self):
        data = struct.pack("!H", 65000)
        buf = ReadBuffer(data)
        assert buf.read_uint16() == 65000

    def test_read_int32(self):
        data = struct.pack("!i", -100000)
        buf = ReadBuffer(data)
        assert buf.read_int32() == -100000

    def test_read_uint32(self):
        data = struct.pack("!I", 3000000000)
        buf = ReadBuffer(data)
        assert buf.read_uint32() == 3000000000

    def test_read_string(self):
        buf = ReadBuffer(b"hello\x00world\x00")
        assert buf.read_string() == "hello"
        assert buf.read_string() == "world"

    def test_read_empty_string(self):
        buf = ReadBuffer(b"\x00")
        assert buf.read_string() == ""

    def test_read_bytes(self):
        buf = ReadBuffer(b"\x01\x02\x03\x04\x05")
        assert buf.read_bytes(3) == b"\x01\x02\x03"
        assert buf.read_bytes(2) == b"\x04\x05"

    def test_read_remaining(self):
        buf = ReadBuffer(b"\x01\x02\x03\x04\x05")
        buf.read_bytes(2)
        assert buf.read_remaining() == b"\x03\x04\x05"

    def test_remaining_property(self):
        buf = ReadBuffer(b"\x01\x02\x03")
        assert buf.remaining == 3
        buf.read_byte()
        assert buf.remaining == 2

    def test_sequential_reads(self):
        data = struct.pack("!ih", 42, 7) + b"test\x00"
        buf = ReadBuffer(data)
        assert buf.read_int32() == 42
        assert buf.read_int16() == 7
        assert buf.read_string() == "test"
        assert buf.remaining == 0


class TestWriteBuffer:
    def test_write_byte(self):
        buf = WriteBuffer()
        buf.write_byte(0x42)
        msg = buf.finish(ord("T"))
        # type(1) + length(4) + payload(1) = 6 bytes
        assert msg[0] == ord("T")
        length = struct.unpack("!i", msg[1:5])[0]
        assert length == 5  # 4 (self) + 1 byte
        assert msg[5] == 0x42

    def test_write_int16(self):
        buf = WriteBuffer()
        buf.write_int16(-1234)
        msg = buf.finish(ord("T"))
        payload = msg[5:]
        assert struct.unpack("!h", payload)[0] == -1234

    def test_write_int32(self):
        buf = WriteBuffer()
        buf.write_int32(-100000)
        msg = buf.finish(ord("T"))
        payload = msg[5:]
        assert struct.unpack("!i", payload)[0] == -100000

    def test_write_string(self):
        buf = WriteBuffer()
        buf.write_string("hello")
        msg = buf.finish(ord("T"))
        payload = msg[5:]
        assert payload == b"hello\x00"

    def test_write_bytes(self):
        buf = WriteBuffer()
        buf.write_bytes(b"\x01\x02\x03")
        msg = buf.finish(ord("T"))
        payload = msg[5:]
        assert payload == b"\x01\x02\x03"

    def test_finish_no_type(self):
        buf = WriteBuffer()
        buf.write_int32(196608)
        msg = buf.finish_no_type()
        # length(4) + payload(4) = 8 bytes
        length = struct.unpack("!I", msg[:4])[0]
        assert length == 8
        version = struct.unpack("!I", msg[4:8])[0]
        assert version == 196608

    def test_complex_message(self):
        """Test building a complete RowDescription-like message."""
        buf = WriteBuffer()
        buf.write_int16(1)  # column count
        buf.write_string("id")  # column name
        buf.write_int32(0)  # table OID
        buf.write_int16(1)  # column number
        buf.write_int32(23)  # type OID (int4)
        buf.write_int16(4)  # type size
        buf.write_int32(-1)  # type modifier
        buf.write_int16(0)  # format code (text)
        msg = buf.finish(ord("T"))
        assert msg[0] == ord("T")


class TestBufferRoundTrip:
    def test_int32_round_trip(self):
        for val in [0, 1, -1, 2**31 - 1, -(2**31)]:
            wbuf = WriteBuffer()
            wbuf.write_int32(val)
            raw = wbuf.finish(ord("X"))
            rbuf = ReadBuffer(raw[5:])  # skip type + length
            assert rbuf.read_int32() == val

    def test_string_round_trip(self):
        for val in ["", "hello", "test with spaces", "unicode: abc"]:
            wbuf = WriteBuffer()
            wbuf.write_string(val)
            raw = wbuf.finish(ord("X"))
            rbuf = ReadBuffer(raw[5:])
            assert rbuf.read_string() == val
