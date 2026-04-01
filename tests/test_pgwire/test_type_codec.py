#
# usqldb -- PostgreSQL 17-compatible catalog layer for UQA
#
# Copyright (c) 2023-2026 Cognica, Inc.
#

"""Unit tests for TypeCodec."""

from __future__ import annotations

import struct
from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import UUID

from usqldb.net.pgwire._type_codec import TypeCodec
from usqldb.pg_compat.oid import TYPE_OIDS


class TestEncodeText:
    def test_none(self):
        assert TypeCodec.encode_text(None) is None

    def test_bool_true(self):
        assert TypeCodec.encode_text(True) == b"t"

    def test_bool_false(self):
        assert TypeCodec.encode_text(False) == b"f"

    def test_int(self):
        assert TypeCodec.encode_text(42) == b"42"
        assert TypeCodec.encode_text(-1) == b"-1"
        assert TypeCodec.encode_text(0) == b"0"

    def test_float(self):
        result = TypeCodec.encode_text(3.14)
        assert b"3.14" in result

    def test_float_nan(self):
        assert TypeCodec.encode_text(float("nan")) == b"NaN"

    def test_float_inf(self):
        assert TypeCodec.encode_text(float("inf")) == b"Infinity"
        assert TypeCodec.encode_text(float("-inf")) == b"-Infinity"

    def test_str(self):
        assert TypeCodec.encode_text("hello") == b"hello"
        assert TypeCodec.encode_text("") == b""

    def test_bytes(self):
        result = TypeCodec.encode_text(b"\x01\x02\x03")
        assert result == b"\\x010203"

    def test_date(self):
        assert TypeCodec.encode_text(date(2024, 1, 15)) == b"2024-01-15"

    def test_datetime(self):
        dt = datetime(2024, 1, 15, 10, 30, 0)
        result = TypeCodec.encode_text(dt)
        assert b"2024-01-15" in result

    def test_uuid(self):
        u = UUID("12345678-1234-5678-1234-567812345678")
        assert TypeCodec.encode_text(u) == b"12345678-1234-5678-1234-567812345678"

    def test_decimal(self):
        assert TypeCodec.encode_text(Decimal("3.14")) == b"3.14"

    def test_list(self):
        assert TypeCodec.encode_text([1, 2, 3]) == b"{1,2,3}"

    def test_list_with_strings(self):
        result = TypeCodec.encode_text(["a", "b"])
        assert result == b'{"a","b"}'

    def test_list_with_null(self):
        result = TypeCodec.encode_text([1, None, 3])
        assert result == b"{1,NULL,3}"


class TestEncodeBinary:
    def test_none(self):
        assert TypeCodec.encode_binary(None) is None

    def test_bool(self):
        assert TypeCodec.encode_binary(True) == b"\x01"
        assert TypeCodec.encode_binary(False) == b"\x00"

    def test_int_default(self):
        result = TypeCodec.encode_binary(42)
        assert struct.unpack("!i", result)[0] == 42

    def test_int_bigint(self):
        result = TypeCodec.encode_binary(42, TYPE_OIDS["bigint"])
        assert struct.unpack("!q", result)[0] == 42

    def test_int_smallint(self):
        result = TypeCodec.encode_binary(42, TYPE_OIDS["smallint"])
        assert struct.unpack("!h", result)[0] == 42

    def test_float_double(self):
        result = TypeCodec.encode_binary(3.14)
        val = struct.unpack("!d", result)[0]
        assert abs(val - 3.14) < 1e-10

    def test_float_real(self):
        result = TypeCodec.encode_binary(3.14, TYPE_OIDS["real"])
        val = struct.unpack("!f", result)[0]
        assert abs(val - 3.14) < 0.01

    def test_str(self):
        assert TypeCodec.encode_binary("hello") == b"hello"

    def test_bytes(self):
        assert TypeCodec.encode_binary(b"\x01\x02") == b"\x01\x02"

    def test_uuid(self):
        u = UUID("12345678-1234-5678-1234-567812345678")
        result = TypeCodec.encode_binary(u)
        assert len(result) == 16
        assert UUID(bytes=result) == u

    def test_date(self):
        d = date(2024, 1, 15)
        result = TypeCodec.encode_binary(d)
        days = struct.unpack("!i", result)[0]
        # Days since 2000-01-01
        expected = (d - date(2000, 1, 1)).days
        assert days == expected

    def test_datetime(self):
        dt = datetime(2024, 1, 15, 10, 30, 0, tzinfo=UTC)
        result = TypeCodec.encode_binary(dt)
        usec = struct.unpack("!q", result)[0]
        assert usec > 0


class TestDecodeText:
    def test_bool_true(self):
        assert TypeCodec.decode_text(b"t", TYPE_OIDS["boolean"]) is True

    def test_bool_false(self):
        assert TypeCodec.decode_text(b"f", TYPE_OIDS["boolean"]) is False

    def test_int(self):
        assert TypeCodec.decode_text(b"42", TYPE_OIDS["integer"]) == 42

    def test_bigint(self):
        big = 2**40
        assert TypeCodec.decode_text(str(big).encode(), TYPE_OIDS["bigint"]) == big

    def test_float(self):
        val = TypeCodec.decode_text(b"3.14", TYPE_OIDS["double precision"])
        assert abs(val - 3.14) < 1e-10

    def test_numeric(self):
        val = TypeCodec.decode_text(b"3.14", TYPE_OIDS["numeric"])
        assert val == Decimal("3.14")

    def test_uuid(self):
        text = b"12345678-1234-5678-1234-567812345678"
        val = TypeCodec.decode_text(text, TYPE_OIDS["uuid"])
        assert isinstance(val, UUID)

    def test_text(self):
        assert TypeCodec.decode_text(b"hello", TYPE_OIDS["text"]) == "hello"

    def test_bytea_hex(self):
        val = TypeCodec.decode_text(b"\\x0102", TYPE_OIDS["bytea"])
        assert val == b"\x01\x02"


class TestDecodeBinary:
    def test_bool(self):
        assert TypeCodec.decode_binary(b"\x01", TYPE_OIDS["boolean"]) is True
        assert TypeCodec.decode_binary(b"\x00", TYPE_OIDS["boolean"]) is False

    def test_int4(self):
        data = struct.pack("!i", 42)
        assert TypeCodec.decode_binary(data, TYPE_OIDS["integer"]) == 42

    def test_int8(self):
        data = struct.pack("!q", 2**40)
        assert TypeCodec.decode_binary(data, TYPE_OIDS["bigint"]) == 2**40

    def test_float4(self):
        data = struct.pack("!f", 3.14)
        val = TypeCodec.decode_binary(data, TYPE_OIDS["real"])
        assert abs(val - 3.14) < 0.01

    def test_float8(self):
        data = struct.pack("!d", 3.14)
        val = TypeCodec.decode_binary(data, TYPE_OIDS["double precision"])
        assert abs(val - 3.14) < 1e-10

    def test_uuid(self):
        u = UUID("12345678-1234-5678-1234-567812345678")
        val = TypeCodec.decode_binary(u.bytes, TYPE_OIDS["uuid"])
        assert val == u

    def test_date(self):
        d = date(2024, 1, 15)
        days = (d - date(2000, 1, 1)).days
        data = struct.pack("!i", days)
        val = TypeCodec.decode_binary(data, TYPE_OIDS["date"])
        assert val == d

    def test_text(self):
        val = TypeCodec.decode_binary(b"hello", TYPE_OIDS["text"])
        assert val == "hello"


class TestInferTypeOID:
    def test_none(self):
        assert TypeCodec.infer_type_oid(None) == TYPE_OIDS["text"]

    def test_bool(self):
        assert TypeCodec.infer_type_oid(True) == TYPE_OIDS["boolean"]

    def test_int(self):
        assert TypeCodec.infer_type_oid(42) == TYPE_OIDS["integer"]

    def test_big_int(self):
        assert TypeCodec.infer_type_oid(2**40) == TYPE_OIDS["bigint"]

    def test_float(self):
        assert TypeCodec.infer_type_oid(3.14) == TYPE_OIDS["double precision"]

    def test_str(self):
        assert TypeCodec.infer_type_oid("hello") == TYPE_OIDS["text"]

    def test_bytes(self):
        assert TypeCodec.infer_type_oid(b"\x01") == TYPE_OIDS["bytea"]

    def test_datetime(self):
        assert TypeCodec.infer_type_oid(datetime.now()) == TYPE_OIDS["timestamp"]

    def test_datetime_tz(self):
        assert TypeCodec.infer_type_oid(datetime.now(UTC)) == TYPE_OIDS["timestamptz"]

    def test_date(self):
        assert TypeCodec.infer_type_oid(date.today()) == TYPE_OIDS["date"]

    def test_uuid(self):
        u = UUID("12345678-1234-5678-1234-567812345678")
        assert TypeCodec.infer_type_oid(u) == TYPE_OIDS["uuid"]

    def test_decimal(self):
        assert TypeCodec.infer_type_oid(Decimal("1.0")) == TYPE_OIDS["numeric"]
