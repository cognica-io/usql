#
# usqldb -- PostgreSQL 17-compatible catalog layer for UQA
#
# Copyright (c) 2023-2026 Cognica, Inc.
#

"""Unit tests for MessageCodec."""

from __future__ import annotations

import struct

from usqldb.net.pgwire._codec import MessageCodec
from usqldb.net.pgwire._constants import (
    AUTH_CLEARTEXT,
    AUTH_MD5,
    AUTH_OK,
    AUTH_SASL,
    FIELD_MESSAGE,
    FIELD_SEVERITY,
    FIELD_SQLSTATE,
    TX_IDLE,
)
from usqldb.net.pgwire._messages import (
    Bind,
    CancelRequest,
    Close,
    ColumnDescription,
    Describe,
    Execute,
    GSSENCRequest,
    Parse,
    PasswordMessage,
    Query,
    SSLRequest,
    StartupMessage,
    Sync,
    Terminate,
)


class TestDecodeStartup:
    def test_decode_ssl_request(self):
        data = struct.pack("!I", 80877103)
        msg = MessageCodec.decode_startup(data)
        assert isinstance(msg, SSLRequest)

    def test_decode_gssenc_request(self):
        data = struct.pack("!I", 80877104)
        msg = MessageCodec.decode_startup(data)
        assert isinstance(msg, GSSENCRequest)

    def test_decode_cancel_request(self):
        data = struct.pack("!Iii", 80877102, 42, 12345)
        msg = MessageCodec.decode_startup(data)
        assert isinstance(msg, CancelRequest)
        assert msg.process_id == 42
        assert msg.secret_key == 12345

    def test_decode_startup_message(self):
        payload = struct.pack("!I", 196608)
        payload += b"user\x00alice\x00"
        payload += b"database\x00mydb\x00"
        payload += b"\x00"
        msg = MessageCodec.decode_startup(payload)
        assert isinstance(msg, StartupMessage)
        assert msg.protocol_version == 196608
        assert msg.parameters["user"] == "alice"
        assert msg.parameters["database"] == "mydb"


class TestDecodeFrontend:
    def test_decode_query(self):
        payload = b"SELECT 1\x00"
        msg = MessageCodec.decode_frontend(ord("Q"), payload)
        assert isinstance(msg, Query)
        assert msg.sql == "SELECT 1"

    def test_decode_parse(self):
        payload = b"stmt1\x00SELECT $1\x00"
        payload += struct.pack("!h", 1)  # 1 param type
        payload += struct.pack("!i", 23)  # int4
        msg = MessageCodec.decode_frontend(ord("P"), payload)
        assert isinstance(msg, Parse)
        assert msg.statement_name == "stmt1"
        assert msg.query == "SELECT $1"
        assert msg.param_type_oids == [23]

    def test_decode_bind(self):
        payload = b"\x00"  # portal name (unnamed)
        payload += b"stmt1\x00"  # statement name
        payload += struct.pack("!h", 1)  # 1 param format code
        payload += struct.pack("!h", 0)  # text format
        payload += struct.pack("!h", 1)  # 1 param value
        payload += struct.pack("!i", 5)  # value length 5
        payload += b"hello"  # value
        payload += struct.pack("!h", 1)  # 1 result format code
        payload += struct.pack("!h", 0)  # text format
        msg = MessageCodec.decode_frontend(ord("B"), payload)
        assert isinstance(msg, Bind)
        assert msg.portal_name == ""
        assert msg.statement_name == "stmt1"
        assert msg.param_format_codes == [0]
        assert msg.param_values == [b"hello"]
        assert msg.result_format_codes == [0]

    def test_decode_bind_with_null(self):
        payload = b"\x00\x00"  # unnamed portal, unnamed stmt
        payload += struct.pack("!h", 0)  # no param format codes
        payload += struct.pack("!h", 1)  # 1 param value
        payload += struct.pack("!i", -1)  # NULL
        payload += struct.pack("!h", 0)  # no result format codes
        msg = MessageCodec.decode_frontend(ord("B"), payload)
        assert isinstance(msg, Bind)
        assert msg.param_values == [None]

    def test_decode_describe_statement(self):
        payload = b"S" + b"stmt1\x00"
        msg = MessageCodec.decode_frontend(ord("D"), payload)
        assert isinstance(msg, Describe)
        assert msg.kind == "S"
        assert msg.name == "stmt1"

    def test_decode_describe_portal(self):
        payload = b"P" + b"\x00"
        msg = MessageCodec.decode_frontend(ord("D"), payload)
        assert isinstance(msg, Describe)
        assert msg.kind == "P"
        assert msg.name == ""

    def test_decode_execute(self):
        payload = b"\x00"  # unnamed portal
        payload += struct.pack("!i", 100)  # max rows
        msg = MessageCodec.decode_frontend(ord("E"), payload)
        assert isinstance(msg, Execute)
        assert msg.portal_name == ""
        assert msg.max_rows == 100

    def test_decode_close(self):
        payload = b"S" + b"stmt1\x00"
        msg = MessageCodec.decode_frontend(ord("C"), payload)
        assert isinstance(msg, Close)
        assert msg.kind == "S"
        assert msg.name == "stmt1"

    def test_decode_sync(self):
        msg = MessageCodec.decode_frontend(ord("S"), b"")
        assert isinstance(msg, Sync)

    def test_decode_terminate(self):
        msg = MessageCodec.decode_frontend(ord("X"), b"")
        assert isinstance(msg, Terminate)

    def test_decode_password(self):
        payload = b"mysecret\x00"
        msg = MessageCodec.decode_frontend(ord("p"), payload)
        assert isinstance(msg, PasswordMessage)
        assert msg.password == "mysecret"

    def test_decode_unknown_raises(self):
        import pytest

        with pytest.raises(ValueError, match="Unknown frontend"):
            MessageCodec.decode_frontend(ord("~"), b"")


class TestEncodeBackend:
    def test_encode_auth_ok(self):
        data = MessageCodec.encode_auth_ok()
        assert data[0] == ord("R")
        length = struct.unpack("!i", data[1:5])[0]
        assert length == 8
        auth_type = struct.unpack("!i", data[5:9])[0]
        assert auth_type == AUTH_OK

    def test_encode_auth_cleartext(self):
        data = MessageCodec.encode_auth_cleartext()
        assert data[0] == ord("R")
        auth_type = struct.unpack("!i", data[5:9])[0]
        assert auth_type == AUTH_CLEARTEXT

    def test_encode_auth_md5(self):
        salt = b"\x01\x02\x03\x04"
        data = MessageCodec.encode_auth_md5(salt)
        assert data[0] == ord("R")
        auth_type = struct.unpack("!i", data[5:9])[0]
        assert auth_type == AUTH_MD5
        assert data[9:13] == salt

    def test_encode_auth_sasl(self):
        data = MessageCodec.encode_auth_sasl(["SCRAM-SHA-256"])
        assert data[0] == ord("R")
        auth_type = struct.unpack("!i", data[5:9])[0]
        assert auth_type == AUTH_SASL
        # Should contain the mechanism name followed by double null.
        assert b"SCRAM-SHA-256\x00\x00" in data

    def test_encode_parameter_status(self):
        data = MessageCodec.encode_parameter_status("server_version", "17.0")
        assert data[0] == ord("S")
        assert b"server_version\x00" in data
        assert b"17.0\x00" in data

    def test_encode_backend_key_data(self):
        data = MessageCodec.encode_backend_key_data(42, 12345)
        assert data[0] == ord("K")
        length = struct.unpack("!i", data[1:5])[0]
        assert length == 12
        pid = struct.unpack("!i", data[5:9])[0]
        secret = struct.unpack("!i", data[9:13])[0]
        assert pid == 42
        assert secret == 12345

    def test_encode_ready_for_query(self):
        data = MessageCodec.encode_ready_for_query(TX_IDLE)
        assert data[0] == ord("Z")
        length = struct.unpack("!i", data[1:5])[0]
        assert length == 5
        assert data[5] == ord("I")

    def test_encode_row_description(self):
        cols = [
            ColumnDescription("id", 0, 1, 23, 4, -1, 0),
            ColumnDescription("name", 0, 2, 25, -1, -1, 0),
        ]
        data = MessageCodec.encode_row_description(cols)
        assert data[0] == ord("T")
        # Parse column count.
        col_count = struct.unpack("!h", data[5:7])[0]
        assert col_count == 2

    def test_encode_data_row(self):
        values = [b"42", b"hello", None]
        data = MessageCodec.encode_data_row(values)
        assert data[0] == ord("D")
        col_count = struct.unpack("!h", data[5:7])[0]
        assert col_count == 3
        # First value: length 2, "42"
        pos = 7
        l1 = struct.unpack("!i", data[pos : pos + 4])[0]
        assert l1 == 2
        assert data[pos + 4 : pos + 6] == b"42"
        pos += 4 + l1
        # Second value: length 5, "hello"
        l2 = struct.unpack("!i", data[pos : pos + 4])[0]
        assert l2 == 5
        pos += 4 + l2
        # Third value: NULL (-1)
        l3 = struct.unpack("!i", data[pos : pos + 4])[0]
        assert l3 == -1

    def test_encode_command_complete(self):
        data = MessageCodec.encode_command_complete("SELECT 5")
        assert data[0] == ord("C")
        assert b"SELECT 5\x00" in data

    def test_encode_empty_query_response(self):
        data = MessageCodec.encode_empty_query_response()
        assert data[0] == ord("I")
        length = struct.unpack("!i", data[1:5])[0]
        assert length == 4

    def test_encode_error_response(self):
        fields = {
            FIELD_SEVERITY: "ERROR",
            FIELD_SQLSTATE: "42601",
            FIELD_MESSAGE: "syntax error",
        }
        data = MessageCodec.encode_error_response(fields)
        assert data[0] == ord("E")
        # Should end with a null terminator.
        assert data[-1] == 0

    def test_encode_parse_complete(self):
        data = MessageCodec.encode_parse_complete()
        assert data[0] == ord("1")
        assert struct.unpack("!i", data[1:5])[0] == 4

    def test_encode_bind_complete(self):
        data = MessageCodec.encode_bind_complete()
        assert data[0] == ord("2")

    def test_encode_close_complete(self):
        data = MessageCodec.encode_close_complete()
        assert data[0] == ord("3")

    def test_encode_no_data(self):
        data = MessageCodec.encode_no_data()
        assert data[0] == ord("n")

    def test_encode_parameter_description(self):
        data = MessageCodec.encode_parameter_description([23, 25])
        assert data[0] == ord("t")
        n = struct.unpack("!h", data[5:7])[0]
        assert n == 2

    def test_encode_portal_suspended(self):
        data = MessageCodec.encode_portal_suspended()
        assert data[0] == ord("s")

    def test_encode_notification(self):
        data = MessageCodec.encode_notification(1, "channel", "payload")
        assert data[0] == ord("A")
        assert b"channel\x00" in data
        assert b"payload\x00" in data
