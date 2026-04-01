#
# usqldb -- PostgreSQL 17-compatible catalog layer for UQA
#
# Copyright (c) 2023-2026 Cognica, Inc.
#

"""Encode and decode PostgreSQL v3 wire protocol messages.

The :class:`MessageCodec` exposes only static methods and carries no
state.  Decoding methods convert raw ``bytes`` into the frontend
message dataclasses defined in :mod:`_messages`.  Encoding methods
produce ``bytes`` ready for writing to the transport.
"""

from __future__ import annotations

import struct

from usqldb.net.pgwire._buffer import ReadBuffer, WriteBuffer
from usqldb.net.pgwire._constants import (
    AUTH_CLEARTEXT,
    AUTH_MD5,
    AUTH_OK,
    AUTH_SASL,
    AUTH_SASL_CONTINUE,
    AUTH_SASL_FINAL,
    CANCEL_REQUEST_CODE,
    COMMAND_COMPLETE,
    COPY_IN_RESPONSE,
    COPY_OUT_RESPONSE,
    DATA_ROW,
    ERROR_RESPONSE,
    GSSENC_REQUEST_CODE,
    NOTICE_RESPONSE,
    NOTIFICATION,
    PARAMETER_DESCRIPTION,
    PARAMETER_STATUS,
    ROW_DESCRIPTION,
    SSL_REQUEST_CODE,
)
from usqldb.net.pgwire._messages import (
    Bind,
    CancelRequest,
    Close,
    ColumnDescription,
    CopyData,
    CopyDone,
    CopyFail,
    Describe,
    Execute,
    Flush,
    FunctionCall,
    GSSENCRequest,
    Parse,
    PasswordMessage,
    Query,
    SASLInitialResponse,
    SASLResponse,
    SSLRequest,
    StartupMessage,
    Sync,
    Terminate,
)

# Type alias for the union of all frontend messages.
FrontendMessage = (
    Query
    | Parse
    | Bind
    | Describe
    | Execute
    | Close
    | Sync
    | Flush
    | Terminate
    | CopyData
    | CopyDone
    | CopyFail
    | PasswordMessage
    | SASLInitialResponse
    | SASLResponse
    | FunctionCall
)

# Type alias for startup-phase messages.
StartupPhaseMessage = StartupMessage | SSLRequest | GSSENCRequest | CancelRequest


class MessageCodec:
    """Stateless encoder/decoder for the PostgreSQL v3 wire protocol."""

    # ==================================================================
    # Decoding -- frontend messages
    # ==================================================================

    @staticmethod
    def decode_startup(data: bytes) -> StartupPhaseMessage:
        """Decode an un-typed startup-phase message.

        The caller has already read the 4-byte length and the full
        payload; *data* is the payload **after** the length field.
        """
        buf = ReadBuffer(data)
        code = buf.read_uint32()

        if code == SSL_REQUEST_CODE:
            return SSLRequest()
        if code == GSSENC_REQUEST_CODE:
            return GSSENCRequest()
        if code == CANCEL_REQUEST_CODE:
            process_id = buf.read_int32()
            secret_key = buf.read_int32()
            return CancelRequest(process_id=process_id, secret_key=secret_key)

        # Regular startup message: code is the protocol version.
        params: dict[str, str] = {}
        while buf.remaining > 1:
            key = buf.read_string()
            if not key:
                break
            value = buf.read_string()
            params[key] = value
        return StartupMessage(protocol_version=code, parameters=params)

    @staticmethod
    def decode_frontend(msg_type: int, payload: bytes) -> FrontendMessage:
        """Decode a typed frontend message."""
        buf = ReadBuffer(payload)

        if msg_type == ord("Q"):
            return Query(sql=buf.read_string())

        if msg_type == ord("P"):
            name = buf.read_string()
            query = buf.read_string()
            n_params = buf.read_int16()
            oids = [buf.read_int32() for _ in range(n_params)]
            return Parse(statement_name=name, query=query, param_type_oids=oids)

        if msg_type == ord("B"):
            portal = buf.read_string()
            statement = buf.read_string()
            n_param_fmt = buf.read_int16()
            param_fmts = [buf.read_int16() for _ in range(n_param_fmt)]
            n_params = buf.read_int16()
            values: list[bytes | None] = []
            for _ in range(n_params):
                length = buf.read_int32()
                if length == -1:
                    values.append(None)
                else:
                    values.append(buf.read_bytes(length))
            n_result_fmt = buf.read_int16()
            result_fmts = [buf.read_int16() for _ in range(n_result_fmt)]
            return Bind(
                portal_name=portal,
                statement_name=statement,
                param_format_codes=param_fmts,
                param_values=values,
                result_format_codes=result_fmts,
            )

        if msg_type == ord("D"):
            kind = chr(buf.read_byte())
            name = buf.read_string()
            return Describe(kind=kind, name=name)

        if msg_type == ord("E"):
            portal = buf.read_string()
            max_rows = buf.read_int32()
            return Execute(portal_name=portal, max_rows=max_rows)

        if msg_type == ord("C"):
            kind = chr(buf.read_byte())
            name = buf.read_string()
            return Close(kind=kind, name=name)

        if msg_type == ord("S"):
            return Sync()

        if msg_type == ord("H"):
            return Flush()

        if msg_type == ord("X"):
            return Terminate()

        if msg_type == ord("d"):
            return CopyData(data=buf.read_remaining())

        if msg_type == ord("c"):
            return CopyDone()

        if msg_type == ord("f"):
            return CopyFail(message=buf.read_string())

        if msg_type == ord("p"):
            # Could be PasswordMessage, SASLInitialResponse, or SASLResponse.
            # The caller determines which based on the auth state.
            # We return a PasswordMessage by default; the auth handler
            # re-parses if needed.
            return PasswordMessage(password=payload[:-1].decode("utf-8"))

        if msg_type == ord("F"):
            oid = buf.read_int32()
            n_arg_fmt = buf.read_int16()
            arg_fmts = [buf.read_int16() for _ in range(n_arg_fmt)]
            n_args = buf.read_int16()
            args: list[bytes | None] = []
            for _ in range(n_args):
                length = buf.read_int32()
                if length == -1:
                    args.append(None)
                else:
                    args.append(buf.read_bytes(length))
            result_fmt = buf.read_int16()
            return FunctionCall(
                function_oid=oid,
                arg_format_codes=arg_fmts,
                arguments=args,
                result_format=result_fmt,
            )

        # Unknown message type -- the connection handler should send an error.
        raise ValueError(f"Unknown frontend message type: {chr(msg_type)!r}")

    @staticmethod
    def decode_sasl_initial_response(payload: bytes) -> SASLInitialResponse:
        """Re-parse a 'p' message payload as SASLInitialResponse."""
        buf = ReadBuffer(payload)
        mechanism = buf.read_string()
        length = buf.read_int32()
        data = buf.read_bytes(length) if length >= 0 else b""
        return SASLInitialResponse(mechanism=mechanism, data=data)

    @staticmethod
    def decode_sasl_response(payload: bytes) -> SASLResponse:
        """Re-parse a 'p' message payload as SASLResponse."""
        return SASLResponse(data=payload)

    # ==================================================================
    # Encoding -- backend messages
    # ==================================================================

    @staticmethod
    def encode_auth_ok() -> bytes:
        """AuthenticationOk (R, type=0)."""
        return struct.pack("!cii", b"R", 8, AUTH_OK)

    @staticmethod
    def encode_auth_cleartext() -> bytes:
        """AuthenticationCleartextPassword (R, type=3)."""
        return struct.pack("!cii", b"R", 8, AUTH_CLEARTEXT)

    @staticmethod
    def encode_auth_md5(salt: bytes) -> bytes:
        """AuthenticationMD5Password (R, type=5) with 4-byte salt."""
        return struct.pack("!cii", b"R", 12, AUTH_MD5) + salt

    @staticmethod
    def encode_auth_sasl(mechanisms: list[str]) -> bytes:
        """AuthenticationSASL (R, type=10) listing mechanisms."""
        body = b""
        for mech in mechanisms:
            body += mech.encode("ascii") + b"\x00"
        body += b"\x00"  # empty string terminator
        length = 4 + 4 + len(body)
        return struct.pack("!cii", b"R", length, AUTH_SASL) + body

    @staticmethod
    def encode_auth_sasl_continue(data: bytes) -> bytes:
        """AuthenticationSASLContinue (R, type=11)."""
        length = 4 + 4 + len(data)
        return struct.pack("!cii", b"R", length, AUTH_SASL_CONTINUE) + data

    @staticmethod
    def encode_auth_sasl_final(data: bytes) -> bytes:
        """AuthenticationSASLFinal (R, type=12)."""
        length = 4 + 4 + len(data)
        return struct.pack("!cii", b"R", length, AUTH_SASL_FINAL) + data

    @staticmethod
    def encode_parameter_status(name: str, value: str) -> bytes:
        """ParameterStatus (S)."""
        buf = WriteBuffer()
        buf.write_string(name)
        buf.write_string(value)
        return buf.finish(PARAMETER_STATUS)

    @staticmethod
    def encode_backend_key_data(pid: int, secret: int) -> bytes:
        """BackendKeyData (K)."""
        return struct.pack("!ciii", b"K", 12, pid, secret)

    @staticmethod
    def encode_ready_for_query(tx_status: int) -> bytes:
        """ReadyForQuery (Z) with transaction status byte."""
        return struct.pack("!ci", b"Z", 5) + bytes([tx_status])

    @staticmethod
    def encode_row_description(columns: list[ColumnDescription]) -> bytes:
        """RowDescription (T) with full column metadata."""
        buf = WriteBuffer()
        buf.write_int16(len(columns))
        for col in columns:
            buf.write_string(col.name)
            buf.write_int32(col.table_oid)
            buf.write_int16(col.column_number)
            buf.write_int32(col.type_oid)
            buf.write_int16(col.type_size)
            buf.write_int32(col.type_modifier)
            buf.write_int16(col.format_code)
        return buf.finish(ROW_DESCRIPTION)

    @staticmethod
    def encode_data_row(values: list[bytes | None]) -> bytes:
        """DataRow (D) with column values."""
        buf = WriteBuffer()
        buf.write_int16(len(values))
        for val in values:
            if val is None:
                buf.write_int32(-1)
            else:
                buf.write_int32(len(val))
                buf.write_bytes(val)
        return buf.finish(DATA_ROW)

    @staticmethod
    def encode_command_complete(tag: str) -> bytes:
        """CommandComplete (C) with command tag string."""
        buf = WriteBuffer()
        buf.write_string(tag)
        return buf.finish(COMMAND_COMPLETE)

    @staticmethod
    def encode_empty_query_response() -> bytes:
        """EmptyQueryResponse (I)."""
        return struct.pack("!ci", b"I", 4)

    @staticmethod
    def encode_error_response(fields: dict[int, str]) -> bytes:
        """ErrorResponse (E) with typed fields."""
        buf = WriteBuffer()
        for code, value in fields.items():
            buf.write_byte(code)
            buf.write_string(value)
        buf.write_byte(0)  # terminator
        return buf.finish(ERROR_RESPONSE)

    @staticmethod
    def encode_notice_response(fields: dict[int, str]) -> bytes:
        """NoticeResponse (N) with typed fields."""
        buf = WriteBuffer()
        for code, value in fields.items():
            buf.write_byte(code)
            buf.write_string(value)
        buf.write_byte(0)
        return buf.finish(NOTICE_RESPONSE)

    @staticmethod
    def encode_parse_complete() -> bytes:
        """ParseComplete (1)."""
        return struct.pack("!ci", b"1", 4)

    @staticmethod
    def encode_bind_complete() -> bytes:
        """BindComplete (2)."""
        return struct.pack("!ci", b"2", 4)

    @staticmethod
    def encode_close_complete() -> bytes:
        """CloseComplete (3)."""
        return struct.pack("!ci", b"3", 4)

    @staticmethod
    def encode_no_data() -> bytes:
        """NoData (n)."""
        return struct.pack("!ci", b"n", 4)

    @staticmethod
    def encode_parameter_description(oids: list[int]) -> bytes:
        """ParameterDescription (t)."""
        buf = WriteBuffer()
        buf.write_int16(len(oids))
        for oid in oids:
            buf.write_int32(oid)
        return buf.finish(PARAMETER_DESCRIPTION)

    @staticmethod
    def encode_portal_suspended() -> bytes:
        """PortalSuspended (s)."""
        return struct.pack("!ci", b"s", 4)

    @staticmethod
    def encode_copy_in_response(
        overall_format: int,
        column_formats: list[int],
    ) -> bytes:
        """CopyInResponse (G)."""
        buf = WriteBuffer()
        buf.write_byte(overall_format)
        buf.write_int16(len(column_formats))
        for fmt in column_formats:
            buf.write_int16(fmt)
        return buf.finish(COPY_IN_RESPONSE)

    @staticmethod
    def encode_copy_out_response(
        overall_format: int,
        column_formats: list[int],
    ) -> bytes:
        """CopyOutResponse (H)."""
        buf = WriteBuffer()
        buf.write_byte(overall_format)
        buf.write_int16(len(column_formats))
        for fmt in column_formats:
            buf.write_int16(fmt)
        return buf.finish(COPY_OUT_RESPONSE)

    @staticmethod
    def encode_notification(pid: int, channel: str, payload: str) -> bytes:
        """NotificationResponse (A)."""
        buf = WriteBuffer()
        buf.write_int32(pid)
        buf.write_string(channel)
        buf.write_string(payload)
        return buf.finish(NOTIFICATION)
