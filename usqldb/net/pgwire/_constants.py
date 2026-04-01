#
# usqldb -- PostgreSQL 17-compatible catalog layer for UQA
#
# Copyright (c) 2023-2026 Cognica, Inc.
#

"""PostgreSQL v3 wire protocol constants.

All magic numbers, message type codes, authentication sub-types,
error field identifiers, and default server parameters are collected
here so that the rest of the pgwire package can import a single module.

References:
    https://www.postgresql.org/docs/17/protocol-message-formats.html
    https://www.postgresql.org/docs/17/protocol-error-fields.html
"""

from __future__ import annotations

# ======================================================================
# Protocol version and special request codes
# ======================================================================

PROTOCOL_VERSION = 196608  # 3.0 = (3 << 16) | 0
SSL_REQUEST_CODE = 80877103
CANCEL_REQUEST_CODE = 80877102
GSSENC_REQUEST_CODE = 80877104

# ======================================================================
# Frontend message type codes (client -> server)
# ======================================================================

QUERY = ord("Q")
PARSE = ord("P")
BIND = ord("B")
DESCRIBE = ord("D")
EXECUTE = ord("E")
CLOSE = ord("C")
SYNC = ord("S")
FLUSH = ord("H")
TERMINATE = ord("X")
COPY_DATA = ord("d")
COPY_DONE = ord("c")
COPY_FAIL = ord("f")
PASSWORD = ord("p")
FUNCTION_CALL = ord("F")

# ======================================================================
# Backend message type codes (server -> client)
# ======================================================================

AUTH = ord("R")
PARAMETER_STATUS = ord("S")
BACKEND_KEY_DATA = ord("K")
READY_FOR_QUERY = ord("Z")
ROW_DESCRIPTION = ord("T")
DATA_ROW = ord("D")
COMMAND_COMPLETE = ord("C")
ERROR_RESPONSE = ord("E")
NOTICE_RESPONSE = ord("N")
EMPTY_QUERY = ord("I")
PARSE_COMPLETE = ord("1")
BIND_COMPLETE = ord("2")
CLOSE_COMPLETE = ord("3")
NO_DATA = ord("n")
PARAMETER_DESCRIPTION = ord("t")
PORTAL_SUSPENDED = ord("s")
COPY_IN_RESPONSE = ord("G")
COPY_OUT_RESPONSE = ord("H")
NOTIFICATION = ord("A")

# ======================================================================
# Authentication sub-type codes (inside 'R' messages)
# ======================================================================

AUTH_OK = 0
AUTH_KERBEROS_V5 = 2
AUTH_CLEARTEXT = 3
AUTH_MD5 = 5
AUTH_SCM_CREDENTIAL = 6
AUTH_GSS = 7
AUTH_GSS_CONTINUE = 8
AUTH_SSPI = 9
AUTH_SASL = 10
AUTH_SASL_CONTINUE = 11
AUTH_SASL_FINAL = 12

# ======================================================================
# Error / Notice field codes
# ======================================================================

FIELD_SEVERITY = ord("S")
FIELD_SEVERITY_V = ord("V")  # non-localized severity
FIELD_SQLSTATE = ord("C")
FIELD_MESSAGE = ord("M")
FIELD_DETAIL = ord("D")
FIELD_HINT = ord("H")
FIELD_POSITION = ord("P")
FIELD_INTERNAL_POSITION = ord("p")
FIELD_INTERNAL_QUERY = ord("q")
FIELD_WHERE = ord("W")
FIELD_SCHEMA = ord("s")
FIELD_TABLE = ord("t")
FIELD_COLUMN = ord("c")
FIELD_DATA_TYPE = ord("d")
FIELD_CONSTRAINT = ord("n")
FIELD_FILE = ord("F")
FIELD_LINE = ord("L")
FIELD_ROUTINE = ord("R")

# ======================================================================
# Transaction status indicators (inside 'Z' ReadyForQuery)
# ======================================================================

TX_IDLE = ord("I")
TX_IN_TRANSACTION = ord("T")
TX_FAILED = ord("E")

# ======================================================================
# Format codes
# ======================================================================

FORMAT_TEXT = 0
FORMAT_BINARY = 1

# ======================================================================
# Default server parameters sent during startup
# ======================================================================

DEFAULT_SERVER_PARAMS: dict[str, str] = {
    "server_version": "17.0",
    "server_encoding": "UTF8",
    "client_encoding": "UTF8",
    "DateStyle": "ISO, MDY",
    "TimeZone": "UTC",
    "integer_datetimes": "on",
    "standard_conforming_strings": "on",
    "is_superuser": "on",
    "session_authorization": "uqa",
    "IntervalStyle": "postgres",
    "application_name": "",
    "default_transaction_read_only": "off",
    "in_hot_standby": "off",
}
