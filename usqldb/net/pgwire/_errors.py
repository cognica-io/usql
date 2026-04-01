#
# usqldb -- PostgreSQL 17-compatible catalog layer for UQA
#
# Copyright (c) 2023-2026 Cognica, Inc.
#

"""PostgreSQL error hierarchy and SQLSTATE mapping.

Every error that the pgwire server can surface to a client is represented
as a :class:`PGWireError` (or subclass) carrying a 5-character SQLSTATE
code.  The :func:`map_engine_exception` helper converts exceptions raised
by ``USQLEngine`` or ``pglast`` into the appropriate subclass.

SQLSTATE reference:
    https://www.postgresql.org/docs/17/errcodes-appendix.html
"""

from __future__ import annotations

from usqldb.net.pgwire._constants import (
    FIELD_DETAIL,
    FIELD_HINT,
    FIELD_MESSAGE,
    FIELD_POSITION,
    FIELD_SEVERITY,
    FIELD_SEVERITY_V,
    FIELD_SQLSTATE,
)


class PGWireError(Exception):
    """Base class for errors surfaced over the wire protocol."""

    severity: str = "ERROR"
    sqlstate: str = "XX000"  # internal_error

    def __init__(
        self,
        message: str,
        *,
        detail: str | None = None,
        hint: str | None = None,
        position: int | None = None,
    ) -> None:
        super().__init__(message)
        self.detail = detail
        self.hint = hint
        self.position = position

    def to_fields(self) -> dict[int, str]:
        """Build the ErrorResponse field dict for wire encoding."""
        fields: dict[int, str] = {
            FIELD_SEVERITY: self.severity,
            FIELD_SEVERITY_V: self.severity,
            FIELD_SQLSTATE: self.sqlstate,
            FIELD_MESSAGE: str(self),
        }
        if self.detail is not None:
            fields[FIELD_DETAIL] = self.detail
        if self.hint is not None:
            fields[FIELD_HINT] = self.hint
        if self.position is not None:
            fields[FIELD_POSITION] = str(self.position)
        return fields


# -- Syntax / Schema errors (Class 42) ------------------------------------


class SQLSyntaxError(PGWireError):
    """42601 -- syntax_error"""

    sqlstate = "42601"


class UndefinedTable(PGWireError):
    """42P01 -- undefined_table"""

    sqlstate = "42P01"


class UndefinedColumn(PGWireError):
    """42703 -- undefined_column"""

    sqlstate = "42703"


class DuplicateTable(PGWireError):
    """42P07 -- duplicate_table"""

    sqlstate = "42P07"


class DuplicateColumn(PGWireError):
    """42701 -- duplicate_column"""

    sqlstate = "42701"


class UndefinedFunction(PGWireError):
    """42883 -- undefined_function"""

    sqlstate = "42883"


class InvalidSchemaName(PGWireError):
    """3F000 -- invalid_schema_name"""

    sqlstate = "3F000"


# -- Constraint violations (Class 23) -------------------------------------


class IntegrityConstraintViolation(PGWireError):
    """23000 -- integrity_constraint_violation"""

    sqlstate = "23000"


class UniqueViolation(PGWireError):
    """23505 -- unique_violation"""

    sqlstate = "23505"


class ForeignKeyViolation(PGWireError):
    """23503 -- foreign_key_violation"""

    sqlstate = "23503"


class NotNullViolation(PGWireError):
    """23502 -- not_null_violation"""

    sqlstate = "23502"


class CheckViolation(PGWireError):
    """23514 -- check_violation"""

    sqlstate = "23514"


# -- Feature / Data errors ------------------------------------------------


class FeatureNotSupported(PGWireError):
    """0A000 -- feature_not_supported"""

    sqlstate = "0A000"


class InvalidParameterValue(PGWireError):
    """22023 -- invalid_parameter_value"""

    sqlstate = "22023"


class DivisionByZero(PGWireError):
    """22012 -- division_by_zero"""

    sqlstate = "22012"


class InvalidTextRepresentation(PGWireError):
    """22P02 -- invalid_text_representation"""

    sqlstate = "22P02"


# -- Connection / Protocol errors ------------------------------------------


class ProtocolViolation(PGWireError):
    """08P01 -- protocol_violation"""

    sqlstate = "08P01"


class InvalidAuthorizationSpecification(PGWireError):
    """28000 -- invalid_authorization_specification"""

    sqlstate = "28000"
    severity = "FATAL"


class InvalidPassword(PGWireError):
    """28P01 -- invalid_password"""

    sqlstate = "28P01"
    severity = "FATAL"


# -- Operational errors ----------------------------------------------------


class QueryCanceled(PGWireError):
    """57014 -- query_canceled"""

    sqlstate = "57014"


class AdminShutdown(PGWireError):
    """57P01 -- admin_shutdown"""

    sqlstate = "57P01"
    severity = "FATAL"


class InvalidTransactionState(PGWireError):
    """25000 -- invalid_transaction_state"""

    sqlstate = "25000"


class InFailedSQLTransaction(PGWireError):
    """25P02 -- in_failed_sql_transaction"""

    sqlstate = "25P02"


# ======================================================================
# Exception mapper
# ======================================================================

# Patterns matched against the string representation of engine exceptions.
# Order matters: first match wins.
_VALUEERROR_PATTERNS: list[tuple[str, type[PGWireError]]] = [
    ("already exists", DuplicateTable),
    ("does not exist", UndefinedTable),
    ("UNIQUE constraint violated", UniqueViolation),
    ("FOREIGN KEY constraint violated", ForeignKeyViolation),
    ("NOT NULL constraint violated", NotNullViolation),
    ("CHECK constraint violated", CheckViolation),
    ("Unsupported statement", FeatureNotSupported),
    ("Transactions require", InvalidTransactionState),
    ("division by zero", DivisionByZero),
    ("Unknown column", UndefinedColumn),
    ("Duplicate column", DuplicateColumn),
    ("Unknown function", UndefinedFunction),
]


def map_engine_exception(exc: Exception) -> PGWireError:
    """Convert a USQLEngine / pglast exception to a :class:`PGWireError`."""
    msg = str(exc)

    # pglast parse errors
    exc_type_name = type(exc).__name__
    if exc_type_name == "ParseError" or exc_type_name == "PSqlParseError":
        return SQLSyntaxError(msg)

    if isinstance(exc, ValueError):
        for pattern, error_cls in _VALUEERROR_PATTERNS:
            if pattern in msg:
                return error_cls(msg)
        return PGWireError(msg, hint="Check your SQL syntax or table definitions.")

    if isinstance(exc, TypeError):
        return InvalidTextRepresentation(msg)

    if isinstance(exc, ZeroDivisionError):
        return DivisionByZero(msg)

    return PGWireError(msg)
