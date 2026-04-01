#
# usqldb -- PostgreSQL 17-compatible catalog layer for UQA
#
# Copyright (c) 2023-2026 Cognica, Inc.
#

"""Unit tests for error hierarchy and exception mapping."""

from __future__ import annotations

from usqldb.net.pgwire._constants import (
    FIELD_MESSAGE,
    FIELD_SEVERITY,
    FIELD_SQLSTATE,
)
from usqldb.net.pgwire._errors import (
    DuplicateTable,
    FeatureNotSupported,
    InvalidTransactionState,
    PGWireError,
    SQLSyntaxError,
    UndefinedTable,
    UniqueViolation,
    map_engine_exception,
)


class TestPGWireError:
    def test_base_error(self):
        err = PGWireError("something broke")
        assert str(err) == "something broke"
        assert err.sqlstate == "XX000"
        assert err.severity == "ERROR"

    def test_error_with_details(self):
        err = PGWireError(
            "bad stuff",
            detail="this is the detail",
            hint="try this instead",
            position=42,
        )
        fields = err.to_fields()
        assert fields[FIELD_SEVERITY] == "ERROR"
        assert fields[FIELD_SQLSTATE] == "XX000"
        assert fields[FIELD_MESSAGE] == "bad stuff"
        assert fields[ord("D")] == "this is the detail"
        assert fields[ord("H")] == "try this instead"
        assert fields[ord("P")] == "42"

    def test_syntax_error(self):
        err = SQLSyntaxError("unexpected token")
        assert err.sqlstate == "42601"

    def test_undefined_table(self):
        err = UndefinedTable("table 'foo' does not exist")
        assert err.sqlstate == "42P01"

    def test_unique_violation(self):
        err = UniqueViolation("duplicate key")
        assert err.sqlstate == "23505"


class TestMapEngineException:
    def test_table_not_exists(self):
        exc = ValueError("Table 'users' does not exist")
        result = map_engine_exception(exc)
        assert isinstance(result, UndefinedTable)
        assert result.sqlstate == "42P01"

    def test_table_already_exists(self):
        exc = ValueError("Table 'users' already exists")
        result = map_engine_exception(exc)
        assert isinstance(result, DuplicateTable)
        assert result.sqlstate == "42P07"

    def test_unique_violation(self):
        exc = ValueError("UNIQUE constraint violated on column 'id'")
        result = map_engine_exception(exc)
        assert isinstance(result, UniqueViolation)

    def test_unsupported_statement(self):
        exc = ValueError("Unsupported statement: CopyStmt")
        result = map_engine_exception(exc)
        assert isinstance(result, FeatureNotSupported)
        assert result.sqlstate == "0A000"

    def test_transaction_error(self):
        exc = ValueError("Transactions require a persistent engine (db_path)")
        result = map_engine_exception(exc)
        assert isinstance(result, InvalidTransactionState)

    def test_pglast_parse_error(self):
        # Simulate a pglast ParseError (we check class name, not type).
        class ParseError(Exception):
            pass

        exc = ParseError("syntax error at position 5")
        result = map_engine_exception(exc)
        assert isinstance(result, SQLSyntaxError)

    def test_generic_value_error(self):
        exc = ValueError("something unexpected")
        result = map_engine_exception(exc)
        assert isinstance(result, PGWireError)

    def test_zero_division(self):
        from usqldb.net.pgwire._errors import DivisionByZero

        exc = ZeroDivisionError("division by zero")
        result = map_engine_exception(exc)
        assert isinstance(result, DivisionByZero)
