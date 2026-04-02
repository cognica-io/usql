#
# usqldb -- PostgreSQL 17-compatible catalog layer for UQA
#
# Copyright (c) 2023-2026 Cognica, Inc.
#

"""Extended SQL compiler with PostgreSQL 17-compatible system catalogs.

Subclasses UQA's SQLCompiler to override the information_schema and
pg_catalog virtual table builders with comprehensive PG17-compatible
implementations.

The original SQLCompiler supports only:
    information_schema: tables, columns
    pg_catalog: pg_tables, pg_views, pg_indexes, pg_type

This extended compiler supports 23 information_schema views and 35+
pg_catalog tables, with consistent OID cross-references across all
of them.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pglast.ast import RangeVar
from uqa.sql.compiler import SQLCompiler, SQLResult

from usqldb.pg_compat.information_schema import InformationSchemaProvider
from usqldb.pg_compat.oid import OIDAllocator
from usqldb.pg_compat.pg_catalog import PGCatalogProvider

if TYPE_CHECKING:
    from uqa.sql.table import Table


def _normalize_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Normalize row values for Arrow-compatible storage.

    UQA's ``_result_to_table`` stores raw dicts in the document store.
    The physical execution engine later builds Arrow arrays from these
    values.  Arrow is strict about types: a column inferred as ``text``
    cannot contain Python ``bool`` objects.

    This normalizer converts:
        - bool -> int (0/1), matching PostgreSQL's boolean-to-integer casting
        - float('nan') / float('inf') -> None
    """
    if not rows:
        return rows
    normalized: list[dict[str, Any]] = []
    for row in rows:
        new_row: dict[str, Any] = {}
        for key, value in row.items():
            if isinstance(value, bool):
                new_row[key] = 1 if value else 0
            elif isinstance(value, float) and (
                value != value or value == float("inf") or value == float("-inf")
            ):
                new_row[key] = None
            else:
                new_row[key] = value
        normalized.append(new_row)
    return normalized


class USQLCompiler(SQLCompiler):
    """SQL compiler with full PostgreSQL 17 catalog compatibility.

    On first access to any information_schema or pg_catalog view during
    a query, an OIDAllocator is created to ensure consistent OIDs across
    all virtual tables.  The allocator is shared within a single
    ``execute()`` call and discarded afterward.
    """

    def __init__(self, engine: Any) -> None:
        super().__init__(engine)
        self._oid_allocator: OIDAllocator | None = None

    def _get_oids(self) -> OIDAllocator:
        """Lazily create the OID allocator on first catalog access."""
        if self._oid_allocator is None:
            self._oid_allocator = OIDAllocator(self._engine)
        return self._oid_allocator

    # ------------------------------------------------------------------
    # Override: unqualified catalog name resolution
    # ------------------------------------------------------------------

    # pg_catalog tables that can be referenced without schema prefix,
    # matching PostgreSQL's implicit pg_catalog search_path behavior.
    _PG_CATALOG_NAMES: frozenset[str] = frozenset(PGCatalogProvider.supported_tables())

    # information_schema views that can be referenced without prefix.
    _INFO_SCHEMA_NAMES: frozenset[str] = frozenset(
        InformationSchemaProvider.supported_views()
    )

    def _resolve_from_single(  # type: ignore[override]
        self, node: Any
    ) -> tuple[Table | None, Any, str | None]:
        """Resolve a single FROM item with implicit catalog lookup.

        When a table name has no schema qualifier and does not exist
        as a user table, view, foreign table, or CTE, try resolving it
        against pg_catalog (for ``pg_`` prefixed names) and then
        information_schema before raising an error.  This matches
        PostgreSQL's behavior where ``pg_catalog`` is always on the
        search path.
        """
        if isinstance(node, RangeVar) and node.schemaname is None:
            name = node.relname
            # Only intercept if the name is not a user-defined object.
            if (
                name not in self._engine._tables
                and name not in self._engine._views
                and name not in self._engine._foreign_tables
                and name not in self._inlined_ctes
            ):
                if name in self._PG_CATALOG_NAMES:
                    alias = node.alias.aliasname if node.alias is not None else name
                    tbl, op = self._build_pg_catalog_table(name)
                    return tbl, op, alias
                if name in self._INFO_SCHEMA_NAMES:
                    alias = node.alias.aliasname if node.alias is not None else name
                    tbl, op = self._build_information_schema_table(name)
                    return tbl, op, alias

        return super()._resolve_from_single(node)

    @staticmethod
    def _walk_ast_for_tables(node: Any, refs: set[str], ast_base: type) -> None:
        """Walk the AST to find table references, excluding catalogs.

        Extends the base implementation to also exclude unqualified
        pg_catalog and information_schema names so they are not treated
        as missing user tables.
        """
        if node is None:
            return
        if isinstance(node, RangeVar):
            if node.schemaname in ("information_schema", "pg_catalog"):
                return
            # Skip unqualified names that match known catalog objects.
            if node.schemaname is None and (
                node.relname in USQLCompiler._PG_CATALOG_NAMES
                or node.relname in USQLCompiler._INFO_SCHEMA_NAMES
            ):
                return
            if node.relname is not None:
                refs.add(node.relname)
            return
        if isinstance(node, (list, tuple)):
            for item in node:
                USQLCompiler._walk_ast_for_tables(item, refs, ast_base)
            return
        if not isinstance(node, ast_base):
            return
        for attr in node.__slots__:
            child = getattr(node, attr, None)
            if child is not None:
                USQLCompiler._walk_ast_for_tables(child, refs, ast_base)

    # ------------------------------------------------------------------
    # Override: information_schema
    # ------------------------------------------------------------------

    def _build_information_schema_table(
        self,
        view_name: str,
    ) -> tuple[Table, None]:
        """Build a PostgreSQL 17-compatible information_schema view."""
        oids = self._get_oids()
        columns, rows = InformationSchemaProvider.build(view_name, self._engine, oids)
        rows = _normalize_rows(rows)
        result = SQLResult(columns, rows)
        internal_name = f"_info_schema_{view_name}"
        table = self._result_to_table(internal_name, result)
        if internal_name in self._engine._tables:
            self._shadowed_tables.setdefault(
                internal_name, self._engine._tables[internal_name]
            )
        self._engine._tables[internal_name] = table
        self._expanded_views.append(internal_name)
        return table, None

    # ------------------------------------------------------------------
    # Override: pg_catalog
    # ------------------------------------------------------------------

    def _build_pg_catalog_table(
        self,
        table_name: str,
    ) -> tuple[Table, None]:
        """Build a PostgreSQL 17-compatible pg_catalog table."""
        oids = self._get_oids()
        columns, rows = PGCatalogProvider.build(table_name, self._engine, oids)
        rows = _normalize_rows(rows)
        result = SQLResult(columns, rows)
        internal_name = f"_pg_{table_name}"
        table = self._result_to_table(internal_name, result)
        if internal_name in self._engine._tables:
            self._shadowed_tables.setdefault(
                internal_name, self._engine._tables[internal_name]
            )
        self._engine._tables[internal_name] = table
        self._expanded_views.append(internal_name)
        return table, None
