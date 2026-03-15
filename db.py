"""
Database module.

Handles connecting to PostgreSQL and executing read-only SELECT queries.
All queries run inside a read-only transaction to prevent accidental writes
even if a malformed query somehow slips through the SQL generator's checks.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any

import psycopg2
import psycopg2.extras


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------

@dataclass
class QueryResult:
    """Result of a database query."""
    columns: list[str]
    rows: list[tuple[Any, ...]]
    row_count: int
    error: str | None = None

    @property
    def success(self) -> bool:
        return self.error is None


_EMPTY = QueryResult(columns=[], rows=[], row_count=0)


# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------

def _get_dsn() -> str:
    """
    Build a PostgreSQL DSN from environment variables.

    Supported variables (all optional, fall back to psycopg2 defaults):
      DB_HOST     (default: localhost)
      DB_PORT     (default: 5432)
      DB_NAME     (default: postgres)
      DB_USER     (default: current OS user)
      DB_PASSWORD (default: none)
    """
    parts: list[str] = []
    if host := os.getenv("DB_HOST", "localhost"):
        parts.append(f"host={host}")
    if port := os.getenv("DB_PORT", "5432"):
        parts.append(f"port={port}")
    if dbname := os.getenv("DB_NAME", "postgres"):
        parts.append(f"dbname={dbname}")
    if user := os.getenv("DB_USER", ""):
        parts.append(f"user={user}")
    if password := os.getenv("DB_PASSWORD", ""):
        parts.append(f"password={password}")
    return " ".join(parts)


def get_connection() -> psycopg2.extensions.connection:
    """
    Open and return a new psycopg2 connection.
    Raises psycopg2.OperationalError on failure.
    """
    dsn = _get_dsn()
    conn = psycopg2.connect(dsn)
    return conn


# ---------------------------------------------------------------------------
# Query execution
# ---------------------------------------------------------------------------

def execute_query(sql: str) -> QueryResult:
    """
    Execute a SELECT query and return a QueryResult.

    The query runs inside a read-only transaction (SET TRANSACTION READ ONLY)
    so that even if a non-SELECT statement somehow reaches here, PostgreSQL
    will reject it at the server level.

    Args:
        sql: The SELECT statement to execute.

    Returns:
        QueryResult with columns, rows, and row_count on success,
        or with error set on failure.
    """
    conn = None
    try:
        conn = get_connection()

        with conn:                         # auto-rollback on exception
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                # Enforce read-only at the PostgreSQL level
                cur.execute("SET TRANSACTION READ ONLY")
                cur.execute(sql)

                if cur.description is None:
                    # Shouldn't happen for a SELECT, but guard anyway
                    return QueryResult(columns=[], rows=[], row_count=0)

                columns = [desc.name for desc in cur.description]
                raw_rows = cur.fetchall()
                rows = [tuple(row[col] for col in columns) for row in raw_rows]

                return QueryResult(
                    columns=columns,
                    rows=rows,
                    row_count=len(rows),
                )

    except psycopg2.OperationalError as exc:
        return QueryResult(
            columns=[], rows=[], row_count=0,
            error=f"Connection error: {exc}",
        )
    except psycopg2.ProgrammingError as exc:
        return QueryResult(
            columns=[], rows=[], row_count=0,
            error=f"SQL error: {exc}",
        )
    except psycopg2.Error as exc:
        return QueryResult(
            columns=[], rows=[], row_count=0,
            error=f"Database error: {exc}",
        )
    finally:
        if conn is not None:
            conn.close()


def test_connection() -> tuple[bool, str]:
    """
    Try to open a connection and run a trivial query.
    Returns (success, message).
    """
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        conn.close()
        return True, "Connection successful."
    except psycopg2.OperationalError as exc:
        return False, f"Cannot connect to PostgreSQL: {exc}"
