"""
Formatter module.

Converts raw QueryResult objects into human-readable text for CLI output.
No external libraries required — formatting is done with plain Python.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from db import QueryResult


# ---------------------------------------------------------------------------
# Value helpers
# ---------------------------------------------------------------------------

def _fmt_value(val: Any) -> str:
    """Format a single cell value as a string."""
    if val is None:
        return "—"
    if isinstance(val, Decimal):
        # Drop trailing zeros; add thousands separator for large numbers
        f = float(val)
        if f == int(f) and abs(f) >= 1000:
            return f"{int(f):,}"
        return f"{f:,.2f}".rstrip("0").rstrip(".")
    if isinstance(val, float):
        return f"{val:,.2f}".rstrip("0").rstrip(".")
    if isinstance(val, int):
        return f"{val:,}"
    return str(val)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def format_result(result: QueryResult, user_query: str = "") -> str:
    """
    Render a QueryResult as a multi-line string suitable for console output.

    Layout:
      - If 0 rows: short "no results" message.
      - If 1 column / 1 row: single-value answer.
      - If 2 columns and the first column looks like a rank/name and the
        second is a numeric measure, render as a numbered list.
      - Otherwise: render as an ASCII table.
    """
    if not result.success:
        return f"\n[!] Ошибка при выполнении запроса:\n    {result.error}\n"

    if result.row_count == 0:
        return "\nРезультат: данные не найдены (0 строк).\n"

    rows = result.rows
    cols = result.columns

    # ------------------------------------------------------------------
    # Single scalar value
    # ------------------------------------------------------------------
    if len(cols) == 1 and len(rows) == 1:
        label = cols[0].replace("_", " ").capitalize()
        value = _fmt_value(rows[0][0])
        return f"\n{label}: {value}\n"

    # ------------------------------------------------------------------
    # Two-column list (e.g. "region | total_amount") → numbered list
    # ------------------------------------------------------------------
    if len(cols) == 2 and len(rows) > 1:
        name_col, val_col = cols[0], cols[1]
        # Check if val column looks numeric
        sample = rows[0][1]
        if isinstance(sample, (int, float, Decimal)):
            title = _make_title(user_query, name_col, val_col)
            lines = [f"\n{title}"]
            for i, row in enumerate(rows, 1):
                name = _fmt_value(row[0])
                value = _fmt_value(row[1])
                lines.append(f"  {i:>2}. {name} — {value}")
            lines.append(f"\nВсего строк: {result.row_count}")
            return "\n".join(lines) + "\n"

    # ------------------------------------------------------------------
    # Generic ASCII table
    # ------------------------------------------------------------------
    return _ascii_table(cols, rows)


def _make_title(user_query: str, name_col: str, val_col: str) -> str:
    """Build a readable title from the user query or column names."""
    if user_query:
        # Use the user's own words, trimmed to 80 chars
        q = user_query.strip().rstrip("?").strip()
        return q[:80] + ("…" if len(q) > 80 else "") + ":"
    name = name_col.replace("_", " ")
    val = val_col.replace("_", " ")
    return f"{name} / {val}:"


def _ascii_table(cols: list[str], rows: list[tuple]) -> str:
    """Render data as a fixed-width ASCII table."""
    # Compute column widths
    widths = [len(c) for c in cols]
    str_rows: list[list[str]] = []
    for row in rows:
        str_row = [_fmt_value(v) for v in row]
        str_rows.append(str_row)
        for i, cell in enumerate(str_row):
            widths[i] = max(widths[i], len(cell))

    sep = "+" + "+".join("-" * (w + 2) for w in widths) + "+"
    header = "|" + "|".join(f" {c.replace('_', ' ').capitalize():<{w}} " for c, w in zip(cols, widths)) + "|"

    lines = ["\n", sep, header, sep]
    for str_row in str_rows:
        line = "|" + "|".join(f" {cell:<{w}} " for cell, w in zip(str_row, widths)) + "|"
        lines.append(line)
    lines.append(sep)
    lines.append(f"\nВсего строк: {len(rows)}\n")

    return "\n".join(lines)
