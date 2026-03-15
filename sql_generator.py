"""
SQL Generator module.

Converts a natural-language user query into a read-only PostgreSQL SELECT statement
by calling the xAI Grok API (OpenAI-compatible).
Returns a structured result with the generated SQL, a short explanation,
and a flag indicating whether the query is safe to execute.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass

import openai

from schema import SCHEMA_DESCRIPTION


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------

@dataclass
class GenerationResult:
    """Result returned by generate_sql()."""
    sql: str | None          # None when the query cannot be answered
    explanation: str         # Short description of what the SQL does (or why it failed)
    is_safe: bool            # False when the query should NOT be executed
    error: str | None = None # Low-level error from the API, if any


# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = f"""You are an expert PostgreSQL analyst.
Your job is to convert a user's natural-language question into a safe, read-only SQL SELECT query.

{SCHEMA_DESCRIPTION}

Rules you MUST follow:
1. Only generate SELECT queries. Never use INSERT, UPDATE, DELETE, DROP, CREATE, TRUNCATE, ALTER, GRANT, REVOKE, EXECUTE, or any DDL/DML.
2. Never use PostgreSQL functions that can modify data or system state (e.g. pg_sleep, dblink, COPY TO/FROM, etc.).
3. If the user's question is ambiguous, make the most reasonable assumption and state it in the explanation.
4. If the question cannot be answered with the available schema, set sql to null and explain why.
5. Always alias aggregated columns for readability (e.g. SUM(revenue) AS total_revenue).
6. Add a LIMIT clause when the user asks for "top N" or a ranked list; default LIMIT is 20 when the user asks for a list without specifying a count.

Respond ONLY with a JSON object matching this schema (no markdown, no code fences):
{{
  "sql": "<SELECT statement or null>",
  "explanation": "<one-sentence description of what the query does, or why it cannot be answered>",
  "is_safe": <true or false>
}}

Set is_safe to false if:
- You cannot produce a valid SELECT query from this request.
- The user is clearly trying to perform a write/destructive operation.
- The question is completely unrelated to the data (e.g. "write me a poem").
"""


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

_FORBIDDEN = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|CREATE|TRUNCATE|ALTER|GRANT|REVOKE|EXECUTE|COPY)\b",
    re.IGNORECASE,
)


def _validate_select(sql: str) -> tuple[bool, str]:
    """Returns (is_valid, reason). Ensures query is a safe SELECT."""
    stripped = sql.strip()
    if not stripped.upper().startswith("SELECT"):
        return False, "Query does not start with SELECT."
    match = _FORBIDDEN.search(stripped)
    if match:
        return False, f"Query contains forbidden keyword: {match.group()}."
    return True, ""


# ---------------------------------------------------------------------------
# xAI client factory
# ---------------------------------------------------------------------------

def _make_client() -> openai.OpenAI:
    """Create an OpenAI-compatible client pointed at xAI's Grok API."""
    api_key = os.getenv("XAI_API_KEY")
    if not api_key:
        raise ValueError("XAI_API_KEY environment variable is not set.")
    return openai.OpenAI(
        api_key=api_key,
        base_url="https://api.x.ai/v1",
    )


# ---------------------------------------------------------------------------
# Main function
# ---------------------------------------------------------------------------

def generate_sql(user_query: str, *, model: str = "grok-3-mini") -> GenerationResult:
    """
    Send the user query to Grok and return a GenerationResult.

    Args:
        user_query: The natural-language question from the user.
        model:      Grok model ID to use (default: grok-3-mini).

    Returns:
        GenerationResult with sql, explanation, is_safe, and optional error.
    """
    try:
        client = _make_client()
    except ValueError as exc:
        return GenerationResult(
            sql=None,
            explanation=str(exc),
            is_safe=False,
            error="missing_api_key",
        )

    try:
        response = client.chat.completions.create(
            model=model,
            max_tokens=1024,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user",   "content": user_query},
            ],
        )
    except openai.AuthenticationError:
        return GenerationResult(
            sql=None,
            explanation="Ошибка аутентификации — проверьте XAI_API_KEY.",
            is_safe=False,
            error="AuthenticationError",
        )
    except openai.APIConnectionError as exc:
        return GenerationResult(
            sql=None,
            explanation="Не удалось подключиться к xAI API. Проверьте интернет-соединение.",
            is_safe=False,
            error=str(exc),
        )
    except openai.APIStatusError as exc:
        return GenerationResult(
            sql=None,
            explanation=f"Ошибка xAI API ({exc.status_code}): {exc.message}",
            is_safe=False,
            error=str(exc),
        )

    raw_text = (response.choices[0].message.content or "").strip()

    if not raw_text:
        return GenerationResult(
            sql=None,
            explanation="Grok вернул пустой ответ.",
            is_safe=False,
            error="empty_response",
        )

    # Strip markdown code fences if Grok wraps the JSON in ```
    raw_text = re.sub(r"^```(?:json)?\s*", "", raw_text, flags=re.IGNORECASE)
    raw_text = re.sub(r"\s*```$", "", raw_text).strip()

    # Parse JSON
    try:
        data = json.loads(raw_text)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", raw_text, re.DOTALL)
        if match:
            try:
                data = json.loads(match.group())
            except json.JSONDecodeError:
                return GenerationResult(
                    sql=None,
                    explanation="Не удалось разобрать ответ Grok как JSON.",
                    is_safe=False,
                    error=f"raw response: {raw_text[:200]}",
                )
        else:
            return GenerationResult(
                sql=None,
                explanation="Не удалось разобрать ответ Grok как JSON.",
                is_safe=False,
                error=f"raw response: {raw_text[:200]}",
            )

    sql         = data.get("sql") or None
    explanation = data.get("explanation", "Объяснение не предоставлено.")
    is_safe     = bool(data.get("is_safe", False))

    # Enforce our own safety check even if Grok says is_safe=True
    if sql:
        valid, reason = _validate_select(sql)
        if not valid:
            return GenerationResult(
                sql=None,
                explanation=f"SQL не прошёл проверку безопасности: {reason}",
                is_safe=False,
            )

    return GenerationResult(sql=sql, explanation=explanation, is_safe=is_safe)
