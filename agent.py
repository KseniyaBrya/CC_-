#!/usr/bin/env python3
"""
SQL Analytics Agent — CLI entry point.

Pipeline:
  1. Read user question (from --query flag OR interactive loop)
  2. Send question to Claude → receive generated SQL + explanation
  3. Show SQL to user
  4. Execute SQL against PostgreSQL (read-only)
  5. Format and display results

Usage:
  python agent.py                          # interactive REPL
  python agent.py -q "Топ-5 регионов..."  # single-shot mode
  python agent.py --help
"""

from __future__ import annotations

import argparse
import os
import sys
import textwrap

# Load .env file if present (optional dependency — silently ignored if not installed)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from db import execute_query, test_connection
from formatter import format_result
from sql_generator import generate_sql


# ---------------------------------------------------------------------------
# ANSI colours (disabled if terminal doesn't support them)
# ---------------------------------------------------------------------------

def _supports_color() -> bool:
    return hasattr(sys.stdout, "isatty") and sys.stdout.isatty()


if _supports_color():
    RESET  = "\033[0m"
    BOLD   = "\033[1m"
    CYAN   = "\033[36m"
    GREEN  = "\033[32m"
    YELLOW = "\033[33m"
    RED    = "\033[31m"
    DIM    = "\033[2m"
else:
    RESET = BOLD = CYAN = GREEN = YELLOW = RED = DIM = ""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _banner() -> str:
    return textwrap.dedent(f"""
    {BOLD}{CYAN}╔══════════════════════════════════════════╗
    ║   SQL Analytics Agent  v2.0  [Grok]     ║
    ║  Спросите что-нибудь на русском языке   ║
    ║  Введите  'exit' или 'quit' для выхода  ║
    ╚══════════════════════════════════════════╝{RESET}
    """)


def _print_sql(sql: str) -> None:
    print(f"\n{BOLD}Сгенерированный SQL:{RESET}")
    print(f"{DIM}{'-' * 60}{RESET}")
    # Indent for readability
    for line in sql.splitlines():
        print(f"  {CYAN}{line}{RESET}")
    print(f"{DIM}{'-' * 60}{RESET}")


def _print_explanation(explanation: str) -> None:
    print(f"\n{DIM}Интерпретация:{RESET} {explanation}")


def _print_error(msg: str) -> None:
    print(f"\n{RED}{BOLD}[Ошибка]{RESET} {msg}\n")


def _print_warning(msg: str) -> None:
    print(f"\n{YELLOW}[!]{RESET} {msg}\n")


# ---------------------------------------------------------------------------
# Core pipeline
# ---------------------------------------------------------------------------

def run_query(user_query: str) -> None:
    """
    Full pipeline for a single user question:
      generate SQL → display SQL → execute → display results.
    """
    print(f"\n{DIM}Генерирую SQL…{RESET}", flush=True)

    result = generate_sql(user_query)

    _print_explanation(result.explanation)

    if result.error:
        _print_error(f"Ошибка API: {result.error}")
        return

    if not result.is_safe or result.sql is None:
        _print_warning(
            "Агент не смог построить безопасный SQL-запрос для этого вопроса.\n"
            f"  Причина: {result.explanation}"
        )
        return

    _print_sql(result.sql)

    print(f"\n{DIM}Выполняю запрос…{RESET}", flush=True)
    db_result = execute_query(result.sql)

    output = format_result(db_result, user_query)
    print(f"\n{BOLD}{GREEN}Результат:{RESET}{output}")


# ---------------------------------------------------------------------------
# CLI modes
# ---------------------------------------------------------------------------

def interactive_mode() -> None:
    """REPL: keep asking for questions until the user exits."""
    print(_banner())

    # Test DB connection once at startup
    ok, msg = test_connection()
    if not ok:
        _print_error(
            f"Не удалось подключиться к PostgreSQL.\n  {msg}\n"
            "  Проверьте переменные окружения DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD."
        )
        sys.exit(1)
    print(f"{GREEN}✓ PostgreSQL подключён{RESET}\n")

    while True:
        try:
            user_input = input(f"{BOLD}Вопрос:{RESET} ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nВыход.")
            break

        if not user_input:
            continue

        if user_input.lower() in {"exit", "quit", "выход", "q"}:
            print("До свидания!")
            break

        run_query(user_input)
        print()  # blank line between queries


def single_shot_mode(query: str) -> None:
    """Execute one query and exit."""
    ok, msg = test_connection()
    if not ok:
        _print_error(
            f"Не удалось подключиться к PostgreSQL.\n  {msg}\n"
            "  Проверьте переменные окружения DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD."
        )
        sys.exit(1)

    run_query(query)


# ---------------------------------------------------------------------------
# Argument parsing & entry point
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="agent",
        description="SQL Analytics Agent — задайте вопрос на естественном языке, получите результат из PostgreSQL.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""
        Примеры:
          python agent.py
          python agent.py -q "Топ-5 регионов по выручке за 2025 год"
          python agent.py -q "Сколько заказов было за январь 2025?"
          python agent.py -q "Средний чек по сегментам клиентов"

        Переменные окружения для подключения к БД:
          DB_HOST      (default: localhost)
          DB_PORT      (default: 5432)
          DB_NAME      (default: postgres)
          DB_USER      (default: текущий пользователь ОС)
          DB_PASSWORD  (default: нет)

        API-ключ xAI Grok:
          XAI_API_KEY  (обязательно, получить на https://console.x.ai)
        """),
    )
    parser.add_argument(
        "-q", "--query",
        metavar="QUESTION",
        help="Вопрос на естественном языке (если не указан — запускается интерактивный режим)",
    )
    parser.add_argument(
        "--model",
        default="grok-3-mini",
        help="Модель Grok для генерации SQL (default: grok-3-mini)",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    # Validate API key early
    if not os.getenv("XAI_API_KEY"):
        _print_error(
            "Переменная окружения XAI_API_KEY не задана.\n"
            "  Получите ключ на https://console.x.ai\n"
            "  Установите: export XAI_API_KEY=xai-..."
        )
        sys.exit(1)

    if args.query:
        single_shot_mode(args.query)
    else:
        interactive_mode()


if __name__ == "__main__":
    main()
