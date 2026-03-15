#!/usr/bin/env python3
"""
Database setup script.

Creates the tables and populates them with realistic mock data
so you can try the agent immediately without a real database.

Usage:
  python setup_db.py            # create tables + load mock data
  python setup_db.py --drop     # drop tables first, then recreate
"""

from __future__ import annotations

import argparse
import random
import sys
from datetime import date, timedelta

import psycopg2

from db import get_connection
from schema import SCHEMA_SQL


# ---------------------------------------------------------------------------
# Mock data parameters
# ---------------------------------------------------------------------------

REGIONS = [
    "Москва", "Санкт-Петербург", "Татарстан",
    "Краснодарский край", "Свердловская область",
    "Новосибирская область", "Ростовская область",
    "Нижегородская область",
]

CITY_BY_REGION: dict[str, list[str]] = {
    "Москва":                  ["Москва"],
    "Санкт-Петербург":         ["Санкт-Петербург"],
    "Татарстан":               ["Казань", "Набережные Челны", "Нижнекамск"],
    "Краснодарский край":      ["Краснодар", "Сочи", "Новороссийск"],
    "Свердловская область":    ["Екатеринбург", "Нижний Тагил"],
    "Новосибирская область":   ["Новосибирск", "Бердск"],
    "Ростовская область":      ["Ростов-на-Дону", "Таганрог"],
    "Нижегородская область":   ["Нижний Новгород", "Дзержинск"],
}

SEGMENTS   = ["Premium", "Standard", "Economy"]
GENDERS    = ["Male", "Female"]
STATUSES   = ["completed", "pending", "cancelled"]
STATUS_W   = [0.70, 0.20, 0.10]   # probability weights

N_CUSTOMERS = 500
N_ORDERS    = 5000

# Orders spread over 2024-01-01 … today
DATE_START  = date(2024, 1, 1)
DATE_END    = date.today()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _random_date(start: date, end: date) -> date:
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))


def _random_amount(segment: str) -> float:
    """Generate a plausible order amount based on customer segment."""
    ranges = {
        "Premium":  (5_000,  80_000),
        "Standard": (1_000,  15_000),
        "Economy":  (200,     3_000),
    }
    lo, hi = ranges.get(segment, (500, 10_000))
    return round(random.uniform(lo, hi), 2)


# ---------------------------------------------------------------------------
# Setup logic
# ---------------------------------------------------------------------------

def drop_tables(conn: psycopg2.extensions.connection) -> None:
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS orders CASCADE")
        cur.execute("DROP TABLE IF EXISTS customers CASCADE")
    conn.commit()
    print("Tables dropped.")


def create_tables(conn: psycopg2.extensions.connection) -> None:
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
    conn.commit()
    print("Tables created.")


def load_customers(conn: psycopg2.extensions.connection) -> list[dict]:
    """Insert N_CUSTOMERS rows, return list of dicts for FK use."""
    customers = []
    for i in range(1, N_CUSTOMERS + 1):
        segment = random.choice(SEGMENTS)
        customers.append({
            "customer_id": i,
            "gender":      random.choice(GENDERS),
            "age":         random.randint(18, 70),
            "segment":     segment,
        })

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            "INSERT INTO customers (customer_id, gender, age, segment) VALUES %s",
            [(c["customer_id"], c["gender"], c["age"], c["segment"]) for c in customers],
        )
        # Sync the serial sequence to avoid PK conflicts
        cur.execute("SELECT setval('customers_customer_id_seq', %s)", (N_CUSTOMERS,))
    conn.commit()
    print(f"Inserted {N_CUSTOMERS} customers.")
    return customers


def load_orders(conn: psycopg2.extensions.connection, customers: list[dict]) -> None:
    """Insert N_ORDERS rows."""
    seg_map = {c["customer_id"]: c["segment"] for c in customers}
    orders = []
    for i in range(1, N_ORDERS + 1):
        cust = random.choice(customers)
        region = random.choice(REGIONS)
        city   = random.choice(CITY_BY_REGION[region])
        status = random.choices(STATUSES, weights=STATUS_W)[0]
        orders.append((
            i,
            _random_date(DATE_START, DATE_END),
            cust["customer_id"],
            region,
            city,
            _random_amount(seg_map[cust["customer_id"]]),
            status,
        ))

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """INSERT INTO orders
               (order_id, order_date, customer_id, region, city, amount, status)
               VALUES %s""",
            orders,
        )
        cur.execute("SELECT setval('orders_order_id_seq', %s)", (N_ORDERS,))
    conn.commit()
    print(f"Inserted {N_ORDERS} orders.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Set up the SQL agent demo database.")
    parser.add_argument("--drop", action="store_true", help="Drop existing tables before creating.")
    args = parser.parse_args()

    try:
        conn = get_connection()
    except psycopg2.OperationalError as exc:
        print(f"[Error] Cannot connect to PostgreSQL: {exc}", file=sys.stderr)
        print("  Set DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD as needed.", file=sys.stderr)
        sys.exit(1)

    try:
        import psycopg2.extras  # ensure available
        if args.drop:
            drop_tables(conn)
        create_tables(conn)
        customers = load_customers(conn)
        load_orders(conn, customers)
        print("\nDatabase ready. Run: python agent.py")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
