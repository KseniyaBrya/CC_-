"""
Database schema definition.
Shared between the SQL generator (for prompting) and the setup script.
"""

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS laptop_sales (
    id          SERIAL PRIMARY KEY,
    region      TEXT    NOT NULL,
    sale_month  DATE    NOT NULL,
    revenue     BIGINT  NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS uix_laptop_sales_region_month
    ON laptop_sales (region, sale_month);
"""

# Schema description passed to Grok as context for SQL generation
SCHEMA_DESCRIPTION = """
База данных: продажи ноутбуков по регионам России.

Таблица: laptop_sales
  - id          INTEGER  (первичный ключ, автоинкремент)
  - region      TEXT     — регион продаж. Возможные значения:
                           'Москва', 'Санкт-Петербург', 'Республика Татарстан',
                           'Свердловская область', 'Самарская область'
  - sale_month  DATE     — месяц продаж (всегда первый день месяца, например 2024-01-01)
  - revenue     BIGINT   — выручка от продаж ноутбуков за месяц в рублях

Данные охватывают период: январь 2023 — март 2025 (27 месяцев × 5 регионов = 135 строк).

Правила написания SQL:
  - Текущая дата в SQL: CURRENT_DATE
  - Для фильтрации по году:   EXTRACT(YEAR FROM sale_month) = <год>
  - Для фильтрации по месяцу: EXTRACT(MONTH FROM sale_month) = <номер месяца>
  - Для фильтрации диапазона: sale_month BETWEEN '2024-01-01' AND '2024-12-01'
  - Для «прошлого года»:      EXTRACT(YEAR FROM sale_month) = EXTRACT(YEAR FROM CURRENT_DATE) - 1
  - Для «текущего года»:      EXTRACT(YEAR FROM sale_month) = EXTRACT(YEAR FROM CURRENT_DATE)
  - Для форматирования месяца в вывод: TO_CHAR(sale_month, 'YYYY-MM')
  - «Выручка» / «продажи» / «revenue» = revenue (суммировать через SUM(revenue))
  - Группировка по месяцу: GROUP BY sale_month ORDER BY sale_month
  - Группировка по региону: GROUP BY region
"""
