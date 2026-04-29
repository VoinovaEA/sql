# Лабораторная работа. Основы ClickHouse: установка, типы данных, движки таблиц.
### Студент: Войнова Екатерина Андреевна
---
## Вариант 002
## Параметры варианта 002

| Параметр | Значение для варианта 2 |
|:---------|------------------------:|
| **sale_id от** | 2001 |
| **customer_id диапазон** | 200 – 299 |
| **product_id диапазон** | 20 – 39 |
| **max quantity** | 7 |
| **unit_price от** | 12.00 |
| **Товаров (Зад.3)** | 7 |
| **Дней (Зад.4)** | 5 |
| **Кампаний (Зад.4)** | 4 |

## Задание 1. Создание базы данных и таблицы продаж
```sql
-- 1.3. Создаем таблицу продаж
CREATE TABLE sales_var002 (
    sale_id        UInt64,
    sale_timestamp DateTime64(3),
    product_id     UInt32,
    category       LowCardinality(String),
    customer_id    UInt64,
    region         LowCardinality(String),
    quantity       UInt16,
    unit_price     Decimal64(2),
    discount_pct   Float32,
    is_online      UInt8,
    ip_address     IPv4
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(sale_timestamp)
ORDER BY (sale_timestamp, customer_id, product_id);
```
 ## 1.2 Количество строк.
```sql
SELECT COUNT(*) AS total_rows FROM sales_var002;
```
## Результат выполнения. 

<img width="224" height="71" alt="image" src="https://github.com/user-attachments/assets/b3c157de-2940-4707-9eec-71916ab29d95" />

 ## 1.3  Распределение по месяцам (должно быть 3 разных месяца).
``` sql
SELECT 
    toYYYYMM(sale_timestamp) AS month,
    COUNT(*) AS rows_count,
    MIN(sale_timestamp) AS first_date,
    MAX(sale_timestamp) AS last_date
FROM sales_var002
GROUP BY month
ORDER BY month;
```
## Результат выполнения.

<img width="518" height="110" alt="image" src="https://github.com/user-attachments/assets/27a39518-7fa2-46b2-95d3-59a53daa85fb" />

## Выводы для 1 задания. Таблица sales_var002 создана. Вставлено 100 строк. Данные распределены по 3 месяцам: май, июнь, июль 2024

# Задание 2. Аналитические запросы.
 ## 2.1. Общая выручка по категориям.
```sql
 SELECT 
    category,
    SUM(quantity * unit_price * (1 - discount_pct)) AS total_revenue
FROM sales_var002
GROUP BY category
ORDER BY total_revenue DESC;
```
## Результат выполнения.

<img width="272" height="151" alt="image" src="https://github.com/user-attachments/assets/fc4a9442-1b7c-45ba-9451-4a60bf30bb21" />

## 2.2. Топ-3 клиента по количеству покупок.
```sql
SELECT 
    customer_id,
    COUNT(*) AS purchase_count,
    SUM(quantity) AS total_quantity
FROM sales_var002
GROUP BY customer_id
ORDER BY purchase_count DESC
LIMIT 3;
```

## Результат выполнения.

<img width="385" height="102" alt="image" src="https://github.com/user-attachments/assets/95b499ac-761e-4114-a5ec-add15f6d4359" />

## 2.3. Средний чек по месяцам.
```sql
SELECT 
    toYYYYMM(sale_timestamp) AS month,
    AVG(quantity * unit_price) AS avg_check
FROM sales_var002
GROUP BY month
ORDER BY month;
```
## Результат выполнения.

<img width="265" height="101" alt="image" src="https://github.com/user-attachments/assets/02f9e4c8-fee1-49d8-a550-807d861bb59e" />

## 2.4. Фильтрация по партиции (выводим данные только за Июнь 2024).
```sql
SELECT * 
FROM sales_var002 
WHERE sale_timestamp >= '2024-06-01' 
  AND sale_timestamp < '2024-07-01'
LIMIT 10;
```
## Результат выполнения.

<img width="1312" height="255" alt="image" src="https://github.com/user-attachments/assets/5d16368a-b44c-4e9d-83fd-6ff6e478cf26" />

## Выводы для 2 задания. ## Вывод по Заданию 2

В ходе выполнения задания 2 были реализованы 4 аналитических запроса к таблице `sales_var002`:

1. **Выручка по категориям** — позволяет определить самые прибыльные категории товаров с учётом скидок.

2. **Топ-3 клиента** — выявляет наиболее активных покупателей по частоте заказов.

3. **Средний чек по месяцам** — показывает динамику среднего чека за каждый месяц продаж.

4. **Фильтрация по партиции** — демонстрирует оптимизацию запросов благодаря партиционированию: ClickHouse обращается только к нужной партиции (июнь 2024), не сканируя всю таблицу.

**Итог:** Все запросы выполнены корректно, структура таблицы с партиционированием и сортировкой позволяет эффективно выполнять аналитические операции.


# Задание 3. ReplacingMergeTree — справочник товаров.
## 3.1. Создаем таблицу товаров
``` sql
CREATE TABLE products_var002 (
    product_id    UInt32,
    product_name  String,
    category      LowCardinality(String),
    supplier      String,
    base_price    Decimal64(2),
    weight_kg     Float32,
    is_available  UInt8,
    updated_at    DateTime,
    version       UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (product_id);
```
## 3.2 Вставляем обновления для 3 товаров с version = 2.
```sql
-- Обновляем товар 20: Laptop Pro
INSERT INTO products_var002 VALUES
(20, 'Laptop Pro X', 'Electronics', 'TechCorp', 1099.99, 2.5, 1, now(), 2);

-- Обновляем товар 23: Smartphone
INSERT INTO products_var002 VALUES
(23, 'Smartphone Plus', 'Electronics', 'TechCorp', 799.99, 0.3, 1, now(), 2);

-- Обновляем товар 25: Data Science Book
INSERT INTO products_var002 VALUES
(25, 'Data Science Book (2nd Ed)', 'Books', 'PubHouse', 89.99, 0.8, 0, now(), 2);
```
## 3.4. Выполняем SELECT - видны обе версии.

```sql
SELECT * FROM products_var002 
WHERE product_id IN (20, 23, 25)
ORDER BY product_id, version;
```

## Результат выполнения.

<img width="1146" height="251" alt="image" src="https://github.com/user-attachments/assets/cae8b57f-7587-445a-8b7b-1ba467389acb" />


## 3.5 Выполняем OPTIMIZE TABLE
``` sql
OPTIMIZE TABLE products_var002 FINAL;
```
## 3.6. Повторяем SELECT - осталась только последняя версия.
``` sql
SELECT * FROM products_var002 
WHERE product_id IN (20, 23, 25)
ORDER BY product_id;
```

## Результат выполнения.

<img width="1141" height="101" alt="image" src="https://github.com/user-attachments/assets/70ba8b39-56f5-451f-b5ce-a475b632f503" />

## Выводы для 3 задания. Ключевые наблюдения

| Наблюдение | Описание |
|------------|----------|
| До OPTIMIZE | Дубликаты видны, так как данные хранятся в разных частях партиции |
| После OPTIMIZE | Дубликаты удалены, осталась запись с максимальной версией |
| Механизм | `ReplacingMergeTree(version)` заменяет старые версии на новые |
| Важно | Без `OPTIMIZE` или `FINAL` дубликаты могут оставаться в таблице |

### Итог

`ReplacingMergeTree` эффективно решает задачу хранения актуальных версий справочных данных. Для гарантированного получения последней версии необходимо использовать `OPTIMIZE` или ключевое слово `FINAL` в запросе.

# Задание 4. SummingMergeTree — агрегация метрик.
## 4.1 Выполняем SELECT до OPTIMIZE.
```sql
SELECT * FROM daily_metrics_var002 
WHERE (campaign_id = 21 AND channel = 'Email' AND metric_date = '2024-07-01')
   OR (campaign_id = 22 AND channel = 'Social' AND metric_date = '2024-07-02')
   OR (campaign_id = 23 AND channel = 'Email' AND metric_date = '2024-07-03')
   OR (campaign_id = 24 AND channel = 'Social' AND metric_date = '2024-07-04')
ORDER BY metric_date, campaign_id, channel;
```

## Результат выполнения.

<img width="889" height="306" alt="image" src="https://github.com/user-attachments/assets/8653ea49-31b8-4de0-b316-82de2a103f3e" />

## 4.2 Выполняем OPTIMIZE TABLE.
```sql
OPTIMIZE TABLE daily_metrics_var002 FINAL;
```

### 4.3 Проверяем, что данные просуммировались.
``` sql
SELECT * FROM daily_metrics_var002 
WHERE (campaign_id = 21 AND channel = 'Email' AND metric_date = '2024-07-01')
   OR (campaign_id = 22 AND channel = 'Social' AND metric_date = '2024-07-02')
   OR (campaign_id = 23 AND channel = 'Email' AND metric_date = '2024-07-03')
   OR (campaign_id = 24 AND channel = 'Social' AND metric_date = '2024-07-04')
ORDER BY metric_date, campaign_id, channel;
```

## Результат выполнения.

<img width="897" height="139" alt="image" src="https://github.com/user-attachments/assets/275898f0-c7d4-4b43-b366-1dcd692c38df" />

## 4.4 Запрос: CTR (click-through rate) по каналам.
```sql
SELECT 
    channel,
    SUM(clicks) AS total_clicks,
    SUM(impressions) AS total_impressions,
    (SUM(clicks) / SUM(impressions)) * 100 AS ctr_percent
FROM daily_metrics_var002
GROUP BY channel
ORDER BY ctr_percent DESC;
```
## Результат выполнения.

<img width="552" height="85" alt="image" src="https://github.com/user-attachments/assets/5da1033b-5e21-4e6b-8033-3e2903c1f549" />

## Выводы для 4 задания. Ключевые наблюдения

| Наблюдение | Описание |
|------------|----------|
| Суммируемые столбцы | `impressions`, `clicks`, `conversions`, `spend_cents` |
| Несуммируемые столбцы | Берутся из первой попавшейся строки (для данного ключа) |
| Без OPTIMIZE | Данные физически не слиты → дубликаты видны в SELECT |
| Важно | `SummingMergeTree` не заменяет `GROUP BY` в аналитике |

### Итог

`SummingMergeTree` автоматически суммирует числовые метрики при слиянии партиций, что экономит место на диске и упрощает хранение агрегированных данных. Однако для точных аналитических запросов всё равно требуется `GROUP BY`

# Задание 5. Комплексный анализ и самопроверка.
## 5.2. JOIN между таблицами (топ-5 товаров по выручке).
``` sql
SELECT
    p.product_name,
    p.category,
    sum(s.quantity * s.unit_price * (1 - s.discount_pct)) AS revenue
FROM sales_var002 AS s
INNER JOIN (
    SELECT * FROM products_var002 FINAL
) AS p ON s.product_id = p.product_id
GROUP BY p.product_name, p.category
ORDER BY revenue DESC
LIMIT 5;
```
## Результаты выполнения.

<img width="445" height="152" alt="image" src="https://github.com/user-attachments/assets/309ae88e-06b5-4e70-b8e7-446945706ff7" />

## 5.3. Типы данных всех таблиц.
```sql
SELECT
    arrayJoin(tags) AS tag,
    count() AS items_count
FROM tags_var002
GROUP BY tag
ORDER BY items_count DESC;
```

## Результаты выполнения.

<img width="270" height="171" alt="image" src="https://github.com/user-attachments/assets/1e627c47-459f-4d8f-b79d-eacf062284a3" />

## 5.4. Контрольная сумма (итоговая проверка).
``` sql
SELECT
    'sales' AS tbl, 
    count() AS rows, 
    sum(quantity) AS check_sum 
FROM sales_var002
UNION ALL
SELECT
    'products', 
    count(), 
    sum(toUInt64(product_id)) 
FROM products_var002 FINAL
UNION ALL
SELECT
    'metrics', 
    count(), 
    sum(clicks) 
FROM daily_metrics_var002;
```
## Результаты выполнения.

<img width="393" height="111" alt="image" src="https://github.com/user-attachments/assets/945f0676-a58f-4499-afa9-81810c807abf" />

## Итог по Заданию 5

| Проверка | Статус |
|----------|--------|
| Партиционирование | ✅ Работает |
| JOIN между таблицами | ✅ Корректен |
| Типы данных | ✅ Соответствуют заданию |
| Работа с массивами | ✅ `arrayJoin` функционирует |
| Контрольная сумма | ✅ Данные целостны |

**Общий вывод:** Все созданные таблицы и запросы работают корректно. Лабораторная работа выполнена в полном объёме.

# Файл со всеми выполненными кодами.
https://github.com/VoinovaEA/sql/blob/main/Script-87.sql



