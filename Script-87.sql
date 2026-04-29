-- ============================================
-- ЛАБОРАТОРНАЯ РАБОТА
-- Вариант 002
-- Студент: Войнова Екатерина Андреевна 
-- ============================================

-- ============================================
-- ЗАДАНИЕ 1. Создание базы данных и таблицы продаж
-- ============================================

DROP TABLE IF EXISTS sales_var002;

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

-- 1.4. Вставляем 100 строк данных (распределенных по 3 месяцам)
-- Для варианта 2:
-- sale_id начинается с 2001
-- customer_id диапазон: 200-299
-- product_id диапазон: 20-39
-- unit_price: от 12.00 до 512.00
-- quantity: от 1 до 7
INSERT INTO sales_var002 
(sale_id, sale_timestamp, product_id, category, customer_id, region, quantity, unit_price, discount_pct, is_online, ip_address)
SELECT 
    number + 2001 AS sale_id,
    toDateTime64(
        CASE 
            WHEN number < 34 THEN 
                concat('2024-05-15 ', 
                       leftPad(toString(10 + (number % 14)), 2, '0'), ':',
                       leftPad(toString(number % 60), 2, '0'), ':00')
            WHEN number < 67 THEN 
                concat('2024-06-20 ', 
                       leftPad(toString(8 + (number % 16)), 2, '0'), ':',
                       leftPad(toString(number % 60), 2, '0'), ':00')
            ELSE 
                concat('2024-07-10 ', 
                       leftPad(toString(9 + (number % 15)), 2, '0'), ':',
                       leftPad(toString(number % 60), 2, '0'), ':00')
        END,
        3
    ) AS sale_timestamp,
    20 + (number % 20) AS product_id,
    CASE (number % 5)
        WHEN 0 THEN 'Electronics'
        WHEN 1 THEN 'Clothing'
        WHEN 2 THEN 'Books'
        WHEN 3 THEN 'Home & Garden'
        ELSE 'Sports'
    END AS category,
    200 + (number % 100) AS customer_id,
    CASE (number % 6)
        WHEN 0 THEN 'North'
        WHEN 1 THEN 'South'
        WHEN 2 THEN 'East'
        WHEN 3 THEN 'West'
        WHEN 4 THEN 'Central'
        ELSE 'International'
    END AS region,
    1 + (number % 7) AS quantity,
    toDecimal64(12 + (number % 501), 2) AS unit_price,
    (number % 100) / 100.0 AS discount_pct,
    number % 2 AS is_online,
    toIPv4(
        concat(
            toString(192 + (number % 64)),
            '.',
            toString(168 + (number % 88)),
            '.',
            toString(number % 256),
            '.',
            toString(1 + (number % 254))
        )
    ) AS ip_address
FROM numbers(100);

-- 1.5. Проверяем количество строк
SELECT COUNT(*) AS total_rows FROM sales_var002;

-- 1.6. Проверяем распределение по месяцам (должно быть 3 разных месяца)
SELECT 
    toYYYYMM(sale_timestamp) AS month,
    COUNT(*) AS rows_count,
    MIN(sale_timestamp) AS first_date,
    MAX(sale_timestamp) AS last_date
FROM sales_var002
GROUP BY month
ORDER BY month;

-- ============================================
-- ЗАДАНИЕ 2. Аналитические запросы
-- ============================================

-- 2.1. Общая выручка по категориям
SELECT 
    category,
    SUM(quantity * unit_price * (1 - discount_pct)) AS total_revenue
FROM sales_var002
GROUP BY category
ORDER BY total_revenue DESC;

-- 2.2. Топ-3 клиента по количеству покупок
SELECT 
    customer_id,
    COUNT(*) AS purchase_count,
    SUM(quantity) AS total_quantity
FROM sales_var002
GROUP BY customer_id
ORDER BY purchase_count DESC
LIMIT 3;

-- 2.3. Средний чек по месяцам
SELECT 
    toYYYYMM(sale_timestamp) AS month,
    AVG(quantity * unit_price) AS avg_check
FROM sales_var002
GROUP BY month
ORDER BY month;

-- 2.4. Фильтрация по партиции (выводим данные только за Июнь 2024)
-- Проверяем, что обращается только к одной партиции
EXPLAIN SELECT * 
FROM sales_var002 
WHERE sale_timestamp >= '2024-06-01' 
  AND sale_timestamp < '2024-07-01';

-- Выполняем сам запрос
SELECT * 
FROM sales_var002 
WHERE sale_timestamp >= '2024-06-01' 
  AND sale_timestamp < '2024-07-01'
LIMIT 10;

-- ============================================
-- ЗАДАНИЕ 3. ReplacingMergeTree — справочник товаров
-- ============================================

-- 3.1. Создаем таблицу товаров
DROP TABLE products_var002;

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

-- 3.2. Вставляем 7 товаров (NNN % 10 + 5 = 2 + 5 = 7) с version = 1
INSERT INTO products_var002 VALUES
(20, 'Laptop Pro', 'Electronics', 'TechCorp', 999.99, 2.5, 1, now(), 1),
(21, 'Cotton T-Shirt', 'Clothing', 'FashionInc', 29.99, 0.2, 1, now(), 1),
(22, 'SQL Guide', 'Books', 'PubHouse', 45.00, 0.5, 1, now(), 1),
(23, 'Smartphone', 'Electronics', 'TechCorp', 699.99, 0.3, 1, now(), 1),
(24, 'Jeans', 'Clothing', 'FashionInc', 59.99, 0.6, 1, now(), 1),
(25, 'Data Science Book', 'Books', 'PubHouse', 79.99, 0.8, 1, now(), 1),
(26, 'Wireless Mouse', 'Electronics', 'TechCorp', 39.99, 0.1, 1, now(), 1);

-- 3.3. Вставляем обновления для 3 товаров с version = 2
-- Обновляем товар 20: Laptop Pro
INSERT INTO products_var002 VALUES
(20, 'Laptop Pro X', 'Electronics', 'TechCorp', 1099.99, 2.5, 1, now(), 2);

-- Обновляем товар 23: Smartphone
INSERT INTO products_var002 VALUES
(23, 'Smartphone Plus', 'Electronics', 'TechCorp', 799.99, 0.3, 1, now(), 2);

-- Обновляем товар 25: Data Science Book
INSERT INTO products_var002 VALUES
(25, 'Data Science Book (2nd Ed)', 'Books', 'PubHouse', 89.99, 0.8, 0, now(), 2);

-- 3.4. Выполняем SELECT - видны обе версии
SELECT * FROM products_var002 
WHERE product_id IN (20, 23, 25)
ORDER BY product_id, version;

-- 3.5. Выполняем OPTIMIZE TABLE
OPTIMIZE TABLE products_var002 FINAL;

-- 3.6. Повторяем SELECT - осталась только последняя версия
SELECT * FROM products_var002 
WHERE product_id IN (20, 23, 25)
ORDER BY product_id;

-- 3.7. Альтернатива: SELECT с FINAL
SELECT * FROM products_var002 FINAL
ORDER BY product_id;

-- ============================================
-- ЗАДАНИЕ 4. SummingMergeTree — агрегация метрик
-- ============================================

-- 4.1. Создаем таблицу daily_metrics
CREATE TABLE daily_metrics_var002 (
    metric_date    Date,
    campaign_id    UInt32,
    channel        LowCardinality(String),
    impressions    UInt64,
    clicks         UInt64,
    conversions    UInt32,
    spend_cents    UInt64
)
ENGINE = SummingMergeTree()
ORDER BY (metric_date, campaign_id, channel);

-- 4.2. Вставляем данные (5 дней, 4 кампании, по 2 канала)
-- Для варианта 2: (2 % 5 + 3 = 5 дней) для (2 % 3 + 2 = 4 кампании) по 2 канала
-- Кампании: campaign_id от 21 (NNN * 10 + 1 = 20 + 1 = 21) до 24
-- Дни: 2024-07-01, 2024-07-02, 2024-07-03, 2024-07-04, 2024-07-05
-- Каналы: 'Email', 'Social'

-- Первая вставка (основные данные)
INSERT INTO daily_metrics_var002 VALUES

('2024-07-01', 21, 'Email', 1000, 50, 5, 5000),
('2024-07-01', 21, 'Social', 2000, 100, 15, 10000),
('2024-07-02', 21, 'Email', 1200, 60, 6, 6000),
('2024-07-02', 21, 'Social', 2200, 110, 18, 11000),
('2024-07-03', 21, 'Email', 1100, 55, 5, 5500),
('2024-07-03', 21, 'Social', 2100, 105, 16, 10500),
('2024-07-04', 21, 'Email', 1300, 65, 7, 6500),
('2024-07-04', 21, 'Social', 2300, 115, 20, 11500),
('2024-07-05', 21, 'Email', 900, 45, 4, 4500),
('2024-07-05', 21, 'Social', 1900, 95, 14, 9500),


('2024-07-01', 22, 'Email', 1500, 75, 8, 7500),
('2024-07-01', 22, 'Social', 2500, 125, 20, 12500),
('2024-07-02', 22, 'Email', 1600, 80, 9, 8000),
('2024-07-02', 22, 'Social', 2700, 135, 22, 13500),
('2024-07-03', 22, 'Email', 1400, 70, 7, 7000),
('2024-07-03', 22, 'Social', 2600, 130, 21, 13000),
('2024-07-04', 22, 'Email', 1700, 85, 10, 8500),
('2024-07-04', 22, 'Social', 2800, 140, 24, 14000),
('2024-07-05', 22, 'Email', 1300, 65, 6, 6500),
('2024-07-05', 22, 'Social', 2400, 120, 19, 12000),


('2024-07-01', 23, 'Email', 800, 40, 4, 4000),
('2024-07-01', 23, 'Social', 1800, 90, 12, 9000),
('2024-07-02', 23, 'Email', 900, 45, 5, 4500),
('2024-07-02', 23, 'Social', 1900, 95, 13, 9500),
('2024-07-03', 23, 'Email', 850, 42, 4, 4250),
('2024-07-03', 23, 'Social', 1850, 92, 12, 9250),
('2024-07-04', 23, 'Email', 950, 48, 5, 4750),
('2024-07-04', 23, 'Social', 1950, 98, 14, 9750),
('2024-07-05', 23, 'Email', 750, 38, 3, 3750),
('2024-07-05', 23, 'Social', 1750, 88, 11, 8750),


('2024-07-01', 24, 'Email', 600, 30, 3, 3000),
('2024-07-01', 24, 'Social', 1600, 80, 10, 8000),
('2024-07-02', 24, 'Email', 700, 35, 4, 3500),
('2024-07-02', 24, 'Social', 1700, 85, 12, 8500),
('2024-07-03', 24, 'Email', 650, 32, 3, 3250),
('2024-07-03', 24, 'Social', 1650, 82, 11, 8250),
('2024-07-04', 24, 'Email', 750, 38, 4, 3750),
('2024-07-04', 24, 'Social', 1750, 88, 13, 8750),
('2024-07-05', 24, 'Email', 550, 28, 2, 2750),
('2024-07-05', 24, 'Social', 1550, 78, 9, 7750);

-- 4.3. Вставляем повторные строки с теми же ключами (для демонстрации суммирования)
INSERT INTO daily_metrics_var002 VALUES

('2024-07-01', 21, 'Email', 500, 25, 2, 2500),

('2024-07-02', 22, 'Social', 300, 15, 2, 1500),

('2024-07-03', 23, 'Email', 150, 8, 1, 750),

('2024-07-04', 24, 'Social', 250, 12, 2, 1250);

-- 4.4. Выполняем SELECT до OPTIMIZE (видим дубликаты)
SELECT * FROM daily_metrics_var002 
WHERE (campaign_id = 21 AND channel = 'Email' AND metric_date = '2024-07-01')
   OR (campaign_id = 22 AND channel = 'Social' AND metric_date = '2024-07-02')
   OR (campaign_id = 23 AND channel = 'Email' AND metric_date = '2024-07-03')
   OR (campaign_id = 24 AND channel = 'Social' AND metric_date = '2024-07-04')
ORDER BY metric_date, campaign_id, channel;

-- 4.5. Выполняем OPTIMIZE TABLE
OPTIMIZE TABLE daily_metrics_var002 FINAL;

-- 4.6. Проверяем, что данные просуммировались
SELECT * FROM daily_metrics_var002 
WHERE (campaign_id = 21 AND channel = 'Email' AND metric_date = '2024-07-01')
   OR (campaign_id = 22 AND channel = 'Social' AND metric_date = '2024-07-02')
   OR (campaign_id = 23 AND channel = 'Email' AND metric_date = '2024-07-03')
   OR (campaign_id = 24 AND channel = 'Social' AND metric_date = '2024-07-04')
ORDER BY metric_date, campaign_id, channel;


-- 4.7. Запрос: CTR (click-through rate) по каналам
SELECT 
    channel,
    SUM(clicks) AS total_clicks,
    SUM(impressions) AS total_impressions,
    (SUM(clicks) / SUM(impressions)) * 100 AS ctr_percent
FROM daily_metrics_var002
GROUP BY channel
ORDER BY ctr_percent DESC;

-- ============================================
-- ЗАДАНИЕ 5. Комплексный анализ и самопроверка
-- ============================================



-- 5.2. JOIN между таблицами (топ-5 товаров по выручке)
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


-- 5.3. Типы данных всех таблиц
DESCRIBE TABLE sales_var002;
DESCRIBE TABLE products_var002;
DESCRIBE TABLE daily_metrics_var002;

-- 5.4. Запрос с массивом (arrayJoin)
-- Создаем временную таблицу tags
CREATE TABLE tags_var002 (
    item_id  UInt32,
    item_name String,
    tags     Array(String)
) ENGINE = MergeTree()
ORDER BY item_id;

-- Вставляем данные
INSERT INTO tags_var002 VALUES
    (1, 'Item A', ['sale', 'popular', 'new']),
    (2, 'Item B', ['premium', 'limited']),
    (3, 'Item C', ['sale', 'clearance']);

-- Выполняем запрос с arrayJoin
SELECT
    arrayJoin(tags) AS tag,
    count() AS items_count
FROM tags_var002
GROUP BY tag
ORDER BY items_count DESC;

-- 5.5. Контрольная сумма (итоговая проверка)
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