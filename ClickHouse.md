# Практическое занятие №1 по ClickHouse.
## Вариант 2.

**Студент:** Войнова Екатерина Андреевна  
**Цель занятия:**
Получить практические навыки работы с колоночной СУБД ClickHouse: подключиться к облачному серверу, освоить создание баз данных и таблиц с правильным выбором типов данных и движков семейства MergeTree.

---

## Содержание
1. Задание 1. Создание базы данных и таблицы продаж
2. Задание 2. Аналитические запросы
3. Задание 3. ReplacingMergeTree — справочник товаров
4. Задание 4. SummingMergeTree — агрегация метрик
5. Задание 5. Комплексный анализ и самопроверка
6. Ответы на вопросы

---

## Задание 1. Создание таблицы продаж.

### SQL-код
```sql

-- Создание таблицы продаж
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

-- Вставка 100 строк данных (распределенных по 3 месяцам)
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
```
**Результаты выполнения:**

**1.1 Создание таблицы и вставка данных.**

<img width="491" height="473" alt="image" src="https://github.com/user-attachments/assets/d4aa6e34-0b80-41a3-a668-08dd3084b0d5" />

**1.2 Проверка количества строк.**
```
SELECT COUNT(*) AS total_rows FROM sales_var002;
```

<img width="245" height="100" alt="image" src="https://github.com/user-attachments/assets/6892e949-ca9b-45df-930a-cc3d7abd6260" />

**1.3 Распределение по месяцам.**

<img width="660" height="163" alt="image" src="https://github.com/user-attachments/assets/da38e1e3-4ad9-4a7a-8923-cb0b031e1b2e" />

## Задание 2. Аналитические запросы.

### SQL-код
```
-- 2.1 Общая выручка по категориям
SELECT 
    category,
    SUM(quantity * unit_price * (1 - discount_pct)) AS total_revenue
FROM sales_var002
GROUP BY category
ORDER BY total_revenue DESC;

-- 2.2 Топ-3 клиента по количеству покупок
SELECT 
    customer_id,
    COUNT(*) AS purchase_count,
    SUM(quantity) AS total_quantity
FROM sales_var002
GROUP BY customer_id
ORDER BY purchase_count DESC
LIMIT 3;

-- 2.3 Средний чек по месяцам
SELECT 
    toYYYYMM(sale_timestamp) AS month,
    AVG(quantity * unit_price) AS avg_check
FROM sales_var002
GROUP BY month
ORDER BY month;

-- 2.4 Фильтрация по партиции (только Июнь 2024)
SELECT 
    category,
    COUNT(*) AS sales_count,
    SUM(quantity * unit_price * (1 - discount_pct)) AS revenue
FROM sales_var002
WHERE sale_timestamp >= '2024-06-01' AND sale_timestamp < '2024-07-01'
GROUP BY category
ORDER BY revenue DESC;
```
**Результаты выполнения:**

**2.1 – Общая выручка по категориям.**

<img width="406" height="211" alt="image" src="https://github.com/user-attachments/assets/1e6f352e-ae5b-4e06-91f9-ab611c3d4673" />

**2.2 – Топ-3 клиента по количеству покупок.**

<img width="572" height="157" alt="image" src="https://github.com/user-attachments/assets/7a8f54c5-79a5-4185-9884-776944a6f6c9" />

**2.3 – Средний чек по месяцам.**

<img width="449" height="142" alt="image" src="https://github.com/user-attachments/assets/3263e5d2-b2c2-43ee-b7f4-ad2eb3ebeb0a" />


## Задание 3. ReplacingMergeTree — справочник товаров.

### SQL-код
```sql
-- 3.1 Создание таблицы товаров
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

-- 3.2 Вставка 7 товаров с version = 1
INSERT INTO products_var002 VALUES
(20, 'Laptop Pro', 'Electronics', 'TechCorp', 999.99, 2.5, 1, now(), 1),
(21, 'Cotton T-Shirt', 'Clothing', 'FashionInc', 29.99, 0.2, 1, now(), 1),
(22, 'SQL Guide', 'Books', 'PubHouse', 45.00, 0.5, 1, now(), 1),
(23, 'Smartphone', 'Electronics', 'TechCorp', 699.99, 0.3, 1, now(), 1),
(24, 'Jeans', 'Clothing', 'FashionInc', 59.99, 0.6, 1, now(), 1),
(25, 'Data Science Book', 'Books', 'PubHouse', 79.99, 0.8, 1, now(), 1),
(26, 'Wireless Mouse', 'Electronics', 'TechCorp', 39.99, 0.1, 1, now(), 1);

-- 3.3 Обновление 3 товаров (version = 2)
INSERT INTO products_var002 VALUES
(20, 'Laptop Pro X', 'Electronics', 'TechCorp', 1099.99, 2.5, 1, now(), 2),
(23, 'Smartphone Plus', 'Electronics', 'TechCorp', 799.99, 0.3, 1, now(), 2),
(25, 'Data Science Book (2nd Ed)', 'Books', 'PubHouse', 89.99, 0.8, 0, now(), 2);

-- 3.4 SELECT до OPTIMIZE (видны обе версии)
SELECT * FROM products_var002 
WHERE product_id IN (20, 23, 25)
ORDER BY product_id, version;

-- 3.5 OPTIMIZE TABLE
OPTIMIZE TABLE products_var002 FINAL;

-- 3.6 SELECT после OPTIMIZE (осталась только версия 2)
SELECT * FROM products_var002 
WHERE product_id IN (20, 23, 25)
ORDER BY product_id;

-- 3.7 Альтернатива: SELECT с FINAL
SELECT * FROM products_var002 FINAL
ORDER BY product_id;
```
**Результаты выполнения:**

**3.1 – SELECT до OPTIMIZE.**

<img width="1184" height="100" alt="image" src="https://github.com/user-attachments/assets/d3b3d75c-c241-40dc-b3d8-17c63858939a" />

**3.2 – SELECT после OPTIMIZE.**

<img width="1208" height="117" alt="image" src="https://github.com/user-attachments/assets/0338582d-bb32-499c-96b6-2f7e6f00021d" />

## Задание 4. SummingMergeTree — агрегация метрик.

### SQL-код
```sql
-- 4.1 Создание таблицы daily_metrics
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

-- 4.2 Вставка основных данных (5 дней, 4 кампании, 2 канала)
INSERT INTO daily_metrics_var002 VALUES
-- Кампания 21
('2024-07-01', 21, 'Email', 1000, 50, 5, 5000),
('2024-07-01', 21, 'Social', 2000, 100, 15, 10000),
-- ... (остальные данные)

-- 4.3 Вставка повторных строк для демонстрации суммирования
INSERT INTO daily_metrics_var002 VALUES
('2024-07-01', 21, 'Email', 500, 25, 2, 2500),
('2024-07-02', 22, 'Social', 300, 15, 2, 1500),
('2024-07-03', 23, 'Email', 150, 8, 1, 750),
('2024-07-04', 24, 'Social', 250, 12, 2, 1250);

-- 4.4 SELECT до OPTIMIZE (видны дубликаты)
SELECT * FROM daily_metrics_var002 
WHERE (campaign_id = 21 AND channel = 'Email' AND metric_date = '2024-07-01')
ORDER BY metric_date, campaign_id, channel;

-- 4.5 OPTIMIZE TABLE
OPTIMIZE TABLE daily_metrics_var002 FINAL;

-- 4.6 SELECT после OPTIMIZE (данные просуммированы)
SELECT * FROM daily_metrics_var002 
WHERE (campaign_id = 21 AND channel = 'Email' AND metric_date = '2024-07-01')
ORDER BY metric_date, campaign_id, channel;

-- 4.7 CTR по каналам
SELECT 
    channel,
    SUM(clicks) AS total_clicks,
    SUM(impressions) AS total_impressions,
    (SUM(clicks) / SUM(impressions)) * 100 AS ctr_percent
FROM daily_metrics_var002
GROUP BY channel
ORDER BY ctr_percent DESC;
```

**4.1 – SELECT до OPTIMIZE (видны дубликаты).**

<img width="946" height="130" alt="image" src="https://github.com/user-attachments/assets/8dc8d376-d2c3-4a89-ae37-20f547b9289f" />


**4.2 – SELECT после OPTIMIZE.**

<img width="942" height="128" alt="image" src="https://github.com/user-attachments/assets/48419890-c3c6-4051-9409-25fea01c4045" />

**4.3 – CTR по каналам.**

<img width="579" height="83" alt="image" src="https://github.com/user-attachments/assets/1e6176c5-3fb7-4d12-a9d0-451cf437340a" />

## Задание 5. Комплексный анализ и самопроверка

### SQL-код
```sql
-- 5.1 Проверка партиций
SELECT
    partition,
    count() AS parts,
    sum(rows) AS total_rows,
    formatReadableSize(sum(bytes_on_disk)) AS size
FROM system.parts
WHERE database = 'db_var002'
  AND table = 'sales_var002'
  AND active
GROUP BY partition
ORDER BY partition;

-- 5.2 JOIN между таблицами (топ-5 товаров по выручке)
SELECT
    p.product_name,
    p.category,
    sum(s.quantity * s.unit_price * (1 - s.discount_pct)) AS revenue
FROM sales_var002 AS s
INNER JOIN products_var002 FINAL AS p
    ON s.product_id = p.product_id
GROUP BY p.product_name, p.category
ORDER BY revenue DESC
LIMIT 5;

-- 5.3 Типы данных всех таблиц
DESCRIBE TABLE sales_var002;
DESCRIBE TABLE products_var002;
DESCRIBE TABLE daily_metrics_var002;

-- 5.4 Запрос с массивом (arrayJoin)
CREATE TABLE tags_var002 (
    item_id  UInt32,
    item_name String,
    tags     Array(String)
) ENGINE = MergeTree()
ORDER BY item_id;

INSERT INTO tags_var002 VALUES
(1, 'Item A', ['sale', 'popular', 'new']),
(2, 'Item B', ['premium', 'limited']),
(3, 'Item C', ['sale', 'clearance']);

SELECT
    arrayJoin(tags) AS tag,
    count() AS items_count
FROM tags_var002
GROUP BY tag
ORDER BY items_count DESC;

-- 5.5 Контрольная сумма
SELECT 'sales' AS tbl, count() AS rows, sum(quantity) AS check_sum FROM sales_var002
UNION ALL
SELECT 'products', count(), sum(toUInt64(product_id)) FROM products_var002 FINAL
UNION ALL
SELECT 'metrics', count(), sum(clicks) FROM daily_metrics_var002;
```
**5.1. JOIN между таблицами (топ-5 товаров по выручке).**

<img width="452" height="181" alt="image" src="https://github.com/user-attachments/assets/d131b245-ea1a-4139-98d1-b9e94fc25d37" />

**5.2. Запрос с массивом.**

<img width="285" height="184" alt="image" src="https://github.com/user-attachments/assets/90b1b671-dc7f-4032-af43-3f34e24bb682" />

**5.5. – Контрольная сумма.**

<img width="418" height="112" alt="image" src="https://github.com/user-attachments/assets/661cddbf-c146-40a1-857b-4a3e50109250" />

# Ответы на вопросы по ClickHouse


---

### 1. Почему LowCardinality(String) эффективнее обычного String для столбца category?

`LowCardinality(String)` эффективнее обычного `String` по следующим причинам:

**Словарное кодирование.** Данные хранятся в виде словаря, содержащего все уникальные строковые значения, и числовых индексов, ссылающихся на этот словарь. Такой подход позволяет избежать многократного хранения одинаковых строк.

**Ускорение запросов.** При выполнении операций `GROUP BY`, `WHERE`, `JOIN` и сортировок ClickHouse работает с компактными числовыми индексами вместо полноценных строк. Это значительно сокращает время сравнения и обработки данных.

**Экономия дискового пространства.** Для столбцов с небольшим количеством уникальных значений, таких как категории товаров, экономия памяти может достигать 80-90% по сравнению с обычным `String`.

**Улучшение кэширования.** Компактное представление данных позволяет разместить больше записей в процессорном кэше, что снижает количество обращений к оперативной памяти и ускоряет выполнение запросов.

**Особенность использования.** Данный тип оптимален для столбцов, где количество уникальных значений не превышает нескольких тысяч. В случае столбца `category` с ограниченным набором значений (Electronics, Clothing, Books, Sports) применение `LowCardinality(String)` дает максимальный эффект оптимизации.

---

### 2. В чём разница между ORDER BY и PRIMARY KEY в ClickHouse?

`ORDER BY` и `PRIMARY KEY` в ClickHouse выполняют разные функции, хотя и связаны между собой.

**ORDER BY** является обязательным параметром для таблиц семейства MergeTree. Он определяет физический порядок сортировки данных на диске. Данные записываются в отсортированном виде согласно полям, указанным в `ORDER BY`. Это влияет на эффективность компрессии, так как похожие данные располагаются рядом, а также определяет скорость сканирования при диапазонных запросах.

**PRIMARY KEY** является опциональным параметром и задает разреженный индекс, который используется для быстрого пропуска блоков данных при выполнении запросов с фильтрацией. Первичный ключ не обеспечивает уникальность записей и не является первичным ключом в классическом реляционном понимании.

**Взаимосвязь.** Первичный ключ обязательно должен быть префиксом `ORDER BY`. Это означает, что поля, указанные в `PRIMARY KEY`, должны идти в начале списка полей `ORDER BY` в том же порядке. Например, при `ORDER BY (timestamp, user_id, event_type)` можно указать `PRIMARY KEY (timestamp, user_id)`, но нельзя указать `PRIMARY KEY (user_id, timestamp)`.

**Влияние на производительность.** `ORDER BY` определяет эффективность компрессии и скорость полносканирующих запросов. `PRIMARY KEY` определяет скорость точечных запросов и запросов с фильтрацией по диапазону.

---

### 3. Когда следует использовать ReplacingMergeTree вместо MergeTree?

`ReplacingMergeTree` следует использовать в следующих сценариях:

**Хранение справочных данных.** Когда необходимо поддерживать актуальное состояние объектов, таких как профили пользователей, товары, цены или остатки. Вставка новой версии записи с более высоким значением поля версии заменяет старую версию в процессе слияния партов.

**Реализация SCD Type 2.** При необходимости отслеживать историю изменений, но в аналитических запросах использовать только последнюю актуальную версию. Это позволяет хранить полную историю изменений, но получать актуальный срез данных с помощью модификатора `FINAL`.

**Дедупликация данных.** Когда в процессе загрузки данных из внешних источников могут возникать дубликаты записей. `ReplacingMergeTree` позволяет автоматически удалять дубликаты при фоновых слияниях на основе ключа сортировки.

**Эмуляция обновлений.** Поскольку ClickHouse не поддерживает операции `UPDATE` и `DELETE` в традиционном виде, `ReplacingMergeTree` предоставляет механизм замены записей через вставку новых версий.

**Когда не следует использовать.** `ReplacingMergeTree` не подходит для сценариев, где требуется хранить полную историю всех изменений без потери старых версий, где критичен порядок поступления данных или где требуется точное управление процессом дедупликации с гарантированным временем выполнения.

---

### 4. Почему SummingMergeTree не заменяет GROUP BY в аналитических запросах?

`SummingMergeTree` является инструментом предварительной агрегации, но не заменяет `GROUP BY` по ряду причин:

**Асинхронность суммирования.** Суммирование числовых значений происходит только в процессе фоновых слияний партов данных. На момент выполнения запроса данные могут быть не полностью агрегированы, что может привести к получению частично суммированных результатов.

**Ограниченный набор агрегатных функций.** `SummingMergeTree` автоматически суммирует только числовые столбцы. Он не поддерживает другие агрегатные функции, такие как `AVG`, `MAX`, `MIN`, `COUNT DISTINCT`, которые часто требуются в аналитических запросах.

**Фиксированный ключ агрегации.** Агрегация в `SummingMergeTree` происходит только по полям, указанным в `ORDER BY`. Если аналитический запрос требует группировки по другому набору полей, `SummingMergeTree` не может предоставить готовый результат.

**Гарантия точности.** Для получения корректных результатов необходимо либо дождаться полного слияния всех партов, что невозможно контролировать, либо использовать `GROUP BY` поверх таблицы, что фактически выполняет агрегацию повторно.

**Правильный подход.** `SummingMergeTree` следует использовать как слой предварительной агрегации для снижения объема данных и ускорения запросов, но в финальных аналитических запросах всегда применять `GROUP BY` для гарантии корректности и поддержки всех необходимых агрегатных функций.

---

### 5. Что произойдёт, если не выполнить OPTIMIZE TABLE FINAL для ReplacingMergeTree?

Отказ от выполнения `OPTIMIZE TABLE FINAL` приводит к следующим последствиям:

**Сосуществование множественных версий.** Данные с разными значениями поля версии продолжают существовать в различных партах таблицы. При этом каждая версия занимает место на диске и учитывается при сканировании.

**Возврат неактуальных данных.** Запрос `SELECT * FROM table` без модификатора `FINAL` может возвращать несколько версий одной записи, включая устаревшие. Это приводит к некорректным результатам при подсчете количества записей или агрегации.

**Избыточное потребление ресурсов.** Старые версии записей занимают дисковое пространство и увеличивают объем данных, которые необходимо сканировать при выполнении запросов. Это негативно влияет на производительность.

**Постепенное фоновое очищение.** Со временем старые версии будут удалены автоматически в процессе фоновых слияний партов. Однако время ожидания может составлять от нескольких минут до нескольких часов в зависимости от настроек слияний, интенсивности вставки данных и объема таблицы.

**Необходимость использования FINAL.** Если не выполнять принудительное слияние, для получения актуальных данных в запросах необходимо всегда использовать модификатор `FINAL`: `SELECT * FROM table FINAL`. Это гарантирует, что из всех версий будет выбрана только последняя, но увеличивает время выполнения запроса, так как требует полного сканирования и разрешения версий в момент выполнения.

**Рекомендация по использованию.** В производственных средах рекомендуется полагаться на автоматические фоновые слияния и использовать модификатор `FINAL` в запросах, требующих актуальных данных. `OPTIMIZE TABLE FINAL` следует применять только для тестирования, разовых операций или в сценариях, где требуется немедленное получение корректных результатов без использования `FINAL`.


## Заключение

В ходе выполнения лабораторной работы для варианта 002 были решены следующие задачи:

1. Создана таблица продаж `sales_var002` с движком MergeTree, данными за три месяца (май, июнь, июль 2024) в количестве 100 записей. Партиционирование по месяцам ускорило фильтрацию данных.

2. Выполнены аналитические запросы: рассчитана выручка по категориям, определены топ-3 клиента по числу покупок, вычислен средний чек по месяцам. Проверена фильтрация только по партиции июня.

3. Создана таблица товаров `products_var002` с движком ReplacingMergeTree. Вставлено 7 товаров (version=1), затем 3 обновлены (version=2). После OPTIMIZE TABLE остались только актуальные версии. Модификатор FINAL позволяет получать их без слияния.

4. Создана таблица метрик `daily_metrics_var002` с движком SummingMergeTree. Вставлены данные за 5 дней, 4 кампании, 2 канала. Повторные строки просуммированы после OPTIMIZE TABLE. Рассчитан CTR по каналам.

5. Выполнен комплексный анализ: проверены партиции, выполнен JOIN таблиц для топ-5 товаров по выручке, выведена структура всех таблиц, реализован запрос с arrayJoin, проверена контрольная сумма.

Все требования варианта 002 выполнены. ClickHouse показал высокую производительность при агрегации данных, эффективность партиционирования и удобство работы с версионированием.

---

*Вариант 002*

