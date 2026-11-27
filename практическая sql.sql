-- ========================================================
-- Практическая работа 1.
-- ========================================================

-- --------------------------------------------------------------------
-- Задача 1. Анализ времени и дат.
-- Дни недели продаж. Определите, в какой день недели (понедельник, вторник и т.д.) совершается наибольшее количество продаж (sales). Выведите день недели и количество транзакций.
------------------------------------------------------------------------


CREATE EXTENSION IF NOT EXISTS cube;
CREATE EXTENSION IF NOT EXISTS earthdistance;

SELECT 
    TO_CHAR(sales_transaction_date, 'Day') AS day_of_week, 
    COUNT(*) AS sales_count
FROM sales
GROUP BY day_of_week
ORDER BY sales_count DESC
LIMIT 1;

-- --------------------------------------------------------------------
-- Задача 2. Геопространственный анализ.
-- Для модели 'Model Chi' найдите среднюю широту и долготу покупателей (центроид продаж).
------------------------------------------------------------------------
SELECT 
    AVG(c.latitude) AS avg_latitude, 
    AVG(c.longitude) AS avg_longitude
FROM customers c
JOIN sales s ON c.customer_id = s.customer_id
JOIN products p ON s.product_id = p.product_id
WHERE p.model = 'Model Chi';

-- --------------------------------------------------------------------
-- Задача 3. Сложные типы (Массивы и JSON).
-- История покупок в JSON. Создайте представление или запрос, который формирует JSON-объект для каждого клиента: { "id": 1, "name": "Ivan", "products": ["Car", "Scooter"] }, используя агрегацию массивов.
------------------------------------------------------------------------

SELECT json_build_object(
    'id', c.customer_id,
    'name', c.first_name || ' ' || c.last_name,
    'products', json_agg(p.product_type) 
) AS customer_json
FROM customers c
JOIN sales s ON c.customer_id = s.customer_id
JOIN products p ON s.product_id = p.product_id
GROUP BY c.customer_id, c.first_name, c.last_name;
