-- ========================================================
-- ЛР 1 Вариант 2
-- ========================================================

-- --------------------------------------------------------------------
-- Задача 1. (SELECT + Сортировка)
-- Дилеры (dealerships) в 'TX', открытые после 2015 года. Сортировка: дата откр.
------------------------------------------------------------------------
select dealership_id, state, postal_code, latitude, longitude, date_opened
from dealerships d
where state = 'TX' and date_opened > '2015-12-31 00:00:00.000'
order by date_opened;

-- Так как нет дилеров, которые открылись после 2015 года, то оставим только дилеров из Тихаса
select dealership_id, state, postal_code, latitude, longitude, date_opened
from dealerships d
where state = 'TX'
order by date_opened;


-- --------------------------------------------------------------------
-- Задача 2. (Логика и Фильтры)
-- Клиенты (customers) без суффикса, но с телефоном.
------------------------------------------------------------------------
select first_name, last_name, suffix, postal_code, latitude, longitude
from customers с
where (suffix is null or suffix = '')
  and phone is not null;
