
# Отчет по самостоятельной работе №1.

## Студент: Войнова Екатерина Андреевна ЦИБ-241.
## Тема: Управление проектами (схема Project Management).



## 1. Цель работы.

Научиться подключаться к базе данных PostgreSQL с использованием Python (`psycopg2`), создавать структуры данных (схемы и таблицы), наполнять их данными, выполнять SQL-запросы и визуализировать результаты в Yandex DataLens.



## 2. Задание 1: Подключение к базе данных.

### 2.1. Выполненные действия.

Было выполнено подключение к локальному серверу PostgreSQL с использованием pgAdmin.

**Параметры подключения:**
- **Сервер:** localhost 
- **Порт:** 5432
- **База данных:** project_management_db
- **Пользователь:** postgres

### 2.2. Результат.

```sql
-- Проверка подключения и версии
SELECT version();
```

**Результат:** PostgreSQL 18 (локальная установка)

---

## 3. Задание 2: Создание схемы и таблиц.

### 3.1. SQL-код создания структуры.

```sql
-- 1. СОЗДАНИЕ СХЕМЫ
CREATE SCHEMA IF NOT EXISTS crm_mgpu_02;

-- 2. ВЫБИРАЕМ СХЕМУ (ВАЖНО!)
SET search_path TO crm_mgpu_02;

-- 3. УДАЛЯЕМ СТАРЫЕ ТАБЛИЦЫ (ЕСЛИ ЕСТЬ)
DROP TABLE IF EXISTS activities CASCADE;
DROP TABLE IF EXISTS deals CASCADE;
DROP TABLE IF EXISTS contacts CASCADE;
DROP TABLE IF EXISTS companies CASCADE;

-- 4. СОЗДАНИЕ ТАБЛИЦ
CREATE TABLE companies (
    company_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    industry VARCHAR(50),
    website VARCHAR(100)
);

CREATE TABLE contacts (
    contact_id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES companies(company_id) ON DELETE CASCADE,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE
);

CREATE TABLE deals (
    deal_id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES companies(company_id) ON DELETE CASCADE,
    deal_name VARCHAR(100) NOT NULL,
    stage VARCHAR(50),
    amount DECIMAL(12, 2)
);

CREATE TABLE activities (
    activity_id SERIAL PRIMARY KEY,
    contact_id INTEGER REFERENCES contacts(contact_id) ON DELETE CASCADE,
    type VARCHAR(50),
    notes TEXT,
    activity_date DATE DEFAULT CURRENT_DATE
);

-- ПРОВЕРЯЕМ, ЧТО ТАБЛИЦЫ СОЗДАЛИСЬ
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'crm_mgpu_06'
ORDER BY table_name;
```

### 3.2. Результат.

Созданы 4 таблицы в схеме crm_mgpu_02:

companies — информация о компаниях

contacts — контактные лица компаний

deals — информация о сделках

activities — записи о взаимодействиях с контактами

---

## 4. Задание 3: Наполнение таблиц данными.

### 4.1. Добавляем данные (пример компания Ромашка).
```-- 2. ДОБАВЛЯЕМ КОНТАКТЫ
INSERT INTO contacts (company_id, first_name, last_name, email) VALUES
-- Компания 1 (ООО Ромашка)
(1, 'Ольга', 'Иванова', 'olga.ivanova@romashka.ru'),
(1, 'Петр', 'Смирнов', 'petr.smirnov@romashka.ru'),
(1, 'Елена', 'Кузнецова', 'elena.kuznetsova@romashka.ru'),
(1, 'Андрей', 'Попов', 'andrey.popov@romashka.ru'),
```


### 4.2. СТАТИСТИКА ПО ТАБЛИЦАМ.

```sql
SELECT 'Компании' as table_name, COUNT(*) as count FROM companies
UNION ALL
SELECT 'Контакты', COUNT(*) FROM contacts
UNION ALL
SELECT 'Сделки', COUNT(*) FROM deals
UNION ALL
SELECT 'Активности', COUNT(*) FROM activities;
```

**Результат:**

<img width="257" height="180" alt="image" src="https://github.com/user-attachments/assets/1dafa69c-95df-4f2e-b540-ddc0ba7524c6" />


---

## 5. Задание 4: SELECT запрос, показывает пример данных.

```sql
-- ПОКАЗЫВАЕМ ПРИМЕРЫ ДАННЫХ
SELECT * FROM companies LIMIT 5;
SELECT * FROM contacts LIMIT 5;
SELECT * FROM deals LIMIT 5;
SELECT * FROM activities LIMIT 5;
```

**Пример вывода:**
<img width="904" height="217" alt="image" src="https://github.com/user-attachments/assets/e26a1780-88ac-495d-8338-db1f987f8e01" />


---

## 6. Задание 5: JOIN запрос.

```sql
SELECT 
    c.name AS company_name,
    c.industry,
    c.website,
    ct.first_name,
    ct.last_name,
    ct.email,
    d.deal_name,
    d.stage AS deal_stage,
    d.amount,
    a.type AS activity_type,
    a.notes,
    a.activity_date
FROM companies c
LEFT JOIN contacts ct ON c.company_id = ct.company_id
LEFT JOIN deals d ON c.company_id = d.company_id
LEFT JOIN activities a ON ct.contact_id = a.contact_id
ORDER BY c.name, a.activity_date;

```

Структура данных: Таблица формируется на основе компаний (companies) с добавлением информации о контактах, сделках и активностях. Для каждой компании могут быть указаны несколько контактов, для каждого контакта — несколько активностей, для каждой компании — несколько сделок.

**Пример вывода:**

<img width="1463" height="367" alt="image" src="https://github.com/user-attachments/assets/9ef070d1-bbec-426e-907e-169c6db4bead" />


---
### Структура базы данных.

<img width="1842" height="962" alt="image" src="https://github.com/user-attachments/assets/36a6bed1-cdc9-4d84-9546-45a5b031c938" />



## 7. Экспорт данных в CSV.

Результат JOIN запроса был экспортирован в CSV файл `join_results.csv` с помощью функции pgAdmin "Download as CSV".

**Структура CSV файла:**
- Название_задачи
- Проект
- Исполнитель
- Срок_выполнения
- Приоритет

---

## 8. Визуализация в Yandex DataLens.

### 8.1. Создание подключения.

1. В Yandex DataLens создано новое подключение типа **"Файлы (CSV, Excel)"**
2. Загружен файл `join_results.csv`

### 8.2. Создание датасета.

Создан датасет со следующими полями:

| Поле | Тип | Описание |
|------|-----|---------|
| company_name | строка | Название компании |
| industry | строка | Отрасль компании |
| website | строка | Веб-сайт компании |
| first_name | строка | Имя контактного лица |
| last_name | строка | Фамилия контактного лица |
| email | строка | Email контактного лица |
| deal_name | строка | Название сделки |
| deal_stage | строка | Этап сделки |
| amount | число | Сумма сделки |
| activity_type | строка | Тип активности |
| notes | строка | Заметки по активности |
| activity_date | дата | Дата активности |

### 8.3. Созданные чарты.

## Чарт 1: Круговая диаграмма "Распределение суммы сделок по этапам"
Тип: Круговая диаграмма

Цвет: deal_stage (этап сделки)

Показатели: sum(amount) — сумма сделок

Подписи: amount (значения сумм)

Фильтры: company_name (AO Mer... и др.)

Результат: Визуализировано распределение сумм по этапам: Закрытие, Переговоры, Реализация, Предложение, Квалификация

Фото 1 чарта.
<img width="1280" height="646" alt="image" src="https://github.com/user-attachments/assets/3d6ada8b-886f-4366-b6ab-79496d6ee2d7" />
## Чарт 2: Столбчатая диаграмма "Суммы сделок по компаниям и этапам"
Тип: Столбчатая диаграмма

X: company_name (название компании)

Y: sum(amount) (сумма сделок)

Цвет: deal_stage (этап сделки)

Фильтры: deal_stage (Закрытие, Квалификация, Переговоры, Предложение, Реализация)

Результат: Столбчатая диаграмма, показывающая распределение сумм сделок по компаниям с разбивкой по этапам

Фото 2 чарта.
<img width="1280" height="672" alt="image" src="https://github.com/user-attachments/assets/1d2fea3c-a6ba-440f-8fb6-dcd3c414653a" />

### 8.4. Создание дашборда.

<img width="1245" height="509" alt="image" src="https://github.com/user-attachments/assets/bb34be47-18a6-4baf-a836-e5bb774bf7cc" />

Создан дашборд "CRM аналитика", объединяющий все чарты для комплексного анализа данных.
https://datalens.yandex/rvle7l7gdtbeb
---

## 9. Выводы

В ходе выполнения самостоятельной работы были решены следующие задачи:

1. **Установлено подключение** к локальному серверу PostgreSQL через pgAdmin
2. **Создана структура базы данных:** схема `crm_mgpu_02` и 4 связанные таблицы (`companies`, `contacts`, `deals`, `activities`)
3. **Добавлены тестовые данные:** компании, контакты, сделки и активности
4. **Выполнены SQL-запросы:**
   - Простой SELECT для просмотра данных в каждой таблице
   - JOIN запрос с левыми соединениями (`LEFT JOIN`) для получения сводной информации о компаниях, контактах, сделках и активностях
5. **Экспортированы результаты** в CSV файл `join_results.csv`
6. **Создана визуализация** в Yandex DataLens с 2 типами диаграмм (круговая и столбчатая) и дашбордом "CRM аналитика"

### 9.1. Полученные метрики

| Метрика | Значение |
| --- | --- |
| Общее количество компаний | 4 |
| Общее количество контактов | 15 |
| Общее количество сделок | 15 |
| Общее количество активностей | 15 |
| Общая сумма всех сделок | 1 455 000,00 руб. |

**Распределение сумм по этапам сделок:**

| Этап сделки | Сумма (руб.) |
| --- | --- |
| Закрытие | 540 000,00 |
| Переговоры | 515 000,00 |
| Реализация | 240 000,00 |
| Предложение | 160 000,00 |
| Квалификация | 0,00 |

**Анализ распределения:**
- Наибольшая сумма сделок зафиксирована в этапе **"Закрытие"** (540 000 руб.) — 37,1% от общей суммы
- Второе место занимает этап **"Переговоры"** (515 000 руб.) — 35,4% от общей суммы
- Этап **"Реализация"** составляет 240 000 руб. — 16,5%
- Этап **"Предложение"** — 160 000 руб. — 11,0%
- Этап **"Квалификация"** не содержит сделок с ненулевой суммой

### 9.2. Навыки, полученные в ходе работы

- Работа с pgAdmin и SQL
- Создание и настройка базы данных PostgreSQL
- Написание SQL запросов (CREATE SCHEMA, CREATE TABLE, INSERT, SELECT, LEFT JOIN, ORDER BY)
- Экспорт данных в CSV
- Визуализация данных в Yandex DataLens
- Создание круговых и столбчатых диаграмм
- Настройка фильтров и цветового кодирования в чартах
- Создание дашбордов для бизнес-аналитики

---

**Дата выполнения:** 27.03.2026  
**Студент:** Войнова Екатерина Андреевна, ЦИБ-241
