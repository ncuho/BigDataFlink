# Лабораторная работа №3 — Потоковая обработка с Apache Flink

## Цель работы
Реализация потоковой обработки данных с помощью Apache Flink:
- Чтение данных из Kafka
- Трансформация в модель "звезда"
- Запись результата в PostgreSQL

## Архитектура решения

CSV файлы → Kafka Producer → Kafka Topic → Flink Job → PostgreSQL (модель "звезда")

### Модель данных "звезда"
- **Измерения:** dim_country, dim_city, dim_location, dim_date, dim_pet_breed, dim_pet, dim_product_category, dim_brand, dim_product, dim_customer, dim_seller, dim_store, dim_supplier
- **Факты:** fact_sales

## Требования

- Docker и Docker Compose
- 10 CSV файлов с исходными данными (MOCK_DATA*.csv) в папке `data/`

## Инструкция по запуску

### Шаг 1. Клонирование репозитория

```bash
git clone https://github.com/ncuho/BigDataFlink.git
cd BigDataFlink
```

### Шаг 2. Запуск всех сервисов

```bash
docker-compose up -d
```

Будут запущены следующие сервисы:

- Zookeeper (порт 2181) — координатор для Kafka
- Kafka (порт 9092) — брокер сообщений
- PostgreSQL (порт 5432) — база данных
- Flink JobManager (порт 8081 — Web UI) — менеджер заданий
- Flink TaskManager — исполнитель заданий
- Kafka Producer — отправляет данные из CSV в Kafka

Подождите 30-60 секунд, пока все контейнеры запустятся.

### Шаг 3. Отправка данных в Kafka

```bash
docker-compose run --rm kafka-producer
```

Приложение прочитает все 10 CSV файлов (10000 записей) и отправит их в Kafka-топик petstore_sales в формате JSON

### Шаг 4. Запуск Flink джобы

```bash
docker-compose exec jobmanager flink run \
    -py /opt/flink/job/flink_streaming_job.py \
    -d
```

Джоба начнёт читать сообщения из Kafka, трансформировать данные в модель "звезда" и записывать в PostgreSQL

### Шаг 5. Проверка результатов

Количество фактов продаж
```bash
docker-compose exec postgres psql -U petstore_user -d petstore \
    -c "SELECT COUNT(*) FROM petstore_dw.fact_sales;"
```

Количество уникальных покупателей
```bash
docker-compose exec postgres psql -U petstore_user -d petstore \
    -c "SELECT COUNT(*) FROM petstore_dw.dim_customer;"
```

Количество уникальных товаров
```bash
docker-compose exec postgres psql -U petstore_user -d petstore \
    -c "SELECT COUNT(*) FROM petstore_dw.dim_product;"
```

Пример данных из фактов
```bash
docker-compose exec postgres psql -U petstore_user -d petstore \
    -c "SELECT * FROM petstore_dw.fact_sales LIMIT 5;"
```

Пример данных из измерений
```bash
docker-compose exec postgres psql -U petstore_user -d petstore \
    -c "SELECT * FROM petstore_dw.dim_customer LIMIT 5;"
```    


## Подключение через DBeaver:

Host: localhost
Port: 5432
Database: petstore
Username: petstore_user
Password: petstore_pass

## Flink Web UI

Откройте в браузере: http://localhost:8081

В веб-интерфейсе доступна информация:
- Статусе запущенных джоб
- Количестве обработанных записей
- Статусе Task Managers
- Логах выполнения

