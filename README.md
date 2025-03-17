# Система сбора и обработки данных Госреестра и электронных торгов

Этот проект представляет собой комплексное решение для сбора, обработки и хранения данных из Госреестра и системы электронных торгов с использованием Apache Airflow, PostgreSQL и Python.

## Компоненты системы

1. **Парсеры API**:
   - Парсер для получения списка активных объектов Госреестра
   - Парсер для получения списка электронных торгов

2. **База данных PostgreSQL**:
   - Схема `gosreestr` для хранения данных Госреестра
   - Схема `auction` для хранения данных электронных торгов

3. **Система загрузки данных**:
   - Скрипт для загрузки данных из CSV в PostgreSQL
   - Скрипт для загрузки данных в data.opengov.kz (подготовлен для будущего использования)

4. **Оркестрация процессов с Apache Airflow**:
   - DAG для автоматического запуска парсеров и загрузки данных

## Структура проекта

```
.
├── docker-compose.yml          # Конфигурация Docker Compose
├── dags/                       # Директория для Airflow DAG
│   └── gosreestr_auction_dag.py  # DAG для сбора и загрузки данных
├── scripts/                    # Скрипты для обработки данных
│   ├── gosreestr_parser.py     # Парсер API Госреестра
│   ├── auction_trades_parser.py  # Парсер API электронных торгов
│   ├── data_loader.py          # Скрипт для загрузки данных в БД
│   └── opengov_loader.py       # Скрипт для загрузки данных в data.opengov.kz
├── postgresql/                 # SQL скрипты для PostgreSQL
│   └── init-gosreestr-db.sql   # Скрипт инициализации БД
├── data/                       # Директория для хранения временных данных
└── logs/                       # Директория для логов
```

## Требования

- Docker и Docker Compose
- Python 3.8+
- PostgreSQL 13+
- Доступ к API Госреестра и электронных торгов

## Установка и запуск


```

### Запуск системы с Docker Compose

```bash
docker-compose up -d
```

### Проверка работы Airflow

Откройте в браузере: http://localhost:8080

Логин и пароль по умолчанию: airflow / airflow

## Использование парсеров напрямую

### Парсер Госреестра

```bash
python scripts/gosreestr_parser.py
```

### Парсер электронных торгов

```bash
python scripts/auction_trades_parser.py --status=AcceptingApplications
```

### Загрузка данных в БД

```bash
python scripts/data_loader.py --source=gosreestr --file=data/gosreestr_objects.csv
python scripts/data_loader.py --source=auction --file=data/auction_trades.csv
```

## Настройка AIRFLOW DAG

DAG `gosreestr_and_auction_etl` настроен на ежедневный запуск в 01:00 и выполняет следующие задачи:

1. Запуск парсера Госреестра
2. Загрузка данных Госреестра в БД
3. Запуск парсера электронных торгов
4. Загрузка данных электронных торгов в БД
5. (Подготовлено на будущее) Загрузка данных в data.opengov.kz

## Подготовка к будущей интеграции с data.opengov.kz

Скрипт `opengov_loader.py` подготовлен для будущей интеграции с порталом data.opengov.kz. Когда портал будет готов, вам потребуется:

1. Получить API ключ для доступа к порталу
2. Настроить идентификатор набора данных
3. Активировать соответствующую задачу в Airflow DAG

## Обслуживание и мониторинг

### Логи

Логи всех компонентов сохраняются в директории `logs/` и доступны через веб-интерфейс Airflow.

### Мониторинг выполнения задач

Мониторинг выполнения задач доступен через веб-интерфейс Airflow.

### Проверка данных в БД

Для проверки данных в БД можно использовать любой SQL клиент, подключившись к PostgreSQL на порту 5433:

```
Host: localhost
Port: 5433
Database: gosreestr_db
User: data_manager
Password: strong_password_here
```

## Устранение неполадок

### Проблемы с доступом к API

Проверьте доступность API с помощью curl или Postman:

```bash
curl https://gr5.e-qazyna.kz/p/ru/api/v1/gr-objects
curl https://e-auction.e-qazyna.kz/p/ru/api/v1/auction-trades?PageNumber=1&Limit=15&Status=AcceptingApplications
```

### Проблемы с базой данных

Проверьте состояние базы данных:

```bash
docker-compose exec gosreestr-db psql -U data_manager -d gosreestr_db -c "\dt"
```
