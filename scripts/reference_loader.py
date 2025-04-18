#!/usr/bin/env python
import json
import argparse
import logging
import os
import sys
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from datetime import datetime

# Настройка логирования
def setup_logging(log_dir="logs"):
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"reference_loader_{timestamp}.log")

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )

    return logging.getLogger('reference_loader')

logger = setup_logging()

class PostgreSQLConnection:
    def __init__(self, host, port, dbname, user, password):
        self.connection_params = {
            'host': host,
            'port': port,
            'dbname': dbname,
            'user': user,
            'password': password
        }
        self.conn = None
        self.cursor = None

    def connect(self):
        try:
            logger.info("Подключение к PostgreSQL")
            self.conn = psycopg2.connect(**self.connection_params)
            self.cursor = self.conn.cursor()
            logger.info("Подключение к PostgreSQL установлено")
            return True
        except Exception as e:
            logger.error(f"Ошибка подключения к PostgreSQL: {e}")
            return False

    def disconnect(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("Соединение с PostgreSQL закрыто")

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()


class ReferenceLoader:
    def __init__(self, db_connection):
        self.db = db_connection
        self.reference_mappings = {
            'опф.json': {
                'table': 'reference.organization_types',
                'fields': ['code', 'name_ru'],
                'processor': self.process_simple_reference
            },
            'отрасль.json': {
                'table': 'reference.industry_sectors',
                'fields': ['code', 'name_ru'],
                'processor': self.process_simple_reference
            },
            'статус.json': {
                'table': 'reference.organization_statuses',
                'fields': ['code', 'name_ru'],
                'processor': self.process_simple_reference
            },
            'тип_торгов.json': {
                'table': 'reference.auction_types',
                'fields': ['code', 'name_ru'],
                'processor': self.process_hierarchical_reference
            },
            'статус_торгов.json': {
                'table': 'reference.auction_statuses',
                'fields': ['code', 'name_ru'],
                'processor': self.process_simple_reference
            },
            'тип_обьекта.json': {
                'table': 'reference.object_types',
                'fields': ['code', 'name_ru'],
                'processor': self.process_simple_reference
            },
            'периодичность_оплаты.json': {
                'table': 'reference.payment_periods',
                'fields': ['code', 'name_ru'],
                'processor': self.process_simple_reference
            },
            'КАТО.json': {
                'table': 'reference.regions',  # Основная таблица, другие будут заполнены в процессе
                'fields': ['code', 'name_ru'],
                'processor': self.process_kato_reference
            }
        }

    def load_json_file(self, file_path):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Ошибка при чтении файла {file_path}: {e}")
            return None

    def process_simple_reference(self, data, table, fields):
        """
        Обработка простого справочника без иерархии
        """
        values = []
        for item in data:
            values.append((
                item.get('Value', ''),
                item.get('Text', '')
            ))

        return values

    def process_hierarchical_reference(self, data, table, fields, parent_id=None):
        """
        Обработка иерархического справочника с вложенными элементами
        """
        values = []

        for item in data:
            values.append((
                item.get('Value', ''),
                item.get('Text', '')
            ))

            # Обрабатываем вложенные элементы
            if 'Items' in item and item['Items']:
                values.extend(self.process_hierarchical_reference(item['Items'], table, fields))

        return values

    def process_kato_reference(self, data, table, fields):
        """
        Обработка справочника КАТО с созданием стран, регионов и районов
        """
        # Сначала добавляем страну
        country_values = []
        region_values = []
        district_values = []

        for country in data:
            country_values.append((
                country.get('Value', ''),
                country.get('Text', '')
            ))

            # Затем регионы
            for region in country.get('Items', []):
                region_values.append((
                    region.get('Value', ''),
                    region.get('Text', ''),
                    country.get('Value', '')  # country_code для внешнего ключа
                ))

                # И наконец районы
                for district in region.get('Items', []):
                    district_values.append((
                        district.get('Value', ''),
                        district.get('Text', ''),
                        region.get('Value', '')  # region_code для внешнего ключа
                    ))

        # Вставляем страны
        self.insert_countries(country_values)

        # Вставляем регионы
        self.insert_regions(region_values)

        # Вставляем районы
        self.insert_districts(district_values)

        return []  # Возвращаем пустой список, так как данные уже вставлены

    def insert_countries(self, values):
        """
        Вставка стран
        """
        if not values:
            return

        logger.info(f"Вставка {len(values)} стран в таблицу reference.countries")

        query = """
            INSERT INTO reference.countries 
            (code, name_ru) 
            VALUES %s
            ON CONFLICT (code) DO UPDATE SET 
                name_ru = EXCLUDED.name_ru
            RETURNING id
        """

        execute_values(self.db.cursor, query, values)
        self.db.commit()

    def insert_regions(self, values):
        """
        Вставка регионов
        """
        if not values:
            return

        logger.info(f"Вставка {len(values)} регионов в таблицу reference.regions")

        processed_values = []
        for code, name, country_code in values:
            # Получаем ID страны по коду
            self.db.cursor.execute("SELECT id FROM reference.countries WHERE code = %s", (country_code,))
            result = self.db.cursor.fetchone()
            if result:
                country_id = result[0]
                processed_values.append((code, name, country_id))

        query = """
            INSERT INTO reference.regions 
            (code, name_ru, country_id) 
            VALUES %s
            ON CONFLICT (code) DO UPDATE SET 
                name_ru = EXCLUDED.name_ru,
                country_id = EXCLUDED.country_id
            RETURNING id
        """

        execute_values(self.db.cursor, query, processed_values)
        self.db.commit()

    def insert_districts(self, values):
        """
        Вставка районов
        """
        if not values:
            return

        logger.info(f"Вставка {len(values)} районов в таблицу reference.districts")

        processed_values = []
        for code, name, region_code in values:
            # Получаем ID региона по коду
            self.db.cursor.execute("SELECT id FROM reference.regions WHERE code = %s", (region_code,))
            result = self.db.cursor.fetchone()
            if result:
                region_id = result[0]
                processed_values.append((code, name, region_id))

        query = """
            INSERT INTO reference.districts 
            (code, name_ru, region_id) 
            VALUES %s
            ON CONFLICT (code) DO UPDATE SET 
                name_ru = EXCLUDED.name_ru,
                region_id = EXCLUDED.region_id
            RETURNING id
        """

        execute_values(self.db.cursor, query, processed_values)
        self.db.commit()

    def insert_reference_data(self, table, fields, values, batch_size=1000):
        """
        Вставка данных справочника
        """
        if not values:
            return 0

        logger.info(f"Вставка {len(values)} записей в таблицу {table}")

        fields_str = ', '.join(fields)
        placeholders = ', '.join(['%s'] * len(fields))

        query = f"""
            INSERT INTO {table} 
            ({fields_str}) 
            VALUES %s
            ON CONFLICT (code) DO UPDATE SET 
                name_ru = EXCLUDED.name_ru
            RETURNING id
        """

        count = 0
        for i in range(0, len(values), batch_size):
            batch = values[i:i + batch_size]
            try:
                result = execute_values(self.db.cursor, query, batch, fetch=True)
                count += len(result)
                self.db.commit()
                logger.info(f"Вставлено {len(result)} записей в {table} (пакет {i//batch_size + 1})")
            except Exception as e:
                logger.error(f"Ошибка при вставке данных в {table}: {e}")
                self.db.rollback()

        return count

    def load_reference(self, json_file):
        """
        Загрузка справочника из JSON-файла
        """
        file_name = os.path.basename(json_file)

        if file_name not in self.reference_mappings:
            logger.error(f"Неизвестный файл справочника: {file_name}")
            return False

        mapping = self.reference_mappings[file_name]
        data = self.load_json_file(json_file)

        if not data:
            return False

        logger.info(f"Обработка справочника {file_name} для таблицы {mapping['table']}")

        # Используем соответствующий процессор для данного справочника
        values = mapping['processor'](data, mapping['table'], mapping['fields'])

        # Если процессор не вернул значения (например, для КАТО, где вставка происходит внутри процессора)
        if not values:
            return True

        # Вставка данных
        count = self.insert_reference_data(mapping['table'], mapping['fields'], values)

        return count > 0

    def load_all_references(self, references_dir):
        """
        Загрузка всех справочников из директории
        """
        success_count = 0
        total_count = 0

        for file_name in self.reference_mappings.keys():
            file_path = os.path.join(references_dir, file_name)

            if os.path.exists(file_path):
                total_count += 1
                if self.load_reference(file_path):
                    success_count += 1
            else:
                logger.warning(f"Файл справочника не найден: {file_path}")

        logger.info(f"Загружено {success_count} из {total_count} справочников")
        return success_count == total_count


def main():
    parser = argparse.ArgumentParser(description="Загрузка справочников из JSON в PostgreSQL")
    parser.add_argument("--dir", required=True, help="Директория с файлами справочников")
    parser.add_argument("--file", help="Конкретный файл справочника для загрузки")
    parser.add_argument("--host", default="localhost", help="Хост PostgreSQL")
    parser.add_argument("--port", type=int, default=5432, help="Порт PostgreSQL")
    parser.add_argument("--dbname", default="gosreestr_db", help="Имя базы данных")
    parser.add_argument("--user", default="data_manager", help="Имя пользователя")
    parser.add_argument("--password", help="Пароль пользователя")

    args = parser.parse_args()

    # Проверка наличия директории справочников
    if not os.path.isdir(args.dir):
        logger.error(f"Директория {args.dir} не найдена")
        sys.exit(1)

    # Создание подключения к БД
    db_connection = PostgreSQLConnection(
        host=args.host,
        port=args.port,
        dbname=args.dbname,
        user=args.user,
        password=args.password
    )

    if not db_connection.connect():
        logger.error("Не удалось подключиться к PostgreSQL")
        sys.exit(1)

    try:
        # Создание загрузчика справочников
        loader = ReferenceLoader(db_connection)

        # Загрузка справочников
        if args.file:
            file_path = os.path.join(args.dir, args.file)
            if not os.path.isfile(file_path):
                logger.error(f"Файл {file_path} не найден")
                sys.exit(1)

            result = loader.load_reference(file_path)
            if result:
                logger.info(f"Справочник {args.file} успешно загружен")
            else:
                logger.error(f"Не удалось загрузить справочник {args.file}")
                sys.exit(1)
        else:
            result = loader.load_all_references(args.dir)
            if result:
                logger.info("Все справочники успешно загружены")
            else:
                logger.error("Не удалось загрузить все справочники")
                sys.exit(1)
    finally:
        # Закрытие соединения с БД
        db_connection.disconnect()


if __name__ == "__main__":
    main()