#!/usr/bin/env python
import argparse
import csv
import json
import logging
import os
import sys
from datetime import datetime
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

# Настройка логирования
def setup_logging(log_dir="logs"):
    """
    Настройка логирования
    
    Args:
        log_dir (str): Директория для хранения логов
    """
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
        
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"data_loader_{timestamp}.log")
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    
    return logging.getLogger('data_loader')

logger = setup_logging()

class PostgreSQLConnection:
    """Класс для подключения к PostgreSQL"""
    
    def __init__(self, host, port, dbname, user, password):
        """
        Инициализация подключения к PostgreSQL
        
        Args:
            host (str): Хост базы данных
            port (int): Порт базы данных
            dbname (str): Имя базы данных
            user (str): Имя пользователя
            password (str): Пароль
        """
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
        """Установка соединения с базой данных"""
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
        """Закрытие соединения с базой данных"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("Соединение с PostgreSQL закрыто")
    
    def commit(self):
        """Фиксация изменений в базе данных"""
        self.conn.commit()
    
    def rollback(self):
        """Отмена изменений в базе данных"""
        self.conn.rollback()


class DataLoader:
    """Базовый класс для загрузки данных в PostgreSQL"""
    
    def __init__(self, db_connection):
        """
        Инициализация загрузчика данных
        
        Args:
            db_connection (PostgreSQLConnection): Подключение к PostgreSQL
        """
        self.db = db_connection
    
    def load_data_from_csv(self, csv_path, batch_size=1000):
        """
        Загрузка данных из CSV файла
        
        Args:
            csv_path (str): Путь к CSV файлу
            batch_size (int): Размер пакета для пакетной вставки
            
        Returns:
            list: Загруженные данные
        """
        try:
            logger.info(f"Загрузка данных из файла {csv_path}")
            
            if not os.path.isfile(csv_path):
                logger.error(f"Файл {csv_path} не найден")
                return []
            
            with open(csv_path, 'r', encoding='utf-8') as csvfile:
                csv_reader = csv.DictReader(csvfile)
                data = list(csv_reader)
            
            logger.info(f"Загружено {len(data)} записей из файла {csv_path}")
            return data
        except Exception as e:
            logger.error(f"Ошибка при загрузке данных из файла {csv_path}: {e}")
            return []
    
    def insert_import_record(self, source, records_count, status, error_message=None, file_name=None, additional_info=None):
        """
        Добавление записи о импорте данных
        
        Args:
            source (str): Источник данных
            records_count (int): Количество загруженных записей
            status (str): Статус загрузки
            error_message (str): Сообщение об ошибке
            file_name (str): Имя файла
            additional_info (dict): Дополнительная информация
        """
        try:
            logger.info(f"Добавление записи о импорте данных из источника {source}")
            
            query = """
                INSERT INTO public.data_imports 
                (source, records_count, status, error_message, file_name, additional_info) 
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            
            # Преобразование additional_info в JSON
            additional_info_json = json.dumps(additional_info) if additional_info else None
            
            self.db.cursor.execute(query, (source, records_count, status, error_message, file_name, additional_info_json))
            self.db.commit()
            
            logger.info("Запись о импорте данных добавлена")
        except Exception as e:
            logger.error(f"Ошибка при добавлении записи о импорте данных: {e}")
            self.db.rollback()


class GosreestrDataLoader(DataLoader):
    """Класс для загрузки данных из Госреестра"""
    
    def transform_data(self, raw_data):
        """
        Преобразование данных из формата CSV в формат для вставки в БД
        
        Args:
            raw_data (list): Исходные данные
            
        Returns:
            list: Преобразованные данные
        """
        transformed_data = []
        
        for row in raw_data:
            # Проверка наличия обязательных полей
            if 'flBin' not in row:
                logger.warning(f"Пропуск записи без БИН: {row}")
                continue
                
            # Преобразование данных
            transformed_row = {
                'bin': row.get('flBin', ''),
                'name_ru': row.get('flNameRu', ''),
                'opf': row.get('flOpf', ''),
                'oked_l0': row.get('flOkedL0', ''),
                'state_involvement': float(row.get('flStateInvolvement', 0)) if row.get('flStateInvolvement') else None,
                'status': row.get('flStatus', '')
            }
            
            transformed_data.append(transformed_row)
        
        logger.info(f"Преобразовано {len(transformed_data)} записей")
        return transformed_data
    
    def insert_data(self, data, batch_size=1000):
        """
        Вставка данных в базу данных
        
        Args:
            data (list): Данные для вставки
            batch_size (int): Размер пакета для пакетной вставки
            
        Returns:
            int: Количество вставленных записей
        """
        try:
            if not data:
                logger.warning("Нет данных для вставки")
                return 0
                
            logger.info(f"Вставка {len(data)} записей в таблицу gosreestr.objects")
            
            # Подготовка запроса на обновление или вставку (UPSERT)
            query = """
                INSERT INTO gosreestr.objects 
                (bin, name_ru, opf, oked_l0, state_involvement, status) 
                VALUES %s
                ON CONFLICT (bin) DO UPDATE SET 
                    name_ru = EXCLUDED.name_ru,
                    opf = EXCLUDED.opf,
                    oked_l0 = EXCLUDED.oked_l0,
                    state_involvement = EXCLUDED.state_involvement,
                    status = EXCLUDED.status,
                    updated_at = NOW()
                RETURNING id
            """
            
            # Преобразование данных в формат для execute_values
            values = [
                (
                    row['bin'],
                    row['name_ru'],
                    row['opf'],
                    row['oked_l0'],
                    row['state_involvement'],
                    row['status']
                )
                for row in data
            ]
            
            # Пакетная вставка
            count = 0
            for i in range(0, len(values), batch_size):
                batch = values[i:i + batch_size]
                result = execute_values(self.db.cursor, query, batch, fetch=True)
                count += len(result)
                self.db.commit()
                logger.info(f"Вставлено {len(result)} записей (пакет {i//batch_size + 1})")
            
            logger.info(f"Всего вставлено {count} записей в таблицу gosreestr.objects")
            return count
        except Exception as e:
            logger.error(f"Ошибка при вставке данных в таблицу gosreestr.objects: {e}")
            self.db.rollback()
            return 0
    
    def load(self, csv_path, batch_size=1000):
        """
        Загрузка данных из CSV файла в базу данных
        
        Args:
            csv_path (str): Путь к CSV файлу
            batch_size (int): Размер пакета для пакетной вставки
            
        Returns:
            bool: Результат загрузки
        """
        try:
            # Загрузка данных из CSV
            raw_data = self.load_data_from_csv(csv_path)
            if not raw_data:
                return False
            
            # Преобразование данных
            transformed_data = self.transform_data(raw_data)
            if not transformed_data:
                return False
            
            # Вставка данных в БД
            inserted_count = self.insert_data(transformed_data, batch_size)
            
            # Добавление записи о импорте
            self.insert_import_record(
                source='gosreestr',
                records_count=inserted_count,
                status='success' if inserted_count > 0 else 'error',
                file_name=os.path.basename(csv_path),
                additional_info={
                    'total_records': len(raw_data),
                    'transformed_records': len(transformed_data),
                    'inserted_records': inserted_count
                }
            )
            
            return inserted_count > 0
        except Exception as e:
            logger.error(f"Ошибка при загрузке данных Госреестра: {e}")
            self.insert_import_record(
                source='gosreestr',
                records_count=0,
                status='error',
                error_message=str(e),
                file_name=os.path.basename(csv_path)
            )
            return False


class AuctionDataLoader(DataLoader):
    """Класс для загрузки данных аукционных торгов"""
    
    def transform_auction_data(self, raw_data):
        """
        Преобразование данных аукционов из формата CSV в формат для вставки в БД
        
        Args:
            raw_data (list): Исходные данные
            
        Returns:
            tuple: (trades_data, objects_data) - данные о торгах и объектах
        """
        trades_data = []
        objects_data = []
        
        for row in raw_data:
            # Проверка наличия обязательных полей
            if 'AuctionId' not in row:
                logger.warning(f"Пропуск записи без AuctionId: {row}")
                continue
            
            # Преобразование данных о торгах
            auction_id = int(row.get('AuctionId'))
            
            trade_row = {
                'auction_id': auction_id,
                'auction_type': row.get('AuctionType', ''),
                'start_date': row.get('StartDate'),
                'publish_date': row.get('PublishDate'),
                'start_price': float(row.get('StartPrice', 0)) if row.get('StartPrice') else None,
                'min_price': float(row.get('MinPrice', 0)) if row.get('MinPrice') else None,
                'guarantee_payment_amount': float(row.get('GuaranteePaymentAmount', 0)) if row.get('GuaranteePaymentAmount') else None,
                'pay_period': row.get('PayPeriod', ''),
                'min_participants_count': int(row.get('MinParticipantsCount', 0)) if row.get('MinParticipantsCount') else None,
                'publish_note_ru': row.get('PublishNoteRu', ''),
                'publish_note_kz': row.get('PublishNoteKz', ''),
                'payments_recipient_info_ru': row.get('PaymentsRecipientInfoRu', ''),
                'payments_recipient_info_kz': row.get('PaymentsRecipientInfoKz', ''),
                'note_ru': row.get('NoteRu', ''),
                'note_kz': row.get('NoteKz', ''),
                'auction_status': row.get('AuctionStatus', ''),
                'win_price': float(row.get('WinPrice', 0)) if row.get('WinPrice') else None,
                'participants_count': int(row.get('ParticipantsCount', 0)) if row.get('ParticipantsCount') else None
            }
            
            trades_data.append(trade_row)
            
            # Извлекаем данные об объекте (поля, начинающиеся с "Object_")
            object_fields = {k[7:]: v for k, v in row.items() if k.startswith('Object_')}
            
            if object_fields:
                # Преобразуем метаданные в JSON, если они есть
                meta_data = object_fields.get('MetaData')
                if meta_data:
                    try:
                        if isinstance(meta_data, str):
                            meta_data = json.loads(meta_data)
                    except json.JSONDecodeError:
                        logger.warning(f"Невозможно декодировать метаданные как JSON: {meta_data}")
                        meta_data = {"raw_data": meta_data}
                
                object_row = {
                    'auction_id': auction_id,
                    'object_type': object_fields.get('ObjectType', ''),
                    'seller_xin': object_fields.get('SellerXin', ''),
                    'seller_info_ru': object_fields.get('SellerInfoRu', ''),
                    'seller_info_kz': object_fields.get('SellerInfoKz', ''),
                    'balanceholder_info_ru': object_fields.get('BalanceholderInfoRu', ''),
                    'balanceholder_info_kz': object_fields.get('BalanceholderInfoKz', ''),
                    'name_ru': object_fields.get('NameRu', ''),
                    'name_kz': object_fields.get('NameKz', ''),
                    'description_ru': object_fields.get('DescriptionRu', ''),
                    'description_kz': object_fields.get('DescriptionKz', ''),
                    'seller_adr_country': object_fields.get('SellerAdrCountry', ''),
                    'seller_adr_obl': object_fields.get('SellerAdrObl', ''),
                    'seller_adr_reg': object_fields.get('SellerAdrReg', ''),
                    'seller_adr_adr': object_fields.get('SellerAdrAdr', ''),
                    'seller_phone_ru': object_fields.get('SellerPhoneRu', ''),
                    'seller_phone_kz': object_fields.get('SellerPhoneKz', ''),
                    'object_adr_country': object_fields.get('ObjectAdrCountry', ''),
                    'object_adr_obl': object_fields.get('ObjectAdrObl', ''),
                    'object_adr_reg': object_fields.get('ObjectAdrReg', ''),
                    'object_adr_adr': object_fields.get('ObjectAdrAdr', ''),
                    'meta_data': meta_data
                }
                
                objects_data.append(object_row)
        
        logger.info(f"Преобразовано {len(trades_data)} записей о торгах и {len(objects_data)} записей об объектах")
        return trades_data, objects_data
    
    def insert_auction_trades(self, trades_data, batch_size=1000):
        """
        Вставка данных о торгах в базу данных
        
        Args:
            trades_data (list): Данные о торгах для вставки
            batch_size (int): Размер пакета для пакетной вставки
            
        Returns:
            int: Количество вставленных записей
        """
        try:
            if not trades_data:
                logger.warning("Нет данных о торгах для вставки")
                return 0
                
            logger.info(f"Вставка {len(trades_data)} записей в таблицу auction.trades")
            
            # Подготовка запроса на обновление или вставку (UPSERT)
            query = """
                INSERT INTO auction.trades 
                (auction_id, auction_type, start_date, publish_date, start_price, min_price, 
                guarantee_payment_amount, pay_period, min_participants_count, publish_note_ru, 
                publish_note_kz, payments_recipient_info_ru, payments_recipient_info_kz, 
                note_ru, note_kz, auction_status, win_price, participants_count) 
                VALUES %s
                ON CONFLICT (auction_id) DO UPDATE SET 
                    auction_type = EXCLUDED.auction_type,
                    start_date = EXCLUDED.start_date,
                    publish_date = EXCLUDED.publish_date,
                    start_price = EXCLUDED.start_price,
                    min_price = EXCLUDED.min_price,
                    guarantee_payment_amount = EXCLUDED.guarantee_payment_amount,
                    pay_period = EXCLUDED.pay_period,
                    min_participants_count = EXCLUDED.min_participants_count,
                    publish_note_ru = EXCLUDED.publish_note_ru,
                    publish_note_kz = EXCLUDED.publish_note_kz,
                    payments_recipient_info_ru = EXCLUDED.payments_recipient_info_ru,
                    payments_recipient_info_kz = EXCLUDED.payments_recipient_info_kz,
                    note_ru = EXCLUDED.note_ru,
                    note_kz = EXCLUDED.note_kz,
                    auction_status = EXCLUDED.auction_status,
                    win_price = EXCLUDED.win_price,
                    participants_count = EXCLUDED.participants_count,
                    updated_at = NOW()
                RETURNING id
            """
            
            # Преобразование данных в формат для execute_values
            values = [
                (
                    row['auction_id'],
                    row['auction_type'],
                    row['start_date'],
                    row['publish_date'],
                    row['start_price'],
                    row['min_price'],
                    row['guarantee_payment_amount'],
                    row['pay_period'],
                    row['min_participants_count'],
                    row['publish_note_ru'],
                    row['publish_note_kz'],
                    row['payments_recipient_info_ru'],
                    row['payments_recipient_info_kz'],
                    row['note_ru'],
                    row['note_kz'],
                    row['auction_status'],
                    row['win_price'],
                    row['participants_count']
                )
                for row in trades_data
            ]
            
            # Пакетная вставка
            count = 0
            for i in range(0, len(values), batch_size):
                batch = values[i:i + batch_size]
                result = execute_values(self.db.cursor, query, batch, fetch=True)
                count += len(result)
                self.db.commit()
                logger.info(f"Вставлено {len(result)} записей о торгах (пакет {i//batch_size + 1})")
            
            logger.info(f"Всего вставлено {count} записей в таблицу auction.trades")
            return count
        except Exception as e:
            logger.error(f"Ошибка при вставке данных в таблицу auction.trades: {e}")
            self.db.rollback()
            return 0
    
    def insert_auction_objects(self, objects_data, batch_size=1000):
        """
        Вставка данных об объектах аукционов в базу данных
        
        Args:
            objects_data (list): Данные об объектах для вставки
            batch_size (int): Размер пакета для пакетной вставки
            
        Returns:
            int: Количество вставленных записей
        """
        try:
            if not objects_data:
                logger.warning("Нет данных об объектах для вставки")
                return 0
                
            logger.info(f"Вставка {len(objects_data)} записей в таблицу auction.objects")
            
            # Подготовка запроса на обновление или вставку
            query = """
                INSERT INTO auction.objects 
                (auction_id, object_type, seller_xin, seller_info_ru, seller_info_kz, 
                balanceholder_info_ru, balanceholder_info_kz, name_ru, name_kz, 
                description_ru, description_kz, seller_adr_country, seller_adr_obl, 
                seller_adr_reg, seller_adr_adr, seller_phone_ru, seller_phone_kz, 
                object_adr_country, object_adr_obl, object_adr_reg, object_adr_adr, meta_data) 
                VALUES %s
                ON CONFLICT (auction_id) DO UPDATE SET 
                    object_type = EXCLUDED.object_type,
                    seller_xin = EXCLUDED.seller_xin,
                    seller_info_ru = EXCLUDED.seller_info_ru,
                    seller_info_kz = EXCLUDED.seller_info_kz,
                    balanceholder_info_ru = EXCLUDED.balanceholder_info_ru,
                    balanceholder_info_kz = EXCLUDED.balanceholder_info_kz,
                    name_ru = EXCLUDED.name_ru,
                    name_kz = EXCLUDED.name_kz,
                    description_ru = EXCLUDED.description_ru,
                    description_kz = EXCLUDED.description_kz,
                    seller_adr_country = EXCLUDED.seller_adr_country,
                    seller_adr_obl = EXCLUDED.seller_adr_obl,
                    seller_adr_reg = EXCLUDED.seller_adr_reg,
                    seller_adr_adr = EXCLUDED.seller_adr_adr,
                    seller_phone_ru = EXCLUDED.seller_phone_ru,
                    seller_phone_kz = EXCLUDED.seller_phone_kz,
                    object_adr_country = EXCLUDED.object_adr_country,
                    object_adr_obl = EXCLUDED.object_adr_obl,
                    object_adr_reg = EXCLUDED.object_adr_reg,
                    object_adr_adr = EXCLUDED.object_adr_adr,
                    meta_data = EXCLUDED.meta_data,
                    updated_at = NOW()
                RETURNING id
            """
            
            # Преобразование данных в формат для execute_values
            values = []
            for row in objects_data:
                # Преобразование meta_data в JSON, если необходимо
                meta_data = row.get('meta_data')
                if meta_data and not isinstance(meta_data, str):
                    meta_data = json.dumps(meta_data)
                
                values.append((
                    row['auction_id'],
                    row['object_type'],
                    row['seller_xin'],
                    row['seller_info_ru'],
                    row['seller_info_kz'],
                    row['balanceholder_info_ru'],
                    row['balanceholder_info_kz'],
                    row['name_ru'],
                    row['name_kz'],
                    row['description_ru'],
                    row['description_kz'],
                    row['seller_adr_country'],
                    row['seller_adr_obl'],
                    row['seller_adr_reg'],
                    row['seller_adr_adr'],
                    row['seller_phone_ru'],
                    row['seller_phone_kz'],
                    row['object_adr_country'],
                    row['object_adr_obl'],
                    row['object_adr_reg'],
                    row['object_adr_adr'],
                    meta_data
                ))
            
            # Пакетная вставка
            count = 0
            for i in range(0, len(values), batch_size):
                batch = values[i:i + batch_size]
                result = execute_values(self.db.cursor, query, batch, fetch=True)
                count += len(result)
                self.db.commit()
                logger.info(f"Вставлено {len(result)} записей об объектах (пакет {i//batch_size + 1})")
            
            logger.info(f"Всего вставлено {count} записей в таблицу auction.objects")
            return count
        except Exception as e:
            logger.error(f"Ошибка при вставке данных в таблицу auction.objects: {e}")
            self.db.rollback()
            return 0
    
    def load(self, csv_path, batch_size=1000):
        """
        Загрузка данных аукционов из CSV файла в базу данных
        
        Args:
            csv_path (str): Путь к CSV файлу
            batch_size (int): Размер пакета для пакетной вставки
            
        Returns:
            bool: Результат загрузки
        """
        try:
            # Загрузка данных из CSV
            raw_data = self.load_data_from_csv(csv_path)
            if not raw_data:
                return False
            
            # Преобразование данных
            trades_data, objects_data = self.transform_auction_data(raw_data)
            if not trades_data:
                return False
            
            # Вставка данных о торгах в БД
            trades_count = self.insert_auction_trades(trades_data, batch_size)
            
            # Вставка данных об объектах в БД
            objects_count = self.insert_auction_objects(objects_data, batch_size)
            
            # Добавление записи о импорте
            self.insert_import_record(
                source='auction',
                records_count=trades_count,
                status='success' if trades_count > 0 else 'error',
                file_name=os.path.basename(csv_path),
                additional_info={
                    'total_records': len(raw_data),
                    'trades_records': len(trades_data),
                    'objects_records': len(objects_data),
                    'inserted_trades': trades_count,
                    'inserted_objects': objects_count
                }
            )
            
            return trades_count > 0
        except Exception as e:
            logger.error(f"Ошибка при загрузке данных аукционов: {e}")
            self.insert_import_record(
                source='auction',
                records_count=0,
                status='error',
                error_message=str(e),
                file_name=os.path.basename(csv_path)
            )
            return False


def main():
    """Основная функция для загрузки данных"""
    parser = argparse.ArgumentParser(description="Загрузка данных из CSV в PostgreSQL")
    parser.add_argument("--source", choices=["gosreestr", "auction"], required=True, help="Источник данных")