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


logger = logging.getLogger('data_loader')


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

    def load_data_from_csv(self, csv_path):
        """
        Загрузка данных из CSV файла

        Args:
            csv_path (str): Путь к CSV файлу

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

    def insert_import_record(self, source, records_count, status, error_message=None, file_name=None,
                             additional_info=None):
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

            self.db.cursor.execute(query,
                                   (source, records_count, status, error_message, file_name, additional_info_json))
            self.db.commit()

            logger.info("Запись о импорте данных добавлена")
        except Exception as e:
            logger.error(f"Ошибка при добавлении записи о импорте данных: {e}")
            self.db.rollback()


class GosreestrNormalizedLoader(DataLoader):
    """Класс для загрузки данных Госреестра в нормализованную БД"""

    def ensure_reference_data(self, row):
        """
        Проверка наличия и добавление справочных данных

        Args:
            row (dict): Строка данных

        Returns:
            dict: Словарь с ID справочных данных
        """
        ref_ids = {}

        # 1. Обработка ОПФ
        if row.get('flOpf'):
            self.db.cursor.execute(
                "INSERT INTO reference.organization_types (code, name_ru) VALUES (%s, %s) \
                 ON CONFLICT (code) DO NOTHING RETURNING id",
                (row.get('flOpf'), row.get('flOpf'))
            )
            result = self.db.cursor.fetchone()
            if result:
                ref_ids['organization_type_id'] = result[0]
            else:
                self.db.cursor.execute(
                    "SELECT id FROM reference.organization_types WHERE code = %s",
                    (row.get('flOpf'),)
                )
                result = self.db.cursor.fetchone()
                if result:
                    ref_ids['organization_type_id'] = result[0]

        # 2. Обработка отрасли (сектор)
        if row.get('flOkedL0'):
            sector_name = {
                'O': 'Государственное управление',
                'P': 'Образование',
                'R': 'Искусство, развлечения и отдых',
                'K': 'Финансовая и страховая деятельность'
            }.get(row.get('flOkedL0'), row.get('flOkedL0'))

            self.db.cursor.execute(
                "INSERT INTO reference.industry_sectors (code, name_ru) VALUES (%s, %s) \
                 ON CONFLICT (code) DO NOTHING RETURNING id",
                (row.get('flOkedL0'), sector_name)
            )
            result = self.db.cursor.fetchone()
            if result:
                ref_ids['industry_sector_id'] = result[0]
            else:
                self.db.cursor.execute(
                    "SELECT id FROM reference.industry_sectors WHERE code = %s",
                    (row.get('flOkedL0'),)
                )
                result = self.db.cursor.fetchone()
                if result:
                    ref_ids['industry_sector_id'] = result[0]

        # 3. Обработка вида деятельности (ОКЭД)
        if row.get('flOkedL3') and 'industry_sector_id' in ref_ids:
            activity_name = {
                '8423': 'Деятельность в области юстиции и правосудия',
                '8411': 'Деятельность органов государственного управления',
                '8412': 'Регулирование деятельности учреждений',
                '8413': 'Регулирование и содействие в коммерческой деятельности',
                '9312': 'Деятельность спортивных клубов',
                '8559': 'Прочие виды образования',
                '6420': 'Деятельность холдинговых компаний'
            }.get(row.get('flOkedL3'), row.get('flOkedL3'))

            self.db.cursor.execute(
                "INSERT INTO reference.industry_activities (code, name_ru, sector_id) VALUES (%s, %s, %s) \
                 ON CONFLICT (code) DO NOTHING RETURNING id",
                (row.get('flOkedL3'), activity_name, ref_ids['industry_sector_id'])
            )
            result = self.db.cursor.fetchone()
            if result:
                ref_ids['industry_activity_id'] = result[0]
            else:
                self.db.cursor.execute(
                    "SELECT id FROM reference.industry_activities WHERE code = %s",
                    (row.get('flOkedL3'),)
                )
                result = self.db.cursor.fetchone()
                if result:
                    ref_ids['industry_activity_id'] = result[0]

        # 4. Обработка KFS уровень 0
        if row.get('flKfsL0'):
            kfs_name = {
                '2': 'Государственная собственность',
                '4': 'Частная собственность'
            }.get(row.get('flKfsL0'), f"Код KFS {row.get('flKfsL0')}")

            self.db.cursor.execute(
                "INSERT INTO reference.kfs_levels (level, code, name_ru) VALUES (%s, %s, %s) \
                 ON CONFLICT (code) DO NOTHING RETURNING id",
                (0, row.get('flKfsL0'), kfs_name)
            )
            result = self.db.cursor.fetchone()
            if result:
                ref_ids['kfs_l0_id'] = result[0]
            else:
                self.db.cursor.execute(
                    "SELECT id FROM reference.kfs_levels WHERE code = %s",
                    (row.get('flKfsL0'),)
                )
                result = self.db.cursor.fetchone()
                if result:
                    ref_ids['kfs_l0_id'] = result[0]

        # 5. Обработка KFS уровень 1
        if row.get('flKfsL1') and 'kfs_l0_id' in ref_ids:
            kfs_name = {
                '214': 'Республиканская собственность',
                '242': 'Судебная система',
                '209': 'Смешанная собственность',
                '441': 'Частная организация'
            }.get(row.get('flKfsL1'), f"Код KFS {row.get('flKfsL1')}")

            self.db.cursor.execute(
                "INSERT INTO reference.kfs_levels (level, code, name_ru, parent_id) VALUES (%s, %s, %s, %s) \
                 ON CONFLICT (code) DO NOTHING RETURNING id",
                (1, row.get('flKfsL1'), kfs_name, ref_ids['kfs_l0_id'])
            )
            result = self.db.cursor.fetchone()
            if result:
                ref_ids['kfs_l1_id'] = result[0]
            else:
                self.db.cursor.execute(
                    "SELECT id FROM reference.kfs_levels WHERE code = %s",
                    (row.get('flKfsL1'),)
                )
                result = self.db.cursor.fetchone()
                if result:
                    ref_ids['kfs_l1_id'] = result[0]

        # 6. Обработка KFS уровень 2
        if row.get('flKfsL2') and 'kfs_l1_id' in ref_ids:
            kfs_name = {
                '214001': 'Центральные государственные органы',
                '242001': 'Органы судебной системы',
                '209001': 'Смешанная государственно-частная собственность',
                '441118': 'Частные организации'
            }.get(row.get('flKfsL2'), f"Код KFS {row.get('flKfsL2')}")

            self.db.cursor.execute(
                "INSERT INTO reference.kfs_levels (level, code, name_ru, parent_id) VALUES (%s, %s, %s, %s) \
                 ON CONFLICT (code) DO NOTHING RETURNING id",
                (2, row.get('flKfsL2'), kfs_name, ref_ids['kfs_l1_id'])
            )
            result = self.db.cursor.fetchone()
            if result:
                ref_ids['kfs_l2_id'] = result[0]
            else:
                self.db.cursor.execute(
                    "SELECT id FROM reference.kfs_levels WHERE code = %s",
                    (row.get('flKfsL2'),)
                )
                result = self.db.cursor.fetchone()
                if result:
                    ref_ids['kfs_l2_id'] = result[0]

        # 7. Обработка статуса
        if row.get('flStatus'):
            status_name = {
                'ACT': 'Активный'
            }.get(row.get('flStatus'), row.get('flStatus'))

            self.db.cursor.execute(
                "INSERT INTO reference.organization_statuses (code, name_ru) VALUES (%s, %s) \
                 ON CONFLICT (code) DO NOTHING RETURNING id",
                (row.get('flStatus'), status_name)
            )
            result = self.db.cursor.fetchone()
            if result:
                ref_ids['status_id'] = result[0]
            else:
                self.db.cursor.execute(
                    "SELECT id FROM reference.organization_statuses WHERE code = %s",
                    (row.get('flStatus'),)
                )
                result = self.db.cursor.fetchone()
                if result:
                    ref_ids['status_id'] = result[0]

        return ref_ids

    def insert_organization(self, row, ref_ids):
        """
        Вставка организации

        Args:
            row (dict): Строка данных
            ref_ids (dict): Словарь с ID справочных данных

        Returns:
            int: ID вставленной организации или None
        """
        try:
            if not row.get('flBin') or not row.get('flBin').strip():
                logger.warning(f"Пропуск записи без БИН: {row}")
                return None

            # Вставка организации
            query = """
                INSERT INTO gosreestr.organizations (
                    bin, name_ru, organization_type_id, industry_sector_id, industry_activity_id, 
                    kfs_l0_id, kfs_l1_id, kfs_l2_id, state_involvement, status_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (bin) DO UPDATE SET
                    name_ru = EXCLUDED.name_ru,
                    organization_type_id = EXCLUDED.organization_type_id,
                    industry_sector_id = EXCLUDED.industry_sector_id,
                    industry_activity_id = EXCLUDED.industry_activity_id,
                    kfs_l0_id = EXCLUDED.kfs_l0_id,
                    kfs_l1_id = EXCLUDED.kfs_l1_id,
                    kfs_l2_id = EXCLUDED.kfs_l2_id,
                    state_involvement = EXCLUDED.state_involvement,
                    status_id = EXCLUDED.status_id,
                    updated_at = NOW()
                RETURNING id
            """

            self.db.cursor.execute(query, (
                row.get('flBin'),
                row.get('flNameRu'),
                ref_ids.get('organization_type_id'),
                ref_ids.get('industry_sector_id'),
                ref_ids.get('industry_activity_id'),
                ref_ids.get('kfs_l0_id'),
                ref_ids.get('kfs_l1_id'),
                ref_ids.get('kfs_l2_id'),
                float(row.get('flStateInvolvement', 0)) if row.get('flStateInvolvement') else None,
                ref_ids.get('status_id')
            ))

            result = self.db.cursor.fetchone()
            if result:
                return result[0]
            return None
        except Exception as e:
            logger.error(f"Ошибка при вставке организации: {e}")
            self.db.rollback()
            return None

    def update_organization_relations(self):
        """
        Обновление связей между организациями (owner и ogu)
        """
        try:
            # Обновление связи owner
            self.db.cursor.execute("""
                UPDATE gosreestr.organizations o
                SET owner_id = owner.id
                FROM gosreestr.organizations owner
                WHERE o.owner_id IS NULL
                  AND o.bin IN (
                      SELECT DISTINCT original.flBin
                      FROM gosreestr.objects original
                      WHERE original.flOwnerBin IS NOT NULL AND original.flOwnerBin != ''
                  )
                  AND owner.bin = (
                      SELECT original.flOwnerBin
                      FROM gosreestr.objects original
                      WHERE original.flBin = o.bin
                      LIMIT 1
                  )
            """)

            # Обновление связи ogu
            self.db.cursor.execute("""
                UPDATE gosreestr.organizations o
                SET ogu_id = ogu.id
                FROM gosreestr.organizations ogu
                WHERE o.ogu_id IS NULL
                  AND o.bin IN (
                      SELECT DISTINCT original.flBin
                      FROM gosreestr.objects original
                      WHERE original.flOguBin IS NOT NULL AND original.flOguBin != ''
                  )
                  AND ogu.bin = (
                      SELECT original.flOguBin
                      FROM gosreestr.objects original
                      WHERE original.flBin = o.bin
                      LIMIT 1
                  )
            """)

            self.db.commit()
            logger.info("Связи между организациями обновлены")
        except Exception as e:
            logger.error(f"Ошибка при обновлении связей между организациями: {e}")
            self.db.rollback()

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

            # Вставка данных в БД
            inserted_count = 0
            for row in raw_data:
                # Проверка наличия BIN
                if not row.get('flBin') or not row.get('flBin').strip():
                    logger.warning(f"Пропуск записи без БИН: {row}")
                    continue

                # Обеспечение наличия справочных данных
                ref_ids = self.ensure_reference_data(row)

                # Вставка организации
                org_id = self.insert_organization(row, ref_ids)
                if org_id:
                    inserted_count += 1

                # Фиксация изменений каждые batch_size записей
                if inserted_count % batch_size == 0:
                    self.db.commit()
                    logger.info(f"Вставлено {inserted_count} записей")

            # Фиксация оставшихся изменений
            self.db.commit()

            # Обновление связей между организациями
            self.update_organization_relations()

            # Добавление записи о импорте
            self.insert_import_record(
                source='gosreestr',
                records_count=inserted_count,
                status='success' if inserted_count > 0 else 'error',
                file_name=os.path.basename(csv_path),
                additional_info={
                    'total_records': len(raw_data),
                    'inserted_records': inserted_count
                }
            )

            logger.info(f"Всего вставлено {inserted_count} записей организаций")
            return inserted_count > 0
        except Exception as e:
            logger.error(f"Ошибка при загрузке данных Госреестра: {e}")
            self.db.rollback()
            self.insert_import_record(
                source='gosreestr',
                records_count=0,
                status='error',
                error_message=str(e),
                file_name=os.path.basename(csv_path)
            )
            return False


class AuctionNormalizedLoader(DataLoader):
    """Класс для загрузки данных аукционных торгов в нормализованную БД"""

    def ensure_reference_data(self, row):
        """
        Проверка наличия и добавление справочных данных

        Args:
            row (dict): Строка данных

        Returns:
            dict: Словарь с ID справочных данных
        """
        ref_ids = {}

        # 1. Обработка типа аукциона
        if row.get('AuctionType'):
            self.db.cursor.execute(
                "INSERT INTO reference.auction_types (code, name_ru) VALUES (%s, %s) \
                 ON CONFLICT (code) DO NOTHING RETURNING id",
                (row.get('AuctionType'), row.get('AuctionType'))
            )
            result = self.db.cursor.fetchone()
            if result:
                ref_ids['auction_type_id'] = result[0]
            else:
                self.db.cursor.execute(
                    "SELECT id FROM reference.auction_types WHERE code = %s",
                    (row.get('AuctionType'),)
                )
                result = self.db.cursor.fetchone()
                if result:
                    ref_ids['auction_type_id'] = result[0]

        # 2. Обработка статуса аукциона
        if row.get('AuctionStatus'):
            status_name = {
                'AcceptingApplications': 'Прием заявок'
            }.get(row.get('AuctionStatus'), row.get('AuctionStatus'))

            self.db.cursor.execute(
                "INSERT INTO reference.auction_statuses (code, name_ru) VALUES (%s, %s) \
                 ON CONFLICT (code) DO NOTHING RETURNING id",
                (row.get('AuctionStatus'), status_name)
            )
            result = self.db.cursor.fetchone()
            if result:
                ref_ids['auction_status_id'] = result[0]
            else:
                self.db.cursor.execute(
                    "SELECT id FROM reference.auction_statuses WHERE code = %s",
                    (row.get('AuctionStatus'),)
                )
                result = self.db.cursor.fetchone()
                if result:
                    ref_ids['auction_status_id'] = result[0]

        # 3. Обработка периода оплаты
        if row.get('PayPeriod'):
            self.db.cursor.execute(
                "INSERT INTO reference.payment_periods (code, name_ru) VALUES (%s, %s) \
                 ON CONFLICT (code) DO NOTHING RETURNING id",
                (row.get('PayPeriod'), row.get('PayPeriod'))
            )
            result = self.db.cursor.fetchone()
            if result:
                ref_ids['payment_period_id'] = result[0]
            else:
                self.db.cursor.execute(
                    "SELECT id FROM reference.payment_periods WHERE code = %s",
                    (row.get('PayPeriod'),)
                )
                result = self.db.cursor.fetchone()
                if result:
                    ref_ids['payment_period_id'] = result[0]

        # 4. Обработка типа объекта аукциона
        if row.get('Object_ObjectType'):
            self.db.cursor.execute(
                "INSERT INTO reference.object_types (code, name_ru) VALUES (%s, %s) \
                 ON CONFLICT (code) DO NOTHING RETURNING id",
                (row.get('Object_ObjectType'), row.get('Object_ObjectType'))
            )
            result = self.db.cursor.fetchone()
            if result:
                ref_ids['object_type_id'] = result[0]
            else:
                self.db.cursor.execute(
                    "SELECT id FROM reference.object_types WHERE code = %s",
                    (row.get('Object_ObjectType'),)
                )
                result = self.db.cursor.fetchone()
                if result:
                    ref_ids['object_type_id'] = result[0]

        # 5. Обработка страны (базовая)
        self.db.cursor.execute(
            "INSERT INTO reference.countries (code, name_ru) VALUES (%s, %s) \
             ON CONFLICT (code) DO NOTHING RETURNING id",
            ('KZ', 'Казахстан')
        )
        result = self.db.cursor.fetchone()
        if result:
            ref_ids['country_id'] = result[0]
        else:
            self.db.cursor.execute(
                "SELECT id FROM reference.countries WHERE code = %s",
                ('KZ',)
            )
            result = self.db.cursor.fetchone()
            if result:
                ref_ids['country_id'] = result[0]

        return ref_ids

    def insert_auction(self, row, ref_ids):
        """
        Вставка аукциона

        Args:
            row (dict): Строка данных
            ref_ids (dict): Словарь с ID справочных данных

        Returns:
            int: ID вставленного аукциона или None
        """
        try:
            if not row.get('AuctionId'):
                logger.warning(f"Пропуск записи без AuctionId: {row}")
                return None

            # Преобразование дат
            start_date = None
            if row.get('StartDate'):
                try:
                    start_date = datetime.strptime(row.get('StartDate'), "%d.%m.%Y %H:%M")
                except ValueError:
                    logger.warning(f"Невозможно преобразовать StartDate: {row.get('StartDate')}")

            publish_date = None
            if row.get('PublishDate'):
                try:
                    publish_date = datetime.strptime(row.get('PublishDate'), "%d.%m.%Y %H:%M")
                except ValueError:
                    logger.warning(f"Невозможно преобразовать PublishDate: {row.get('PublishDate')}")

            # Вставка аукциона
            query = """
                INSERT INTO auction.auctions (
                    auction_id, auction_type_id, auction_status_id, start_date, publish_date,
                    start_price, min_price, guarantee_payment_amount, payment_period_id,
                    min_participants_count, participants_count, win_price, note_ru, note_kz
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (auction_id) DO UPDATE SET
                    auction_type_id = EXCLUDED.auction_type_id,
                    auction_status_id = EXCLUDED.auction_status_id,
                    start_date = EXCLUDED.start_date,
                    publish_date = EXCLUDED.publish_date,
                    start_price = EXCLUDED.start_price,
                    min_price = EXCLUDED.min_price,
                    guarantee_payment_amount = EXCLUDED.guarantee_payment_amount,
                    payment_period_id = EXCLUDED.payment_period_id,
                    min_participants_count = EXCLUDED.min_participants_count,
                    participants_count = EXCLUDED.participants_count,
                    win_price = EXCLUDED.win_price,
                    note_ru = EXCLUDED.note_ru,
                    note_kz = EXCLUDED.note_kz,
                    updated_at = NOW()
                RETURNING id
            """

            self.db.cursor.execute(query, (
                int(row.get('AuctionId')),
                ref_ids.get('auction_type_id'),
                ref_ids.get('auction_status_id'),
                start_date,
                publish_date,
                float(row.get('StartPrice')) if row.get('StartPrice') else None,
                float(row.get('MinPrice')) if row.get('MinPrice') else None,
                float(row.get('GuaranteePaymentAmount')) if row.get('GuaranteePaymentAmount') else None,
                ref_ids.get('payment_period_id'),
                int(row.get('MinParticipantsCount')) if row.get('MinParticipantsCount') else None,
                int(row.get('ParticipantsCount')) if row.get('ParticipantsCount') else None,
                float(row.get('WinPrice')) if row.get('WinPrice') else None,
                row.get('NoteRu'),
                row.get('NoteKz')
            ))

            result = self.db.cursor.fetchone()
            if result:
                return result[0]
            return None
        except Exception as e:
            logger.error(f"Ошибка при вставке аукциона: {e}")
            self.db.rollback()
            return None

        def insert_auction_object(self, auction_id, row, ref_ids):
            """
            Вставка объекта аукциона

            Args:
                auction_id (int): ID аукциона
                row (dict): Строка данных
                ref_ids (dict): Словарь с ID справочных данных

            Returns:
                int: ID вставленного объекта или None
            """
            try:
                if not auction_id:
                    logger.warning(f"Пропуск вставки объекта без ID аукциона")
                    return None

                # Преобразование метаданных в JSON
                meta_data = None
                if row.get('Object_MetaData'):
                    try:
                        meta_data = json.loads(row.get('Object_MetaData'))
                    except (json.JSONDecodeError, TypeError):
                        meta_data = {"raw_data": row.get('Object_MetaData')}

                # Преобразование даты SduDate
                sdu_date = None
                if row.get('Object_SduDate'):
                    try:
                        sdu_date = datetime.strptime(row.get('Object_SduDate'), "%d.%m.%Y %H:%M")
                    except ValueError:
                        logger.warning(f"Невозможно преобразовать Object_SduDate: {row.get('Object_SduDate')}")

                # Поиск организации по SellerXin
                seller_id = None
                if row.get('Object_SellerXin'):
                    self.db.cursor.execute(
                        "SELECT id FROM gosreestr.organizations WHERE bin = %s",
                        (row.get('Object_SellerXin'),)
                    )
                    result = self.db.cursor.fetchone()
                    if result:
                        seller_id = result[0]

                # Вставка объекта аукциона
                query = """
                                INSERT INTO auction.objects (
                                    auction_id, object_type_id, name_ru, name_kz, description_ru, description_kz,
                                    balanceholder_info_ru, balanceholder_info_kz, seller_id, seller_xin,
                                    seller_info_ru, seller_info_kz, seller_phone_ru, seller_phone_kz,
                                    meta_data, sdu_date
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (auction_id) DO UPDATE SET
                                    object_type_id = EXCLUDED.object_type_id,
                                    name_ru = EXCLUDED.name_ru,
                                    name_kz = EXCLUDED.name_kz,
                                    description_ru = EXCLUDED.description_ru,
                                    description_kz = EXCLUDED.description_kz,
                                    balanceholder_info_ru = EXCLUDED.balanceholder_info_ru,
                                    balanceholder_info_kz = EXCLUDED.balanceholder_info_kz,
                                    seller_id = EXCLUDED.seller_id,
                                    seller_xin = EXCLUDED.seller_xin,
                                    seller_info_ru = EXCLUDED.seller_info_ru,
                                    seller_info_kz = EXCLUDED.seller_info_kz,
                                    seller_phone_ru = EXCLUDED.seller_phone_ru,
                                    seller_phone_kz = EXCLUDED.seller_phone_kz,
                                    meta_data = EXCLUDED.meta_data,
                                    sdu_date = EXCLUDED.sdu_date,
                                    updated_at = NOW()
                                RETURNING id
                            """

                self.db.cursor.execute(query, (
                    auction_id,
                    ref_ids.get('object_type_id'),
                    row.get('Object_NameRu'),
                    row.get('Object_NameKz'),
                    row.get('Object_DescriptionRu'),
                    row.get('Object_DescriptionKz'),
                    row.get('Object_BalanceholderInfoRu'),
                    row.get('Object_BalanceholderInfoKz'),
                    seller_id,
                    row.get('Object_SellerXin'),
                    row.get('Object_SellerInfoRu'),
                    row.get('Object_SellerInfoKz'),
                    row.get('Object_SellerPhoneRu'),
                    row.get('Object_SellerPhoneKz'),
                    json.dumps(meta_data) if meta_data else None,
                    sdu_date
                ))

                result = self.db.cursor.fetchone()
                if result:
                    return result[0]
                return None
            except Exception as e:
                logger.error(f"Ошибка при вставке объекта аукциона: {e}")
                self.db.rollback()
                return None

        def insert_address(self, entity_type, entity_id, row, ref_ids):
            """
            Вставка адреса

            Args:
                entity_type (str): Тип сущности ('auction_object', 'seller')
                entity_id (int): ID сущности
                row (dict): Строка данных
                ref_ids (dict): Словарь с ID справочных данных

            Returns:
                int: ID вставленного адреса или None
            """
            try:
                if not entity_id or not ref_ids.get('country_id'):
                    return None

                address = None
                if entity_type == 'auction_object' and row.get('Object_ObjectAdrAdr'):
                    address = row.get('Object_ObjectAdrAdr')
                elif entity_type == 'seller' and row.get('Object_SellerAdrAdr'):
                    address = row.get('Object_SellerAdrAdr')

                if not address:
                    return None

                # Проверка наличия адреса в базе
                self.db.cursor.execute(
                    "SELECT id FROM gosreestr.addresses WHERE entity_type = %s AND entity_id = %s",
                    (entity_type, entity_id)
                )
                result = self.db.cursor.fetchone()
                if result:
                    # Обновление существующего адреса
                    self.db.cursor.execute(
                        "UPDATE gosreestr.addresses SET address = %s, updated_at = NOW() \
                         WHERE entity_type = %s AND entity_id = %s",
                        (address, entity_type, entity_id)
                    )
                    return result[0]
                else:
                    # Вставка нового адреса
                    self.db.cursor.execute(
                        "INSERT INTO gosreestr.addresses (entity_type, entity_id, country_id, address) \
                         VALUES (%s, %s, %s, %s) RETURNING id",
                        (entity_type, entity_id, ref_ids.get('country_id'), address)
                    )
                    result = self.db.cursor.fetchone()
                    if result:
                        return result[0]
                return None
            except Exception as e:
                logger.error(f"Ошибка при вставке адреса: {e}")
                self.db.rollback()
                return None

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

                # Вставка данных в БД
                auction_count = 0
                object_count = 0
                address_count = 0

                for row in raw_data:
                    # Проверка наличия AuctionId
                    if not row.get('AuctionId'):
                        logger.warning(f"Пропуск записи без AuctionId: {row}")
                        continue

                    # Обеспечение наличия справочных данных
                    ref_ids = self.ensure_reference_data(row)

                    # Вставка аукциона
                    auction_id = self.insert_auction(row, ref_ids)
                    if auction_id:
                        auction_count += 1

                        # Вставка объекта аукциона
                        object_id = self.insert_auction_object(auction_id, row, ref_ids)
                        if object_id:
                            object_count += 1

                            # Вставка адреса объекта
                            address_id = self.insert_address('auction_object', object_id, row, ref_ids)
                            if address_id:
                                address_count += 1

                            # Вставка адреса продавца
                            seller_address_id = self.insert_address('seller', object_id, row, ref_ids)
                            if seller_address_id:
                                address_count += 1

                    # Фиксация изменений каждые batch_size записей
                    if auction_count % batch_size == 0:
                        self.db.commit()
                        logger.info(
                            f"Вставлено {auction_count} аукционов, {object_count} объектов, {address_count} адресов")

                # Фиксация оставшихся изменений
                self.db.commit()

                # Добавление записи о импорте
                self.insert_import_record(
                    source='auction',
                    records_count=auction_count,
                    status='success' if auction_count > 0 else 'error',
                    file_name=os.path.basename(csv_path),
                    additional_info={
                        'total_records': len(raw_data),
                        'inserted_auctions': auction_count,
                        'inserted_objects': object_count,
                        'inserted_addresses': address_count
                    }
                )

                logger.info(
                    f"Всего вставлено {auction_count} аукционов, {object_count} объектов, {address_count} адресов")
                return auction_count > 0
            except Exception as e:
                logger.error(f"Ошибка при загрузке данных аукционов: {e}")
                self.db.rollback()
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
            # Настройка логирования
            logger = setup_logging()

            # Настройка аргументов командной строки
            parser = argparse.ArgumentParser(description="Загрузка данных из CSV в нормализованную PostgreSQL БД")
            parser.add_argument("--source", choices=["gosreestr", "auction"], required=True, help="Источник данных")
            parser.add_argument("--file", required=True, help="Путь к CSV файлу")
            parser.add_argument("--host", default="localhost", help="Хост PostgreSQL")
            parser.add_argument("--port", type=int, default=5432, help="Порт PostgreSQL")
            parser.add_argument("--dbname", default="gosreestr_db", help="Имя базы данных")
            parser.add_argument("--user", default="data_manager", help="Имя пользователя")
            parser.add_argument("--password", help="Пароль пользователя")
            parser.add_argument("--batch-size", type=int, default=1000, help="Размер пакета для вставки")

            args = parser.parse_args()

            # Проверка наличия файла
            if not os.path.isfile(args.file):
                logger.error(f"Файл {args.file} не найден")
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
                # Выбор загрузчика в зависимости от источника
                if args.source == "gosreestr":
                    loader = GosreestrNormalizedLoader(db_connection)
                else:  # auction
                    loader = AuctionNormalizedLoader(db_connection)

                # Загрузка данных
                result = loader.load(args.file, args.batch_size)

                if result:
                    logger.info(f"Данные из файла {args.file} успешно загружены в базу данных")
                else:
                    logger.error(f"Не удалось загрузить данные из файла {args.file}")
                    sys.exit(1)
            finally:
                # Закрытие соединения с БД
                db_connection.disconnect()

        if __name__ == "__main__":
            main()