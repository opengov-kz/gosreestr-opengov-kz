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


class DataLoader:
    def __init__(self, db_connection):
        self.db = db_connection

        # Установка стиля даты для правильной обработки
        try:
            self.db.cursor.execute("SET datestyle TO 'ISO, MDY'")
            self.db.commit()
            logger.info("Настроен стиль даты для PostgreSQL: ISO, MDY")
        except Exception as e:
            logger.warning(f"Не удалось установить стиль даты: {e}")

    def load_data_from_csv(self, csv_path):
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

    def get_or_create_reference_item(self, table, code, name=None, extra_fields=None):
        """
        Получение или создание записи в справочнике

        Args:
            table (str): Имя таблицы справочника
            code (str): Код записи
            name (str): Название записи (если None или совпадает с кодом, будет использовано человекочитаемое название из словаря)
            extra_fields (dict): Дополнительные поля

        Returns:
            int: ID записи
        """
        try:
            # Словари читаемых названий для кодов
            industry_sectors = {
                'A': 'Сельское, лесное и рыбное хозяйство',
                'B': 'Горнодобывающая промышленность',
                'C': 'Обрабатывающая промышленность',
                'D': 'Электроснабжение, подача газа и пара',
                'E': 'Водоснабжение, канализация и утилизация отходов',
                'F': 'Строительство',
                'G': 'Оптовая и розничная торговля',
                'H': 'Транспорт и складирование',
                'I': 'Услуги по проживанию и питанию',
                'J': 'Информация и связь',
                'K': 'Финансовая и страховая деятельность',
                'L': 'Операции с недвижимым имуществом',
                'M': 'Профессиональная и научная деятельность',
                'N': 'Административное и вспомогательное обслуживание',
                'O': 'Государственное управление',
                'P': 'Образование',
                'Q': 'Здравоохранение и социальное обслуживание',
                'R': 'Искусство, развлечения и отдых',
                'S': 'Предоставление прочих видов услуг',
                'T': 'Деятельность домашних хозяйств',
                'U': 'Деятельность экстерриториальных организаций'
            }

            organization_types = {
                'ГУО': 'Государственное учреждение образования',
                'ГУН': 'Государственное учреждение (некоммерческое)',
                'ТОО': 'Товарищество с ограниченной ответственностью',
                'ФИЛ': 'Филиал',
                'ДНУ': 'Другое некоммерческое учреждение',
                'ГПО': 'Государственное предприятие',
                'ГПХ': 'Государственное предприятие (хозяйственное ведение)',
                'ДКУ': 'Другое коммерческое учреждение',
                'АО_': 'Акционерное общество',
                'ПРД': 'Представительство',
                'ДГП': 'Дочернее государственное предприятие',
                'ГБУ': 'Государственное бюджетное учреждение',
                'АОЗ': 'Акционерное общество закрытого типа',
                'КГУ': 'Коммунальное государственное учреждение',
                'КГП': 'Коммунальное государственное предприятие',
                'РГП': 'Республиканское государственное предприятие',
                'РГУ': 'Республиканское государственное учреждение',
                'ИП_': 'Индивидуальный предприниматель',
                'ОАО': 'Открытое акционерное общество',
                'ЗАО': 'Закрытое акционерное общество',
                'ОО_': 'Общественное объединение',
                'ГКП': 'Государственное коммунальное предприятие'
            }

            organization_statuses = {
                'ACT': 'Активный',
                'LIQ': 'Ликвидирован',
                'REO': 'Реорганизован',
                'PND': 'В процессе ликвидации',
                'SUS': 'Приостановлен',
                'BNK': 'Банкрот'
            }

            auction_types = {
                'TenderArenda10082019': 'Тендер по аренде',
                'SellingTaxPayerProperty': 'Продажа имущества налогоплательщика',
                'EnglandWithStartOffer': 'Английский метод с начальной ценой',
                'PriceDown17012021': 'Метод понижения цены',
                'TenderArendaPrivateSellers': 'Тендер по аренде от частных продавцов',
                'PriceUp17012021': 'Метод повышения цены',
                'PriceDown': 'Голландский метод',
                'CompetitionHunt09082023': 'Конкурс охотничьих угодий',
                'TmTenderWithoutPurchaseSince10082019': 'Тендер без выкупа',
                'Competition': 'Конкурс',
                'StateCompanies': 'Государственные компании',
                'PriceUpConfiscate': 'Метод повышения цены (конфискат)',
                'PriceDownConfiscate': 'Метод понижения цены (конфискат)',
                'PriceUpKse': 'Метод повышения цены на КСЕ',
                'TmTenderWithPurchase': 'Тендер с правом выкупа',
                'HuntingLand': 'Охотничьи угодья'
            }

            auction_statuses = {
                'AcceptingApplications': 'Прием заявок',
                'Scheduled': 'Запланирован',
                'InProgress': 'В процессе',
                'Completed': 'Завершен',
                'Cancelled': 'Отменен',
                'Pending': 'Ожидание',
                'Failed': 'Не состоялся',
                'OnHold': 'Приостановлен',
                'Draft': 'Черновик'
            }

            object_types = {
                'Building': 'Здание',
                'Land': 'Земельный участок',
                'Equipment': 'Оборудование',
                'Vehicle': 'Транспортное средство',
                'Property': 'Имущественный комплекс',
                'RealEstate': 'Недвижимость',
                'MovableProperty': 'Движимое имущество',
                'Right': 'Право',
                'Other': 'Прочее',
                'Stock': 'Акции',
                'Share': 'Доля',
                'HuntingLand': 'Охотничьи угодья',
                'FishingArea': 'Рыболовный участок',
                'Forest': 'Лесной массив',
                'AgriculturalLand': 'Сельскохозяйственная земля'
            }

            payment_periods = {
                'Monthly': 'Ежемесячно',
                'Quarterly': 'Ежеквартально',
                'Semiannually': 'Раз в полгода',
                'Annually': 'Ежегодно',
                'OneTime': 'Единовременно',
                'Other': 'Другое',
                'Weekly': 'Еженедельно',
                'Biweekly': 'Раз в две недели'
            }

            countries = {
                'KZ': 'Казахстан',
                'RU': 'Россия',
                'BY': 'Беларусь',
                'UZ': 'Узбекистан',
                'KG': 'Кыргызстан',
                'TJ': 'Таджикистан',
                'TM': 'Туркменистан',
                'AZ': 'Азербайджан',
                'AM': 'Армения',
                'GE': 'Грузия',
                'UA': 'Украина',
                'MD': 'Молдова'
            }

            # Получаем тип справочника из имени таблицы
            table_type = table.split('.')[-1]

            # Выбираем соответствующий словарь
            code_dict = None
            if table_type == 'industry_sectors':
                code_dict = industry_sectors
            elif table_type == 'organization_types':
                code_dict = organization_types
            elif table_type == 'organization_statuses':
                code_dict = organization_statuses
            elif table_type == 'auction_types':
                code_dict = auction_types
            elif table_type == 'auction_statuses':
                code_dict = auction_statuses
            elif table_type == 'object_types':
                code_dict = object_types
            elif table_type == 'payment_periods':
                code_dict = payment_periods
            elif table_type == 'countries':
                code_dict = countries

            # Ограничиваем длину кода в зависимости от таблицы
            max_code_length = 10  # По умолчанию для всех таблиц

            # Для некоторых таблиц устанавливаем другие ограничения
            if table_type == 'auction_types' or table_type == 'auction_statuses' or table_type == 'object_types':
                max_code_length = 50
            elif table_type == 'organization_types':
                max_code_length = 10

            # Обрезаем код, если он длиннее максимальной длины
            if code and len(str(code)) > max_code_length:
                logger.warning(f"Код '{code}' слишком длинный для {table}, обрезаем до {max_code_length} символов")
                code = str(code)[:max_code_length]

            # Определяем человекочитаемое название
            if name is None or name == code:
                # Пытаемся получить название из словаря
                if code_dict and code in code_dict:
                    name = code_dict[code]
                else:
                    # Если нет в словаре, создаем базовое название
                    if table_type == 'industry_sectors':
                        name = f"Отрасль {code}"
                    elif table_type == 'organization_types':
                        name = f"Тип организации {code}"
                    elif table_type == 'organization_statuses':
                        name = f"Статус {code}"
                    elif table_type == 'auction_types':
                        name = f"Тип аукциона {code}"
                    elif table_type == 'auction_statuses':
                        name = f"Статус аукциона {code}"
                    elif table_type == 'object_types':
                        name = f"Тип объекта {code}"
                    elif table_type == 'payment_periods':
                        name = f"Период оплаты {code}"
                    elif table_type == 'countries':
                        name = f"Страна {code}"
                    elif table_type == 'regions':
                        name = f"Регион {code}"
                    elif table_type == 'districts':
                        name = f"Район {code}"
                    elif table_type == 'kfs_levels':
                        name = f"Уровень КФС {code}"
                    else:
                        name = f"{code}"

            # Проверяем существование записи
            query = f"SELECT id FROM {table} WHERE code = %s"
            self.db.cursor.execute(query, (code,))
            result = self.db.cursor.fetchone()

            if result:
                # Если запись уже существует, обновляем название
                update_query = f"UPDATE {table} SET name_ru = %s WHERE code = %s"
                self.db.cursor.execute(update_query, (name, code))
                self.db.commit()
                return result[0]

            # Создаем новую запись
            fields = ['code', 'name_ru']
            values = [code, name]

            if extra_fields:
                for field, value in extra_fields.items():
                    fields.append(field)
                    values.append(value)

            fields_str = ', '.join(fields)
            placeholders = ', '.join(['%s'] * len(fields))

            query = f"INSERT INTO {table} ({fields_str}) VALUES ({placeholders}) RETURNING id"
            self.db.cursor.execute(query, values)
            new_id = self.db.cursor.fetchone()[0]

            return new_id
        except Exception as e:
            logger.error(f"Ошибка при получении/создании записи в {table}: {e}")
            raise


class GosreestrDataLoader(DataLoader):
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

            # Получаем или создаем справочные данные
            org_type_id = None
            if 'flOpf' in row and row['flOpf']:
                # Обрезаем значение до 10 символов, если оно длиннее
                opf_code = str(row['flOpf'])[:10]
                org_type_id = self.get_or_create_reference_item(
                    'reference.organization_types',
                    opf_code,
                    row['flOpf']
                )

            industry_sector_id = None
            if 'flOkedL0' in row and row['flOkedL0']:
                # Обрезаем значение до 10 символов
                oked_code = str(row['flOkedL0'])[:10]
                industry_sector_id = self.get_or_create_reference_item(
                    'reference.industry_sectors',
                    oked_code,
                    row['flOkedL0']
                )

            industry_activity_id = None
            if 'flOkedL3' in row and row['flOkedL3']:
                # Обрезаем значение до 10 символов
                oked_l3_code = str(row['flOkedL3'])[:10]
                industry_activity_id = self.get_or_create_reference_item(
                    'reference.industry_activities',
                    oked_l3_code,
                    row['flOkedL3'],
                    {'sector_id': industry_sector_id}
                )

            kfs_l0_id = None
            if 'flKfsL0' in row and row['flKfsL0']:
                # Обрезаем значение до 10 символов
                kfs_l0_code = str(row['flKfsL0'])[:10]
                kfs_l0_id = self.get_or_create_reference_item(
                    'reference.kfs_levels',
                    kfs_l0_code,
                    f"Level 0 - {row['flKfsL0']}",
                    {'level': 0, 'parent_id': None}
                )

            kfs_l1_id = None
            if 'flKfsL1' in row and row['flKfsL1']:
                # Обрезаем значение до 10 символов
                kfs_l1_code = str(row['flKfsL1'])[:10]
                kfs_l1_id = self.get_or_create_reference_item(
                    'reference.kfs_levels',
                    kfs_l1_code,
                    f"Level 1 - {row['flKfsL1']}",
                    {'level': 1, 'parent_id': kfs_l0_id}
                )

            kfs_l2_id = None
            if 'flKfsL2' in row and row['flKfsL2']:
                # Обрезаем значение до 10 символов
                kfs_l2_code = str(row['flKfsL2'])[:10]
                kfs_l2_id = self.get_or_create_reference_item(
                    'reference.kfs_levels',
                    kfs_l2_code,
                    f"Level 2 - {row['flKfsL2']}",
                    {'level': 2, 'parent_id': kfs_l1_id}
                )

            status_id = None
            if 'flStatus' in row and row['flStatus']:
                # Обрезаем значение до 10 символов
                status_code = str(row['flStatus'])[:10]
                status_id = self.get_or_create_reference_item(
                    'reference.organization_statuses',
                    status_code,
                    row['flStatus']
                )

            # Обрезаем БИН если он длиннее 12 символов
            bin_value = str(row.get('flBin', ''))[:12]

            transformed_row = {
                'bin': bin_value,
                'name_ru': row.get('flNameRu', ''),
                'organization_type_id': org_type_id,
                'industry_sector_id': industry_sector_id,
                'industry_activity_id': industry_activity_id,
                'kfs_l0_id': kfs_l0_id,
                'kfs_l1_id': kfs_l1_id,
                'kfs_l2_id': kfs_l2_id,
                'state_involvement': float(row.get('flStateInvolvement', 0)) if row.get('flStateInvolvement') else None,
                'status_id': status_id,
                'owner_bin': str(row.get('flOwnerBin', ''))[:12],  # Обрезаем до 12 символов
                'ogu_bin': str(row.get('flOguBin', ''))[:12]  # Обрезаем до 12 символов
            }

            transformed_data.append(transformed_row)

        logger.info(f"Преобразовано {len(transformed_data)} записей организаций")
        return transformed_data

    # Обновите метод insert_data в классе GosreestrDataLoader

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

            # Устраняем дубликаты по полю bin
            unique_data = {}
            for item in data:
                bin_value = item['bin']
                # Если уже видели этот БИН, пропускаем
                if bin_value in unique_data:
                    logger.warning(f"Найден дубликат для БИН {bin_value}, пропускаем")
                    continue
                unique_data[bin_value] = item

            # Преобразуем обратно в список
            deduplicated_data = list(unique_data.values())
            logger.info(f"После удаления дубликатов осталось {len(deduplicated_data)} из {len(data)} записей")

            # Этап 1: Вставка основных организаций без ссылок на владельцев и ОГУ
            logger.info(f"Вставка {len(deduplicated_data)} записей в таблицу gosreestr.organizations (первый этап)")

            query = """
                INSERT INTO gosreestr.organizations 
                (bin, name_ru, organization_type_id, industry_sector_id, industry_activity_id,
                kfs_l0_id, kfs_l1_id, kfs_l2_id, state_involvement, status_id) 
                VALUES %s
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
                RETURNING id, bin
            """

            values = []
            for row in deduplicated_data:
                values.append((
                    row['bin'],
                    row['name_ru'],
                    row['organization_type_id'],
                    row['industry_sector_id'],
                    row['industry_activity_id'],
                    row['kfs_l0_id'],
                    row['kfs_l1_id'],
                    row['kfs_l2_id'],
                    row['state_involvement'],
                    row['status_id']
                ))

            # Создаем словарь соответствия БИН -> ID
            organizations_map = {}
            count = 0

            for i in range(0, len(values), batch_size):
                batch = values[i:i + batch_size]

                # Дополнительная защита от дубликатов внутри пакета
                seen_bins = set()
                unique_batch = []

                for value in batch:
                    bin_value = value[0]  # БИН - первый элемент в кортеже
                    if bin_value not in seen_bins:
                        seen_bins.add(bin_value)
                        unique_batch.append(value)
                    else:
                        logger.warning(f"Пропуск дубликата {bin_value} внупуетри пакета {i // batch_size + 1}")

                if not unique_batch:
                    logger.warning(f"Пакет {i // batch_size + 1} не содержит уникальных записей, пропускаем")
                    continue

                result = execute_values(self.db.cursor, query, unique_batch, fetch=True)
                count += len(result)

                # Заполняем словарь соответствия
                for org_id, org_bin in result:
                    organizations_map[org_bin] = org_id

                self.db.commit()
                logger.info(f"Вставлено {len(result)} записей организаций (пакет {i // batch_size + 1})")

            # Этап 2: Обновление ссылок на владельцев и ОГУ
            logger.info("Обновление ссылок на владельцев и ОГУ")

            for row in deduplicated_data:
                if not row['owner_bin'] and not row['ogu_bin']:
                    continue

                org_id = organizations_map.get(row['bin'])
                if not org_id:
                    continue

                owner_id = organizations_map.get(row['owner_bin'])
                ogu_id = organizations_map.get(row['ogu_bin'])

                if owner_id or ogu_id:
                    update_query = """
                        UPDATE gosreestr.organizations
                        SET owner_id = %s, ogu_id = %s
                        WHERE id = %s
                    """
                    self.db.cursor.execute(update_query, (owner_id, ogu_id, org_id))
                    self.db.commit()

            logger.info(f"Всего вставлено/обновлено {count} записей в таблицу gosreestr.organizations")
            return count
        except Exception as e:
            logger.error(f"Ошибка при вставке данных в таблицу gosreestr.organizations: {e}")
            self.db.rollback()
            return 0

    def load(self, csv_path, batch_size=1000):
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
        sellers_data = []

        for row in raw_data:
            # Проверка наличия обязательных полей
            if 'AuctionId' not in row:
                logger.warning(f"Пропуск записи без AuctionId: {row}")
                continue

            # Получаем или создаем справочные данные
            auction_type_id = None
            if 'AuctionType' in row and row['AuctionType']:
                auction_type_id = self.get_or_create_reference_item(
                    'reference.auction_types',
                    row['AuctionType'],
                    row['AuctionType']
                )

            auction_status_id = None
            if 'AuctionStatus' in row and row['AuctionStatus']:
                auction_status_id = self.get_or_create_reference_item(
                    'reference.auction_statuses',
                    row['AuctionStatus'],
                    row['AuctionStatus']
                )

            payment_period_id = None
            if 'PayPeriod' in row and row['PayPeriod']:
                payment_period_id = self.get_or_create_reference_item(
                    'reference.payment_periods',
                    row['PayPeriod'],
                    row['PayPeriod']
                )

            # Преобразование данных о торгах
            auction_id = int(row.get('AuctionId'))

            # Преобразование дат из формата "DD.MM.YYYY HH:MM" в формат ISO для PostgreSQL
            start_date = None
            if row.get('StartDate'):
                try:
                    from datetime import datetime
                    # Предполагаем формат "DD.MM.YYYY HH:MM"
                    date_obj = datetime.strptime(row['StartDate'], "%d.%m.%Y %H:%M")
                    start_date = date_obj.strftime("%Y-%m-%d %H:%M:%S")
                except (ValueError, TypeError) as e:
                    logger.warning(f"Ошибка преобразования StartDate '{row.get('StartDate')}': {e}")

            publish_date = None
            if row.get('PublishDate'):
                try:
                    date_obj = datetime.strptime(row['PublishDate'], "%d.%m.%Y %H:%M")
                    publish_date = date_obj.strftime("%Y-%m-%d %H:%M:%S")
                except (ValueError, TypeError) as e:
                    logger.warning(f"Ошибка преобразования PublishDate '{row.get('PublishDate')}': {e}")

            auction_row = {
                'auction_id': auction_id,
                'auction_type_id': auction_type_id,
                'auction_status_id': auction_status_id,
                'start_date': start_date,
                'publish_date': publish_date,
                'start_price': float(row.get('StartPrice', 0)) if row.get('StartPrice') else None,
                'min_price': float(row.get('MinPrice', 0)) if row.get('MinPrice') else None,
                'guarantee_payment_amount': float(row.get('GuaranteePaymentAmount', 0)) if row.get(
                    'GuaranteePaymentAmount') else None,
                'payment_period_id': payment_period_id,
                'min_participants_count': int(row.get('MinParticipantsCount', 0)) if row.get(
                    'MinParticipantsCount') else None,
                'participants_count': int(row.get('ParticipantsCount', 0)) if row.get('ParticipantsCount') else None,
                'win_price': float(row.get('WinPrice', 0)) if row.get('WinPrice') else None,
                'note_ru': row.get('NoteRu', ''),
                'note_kz': row.get('NoteKz', ''),
                'publish_note_ru': row.get('PublishNoteRu', ''),
                'publish_note_kz': row.get('PublishNoteKz', ''),
                'payments_recipient_info_ru': row.get('PaymentsRecipientInfoRu', ''),
                'payments_recipient_info_kz': row.get('PaymentsRecipientInfoKz', '')
            }

            trades_data.append(auction_row)

            # Извлекаем данные об объекте и продавце
            seller_xin = None
            if 'Object_SellerXin' in row and row['Object_SellerXin']:
                # Обрезаем SellerXin до 12 символов, если он длиннее
                seller_xin = str(row['Object_SellerXin'])[:12]

                # Данные о продавце
                seller_row = {
                    'xin': seller_xin,
                    'name_ru': row.get('Object_SellerInfoRu', ''),
                    'name_kz': row.get('Object_SellerInfoKz', ''),
                    'info_ru': row.get('Object_SellerInfoRu', ''),
                    'info_kz': row.get('Object_SellerInfoKz', '')
                }

                sellers_data.append(seller_row)

            # Получаем тип объекта
            object_type_id = None
            if 'Object_ObjectType' in row and row['Object_ObjectType']:
                object_type_id = self.get_or_create_reference_item(
                    'reference.object_types',
                    row['Object_ObjectType'],
                    row['Object_ObjectType']
                )

            # Данные об объекте
            object_row = {
                'auction_id': auction_id,
                'object_type_id': object_type_id,
                'name_ru': row.get('Object_NameRu', ''),
                'name_kz': row.get('Object_NameKz', ''),
                'description_ru': row.get('Object_DescriptionRu', ''),
                'description_kz': row.get('Object_DescriptionKz', ''),
                'seller_xin': seller_xin,
                'balanceholder_info_ru': row.get('Object_BalanceholderInfoRu', ''),
                'balanceholder_info_kz': row.get('Object_BalanceholderInfoKz', ''),
                'address_country': row.get('Object_ObjectAdrCountry', ''),
                'address_region': row.get('Object_ObjectAdrObl', ''),
                'address_district': row.get('Object_ObjectAdrReg', ''),
                'address': row.get('Object_ObjectAdrAdr', ''),
                'meta_data': row.get('Object_MetaData', None)
            }

            objects_data.append(object_row)

        logger.info(
            f"Преобразовано {len(trades_data)} записей о торгах, {len(sellers_data)} записей о продавцах и {len(objects_data)} записей об объектах")
        return trades_data, sellers_data, objects_data

    def insert_auction_data(self, auctions_data, batch_size=1000):
        try:
            if not auctions_data:
                logger.warning("Нет данных о торгах для вставки")
                return 0, {}

            logger.info(f"Вставка {len(auctions_data)} записей в таблицу auction.auctions")

            query = """
                INSERT INTO auction.auctions 
                (auction_id, auction_type_id, auction_status_id, start_date, publish_date, 
                 start_price, min_price, guarantee_payment_amount, payment_period_id, 
                 min_participants_count, participants_count, win_price, note_ru, note_kz, 
                 publish_note_ru, publish_note_kz, payments_recipient_info_ru, payments_recipient_info_kz) 
                VALUES %s
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
                    publish_note_ru = EXCLUDED.publish_note_ru,
                    publish_note_kz = EXCLUDED.publish_note_kz,
                    payments_recipient_info_ru = EXCLUDED.payments_recipient_info_ru,
                    payments_recipient_info_kz = EXCLUDED.payments_recipient_info_kz,
                    updated_at = NOW()
                RETURNING id, auction_id
            """

            values = []
            for row in auctions_data:
                values.append((
                    row['auction_id'],
                    row['auction_type_id'],
                    row['auction_status_id'],
                    row['start_date'],
                    row['publish_date'],
                    row['start_price'],
                    row['min_price'],
                    row['guarantee_payment_amount'],
                    row['payment_period_id'],
                    row['min_participants_count'],
                    row['participants_count'],
                    row['win_price'],
                    row['note_ru'],
                    row['note_kz'],
                    row['publish_note_ru'],
                    row['publish_note_kz'],
                    row['payments_recipient_info_ru'],
                    row['payments_recipient_info_kz']
                ))

            # Создаем словарь соответствия auction_id -> id
            auctions_map = {}
            count = 0

            for i in range(0, len(values), batch_size):
                batch = values[i:i + batch_size]
                result = execute_values(self.db.cursor, query, batch, fetch=True)
                count += len(result)

                # Заполняем словарь соответствия
                for db_id, auction_id in result:
                    auctions_map[auction_id] = db_id

                self.db.commit()
                logger.info(f"Вставлено {len(result)} записей о торгах (пакет {i // batch_size + 1})")

            logger.info(f"Всего вставлено/обновлено {count} записей в таблицу auction.auctions")
            return count, auctions_map
        except Exception as e:
            logger.error(f"Ошибка при вставке данных в таблицу auction.auctions: {e}")
            self.db.rollback()
            return 0, {}

    def insert_sellers_data(self, sellers_data, batch_size=1000):
        try:
            if not sellers_data:
                logger.warning("Нет данных о продавцах для вставки")
                return 0, {}

            # Удаляем дубликаты по xin
            unique_sellers = {}
            for seller in sellers_data:
                if seller['xin'] not in unique_sellers:
                    unique_sellers[seller['xin']] = seller

            sellers_data = list(unique_sellers.values())
            logger.info(f"Вставка {len(sellers_data)} записей в таблицу auction.sellers")

            query = """
                INSERT INTO auction.sellers 
                (xin, name_ru, name_kz, info_ru, info_kz) 
                VALUES %s
                ON CONFLICT (xin) DO UPDATE SET 
                    name_ru = EXCLUDED.name_ru,
                    name_kz = EXCLUDED.name_kz,
                    info_ru = EXCLUDED.info_ru,
                    info_kz = EXCLUDED.info_kz,
                    updated_at = NOW()
                RETURNING id, xin
            """

            values = []
            for row in sellers_data:
                values.append((
                    row['xin'],
                    row['name_ru'],
                    row['name_kz'],
                    row['info_ru'],
                    row['info_kz']
                ))

            # Создаем словарь соответствия xin -> id
            sellers_map = {}
            count = 0

            for i in range(0, len(values), batch_size):
                batch = values[i:i + batch_size]
                result = execute_values(self.db.cursor, query, batch, fetch=True)
                count += len(result)

                # Заполняем словарь соответствия
                for seller_id, seller_xin in result:
                    sellers_map[seller_xin] = seller_id

                self.db.commit()
                logger.info(f"Вставлено {len(result)} записей о продавцах (пакет {i // batch_size + 1})")

            # Добавляем контактные данные, но теперь проверяем наличие полей
            seller_contacts_data = []
            for seller in sellers_data:
                seller_id = sellers_map.get(seller['xin'])
                if not seller_id:
                    continue

                if 'phone_ru' in seller and seller['phone_ru']:
                    seller_contacts_data.append({
                        'seller_id': seller_id,
                        'contact_type': 'phone',
                        'contact_value': seller['phone_ru'],
                        'language': 'ru',
                        'is_primary': True
                    })

                if 'phone_kz' in seller and seller['phone_kz']:
                    seller_contacts_data.append({
                        'seller_id': seller_id,
                        'contact_type': 'phone',
                        'contact_value': seller['phone_kz'],
                        'language': 'kz',
                        'is_primary': True
                    })

            if seller_contacts_data:
                self.insert_seller_contacts(seller_contacts_data)

            logger.info(f"Всего вставлено/обновлено {count} записей в таблицу auction.sellers")
            return count, sellers_map
        except Exception as e:
            logger.error(f"Ошибка при вставке данных в таблицу auction.sellers: {e}")
            self.db.rollback()
            return 0, {}

    def insert_seller_contacts(self, contacts_data, batch_size=1000):
        try:
            if not contacts_data:
                return 0

            logger.info(f"Вставка {len(contacts_data)} записей в таблицу auction.seller_contacts")

            # Удаляем существующие контакты
            for contact in contacts_data:
                if 'seller_id' in contact and contact['seller_id']:
                    self.db.cursor.execute(
                        "DELETE FROM auction.seller_contacts WHERE seller_id = %s AND contact_type = %s AND language = %s",
                        (contact['seller_id'], contact['contact_type'], contact['language'])
                    )

            query = """
                INSERT INTO auction.seller_contacts 
                (seller_id, contact_type, contact_value, language, is_primary) 
                VALUES %s
            """

            values = []
            for row in contacts_data:
                if 'seller_id' in row and row['seller_id']:
                    values.append((
                        row['seller_id'],
                        row['contact_type'],
                        row['contact_value'],
                        row['language'],
                        row['is_primary']
                    ))

            count = 0
            if values:
                for i in range(0, len(values), batch_size):
                    batch = values[i:i + batch_size]
                    result = execute_values(self.db.cursor, query, batch)
                    count += len(batch)
                    self.db.commit()

            logger.info(f"Всего вставлено {count} записей в таблицу auction.seller_contacts")
            return count
        except Exception as e:
            logger.error(f"Ошибка при вставке данных в таблицу auction.seller_contacts: {e}")
            self.db.rollback()
            return 0

    def insert_auction_objects(self, objects_data, auctions_map, sellers_map, batch_size=1000):
        """
        Вставка данных об объектах аукционов в базу данных

        Args:
            objects_data (list): Данные об объектах для вставки
            auctions_map (dict): Словарь соответствия внешних ID аукционов внутренним ID
            sellers_map (dict): Словарь соответствия SellerXin ID продавцов
            batch_size (int): Размер пакета для пакетной вставки

        Returns:
            int: Количество вставленных записей
        """
        try:
            if not objects_data:
                logger.warning("Нет данных об объектах для вставки")
                return 0

            logger.info(f"Вставка {len(objects_data)} записей в таблицу auction.auction_objects")

            # Добавляем страны, регионы и районы
            countries_map = {}
            regions_map = {}
            districts_map = {}

            for obj in objects_data:
                # Страна
                if obj['address_country'] and obj['address_country'] not in countries_map:
                    country_id = self.get_or_create_reference_item(
                        'reference.countries',
                        obj['address_country'],
                        obj['address_country']
                    )
                    countries_map[obj['address_country']] = country_id

                # Регион
                if obj['address_region'] and obj['address_region'] not in regions_map:
                    country_id = countries_map.get(obj['address_country'])
                    region_id = self.get_or_create_reference_item(
                        'reference.regions',
                        obj['address_region'],
                        obj['address_region'],
                        {'country_id': country_id}
                    )
                    regions_map[obj['address_region']] = region_id

                # Район
                if obj['address_district'] and obj['address_district'] not in districts_map:
                    region_id = regions_map.get(obj['address_region'])
                    district_id = self.get_or_create_reference_item(
                        'reference.districts',
                        obj['address_district'],
                        obj['address_district'],
                        {'region_id': region_id}
                    )
                    districts_map[obj['address_district']] = district_id

            query = """
                INSERT INTO auction.auction_objects 
                (auction_id, object_type_id, name_ru, name_kz, description_ru, description_kz, 
                seller_id, balanceholder_info_ru, balanceholder_info_kz, meta_data) 
                VALUES %s
                RETURNING id
            """

            values = []
            addresses_data = []

            for row in objects_data:
                # Получаем ID аукциона из словаря соответствия
                db_auction_id = auctions_map.get(row['auction_id'])
                if not db_auction_id:
                    logger.warning(f"Не найден ID аукциона для внешнего ID {row['auction_id']}")
                    continue

                # Получаем ID продавца из словаря соответствия
                seller_id = sellers_map.get(row['seller_xin']) if row['seller_xin'] else None

                # Обработка meta_data для обеспечения валидного JSON
                meta_data = row.get('meta_data')
                try:
                    # Если это строка, попробуем преобразовать в JSON
                    if meta_data and isinstance(meta_data, str):
                        try:
                            meta_data = json.loads(meta_data)
                        except json.JSONDecodeError:
                            # Если не удалось преобразовать, сохраняем как обычную строку в JSON
                            meta_data = {"raw_value": meta_data}

                    # Преобразуем в JSON-строку
                    if meta_data:
                        meta_data = json.dumps(meta_data)
                    else:
                        # Пустой JSON-объект, если нет данных
                        meta_data = '{}'
                except Exception as e:
                    logger.warning(f"Ошибка обработки meta_data для auction_id {row['auction_id']}: {e}")
                    # В случае ошибки используем пустой объект
                    meta_data = '{}'

                values.append((
                    db_auction_id,
                    row['object_type_id'],
                    row['name_ru'],
                    row['name_kz'],
                    row['description_ru'],
                    row['description_kz'],
                    seller_id,
                    row['balanceholder_info_ru'],
                    row['balanceholder_info_kz'],
                    meta_data
                ))

                # Сохраняем адрес для последующей вставки
                if row['address']:
                    addresses_data.append({
                        'entity_type': 'auction_object',
                        'entity_id': db_auction_id,  # Используем ID аукциона, так как объект еще не создан
                        'address_type': 'object',
                        'country_id': countries_map.get(row['address_country']),
                        'region_id': regions_map.get(row['address_region']),
                        'district_id': districts_map.get(row['address_district']),
                        'address': row['address']
                    })

            count = 0
            objects_ids = []

            for i in range(0, len(values), batch_size):
                batch = values[i:i + batch_size]
                try:
                    result = execute_values(self.db.cursor, query, batch, fetch=True)
                    count += len(result)

                    # Собираем ID объектов
                    for (obj_id,) in result:
                        objects_ids.append(obj_id)

                    self.db.commit()
                    logger.info(f"Вставлено {len(result)} записей об объектах (пакет {i // batch_size + 1})")
                except Exception as e:
                    logger.error(f"Ошибка при вставке пакета {i // batch_size + 1}: {e}")
                    # Пробуем вставить по одной записи для выявления проблемных
                    for j, value in enumerate(batch):
                        try:
                            single_query = """
                                INSERT INTO auction.auction_objects 
                                (auction_id, object_type_id, name_ru, name_kz, description_ru, description_kz, 
                                seller_id, balanceholder_info_ru, balanceholder_info_kz, meta_data) 
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                RETURNING id
                            """
                            self.db.cursor.execute(single_query, value)
                            result = self.db.cursor.fetchone()
                            if result:
                                count += 1
                                objects_ids.append(result[0])
                            self.db.commit()
                        except Exception as item_error:
                            logger.error(f"Ошибка при вставке записи {j} в пакете {i // batch_size + 1}: {item_error}")
                            self.db.rollback()

            # Вставляем адреса
            if addresses_data:
                self.insert_addresses(addresses_data)

            logger.info(f"Всего вставлено/обновлено {count} записей в таблицу auction.auction_objects")
            return count
        except Exception as e:
            logger.error(f"Ошибка при вставке данных в таблицу auction.auction_objects: {e}")
            self.db.rollback()
            return 0

    def insert_addresses(self, addresses_data, batch_size=1000):
        try:
            if not addresses_data:
                return 0

            logger.info(f"Вставка {len(addresses_data)} записей в таблицу gosreestr.addresses")

            # Фильтруем адреса с некорректными ссылками на справочники
            valid_addresses = []

            for addr in addresses_data:
                # Проверяем, что district_id существует, если задан
                if addr.get('district_id'):
                    query = "SELECT 1 FROM reference.districts WHERE id = %s"
                    self.db.cursor.execute(query, (addr['district_id'],))
                    if not self.db.cursor.fetchone():
                        logger.warning(f"Район с ID {addr['district_id']} не найден, удаляем ссылку")
                        addr['district_id'] = None

                # Проверяем, что region_id существует, если задан
                if addr.get('region_id'):
                    query = "SELECT 1 FROM reference.regions WHERE id = %s"
                    self.db.cursor.execute(query, (addr['region_id'],))
                    if not self.db.cursor.fetchone():
                        logger.warning(f"Регион с ID {addr['region_id']} не найден, удаляем ссылку")
                        addr['region_id'] = None

                # Проверяем, что country_id существует, если задан
                if addr.get('country_id'):
                    query = "SELECT 1 FROM reference.countries WHERE id = %s"
                    self.db.cursor.execute(query, (addr['country_id'],))
                    if not self.db.cursor.fetchone():
                        logger.warning(f"Страна с ID {addr['country_id']} не найдена, удаляем ссылку")
                        addr['country_id'] = None

                valid_addresses.append(addr)

            if not valid_addresses:
                logger.warning("Нет валидных адресов для вставки")
                return 0

            query = """
                INSERT INTO gosreestr.addresses 
                (entity_type, entity_id, address_type, country_id, region_id, district_id, address) 
                VALUES %s
                ON CONFLICT (entity_type, entity_id, address_type) DO UPDATE SET 
                    country_id = EXCLUDED.country_id,
                    region_id = EXCLUDED.region_id,
                    district_id = EXCLUDED.district_id,
                    address = EXCLUDED.address,
                    updated_at = NOW()
            """

            values = []
            for row in valid_addresses:
                values.append((
                    row['entity_type'],
                    row['entity_id'],
                    row['address_type'],
                    row['country_id'],
                    row['region_id'],
                    row['district_id'],
                    row['address']
                ))

            count = 0
            for i in range(0, len(values), batch_size):
                batch = values[i:i + batch_size]
                try:
                    result = execute_values(self.db.cursor, query, batch)
                    count += len(batch)
                    self.db.commit()
                    logger.info(f"Вставлено {len(batch)} адресов (пакет {i // batch_size + 1})")
                except Exception as e:
                    logger.error(f"Ошибка при вставке пакета адресов {i // batch_size + 1}: {e}")
                    self.db.rollback()

                    # Пробуем вставить по одному
                    for j, value in enumerate(batch):
                        try:
                            single_query = """
                                INSERT INTO gosreestr.addresses 
                                (entity_type, entity_id, address_type, country_id, region_id, district_id, address) 
                                VALUES (%s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (entity_type, entity_id, address_type) DO UPDATE SET 
                                    country_id = EXCLUDED.country_id,
                                    region_id = EXCLUDED.region_id,
                                    district_id = EXCLUDED.district_id,
                                    address = EXCLUDED.address,
                                    updated_at = NOW()
                            """
                            self.db.cursor.execute(single_query, value)
                            self.db.commit()
                            count += 1
                        except Exception as item_error:
                            logger.error(f"Ошибка при вставке адреса {j} в пакете {i // batch_size + 1}: {item_error}")
                            self.db.rollback()

            logger.info(f"Всего вставлено/обновлено {count} записей в таблицу gosreestr.addresses")
            return count
        except Exception as e:
            logger.error(f"Ошибка при вставке данных в таблицу gosreestr.addresses: {e}")
            self.db.rollback()
            return 0

    def load(self, csv_path, batch_size=1000):
        try:
            # Загрузка данных из CSV
            raw_data = self.load_data_from_csv(csv_path)
            if not raw_data:
                return False

            # Преобразование данных
            auctions_data, sellers_data, objects_data = self.transform_auction_data(raw_data)
            if not auctions_data:
                return False

            # Вставка данных в БД
            auctions_count, auctions_map = self.insert_auction_data(auctions_data, batch_size)
            sellers_count, sellers_map = self.insert_sellers_data(sellers_data, batch_size)
            objects_count = self.insert_auction_objects(objects_data, auctions_map, sellers_map, batch_size)

            # Добавление записи о импорте
            self.insert_import_record(
                source='auction',
                records_count=auctions_count,
                status='success' if auctions_count > 0 else 'error',
                file_name=os.path.basename(csv_path),
                additional_info={
                    'total_records': len(raw_data),
                    'auctions_records': len(auctions_data),
                    'sellers_records': len(sellers_data),
                    'objects_records': len(objects_data),
                    'inserted_auctions': auctions_count,
                    'inserted_sellers': sellers_count,
                    'inserted_objects': objects_count
                }
            )

            return auctions_count > 0
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
            loader = GosreestrDataLoader(db_connection)
        else:  # auction
            loader = AuctionDataLoader(db_connection)

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
