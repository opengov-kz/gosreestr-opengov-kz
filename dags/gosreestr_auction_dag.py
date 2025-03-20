"""
DAG для сбора и загрузки данных из Госреестра и системы электронных торгов
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
import os
import logging
import subprocess
import shutil

# Настройка логирования
logger = logging.getLogger(__name__)

# Определение путей
BASE_DIR = "/opt/airflow"
DATA_DIR = f"{BASE_DIR}/data"
SCRIPTS_DIR = f"{BASE_DIR}/scripts"

# Создание директорий, если они не существуют
os.makedirs(DATA_DIR, exist_ok=True)

# Конфигурация по умолчанию
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_gosreestr_parser(**context):
    """Запуск парсера для списка активных объектов Госреестра"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"{DATA_DIR}/gosreestr_objects_{timestamp}.csv"
    
    try:
        # Запуск скрипта парсера
        logger.info(f"Запуск парсера с выходным файлом: {output_file}")
        result = subprocess.run(
            ["python", f"{SCRIPTS_DIR}/gosreestr_parser.py", "--output", output_file],
            check=True,
            capture_output=True,
            text=True
        )
        
        logger.info(f"Парсер Госреестра успешно выполнен: {result.stdout}")
        
        # Проверка наличия файла
        if not os.path.exists(output_file):
            logger.warning(f"Файл {output_file} не был создан парсером")
            
            # Поиск файла в корневой директории
            root_file = os.path.join(BASE_DIR, os.path.basename(output_file))
            if os.path.exists(root_file):
                logger.info(f"Найден файл в корне проекта: {root_file}")
                # Копируем файл в нужную директорию
                import shutil
                shutil.copy(root_file, output_file)
                logger.info(f"Файл скопирован из {root_file} в {output_file}")
        
        # Сохранение пути к файлу для использования в следующих задачах
        context['ti'].xcom_push(key='gosreestr_csv_file', value=output_file)
        
        return output_file
    except Exception as e:
        logger.error(f"Ошибка при выполнении парсера Госреестра: {str(e)}")
        raise

def run_auction_parser(**context):
    """Запуск парсера для списка электронных торгов"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"{DATA_DIR}/auction_trades_{timestamp}.csv"
    
    try:
        # Запуск скрипта парсера
        logger.info(f"Запуск парсера с выходным файлом: {output_file}")
        result = subprocess.run(
            [
                "python", 
                f"{SCRIPTS_DIR}/auction_trades_parser.py", 
                "--status", "AcceptingApplications", 
                "--output", output_file
            ],
            check=True,
            capture_output=True,
            text=True
        )
        
        logger.info(f"Парсер электронных торгов успешно выполнен: {result.stdout}")
        
        # Проверка наличия файла
        if not os.path.exists(output_file):
            logger.warning(f"Файл {output_file} не был создан парсером")
            
            # Поиск файла в корневой директории
            root_file = os.path.join(BASE_DIR, os.path.basename(output_file))
            if os.path.exists(root_file):
                logger.info(f"Найден файл в корне проекта: {root_file}")
                # Копируем файл в нужную директорию
                import shutil
                shutil.copy(root_file, output_file)
                logger.info(f"Файл скопирован из {root_file} в {output_file}")
        
        # Сохранение пути к файлу для использования в следующих задачах
        context['ti'].xcom_push(key='auction_csv_file', value=output_file)
        
        return output_file
    except Exception as e:
        logger.error(f"Ошибка при выполнении парсера электронных торгов: {str(e)}")
        raise

def load_gosreestr_to_db(**context):
    """Загрузка данных Госреестра в БД"""
    # Получение пути к файлу из предыдущей задачи
    csv_file = context['ti'].xcom_pull(task_ids='run_gosreestr_parser', key='gosreestr_csv_file')
    
    logger.info(f"Получен путь к файлу CSV: {csv_file}")
    
    try:
        # Прямое подключение к базе данных
        import psycopg2
        conn = psycopg2.connect(
            host='gosreestr-db',
            port=5432,
            dbname='gosreestr_db',
            user='data_manager',
            password='strong_password_here',
            options="-c datestyle=ISO,DMY"
        )
        cur = conn.cursor()
        
        # Проверка структуры базы данных
        cur.execute("SELECT schema_name FROM information_schema.schemata")
        schemas = [row[0] for row in cur.fetchall()]
        logger.info(f"Существующие схемы: {schemas}")
        
        # Проверка таблиц в схеме gosreestr
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'gosreestr'")
        tables = [row[0] for row in cur.fetchall()]
        logger.info(f"Таблицы в схеме gosreestr: {tables}")
        
        # Если нужной схемы нет, создаем ее
        if 'gosreestr' not in schemas:
            logger.info("Схема 'gosreestr' не найдена, создаем...")
            cur.execute("CREATE SCHEMA IF NOT EXISTS gosreestr")
            conn.commit()
        
        # Если нужной таблицы нет, создаем ее
        if 'objects' not in tables:
            logger.info("Таблица 'gosreestr.objects' не найдена, создаем...")
            cur.execute("""
                CREATE TABLE gosreestr.objects (
                    id SERIAL PRIMARY KEY,
                    bin VARCHAR(12) NOT NULL,
                    name_ru TEXT,
                    opf VARCHAR(100),
                    oked_l0 VARCHAR(100),
                    state_involvement NUMERIC,
                    status VARCHAR(50),
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
                )
            """)
            conn.commit()
        
        # Читаем CSV файл и вставляем данные напрямую
        logger.info(f"Чтение CSV файла: {csv_file}")
        import csv
        with open(csv_file, 'r', encoding='utf-8') as csvfile:
            csv_reader = csv.DictReader(csvfile)
            for row in csv_reader:
                # В цикле по записям CSV добавьте проверку
                if not row.get('flBin', '').strip():
                    logger.warning(f"Пропуск записи с пустым БИН: {row}")
                    continue
                logger.info(f"Вставка строки: {row}")
                cur.execute("""
                    INSERT INTO gosreestr.objects
                    (bin, name_ru, opf, oked_l0, state_involvement, status)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    row.get('flBin', ''),
                    row.get('flNameRu', ''),
                    row.get('flOpf', ''),
                    row.get('flOkedL0', ''),
                    float(row.get('flStateInvolvement', 0)) if row.get('flStateInvolvement') else None,
                    row.get('flStatus', '')
                ))
        
        # Коммит транзакции
        conn.commit()
        
        # Проверка количества записей
        cur.execute("SELECT COUNT(*) FROM gosreestr.objects")
        count = cur.fetchone()[0]
        logger.info(f"Количество записей в таблице после вставки: {count}")
        
        # Закрытие соединения
        cur.close()
        conn.close()
        
        return True
    except Exception as e:
        logger.error(f"Ошибка при прямой вставке данных: {str(e)}")
        # В случае ошибки всё равно запускаем оригинальный скрипт
        try:
            # Запуск скрипта загрузки данных
            result = subprocess.run(
                [
                    "python", 
                    f"{SCRIPTS_DIR}/data_loader.py", 
                    "--source", "gosreestr", 
                    "--file", csv_file,
                    "--host", "gosreestr-db",
                    "--port", "5432",
                    "--dbname", "gosreestr_db",
                    "--user", "data_manager",
                    "--password", "strong_password_here"
                ],
                check=True,
                capture_output=True,
                text=True
            )
            
            logger.info(f"Загрузка данных Госреестра в БД успешно выполнена: {result.stdout}")
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"Ошибка при загрузке данных Госреестра в БД: {e.stderr}")
            raise


def load_auction_to_db(**context):
    """Загрузка данных электронных торгов в БД"""
    # Получение пути к файлу из предыдущей задачи
    csv_file = context['ti'].xcom_pull(task_ids='run_auction_parser', key='auction_csv_file')
    
    logger.info(f"Получен путь к файлу CSV: {csv_file}")
    
    try:
        # Прямое подключение к базе данных
        import psycopg2
        from datetime import datetime
        
        conn = psycopg2.connect(
            host='gosreestr-db',
            port=5432,
            dbname='gosreestr_db',
            user='data_manager',
            password='strong_password_here'
        )
        cur = conn.cursor()
        
        # Проверка структуры базы данных
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'auction'")
        tables = [row[0] for row in cur.fetchall()]
        logger.info(f"Таблицы в схеме auction: {tables}")
        
        # Если нужной таблицы нет, создаем ее (хотя она должна быть уже создана)
        if 'trades' not in tables:
            logger.warning("Таблица 'auction.trades' не найдена")
            
        # Читаем CSV файл и вставляем данные напрямую
        logger.info(f"Чтение CSV файла: {csv_file}")
        import csv
        with open(csv_file, 'r', encoding='utf-8') as csvfile:
            csv_reader = csv.DictReader(csvfile)
            rows_inserted = 0
            
            for row in csv_reader:
                # Проверка наличия обязательных полей
                if 'AuctionId' not in row:
                    logger.warning(f"Пропуск записи без AuctionId: {row}")
                    continue
                    
                # Преобразование данных из CSV формата
                auction_id = int(row['AuctionId']) if row.get('AuctionId') else None
                
                # Преобразование дат из формата "DD.MM.YYYY HH:MM" в формат PostgreSQL
                start_date = None
                if row.get('StartDate'):
                    try:
                        date_obj = datetime.strptime(row['StartDate'], "%d.%m.%Y %H:%M")
                        start_date = date_obj.strftime("%Y-%m-%d %H:%M:%S")
                    except ValueError as e:
                        logger.warning(f"Ошибка преобразования StartDate: {e}")
                
                publish_date = None
                if row.get('PublishDate'):
                    try:
                        date_obj = datetime.strptime(row['PublishDate'], "%d.%m.%Y %H:%M")
                        publish_date = date_obj.strftime("%Y-%m-%d %H:%M:%S")
                    except ValueError as e:
                        logger.warning(f"Ошибка преобразования PublishDate: {e}")
                
                start_price = float(row['StartPrice']) if row.get('StartPrice') else None
                min_price = float(row['MinPrice']) if row.get('MinPrice') else None
                guarantee = float(row['GuaranteePaymentAmount']) if row.get('GuaranteePaymentAmount') else None
                min_participants = int(row['MinParticipantsCount']) if row.get('MinParticipantsCount') else None
                win_price = float(row['WinPrice']) if row.get('WinPrice') else None
                participants_count = int(row['ParticipantsCount']) if row.get('ParticipantsCount') else None
                
                # Вставка данных
                logger.info(f"Вставка данных для AuctionId: {auction_id}, StartDate: {start_date}, PublishDate: {publish_date}")
                cur.execute("""
                    INSERT INTO auction.trades
                    (auction_id, auction_type, start_date, publish_date, start_price, min_price,
                    guarantee_payment_amount, pay_period, min_participants_count, publish_note_ru,
                    publish_note_kz, payments_recipient_info_ru, payments_recipient_info_kz,
                    note_ru, note_kz, auction_status, win_price, participants_count)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                """, (
                    auction_id,
                    row.get('AuctionType', ''),
                    start_date,
                    publish_date,
                    start_price,
                    min_price,
                    guarantee,
                    row.get('PayPeriod', ''),
                    min_participants,
                    row.get('PublishNoteRu', ''),
                    row.get('PublishNoteKz', ''),
                    row.get('PaymentsRecipientInfoRu', ''),
                    row.get('PaymentsRecipientInfoKz', ''),
                    row.get('NoteRu', ''),
                    row.get('NoteKz', ''),
                    row.get('AuctionStatus', ''),
                    win_price,
                    participants_count
                ))
                rows_inserted += 1
            
            # Фиксируем транзакцию
            conn.commit()
            
            # Проверка количества записей
            cur.execute("SELECT COUNT(*) FROM auction.trades")
            count = cur.fetchone()[0]
            logger.info(f"Количество записей в таблице после вставки: {count}")
            
            # Закрытие соединения
            cur.close()
            conn.close()
            
            logger.info(f"Успешно вставлено {rows_inserted} записей в таблицу auction.trades")
            return True
    except Exception as e:
        logger.error(f"Ошибка при прямой вставке данных: {str(e)}")
        # В случае ошибки всё равно запускаем оригинальный скрипт
        try:
            # Запуск скрипта загрузки данных
            result = subprocess.run(
                [
                    "python", 
                    f"{SCRIPTS_DIR}/data_loader.py", 
                    "--source", "auction", 
                    "--file", csv_file,
                    "--host", "gosreestr-db",
                    "--port", "5432",
                    "--dbname", "gosreestr_db",
                    "--user", "data_manager",
                    "--password", "strong_password_here"
                ],
                check=True,
                capture_output=True,
                text=True
            )
            
            logger.info(f"Загрузка данных электронных торгов в БД успешно выполнена: {result.stdout}")
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"Ошибка при загрузке данных электронных торгов в БД: {e.stderr}")
            raise
# Создание DAG
with DAG(
    'gosreestr_and_auction_etl',
    default_args=default_args,
    description='DAG для сбора и загрузки данных из Госреестра и системы электронных торгов',
    schedule_interval='0 19 * * *',  # Запуск каждый день в 00:00
    start_date=datetime(2025, 3, 1),
    catchup=False,
    tags=['gosreestr', 'auction', 'etl'],
) as dag:

    # Задачи
    task_gosreestr_parser = PythonOperator(
        task_id='run_gosreestr_parser',
        python_callable=run_gosreestr_parser,
        provide_context=True,
    )
    
    task_auction_parser = PythonOperator(
        task_id='run_auction_parser',
        python_callable=run_auction_parser,
        provide_context=True,
    )
    
    task_load_gosreestr_to_db = PythonOperator(
        task_id='load_gosreestr_to_db',
        python_callable=load_gosreestr_to_db,
        provide_context=True,
    )
    
    task_load_auction_to_db = PythonOperator(
        task_id='load_auction_to_db',
        python_callable=load_auction_to_db,
        provide_context=True,
    )
    
    # Загрузка в data.opengov.kz будет реализована позже, пока оставляем заглушку
    task_load_to_opengov = BashOperator(
        task_id='load_to_opengov',
        bash_command='echo "Загрузка в data.opengov.kz будет реализована позже"',
    )
    
    # Определение порядка выполнения задач
    task_gosreestr_parser >> task_load_gosreestr_to_db
    task_auction_parser >> task_load_auction_to_db
    
    # Загрузка в data.opengov.kz будет зависеть только от загрузки Госреестра
    task_load_gosreestr_to_db >> task_load_to_opengov
