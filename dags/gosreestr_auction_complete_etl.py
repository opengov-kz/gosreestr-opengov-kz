"""
DAG для сбора и загрузки данных из Госреестра и системы электронных торгов
в нормализованную базу данных, а также публикации на портале data.opengov.kz
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
import glob

# Настройка логирования
logger = logging.getLogger(__name__)

# Определение путей
BASE_DIR = "/opt/airflow"
DATA_DIR = f"{BASE_DIR}/data"
SCRIPTS_DIR = f"{BASE_DIR}/scripts"

# Создание директорий, если они не существуют
os.makedirs(DATA_DIR, exist_ok=True)

# Параметры для загрузки на data.opengov.kz
# Эти параметры можно хранить в переменных Airflow
OPENGOV_API_URL = Variable.get("opengov_api_url", default_var="https://data.opengov.kz/api/3")
OPENGOV_API_KEY = Variable.get("opengov_api_key", default_var="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJqdGkiOiJVUVgxM0xCN3Zfbk9HOFRDWmVjZEJZQU1CaGVkM1BSU3ZlZ1NrNGRBSldnIiwiaWF0IjoxNzQ1NTA1NzkyfQ.3JgDgZKTIKc6_9H7M0AJ2UypuugGC6fNoMQki2aS3w4")
GOSREESTR_DATASET_ID = Variable.get("gosreestr_dataset_id", default_var="gosreestr_objects")
AUCTION_DATASET_ID = Variable.get("auction_dataset_id", default_var="auction_trades")
ORGANIZATION_ID = Variable.get("organization_id", default_var="state-property-research")

# Конфигурация по умолчанию
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def find_file_by_pattern(pattern):
    """Поиск файла по шаблону в разных директориях"""
    # Список возможных директорий
    directories = [
        DATA_DIR,
        BASE_DIR,
        "/tmp",
        "/",
        ".",
        "..",
        "/home/airflow",
        os.getcwd()
    ]
    
    for directory in directories:
        # Избегаем ошибок из-за отсутствия прав
        try:
            path_pattern = os.path.join(directory, pattern)
            found_files = glob.glob(path_pattern)
            if found_files:
                logger.info(f"Найден файл по шаблону {pattern} в директории {directory}: {found_files[0]}")
                return found_files[0]
        except Exception as e:
            logger.debug(f"Ошибка при поиске в {directory}: {e}")
    
    return None

def run_gosreestr_parser(**context):
    """Запуск парсера для списка активных объектов Госреестра"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f"gosreestr_objects_{timestamp}.csv"
    output_file = os.path.join(DATA_DIR, output_filename)
    
    try:
        # Запуск скрипта парсера с явным указанием выходного файла
        logger.info(f"Запуск парсера госреестра с выходным файлом: {output_file}")
        
        # Запускаем процесс как подпроцесс с перехватом вывода
        result = subprocess.run(
            ["python", f"{SCRIPTS_DIR}/gosreestr_parser.py", "--output", output_file],
            check=True,
            capture_output=True,
            text=True
        )
        
        logger.info(f"Парсер Госреестра успешно выполнен. Stdout: {result.stdout}")
        if result.stderr:
            logger.warning(f"Stderr парсера: {result.stderr}")
        
        # Проверка наличия файла
        if not os.path.exists(output_file):
            logger.warning(f"Файл {output_file} не был создан парсером")
            
            # Поиск файла по шаблону во всей системе
            pattern = f"*{output_filename}"
            found_file = find_file_by_pattern(pattern)
            
            if found_file:
                logger.info(f"Найден файл в другом месте: {found_file}")
                # Копируем файл в нужную директорию
                shutil.copy(found_file, output_file)
                logger.info(f"Файл скопирован из {found_file} в {output_file}")
            else:
                # Если файл не найден, но парсер отработал успешно, создаем пустой файл
                # для продолжения процесса и записываем предупреждение
                logger.error("Файл не найден ни в одной директории. Создаем пустой файл для продолжения.")
                with open(output_file, 'w') as f:
                    f.write("flBin,flNameRu,flOpf,flOkedL0,flStateInvolvement,flStatus,flKfsL0,flKfsL1,flKfsL2,flOkedL3,flOwnerBin,flOguBin\n")
        else:
            logger.info(f"Файл {output_file} успешно создан")
            
            # Выводим первые несколько строк файла для отладки
            try:
                with open(output_file, 'r', encoding='utf-8') as f:
                    head = ''.join([next(f) for _ in range(5)])
                logger.info(f"Первые строки файла: {head}")
            except Exception as e:
                logger.warning(f"Не удалось прочитать файл для отладки: {e}")
        
        # Сохранение пути к файлу для использования в следующих задачах
        context['ti'].xcom_push(key='gosreestr_csv_file', value=output_file)
        
        return output_file
    except Exception as e:
        logger.error(f"Ошибка при выполнении парсера Госреестра: {str(e)}")
        # В случае ошибки создаем пустой файл для продолжения процесса
        logger.info("Создание пустого файла в случае ошибки парсера")
        with open(output_file, 'w') as f:
            f.write("flBin,flNameRu,flOpf,flOkedL0,flStateInvolvement,flStatus,flKfsL0,flKfsL1,flKfsL2,flOkedL3,flOwnerBin,flOguBin\n")
        context['ti'].xcom_push(key='gosreestr_csv_file', value=output_file)
        raise

def run_auction_parser(**context):
    """Запуск парсера для списка электронных торгов"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f"auction_trades_{timestamp}.csv"
    output_file = os.path.join(DATA_DIR, output_filename)
    
    try:
        # Запуск скрипта парсера с явным указанием выходного файла
        logger.info(f"Запуск парсера аукционов с выходным файлом: {output_file}")
        
        # Запускаем процесс как подпроцесс с перехватом вывода
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
        
        logger.info(f"Парсер электронных торгов успешно выполнен. Stdout: {result.stdout}")
        if result.stderr:
            logger.warning(f"Stderr парсера: {result.stderr}")
        
        # Проверка наличия файла
        if not os.path.exists(output_file):
            logger.warning(f"Файл {output_file} не был создан парсером")
            
            # Поиск файла по шаблону во всей системе
            pattern = f"*{output_filename}"
            found_file = find_file_by_pattern(pattern)
            
            if found_file:
                logger.info(f"Найден файл в другом месте: {found_file}")
                # Копируем файл в нужную директорию
                shutil.copy(found_file, output_file)
                logger.info(f"Файл скопирован из {found_file} в {output_file}")
            else:
                # Если файл не найден, но парсер отработал успешно, создаем пустой файл
                # для продолжения процесса и записываем предупреждение
                logger.error("Файл не найден ни в одной директории. Создаем пустой файл для продолжения.")
                with open(output_file, 'w') as f:
                    f.write("AuctionId,AuctionStatus,AuctionType,GuaranteePaymentAmount,MinParticipantsCount,MinPrice,Object_NameRu,StartDate,StartPrice,WinPrice\n")
        else:
            logger.info(f"Файл {output_file} успешно создан")
            
            # Выводим первые несколько строк файла для отладки
            try:
                with open(output_file, 'r', encoding='utf-8') as f:
                    head = ''.join([next(f) for _ in range(5)])
                logger.info(f"Первые строки файла: {head}")
            except Exception as e:
                logger.warning(f"Не удалось прочитать файл для отладки: {e}")
        
        # Сохранение пути к файлу для использования в следующих задачах
        context['ti'].xcom_push(key='auction_csv_file', value=output_file)
        
        return output_file
    except Exception as e:
        logger.error(f"Ошибка при выполнении парсера электронных торгов: {str(e)}")
        # В случае ошибки создаем пустой файл для продолжения процесса
        logger.info("Создание пустого файла в случае ошибки парсера")
        with open(output_file, 'w') as f:
            f.write("AuctionId,AuctionStatus,AuctionType,GuaranteePaymentAmount,MinParticipantsCount,MinPrice,Object_NameRu,StartDate,StartPrice,WinPrice\n")
        context['ti'].xcom_push(key='auction_csv_file', value=output_file)
        raise

def load_gosreestr_to_db(**context):
    """Загрузка данных Госреестра в нормализованную БД"""
    # Получение пути к файлу из предыдущей задачи
    csv_file = context['ti'].xcom_pull(task_ids='run_gosreestr_parser', key='gosreestr_csv_file')
    
    if not csv_file:
        logger.error("Не получен путь к CSV файлу из предыдущей задачи")
        raise ValueError("Файл CSV не найден: путь не получен")
    
    if not os.path.exists(csv_file):
        logger.error(f"Файл {csv_file} не существует")
        raise ValueError(f"Файл CSV не найден по пути: {csv_file}")
    
    logger.info(f"Получен путь к файлу CSV для загрузки: {csv_file}")
    
    # Проверка размера файла
    file_size = os.path.getsize(csv_file)
    logger.info(f"Размер файла: {file_size} байт")
    
    if file_size == 0:
        logger.warning("Файл пустой. Создаем минимальный файл с заголовками")
        with open(csv_file, 'w') as f:
            f.write("flBin,flNameRu,flOpf,flOkedL0,flStateInvolvement,flStatus,flKfsL0,flKfsL1,flKfsL2,flOkedL3,flOwnerBin,flOguBin\n")
    
    try:
        # Запуск скрипта загрузки данных с расширенным выводом
        logger.info(f"Запуск скрипта загрузки данных с файлом: {csv_file}")
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
        
        logger.info("Загрузка данных Госреестра в БД успешно выполнена")
        logger.info(f"Stdout: {result.stdout}")
        if result.stderr:
            logger.warning(f"Stderr: {result.stderr}")
        
        # Сохраняем путь к файлу для следующего шага загрузки на data.opengov.kz
        context['ti'].xcom_push(key='gosreestr_processed_csv_file', value=csv_file)
        
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Ошибка при загрузке данных Госреестра в БД: {e}")
        logger.error(f"Stdout: {e.stdout}")
        logger.error(f"Stderr: {e.stderr}")
        raise

def load_auction_to_db(**context):
    """Загрузка данных электронных торгов в нормализованную БД"""
    # Получение пути к файлу из предыдущей задачи
    csv_file = context['ti'].xcom_pull(task_ids='run_auction_parser', key='auction_csv_file')
    
    if not csv_file:
        logger.error("Не получен путь к CSV файлу из предыдущей задачи")
        raise ValueError("Файл CSV не найден: путь не получен")
    
    if not os.path.exists(csv_file):
        logger.error(f"Файл {csv_file} не существует")
        raise ValueError(f"Файл CSV не найден по пути: {csv_file}")
    
    logger.info(f"Получен путь к файлу CSV для загрузки: {csv_file}")
    
    # Проверка размера файла
    file_size = os.path.getsize(csv_file)
    logger.info(f"Размер файла: {file_size} байт")
    
    if file_size == 0:
        logger.warning("Файл пустой. Создаем минимальный файл с заголовками")
        with open(csv_file, 'w') as f:
            f.write("AuctionId,AuctionStatus,AuctionType,GuaranteePaymentAmount,MinParticipantsCount,MinPrice,NoteRu,NoteKz,Object_BalanceholderInfoRu,Object_BalanceholderInfoKz,Object_DescriptionRu,Object_DescriptionKz,Object_MetaData,Object_NameRu,Object_NameKz,Object_ObjectAdrAdr,Object_ObjectAdrCountry,Object_ObjectAdrObl,Object_ObjectAdrReg,Object_ObjectType,Object_SellerAdrAdr,Object_SellerAdrCountry,Object_SellerAdrObl,Object_SellerAdrReg,Object_SellerInfoRu,Object_SellerInfoKz,Object_SellerPhoneRu,Object_SellerPhoneKz,Object_SellerXin,ParticipantsCount,PayPeriod,PaymentsRecipientInfoRu,PaymentsRecipientInfoKz,PublishDate,PublishNoteRu,PublishNoteKz,StartDate,StartPrice,WinPrice\n")
    
    try:
        # Запуск скрипта загрузки данных с расширенным выводом
        logger.info(f"Запуск скрипта загрузки данных с файлом: {csv_file}")
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
        
        logger.info("Загрузка данных электронных торгов в БД успешно выполнена")
        logger.info(f"Stdout: {result.stdout}")
        if result.stderr:
            logger.warning(f"Stderr: {result.stderr}")
        
        # Сохраняем путь к файлу для следующего шага загрузки на data.opengov.kz
        context['ti'].xcom_push(key='auction_processed_csv_file', value=csv_file)
        
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Ошибка при загрузке данных электронных торгов в БД: {e}")
        logger.error(f"Stdout: {e.stdout}")
        logger.error(f"Stderr: {e.stderr}")
        raise

def upload_gosreestr_to_opengov(**context):
    """Загрузка данных Госреестра на портал data.opengov.kz"""
    # Получение пути к файлу из предыдущей задачи
    csv_file = context['ti'].xcom_pull(task_ids='load_gosreestr_to_db', key='gosreestr_processed_csv_file')
    
    if not csv_file:
        # Попробуем получить путь из задачи парсинга
        csv_file = context['ti'].xcom_pull(task_ids='run_gosreestr_parser', key='gosreestr_csv_file')
    
    if not csv_file:
        logger.error("Не получен путь к CSV файлу из предыдущих задач")
        raise ValueError("Файл CSV не найден: путь не получен")
    
    if not os.path.exists(csv_file):
        logger.error(f"Файл {csv_file} не существует")
        raise ValueError(f"Файл CSV не найден по пути: {csv_file}")
    
    logger.info(f"Получен путь к файлу CSV для загрузки на OpenGov: {csv_file}")
    
    try:
        # Запуск скрипта загрузки данных на портал data.opengov.kz
        logger.info(f"Запуск скрипта загрузки данных на OpenGov с файлом: {csv_file}")
        result = subprocess.run(
            [
                "python", 
                f"{SCRIPTS_DIR}/opengov_loader.py", 
                "--file", csv_file,
                "--dataset", GOSREESTR_DATASET_ID,
                "--name", "Данные Госреестра",
                "--description", f"Данные из Госреестра по состоянию на {datetime.now().strftime('%d.%m.%Y')}",
                "--api-url", OPENGOV_API_URL,
                "--api-key", OPENGOV_API_KEY,
                "--strategy", "update_latest"
            ],
            check=True,
            capture_output=True,
            text=True
        )
        
        logger.info("Загрузка данных Госреестра на OpenGov успешно выполнена")
        logger.info(f"Stdout: {result.stdout}")
        if result.stderr:
            logger.warning(f"Stderr: {result.stderr}")
        
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Ошибка при загрузке данных Госреестра на OpenGov: {e}")
        logger.error(f"Stdout: {e.stdout}")
        logger.error(f"Stderr: {e.stderr}")
        # Не вызываем raise, чтобы ошибка на этом шаге не остановила весь пайплайн
        return False

def upload_auction_to_opengov(**context):
    """Загрузка данных электронных торгов на портал data.opengov.kz"""
    # Получение пути к файлу из предыдущей задачи
    csv_file = context['ti'].xcom_pull(task_ids='load_auction_to_db', key='auction_processed_csv_file')
    
    if not csv_file:
        # Попробуем получить путь из задачи парсинга
        csv_file = context['ti'].xcom_pull(task_ids='run_auction_parser', key='auction_csv_file')
    
    if not csv_file:
        logger.error("Не получен путь к CSV файлу из предыдущих задач")
        raise ValueError("Файл CSV не найден: путь не получен")
    
    if not os.path.exists(csv_file):
        logger.error(f"Файл {csv_file} не существует")
        raise ValueError(f"Файл CSV не найден по пути: {csv_file}")
    
    logger.info(f"Получен путь к файлу CSV для загрузки на OpenGov: {csv_file}")
    
    try:
        # Запуск скрипта загрузки данных на портал data.opengov.kz
        logger.info(f"Запуск скрипта загрузки данных на OpenGov с файлом: {csv_file}")
        result = subprocess.run(
            [
                "python", 
                f"{SCRIPTS_DIR}/opengov_loader.py", 
                "--file", csv_file,
                "--dataset", AUCTION_DATASET_ID,
                "--name", "Данные электронных торгов",
                "--description", f"Данные из системы электронных торгов по состоянию на {datetime.now().strftime('%d.%m.%Y')}",
                "--api-url", OPENGOV_API_URL,
                "--api-key", OPENGOV_API_KEY,
                "--strategy", "update_latest"
            ],
            check=True,
            capture_output=True,
            text=True
        )
        
        logger.info("Загрузка данных электронных торгов на OpenGov успешно выполнена")
        logger.info(f"Stdout: {result.stdout}")
        if result.stderr:
            logger.warning(f"Stderr: {result.stderr}")
        
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Ошибка при загрузке данных электронных торгов на OpenGov: {e}")
        logger.error(f"Stdout: {e.stdout}")
        logger.error(f"Stderr: {e.stderr}")
        # Не вызываем raise, чтобы ошибка на этом шаге не остановила весь пайплайн
        return False

# Создание DAG
with DAG(
    'gosreestr_auction_complete_etl',
    default_args=default_args,
    description='DAG для сбора и загрузки данных из Госреестра и системы электронных торгов в БД и на data.opengov.kz',
    schedule_interval='0 1 * * *',  # Запуск каждый день в 01:00
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['gosreestr', 'auction', 'etl', 'normalized', 'opengov'],
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
    
    task_upload_gosreestr_to_opengov = PythonOperator(
        task_id='upload_gosreestr_to_opengov',
        python_callable=upload_gosreestr_to_opengov,
        provide_context=True,
    )
    
    task_upload_auction_to_opengov = PythonOperator(
        task_id='upload_auction_to_opengov',
        python_callable=upload_auction_to_opengov,
        provide_context=True,
    )
    
    # Определение порядка выполнения задач
    task_gosreestr_parser >> task_load_gosreestr_to_db >> task_upload_gosreestr_to_opengov
    task_auction_parser >> task_load_auction_to_db >> task_upload_auction_to_opengov