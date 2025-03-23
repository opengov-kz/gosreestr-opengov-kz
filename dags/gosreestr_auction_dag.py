"""
DAG для сбора и загрузки данных из Госреестра и системы электронных торгов в нормализованную БД
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

# Настройки для подключения к БД
DB_HOST = "gosreestr-db"
DB_PORT = 5432
DB_NAME = "gosreestr_db"
DB_USER = "data_manager"
DB_PASSWORD = "strong_password_here"

# Конфигурация по умолчанию
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# Функции для выполнения задач
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
                shutil.copy(root_file, output_file)
                logger.info(f"Файл скопирован из {root_file} в {output_file}")

        # Сохранение пути к файлу для использования в следующих задачах
        context['ti'].xcom_push(key='auction_csv_file', value=output_file)

        return output_file
    except Exception as e:
        logger.error(f"Ошибка при выполнении парсера электронных торгов: {str(e)}")
        raise


def load_gosreestr_to_db(**context):
    """Загрузка данных Госреестра в нормализованную БД"""
    # Получение пути к файлу из предыдущей задачи
    csv_file = context['ti'].xcom_pull(task_ids='run_gosreestr_parser', key='gosreestr_csv_file')

    logger.info(f"Получен путь к файлу CSV: {csv_file}")

    # Проверка наличия файла
    if not csv_file or not os.path.exists(csv_file):
        # Поиск файла по шаблону
        import glob
        files = glob.glob(f"{DATA_DIR}/gosreestr_objects_*.csv")
        if files:
            csv_file = sorted(files)[-1]  # Берем самый новый файл
            logger.info(f"Найден альтернативный файл: {csv_file}")
        else:
            raise ValueError(f"Файл CSV не найден: {csv_file}")

    try:
        # Запуск скрипта загрузки данных
        logger.info(f"Запуск скрипта загрузки с файлом: {csv_file}")
        result = subprocess.run(
            [
                "python",
                f"{SCRIPTS_DIR}/data_loader.py",
                "--source", "gosreestr",
                "--file", csv_file,
                "--host", DB_HOST,
                "--port", str(DB_PORT),
                "--dbname", DB_NAME,
                "--user", DB_USER,
                "--password", DB_PASSWORD
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
    """Загрузка данных электронных торгов в нормализованную БД"""
    # Получение пути к файлу из предыдущей задачи
    csv_file = context['ti'].xcom_pull(task_ids='run_auction_parser', key='auction_csv_file')

    logger.info(f"Получен путь к файлу CSV: {csv_file}")

    # Проверка наличия файла
    if not csv_file or not os.path.exists(csv_file):
        # Поиск файла по шаблону
        import glob
        files = glob.glob(f"{DATA_DIR}/auction_trades_*.csv")
        if files:
            csv_file = sorted(files)[-1]  # Берем самый новый файл
            logger.info(f"Найден альтернативный файл: {csv_file}")
        else:
            raise ValueError(f"Файл CSV не найден: {csv_file}")

    try:
        # Запуск скрипта загрузки данных
        logger.info(f"Запуск скрипта загрузки с файлом: {csv_file}")
        result = subprocess.run(
            [
                "python",
                f"{SCRIPTS_DIR}/data_loader.py",
                "--source", "auction",
                "--file", csv_file,
                "--host", DB_HOST,
                "--port", str(DB_PORT),
                "--dbname", DB_NAME,
                "--user", DB_USER,
                "--password", DB_PASSWORD
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


def generate_report(**context):
    """Генерация отчета о загруженных данных"""
    import psycopg2

    try:
        logger.info("Подключение к БД для генерации отчета")
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = conn.cursor()

        # Получение статистики по данным госреестра
        cursor.execute("SELECT COUNT(*) FROM gosreestr.organizations")
        org_count = cursor.fetchone()[0]

        cursor.execute("""
            SELECT COUNT(*) FROM gosreestr.organizations 
            WHERE updated_at >= NOW() - INTERVAL '1 day'
        """)
        new_org_count = cursor.fetchone()[0]

        # Получение статистики по данным аукционов
        cursor.execute("SELECT COUNT(*) FROM auction.auctions")
        auction_count = cursor.fetchone()[0]

        cursor.execute("""
            SELECT COUNT(*) FROM auction.auctions 
            WHERE updated_at >= NOW() - INTERVAL '1 day'
        """)
        new_auction_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM auction.objects")
        object_count = cursor.fetchone()[0]

        # Составление отчета
        report = f"""
        Отчет о загруженных данных (от {datetime.now().strftime('%Y-%m-%d %H:%M:%S')})

        Данные Госреестра:
        - Всего организаций: {org_count}
        - Новых/обновленных за последние 24 часа: {new_org_count}

        Данные аукционов:
        - Всего аукционов: {auction_count}
        - Новых/обновленных за последние 24 часа: {new_auction_count}
        - Всего объектов аукционов: {object_count}
        """

        # Сохранение отчета в файл
        report_file = f"{DATA_DIR}/report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(report)

        logger.info(f"Отчет сохранен в файл {report_file}")

        # Закрытие соединения
        cursor.close()
        conn.close()

        return report_file
    except Exception as e:
        logger.error(f"Ошибка при генерации отчета: {str(e)}")
        raise


# Создание DAG
with DAG(
        'gosreestr_and_auction_normalized_etl',
        default_args=default_args,
        description='DAG для сбора и загрузки данных в нормализованную БД',
        schedule_interval='0 1 * * *',  # Запуск каждый день в 01:00
        start_date=datetime(2023, 1, 1),
        catchup=False,
        tags=['gosreestr', 'auction', 'etl', 'normalized'],
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

    task_generate_report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report,
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

    # Генерация отчета после загрузки всех данных
    [task_load_gosreestr_to_db, task_load_auction_to_db] >> task_generate_report

    # Загрузка в data.opengov.kz будет зависеть только от загрузки Госреестра
    task_load_gosreestr_to_db >> task_load_to_opengov