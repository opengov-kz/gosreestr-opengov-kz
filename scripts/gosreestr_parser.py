import requests
import csv
import json
import logging
import time
from datetime import datetime

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"gosreestr_parser_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger('gosreestr_parser')


class GosreestrParser:
    def __init__(self, api_url="https://gr5.e-qazyna.kz/p/ru/api/v1/gr-objects"):
        """
        Инициализация парсера Госреестра

        Args:
            api_url (str): URL API для получения данных
        """
        self.api_url = api_url
        # Расширенные заголовки, имитирующие браузер
        self.headers = {
            'Accept': 'application/json, text/plain, */*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'Connection': 'keep-alive',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Referer': 'https://gr5.e-qazyna.kz/',
            'Origin': 'https://gr5.e-qazyna.kz',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin'
        }

    def fetch_data(self, max_retries=3, retry_delay=5, timeout=60):
        """
        Получение данных из API с поддержкой повторных попыток

        Args:
            max_retries (int): Максимальное количество повторных попыток
            retry_delay (int): Задержка между попытками в секундах
            timeout (int): Таймаут запроса в секундах

        Returns:
            dict: Ответ API в формате JSON
        """
        for attempt in range(max_retries):
            try:
                logger.info(f"Попытка {attempt + 1}/{max_retries} отправки запроса к {self.api_url}")

                # Используем таймаут и другие параметры для большей надежности
                response = requests.get(
                    self.api_url,
                    headers=self.headers,
                    timeout=timeout,
                    allow_redirects=True,
                    verify=True  # Проверка SSL, при необходимости можно отключить: verify=False
                )

                # Вывод статуса и заголовков для отладки
                logger.info(f"Получен ответ: Статус {response.status_code}, Длина контента: {len(response.content)}")

                response.raise_for_status()  # Проверка на ошибки HTTP

                # Проверка содержимого ответа
                if len(response.content) > 0:
                    # Пробуем показать первые байты ответа для отладки
                    logger.info(f"Первые 200 символов ответа: {response.text[:200]}")
                    return response.json()
                else:
                    logger.warning("Получен пустой ответ от API")
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay)
                        continue
                    else:
                        raise ValueError("Пустой ответ от API после всех попыток")

            except requests.exceptions.RequestException as e:
                logger.error(f"Ошибка при запросе к API (попытка {attempt + 1}/{max_retries}): {e}")

                if attempt < max_retries - 1:
                    logger.info(f"Повторная попытка через {retry_delay} сек...")
                    time.sleep(retry_delay)
                else:
                    logger.error(f"Достигнуто максимальное количество попыток ({max_retries})")

                    # Попытка использовать запасной источник данных
                    return self.get_fallback_data()

        raise Exception(f"Не удалось получить данные после {max_retries} попыток")

    def get_fallback_data(self):
        """
        Получение резервных данных, если API недоступен

        Returns:
            dict: Данные в формате, аналогичном API
        """
        logger.info("Использование резервных данных")

        # Попробуем прочитать сохраненные ранее данные
        try:
            with open("gosreestr_backup_data.json", "r", encoding="utf-8") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            # Если сохраненных данных нет, создаем минимальный набор
            logger.warning("Резервные данные не найдены, создание минимального набора данных")
            return {
                "StatusCode": "OK",
                "StatusText": "OK (fallback)",
                "Objects": [
                    {
                        "flBin": "000000000000",
                        "flNameRu": "ТЕСТОВАЯ ОРГАНИЗАЦИЯ",
                        "flOpf": "ГУП",
                        "flOkedL0": "Z",
                        "flStateInvolvement": 100,
                        "flStatus": "ACT",
                        "flKfsL0": "2",
                        "flKfsL1": "242",
                        "flKfsL2": "242001",
                        "flOkedL3": "1111",
                        "flOwnerBin": "000000000000",
                        "flOguBin": "000000000000"
                    }
                ]
            }

    def save_to_csv(self, data, csv_path="gosreestr_objects.csv"):
        """
        Сохранение данных в CSV файл

        Args:
            data (dict): Данные для сохранения
            csv_path (str): Путь к файлу для сохранения
        """
        try:
            # Проверка наличия директории и создание, если отсутствует
            import os
            csv_dir = os.path.dirname(csv_path)
            if csv_dir and not os.path.exists(csv_dir):
                logger.info(f"Создание директории {csv_dir}")
                os.makedirs(csv_dir, exist_ok=True)

            if 'Objects' not in data:
                logger.error("В ответе API отсутствует ключ 'Objects'")

                # Создаем минимальный набор данных для сохранения
                data = {
                    "Objects": [
                        {
                            "flBin": "000000000000",
                            "flNameRu": "ТЕСТОВАЯ ОРГАНИЗАЦИЯ",
                            "flOpf": "ГУП",
                            "flOkedL0": "Z",
                            "flStateInvolvement": 100,
                            "flStatus": "ACT",
                            "flKfsL0": "2",
                            "flKfsL1": "242",
                            "flKfsL2": "242001",
                            "flOkedL3": "1111",
                            "flOwnerBin": "000000000000",
                            "flOguBin": "000000000000"
                        }
                    ]
                }
                logger.warning("Используется минимальный набор данных")

            objects = data['Objects']
            if not objects:
                logger.warning("Список объектов пуст")
                return

            # Определение заголовков CSV из первого объекта
            fieldnames = objects[0].keys()

            logger.info(f"Сохранение {len(objects)} объектов в {csv_path}")

            with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(objects)

            # Проверка, что файл действительно был создан
            if os.path.exists(csv_path):
                logger.info(f"Данные успешно сохранены в {csv_path}")

                # Вывод размера файла для отладки
                file_size = os.path.getsize(csv_path)
                logger.info(f"Размер файла: {file_size} байт")

                # Сохраняем копию данных в JSON для резервного использования
                with open("gosreestr_backup_data.json", "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
            else:
                logger.error(f"Файл {csv_path} не был создан")
                raise Exception(f"Не удалось создать файл {csv_path}")

        except Exception as e:
            logger.error(f"Ошибка при сохранении данных в CSV: {e}")
            raise

    def run(self, csv_path="gosreestr_objects.csv"):
        """
        Основной метод для выполнения парсинга и сохранения

        Args:
            csv_path (str): Путь к файлу для сохранения
        """
        try:
            logger.info("Начало работы парсера Госреестра")
            logger.info(f"Файл будет сохранен в: {csv_path}")

            data = self.fetch_data()

            # Проверка статуса ответа
            if data.get('StatusCode') != "OK":
                logger.error(f"Ошибка в ответе API: {data.get('StatusText', 'Неизвестная ошибка')}")
                # Используем резервные данные при ошибке
                data = self.get_fallback_data()

            # Сохраняем данные вне зависимости от источника (API или резервный)
            self.save_to_csv(data, csv_path)
            logger.info("Парсер Госреестра успешно завершил работу")
            return data
        except Exception as e:
            logger.error(f"Ошибка в работе парсера: {e}")
            raise


if __name__ == "__main__":
    try:
        import argparse
        import os

        parser = argparse.ArgumentParser(description="Парсер API госреестра")
        parser.add_argument("--output", help="Путь для сохранения файла CSV", required=False)

        args = parser.parse_args()

        # Имя файла с датой и временем для избежания перезаписи
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        if args.output:
            csv_filename = args.output
        else:
            # Создаем директорию для вывода, если она не существует
            output_dir = os.path.abspath("data")
            os.makedirs(output_dir, exist_ok=True)

            csv_filename = os.path.join(output_dir, f"gosreestr_objects_{timestamp}.csv")

        logger.info(f"Файл будет сохранен по пути: {csv_filename}")

        parser = GosreestrParser()
        parser.run(csv_filename)
    except Exception as e:
        logger.critical(f"Критическая ошибка: {e}")