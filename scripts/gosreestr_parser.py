#!/usr/bin/env python
import argparse
import csv
import json
import logging
import os
from datetime import datetime
import requests


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
    log_file = os.path.join(log_dir, f"gosreestr_parser_{timestamp}.log")

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )

    return logging.getLogger('gosreestr_parser')


logger = logging.getLogger('gosreestr_parser')


class GosreestrParser:
    def __init__(self, api_url="https://gr5.e-qazyna.kz/p/ru/api/v1/gr-objects"):
        """
        Инициализация парсера Госреестра

        Args:
            api_url (str): URL API для получения данных
        """
        self.api_url = api_url
        self.headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }

    def fetch_data(self):
        """
        Получение данных из API

        Returns:
            dict: Ответ API в формате JSON
        """
        try:
            logger.info(f"Отправка запроса к {self.api_url}")
            response = requests.get(self.api_url, headers=self.headers)
            response.raise_for_status()  # Проверка на ошибки HTTP

            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка при запросе к API: {e}")
            raise

    def save_to_csv(self, data, csv_path):
        """
        Сохранение данных в CSV файл

        Args:
            data (dict): Данные для сохранения
            csv_path (str): Путь к файлу для сохранения
        """
        try:
            if 'Objects' not in data:
                logger.error("В ответе API отсутствует ключ 'Objects'")
                return

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

            logger.info(f"Данные успешно сохранены в {csv_path}")
        except Exception as e:
            logger.error(f"Ошибка при сохранении данных в CSV: {e}")
            raise

    def run(self, csv_path):
        """
        Основной метод для выполнения парсинга и сохранения

        Args:
            csv_path (str): Путь к файлу для сохранения
        """
        try:
            logger.info("Начало работы парсера Госреестра")
            data = self.fetch_data()

            # Проверка статуса ответа
            if data.get('StatusCode') != "OK":
                logger.error(f"Ошибка в ответе API: {data.get('StatusText', 'Неизвестная ошибка')}")
                return

            self.save_to_csv(data, csv_path)
            logger.info("Парсер Госреестра успешно завершил работу")
            return data
        except Exception as e:
            logger.error(f"Ошибка в работе парсера: {e}")
            raise


if __name__ == "__main__":
    # Настройка логирования
    logger = setup_logging()

    try:
        # Настройка аргументов командной строки
        parser = argparse.ArgumentParser(description="Парсер API госреестра")
        parser.add_argument("--output", help="Путь для сохранения файла CSV", required=False)

        args = parser.parse_args()

        # Имя файла с датой и временем для избежания перезаписи
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        if args.output:
            csv_filename = args.output
        else:
            csv_filename = f"gosreestr_objects_{timestamp}.csv"

        logger.info(f"Файл будет сохранен по пути: {csv_filename}")

        # Запуск парсера
        parser = GosreestrParser()
        parser.run(csv_filename)
    except Exception as e:
        logger.critical(f"Критическая ошибка: {e}")