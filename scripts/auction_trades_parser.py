#!/usr/bin/env python
import argparse
import csv
import json
import logging
import os
import sys
from datetime import datetime
import time
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
    log_file = os.path.join(log_dir, f"auction_trades_parser_{timestamp}.log")

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )

    return logging.getLogger('auction_trades_parser')


logger = logging.getLogger('auction_trades_parser')


class AuctionTradesParser:
    def __init__(self,
                 base_url="https://e-auction.e-qazyna.kz/p/ru",
                 version="v1",
                 page_size=100):
        """
        Инициализация парсера API электронных торгов

        Args:
            base_url (str): Базовый URL API
            version (str): Версия API (v1 или v2)
            page_size (int): Количество записей на страницу
        """
        self.base_url = base_url
        self.version = version
        self.page_size = page_size
        self.headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }

        # URL для разных версий API
        self.api_urls = {
            "v1": f"{base_url}/api/v1/auction-trades",
            "v2": f"{base_url}/api/v2/auction-trades/token"
        }

    def fetch_data_v1(self, page_number=1, status="AcceptingApplications", limit=None):
        """
        Получение данных из API v1

        Args:
            page_number (int): Номер страницы
            status (str): Статус торгов (например, "AcceptingApplications")
            limit (int): Количество записей на страницу

        Returns:
            dict: Ответ API в формате JSON
        """
        if limit is None:
            limit = self.page_size

        url = f"{self.api_urls['v1']}?PageNumber={page_number}&Limit={limit}&Status={status}"

        try:
            logger.info(f"Отправка запроса к {url}")
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка при запросе к API v1: {e}")
            raise

    def fetch_data_v2(self, token):
        """
        Получение данных из API v2 по токену

        Args:
            token (str): Токен для доступа к данным

        Returns:
            dict: Ответ API в формате JSON
        """
        url = f"{self.api_urls['v2']}?token={token}"

        try:
            logger.info(f"Отправка запроса к {url}")
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка при запросе к API v2: {e}")
            raise

    def fetch_all_pages(self, status="AcceptingApplications", max_pages=100):
        """
        Получение всех страниц данных из API v1

        Args:
            status (str): Статус торгов
            max_pages (int): Максимальное количество страниц для загрузки

        Returns:
            list: Список всех полученных торгов
        """
        all_trades = []
        current_page = 1
        total_count = None

        while True:
            data = self.fetch_data_v1(page_number=current_page, status=status)

            # Для первой страницы получаем общее количество торгов
            if current_page == 1:
                total_count = data.get('TradesCount', 0)
                logger.info(f"Всего торгов: {total_count}")

            trades = data.get('Trades', [])
            if not trades:
                logger.info(f"Получена пустая страница {current_page}, завершаем загрузку")
                break

            all_trades.extend(trades)
            logger.info(
                f"Получено {len(trades)} торгов на странице {current_page}. Всего загружено: {len(all_trades)}/{total_count}")

            # Проверяем, есть ли еще страницы
            if len(all_trades) >= total_count or current_page >= max_pages:
                break

            current_page += 1
            # Небольшая задержка, чтобы не нагружать сервер
            time.sleep(0.5)

        return all_trades

    def save_to_csv(self, trades, csv_path):
        """
        Сохранение данных в CSV файл

        Args:
            trades (list): Список торгов
            csv_path (str): Путь к файлу для сохранения
        """
        try:
            if not trades:
                logger.warning("Список торгов пуст")
                return

            # Определение заголовков CSV из всех ключей всех объектов
            fieldnames = set()
            for trade in trades:
                fieldnames.update(trade.keys())

            fieldnames = sorted(list(fieldnames))

            logger.info(f"Сохранение {len(trades)} торгов в {csv_path}")

            with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(trades)

            logger.info(f"Данные успешно сохранены в {csv_path}")
        except Exception as e:
            logger.error(f"Ошибка при сохранении данных в CSV: {e}")
            raise

    def run(self, status="AcceptingApplications", token=None, csv_path=None):
        """
        Основной метод для выполнения парсинга и сохранения

        Args:
            status (str): Статус торгов для API v1
            token (str): Токен для API v2, если None, используется API v1
            csv_path (str): Путь к файлу для сохранения
        """
        try:
            logger.info("Начало работы парсера электронных торгов")

            if csv_path is None:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                csv_path = f"auction_trades_{timestamp}.csv"

            # Выбор API в зависимости от наличия токена
            if token:
                logger.info("Использование API v2 с токеном")
                data = self.fetch_data_v2(token)
                trades = data.get('Trades', [])
            else:
                logger.info(f"Использование API v1 для получения всех торгов со статусом '{status}'")
                trades = self.fetch_all_pages(status=status)

            self.save_to_csv(trades, csv_path)
            logger.info("Парсер электронных торгов успешно завершил работу")

            return trades
        except Exception as e:
            logger.error(f"Ошибка в работе парсера: {e}")
            raise


if __name__ == "__main__":
    # Настройка логирования
    logger = setup_logging()

    try:
        # Настройка аргументов командной строки
        parser = argparse.ArgumentParser(description="Парсер API электронных торгов")
        parser.add_argument("--status", default="AcceptingApplications", help="Статус торгов для API v1")
        parser.add_argument("--token", help="Токен для API v2 (если не указан, используется API v1)")
        parser.add_argument("--output", help="Путь к файлу для сохранения результатов")
        parser.add_argument("--page-size", type=int, default=100, help="Количество записей на страницу")
        parser.add_argument("--api-version", choices=["v1", "v2"], default="v1", help="Версия API")

        args = parser.parse_args()

        # Имя файла с датой и временем для избежания перезаписи
        if args.output:
            csv_filename = args.output
        else:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            csv_filename = f"auction_trades_{timestamp}.csv"

        logger.info(f"Файл будет сохранен по пути: {csv_filename}")

        # Запуск парсера
        parser = AuctionTradesParser(version=args.api_version, page_size=args.page_size)
        parser.run(status=args.status, token=args.token, csv_path=csv_filename)
    except Exception as e:
        logger.critical(f"Критическая ошибка: {e}")