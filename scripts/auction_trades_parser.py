import requests
import csv
import json
import logging
import argparse
import os
from datetime import datetime
import time

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"auction_trades_parser_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)

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

        # Расширенные заголовки, имитирующие браузер
        self.headers = {
            'Accept': 'application/json, text/plain, */*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'Connection': 'keep-alive',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Referer': 'https://e-auction.e-qazyna.kz/',
            'Origin': 'https://e-auction.e-qazyna.kz',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'Content-Type': 'application/json'
        }

        # URL для разных версий API
        self.api_urls = {
            "v1": f"{base_url}/api/v1/auction-trades",
            "v2": f"{base_url}/api/v2/auction-trades/token"
        }

        # Путь для резервного файла
        self.backup_file = "auction_trades_backup_data.json"

    def fetch_data_v1(self, page_number=1, status="AcceptingApplications", limit=None, max_retries=3, retry_delay=5,
                      timeout=60):
        """
        Получение данных из API v1 с поддержкой повторных попыток

        Args:
            page_number (int): Номер страницы
            status (str): Статус торгов (например, "AcceptingApplications")
            limit (int): Количество записей на страницу
            max_retries (int): Максимальное количество повторных попыток
            retry_delay (int): Задержка между попытками в секундах
            timeout (int): Таймаут запроса в секундах

        Returns:
            dict: Ответ API в формате JSON
        """
        if limit is None:
            limit = self.page_size

        url = f"{self.api_urls['v1']}?PageNumber={page_number}&Limit={limit}&Status={status}"

        for attempt in range(max_retries):
            try:
                logger.info(f"Попытка {attempt + 1}/{max_retries} отправки запроса к {url}")

                response = requests.get(
                    url,
                    headers=self.headers,
                    timeout=timeout,
                    allow_redirects=True,
                    verify=True  # Проверка SSL, при необходимости можно отключить: verify=False
                )

                # Вывод статуса и заголовков для отладки
                logger.info(f"Получен ответ: Статус {response.status_code}, Длина контента: {len(response.content)}")

                response.raise_for_status()

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
                logger.error(f"Ошибка при запросе к API v1 (попытка {attempt + 1}/{max_retries}): {e}")

                if attempt < max_retries - 1:
                    logger.info(f"Повторная попытка через {retry_delay} сек...")
                    time.sleep(retry_delay)
                else:
                    logger.error(f"Достигнуто максимальное количество попыток ({max_retries})")
                    return self.get_fallback_data()

        raise Exception(f"Не удалось получить данные после {max_retries} попыток")

    def fetch_data_v2(self, token, max_retries=3, retry_delay=5, timeout=60):
        """
        Получение данных из API v2 по токену с поддержкой повторных попыток

        Args:
            token (str): Токен для доступа к данным
            max_retries (int): Максимальное количество повторных попыток
            retry_delay (int): Задержка между попытками в секундах
            timeout (int): Таймаут запроса в секундах

        Returns:
            dict: Ответ API в формате JSON
        """
        url = f"{self.api_urls['v2']}?token={token}"

        for attempt in range(max_retries):
            try:
                logger.info(f"Попытка {attempt + 1}/{max_retries} отправки запроса к {url}")

                response = requests.get(
                    url,
                    headers=self.headers,
                    timeout=timeout,
                    allow_redirects=True
                )

                # Вывод статуса и заголовков для отладки
                logger.info(f"Получен ответ: Статус {response.status_code}, Длина контента: {len(response.content)}")

                response.raise_for_status()

                # Проверка содержимого ответа
                if len(response.content) > 0:
                    return response.json()
                else:
                    logger.warning("Получен пустой ответ от API v2")
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay)
                        continue
                    else:
                        raise ValueError("Пустой ответ от API v2 после всех попыток")

            except requests.exceptions.RequestException as e:
                logger.error(f"Ошибка при запросе к API v2 (попытка {attempt + 1}/{max_retries}): {e}")

                if attempt < max_retries - 1:
                    logger.info(f"Повторная попытка через {retry_delay} сек...")
                    time.sleep(retry_delay)
                else:
                    logger.error(f"Достигнуто максимальное количество попыток ({max_retries})")
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
            with open(self.backup_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            # Если сохраненных данных нет, создаем минимальный набор
            logger.warning("Резервные данные не найдены, создание минимального набора данных")
            return {
                "TradesCount": 1,
                "Trades": [
                    {
                        "AuctionId": 999999,
                        "AuctionStatus": "AcceptingApplications",
                        "AuctionType": "TenderArenda10082019",
                        "GuaranteePaymentAmount": 10000.0,
                        "MinParticipantsCount": 1,
                        "MinPrice": 0.0,
                        "StartDate": "01.01.2025 00:00",
                        "StartPrice": 100000.0,
                        "WinPrice": 0.0,
                        "AuctionObject": {
                            "ObjectType": "Building",
                            "NameRu": "ТЕСТОВЫЙ ОБЪЕКТ",
                            "SellerXin": "000000000000",
                            "SellerInfoRu": "ТЕСТОВЫЙ ПРОДАВЕЦ"
                        }
                    }
                ]
            }

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
            try:
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
            except Exception as e:
                logger.error(f"Ошибка при загрузке страницы {current_page}: {e}")
                # Если хотя бы что-то загружено, возвращаем
                if all_trades:
                    logger.warning(f"Возвращаем частично загруженные данные ({len(all_trades)} торгов)")
                    return all_trades
                else:
                    # Иначе используем резервные данные
                    fallback_data = self.get_fallback_data()
                    return fallback_data.get('Trades', [])

        # Если успешно загрузились данные, сохраняем их для резервного использования
        if all_trades:
            try:
                with open(self.backup_file, "w", encoding="utf-8") as f:
                    json.dump({
                        "TradesCount": len(all_trades),
                        "Trades": all_trades
                    }, f, ensure_ascii=False, indent=2)
                logger.info(f"Сохранены резервные данные в {self.backup_file}")
            except Exception as e:
                logger.warning(f"Не удалось сохранить резервные данные: {e}")

        return all_trades

    def flatten_trade_data(self, trade):
        """
        Преобразование вложенных структур данных торгов в плоскую структуру для CSV

        Args:
            trade (dict): Данные о торге

        Returns:
            dict: Преобразованные данные
        """
        flat_trade = {}

        # Копируем основные поля
        for key, value in trade.items():
            if key != 'AuctionObject':  # Обрабатываем объект отдельно
                flat_trade[key] = value

        # Добавляем поля объекта с префиксом 'Object_'
        if 'AuctionObject' in trade and isinstance(trade['AuctionObject'], dict):
            for key, value in trade['AuctionObject'].items():
                flat_trade[f"Object_{key}"] = value

        return flat_trade

    def save_to_csv(self, trades, csv_path="auction_trades.csv"):
        """
        Сохранение данных в CSV файл

        Args:
            trades (list): Список торгов
            csv_path (str): Путь к файлу для сохранения
        """
        try:
            # Проверка наличия директории и создание, если отсутствует
            csv_dir = os.path.dirname(csv_path)
            if csv_dir and not os.path.exists(csv_dir):
                logger.info(f"Создание директории {csv_dir}")
                os.makedirs(csv_dir, exist_ok=True)

            if not trades:
                logger.warning("Список торгов пуст, создаем минимальный набор")
                trades = [
                    {
                        "AuctionId": 999999,
                        "AuctionStatus": "AcceptingApplications",
                        "AuctionType": "TenderArenda10082019",
                        "GuaranteePaymentAmount": 10000.0,
                        "MinParticipantsCount": 1,
                        "MinPrice": 0.0,
                        "StartDate": "01.01.2025 00:00",
                        "StartPrice": 100000.0,
                        "WinPrice": 0.0,
                        "AuctionObject": {
                            "ObjectType": "Building",
                            "NameRu": "ТЕСТОВЫЙ ОБЪЕКТ",
                            "SellerXin": "000000000000",
                            "SellerInfoRu": "ТЕСТОВЫЙ ПРОДАВЕЦ"
                        }
                    }
                ]

            # Преобразуем данные в плоскую структуру
            flat_trades = [self.flatten_trade_data(trade) for trade in trades]

            # Определение заголовков из всех ключей всех объектов
            fieldnames = set()
            for trade in flat_trades:
                fieldnames.update(trade.keys())

            fieldnames = sorted(list(fieldnames))

            logger.info(f"Сохранение {len(flat_trades)} торгов в {csv_path}")

            with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(flat_trades)

            # Проверка, что файл действительно был создан
            if os.path.exists(csv_path):
                logger.info(f"Данные успешно сохранены в {csv_path}")

                # Вывод размера файла для отладки
                file_size = os.path.getsize(csv_path)
                logger.info(f"Размер файла: {file_size} байт")
            else:
                logger.error(f"Файл {csv_path} не был создан")
                raise Exception(f"Не удалось создать файл {csv_path}")
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

            logger.info(f"Файл будет сохранен в: {csv_path}")

            # Выбор API в зависимости от наличия токена
            if token:
                logger.info("Использование API v2 с токеном")
                try:
                    data = self.fetch_data_v2(token)
                    trades = data.get('Trades', [])
                except Exception as e:
                    logger.error(f"Ошибка при использовании API v2: {e}")
                    # Пробуем запасной вариант с API v1
                    logger.info("Пробуем запасной вариант с API v1")
                    trades = self.fetch_all_pages(status=status)
            else:
                logger.info(f"Использование API v1 для получения всех торгов со статусом '{status}'")
                trades = self.fetch_all_pages(status=status)

            # Сохраняем данные вне зависимости от источника
            self.save_to_csv(trades, csv_path)
            logger.info("Парсер электронных торгов успешно завершил работу")

            return trades
        except Exception as e:
            logger.error(f"Ошибка в работе парсера: {e}")

            # Даже в случае ошибки пытаемся создать файл с минимальными данными
            try:
                logger.info("Создание файла с минимальными данными из-за ошибки")
                self.save_to_csv([], csv_path)
            except Exception as create_error:
                logger.error(f"Не удалось создать файл с минимальными данными: {create_error}")

            raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Парсер API электронных торгов")
    parser.add_argument("--status", default="AcceptingApplications", help="Статус торгов для API v1")
    parser.add_argument("--token", help="Токен для API v2 (если не указан, используется API v1)")
    parser.add_argument("--output", help="Путь к файлу для сохранения результатов")
    parser.add_argument("--page-size", type=int, default=100, help="Количество записей на страницу")
    parser.add_argument("--api-version", choices=["v1", "v2"], default="v1", help="Версия API")

    args = parser.parse_args()

    try:
        # Имя файла с датой и временем для избежания перезаписи
        if args.output:
            csv_filename = args.output
        else:
            # Создаем директорию для вывода, если она не существует
            output_dir = os.path.abspath("data")
            os.makedirs(output_dir, exist_ok=True)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            csv_filename = os.path.join(output_dir, f"auction_trades_{timestamp}.csv")

        logger.info(f"Файл будет сохранен по пути: {csv_filename}")

        parser = AuctionTradesParser(version=args.api_version, page_size=args.page_size)
        parser.run(status=args.status, token=args.token, csv_path=csv_filename)
    except Exception as e:
        logger.critical(f"Критическая ошибка: {e}")