#!/usr/bin/env python
import argparse
import csv
import json
import logging
import os
import sys
import time
import requests
from datetime import datetime

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
    log_file = os.path.join(log_dir, f"opengov_loader_{timestamp}.log")
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    
    return logging.getLogger('opengov_loader')

logger = setup_logging()

class OpenGovDataLoader:
    """Класс для загрузки данных на портал data.opengov.kz"""
    
    def __init__(self, api_url="https://data.opengov.kz/api/v1", api_key=None):
        """
        Инициализация загрузчика данных
        
        Args:
            api_url (str): URL API для загрузки данных
            api_key (str): API ключ для авторизации
        """
        self.api_url = api_url
        self.api_key = api_key
        self.headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        
        if api_key:
            self.headers['Authorization'] = f'Bearer {api_key}'
    
    def check_connection(self):
        """
        Проверка соединения с API
        
        Returns:
            bool: Результат проверки
        """
        try:
            response = requests.get(f"{self.api_url}/status", headers=self.headers)
            response.raise_for_status()
            
            logger.info("Соединение с API data.opengov.kz установлено")
            return True
        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка при проверке соединения с API: {e}")
            return False
    
    def get_dataset_info(self, dataset_id):
        """
        Получение информации о наборе данных
        
        Args:
            dataset_id (str): Идентификатор набора данных
            
        Returns:
            dict: Информация о наборе данных
        """
        try:
            response = requests.get(f"{self.api_url}/datasets/{dataset_id}", headers=self.headers)
            response.raise_for_status()
            
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка при получении информации о наборе данных {dataset_id}: {e}")
            return None
    
    def create_upload_session(self, dataset_id, description):
        """
        Создание сессии для загрузки данных
        
        Args:
            dataset_id (str): Идентификатор набора данных
            description (str): Описание загрузки
            
        Returns:
            str: Идентификатор сессии загрузки
        """
        try:
            payload = {
                "datasetId": dataset_id,
                "description": description,
                "uploadType": "FULL_REPLACE"  # Полная замена данных
            }
            
            response = requests.post(
                f"{self.api_url}/uploads/create",
                headers=self.headers,
                data=json.dumps(payload)
            )
            response.raise_for_status()
            
            result = response.json()
            upload_id = result.get("uploadId")
            
            if not upload_id:
                logger.error("API не вернул идентификатор сессии загрузки")
                return None
            
            logger.info(f"Создана сессия загрузки с ID: {upload_id}")
            return upload_id
        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка при создании сессии загрузки: {e}")
            return None
    
    def upload_csv_file(self, upload_id, csv_file):
        """
        Загрузка CSV файла
        
        Args:
            upload_id (str): Идентификатор сессии загрузки
            csv_file (str): Путь к CSV файлу
            
        Returns:
            bool: Результат загрузки
        """
        try:
            with open(csv_file, 'rb') as file:
                files = {'file': (os.path.basename(csv_file), file, 'text/csv')}
                
                response = requests.post(
                    f"{self.api_url}/uploads/{upload_id}/files",
                    headers={'Authorization': self.headers.get('Authorization')},
                    files=files
                )
                response.raise_for_status()
            
            logger.info(f"Файл {csv_file} успешно загружен")
            return True
        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка при загрузке файла {csv_file}: {e}")
            return False
    
    def commit_upload(self, upload_id):
        """
        Подтверждение загрузки данных
        
        Args:
            upload_id (str): Идентификатор сессии загрузки
            
        Returns:
            bool: Результат подтверждения
        """
        try:
            response = requests.post(
                f"{self.api_url}/uploads/{upload_id}/commit",
                headers=self.headers
            )
            response.raise_for_status()
            
            logger.info(f"Загрузка с ID {upload_id} успешно подтверждена")
            return True
        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка при подтверждении загрузки: {e}")
            return False
    
    def check_upload_status(self, upload_id):
        """
        Проверка статуса загрузки
        
        Args:
            upload_id (str): Идентификатор сессии загрузки
            
        Returns:
            str: Статус загрузки
        """
        try:
            response = requests.get(
                f"{self.api_url}/uploads/{upload_id}/status",
                headers=self.headers
            )
            response.raise_for_status()
            
            result = response.json()
            status = result.get("status")
            
            logger.info(f"Статус загрузки {upload_id}: {status}")
            return status
        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка при проверке статуса загрузки: {e}")
            return None
    
    def wait_for_upload_completion(self, upload_id, max_wait_time=600, check_interval=10):
        """
        Ожидание завершения загрузки
        
        Args:
            upload_id (str): Идентификатор сессии загрузки
            max_wait_time (int): Максимальное время ожидания в секундах
            check_interval (int): Интервал проверки в секундах
            
        Returns:
            bool: Результат завершения загрузки
        """
        start_time = time.time()
        
        while time.time() - start_time < max_wait_time:
            status = self.check_upload_status(upload_id)
            
            if status == "COMPLETED":
                logger.info(f"Загрузка {upload_id} успешно завершена")
                return True
            elif status in ["FAILED", "REJECTED", "CANCELLED"]:
                logger.error(f"Загрузка {upload_id} завершилась с ошибкой: {status}")
                return False
            
            # Продолжаем ожидание
            logger.info(f"Загрузка {upload_id} в процессе. Ожидание {check_interval} секунд...")
            time.sleep(check_interval)
        
        logger.error(f"Превышено максимальное время ожидания ({max_wait_time} сек) для загрузки {upload_id}")
        return False
    
    def load_gosreestr_csv(self, csv_file, dataset_id):
        """
        Загрузка CSV файла с данными Госреестра
        
        Args:
            csv_file (str): Путь к CSV файлу
            dataset_id (str): Идентификатор набора данных
            
        Returns:
            bool: Результат загрузки
        """
        # Проверка соединения с API
        if not self.check_connection():
            return False
        
        # Получение информации о наборе данных
        dataset_info = self.get_dataset_info(dataset_id)
        if not dataset_info:
            return False
        
        logger.info(f"Начало загрузки файла {csv_file} в набор данных {dataset_id}")
        
        # Создание сессии загрузки
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        upload_id = self.create_upload_session(
            dataset_id,
            f"Автоматическая загрузка данных Госреестра от {timestamp}"
        )
        
        if not upload_id:
            return False
        
        # Загрузка файла
        if not self.upload_csv_file(upload_id, csv_file):
            return False
        
        # Подтверждение загрузки
        if not self.commit_upload(upload_id):
            return False
        
        # Ожидание завершения загрузки
        return self.wait_for_upload_completion(upload_id)

def main():
    """Основная функция для загрузки данных в data.opengov.kz"""
    parser = argparse.ArgumentParser(description="Загрузка CSV файла в data.opengov.kz")
    parser.add_argument("--file", required=True, help="Путь к CSV файлу")
    parser.add_argument("--dataset", required=True, help="Идентификатор набора данных")
    parser.add_argument("--api-url", default="https://data.opengov.kz/api/v1", help="URL API")
    parser.add_argument("--api-key", required=True, help="API ключ для авторизации")
    
    args = parser.parse_args()
    
    # Проверка наличия файла
    if not os.path.isfile(args.file):
        logger.error(f"Файл {args.file} не найден")
        sys.exit(1)
    
    # Создание загрузчика
    loader = OpenGovDataLoader(api_url=args.api_url, api_key=args.api_key)
    
    # Загрузка данных
    result = loader.load_gosreestr_csv(args.file, args.dataset)
    
    if result:
        logger.info(f"Данные из файла {args.file} успешно загружены в data.opengov.kz")
    else:
        logger.error(f"Не удалось загрузить данные из файла {args.file} в data.opengov.kz")
        sys.exit(1)


if __name__ == "__main__":
    main()