import argparse
import psycopg2
import configparser
from tabulate import tabulate # Для красивого вывода таблиц
import logging
import os  # Импортируем модуль os для работы с путями

# Создаем папку для логов, если ее нет
log_dir = 'error_logs'  # Папка для логов
os.makedirs(log_dir, exist_ok=True)

# Настройка логирования
logging.basicConfig(
    filename=os.path.join(log_dir, 'console.log'),  # Путь к файлу лога
    level=logging.ERROR,  # Уровень логирования (ERROR, WARNING, INFO, DEBUG)
    format='%(asctime)s - %(levelname)s - %(message)s'  # Формат сообщения
)

def main():
    # Читаем параметры из config.ini
    config = configparser.ConfigParser()
    config.read('config.ini', encoding='utf-8')
    db_host = config['database']['host']
    db_port = config['database']['port']
    db_name = config['database']['database']
    db_user = config['database']['user']
    db_password = config['database']['password']

    # Создаем парсер аргументов командной строки
    parser = argparse.ArgumentParser(description='Просмотр логов Apache из базы данных.')
    parser.add_argument('--ip', type=str, help='Фильтр по IP-адресу')
    parser.add_argument('--start_date', type=str, help='Начальная дата (YYYY-MM-DD)')
    parser.add_argument('--end_date', type=str, help='Конечная дата (YYYY-MM-DD)')
    parser.add_argument('--group_by', type=str, choices=['ip', 'date'], help='Группировка по IP или дате') #добавил
    args = parser.parse_args()

    # Формируем SQL-запрос
    query = "SELECT ip, datetime, request, status, size, referrer, user_agent FROM apache_logs WHERE 1=1"
    params = []

    if args.ip:
        query += " AND ip = %s"
        params.append(args.ip)
    if args.start_date:
        query += " AND datetime >= %s"
        params.append(args.start_date)
    if args.end_date:
        query += " AND datetime <= %s"
        params.append(args.end_date)

    if args.group_by == 'ip':  #группировка по IP
        query += " GROUP BY ip"
    elif args.group_by == 'date': #группировка по дате
        query += " GROUP BY DATE(datetime)" #DATE(datetime) - для группировки по дате без времени

    # Подключаемся к базе данных и выполняем запрос
    conn = None
    try:
        conn = psycopg2.connect(host=db_host, port=db_port, database=db_name, user=db_user, password=db_password)
        cur = conn.cursor()
        cur.execute(query, tuple(params))
        rows = cur.fetchall()

        # Выводим результаты в виде таблицы
        headers = ["IP", "Дата и время", "Запрос", "Статус", "Размер", "Referrer", "User-Agent"] # Шапка таблицы
        print(tabulate(rows, headers=headers))  #используем tabulate для красивого вывода

    except Exception as e:
        logging.error(f"Ошибка: {e}")  # Логируем ошибку
        print(f"Ошибка: {e}")
    finally:
        if conn is not None:
            conn.close()

if __name__ == "__main__":
    main()