import configparser  # добавляем хуйню которая смотрит в конфиг.ини и читает его
import glob  # Импортируем модуль glob для поиска файлов по маске
import re  # регулярки
import psycopg2  # для работы с postgres
import logging  # Импортируем модуль logging
import os  # Импортируем модуль os для работы с путями

# Создаем папку для логов, если ее нет
log_dir = 'error_logs'  # Папка для логов
os.makedirs(log_dir, exist_ok=True)

# Настройка логирования
logging.basicConfig(
    filename=os.path.join(log_dir, 'aggregator.log'),  # Путь к файлу лога
    level=logging.ERROR,  # Уровень логирования (ERROR, WARNING, INFO, DEBUG)
    format='%(asctime)s - %(levelname)s - %(message)s'  # Формат сообщения
)

def create_table(conn):  # создаем таблицу
    """Создает таблицу для хранения данных логов (если она не существует)."""
    try:
        cur = conn.cursor()  # штука для запросов в бд
        cur.execute("""
            CREATE TABLE IF NOT EXISTS apache_logs (
                id SERIAL PRIMARY KEY,
                ip VARCHAR(255),
                datetime TIMESTAMP,
                request TEXT,
                status INTEGER,
                size INTEGER,
                referrer TEXT,
                user_agent TEXT
            )
        """)  # код создания бд
        conn.commit()  # применяем
        cur.close()  # закрываем
        print("Table 'apache_logs' created (or already exists).")
    except Exception as e:
        logging.error(f"Error creating table: {e}")  # Логируем ошибку

def read_logs(config, conn):  # читаем логи и пишем в бд
    """Читает логи, парсит строки и сохраняет данные в базу данных."""
    log_dir = config['logging']['log_dir']
    log_mask = config['logging']['log_mask']
    log_format = config['logging']['log_format']
    log_path = f"{log_dir}/{log_mask}"

    combined_log_regex = re.compile(
        r'(?P<ip>[\d\.]+)\s'
        r'-\s'
        r'-\s'
        r'\[(?P<datetime>[^\]]+)\]\s'
        r'"(?P<request>[^"]+)"\s'
        r'(?P<status>\d+)\s'
        r'(?P<size>\d+)\s'
        r'"(?P<referrer>[^"]*)"\s'
        r'"(?P<user_agent>[^"]*)"'
    )

    try:
        log_files = glob.glob(log_path)
        if not log_files:
            print(f"No log files found matching: {log_path}")
            return

        for log_file in log_files:
            print(f"Reading log file: {log_file}")
            try:
                with open(log_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        match = combined_log_regex.match(line)
                        if match:
                            log_data = match.groupdict()
                            try:
                                # Преобразуем строку datetime в объект datetime
                                from datetime import datetime
                                datetime_obj = datetime.strptime(log_data['datetime'], '%d/%b/%Y:%H:%M:%S %z')

                                cur = conn.cursor()  # штука для запросов в бд
                                cur.execute("""
                                    INSERT INTO apache_logs (ip, datetime, request, status, size, referrer, user_agent)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                                """, (log_data['ip'], datetime_obj, log_data['request'], log_data['status'], log_data['size'], log_data['referrer'], log_data['user_agent']))  # запрос на добавление
                                conn.commit()  # применяем
                                cur.close()  # закрываем
                            except Exception as e:
                                logging.error(f"Error inserting data into database: {e}")  # Логируем ошибку
                        else:
                            print(f"  Could not parse line: {line.strip()}")
            except IOError as e:
                logging.error(f"Error reading file {log_file}: {e}")  # Логируем ошибку
    except FileNotFoundError:
        logging.error(f"Error: Log directory not found: {log_dir}")  # Логируем ошибку
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")  # Логируем ошибку

def main():  # основная функция
    """Читает конфиг, подключается к БД, читает логи и сохраняет данные."""
    config = configparser.ConfigParser()
    config.read('config.ini', encoding='utf-8')

    db_host = config['database']['host']
    db_port = config['database']['port']
    db_name = config['database']['database']
    db_user = config['database']['user']
    db_password = config['database']['password']

    try:
        conn = psycopg2.connect(host=db_host, port=db_port, database=db_name, user=db_user, password=db_password)
        print("Connected to PostgreSQL database.")

        # Создаем таблицу (если она не существует)
        create_table(conn)

        # Читаем логи и сохраняем данные в базу данных
        read_logs(config, conn)

        conn.close()  # Закрываем соединение с базой данных
        print("Connection to PostgreSQL database closed.")

    except Exception as e:
        logging.error(f"Error connecting to database: {e}")  # Логируем ошибку
   
if __name__ == "__main__":
    main()