from flask import Flask, jsonify, request
import psycopg2
import configparser
import logging
import os  # Импортируем модуль os для работы с путями

app = Flask(__name__)

# Создаем папку для логов, если ее нет
log_dir = 'error_logs'  # Папка для логов
os.makedirs(log_dir, exist_ok=True)

# Настройка логирования
logging.basicConfig(
    filename=os.path.join(log_dir, 'api.log'),  # Путь к файлу лога
    level=logging.ERROR,  # Уровень логирования (ERROR, WARNING, INFO, DEBUG)
    format='%(asctime)s - %(levelname)s - %(message)s'  # Формат сообщения
)

# Читаем параметры подключения из конфига
config = configparser.ConfigParser()
config.read('config.ini', encoding='utf-8')
db_host = config['database']['host']
db_port = config['database']['port']
db_name = config['database']['database']
db_user = config['database']['user']
db_password = config['database']['password']

# Функция для получения данных из базы
def get_logs(ip_address=None, start_date=None, end_date=None):
    conn = None
    try:
        conn = psycopg2.connect(host=db_host, port=db_port, database=db_name, user=db_user, password=db_password)
        cur = conn.cursor()
        # Формируем SQL-запрос с учетом фильтров
        query = "SELECT ip, datetime, request, status, size, referrer, user_agent FROM apache_logs WHERE 1=1"  # 1=1 чтобы удобно добавлять условия
        params = []

        if ip_address:
            query += " AND ip = %s"
            params.append(ip_address)
        if start_date:
            query += " AND datetime >= %s"
            params.append(start_date)
        if end_date:
            query += " AND datetime <= %s"
            params.append(end_date)

        cur.execute(query, tuple(params)) # Convert params to tuple
        rows = cur.fetchall()

        # Преобразуем данные в формат JSON
        log_list = []
        for row in rows:
            log_list.append({
                'ip': row[0],
                'datetime': row[1].isoformat() if row[1] else None,
                'request': row[2],
                'status': row[3],
                'size': row[4],
                'referrer': row[5],
                'user_agent': row[6]
            })
        return log_list
    except Exception as e:
        logging.error(f"Error: {e}")  # Логируем ошибку
        print(f"Error: {e}")
        return []
    finally:
        if conn is not None:
            conn.close()

# Определяем маршрут для API
@app.route('/api/logs', methods=['GET'])
def api_logs():
    ip_address = request.args.get('ip')
    start_date = request.args.get('start_date') # Get start_date from request
    end_date = request.args.get('end_date') # Get end date from request
    logs = get_logs(ip_address, start_date, end_date) # Pass start_date and end_date to get_logs
    return jsonify(logs)

if __name__ == '__main__':
    app.run(debug=True)