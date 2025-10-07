
import os
import glob
from loguru import logger
import configparser  # импортируем библиотеку для чтения конфигов
from dotenv import load_dotenv
import pyodbc
from bs4 import BeautifulSoup
from datetime import datetime, timedelta


def configure_logger():
    ini_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "", "settings.ini"))
    config = configparser.ConfigParser()
    config.read(ini_path)

    log_level = config.get("log", "level", fallback="INFO")
    log_path = config.get("log", "path", fallback="")
    log_rotation = config.get("log", "rotation", fallback="10 MB")
    log_retention = config.get("log", "retention", fallback="7 days")
    log_compression = config.get("log", "compression", fallback="zip")

    log_file = os.path.join(log_path, "clearint_load.log") if log_path else "clearint_load.log"

    logger.remove()
    logger.add(log_file, level=log_level, rotation=log_rotation, retention=log_retention, compression=log_compression)
    # logger.info(f"Настройки логирования: уровень={log_level}, путь={log_path}, файл={log_file}, ротация={log_rotation}, хранение={log_retention}, сжатие={log_compression}")

configure_logger()

# === Загрузка .env ===
load_dotenv()

DB_SERVER = os.getenv("DB_SERVER")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# === Подключение к SQL Server ===
conn_str = (
    f"Driver={{ODBC Driver 11 for SQL Server}};"
    f"Server={DB_SERVER};"
    f"Database={DB_NAME};"
    f"UID={DB_USER};"
    f"PWD={DB_PASSWORD};"
)
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# === Определяем путь и маску файлов по текущей дате ===
today_str = datetime.now().strftime("%Y%m%d")
directory_path = r"S:"
pattern = f"CLEARINT_{today_str}_*.html"
# pattern = f"CLEARINT_20250908_*.html"
html_files = glob.glob(os.path.join(directory_path, pattern))

# === Обработка файлов ===
for html_file in html_files:
    logger.info(f"Обработка файла: {html_file}")
    try:
        with open(html_file, encoding="windows-1251") as f:
            soup = BeautifulSoup(f, "lxml")
    except Exception as e:
        logger.info(f"Ошибка открытия файла {html_file}: {e}")
        continue

    tables = soup.find_all("table", class_="tbl")
    if not tables:
        logger.info(f"Нет таблицы с классом 'tbl' в файле {html_file}")
        continue

    table = tables[0]
    rows = table.find_all("tr")[1:]

    for row in rows:
        cols = [td.get_text(strip=True).replace(',', '.').replace(' ', '') for td in row.find_all("td")]
        if len(cols) != 10:
            continue

        try:
            settlement_date = datetime.strptime(cols[0], "%d.%m.%Y").date()
            operation_type = cols[1]
            direction = cols[2]
            operation_count = int(cols[3])
            amount_account = float(cols[4])
            account_currency = cols[5]
            amount_settlement = float(cols[6])
            settlement_currency = cols[7]
            amount_for_settlement = float(cols[8])
            amount_for_settlement_currency = cols[9]
        except Exception as e:
            logger.error(f"Ошибка разбора строки: {cols} — {e}")
            continue

        cursor.execute("""
            SELECT 1 
              FROM tRdbClearint
             WHERE SettlementDate = ? 
               AND OperationType = ? 
               AND Direction = ?
        """, settlement_date, operation_type, direction)

        if cursor.fetchone():
            continue

        cursor.execute("""
            INSERT INTO tRdbClearint (
                SettlementDate, OperationType, Direction,
                OperationCount, AmountAccountCurrency, AccountCurrencyCode,
                AmountSettlementCurrency, SettlementCurrencyCode,
                AmountForSettlement, AmountForSettlementCurrencyCode
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, settlement_date, operation_type, direction,
             operation_count, amount_account, account_currency,
             amount_settlement, settlement_currency,
             amount_for_settlement, amount_for_settlement_currency)

        logger.info(f"Добавлено: {settlement_date} | {operation_type} | {direction}")
       
    # === Обработка секции "Расчеты по МИР" ===
    mir_tables = soup.find_all("table", class_="tbl")
    mir_section = None

    # Ищем таблицу, где первая строка содержит "Расчеты по МИР"
    for tbl in mir_tables:
        header = tbl.find_previous_sibling("h2")
        if header and "Расчеты по МИР" in header.get_text():
            mir_section = tbl
            break

    if mir_section:
        mir_rows = mir_section.find_all("tr")[1:]  # пропускаем заголовок
        for row in mir_rows:
            cols = [td.get_text(strip=True).replace(',', '.').replace(' ', '') for td in row.find_all("td")]
            if len(cols) != 5:
                continue

            try:
                settlement_date = datetime.strptime(cols[0], "%d.%m.%Y").date()
                currency = cols[1]
                amount = float(cols[2])
                direction = cols[3]
                purpose = cols[4]
            except Exception as e:
                logger.error(f"Ошибка разбора строки 'Расчеты по МИР': {cols} — {e}")
                continue

            # Проверка на дубликат
            cursor.execute("""
                SELECT 1 
                FROM tRdbClearintMir (nolock)
                WHERE SettlementDate = ? 
                AND CurrencyCode = ? 
                AND Amount = ?
                AND Direction = ?
            """, settlement_date, currency, amount, direction)

            if cursor.fetchone():
                continue

            # Вставка
            cursor.execute("""
                INSERT INTO tRdbClearintMir (
                    SettlementDate, CurrencyCode, Amount, Direction, Purpose
                ) VALUES (?, ?, ?, ?, ?)
            """, settlement_date, currency, amount, direction, purpose)

            logger.info(f"Добавлено МИР: {settlement_date} | {currency} | {amount} | {direction} | {purpose}")
            
    else:
        logger.info("Секция 'Расчеты по МИР' не найдена")

    # чтобы шедулер диасофта не загружать перенесем задание на следубщий день
    dt = (datetime.now() + timedelta(days=1)).strftime("%Y%m%d 08:00:00.000")
    sql = f"EXEC ReplTask_UpdateTime 10000000062, 2, '{dt}'"
    logger.info("Executing:", sql)
    cursor.execute(sql)

# === Завершение ===
conn.commit()
cursor.close()
conn.close()
