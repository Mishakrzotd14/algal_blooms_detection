import logging

import psycopg2

logger = logging.getLogger(__name__)


def check_table_exists(conn, table_name: str):
    """
    Проверяет существование таблицы в базе данных.
    """
    cur = None
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}')")
        exists = cur.fetchone()[0]
        return exists
    except psycopg2.Error as e:
        logger.error(f"Error checking table existence: {e}")
        return False
    finally:
        if cur:
            cur.close()


def create_table(conn, table_name: str):
    """
    Создает таблицу в базе данных, если её не существует.
    """
    cur = None
    try:
        cur = conn.cursor()
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id SERIAL not null,
                date DATE,
                satellite TEXT,
                status BOOLEAN
            )
        """
        )
        conn.commit()
        logger.info("Table created")
    except psycopg2.Error as e:
        logger.error(f"Error creating table: {e}")
    finally:
        if cur:
            cur.close()


def update_table_status(conn, table_name: str, date, new_status: int):
    """
    Обновляет статус записей в таблице на основе даты.
    """
    cur = None
    try:
        cur = conn.cursor()
        cur.execute(
            f"""
            UPDATE {table_name}
            SET status = %s
            WHERE date = %s
        """,
            (
                new_status,
                date,
            ),
        )
        conn.commit()
        logger.info("Table status updated")
    except psycopg2.Error as e:
        logger.error(f"Error updating table status: {e}")
    finally:
        if cur:
            cur.close()
