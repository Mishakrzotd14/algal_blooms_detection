import datetime
import logging

import requests

from db.table_span import check_table_exists, create_table

URL_SPECTATOR = "https://api.spectator.earth/overpass/"
SATELLITES = "Sentinel-2A,Sentinel-2B"
DAYS_AFTER = 7
DAYS_BEFORE = 0

logger = logging.getLogger(__name__)


def get_response(params: dict):
    """
    Отправьте запрос GET по-указанному URL-адресу с параметрами.
    """
    try:
        response = requests.get(URL_SPECTATOR, params=params)
        response.raise_for_status()
        return response.json()["overpasses"] if response.status_code == 200 else None
    except requests.RequestException as e:
        logger.error(f"Request failed: {e}")
        return None


def store_data(cursor, item: dict, span_table_name: str):
    """
    Поместить данные из запроса в базу данных.
    """
    id_span = item["id"]
    date_str = item["date"]
    date_span = datetime.datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ").date()
    satellite = item["satellite"]
    acquisition = item["footprints"]["features"][0]["properties"]["acquisition"]

    cursor.execute(f"SELECT * FROM {span_table_name} WHERE date = %s", (date_span,))
    row_by_date = cursor.fetchone()
    if row_by_date:
        return None

    cursor.execute(f"SELECT * FROM {span_table_name} WHERE id = %s", (id_span,))
    row = cursor.fetchone()

    if row:
        cursor.execute(
            f"UPDATE {span_table_name} SET date = %s, satellite = %s WHERE id = %s",
            (date_span, satellite, id_span),
        )
    else:
        cursor.execute(
            f"INSERT INTO {span_table_name} (id, date, satellite) VALUES (%s, %s, %s)",
            (id_span, date_span, satellite),
        )

    if acquisition is None or acquisition == False:
        cursor.execute(f"DELETE FROM {span_table_name} WHERE id = %s", (id_span,))


def get_info_span(conn, bbox: str, api_key: str, span_table_name: str, now):
    """
    Получить информацию о пролёте спутника.
    """
    params = {
        "api_key": api_key,
        "bbox": bbox,
        "satellites": SATELLITES,
        "days_after": DAYS_AFTER,
        "days_before": DAYS_BEFORE,
    }

    if not check_table_exists(conn, span_table_name):
        create_table(conn, span_table_name)
    try:
        with conn.cursor() as cursor:
            response_data = get_response(params)
            if response_data:
                for item in response_data:
                    store_data(cursor, item, span_table_name)

            cursor.execute(f"SELECT * FROM {span_table_name}")
            sentinel2 = cursor.fetchall()

            dates = [datetime.datetime.strptime(row[1].strftime("%Y-%m-%d"), "%Y-%m-%d").date() for row in sentinel2]
            status = [row[3] for row in sentinel2]
            for date in dates:
                if date == now:
                    pos = dates.index(date)
                    if not status[pos]:
                        logger.info(f"Date: {date}")
                        return date
                    else:
                        logger.info(f"Processing already performed: {date}")
                        return
            else:
                filtered_dates = filter(lambda d: d < now, dates)
                closest_date = min(filtered_dates, default=None, key=lambda d: abs(now - d))
                if closest_date is None:
                    logger.info("No dates found in the past")
                else:
                    pos = dates.index(closest_date)

                    if not status[pos]:
                        logger.info(f"Closest past date: {closest_date}")
                        return closest_date
                    else:
                        logger.info(f"Processing already performed: {closest_date}")
                        return None

    except Exception as e:
        logger.exception(f"An error occurred: {e}")
    finally:
        conn.commit()
