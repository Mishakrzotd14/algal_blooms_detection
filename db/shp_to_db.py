import logging

import geopandas as gpd
import numpy as np
from sqlalchemy import inspect
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)


def shp_to_postgresql(conn_engine, vector_data, schema: str, table_name: str):
    """
    Загружает геоданные типа GeoDataFrame в базу данных PostgreSQL.
    """
    inspector = inspect(conn_engine)

    try:
        table_exists = inspector.has_table(table_name, schema=schema)
        if table_exists:
            logger.info(f"Table {schema}.{table_name} exists. Checking for new records to add.")
            existing_data = gpd.read_postgis(f"SELECT * FROM {schema}.{table_name}", conn_engine, geom_col="shape")

            if all(col in vector_data.columns for col in ["id_polygon", "date_str"]) and all(
                col in existing_data.columns for col in ["id_polygon", "date_str"]
            ):
                new_records = vector_data[
                    ~vector_data.set_index(["poly_id", "date_str"]).index.isin(
                        existing_data.set_index(["poly_id", "date_str"]).index
                    )
                ]
            else:
                new_records = vector_data
            if not new_records.empty:
                new_records["objectid"] = np.arange(len(new_records)) + len(existing_data) + 1
                new_records["objectid"] = new_records["objectid"].astype("int32")
                new_records.to_postgis(name=table_name, con=conn_engine, if_exists="append", schema=schema)
                logger.info(f"Новых объектов добавлены в таблицу {table_name}: {len(new_records)}.")
        else:
            logger.info(f"Table {schema}.{table_name} does not exist. Creating new table and adding records.")
            vector_data["objectid"] = np.arange(1, len(vector_data) + 1)
            vector_data["objectid"] = vector_data["objectid"].astype("int32")
            vector_data.to_postgis(name=table_name, con=conn_engine, schema=schema)

            logger.info(f"Vector layer {table_name} added to database successfully")
    except SQLAlchemyError as e:
        logger.error(f"Error occurred while adding vector layer {table_name} to database: {e}", exc_info=True)
