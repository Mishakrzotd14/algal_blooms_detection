import datetime
import logging.config
import os
import sys

import geopandas as gpd
from shapely.geometry import box

from api.api_spectator import get_info_span
from config import settings
from config.config_logging import logging_config
from db.shp_to_db import shp_to_postgresql
from db.table_span import update_table_status
from processing.water_resource_analysis import processing_images

logging.config.dictConfig(logging_config)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    try:
        logger.info("----------------------------------------------------------------")
        logger.info("Starting the application")

        today_datetime = datetime.datetime.now()
        today_date = today_datetime.date()

        lakes_shp = gpd.read_postgis(
            f"SELECT * FROM {settings.DB_USER}.{settings.NAME_SHP_FILE}",
            settings.engine,
            geom_col="shape",
        )

        if lakes_shp.crs != settings.CRS_FOR_COPERNICUS:
            lakes_shp.to_crs(settings.CRS_FOR_COPERNICUS, inplace=True)

        coordinates = ",".join(str(coord) for coord in lakes_shp.total_bounds)
        footprint = box(*lakes_shp.total_bounds).wkt

        flight_date = get_info_span(
            settings.connection_db, coordinates, settings.SPECTATOR_API_KEY, settings.SPAN_TABLE_NAME, today_date
        )

        if flight_date is None:
            logger.info("Application finished. The provided flight date has already been processed.")
            sys.exit()

        if not os.path.exists(settings.DIR_DOWNLOAD):
            os.makedirs(os.path.join(settings.DIR_DOWNLOAD))
        else:
            logger.info("Directory already exists.")

        lakes_shp.to_crs(settings.CRS_FOR_PROCESSING, inplace=True)

        query_params = {
            "setillite": settings.PLATFORM,
            "producttype": settings.LEVEL,
            "cloud_percentage": settings.MAXCC,
            "footprint": footprint,
            "date_start": flight_date,
            "date_end": flight_date,
        }

        final_cvetinie, zonal_stat_vector = processing_images(
            settings.COPERNICUS_ACCESS_KEY,
            settings.COPERNICUS_SECRET_KEY,
            query_params,
            lakes_shp,
            settings.DIR_DOWNLOAD,
            flight_date,
        )

        if zonal_stat_vector is not None:
            shp_to_postgresql(
                settings.engine,
                zonal_stat_vector,
                settings.DB_USER,
                settings.NAROCH_ZONAL_STATS_TABLE_NAME,
            )
            if final_cvetinie is not None:
                shp_to_postgresql(
                    settings.engine,
                    final_cvetinie,
                    settings.DB_USER,
                    settings.NAROCH_CVETINIE_TABLE_NAME,
                )
            update_table_status(settings.connection_db, settings.SPAN_TABLE_NAME, flight_date, new_status=1)

        logger.info("Application finished successfully")
    except Exception as e:
        logger.exception(f"An error occurred: {e}")
        sys.exit(1)
