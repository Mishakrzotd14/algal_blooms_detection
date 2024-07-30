import os

import psycopg2
from sqlalchemy import create_engine

from dotenv import load_dotenv

current_directory = os.getcwd()
dotenv_path = os.path.join(current_directory, "dotenv", ".env")
load_dotenv(dotenv_path)

DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]
DB_HOST = os.environ["DB_HOST"]
DB_PORT = os.environ["DB_PORT"]
DB_DATABASE = os.environ["DB_DATABASE"]

connection_db = psycopg2.connect(user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT, database=DB_DATABASE)

conn = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_DATABASE}"
engine = create_engine(conn)


SPECTATOR_API_KEY = os.environ["SPECTATOR_API_KEY"]
COPERNICUS_ACCESS_KEY = os.environ["COPERNICUS_ACCESS_KEY"]
COPERNICUS_SECRET_KEY = os.environ["COPERNICUS_SECRET_KEY"]

NAME_SHP_FILE = "naroch_lakes_without_makrof_2024"

DIR_DOWNLOAD = r"D:\satellite_images\naroch_images"

PLATFORM = "SENTINEL-2"
LEVEL = "S2MSI2A"
MAXCC = 80
CRS_FOR_COPERNICUS = "EPSG:4326"
CRS_FOR_PROCESSING = "EPSG:32635"
SPAN_TABLE_NAME = "naroch_span_sentinel2"
NAROCH_CVETINIE_TABLE_NAME = "naroch_cvetinie_sz"
NAROCH_ZONAL_STATS_TABLE_NAME = "naroch_zonal_stats"
SMTP_USER = os.environ["SMTP_USER"]
SMTP_PASSWORD = os.environ["SMTP_PASSWORD"]

EMAILS_LIST = ["egor_kurzenkov@mail.ru", SMTP_USER]

THRESHOLD = (20 - 1.6194) / 602.02
