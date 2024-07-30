import logging
import os
import shutil
import sys

import boto3
import requests
from fp.fp import FreeProxy
from tqdm import tqdm

from config import settings
from db.table_span import update_table_status

SRID = "4326"
TIME_FORMAT = "T00:00:00.000Z"
TIME_END_FORMAT = "T12:00:00.000Z"
ENDPOINT_URL = "https://eodata.dataspace.copernicus.eu/"
BUCKET = "eodata"
TILE_NAME = "35UMA"

logger = logging.getLogger(__name__)


def get_folder_size(folder_path: str) -> int:
    """
    Получить размер папки.
    """
    total_size = 0
    for path, _, files in os.walk(folder_path):
        for file in files:
            file_path = os.path.join(path, file)
            try:
                total_size += os.path.getsize(file_path)
            except OSError as e:
                logger.error(f"Could not calculate the size of {file_path}: {e}")
    return total_size


def make_path(path: str) -> None:
    """
    Создать путь.
    """
    try:
        os.makedirs(path, exist_ok=True)
    except FileExistsError:
        pass
    except OSError as e:
        logger.error(f"Error creating directory {path}: {e}")


def generate_filter_query(qp) -> str:
    """
    Сгенерировать строку фильтра для запроса к хранилищу данных.
    """
    filter_query = (
        f"Collection/Name eq '{qp['setillite']}' "
        f"and Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'productType' "
        f"and att/OData.CSC.StringAttribute/Value eq '{qp['producttype']}') "
        f"and Attributes/OData.CSC.DoubleAttribute/any(att:att/Name eq 'cloudCover' "
        f"and att/OData.CSC.DoubleAttribute/Value lt {qp['cloud_percentage']}) "
        f"and OData.CSC.Intersects(area=geography'SRID={SRID};{qp['footprint']}') "
        f"and ContentDate/Start gt {qp['date_start']}{TIME_FORMAT} "
        f"and ContentDate/Start lt {qp['date_end']}{TIME_END_FORMAT}"
    )
    return filter_query


def get_s3path(qp) -> str:
    """
    Получить путь к Amazon S3 bucket odata, используя метаданные запроса dataspace.copernicus.eu.
    """
    try:
        filter_query = generate_filter_query(qp)
        all_proxies = FreeProxy(timeout=1, rand=True).get_proxy_list(repeat=False)

        for proxy in all_proxies:
            proxies = {"http": proxy, "https": proxy}
            url = f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter={filter_query}"

            try:
                result = requests.get(url, timeout=60, allow_redirects=False, proxies=proxies).json()
                if result.get("value"):
                    products_s3path = [
                        product["S3Path"] for product in result["value"] if product["Name"][39:44] == TILE_NAME
                    ]
                    if products_s3path:
                        return products_s3path[0]
                    else:
                        logger.info("No tiles found in CDSE catalogue with the stated parameters")
                        update_table_status(
                            settings.connection_db,
                            settings.SPAN_TABLE_NAME,
                            qp["date_start"],
                            new_status=0,
                        )
                        sys.exit()
                else:
                    logger.info("No products found in CDSE catalogue within the extent of the vector layer")
                    update_table_status(
                        settings.connection_db,
                        settings.SPAN_TABLE_NAME,
                        qp["date_start"],
                        new_status=0,
                    )
                    sys.exit()

            except requests.RequestException:
                continue
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        sys.exit()


def download_file(resource, obj, target_directory: str):
    """
    Загрузить файлы снимков.
    """
    target = os.path.join(target_directory, obj.key)

    if obj.key.endswith("/"):
        make_path(target)
    else:
        dirname = os.path.dirname(target)
        if dirname != "":
            make_path(dirname)
        resource.Object(BUCKET, obj.key).download_file(target)


def download_sentinel_images(access_key: str, secret_key: str, qp, target_directory: str):
    """
    Загрузить изображения спутника Sentinel-2.
    """
    try:
        s3path_prod = get_s3path(qp)

        s3path = s3path_prod.removeprefix(f"/{BUCKET}/")
        file_name = s3path.split("/")[-1]
        dirname = os.path.dirname(s3path)

        if not os.path.exists(os.path.join(target_directory, dirname)):
            make_path(os.path.join(target_directory, dirname))
        fls = os.listdir(os.path.join(target_directory, dirname))

        resource = boto3.resource(
            service_name="s3",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            endpoint_url=ENDPOINT_URL,
        )
        s3path = s3path + ""
        objects = list(resource.Bucket(BUCKET).objects.filter(Prefix=s3path))
        objs = [obj for obj in objects if "/IMG_DATA/" in obj.key]

        s3_size = sum([obj.size for obj in objs])
        if not objs:
            raise Exception(f"could not find product '' in CDSE object store")

        size_directory = get_folder_size(os.path.join(target_directory, s3path))

        download_location = os.path.join(target_directory, s3path)
        if file_name in fls and s3_size == size_directory:
            logger.info(f"Файл {file_name} находится в папке")
        else:
            if os.path.exists(os.path.join(download_location, "GRANULE")):
                shutil.rmtree(os.path.join(download_location, "GRANULE"))

            logger.info(f"\nФайл {file_name} не находится в папке. Скачивание...")

            for obj in tqdm(objs, desc=f"Downloading {file_name}", unit="file"):
                download_file(resource, obj, target_directory)
            logger.info(f"Снимок {file_name} скачан!")

        return download_location
    except Exception as e:
        logger.error(f"An error occurred: {e}")
