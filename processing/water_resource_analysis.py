import logging

import numpy as np
from rasterstats import zonal_stats
from shapely.geometry import MultiPolygon, Polygon

from api.dataspace_api import download_sentinel_images
from config import settings
from processing.raster_utils import calculate_index, get_array, raster_to_vector

logger = logging.getLogger(__name__)


def find_blooms_polygons(ndci, input_shapefile, date):
    """
    Находит полигоны цветения на отдельных водоёмах.
    """
    threshold_value = np.where((ndci >= settings.THRESHOLD) & (ndci <= 1), 1, 0)
    vector_ndci = raster_to_vector(ndci, threshold_value).clip(input_shapefile).explode(index_parts=False)

    vector_ndci["area_sz"] = vector_ndci.area
    final_vector = vector_ndci[(vector_ndci["area_sz"] >= 900)]
    if not final_vector.geometry.is_empty.all():
        intersection_polygons = input_shapefile.overlay(final_vector, how="intersection")
        intersection_polygons = intersection_polygons.rename_geometry("shape")
        blooms_polygons = intersection_polygons.dissolve(
            by="poly_id",
            aggfunc={
                **{k: "first" for k in intersection_polygons.columns if k not in ["shape", "area_sz"]},
                "area_sz": "sum",
            },
        )
        blooms_polygons["shape"] = blooms_polygons["shape"].apply(
            lambda geom: MultiPolygon([geom]) if isinstance(geom, Polygon) else geom
        )
        blooms_polygons = blooms_polygons.drop(columns=["objectid", "gdb_geomattr_data"])
        blooms_polygons["date"] = date
        blooms_polygons["date_str"] = date.strftime("%Y-%m-%d")

        return blooms_polygons


def calculate_zonal_statistics(ndci, input_shapefile, buffer_cloud, date):
    """
    Вычисляет зональную статистику для полигонов открытой от облачности воды.
    """
    array = ndci.values[0]
    affine = ndci.rio.transform()
    nodata = ndci.rio.nodata
    open_water_vector = input_shapefile.overlay(buffer_cloud, how="difference")
    if not open_water_vector.geometry.is_empty.all():
        open_water_vector["date"] = date
        open_water_vector["date_str"] = date.strftime("%Y-%m-%d")
        open_water_vector = open_water_vector.drop(columns=["objectid", "gdb_geomattr_data"])
        open_water_vector["area_open_water"] = open_water_vector.area
        open_water_vector["percent_open_water"] = round(
            open_water_vector["area_open_water"] / open_water_vector["lake_area"] * 100
        )
        stats = zonal_stats(
            open_water_vector,
            array,
            affine=affine,
            nodata=nodata,
            stats="min max std mean median",
        )
        for stat in ["min", "max", "std", "mean", "median"]:
            open_water_vector[stat] = [f[stat] for f in stats]

        return open_water_vector


def processing_images(
    access_key: str,
    secret_key: str,
    query_params: dict,
    input_shapefile,
    download_folder: str,
    date,
):
    """
    Обрабатывает изображения Sentinel-2, вычисляет спектральный индекс NDСI, на основе индекса строится вектора
    полигонов цветения сине-зелёных водорослей, а также рассчитывается зональная статистика(mean median min std max)
    для отдельных озёр.
    """
    s2_path = download_sentinel_images(access_key, secret_key, query_params, download_folder)

    qi = (
        next(get_array(s2_path, "SCL_60m.jp2"))
        .rio.reproject(input_shapefile.crs)
        .rio.clip(input_shapefile.geometry, from_disk=True)
    )
    cloud = np.where((qi == 3) | (qi == 7) | (qi == 8) | (qi == 9) | (qi == 10), 1, 2)
    buffer_cloud = raster_to_vector(qi, cloud)
    buffer_cloud["geometry"] = buffer_cloud.buffer(120)
    ndci = calculate_index(s2_path, input_shapefile, "B05_20m.jp2", "B04_10m.jp2")
    if not buffer_cloud.geometry.is_empty.all():
        ndci = ndci.rio.clip(buffer_cloud.geometry, buffer_cloud.crs, invert=True)
    logger.info(f"Created NDCI, date: {s2_path.split('/')[-1][11:19]}")

    blooms_vector = find_blooms_polygons(ndci, input_shapefile, date)
    vector_zonal_stats = calculate_zonal_statistics(ndci, input_shapefile, buffer_cloud, date)
    if blooms_vector is not None:
        vector_zonal_stats = vector_zonal_stats.merge(blooms_vector[["area_sz"]], on="poly_id", how="left")
        vector_zonal_stats["area_sz"] = vector_zonal_stats["area_sz"].fillna(0)
        vector_zonal_stats["percent_blooms"] = round(
            vector_zonal_stats["area_sz"] / vector_zonal_stats["lake_area"] * 100
        )

    return blooms_vector, vector_zonal_stats
