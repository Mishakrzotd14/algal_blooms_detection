import os

import geopandas as gpd
import numpy as np
import rasterio
import rioxarray as rxr
from rasterio.enums import Resampling
from shapely.geometry import shape


def resample(raster_file_path: str, scale_factor: int = 2):
    """
    Изменяет разрешение растра в заданное количество раз(scale_factor).
    """
    with rxr.open_rasterio(raster_file_path) as raster:
        new_width = raster.rio.width * scale_factor
        new_height = raster.rio.height * scale_factor
        sampled = raster.rio.reproject(
            raster.rio.crs,
            shape=(int(new_height), int(new_width)),
            resampling=Resampling.bilinear,
        )

    return sampled


def get_array(s2_image_path: str, channel_name: str):
    """
    Возвращает генератор, который читает растровые каналы из заданного каталога.
    """
    for root, dirs, files in os.walk(s2_image_path):
        for name in files:
            channel_path = os.path.join(root, name)
            if "20m" in channel_name:
                if name.endswith(channel_name):
                    yield resample(channel_path)
            else:
                if name.endswith(channel_name):
                    with rxr.open_rasterio(channel_path, masked=True) as open_raster:
                        yield open_raster


def norm_diff(ndarray_first: np.ndarray, ndarray_second: np.ndarray, input_shapefile) -> np.ndarray:
    """
    Вычисляет нормализованную разницу между двумя растровыми массивами.
    """
    norm_difference = np.divide(
        ndarray_first.astype(np.float16) - ndarray_second.astype(np.float16),
        ndarray_first + ndarray_second,
    )

    return norm_difference.rio.reproject(input_shapefile.crs).rio.clip(input_shapefile.geometry, from_disk=True)


def calculate_index(s2_image_path: str, input_shapefile, band1_name: str, band2_name: str):
    """
    Вычисляет индекс нормализованной разницы (Normalized Difference Index, NDI) между двумя растровыми каналами.
    """
    band1 = next(get_array(s2_image_path, band1_name))
    band2 = next(get_array(s2_image_path, band2_name))

    return norm_diff(band1, band2, input_shapefile)


# Function to convert a raster to a vector
def raster_to_vector(raster, condition: np.ndarray):
    """
    Преобразует растровые данные в векторные данные с использованием заданного условия.
    """
    geometries = [
        shape(geometry)
        for geometry, value in rasterio.features.shapes(condition.astype(np.uint8), transform=raster.rio.transform())
        if value == 1
    ]
    geodataframe = gpd.GeoDataFrame(geometry=geometries, crs=str(raster.rio.crs))

    return geodataframe
