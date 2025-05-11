import os
import logging

import geopandas as gpd
import numpy as np
import rasterio
from shapely.geometry import mapping
from rasterio.transform import from_origin
from rasterio.features import rasterize, geometry_mask
from rasterio.fill import fillnodata

from app.extensions import db
from app.repository.repo_kurva_banjir import get_reference_curves_banjir
from app.repository.repo_kurva_gempa import get_reference_curves as get_reference_curves_gempa
from app.repository.repo_kurva_gunungberapi import get_reference_curves_gunungberapi
from app.repository.repo_kurva_longsor import get_reference_curves_longsor

# Supaya rasterio/pyproj menemukan proj.db yang benar
os.environ['PROJ_LIB'] = r"E:\Geodesi dan Geomatika\Semester 7\TA\CobaPython\myenv311\Lib\site-packages\rasterio\proj_data"

# --- Logging setup ---
logger = logging.getLogger(__name__)
# Pastikan di konfigurasi aplikasi logger level DEBUG diaktifkan:
# logging.basicConfig(level=logging.DEBUG)

# Folder cache absolut di dalam direktori app/
BASE_APP_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
CACHE_DIR    = os.path.join(BASE_APP_DIR, 'cache')

class VisualisasiHazardService:
    _CURVE_MAPPING = {
        'banjir':       get_reference_curves_banjir,
        'gempa':        get_reference_curves_gempa,
        'gunungberapi': get_reference_curves_gunungberapi,
        'longsor':      get_reference_curves_longsor,
    }

    _RASTER_MAPPING = {
        'banjir':       ('model_intensitas_banjir', 'depth_100'),
        'gempa':        ('model_intensitas_gempa', 'mmi_500'),
        'gunungberapi': ('model_intensitas_gunungberapi', 'kpa_250'),
        'longsor':      ('model_intensitas_longsor', 'mflux_5'),
    }

    @classmethod
    def generate_density_geotiff(
        cls,
        hazard_type: str,
        pixel_size_km: float = 9.0,
        sample_size: int = 10000
    ) -> str:
        logger.debug("=== Mulai generate_density_geotiff ===")
        if hazard_type not in cls._RASTER_MAPPING:
            logger.error(f"Unknown hazard type: {hazard_type}")
            raise ValueError(f"Unknown hazard type for raster: {hazard_type}")

        table, value_field = cls._RASTER_MAPPING[hazard_type]
        os.makedirs(CACHE_DIR, exist_ok=True)
        output_path = os.path.join(CACHE_DIR, f"{hazard_type}_density.tif")
        logger.debug(f"Output GeoTIFF path: {output_path}")

        # 1. Load data
        sql = f"SELECT geom{', ' + value_field if value_field else ''} FROM {table}"
        logger.debug(f"Executing SQL: {sql}")
        gdf = gpd.read_postgis(
            sql,
            db.get_engine(),
            geom_col="geom",
            crs="EPSG:4326"
        )
        n_total = len(gdf)
        logger.debug(f"Total titik dibaca: {n_total}")

        # 2. Sampling untuk menentukan resolusi
        if n_total > sample_size:
            sampled_gdf = gdf.sample(n=sample_size, random_state=42)
            logger.debug(f"Sampling {sample_size} titik acak untuk hitung pixel_size")
        else:
            sampled_gdf = gdf
            logger.debug("Jumlah titik kecil, tidak dilakukan sampling")

        xs = np.unique(sampled_gdf.geometry.x.values)
        ys = np.unique(sampled_gdf.geometry.y.values)
        logger.debug(f"Unique X count: {len(xs)}, Unique Y count: {len(ys)}")

        xs.sort(); ys.sort()
        if len(xs) > 1 and len(ys) > 1:
            diffs_x = np.diff(xs)
            diffs_y = np.diff(ys)
            res_x = np.median(diffs_x)
            res_y = np.median(diffs_y)
            pixel_size = float(np.round((res_x + res_y) / 2, 8))
            logger.debug(f"Computed pixel_size (deg): res_x={res_x}, res_y={res_y}, pixel_size={pixel_size}")
        else:
            deg_per_km = 1.0 / 111.32
            pixel_size = pixel_size_km * deg_per_km
            logger.debug(f"Fallback pixel_size dari km: {pixel_size} deg (km={pixel_size_km})")

        # 3. Hitung origin agar pusat piksel tepat pada titik
        half = pixel_size / 2.0
        minx, maxy = xs[0], ys[-1]
        origin_x = minx - half
        origin_y = maxy + half
        logger.debug(f"Origin: ({origin_x}, {origin_y}), half={half}")

        # 4. Grid dimensi
        width  = int(np.ceil((xs[-1] - xs[0]) / pixel_size)) + 1
        height = int(np.ceil((ys[-1] - ys[0]) / pixel_size)) + 1
        logger.debug(f"Grid size: width={width}, height={height}")
        transform = from_origin(origin_x, origin_y, pixel_size, pixel_size)

        # 5. Prepare shapes untuk rasterize
        if value_field:
            shapes = ((row.geom, getattr(row, value_field)) for row in gdf.itertuples())
            logger.debug(f"Rasterize dengan field nilai: {value_field}")
        else:
            shapes = ((row.geom, 1) for row in gdf.itertuples())
            logger.debug("Rasterize dengan count=1 per titik")

        # 6. Rasterize
        logger.debug("Mulai rasterize …")
        raster = rasterize(
            shapes,
            out_shape=(height, width),
            transform=transform,
            fill=0,
            all_touched=True,
            merge_alg=rasterio.enums.MergeAlg.add,
            dtype="float32"
        )
        logger.debug("Rasterize selesai")

        # 7. Clip & fill
        logger.debug("Load batas provinsi untuk clipping")
        prov = gpd.read_postgis("SELECT geom FROM provinsi", db.get_engine(),
                                geom_col="geom", crs="EPSG:4326")
        prov['geom'] = prov['geom'].buffer(0)
        union_geom = prov.geometry.unary_union
        logger.debug("Membangun mask luar area provinsi")
        mask_outside = geometry_mask([mapping(union_geom)], transform=transform,
                                     invert=False, out_shape=raster.shape)
        raster[mask_outside] = 0.0
        logger.debug(f"Masked {mask_outside.sum()} sel di luar provinsi")

        hole_mask = (raster == 0.0) & (~mask_outside)
        if hole_mask.any():
            logger.debug(f"Filling nodata di {hole_mask.sum()} sel")
            filled = fillnodata(raster, mask=hole_mask,
                                max_search_distance=9999, smoothing_iterations=0)
            filled[mask_outside] = 0.0
            raster = filled
            logger.debug("Fillnodata selesai")
        else:
            logger.debug("Tidak ada hole yang perlu di-fill")

        # 8. Tulis GeoTIFF
        profile = {
            "driver":   "GTiff",
            "height":   height, "width": width, "count": 1,
            "dtype":    "float32", "crs": "EPSG:4326",
            "transform": transform, "compress": "lzw", "nodata": 0.0
        }
        logger.debug(f"Menulis GeoTIFF dengan profile: {profile}")
        with rasterio.open(output_path, "w", **profile) as dst:
            dst.write(raster, 1)
        logger.info(f"GeoTIFF berhasil ditulis: {output_path}")
        logger.debug("=== Selesai generate_density_geotiff ===")
        return output_path
