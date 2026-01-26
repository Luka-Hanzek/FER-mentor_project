import sedona.sql.types
import pyspark.sql
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType,
    StructField,
    DoubleType,
    StringType,
    TimestampType
)
from pyspark.sql.functions import expr

import subprocess
import os

import gpxpy

import sedona_fer.util


class LoaderError(Exception):
    pass


class Loader(sedona_fer.util.LoggingMixin):
    def __init__(self, spark_session: SparkSession):
        self._spark_session = spark_session
        super().__init__()

    def load_dataframe(self, data_path: str) -> pyspark.sql.DataFrame:
        raise NotImplementedError


class PbfLoader(Loader):
    def load_dataframe(self, data_path: str) -> pyspark.sql.DataFrame:
        
        return self._spark_session.read.format("osmpbf").load(data_path)

class OsmLoader(Loader):
    def __init__(self, spark_session):
        super().__init__(spark_session)

        self._cached_maps_dir = os.path.join(
            os.path.dirname(__file__),
            "osm_loader_cached_maps"
        )

        os.makedirs(self._cached_maps_dir, exist_ok=True)

    def load_dataframe(self, data_path: str) -> pyspark.sql.DataFrame:
        in_filename_without_ext = sedona_fer.util.get_filename_without_extension(data_path)
        pbf_file_path = f"{os.path.join(self._cached_maps_dir, in_filename_without_ext)}.pbf"
        
        if os.path.exists(pbf_file_path):
            while (answer := input(
                f"Cached PBF file {pbf_file_path} already exists at: {pbf_file_path}. \
                    Regenerate? (y/n) ")
            ) not in ("y", "n"):
                pass
            if answer == "n":
                self._logger.info("Using cached PBF file %s", pbf_file_path)
                return self._spark_session.read.format("osmpbf").load(pbf_file_path)

        result: subprocess.CompletedProcess = subprocess.run(
            args=[
                "osmconvert",
                data_path,
                f"-o={pbf_file_path}",
            ],
            capture_output=True
        )
        if result.returncode != 0:
            self._logger.error(
                "osmconvert failed with code %d: %s",
                result.returncode,
                result.stderr.decode("utf-8"),
            )
            raise LoaderError("Failed to convert OSM to PBF format")

        return self._spark_session.read.format("osmpbf").load(pbf_file_path)


class ParquetLoader(Loader):
    def load_dataframe(self, data_path: str) -> pyspark.sql.DataFrame:
        df = (
            self._spark_session
            .read.format("geoparquet")
            .load(data_path)
        )
        assert isinstance(
            df.schema["geometry"].dataType,
            sedona.sql.types.GeometryType,
        )

        return df


class Writer(sedona_fer.util.LoggingMixin):
    def __init__(self, spark_session: SparkSession):
        self._spark_session = spark_session
        super().__init__()

    def write_dataframe(self, df: pyspark.sql.DataFrame, output_path: str):
        raise NotImplementedError


class ParquetWriter(Writer):
    def write_dataframe(self, df: pyspark.sql.DataFrame, output_path: str):
        df.write.format("geoparquet").save(output_path)


class VehicleLoader(Loader):
    def load_dataframe(self, data_path):
        rows = []

        for filename in os.listdir(data_path):
            if filename.endswith(".gpx"):
                filepath = os.path.join(data_path, filename)

                with open(filepath, "r") as gpx_file:
                    gpx = gpxpy.parse(gpx_file)

                    for track in gpx.tracks:
                        track_name = track.name or os.path.splitext(filename)[0]

                        for segment in track.segments:
                            for point in segment.points:
                                rows.append(
                                    Row(
                                        lat=float(point.latitude),
                                        lon=float(point.longitude),
                                        track=track_name,
                                        time=point.time
                                    )
                                )

        schema = StructType([
            StructField("lat", DoubleType(), False),
            StructField("lon", DoubleType(), False),
            StructField("track", StringType(), False),
            StructField("time", TimestampType(), True)
        ])

        sdf = self._spark_session.createDataFrame(rows, schema=schema)

        sdf = sdf.withColumn(
            "geometry",
            expr("ST_Point(lon, lat)")
        )

        return sdf
