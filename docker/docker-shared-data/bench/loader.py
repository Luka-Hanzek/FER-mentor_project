import pyspark
import sedona
import os

import util
from enum import Enum

from pyspark.sql import SparkSession

from collections import defaultdict


class MapFormat(Enum):
    PBF = 1
    PARQUET = 2


class LoaderErrror(Exception):
    pass


class Loader:
    def __init__(self, maps_directory: str, spark_session: SparkSession):
        self._maps_directory = maps_directory
        self._spark_session = spark_session

        if not os.path.isdir(maps_directory):
            raise LoaderErrror(f"{maps_directory} is not a directory.")

        self._map_files: dict[str, list[dict]] = defaultdict(list)
        for filename in os.listdir(self._maps_directory):
            extension = util.get_filename_extension(filename)
            map_name = util.get_filename_without_extension(filename)
            full_path=os.path.join(self._maps_directory, filename)

            if extension in (".osm.pbf", ".pbf", ):
                self._map_files[map_name].append(dict(
                    format=MapFormat.PBF,
                    full_path=full_path,
                ))
            elif extension in (".parquet", ):
                self._map_files[map_name].append(dict(
                    format=MapFormat.PARQUET,
                    full_path=full_path,
                ))
            else:
                continue

    def load_map(self, map_name: str, format: MapFormat) -> pyspark.sql.DataFrame:
        if map_name not in self._map_files:
            raise LoaderErrror(f"Map {map_name} not found.")

        maps = self._map_files[map_name]
        if format not in [map_info["format"] for map_info in maps]:
            raise LoaderErrror(f"Map {map_name} not available in format {format.name}.")
        
        maps = self._map_files[map_name]
        for map_info in maps:
            if map_info["format"] == MapFormat.PARQUET:
                df = self._spark_session.read.parquet(map_info["full_path"])
                df = df.withColumn("geometry", sedona.spark.sql.ST_GeomFromWKB("geometry"))
                return df
            elif map_info["format"] == MapFormat.PBF:
                return sedona.read.format("osmpbf").load(map_info["full_path"])
