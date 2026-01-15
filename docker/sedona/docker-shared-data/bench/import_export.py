import sedona.sql.types
import pyspark.sql
from pyspark.sql import SparkSession

import util


class LoaderError(Exception):
    pass


class Loader(util.LoggingMixin):
    def __init__(self, spark_session: SparkSession):
        self._spark_session = spark_session
        super().__init__()

    def load_dataframe(self, data_path: str) -> pyspark.sql.DataFrame:
        raise NotImplementedError


class PbfLoader(Loader):
    def load_dataframe(self, data_path: str) -> pyspark.sql.DataFrame:
        return self._spark_session.read.format("osmpbf").load(data_path)


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


class Writer(util.LoggingMixin):
    def __init__(self, spark_session: SparkSession):
        self._spark_session = spark_session
        super().__init__()

    def write_dataframe(self, df: pyspark.sql.DataFrame, output_path: str):
        raise NotImplementedError


class ParquetWriter(Writer):
    def write_dataframe(self, df: pyspark.sql.DataFrame, output_path: str):
        df.write.format("geoparquet").save(output_path)
