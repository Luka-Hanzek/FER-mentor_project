import sys
sys.path.append("/docker-shared-data/src/bench")    # TODO: fix module imports
import import_export


PARQUET_PATH = "/docker-shared-data/geom/zagreb01/zagreb01_points.parquet"

import pyspark.sql
import sedona.spark

# TODO: Properly configure spark session
spark = pyspark.sql.SparkSession.builder \
    .master("local[4]") \
    .appName("Learning Sedona") \
    .getOrCreate()

spark_session = sedona.spark.SedonaContext.create(spark=spark)

parquet_loader = import_export.ParquetLoader(spark_session=spark_session)
df = parquet_loader.load_dataframe(data_path=PARQUET_PATH)

df.show(10, truncate=False)
