import dataclasses

from pyspark.sql import SparkSession
from sedona.spark import SedonaContext, SedonaRegistrator


@dataclasses.dataclass
class SparkConfig:
    workers: int
    driver_memory: str
    executor_memory: str
    logs_directory: str


def create_session(spark_config: SparkConfig) -> SparkSession:

    builder = SedonaContext.builder() \
        .appName("Sedona-Benchmark") \
        .master(f"local[{spark_config.workers}]") \
        .config("spark.driver.memory", spark_config.driver_memory) \
        .config("spark.executor.memory", spark_config.executor_memory) \
        .config(
            "spark.driver.extraJavaOptions",
            f"-Dlog4j.configurationFile=bench-logger.properties -Dspark.log.dir={spark_config.logs_directory}"
        )

    spark_session = builder.getOrCreate()
    SedonaRegistrator.registerAll(spark_session)
    spark_session = SedonaContext.create(spark=spark_session)
    
    return spark_session
    