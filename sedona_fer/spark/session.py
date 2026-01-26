import dataclasses

from pyspark.sql import SparkSession
from sedona.spark import SedonaContext


@dataclasses.dataclass
class SparkConfig:
    workers: int
    driver_memory: str
    executor_memory: str
    additional_config: dict = dataclasses.field(default_factory=dict)


def create_session(spark_config: SparkConfig) -> SparkSession:
    builder = SedonaContext.builder() \
        .appName("Sedona-Benchmark") \
        .master(f"local[{spark_config.workers}]") \
        .config("spark.driver.memory", spark_config.driver_memory) \
        .config("spark.executor.memory", spark_config.executor_memory) \
        .config(
            "spark.driver.extraJavaOptions",
            "-Dlog4j.configurationFile=bench-logger.properties"
        )

    # Add any additional Spark configurations
    for key, value in spark_config.additional_config.items():
        builder = builder.config(key, value)

    spark_session = builder.getOrCreate()
    return spark_session
