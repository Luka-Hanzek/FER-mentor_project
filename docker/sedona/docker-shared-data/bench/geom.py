import pyspark.sql

import util


class GeometryMaker(util.LoggingMixin):
    def __init__(self, spark_session: pyspark.sql.SparkSession):
        self.spark_session = spark_session
        super().__init__()


    def process_points(self, df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        from pyspark.sql.functions import col
        from sedona.spark.sql import ST_Point

        df_nodes = df.filter(df.kind == "node")

        self._logger.info("Ignoring ways with NULL location.")
        invalid_nodes_filter = df.location.isNotNull()
        if df_nodes.count() > (
            df_temp := df_nodes.filter(invalid_nodes_filter)
        ).count():
            self._logger.warn(
                f"Dropping {df_nodes.count() - df_temp.count()} \
                    nodes with NULL location."
            )
            df_nodes = df_temp


        df_nodes = df_nodes \
            .withColumn(
                "geometry",
                ST_Point(col("location.longitude"), col("location.latitude"))
            ) \
            .select(
                "id",
                "tags",
                "geometry",
            )

        return df_nodes

    def process_ways(self, df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        # Constructing way geometries

        from pyspark.sql.functions import col
        from sedona.spark.sql import ST_Point, ST_MakeLine
        from pyspark.sql.functions import (
            sort_array, collect_list, struct, posexplode, size,
        )

        df_ways = df.filter(col("kind") == "way")
        df_nodes = df.filter(col("kind") == "node")

        self._logger.info("Ignoring ways with 0 refs.")
        refs_not_null_filter = col("refs").isNotNull() & (size(col("refs")) > 0)
        if df_ways.count() > (
            df_temp := df_ways.filter(refs_not_null_filter)
        ).count():
            self._logger.warn(
                f"Dropping {df_ways.count() - df_temp.count()} \
                    ways with NULL or empty refs."
            )
            df_ways = df_temp

        df_ways = df_ways \
            .select(
                "*",
                posexplode("refs").alias("pos", "node_id"),
            ) \
            .alias("ways").join(other=df_nodes.alias("nodes"), on=(col("ways.node_id") == col("nodes.id"))) \
            .groupBy("ways.id", "ways.tags").agg(
                ST_MakeLine(
                    sort_array(
                        collect_list(
                            struct(
                                "pos",
                                ST_Point(col("nodes.location.longitude"), col("nodes.location.latitude")).alias("point"),
                            )
                        )
                    )["point"]
                ).alias("geometry")
            )

        return df_ways
