import argparse
from pathlib import Path

import sedona_fer.data.import_export
import sedona_fer.data.geom
import sedona_fer.util

import pyspark.sql
import sedona.spark


def parse_args():
    parser = argparse.ArgumentParser(
        description="Load OSM or PBF file and export geometries to GeoParquet"
    )

    parser.add_argument(
        "--src-file",
        required=True,
        help="Path to input OSM or PBF file"
    )

    parser.add_argument(
        "--out-geom-dir",
        required=True,
        help="Directory where output GeoParquet files will be written"
    )

    return parser.parse_args()


def main():
    args = parse_args()

    src_file = args.src_file
    out_geom_dir = Path(args.out_geom_dir)
    out_geom_dir.mkdir(parents=True, exist_ok=True)

    spark = pyspark.sql.SparkSession.builder \
        .master("local[*]") \
        .appName("Creating geometries") \
        .getOrCreate()

    sedona.spark.SedonaRegistrator.registerAll(spark)
    spark_session = sedona.spark.SedonaContext.create(spark=spark)

    if src_file.endswith(".osm"):
        loader = sedona_fer.data.import_export.OsmLoader(spark_session=spark_session)
    elif src_file.endswith(".pbf") or src_file.endswith(".osm.pbf"):
        loader = sedona_fer.data.import_export.PbfLoader(spark_session=spark_session)
    else:
        raise ValueError(f"Unsupported file format: {src_file}")

    df = loader.load_dataframe(data_path=src_file)

    geom_maker = sedona_fer.data.geom.GeometryMaker(spark_session=spark_session)

    filename_without_extension = sedona_fer.util.get_filename_without_extension(src_file)

    # Points
    points = geom_maker.process_points(df)
    points.write.format("geoparquet").save(
        str(out_geom_dir / f"{filename_without_extension}_points.parquet")
    )
    # Free memory
    points.unpersist()
    del points

    # Ways
    ways = geom_maker.process_ways(df)
    ways.write.format("geoparquet").save(
        str(out_geom_dir / f"{filename_without_extension}_ways.parquet")
    )
    # Free memory
    ways.unpersist()
    del ways


if __name__ == "__main__":
    main()
