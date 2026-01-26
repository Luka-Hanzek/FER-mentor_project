import os
import gpxpy
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType,
    StructField,
    DoubleType,
    StringType,
    TimestampType
)
from pyspark.sql.functions import expr


def load_vehicle_tracks_from_folder(folder_path, sedona):
    rows = []

    for filename in os.listdir(folder_path):
        if filename.endswith(".gpx"):
            filepath = os.path.join(folder_path, filename)

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

    sdf = sedona.createDataFrame(rows, schema=schema)

    sdf = sdf.withColumn(
        "geometry",
        expr("ST_Point(lon, lat)")
    )

    return sdf
