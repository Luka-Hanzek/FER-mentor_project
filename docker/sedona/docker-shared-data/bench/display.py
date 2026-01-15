import pyspark.sql
from pyspark.sql.functions import lit
import geopandas as gpd


def visualize(df: pyspark.sql.DataFrame, image_path: str):
    gdf = gpd.GeoDataFrame(df, geometry="geometry")

    ax = gdf.toPandas().plot(figsize=(12, 12), color="blue", linewidth=1)
    fig = ax.get_figure()
    fig.savefig(image_path)
    print(f"Saved plot to {image_path}")
