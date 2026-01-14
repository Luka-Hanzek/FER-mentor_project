import osmium
from shapely import wkb
import geopandas as gpd


FILE = "/home/luka/FER/PROJECT/docker/sedona/docker-shared-data/openstreetmap/maps/croatia-260106.osm.pbf"

LIMIT = 10


class NodeHandler(osmium.SimpleHandler):
    def __init__(self):
        super().__init__()
        self.factory = osmium.geom.WKBFactory()
        self.rows = []

    def node(self, n):
        print(n)

        if not n.location.valid():
            return

        geom = wkb.loads(bytes.fromhex(
            self.factory.create_point(n.location)
        ))

        self.rows.append({
            "osm_id": n.id,
            "tags": dict(n.tags),
            "geometry": geom
        })

node_handler = NodeHandler()
node_handler.apply_file(
    FILE
)

gdf = gpd.GeoDataFrame(
    data=node_handler.rows,
    geometry="geometry"
)

gdf.to_parquet("ways.parquet")
