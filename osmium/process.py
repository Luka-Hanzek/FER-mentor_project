import shapely
import osmium
import osmium.geom
import osmium.osm.types
import os
import geopandas


class ExportError(Exception):
    pass


class Exporter:
    def __init__(self, dest: str):
        self._dest = dest

    def export(self, rows: list[dict], overwrite: bool = False):
        out_file_path = f"{self._dest}.parquet"
        
        if os.path.exists(out_file_path) and not overwrite:
            raise ExportError(f"File {self._dest}.parquet already exists.")

        geometry = self._create_geometry_export_column(
            data=[row["geometry"] for row in rows]
        )

        gdf = geopandas.GeoDataFrame(
            data=rows,
            columns=[
                "osm_id",
                "tags"
            ],
            geometry=geometry,
        )

        gdf.to_parquet(
            path=out_file_path,
            geometry_encoding="WKB",
        )


    def _create_geometry_export_column(self, data: list) -> list[shapely.geometry.base.BaseGeometry]:
        return shapely.wkt.loads(data)


class NodeHandler(osmium.SimpleHandler):
    def __init__(self):
        self.factory = osmium.geom.WKTFactory()
        self.rows = []

    def node(self, n: osmium.osm.types.Node):
        if not n.location.valid():
            raise Exception(
                f"Invalid node location. osm_id: {n.id}. \
                Don't know how to handle this."
            )

        geom = self.factory.create_point(n.location)
        self.rows.append({
            "osm_id": n.id,
            "tags": dict(n.tags),
            "geometry": geom
        })


class WayHandler(osmium.SimpleHandler):
    def __init__(self):
        self.factory = osmium.geom.WKTFactory()
        self.rows = []

    def way(self, w: osmium.osm.types.Way):
        geom = self.factory.create_linestring(w)

        self.rows.append({
            "osm_id": w.id,
            "tags": dict(w.tags),
            "geometry": geom
        })


def validate_output_directory(dir):
    os.makedirs(dir, exist_ok=True)

    return dir


if __name__ == "__main__":
    import argparse

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("--input-file", type=str, required=True)
    arg_parser.add_argument(
        "--output-directory",
        required=True,
        type=validate_output_directory,
    )

    cli_args = arg_parser.parse_args()


    node_handler = NodeHandler()
    way_handler = WayHandler()

    node_handler.apply_file(cli_args.input_file)

    # NOTE: locations=True is required for geometry reconstruction in WayHandler
    #       see: create_linestring
    way_handler.apply_file(cli_args.input_file, locations=True)

    exporter = Exporter(dest=os.path.join(cli_args.output_directory, "nodes"))
    exporter.export(node_handler.rows, overwrite=False)
    exporter = Exporter(dest=os.path.join(cli_args.output_directory, "ways"))
    exporter.export(way_handler.rows, overwrite=False)
