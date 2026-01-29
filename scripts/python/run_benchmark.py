import argparse
from pathlib import Path

import sedona_fer.bench.benchmark


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run benchmark"
    )

    def validate(value):
        if not Path(value).is_file():
            raise argparse.ArgumentTypeError(f"Config file {value} does not exist")
        return value

    parser.add_argument(
        "--config",
        required=True,
        help="Path to yaml configuration file",
        type=validate
    )
    parser.add_argument(
        "--src-file",
        required=True,
        help="Path to input OSM or PBF file"
    )

    parser.add_argument(
        "--out-geom-dir",
        required=True,
        help="Directory where output GeoParquet files will be written (WARNING: Existing files will be overwritten!)"
    )

    return parser.parse_args()


def main():
    cli_args = parse_args()

    benchmark = sedona_fer.bench.benchmark.SedonaBenchmark(cli_args)

    benchmark.run_benchmarks()
    
    benchmark.export_results()


if __name__ == "__main__":
    main()