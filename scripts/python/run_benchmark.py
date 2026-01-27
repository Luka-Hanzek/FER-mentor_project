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

    return parser.parse_args()


def main():
    cli_args = parse_args()

    benchmark = sedona_fer.bench.benchmark.SedonaBenchmark(cli_args.config)

    benchmark.run_benchmarks()
    
    benchmark.export_results()


if __name__ == "__main__":
    main()