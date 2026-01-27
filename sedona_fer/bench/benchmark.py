import time
import yaml

import matplotlib.pyplot as plt
import pandas as pd

from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any

import sedona_fer.data.import_export
import sedona_fer.spark.session
import sedona_fer.bench.results
import sedona_fer.bench.models as models


class SedonaBenchmark:
    def __init__(self, config_path: str):
        """Initialize benchmark framework with configuration"""

        self.config = self._load_config(config_path)
        self._benchamrk_timestamp: str = None
        self.results: models.BenchmarkResult = models.BenchmarkResult(
            config=self.config,
            timestamp=None,
            query_results=[],
        )
        self.spark_session: sedona_fer.spark.session.SparkSession = None

    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load benchmark configuration from YAML file"""

        with open(config_path, 'r') as f:
            return yaml.safe_load(f)

    def _init_spark(self, spark_config: dict):
        """Initialize Spark session with Sedona extensions"""

        self.spark_session = sedona_fer.spark.session.create_session(
            spark_config=sedona_fer.spark.session.SparkConfig(**spark_config)
        )

        print(f"Spark initialized with {spark_config['workers']} workers")
        print(f"Driver memory: {spark_config['driver_memory']}")
        print(f"Executor memory: {spark_config['executor_memory']}")

    def _load_dataset(self, dataset_config: dict):
        """Load the dataset specified in configuration"""

        path = dataset_config['path']
        format_type = dataset_config['format']

        # TODO: Since data is contained in multiple directories (points, ways[, relations]),
        #   implement logic to load all relevant files.
        if format_type == 'parquet':
            loader = sedona_fer.data.import_export.ParquetLoader(spark_session=self.spark_session)
            df = loader.load_dataframe(path)
        elif format_type == 'gpx':
            loader = sedona_fer.data.import_export.VehicleLoader(spark_session=self.spark_session)
            df = loader.load_dataframe(data_path=path)
        else:
            raise ValueError(f"Unsupported format: {format_type}")

        # Register as temp view
        view_name = dataset_config['view_name']
        df.createOrReplaceTempView(view_name)
        print(f"Dataset registered as view: {view_name}")

        return df

    def _discover_queries(self, directory: str) -> List[Path]:
        """Find all .sql files in queries directory"""

        sql_files = sorted(Path(directory).glob('*.sql'))

        if not sql_files:
            raise FileNotFoundError(f"No .sql files found in {directory}")

        print(f"Found {len(sql_files)} queries:")
        for sql_file in sql_files:
            print(f"  - {sql_file.name}")

        return sql_files

    def _execute_query(self, query: str, num_runs: int, warmup_runs: int) -> List[models.QueryRun]:
        """Execute a query multiple times and record execution times"""

        results = []

        # Warmup runs (not recorded)
        print(f"Warming up ({warmup_runs} runs)", end=": ")
        for _ in range(warmup_runs):
            self.spark_session.sql(query).collect()
            print("✓", end=" ", flush=True)
        print()

        # Actual benchmark runs
        print(f"Executing {num_runs} benchmark runs", end=": ")
        for run in range(num_runs):
            start_time = time.time()

            result_df = self.spark_session.sql(query)
            result_df.collect()

            end_time = time.time()
            execution_time = end_time - start_time

            query_output = result_df.toPandas().to_json()

            results.append(models.QueryRun(
                run=run,
                execution_time=execution_time,
                query_output=query_output
            ))

            print(f"✓ ", end="", flush=True)
        print("\nAll runs completed.")

        return results

    def run_benchmarks(self):
        """Main benchmark execution method"""

        print("Starting benchmark")
        self.results.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Initialize Spark
        print("Initializing environment.")
        self._init_spark(self.config['spark'])

        # Load multiple datasets
        for dataset_cfg in self.config['datasets']:
            print(f"Loading dataset: {dataset_cfg['view_name']}")
            self._load_dataset(dataset_cfg)

        # Discover queries
        print("Discovering queries...", end=" ")
        queries_directory = self.config['queries']['directory']
        query_files = self._discover_queries(queries_directory)

        # Execute each query
        print("\n" + "=" * 60)
        print(" " * 10 + "Running Benchmarks")
        print("=" * 60)

        for query_file in query_files:
            print(f"Running query: {query_file.name}...")

            # Read query
            with open(query_file, 'r') as f:
                query = f.read()

            # Execute and time
            # TODO: Add timeout handling
            num_runs = self.config['benchmark']['num_runs']
            warmup_runs = self.config['benchmark'].get('warmup_runs', 1)
            results = self._execute_query(query, num_runs, warmup_runs)

            # Store results

            execution_times = [result.execution_time for result in results]
            # All query outputs should be the same
            for query_output in [result.query_output for result in results]:
                assert query_output == results[0].query_output
            result = models.BenchmarkQueryResult(
                query=models.Query(
                    query_name=query_file.name
                ),
                timing=models.Timing(
                    execution_times=execution_times,
                    avg_time=sum(execution_times) / len(execution_times),
                    min_time=min(execution_times),
                    max_time=max(execution_times),
                ),
                query_output=models.QueryOutput(
                    output=query_output
                )
            )
            self.results.query_results.append(result)

            print(f"Avg: {result.timing.avg_time:.3f}s | "
                  f"Min: {result.timing.min_time:.3f}s | "
                  f"Max: {result.timing.max_time:.3f}s")

        # Cleanup
        self.spark_session.stop()
        print("Benchmark completed successfully!")

    def export_results(self):
        sedona_fer.bench.results.write_config(
            self.results,
            self.config["output"]["directory"],
        )
        sedona_fer.bench.results.write_timing_results(
            self.results,
            self.config["output"]["directory"],
        )
        sedona_fer.bench.results.generate_query_outputs(
            self.results,
            self.config["output"]["directory"],
        )
        sedona_fer.bench.results.generate_plots(
            self.results,
            self.config["output"]["directory"],
        )
