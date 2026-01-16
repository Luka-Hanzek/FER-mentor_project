import time
import json
import yaml

import matplotlib.pyplot as plt
import pandas as pd

from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any

from pyspark.sql import SparkSession
from sedona.spark import SedonaContext

import sedona_fer.data.import_export


class SedonaBenchmark:
    def __init__(self, config_path: str = "bench_config.yaml"):
        """Initialize benchmark framework with configuration"""

        self.config = self._load_config(config_path)
        self.results = []
        self.spark_session = None

    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load benchmark configuration from YAML file"""

        with open(config_path, 'r') as f:
            return yaml.safe_load(f)

    def _init_spark(self, spark_config: dict):
        """Initialize Spark session with Sedona extensions"""

        builder = SparkSession.builder \
            .appName("Sedona-Benchmark") \
            .master(f"local[{spark_config['workers']}]") \
            .config("spark.driver.memory", spark_config['driver_memory']) \
            .config("spark.executor.memory", spark_config['executor_memory']) \
            .config(
                "spark.driver.extraJavaOptions",
                "-Dlog4j.configurationFile=bench-logger.properties"
            )

        # Add any additional Spark configurations
        for key, value in spark_config.get('additional_config', {}).items():
            builder = builder.config(key, value)

        self.spark_session = builder.getOrCreate()
        self.sedona = SedonaContext.create(self.spark_session)

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

    def _execute_query(self, query: str, num_runs: int, warmup_runs: int) -> List[float]:
        """Execute a query multiple times and record execution times"""

        execution_times = []

        # Warmup runs (not recorded)
        print(f"Warming up ({warmup_runs} runs)", end=": ")
        for _ in range(warmup_runs):
            self.sedona.sql(query).collect()
            print("✓", end=" ", flush=True)
        print()

        # Actual benchmark runs
        print(f"Executing {num_runs} benchmark runs", end=": ")
        for run in range(num_runs):
            start_time = time.time()

            result_df = self.sedona.sql(query)
            result_df.collect()

            end_time = time.time()

            execution_time = end_time - start_time
            execution_times.append(execution_time)

            print(f"✓ ", end="", flush=True)
        print("\nAll runs completed.")

        return execution_times

    def run_benchmarks(self):
        """Main benchmark execution method"""

        print("Starting benchmark")

        # Initialize Spark
        print("Initializing environment.")
        spark_config = self.config['spark']
        self._init_spark(spark_config)

        # Load dataset
        print(f"Loading dataset from: {self.config['dataset']['path']}.")
        self._load_dataset(self.config['dataset'])
        
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
            execution_times = self._execute_query(query, num_runs, warmup_runs)


            # Store results
            result = {
                'query_name': query_file.stem,
                'query_file': query_file.name,
                'execution_times': execution_times,
                'avg_time': sum(execution_times) / len(execution_times),
                'min_time': min(execution_times),
                'max_time': max(execution_times),
                'timestamp': datetime.now().isoformat()
            }
            self.results.append(result)

            print(f"Avg: {result['avg_time']:.3f}s | "
                  f"Min: {result['min_time']:.3f}s | "
                  f"Max: {result['max_time']:.3f}s")

        # Cleanup
        self.spark_session.stop()
        print("Benchmark completed successfully!")

    def save_results(self):
        """Save benchmark results to JSON file"""

        output_dir = Path(self.config['output']['directory'])
        output_dir.mkdir(exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        results_file = output_dir / f"benchmark_results_{timestamp}.json"

        with open(results_file, 'w') as f:
            json.dump({
                'config': self.config,
                'results': self.results
            }, f, indent=2)

        print(f"Results saved to: {results_file}")

    def generate_plots(self):
        """Generate performance visualization graphs"""

    def generate_plots(self):
        """Generate performance visualization graphs"""

        output_dir = Path(self.config['output']['directory'])
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Create DataFrame for easier plotting
        data = []
        for result in self.results:
            for i, exec_time in enumerate(result['execution_times']):
                data.append({
                    'Query': result['query_name'],
                    'Run': i + 1,
                    'Time (s)': exec_time
                })
        df = pd.DataFrame(data)

        # 1. Bar chart: Average execution time per query
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))

        avg_times = df.groupby('Query')['Time (s)'].mean().sort_values()
        avg_times.plot(kind='barh', ax=ax1, color='steelblue')
        ax1.set_xlabel('Average Execution Time (s)')
        ax1.set_title('Average Query Performance')
        ax1.grid(axis='x', alpha=0.3)

        # 2. Box plot: Distribution of execution times
        queries = df['Query'].unique()
        data_for_box = [df[df['Query'] == q]['Time (s)'].values for q in queries]
        ax2.boxplot(data_for_box, labels=queries, vert=False)
        ax2.set_xlabel('Execution Time (s)')
        ax2.set_title('Query Performance Distribution')
        ax2.grid(axis='x', alpha=0.3)

        plt.tight_layout()
        plot_file = output_dir / f"benchmark_plot_{timestamp}.png"
        plt.savefig(plot_file, dpi=300, bbox_inches='tight')
        print(f"Visualization saved to: {plot_file}")

        # 3. Detailed line plot for each query
        fig, ax = plt.subplots(figsize=(12, 6))
        for query in queries:
            query_data = df[df['Query'] == query]
            ax.plot(query_data['Run'], query_data['Time (s)'],
                    marker='o', label=query, linewidth=2)

        ax.set_xlabel('Run Number')
        ax.set_ylabel('Execution Time (s)')
        ax.set_title('Query Performance Across Runs')
        ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        ax.grid(alpha=0.3)

        plt.tight_layout()
        detail_plot_file = output_dir / f"benchmark_detail_{timestamp}.png"
        plt.savefig(detail_plot_file, dpi=300, bbox_inches='tight')
        print(f"Detailed plot saved to: {detail_plot_file}")


def main():
    benchmark = SedonaBenchmark("bench_config.yaml")
    benchmark.run_benchmarks()
    benchmark.save_results()
    benchmark.generate_plots()


if __name__ == "__main__":
    main()
