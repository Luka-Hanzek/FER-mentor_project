import time
import json
import yaml
import argparse
import statistics
import psutil
import os

import matplotlib.pyplot as plt
import pandas as pd

from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional

from pyspark.sql import SparkSession
from sedona.spark import SedonaContext

import sedona_fer.data.import_export


class QueryStats:
    """Statistics calculator for query execution times"""
    
    def __init__(self, execution_times: List[float]):
        self.times = sorted(execution_times)
        self.count = len(execution_times)
    
    def calculate(self) -> Dict[str, float]:
        """Calculate comprehensive statistics"""
        if not self.times:
            return {}
        
        return {
            'count': self.count,
            'mean': statistics.mean(self.times),
            'median': statistics.median(self.times),
            'stdev': statistics.stdev(self.times) if self.count > 1 else 0,
            'variance': statistics.variance(self.times) if self.count > 1 else 0,
            'min': min(self.times),
            'max': max(self.times),
            'range': max(self.times) - min(self.times),
            'p25': self._percentile(25),
            'p50': self._percentile(50),
            'p75': self._percentile(75),
            'p95': self._percentile(95),
            'p99': self._percentile(99),
            'coefficient_of_variation': (statistics.stdev(self.times) / statistics.mean(self.times)) if self.count > 1 and statistics.mean(self.times) > 0 else 0,
        }
    
    def _percentile(self, percentile: float) -> float:
        """Calculate percentile value"""
        if not self.times:
            return 0
        index = (percentile / 100) * (self.count - 1)
        lower = int(index)
        upper = lower + 1
        
        if upper >= len(self.times):
            return self.times[lower]
        
        weight = index - lower
        return self.times[lower] * (1 - weight) + self.times[upper] * weight


class SedonaBenchmark:
    def __init__(self, config_path: str = "bench_config.yaml", 
                 overrides: Optional[Dict[str, Any]] = None,
                 query_filter: Optional[List[str]] = None,
                 include_query_plans: bool = False,
                 include_memory_stats: bool = False):
        """Initialize benchmark framework with configuration
        
        Args:
            config_path: Path to benchmark configuration file
            overrides: Dictionary of config overrides (e.g., {'benchmark.num_runs': 5})
            query_filter: List of query names to include (if None, run all)
            include_query_plans: Whether to extract and store Spark query plans
            include_memory_stats: Whether to collect memory usage statistics
        """

        self.config = self._load_config(config_path)
        self._apply_overrides(overrides)
        self.query_filter = query_filter
        self.include_query_plans = include_query_plans
        self.include_memory_stats = include_memory_stats
        self.results = []
        self.spark_session = None
        self.process = psutil.Process(os.getpid())

    def _apply_overrides(self, overrides: Optional[Dict[str, Any]]) -> None:
        """Apply configuration overrides using dot notation (e.g., 'spark.workers': 4)"""
        if not overrides:
            return
        
        for key, value in overrides.items():
            parts = key.split('.')
            current = self.config
            for part in parts[:-1]:
                if part not in current:
                    current[part] = {}
                current = current[part]
            current[parts[-1]] = value

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

    def _execute_query(self, query: str, num_runs: int, warmup_runs: int) -> Dict[str, Any]:
        """Execute a query multiple times and record execution times and metrics"""

        execution_times = []
        query_plans = []
        memory_stats = []

        # Warmup runs (not recorded)
        print(f"Warming up ({warmup_runs} runs)", end=": ")
        for _ in range(warmup_runs):
            self.sedona.sql(query).collect()
            print("✓", end=" ", flush=True)
        print()

        # Actual benchmark runs
        print(f"Executing {num_runs} benchmark runs", end=": ")
        for run in range(num_runs):
            mem_before = None
            if self.include_memory_stats:
                mem_before = self.process.memory_info().rss / 1024 / 1024  # MB
            
            start_time = time.time()

            result_df = self.sedona.sql(query)
            
            # Capture query plan if requested (only on first run to save memory)
            if self.include_query_plans and run == 0:
                try:
                    # Capture the explain output
                    import io
                    from contextlib import redirect_stdout
                    f = io.StringIO()
                    with redirect_stdout(f):
                        result_df.explain(mode='formatted')
                    query_plans.append(f.getvalue())
                except Exception as e:
                    query_plans.append(f"Failed to capture plan: {str(e)}")
            
            result_df.collect()

            end_time = time.time()

            if self.include_memory_stats:
                mem_after = self.process.memory_info().rss / 1024 / 1024  # MB
                memory_stats.append({
                    'memory_before_mb': mem_before,
                    'memory_after_mb': mem_after,
                    'memory_delta_mb': mem_after - mem_before
                })

            execution_time = end_time - start_time
            execution_times.append(execution_time)

            print(f"✓ ", end="", flush=True)
        print("\nAll runs completed.")

        result = {
            'execution_times': execution_times,
            'query_plans': query_plans if self.include_query_plans else None,
            'memory_stats': memory_stats if self.include_memory_stats else None,
        }

        return result

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
        
        # Filter queries if specified
        if self.query_filter:
            query_files = [qf for qf in query_files if qf.stem in self.query_filter]
            print(f"\nFiltered to {len(query_files)} queries")
        
        # Execute each query
        print("\n" + "=" * 70)
        print(" " * 15 + "Running Benchmarks")
        print("=" * 70)

        for query_file in query_files:
            print(f"\nRunning query: {query_file.name}...")

            # Read query
            with open(query_file, 'r') as f:
                query = f.read()

            # Execute and time
            num_runs = self.config['benchmark']['num_runs']
            warmup_runs = self.config['benchmark'].get('warmup_runs', 1)
            exec_result = self._execute_query(query, num_runs, warmup_runs)
            execution_times = exec_result['execution_times']

            # Calculate statistics
            stats_calc = QueryStats(execution_times)
            stats = stats_calc.calculate()

            # Store results
            result = {
                'query_name': query_file.stem,
                'query_file': query_file.name,
                'execution_times': execution_times,
                'statistics': stats,
                'query_plan': exec_result['query_plans'][0] if exec_result['query_plans'] else None,
                'memory_stats': exec_result['memory_stats'],
                'timestamp': datetime.now().isoformat()
            }
            self.results.append(result)

            # Print summary
            print(f"Mean: {stats['mean']:.4f}s | "
                  f"Median: {stats['median']:.4f}s | "
                  f"Stdev: {stats['stdev']:.4f}s | "
                  f"P95: {stats['p95']:.4f}s")

        # Cleanup
        self.spark_session.stop()
        print("\n" + "=" * 70)
        print("Benchmark completed successfully!")
        print("=" * 70)

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

        # 1. Bar chart: Average execution time per query + error bars
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))

        # Average with error bars (stdev)
        query_means = df.groupby('Query')['Time (s)'].mean()
        query_stdev = df.groupby('Query')['Time (s)'].std()
        query_means_sorted = query_means.sort_values()
        query_stdev_sorted = query_stdev[query_means_sorted.index]
        
        query_means_sorted.plot(kind='barh', ax=ax1, color='steelblue', xerr=query_stdev_sorted)
        ax1.set_xlabel('Average Execution Time (s)')
        ax1.set_title('Average Query Performance (with Standard Deviation)')
        ax1.grid(axis='x', alpha=0.3)

        # 2. Box plot: Distribution of execution times
        queries = sorted(df['Query'].unique())
        data_for_box = [df[df['Query'] == q]['Time (s)'].values for q in queries]
        ax2.boxplot(data_for_box, labels=queries, vert=False)
        ax2.set_xlabel('Execution Time (s)')
        ax2.set_title('Query Performance Distribution (Box Plot)')
        ax2.grid(axis='x', alpha=0.3)

        # 3. Violin plot: Distribution shape
        df_sorted = df.copy()
        df_sorted['Query'] = pd.Categorical(df_sorted['Query'], categories=queries, ordered=True)
        df_sorted = df_sorted.sort_values('Query')
        for i, query in enumerate(queries, 1):
            query_data = df_sorted[df_sorted['Query'] == query]['Time (s)'].values
            parts = ax3.violinplot([query_data], positions=[i], vert=False, showmeans=True)
        ax3.set_yticks(range(1, len(queries) + 1))
        ax3.set_yticklabels(queries)
        ax3.set_xlabel('Execution Time (s)')
        ax3.set_title('Query Performance Distribution (Violin Plot)')
        ax3.grid(axis='x', alpha=0.3)

        # 4. Line plot: Performance across runs (showing stability)
        for query in queries:
            query_data = df[df['Query'] == query]
            ax4.plot(query_data['Run'], query_data['Time (s)'],
                    marker='o', label=query, linewidth=1.5, markersize=4, alpha=0.7)

        ax4.set_xlabel('Run Number')
        ax4.set_ylabel('Execution Time (s)')
        ax4.set_title('Query Performance Stability Across Runs')
        ax4.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=8)
        ax4.grid(alpha=0.3)

        plt.tight_layout()
        plot_file = output_dir / f"benchmark_analysis_{timestamp}.png"
        plt.savefig(plot_file, dpi=300, bbox_inches='tight')
        print(f"Analysis plot saved to: {plot_file}")
        plt.close()

        # 5. Statistical summary plot
        fig, axes = plt.subplots(2, 2, figsize=(14, 10))
        
        # Percentile comparison
        percentiles_data = []
        for result in self.results:
            stats = result['statistics']
            percentiles_data.append({
                'Query': result['query_name'],
                'p50': stats['p50'],
                'p75': stats['p75'],
                'p95': stats['p95'],
                'p99': stats['p99'],
            })
        pdf = pd.DataFrame(percentiles_data).set_index('Query')
        pdf.plot(kind='bar', ax=axes[0, 0], color=['green', 'orange', 'red', 'darkred'])
        axes[0, 0].set_title('Percentile Analysis')
        axes[0, 0].set_ylabel('Time (s)')
        axes[0, 0].legend(title='Percentile')
        axes[0, 0].grid(axis='y', alpha=0.3)

        # Coefficient of Variation (stability metric)
        cv_data = [result['statistics']['coefficient_of_variation'] for result in self.results]
        queries_cv = [result['query_name'] for result in self.results]
        axes[0, 1].bar(range(len(queries_cv)), cv_data, color=['green' if cv < 0.1 else 'orange' if cv < 0.2 else 'red' for cv in cv_data])
        axes[0, 1].set_xticks(range(len(queries_cv)))
        axes[0, 1].set_xticklabels(queries_cv, rotation=45, ha='right')
        axes[0, 1].set_ylabel('Coefficient of Variation')
        axes[0, 1].set_title('Performance Stability (Lower is Better)')
        axes[0, 1].axhline(y=0.1, color='g', linestyle='--', alpha=0.5, label='Excellent (<0.1)')
        axes[0, 1].axhline(y=0.2, color='orange', linestyle='--', alpha=0.5, label='Good (<0.2)')
        axes[0, 1].grid(axis='y', alpha=0.3)

        # Min/Max range
        ranges = [result['statistics']['range'] for result in self.results]
        axes[1, 0].bar(range(len(queries_cv)), ranges, color='skyblue')
        axes[1, 0].set_xticks(range(len(queries_cv)))
        axes[1, 0].set_xticklabels(queries_cv, rotation=45, ha='right')
        axes[1, 0].set_ylabel('Time Range (s)')
        axes[1, 0].set_title('Min-Max Range (Smaller is More Consistent)')
        axes[1, 0].grid(axis='y', alpha=0.3)

        # Mean comparison
        means = [result['statistics']['mean'] for result in self.results]
        axes[1, 1].bar(range(len(queries_cv)), means, color='steelblue')
        axes[1, 1].set_xticks(range(len(queries_cv)))
        axes[1, 1].set_xticklabels(queries_cv, rotation=45, ha='right')
        axes[1, 1].set_ylabel('Mean Time (s)')
        axes[1, 1].set_title('Mean Execution Time')
        axes[1, 1].grid(axis='y', alpha=0.3)

        plt.tight_layout()
        stats_plot_file = output_dir / f"benchmark_statistics_{timestamp}.png"
        plt.savefig(stats_plot_file, dpi=300, bbox_inches='tight')
        print(f"Statistics plot saved to: {stats_plot_file}")
        plt.close()


def main():
    parser = argparse.ArgumentParser(
        description="Apache Sedona Benchmark Framework",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run with default config
  python run.py
  
  # Run with specific config file
  python run.py --config custom_config.yaml
  
  # Run only specific queries
  python run.py --queries query_01_count_roads query_02_road_length
  
  # Override configuration options
  python run.py --override benchmark.num_runs=20 spark.workers=16
  
  # Enable advanced analysis
  python run.py --query-plans --memory-stats
  
  # Combine options
  python run.py --queries query_01_count_roads --num-runs 50 --memory-stats
        """
    )
    
    parser.add_argument(
        '--config',
        default='bench_config.yaml',
        help='Path to benchmark configuration file (default: bench_config.yaml)'
    )
    
    parser.add_argument(
        '--num-runs',
        type=int,
        help='Override number of benchmark runs'
    )
    
    parser.add_argument(
        '--warmup-runs',
        type=int,
        help='Override number of warmup runs'
    )
    
    parser.add_argument(
        '--workers',
        type=int,
        help='Override number of Spark workers'
    )
    
    parser.add_argument(
        '--driver-memory',
        help='Override Spark driver memory (e.g., 8g)'
    )
    
    parser.add_argument(
        '--executor-memory',
        help='Override Spark executor memory (e.g., 8g)'
    )
    
    parser.add_argument(
        '--queries',
        nargs='+',
        help='Run only specific queries (query names without extension)'
    )
    
    parser.add_argument(
        '--override',
        nargs='+',
        help='Override config values using dot notation (e.g., benchmark.num_runs=20)'
    )
    
    parser.add_argument(
        '--query-plans',
        action='store_true',
        help='Extract and save Spark query plans (can be large)'
    )
    
    parser.add_argument(
        '--memory-stats',
        action='store_true',
        help='Collect memory usage statistics during execution'
    )
    
    parser.add_argument(
        '--skip-plots',
        action='store_true',
        help='Skip visualization generation'
    )
    
    args = parser.parse_args()
    
    # Build overrides from command-line arguments
    overrides = {}
    
    if args.num_runs:
        overrides['benchmark.num_runs'] = args.num_runs
    if args.warmup_runs:
        overrides['benchmark.warmup_runs'] = args.warmup_runs
    if args.workers:
        overrides['spark.workers'] = args.workers
    if args.driver_memory:
        overrides['spark.driver_memory'] = args.driver_memory
    if args.executor_memory:
        overrides['spark.executor_memory'] = args.executor_memory
    
    # Parse additional overrides
    if args.override:
        for override in args.override:
            key, value = override.split('=')
            # Try to parse as int/float/bool
            try:
                value = int(value)
            except ValueError:
                try:
                    value = float(value)
                except ValueError:
                    if value.lower() in ('true', 'false'):
                        value = value.lower() == 'true'
            overrides[key] = value
    
    # Run benchmark
    benchmark = SedonaBenchmark(
        config_path=args.config,
        overrides=overrides if overrides else None,
        query_filter=args.queries,
        include_query_plans=args.query_plans,
        include_memory_stats=args.memory_stats
    )
    
    benchmark.run_benchmarks()
    benchmark.save_results()
    
    if not args.skip_plots:
        benchmark.generate_plots()


if __name__ == "__main__":
    main()
