# Enhanced Sedona Benchmark Framework

## New Features

### 1. **Advanced Statistical Analysis**
- **Comprehensive metrics**: Mean, median, standard deviation, variance, min, max, range
- **Percentile analysis**: p25, p50, p75, p95, p99 for understanding distribution
- **Stability metrics**: Coefficient of Variation (CV) to assess query consistency
- All statistics are now stored in the JSON output for reproducibility

### 2. **Command-Line Options**

Run benchmarks with flexible configuration:

```bash
# Basic run
python run.py

# Custom config file
python run.py --config my_config.yaml

# Run specific queries only
python run.py --queries query_01_count_roads query_03_roads_by_highway_type

# Override Spark settings
python run.py --workers 16 --driver-memory 8g --executor-memory 8g

# Override benchmark parameters
python run.py --num-runs 50 --warmup-runs 5

# Enable advanced metrics
python run.py --query-plans --memory-stats

# Skip visualization
python run.py --skip-plots

# Combine options
python run.py --queries query_01 query_02 --num-runs 25 --memory-stats
```

**Configuration override syntax:**
```bash
python run.py --override benchmark.num_runs=20 spark.workers=16 spark.driver_memory=8g
```

### 3. **Enhanced Visualizations**

The framework now generates **two comprehensive visualization suites**:

#### `benchmark_analysis_*.png` - Four-panel analysis dashboard:
- **Top-left**: Average performance with standard deviation error bars
- **Top-right**: Box plots showing distribution and outliers
- **Bottom-left**: Violin plots revealing distribution shape
- **Bottom-right**: Line plots showing performance stability across runs

#### `benchmark_statistics_*.png` - Statistical insights:
- **Top-left**: Percentile comparison (p50, p75, p95, p99)
- **Top-right**: Coefficient of Variation (stability metric, colored: green <0.1, orange <0.2, red >0.2)
- **Bottom-left**: Min-Max range analysis
- **Bottom-right**: Mean execution times comparison

### 4. **Result Analysis Tools**

Analyze benchmark results after execution:

```bash
# Print full analysis report
python analyze.py results/benchmark_results_20260117_095331.json

# Export to CSV for external analysis
python analyze.py results/benchmark_results_20260117_095331.json --csv results.csv

# Save report to file
python analyze.py results/benchmark_results_20260117_095331.json --output report.txt

# Compare with baseline
python analyze.py current_run.json --compare baseline_run.json

# Print summary statistics only
python analyze.py results/benchmark_results_20260117_095331.json --summary
```

### 5. **Enhanced JSON Output**

Results now include:

```json
{
  "query_name": "query_01_count_roads",
  "execution_times": [0.186, 0.173, ...],
  "statistics": {
    "count": 10,
    "mean": 0.1529,
    "median": 0.1530,
    "stdev": 0.0212,
    "variance": 0.0004,
    "min": 0.1295,
    "max": 0.1867,
    "range": 0.0572,
    "p25": 0.1410,
    "p50": 0.1530,
    "p75": 0.1651,
    "p95": 0.1790,
    "p99": 0.1862,
    "coefficient_of_variation": 0.1387
  },
  "query_plan": "== Physical Plan ==\nCollectLimit 1\n...",
  "memory_stats": [
    {
      "memory_before_mb": 512,
      "memory_after_mb": 548,
      "memory_delta_mb": 36
    }
  ]
}
```

### 6. **Memory Profiling** (Optional)

Enable memory usage tracking:
```bash
python run.py --memory-stats
```

Results include:
- Memory before execution
- Memory after execution
- Memory delta per query run

### 7. **Query Plan Extraction** (Optional)

Capture Spark query execution plans for optimization analysis:
```bash
python run.py --query-plans
```

Plans are saved in JSON output for detailed analysis of:
- Physical execution strategy
- Stage boundaries
- Partition counts
- Shuffle operations

## Configuration

Update `bench_config.yaml` for persistent settings:

```yaml
spark:
  workers: 8
  driver_memory: "4g"
  executor_memory: "4g"
  additional_config:
    spark.sql.adaptive.enabled: true

benchmark:
  num_runs: 10
  warmup_runs: 2

queries:
  directory: "/dev-root/sedona_fer/bench/queries"

output:
  directory: "results"
  generate_plots: true
```

## Workflow Examples

### Example 1: Quick Validation Run
```bash
# Fast 5-run validation with fewer workers
python run.py --workers 4 --num-runs 5
```

### Example 2: Thorough Profiling
```bash
# Detailed 50-run profiling with memory and query plan analysis
python run.py --num-runs 50 --warmup-runs 3 --memory-stats --query-plans
```

### Example 3: Specific Query Testing
```bash
# Test only spatial and join-heavy queries
python run.py --queries query_11_buffer_analysis query_12_spatial_filter query_15_self_join_nearby --num-runs 20
```

### Example 4: Baseline Comparison
```bash
# Run and compare with previous baseline
python run.py --queries query_01 query_02 query_03 > current_run.json
python analyze.py current_run.json --compare baseline.json
```

### Example 5: Resource Optimization Testing
```bash
# Test with different Spark configurations
python run.py --workers 8 --driver-memory 4g --executor-memory 4g
python run.py --workers 16 --driver-memory 8g --executor-memory 8g

# Compare the two runs
python analyze.py results/run1.json --compare results/run2.json
```

## Performance Metrics Explained

| Metric | Description | Use Case |
|--------|-------------|----------|
| **Mean** | Average execution time | Overall performance |
| **Median** | Middle value (50th percentile) | Typical performance (robust to outliers) |
| **Stdev** | Standard deviation | Consistency/reliability |
| **CV (Coefficient of Variation)** | Stdev / Mean | Relative consistency (unitless) |
| **p95, p99** | 95th, 99th percentiles | Worst-case scenarios |
| **Range** | Max - Min | Variability bounds |

### Interpreting Stability:
- **CV < 0.10**: Excellent (very consistent)
- **CV < 0.20**: Good (consistent)
- **CV > 0.20**: Poor (high variability - check for competing workloads)

## Tips for Best Results

1. **Close other applications** - Reduces system noise
2. **Run warmup iterations** - Caches data and compiles code
3. **Use consistent Spark configuration** - For reproducibility
4. **Run multiple iterations** (10+) - Statistics improve with sample size
5. **Check CV values** - High CV suggests environmental factors
6. **Compare across multiple runs** - Use the comparison feature

## Architecture

```
run.py                    # Main benchmark runner
├── SedonaBenchmark       # Core benchmarking logic
├── QueryStats            # Statistical calculations
└── generate_plots()      # Enhanced visualization

analyze.py                # Result analysis CLI
└── BenchmarkAnalysis     # Analysis utilities
    ├── generate_report()     # Text report generation
    ├── export_to_csv()       # CSV export
    └── compare_with()        # Baseline comparison

bench_config.yaml         # Configuration file
```

## Troubleshooting

**High CV values (>0.30)?**
- System is under other load
- Run benchmark during quiet hours
- Increase warmup runs

**Memory stats unavailable?**
- Ensure `psutil` is installed: `pip install psutil`

**Query plans too large?**
- Use `--query-plans` selectively for a few queries
- Store only for complex queries (buffer, join operations)
