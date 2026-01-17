# Development & Extension Guide

## Architecture Overview

```
Benchmark Framework
├── Core Components
│   ├── SedonaBenchmark (run.py)
│   │   ├── Config management
│   │   ├── Spark initialization
│   │   ├── Query execution
│   │   └── Result storage
│   └── QueryStats (run.py)
│       └── Statistical calculations
│
├── Analysis Tools
│   ├── BenchmarkAnalysis (analysis.py)
│   │   ├── Report generation
│   │   ├── CSV export
│   │   └── Comparison logic
│   ├── BenchmarkComparator (compare.py)
│   │   ├── Trend analysis
│   │   └── Visualization
│   └── CLI Tools
│       ├── analyze.py
│       └── compare.py
│
└── Configuration
    └── bench_config.yaml
```

## Adding New Metrics

### Add to QueryStats.calculate()

```python
def calculate(self) -> Dict[str, float]:
    # Existing code...
    
    # Add new metric
    return {
        # ... existing metrics ...
        'your_new_metric': self._your_new_metric(),
    }

def _your_new_metric(self) -> float:
    """Calculate custom metric"""
    # Implementation
    return result
```

Example: Adding geometric mean
```python
import math

def _geometric_mean(self) -> float:
    """Calculate geometric mean"""
    if not self.times:
        return 0
    product = 1
    for t in self.times:
        product *= t
    return product ** (1 / len(self.times))
```

## Adding New CLI Options

### In main() function in run.py

```python
parser.add_argument(
    '--your-option',
    type=str,  # or int, float, etc.
    help='Description of your option'
)

# Handle in SedonaBenchmark initialization
benchmark = SedonaBenchmark(
    config_path=args.config,
    # ... existing args ...
    your_option=args.your_option,
)
```

## Adding New Visualizations

### Create function in SedonaBenchmark

```python
def generate_custom_plot(self):
    """Generate custom visualization"""
    output_dir = Path(self.config['output']['directory'])
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Create figure
    fig, ax = plt.subplots(figsize=(12, 6))
    
    # Your plotting code here
    
    # Save
    plot_file = output_dir / f"custom_plot_{timestamp}.png"
    plt.savefig(plot_file, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"Plot saved to: {plot_file}")
```

### Call from run_benchmarks()

```python
def run_benchmarks(self):
    # ... existing code ...
    self.generate_plots()
    self.generate_custom_plot()  # Add this
```

## Adding New Analysis Features

### Create new method in BenchmarkAnalysis

```python
def your_analysis(self) -> Dict[str, Any]:
    """Your analysis description"""
    results = []
    for result in self.results:
        # Your analysis logic
        results.append({
            'query': result['query_name'],
            'your_metric': value,
        })
    return results
```

### Expose via CLI in analyze.py

```python
parser.add_argument(
    '--your-analysis',
    action='store_true',
    help='Run your analysis'
)

# In main()
if args.your_analysis:
    analysis = BenchmarkAnalysis(args.result_file)
    results = analysis.your_analysis()
    # Display or save results
```

## Extending for Other Databases

### Create new benchmark class

```python
class PostGIsBenchmark(SedonaBenchmark):
    """PostGIS-specific benchmark implementation"""
    
    def _init_spark(self, spark_config: dict):
        # Override for PostGIS connection
        pass
    
    def _load_dataset(self, dataset_config: dict):
        # Override for PostGIS loading
        pass
    
    def _execute_query(self, query: str, num_runs: int, warmup_runs: int):
        # Override for PostGIS execution
        pass
```

## Integration Points

### Spark Session Access
```python
# In SedonaBenchmark methods
result_df = self.sedona.sql(query)
result_df.collect()
```

### Configuration Management
```python
# Access config anywhere in SedonaBenchmark
config = self.config['benchmark']['num_runs']
```

### Result Storage
```python
# Results stored in self.results list
self.results.append({
    'query_name': name,
    'statistics': stats,
    # ... more fields
})
```

## Testing

### Unit Test Template

```python
import unittest
from sedona_fer.bench.run import QueryStats

class TestQueryStats(unittest.TestCase):
    def test_calculate_basic(self):
        times = [1.0, 2.0, 3.0, 4.0, 5.0]
        stats = QueryStats(times)
        result = stats.calculate()
        
        self.assertEqual(result['mean'], 3.0)
        self.assertEqual(result['min'], 1.0)
        self.assertEqual(result['max'], 5.0)
    
    def test_empty_times(self):
        stats = QueryStats([])
        result = stats.calculate()
        self.assertEqual(result, {})

if __name__ == '__main__':
    unittest.main()
```

### Run tests
```bash
python -m pytest tests/
```

## Performance Optimization Ideas

1. **Parallel Query Execution**: Run multiple queries in parallel
2. **Incremental Results**: Stream results as they complete
3. **Smart Caching**: Cache dataset between benchmarks
4. **Adaptive Sampling**: Auto-adjust runs based on CV
5. **Distributed Analysis**: Offload to workers

## Monitoring & Logging

### Add debug logging

```python
import logging

logger = logging.getLogger(__name__)

# In your code
logger.debug(f"Executing query: {query}")
logger.info(f"Mean execution time: {stats['mean']}")
logger.warning(f"High CV detected: {stats['cv']}")
```

## Documentation Standards

### Docstring Format

```python
def your_function(param1: str, param2: int) -> Dict[str, Any]:
    """
    Brief description of what this function does.
    
    Longer description with more details if needed.
    
    Args:
        param1: Description of param1
        param2: Description of param2
    
    Returns:
        Dictionary with keys:
        - 'key1': Description
        - 'key2': Description
    
    Raises:
        ValueError: If something goes wrong
    
    Example:
        >>> result = your_function("test", 42)
        >>> print(result['key1'])
    """
    pass
```

## Common Patterns

### Iterating over results with metrics

```python
for result in self.results:
    stats = result['statistics']
    query_name = result['query_name']
    
    print(f"{query_name}:")
    print(f"  Mean: {stats['mean']:.4f}s")
    print(f"  CV: {stats['coefficient_of_variation']:.4f}")
```

### Filtering results

```python
# By query name
spatial_queries = [r for r in self.results if 'spatial' in r['query_name']]

# By performance
slow_queries = [r for r in self.results if r['statistics']['mean'] > 1.0]

# By stability
unstable_queries = [r for r in self.results if r['statistics']['coefficient_of_variation'] > 0.3]
```

### Creating comparison matrices

```python
import pandas as pd

data = []
for result in self.results:
    stats = result['statistics']
    data.append({
        'Query': result['query_name'],
        'Mean': stats['mean'],
        'Median': stats['median'],
        'P95': stats['p95'],
    })

df = pd.DataFrame(data)
print(df.to_string())
```

## Troubleshooting Development Issues

### Import errors
```python
# Ensure PYTHONPATH includes sedona_fer
import sys
sys.path.insert(0, '/home/njamic/mentor-projekt')
import sedona_fer
```

### Spark not initialized
```python
# Ensure _init_spark is called before using self.sedona
benchmark = SedonaBenchmark()
benchmark._init_spark(benchmark.config['spark'])
```

### Results not saved
```python
# Call save_results() explicitly
benchmark.save_results()
```

## Future Enhancement Ideas

1. **Adaptive benchmarking**: Auto-adjust runs based on CV
2. **Query plan optimization suggestions**: Analyze and suggest improvements
3. **Multi-database comparison**: Compare Sedona vs PostGIS vs DuckDB
4. **Real-time monitoring**: Dashboard for live benchmark progress
5. **ML-based anomaly detection**: Flag unusual performance patterns
6. **Automatic regression reporting**: Email reports on regressions
7. **Performance prediction**: Predict performance under different configs

---

Happy extending! 🚀
