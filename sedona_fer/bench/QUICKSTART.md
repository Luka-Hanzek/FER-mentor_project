# Quick Start Guide - Enhanced Benchmarks

## 🚀 Running Your First Enhanced Benchmark

### Step 1: Basic Run (Default Settings)
```bash
cd sedona_fer/bench
python run.py
```

This will:
- Execute all 15 queries, 10 runs each
- Show progress with mean, median, stdev, and p95
- Generate beautiful analysis plots
- Save results in `results/` folder

### Step 2: View Results
```bash
# Print analysis report
python analyze.py results/benchmark_results_*.json

# Or export as CSV
python analyze.py results/benchmark_results_*.json --csv results.csv
```

## 📊 Common Use Cases

### Run a Quick Test (5 queries, 5 runs)
```bash
python run.py --queries query_01_count_roads query_02_road_length query_03_roads_by_highway_type query_11_buffer_analysis query_15_self_join_nearby --num-runs 5 --warmup-runs 1
```

### Test Spatial Queries Only
```bash
python run.py --queries query_11_buffer_analysis query_12_spatial_filter query_15_self_join_nearby --num-runs 15
```

### Detailed Analysis with Memory Profiling
```bash
python run.py --num-runs 20 --memory-stats --query-plans
```

### Performance Regression Testing
```bash
# Run baseline
python run.py --num-runs 30 > baseline.json

# Later, compare new run
python run.py --num-runs 30 > current.json
python analyze.py current.json --compare baseline.json
```

## 📈 Understanding the Output

### Console Output
```
Mean: 0.1529s | Median: 0.1530s | Stdev: 0.0212s | P95: 0.1790s
```
- **Mean**: Average time (use this for comparisons)
- **Median**: Middle value (robust to outliers)
- **Stdev**: Consistency (lower = more stable)
- **P95**: 95% of runs were faster than this

### Generated Plots

1. **benchmark_analysis_*.png** - Four-panel dashboard showing:
   - Average performance with error bars
   - Distribution (box and violin plots)
   - Stability across runs

2. **benchmark_statistics_*.png** - Deep statistical analysis:
   - Percentile breakdown (p50, p75, p95, p99)
   - Stability index (CV - Coefficient of Variation)
   - Min-max ranges

### Analysis Report
```
DETAILED STATISTICS
query_01_count_roads
  Executions:       10
  Mean:             0.152948s
  Median:           0.152968s
  Std Dev:          0.021230s
  Coefficient of Variation: 0.1387
  Percentiles:
    95th:           0.179005s
    99th:           0.186209s
```

## 🎯 Advanced Options

### Override Configuration at Runtime
```bash
# Run with double workers, higher memory, more iterations
python run.py --workers 16 --driver-memory 8g --executor-memory 8g --num-runs 50
```

### Test Query Optimization
```bash
# Before optimization
python run.py --num-runs 30 > before.json

# After optimization
python run.py --num-runs 30 > after.json

# Compare
python analyze.py after.json --compare before.json
```

### Stability Test (Check for environmental noise)
```bash
python run.py --num-runs 50 --warmup-runs 5
```

Then look at the CV value:
- CV < 0.10 = Excellent (repeatable)
- CV < 0.20 = Good (reliable)
- CV > 0.30 = High variance (noisy environment)

## 📝 Example Workflow

```bash
# 1. Establish baseline with multiple runs
python run.py --num-runs 30 --warmup-runs 5 --skip-plots

# 2. Make changes to queries or configuration

# 3. Run new benchmark
python run.py --num-runs 30 --warmup-runs 5

# 4. Compare and analyze
python analyze.py results/benchmark_results_NEW.json --compare results/benchmark_results_BASELINE.json

# 5. Export detailed results
python analyze.py results/benchmark_results_NEW.json --csv analysis.csv
```

## 🔍 Interpreting Stability

**Coefficient of Variation (CV) = Stdev / Mean**

| CV Range | Interpretation | Action |
|----------|---|---|
| < 0.05 | Excellent - Very stable | Production ready |
| 0.05-0.10 | Good - Stable | OK for benchmarking |
| 0.10-0.20 | Acceptable - Minor variance | May need more runs |
| 0.20-0.30 | Poor - Noticeable variance | Run during quiet hours |
| > 0.30 | Very poor - Unstable | Check for competing loads |

## 💡 Tips for Reliable Benchmarks

1. **Warm up first**: Allow Spark to cache data and compile code
   ```bash
   python run.py --warmup-runs 5
   ```

2. **Run multiple times**: More runs = more reliable statistics
   ```bash
   python run.py --num-runs 50
   ```

3. **Close other applications**: Reduces system noise

4. **Run during quiet hours**: Fewer competing processes

5. **Check CV values**: If > 0.2, investigate system load

6. **Use the same config**: For reproducible comparisons
   ```bash
   python run.py --config bench_config.yaml
   ```

## 📦 Troubleshooting

**"psutil not found"**
```bash
pip install psutil
```

**High CV values (unstable results)?**
- Try running with fewer workers: `--workers 4`
- Increase warmup runs: `--warmup-runs 10`
- Run benchmark at a different time

**Memory stats not appearing?**
- Ensure you're using `--memory-stats` flag
- Check that psutil is installed

**Plots not generating?**
- Run with `--skip-plots false` explicitly
- Check that matplotlib is installed: `pip install matplotlib`

## 🎓 Learning Resources

See `ENHANCEMENTS.md` for:
- Full documentation of all CLI options
- Performance metrics explained
- Architecture overview
- Advanced configuration

---

Ready to benchmark? Start with:
```bash
cd sedona_fer/bench
python run.py --num-runs 10
python analyze.py results/benchmark_results_*.json
```
