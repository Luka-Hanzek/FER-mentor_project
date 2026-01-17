"""Benchmark result analysis and reporting utilities"""

import json
import statistics
from pathlib import Path
from typing import Dict, List, Any
from datetime import datetime
import pandas as pd


class BenchmarkAnalysis:
    """Analyze and compare benchmark results"""
    
    def __init__(self, result_file: str):
        """Load benchmark results from JSON file"""
        with open(result_file, 'r') as f:
            self.data = json.load(f)
        self.results = self.data['results']
    
    def generate_report(self, output_file: str = None) -> str:
        """Generate a comprehensive text report of benchmark results"""
        
        lines = []
        lines.append("=" * 80)
        lines.append("SEDONA BENCHMARK ANALYSIS REPORT")
        lines.append("=" * 80)
        lines.append("")
        
        # Configuration summary
        lines.append("CONFIGURATION")
        lines.append("-" * 80)
        config = self.data['config']
        lines.append(f"Spark Workers:        {config['spark']['workers']}")
        lines.append(f"Driver Memory:        {config['spark']['driver_memory']}")
        lines.append(f"Executor Memory:      {config['spark']['executor_memory']}")
        lines.append(f"Dataset:              {config['dataset']['path']}")
        lines.append(f"Benchmark Runs:       {config['benchmark']['num_runs']}")
        lines.append(f"Warmup Runs:          {config['benchmark']['warmup_runs']}")
        lines.append("")
        
        # Query results summary
        lines.append("QUERY RESULTS SUMMARY")
        lines.append("-" * 80)
        lines.append(f"{'Query':<30} {'Mean':<12} {'Median':<12} {'Stdev':<12} {'P95':<12}")
        lines.append("-" * 80)
        
        for result in self.results:
            stats = result.get('statistics', {})
            mean = stats.get('mean', 0)
            median = stats.get('median', 0)
            stdev = stats.get('stdev', 0)
            p95 = stats.get('p95', 0)
            
            lines.append(f"{result['query_name']:<30} {mean:<12.4f} {median:<12.4f} {stdev:<12.4f} {p95:<12.4f}")
        
        lines.append("")
        
        # Detailed statistics per query
        lines.append("DETAILED STATISTICS")
        lines.append("-" * 80)
        
        for result in self.results:
            lines.append(f"\n{result['query_name']}")
            lines.append("  " + "-" * 76)
            
            stats = result.get('statistics', {})
            if stats:
                lines.append(f"  Executions:       {stats.get('count', 0)}")
                lines.append(f"  Mean:             {stats.get('mean', 0):.6f}s")
                lines.append(f"  Median:           {stats.get('median', 0):.6f}s")
                lines.append(f"  Std Dev:          {stats.get('stdev', 0):.6f}s")
                lines.append(f"  Variance:         {stats.get('variance', 0):.6f}s²")
                lines.append(f"  Min:              {stats.get('min', 0):.6f}s")
                lines.append(f"  Max:              {stats.get('max', 0):.6f}s")
                lines.append(f"  Range:            {stats.get('range', 0):.6f}s")
                lines.append(f"  Coefficient of Variation: {stats.get('coefficient_of_variation', 0):.4f}")
                lines.append(f"  Percentiles:")
                lines.append(f"    25th:           {stats.get('p25', 0):.6f}s")
                lines.append(f"    50th (median):  {stats.get('p50', 0):.6f}s")
                lines.append(f"    75th:           {stats.get('p75', 0):.6f}s")
                lines.append(f"    95th:           {stats.get('p95', 0):.6f}s")
                lines.append(f"    99th:           {stats.get('p99', 0):.6f}s")
        
        lines.append("")
        lines.append("=" * 80)
        
        report = "\n".join(lines)
        
        if output_file:
            with open(output_file, 'w') as f:
                f.write(report)
        
        return report
    
    def get_summary_stats(self) -> Dict[str, Any]:
        """Get aggregate statistics across all queries"""
        all_times = []
        
        for result in self.results:
            all_times.extend(result['execution_times'])
        
        if not all_times:
            return {}
        
        return {
            'total_queries': len(self.results),
            'total_executions': len(all_times),
            'aggregate_mean': statistics.mean(all_times),
            'aggregate_median': statistics.median(all_times),
            'aggregate_min': min(all_times),
            'aggregate_max': max(all_times),
            'fastest_query': min(self.results, key=lambda r: r['statistics']['mean'])['query_name'],
            'slowest_query': max(self.results, key=lambda r: r['statistics']['mean'])['query_name'],
        }
    
    def export_to_csv(self, output_file: str) -> None:
        """Export results to CSV format for external analysis"""
        rows = []
        
        for result in self.results:
            stats = result.get('statistics', {})
            rows.append({
                'query_name': result['query_name'],
                'count': stats.get('count', 0),
                'mean_s': stats.get('mean', 0),
                'median_s': stats.get('median', 0),
                'stdev_s': stats.get('stdev', 0),
                'min_s': stats.get('min', 0),
                'max_s': stats.get('max', 0),
                'range_s': stats.get('range', 0),
                'p25_s': stats.get('p25', 0),
                'p50_s': stats.get('p50', 0),
                'p75_s': stats.get('p75', 0),
                'p95_s': stats.get('p95', 0),
                'p99_s': stats.get('p99', 0),
                'cv': stats.get('coefficient_of_variation', 0),
            })
        
        df = pd.DataFrame(rows)
        df.to_csv(output_file, index=False)
        print(f"Results exported to: {output_file}")
    
    def compare_with(self, other_result_file: str) -> Dict[str, Any]:
        """Compare current results with another benchmark run"""
        with open(other_result_file, 'r') as f:
            other_data = json.load(f)
        
        comparison = []
        
        for result in self.results:
            query_name = result['query_name']
            current_mean = result['statistics']['mean']
            
            # Find matching query in other results
            other_result = next(
                (r for r in other_data['results'] if r['query_name'] == query_name),
                None
            )
            
            if other_result:
                other_mean = other_result['statistics']['mean']
                diff = current_mean - other_mean
                percent_change = (diff / other_mean * 100) if other_mean > 0 else 0
                
                comparison.append({
                    'query': query_name,
                    'current_mean': current_mean,
                    'other_mean': other_mean,
                    'difference': diff,
                    'percent_change': percent_change,
                    'faster_or_slower': 'Faster' if diff < 0 else 'Slower'
                })
        
        return comparison
