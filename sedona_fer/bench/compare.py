#!/usr/bin/env python3
"""
Benchmark comparison utility for tracking performance over time.
Useful for regression testing and performance trend analysis.
"""

import json
import argparse
from pathlib import Path
from typing import Dict, List, Any
from datetime import datetime
import matplotlib.pyplot as plt
import pandas as pd


class BenchmarkComparator:
    """Compare multiple benchmark runs"""
    
    def __init__(self, result_files: List[str]):
        """Load multiple benchmark results"""
        self.results = []
        self.labels = []
        
        for result_file in result_files:
            with open(result_file, 'r') as f:
                data = json.load(f)
            self.results.append(data)
            self.labels.append(Path(result_file).stem)
    
    def compare_queries(self, query_name: str) -> Dict[str, Any]:
        """Compare a specific query across all runs"""
        comparison = {
            'query': query_name,
            'runs': []
        }
        
        for i, result in enumerate(self.results):
            query_result = next(
                (r for r in result['results'] if r['query_name'] == query_name),
                None
            )
            
            if query_result:
                stats = query_result.get('statistics', {})
                comparison['runs'].append({
                    'run': self.labels[i],
                    'mean': stats.get('mean', 0),
                    'median': stats.get('median', 0),
                    'stdev': stats.get('stdev', 0),
                    'p95': stats.get('p95', 0),
                    'cv': stats.get('coefficient_of_variation', 0),
                })
        
        return comparison
    
    def generate_trend_report(self) -> str:
        """Generate a trend analysis report"""
        lines = []
        lines.append("=" * 100)
        lines.append("BENCHMARK TREND ANALYSIS")
        lines.append("=" * 100)
        lines.append("")
        
        # Get all unique queries
        all_queries = set()
        for result in self.results:
            for query_result in result['results']:
                all_queries.add(query_result['query_name'])
        
        all_queries = sorted(all_queries)
        
        for query in all_queries:
            comparison = self.compare_queries(query)
            lines.append(f"\n{query}")
            lines.append("-" * 100)
            
            if comparison['runs']:
                lines.append(f"{'Run':<30} {'Mean (s)':<15} {'Median (s)':<15} {'Stdev':<15} {'P95 (s)':<15} {'CV':<10}")
                lines.append("-" * 100)
                
                means = []
                for run in comparison['runs']:
                    lines.append(f"{run['run']:<30} {run['mean']:<15.6f} {run['median']:<15.6f} "
                               f"{run['stdev']:<15.6f} {run['p95']:<15.6f} {run['cv']:<10.4f}")
                    means.append(run['mean'])
                
                if len(means) > 1:
                    # Calculate trend
                    improvement = ((means[0] - means[-1]) / means[0] * 100)
                    trend = "📈 Slower" if improvement < 0 else "📉 Faster"
                    lines.append(f"\nTrend: {trend} ({abs(improvement):.2f}%)")
                lines.append("")
        
        return "\n".join(lines)
    
    def generate_trend_plots(self, output_dir: str = ".") -> None:
        """Generate trend visualization plots"""
        output_dir = Path(output_dir)
        output_dir.mkdir(exist_ok=True)
        
        # Get all unique queries
        all_queries = set()
        for result in self.results:
            for query_result in result['results']:
                all_queries.add(query_result['query_name'])
        
        all_queries = sorted(all_queries)
        
        # Plot 1: Mean execution time trends
        fig, axes = plt.subplots(len(all_queries) // 4 + 1, 4, figsize=(20, 12))
        axes = axes.flatten()
        
        for idx, query in enumerate(all_queries):
            comparison = self.compare_queries(query)
            
            if comparison['runs']:
                runs = [r['run'] for r in comparison['runs']]
                means = [r['mean'] for r in comparison['runs']]
                stdevs = [r['stdev'] for r in comparison['runs']]
                
                axes[idx].errorbar(range(len(runs)), means, yerr=stdevs, marker='o', capsize=5)
                axes[idx].set_title(query, fontsize=10)
                axes[idx].set_xticks(range(len(runs)))
                axes[idx].set_xticklabels(runs, rotation=45, ha='right', fontsize=8)
                axes[idx].set_ylabel('Time (s)')
                axes[idx].grid(True, alpha=0.3)
        
        # Hide unused subplots
        for idx in range(len(all_queries), len(axes)):
            axes[idx].set_visible(False)
        
        plt.tight_layout()
        plot_file = output_dir / f"trend_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
        plt.savefig(plot_file, dpi=300, bbox_inches='tight')
        print(f"Trend plot saved to: {plot_file}")
        plt.close()
        
        # Plot 2: Overall performance comparison
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
        
        # Aggregate means
        run_means = []
        for run_idx, result in enumerate(self.results):
            query_means = [
                r.get('statistics', {}).get('mean', 0)
                for r in result['results']
            ]
            run_means.append(sum(query_means) / len(query_means) if query_means else 0)
        
        runs = self.labels
        ax1.plot(range(len(runs)), run_means, marker='o', linewidth=2, markersize=8, color='steelblue')
        ax1.fill_between(range(len(runs)), run_means, alpha=0.3, color='steelblue')
        ax1.set_xticks(range(len(runs)))
        ax1.set_xticklabels(runs, rotation=45, ha='right')
        ax1.set_ylabel('Average Query Time (s)')
        ax1.set_title('Overall Performance Trend')
        ax1.grid(True, alpha=0.3)
        
        # Add improvement annotations
        for i in range(1, len(run_means)):
            improvement = ((run_means[i-1] - run_means[i]) / run_means[i-1] * 100)
            ax1.annotate(f'{abs(improvement):.1f}%', 
                        xy=(i-0.5, (run_means[i-1] + run_means[i])/2),
                        ha='center', fontsize=9,
                        color='green' if improvement > 0 else 'red')
        
        # Stability comparison (CV)
        run_cvs = []
        for result in self.results:
            cvs = [
                r.get('statistics', {}).get('coefficient_of_variation', 0)
                for r in result['results']
            ]
            run_cvs.append(sum(cvs) / len(cvs) if cvs else 0)
        
        ax2.bar(range(len(runs)), run_cvs, color=['green' if cv < 0.1 else 'orange' if cv < 0.2 else 'red' for cv in run_cvs])
        ax2.set_xticks(range(len(runs)))
        ax2.set_xticklabels(runs, rotation=45, ha='right')
        ax2.set_ylabel('Average Coefficient of Variation')
        ax2.set_title('Stability Trend')
        ax2.axhline(y=0.1, color='green', linestyle='--', alpha=0.5, label='Excellent')
        ax2.axhline(y=0.2, color='orange', linestyle='--', alpha=0.5, label='Good')
        ax2.legend()
        ax2.grid(True, alpha=0.3, axis='y')
        
        plt.tight_layout()
        trend_file = output_dir / f"overall_trend_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
        plt.savefig(trend_file, dpi=300, bbox_inches='tight')
        print(f"Overall trend plot saved to: {trend_file}")
        plt.close()


def main():
    parser = argparse.ArgumentParser(
        description="Compare and analyze benchmark result trends",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Compare two benchmark runs
  python compare.py run1.json run2.json
  
  # Compare multiple runs with trend analysis
  python compare.py baseline.json run1.json run2.json run3.json --report
  
  # Generate trend plots
  python compare.py baseline.json current.json --plots
  
  # Get detailed trend report
  python compare.py run1.json run2.json run3.json --report --output trends.txt
        """
    )
    
    parser.add_argument(
        'result_files',
        nargs='+',
        help='Benchmark result JSON files to compare (in chronological order)'
    )
    
    parser.add_argument(
        '--report',
        action='store_true',
        help='Print detailed trend analysis report'
    )
    
    parser.add_argument(
        '--plots',
        action='store_true',
        help='Generate trend visualization plots'
    )
    
    parser.add_argument(
        '--output',
        help='Save report to file'
    )
    
    parser.add_argument(
        '--plot-dir',
        default='trends',
        help='Directory to save plots (default: trends)'
    )
    
    args = parser.parse_args()
    
    # Verify files exist
    for result_file in args.result_files:
        if not Path(result_file).exists():
            print(f"Error: File not found: {result_file}")
            return 1
    
    comparator = BenchmarkComparator(args.result_files)
    
    # Print or save report
    if args.report or args.output:
        report = comparator.generate_trend_report()
        print(report)
        
        if args.output:
            with open(args.output, 'w') as f:
                f.write(report)
            print(f"\nReport saved to: {args.output}")
    
    # Generate plots
    if args.plots:
        comparator.generate_trend_plots(args.plot_dir)
    
    return 0


if __name__ == "__main__":
    exit(main())
