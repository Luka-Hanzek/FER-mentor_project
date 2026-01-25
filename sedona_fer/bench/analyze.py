#!/usr/bin/env python3
"""CLI tool for analyzing benchmark results"""

import argparse
from pathlib import Path
from sedona_fer.bench.analysis import BenchmarkAnalysis


def main():
    parser = argparse.ArgumentParser(
        description="Analyze Sedona Benchmark Results",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Print analysis report
  python analyze.py results/benchmark_results_20260117_095331.json
  
  # Export to CSV
  python analyze.py results/benchmark_results_20260117_095331.json --csv summary.csv
  
  # Compare two benchmark runs
  python analyze.py results/benchmark_results_run1.json --compare results/benchmark_results_run2.json
  
  # Save report to file
  python analyze.py results/benchmark_results_20260117_095331.json --output report.txt
        """
    )
    
    parser.add_argument(
        'result_file',
        help='Path to benchmark result JSON file'
    )
    
    parser.add_argument(
        '--output',
        help='Save analysis report to file'
    )
    
    parser.add_argument(
        '--csv',
        help='Export results to CSV file'
    )
    
    parser.add_argument(
        '--compare',
        help='Compare with another benchmark result file'
    )
    
    parser.add_argument(
        '--summary',
        action='store_true',
        help='Print summary statistics only'
    )
    
    args = parser.parse_args()
    
    # Verify file exists
    if not Path(args.result_file).exists():
        print(f"Error: Result file not found: {args.result_file}")
        return 1
    
    analysis = BenchmarkAnalysis(args.result_file)
    
    # Generate and print report
    if not args.summary:
        report = analysis.generate_report(args.output)
        print(report)
    else:
        summary = analysis.get_summary_stats()
        print("Summary Statistics:")
        for key, value in summary.items():
            print(f"  {key}: {value}")
    
    # Export to CSV if requested
    if args.csv:
        analysis.export_to_csv(args.csv)
    
    # Compare with another result if requested
    if args.compare:
        if not Path(args.compare).exists():
            print(f"Error: Comparison file not found: {args.compare}")
            return 1
        
        print("\n" + "=" * 80)
        print("COMPARISON WITH BASELINE")
        print("=" * 80)
        
        comparison = analysis.compare_with(args.compare)
        
        print(f"{'Query':<30} {'Current':<12} {'Baseline':<12} {'Diff':<12} {'% Change':<12}")
        print("-" * 80)
        
        for comp in comparison:
            change_color = '📈' if comp['percent_change'] > 0 else '📉'
            print(f"{comp['query']:<30} {comp['current_mean']:<12.4f} "
                  f"{comp['other_mean']:<12.4f} {comp['difference']:<12.4f} "
                  f"{comp['percent_change']:>10.2f}% {change_color}")
    
    return 0


if __name__ == "__main__":
    exit(main())
