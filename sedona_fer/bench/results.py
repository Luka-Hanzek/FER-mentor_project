from .models import BenchmarkResult
from pathlib import Path
import json
import pandas as pd
import io
from matplotlib import pyplot as plt


def write_config(benchmark_result: BenchmarkResult, output_directory: str):
    output_dir = Path(output_directory) / f"benchmark_results_{benchmark_result.timestamp}"
    output_dir.mkdir(exist_ok=True, parents=True)

    config_file = output_dir / "config.json"
    with open(config_file, 'w') as f:
        json.dump(benchmark_result.config, f, indent=2)


def write_timing_results(
    benchmark_result: BenchmarkResult,
    output_directory: str,
):
    output_dir = Path(output_directory)
    output_dir.mkdir(exist_ok=True, parents=True)

    results_directory = output_dir / f"benchmark_results_{benchmark_result.timestamp}"
    results_directory.mkdir(exist_ok=True)

    timing_file = results_directory / "timing.json"

    # Generate timing file
    with open(timing_file, 'w') as f:
        json.dump(
            [
                {
                    'query': benchmark_query_result.query.query_name,
                    'timing': benchmark_query_result.timing.to_dict(),
                }
                for benchmark_query_result in benchmark_result.query_results
            ],
            f,
            indent=2,
        )

def generate_query_outputs(
    benchmark_result: BenchmarkResult,
    output_directory: str
):
    output_dir = Path(output_directory)
    output_dir.mkdir(exist_ok=True, parents=True)

    results_directory = output_dir / f"benchmark_results_{benchmark_result.timestamp}"
    results_directory.mkdir(exist_ok=True)

    query_output_file = results_directory / "query_outputs.html"


    # Generate HTML with query outputs
    html_parts = ["<html><body>"]
    for result in benchmark_result.query_results:
        query_name = result.query.query_name
        query_output = result.query_output.output

        df = pd.read_json(io.StringIO(query_output))
        table_html = df.to_html(index=False)

        html_parts.append(f"<p>Query file: {query_name}</p>")
        html_parts.append(table_html)

    html_parts.append("</body></html>")

    # Join all parts into one HTML string
    html_content = "\n".join(html_parts)
    with open(query_output_file, 'w') as f:
        f.write(html_content)


def generate_plots(
    benchmark_result: BenchmarkResult,
    output_directory: str,
):
    output_dir = Path(output_directory) / f"benchmark_results_{benchmark_result.timestamp}"

    # Create DataFrame for easier plotting
    data = []
    for result in benchmark_result.query_results:
        for i, exec_time in enumerate(result.timing.execution_times):
            data.append({
                'Query': result.query.query_name,
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
    plot_file = output_dir / f"benchmark_plot_{benchmark_result.timestamp}.png"
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
    detail_plot_file = output_dir / f"benchmark_detail_{benchmark_result.timestamp}.png"
    plt.savefig(detail_plot_file, dpi=300, bbox_inches='tight')
    print(f"Detailed plot saved to: {detail_plot_file}")
