import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import os

## Config
# Script location
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# Directory where benchmark results are stored
BASE_DIR = os.path.join(SCRIPT_DIR, '..', 'measurements')
# Output directory for plots
OUT_DIR = os.path.join(SCRIPT_DIR, '..', 'plots')
# Experiments names (that are part of the subdirs in BASE_DIR)
EXPERIMENTS = ['linear-capacity', 'stress-latency', 'ideal-latency']

def discover_paths(base_dir):
    """
    Searches in the BASE_DIR for benchmark results.
    This is done by using the folder structure defined in the benchmark CLI: /measurements/<experiment-name>/<clusterSize>_<timestamp>/results.csv
    Also searches for 'disk_cpu_usage.xlsx' files for resource usage data (currently only used for linear-capacity exp.)
    :param base_dir: The base directory to search within.
    :return: A dictionary mapping experiment names to node counts and their corresponding result file paths.
    """
    detected_paths = {exp: {} for exp in EXPERIMENTS}
    detected_paths['resources'] = None
    
    base_dir = os.path.normpath(base_dir)

    if not os.path.exists(base_dir):
        print(f"Directory '{base_dir}' not found.")
        print(f"Current working directory is: {os.getcwd()}")
        return detected_paths

    for folder_name in EXPERIMENTS:
        search_path = os.path.join(base_dir, folder_name)
        
        if not os.path.exists(search_path):
            print(f"Folder '{folder_name}' not found in {base_dir}")
            continue

        # Find subdirectories like <clusterSize>_<timestamp>
        subdirs = [d for d in os.listdir(search_path) if os.path.isdir(os.path.join(search_path, d))]
        
        for subdir in subdirs:
            try:
                # Extract Node Count (e.g. '3' from '3_2026...')
                node_count = int(subdir.split('_')[0])
                
                full_dir = os.path.join(search_path, subdir)
                result_file = os.path.join(full_dir, 'results.csv')

                # Add the found path if a results.csv exists within
                if os.path.exists(result_file):
                    detected_paths[folder_name][node_count] = result_file
                    print(f"  [{folder_name}] Found {node_count}-node run.")
                    
                    # Search for native Excel file for resource usage metrics
                    resource_file = os.path.join(full_dir, 'disk_cpu_usage.xlsx')
                    
                    # We only want one resource usage file => prefer the largest cluster size
                    if folder_name == 'linear-capacity' and os.path.exists(resource_file):
                        if detected_paths['resources'] is None or node_count >= 9:
                             detected_paths['resources'] = resource_file

            except ValueError:
                continue

    if detected_paths['resources']:
        print(f"  [linear-capacity] Found Azure resource metrics file: {os.path.basename(detected_paths['resources'])}")
    else:
        print("  [linear-capacity] No 'disk_cpu_usage.xlsx' found.")

    return detected_paths

def get_summary_metrics(paths):
    """
    Extracts summary metrics from the result CSV files.
    :param paths: A dictionary (from discover_paths()) mapping experiment names to node counts and their corresponding result file paths.
    :return: Three dictionaries mapping node counts to mean throughput, stress p99 latency, and ideal p99 latency.
    """
    throughput_means = {}
    stress_p99_means = {}
    ideal_p99_means = {}

    ## Read only relevant metrics from each experiment
    # Linear Capacity => interval_throughput
    for nodes, path in paths['linear-capacity'].items():
        try:
            df = pd.read_csv(path)
            valid = df[df['interval_throughput'] > 1000]['interval_throughput']
            throughput_means[nodes] = valid.mean() if not valid.empty else 0
        except Exception:
            throughput_means[nodes] = 0

    # Stress Latency => interval_latency_p99_us
    for nodes, path in paths['stress-latency'].items():
        try:
            df = pd.read_csv(path)
            valid = df[df['interval_latency_p99_us'] > 0]['interval_latency_p99_us']
            stress_p99_means[nodes] = (valid.mean() / 1000.0) if not valid.empty else 0
        except Exception:
            stress_p99_means[nodes] = 0

    # 3. Ideal Latency = interval_latency_p99_us
    for nodes, path in paths['ideal-latency'].items():
        try:
            df = pd.read_csv(path)
            valid = df[df['interval_latency_p99_us'] > 0]['interval_latency_p99_us']
            ideal_p99_means[nodes] = (valid.mean() / 1000.0) if not valid.empty else 0
        except Exception:
            ideal_p99_means[nodes] = 0

    return throughput_means, stress_p99_means, ideal_p99_means

def load_azure_resource_data(path):
    if not path or not os.path.exists(path): return None
    try:
        # Load the exported Azure metrics file via openpyxl.
        # Relevant metrics headers start at line 11
        df = pd.read_excel(path, header=10, engine='openpyxl')
        
        # Rename the first column to 'Timestamp' and convert to datetime
        df.rename(columns={df.columns[0]: 'Timestamp'}, inplace=True)
        df['Timestamp'] = pd.to_datetime(df['Timestamp'])
        
        # We expect at least 19 columns here => Timestamp + 9 CPU + 9 Disk
        if len(df.columns) < 19: return None
        
        # Columns 1-9: CPU %
        # Columns 10-18: Disk Write Bytes/sec
        cpu_cols = df.columns[1:10]
        disk_cols = df.columns[10:19]

        # Convert all relevant columns, since they are currently strings
        for col in list(cpu_cols) + list(disk_cols):
            df[col] = pd.to_numeric(df[col], errors='coerce')

        # Convert to cluster-wide metrics that we want to plot
        df['Cluster_Avg_CPU'] = df[cpu_cols].mean(axis=1)
        df['Cluster_Total_Disk_MBs'] = df[disk_cols].sum(axis=1) / (1024 * 1024)
        return df
    except Exception as e:
        print(f"Error loading Excel file: {e}")
        return None

def plot_all():
    """
    Main function to generate all plots and save them to the output directory.
    """

    # Get all paths to the benchmark result files
    paths = discover_paths(BASE_DIR)

    # Calculate the metrics that we want to plot
    tp_means, stress_means, ideal_means = get_summary_metrics(paths)
    
    cluster_sizes = sorted(tp_means.keys())
    if not cluster_sizes:
        print("No valid data found, which may be caused due to wrong paths (BASE_DIR)")
        return

    labels = [f"{n} Nodes" for n in cluster_sizes]
    
    # Plot 1: Throughput
    # Bar chart of throughput per cluster size
    plt.figure(figsize=(8, 6))
    values = [tp_means[n] for n in cluster_sizes]
    bars = plt.bar(labels, values, color='#1f77b4', edgecolor='black', alpha=0.8)
    plt.title('Throughput Scalability', fontsize=14)
    plt.ylabel('Throughput (msgs/sec)', fontsize=12)
    plt.grid(axis='y', linestyle='--', alpha=0.5)
    for bar in bars:
        plt.text(bar.get_x() + bar.get_width()/2., bar.get_height(), 
                 f'{int(bar.get_height()):,}', ha='center', va='bottom')
    plt.tight_layout()
    plt.savefig(os.path.join(OUT_DIR, f'1_throughput_scalability.svg'), format='svg')

    # Plot 2: Latency Gradient
    # Bar chart comparing ideal vs stress p99 latencies for each cluster size
    if stress_means and ideal_means:
        plt.figure(figsize=(8, 6))
        x = np.arange(len(cluster_sizes))
        width = 0.35
        ideal_vals = [ideal_means.get(n, 0) for n in cluster_sizes]
        stress_vals = [stress_means.get(n, 0) for n in cluster_sizes]
        
        plt.bar(x - width/2, ideal_vals, width, label='Ideal', color='#2ca02c', edgecolor='black')
        plt.bar(x + width/2, stress_vals, width, label='Stress', color='#d62728', edgecolor='black')
        plt.xticks(x, labels)
        plt.title('Latency Gradient: Ideal vs. Stress', fontsize=14)
        plt.ylabel('P99 Latency (ms)', fontsize=12)
        plt.yscale('log')
        plt.legend()
        plt.grid(axis='y', linestyle='--', alpha=0.5)
        plt.tight_layout()
        plt.savefig(os.path.join(OUT_DIR, f'2_latency_gradient.svg'), format='svg')

    # Plot 3: Consensus Tax
    # Line plot of ideal p99 latency per cluster size
    if ideal_means:
        plt.figure(figsize=(8, 6))
        vals = [ideal_means.get(n, 0) for n in cluster_sizes]
        plt.plot(labels, vals, marker='o', linewidth=2, color='#9467bd')
        plt.title('Consensus Coordination Tax', fontsize=14)
        plt.ylabel('Ideal P99 Latency (ms)', fontsize=12)
        plt.grid(True, linestyle='--', alpha=0.5)
        for i, v in enumerate(vals):
            plt.text(i, v, f"{v:.2f} ms", ha='center', va='bottom')
        plt.tight_layout()
        plt.savefig(os.path.join(OUT_DIR, f'3_consensus_tax.svg'), format='svg')

    # Plot 4: Resource Saturation
    # Line plot of CPU and Disk usage over time from the exported Azure resource metrics (9-node cluster run)
    df_res = load_azure_resource_data(paths['resources'])
    if df_res is not None:
        _, ax1 = plt.subplots(figsize=(12, 7))
        ax1.set_xlabel('Time')
        ax1.set_ylabel('Avg Cluster CPU (%)', color='#1f77b4')
        ax1.plot(df_res['Timestamp'], df_res['Cluster_Avg_CPU'], color='#1f77b4', label='CPU')
        ax1.tick_params(axis='y', labelcolor='#1f77b4')
        ax1.set_ylim(0, 100)
        
        ax2 = ax1.twinx()
        ax2.set_ylabel('Total Disk Write (MB/s)', color='#d62728')
        ax2.plot(df_res['Timestamp'], df_res['Cluster_Total_Disk_MBs'], color='#d62728', label='Disk')
        ax2.tick_params(axis='y', labelcolor='#d62728')
        
        ax1.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
        plt.title('Resource Saturation', fontsize=16)
        plt.tight_layout()
        plt.savefig(os.path.join(OUT_DIR, f'4_resource_saturation.svg'), format='svg')

    # Plot 5: Tail Latency
    # Line plot of P50 and P99 latency over time for the largest cluster size in stress-latency experiment
    # TODO: Do we want to plot more than just the largest cluster size here?
    if paths['stress-latency']:
        max_node = max(paths['stress-latency'].keys())
        try:
            df = pd.read_csv(paths['stress-latency'][max_node])
            plt.figure(figsize=(12, 6))
            plt.plot(df['elapsed_seconds'], df['interval_latency_p99_us']/1000, label='P99', color='#d62728')
            plt.plot(df['elapsed_seconds'], df['interval_latency_p50_us']/1000, label='P50', color='#2ca02c')
            plt.title(f'Tail Latency ({max_node} Nodes)', fontsize=14)
            plt.ylabel('Latency (ms)')
            plt.legend()
            plt.grid(True, linestyle='--', alpha=0.7)
            plt.tight_layout()
            plt.savefig(os.path.join(OUT_DIR, f'5_latency_distribution.svg'), format='svg')
        except Exception:
            pass

    print(f"Plots saved to {OUT_DIR}")

if __name__ == "__main__":
    plot_all()