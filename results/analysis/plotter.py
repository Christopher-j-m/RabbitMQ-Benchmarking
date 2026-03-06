import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.dates as mdates
import numpy as np
import json
import os

plt.rcParams.update({
    'font.size': 14,
    'axes.titlesize': 16,
    'axes.labelsize': 14,
    'xtick.labelsize': 12,
    'ytick.labelsize': 12,
    'legend.fontsize': 12,
})

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.join(SCRIPT_DIR, '..', 'measurements')
OUT_DIR = os.path.join(SCRIPT_DIR, '..', 'plots')
EXPERIMENTS = ['linear-capacity', 'stress-latency', 'ideal-latency']

COLORS = {
    'blue': '#1f77b4',
    'green': '#2ca02c',
    'red': '#d62728',
    'purple': '#9467bd',
    'orange': '#ff7f0e',
    'gray': '#7f7f7f',
}

CLUSTER_COLORS = {3: '#1f77b4', 6: '#ff7f0e', 9: '#2ca02c'}


def save_plot(fig, name, **kwargs):
    """Save a figure as PDF for LaTeX inclusion"""
    kwargs.setdefault('bbox_inches', 'tight')
    kwargs.setdefault('pad_inches', 0.03)
    fig.savefig(os.path.join(OUT_DIR, f'{name}.pdf'), format='pdf', **kwargs)


def discover_paths(base_dir):
    """
    Collect all results.csv paths grouped by experiment and node count.
    Each experiment/node_count maps to a list of CSV paths => one per run.
    Also selects the resource usage Excel file from the largest linear-capacity cluster.
    """
    detected_paths = {exp: {} for exp in EXPERIMENTS}
    detected_paths['resources'] = None

    base_dir = os.path.normpath(base_dir)
    if not os.path.exists(base_dir):
        print(f"Directory '{base_dir}' not found.")
        print(f"Current working directory is: {os.getcwd()}")
        return detected_paths

    for experiment_name in EXPERIMENTS:
        experiment_dir = os.path.join(base_dir, experiment_name)
        if not os.path.exists(experiment_dir):
            print(f"Folder '{experiment_name}' not found in {base_dir}")
            continue

        subdirs = sorted(
            d for d in os.listdir(experiment_dir)
            if os.path.isdir(os.path.join(experiment_dir, d))
        )

        for subdir in subdirs:
            try:
                node_count = int(subdir.split('_')[0])
            except ValueError:
                continue

            full_dir = os.path.join(experiment_dir, subdir)
            result_file = os.path.join(full_dir, 'results.csv')
            if not os.path.exists(result_file):
                continue

            detected_paths[experiment_name].setdefault(node_count, []).append(result_file)

            resource_file = os.path.join(full_dir, 'disk_cpu_usage.xlsx')
            if experiment_name == 'linear-capacity' and os.path.exists(resource_file):
                if detected_paths['resources'] is None or node_count >= 9:
                    detected_paths['resources'] = resource_file

        for node_count in sorted(detected_paths[experiment_name]):
            run_count = len(detected_paths[experiment_name][node_count])
            print(f"{experiment_name}: {node_count}-node: {run_count} run(s)")

    if detected_paths['resources']:
        print(f"Found Azure resource metrics: {os.path.basename(detected_paths['resources'])}")
    else:
        print("No Azure resource usage file found.")

    return detected_paths


def load_config(csv_path):
    """Load the correspondong JSON config file of a results CSV"""
    config_path = os.path.join(os.path.dirname(csv_path), 'results.config')
    if os.path.exists(config_path):
        with open(config_path) as f:
            return json.load(f)
    return {}


def get_summary_metrics(paths):
    """
    Compute per-cluster-size summary statistics with inter-run aggregation.
    For each experiment and cluster size, computes per-run summary values and then
    reports the mean-of-means and inter-run std dev
    => Mainly for debugging purposes
    """
    throughput = {}
    stress_latency = {}
    ideal_latency = {}

    for node_count, path_list in paths['linear-capacity'].items():
        run_means = []
        for path in path_list:
            try:
                df = pd.read_csv(path)
                valid_rows = df[df['interval_throughput'] > 1000]['interval_throughput']
                if not valid_rows.empty:
                    run_means.append(valid_rows.mean())
            except Exception:
                pass

        if run_means:
            throughput[node_count] = {
                'mean': np.mean(run_means),
                'std': np.std(run_means, ddof=1) if len(run_means) > 1 else 0.0,
                'count': len(run_means),
            }
        else:
            throughput[node_count] = {'mean': 0, 'std': 0, 'count': 0}

    for node_count, path_list in paths['stress-latency'].items():
        run_p99_values = []
        run_mean_values = []
        total_valid_rows = 0
        total_rows = 0
        for path in path_list:
            try:
                df = pd.read_csv(path)
                valid_rows = df[df['interval_latency_p99_us'] > 0]
                total_valid_rows += len(valid_rows)
                total_rows += len(df)
                if not valid_rows.empty:
                    run_p99_values.append(valid_rows['interval_latency_p99_us'].mean() / 1000.0)
                    run_mean_values.append(valid_rows['interval_latency_mean_us'].mean() / 1000.0)
            except Exception:
                pass

        if run_p99_values:
            stress_latency[node_count] = {
                'mean_p99_ms': np.mean(run_p99_values),
                'std_p99_ms': np.std(run_p99_values, ddof=1) if len(run_p99_values) > 1 else 0.0,
                'mean_mean_ms': np.mean(run_mean_values),
                'std_mean_ms': np.std(run_mean_values, ddof=1) if len(run_mean_values) > 1 else 0.0,
                'valid_rows': total_valid_rows,
                'total_rows': total_rows,
            }
        else:
            stress_latency[node_count] = {
                'mean_p99_ms': 0, 'std_p99_ms': 0,
                'mean_mean_ms': 0, 'std_mean_ms': 0,
                'valid_rows': 0, 'total_rows': total_rows,
            }

    for node_count, path_list in paths['ideal-latency'].items():
        run_p99_values = []
        run_mean_latencies = []
        run_std_devs_us = []
        all_p99_values = []
        all_mean_values = []
        for path in path_list:
            try:
                df = pd.read_csv(path)
                valid_rows = df[df['interval_latency_p99_us'] > 0]
                if not valid_rows.empty:
                    run_p99_values.append(valid_rows['interval_latency_p99_us'].mean() / 1000.0)
                    run_mean_latencies.append(valid_rows['interval_latency_mean_us'].mean() / 1000.0)
                    run_std_devs_us.append(valid_rows['interval_std_dev_us'].mean())
                    all_p99_values.append(valid_rows['interval_latency_p99_us'].values / 1000.0)
                    all_mean_values.append(valid_rows['interval_latency_mean_us'].values / 1000.0)
            except Exception:
                pass

        if run_p99_values:
            ideal_latency[node_count] = {
                'mean_p99_ms': np.mean(run_p99_values),
                'std_p99_ms': np.std(run_p99_values, ddof=1) if len(run_p99_values) > 1 else 0.0,
                'mean_mean_ms': np.mean(run_mean_latencies),
                'std_mean_ms': np.std(run_mean_latencies, ddof=1) if len(run_mean_latencies) > 1 else 0.0,
                'mean_std_us': np.mean(run_std_devs_us),
                'p99_values': np.concatenate(all_p99_values),
                'mean_values': np.concatenate(all_mean_values),
            }
        else:
            ideal_latency[node_count] = {
                'mean_p99_ms': 0, 'std_p99_ms': 0,
                'mean_mean_ms': 0, 'std_mean_ms': 0,
                'mean_std_us': 0, 'p99_values': np.array([]),
                'mean_values': np.array([]),
            }

    return throughput, stress_latency, ideal_latency


def load_azure_resource_data(path):
    """Load Azure VM metrics from an exported Excel file => Generated file by Azure on the dashboard"""
    if not path or not os.path.exists(path):
        return None
    try:
        # Azure export has metric headers starting at row 11
        df = pd.read_excel(path, header=10, engine='openpyxl')

        df.rename(columns={df.columns[0]: 'Timestamp'}, inplace=True)
        df['Timestamp'] = pd.to_datetime(df['Timestamp'])

        # Layout: Timestamp + 9 CPU columns + 9 Disk columns
        if len(df.columns) < 19:
            return None

        cpu_columns = df.columns[1:10]
        disk_columns = df.columns[10:19]

        for col in list(cpu_columns) + list(disk_columns):
            df[col] = pd.to_numeric(df[col], errors='coerce')

        df['Cluster_Avg_CPU'] = df[cpu_columns].mean(axis=1)
        df['Cluster_Total_Disk_MBs'] = df[disk_columns].sum(axis=1) / (1024 * 1024)
        return df
    except Exception as e:
        print(f"Error loading Excel file: {e}")
        return None


def plot_throughput_scalability(throughput, cluster_sizes, labels):
    """Bar chart with error bars and theoretical linear scaling reference line"""
    fig, ax = plt.subplots(figsize=(8, 5))

    means = [throughput[n]['mean'] for n in cluster_sizes]
    stds = [throughput[n]['std'] for n in cluster_sizes]
    colors = [CLUSTER_COLORS.get(n, COLORS['blue']) for n in cluster_sizes]

    bars = ax.bar(labels, means, color=colors, edgecolor='black', alpha=0.85,
                  yerr=stds, capsize=6, error_kw={'linewidth': 1.2})

    baseline_throughput = means[0]
    baseline_nodes = cluster_sizes[0]
    theoretical_linear = [baseline_throughput * (n / baseline_nodes) for n in cluster_sizes]
    ax.plot(labels, theoretical_linear, '--', color=COLORS['gray'], linewidth=1.5,
            marker='D', markersize=5, label='Theoretical linear', zorder=5)

    max_std = max(stds)
    for bar, mean, std in zip(bars, means, stds):
        label_y = mean + std + max_std * 0.3
        ax.text(bar.get_x() + bar.get_width() / 2., label_y,
                f'{int(mean):,}', ha='center', va='bottom', fontsize=13, fontweight='bold')

    ax.set_ylabel('Throughput (msgs/sec)', fontsize=12)
    ax.set_xlabel('Cluster Size', fontsize=12)
    ax.legend(fontsize=10)
    ax.grid(axis='y', linestyle='--', alpha=0.5)
    ax.set_ylim(0, max(means) * 1.25)
    fig.tight_layout()
    save_plot(fig, '1_throughput_scalability')
    plt.close(fig)
    print("Throughput scalability saved.")


def plot_throughput_timeseries(paths, cluster_sizes):
    """Time series of throughput per cluster size"""
    fig, ax = plt.subplots(figsize=(10, 5))

    for node_count in cluster_sizes:
        path_list = paths['linear-capacity'].get(node_count, [])
        if not path_list:
            continue

        run_series = []
        for path in path_list:
            df = pd.read_csv(path)
            series = df.set_index(df['elapsed_seconds'].astype(int))['interval_throughput']
            series = series[series > 1000]
            run_series.append(series)

        if not run_series:
            continue

        # Align all runs on their shared time indices
        common_index = run_series[0].index
        for series in run_series[1:]:
            common_index = common_index.intersection(series.index)
        common_index = common_index.sort_values()

        aligned_values = np.array([s.loc[common_index].values for s in run_series])
        mean_throughput = aligned_values.mean(axis=0)
        std_throughput = aligned_values.std(axis=0, ddof=1) if len(run_series) > 1 else np.zeros_like(mean_throughput)

        time_minutes = common_index.values / 60.0
        color = CLUSTER_COLORS.get(node_count, COLORS['blue'])

        ax.plot(time_minutes, mean_throughput, color=color, alpha=0.8, linewidth=0.6,
                label=f'{node_count} Nodes')
        if len(run_series) > 1:
            ax.fill_between(time_minutes, mean_throughput - std_throughput,
                            mean_throughput + std_throughput, color=color, alpha=0.15)

    ax.set_ylabel('Throughput (msgs/sec)', fontsize=18)
    ax.set_xlabel('Time (minutes)', fontsize=18)
    ax.tick_params(axis='both', labelsize=15)
    legend = ax.legend(fontsize=18, loc='lower center', ncol=3, framealpha=0.9,
                       handlelength=2, handletextpad=0.8)
    for line in legend.get_lines():
        line.set_linewidth(5)
    ax.grid(True, linestyle='--', alpha=0.5)
    ax.set_ylim(bottom=0)
    fig.tight_layout()
    save_plot(fig, '6_throughput_timeseries')
    plt.close(fig)
    print("Throughput time series saved.")


def plot_latency_gradient(stress_latency, ideal_latency, cluster_sizes, labels):
    """Comparison bar charts that compare ideal vs stress P99 latency per cluster size"""
    fig, (ideal_axis, stress_axis) = plt.subplots(1, 2, figsize=(8, 5),
                                                   gridspec_kw={'width_ratios': [1, 1]})
    x = np.arange(len(cluster_sizes))
    bar_width = 0.5
    colors = [CLUSTER_COLORS.get(n, COLORS['blue']) for n in cluster_sizes]

    ideal_p99 = [ideal_latency[n]['mean_p99_ms'] for n in cluster_sizes]
    ideal_std = [ideal_latency[n]['std_p99_ms'] for n in cluster_sizes]
    ideal_bars = ideal_axis.bar(x, ideal_p99, bar_width, color=colors, edgecolor='black',
                                alpha=0.85, yerr=ideal_std, capsize=5)
    for bar, value, std in zip(ideal_bars, ideal_p99, ideal_std):
        label_y = value + std + max(ideal_std) * 0.3
        ideal_axis.text(bar.get_x() + bar.get_width() / 2., label_y,
                        f'{value:.1f}', ha='center', va='bottom', fontsize=13, fontweight='bold')
    ideal_axis.set_xticks(x)
    ideal_axis.set_xticklabels(labels)
    ideal_axis.set_ylabel('P99 Latency (ms)')
    ideal_axis.set_title('Ideal Latency')
    ideal_axis.grid(axis='y', linestyle='--', alpha=0.5)
    ideal_axis.set_ylim(0, max(ideal_p99) * 1.45)

    stress_p99 = [stress_latency[n]['mean_p99_ms'] for n in cluster_sizes]
    stress_std = [stress_latency[n]['std_p99_ms'] for n in cluster_sizes]
    stress_bars = stress_axis.bar(x, stress_p99, bar_width, color=colors, edgecolor='black',
                                  alpha=0.85, yerr=stress_std, capsize=5)
    for bar, value, std in zip(stress_bars, stress_p99, stress_std):
        label_y = value + std + max(stress_std) * 0.3
        stress_axis.text(bar.get_x() + bar.get_width() / 2., label_y,
                         f'{value:.0f}', ha='center', va='bottom', fontsize=13, fontweight='bold')
    stress_axis.set_xticks(x)
    stress_axis.set_xticklabels(labels)
    stress_axis.set_ylabel('P99 Latency (ms)')
    stress_axis.set_title('Stress Latency')
    stress_axis.grid(axis='y', linestyle='--', alpha=0.5)
    stress_axis.set_ylim(0, max(stress_p99) * 1.45)

    fig.tight_layout()
    save_plot(fig, '2_latency_gradient', bbox_inches='tight')
    plt.close(fig)
    print("Latency gradient saved.")


def plot_consensus_tax(ideal_latency, cluster_sizes, labels):
    """Mean and P99 latency lines with std dev bands that show Raft consensus overhead"""
    fig, ax = plt.subplots(figsize=(8, 5))

    mean_latencies = [ideal_latency[n]['mean_mean_ms'] for n in cluster_sizes]
    mean_stds = [ideal_latency[n]['std_mean_ms'] for n in cluster_sizes]
    p99_latencies = [ideal_latency[n]['mean_p99_ms'] for n in cluster_sizes]
    p99_stds = [ideal_latency[n]['std_p99_ms'] for n in cluster_sizes]

    x = np.arange(len(cluster_sizes))

    ax.plot(x, mean_latencies, marker='o', linewidth=2, color=COLORS['blue'],
            label='Mean Latency', zorder=5)
    ax.fill_between(x,
                    [m - s for m, s in zip(mean_latencies, mean_stds)],
                    [m + s for m, s in zip(mean_latencies, mean_stds)],
                    alpha=0.2, color=COLORS['blue'])

    ax.plot(x, p99_latencies, marker='s', linewidth=2, color=COLORS['red'],
            label='P99 Latency', zorder=5)
    ax.fill_between(x,
                    [p - s for p, s in zip(p99_latencies, p99_stds)],
                    [p + s for p, s in zip(p99_latencies, p99_stds)],
                    alpha=0.2, color=COLORS['red'])

    for i in range(len(cluster_sizes)):
        ax.text(x[i], mean_latencies[i] - mean_stds[i] - 0.3,
                f'{mean_latencies[i]:.2f}', ha='center', va='top', fontsize=13,
                color=COLORS['blue'])
        ax.text(x[i], p99_latencies[i] + p99_stds[i] + 0.1,
                f'{p99_latencies[i]:.2f}', ha='center', va='bottom', fontsize=13,
                color=COLORS['red'])

    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.set_ylabel('Latency (ms)')
    ax.set_xlabel('Cluster Size')

    legend = ax.legend(fontsize=14, loc='upper left')
    for line in legend.get_lines():
        line.set_linewidth(4)
    ax.grid(True, linestyle='--', alpha=0.5)
    max_p99_top = max(p + s for p, s in zip(p99_latencies, p99_stds))
    ax.set_ylim(bottom=0, top=max_p99_top + 1.5)
    fig.tight_layout()
    save_plot(fig, '3_consensus_tax')
    plt.close(fig)
    print("Raft Consensus tax plot saved.")


def plot_resource_saturation(paths):
    """Time series of cluster-wide CPU and disk write throughput"""
    resource_data = load_azure_resource_data(paths['resources'])
    if resource_data is None:
        return

    fig, cpu_axis = plt.subplots(figsize=(8, 5))
    cpu_axis.set_xlabel('Time')
    cpu_axis.set_ylabel('Avg Cluster CPU (%)', color=COLORS['blue'])
    cpu_axis.plot(resource_data['Timestamp'], resource_data['Cluster_Avg_CPU'],
                  color=COLORS['blue'], label='CPU', linewidth=1)
    cpu_axis.tick_params(axis='y', labelcolor=COLORS['blue'])
    cpu_axis.set_ylim(0, 100)

    disk_axis = cpu_axis.twinx()
    disk_axis.set_ylabel('Total Disk Write (MB/s)', color=COLORS['red'])
    disk_axis.plot(resource_data['Timestamp'], resource_data['Cluster_Total_Disk_MBs'],
                   color=COLORS['red'], label='Disk', linewidth=1)
    disk_axis.tick_params(axis='y', labelcolor=COLORS['red'])

    cpu_axis.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))

    fig.tight_layout()
    save_plot(fig, '4_resource_saturation')
    plt.close(fig)
    print("Resource saturation plot saved.")


def plot_raft_replication():
    """Raft replication diagram for a quorum queue publish of a 6-node cluster"""
    fig, ax = plt.subplots(figsize=(8, 5.5))
    ax.set_xlim(-0.2, 9.3)
    ax.set_ylim(2.5, 13)
    ax.axis('off')

    publisher_x = 0.8
    leader_x = 2.8
    follower_xs = [4.5, 5.6, 6.7, 7.8, 8.9]
    all_xs = [publisher_x, leader_x] + follower_xs
    lifeline_labels = ['Publisher', 'Leader', 'F1', 'F2', 'F3', 'F4', 'F5']

    top_y = 12.5
    bottom_y = 3.0

    box_style = dict(boxstyle='round,pad=0.3', facecolor='#e8e8e8', edgecolor='black', linewidth=1.2)
    leader_style = dict(boxstyle='round,pad=0.3', facecolor='#d4e6f1', edgecolor='black', linewidth=1.2)

    for x, label in zip(all_xs, lifeline_labels):
        style = leader_style if label == 'Leader' else box_style
        ax.text(x, top_y, label, ha='center', va='center', fontsize=10, fontweight='bold', bbox=style)
        ax.plot([x, x], [top_y - 0.5, bottom_y], color='#999999', linewidth=0.8, linestyle='--')

    arrow = dict(arrowstyle='->', color='black', linewidth=2.0)
    ack_arrow = dict(arrowstyle='->', color=COLORS['green'], linewidth=2.0)
    confirm_arrow = dict(arrowstyle='->', color=COLORS['blue'], linewidth=2.0)

    # 1. Publisher -> Leader: publish
    y = 11.5
    ax.annotate('', xy=(leader_x, y - 0.3), xytext=(publisher_x, y), arrowprops=arrow)
    ax.text((publisher_x + leader_x) / 2, y + 0.2, 'publish', ha='center', fontsize=12, style='italic')

    # 2. Leader appends to WAL
    ax.text(leader_x + 0.15, 10.6, 'append to WAL', ha='left', fontsize=11, style='italic', color='black')

    # 3. Leader -> Followers: replicate
    replicate_base_y = 9.6
    replicate_step = 0.3
    for i, follower_x in enumerate(follower_xs):
        y_src = replicate_base_y - i * replicate_step
        ax.annotate('', xy=(follower_x, y_src - 0.15), xytext=(leader_x, y_src), arrowprops=arrow)
    ax.text((leader_x + follower_xs[-1]) / 2, replicate_base_y + 0.25,
            'replicate log entry', ha='center', fontsize=12, style='italic')

    # 4. First 3 followers ACK
    ack_base_y = 7.0
    ack_step = 0.3
    for i, follower_x in enumerate(follower_xs[:3]):
        y_dst = ack_base_y - i * ack_step
        ax.annotate('', xy=(leader_x, y_dst), xytext=(follower_x, y_dst), arrowprops=ack_arrow)
    ax.text((leader_x + follower_xs[2]) / 2, ack_base_y + 0.35, 'ACK', ha='center', fontsize=12,
            color=COLORS['green'], fontweight='bold')

    # 5. Quorum reached
    ax.text(leader_x + 0.15, 5.4, 'quorum reached\n(4 of 6 persisted)', ha='left', fontsize=11,
            color=COLORS['green'], fontweight='bold')

    # 6. Slower followers ACK after commit
    late_ack_base_y = 4.6
    late_ack_step = 0.3
    for i, follower_x in enumerate(follower_xs[3:]):
        y_dst = late_ack_base_y - i * late_ack_step
        ax.annotate('', xy=(leader_x, y_dst), xytext=(follower_x, y_dst),
                    arrowprops=dict(arrowstyle='->', color='black', linewidth=1.8, linestyle='--'))
    ax.text((leader_x + follower_xs[4]) / 2, late_ack_base_y + 0.35,
            'ACK (late)', ha='center', fontsize=12, color='black')

    # 7. Leader -> Publisher: confirm
    confirm_y = 3.8
    ax.annotate('', xy=(publisher_x, confirm_y + 0.3), xytext=(leader_x, confirm_y),
                arrowprops=confirm_arrow)
    ax.text((publisher_x + leader_x) / 2, confirm_y + 0.55, 'confirm', ha='center', fontsize=12,
            color=COLORS['blue'], fontweight='bold')

    ax.annotate('', xy=(0.1, bottom_y + 0.5), xytext=(0.1, top_y - 0.8),
                arrowprops=dict(arrowstyle='->', color='#555555', linewidth=1.5))
    ax.text(-0.05, (top_y + bottom_y) / 2, 'time', ha='center', va='center', fontsize=12,
            color='#555555', rotation=90)

    fig.tight_layout()
    save_plot(fig, '0A_raft_replication')
    plt.close(fig)

def plot_infrastructure():
    """Azure infrastructure with Load Generator + N cluster nodes in a vnet"""
    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax.set_xlim(0.1, 9.9)
    ax.set_ylim(0.0, 5.0)
    ax.axis('off')

    vnet_rect = mpatches.FancyBboxPatch((0.3, 0.3), 9.4, 4.4, boxstyle='round,pad=0.15',
                                         facecolor='#f0f4f8', edgecolor='#5b9bd5',
                                         linewidth=1.5, linestyle='--')
    ax.add_patch(vnet_rect)
    ax.text(5.0, 4.5, 'Azure Virtual Network (Subnet)', ha='center', fontsize=13,
            color='#5b9bd5', fontweight='bold')

    generator_style = dict(boxstyle='round,pad=0.5', facecolor='#d5e8d4', edgecolor='#82b366', linewidth=1.5)
    ax.text(2.2, 2.5, 'Load Generator\nF32s v2\n32 vCPUs\n64 GB RAM', ha='center', va='center',
            fontsize=12, fontweight='bold', bbox=generator_style)

    node_style = dict(boxstyle='round,pad=0.4', facecolor='#dae8fc', edgecolor='#6c8ebf', linewidth=1.5)
    node_positions = [(7.2, 3.8), (7.2, 2.5), (7.2, 1.2)]
    node_labels = ['Node 1', 'Node 2', 'Node N']

    for i, (node_x, node_y) in enumerate(node_positions):
        if i == 2:
            ax.text(node_x, node_y + 0.75, '...', ha='center', va='center',
                    fontsize=20, color='#333333', fontweight='bold')
        spec_label = 'D4s v6\n4 vCPUs, 16 GB RAM'
        ax.text(node_x, node_y, f'{node_labels[i]}\n{spec_label}', ha='center', va='center',
                fontsize=12, fontweight='bold', bbox=node_style)

    ax.text(9.1, 2.5, 'RabbitMQ\nCluster', ha='center', va='center', fontsize=13,
            color='#6c8ebf', fontweight='bold', rotation=270)

    arrow_props = dict(arrowstyle='->', color='#333333', linewidth=1.4)
    for node_x, node_y in node_positions:
        ax.annotate('', xy=(5.8, node_y), xytext=(3.6, 2.5), arrowprops=arrow_props)

    ax.text(4.5, 3.7, 'AMQP', ha='center', fontsize=13, style='italic', color='#333333')

    fig.tight_layout()
    save_plot(fig, '0B_infrastructure')
    plt.close(fig)

def plot_all():
    """Generate all plots"""
    os.makedirs(OUT_DIR, exist_ok=True)

    # Diagrams
    # 0A_raft_replication
    plot_raft_replication()

    # 0B_infrastructure
    plot_infrastructure()

    paths = discover_paths(BASE_DIR)
    throughput, stress_latency, ideal_latency = get_summary_metrics(paths)

    cluster_sizes = sorted(throughput.keys())
    if not cluster_sizes:
        print("No valid data found.")
        return

    labels = [f"{n} Nodes" for n in cluster_sizes]

    for node_count in cluster_sizes:
        tp = throughput[node_count]
        print(f"[{node_count}-node] Throughput: mean={tp['mean']:,.0f} msgs/s, std={tp['std']:,.0f}")
    for node_count in cluster_sizes:
        il = ideal_latency.get(node_count, {})
        print(f"[{node_count}-node] Ideal P99: mean={il.get('mean_p99_ms', 0):.2f} ms, "
              f"mean_mean={il.get('mean_mean_ms', 0):.2f} ms")
    for node_count in cluster_sizes:
        sl = stress_latency.get(node_count, {})
        print(f"[{node_count}-node] Stress P99: mean={sl.get('mean_p99_ms', 0):.1f} ms, "
              f"valid={sl.get('valid_rows', 0)}/{sl.get('total_rows', 0)}")
    print()

    # Benchmark result plots
    # 1_throughput_scalability
    plot_throughput_scalability(throughput, cluster_sizes, labels)

    # 2_latency_gradient
    if stress_latency and ideal_latency:
        plot_latency_gradient(stress_latency, ideal_latency, cluster_sizes, labels)

    # 3_consensus_tax
    if ideal_latency:
        plot_consensus_tax(ideal_latency, cluster_sizes, labels)

    # 4_resource_saturation
    plot_resource_saturation(paths)  

    # 6_throughput_timeseries
    plot_throughput_timeseries(paths, cluster_sizes)

    print(f"\nAll plots saved to {OUT_DIR}")


if __name__ == "__main__":
    plot_all()
