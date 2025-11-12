# -*- coding: utf-8 -*-
"""
project_3_plotter
~~~~~~~~~~~~~~~~~

读取 `data/project_3/origin_data.csv`，基于外部归并改进实验的多组 k 结果生成可视化图表，
帮助分析 run 长度分布、整体耗时以及哈夫曼归并树深度等指标。

使用方法:
    python project_3_plotter.py ../data/project_3/origin_data.csv --output-dir ../data/project_3/plots
"""

import argparse
import os
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

# 默认路径（相对于 python/ 目录）
DEFAULT_CSV = "../data/project_3/origin_data.csv"
DEFAULT_OUTPUT_DIR = "../data/project_3/plots"

REQUIRED_COLUMNS = [
    "k",
    "run_count",
    "total_numbers",
    "min_run_length",
    "max_run_length",
    "avg_run_length",
    "total_time_ms",
    "max_tree_depth",
    "weighted_path_length",
]


def configure_style():
    """统一绘图风格，方便与前两个实验保持一致的观感。"""
    sns.set_theme(
        context="paper",
        style="whitegrid",
        palette="viridis",
        rc={
            "figure.figsize": (10, 6),
            "axes.titlesize": 16,
            "axes.labelsize": 13,
            "xtick.labelsize": 11,
            "ytick.labelsize": 11,
            "legend.fontsize": 11,
        },
    )


def validate_dataframe(df: pd.DataFrame):
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"CSV 文件缺少必要列: {', '.join(missing)}")


def plot_run_length_stats(df: pd.DataFrame, output_dir: Path):
    fig, ax = plt.subplots()
    for col, label in [
        ("min_run_length", "最短顺串"),
        ("avg_run_length", "平均顺串"),
        ("max_run_length", "最长顺串"),
    ]:
        ax.plot(df["k"], df[col], marker="o", label=label)

    ax.set_title("不同 k 下的顺串长度统计")
    ax.set_xlabel("k（败者树叶子数）")
    ax.set_ylabel("顺串长度")
    ax.legend()
    fig.tight_layout()

    path = output_dir / "run_length_stats.png"
    fig.savefig(path, dpi=300)
    plt.close(fig)
    print(f"[Plot] 已生成 {path}")


def plot_performance(df: pd.DataFrame, output_dir: Path):
    fig, ax1 = plt.subplots()

    color_time = "tab:blue"
    ax1.set_xlabel("k（败者树叶子数）")
    ax1.set_ylabel("排序耗时 / ms", color=color_time)
    ax1.plot(df["k"], df["total_time_ms"], marker="s", color=color_time, label="耗时 (ms)")
    ax1.tick_params(axis="y", labelcolor=color_time)

    ax2 = ax1.twinx()
    color_runs = "tab:green"
    ax2.set_ylabel("顺串数量", color=color_runs)
    ax2.bar(df["k"], df["run_count"], color=color_runs, alpha=0.35, label="顺串数量")
    ax2.tick_params(axis="y", labelcolor=color_runs)

    fig.suptitle("不同 k 下的耗时与顺串数量")
    fig.tight_layout()
    path = output_dir / "performance.png"
    fig.savefig(path, dpi=300)
    plt.close(fig)
    print(f"[Plot] 已生成 {path}")


def plot_tree_metrics(df: pd.DataFrame, output_dir: Path):
    fig, ax = plt.subplots()
    ax.bar(df["k"], df["max_tree_depth"], label="最大树深度", alpha=0.6)
    ax.set_xlabel("k（败者树叶子数）")
    ax.set_ylabel("最大树深度")

    ax2 = ax.twinx()
    ax2.plot(
        df["k"],
        df["weighted_path_length"],
        color="tab:red",
        marker="^",
        label="加权路径长度",
    )
    ax2.set_ylabel("加权路径长度")

    fig.suptitle("哈夫曼归并树结构指标")
    fig.tight_layout()
    path = output_dir / "merge_tree_metrics.png"
    fig.savefig(path, dpi=300)
    plt.close(fig)
    print(f"[Plot] 已生成 {path}")


def generate_summary_csv(df: pd.DataFrame, output_dir: Path):
    """计算若干扩展指标并写入 summary CSV，便于后续文档引用。"""
    summary = df.copy()
    summary["numbers_per_run"] = (
        summary["total_numbers"] / summary["run_count"].replace(0, pd.NA)
    )
    summary["time_per_number_ms"] = summary["total_time_ms"] / summary["total_numbers"]

    summary_path = output_dir / "project_3_analysis.csv"
    summary.to_csv(summary_path, index=False)
    print(f"[CSV] 已输出分析数据: {summary_path}")


def parse_args():
    parser = argparse.ArgumentParser(
        description="读取 project_3 origin_data.csv 并生成可视化/分析结果"
    )
    parser.add_argument(
        "csv",
        nargs="?",
        default=DEFAULT_CSV,
        help=f"输入 CSV 文件路径 (默认: {DEFAULT_CSV})",
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help=f"输出目录 (默认: {DEFAULT_OUTPUT_DIR})",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    csv_path = Path(args.csv).expanduser()
    output_dir = Path(args.output_dir).expanduser()
    output_dir.mkdir(parents=True, exist_ok=True)

    configure_style()

    df = pd.read_csv(csv_path)
    validate_dataframe(df)
    df = df.sort_values(by="k")

    plot_run_length_stats(df, output_dir)
    plot_performance(df, output_dir)
    plot_tree_metrics(df, output_dir)
    generate_summary_csv(df, output_dir)


if __name__ == "__main__":
    main()
