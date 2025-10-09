# -*- coding: utf-8 -*-
"""
一个可复用的脚本，用于聚合多个CSV文件并根据 'dimension' 生成分面热力图。

该脚本会执行以下操作:
1. 在指定的数据目录 (默认为 ../data/) 中查找所有 'evaluation_*.csv' 文件。
2. 从文件名中提取分面标识 (例如 'Sijk')，并将其作为新列添加到数据中。
3. 将所有CSV文件的数据合并到一个大的DataFrame中。
4. 按 'dimension' 列的值对数据进行分组。
5. 为每一个 'dimension' 值，生成一张单独的图片。
6. 在每张图片中，使用从文件名提取的标识作为分面，绘制热力图。
   - X轴: cache_line_size
   - Y轴: cache_line_number
   - 颜色 (Z轴): cache_miss
7. 所有配置参数均可在脚本顶部的"配置"部分进行自定义。
"""

import os
import glob
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.interpolate import griddata
from matplotlib.colors import LogNorm
from math import ceil
import re

# ==============================================================================
# 配置 (Configuration)
# 用户可以根据需要修改此部分参数
# ==============================================================================
# 定义数据和输出目录路径
DATA_DIR = '../data'
OUTPUT_DIR = '../output'

# 定义CSV文件中的原始列名
# 'group': 用于分组生成不同图片的变量
# 'x', 'y', 'z': 热力图的维度和颜色值
COLUMN_NAMES = {
    'group': 'dimension',
    'x': 'cache_line_size',
    'y': 'cache_line_number',
    'z': 'cache_miss'
}

# 新增：用于存储从文件名中提取的分面标识的列名
FACET_COL_NAME = 'evaluation_type'

# 插值和平滑处理相关的参数
INTERPOLATION_POINTS = 50j
INTERPOLATION_METHOD = 'cubic'

# 绘图美学相关的参数
CONTOUR_LEVELS = 200  # 增加层级数以获得更平滑的颜色过渡
COLOR_MAP = 'viridis'
FIGURE_DPI = 300
SUBPLOT_LAYOUT_COLS = 3

# 是否对Z轴（颜色）使用对数刻度
USE_LOG_SCALE = False

# ==============================================================================
# 核心功能函数 (Core Functions)
# ==============================================================================

def setup_plot_style():
    """
    设置一个现代、适合学术出版的全局绘图风格。
    """
    sns.set_theme(
        context='paper',
        style='whitegrid',
        palette=COLOR_MAP,
        font='sans-serif',
        rc={
            'figure.figsize': (10, 8),
            'axes.labelsize': 12,
            'xtick.labelsize': 10,
            'ytick.labelsize': 10,
            'legend.fontsize': 10,
            'axes.titlesize': 14,
            'figure.dpi': FIGURE_DPI
        }
    )

def plot_single_heatmap(ax, df, x_col, y_col, z_col, config):
    """
    在给定的 Axes 对象上执行数据插值并绘制单个热力图。
    """
    x = df[x_col]
    y = df[y_col]
    z = df[z_col]
    
    if len(df) < 3:
        ax.text(0.5, 0.5, 'Data points < 3\nCannot interpolate', 
                ha='center', va='center', style='italic', color='gray')
        return None

    grid_x, grid_y = np.mgrid[
        x.min():x.max():config['interp_points'],
        y.min():y.max():config['interp_points']
    ]
    
    points = np.vstack((x, y)).T
    grid_z = griddata(points, z, (grid_x, grid_y), method=config['interp_method'])
    
    contour = ax.contourf(
        grid_x, grid_y, grid_z,
        levels=config['contour_levels'],
        cmap=config['cmap'],
        vmin=config.get('vmin'),
        vmax=config.get('vmax'),
        norm=config.get('norm', None)
    )
    return contour

def create_faceted_heatmap(df, facet_col, output_path, col_names, config, main_title):
    """
    从一个DataFrame创建并保存分面热力图。
    """
    x_col = col_names['x']
    y_col = col_names['y']
    z_col = col_names['z']
    
    facet_values = sorted(df[facet_col].unique())
    num_facets = len(facet_values)
    
    if num_facets == 0:
        print(f"警告: 在数据中没有找到有效的分面数据用于生成 {os.path.basename(output_path)}。")
        return

    ncols = min(config['subplot_cols'], num_facets)
    nrows = ceil(num_facets / ncols)
    
    fig, axes = plt.subplots(nrows, ncols, figsize=(4.5 * ncols, 3.5 * nrows), squeeze=False)
    
    config['norm'] = None
    if config.get('use_log_scale'):
        z_positive = df[df[z_col] > 0][z_col]
        if not z_positive.empty:
            vmin, vmax = z_positive.min(), df[z_col].max()
            if vmin < vmax:
                config['norm'] = LogNorm(vmin=vmin, vmax=vmax)
    
    if not config.get('norm'):
        config['vmin'], config['vmax'] = df[z_col].min(), df[z_col].max()
    
    contour_map = None
    
    for i, facet_val in enumerate(facet_values):
        row, col = divmod(i, ncols)
        ax = axes[row, col]
        
        subset_df = df[df[facet_col] == facet_val].copy()
        
        contour_map = plot_single_heatmap(ax, subset_df, x_col, y_col, z_col, config)
        
        ax.set_title(f"{facet_col.replace('_', ' ').title()} = {facet_val}")
        ax.set_xlabel(x_col.replace('_', ' ').title())
        ax.set_ylabel(y_col.replace('_', ' ').title())

    for i in range(num_facets, nrows * ncols):
        row, col = divmod(i, ncols)
        fig.delaxes(axes[row, col])
        
    if contour_map:
        cbar = fig.colorbar(contour_map, ax=axes.ravel().tolist(), pad=0.02)
        label = z_col.replace('_', ' ').title()
        if config.get('use_log_scale') and isinstance(config.get('norm'), LogNorm):
             label += ' (Log Scale)'
        cbar.set_label(label)
        
    fig.subplots_adjust(left=0.07, right=0.75, bottom=0.1, top=0.9, wspace=0.3, hspace=0.4)
    fig.suptitle(main_title, fontsize=16, weight='bold')
    
    plt.savefig(output_path, dpi=FIGURE_DPI, bbox_inches='tight')
    plt.close(fig)
    print(f"已成功绘制并保存图像: {output_path}")

# ==============================================================================
# 主执行逻辑 (Main Execution)
# ==============================================================================

def main():
    """
    主函数，负责加载、合并所有CSV数据，并按'dimension'分组调用绘图函数。
    """
    setup_plot_style()
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    csv_files = glob.glob(os.path.join(DATA_DIR, 'evaluation_*.csv'))
    
    if not csv_files:
        print(f"错误: 在目录 '{DATA_DIR}' 中未找到任何 'evaluation_*.csv' 文件。")
        return
        
    print(f"找到 {len(csv_files)} 个CSV文件，开始聚合数据...")
    
    all_dfs = []
    for file_path in csv_files:
        try:
            df = pd.read_csv(file_path)
            
            # 从文件名提取分面标识 (e.g., Sijk)
            match = re.search(r'evaluation_(.*)\.csv', os.path.basename(file_path))
            if match:
                facet_name = match.group(1)
                df[FACET_COL_NAME] = facet_name
                all_dfs.append(df)
            else:
                print(f"跳过文件 {os.path.basename(file_path)}: 文件名格式不匹配。")

        except Exception as e:
            print(f"读取文件 {os.path.basename(file_path)} 时发生错误: {e}")
            
    if not all_dfs:
        print("错误: 未能成功加载任何数据。")
        return
        
    # 合并所有数据
    merged_df = pd.concat(all_dfs, ignore_index=True)
    
    group_col = COLUMN_NAMES['group']
    unique_groups = sorted(merged_df[group_col].unique())
    
    print(f"数据聚合完毕。将按 '{group_col}' 列的 {len(unique_groups)} 个唯一值生成图像...")
    
    plot_config = {
        'interp_points': INTERPOLATION_POINTS,
        'interp_method': INTERPOLATION_METHOD,
        'contour_levels': CONTOUR_LEVELS,
        'cmap': COLOR_MAP,
        'subplot_cols': SUBPLOT_LAYOUT_COLS,
        'use_log_scale': USE_LOG_SCALE
    }
    
    # 按 'dimension' (group_col) 循环生成图像
    for group_val in unique_groups:
        df_subset = merged_df[merged_df[group_col] == group_val].copy()
        
        output_path = os.path.join(OUTPUT_DIR, f"{group_col}_{group_val}.png")
        main_title = f"{group_col.replace('_', ' ').title()} = {group_val}"
        
        create_faceted_heatmap(
            df=df_subset,
            facet_col=FACET_COL_NAME,
            output_path=output_path,
            col_names=COLUMN_NAMES,
            config=plot_config,
            main_title=main_title
        )

if __name__ == "__main__":
    main()

