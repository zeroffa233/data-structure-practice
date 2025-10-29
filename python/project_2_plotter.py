# -*- coding: utf-8 -*-
"""
一个用于从 CSV 文件生成折线图的 Python 脚本。

该脚本会读取一个包含 'run_length' 和 'elapsed_time_ms' 列的 CSV 文件，
并根据这两列数据绘制一个带标记的折线图，以展示运行时间随运行长度的变化趋势。
最终生成的图表会保存为图片文件。

使用方法:
    python generate_histogram.py <your_csv_file.csv> --output <output_image_name.png>

参数:
    input_csv (str): 输入的 CSV 文件路径。
    --output (str, optional): 输出图像文件的路径。默认为 'line_plot.png'。
"""
import pandas as pd
import matplotlib.pyplot as plt
import argparse

def generate_line_plot(input_csv_path, output_image_path):
    """
    根据给定的CSV文件中的'run_length'和'elapsed_time_ms'列生成并保存折线图。

    Args:
        input_csv_path (str): 输入的 CSV 文件路径。
        output_image_path (str): 保存生成折线图的图片路径。
    """
    try:
        # 1. 使用 pandas 读取 CSV 文件
        data_frame = pd.read_csv(input_csv_path)

        # 2. 检查所需列是否存在
        required_columns = ['run_length', 'elapsed_time_ms']
        if not all(col in data_frame.columns for col in required_columns):
            print(f"错误: CSV 文件 '{input_csv_path}' 中必须同时包含 'run_length' 和 'elapsed_time_ms' 列。")
            return

        # 3. 为了保证折线图的正确连接顺序，我们根据 run_length 对数据进行排序
        data_frame = data_frame.sort_values(by='run_length')
        
        # 提取 x 和 y 轴的数据
        run_length = data_frame['run_length']
        elapsed_time = data_frame['elapsed_time_ms']

        # 4. 配置绘图环境
        plt.style.use('seaborn-v0_8-whitegrid') # 使用一个美观的绘图风格
        fig, ax = plt.subplots(figsize=(12, 8)) # 创建一个图形和坐标轴，设置尺寸

        # 5. 绘制带标记的折线图
        ax.plot(run_length, elapsed_time, marker='o', linestyle='-', color='royalblue', label='Elapsed Time')

        # 6. 添加全英文的标题和坐标轴标签
        ax.set_title('Run Length vs. Elapsed Time', fontsize=18, fontweight='bold')
        ax.set_xlabel('Run Length', fontsize=14)
        ax.set_ylabel('Elapsed Time (ms)', fontsize=14)

        # 7. 美化图表
        ax.tick_params(axis='both', which='major', labelsize=12)
        ax.grid(True, which='both', linestyle='--', linewidth=0.5)
        ax.legend(fontsize=12) # 显示图例
        fig.tight_layout() # 自动调整布局，防止标签重叠

        # 8. 保存图表到文件
        plt.savefig(output_image_path, dpi=300) # dpi=300 保证图片高清

        print(f"折线图已成功生成并保存至: {output_image_path}")

    except FileNotFoundError:
        print(f"错误: 文件未找到 '{input_csv_path}'")
    except Exception as e:
        print(f"处理过程中发生错误: {e}")


def main():
    """
    主函数，用于解析命令行参数并调用核心功能。
    """
    parser = argparse.ArgumentParser(
        description="从 CSV 文件生成 'run_length' vs 'elapsed_time_ms' 的折线图。"
    )
    parser.add_argument(
        "input_csv",
        type=str,
        help="输入的 CSV 文件路径。"
    )
    parser.add_argument(
        "--output",
        type=str,
        default="line_plot.png",
        help="输出图像文件的路径 (默认为: line_plot.png)。"
    )
    args = parser.parse_args()

    generate_line_plot(args.input_csv, args.output)

if __name__ == "__main__":
    main()

