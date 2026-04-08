#!/usr/bin/env python3
"""
绘制EdgeX和Lighter交易所之间的价差曲线图
价差定义：
1. lighter_best_bid - edgex_best_bid
2. lighter_best_bid - edgex_best_ask
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
import sys
import os
import subprocess
import platform

def plot_spread_curves(csv_file_path):
    """读取CSV文件并绘制价差曲线"""
    
    # 读取CSV文件
    print(f"Reading file: {csv_file_path}")
    df = pd.read_csv(csv_file_path)
    
    # 转换时间戳（处理不同格式的时间戳）
    df['timestamp'] = pd.to_datetime(df['timestamp'], format='mixed', errors='coerce', utc=True)
    
    # 将价格列转换为数值类型，空值转换为NaN
    price_columns = ['edgex_best_bid', 'edgex_best_ask', 'lighter_best_bid', 'lighter_best_ask']
    for col in price_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # 过滤出同时有edgex和lighter数据的行
    valid_mask = (
        df['edgex_best_bid'].notna() & 
        df['edgex_best_ask'].notna() & 
        df['lighter_best_bid'].notna()
    )
    df_valid = df[valid_mask].copy()
    
    print(f"Total records: {len(df)}")
    print(f"Valid records (with both edgex and lighter data): {len(df_valid)}")
    
    if len(df_valid) == 0:
        print("Error: No valid data to plot")
        return
    
    # 计算价差
    df_valid['spread_bid_bid'] = df_valid['lighter_best_bid'] - df_valid['edgex_best_bid']
    df_valid['spread_bid_ask'] = df_valid['lighter_best_bid'] - df_valid['edgex_best_ask']
    
    # 创建图表
    fig, ax = plt.subplots(figsize=(14, 8))
    
    # 绘制两条曲线
    ax.plot(df_valid['timestamp'], df_valid['spread_bid_bid'], 
            label='lighter_best_bid - edgex_best_bid', 
            linewidth=1.5, alpha=0.8, color='#2E86AB')
    ax.plot(df_valid['timestamp'], df_valid['spread_bid_ask'], 
            label='lighter_best_bid - edgex_best_ask', 
            linewidth=1.5, alpha=0.8, color='#A23B72')
    
    # 添加零线
    ax.axhline(y=0, color='gray', linestyle='--', linewidth=0.8, alpha=0.5)
    
    # 设置图表标题和标签
    ax.set_title('EdgeX vs Lighter Spread Curve (BTC)', fontsize=16, fontweight='bold', pad=20)
    ax.set_xlabel('Time', fontsize=12)
    ax.set_ylabel('Spread (USD)', fontsize=12)
    ax.legend(loc='best', fontsize=11, framealpha=0.9)
    ax.grid(True, alpha=0.3, linestyle='--')
    
    # 格式化x轴时间显示
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M'))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.xticks(rotation=45, ha='right')
    
    # 调整布局
    plt.tight_layout()
    
    # 保存图片
    output_file = 'logs/spread_curve_BTC.png'
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"\nChart saved to: {output_file}")
    
    # 显示统计信息
    print("\n=== Spread Statistics ===")
    print(f"lighter_best_bid - edgex_best_bid:")
    print(f"  Mean: {df_valid['spread_bid_bid'].mean():.2f} USD")
    print(f"  Median: {df_valid['spread_bid_bid'].median():.2f} USD")
    print(f"  Max: {df_valid['spread_bid_bid'].max():.2f} USD")
    print(f"  Min: {df_valid['spread_bid_bid'].min():.2f} USD")
    print(f"  Std Dev: {df_valid['spread_bid_bid'].std():.2f} USD")
    
    print(f"\nlighter_best_bid - edgex_best_ask:")
    print(f"  Mean: {df_valid['spread_bid_ask'].mean():.2f} USD")
    print(f"  Median: {df_valid['spread_bid_ask'].median():.2f} USD")
    print(f"  Max: {df_valid['spread_bid_ask'].max():.2f} USD")
    print(f"  Min: {df_valid['spread_bid_ask'].min():.2f} USD")
    print(f"  Std Dev: {df_valid['spread_bid_ask'].std():.2f} USD")
    
    # 自动打开图片（macOS/Linux/Windows）
    try:
        if platform.system() == 'Darwin':  # macOS
            subprocess.run(['open', output_file])
            print(f"\nImage opened in default viewer")
        elif platform.system() == 'Linux':
            subprocess.run(['xdg-open', output_file])
            print(f"\nImage opened in default viewer")
        elif platform.system() == 'Windows':
            os.startfile(output_file)
            print(f"\nImage opened in default viewer")
    except Exception as e:
        print(f"\nNote: Could not auto-open image: {e}")
        print(f"Please open manually: {os.path.abspath(output_file)}")
    
    # 显示交互式图表窗口
    plt.show()


if __name__ == "__main__":
    # 默认文件路径
    default_file = "logs/bbo_record_BTC.csv"
    
    # 如果提供了命令行参数，使用该参数
    if len(sys.argv) > 1:
        csv_file = sys.argv[1]
    else:
        csv_file = default_file
    
    # 检查文件是否存在
    if not os.path.exists(csv_file):
        print(f"错误: 文件不存在: {csv_file}")
        sys.exit(1)
    
    plot_spread_curves(csv_file)

