#!/usr/bin/env python3
"""
后台管理监控程序 - Web界面
提供价差曲线图展示，支持选择标的和时间范围
"""
import os
import sys
import sqlite3
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Tuple
import pandas as pd
import numpy as np
from flask import Flask, render_template, request, jsonify
import pytz

# 添加项目根目录到路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

# 设置Flask应用的模板目录
template_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates')
app = Flask(__name__, template_folder=template_dir)

# 数据库路径
DB_DIR = os.path.join(project_root, "logs")
EXCHANGE_BBO_DB = os.path.join(DB_DIR, "exchange_bbo.db")
ARB_INFO_DB = os.path.join(DB_DIR, "arb_info.db")

# 交易所数据库映射
EXCHANGE_DB_MAP = {
    'edgex': EXCHANGE_BBO_DB,
    'lighter': EXCHANGE_BBO_DB,
    'backpack': EXCHANGE_BBO_DB
}

# 定义 UTC+8 时区（数据库存储的时间是UTC+8，但没有时区标记）
TZ_UTC8 = pytz.timezone('Asia/Shanghai')


def get_available_tickers() -> List[str]:
    """获取数据库中可用的标的列表"""
    tickers = set()
    
    db_path = EXCHANGE_BBO_DB
    if not os.path.exists(db_path):
        return []
    for exchange in EXCHANGE_DB_MAP:
        try:
            conn = sqlite3.connect(db_path, timeout=10.0)
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT symbol FROM bbo_data WHERE exchange = ?", (exchange,))
            exchange_tickers = [row[0] for row in cursor.fetchall()]
            tickers.update(exchange_tickers)
            conn.close()
        except Exception as e:
            print(f"Error reading {exchange} database: {e}")
    
    return sorted(list(tickers))


def query_bbo_data(exchange: str, ticker: str, start_time: Optional[str] = None, 
                   end_time: Optional[str] = None) -> pd.DataFrame:
    """
    从数据库查询BBO数据
    
    Args:
        exchange: 'edgex', 'lighter' 或 'backpack'
        ticker: 交易对符号
        start_time: 开始时间（ISO格式字符串）
        end_time: 结束时间（ISO格式字符串）
    
    Returns:
        DataFrame with columns: timestamp, best_bid, best_ask, best_bid_size, best_ask_size
    """
    exchange_lower = exchange.lower()
    if exchange_lower not in EXCHANGE_DB_MAP:
        print(f"Unknown exchange: {exchange}")
        return pd.DataFrame()
    
    db_path = EXCHANGE_DB_MAP[exchange_lower]
    
    try:
        conn = sqlite3.connect(db_path, timeout=10.0)
        
        query = (
            "SELECT timestamp, best_bid, best_ask, best_bid_size, best_ask_size "
            "FROM bbo_data WHERE exchange = ? AND symbol = ?"
        )
        params = [exchange_lower, ticker]
        
        if start_time:
            query += " AND timestamp >= ?"
            params.append(start_time)
        
        if end_time:
            query += " AND timestamp <= ?"
            params.append(end_time)
        
        query += " ORDER BY timestamp ASC"
        
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        
        # 转换时间戳
        if not df.empty:
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
            df = df.dropna(subset=['timestamp'])
            # 转换价格列为数值类型
            df['best_bid'] = pd.to_numeric(df['best_bid'], errors='coerce')
            df['best_ask'] = pd.to_numeric(df['best_ask'], errors='coerce')
        
        return df
    except Exception as e:
        print(f"Error querying {exchange} database: {e}")
        return pd.DataFrame()


def merge_and_resample_data(df1: pd.DataFrame, df2: pd.DataFrame, 
                            exchange1: str, exchange2: str,
                            window_seconds: int = 30) -> pd.DataFrame:
    """
    合并两个交易所的数据，使用时间对齐（前向填充）确保数据连续性
    
    Args:
        df1: 第一个交易所的BBO数据
        df2: 第二个交易所的BBO数据
        exchange1: 第一个交易所名称
        exchange2: 第二个交易所名称
        window_seconds: 时间窗口大小（秒），用于时间对齐的容差
    
    Returns:
        合并后的DataFrame，包含时间对齐的数据
    """
    if df1.empty and df2.empty:
        return pd.DataFrame()
    
    # 设置时间戳为索引并转换为datetime
    if not df1.empty:
        df1 = df1.copy()
        df1['timestamp'] = pd.to_datetime(df1['timestamp'], errors='coerce')
        df1 = df1.dropna(subset=['timestamp'])
        df1 = df1.set_index('timestamp')
        df1 = df1.add_prefix(f'{exchange1}_')
    if not df2.empty:
        df2 = df2.copy()
        df2['timestamp'] = pd.to_datetime(df2['timestamp'], errors='coerce')
        df2 = df2.dropna(subset=['timestamp'])
        df2 = df2.set_index('timestamp')
        df2 = df2.add_prefix(f'{exchange2}_')
    
    # 合并数据（外连接，保留所有时间点）
    if not df1.empty and not df2.empty:
        merged = pd.merge(df1, df2, 
                         left_index=True, right_index=True, how='outer', sort=True)
        
        # 对价格数据进行前向填充，确保数据连续性（最多填充30秒内的数据）
        price_cols = [col for col in merged.columns if 'best_bid' in col or 'best_ask' in col]
        for col in price_cols:
            # 使用前向填充，但限制在30秒内
            merged[col] = merged[col].ffill(limit=3)  # limit=3表示最多填充3个时间点（约30秒）
    elif not df1.empty:
        merged = df1
    elif not df2.empty:
        merged = df2
    else:
        return pd.DataFrame()
    
    # 重置索引，使timestamp成为列
    merged = merged.reset_index()
    
    return merged


def calculate_spreads(df: pd.DataFrame, exchange1: str, exchange2: str) -> pd.DataFrame:
    """
    计算价差
    
    Args:
        df: 包含两个交易所BBO数据的DataFrame
        exchange1: 第一个交易所名称
        exchange2: 第二个交易所名称
    
    Returns:
        添加了价差列的DataFrame
    """
    df = df.copy()
    
    # 获取列名前缀
    prefix1 = f'{exchange1}_'
    prefix2 = f'{exchange2}_'
    
    # 初始化价差列为NaN
    df['spread_1'] = np.nan
    df['spread_2'] = np.nan
    
    # 计算价差1: (exchange2_best_bid - exchange1_best_ask) / exchange2_best_bid
    bid_col2 = f'{prefix2}best_bid'
    ask_col1 = f'{prefix1}best_ask'
    
    # 检查所需的列是否存在
    if bid_col2 in df.columns and ask_col1 in df.columns:
        mask1 = (df[bid_col2].notna() & 
                 df[ask_col1].notna() & 
                 (df[bid_col2] != 0))
        if mask1.any():
            df.loc[mask1, 'spread_1'] = (
                (df.loc[mask1, bid_col2] - df.loc[mask1, ask_col1]) / df.loc[mask1, ask_col1]
            )
    
    # 计算价差2: (exchange1_best_bid - exchange2_best_ask) / exchange1_best_bid
    bid_col1 = f'{prefix1}best_bid'
    ask_col2 = f'{prefix2}best_ask'
    
    # 检查所需的列是否存在
    if bid_col1 in df.columns and ask_col2 in df.columns:
        mask2 = (df[bid_col1].notna() & 
                 df[ask_col2].notna() & 
                 (df[bid_col1] != 0))
        if mask2.any():
            df.loc[mask2, 'spread_2'] = (
                (df.loc[mask2, bid_col1] - df.loc[mask2, ask_col2]) / df.loc[mask2, ask_col2]
           )
    
    return df


def query_spread_signals(exchange_pair: str, symbol: str, start_time: Optional[str] = None,
                         end_time: Optional[str] = None) -> List[Dict]:
    """
    查询价差信号数据
    
    Args:
        exchange_pair: 交易所对，如 'edgex-lighter'
        symbol: 交易对符号
        start_time: 开始时间（ISO格式字符串）
        end_time: 结束时间（ISO格式字符串）
    
    Returns:
        价差信号列表，每个信号包含 timestamp, spread_pct, direction 等字段
    """
    # 从第一个交易所的数据库查询（spread_signal表在每个数据库中都有）
    # 提取第一个交易所名称
    first_exchange = exchange_pair.split('-')[0].lower() if '-' in exchange_pair else exchange_pair.lower()
    
    if first_exchange not in EXCHANGE_DB_MAP:
        return []
    
    db_path = EXCHANGE_DB_MAP[first_exchange]
    
    try:
        if not os.path.exists(db_path):
            return []
        
        conn = sqlite3.connect(db_path, timeout=10.0)
        conn.row_factory = sqlite3.Row  # 返回字典格式的结果
        
        query = """
            SELECT timestamp, spread_pct, direction, expected_profit_pct, current_z
            FROM spread_signal 
            WHERE exchange_pair = ? AND symbol = ?
        """
        params = [exchange_pair, symbol]
        
        if start_time:
            query += " AND timestamp >= ?"
            params.append(start_time)
        
        if end_time:
            query += " AND timestamp <= ?"
            params.append(end_time)
        
        query += " ORDER BY timestamp ASC"
        
        cursor = conn.cursor()
        cursor.execute(query, params)
        results = cursor.fetchall()
        conn.close()
        
        # 转换为字典列表
        return [dict(row) for row in results]
    except Exception as e:
        print(f"Error querying spread signals from {first_exchange} database: {e}")
        return []




def get_arb_info_tickers() -> List[str]:
    """获取arb_info数据库中可用的标的列表"""
    try:
        if not os.path.exists(ARB_INFO_DB):
            return []
        conn = sqlite3.connect(ARB_INFO_DB, timeout=10.0)
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT symbol FROM arb_info ORDER BY symbol")
        tickers = [row[0] for row in cursor.fetchall()]
        conn.close()
        return tickers
    except Exception as e:
        print(f"Error reading arb_info database: {e}")
        return []


def query_arb_info_aggregated(symbol: Optional[str] = None, 
                               start_time: Optional[str] = None,
                               end_time: Optional[str] = None,
                               limit: int = 100) -> List[Dict]:
    """
    查询arb_info数据，按client_order_id聚合
    
    Args:
        symbol: 交易对符号（可选）
        start_time: 开始时间（ISO格式字符串）
        end_time: 结束时间（ISO格式字符串）
        limit: 返回记录数限制
    
    Returns:
        聚合后的套利记录列表
    """
    try:
        if not os.path.exists(ARB_INFO_DB):
            return []
        
        conn = sqlite3.connect(ARB_INFO_DB, timeout=10.0)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # 查询所有相关记录
        query = "SELECT * FROM arb_info WHERE client_order_id IS NOT NULL"
        params = []
        
        if symbol:
            query += " AND symbol = ?"
            params.append(symbol)
        
        if start_time:
            query += " AND timestamp >= ?"
            params.append(start_time)
        
        if end_time:
            query += " AND timestamp <= ?"
            params.append(end_time)
        
        query += " ORDER BY timestamp DESC"
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        conn.close()
        
        # 按client_order_id聚合
        aggregated = {}
        for row in rows:
            row_dict = dict(row)
            client_order_id = row_dict.get('client_order_id')
            if not client_order_id:
                continue
            
            if client_order_id not in aggregated:
                aggregated[client_order_id] = {
                    'client_order_id': client_order_id,
                    'symbol': row_dict.get('symbol'),
                    'discovery_time': None,
                    'discovery_spread_ratio': None,
                    'actual_spread_ratio': None,
                    'received_time_gap': None,
                    'edgex_filled_time_gap': None,
                    'lighter_filled_time_gap': None,
                    'edgex_direction': None,
                    'lighter_direction': None,
                }
            
            info_type = row_dict.get('info_type')
            
            if info_type == 'discovery':
                aggregated[client_order_id]['discovery_time'] = row_dict.get('timestamp')
                aggregated[client_order_id]['discovery_spread_ratio'] = row_dict.get('spread_ratio')
                aggregated[client_order_id]['actual_spread_ratio'] = row_dict.get('actual_spread_ratio')
                aggregated[client_order_id]['edgex_direction'] = row_dict.get('edgex_direction')
                aggregated[client_order_id]['lighter_direction'] = row_dict.get('lighter_direction')
            elif info_type == 'received':
                aggregated[client_order_id]['received_time_gap'] = row_dict.get('time_gap')
                # 如果discovery没有设置方向，从received获取
                if not aggregated[client_order_id]['edgex_direction']:
                    aggregated[client_order_id]['edgex_direction'] = row_dict.get('edgex_direction')
                if not aggregated[client_order_id]['lighter_direction']:
                    aggregated[client_order_id]['lighter_direction'] = row_dict.get('lighter_direction')
                # 如果没有discovery_time，使用received的时间作为参考
                if not aggregated[client_order_id]['discovery_time']:
                    aggregated[client_order_id]['discovery_time'] = row_dict.get('timestamp')
                if not aggregated[client_order_id]['discovery_spread_ratio']:
                    aggregated[client_order_id]['discovery_spread_ratio'] = row_dict.get('spread_ratio')
            elif info_type == 'edgex_filled':
                aggregated[client_order_id]['edgex_filled_time_gap'] = row_dict.get('time_gap')
            elif info_type == 'lighter_filled':
                aggregated[client_order_id]['lighter_filled_time_gap'] = row_dict.get('time_gap')
        
        # 转换为列表并按discovery_time排序
        result = list(aggregated.values())
        result.sort(key=lambda x: x.get('discovery_time') or '', reverse=True)
        
        # 限制返回数量
        return result[:limit]
        
    except Exception as e:
        print(f"Error querying arb_info database: {e}")
        import traceback
        traceback.print_exc()
        return []


@app.route('/')
def index():
    """主页面"""
    tickers = get_available_tickers()
    
    # 默认时间范围：3天前到现在
    default_end = datetime.now()
    default_start = default_end - timedelta(days=3)
    
    return render_template('monitor.html', 
                         tickers=tickers,
                         default_start=default_start.strftime('%Y-%m-%dT%H:%M'),
                         default_end=default_end.strftime('%Y-%m-%dT%H:%M'))


@app.route('/arb_info')
def arb_info_page():
    """套利信息页面"""
    tickers = get_arb_info_tickers()
    
    # 默认时间范围：1小时前到现在
    default_end = datetime.now()
    default_start = default_end - timedelta(hours=1)
    
    return render_template('arb_info.html', 
                         tickers=tickers,
                         default_start=default_start.strftime('%Y-%m-%dT%H:%M'),
                         default_end=default_end.strftime('%Y-%m-%dT%H:%M'))


@app.route('/api/arb_info', methods=['GET'])
def get_arb_info():
    """获取套利信息API"""
    symbol = request.args.get('symbol', '')
    start_time = request.args.get('start_time')
    end_time = request.args.get('end_time')
    limit = int(request.args.get('limit', 100))
    
    # 如果没有提供时间范围，使用默认值（1小时前到现在）
    if not start_time:
        end_dt = datetime.now(TZ_UTC8)
        start_dt = end_dt - timedelta(hours=1)
        start_time = start_dt.replace(tzinfo=None).isoformat()
        end_time = end_dt.replace(tzinfo=None).isoformat()
    else:
        # 前端发送的是UTC时间（ISO格式带Z），需要转换为UTC+8时间
        try:
            if start_time.endswith('Z'):
                dt_utc = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                dt_utc8 = dt_utc.astimezone(TZ_UTC8)
                start_time = dt_utc8.replace(tzinfo=None).isoformat()
            elif '+' in start_time or start_time.count('-') > 2:
                dt = datetime.fromisoformat(start_time)
                dt_utc8 = dt.astimezone(TZ_UTC8)
                start_time = dt_utc8.replace(tzinfo=None).isoformat()
        except Exception as e:
            print(f"Error parsing start_time {start_time}: {e}")
        
        try:
            if end_time and end_time.endswith('Z'):
                dt_utc = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
                dt_utc8 = dt_utc.astimezone(TZ_UTC8)
                end_time = dt_utc8.replace(tzinfo=None).isoformat()
            elif end_time and ('+' in end_time or end_time.count('-') > 2):
                dt = datetime.fromisoformat(end_time)
                dt_utc8 = dt.astimezone(TZ_UTC8)
                end_time = dt_utc8.replace(tzinfo=None).isoformat()
        except Exception as e:
            print(f"Error parsing end_time {end_time}: {e}")
    
    # 查询数据
    data = query_arb_info_aggregated(
        symbol=symbol if symbol else None,
        start_time=start_time,
        end_time=end_time,
        limit=limit
    )
    
    return jsonify({
        'success': True,
        'data': data,
        'count': len(data)
    })


@app.route('/api/spread_data', methods=['GET'])
def get_spread_data():
    """获取价差数据API"""
    ticker = request.args.get('ticker', 'BTC')
    exchange1 = request.args.get('exchange1', 'edgex')
    exchange2 = request.args.get('exchange2', 'lighter')
    start_time = request.args.get('start_time')
    end_time = request.args.get('end_time')
    window_seconds = int(request.args.get('window_seconds', 30))
    
    # 验证交易所参数
    if exchange1 not in EXCHANGE_DB_MAP or exchange2 not in EXCHANGE_DB_MAP:
        return jsonify({
            'success': False,
            'message': f'Invalid exchange. Available: {", ".join(EXCHANGE_DB_MAP.keys())}'
        })
    
    if exchange1 == exchange2:
        return jsonify({
            'success': False,
            'message': 'Please select two different exchanges'
        })
    
    # 如果没有提供时间范围，使用默认值（3天前到现在）
    if not start_time:
        end_dt = datetime.now(TZ_UTC8)
        start_dt = end_dt - timedelta(days=3)
        # 数据库存储的时间格式是ISO格式但没有时区标记（实际是UTC+8）
        start_time = start_dt.replace(tzinfo=None).isoformat()
        end_time = end_dt.replace(tzinfo=None).isoformat()
    else:
        # 前端发送的是UTC时间（ISO格式带Z），需要转换为UTC+8时间（去掉时区标记）
        try:
            # 解析ISO格式时间（可能带Z表示UTC）
            if start_time.endswith('Z'):
                # UTC时间，转换为UTC+8
                dt_utc = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                dt_utc8 = dt_utc.astimezone(TZ_UTC8)
                start_time = dt_utc8.replace(tzinfo=None).isoformat()
            elif '+' in start_time or start_time.count('-') > 2:
                # 带时区信息的时间，转换为UTC+8
                dt = datetime.fromisoformat(start_time)
                dt_utc8 = dt.astimezone(TZ_UTC8)
                start_time = dt_utc8.replace(tzinfo=None).isoformat()
            # 如果没有时区信息，假设已经是UTC+8格式，直接使用
        except Exception as e:
            print(f"Error parsing start_time {start_time}: {e}")
        
        try:
            if end_time.endswith('Z'):
                # UTC时间，转换为UTC+8
                dt_utc = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
                dt_utc8 = dt_utc.astimezone(TZ_UTC8)
                end_time = dt_utc8.replace(tzinfo=None).isoformat()
            elif '+' in end_time or end_time.count('-') > 2:
                # 带时区信息的时间，转换为UTC+8
                dt = datetime.fromisoformat(end_time)
                dt_utc8 = dt.astimezone(TZ_UTC8)
                end_time = dt_utc8.replace(tzinfo=None).isoformat()
            # 如果没有时区信息，假设已经是UTC+8格式，直接使用
        except Exception as e:
            print(f"Error parsing end_time {end_time}: {e}")
    
    # 查询数据
    df1 = query_bbo_data(exchange1, ticker, start_time, end_time)
    df2 = query_bbo_data(exchange2, ticker, start_time, end_time)
    
    # 合并和重采样
    merged_df = merge_and_resample_data(df1, df2, exchange1, exchange2, window_seconds)
    
    if merged_df.empty:
        return jsonify({
            'success': False,
            'message': 'No data available for the selected time range'
        })
    
    # 计算价差
    spread_df = calculate_spreads(merged_df, exchange1, exchange2)
    
    # 查询价差信号（交易所对格式固定为 exchange1-exchange2）
    exchange_pair = f'{exchange1}-{exchange2}'
    spread_signals = query_spread_signals(exchange_pair, ticker, start_time, end_time)
    
    # 准备返回数据 - 转换为JSON格式供前端Chart.js使用
    # 使用合并后的时间戳用于价差图表
    timestamps = spread_df['timestamp'].dt.strftime('%Y-%m-%dT%H:%M:%S').tolist()
    
    # 获取价格数据列名
    prefix1 = f'{exchange1}_'
    prefix2 = f'{exchange2}_'
    
    # 分别处理每个交易所的原始数据，避免因时间戳不一致导致数据丢失
    # 为每个交易所单独准备时间戳和价格数据
    if not df1.empty:
        df1_timestamps = pd.to_datetime(df1['timestamp'], errors='coerce')
        exchange1_timestamps = df1_timestamps.dt.strftime('%Y-%m-%dT%H:%M:%S').tolist()
        exchange1_bid_list = df1['best_bid'].replace({np.nan: None}).tolist()
        exchange1_ask_list = df1['best_ask'].replace({np.nan: None}).tolist()
    else:
        exchange1_timestamps = []
        exchange1_bid_list = []
        exchange1_ask_list = []
    
    if not df2.empty:
        df2_timestamps = pd.to_datetime(df2['timestamp'], errors='coerce')
        exchange2_timestamps = df2_timestamps.dt.strftime('%Y-%m-%dT%H:%M:%S').tolist()
        exchange2_bid_list = df2['best_bid'].replace({np.nan: None}).tolist()
        exchange2_ask_list = df2['best_ask'].replace({np.nan: None}).tolist()
    else:
        exchange2_timestamps = []
        exchange2_bid_list = []
        exchange2_ask_list = []
    
    result = {
        'success': True,
        'ticker': ticker,
        'exchange1': exchange1,
        'exchange2': exchange2,
        'data_points': len(spread_df),
        'timestamps': timestamps,  # 用于价差图表
        'spread_1': spread_df['spread_1'].replace({np.nan: None}).tolist() if 'spread_1' in spread_df.columns else [],
        'spread_2': spread_df['spread_2'].replace({np.nan: None}).tolist() if 'spread_2' in spread_df.columns else [],
        # 分别返回每个交易所的原始数据（包含独立的时间戳）
        'exchange1_timestamps': exchange1_timestamps,
        'exchange1_bid': exchange1_bid_list,
        'exchange1_ask': exchange1_ask_list,
        'exchange2_timestamps': exchange2_timestamps,
        'exchange2_bid': exchange2_bid_list,
        'exchange2_ask': exchange2_ask_list,
        # 价差信号数据
        'spread_signals': spread_signals
    }
    
    # 添加统计信息
    if 'spread_1' in spread_df.columns:
        spread1_data = spread_df['spread_1'].dropna()
        if not spread1_data.empty:
            result['spread_1_stats'] = {
                'mean': float(spread1_data.mean() * 100),
                'median': float(spread1_data.median() * 100),
                'max': float(spread1_data.max() * 100),
                'min': float(spread1_data.min() * 100),
                'std': float(spread1_data.std() * 100)
            }
    
    if 'spread_2' in spread_df.columns:
        spread2_data = spread_df['spread_2'].dropna()
        if not spread2_data.empty:
            result['spread_2_stats'] = {
                'mean': float(spread2_data.mean() * 100),
                'median': float(spread2_data.median() * 100),
                'max': float(spread2_data.max() * 100),
                'min': float(spread2_data.min() * 100),
                'std': float(spread2_data.std() * 100)
            }
    
    return jsonify(result)


if __name__ == '__main__':
    # 确保模板目录存在
    os.makedirs(template_dir, exist_ok=True)
    
    print(f"启动监控程序...")
    print(f"访问地址: http://localhost:")
    print(f"模板目录: {template_dir}")
    
    app.run(host='0.0.0.0', port=8700, debug=False)
