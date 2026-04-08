#!/usr/bin/env python3
"""
导出交易所 BBO 数据库中 bbo_data 表最近 N 小时的数据到 CSV 文件
支持 edgex_bbo.db、lighter_bbo.db 等
"""
import sqlite3
import csv
import os
import sys
import shutil
from datetime import datetime, timedelta
import pytz


def _has_exchange_column(cursor: sqlite3.Cursor) -> bool:
    cursor.execute("PRAGMA table_info(bbo_data)")
    return any(row[1] == "exchange" for row in cursor.fetchall())


def export_bbo_data_to_csv(db_path: str, output_csv: str = None, hours: int = 24):
    """
    从数据库导出最近 N 小时的 BBO 数据到 CSV 文件
    
    Args:
        db_path: 数据库文件路径
        output_csv: 输出 CSV 文件路径，如果为 None 则自动生成
        hours: 导出最近几小时的数据，默认 24 小时
    """
    # 将相对路径转换为绝对路径
    if not os.path.isabs(db_path):
        db_path = os.path.abspath(db_path)
    
    # 检查数据库文件是否存在
    if not os.path.exists(db_path):
        print(f"错误: 数据库文件不存在: {db_path}")
        sys.exit(1)
    
    # 如果没有指定输出文件，自动生成文件名
    if output_csv is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        # 从数据库文件名提取交易所名称
        db_name = os.path.basename(db_path)
        exchange_name = db_name.replace('_bbo.db', '').replace('.db', '')
        # 输出文件保存到数据库所在目录
        db_dir = os.path.dirname(os.path.abspath(db_path))
        output_csv = os.path.join(db_dir, f"{exchange_name}_bbo_data_{hours}hours_{timestamp}.csv")
    
    # 计算 N 小时前的时间戳（ISO 格式）
    # 使用 UTC+8 时区，与数据库存储格式一致
    TZ_UTC8 = pytz.timezone('Asia/Shanghai')
    end_time = datetime.now(TZ_UTC8)
    start_time = end_time - timedelta(hours=hours)
    # 格式化为秒级别（去掉微秒和时区信息），与数据库存储格式一致
    start_time_str = start_time.replace(microsecond=0, tzinfo=None).isoformat()
    end_time_str = end_time.replace(microsecond=0, tzinfo=None).isoformat()
    
    print(f"正在从数据库导出数据...")
    print(f"数据库路径: {db_path}")
    print(f"时间范围: {start_time_str} 至 {end_time_str}")
    print(f"输出文件: {output_csv}")
    
    try:
        # 先尝试执行 WAL 检查点（如果数据库正在被其他进程使用）
        # 这需要在连接之前处理 WAL 文件
        wal_path = f"{db_path}-wal"
        if os.path.exists(wal_path):
            try:
                temp_conn = sqlite3.connect(db_path, timeout=10.0)
                temp_conn.execute('PRAGMA wal_checkpoint(TRUNCATE)')
                temp_conn.close()
            except:
                pass  # 如果失败，继续尝试正常连接
        
        # 连接数据库，使用正常模式
        # 由于 sqlite3 命令行可以正常访问，说明数据库文件本身没问题
        conn = sqlite3.connect(db_path, timeout=30.0)
        
        conn.row_factory = sqlite3.Row  # 返回字典格式的结果
        cursor = conn.cursor()
        has_exchange_column = _has_exchange_column(cursor)

        selected_exchange = "exchange," if has_exchange_column else ""
        
        # 先尝试使用索引查询（快速方式）
        rows = []
        use_fallback = False
        
        try:
            query = f"""
                SELECT 
                    id,
                    {selected_exchange}
                    symbol,
                    best_bid,
                    best_bid_size,
                    best_ask,
                    best_ask_size,
                    price_timestamp,
                    timestamp,
                    created_at
                FROM bbo_data
                WHERE (timestamp >= ? OR (timestamp IS NULL AND created_at >= ?))
                ORDER BY COALESCE(timestamp, created_at) ASC
            """
            cursor.execute(query, (start_time_str, start_time_str))
            rows = cursor.fetchall()
            print(f"使用索引查询成功，找到 {len(rows)} 条记录")
        except sqlite3.DatabaseError as e:
            if "malformed" in str(e).lower():
                print("警告: 索引查询失败（数据库可能有索引损坏），切换到分批读取模式...")
                use_fallback = True
            else:
                raise
        
        # 如果索引查询失败，使用分批读取并在 Python 中过滤
        if use_fallback:
            batch_size = 10000
            offset = 0
            rows = []
            
            while True:
                # 使用简单的查询，不使用 WHERE 和 ORDER BY
                query = f"""
                    SELECT 
                        id,
                        {selected_exchange}
                        symbol,
                        best_bid,
                        best_bid_size,
                        best_ask,
                        best_ask_size,
                        price_timestamp,
                        timestamp,
                        created_at
                    FROM bbo_data
                    LIMIT {batch_size} OFFSET {offset}
                """
                
                try:
                    cursor.execute(query)
                    batch_rows = cursor.fetchall()
                    
                    if not batch_rows:
                        break
                    
                    # 在 Python 中过滤数据
                    for row in batch_rows:
                        row_timestamp = row['timestamp']
                        row_created_at = row['created_at']
                        
                        # 检查是否在时间范围内
                        if row_timestamp and row_timestamp >= start_time_str:
                            rows.append(row)
                        elif not row_timestamp and row_created_at:
                            # 将 created_at 转换为 ISO 格式进行比较
                            try:
                                if isinstance(row_created_at, str):
                                    # created_at 可能是 'YYYY-MM-DD HH:MM:SS' 格式
                                    created_at_dt = datetime.strptime(row_created_at, '%Y-%m-%d %H:%M:%S')
                                    # 格式化为秒级别（去掉微秒），与 start_time_str 格式一致
                                    created_at_str = created_at_dt.replace(microsecond=0).isoformat()
                                    if created_at_str >= start_time_str:
                                        rows.append(row)
                            except:
                                pass
                    
                    offset += batch_size
                    if offset % 100000 == 0:
                        print(f"已处理 {offset} 条记录，找到 {len(rows)} 条符合条件的数据...")
                    
                except sqlite3.DatabaseError as e:
                    if "malformed" in str(e).lower():
                        print(f"警告: 在偏移量 {offset} 处遇到损坏的数据，跳过该批次...")
                        offset += batch_size
                        continue
                    else:
                        raise
            
            # 按时间排序
            def get_sort_key(row):
                if row['timestamp']:
                    return row['timestamp']
                elif row['created_at']:
                    try:
                        dt = datetime.strptime(row['created_at'], '%Y-%m-%d %H:%M:%S')
                        # 格式化为秒级别（去掉微秒），与 timestamp 格式一致
                        return dt.replace(microsecond=0).isoformat()
                    except:
                        return ''
                return ''
            
            rows.sort(key=get_sort_key)
            print(f"分批读取完成，找到 {len(rows)} 条符合条件的数据")
        
        if not rows:
            print(f"警告: 在指定时间范围内没有找到数据")
            conn.close()
            return
        
        print(f"找到 {len(rows)} 条记录")
        
        # 写入 CSV 文件
        with open(output_csv, 'w', newline='', encoding='utf-8') as csvfile:
            # 定义 CSV 列名
            fieldnames = [
                'id',
                'exchange',
                'symbol',
                'best_bid',
                'best_bid_size',
                'best_ask',
                'best_ask_size',
                'price_timestamp',
                'timestamp',
                'created_at'
            ]
            
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            # 写入数据
            for row in rows:
                writer.writerow({
                    'id': row['id'],
                    'exchange': row['exchange'] if has_exchange_column else '',
                    'symbol': row['symbol'],
                    'best_bid': row['best_bid'],
                    'best_bid_size': row['best_bid_size'],
                    'best_ask': row['best_ask'],
                    'best_ask_size': row['best_ask_size'],
                    'price_timestamp': row['price_timestamp'],
                    'timestamp': row['timestamp'],
                    'created_at': row['created_at']
                })
        
        conn.close()
        print(f"✓ 成功导出 {len(rows)} 条记录到 {output_csv}")
        
    except sqlite3.DatabaseError as e:
        error_msg = str(e).lower()
        if "malformed" in error_msg or "disk image" in error_msg:
            print(f"错误: 数据库文件可能损坏: {e}")
            print("\n尝试修复数据库...")
            
            # 尝试修复：删除 WAL 和 SHM 文件
            wal_path = f"{db_path}-wal"
            shm_path = f"{db_path}-shm"
            
            try:
                if os.path.exists(wal_path):
                    print(f"删除 WAL 文件: {wal_path}")
                    os.remove(wal_path)
                if os.path.exists(shm_path):
                    print(f"删除 SHM 文件: {shm_path}")
                    os.remove(shm_path)
                print("已删除 WAL/SHM 文件，请重新运行脚本")
            except Exception as repair_error:
                print(f"修复失败: {repair_error}")
            
            print("\n提示: 如果问题仍然存在，请检查数据库文件是否完整，或尝试使用备份文件")
        else:
            print(f"数据库错误: {e}")
        sys.exit(1)
    except sqlite3.OperationalError as e:
        print(f"数据库操作错误: {e}")
        print("提示: 数据库可能正在被其他程序使用，请稍后重试")
        sys.exit(1)
    except Exception as e:
        print(f"导出失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


def main():
    """主函数"""
    # 获取脚本所在目录
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # 解析命令行参数
    import argparse
    parser = argparse.ArgumentParser(description='导出交易所 BBO 数据库中最近 N 小时的 BBO 数据到 CSV')
    parser.add_argument('--exchange', '-e', type=str, default='lighter', 
                        choices=['edgex', 'lighter', 'backpack'],
                        help='交易所名称（默认: edgex，可选: edgex, lighter, backpack）')
    parser.add_argument('--db', type=str, default=None, 
                        help='数据库文件路径（如果指定则覆盖 --exchange 参数）')
    parser.add_argument('--output', '-o', type=str, default=None, 
                        help='输出 CSV 文件路径（默认: 自动生成）')
    parser.add_argument('--hours', type=int, default=18, 
                        help='导出最近几小时的数据（默认: 72）')
    
    args = parser.parse_args()
    
    # 确定数据库路径
    if args.db:
        db_path = args.db
    else:
        # 根据交易所名称构建数据库路径，默认使用 ../data/ 目录
        data_dir = os.path.join(os.path.dirname(script_dir), 'data')
        db_path = os.path.join(data_dir, f'{args.exchange}_bbo.db')
        db_path = os.path.abspath(os.path.normpath(db_path))
    
    # 执行导出
    export_bbo_data_to_csv(db_path, args.output, args.hours)


if __name__ == '__main__':
    main()
