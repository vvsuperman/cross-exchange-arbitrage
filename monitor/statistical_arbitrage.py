import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
from collections import deque
import logging
import json
from typing import Dict, Tuple, Optional, List
import sys
import os
import threading
# 添加项目根目录到 Python 路径，以便导入 helpers 模块
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from redis_client import RedisPriceClient
from db.record_db import BBODataDB
from helpers.help import send_webhook_alert
from csv_trade_logger import CSVTradeLogger

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('statistical_arbitrage.log')
    ]
)
logger = logging.getLogger(__name__)

def load_symbols_from_tickers(tickers_file: str = 'tickers.txt') -> List[str]:
    """
    从tickers.txt文件中读取标的列表
    
    参数:
    tickers_file: tickers.txt文件路径，默认为项目根目录下的tickers.txt
    
    返回:
    标的列表
    格式：SYMBOL 开仓数量 最大开仓数量（只读取SYMBOL，忽略后面的数量）
    例如：BTC 0.001 0.01 -> 返回 ['BTC']
    """
    symbols = []
    
    # 如果文件路径不是绝对路径，尝试从项目根目录查找
    if not os.path.isabs(tickers_file):
        # 尝试多个可能的路径
        possible_paths = [
            tickers_file,  # 当前目录
            os.path.join(os.path.dirname(os.path.dirname(__file__)), tickers_file),  # 项目根目录
            os.path.join(os.path.dirname(__file__), tickers_file),  # monitor目录
        ]
        
        for path in possible_paths:
            if os.path.exists(path):
                tickers_file = path
                break
        else:
            logger.warning(f"未找到tickers.txt文件，使用默认symbols列表")
            return ['BTC', 'ETH', 'SOL', 'BNB', 'XRP', 'ENA', 'ASTER', 'LIT', 'HYPE', 'BERA', 'PUMP', 'PAXG']
    
    try:
        with open(tickers_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                # 跳过空行和注释行
                if line and not line.startswith('#'):
                    # 只取第一列作为symbol，忽略后面的开仓数量和最大开仓数量
                    parts = line.split()
                    if len(parts) >= 1:
                        symbols.append(parts[0].upper())
        
        if not symbols:
            logger.warning(f"tickers.txt文件为空，使用默认symbols列表")
            return ['BTC', 'ETH', 'SOL', 'BNB', 'XRP', 'ENA', 'ASTER', 'LIT', 'HYPE', 'BERA', 'PUMP', 'PAXG']
        
        logger.info(f"从{tickers_file}读取了{len(symbols)}个标的: {symbols}")
        return symbols
        
    except Exception as e:
        logger.error(f"读取tickers.txt文件失败: {e}，使用默认symbols列表")
        return ['BTC', 'ETH', 'SOL', 'BNB', 'XRP', 'ENA', 'ASTER', 'LIT', 'HYPE', 'BERA', 'PUMP', 'PAXG']

class StatisticalArbitrage:
    """统计套利算法核心类（完整修正版）"""
    
    def __init__(self, config: dict = None):
        """
        初始化统计套利算法
        
        参数:
        config: 算法配置参数
        """
        # 默认配置（已修正所有参数）
        default_config = {
            # ========== 成本参数（已修正！）==========
            'edgex_fee': 0.00026,      # edgex单边手续费 0.026%
            'lighter_fee': 0.0,        # lighter单边手续费 0%
            'profit_target': 0.0001,   # 每笔交易目标净利润 0.01%
            
            # 总成本 = 双边手续费 + 盈利目标
            # 完整交易循环：edgex买(0.026%) + edgex卖(0.026%) + 利润(0.01%)
            'total_requirement': 0.0005,  # 0.05%
            
            # ========== 统计参数 ==========
            'Z_open': 2,      # 开仓Z阈值（95%分位数）
            'Z_close': 0.5,     # 平仓Z阈值
            #'Z_stop_loss': 3.0, # 止损Z阈值，先不用
            
            # ========== 窗口参数 ==========
            'window_size': 1000,      # 滚动窗口大小（秒级数据对应1000个点）
            'min_data_points': 100,  # 最小数据点数才开始交易
            
            # ========== 交易参数 ==========
            'symbols': None,  # 如果为None，将从tickers.txt读取
            'tickers_file': 'tickers.txt',  # tickers文件路径
            'update_interval': 1,    # 更新间隔（秒）
            'max_hold_time': 600,    # 最大持仓时间（秒）
            'max_half_life_seconds': 180,  # 均值回复半衰期上限（秒）
            'timeout_cooldown_seconds': 120,  # 超时平仓后的冷却时间（秒）
            'position_size_usd': 1000,  # 每笔交易规模（USD）
            
            # ========== 风险控制 ==========
            'max_positions_per_symbol': 10,      # 每个交易对最大持仓数
            'daily_loss_limit_pct': 0.05,       # 每日最大亏损百分比
            'max_drawdown_pct': 0.10,           # 最大回撤百分比
            
            # ========== 固定基准Z-score参数 ==========
            'use_fixed_baseline_zscore': True,  # 是否使用固定基准Z-score（平仓时）
        }
        
        # 合并配置
        self.config = {**default_config, **(config or {})}
        
        # 如果symbols未指定，从tickers.txt读取
        if self.config['symbols'] is None:
            tickers_file = self.config.get('tickers_file', 'tickers.txt')
            self.config['symbols'] = load_symbols_from_tickers(tickers_file)
        
        # 初始化Redis客户端
        try:
            self.redis_client = RedisPriceClient()
            logger.info("Redis客户端初始化成功")
        except Exception as e:
            logger.error(f"Redis客户端初始化失败: {e}")
            raise
        
        # 初始化数据库（用于存储价差信号，使用第一个交易对作为默认ticker）
        try:
            default_symbol = self.config['symbols'][0] if self.config['symbols'] else 'BTC'
            self.signal_db = BBODataDB(exchange='edgex', ticker=default_symbol)
            logger.info("信号数据库初始化成功")
        except Exception as e:
            logger.error(f"信号数据库初始化失败: {e}")
            # 不抛出异常，允许在没有数据库的情况下运行
            self.signal_db = None
        
        # 初始化数据结构
        self.spread_data = {}      # 存储各交易对的价差数据
        self.statistics = {}       # 存储各交易对的统计量
        self.positions = {}        # 存储持仓信息
        self.cooldown_until = {}   # 超时平仓冷却时间
        self.trade_history = []    # 交易历史记录
        self.daily_pnl = 0.0       # 当日盈亏
        self.equity_curve = []     # 权益曲线
        
        # 初始化每个交易对
        self._init_symbols()
        
        # 使用历史数据建立初始数据集
        self._initialize_history_data()
        
        # 初始化CSV交易日志记录器
        csv_file = self.config.get('csv_log_file', 'logs/stat_log.csv')
        # 如果路径不是绝对路径，则相对于项目根目录
        if not os.path.isabs(csv_file):
            csv_file = os.path.join(project_root, csv_file)
        self.csv_logger = CSVTradeLogger(csv_file=csv_file)
        
        logger.info(f"统计套利算法初始化完成")
        logger.info(f"监控交易对: {self.config['symbols']}")
        logger.info(f"总成本要求: {self.config['total_requirement']*100:.4f}%")
        logger.info(f"Z阈值: 开仓={self.config['Z_open']}, 平仓={self.config['Z_close']}")
    
    def _init_symbols(self):
        """初始化每个交易对的数据结构"""
        for symbol in self.config['symbols']:
            # 使用deque实现滚动窗口
            self.spread_data[symbol] = {
                'timestamps': deque(maxlen=self.config['window_size']),
                'log_spread1': deque(maxlen=self.config['window_size']),  # edgex买，lighter卖
                'log_spread2': deque(maxlen=self.config['window_size']),  # lighter买，edgex卖
                'spread1_pct': deque(maxlen=self.config['window_size']),  # 百分比形式
                'spread2_pct': deque(maxlen=self.config['window_size']),  # 百分比形式
            }
            
            # 初始化统计量
            self.statistics[symbol] = {
                'mean1': 0.0,      # 对数价差1的均值
                'std1': 0.0,       # 对数价差1的标准差
                'mean2': 0.0,      # 对数价差2的均值
                'std2': 0.0,       # 对数价差2的标准差
                'count': 0,        # 数据点数
                'last_update': None,
            }
            
            # 初始化持仓
            self.positions[symbol] = {
                'direction': None,           # 'long_spread1' 或 'long_spread2'
                'entry_time': None,          # 入场时间
                'entry_log_spread': None,    # 入场对数价差
                'entry_spread_pct': None,    # 入场百分比价差
                'entry_zscore': None,        # 入场Z-score
                'entry_price_edgex': None,   # edgex入场价格
                'entry_price_lighter': None, # lighter入场价格
                'entry_client_order_id': None,  # 开仓时的 client_order_id
                'position_size': 0,          # 持仓数量
                'pnl': 0.0,                  # 当前浮动盈亏
                # 固定基准Z-score相关字段
                'baseline_mean1': None,      # 基准均值1（开仓时记录）
                'baseline_std1': None,       # 基准标准差1
                'baseline_mean2': None,      # 基准均值2
                'baseline_std2': None,       # 基准标准差2
            }
            
            # 初始化冷却时间
            self.cooldown_until[symbol] = None
    
    def _initialize_history_data(self):
        """使用历史数据建立初始数据集"""
        logger.info("开始初始化历史数据...")
        limit = min(self.config['window_size'], 1000)  # 获取足够的数据点
        
        for symbol in self.config['symbols']:
            try:
                # 获取历史数据
                edgex_data_list, lighter_data_list = self.fetch_window_data(symbol, limit)
                
                if not edgex_data_list or not lighter_data_list:
                    logger.warning(f"{symbol}: 历史数据不足，跳过初始化")
                    continue
                
                # 匹配数据并计算价差（按索引匹配，因为数据是按时间排序的）
                matched_count = 0
                min_len = min(len(edgex_data_list), len(lighter_data_list))
                
                for i in range(min_len):
                    edgex_data = edgex_data_list[i]
                    lighter_data = lighter_data_list[i]
                    
                    # 计算价差
                    log_spread1, log_spread2, pct_spread1, pct_spread2 = self.calculate_log_spread(
                        edgex_data, lighter_data
                    )
                    
                    if log_spread1 is None:
                        continue
                    
                    # 使用 edgex 的时间戳
                    timestamp = edgex_data.get('timestamp')
                    if timestamp is None:
                        timestamp = datetime.now()
                    
                    # 更新统计量
                    self.update_statistics(symbol, log_spread1, log_spread2, pct_spread1, pct_spread2, timestamp)
                    matched_count += 1
                
                logger.info(f"{symbol}: 初始化了 {matched_count} 条历史数据")
                
            except Exception as e:
                logger.error(f"{symbol}: 初始化历史数据失败: {e}", exc_info=True)
        
        logger.info("历史数据初始化完成")

    def fetch_window_data(self, symbol: str, limit: int = 1000) -> Tuple[List[dict], List[dict]]:
        """
        获取历史数据（从Redis获取）
        
        参数:
        symbol: 交易对符号
        limit: 获取的数据条数，默认1000条
        
        返回:
        (edgex_data_list, lighter_data_list)
        """
        try:
            # 从Redis获取历史数据
            edgex_history = self.redis_client.get_history_bbo('edgex', symbol, limit)
            lighter_history = self.redis_client.get_history_bbo('lighter', symbol, limit)
            
            # 转换为需要的格式
            edgex_data = []
            lighter_data = []
            
            for item in edgex_history:
                try:
                    edgex_data.append({
                        'symbol': symbol,
                        'best_bid': float(item['best_bid']) if item.get('best_bid') else 0.0,
                        'best_ask': float(item['best_ask']) if item.get('best_ask') else 0.0,
                        'timestamp': item.get('timestamp')
                    })
                except Exception as e:
                    logger.debug(f"转换edgex历史数据失败: {e}")
                    continue
            
            for item in lighter_history:
                try:
                    lighter_data.append({
                        'symbol': symbol,
                        'best_bid': float(item['best_bid']) if item.get('best_bid') else 0.0,
                        'best_ask': float(item['best_ask']) if item.get('best_ask') else 0.0,
                        'timestamp': item.get('timestamp')
                    })
                except Exception as e:
                    logger.debug(f"转换lighter历史数据失败: {e}")
                    continue
            
            return edgex_data, lighter_data
            
        except Exception as e:
            logger.error(f"获取{symbol}历史数据失败: {e}", exc_info=True)
            return [], []
    
    def calculate_log_spread(self, edgex_data: dict, lighter_data: dict) -> Tuple[float, float, float, float]:
        """
        计算两个方向的对数价差和百分比价差
        
        返回:
        (log_spread1, log_spread2, pct_spread1, pct_spread2)
        
        log_spread1: log(lighter_bid) - log(edgex_ask)  # edgex买，lighter卖
        log_spread2: log(edgex_bid) - log(lighter_ask)  # lighter买，edgex卖
        """
        if not edgex_data or not lighter_data:
            return None, None, None, None
        
        try:
            # 获取价格
            edgex_ask = float(edgex_data['best_ask'])
            edgex_bid = float(edgex_data['best_bid'])
            lighter_ask = float(lighter_data['best_ask'])
            lighter_bid = float(lighter_data['best_bid'])
            
            # 确保价格为正
            if edgex_ask <= 0 or lighter_bid <= 0 or edgex_bid <= 0 or lighter_ask <= 0:
                return None, None, None, None
            
            # 计算对数价差
            log_spread1 = np.log(lighter_bid) - np.log(edgex_ask)
            log_spread2 = np.log(edgex_bid) - np.log(lighter_ask)
            
            # 计算百分比价差
            pct_spread1 = (lighter_bid - edgex_ask) / edgex_ask
            pct_spread2 = (edgex_bid - lighter_ask) / lighter_ask
            
            return log_spread1, log_spread2, pct_spread1, pct_spread2
            
        except Exception as e:
            logger.error(f"计算价差失败: {e}")
            return None, None, None, None
    
    def update_statistics(self, symbol: str, log_spread1: float, log_spread2: float, 
                         pct_spread1: float, pct_spread2: float, timestamp):
        """
        更新滚动统计量
        对于固定窗口，当窗口满后重新计算所有窗口内数据的统计量
        窗口未满时使用Welford在线算法进行增量更新
        """
        if log_spread1 is None or log_spread2 is None:
            return
        
        # 将数据添加到滚动窗口
        self.spread_data[symbol]['timestamps'].append(timestamp)
        self.spread_data[symbol]['log_spread1'].append(log_spread1)
        self.spread_data[symbol]['log_spread2'].append(log_spread2)
        self.spread_data[symbol]['spread1_pct'].append(pct_spread1)
        self.spread_data[symbol]['spread2_pct'].append(pct_spread2)
        
        # 获取当前窗口大小
        n = len(self.spread_data[symbol]['log_spread1'])
        window_size = self.config['window_size']
        
        # 如果窗口满了，重新计算所有窗口内数据的统计量
        if n >= window_size:
            # 重新计算所有窗口内数据的统计量（固定窗口）
            log_spread1_list = list(self.spread_data[symbol]['log_spread1'])
            log_spread2_list = list(self.spread_data[symbol]['log_spread2'])
            
            # 计算均值
            mean1 = np.mean(log_spread1_list)
            mean2 = np.mean(log_spread2_list)
            
            # 计算标准差（使用样本标准差，ddof=1）
            if n > 1:
                std1 = np.std(log_spread1_list, ddof=1)  # ddof=1 表示样本标准差
                std2 = np.std(log_spread2_list, ddof=1)
                m2_1 = std1 ** 2 * (n - 1)  # 转换为 m2 形式（用于兼容性）
                m2_2 = std2 ** 2 * (n - 1)
            else:
                std1 = 0.0
                std2 = 0.0
                m2_1 = 0.0
                m2_2 = 0.0
            
            # 更新统计量
            self.statistics[symbol]['mean1'] = mean1
            self.statistics[symbol]['m2_1'] = m2_1
            self.statistics[symbol]['mean2'] = mean2
            self.statistics[symbol]['m2_2'] = m2_2
            self.statistics[symbol]['std1'] = std1
            self.statistics[symbol]['std2'] = std2
        else:
            # 窗口未满，使用增量更新（Welford算法）
            if n == 1:
                # 初始化
                self.statistics[symbol]['mean1'] = log_spread1
                self.statistics[symbol]['m2_1'] = 0.0
                self.statistics[symbol]['mean2'] = log_spread2
                self.statistics[symbol]['m2_2'] = 0.0
                self.statistics[symbol]['std1'] = 0.0
                self.statistics[symbol]['std2'] = 0.0
            else:
                # 更新方向1
                old_mean1 = self.statistics[symbol]['mean1']
                delta1 = log_spread1 - old_mean1
                self.statistics[symbol]['mean1'] = old_mean1 + delta1 / n
                self.statistics[symbol]['m2_1'] += delta1 * (log_spread1 - self.statistics[symbol]['mean1'])
                
                # 更新方向2
                old_mean2 = self.statistics[symbol]['mean2']
                delta2 = log_spread2 - old_mean2
                self.statistics[symbol]['mean2'] = old_mean2 + delta2 / n
                self.statistics[symbol]['m2_2'] += delta2 * (log_spread2 - self.statistics[symbol]['mean2'])
                
                # 计算标准差
                if n > 1:
                    self.statistics[symbol]['std1'] = np.sqrt(self.statistics[symbol]['m2_1'] / (n - 1))
                    self.statistics[symbol]['std2'] = np.sqrt(self.statistics[symbol]['m2_2'] / (n - 1))
        
        self.statistics[symbol]['count'] = n
        self.statistics[symbol]['last_update'] = timestamp
    
    def calculate_zscore(self, symbol: str, log_spread1: float, log_spread2: float) -> Tuple[float, float]:
        """
        计算Z-score（基于对数价差，使用动态更新的统计量）
        """
        stats = self.statistics[symbol]
        
        if stats['std1'] > 0:
            z1 = (log_spread1 - stats['mean1']) / stats['std1']
        else:
            z1 = 0.0
            
        if stats['std2'] > 0:
            z2 = (log_spread2 - stats['mean2']) / stats['std2']
        else:
            z2 = 0.0
            
        return z1, z2
    
    def calculate_fixed_baseline_zscore(self, symbol: str, log_spread1: float, log_spread2: float) -> Tuple[float, float]:
        """
        计算固定基准Z-score（使用开仓时记录的基准统计量）
        
        如果有持仓且记录了基准统计量，使用固定基准；否则使用动态Z-score
        """
        position = self.positions[symbol]
        
        # 如果有持仓且记录了基准，使用固定基准
        if position['direction'] and position['baseline_mean1'] is not None:
            if position['baseline_std1'] > 0:
                z1 = (log_spread1 - position['baseline_mean1']) / position['baseline_std1']
            else:
                z1 = 0.0
                
            if position['baseline_std2'] > 0:
                z2 = (log_spread2 - position['baseline_mean2']) / position['baseline_std2']
            else:
                z2 = 0.0
        else:
            # 没有持仓或没有基准，使用动态Z-score
            z1, z2 = self.calculate_zscore(symbol, log_spread1, log_spread2)
        
        return z1, z2
    
    def calculate_dynamic_thresholds(self, symbol: str, direction: str = 'spread1') -> Tuple[float, float]:
        """
        计算动态阈值
        
        返回: (open_threshold_log, close_threshold_log)
        """
        stats = self.statistics[symbol]
        
        if direction == 'spread1':
            μ = stats['mean1']
            σ = stats['std1']
        else:  # spread2
            μ = stats['mean2']
            σ = stats['std2']
        
        # 动态阈值（对数形式）
        open_threshold = μ + self.config['Z_open'] * σ
        close_threshold = μ + self.config['Z_close'] * σ
        
        return open_threshold, close_threshold
    
    def estimate_half_life(self, symbol: str, direction: str) -> Optional[float]:
        """
        估算均值回复半衰期（秒）
        使用 Δspread = a + b * spread_{t-1} 的一阶回归
        """
        if direction == 'spread1':
            spread_series = self.spread_data[symbol]['log_spread1']
        else:
            spread_series = self.spread_data[symbol]['log_spread2']
        
        if len(spread_series) < self.config['min_data_points']:
            return None
        
        spread = np.array(spread_series, dtype=float)
        x = spread[:-1]
        y = spread[1:] - spread[:-1]
        
        if np.allclose(x, x[0]):
            return None
        
        try:
            slope, _intercept = np.polyfit(x, y, 1)
        except Exception:
            return None
        
        phi = 1 + slope
        if phi <= 0 or phi >= 1:
            return None
        
        try:
            half_life = -np.log(2) / np.log(phi) * self.config['update_interval']
        except Exception:
            return None
        
        if not np.isfinite(half_life) or half_life <= 0:
            return None
        
        return float(half_life)
    
    def validate_trade_opportunity(self, symbol: str, direction: str,
                                 current_log_spread: float, current_z: float) -> Tuple[bool, str, float]:
        """
        验证交易机会是否真的有利润
        
        返回: (is_valid, reason, expected_profit_pct)
        """
        stats = self.statistics[symbol]
        
        # 1. 检查是否有足够数据
        if stats['count'] < self.config['min_data_points']:
            return False, f"数据不足: {stats['count']} < {self.config['min_data_points']}", 0.0
        
        # 2. 检查价差是否为正（实际套利的前提条件）
        if direction == 'spread1':
            # spread1 = log(lighter_bid) - log(edgex_ask)
            # 只有当 lighter_bid > edgex_ask 时才能套利
            if current_log_spread <= 0:
                return False, f"价差非正，无法套利: {current_log_spread:.8f}", 0.0
            
            # 检查预期回归价差是否为正（如果均值<0，回归后可能为负）
            if stats['mean1'] < 0:
                expected_close_spread = stats['mean1'] + self.config['Z_close'] * stats['std1']
                if expected_close_spread <= 0:
                    return False, f"预期回归价差为负: {expected_close_spread:.8f}", 0.0
        else:  # spread2
            # spread2 = log(edgex_bid) - log(lighter_ask)
            # 只有当 edgex_bid > lighter_ask 时才能套利
            if current_log_spread <= 0:
                return False, f"价差非正，无法套利: {current_log_spread:.8f}", 0.0
            
            # 检查预期回归价差是否为正
            if stats['mean2'] < 0:
                expected_close_spread = stats['mean2'] + self.config['Z_close'] * stats['std2']
                if expected_close_spread <= 0:
                    return False, f"预期回归价差为负: {expected_close_spread:.8f}", 0.0
        
        # 3. 检查均值回复速度
        half_life = self.estimate_half_life(symbol, direction)
        max_half_life = self.config.get(
            'max_half_life_seconds',
            self.config['max_hold_time'] * 0.6
        )
        if half_life is None:
            return False, "价差无明显均值回复（half-life不可估计）", 0.0
        if half_life > max_half_life:
            return False, f"均值回复过慢: {half_life:.1f}s > {max_half_life:.1f}s", 0.0
        
        # 4. 计算预期利润
        Z_diff = self.config['Z_open'] - self.config['Z_close']
        σ = stats['std1'] if direction == 'spread1' else stats['std2']
        
        expected_profit_log = Z_diff * σ
        expected_profit_pct = np.exp(expected_profit_log) - 1
        
        # 5. 检查是否满足最小利润要求
        if expected_profit_pct <= self.config['total_requirement']:
            return False, f"预期利润不足: {expected_profit_pct*100:.4f}% <= {self.config['total_requirement']*100:.4f}%", expected_profit_pct
        
        # 6. 检查Z-score是否达到开仓阈值
        if current_z < self.config['Z_open']:
            return False, f"Z-score不足: {current_z:.2f} < {self.config['Z_open']}", expected_profit_pct
        
        # 所有条件满足
        return True, "机会有效", expected_profit_pct
    
    def check_trading_signals(self, symbol: str, log_spread1: float, log_spread2: float,
                            pct_spread1: float, pct_spread2: float,
                            z1: float, z2: float, edgex_data: dict, lighter_data: dict) -> List[dict]:
        """
        检查所有交易信号
        
        返回: 信号列表
        """
        signals = []
        position = self.positions[symbol]
        
        # 如果有持仓，先检查平仓信号
        if position['direction']:
            # 如果使用固定基准Z-score，重新计算Z-score
            if self.config.get('use_fixed_baseline_zscore', True):
                z1_old, z2_old = z1, z2
                z1, z2 = self.calculate_fixed_baseline_zscore(symbol, log_spread1, log_spread2)
                # 记录Z-score变化（用于调试）
                if position['direction'] == 'long_spread1' and abs(z1 - z1_old) > 0.1:
                    logger.debug(f"{symbol} 固定基准Z-score: 动态Z={z1_old:.2f} -> 固定Z={z1:.2f}")
                elif position['direction'] == 'long_spread2' and abs(z2 - z2_old) > 0.1:
                    logger.debug(f"{symbol} 固定基准Z-score: 动态Z={z2_old:.2f} -> 固定Z={z2:.2f}")
            signals.extend(self._check_close_signals(symbol, position, z1, z2, log_spread1, log_spread2))
        
        # 如果没有持仓，检查开仓信号
        if position['direction'] is None:
            cooldown_until = self.cooldown_until.get(symbol)
            if cooldown_until and datetime.now() < cooldown_until:
                return signals
            signals.extend(self._check_open_signals(symbol, log_spread1, log_spread2, pct_spread1, pct_spread2, z1, z2, edgex_data, lighter_data))
        
        return signals
    
    def _check_open_signals(self, symbol: str, log_spread1: float, log_spread2: float,
                          pct_spread1: float, pct_spread2: float,
                          z1: float, z2: float, edgex_data: dict, lighter_data: dict) -> List[dict]:
        """检查开仓信号"""
        signals = []
        
        # 检查方向1：edgex买，lighter卖
        is_valid1, reason1, expected_profit1 = self.validate_trade_opportunity(
            symbol, 'spread1', log_spread1, z1
        )
        
        if is_valid1:
            # 立即生成client_order_id并推送信号到Redis（加快执行速度）
            client_order_id = str(int(time.time() * 1000))
            
            # 推送交易信号到Redis: symbol+buy+edgex+sell+lighter+client_order_id
            # long_spread1: edgex buy, lighter sell
            if self.redis_client:
                try:
                    self.redis_client.publish_trading_signal(symbol, 'buy', 'sell', client_order_id)
                    logger.info(f"📤 推送开仓信号到Redis: {symbol}+buy+edgex+sell+lighter+{client_order_id}")
                except Exception as e:
                    logger.warning(f"推送交易信号到Redis失败: {e}")
            
            # 计算入场价格和数量
            edgex_ask = float(edgex_data['best_ask'])
            lighter_bid = float(lighter_data['best_bid'])
            
            position_size = self.config['position_size_usd'] / edgex_ask
            spread_pct = pct_spread1  # 直接使用已计算的百分比价差
            
          

            
            signals.append({
                'signal': 'open_long_spread1',
                'direction': 'long_spread1',
                'reason': reason1,
                'client_order_id': client_order_id,  # 包含client_order_id
                'details': {
                    'log_spread': log_spread1,
                    'spread_pct': spread_pct,
                    'z_score': z1,
                    'expected_profit_pct': expected_profit1,
                    'edgex_price': edgex_ask,
                    'lighter_price': lighter_bid,
                    'position_size': position_size,
                }
            })
        
        # 检查方向2：lighter买，edgex卖
        is_valid2, reason2, expected_profit2 = self.validate_trade_opportunity(
            symbol, 'spread2', log_spread2, z2
        )
        
        if is_valid2:
            # 立即生成client_order_id并推送信号到Redis（加快执行速度）
            client_order_id = str(int(time.time() * 1000))
            
            # 推送交易信号到Redis: symbol+sell+edgex+buy+lighter+client_order_id
            # long_spread2: edgex sell, lighter buy
            if self.redis_client:
                try:
                    self.redis_client.publish_trading_signal(symbol, 'sell', 'buy', client_order_id)
                    logger.info(f"📤 立即推送开仓信号到Redis: {symbol}+sell+edgex+buy+lighter+{client_order_id}")
                except Exception as e:
                    logger.warning(f"推送交易信号到Redis失败: {e}")
            
            edgex_bid = float(edgex_data['best_bid'])
            lighter_ask = float(lighter_data['best_ask'])
            
            position_size = self.config['position_size_usd'] / lighter_ask
            spread_pct = pct_spread2  # 直接使用已计算的百分比价差
            
            # 发送webhook通知
            # try:
            #     alert_msg = (
            #         f"【统计套利机会】{symbol} | "
            #         f"方向: lighter买/edgex卖 | "
            #         f"价差: {spread_pct*100:.4f}% | "
            #         f"Z-score: {z2:.2f} | "
            #         f"预期利润: {expected_profit2*100:.4f}% | "
            #         f"edgex价格: {edgex_bid:.4f} | "
            #         f"lighter价格: {lighter_ask:.4f}"
            #     )
            #     send_webhook_alert(alert_msg)
            # except Exception as e:
            #     logger.warning(f"发送webhook通知失败: {e}")
            
            signals.append({
                'signal': 'open_long_spread2',
                'direction': 'long_spread2',
                'reason': reason2,
                'client_order_id': client_order_id,  # 包含client_order_id
                'details': {
                    'log_spread': log_spread2,
                    'spread_pct': spread_pct,
                    'z_score': z2,
                    'expected_profit_pct': expected_profit2,
                    'edgex_price': edgex_bid,
                    'lighter_price': lighter_ask,
                    'position_size': position_size,
                }
            })
        
        return signals
    
    def _check_close_signals(self, symbol: str, position: dict, z1: float, z2: float,
                           log_spread1: float, log_spread2: float) -> List[dict]:
        """检查平仓信号"""
        signals = []
        current_time = datetime.now()
        
        # 检查持仓时间
        if position['entry_time']:
            hold_time = (current_time - position['entry_time']).total_seconds()
            if hold_time > self.config['max_hold_time']:
                signals.append({
                    'signal': 'close_timeout',
                    'reason': f'持仓时间超过{self.config["max_hold_time"]}秒',
                    'details': {'hold_time': hold_time}
                })
        
        # 根据持仓方向检查统计平仓条件
        if position['direction'] == 'long_spread1':
            # 检查平仓：Z-score回归
            if z1 < self.config['Z_close']:
                signals.append({
                    'signal': 'close_normal',
                    'reason': f'Z-score回归到{z1:.2f}',
                    'details': {'current_z': z1}
                })
            
            # 检查动态平仓阈值
            _, close_threshold = self.calculate_dynamic_thresholds(symbol, 'spread1')
            
            if log_spread1 < close_threshold:
                signals.append({
                    'signal': 'close_threshold',
                    'reason': f'价差回归到阈值{log_spread1:.8f} < {close_threshold:.8f}',
                    'details': {'current_spread': log_spread1}
                })
        
        elif position['direction'] == 'long_spread2':
            # 检查平仓：Z-score回归
            if z2 < self.config['Z_close']:
                signals.append({
                    'signal': 'close_normal',
                    'reason': f'Z-score回归到{z2:.2f}',
                    'details': {'current_z': z2}
                })
            
            # 检查动态平仓阈值
            _, close_threshold = self.calculate_dynamic_thresholds(symbol, 'spread2')
            
            if log_spread2 < close_threshold:
                signals.append({
                    'signal': 'close_threshold',
                    'reason': f'价差回归到阈值{log_spread2:.8f} < {close_threshold:.8f}',
                    'details': {'current_spread': log_spread2}
                })
        
        return signals
    
    def execute_signal(self, symbol: str, signal: dict, edgex_data: dict = None, lighter_data: dict = None):
        """执行交易信号"""
        position = self.positions[symbol]
        signal_type = signal['signal']
        
        if signal_type.startswith('open_'):
            self._execute_open_signal(symbol, signal, edgex_data, lighter_data)
        
        elif signal_type.startswith('close_'):
            self._execute_close_signal(symbol, signal, edgex_data, lighter_data)
    
    def _execute_open_signal(self, symbol: str, signal: dict, edgex_data: dict, lighter_data: dict):
        """执行开仓信号"""
        direction = signal['direction']
        details = signal['details']
        
        # 获取当前价格
        '''long_spread1: edgex买，lighter卖'''
        '''long_spread2: lighter买，edgex卖'''
        if direction == 'long_spread1':
            edgex_price = float(edgex_data['best_ask'])
            lighter_price = float(lighter_data['best_bid'])
        else:         # long_spread2
            edgex_price = float(edgex_data['best_bid'])
            lighter_price = float(lighter_data['best_ask'])
        
        # 记录基准统计量（用于固定基准Z-score）
        stats = self.statistics[symbol]
        
        # 获取开仓时的 client_order_id
        entry_client_order_id = signal.get('client_order_id', '')
        
        # 更新持仓信息
        self.positions[symbol] = {
            'direction': direction,
            'entry_time': datetime.now(),
            'entry_log_spread': details['log_spread'],
            'entry_spread_pct': details['spread_pct'],
            'entry_zscore': details['z_score'],
            'entry_price_edgex': edgex_price,
            'entry_price_lighter': lighter_price,
            'position_size': details['position_size'],
            'entry_client_order_id': entry_client_order_id,  # 保存开仓时的 client_order_id
            'pnl': 0.0,
            # 记录基准统计量（固定基准Z-score）
            'baseline_mean1': stats['mean1'],
            'baseline_std1': stats['std1'],
            'baseline_mean2': stats['mean2'],
            'baseline_std2': stats['std2'],
        }
        
        # 记录交易
        trade_record = {
            'timestamp': datetime.now(),
            'symbol': symbol,
            'action': 'open',
            'direction': direction,
            'edgex_price': edgex_price,
            'lighter_price': lighter_price,
            'spread_pct': details['spread_pct'],
            'z_score': details['z_score'],
            'position_size': details['position_size'],
            'reason': signal['reason'],
        }
        self.trade_history.append(trade_record)
        
        logger.info(
            f"{symbol} 开仓: {direction} | "
            f"价差={details['spread_pct']*100:.4f}% | "
            f"Z={details['z_score']:.2f} | "
            f"预期利润={details['expected_profit_pct']*100:.4f}% | "
            f"仓位={details['position_size']:.6f} | "
            f"edgex价格={edgex_price:.6f} | "
            f"lighter价格={lighter_price:.6f} | "
            f"基准均值1={stats['mean1']:.8f} | "
            f"基准标准差1={stats['std1']:.8f}"
        )
        
        # 异步写入CSV日志（包含client_order_id）
        try:
            client_order_id = signal.get('client_order_id', '')
            self.csv_logger.log_open(
                symbol=symbol,
                direction=direction,
                spread_pct=details['spread_pct'],
                z_score=details['z_score'],
                expected_profit_pct=details['expected_profit_pct'],
                edgex_price=edgex_price,
                lighter_price=lighter_price,
                position_size=details['position_size'],
                baseline_mean1=stats['mean1'],
                baseline_std1=stats['std1'],
                baseline_mean2=stats['mean2'],
                baseline_std2=stats['std2'],
                reason=signal.get('reason', ''),
                client_order_id=client_order_id
            )
        except Exception as e:
            logger.warning(f"写入CSV开仓日志失败: {e}")
        
        # 异步存储价差信号到数据库（包含client_order_id）
        if self.signal_db:
            def save_to_db():
                try:
                    current_time = datetime.now()
                    current_time_str = current_time.isoformat()
                    
                    # 根据direction确定参数
                    if direction == 'long_spread1':
                        self.signal_db.log_spread_signal(
                            exchange_pair='edgex-lighter',
                            symbol=symbol,
                            timestamp=current_time_str,
                            expected_profit_pct=details['expected_profit_pct'],
                            current_z=details['z_score'],
                            log_spread=details['log_spread'],
                            spread_pct=details['spread_pct'],
                            direction='long_spread1',
                            edgex_price=edgex_price,
                            lighter_price=lighter_price,
                            client_order_id=client_order_id
                        )
                    else:  # long_spread2
                        self.signal_db.log_spread_signal(
                            exchange_pair='edgex-lighter',
                            symbol=symbol,
                            timestamp=current_time_str,
                            expected_profit_pct=details['expected_profit_pct'],
                            current_z=details['z_score'],
                            log_spread=details['log_spread'],
                            spread_pct=details['spread_pct'],
                            direction='long_spread2',
                            edgex_price=edgex_price,
                            lighter_price=lighter_price,
                            client_order_id=client_order_id
                        )
                except Exception as e:
                    logger.warning(f"异步存储价差信号到数据库失败: {e}")
            
            # 在后台线程中异步执行数据库存储
            db_thread = threading.Thread(target=save_to_db, daemon=True)
            db_thread.start()
    
    def _execute_close_signal(self, symbol: str, signal: dict, edgex_data: dict, lighter_data: dict):
        """执行平仓信号"""
        position = self.positions[symbol]
        
        if not position['direction']:
            return
        
        # 生成平仓时的 client_order_id
        client_order_id = str(int(time.time() * 1000))
        
        # 根据持仓方向确定平仓时的交易方向
        if position['direction'] == 'long_spread1':
            # 平仓时：卖出edgex，买入lighter（与开仓相反）
            edgex_direction = 'sell'
            lighter_direction = 'buy'
        else:  # long_spread2
            # 平仓时：买入edgex，卖出lighter（与开仓相反）
            edgex_direction = 'buy'
            lighter_direction = 'sell'
        
        # 推送平仓信号到Redis
        if self.redis_client:
            try:
                self.redis_client.publish_trading_signal(symbol, edgex_direction, lighter_direction, client_order_id)
                logger.info(f"📤 推送平仓信号到Redis: {symbol}+{edgex_direction}+edgex+{lighter_direction}+lighter+{client_order_id}")
            except Exception as e:
                logger.warning(f"推送平仓交易信号到Redis失败: {e}")
        
        # 获取平仓价格
        if position['direction'] == 'long_spread1':
            # 开仓时：log_spread1 = log(lighter_bid) - log(edgex_ask) (edgex买，lighter卖)
            # 平仓时：卖出edgex，买入lighter
            edgex_price = float(edgex_data['best_bid'])  # 卖出价
            lighter_price = float(lighter_data['best_ask'])  # 买入价
            
            # 计算平仓价差（反向价差）：log(lighter_ask) - log(edgex_bid)
            close_log_spread = np.log(lighter_price) - np.log(edgex_price)
            close_spread_pct = (lighter_price - edgex_price) / edgex_price
            
        else:  # long_spread2
            # 开仓时：log_spread2 = log(edgex_bid) - log(lighter_ask) (lighter买，edgex卖)
            # 平仓时：卖出lighter，买入edgex
            edgex_price = float(edgex_data['best_ask'])  # 买入价
            lighter_price = float(lighter_data['best_bid'])  # 卖出价
            
            # 计算平仓价差（反向价差）：log(edgex_ask) - log(lighter_bid)
            close_log_spread = np.log(edgex_price) - np.log(lighter_price)
            close_spread_pct = (edgex_price - lighter_price) / lighter_price
        
        # 计算盈亏
        position_size = position['position_size']
        
        # 计算毛利润（忽略手续费）
        if position['direction'] == 'long_spread1':
            # 开仓：买edgex，卖lighter
            # 平仓：卖edgex，买lighter
            gross_pnl = position_size * (
                (edgex_price - position['entry_price_edgex']) +  # edgex部分
                (position['entry_price_lighter'] - lighter_price)  # lighter部分
            )
        else:  # long_spread2
            gross_pnl = position_size * (
                (lighter_price - position['entry_price_lighter']) +  # lighter部分
                (position['entry_price_edgex'] - edgex_price)  # edgex部分
            )
        
        # 计算手续费成本
        edgex_fee_cost = position_size * (
            position['entry_price_edgex'] * self.config['edgex_fee'] +
            edgex_price * self.config['edgex_fee']
        )
        
        lighter_fee_cost = position_size * (
            position['entry_price_lighter'] * self.config['lighter_fee'] +
            lighter_price * self.config['lighter_fee']
        )
        
        total_fee = edgex_fee_cost + lighter_fee_cost
        net_pnl = gross_pnl - total_fee
        
        # 更新当日盈亏
        self.daily_pnl += net_pnl
        
        # 记录交易
        trade_record = {
            'timestamp': datetime.now(),
            'symbol': symbol,
            'action': 'close',
            'direction': position['direction'],
            'entry_edgex_price': position['entry_price_edgex'],
            'entry_lighter_price': position['entry_price_lighter'],
            'close_edgex_price': edgex_price,
            'close_lighter_price': lighter_price,
            'entry_spread_pct': position['entry_spread_pct'],
            'close_spread_pct': close_spread_pct,
            'gross_pnl': gross_pnl,
            'total_fee': total_fee,
            'net_pnl': net_pnl,
            'position_size': position_size,
            'reason': signal['reason'],
        }
        self.trade_history.append(trade_record)
        
        # 超时平仓后进入冷却期，避免持续追涨杀跌
        if signal.get('signal') == 'close_timeout':
            cooldown_seconds = self.config.get('timeout_cooldown_seconds', 0)
            if cooldown_seconds > 0:
                self.cooldown_until[symbol] = datetime.now() + timedelta(seconds=cooldown_seconds)
                logger.info(f"{symbol} 触发冷却: {cooldown_seconds}s")
        
        # 重置持仓
        self.positions[symbol] = {
            'direction': None,
            'entry_time': None,
            'entry_log_spread': None,
            'entry_spread_pct': None,
            'entry_zscore': None,
            'entry_price_edgex': None,
            'entry_price_lighter': None,
            'entry_client_order_id': None,
            'position_size': 0,
            'pnl': 0.0,
            # 重置基准统计量
            'baseline_mean1': None,
            'baseline_std1': None,
            'baseline_mean2': None,
            'baseline_std2': None,
        }
        
        hold_time_seconds = (datetime.now() - position['entry_time']).total_seconds()
        
        logger.info(
            f"{symbol} 平仓: {position['direction']} | "
            f"持仓时间={hold_time_seconds:.1f}s | "
            f"开仓价差={position['entry_spread_pct']*100:.4f}% | "
            f"平仓价差={close_spread_pct*100:.4f}% | "
            f"开仓edgex价格={position['entry_price_edgex']:.6f} | "
            f"开仓lighter价格={position['entry_price_lighter']:.6f} | "
            f"平仓edgex价格={edgex_price:.6f} | "
            f"平仓lighter价格={lighter_price:.6f} | "
            f"毛利={gross_pnl:.4f} USD | "
            f"手续费={total_fee:.4f} USD | "
            f"净利={net_pnl:.4f} USD | "
            f"原因: {signal['reason']}"
        )
        
        # 异步写入CSV日志
        try:
            self.csv_logger.log_close(
                symbol=symbol,
                direction=position['direction'],
                entry_spread_pct=position['entry_spread_pct'],
                close_spread_pct=close_spread_pct,
                entry_zscore=position['entry_zscore'],
                entry_edgex_price=position['entry_price_edgex'],
                entry_lighter_price=position['entry_price_lighter'],
                close_edgex_price=edgex_price,
                close_lighter_price=lighter_price,
                position_size=position_size,
                hold_time_seconds=hold_time_seconds,
                gross_pnl=gross_pnl,
                total_fee=total_fee,
                net_pnl=net_pnl,
                reason=signal.get('reason', '')
            )
        except Exception as e:
            logger.warning(f"写入CSV平仓日志失败: {e}")
    
    def update_symbol(self, symbol: str):
        """更新单个交易对的逻辑"""
        try:
            # 1. 获取最新数据
            edgex_data, lighter_data = self.fetch_latest_data(symbol)
            
            if not edgex_data or not lighter_data:
                logger.debug(f"{symbol}: 数据获取失败")
                return
            
            # 2. 计算价差
            log_spread1, log_spread2, pct_spread1, pct_spread2 = self.calculate_log_spread(
                edgex_data, lighter_data
            )
            
            if log_spread1 is None:
                return
            
            # 3. 更新统计量（使用 edgex 的时间戳）
            current_time = edgex_data.get('timestamp')
            if current_time is None:
                current_time = datetime.now()
            
            self.update_statistics(symbol, log_spread1, log_spread2, pct_spread1, pct_spread2, current_time)
            
            # 5. 计算Z-score
            z1, z2 = self.calculate_zscore(symbol, log_spread1, log_spread2)
            
            # 6. 检查交易信号
            signals = self.check_trading_signals(symbol, log_spread1, log_spread2, pct_spread1, pct_spread2, z1, z2, edgex_data, lighter_data)
            
            # 7. 执行信号（只执行优先级最高的信号）
            if signals:
                # 优先处理平仓信号
                close_signals = [s for s in signals if s['signal'].startswith('close_')]
                open_signals = [s for s in signals if s['signal'].startswith('open_')]
                
                if close_signals:
                    self.execute_signal(symbol, close_signals[0], edgex_data, lighter_data)
                elif open_signals:
                    # 检查是否已有持仓
                    if self.positions[symbol]['direction'] is None:
                        self.execute_signal(symbol, open_signals[0], edgex_data, lighter_data)
            
            # 8. 定期记录状态
            if self.statistics[symbol]['count'] % 60 == 0:  # 每60次记录一次
                self.log_symbol_status(symbol, log_spread1, log_spread2, z1, z2)
                
        except Exception as e:
            logger.error(f"更新{symbol}时出错: {e}", exc_info=True)
    
    def log_symbol_status(self, symbol: str, log_spread1: float, log_spread2: float, z1: float, z2: float):
        """记录交易对状态"""
        stats = self.statistics[symbol]
        position = self.positions[symbol]
        
        status_msg = (
            f"{symbol} 状态 | "
            f"数据点={stats['count']} | "
            f"μ1={stats['mean1']:.8f}, σ1={stats['std1']:.8f} | "
            f"价差1={np.exp(log_spread1)-1:.6f}(Z={z1:.2f}) | "
            f"价差2={np.exp(log_spread2)-1:.6f}(Z={z2:.2f}) | "
            f"持仓={position['direction'] or '无'}"
        )
        
        if position['direction']:
            hold_time = (datetime.now() - position['entry_time']).total_seconds()
            status_msg += f" | 持仓时间={hold_time:.1f}s"
        
        logger.info(status_msg)
    
    def fetch_latest_data(self, symbol: str) -> Tuple[Optional[dict], Optional[dict]]:
        """
        获取最新数据（从Redis获取）
        
        返回:
        (edgex_data, lighter_data)
        """
        try:
            # 从Redis获取最新数据
            edgex_redis_data = self.redis_client.get_latest_bbo('edgex', symbol)
            lighter_redis_data = self.redis_client.get_latest_bbo('lighter', symbol)
            
            # 转换为数据库格式
            edgex_data = None
            lighter_data = None
            
            if edgex_redis_data:
                try:
                    edgex_data = {
                        'symbol': symbol,
                        'best_bid': float(edgex_redis_data['best_bid']) if edgex_redis_data.get('best_bid') else 0.0,
                        'best_ask': float(edgex_redis_data['best_ask']) if edgex_redis_data.get('best_ask') else 0.0,
                        'timestamp': edgex_redis_data.get('timestamp')
                    }
                except Exception as e:
                    logger.debug(f"转换edgex数据格式失败: {e}")
                    edgex_data = None
            
            if lighter_redis_data:
                try:
                    lighter_data = {
                        'symbol': symbol,
                        'best_bid': float(lighter_redis_data['best_bid']) if lighter_redis_data.get('best_bid') else 0.0,
                        'best_ask': float(lighter_redis_data['best_ask']) if lighter_redis_data.get('best_ask') else 0.0,
                        'timestamp': lighter_redis_data.get('timestamp')
                    }
                except Exception as e:
                    logger.debug(f"转换lighter数据格式失败: {e}")
                    lighter_data = None
            
            return edgex_data, lighter_data
            
        except Exception as e:
            logger.error(f"获取{symbol}数据失败: {e}", exc_info=True)
            return None, None
    
    def run_once(self):
        """运行一次所有交易对的检查"""
        for symbol in self.config['symbols']:
            self.update_symbol(symbol)
    
    def run_continuous(self, duration_hours: float = None):
        """持续运行算法"""
        logger.info("开始持续运行统计套利算法...")
        logger.info(f"总成本要求: {self.config['total_requirement']*100:.4f}%")
        logger.info(f"Z阈值: 开仓={self.config['Z_open']}, 平仓={self.config['Z_close']}")
        
        start_time = time.time()
        iteration = 0
        
        try:
            while True:
                iteration_start = time.time()
                iteration += 1
                
                # 运行一次检查
                self.run_once()
                
                # 每小时记录一次汇总
                if iteration % 3600 == 0:
                    self.log_summary()
                
                # 计算睡眠时间
                elapsed = time.time() - iteration_start
                sleep_time = max(0.1, self.config['update_interval'] - elapsed)
                
                # 检查运行时间
                if duration_hours and (time.time() - start_time) > duration_hours * 3600:
                    logger.info(f"达到运行时间{duration_hours}小时，停止运行")
                    break
                
                time.sleep(sleep_time)
                
        except KeyboardInterrupt:
            logger.info("接收到中断信号，停止运行...")
        except Exception as e:
            logger.error(f"运行过程中出错: {e}", exc_info=True)
        finally:
            self.log_final_summary()
            # 关闭CSV日志记录器
            if hasattr(self, 'csv_logger') and self.csv_logger:
                self.csv_logger.close()
            if hasattr(self, 'redis_client') and self.redis_client:
                self.redis_client.close()
            if hasattr(self, 'signal_db') and self.signal_db:
                self.signal_db.close()
    
    def log_summary(self):
        """记录汇总信息"""
        active_positions = sum(1 for symbol in self.config['symbols'] 
                             if self.positions[symbol]['direction'] is not None)
        
        total_trades = len([t for t in self.trade_history if t['action'] == 'close'])
        winning_trades = len([t for t in self.trade_history 
                            if t['action'] == 'close' and t['net_pnl'] > 0])
        
        win_rate = winning_trades / total_trades if total_trades > 0 else 0
        total_pnl = sum(t['net_pnl'] for t in self.trade_history if t['action'] == 'close')
        
        logger.info(
            f"汇总 | "
            f"活跃持仓={active_positions} | "
            f"总交易数={total_trades} | "
            f"胜率={win_rate:.1%} | "
            f"总盈亏={total_pnl:.4f} USD | "
            f"当日盈亏={self.daily_pnl} USD"
        )
    
    def log_final_summary(self):
        """记录最终汇总信息"""
        logger.info("=" * 80)
        logger.info("最终汇总")
        logger.info("=" * 80)
        self.log_summary()
        logger.info("=" * 80)


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='统计套利算法')
    parser.add_argument('--tickers-file', type=str, default='tickers.txt',
                        help='tickers文件路径 (默认: tickers.txt)')
    parser.add_argument('--duration-hours', type=float, default=None,
                        help='运行时长（小时），默认持续运行')
    parser.add_argument('--config', type=str, default=None,
                        help='配置文件路径（JSON格式）')
    
    args = parser.parse_args()
    
    # 加载配置
    config = {}
    if args.config and os.path.exists(args.config):
        try:
            with open(args.config, 'r', encoding='utf-8') as f:
                config = json.load(f)
            logger.info(f"从配置文件加载配置: {args.config}")
        except Exception as e:
            logger.error(f"加载配置文件失败: {e}")
    
    # 设置tickers文件路径
    config['tickers_file'] = args.tickers_file
    
    # 创建统计套利实例
    try:
        arb = StatisticalArbitrage(config=config)
        
        # 持续运行
        arb.run_continuous(duration_hours=args.duration_hours)
        
    except Exception as e:
        logger.error(f"运行失败: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
