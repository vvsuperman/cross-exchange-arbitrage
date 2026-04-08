#!/usr/bin/env python3
"""
单独的程序来记录EdgeX、Lighter和Backpack的best bid、best ask、size和时间
每10秒采样一次，存到SQLite3数据库
每个交易所使用独立的数据库
使用websocket订阅获取实时价格数据（Backpack使用REST API）
注意：Backpack相关逻辑已注释
"""
import asyncio
import os
import sys
import signal
import argparse
import json
import logging
import random
from decimal import Decimal
from datetime import datetime
import pytz
from dotenv import load_dotenv
from typing import Optional, Tuple

# 定义 UTC+8 时区
TZ_UTC8 = pytz.timezone('Asia/Shanghai')

# 直接导入，避免通过__init__.py导入不存在的factory模块
# 将项目根目录添加到sys.path，以便导入exchanges模块
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from exchanges.edgex import EdgeXClient
from exchanges.lighter import LighterClient
# from exchanges.backpack import BackpackClient, get_backpack_bbo_from_rest
from edgex_sdk import GetOrderBookDepthParams
from db.record_db import BBODataDB
from websocket_managers import (
    EdgeXMultiTickerWebSocketManager,
    LighterMultiTickerWebSocketManager,
    # BackpackMultiTickerWebSocketManager
)

# 设置 logging

# 配置日志格式
log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
formatter = logging.Formatter(log_format)


# 创建控制台handler（输出到stdout）
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.DEBUG)
console_handler.setFormatter(formatter)

# 配置根logger
root_logger = logging.getLogger()
root_logger.setLevel(logging.DEBUG)
root_logger.handlers.clear()  # 清除已有的handlers
root_logger.addHandler(console_handler)

# 禁用外部库的详细日志
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('requests').setLevel(logging.WARNING)
logging.getLogger('websockets').setLevel(logging.WARNING)

# 确保backpack模块的logger也输出日志
# backpack_logger = logging.getLogger('exchanges.backpack')
# backpack_logger.setLevel(logging.INFO)
# backpack_logger会继承root_logger的handlers，所以不需要单独添加

logger = logging.getLogger('record_bbo')
# 确保logger级别正确设置
logger.setLevel(logging.INFO)


def format_timestamp_to_seconds(dt: datetime) -> str:
    """格式化时间戳到秒级别（去掉毫秒和时区）"""
    return dt.replace(microsecond=0, tzinfo=None).isoformat()


class Config:
    """Simple config class to wrap dictionary for exchange clients."""
    def __init__(self, config_dict):
        for key, value in config_dict.items():
            setattr(self, key, value)


class BBODataLogger:
    """记录单个交易所的BBO数据到SQLite3数据库"""
    
    def __init__(self, exchange: str, ticker: str):
        """
        初始化数据记录器
        
        Args:
            exchange: 交易所名称（如 'edgex' 或 'lighter'）
            ticker: 交易对符号
        """
        self.exchange = exchange.lower()
        self.ticker = ticker
        # 每个交易所使用独立的数据库
        self.db = BBODataDB(exchange, ticker)
        
    def log_bbo_data(self, best_bid: Decimal, best_bid_size: Decimal,
                     best_ask: Decimal, best_ask_size: Decimal,
                     price_timestamp: Optional[str]):
        """
        记录BBO数据到数据库
        
        Args:
            best_bid: 最佳买价
            best_bid_size: 最佳买价数量
            best_ask: 最佳卖价
            best_ask_size: 最佳卖价数量
            price_timestamp: 价格时间戳
        """
        timestamp = format_timestamp_to_seconds(datetime.now(TZ_UTC8))
        
        # 写入数据库
        self.db.log_bbo_data(
            best_bid, best_bid_size,
            best_ask, best_ask_size,
            price_timestamp,
            timestamp=timestamp,
            symbol=self.ticker
        )
        
        # 记录日志
        bid_str = str(best_bid) if best_bid else 'N/A'
        bid_size_str = str(best_bid_size) if best_bid_size else 'N/A'
        ask_str = str(best_ask) if best_ask else 'N/A'
        ask_size_str = str(best_ask_size) if best_ask_size else 'N/A'
        ts_str = price_timestamp or 'N/A'
        
        # logger.info(f"[{self.exchange.upper()}] [{self.ticker}] [{timestamp}] bid={bid_str}@{bid_size_str}, ask={ask_str}@{ask_size_str} (ts={ts_str})")


async def get_edgex_bbo_from_rest(edgex_client: EdgeXClient, contract_id: str) -> Tuple[Optional[Decimal], Optional[Decimal], Optional[Decimal], Optional[Decimal], Optional[str]]:
    """从REST API获取EdgeX的best bid/ask和对应的size"""
    try:
        depth_params = GetOrderBookDepthParams(contract_id=contract_id, limit=15)
        order_book = await edgex_client.client.quote.get_order_book_depth(depth_params)
        order_book_data = order_book['data']
        
        # Get the first (and should be only) order book entry
        order_book_entry = order_book_data[0]
        
        # Extract bids and asks from the entry
        bids = order_book_entry.get('bids', [])
        asks = order_book_entry.get('asks', [])
        
        # Best bid is the highest price (first in the list, as it's sorted)
        best_bid = None
        best_bid_size = None
        if bids and len(bids) > 0:
            best_bid = Decimal(bids[0]['price'])
            best_bid_size = Decimal(bids[0]['size'])
        
        # Best ask is the lowest price (first in the list, as it's sorted)
        best_ask = None
        best_ask_size = None
        if asks and len(asks) > 0:
            best_ask = Decimal(asks[0]['price'])
            best_ask_size = Decimal(asks[0]['size'])
        
        # 使用当前时间作为时间戳（REST API通常不返回时间戳）
        price_timestamp = format_timestamp_to_seconds(datetime.now(TZ_UTC8))
        
        return best_bid, best_bid_size, best_ask, best_ask_size, price_timestamp
    except Exception as e:
        logger.error(f"Error getting EdgeX BBO from REST API: {e}", exc_info=True)
        return None, None, None, None, None


class TickerRecorder:
    """单个标的的记录器"""
    
    def __init__(self, ticker: str, edgex_ws_manager=None, lighter_ws_manager=None, backpack_ws_manager=None):
        """初始化单个标的的记录器"""
        self.ticker = ticker
        # 共享的 WebSocket managers
        self.edgex_ws_manager = edgex_ws_manager
        self.lighter_ws_manager = lighter_ws_manager
        # self.backpack_ws_manager = backpack_ws_manager
        # 为每个交易所创建独立的数据记录器
        self.edgex_logger = BBODataLogger('edgex', ticker)
        self.lighter_logger = BBODataLogger('lighter', ticker)
        # self.backpack_logger = BBODataLogger('backpack', ticker)
        self.edgex_client = None
        self.lighter_client = None
        # self.backpack_client = None
        self.edgex_contract_id = None
        self.lighter_contract_id = None
        # self.backpack_contract_id = None
    
    async def record_data(self):
        """记录一次数据 - 从共享的WebSocket managers获取数据"""
        try:
            # 获取EdgeX的BBO数据 - 从共享的WebSocket manager获取
            edgex_bid, edgex_bid_size, edgex_ask, edgex_ask_size, edgex_price_timestamp = None, None, None, None, None
            if self.edgex_ws_manager:
                edgex_bid, edgex_bid_size, edgex_ask, edgex_ask_size, edgex_price_timestamp = self.edgex_ws_manager.get_ticker_data(self.ticker)
            
            # 如果WebSocket数据不可用，回退到REST API
            if edgex_bid is None or edgex_ask is None:
                try:
                    logger.warning(f" ------------- {self.ticker} EdgeX REST API获取BBO数据-------------")
                    edgex_bid, edgex_bid_size, edgex_ask, edgex_ask_size, edgex_price_timestamp = await get_edgex_bbo_from_rest(
                        self.edgex_client, self.edgex_contract_id
                    )
                except Exception as e:
                    logger.error(f"{self.ticker} EdgeX REST API获取失败: {e}", exc_info=True)
            
            # 获取Lighter的BBO数据 - 从共享的WebSocket manager获取
            lighter_bid, lighter_bid_size, lighter_ask, lighter_ask_size, lighter_price_timestamp = None, None, None, None, None
            if self.lighter_ws_manager:
                lighter_bid, lighter_bid_size, lighter_ask, lighter_ask_size, lighter_price_timestamp = self.lighter_ws_manager.get_ticker_data(self.ticker)
            
            # 如果WebSocket数据不可用，回退到REST API
            # if lighter_bid is None or lighter_ask is None:
            #     try:
            #         logger.warning(f" ------------- {self.ticker} Lighter REST API获取BBO数据-------------")
            #         lighter_bid, lighter_bid_size, lighter_ask, lighter_ask_size = await self.lighter_client.get_best_levels(symbol=self.ticker)
            #         lighter_price_timestamp = format_timestamp_to_seconds(datetime.now(TZ_UTC8))
            #     except Exception as e:
            #         logger.error(f"{self.ticker} Lighter REST API获取失败: {e}", exc_info=True)
            
            # 获取Backpack的BBO数据 - 从共享的WebSocket manager获取
            # backpack_bid, backpack_bid_size, backpack_ask, backpack_ask_size, backpack_price_timestamp = None, None, None, None, None
            # if self.backpack_ws_manager:
            #     backpack_bid, backpack_bid_size, backpack_ask, backpack_ask_size, backpack_price_timestamp = self.backpack_ws_manager.get_ticker_data(self.ticker)
            # 
            # # 如果WebSocket数据不可用，回退到REST API
            # if backpack_bid is None or backpack_ask is None:
            #     try:
            #         backpack_bid, backpack_bid_size, backpack_ask, backpack_ask_size, backpack_price_timestamp = await get_backpack_bbo_from_rest(
            #             self.backpack_client, self.backpack_contract_id
            #         )
            #     except Exception as e:
            #         logger.error(f"{self.ticker} Backpack REST API获取失败: {e}", exc_info=True)
            
            # 分别记录每个交易所的数据到各自的数据库
            recorded = False
            
            timestamp = format_timestamp_to_seconds(datetime.now(TZ_UTC8))
            # 记录EdgeX数据
            if edgex_bid is not None and edgex_ask is not None:
                self.edgex_logger.log_bbo_data(
                    edgex_bid, edgex_bid_size,
                    edgex_ask, edgex_ask_size,
                    # edgex_price_timestamp
                    timestamp
                )
                recorded = True
            
            # 记录Lighter数据
            if lighter_bid is not None and lighter_ask is not None:
                self.lighter_logger.log_bbo_data(
                    lighter_bid, lighter_bid_size,
                    lighter_ask, lighter_ask_size,
                    # lighter_price_timestamp
                    timestamp
                )
                recorded = True
            
            # 记录Backpack数据
            # if backpack_bid is not None and backpack_ask is not None:
            #     self.backpack_logger.log_bbo_data(
            #         backpack_bid, backpack_bid_size,
            #         backpack_ask, backpack_ask_size,
            #         # backpack_price_timestamp
            #         timestamp
            #     )
            #     recorded = True
            
            if not recorded:
                logger.warning(f"{self.ticker} 无法获取BBO数据，跳过本次记录")
                return False
            
            return True
        except Exception as e:
            logger.error(f"{self.ticker} 记录数据时出错: {e}", exc_info=True)
            return False
    
    async def cleanup(self):
        """清理资源"""
        logger.info(f"正在断开 {self.ticker} 的连接...")
        
        # 关闭数据库连接（执行WAL检查点）
        try:
            if hasattr(self, 'edgex_logger') and hasattr(self.edgex_logger, 'db'):
                self.edgex_logger.db.close()
                logger.debug(f"{self.ticker} EdgeX数据库连接已关闭")
        except Exception as e:
            logger.warning(f"{self.ticker} 关闭EdgeX数据库连接时出错: {e}")
        
        try:
            if hasattr(self, 'lighter_logger') and hasattr(self.lighter_logger, 'db'):
                self.lighter_logger.db.close()
                logger.debug(f"{self.ticker} Lighter数据库连接已关闭")
        except Exception as e:
            logger.warning(f"{self.ticker} 关闭Lighter数据库连接时出错: {e}")
        
        # try:
        #     if hasattr(self, 'backpack_logger') and hasattr(self.backpack_logger, 'db'):
        #         self.backpack_logger.db.close()
        #         logger.debug(f"{self.ticker} Backpack数据库连接已关闭")
        # except Exception as e:
        #     logger.warning(f"{self.ticker} 关闭Backpack数据库连接时出错: {e}")
        
        # 断开交易所连接
        if self.edgex_client:
            try:
                await self.edgex_client.disconnect()
            except Exception as e:
                logger.warning(f"{self.ticker} 断开EdgeX连接时出错: {e}")
        if self.lighter_client:
            try:
                await self.lighter_client.disconnect()
            except Exception as e:
                logger.warning(f"{self.ticker} 断开Lighter连接时出错: {e}")
        # if self.backpack_client:
        #     try:
        #         await self.backpack_client.disconnect()
        #     except Exception as e:
        #         logger.warning(f"{self.ticker} 断开Backpack连接时出错: {e}")
        
        logger.info(f"{self.ticker} 已断开所有连接")


class BBORecorder:
    """BBO数据记录器主类 - 支持多个标的"""
    
    def __init__(self, tickers: list, interval: int = 10):
        """初始化记录器"""
        self.tickers = tickers
        self.interval = interval
        self.stop_flag = False
        self.ticker_recorders: Tuple[TickerRecorder] = {}  # {ticker: TickerRecorder}
        
        # 共享的 WebSocket managers
        self.edgex_ws_manager = None
        self.lighter_ws_manager = None
        # self.backpack_ws_manager = None
        
        # 存储所有标的的合约ID映射
        self.ticker_contract_ids = {}  # {ticker: {edgex: contract_id, lighter: contract_id, backpack: contract_id}}
        
    async def initialize_exchanges(self):
        """初始化所有标的的交易所连接和共享的WebSocket订阅"""
        logger.info(f"正在初始化 {len(self.tickers)} 个标的的交易所连接...")
        
        # 步骤1: 为每个标的创建客户端并获取合约ID
        ticker_to_edgex_contract = {}  # {ticker: contract_id}
        ticker_to_lighter_contract = {}  # {ticker: market_index}
        # ticker_to_backpack_contract = {}  # {ticker: contract_id}
        
        edgex_ws_manager_instance = None
        lighter_client_instance = None
        lighter_account_index = None
        
        for ticker in self.tickers:
            # 创建 TickerRecorder（不初始化 WebSocket，稍后传入共享的 managers）
            recorder = TickerRecorder(ticker)
            
            # 初始化 EdgeX 客户端并获取合约ID
            edgex_config = Config({
                'ticker': ticker,
                'contract_id': None,
                'tick_size': Decimal('0.01'),
                'quantity': Decimal('0.001'),
                'close_order_side': 'buy'
            })
            recorder.edgex_client = EdgeXClient(edgex_config)
            # 不需要 connect()，只需要获取合约ID和 ws_manager 实例
            recorder.edgex_contract_id, _ = await recorder.edgex_client.get_contract_attributes()
            ticker_to_edgex_contract[ticker] = recorder.edgex_contract_id
            if edgex_ws_manager_instance is None:
                edgex_ws_manager_instance = recorder.edgex_client.ws_manager
            
            # 初始化 Lighter 客户端并获取合约ID
            lighter_config = Config({
                'ticker': ticker,
                'contract_id': None,
                'tick_size': Decimal('0.01'),
                'quantity': Decimal('0.001'),
                'close_order_side': 'buy'
            })
            recorder.lighter_client = LighterClient(lighter_config)
            # 只需要初始化 lighter_client（用于 WebSocket manager），不需要 connect()
            await recorder.lighter_client._initialize_lighter_client()
            if not recorder.lighter_client.config.contract_id:
                recorder.lighter_contract_id, _ = await recorder.lighter_client.get_contract_attributes()
            else:
                recorder.lighter_contract_id = recorder.lighter_client.config.contract_id
            ticker_to_lighter_contract[ticker] = int(recorder.lighter_contract_id)
            if lighter_client_instance is None:
                lighter_client_instance = recorder.lighter_client.lighter_client
                lighter_account_index = recorder.lighter_client.account_index
            
            # 初始化 Backpack 客户端并获取合约ID
            # backpack_config = Config({
            #     'ticker': ticker,
            #     'contract_id': None,
            #     'tick_size': Decimal('0.01'),
            #     'quantity': Decimal('0.001'),
            #     'close_order_side': 'buy'
            # })
            # recorder.backpack_client = BackpackClient(backpack_config)
            # await recorder.backpack_client.connect()
            # recorder.backpack_contract_id, _ = await recorder.backpack_client.get_contract_attributes()
            # ticker_to_backpack_contract[ticker] = recorder.backpack_contract_id
            
            self.ticker_recorders[ticker] = recorder
            logger.info(f"{ticker} 交易所连接成功: EdgeX={recorder.edgex_contract_id}, Lighter={recorder.lighter_contract_id}")  # , Backpack={recorder.backpack_contract_id}")
        
        # 步骤2: 创建共享的 WebSocket managers 并订阅所有标的
        logger.info("正在创建共享的WebSocket订阅...")
        
        # EdgeX WebSocket manager
        self.edgex_ws_manager = EdgeXMultiTickerWebSocketManager(edgex_ws_manager_instance, logger)
        tickers_list = list(self.tickers)
        edgex_contracts_list = [ticker_to_edgex_contract[t] for t in tickers_list]
        await self.edgex_ws_manager.subscribe_all(tickers_list, edgex_contracts_list)
        
        # Lighter WebSocket manager
        self.lighter_ws_manager = LighterMultiTickerWebSocketManager(lighter_client_instance, lighter_account_index, logger)
        lighter_markets_list = [ticker_to_lighter_contract[t] for t in tickers_list]
        await self.lighter_ws_manager.subscribe_all(tickers_list, lighter_markets_list)
        
        # Backpack WebSocket manager
        # self.backpack_ws_manager = BackpackMultiTickerWebSocketManager(logger)
        # backpack_contracts_list = [ticker_to_backpack_contract[t] for t in tickers_list]
        # await self.backpack_ws_manager.subscribe_all(tickers_list, backpack_contracts_list)
        
        # 存储合约ID映射
        for ticker in self.tickers:
            self.ticker_contract_ids[ticker] = {
                'edgex': ticker_to_edgex_contract[ticker],
                'lighter': ticker_to_lighter_contract[ticker],
                # 'backpack': ticker_to_backpack_contract[ticker]
            }
        
        # 将共享的 WebSocket managers 传递给每个 TickerRecorder
        for ticker, recorder in self.ticker_recorders.items():
            recorder.edgex_ws_manager = self.edgex_ws_manager
            recorder.lighter_ws_manager = self.lighter_ws_manager
            # recorder.backpack_ws_manager = self.backpack_ws_manager
        
        logger.info(f"所有 {len(self.tickers)} 个标的的连接和WebSocket订阅已初始化完成")
    
    def _check_redis_randomly(self):
        """随机检查Redis中的价格记录是否正确"""
        if not self.tickers:
            return
        
        # 随机选择一个交易所和交易对
        exchanges = ['edgex', 'lighter']
        exchange = random.choice(exchanges)
        ticker = random.choice(self.tickers)
        
        # 获取对应交易所的 Redis 客户端
        redis_client = None
        if exchange == 'edgex' and self.edgex_ws_manager:
            redis_client = self.edgex_ws_manager.redis_client
        elif exchange == 'lighter' and self.lighter_ws_manager:
            redis_client = self.lighter_ws_manager.redis_client
        
        if not redis_client:
            return
        
        try:
            # 从Redis获取最新价格
            redis_data = redis_client.get_latest_bbo(exchange, ticker)
            
            if redis_data:
                # 验证数据完整性
                has_bid = redis_data.get('best_bid') is not None
                has_ask = redis_data.get('best_ask') is not None
                has_timestamp = redis_data.get('timestamp') is not None
                
                if has_bid and has_ask and has_timestamp:
                    logger.info(
                        f"✓ Redis检查 [{exchange}:{ticker}]: "
                        f"bid={redis_data['best_bid']}, ask={redis_data['best_ask']}, "
                        f"timestamp={redis_data['timestamp']}"
                    )
                else:
                    logger.warning(
                        f"⚠ Redis检查 [{exchange}:{ticker}]: 数据不完整 - "
                        f"bid={has_bid}, ask={has_ask}, timestamp={has_timestamp}"
                    )
            else:
                logger.warning(f"⚠ Redis检查 [{exchange}:{ticker}]: 未找到数据")
                if exchange == 'edgex' and self.edgex_ws_manager:
                    self.edgex_ws_manager.request_reconnect(
                        reason=f"Redis未找到数据 [{exchange}:{ticker}]",
                        force=False
                    )
                elif exchange == 'lighter' and self.lighter_ws_manager:
                    self.lighter_ws_manager.request_reconnect(
                        reason=f"Redis未找到数据 [{exchange}:{ticker}]",
                        force=False
                    )
        except Exception as e:
            logger.error(f"Redis检查失败 [{exchange}:{ticker}]: {e}", exc_info=True)
        
    async def record_loop(self):
        """主记录循环 - 串行记录所有标的的数据"""
        logger.info(f"开始记录 {len(self.tickers)} 个标的的BBO数据，每{self.interval}秒采样一次...")
        logger.info(f"EdgeX数据将保存到: logs/edgex_bbo.db (表: bbo_data)")
        logger.info(f"Lighter数据将保存到: logs/lighter_bbo.db (表: bbo_data)")
        # logger.info(f"Backpack数据将保存到: logs/backpack_bbo.db (表: bbo_data)")
        logger.info(f"交易对列表: {', '.join(self.tickers)}")
        
        check_counter = 0  # 添加计数器
        check_interval = 10  # 每10次循环检查一次

        # 1s 记录一次  
        wait_interval = 60/len(self.tickers)
        while not self.stop_flag:
            try:
                # 串行获取所有标的的数据，避免REST API并发超时
                for recorder in self.ticker_recorders.values():
                    try:
                        await recorder.record_data()
                        await asyncio.sleep(wait_interval)
                    except Exception as e:
                        logger.error(f"记录数据时出错: {e}", exc_info=True)
                        await asyncio.sleep(30)
                
                # 随机检查Redis记录（每10次循环检查一次）
                check_counter += 1
                if check_counter >= check_interval:
                    self._check_redis_randomly()
                    check_counter = 0
                
            except Exception as e:
                logger.error(f"记录循环出错: {e}", exc_info=True)
                await asyncio.sleep(1)
            # 等待指定间隔
            # await asyncio.sleep(self.interval)
    
    async def cleanup(self):
        """清理资源"""
        logger.info("正在断开所有连接...")
        
        # 断开共享的 WebSocket managers
        if self.edgex_ws_manager:
            try:
                await self.edgex_ws_manager.disconnect()
            except Exception as e:
                logger.warning(f"断开EdgeX WebSocket时出错: {e}")
        
        if self.lighter_ws_manager:
            try:
                await self.lighter_ws_manager.disconnect()
            except Exception as e:
                logger.warning(f"断开Lighter WebSocket时出错: {e}")
        
        # if self.backpack_ws_manager:
        #     try:
        #         await self.backpack_ws_manager.disconnect()
        #     except Exception as e:
        #         logger.warning(f"断开Backpack WebSocket时出错: {e}")
        
        # 并发清理所有记录器的资源
        cleanup_tasks = [recorder.cleanup() for recorder in self.ticker_recorders.values()]
        await asyncio.gather(*cleanup_tasks, return_exceptions=True)
        
        # 额外保障：确保所有数据库连接都已关闭
        for recorder in self.ticker_recorders.values():
            for logger_name in ['edgex_logger', 'lighter_logger']:  # , 'backpack_logger']:
                if hasattr(recorder, logger_name):
                    logger_obj = getattr(recorder, logger_name)
                    if hasattr(logger_obj, 'db'):
                        try:
                            logger_obj.db.close()
                        except Exception:
                            pass  # 忽略错误，因为可能已经关闭了
        
        logger.info("已断开所有连接")
    
    async def run(self):
        """运行记录器"""
        try:
            await self.initialize_exchanges()
            await self.record_loop()
        except KeyboardInterrupt:
            logger.info("程序被用户中断")
        except Exception as e:
            logger.error(f"程序运行出错: {e}", exc_info=True)
        finally:
            await self.cleanup()


def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description='记录EdgeX、Lighter和Backpack的BBO数据（注意：Backpack相关逻辑已注释）',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument('--ticker', type=str, default='BTC,ETH, SOL, BNB,HYPE',
                       help='交易对符号 (默认: BTC)，可以指定多个，用逗号分隔，如: BTC,ETH')
    parser.add_argument('--interval', type=int, default=5,
                       help='采样间隔（秒）(默认: 10)')
    parser.add_argument('--tickers-file', type=str, default=None,
                       help='从文件读取标的列表（每行一个ticker）')
    
    return parser.parse_args()


def load_tickers_from_file(filepath: str) -> list:
    """从文件加载标的列表
    
    文件格式: SYMBOL size max-position
    例如: BTC 0.001 0.01
    只提取SYMBOL部分作为ticker
    """
    tickers = []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):  # 忽略空行和注释
                    # 分割行，取第一个字段作为ticker
                    parts = line.split()
                    if parts:
                        ticker = parts[0].upper()
                        tickers.append(ticker)
    except FileNotFoundError:
        logger.error(f"文件不存在: {filepath}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"读取文件出错: {e}")
        sys.exit(1)
    return tickers


async def main():
    """主函数"""
    args = parse_arguments()
    
    # 加载环境变量（与 arbitrage.py 保持一致）
    load_dotenv()
    
    # 获取标的列表
    if args.tickers_file:
        tickers = load_tickers_from_file(args.tickers_file)
        logger.info(f"从文件加载了 {len(tickers)} 个标的: {', '.join(tickers)}")
    else:
        # 从命令行参数解析，支持逗号分隔的多个ticker
        tickers = [t.strip().upper() for t in args.ticker.split(',') if t.strip()]
    
    if not args.interval:
        args.interval = 10
    if not args.ticker:
        args.ticker = 'BTC'

    
    # 创建记录器（支持多个标的）
    recorder = BBORecorder(tickers=tickers, interval=args.interval)
    
    # 设置信号处理（重要：启用信号处理以确保优雅关闭）
    def signal_handler(sig, frame):
        signal_name = "SIGINT" if sig == signal.SIGINT else "SIGTERM"
        logger.info(f"收到信号 {signal_name}，正在优雅关闭...")
        recorder.stop_flag = True
    
    # 注册信号处理器
    signal.signal(signal.SIGINT, signal_handler)   # Ctrl+C
    signal.signal(signal.SIGTERM, signal_handler)  # kill 默认信号
    
    # 运行记录器（run() 方法中的 finally 会执行 cleanup）
    await recorder.run()
    
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
