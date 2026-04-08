#!/usr/bin/env python3
"""
Redis客户端封装类，用于存储价格数据
使用合理的数据结构存储BBO数据：
- Hash: 存储每个交易所+交易对的最新BBO数据
"""
import json
import logging
import os
from decimal import Decimal
from datetime import datetime
from typing import Optional, Dict, Any
import redis
import redis.asyncio as aioredis
from dotenv import load_dotenv
import asyncio

logger = logging.getLogger('redis_client')

# 加载环境变量
load_dotenv()


class RedisPriceClient:
    """Redis价格数据客户端"""
    
    def __init__(self, host: Optional[str] = None, port: int = 6379, 
                 db: int = 0, password: Optional[str] = None,
                 decode_responses: bool = True):
        """
        初始化Redis客户端
        
        Args:
            host: Redis服务器地址，默认从环境变量REDIS_HOST获取，或使用localhost
            port: Redis服务器端口，默认6379
            db: Redis数据库编号，默认0
            password: Redis密码，默认从环境变量REDIS_PASSWORD获取
            decode_responses: 是否自动解码响应为字符串，默认True
        """
        self.host = host or os.getenv('REDIS_HOST', '172.18.58.232')
        self.port = port or int(os.getenv('REDIS_PORT', '6379'))
        self.db = db or int(os.getenv('REDIS_DB', '0'))
        self.password = password or os.getenv('REDIS_PASSWORD')
        
        self.decode_responses = decode_responses
        
        try:
            self.client = redis.Redis(
                host=self.host,
                port=self.port,
                db=self.db,
                password=self.password,
                decode_responses=decode_responses,
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_timeout=True
            )
            # 测试连接
            self.client.ping()
            logger.info(f"Redis连接成功: {self.host}:{self.port}/{self.db}")
        except redis.ConnectionError as e:
            logger.error(f"Redis连接失败: {e}")
            raise
        except Exception as e:
            logger.error(f"Redis初始化失败: {e}")
            raise
        
        # 异步客户端（用于订阅，延迟初始化）
        self._async_client = None
    
    def _get_latest_key(self, exchange: str, ticker: str) -> str:
        """获取最新价格数据的key"""
        return f"bbo:latest:{exchange.lower()}:{ticker.upper()}"
    
    def _get_history_key(self, exchange: str, ticker: str) -> str:
        """获取历史价格数据的key"""
        return f"bbo:history:{exchange.lower()}:{ticker.upper()}"
    
    def store_latest_bbo(self, exchange: str, ticker: str,
                        best_bid: Optional[Decimal], best_bid_size: Optional[Decimal],
                        best_ask: Optional[Decimal], best_ask_size: Optional[Decimal],
                        price_timestamp: Optional[str],
                        timestamp: Optional[str] = None,
                        edgex_gap: Optional[int] = None,
                        edgex_start_version: Optional[int] = None,
                        edgex_end_version: Optional[int] = None,
                        lighter_exchange_ts_ms: Optional[float] = None,
                        lighter_offset: Optional[int] = None) -> bool:
        """
        存储最新的BBO数据到Redis Hash。多实例写时：EdgeX仅当end_version更新时覆盖，Lighter仅当timestamp更新时覆盖。
        
        Args:
            exchange, ticker, best_bid, best_ask 等: 基础字段
            edgex_gap: EdgeX版本间隔(startVersion-lastEndVersion)，用于延迟/漏更新判断
            edgex_start_version, edgex_end_version: EdgeX版本号，用于条件写(end_version>redis才覆盖)
            lighter_exchange_ts_ms: Lighter交易所时间戳(ms)，用于条件写(timestamp>redis才覆盖)和延迟判断
            lighter_offset: Lighter offset，用于记录
        
        Returns:
            是否存储成功
        """
        if timestamp is None:
            timestamp = datetime.now().isoformat()
        
        try:
            key = self._get_latest_key(exchange, ticker)
            history_key = self._get_history_key(exchange, ticker)
            ex = exchange.lower()
            
            # 多实例条件写：先读再比较
            if ex == 'edgex' and edgex_end_version is not None:
                current = self.client.hget(key, 'end_version')
                if current is not None:
                    try:
                        if edgex_end_version <= int(current):
                            return False  # 不比当前新，不覆盖
                    except (ValueError, TypeError):
                        pass
            elif ex == 'lighter' and lighter_exchange_ts_ms is not None:
                current = self.client.hget(key, 'exchange_ts_ms')
                if current is not None:
                    try:
                        if lighter_exchange_ts_ms <= float(current):
                            return False  # 不比当前新，不覆盖
                    except (ValueError, TypeError):
                        pass
            
            hash_data = {
                'best_bid': str(best_bid) if best_bid is not None else '',
                'best_bid_size': str(best_bid_size) if best_bid_size is not None else '',
                'best_ask': str(best_ask) if best_ask is not None else '',
                'best_ask_size': str(best_ask_size) if best_ask_size is not None else '',
                'price_timestamp': price_timestamp or '',
                'timestamp': timestamp,
                'exchange': ex,
                'ticker': ticker.upper()
            }
            if ex == 'edgex':
                if edgex_gap is not None:
                    hash_data['gap'] = str(edgex_gap)
                if edgex_start_version is not None:
                    hash_data['start_version'] = str(edgex_start_version)
                if edgex_end_version is not None:
                    hash_data['end_version'] = str(edgex_end_version)
            elif ex == 'lighter':
                if lighter_exchange_ts_ms is not None:
                    hash_data['exchange_ts_ms'] = str(int(lighter_exchange_ts_ms))
                if lighter_offset is not None:
                    hash_data['offset'] = str(lighter_offset)
            
            self.client.hset(key, mapping=hash_data)
            
            # 设置过期时间（24小时），防止数据堆积
            self.client.expire(key, 86400)
            
            # 将数据添加到历史列表（最近1000条）
            history_data = json.dumps(hash_data)
            self.client.lpush(history_key, history_data)
            # 保持列表长度为1000条（索引0-999）
            self.client.ltrim(history_key, 0, 999)
            # 设置历史数据的过期时间（1小时），防止数据堆积
            self.client.expire(history_key, 7200)
            
            return True
        except Exception as e:
            logger.error(f"存储最新BBO数据失败 [{exchange}:{ticker}]: {e}", exc_info=True)
            return False
    
    def get_latest_bbo(self, exchange: str, ticker: str) -> Optional[Dict[str, Any]]:
        """
        获取最新的BBO数据
        
        Args:
            exchange: 交易所名称
            ticker: 交易对符号
        
        Returns:
            BBO数据字典，如果不存在则返回None
        """
        try:
            key = self._get_latest_key(exchange, ticker)
            data = self.client.hgetall(key)
            
            if not data:
                return None
            
            result = {
                'best_bid': Decimal(data['best_bid']) if data.get('best_bid') else None,
                'best_bid_size': Decimal(data['best_bid_size']) if data.get('best_bid_size') else None,
                'best_ask': Decimal(data['best_ask']) if data.get('best_ask') else None,
                'best_ask_size': Decimal(data['best_ask_size']) if data.get('best_ask_size') else None,
                'price_timestamp': data.get('price_timestamp') or None,
                'timestamp': data.get('timestamp'),
                'exchange': data.get('exchange'),
                'ticker': data.get('ticker')
            }
            if data.get('gap') is not None:
                try:
                    result['gap'] = int(data['gap'])
                except (ValueError, TypeError):
                    result['gap'] = None
            else:
                result['gap'] = None
            if data.get('start_version') is not None:
                try:
                    result['start_version'] = int(data['start_version'])
                except (ValueError, TypeError):
                    result['start_version'] = None
            else:
                result['start_version'] = None
            if data.get('end_version') is not None:
                try:
                    result['end_version'] = int(data['end_version'])
                except (ValueError, TypeError):
                    result['end_version'] = None
            else:
                result['end_version'] = None
            if data.get('exchange_ts_ms') is not None:
                try:
                    result['exchange_ts_ms'] = float(data['exchange_ts_ms'])
                except (ValueError, TypeError):
                    result['exchange_ts_ms'] = None
            else:
                result['exchange_ts_ms'] = None
            if data.get('offset') is not None:
                try:
                    result['offset'] = int(data['offset'])
                except (ValueError, TypeError):
                    result['offset'] = None
            else:
                result['offset'] = None
            return result
        except Exception as e:
            logger.error(f"获取最新BBO数据失败 [{exchange}:{ticker}]: {e}", exc_info=True)
            return None
            
    def set_risk_status(self, exchange: str, ticker: str, risk_code: str, ttl: int = 5) -> bool:
        """
        Store the global risk status in Redis.
        risk_code: string representing risk anomalies (e.g. "00000" for fresh/safe)
        ttl: Expiration stringency (defaults to 5s to fail-safe if risk module crashes).
        """
        try:
            key = f"risk:stale_flag:{exchange.lower()}:{ticker.upper()}"
            self.client.setex(key, ttl, risk_code)
            return True
        except Exception as e:
            logger.error(f"Failed to set risk status [{exchange}:{ticker}]: {e}")
            return False

    def get_risk_status(self, exchange: str, ticker: str) -> str:
        """
        Get the global risk status from Redis.
        Fail-safe: Returns "11111" (stale/unsafe) if the key does NOT exist.
        """
        try:
            key = f"risk:stale_flag:{exchange.lower()}:{ticker.upper()}"
            val = self.client.get(key)
            if val is None:
                # Fail-safe: If the daemon isn't sending heartbeats, assume STALE
                return "11111"
            
            # backward compatibility
            if val == "1":
                return "11111"
            elif val == "0":
                return "00000"
                
            return val
        except Exception as e:
            logger.error(f"Failed to get risk status [{exchange}:{ticker}]: {e}")
            # Fail-safe on error
            return "11111"
    
    def get_history_bbo(self, exchange: str, ticker: str, limit: int = 1000) -> list:
        """
        获取最近的历史BBO数据
        
        Args:
            exchange: 交易所名称
            ticker: 交易对符号
            limit: 获取的记录数量，默认1000条
        
        Returns:
            BBO数据列表，按时间从新到旧排序，如果不存在则返回空列表
        """
        try:
            history_key = self._get_history_key(exchange, ticker)
            
            # 从Redis列表获取数据（索引0到limit-1，最新的在前）
            raw_data = self.client.lrange(history_key, 0, limit - 1)
            
            if not raw_data:
                return []
            
            # 解析JSON数据并转换类型
            result = []
            for item in raw_data:
                try:
                    data = json.loads(item)
                    # 转换数据类型
                    parsed_item = {
                        'best_bid': Decimal(data['best_bid']) if data.get('best_bid') else None,
                        'best_bid_size': Decimal(data['best_bid_size']) if data.get('best_bid_size') else None,
                        'best_ask': Decimal(data['best_ask']) if data.get('best_ask') else None,
                        'best_ask_size': Decimal(data['best_ask_size']) if data.get('best_ask_size') else None,
                        'price_timestamp': data.get('price_timestamp') or None,
                        'timestamp': data.get('timestamp'),
                        'exchange': data.get('exchange'),
                        'ticker': data.get('ticker')
                    }
                    result.append(parsed_item)
                except (json.JSONDecodeError, KeyError, ValueError) as e:
                    logger.warning(f"解析历史BBO数据失败 [{exchange}:{ticker}]: {e}")
                    continue
            
            return result
        except Exception as e:
            logger.error(f"获取历史BBO数据失败 [{exchange}:{ticker}]: {e}", exc_info=True)
            return []
    
    def publish_trading_signal(self, symbol: str, edgex_direction: str, lighter_direction: str, client_order_id: str = None) -> bool:
        """
        发布交易信号到Redis (同时支持 List 和 Pub/Sub)
        
        Args:
            symbol: 交易对符号
            edgex_direction: EdgeX方向 ('buy' 或 'sell')
            lighter_direction: Lighter方向 ('buy' 或 'sell')
            client_order_id: 客户端订单ID（可选，如果为None则自动生成）
        
        Returns:
            是否发布成功
        """
        try:
            # 如果没有提供client_order_id，生成一个
            if client_order_id is None:
                import time
                client_order_id = str(int(time.time() * 1000))
            
            # 格式: symbol+{edgex_direction}+edgex+{lighter_direction}+lighter+{client_order_id}
            # 例如: BTC+sell+edgex+buy+lighter+1234567890
            signal_message = f"{symbol}+{edgex_direction}+edgex+{lighter_direction}+lighter+{client_order_id}"
            
            # 1. 存入 list (兼容原有的轮询模式)
            key = f"trading_signals:{symbol.upper()}"
            self.client.rpush(key, signal_message)
            self.client.expire(key, 3600)
            
            # 2. 发布到频道 (支持推送模式)
            channel = f"trading_channel:{symbol.upper()}"
            self.client.publish(channel, signal_message)
            
            logger.info(f"发布交易信号:channel {channel} singal {signal_message}")
            return True
        except Exception as e:
            logger.error(f"发布交易信号失败 [{symbol}]: {e}", exc_info=True)
            return False
    
    def get_trading_signal(self, symbol: str) -> Optional[str]:
        """
        获取交易信号（从左侧弹出，FIFO）
        
        Args:
            symbol: 交易对符号
        
        Returns:
            信号消息，如果不存在则返回None
        """
        try:
            key = f"trading_signals:{symbol.upper()}"
            # 从左侧弹出（LPOP），非阻塞，如果没有数据返回None
            signal = self.client.lpop(key)
            return signal
        except Exception as e:
            logger.error(f"获取交易信号失败 [{symbol}]: {e}", exc_info=True)
            return None
    
    def close(self):
        """关闭Redis连接（同步）"""
        try:
            if self.client:
                self.client.close()
                logger.info("Redis连接已关闭")
        except Exception as e:
            logger.warning(f"关闭Redis连接时出错: {e}")
    
    async def close_async(self):
        """关闭异步Redis连接"""
        try:
            if self._async_client:
                await self._async_client.close()
                self._async_client = None
                logger.info("异步Redis连接已关闭")
        except Exception as e:
            logger.warning(f"关闭异步Redis连接时出错: {e}")

    async def _get_async_client(self):
        """获取或创建异步 Redis 客户端"""
        if self._async_client is None:
            self._async_client = aioredis.Redis(
                host=self.host,
                port=self.port,
                db=self.db,
                password=self.password,
                decode_responses=self.decode_responses,
                socket_connect_timeout=5,
                socket_timeout=5,
            )
            # 测试连接
            await self._async_client.ping()
            logger.info(f"异步Redis连接成功: {self.host}:{self.port}/{self.db}")
        return self._async_client

    async def listen_trading_signals(self, symbol: str):
        """
        监听交易信号频道 (推送模式) - 使用原生异步 pubsub，无线程切换开销
        
        Args:
            symbol: 交易对符号
        
        Yields:
            信号消息字符串
        """
        channel = f"trading_channel:{symbol.upper()}"
        
        # 为 pubsub 创建专用客户端，socket_timeout=None 避免超时断开
        pubsub_client = aioredis.Redis(
            host=self.host,
            port=self.port,
            db=self.db,
            password=self.password,
            decode_responses=self.decode_responses,
            socket_connect_timeout=5,
            socket_timeout=None,  # Pub/Sub 需要长连接，不能超时
        )
        pubsub = pubsub_client.pubsub()
        await pubsub.subscribe(channel)
        
        logger.info(f"开始监听Redis推送频道 (原生异步): {channel}")
        
        try:
            async for message in pubsub.listen():
                if message['type'] == 'message':
                    logger.info(f"🧧收到Redis推送消息: {channel} {message['data']}")
                    yield message['data']
                elif message['type'] == 'subscribe':
                    logger.info(f"✅ Redis订阅就绪: {channel}")
        except asyncio.CancelledError:
            logger.info(f"Redis订阅任务被取消: {channel}")
            raise
        except Exception as e:
            logger.error(f"监听Redis推送消息时出错 [{channel}]: {e}", exc_info=True)
            raise
        finally:
            try:
                await pubsub.unsubscribe(channel)
                await pubsub.close()
                await pubsub_client.close()
                logger.info(f"已关闭Redis订阅: {channel}")
            except Exception as e:
                logger.warning(f"关闭Redis pubsub时出错: {e}")
