"""
Exchange clients module for cross-exchange-arbitrage.
This module provides a unified interface for different exchange implementations.
"""

from .base import BaseExchangeClient, query_retry

# 直接导入可用的exchange客户端
try:
    from .edgex import EdgeXClient
except ImportError:
    pass

try:
    from .lighter import LighterClient
except ImportError:
    pass

__all__ = [
    'BaseExchangeClient', 'query_retry'
]
