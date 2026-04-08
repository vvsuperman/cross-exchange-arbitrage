"""
Simple logger wrapper for trading applications.
"""
import logging
import os


class TradingLogger:
    """Simple logger wrapper that provides a log() method compatible with the trading system."""
    
    def __init__(self, exchange: str, ticker: str, log_to_console: bool = True):
        """Initialize TradingLogger."""
        self.exchange = exchange
        self.ticker = ticker
        self.log_to_console = log_to_console
        
        # Create logger
        logger_name = f"{exchange}_{ticker}"
        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(logging.INFO)
        
        # Clear existing handlers
        self.logger.handlers.clear()
        
        # Create file handler
        # os.makedirs("logs", exist_ok=True)
        # log_filename = f"logs/{exchange}_{ticker}_log.txt"
        # file_handler = logging.FileHandler(log_filename)
        # file_handler.setLevel(logging.INFO)
        
        # Create formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        # file_handler.setFormatter(formatter)
        
        # # Add file handler
        # self.logger.addHandler(file_handler)
        
        # Add console handler if requested
        if log_to_console:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)
    
    def log(self, message: str, level: str = "INFO"):
        """Log a message with the specified level."""
        level = level.upper()
        if level == "DEBUG":
            self.logger.debug(message)
        elif level == "INFO":
            self.logger.info(message)
        elif level == "WARNING":
            self.logger.warning(message)
        elif level == "ERROR":
            self.logger.error(message)
        elif level == "CRITICAL":
            self.logger.critical(message)
        else:
            self.logger.info(message)

