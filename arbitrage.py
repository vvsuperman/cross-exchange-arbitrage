import asyncio
import sys
import argparse
import os
import signal
from decimal import Decimal
import dotenv

from strategy.edgex_arb import EdgexArb
# from strategy.edgex_arb_old import EdgexArb


def load_symbol_config(tickers_file='tickers.txt'):
    """Load configuration from tickers.txt file.
    
    Expected format: SYMBOL size max-position
    Example: BTC 0.001 0.01
    Reads all valid lines (all symbols).
    
    Returns:
        List of dicts, each containing 'ticker', 'size', and 'max_position'
    """
    if not os.path.exists(tickers_file):
        raise FileNotFoundError(f"Tickers file not found: {tickers_file}")
    
    configs = []
    with open(tickers_file, 'r') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            # 跳过空行和注释行
            if line and not line.startswith('#'):
                parts = line.split()
                if len(parts) < 3:
                    raise ValueError(
                        f"Invalid tickers format at line {line_num}. Expected at least 3 values, got {len(parts)}. "
                        f"Format: SYMBOL size max-position"
                    )
                
                ticker = parts[0]
                size = Decimal(parts[1])
                max_position = Decimal(parts[2])
                
                configs.append({
                    'ticker': ticker,
                    'size': size,
                    'max_position': max_position
                })
    
    if not configs:
        raise ValueError(f"Tickers file is empty or contains no valid entries: {tickers_file}")
    
    return configs


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Cross-Exchange Arbitrage Bot Entry Point',
        formatter_class=argparse.RawDescriptionHelpFormatter
        )

    parser.add_argument('--exchange', type=str, default='edgex',
                        help='Exchange to use (edgex)')
    parser.add_argument('--tickers-file', type=str, default='tickers.txt',
                        help='Path to tickers configuration file (default: tickers.txt)')
    parser.add_argument('--env-file', type=str, default=None,
                        help='Path to .env file (default: use .env if present)')
    return parser.parse_args()


def validate_exchange(exchange):
    """Validate that the exchange is supported."""
    supported_exchanges = ['edgex']
    if exchange.lower() not in supported_exchanges:
        print(f"Error: Unsupported exchange '{exchange}'")
        print(f"Supported exchanges: {', '.join(supported_exchanges)}")
        sys.exit(1)


# Global list to store all bot instances for graceful shutdown
_bots = []


def shutdown_handler(signum, frame):
    """Global shutdown handler for all bots."""
    print(f"\n🛑 Received signal {signum}, shutting down all bots...")
    for bot in _bots:
        if bot and not bot.stop_flag:
            bot.shutdown()


async def run_bot(config):
    """Run a single arbitrage bot instance."""
    bot = None
    try:
        bot = EdgexArb(
            ticker=config['ticker'].upper(),
            order_quantity=config['size'],
            max_position=config['max_position']
        )
        _bots.append(bot)
        # Run the bot (it will setup its own signal handlers, but global handler will override)
        await bot.run()
    except asyncio.CancelledError:
        # Task was cancelled, shutdown the bot
        if bot:
            bot.shutdown()
        raise
    except Exception as e:
        print(f"Error running bot for {config['ticker']}: {e}")
        import traceback
        print(f"Full traceback: {traceback.format_exc()}")
        if bot:
            bot.shutdown()
        raise


async def main():
    """Main entry point that creates and runs the cross-exchange arbitrage bot."""
    args = parse_arguments()

    if args.env_file:
        dotenv.load_dotenv(dotenv_path=args.env_file)
    else:
        dotenv.load_dotenv()

    # Validate exchange
    validate_exchange(args.exchange)

    # Setup global signal handlers
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    try:
        # Load configuration from tickers.txt (all symbols)
        configs = load_symbol_config(args.tickers_file)
        
        print(f"Loaded configuration from {args.tickers_file}:")
        print(f"  Found {len(configs)} ticker(s):")
        for config in configs:
            print(f"    - {config['ticker']}: size={config['size']}, max_position={config['max_position']}")
        
        # Create and run bots for all tickers concurrently
        tasks = []
        for config in configs:
            task = asyncio.create_task(run_bot(config))
            tasks.append(task)
        
        # Wait for all bots to complete (or until one fails)
        # Re-setup signal handlers after all bots are created (they may have overwritten it)
        signal.signal(signal.SIGINT, shutdown_handler)
        signal.signal(signal.SIGTERM, shutdown_handler)
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Check if any bot failed
        for i, result in enumerate(results):
            if isinstance(result, Exception) and not isinstance(result, asyncio.CancelledError):
                print(f"Bot for {configs[i]['ticker']} failed: {result}")

    
    except KeyboardInterrupt:
        print("\n🛑 Received interrupt signal, shutting down all bots...")
        return 0
    except Exception as e:
        print(f"Error running cross-exchange arbitrage: {e}")
        import traceback
        print(f"Full traceback: {traceback.format_exc()}")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
