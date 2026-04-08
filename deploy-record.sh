#!/bin/bash

# BBO记录程序部署脚本
HOST="root@47.91.11.166" 
#HOST="root@47.79.137.72"  # seeker1
#HOST="root@47.79.137.222"  # lumao2
PROJECT_PATH="/home/admin/myproject"

set -e

# 默认标的列表（可以通过环境变量或参数覆盖）
DEFAULT_TICKERS="BTC,ETH,SOL,PAXG"
INTERVAL=5
TICKERS_FILE="tickers.txt"

# 解析命令行参数
# 用法: ./deploy_record_bbo.sh [tickers|tickers_file] [interval]
# 例如: ./deploy_record_bbo.sh BTC,ETH 30
# 或者: ./deploy_record_bbo.sh tickers.txt 30

if [ -n "$1" ]; then
    if [ -f "$1" ]; then
        # 如果第一个参数是文件，则从文件读取
        TICKERS_FILE="$1"
        TICKERS=""  # 空字符串表示从文件读取
        INTERVAL="${2:-$INTERVAL}"
    else
        # 否则作为ticker列表
        TICKERS="$1"
        TICKERS_FILE=""  # 不使用文件
        INTERVAL="${2:-$INTERVAL}"
    fi
else
    # 没有参数时，默认使用 tickers.txt 文件（如果存在），否则使用默认列表
    if [ -f "$TICKERS_FILE" ]; then
        TICKERS=""  # 空字符串表示从文件读取
    else
        TICKERS="$DEFAULT_TICKERS"
        TICKERS_FILE=""  # 不使用文件
    fi
    INTERVAL="${2:-$INTERVAL}"
fi

echo "=========================================="
echo "BBO记录程序部署脚本"
echo "=========================================="
if [ -n "$TICKERS" ]; then
    echo "标的列表: $TICKERS"
else
    echo "标的文件: $TICKERS_FILE"
fi
echo "采样间隔: ${INTERVAL}秒"
echo "目标服务器: $HOST"
echo "=========================================="

# 删除旧文件并打包
rm -f ../cross-exchange-arbitrage.zip
# 打包当前目录 (.) 到上一级目录的 zip 文件中
zip -r ../cross-exchange-arbitrage.zip . \
    -x "*.log" \
    -x "*.git/*" \
    -x "__pycache__/*" \
    -x "*.pyc" \
    -x "*.csv" \
    -x "keys_*.py" \
    -x "*.lock" \
    -x "venv/*" \
    -x ".DS_Store" \
    -x ".idea/*" \
    -x ".vscode/*" \
    -x "arbtrage/downloaded_files/*" \
    -x "**/downloaded_files/*" \
    -x "**/.pytest_cache/*" \
    -x "*.db" 

echo "✅ 代码打包完成"

# 上传并部署
scp ../cross-exchange-arbitrage.zip $HOST:$PROJECT_PATH/

echo "✅ 代码上传完成"

ssh "$HOST" "bash -s" << EOF
    # 停止旧的记录进程
    echo "正在停止旧的记录进程..."
    pkill -9 -f 'record_exchange_data.py' || true
    pkill -9 -f 'monitor_app.py' || true
    
    cd $PROJECT_PATH
    source lumao_env/bin/activate
    
    # 准备目录
    mkdir -p cross-exchange-arbitrage
    mv cross-exchange-arbitrage.zip cross-exchange-arbitrage/ 2>/dev/null || true
    cd cross-exchange-arbitrage
    
    # 解压覆盖
    unzip -o cross-exchange-arbitrage.zip
    
    # 确保logs目录存在
    mkdir -p logs
    
    # 解析标的列表
    # 注意：TICKERS 和 TICKERS_FILE 在本地已展开，空字符串会正确传递
    if [ -n "$TICKERS" ]; then
        # 从命令行参数读取，转换为逗号分隔的字符串
        TICKER_LIST="$TICKERS"
    else
        # 从文件读取，转换为逗号分隔的字符串
        # TICKERS_FILE 已在本地展开为实际值（如 "tickers.txt"）
        TICKERS_FILE_VALUE="$TICKERS_FILE"
        if [ -f "\$TICKERS_FILE_VALUE" ]; then
            TICKER_LIST=""
            while IFS= read -r line || [ -n "\$line" ]; do
                ticker=\$(echo "\$line" | sed 's/#.*//' | xargs | tr '[:lower:]' '[:upper:]')  # 去除注释和空格并转大写
                if [ -n "\$ticker" ]; then
                    if [ -z "\$TICKER_LIST" ]; then
                        TICKER_LIST="\$ticker"
                    else
                        TICKER_LIST="\$TICKER_LIST,\$ticker"
                    fi
                fi
            done < "\$TICKERS_FILE_VALUE"
        else
            echo "❌ 错误: 找不到标的文件 \$TICKERS_FILE_VALUE"
            echo "当前目录: \$(pwd)"
            echo "文件列表:"
            ls -la *.txt 2>/dev/null || echo "没有找到 .txt 文件"
            exit 1
        fi
    fi
    
    echo "=========================================="
    echo "启动BBO记录进程"
    echo "=========================================="
    echo "标的列表: \$TICKER_LIST"
    echo "采样间隔: ${INTERVAL}秒"
    echo ""
    
    # 启动单个进程处理所有标的
    if [ -n "$TICKERS" ]; then
        # 使用命令行参数
        nohup python monitor/record_exchange_data.py --ticker "\$TICKER_LIST" --interval $INTERVAL > logs/record_bbo.log 2>&1 &
    else
        # 使用文件（TICKERS_FILE 已在本地展开）
        TICKERS_FILE_VALUE="$TICKERS_FILE"
        nohup python monitor/record_exchange_data.py --tickers-file "\$TICKERS_FILE_VALUE" --interval $INTERVAL > logs/record_bbo.log 2>&1 &
    fi

    nohup python monitor/monitor_app.py > logs/monitor_app.log 2>&1 &
    
    PID=\$!
    echo "✅ BBO记录进程已启动 (PID: \$PID)"
    
    echo "=========================================="
    echo "记录进程已启动"
    echo "=========================================="
    
    # 等待一下，然后显示进程状态
    sleep 2
    echo ""
    echo "当前运行的记录进程:"
    ps aux | grep '[r]ecord_bbo.py' || echo "未找到运行中的进程"
    

EOF

echo ""
echo "=========================================="
echo "部署完成！"
echo "=========================================="
echo ""
echo "查看日志:"
ssh $HOST 'tail -f /home/admin/myproject/cross-exchange-arbitrage/logs/record_bbo.log'
echo ""
echo "停止记录进程:"
echo "  ssh $HOST 'pkill -f record_bbo.py'"
echo ""
echo "查看进程状态:"
echo "  ssh $HOST 'ps aux | grep record_bbo.py'"
echo ""

