#!/bin/bash

# 基本配置
HOST="root@47.91.11.166" 
#HOST="root@47.79.137.72"  # seeker1
PROJECT_PATH="/home/admin/myproject"

# 删除旧文件并打包
rm -f ../cross-simple.zip
# 打包当前目录 (.) 到上一级目录的 zip 文件中
zip -r ../cross-simple.zip . \
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

# 上传并部署
scp ../cross-simple.zip $HOST:$PROJECT_PATH/

ssh "$HOST" "bash -s" << 'EOF'
    # 停止旧进程
    pkill -9 -f 'simple_stat.py'
    
    cd /home/admin/myproject
    source lumao_env/bin/activate
    
    # 准备目录
    if [ ! -d "cross-simple" ]; then
        mkdir -p cross-simple
    fi
    mv cross-simple.zip cross-simple/
    
    cd cross-simple
    rm -rf cross-exchange-arbitrage
    
    # 解压覆盖到 cross-exchange-arbitrage 目录
    unzip -o cross-simple.zip -d cross-exchange-arbitrage
    cd cross-exchange-arbitrage
    
    # 启动统计套利应用
    # 配置从 tickers.txt 文件读取标的列表
    nohup python monitor/simple_stat.py > nohup.out 2>&1 &
    
    sleep 2
    tail -f  nohup.out
EOF
