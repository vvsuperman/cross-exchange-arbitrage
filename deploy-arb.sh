#!/bin/bash

# 基本配置

HOST="root@47.91.11.166" 
#HOST="root@47.79.137.72"  # lumao2
PROJECT_PATH="/home/admin/myproject"

ENV_FILE="arb.env"
#ENV_FILE="simple.env"
# if [ "$HOST" = "root@47.79.137.72" ]; then
#     ENV_FILE="simple.env"
# fi

# 删除旧文件并打包
rm -f ../cross-exchange-arbitrage-real.zip
# 打包当前目录 (.) 到上一级目录的 zip 文件中
zip -r ../cross-exchange-arbitrage-real.zip . \
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
scp ../cross-exchange-arbitrage-real.zip $HOST:$PROJECT_PATH/

ssh "$HOST" "bash -s" << 'EOF'
    # 停止旧进程
    pkill -9 -f 'arbitrage.py'
    
    cd /home/admin/myproject
    source lumao_env/bin/activate
    
    # 准备目录
    if [ ! -d "cross-exchange-arbitrage-real" ]; then
        mkdir -p cross-exchange-arbitrage-real
    fi
    mv cross-exchange-arbitrage-real.zip cross-exchange-arbitrage-real/
    
    cd cross-exchange-arbitrage-real
    rm -rf cross-exchange-arbitrage
    
    # 解压覆盖到 cross-exchange-arbitrage 目录
    unzip -o cross-exchange-arbitrage-real.zip -d cross-exchange-arbitrage
    cd cross-exchange-arbitrage
    # 启动应用
    # 配置从 strategy/symbol.txt 文件读取
    nohup python arbitrage.py --env-file "$ENV_FILE" > nohup.out 2>&1 &
    
    sleep 2
    tail -f nohup.out
EOF