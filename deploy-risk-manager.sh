#!/bin/bash

# 基本配置

HOST="root@47.91.11.166" 
#HOST="root@47.79.137.72"  # lumao2
PROJECT_PATH="/home/admin/myproject"

ENV_FILE="arb.env"

# 删除旧文件并打包
rm -f ../cross-exchange-arbitrage-risk.zip
# 打包当前目录 (.) 到上一级目录的 zip 文件中
zip -r ../cross-exchange-arbitrage-risk.zip . \
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
scp ../cross-exchange-arbitrage-risk.zip $HOST:$PROJECT_PATH/

ssh "$HOST" "bash -s" << 'EOF'
    # 停止旧进程
    pkill -9 -f 'arb_risk_manager.py'
    
    cd /home/admin/myproject
    source lumao_env/bin/activate
    
    # 准备目录
    if [ ! -d "cross-exchange-arbitrage-risk" ]; then
        mkdir -p cross-exchange-arbitrage-risk
    fi
    mv cross-exchange-arbitrage-risk.zip cross-exchange-arbitrage-risk/
    
    cd cross-exchange-arbitrage-risk
    rm -rf cross-exchange-arbitrage
    
    # 解压覆盖到 cross-exchange-arbitrage 目录
    unzip -o cross-exchange-arbitrage-risk.zip -d cross-exchange-arbitrage
    cd cross-exchange-arbitrage
    
    # 启动应用
    echo "Starting arb_risk_manager.py ..."
    nohup python strategy/arb_risk_manager.py > nohup_risk.out 2>&1 &
    
    sleep 2
    tail -f nohup_risk.out
EOF
