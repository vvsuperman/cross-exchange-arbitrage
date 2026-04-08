#!/bin/bash
# 启动后台管理监控程序

cd "$(dirname "$0")/.."

# 检查Python环境
if ! command -v python3 &> /dev/null; then
    echo "错误: 未找到 python3"
    exit 1
fi

# 检查是否安装了依赖
echo "检查依赖..."
python3 -c "import flask, pandas, matplotlib, numpy" 2>/dev/null
if [ $? -ne 0 ]; then
    echo "警告: 部分依赖可能未安装，请运行: pip install -r requirements.txt"
fi

# 启动Flask应用
echo "启动监控程序..."
echo "访问地址: http://localhost:5000"
echo "按 Ctrl+C 停止服务"
echo ""

cd monitor
python3 monitor_app.py

