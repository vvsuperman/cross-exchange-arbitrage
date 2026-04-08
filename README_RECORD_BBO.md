# BBO数据记录程序使用说明

## 功能说明

`record_bbo.py` 是一个独立的程序，用于记录 EdgeX 和 Lighter 交易所的 best bid、best ask、size 和时间数据。

### 主要特性

- ✅ 支持多个标的，每个标的单独保存到独立的CSV文件
- ✅ 可配置采样间隔（默认30秒）
- ✅ 支持从命令行或文件读取标的列表
- ✅ 自动连接和重连机制
- ✅ 优雅的进程管理和信号处理

## 本地使用

### 基本用法

```bash
# 记录单个标的（BTC）
python record_bbo.py --ticker BTC

# 记录多个标的（逗号分隔）
python record_bbo.py --ticker BTC,ETH,SOL

# 指定采样间隔（60秒）
python record_bbo.py --ticker BTC --interval 60

# 从文件读取标的列表
python record_bbo.py --tickers-file tickers.txt --interval 30
```

### 标的文件格式

创建 `tickers.txt` 文件，每行一个标的：

```
# BBO记录标的列表
# 以#开头的行为注释
BTC
ETH
SOL
# MATIC  # 被注释的标的不会被执行
```

## 服务器部署

### 使用部署脚本

```bash
# 部署并启动记录程序（使用默认标的：BTC,ETH）
./deploy_record_bbo.sh

# 指定标的列表
./deploy_record_bbo.sh "BTC,ETH,SOL" 30

# 从文件读取标的列表
./deploy_record_bbo.sh tickers.txt 30
```

### 部署脚本功能

1. **打包代码**：自动打包项目代码（排除日志、CSV等文件）
2. **上传到服务器**：通过SCP上传到指定服务器
3. **停止旧进程**：自动停止之前运行的记录进程
4. **启动新进程**：为每个标的启动独立的记录进程
5. **显示状态**：显示进程状态和日志文件位置

### 服务器管理命令

```bash
# 查看运行中的记录进程
ssh root@47.91.11.166 'ps aux | grep record_bbo.py'

# 查看日志
ssh root@47.91.11.166 'tail -f /home/admin/myproject/cross-exchange-arbitrage/logs/record_bbo_BTC.log'

# 停止所有记录进程
ssh root@47.91.11.166 'pkill -f record_bbo.py'

# 查看CSV文件
ssh root@47.91.11.166 'ls -lh /home/admin/myproject/cross-exchange-arbitrage/logs/bbo_record_*.csv'
```

## 输出文件

### CSV文件格式

每个标的会生成一个独立的CSV文件：`logs/bbo_record_{TICKER}.csv`

CSV文件包含以下列：

| 列名 | 说明 |
|------|------|
| timestamp | 时间戳（ISO格式，UTC时区） |
| edgex_best_bid | EdgeX最佳买价 |
| edgex_best_bid_size | EdgeX最佳买价对应的数量 |
| edgex_best_ask | EdgeX最佳卖价 |
| edgex_best_ask_size | EdgeX最佳卖价对应的数量 |
| lighter_best_bid | Lighter最佳买价 |
| lighter_best_bid_size | Lighter最佳买价对应的数量 |
| lighter_best_ask | Lighter最佳卖价 |
| lighter_best_ask_size | Lighter最佳卖价对应的数量 |

### 日志文件

每个标的会生成一个独立的日志文件：`logs/record_bbo_{TICKER}.log`

## 配置说明

### 环境变量

程序需要以下环境变量（与主程序相同）：

- `EDGEX_ACCOUNT_ID`: EdgeX账户ID
- `EDGEX_STARK_PRIVATE_KEY`: EdgeX Stark私钥
- `API_KEY_PRIVATE_KEY`: Lighter API密钥私钥
- `LIGHTER_ACCOUNT_INDEX`: Lighter账户索引
- `LIGHTER_API_KEY_INDEX`: Lighter API密钥索引

### 部署脚本配置

编辑 `deploy_record_bbo.sh` 修改以下配置：

```bash
HOST="root@47.91.11.166"  # 服务器地址
PROJECT_PATH="/home/admin/myproject"  # 项目路径
DEFAULT_TICKERS="BTC,ETH"  # 默认标的列表
INTERVAL=30  # 默认采样间隔（秒）
```

## 注意事项

1. **资源占用**：每个标的会启动一个独立的进程，请根据服务器资源合理配置标的数量
2. **网络连接**：确保服务器可以访问 EdgeX 和 Lighter 的API和WebSocket
3. **磁盘空间**：CSV文件会持续增长，请定期清理或归档
4. **进程管理**：使用 `pkill -f record_bbo.py` 可以停止所有记录进程

## 故障排查

### 无法连接交易所

- 检查环境变量是否正确设置
- 检查网络连接是否正常
- 查看日志文件了解详细错误信息

### 数据缺失

- 检查WebSocket连接是否正常
- 查看日志文件确认是否有错误
- 确认标的名称是否正确（区分大小写）

### 进程意外停止

- 查看日志文件了解停止原因
- 检查服务器资源（内存、CPU）
- 使用部署脚本重新启动进程

## 示例

### 本地测试

```bash
# 测试单个标的
python record_bbo.py --ticker BTC --interval 10

# 测试多个标的
python record_bbo.py --ticker BTC,ETH --interval 30
```

### 服务器部署

```bash
# 部署到服务器，记录BTC和ETH
./deploy_record_bbo.sh "BTC,ETH" 30

# 使用文件配置部署
./deploy_record_bbo.sh tickers.txt 60
```

