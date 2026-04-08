# 后台管理监控程序使用说明

## 功能特性

1. **价差曲线图展示**
   - 价差1: `(lighter_best_bid - edgex_best_ask) / lighter_best_bid`
   - 价差2: `(edgex_best_bid - edgex_best_ask) / edgex_best_bid`

2. **时间窗口平均**
   - 支持30秒、60秒、120秒、300秒（5分钟）时间窗口
   - 自动处理采集时间不一致的问题

3. **灵活的时间范围选择**
   - 默认显示最近3天的数据
   - 支持自定义开始和结束时间

4. **统计信息**
   - 平均值、中位数、最大值、最小值、标准差

## 安装依赖

```bash
pip install -r requirements.txt
```

主要依赖：
- Flask >= 2.0.0
- pandas >= 1.3.0
- numpy >= 1.21.0

前端图表库（通过CDN加载）：
- Chart.js >= 4.4.0
- chartjs-adapter-date-fns >= 3.0.0

## 启动程序

### 方法1: 使用启动脚本（推荐）

```bash
./monitor/start_monitor.sh
```

### 方法2: 直接运行Python脚本

```bash
cd monitor
python3 monitor_app.py
```

## 访问界面

启动后，在浏览器中访问：

```
http://localhost:5000
```

## 使用说明

1. **选择标的**: 从下拉菜单中选择要查看的交易对（如BTC、ETH等）

2. **选择时间范围**: 
   - 开始时间：数据查询的起始时间
   - 结束时间：数据查询的结束时间
   - 默认是3天前到现在

3. **选择时间窗口**: 
   - 30秒：每30秒取一次平均值（默认）
   - 60秒：每60秒取一次平均值
   - 120秒：每2分钟取一次平均值
   - 300秒：每5分钟取一次平均值

4. **加载数据**: 点击"加载数据"按钮，系统会：
   - 从数据库读取EdgeX和Lighter的BBO数据
   - 按时间窗口进行重采样（取平均值）
   - 计算价差
   - 绘制曲线图
   - 显示统计信息

## 数据来源

程序从以下数据库读取数据：
- `logs/edgex_bbo.db` - EdgeX交易所的BBO数据
- `logs/lighter_bbo.db` - Lighter交易所的BBO数据

确保这些数据库文件存在且包含数据。如果没有数据，请先运行 `monitor/record_exchange_data.py` 来记录数据。

## 技术说明

### 数据处理流程

1. **数据查询**: 从两个独立的SQLite数据库查询指定时间范围内的BBO数据
2. **时间对齐**: 将两个交易所的数据按时间戳对齐
3. **重采样**: 使用pandas的resample功能，按指定时间窗口取平均值
4. **价差计算**: 
   - 价差1 = (lighter_best_bid - edgex_best_ask) / lighter_best_bid
   - 价差2 = (edgex_best_bid - edgex_best_ask) / edgex_best_bid
5. **数据返回**: 服务器返回JSON格式的数据（时间戳和价差值）
6. **图表绘制**: 前端使用Chart.js在浏览器中绘制交互式图表

### 时间窗口处理

由于两个交易所的数据采集时间可能不一致，程序采用以下策略：
- 使用pandas的resample功能，按固定时间窗口（如30秒）进行重采样
- 在每个时间窗口内，对数据进行平均
- 使用前向填充和后向填充来处理缺失值

## 注意事项

1. 确保数据库文件存在且有数据
2. 如果查询的时间范围内没有数据，会显示错误提示
3. 图表显示的是百分比形式的价差（乘以100）
4. 程序默认监听所有网络接口（0.0.0.0），端口5000

## 故障排除

### 问题：无法访问网页
- 检查防火墙设置
- 确认端口5000未被占用
- 检查程序是否正常启动

### 问题：没有数据显示
- 确认数据库文件存在
- 检查数据库中是否有对应标的的数据
- 确认选择的时间范围内有数据

### 问题：图表显示异常
- 检查浏览器控制台是否有JavaScript错误
- 确认Chart.js CDN链接可访问
- 确认数据格式正确
- 检查网络连接（Chart.js通过CDN加载）

## 开发说明

程序结构：
- `monitor_app.py`: Flask应用主文件，包含路由和数据处理逻辑
- `templates/monitor.html`: Web界面模板（包含Chart.js图表代码）
- `start_monitor.sh`: 启动脚本

如需修改：
- 修改时间窗口选项：编辑 `monitor.html` 中的 `<select id="window_seconds">`
- 修改默认时间范围：编辑 `monitor_app.py` 中的 `index()` 函数
- 修改图表样式：编辑 `monitor.html` 中的 `createChart()` 函数（Chart.js配置）
- 图表支持交互功能：缩放、平移、悬停提示等（Chart.js内置功能）

