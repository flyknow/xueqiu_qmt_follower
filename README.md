# 雪球组合 QMT 跟踪交易脚本

> **基于 miniQMT（xtquant）自动跟踪雪球投资组合调仓，以固定金额执行买入/卖出**

---

## 📁 文件结构

```
xueqiu_qmt_follower/
├── config.py          # ⚙️  配置文件（必须修改）
├── xueqiu_client.py   # 雪球数据获取模块
├── qmt_trader.py      # miniQMT 交易执行模块
├── follower.py        # 跟踪主逻辑
├── main.py            # 程序入口
└── logs/              # 运行日志（自动创建）
```

---

## 🚀 快速开始

### 1. 安装依赖

```bash
pip install requests
# xtquant 已内置在 miniQMT 安装目录中，无需 pip 安装
```

### 2. 配置 `config.py`

必须修改以下 4 项：

| 配置项 | 说明 | 示例 |
|--------|------|------|
| `QMT_PATH` | miniQMT `userdata_mini` 目录绝对路径 | `r"C:\国金证券QMT交易端\userdata_mini"` |
| `ACCOUNT_ID` | 资金账号（数字字符串） | `"1234567890"` |
| `PORTFOLIO_ID` | 雪球组合代码 | `"ZH123456"` |
| `XUEQIU_COOKIE` | 雪球登录 Cookie | 见下方获取方法 |

**可选调整：**

```python
FIXED_AMOUNT       = 10000.0   # 每只股票买入固定金额（元）
TRADE_START_TIME   = "09:30"   # 交易开始时间
TRADE_END_TIME     = "14:55"   # 交易结束时间
FOLLOW_INCREASE    = True      # 是否跟随加仓
FOLLOW_DECREASE    = True      # 是否跟随减仓
MAX_DAILY_TRADES   = 20        # 单日最大交易笔数
MAX_SINGLE_ORDER_AMOUNT = 50000 # 单笔最大金额（元）
LIMIT_PROTECTION   = True      # 涨跌停保护
```

### 3. 获取雪球 Cookie

1. Chrome 浏览器登录 [xueqiu.com](https://xueqiu.com)
2. 按 `F12` 打开开发者工具 → Network 标签
3. 刷新页面，点击任意请求
4. 在 Request Headers 中找到 `Cookie:` 行
5. 复制完整 Cookie 字符串粘贴到 `config.py`

> **注意**：Cookie 有效期通常为 30 天，过期后需重新获取

### 4. 确保 miniQMT 已登录

运行脚本前，必须先打开 miniQMT 客户端并成功登录账号。

### 5. 运行

```bash
# 进入脚本目录（或配置 Python 路径指向 xtquant）
cd xueqiu_qmt_follower

# Windows: 需要用 miniQMT 内置的 Python 运行（包含 xtquant 库）
# 方法一：直接用内置 Python
C:\国金证券QMT交易端\bin.x64\python.exe main.py

# 方法二：将 xtquant 目录加入 sys.path（推荐）
python main.py
```

---

## ⚙️ 工作原理

```
每 3 秒
  ↓
轮询雪球消息通知接口
  ↓
检测到"组合调仓"推送？
  ↓ YES
获取最新调仓详情
  ↓
与上次调仓 ID 对比（去重）
  ↓
分类：新建仓 / 清仓 / 加仓 / 减仓
  ↓
风控校验（资金/涨跌停/日限/单笔上限）
  ↓
调用 miniQMT 下单（固定金额买入 / 全仓卖出）
```

**低频轮询防反爬**：
- 每隔 60 秒只请求 1 次"消息通知"接口（轻量接口）
- 仅检测到调仓通知时，才请求 1 次"调仓详情"接口
- 每 5 分钟主动核查 1 次（兜底防漏单）

---

## 🛡️ 风控机制

| 风控项 | 说明 |
|--------|------|
| 单笔上限 | `MAX_SINGLE_ORDER_AMOUNT`，默认 5 万元 |
| 日交易次数 | `MAX_DAILY_TRADES`，默认 20 笔/日 |
| 可用资金下限 | 可用资金不足总资产 5% 时禁止新建仓 |
| 涨停保护 | 股价接近涨停（+9.5%+）时不追买 |
| 跌停提示 | 股价接近跌停时发出警告（不强制卖，防止无法成交） |
| 重复下单保护 | 记录最后执行的调仓 ID，防止重复下单 |
| T+1 保护 | 自动识别当日买入的股票（可用量=0），不会重复卖出 |

---

## 💡 买入规则说明

- **新建仓**：组合新增股票 → 买入 `FIXED_AMOUNT` 元
- **加仓**：组合某股票权重上升 → 再买入 `FIXED_AMOUNT` 元
- **减仓**：组合某股票权重下降 → 按比例卖出持仓
  - 例：权重 30%→20%，减少 33%，则卖出持仓的 33%
- **清仓**：组合清除某股票 → 全部卖出该股票持仓

---

## ⚠️ 注意事项

1. **本脚本仅适用于 A 股（SH/SZ/BJ），不适用港股和美股实盘**
2. **Cookie 泄露有安全风险，请勿分享 config.py 给他人**
3. **雪球 API 为非官方接口，可能随时变更，需关注更新**
4. **实盘使用前请先用模拟账号充分测试**
5. **网络中断或 miniQMT 断线时，脚本会报错但不崩溃，重启即可**
6. **跟单有滞后性（1-5秒），高波动市场中可能出现追高/追低**

---

## 🔧 常见问题

**Q: 提示 `xtquant 未安装，进入模拟模式`**

A: 需要将 xtquant 目录加入 Python 路径，或使用 miniQMT 自带的 Python 运行脚本。路径通常为：
```python
import sys
sys.path.append(r"C:\国金证券QMT交易端\bin.x64\Lib\site-packages")
```

**Q: 雪球接口返回 401 / Cookie 失效**

A: 重新从浏览器获取 Cookie 并更新 `config.py`。

**Q: 调仓通知检测不到**

A: 检查是否已在雪球 App 中**关注**目标组合，并开启消息通知。程序同时有 5 分钟兜底轮询，不会漏掉调仓。

**Q: 下单后立即被撤单**

A: 可能是账号权限问题或价格超出涨跌停限制。查看 logs/ 目录下的日志文件获取详细信息。
