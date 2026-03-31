"""
qmt_trader.py
────────────────────────────────────────────────────────────
miniQMT 交易执行模块

功能：
  1. 连接 miniQMT 交易端
  2. 查询账户资产、持仓
  3. 固定金额买入 / 卖出（市价单 & 限价单）
  4. 回调处理（委托状态、成交回报、错误）
────────────────────────────────────────────────────────────
"""

import time
import math
import logging
from typing import Optional, Dict, List

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# 懒加载 xtquant，避免在非 QMT 环境下直接报错
# ─────────────────────────────────────────────────────────────
try:
    from xtquant.xttrader import XtQuantTrader, XtQuantTraderCallback
    from xtquant.xttype import StockAccount
    from xtquant import xtconstant, xtdata
    HAS_XTQUANT = True
except ImportError:
    HAS_XTQUANT = False
    logger.warning("xtquant 未安装，进入模拟模式（仅打印交易指令）")


# ─────────────────────────────────────────────────────────────
# 回调类
# ─────────────────────────────────────────────────────────────
class _TraderCallback:
    """XtQuantTraderCallback 的简单封装，统一输出日志"""

    def on_disconnected(self):
        logger.error("【QMT】连接断开！请检查 miniQMT 客户端是否在线")

    def on_stock_order(self, order):
        logger.info(
            f"【委托回报】{order.stock_code} "
            f"{'买入' if order.order_type == 23 else '卖出'} "
            f"{order.order_volume}股 "
            f"价格={order.price:.3f} "
            f"状态={order.order_status} "
            f"备注={order.order_remark}"
        )

    def on_stock_trade(self, trade):
        logger.info(
            f"【成交回报】{trade.stock_code} "
            f"成交{trade.traded_volume}股 "
            f"均价={trade.traded_price:.3f} "
            f"备注={trade.order_remark}"
        )

    def on_order_error(self, order_error):
        logger.error(
            f"【委托失败】{order_error.stock_code} "
            f"错误={order_error.error_msg} "
            f"seq={order_error.order_sysid}"
        )

    def on_cancel_error(self, cancel_error):
        logger.error(f"【撤单失败】{cancel_error.order_sysid} 错误={cancel_error.error_msg}")

    def on_account_status(self, status):
        logger.debug(f"账户状态变更: {status.account_id} -> {status.status}")


# ─────────────────────────────────────────────────────────────
# 主交易类
# ─────────────────────────────────────────────────────────────
class QMTTrader:
    """miniQMT 交易接口封装"""

    def __init__(self, qmt_path: str, account_id: str, account_type: str = "STOCK"):
        """
        Args:
            qmt_path:     miniQMT userdata_mini 目录，如
                          r"C:\\国金证券QMT交易端\\userdata_mini"
            account_id:   资金账号字符串
            account_type: "STOCK"（普通股票）或 "CREDIT"（融资融券）
        """
        self.qmt_path     = qmt_path
        self.account_id   = account_id
        self.account_type = account_type

        self._trader   = None
        self._account  = None
        self._callback = None
        self._connected = False

        # 模拟模式标志（xtquant 未安装时自动开启）
        self._mock = not HAS_XTQUANT

        # 当日交易计数
        self._daily_trade_count = 0

    # ─────────────────────────────────────────────────────────
    # 连接 & 断开
    # ─────────────────────────────────────────────────────────
    def connect(self) -> bool:
        """
        启动并连接 miniQMT 交易端。

        Returns:
            True  — 连接成功
            False — 连接失败
        """
        if self._mock:
            logger.warning("【模拟模式】跳过 QMT 连接")
            self._connected = True
            return True

        try:
            session_id = int(time.time() * 1000) % 1000000  # 6位 session id
            self._trader = XtQuantTrader(self.qmt_path, session_id)
            self._account = StockAccount(self.account_id, self.account_type)

            # 注册回调
            if HAS_XTQUANT:
                cb = type("CB", (XtQuantTraderCallback,), {
                    "on_disconnected":  _TraderCallback.on_disconnected,
                    "on_stock_order":   _TraderCallback.on_stock_order,
                    "on_stock_trade":   _TraderCallback.on_stock_trade,
                    "on_order_error":   _TraderCallback.on_order_error,
                    "on_cancel_error":  _TraderCallback.on_cancel_error,
                    "on_account_status":_TraderCallback.on_account_status,
                })()
                self._callback = cb
                self._trader.register_callback(cb)

            self._trader.start()
            result = self._trader.connect()
            if result != 0:
                logger.error(f"连接 QMT 失败，返回值: {result}（请确认 miniQMT 客户端已登录）")
                return False

            # 订阅账户
            sub_result = self._trader.subscribe(self._account)
            if sub_result != 0:
                logger.warning(f"订阅账户失败，返回值: {sub_result}")

            self._connected = True
            logger.info(f"QMT 连接成功，账号: {self.account_id}")
            return True

        except Exception as e:
            logger.error(f"连接 QMT 异常: {e}")
            return False

    def disconnect(self):
        if self._trader and self._connected:
            try:
                self._trader.unsubscribe(self._account)
                self._trader.stop()
            except Exception:
                pass
        self._connected = False
        logger.info("QMT 已断开连接")

    # ─────────────────────────────────────────────────────────
    # 查询接口
    # ─────────────────────────────────────────────────────────
    def get_cash(self) -> float:
        """查询账户可用资金（元）"""
        if self._mock:
            return 100000.0
        try:
            asset = self._trader.query_stock_asset(self._account)
            return float(asset.cash) if asset else 0.0
        except Exception as e:
            logger.error(f"查询可用资金失败: {e}")
            return 0.0

    def get_total_asset(self) -> float:
        """查询账户总资产（元）"""
        if self._mock:
            return 200000.0
        try:
            asset = self._trader.query_stock_asset(self._account)
            return float(asset.total_asset) if asset else 0.0
        except Exception as e:
            logger.error(f"查询总资产失败: {e}")
            return 0.0

    def get_positions(self) -> Dict[str, Dict]:
        """
        查询当前持仓。

        Returns:
            {
              "600519.SH": {
                "volume":         100,   # 总持仓股数
                "can_use_volume": 100,   # 可用（可卖）股数
                "open_price":     1680,  # 开仓均价
                "market_value":   168000 # 市值
              },
              ...
            }
        """
        if self._mock:
            return {}
        try:
            positions = self._trader.query_stock_positions(self._account)
            result = {}
            if positions:
                for pos in positions:
                    if pos.volume > 0:
                        result[pos.stock_code] = {
                            "volume":         int(pos.volume),
                            "can_use_volume": int(pos.can_use_volume),
                            "open_price":     float(pos.open_price),
                            "market_value":   float(pos.market_value),
                        }
            return result
        except Exception as e:
            logger.error(f"查询持仓失败: {e}")
            return {}

    def get_latest_price(self, stock_code: str) -> Optional[float]:
        """
        获取股票最新价（通过 xtdata.get_full_tick）

        Returns:
            最新价（float），失败返回 None
        """
        if self._mock:
            return 10.0   # 模拟价格
        try:
            tick = xtdata.get_full_tick([stock_code])
            if tick and stock_code in tick:
                d = tick[stock_code]
                # lastPrice 字段
                price = d.get("lastPrice") or d.get("last_price") or 0
                return float(price) if price else None
        except Exception as e:
            logger.error(f"获取行情 {stock_code} 失败: {e}")
        return None

    # ─────────────────────────────────────────────────────────
    # 计算下单量
    # ─────────────────────────────────────────────────────────
    @staticmethod
    def calc_buy_volume(amount: float, price: float, min_lot: int = 100) -> int:
        """
        根据金额和价格计算买入手数（向下取整到 min_lot 的整数倍）

        Args:
            amount:  买入金额（元）
            price:   当前价格（元/股）
            min_lot: 最小交易单位（A股=100股=1手）

        Returns:
            买入股数（int）
        """
        if price <= 0:
            return 0
        raw = amount / price
        volume = int(raw // min_lot) * min_lot
        return volume

    # ─────────────────────────────────────────────────────────
    # 下单接口
    # ─────────────────────────────────────────────────────────
    def buy(
        self,
        stock_code: str,
        amount: float,
        price: Optional[float] = None,
        strategy_name: str = "雪球跟单",
        remark: str = "",
    ) -> Optional[int]:
        """
        固定金额买入

        Args:
            stock_code:     股票代码，如 '600519.SH'
            amount:         买入金额（元）
            price:          委托价格；None 表示用最新价
            strategy_name:  策略名（显示在 QMT 委托记录）
            remark:         委托备注

        Returns:
            order_id（int > 0 = 成功，-1 = 失败，None = 模拟）
        """
        if price is None:
            price = self.get_latest_price(stock_code)
            if price is None:
                logger.error(f"买入失败: 无法获取 {stock_code} 最新价")
                return -1

        volume = self.calc_buy_volume(amount, price)
        if volume <= 0:
            logger.warning(f"买入 {stock_code}: 计算股数为 0（金额={amount:.0f}, 价格={price:.3f}）")
            return -1

        actual_amount = volume * price
        logger.info(
            f"【买入】{stock_code} "
            f"金额={amount:.0f}元 → {volume}股 @ {price:.3f} "
            f"实际金额≈{actual_amount:.0f}元 [{remark}]"
        )

        if self._mock:
            self._daily_trade_count += 1
            logger.info(f"[模拟] 买入指令已记录（未实际下单）")
            return None

        try:
            order_id = self._trader.order_stock(
                account=self._account,
                stock_code=stock_code,
                order_type=xtconstant.STOCK_BUY,
                order_volume=volume,
                price_type=xtconstant.FIX_PRICE,
                price=price,
                strategy_name=strategy_name,
                order_remark=remark or f"雪球跟单买入-{stock_code}",
            )
            if order_id > 0:
                self._daily_trade_count += 1
                logger.info(f"买入委托成功 order_id={order_id}")
            else:
                logger.error(f"买入委托失败 order_id={order_id}")
            return order_id
        except Exception as e:
            logger.error(f"下单异常 {stock_code}: {e}")
            return -1

    def sell(
        self,
        stock_code: str,
        volume: Optional[int] = None,
        price: Optional[float] = None,
        strategy_name: str = "雪球跟单",
        remark: str = "",
    ) -> Optional[int]:
        """
        卖出持仓

        Args:
            stock_code: 股票代码
            volume:     卖出股数；None = 全部卖出
            price:      委托价格；None = 最新价
            ...

        Returns:
            order_id
        """
        # 查询可卖数量
        positions = self.get_positions()
        pos = positions.get(stock_code)
        if pos is None:
            logger.warning(f"卖出 {stock_code}: 账户中无持仓，跳过")
            return -1

        can_use = pos["can_use_volume"]
        if can_use <= 0:
            logger.warning(f"卖出 {stock_code}: 可用股数为 0（今日买入，T+1 才能卖出）")
            return -1

        sell_volume = volume if volume else can_use
        sell_volume = min(sell_volume, can_use)  # 不超过可用量

        if price is None:
            price = self.get_latest_price(stock_code)
            if price is None:
                logger.error(f"卖出失败: 无法获取 {stock_code} 最新价")
                return -1

        logger.info(
            f"【卖出】{stock_code} "
            f"{sell_volume}股 @ {price:.3f} "
            f"预计金额≈{sell_volume * price:.0f}元 [{remark}]"
        )

        if self._mock:
            self._daily_trade_count += 1
            logger.info(f"[模拟] 卖出指令已记录（未实际下单）")
            return None

        try:
            order_id = self._trader.order_stock(
                account=self._account,
                stock_code=stock_code,
                order_type=xtconstant.STOCK_SELL,
                order_volume=sell_volume,
                price_type=xtconstant.FIX_PRICE,
                price=price,
                strategy_name=strategy_name,
                order_remark=remark or f"雪球跟单卖出-{stock_code}",
            )
            if order_id > 0:
                self._daily_trade_count += 1
                logger.info(f"卖出委托成功 order_id={order_id}")
            else:
                logger.error(f"卖出委托失败 order_id={order_id}")
            return order_id
        except Exception as e:
            logger.error(f"下单异常 {stock_code}: {e}")
            return -1

    def sell_by_ratio(
        self,
        stock_code: str,
        ratio: float,
        price: Optional[float] = None,
        **kwargs,
    ) -> Optional[int]:
        """
        按比例减仓

        Args:
            ratio: 0.0~1.0，卖出持仓的比例（如 0.5 表示卖出一半）
        """
        positions = self.get_positions()
        pos = positions.get(stock_code)
        if not pos:
            return -1
        # 向下取整到 100 股的整数倍
        raw_vol  = pos["can_use_volume"] * ratio
        sell_vol = int(raw_vol // 100) * 100
        if sell_vol <= 0:
            logger.warning(f"减仓 {stock_code}: 计算卖出量为 0（ratio={ratio:.2f}, can_use={pos['can_use_volume']}）")
            return -1
        return self.sell(stock_code, volume=sell_vol, price=price, **kwargs)

    # ─────────────────────────────────────────────────────────
    # 撤单
    # ─────────────────────────────────────────────────────────
    def cancel_order(self, order_id: int) -> bool:
        """撤销指定委托"""
        if self._mock:
            logger.info(f"[模拟] 撤单 order_id={order_id}")
            return True
        try:
            result = self._trader.cancel_order_stock(self._account, order_id)
            if result == 0:
                logger.info(f"撤单成功 order_id={order_id}")
                return True
            else:
                logger.warning(f"撤单失败 order_id={order_id} result={result}")
                return False
        except Exception as e:
            logger.error(f"撤单异常: {e}")
            return False

    def cancel_all_pending(self):
        """撤销所有未成交委托"""
        if self._mock:
            logger.info("[模拟] 全部撤单")
            return
        try:
            orders = self._trader.query_stock_orders(self._account, cancelable_only=True)
            if not orders:
                return
            for order in orders:
                self.cancel_order(order.order_id)
                time.sleep(0.1)
        except Exception as e:
            logger.error(f"全部撤单异常: {e}")

    def get_pending_orders(self, stock_code: Optional[str] = None) -> List[Dict]:
        """
        查询未成交（可撤）委托列表

        Args:
            stock_code: 指定股票代码过滤；None = 返回全部可撤委托

        Returns:
            [
              {
                "order_id":     123456,
                "stock_code":   "510310.SH",
                "order_type":   "BUY" | "SELL",
                "order_volume": 1000,
                "price":        4.235,
                "traded_volume":0,
                "order_remark": "...",
              },
              ...
            ]
        """
        if self._mock:
            return []
        try:
            orders = self._trader.query_stock_orders(self._account, cancelable_only=True)
            if not orders:
                return []
            result = []
            for o in orders:
                if stock_code and o.stock_code != stock_code:
                    continue
                result.append({
                    "order_id":     o.order_id,
                    "stock_code":   o.stock_code,
                    "order_type":   "BUY" if o.order_type == xtconstant.STOCK_BUY else "SELL",
                    "order_volume": int(o.order_volume),
                    "traded_volume":int(o.traded_volume),
                    "price":        float(o.price),
                    "order_remark": getattr(o, "order_remark", ""),
                })
            return result
        except Exception as e:
            logger.error(f"查询未成交委托异常: {e}")
            return []

    def cancel_orders_for_stock(self, stock_code: str) -> int:
        """
        撤销指定股票的全部未成交委托

        Args:
            stock_code: 股票代码

        Returns:
            成功撤单的数量
        """
        orders = self.get_pending_orders(stock_code)
        if not orders:
            return 0
        count = 0
        for o in orders:
            logger.info(
                f"撤单 {stock_code} order_id={o['order_id']} "
                f"{o['order_type']} {o['order_volume']}股 @ {o['price']:.3f}"
            )
            if self.cancel_order(o["order_id"]):
                count += 1
            time.sleep(0.05)
        return count

    def cancel_orders_for_stocks(self, stock_codes: List[str]) -> Dict[str, int]:
        """
        批量撤销多只股票的全部未成交委托

        Args:
            stock_codes: 股票代码列表

        Returns:
            {stock_code: 撤单数量}
        """
        result = {}
        for code in stock_codes:
            n = self.cancel_orders_for_stock(code)
            if n > 0:
                result[code] = n
        return result

    # ─────────────────────────────────────────────────────────
    # 属性
    # ─────────────────────────────────────────────────────────
    @property
    def is_connected(self) -> bool:
        return self._connected

    @property
    def daily_trade_count(self) -> int:
        return self._daily_trade_count

    def reset_daily_count(self):
        self._daily_trade_count = 0
