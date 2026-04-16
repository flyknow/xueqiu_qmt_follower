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

    def __init__(self, trader_ref=None):
        # 持有 QMTTrader 弱引用，用于触发重连
        self._trader_ref = trader_ref

    def on_disconnected(self):
        logger.error("【QMT】连接断开！请检查 miniQMT 客户端是否在线")
        # 通知 QMTTrader 标记断线，由主循环触发重连
        if self._trader_ref is not None:
            self._trader_ref._connected = False

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

        # 重连参数
        self._reconnect_interval = 30   # 断线后每 30 秒尝试重连一次
        self._last_reconnect_ts  = 0.0

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
                _cb_self = self   # 闭包捕获
                cb = type("CB", (XtQuantTraderCallback,), {
                    "on_disconnected":  lambda s: _TraderCallback(_cb_self).on_disconnected() or _cb_self.__setattr__("_connected", False),
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

    def get_tick(self, stock_code: str) -> Optional[Dict]:
        """
        获取股票完整 tick 数据（含涨跌停价、昨收等）

        Returns:
            {
              "lastPrice":   4.235,
              "high":        4.30,
              "low":         4.20,
              "open":        4.22,
              "lastClose":   4.20,   # 昨收
              "upperLimit":  4.62,   # 涨停价（+10%）
              "lowerLimit":  3.78,   # 跌停价（-10%）
              "askPrice":    [...],
              "bidPrice":    [...],
            }
            失败返回 None
        """
        if self._mock:
            return {
                "lastPrice": 10.0,
                "lastClose": 9.5,
                "upperLimit": 10.45,
                "lowerLimit": 8.55,
                "askPrice": [10.02, 10.03, 10.04, 10.05, 10.06],
                "bidPrice": [9.98, 9.97, 9.96, 9.95, 9.94],
            }
        try:
            tick = xtdata.get_full_tick([stock_code])
            if tick and stock_code in tick:
                return tick[stock_code]
        except Exception as e:
            logger.error(f"获取 tick {stock_code} 失败: {e}")
        return None

    def get_ask_price(self, stock_code: str) -> Optional[float]:
        """
        获取卖一价（askPrice[0]）。
        买入时使用：确保以卖方最低报价成交，不会买贵。
        """
        tick = self.get_tick(stock_code)
        if tick is None:
            return None
        ask_list = tick.get("askPrice")
        if ask_list and len(ask_list) > 0:
            return float(ask_list[0])
        # fallback 到最新价
        return float(tick.get("lastPrice") or 0) or None

    def get_bid_price(self, stock_code: str) -> Optional[float]:
        """
        获取买一价（bidPrice[0]）。
        卖出时使用：确保以买方最高报价成交，不会卖便宜。
        """
        tick = self.get_tick(stock_code)
        if tick is None:
            return None
        bid_list = tick.get("bidPrice")
        if bid_list and len(bid_list) > 0:
            return float(bid_list[0])
        # fallback 到最新价
        return float(tick.get("lastPrice") or 0) or None

    def is_limit_up(self, stock_code: str) -> bool:
        """
        判断股票是否已涨停（最新价 >= 涨停价）

        涨停价判断规则：
          - tick 中有 upperLimit 字段时，直接比较
          - 没有时，用昨收 × 1.10 估算（普通股票10%）
          - 科创板/创业板（688/300开头）× 1.20
          - ST股票（标识含"ST"）× 1.05

        Returns:
            True  — 已涨停，不宜追买
            False — 未涨停
        """
        tick = self.get_tick(stock_code)
        if tick is None:
            return False

        last_price   = float(tick.get("lastPrice") or 0)
        upper_limit  = float(tick.get("upperLimit") or 0)
        last_close   = float(tick.get("lastClose") or 0)

        if last_price <= 0:
            return False

        if upper_limit > 0:
            return last_price >= upper_limit * 0.999   # 留0.1%容差

        # 无涨停价字段，自行估算
        if last_close > 0:
            code_num = stock_code.split(".")[0]
            if code_num.startswith("688") or code_num.startswith("300"):
                estimated_limit = last_close * 1.20
            else:
                estimated_limit = last_close * 1.10
            return last_price >= estimated_limit * 0.999

        return False

    def is_limit_down(self, stock_code: str) -> bool:
        """
        判断股票是否已跌停（最新价 <= 跌停价）

        Returns:
            True  — 已跌停，不宜下卖单
            False — 未跌停
        """
        tick = self.get_tick(stock_code)
        if tick is None:
            return False

        last_price  = float(tick.get("lastPrice") or 0)
        lower_limit = float(tick.get("lowerLimit") or 0)
        last_close  = float(tick.get("lastClose") or 0)

        if last_price <= 0:
            return False

        if lower_limit > 0:
            return last_price <= lower_limit * 1.001   # 留0.1%容差

        if last_close > 0:
            code_num = stock_code.split(".")[0]
            if code_num.startswith("688") or code_num.startswith("300"):
                estimated_limit = last_close * 0.80
            else:
                estimated_limit = last_close * 0.90
            return last_price <= estimated_limit * 1.001

        return False

    # ─────────────────────────────────────────────────────────
    # 品种辅助
    # ─────────────────────────────────────────────────────────
    @staticmethod
    def get_lot_size(stock_code: str) -> int:
        """
        返回品种最小交易单位（手的张/股数）。
        可转债规则：沪市 11xxxx.SH、深市 12xxxx.SZ → 10张/手
        其余（A股等）→ 100股/手
        """
        prefix = stock_code[:2]
        if prefix in ("11", "12"):
            return 10
        return 100

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

        volume = self.calc_buy_volume(amount, price, min_lot=self.get_lot_size(stock_code))
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
        # 向下取整到品种最小交易单位整数倍（股票100，可转债10）
        lot      = self.get_lot_size(stock_code)
        raw_vol  = pos["can_use_volume"] * ratio
        sell_vol = int(raw_vol // lot) * lot
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

    def wait_until_all_cancelled(self, timeout: float = 5.0, interval: float = 0.3,
                                  stock_code: Optional[str] = None) -> bool:
        """
        等待直到可撤委托都被撤销（轮询确认），避免盲等固定秒数。

        Args:
            timeout:     最长等待秒数，超时后返回 False
            interval:    每次轮询间隔（秒）
            stock_code:  指定股票代码过滤；None = 等待所有委托撤完

        Returns:
            True  — 全部撤单已确认
            False — 超时仍有残留委托
        """
        if self._mock:
            return True
        deadline = time.time() + timeout
        while time.time() < deadline:
            remaining = self.get_pending_orders(stock_code=stock_code)
            if not remaining:
                scope = stock_code or "全部"
                logger.debug(f"撤单确认：{scope} 委托已撤销")
                return True
            logger.debug(f"撤单确认：仍有 {len(remaining)} 笔委托未撤，继续等待...")
            time.sleep(interval)
        remaining = self.get_pending_orders(stock_code=stock_code)
        if remaining:
            scope = stock_code or "全部"
            logger.warning(
                f"撤单等待超时（{timeout}s）[{scope}]，仍有 {len(remaining)} 笔委托：\n"
                + "\n".join(f"  {o['stock_code']} {o['order_type']} {o['order_volume']}股 order_id={o['order_id']}"
                            for o in remaining)
            )
            return False
        return True

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

    def reconnect_if_needed(self) -> bool:
        """
        若当前连接已断开，尝试重连（有冷却时间保护，避免频繁重连）。

        Returns:
            True  — 当前已连接（无需重连 或 重连成功）
            False — 仍未连接
        """
        if self._mock:
            return True
        if self._connected:
            return True

        now_ts = time.time()
        if now_ts - self._last_reconnect_ts < self._reconnect_interval:
            return False   # 还在冷却期

        self._last_reconnect_ts = now_ts
        logger.warning(f"【重连】检测到 QMT 断线，尝试重新连接...")

        # 先清理旧对象
        try:
            if self._trader:
                self._trader.stop()
        except Exception:
            pass
        self._trader = None

        ok = self.connect()
        if ok:
            logger.info("【重连】QMT 重连成功")
        else:
            logger.error("【重连】QMT 重连失败，将在下次循环再试")
        return ok
