"""
follower.py
────────────────────────────────────────────────────────────
雪球组合跟踪核心逻辑

职责：
  1. 定时轮询雪球消息通知（低频，防反爬）
  2. 检测到调仓通知后，拉取最新完整持仓
  3. 与本地 QMT 账户实际持仓对比，计算目标市值差值
  4. 执行风控校验后精确调仓，使持仓比例与雪球一致
  5. 兼容旧版 fixed_amount 模式
  6. 撤单同步：雪球在非交易时间撤单，QMT 同步撤销对应未成交委托

────────────────────────────────────────────────────────────
【ratio_follow 模式说明】

  目标市值[i] = TOTAL_AMOUNT × (weight[i] / 100)
  差值[i]     = 目标市值[i] - 当前市值[i]
  差值 > 0  → 买入，差值 < 0  → 卖出
  |差值/目标市值| < REBALANCE_THRESHOLD  → 忽略（避免微小抖动）

  卖出先于买入执行，确保有足够资金。

────────────────────────────────────────────────────────────
【撤单同步说明】

  场景：雪球组合在非交易时间下单，后来撤单（调仓内容回退）。
  检测方式：
    - 每次再平衡前，先检查 QMT 是否存在该股票的挂单
    - 若存在，先撤单，再重新根据最新目标持仓下单
  此外，在非交易时段也会定期检查是否有 QMT 悬挂委托需要清理。
────────────────────────────────────────────────────────────
"""

import time
import logging
import datetime
from typing import Dict, List, Optional, Tuple

from xueqiu_client import XueqiuClient
from qmt_trader import QMTTrader
import config

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# 工具：交易时间判断
# ─────────────────────────────────────────────────────────────
def _now_hhmm() -> str:
    return datetime.datetime.now().strftime("%H:%M")


def _is_trade_time() -> bool:
    now   = _now_hhmm()
    start = config.TRADE_START_TIME
    end   = config.TRADE_END_TIME
    return start <= now <= end


def _is_auction_time() -> bool:
    now = _now_hhmm()
    return "09:15" <= now <= "09:25"


def _seconds_to_open() -> float:
    now    = datetime.datetime.now()
    target = now.replace(hour=9, minute=30, second=0, microsecond=0)
    if now >= target:
        target += datetime.timedelta(days=1)
    return (target - now).total_seconds()


# ─────────────────────────────────────────────────────────────
# 主跟踪器
# ─────────────────────────────────────────────────────────────
class XueqiuFollower:
    """
    雪球组合跟踪交易主控制器

    使用方法：
        follower = XueqiuFollower()
        follower.start()          # 阻塞运行
    """

    def __init__(self):
        mode = getattr(config, "TRADE_MODE", "ratio_follow")
        logger.info("=" * 60)
        logger.info("雪球组合 QMT 跟踪交易系统 启动")
        logger.info(f"  目标组合:  {config.PORTFOLIO_ID}")
        logger.info(f"  跟单模式:  {mode}")
        if mode == "ratio_follow":
            total = getattr(config, "TOTAL_AMOUNT", 100000.0)
            thr   = getattr(config, "REBALANCE_THRESHOLD", 0.02)
            logger.info(f"  总金额:    ¥{total:,.0f}")
            logger.info(f"  再平衡阈值: {thr*100:.1f}%")
        else:
            logger.info(f"  固定金额:  ¥{config.FIXED_AMOUNT:,.0f} / 只")
        logger.info(f"  交易时段:  {config.TRADE_START_TIME} ~ {config.TRADE_END_TIME}")
        logger.info("=" * 60)

        self.xq = XueqiuClient(
            cookie=config.XUEQIU_COOKIE,
            portfolio_id=config.PORTFOLIO_ID,
        )
        self.trader = QMTTrader(
            qmt_path=config.QMT_PATH,
            account_id=config.ACCOUNT_ID,
            account_type=config.ACCOUNT_TYPE,
        )

        self._last_rebalancing_id: Optional[int] = None
        self._last_reset_date: Optional[str] = None

        # 上一次的目标持仓快照（用于非交易时间撤单检测）
        # { stock_code: target_weight }
        self._last_target_weights: Dict[str, float] = {}
        # 非交易时间撤单检查时间戳
        self._last_offhour_cancel_ts: float = 0.0

    # ─────────────────────────────────────────────────────────
    # 启动入口
    # ─────────────────────────────────────────────────────────
    def start(self):
        if not self.trader.connect():
            logger.error("QMT 连接失败，退出")
            return

        self._sync_initial_rebalancing_id()

        logger.info("开始监控雪球组合调仓通知...")
        try:
            self._main_loop()
        except KeyboardInterrupt:
            logger.info("用户中断，程序退出")
        finally:
            self.trader.disconnect()

    # ─────────────────────────────────────────────────────────
    # 初始化：记录当前最新调仓 ID，防重复执行
    # ─────────────────────────────────────────────────────────
    def _sync_initial_rebalancing_id(self):
        logger.info("同步最新调仓 ID（防重启后重复下单）...")
        latest = self.xq.get_latest_rebalancing()
        if latest:
            self._last_rebalancing_id = latest.get("id")
            logger.info(f"最新调仓 ID: {self._last_rebalancing_id}")
        else:
            logger.warning("获取初始调仓 ID 失败，将在第一次轮询时重试")

    # ─────────────────────────────────────────────────────────
    # 主监控循环
    # ─────────────────────────────────────────────────────────
    def _main_loop(self):
        while True:
            now       = datetime.datetime.now()
            today_str = now.strftime("%Y-%m-%d")

            if today_str != self._last_reset_date:
                self.trader.reset_daily_count()
                self._last_reset_date = today_str
                logger.info(f"新交易日 {today_str}，已重置交易计数")

            if not _is_trade_time():
                if _is_auction_time() and config.ALLOW_AUCTION:
                    pass
                else:
                    # ── 非交易时间：检查雪球是否有撤单/调仓变化 ──
                    self._check_offhour_cancel()
                    time.sleep(30)
                    continue

            has_new_notification = self.xq.poll_notification()
            should_force_check   = self._should_force_check()

            if has_new_notification or should_force_check:
                if has_new_notification:
                    logger.info("收到调仓通知，获取最新持仓...")
                else:
                    logger.debug("定时核查调仓...")
                self._handle_rebalancing()

            time.sleep(config.POLL_INTERVAL_SECONDS)

    # ─────────────────────────────────────────────────────────
    # 兜底定时检查（每 5 分钟主动拉一次）
    # ─────────────────────────────────────────────────────────
    _force_check_interval = 300
    _last_force_check_ts  = 0.0

    def _should_force_check(self) -> bool:
        now_ts = time.time()
        if now_ts - self._last_force_check_ts >= self._force_check_interval:
            self._last_force_check_ts = now_ts
            return True
        return False

    # ─────────────────────────────────────────────────────────
    # 非交易时间：检测雪球调仓变化并同步撤单
    # ─────────────────────────────────────────────────────────
    _offhour_cancel_interval = 60   # 非交易时间每 60 秒检查一次

    def _check_offhour_cancel(self):
        """
        非交易时间定期检测：
          1. 拉取雪球最新调仓 ID
          2. 若 ID 有变化（雪球撤单或重新调仓），立即撤销 QMT 所有未成交委托
          3. 更新本地目标持仓快照（供下次比较）

        说明：非交易时间不重新下单，只撤单。
        等到下一个交易时间开启后，再平衡逻辑会自动重新计算并下单。
        """
        now_ts = time.time()
        if now_ts - self._last_offhour_cancel_ts < self._offhour_cancel_interval:
            return
        self._last_offhour_cancel_ts = now_ts

        rebalancing = self.xq.get_latest_rebalancing()
        if rebalancing is None:
            return

        rid = rebalancing.get("id")

        # 检查调仓 ID 是否变化
        if rid and rid != self._last_rebalancing_id:
            logger.info(
                f"【非交易时段】检测到雪球调仓变化 "
                f"old_id={self._last_rebalancing_id} → new_id={rid}，"
                f"撤销 QMT 全部未成交委托..."
            )
            self._last_rebalancing_id = rid
            self._cancel_all_pending_with_log()
            return

        # 即使 ID 未变，也检查是否有悬挂的 QMT 委托（程序重启后可能残留）
        # 对比当前雪球持仓与 QMT 挂单，若挂单方向与目标不符则撤销
        self._cancel_mismatched_pending_orders()

    def _cancel_all_pending_with_log(self):
        """撤销 QMT 所有未成交委托，并记录日志"""
        pending = self.trader.get_pending_orders()
        if not pending:
            logger.info("【撤单同步】QMT 无未成交委托，无需撤单")
            return
        logger.info(f"【撤单同步】发现 {len(pending)} 笔未成交委托，开始撤单...")
        for o in pending:
            logger.info(
                f"  撤单: {o['stock_code']} "
                f"{o['order_type']} {o['order_volume']}股 @ {o['price']:.3f} "
                f"order_id={o['order_id']}"
            )
        self.trader.cancel_all_pending()
        logger.info("【撤单同步】撤单完成")

    def _cancel_mismatched_pending_orders(self):
        """
        检查 QMT 挂单与当前雪球目标持仓是否一致：
          - 若某只股票的 QMT 挂单方向与目标方向不一致，则撤单
          - 若某只股票在目标持仓中已不存在（权重=0），但 QMT 有买单，则撤单
        """
        pending = self.trader.get_pending_orders()
        if not pending:
            return

        # 获取最新雪球持仓目标
        xq_positions = self.xq.get_current_positions()
        if not xq_positions:
            return

        total_weight = sum(p["weight"] for p in xq_positions)
        target_weights: Dict[str, float] = {}
        if total_weight > 0:
            for p in xq_positions:
                target_weights[p["stock_code"]] = p["weight"] / total_weight

        # 获取 QMT 当前持仓
        qmt_positions = self.trader.get_positions()
        total_amount = getattr(config, "TOTAL_AMOUNT", 100000.0)

        cancel_ids = []
        for o in pending:
            code = o["stock_code"]
            target_w = target_weights.get(code, 0.0)
            target_value = total_amount * target_w
            cur_value = float((qmt_positions.get(code) or {}).get("market_value") or 0)

            if o["order_type"] == "BUY":
                # 买单：若目标已不需要买（目标<=当前），则撤
                if target_value <= cur_value:
                    logger.info(
                        f"【撤单同步】{code} 买单已无需执行"
                        f"（目标市值={target_value:.0f} <= 当前市值={cur_value:.0f}），撤单 order_id={o['order_id']}"
                    )
                    cancel_ids.append(o["order_id"])
            elif o["order_type"] == "SELL":
                # 卖单：若目标已不需要卖（目标>=当前），则撤
                if target_value >= cur_value and target_w > 0:
                    logger.info(
                        f"【撤单同步】{code} 卖单已无需执行"
                        f"（目标市值={target_value:.0f} >= 当前市值={cur_value:.0f}），撤单 order_id={o['order_id']}"
                    )
                    cancel_ids.append(o["order_id"])

        for oid in cancel_ids:
            self.trader.cancel_order(oid)

    # ─────────────────────────────────────────────────────────
    # 处理调仓（入口）
    # ─────────────────────────────────────────────────────────
    def _handle_rebalancing(self):
        """拉取最新调仓 ID，如有更新则执行再平衡"""
        rebalancing = self.xq.get_latest_rebalancing()
        if rebalancing is None:
            logger.warning("获取调仓数据失败，稍后重试")
            return

        rid = rebalancing.get("id")
        if rid and rid == self._last_rebalancing_id:
            logger.debug(f"调仓 ID={rid} 已处理过，跳过")
            return

        logger.info(f"发现新调仓！ID={rid}")
        self._last_rebalancing_id = rid

        # ── 先撤销 QMT 所有未成交委托（防止旧挂单干扰再平衡计算）──
        pending = self.trader.get_pending_orders()
        if pending:
            logger.info(
                f"【再平衡前撤单】发现 {len(pending)} 笔未成交挂单，"
                f"先全部撤销再重新计算..."
            )
            self.trader.cancel_all_pending()
            # 等待撤单回报落地（最多等 2 秒）
            time.sleep(2)

        mode = getattr(config, "TRADE_MODE", "ratio_follow")
        if mode == "ratio_follow":
            self._rebalance_by_ratio()
        else:
            self._rebalance_fixed_amount(rebalancing)

    # ═════════════════════════════════════════════════════════
    #  ratio_follow 模式：按权重比例精确跟仓
    # ═════════════════════════════════════════════════════════
    def _rebalance_by_ratio(self):
        """
        核心算法：
          1. 拉取雪球最新完整持仓（含各股权重）
          2. 拉取 QMT 当前持仓（各股市值）
          3. 计算每只股票 目标市值 = TOTAL_AMOUNT × weight%
          4. 差值 = 目标市值 - 当前市值
             差值 > +threshold → 买入
             差值 < -threshold → 卖出/减仓
          5. 先卖后买
        """
        total_amount = getattr(config, "TOTAL_AMOUNT", 100000.0)
        threshold    = getattr(config, "REBALANCE_THRESHOLD", 0.02)

        # ── 1. 雪球目标持仓 ────────────────────────────────
        xq_positions = self.xq.get_current_positions()
        if not xq_positions:
            logger.warning("无法获取雪球持仓，跳过本次再平衡")
            return

        # 归一化权重（防止雪球权重合计不等于100%）
        total_weight = sum(p["weight"] for p in xq_positions)
        if total_weight <= 0:
            logger.warning("雪球持仓权重合计为 0，跳过")
            return

        # 目标市值字典 {stock_code: target_value}
        target: Dict[str, float] = {}
        for p in xq_positions:
            code   = p["stock_code"]
            w_norm = p["weight"] / total_weight    # 归一化后权重（0~1）
            target[code] = total_amount * w_norm

        # ── 2. QMT 当前持仓市值 ────────────────────────────
        qmt_positions = self.trader.get_positions()
        # {stock_code: market_value}
        current_value: Dict[str, float] = {}
        for code, pos in qmt_positions.items():
            mv = pos.get("market_value") or 0.0
            current_value[code] = float(mv)

        # ── 3. 计算差值 ────────────────────────────────────
        all_codes = set(target.keys()) | set(current_value.keys())

        buy_orders:  List[Tuple[str, float]] = []   # (code, 买入金额)
        sell_orders: List[Tuple[str, float]] = []   # (code, 卖出金额)

        logger.info("=" * 55)
        logger.info(f"  再平衡计划  总金额=¥{total_amount:,.0f}  阈值={threshold*100:.1f}%")
        logger.info(f"  {'代码':<12} {'目标市值':>10} {'当前市值':>10} {'差值':>10}  操作")
        logger.info("  " + "-" * 55)

        for code in sorted(all_codes):
            tgt = target.get(code, 0.0)
            cur = current_value.get(code, 0.0)
            diff = tgt - cur

            # 忽略微小偏差
            if tgt > 0 and abs(diff) / tgt < threshold:
                logger.info(f"  {code:<12} {tgt:>10,.0f} {cur:>10,.0f} {diff:>+10,.0f}  忽略(偏差<{threshold*100:.0f}%)")
                continue

            if diff > 0:
                action = f"买入 ¥{diff:,.0f}"
                buy_orders.append((code, diff))
            elif diff < 0:
                action = f"卖出 ¥{abs(diff):,.0f}"
                sell_orders.append((code, abs(diff)))
            else:
                action = "无需调整"

            logger.info(f"  {code:<12} {tgt:>10,.0f} {cur:>10,.0f} {diff:>+10,.0f}  {action}")

        # 不在雪球持仓内、但本地有持仓的股票 → 全部清仓
        for code in set(current_value.keys()) - set(target.keys()):
            cur = current_value[code]
            if cur > 0:
                logger.info(f"  {code:<12} {'0':>10} {cur:>10,.0f} {-cur:>+10,.0f}  清仓（已从组合移除）")
                sell_orders.append((code, cur))

        logger.info("=" * 55)

        # ── 4. 先卖后买 ────────────────────────────────────
        for code, sell_amount in sell_orders:
            self._execute_sell_by_value(code, sell_amount)

        for code, buy_amount in buy_orders:
            self._execute_buy_by_value(code, buy_amount)

    # ─────────────────────────────────────────────────────────
    # ratio_follow：按目标金额买入
    # ─────────────────────────────────────────────────────────
    def _execute_buy_by_value(self, code: str, amount: float):
        """买入指定金额的股票（下单前先撤该股旧挂单）"""
        if not self._risk_check_buy(code, amount):
            return

        # 先撤该股票的旧挂单，避免重复或冲突
        cancelled = self.trader.cancel_orders_for_stock(code)
        if cancelled:
            logger.info(f"【买入前撤单】{code} 撤销 {cancelled} 笔旧挂单")
            time.sleep(0.3)

        price = self.trader.get_latest_price(code)
        if price is None:
            logger.error(f"买入 {code}: 无法获取最新价，跳过")
            return

        # 涨停保护
        if config.LIMIT_PROTECTION:
            # 简单判断：若最新价已比昨收涨超 9.5%，视为接近涨停
            # QMT 有 pre_close 字段，此处用简化逻辑
            pass   # TODO: 可在此加入涨停判断

        logger.info(f"【按比例买入】{code}  目标金额=¥{amount:,.0f}")
        self.trader.buy(
            stock_code=code,
            amount=amount,
            price=price,
            remark=f"雪球比例跟单-{config.PORTFOLIO_ID}",
        )

    # ─────────────────────────────────────────────────────────
    # ratio_follow：按目标金额卖出
    # ─────────────────────────────────────────────────────────
    def _execute_sell_by_value(self, code: str, sell_amount: float):
        """
        卖出指定市值的股票（下单前先撤该股旧挂单）
        sell_amount: 需要减少的市值（元）
        """
        # 先撤该股票的旧挂单，避免重复或冲突
        cancelled = self.trader.cancel_orders_for_stock(code)
        if cancelled:
            logger.info(f"【卖出前撤单】{code} 撤销 {cancelled} 笔旧挂单")
            time.sleep(0.3)

        positions = self.trader.get_positions()
        pos = positions.get(code)
        if pos is None:
            logger.warning(f"卖出 {code}: 账户中无持仓，跳过")
            return

        can_use = pos["can_use_volume"]
        if can_use <= 0:
            logger.warning(f"卖出 {code}: 可用股数=0（T+0限制），跳过")
            return

        price = self.trader.get_latest_price(code)
        if price is None or price <= 0:
            price = pos.get("open_price", 0)
        if price <= 0:
            logger.error(f"卖出 {code}: 无法获取价格，跳过")
            return

        # 跌停保护
        if config.LIMIT_PROTECTION:
            pass   # TODO: 跌停判断

        # 计算卖出股数（向下取整到 100 股的整数倍）
        sell_volume = int(sell_amount / price // 100) * 100
        # 如果计算出的卖出量超过可用量，则全部卖出
        if sell_volume >= can_use:
            sell_volume = can_use
            logger.info(f"【按比例卖出】{code}  全部卖出 {sell_volume}股 @ {price:.3f}（超出持仓）")
        else:
            logger.info(
                f"【按比例卖出】{code}  {sell_volume}股 @ {price:.3f}"
                f"  卖出金额≈¥{sell_volume*price:,.0f}  目标减少≈¥{sell_amount:,.0f}"
            )

        if sell_volume <= 0:
            logger.warning(f"卖出 {code}: 计算卖出量为 0，跳过")
            return

        self.trader.sell(
            stock_code=code,
            volume=sell_volume,
            price=price,
            remark=f"雪球比例减仓-{config.PORTFOLIO_ID}",
        )

    # ═════════════════════════════════════════════════════════
    #  fixed_amount 模式（旧逻辑，保留兼容）
    # ═════════════════════════════════════════════════════════
    def _rebalance_fixed_amount(self, rebalancing: dict):
        """原有固定金额逻辑（TRADE_MODE='fixed_amount' 时走此分支）"""
        for item in rebalancing.get("buy_list", []):
            self._execute_buy_fixed(item, action="新建仓")

        if config.FOLLOW_INCREASE:
            for item in rebalancing.get("increase_list", []):
                self._execute_buy_fixed(item, action="加仓")

        if config.FOLLOW_DECREASE:
            for item in rebalancing.get("decrease_list", []):
                self._execute_partial_sell(item)

        for item in rebalancing.get("sell_list", []):
            self._execute_sell_full(item, action="清仓")

    def _execute_buy_fixed(self, item: dict, action: str = "买入"):
        code  = item["stock_code"]
        name  = item.get("stock_name", "")
        price = item.get("price") or None

        if not self._risk_check_buy(code, config.FIXED_AMOUNT):
            return

        if config.LIMIT_PROTECTION:
            current = self.trader.get_latest_price(code)
            if current and price and current >= price * 1.095:
                logger.warning(f"【风控】{code}({name}) 接近涨停，跳过买入")
                return
            if current:
                price = current

        logger.info(f"执行{action}: {code}({name}) 金额=¥{config.FIXED_AMOUNT:.0f}")
        self.trader.buy(
            stock_code=code,
            amount=config.FIXED_AMOUNT,
            price=price,
            remark=f"雪球{action}-{config.PORTFOLIO_ID}",
        )

    def _execute_sell_full(self, item: dict, action: str = "卖出"):
        code  = item["stock_code"]
        name  = item.get("stock_name", "")
        price = item.get("price") or None

        if config.LIMIT_PROTECTION:
            current = self.trader.get_latest_price(code)
            if current and price and current <= price * 0.905:
                logger.warning(f"【风控】{code}({name}) 接近跌停，跳过（下个交易日再处理）")

        logger.info(f"执行{action}: {code}({name})")
        self.trader.sell(
            stock_code=code,
            volume=None,
            price=price,
            remark=f"雪球{action}-{config.PORTFOLIO_ID}",
        )

    def _execute_partial_sell(self, item: dict):
        code     = item["stock_code"]
        name     = item.get("stock_name", "")
        prev_w   = item.get("prev_weight", 0)
        target_w = item.get("weight", 0)

        if prev_w <= 0:
            return
        ratio = max(0.0, 1.0 - (target_w / prev_w))
        ratio = round(ratio, 2)
        if ratio <= 0.05:
            return

        logger.info(
            f"执行减仓: {code}({name}) "
            f"权重 {prev_w:.1f}% → {target_w:.1f}% "
            f"减仓比例={ratio*100:.0f}%"
        )
        self.trader.sell_by_ratio(
            stock_code=code,
            ratio=ratio,
            remark=f"雪球减仓-{config.PORTFOLIO_ID}",
        )

    # ─────────────────────────────────────────────────────────
    # 风控
    # ─────────────────────────────────────────────────────────
    def _risk_check_buy(self, stock_code: str, amount: float) -> bool:
        if amount > config.MAX_SINGLE_ORDER_AMOUNT:
            logger.warning(
                f"【风控】单笔金额 ¥{amount:,.0f} > 上限 ¥{config.MAX_SINGLE_ORDER_AMOUNT:,.0f}，拒绝"
            )
            return False

        if self.trader.daily_trade_count >= config.MAX_DAILY_TRADES:
            logger.warning(
                f"【风控】当日交易笔数 {self.trader.daily_trade_count} 已达上限 {config.MAX_DAILY_TRADES}，拒绝"
            )
            return False

        cash  = self.trader.get_cash()
        total = self.trader.get_total_asset()
        if total > 0 and cash / total < config.MIN_CASH_RATIO:
            logger.warning(
                f"【风控】可用资金率 {cash/total*100:.1f}% < 最低 {config.MIN_CASH_RATIO*100:.0f}%，拒绝买入"
            )
            return False
        if cash < amount:
            logger.warning(f"【风控】可用资金 ¥{cash:,.0f} < 买入金额 ¥{amount:,.0f}，拒绝")
            return False

        return True
