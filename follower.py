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
import json
import logging
import datetime
import pathlib
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

        # 「待开盘重算」标记：
        #   非交易时间撤单后置 True，开盘后强制执行一次再平衡，完成后清除
        self._pending_rebalance: bool = False

        # 挂单追踪：记录已下但可能未成交的订单，30 秒后检查是否需要撤单重下
        # 格式：{ order_id: {
        #     "stock_code": str,
        #     "direction": "BUY" | "SELL",
        #     "amount":    float,   # 买入时记录金额（元）
        #     "volume":    int,     # 卖出时记录股数
        #     "ts":        float,   # 下单时间戳
        # } }
        self._chase_orders: Dict[int, dict] = {}
        self._last_chase_ts: float = 0.0

        # 状态持久化文件（保存 last_rebalancing_id，重启后不丢失）
        self._state_file = pathlib.Path(
            getattr(config, "STATE_FILE", "./state.json")
        )
        self._load_state()

    # ─────────────────────────────────────────────────────────
    # 状态持久化（跨重启保留 last_rebalancing_id）
    # ─────────────────────────────────────────────────────────
    def _load_state(self):
        """从磁盘恢复上次运行的状态"""
        try:
            if self._state_file.exists():
                data = json.loads(self._state_file.read_text(encoding="utf-8"))
                self._last_rebalancing_id = data.get("last_rebalancing_id")
                self._pending_rebalance   = bool(data.get("pending_rebalance", False))
                logger.info(
                    f"已恢复本地状态：last_rebalancing_id={self._last_rebalancing_id}"
                    + ("  ⚡ pending_rebalance=True（上次退出前有未完成的开盘再平衡任务）" if self._pending_rebalance else "")
                )
        except Exception as e:
            logger.warning(f"读取状态文件失败: {e}，将从头开始")

    def _save_state(self):
        """将当前关键状态写入磁盘"""
        try:
            data = {
                "last_rebalancing_id": self._last_rebalancing_id,
                "pending_rebalance":   self._pending_rebalance,
            }
            self._state_file.write_text(
                json.dumps(data, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception as e:
            logger.warning(f"保存状态文件失败: {e}")

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

        # 若本地已有持久化 ID，优先使用（_load_state 已在 __init__ 中调用）
        if self._last_rebalancing_id is not None:
            logger.info(f"复用本地持久化 ID: {self._last_rebalancing_id}")
            return

        latest = self.xq.get_latest_rebalancing()
        if latest:
            self._last_rebalancing_id = latest.get("id")
            logger.info(f"最新调仓 ID: {self._last_rebalancing_id}")
            self._save_state()
        else:
            logger.warning("获取初始调仓 ID 失败，将在第一次轮询时重试")

    # ─────────────────────────────────────────────────────────
    # 主监控循环
    # ─────────────────────────────────────────────────────────
    def _main_loop(self):
        while True:
            now       = datetime.datetime.now()
            today_str = now.strftime("%Y-%m-%d")

            # ── 断线重连检测（每次循环都检查）──
            if not self.trader.reconnect_if_needed():
                logger.warning("QMT 未连接，等待重连...")
                time.sleep(30)
                continue

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

            # 「待开盘重算」：非交易时间撤单后，开盘第一次循环强制执行再平衡
            if self._pending_rebalance:
                logger.info(
                    "【待开盘重算】检测到非交易时间有撤单/调仓变化，"
                    "强制执行一次再平衡以确保持仓与雪球一致..."
                )
                self._rebalance_pending()
                # _rebalance_pending 内部会清除标记并保存状态

            elif has_new_notification or should_force_check:
                if has_new_notification:
                    logger.info("收到调仓通知，获取最新持仓...")
                else:
                    logger.debug("定时核查调仓...")
                self._handle_rebalancing()

            # 交易时间内：每 30 秒检查一次未成交挂单，撤单后按最新价重下
            self._chase_unfinished_orders()

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
    @property
    def _offhour_cancel_interval(self) -> int:
        """非交易时间撤单检查间隔（秒），从 config 读取，默认 60"""
        return getattr(config, "OFFHOUR_CANCEL_INTERVAL", 60)

    def _check_offhour_cancel(self):
        """
        非交易时间定期检测：
          1. 拉取雪球最新调仓 ID
          2. 若 ID 有变化，立即撤销 QMT 所有未成交委托，并标记「待开盘重算」
          3. 即使 ID 未变，也检查 QMT 悬挂委托与目标是否一致

        ┌─────────────────────────────────────────────────────────────┐
        │  非交易时间撤单后的完整处理流程                               │
        │                                                             │
        │  场景一：雪球发出了「新调仓」（旧→新调仓方向变了）              │
        │    检测到 rid 变化 → 撤销 QMT 委托 → 标记 pending_rebalance  │
        │    → 开盘后 _rebalance_pending() 强制按新目标重新下单          │
        │                                                             │
        │  场景二：雪球「撤回了原有调仓」（恢复到更早的状态）             │
        │    检测到 rid 变化 → 撤销 QMT 委托 → 标记 pending_rebalance  │
        │    → 开盘后 _rebalance_pending() 按当前真实持仓重新对齐        │
        │                                                             │
        │  共同结论：无论哪种场景，只要 rid 变化，都撤单+待开盘重算      │
        └─────────────────────────────────────────────────────────────┘

        说明：非交易时间不重新下单，只撤单+打标记。
        等到下一个交易时间开启后，主循环检测到 pending_rebalance 后强制执行。
        """
        now_ts = time.time()
        if now_ts - self._last_offhour_cancel_ts < self._offhour_cancel_interval:
            return
        self._last_offhour_cancel_ts = now_ts

        rebalancing = self.xq.get_latest_rebalancing()
        if rebalancing is None:
            return

        rid = rebalancing.get("id")

        # ── 检查调仓 ID 是否变化 ─────────────────────────────
        if rid and rid != self._last_rebalancing_id:
            self._handle_offhour_id_change(rid, rebalancing)
            return

        # ── ID 未变：检查是否有不该存在的悬挂委托（程序重启后残留等）──
        self._cancel_mismatched_pending_orders()

    def _handle_offhour_id_change(self, new_rid: int, rebalancing: dict):
        """
        非交易时间检测到调仓 ID 变化时的处理：
          - 区分「新调仓」与「撤单/回退」并输出对应日志
          - 撤销 QMT 全部未成交委托
          - 标记 pending_rebalance，等开盘后强制重算一次
        """
        old_rid = self._last_rebalancing_id

        # 判断是哪种场景（雪球 API 里 rebalancing_id 单调递增，变小即为回退）
        if old_rid is not None and isinstance(new_rid, int) and isinstance(old_rid, int):
            if new_rid < old_rid:
                logger.warning(
                    f"【非交易时段-撤单回退】雪球组合撤回了之前的调仓 "
                    f"id={old_rid} → id={new_rid}（ID 减小，说明是撤单/回退）。\n"
                    f"  将撤销 QMT 全部未成交委托，并在开盘后按最新持仓重新对齐。"
                )
            else:
                logger.info(
                    f"【非交易时段-新调仓】雪球组合发出新调仓 "
                    f"id={old_rid} → id={new_rid}。\n"
                    f"  将撤销 QMT 全部未成交委托，并在开盘后按新目标重新下单。"
                )
        else:
            logger.info(
                f"【非交易时段】检测到雪球调仓变化 "
                f"old_id={old_rid} → new_id={new_rid}，"
                f"撤销 QMT 全部未成交委托..."
            )

        # 撤单
        self._cancel_all_pending_with_log()

        # 更新 ID + 标记待开盘重算
        self._last_rebalancing_id = new_rid
        self._pending_rebalance   = True
        self._save_state()

        logger.info(
            "【非交易时段】已标记「待开盘重算」，"
            "下次开盘（09:30）后将自动执行再平衡。"
        )

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
                # 注意：target_w > 0 不应作为前提条件；
                # 即使 target_w==0（完全清仓），若持仓已全卖完（cur_value==0），
                # 继续挂着也无意义，同样需要撤单
                if target_value >= cur_value:
                    logger.info(
                        f"【撤单同步】{code} 卖单已无需执行"
                        f"（目标市值={target_value:.0f} >= 当前市值={cur_value:.0f}），撤单 order_id={o['order_id']}"
                    )
                    cancel_ids.append(o["order_id"])

        for oid in cancel_ids:
            self.trader.cancel_order(oid)

    # ─────────────────────────────────────────────────────────
    # 待开盘重算：处理非交易时间撤单/调仓变化后的再平衡
    # ─────────────────────────────────────────────────────────
    def _rebalance_pending(self):
        """
        开盘后处理「待开盘重算」标记：
          1. 先确认雪球最新调仓 ID（防止开盘前又有变化）
          2. 执行再平衡（与正常调仓流程完全一致）
          3. 清除 pending_rebalance 标记

        注意：此方法只在交易时间内调用，不会在非交易时间触发。
        """
        # 再拉一次最新 ID，防止开盘前雪球又有变化
        latest = self.xq.get_latest_rebalancing()
        if latest:
            latest_rid = latest.get("id")
            if latest_rid and latest_rid != self._last_rebalancing_id:
                logger.info(
                    f"【待开盘重算】开盘前雪球又发生了调仓变化 "
                    f"{self._last_rebalancing_id} → {latest_rid}，更新 ID 后继续执行再平衡"
                )
                self._last_rebalancing_id = latest_rid

        mode = getattr(config, "TRADE_MODE", "ratio_follow")
        try:
            if mode == "ratio_follow":
                self._rebalance_by_ratio()
            else:
                if latest:
                    self._rebalance_fixed_amount(latest)
                else:
                    logger.warning("【待开盘重算】获取调仓详情失败，跳过 fixed_amount 再平衡")
                    return
        except Exception as e:
            logger.error(f"【待开盘重算】再平衡执行异常: {e}")
            return

        # 再平衡成功后清除标记并持久化
        self._pending_rebalance = False
        self._save_state()
        logger.info("【待开盘重算】再平衡完成，pending_rebalance 标记已清除")

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
        # 若同时有 pending_rebalance 标记，一并清除（避免重复执行）
        self._pending_rebalance = False
        self._save_state()   # 持久化，重启后不重复执行

        # ── 先撤销 QMT 所有未成交委托（防止旧挂单干扰再平衡计算）──
        pending = self.trader.get_pending_orders()
        if pending:
            logger.info(
                f"【再平衡前撤单】发现 {len(pending)} 笔未成交挂单，"
                f"先全部撤销再重新计算..."
            )
            self.trader.cancel_all_pending()
            # 轮询等待撤单回报落地（最多 5 秒），比盲等 2 秒更可靠
            self.trader.wait_until_all_cancelled(timeout=5.0)

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
          1. 读取 config.TOTAL_AMOUNT 作为跟单基准金额
          2. 拉取雪球最新完整持仓（含各股权重）
          3. 计算每只股票 目标市值 = TOTAL_AMOUNT × weight%
             （weight 是雪球原始权重，各股之和 < 100% 时剩余即为现金比例）
          4. 差值 = 目标市值 - 当前市值
             差值 > +threshold → 买入
             差值 < -threshold → 卖出/减仓
          5. 先卖后买，买入前按可用现金按比例分配
        """
        total_amount = getattr(config, "TOTAL_AMOUNT", 100000.0)
        threshold    = getattr(config, "REBALANCE_THRESHOLD", 0.02)

        # ── 1. 雪球目标持仓 ────────────────────────────────
        xq_positions = self.xq.get_current_positions()
        if not xq_positions:
            logger.warning("无法获取雪球持仓，跳过本次再平衡")
            return

        # 雪球权重直接使用原始值（占组合总资产的百分比，0~100）
        # 各股权重之和 < 100% 的部分即为雪球的现金比例
        total_stock_weight = sum(p["weight"] for p in xq_positions)
        if total_stock_weight <= 0:
            logger.warning("雪球持仓权重合计为 0，跳过")
            return

        # 雪球现金比例（%）及目标现金金额
        xq_cash_ratio = 100.0 - total_stock_weight
        target_cash = total_amount * (xq_cash_ratio / 100.0)

        # 目标市值字典 {stock_code: target_value}
        # 直接用雪球原始权重（0~100），除以100转为比例
        target: Dict[str, float] = {}
        for p in xq_positions:
            code   = p["stock_code"]
            target[code] = total_amount * (p["weight"] / 100.0)

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
        logger.info(
            f"  再平衡计划  总金额=¥{total_amount:,.0f}  阈值={threshold*100:.1f}%  "
            f"雪球现金={xq_cash_ratio:.1f}%  目标现金=¥{target_cash:,.0f}"
        )
        logger.info(f"  {'代码':<12} {'目标市值':>10} {'当前市值':>10} {'差值':>10}  操作")
        logger.info("  " + "-" * 55)

        for code in sorted(all_codes):
            tgt = target.get(code, 0.0)
            cur = current_value.get(code, 0.0)
            diff = tgt - cur

            # tgt==0 说明该股已从组合移除，交由下方清仓专项循环处理，此处跳过
            if tgt == 0:
                continue

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
        results: List[Tuple[str, str, str]] = []   # (code, direction, status)

        for code, sell_amount in sell_orders:
            status = self._execute_sell_by_value(code, sell_amount)
            results.append((code, "卖出", status))

        # 卖出完成后，重新读取可用现金，按比例分配给各买入单
        # 避免可用现金不足时多笔买单全部失败
        if buy_orders:
            available_cash = self.trader.get_cash()
            total_buy_needed = sum(amt for _, amt in buy_orders)

            if total_buy_needed > 0 and available_cash < total_buy_needed:
                logger.warning(
                    f"  可用现金=¥{available_cash:,.0f}  买入需求=¥{total_buy_needed:,.0f}  "
                    f"现金不足，按比例缩减各买单"
                )
                # 按各买单占总需求的比例分配可用现金
                adjusted_buy_orders = [
                    (code, available_cash * (amt / total_buy_needed))
                    for code, amt in buy_orders
                ]
            else:
                adjusted_buy_orders = buy_orders

            for code, buy_amount in adjusted_buy_orders:
                status = self._execute_buy_by_value(code, buy_amount)
                results.append((code, "买入", status))

        # ── 5. 执行结果汇总 ─────────────────────────────────
        if results:
            ok_count = sum(1 for _, _, s in results if s == "ok")
            skip_count = sum(1 for _, _, s in results if s == "skip")
            fail_count = sum(1 for _, _, s in results if s == "fail")
            logger.info(
                f"  再平衡执行完毕：共 {len(results)} 笔  "
                f"成功={ok_count}  跳过={skip_count}  失败={fail_count}"
            )
            if fail_count > 0:
                fails = [f"{c} {d}({s})" for c, d, s in results if s == "fail"]
                logger.warning(f"  失败明细: {', '.join(fails)}")
            if skip_count > 0:
                skips = [f"{c} {d}" for c, d, s in results if s == "skip"]
                logger.debug(f"  跳过明细: {', '.join(skips)}")
        else:
            logger.info("  无需执行买卖操作")

        # ── 6. 现金对比提示 ───────────────────────────────────
        actual_cash = self.trader.get_cash()
        cash_diff = actual_cash - target_cash
        logger.info(
            f"  现金状态：当前可用=¥{actual_cash:,.0f}  "
            f"目标=¥{target_cash:,.0f}  偏差=¥{cash_diff:+,.0f}"
        )

    # ─────────────────────────────────────────────────────────
    # ratio_follow：按目标金额买入
    # ─────────────────────────────────────────────────────────
    def _execute_buy_by_value(self, code: str, amount: float) -> str:
        """
        买入指定金额的股票（下单前先撤该股旧挂单）

        Returns:
            "ok"      — 下单成功
            "skip"    — 被风控/涨停/无价格等跳过
            "fail"    — 下单失败（QMT 返回失败）
        """
        if not self._risk_check_buy(code, amount):
            return "skip"

        # 先撤该股票的旧挂单，避免重复或冲突
        cancelled = self.trader.cancel_orders_for_stock(code)
        if cancelled:
            logger.info(f"【买入前撤单】{code} 撤销 {cancelled} 笔旧挂单")
            ok = self.trader.wait_until_all_cancelled(
                timeout=3.0, stock_code=code
            )
            if not ok:
                logger.warning(
                    f"【买入前撤单】{code} 撤单未完全确认，"
                    f"但继续执行（旧挂单由 QMT 兜底：可用量校验）"
                )

        price = self.trader.get_latest_price(code)
        if price is None:
            logger.error(f"买入 {code}: 无法获取最新价，跳过")
            return "skip"

        # 涨停保护：涨停时不追买
        if config.LIMIT_PROTECTION and self.trader.is_limit_up(code):
            logger.warning(f"【风控-涨停】{code} 当前已涨停，跳过买入（等下个交易日）")
            return "skip"

        logger.info(f"【按比例买入】{code}  目标金额=¥{amount:,.0f}")
        order_id = self.trader.buy(
            stock_code=code,
            amount=amount,
            price=price,
            remark=f"雪球比例跟单-{config.PORTFOLIO_ID}",
        )
        if order_id is not None and order_id > 0:
            self._chase_orders[order_id] = {
                "stock_code": code,
                "direction":  "BUY",
                "amount":     amount,
                "volume":     0,
                "ts":         time.time(),
            }
            return "ok"
        return "fail"

    # ─────────────────────────────────────────────────────────
    # ratio_follow：按目标金额卖出
    # ─────────────────────────────────────────────────────────
    def _execute_sell_by_value(self, code: str, sell_amount: float) -> str:
        """
        卖出指定市值的股票（下单前先撤该股旧挂单）

        sell_amount: 需要减少的市值（元）

        Returns:
            "ok"      — 下单成功
            "skip"    — 被风控/跌停/无持仓等跳过
            "fail"    — 下单失败

        规则：
          1. 按比例计算卖出股数，向下取整到 100 股的整数倍
          2. 若计算结果 >= can_use，则全部卖出（清仓）
          3. 若计算结果 < 100（不足一手），也全部卖出——
             持仓量太小，强制清仓以完全跟紧组合，避免残留碎股
          4. 若上述均为 0（持仓本就是 0），则跳过
        """
        # 先撤该股票的旧挂单，避免重复或冲突
        cancelled = self.trader.cancel_orders_for_stock(code)
        if cancelled:
            logger.info(f"【卖出前撤单】{code} 撤销 {cancelled} 笔旧挂单")
            ok = self.trader.wait_until_all_cancelled(
                timeout=3.0, stock_code=code
            )
            if not ok:
                logger.warning(
                    f"【卖出前撤单】{code} 撤单未完全确认，"
                    f"但继续执行（旧挂单由 QMT 兜底：可用量校验）"
                )

        positions = self.trader.get_positions()
        pos = positions.get(code)
        if pos is None:
            logger.warning(f"卖出 {code}: 账户中无持仓，跳过")
            return "skip"

        can_use = pos["can_use_volume"]
        if can_use <= 0:
            logger.warning(f"卖出 {code}: 可用股数=0（T+0限制），跳过")
            return "skip"

        price = self.trader.get_latest_price(code)
        if price is None or price <= 0:
            price = pos.get("open_price", 0)
        if price <= 0:
            logger.error(f"卖出 {code}: 无法获取最新价，跳过")
            return "skip"

        # 跌停保护：跌停时不下卖单（次日再处理）
        if config.LIMIT_PROTECTION and self.trader.is_limit_down(code):
            logger.warning(f"【风控-跌停】{code} 当前已跌停，跳过卖出（等下个交易日）")
            return "skip"

        # 按目标差值计算应卖出股数（向下取整到品种最小交易单位整数倍）
        lot = self.trader.get_lot_size(code)
        sell_volume = int(sell_amount / price // lot) * lot

        if sell_volume >= can_use:
            # 超出或等于可用量 → 全部清仓
            sell_volume = can_use
            logger.info(
                f"【按比例卖出】{code}  全部卖出 {sell_volume}股 @ {price:.3f}"
                f"  目标减少≈¥{sell_amount:,.0f}（超出/等于可用持仓，清仓）"
            )
        elif sell_volume < lot:
            # 不足一手 → 取1手，若持仓本身不足一手则全部卖出
            sell_volume = min(lot, can_use)
            logger.info(
                f"【按比例卖出】{code}  比例不足一手({lot}张/股)，取1手 {sell_volume}股 @ {price:.3f}"
                f"  目标减少≈¥{sell_amount:,.0f}"
            )
        else:
            logger.info(
                f"【按比例卖出】{code}  {sell_volume}股 @ {price:.3f}"
                f"  卖出金额≈¥{sell_volume*price:,.0f}  目标减少≈¥{sell_amount:,.0f}"
            )

        if sell_volume <= 0:
            logger.warning(f"卖出 {code}: 可用股数为 0，跳过")
            return "skip"

        order_id = self.trader.sell(
            stock_code=code,
            volume=sell_volume,
            price=price,
            remark=f"雪球比例减仓-{config.PORTFOLIO_ID}",
        )
        if order_id is not None and order_id > 0:
            self._chase_orders[order_id] = {
                "stock_code": code,
                "direction":  "SELL",
                "amount":     0,
                "volume":     sell_volume,
                "ts":         time.time(),
            }
            return "ok"
        return "fail"

    # ─────────────────────────────────────────────────────────
    # 挂单追踪：30 秒未成交撤单按最新价重下
    # ─────────────────────────────────────────────────────────
    _CHASE_INTERVAL = 30   # 检查间隔（秒）

    def _chase_unfinished_orders(self):
        """
        每 30 秒调用一次（仅在交易时间内）：

        1. 遍历 _chase_orders 中超过 30 秒的订单
        2. 通过 get_pending_orders 判断该 order_id 是否仍在挂单
           - 不在挂单列表 → 已成交或已撤，移出追踪
           - 仍在挂单     → 撤单，按最新价重新下单，用新 order_id 替换追踪记录
        3. 新下的单同样纳入追踪，下次 30 秒再检查
        """
        if not self._chase_orders:
            return

        now_ts = time.time()
        if now_ts - self._last_chase_ts < self._CHASE_INTERVAL:
            return
        self._last_chase_ts = now_ts

        # 取当前所有未成交委托（一次性查询，减少 QMT 调用次数）
        pending = self.trader.get_pending_orders()          # 全量，不传 stock_code
        pending_ids = {o["order_id"] for o in pending}

        to_remove: List[int] = []
        to_add:    Dict[int, dict] = {}

        for oid, info in list(self._chase_orders.items()):
            age = now_ts - info["ts"]
            if age < self._CHASE_INTERVAL:
                continue   # 还没到30秒，跳过

            code = info["stock_code"]
            direction = info["direction"]

            if oid not in pending_ids:
                # 已成交或已被撤，移出追踪
                logger.debug(f"【追单】{code} order_id={oid} 已成交/已撤，移出追踪")
                to_remove.append(oid)
                continue

            # 仍未成交 → 撤单后按最新价重下
            logger.info(
                f"【追单】{code} {direction} order_id={oid} "
                f"挂单超 {int(age)}s 未成交，撤单重下..."
            )
            cancelled = self.trader.cancel_orders_for_stock(code)
            if cancelled:
                self.trader.wait_until_all_cancelled(timeout=3.0, stock_code=code)

            to_remove.append(oid)

            # 重新下单
            new_price = self.trader.get_latest_price(code)
            if new_price is None or new_price <= 0:
                logger.error(f"【追单】{code} 无法获取最新价，放弃重下")
                continue

            if direction == "BUY":
                amount = info["amount"]
                # 涨停保护
                if config.LIMIT_PROTECTION and self.trader.is_limit_up(code):
                    logger.warning(f"【追单-风控】{code} 已涨停，放弃重下买入")
                    continue
                # 预估股数仅用于日志，实际下单以 amount 为准（trader.buy 内部换算）
                est_volume = self.trader.calc_buy_volume(
                    amount, new_price, min_lot=self.trader.get_lot_size(code)
                )
                if est_volume <= 0:
                    logger.warning(f"【追单】{code} 计算买入股数为0，放弃重下")
                    continue
                logger.info(
                    f"【追单-重下买入】{code} 约{est_volume}股 @ {new_price:.3f} "
                    f"（目标金额≈¥{amount:,.0f}）"
                )
                new_oid = self.trader.buy(
                    stock_code=code,
                    amount=amount,
                    price=new_price,
                    remark=f"雪球追单买入-{config.PORTFOLIO_ID}",
                )
                chase_volume = est_volume
            else:  # SELL
                volume = info["volume"]
                # 检查可用持仓（持仓可能因部分成交而减少）
                positions = self.trader.get_positions()
                pos = positions.get(code)
                can_use = pos["can_use_volume"] if pos else 0
                if can_use <= 0:
                    logger.warning(f"【追单】{code} 可用持仓为0，无需重下卖出")
                    continue
                sell_vol = min(volume, can_use)
                # 跌停保护
                if config.LIMIT_PROTECTION and self.trader.is_limit_down(code):
                    logger.warning(f"【追单-风控】{code} 已跌停，放弃重下卖出")
                    continue
                logger.info(
                    f"【追单-重下卖出】{code} {sell_vol}股 @ {new_price:.3f}"
                )
                new_oid = self.trader.sell(
                    stock_code=code,
                    volume=sell_vol,
                    price=new_price,
                    remark=f"雪球追单卖出-{config.PORTFOLIO_ID}",
                )
                chase_volume = volume   # SELL 记录原始目标量

            if new_oid is not None and new_oid > 0:
                to_add[new_oid] = {
                    "stock_code": code,
                    "direction":  direction,
                    "amount":     info["amount"],
                    "volume":     chase_volume,
                    "ts":         time.time(),
                }
                logger.info(f"【追单】{code} 新委托 order_id={new_oid}")
            else:
                logger.error(f"【追单】{code} 重下失败")

        for oid in to_remove:
            self._chase_orders.pop(oid, None)
        self._chase_orders.update(to_add)

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
            if self.trader.is_limit_up(code):
                logger.warning(f"【风控-涨停】{code}({name}) 已涨停，跳过买入")
                return
            price = self.trader.get_latest_price(code) or price

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
            if self.trader.is_limit_down(code):
                logger.warning(f"【风控-跌停】{code}({name}) 已跌停，跳过（下个交易日再处理）")
                return

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
