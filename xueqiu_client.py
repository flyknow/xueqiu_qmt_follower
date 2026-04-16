"""
xueqiu_client.py
────────────────────────────────────────────────────────────
雪球组合数据获取模块

功能：
  1. 获取组合当前持仓（positions）
  2. 获取最新调仓记录（rebalancing history）
  3. 轮询消息通知，检测"组合调仓"推送（低频，避免被反爬）

使用方法：
  client = XueqiuClient(cookie, portfolio_id)
  holdings = client.get_holdings()
  rebalance = client.get_latest_rebalancing()
  has_new   = client.poll_notification()

────────────────────────────────────────────────────────────
【接口说明 - 2025/2026 有效版本】
  /cubes/rebalancing/show.json   → 已废弃，返回 404
  /cubes/rebalancing/history.json → 有效，获取调仓历史列表
  stock.xueqiu.com/v5/cube/rebalancing/current.json → 有效，获取当前持仓快照
────────────────────────────────────────────────────────────
"""

import time
import logging
import requests
from typing import Optional, Dict, List

logger = logging.getLogger(__name__)

# ── 雪球 API 基础 URL ──────────────────────────────────────
BASE_URL    = "https://xueqiu.com"
CUBE_BASE   = "https://xueqiu.com/cubes"
STOCK_BASE  = "https://stock.xueqiu.com"   # v5 接口域名

# ── Cookie 过期告警（每 10 分钟最多告警一次，避免刷屏）──────
_last_cookie_alert_ts: float = 0.0
_COOKIE_ALERT_INTERVAL = 600  # 秒


def _alert_cookie_expired():
    """Cookie 失效时输出高优先级日志，并可扩展钉钉/企微告警"""
    global _last_cookie_alert_ts
    now_ts = time.time()
    if now_ts - _last_cookie_alert_ts < _COOKIE_ALERT_INTERVAL:
        return
    _last_cookie_alert_ts = now_ts

    msg = (
        "雪球Token已失效"
    )
    logger.critical(msg)

    # ── 可选：钉钉告警（填写 webhook 后取消注释）──────────────
    _send_dingtalk(msg)

    # ── 可选：企业微信机器人（填写 webhook 后取消注释）──────────
    # _send_wecom(msg)


def _send_dingtalk(text: str):
    """发送钉钉机器人告警（需在 config.py 配置 DINGTALK_WEBHOOK）"""
    try:
        import config as _cfg
        webhook = getattr(_cfg, "DINGTALK_WEBHOOK", "")
        if not webhook:
            return
        requests.post(webhook, json={"msgtype": "text", "text": {"content": text}}, timeout=5)
    except Exception:
        pass


def _send_wecom(text: str):
    """发送企业微信机器人告警（需在 config.py 配置 WECOM_WEBHOOK）"""
    try:
        import config as _cfg
        webhook = getattr(_cfg, "WECOM_WEBHOOK", "")
        if not webhook:
            return
        requests.post(webhook, json={"msgtype": "text", "text": {"content": text}}, timeout=5)
    except Exception:
        pass


class XueqiuClient:
    """雪球组合数据客户端"""

    def __init__(self, cookie: str, portfolio_id: str):
        """
        Args:
            cookie:       登录后的雪球 Cookie 字符串
            portfolio_id: 组合代码，如 'ZH123456'
        """
        self.portfolio_id = portfolio_id.upper()
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            "Referer": "https://xueqiu.com/",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "Cookie": cookie,
        })
        # 首次访问主页以初始化 session（获取 token 等 cookie）
        self._init_session()
        # 记录上次轮询到的最新消息 id，避免重复触发
        self._last_notification_id: Optional[int] = None

    # ─────────────────────────────────────────────────────────
    # 内部辅助
    # ─────────────────────────────────────────────────────────
    def _init_session(self):
        """访问主页以刷新 session"""
        try:
            self.session.get(BASE_URL, timeout=10)
            logger.debug("XueqiuClient: session 初始化完成")
        except Exception as e:
            logger.warning(f"XueqiuClient: session 初始化失败 ({e})，继续运行")

    def _get(self, url: str, params: dict = None) -> Optional[dict]:
        """带错误处理的 GET 请求"""
        try:
            resp = self.session.get(url, params=params, timeout=10)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code
            logger.error(f"HTTP 错误 {status}: {url}")
            if status == 401:
                _alert_cookie_expired()
            elif status == 403:
                logger.error("403 Forbidden：可能触发雪球反爬，建议降低轮询频率或更换 Cookie")
        except requests.exceptions.RequestException as e:
            logger.error(f"请求异常: {url} -> {e}")
        return None

    # ─────────────────────────────────────────────────────────
    # 核心接口
    # ─────────────────────────────────────────────────────────
    def get_current_positions(self) -> List[Dict]:
        """
        获取组合当前最新持仓快照（2025/2026 有效版本）

        接口优先级：
          1. xueqiu.com/cubes/rebalancing/current.json （新版，推荐）
          2. stock.xueqiu.com/v5/cube/rebalancing/current.json （旧版，可能404）
          3. xueqiu.com/cubes/rebalancing/history.json （降级）

        Returns:
            [{"stock_code": "600519.SH", "stock_name": "贵州茅台",
              "weight": 30.5, "prev_weight": 25.0, "price": 1680.0}, ...]
        """
        # 方案1：新版接口 (xueqiu.com)
        url1 = f"{BASE_URL}/cubes/rebalancing/current.json"
        params1 = {"cube_symbol": self.portfolio_id}
        data = self._get(url1, params1)
        source = "current"

        # 方案2：旧版v5接口 (stock.xueqiu.com)
        if data is None:
            url2 = f"{STOCK_BASE}/v5/cube/rebalancing/current.json"
            params2 = {"cube_symbol": self.portfolio_id, "count": 1, "page": 1}
            data = self._get(url2, params2)
            source = "v5"

        if data is None:
            # 降级：尝试从调仓历史第一条解析
            return self._get_positions_from_history()

        positions = []
        try:
            if source == "current":
                # 新版接口格式
                last_rb = data.get("data", {}).get("last_rb") or data.get("last_rb") or {}
            else:
                # v5接口格式
                last_rb = data.get("data", {}).get("last_rb") or data.get("last_rb") or {}

            holdings = last_rb.get("holdings", [])
            for item in holdings:
                weight = float(item.get("weight") or 0)
                if weight <= 0:
                    continue
                raw_code = str(item.get("stock_symbol", "")).upper()
                positions.append({
                    "stock_code":   _to_qmt_code(raw_code),
                    "stock_symbol": raw_code,
                    "stock_name":   item.get("stock_name", ""),
                    "weight":       weight,
                    "prev_weight":  float(item.get("prev_weight") or 0),
                    "price":        float(item.get("price") or 0),
                })
        except Exception as e:
            logger.warning(f"{source} 接口解析失败({e})，降级到 history 接口")
            return self._get_positions_from_history()

        logger.info(f"[{self.portfolio_id}] 当前持仓 {len(positions)} 只 ({source})")
        return positions

    def _get_positions_from_history(self) -> List[Dict]:
        """
        降级方案：从调仓历史第一条提取持仓（当 v5 接口不可用时）
        """
        url = f"{CUBE_BASE}/rebalancing/history.json"
        params = {"cube_symbol": self.portfolio_id, "count": 1, "page": 1}
        data = self._get(url, params)
        if data is None:
            return []

        positions = []
        try:
            records = data.get("list", [])
            if not records:
                return []
            latest = records[0]
            for item in latest.get("rebalancing_histories", []):
                weight = float(item.get("target_weight") or item.get("weight") or 0)
                if weight <= 0:
                    continue
                raw_code = str(item.get("stock_symbol", "")).upper()
                positions.append({
                    "stock_code":   _to_qmt_code(raw_code),
                    "stock_symbol": raw_code,
                    "stock_name":   item.get("stock_name", ""),
                    "weight":       weight,
                    "prev_weight":  float(item.get("prev_weight") or 0),
                    "price":        float(item.get("price") or 0),
                })
        except Exception as e:
            logger.error(f"history 接口解析持仓失败: {e}")

        logger.info(f"[{self.portfolio_id}] 当前持仓 {len(positions)} 只 (history降级)")
        return positions

    # 保留别名，兼容旧调用
    def get_holdings(self) -> List[Dict]:
        return self.get_current_positions()

    def get_latest_rebalancing(self) -> Optional[Dict]:
        """
        获取最新一次调仓记录

        接口优先级：
          1. xueqiu.com/cubes/rebalancing/history.json （推荐）
          2. xueqiu.com/snowmart/cube/rebalancing/history.json （备用）

        Returns:
            {
              "id":          123456,
              "created_at":  1700000000000,   # 毫秒时间戳
              "buy_list":    [{"stock_code": "600519.SH", "weight": 30.5, ...}],
              "sell_list":   [{"stock_code": "000858.SZ", "weight": 0,   ...}],
              "increase_list": [...],   # 加仓
              "decrease_list": [...],   # 减仓
            }
            返回 None 表示获取失败
        """
        # 方案1：标准接口
        url = f"{BASE_URL}/cubes/rebalancing/history.json"
        params = {
            "cube_symbol": self.portfolio_id,
            "count": 1,
            "page": 1,
        }
        data = self._get(url, params)

        # 方案2：备用接口 (snowmart)
        if data is None:
            url2 = f"{BASE_URL}/snowmart/cube/rebalancing/history.json"
            data = self._get(url2, params)

        if data is None:
            return None

        try:
            records = data.get("list", [])
            if not records:
                return None
            latest = records[0]
            result = {
                "id":         latest.get("id"),
                "created_at": latest.get("created_at"),
                "buy_list":      [],
                "sell_list":     [],
                "increase_list": [],
                "decrease_list": [],
                "raw": latest,
            }
            for item in latest.get("rebalancing_histories", []):
                prev   = float(item.get("prev_weight") or 0)
                target = float(item.get("target_weight") or item.get("weight") or 0)
                qmt    = _to_qmt_code(str(item.get("stock_symbol", "")).upper())
                entry  = {
                    "stock_code":  qmt,
                    "stock_name":  item.get("stock_name", ""),
                    "prev_weight": prev,
                    "weight":      target,
                    "price":       float(item.get("price") or 0),
                }
                if prev == 0 and target > 0:
                    result["buy_list"].append(entry)
                elif prev > 0 and target == 0:
                    result["sell_list"].append(entry)
                elif target > prev:
                    result["increase_list"].append(entry)
                elif target < prev:
                    result["decrease_list"].append(entry)

            logger.info(
                f"[{self.portfolio_id}] 最新调仓: "
                f"买入{len(result['buy_list'])}只 卖出{len(result['sell_list'])}只 "
                f"加仓{len(result['increase_list'])}只 减仓{len(result['decrease_list'])}只"
            )
            return result
        except Exception as e:
            logger.error(f"解析调仓记录失败: {e}")
            return None

    def poll_notification(self) -> bool:
        """
        轮询雪球消息通知，检测是否有新的"组合调仓"推送。

        原理：定期检查最新调仓记录的 ID，如发现新 ID 则认为有新调仓。

        Returns:
            True  — 检测到新的组合调仓通知
            False — 无新通知
        """
        # 直接检查最新调仓记录的 ID
        latest = self.get_latest_rebalancing()
        if latest is None:
            return False

        try:
            latest_id = latest.get("id")
            if latest_id is None:
                return False

            # 首次运行，仅记录 id，不触发
            if self._last_notification_id is None:
                self._last_notification_id = latest_id
                logger.debug(f"初始化调仓 ID: {latest_id}")
                return False

            if latest_id == self._last_notification_id:
                return False  # 没有新调仓

            # 发现新调仓
            logger.info(f"检测到 [{self.portfolio_id}] 新调仓！ID={latest_id}")
            self._last_notification_id = latest_id
            return True

        except Exception as e:
            logger.error(f"解析调仓通知失败: {e}")
            return False



# ─────────────────────────────────────────────────────────────
# 工具函数
# ─────────────────────────────────────────────────────────────
def _to_qmt_code(raw_code: str) -> str:
    """
    将雪球股票代码转为 miniQMT 格式

    处理以下输入格式：
      SH510310  → 510310.SH   （雪球前缀格式）
      SZ000001  → 000001.SZ
      BJ430047  → 430047.BJ
      HK00700   → 00700.HK
      600519    → 600519.SH   （纯数字，按首位判断）
      000858    → 000858.SZ
      510310.SH → 510310.SH   （已是QMT格式，原样返回）
    """
    code = raw_code.strip().upper()

    # 已是 QMT 格式（含 .）→ 原样返回
    if "." in code:
        return code

    # 雪球 SH/SZ/BJ/HK 前缀格式 → 转为 数字.后缀
    if code.startswith("SH") and len(code) == 8:
        return f"{code[2:]}.SH"
    if code.startswith("SZ") and len(code) == 8:
        return f"{code[2:]}.SZ"
    if code.startswith("BJ") and len(code) == 8:
        return f"{code[2:]}.BJ"
    if code.startswith("HK") and len(code) == 7:
        return f"{code[2:]}.HK"

    # 纯6位数字 → 按首位判断市场
    if code.isdigit() and len(code) == 6:
        if code.startswith("6"):
            return f"{code}.SH"
        elif code.startswith(("0", "3")):
            return f"{code}.SZ"
        elif code.startswith(("8", "4")):
            return f"{code}.BJ"

    # 港股：纯5位数字
    if code.isdigit() and len(code) == 5:
        return f"{code}.HK"

    return code
