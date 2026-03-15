# -*- coding: utf-8 -*-
"""
CTP实时数据采集器（独立运行版）
连接CTP行情接口，实时收集Tick数据并聚合为1分钟K线存入vnpy数据库

CTP账户支持最多5个并发连接，可与trade2026同时运行。

用法:
  # 采集指定品种的期货+期权
  python3 ctp_data_collector.py --product AG SA TA

  # 采集全市场期权（含对应期货）
  python3 ctp_data_collector.py --all-options

  # 采集指定合约
  python3 ctp_data_collector.py ag2506 SA605C1500

Version: 3.0
Date: 2026-02-26
"""

import os
import re
import sys
import json
import time
import sqlite3
import signal
import logging
import argparse
import subprocess
import threading
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set
from collections import Counter

# ============================================================================
# 日志配置
# ============================================================================

LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

_log_file = LOG_DIR / f"collector_{datetime.now():%Y%m%d}.log"
_is_daemon = "--daemon" in sys.argv or "-d" in sys.argv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.FileHandler(_log_file, encoding="utf-8"),
    ] if _is_daemon else [
        logging.StreamHandler(),
        logging.FileHandler(_log_file, encoding="utf-8"),
    ],
)
logger = logging.getLogger("CTPCollector")

# ============================================================================
# vnpy导入
# ============================================================================

try:
    from vnpy.event import EventEngine, Event
    from vnpy.trader.engine import MainEngine
    from vnpy.trader.constant import Exchange, Product
    from vnpy.trader.object import SubscribeRequest, TickData as VnTickData
    from vnpy.trader.event import EVENT_TICK, EVENT_CONTRACT
    from vnpy_ctp import CtpGateway
    VNPY_OK = True
except ImportError as e:
    VNPY_OK = False
    logger.error(f"vnpy/vnpy_ctp 未安装: {e}")
    logger.error("请运行: pip3 install vnpy vnpy-ctp")

def notify(title: str, message: str):
    """macOS系统通知"""
    try:
        subprocess.run([
            "osascript", "-e",
            f'display notification "{message}" with title "{title}" sound name "Glass"'
        ], capture_output=True, timeout=5)
    except Exception:
        pass

# ============================================================================
# 交易时段
# ============================================================================

TRADING_SESSIONS = [
    (21, 0, 23, 59, "夜盘前半"),
    (0, 0, 2, 30, "夜盘后半"),
    (9, 0, 11, 30, "早盘"),       # 合并为一个时段，中间休息不退出
    (13, 30, 15, 0, "午盘"),
]


EARLY_BUFFER = 15  # 提前15分钟唤醒连接CTP（连接可能耗时数分钟）
LATE_BUFFER = 5    # 延后5分钟关闭（确保尾盘数据写入）


def is_trading_day() -> bool:
    """判断今天是否为交易日（周一至周五，不含节假日）"""
    now = datetime.now()
    # 周六=5, 周日=6
    if now.weekday() >= 5:
        return False
    # 节假日配置文件（与start_ctp_reader.sh共用）
    import os
    holidays_file = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                 "trading_holidays_2026.conf")
    if os.path.exists(holidays_file):
        today_str = now.strftime("%Y-%m-%d")
        with open(holidays_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and line.split()[0] == today_str:
                    return False
    return True


def is_trading_time() -> bool:
    """判断当前是否在交易时段内（含工作日判断 + 提前15分钟唤醒）"""
    now = datetime.now()
    weekday = now.weekday()  # 0=周一 ... 6=周日

    # 周六周日不交易（周五夜盘算周五，凌晨段在周六但属于周五夜盘）
    if weekday == 6:  # 周日：全天不交易
        return False
    if weekday == 5:  # 周六：只有凌晨段（周五夜盘延续到周六凌晨）
        t = now.hour * 60 + now.minute
        for sh, sm, eh, em, name in TRADING_SESSIONS:
            if sh < 3:  # 只匹配凌晨段 (0:00-2:30)
                end = eh * 60 + em + LATE_BUFFER
                if t <= end:
                    return True
        return False
    if weekday == 0:  # 周一凌晨不交易（周日没有夜盘，不存在延续段）
        t = now.hour * 60 + now.minute
        if t < 3 * 60:
            return False

    # 周一至周五：正常判断时间
    if not is_trading_day():
        return False

    t = now.hour * 60 + now.minute
    for sh, sm, eh, em, _ in TRADING_SESSIONS:
        start = sh * 60 + sm - EARLY_BUFFER
        end = eh * 60 + em + LATE_BUFFER
        if start <= t <= end:
            return True
    return False


def next_session_wait_seconds() -> int:
    """计算距离下一个交易时段的等待秒数"""
    now = datetime.now()
    t = now.hour * 60 + now.minute
    weekday = now.weekday()

    # 如果是周末，直接算到周一早上
    if weekday == 5:  # 周六
        # 等到周一08:45 (早盘提前15分钟)
        # 周六到周一 = 2天
        return (2 * 24 * 60 - t + (9 * 60 - EARLY_BUFFER)) * 60
    if weekday == 6:  # 周日
        return (1 * 24 * 60 - t + (9 * 60 - EARLY_BUFFER)) * 60

    # 工作日：按时间顺序找下一个时段
    starts = sorted(sh * 60 + sm - EARLY_BUFFER for sh, sm, _, _, _ in TRADING_SESSIONS)
    for s in starts:
        if s > t:
            return (s - t) * 60
    # 今天所有时段都过了，等到明天第一个时段（夜盘 20:45）
    return (24 * 60 - t + starts[0]) * 60


def extract_product_prefix(symbol: str) -> str:
    """从合约代码中提取品种前缀: ag2606C7000 → AG, SA605 → SA"""
    m = re.match(r'^([A-Za-z]+)', symbol)
    return m.group(1).upper() if m else ""

# ============================================================================
# K线聚合器
# ============================================================================

class BarAggregator:
    """将Tick数据聚合为1分钟K线"""

    def __init__(self):
        self._bars: Dict[str, dict] = {}
        self._lock = threading.Lock()
        self._completed: List[dict] = []

    def update_tick(self, symbol, exchange, tick_time, price, volume, turnover, oi):
        if tick_time.tzinfo:
            tick_time = tick_time.replace(tzinfo=None)
        bar_minute = tick_time.replace(second=0, microsecond=0)
        with self._lock:
            if symbol in self._bars:
                bar = self._bars[symbol]
                if bar_minute > bar["datetime"]:
                    completed_bar = bar.copy()
                    completed_bar.pop("_last_tick", None)
                    self._completed.append(completed_bar)
                    self._bars[symbol] = self._new_bar(symbol, exchange, bar_minute, price, volume, turnover, oi)
                else:
                    bar["high"] = max(bar["high"], price)
                    bar["low"] = min(bar["low"], price)
                    bar["close"] = price
                    bar["volume"] = volume
                    bar["turnover"] = turnover
                    bar["open_interest"] = oi
            else:
                self._bars[symbol] = self._new_bar(symbol, exchange, bar_minute, price, volume, turnover, oi)
            self._bars[symbol]["_last_tick"] = tick_time

    @staticmethod
    def _new_bar(symbol, exchange, dt, price, volume, turnover, oi):
        return {"symbol": symbol, "exchange": exchange, "datetime": dt,
                "open": price, "high": price, "low": price, "close": price,
                "volume": volume, "turnover": turnover, "open_interest": oi}

    def flush(self):
        with self._lock:
            bars = self._completed
            self._completed = []
            # 把超过2分钟没有新tick的bar也输出（低流动性合约兜底）
            now = datetime.now()
            stale_syms = []
            for sym, bar in self._bars.items():
                last_tick = bar.get("_last_tick")
                if not last_tick:
                    continue
                if (now - last_tick).total_seconds() > 120:
                    bar_copy = bar.copy()
                    bar_copy.pop("_last_tick", None)
                    bars.append(bar_copy)
                    stale_syms.append(sym)
            for sym in stale_syms:
                del self._bars[sym]
            return bars

    def flush_all(self):
        with self._lock:
            bars = self._completed
            self._completed = []
            for bar in self._bars.values():
                bar_copy = bar.copy()
                bar_copy.pop("_last_tick", None)
                bars.append(bar_copy)
            self._bars.clear()
            return bars

# ============================================================================
# 数据库写入
# ============================================================================

class DatabaseWriter:
    def __init__(self, db_path=None):
        if db_path is None:
            db_path = str(Path.home() / ".vntrader" / "database.db")
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_db()
        self._total_written = 0

    def _init_db(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS dbbardata (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL, exchange TEXT NOT NULL,
                datetime TEXT NOT NULL, interval TEXT NOT NULL,
                volume REAL, turnover REAL, open_interest REAL,
                open_price REAL, high_price REAL, low_price REAL, close_price REAL,
                UNIQUE(symbol, exchange, interval, datetime)
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_dbbardata_symbol_datetime
            ON dbbardata(symbol, interval, datetime)
        """)
        conn.commit()
        conn.close()
        logger.info(f"数据库: {self.db_path}")

    def write_bars(self, bars):
        if not bars:
            return
        for attempt in range(3):
            try:
                conn = sqlite3.connect(self.db_path, timeout=30)
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA busy_timeout=30000")
                written = 0
                for bar in bars:
                    try:
                        conn.execute(
                            "INSERT OR REPLACE INTO dbbardata "
                            "(symbol,exchange,datetime,interval,volume,turnover,"
                            "open_interest,open_price,high_price,low_price,close_price) "
                            "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                            (bar["symbol"], bar["exchange"],
                             bar["datetime"].strftime("%Y-%m-%d %H:%M:%S"), "1m",
                             bar["volume"], bar["turnover"], bar["open_interest"],
                             bar["open"], bar["high"], bar["low"], bar["close"]))
                        written += 1
                    except Exception as e:
                        logger.error(f"写入失败 {bar['symbol']}: {e}")
                conn.commit()
                conn.close()
                self._total_written += written
                if written > 0:
                    logger.info(f"写入 {written} 条K线 (累计 {self._total_written})")
                return
            except sqlite3.OperationalError as e:
                logger.warning(f"数据库锁冲突(第{attempt+1}次): {e}")
                try:
                    conn.close()
                except Exception:
                    pass
                time.sleep(2 * (attempt + 1))
        logger.error("数据库写入3次重试均失败，跳过本批次")

    @property
    def total_written(self):
        return self._total_written

# ============================================================================
# 主采集器
# ============================================================================

class CTPDataCollector:

    def __init__(self, config_path, symbols=None, products=None, all_options=False,
                 full_option_products=None):
        self.config = self._load_config(config_path)
        self.target_symbols = [s.lower() for s in (symbols or [])]
        self.target_products = [p.upper() for p in (products or [])]
        self.all_options = all_options
        # 这些品种订阅全部期权（不限档数）
        self.full_option_products = set(p.upper() for p in (full_option_products or []))
        self.event_engine = None
        self.main_engine = None
        self.aggregator = BarAggregator()
        self.db_writer = DatabaseWriter()
        self._connected = False
        self._running = False
        self._subscribed: Set[str] = set()
        self._ticked_contracts: Set[str] = set()  # 实际收到tick的合约
        self._tick_count = 0
        self._last_status_time = time.time()
        self._subscribe_batch_size = 200  # 每批订阅数量
        self._subscribe_batch_delay = 0.3  # 批次间延迟(秒)
        self._last_all_contracts = []  # 缓存最近一次all_contracts用于新合约发现

    @staticmethod
    def _load_config(path):
        config_file = Path(path)
        if not config_file.exists():
            alt = Path(__file__).parent / "ctp_config.json"
            if alt.exists():
                config_file = alt
            else:
                logger.error(f"CTP配置文件不存在: {path}")
                sys.exit(1)
        with open(config_file, "r", encoding="utf-8") as f:
            config = json.load(f)
        logger.info(f"配置: {config_file}")
        return config

    def connect(self) -> bool:
        if not VNPY_OK:
            return False

        self.event_engine = EventEngine()
        self.main_engine = MainEngine(self.event_engine)
        self.main_engine.add_gateway(CtpGateway)

        self.event_engine.register(EVENT_TICK, self._on_tick)
        self.event_engine.register(EVENT_CONTRACT, self._on_contract)

        setting = {
            "用户名": self.config.get("userid", ""),
            "密码": self.config.get("password", ""),
            "经纪商代码": self.config.get("brokerid", ""),
            "交易服务器": self.config.get("td_address", ""),
            "行情服务器": self.config.get("md_address", ""),
            "产品名称": self.config.get("product_info") or self.config.get("appid", ""),
            "授权编码": self.config.get("auth_code", ""),
        }

        logger.info(f"CTP设置: 用户={setting['用户名']}, 经纪商={setting['经纪商代码']}, "
                    f"产品名称={setting['产品名称']}, 行情={setting['行情服务器']}")
        logger.info("正在连接CTP...")
        self.main_engine.connect(setting, "CTP")

        # 等待合约加载完成（合约数量稳定3秒后认为加载完毕，最多90秒）
        last_count = 0
        stable_ticks = 0
        for i in range(900):
            time.sleep(0.1)
            count = len(self.main_engine.get_all_contracts())
            if count > 0 and count == last_count:
                stable_ticks += 1
                if stable_ticks >= 30:  # 连续3秒数量不变
                    break
            else:
                stable_ticks = 0
            last_count = count
            if (i + 1) % 100 == 0:
                logger.info(f"连接中... {(i+1)*0.1:.0f}秒, 已加载 {count} 个合约")

        total = len(self.main_engine.get_all_contracts())
        if total > 0:
            self._connected = True
            options = sum(1 for c in self.main_engine.get_all_contracts()
                         if c.product == Product.OPTION)
            futures = total - options
            logger.info(f"CTP连接成功！合约: {total} (期货{futures} + 期权{options})")
            notify("CTP数据采集", f"连接成功，{total}个合约")
            return True

        logger.error("CTP连接超时（90秒）")
        notify("CTP数据采集", "连接超时，请检查网络")
        return False

    @staticmethod
    def _extract_month(symbol: str) -> str:
        """提取合约月份: ag2606 → 2606, SA605 → 605, TA604C4800 → 604"""
        prefix = extract_product_prefix(symbol)
        rest = symbol[len(prefix):]
        m = re.match(r'(\d{3,4})', rest)
        return m.group(1) if m else ""

    @staticmethod
    def _parse_option(symbol: str):
        """解析期权合约: TA605C5000 → ('TA','605','C',5000), ag2606C7000 → ('ag','2606','C',7000)"""
        m = re.match(r'^([A-Za-z]+)(\d{3,4})-?([CP])-?(\d+)', symbol)
        if m:
            return m.group(1), m.group(2), m.group(3), int(m.group(4))
        return None

    # 每个品种每侧（Call/Put）选取的虚值期权档数
    OTM_STRIKES_PER_SIDE = 20

    def subscribe_symbols(self):
        if not self._connected:
            return

        all_contracts = self.main_engine.get_all_contracts()
        self._last_all_contracts = list(all_contracts)

        if self.all_options:
            self._subscribe_all_options_smart(all_contracts)
        else:
            self._subscribe_by_filter(all_contracts)

    def _subscribe_by_filter(self, all_contracts):
        """指定品种/合约模式"""
        to_subscribe = []
        for contract in all_contracts:
            symbol_lower = contract.symbol.lower()
            prefix = extract_product_prefix(contract.symbol)
            matched = False
            if self.target_symbols and symbol_lower in self.target_symbols:
                matched = True
            if self.target_products and prefix in self.target_products:
                matched = True
            if matched:
                to_subscribe.append(contract)

        self._do_subscribe(to_subscribe)

    def _subscribe_all_options_smart(self, all_contracts):
        """
        全市场期权智能订阅（两阶段）:
        阶段1: 订阅每个有期权品种的近月期货，获取价格
        阶段2: 根据期货价格，选择每侧10档虚值期权订阅
        """
        from collections import defaultdict

        # ---- 分组 ----
        options_by_product = defaultdict(list)
        futures_by_product = defaultdict(list)

        for c in all_contracts:
            prefix = extract_product_prefix(c.symbol)
            if c.product == Product.OPTION:
                options_by_product[prefix].append(c)
            elif c.product == Product.FUTURES:
                futures_by_product[prefix].append(c)

        option_products = sorted(options_by_product.keys())
        logger.info(f"有期权的品种 ({len(option_products)}): {option_products}")

        # ---- 每个品种选近月期货 (最多2个月) ----
        selected_futures = []
        product_months = {}  # {品种: [近月, 次近月]}

        for prod in option_products:
            futs = futures_by_product.get(prod, [])
            opts = options_by_product[prod]

            # 期权涉及的月份
            opt_months = sorted(set(self._extract_month(c.symbol) for c in opts))
            active_months = opt_months[:2] if len(opt_months) >= 2 else opt_months
            product_months[prod] = active_months

            # 选对应月份期货
            for f in futs:
                if self._extract_month(f.symbol) in active_months:
                    selected_futures.append(f)

        # ---- 阶段1: 先订阅期货，等待价格 ----
        logger.info(f"阶段1: 订阅 {len(selected_futures)} 个期货，等待价格...")
        for f in selected_futures:
            req = SubscribeRequest(symbol=f.symbol, exchange=f.exchange)
            self.main_engine.subscribe(req, "CTP")
            self._subscribed.add(f.vt_symbol)

        # 等待期货价格到达（最多15秒）
        time.sleep(5)
        futures_prices = {}  # {品种+月份: 价格}
        for attempt in range(10):
            got_new = False
            for f in selected_futures:
                key = extract_product_prefix(f.symbol) + self._extract_month(f.symbol)
                if key in futures_prices:
                    continue
                tick = self.main_engine.get_tick(f.vt_symbol)
                if tick and tick.last_price and tick.last_price > 0:
                    futures_prices[key] = tick.last_price
                    got_new = True
            if not got_new and len(futures_prices) > 0:
                break
            time.sleep(1)

        # 对缺失价格的期货，尝试从数据库获取上次收盘价作为兜底
        missing_keys = []
        for f in selected_futures:
            key = extract_product_prefix(f.symbol) + self._extract_month(f.symbol)
            if key not in futures_prices:
                missing_keys.append((key, f.symbol))
        if missing_keys:
            try:
                import sqlite3 as _sql
                _db = _sql.connect(self.db_writer.db_path)
                _cur = _db.cursor()
                for key, sym in missing_keys:
                    _cur.execute(
                        "SELECT close_price FROM dbbardata WHERE symbol=? "
                        "AND close_price>0 ORDER BY datetime DESC LIMIT 1",
                        (sym,))
                    row = _cur.fetchone()
                    if row and row[0]:
                        futures_prices[key] = row[0]
                        logger.info(f"从数据库补充期货价格: {sym} = {row[0]}")
                _db.close()
            except Exception as e:
                logger.warning(f"从数据库补充价格失败: {e}")

        logger.info(f"获取到 {len(futures_prices)}/{len(selected_futures)} 个期货价格")

        # ---- 阶段2: 订阅所有活跃月份的全部期权 ----
        selected_options = []
        for prod in option_products:
            active_months = product_months.get(prod, [])
            opts = options_by_product[prod]
            for c in opts:
                if self._extract_month(c.symbol) in active_months:
                    selected_options.append(c)

        # ---- 分批订阅期权（防止CTP静默丢弃） ----
        self._batch_subscribe(selected_options, label="期权")

        # ---- 阶段3: 额外订阅无期权的纯期货品种（价差监控用） ----
        _EXTRA_FUTURES = {'HC', 'NR', 'CY', 'LU', 'J'}  # 无期权但价差监控需要
        extra_futures = []
        already_subscribed_products = set(option_products)
        for c in all_contracts:
            prefix = extract_product_prefix(c.symbol)
            if prefix in _EXTRA_FUTURES and prefix not in already_subscribed_products:
                if c.product == Product.FUTURES:
                    month = self._extract_month(c.symbol)
                    if month:
                        extra_futures.append(c)
        # 每个品种只取近月2个
        from itertools import groupby
        extra_dedup = []
        extra_futures.sort(key=lambda c: (extract_product_prefix(c.symbol), c.symbol))
        for prod, group in groupby(extra_futures, key=lambda c: extract_product_prefix(c.symbol)):
            contracts_list = sorted(group, key=lambda c: self._extract_month(c.symbol))
            for c in contracts_list[:2]:
                req = SubscribeRequest(symbol=c.symbol, exchange=c.exchange)
                self.main_engine.subscribe(req, "CTP")
                self._subscribed.add(c.vt_symbol)
                extra_dedup.append(c)
        if extra_dedup:
            logger.info(f"阶段3: 额外订阅 {len(extra_dedup)} 个纯期货 "
                        f"({', '.join(set(extract_product_prefix(c.symbol) for c in extra_dedup))})")

        # ---- 统计 ----
        opt_by_prod = Counter(extract_product_prefix(c.symbol) for c in selected_options)
        logger.info(f"阶段2: 订阅 {len(selected_options)} 个期权")
        logger.info(f"总计: {len(selected_futures)+len(extra_dedup)}期货 + {len(selected_options)}期权 "
                    f"= {len(self._subscribed)}合约")
        for prod in sorted(opt_by_prod.keys()):
            logger.info(f"  {prod}: {opt_by_prod[prod]}期权")

    def _do_subscribe(self, contracts):
        if not contracts:
            logger.warning("没有匹配的合约！")
            return
        self._batch_subscribe(contracts)
        logger.info(f"已订阅 {len(contracts)} 个合约")

    def _batch_subscribe(self, contracts, label=""):
        """分批订阅合约，防止CTP因一次性大量订阅而静默丢弃"""
        total = len(contracts)
        batch_size = self._subscribe_batch_size
        for i in range(0, total, batch_size):
            batch = contracts[i:i + batch_size]
            for c in batch:
                req = SubscribeRequest(symbol=c.symbol, exchange=c.exchange)
                self.main_engine.subscribe(req, "CTP")
                self._subscribed.add(c.vt_symbol)
            batch_num = i // batch_size + 1
            total_batches = (total + batch_size - 1) // batch_size
            if total > batch_size:
                logger.debug(f"{label}订阅批次 {batch_num}/{total_batches}: {len(batch)}个")
            if i + batch_size < total:
                time.sleep(self._subscribe_batch_delay)

    def _on_tick(self, event):
        tick = event.data
        if not tick.last_price or tick.last_price <= 0:
            return
        self._tick_count += 1
        self._ticked_contracts.add(tick.vt_symbol)
        self.aggregator.update_tick(
            tick.symbol,
            tick.exchange.value if hasattr(tick.exchange, 'value') else str(tick.exchange),
            tick.datetime or datetime.now(),
            tick.last_price,
            int(tick.volume) if tick.volume else 0,
            tick.turnover or 0.0,
            tick.open_interest or 0.0,
        )

    def _on_contract(self, event):
        pass

    def _write_loop(self):
        while self._running:
            time.sleep(60)
            if not self._running:
                break
            try:
                bars = self.aggregator.flush()
                self.db_writer.write_bars(bars)
            except Exception as e:
                logger.error(f"写入循环异常(不退出): {e}")

            now = time.time()
            if now - self._last_status_time > 300:
                self._last_status_time = now
                ticked = len(self._ticked_contracts)
                subscribed = len(self._subscribed)
                silent = subscribed - ticked
                logger.info(f"状态: {self._tick_count} ticks | "
                           f"{self.db_writer.total_written} bars | "
                           f"{subscribed} 订阅 | {ticked} 活跃 | {silent} 沉默")

    def _subscription_health_loop(self):
        """订阅健康监控：定期检查沉默合约并重新订阅 + 发现新上市合约"""
        # 启动后等待3分钟，让合约有足够时间产生tick
        _initial_wait = 180
        for _ in range(int(_initial_wait / 5)):
            time.sleep(5)
            if not self._running:
                return

        _check_interval = 900  # 每15分钟检查一次
        while self._running:
            try:
                self._resub_silent_contracts()
                self._discover_new_contracts()
            except Exception as e:
                logger.error(f"订阅健康检查异常: {e}")

            for _ in range(int(_check_interval / 10)):
                time.sleep(10)
                if not self._running:
                    return

    def _resub_silent_contracts(self):
        """重新订阅已订阅但未收到任何tick的合约"""
        if not self._ticked_contracts:
            return

        silent = self._subscribed - self._ticked_contracts
        if not silent:
            logger.info(f"[健康检查] 全部 {len(self._subscribed)} 个订阅均正常")
            return

        # 过滤：只重订阅期权（期货流动性差的可以不管）
        silent_options = [vt for vt in silent if 'C' in vt.upper().split('.')[0] or 'P' in vt.upper().split('.')[0]]
        if not silent_options:
            logger.info(f"[健康检查] {len(silent)} 个沉默合约均为期货，跳过")
            return

        logger.warning(f"[健康检查] 发现 {len(silent_options)} 个期权订阅沉默，重新订阅...")
        # 从vt_symbol解析出symbol和exchange重新订阅
        resub_count = 0
        batch = []
        for vt in silent_options:
            parts = vt.split(".")
            if len(parts) == 2:
                symbol, exchange_str = parts
                try:
                    exchange = Exchange(exchange_str)
                    # 用一个简单的对象模拟contract
                    class _C:
                        pass
                    c = _C()
                    c.symbol = symbol
                    c.exchange = exchange
                    c.vt_symbol = vt
                    batch.append(c)
                except Exception:
                    pass

        if batch:
            # 重订阅前先清除这些合约的订阅记录，让_batch_subscribe重新添加
            for c in batch:
                self._subscribed.discard(c.vt_symbol)
            self._batch_subscribe(batch, label="重订阅")
            logger.info(f"[健康检查] 重新订阅了 {len(batch)} 个沉默合约")
        else:
            logger.info(f"[健康检查] 无法解析沉默合约，跳过重订阅")

    def _discover_new_contracts(self):
        """查询CTP合约列表，发现新上市的合约并自动订阅"""
        if not self._connected or not self.main_engine:
            return

        try:
            current_contracts = self.main_engine.get_all_contracts()
        except Exception as e:
            logger.warning(f"[新合约发现] 查询合约失败: {e}")
            return

        current_vt_set = {c.vt_symbol for c in current_contracts}
        old_vt_set = {c.vt_symbol for c in self._last_all_contracts} if self._last_all_contracts else set()
        new_vt_symbols = current_vt_set - old_vt_set

        if not new_vt_symbols:
            return

        # 筛选新合约中属于已订阅品种的活跃月份期权
        new_contracts = [c for c in current_contracts if c.vt_symbol in new_vt_symbols]
        new_to_sub = []
        for c in new_contracts:
            if c.product != Product.OPTION:
                continue
            # 检查是否属于已订阅的品种范围
            prefix = extract_product_prefix(c.symbol)
            month = self._extract_month(c.symbol)
            # 通过已有订阅推断活跃月份
            for existing_vt in self._subscribed:
                existing_sym = existing_vt.split(".")[0]
                existing_prefix = extract_product_prefix(existing_sym)
                if existing_prefix == prefix:
                    existing_month = self._extract_month(existing_sym)
                    if existing_month == month:
                        new_to_sub.append(c)
                        break

        if new_to_sub:
            logger.info(f"[新合约发现] 发现 {len(new_to_sub)} 个新期权: "
                       f"{[c.symbol for c in new_to_sub[:10]]}")
            self._batch_subscribe(new_to_sub, label="新合约")
            notify("CTP数据采集", f"新增订阅 {len(new_to_sub)} 个新上市期权")

        self._last_all_contracts = list(current_contracts)

    def run(self, daemon_mode=False):
        if not self.connect():
            return

        self.subscribe_symbols()
        if not self._subscribed:
            logger.error("没有订阅任何合约，退出")
            self.shutdown()
            return

        self._running = True

        write_thread = threading.Thread(target=self._write_loop, daemon=True)
        write_thread.start()

        # 订阅健康监控线程：检查沉默合约 + 发现新合约
        health_thread = threading.Thread(target=self._subscription_health_loop, daemon=True)
        health_thread.start()

        logger.info("=" * 50)
        logger.info("数据采集已启动")
        logger.info(f"数据库: {self.db_writer.db_path}")
        logger.info(f"合约数: {len(self._subscribed)}")
        logger.info("订阅健康监控: 3分钟后首次检查，每15分钟复查")
        logger.info("交易时段结束后自动退出")
        logger.info("=" * 50)

        # daemon模式下不覆盖信号处理器，由run_daemon统一管理
        if not daemon_mode:
            signal.signal(signal.SIGINT, lambda s, f: self.shutdown())
            signal.signal(signal.SIGTERM, lambda s, f: self.shutdown())

        try:
            while self._running:
                time.sleep(10)
                if not is_trading_time():
                    logger.info("交易时段结束")
                    break
        except KeyboardInterrupt:
            pass

        self.shutdown()

    def shutdown(self):
        if not self._running and not self._connected:
            return
        self._running = False
        self._connected = False
        self._engine_close_failed = False
        logger.info("正在关闭...")

        try:
            remaining = self.aggregator.flush_all()
            self.db_writer.write_bars(remaining)
        except Exception as e:
            logger.error(f"flush数据失败: {e}")

        if self.main_engine:
            # main_engine.close() 调用CTP C++底层，可能段错误或死锁
            # 用子线程+超时保护，避免杀死daemon进程
            def _close_engine():
                try:
                    self.main_engine.close()
                except Exception as e:
                    logger.error(f"关闭引擎异常: {e}")
                    self._engine_close_failed = True
            t = threading.Thread(target=_close_engine, daemon=True)
            t.start()
            t.join(timeout=10)
            if t.is_alive():
                logger.warning("关闭引擎超时(10秒)，跳过")
                self._engine_close_failed = True
            self.main_engine = None

        total = self.db_writer.total_written
        logger.info(f"采集结束: {self._tick_count} ticks, {total} 条K线")
        notify("CTP数据采集", f"结束，{total}条K线")


# ============================================================================
# 入口
# ============================================================================

def run_daemon(args):
    """守护进程模式：自动管理交易时段，不依赖cron"""
    logger.info("=" * 50)
    logger.info("CTP数据采集 - 守护进程模式")
    logger.info("自动在交易时段连接采集，非交易时段休眠等待")
    logger.info("Ctrl+C 退出")
    logger.info("=" * 50)

    stop_flag = False

    def _handle_signal(sig, frame):
        nonlocal stop_flag
        stop_flag = True
        logger.info("收到退出信号")

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    while not stop_flag:
        if not is_trading_time():
            wait = next_session_wait_seconds()
            hours, remainder = divmod(wait, 3600)
            mins = remainder // 60
            next_time = datetime.now() + timedelta(seconds=wait)
            logger.info(f"非交易时段，等待 {int(hours)}小时{int(mins)}分钟 "
                       f"(预计 {next_time:%H:%M} 启动)")
            # 每30秒检查一次，以便及时响应退出信号
            for _ in range(wait // 30 + 1):
                if stop_flag:
                    break
                if is_trading_time():
                    break
                time.sleep(30)
            if stop_flag:
                break
            if not is_trading_time():
                continue

        logger.info("进入交易时段，启动采集...")
        notify("CTP数据采集", "交易时段开始，正在连接...")

        retry_count = 0
        max_retries = 3
        while is_trading_time() and not stop_flag and retry_count < max_retries:
            try:
                collector = CTPDataCollector(
                    config_path=args.config,
                    symbols=args.symbols,
                    products=args.product,
                    all_options=args.all_options,
                    full_option_products=getattr(args, 'full_options', None),
                )
                collector.run(daemon_mode=True)
            except Exception as e:
                logger.error(f"采集器异常: {e}")
                try:
                    collector.shutdown()
                except Exception:
                    pass

            if stop_flag or not is_trading_time():
                break
            # run()提前返回说明出了问题，等10秒后重试
            retry_count += 1
            logger.info(f"采集异常退出，{10}秒后重试 ({retry_count}/{max_retries})...")
            time.sleep(10)

        if stop_flag:
            break

        # CTP引擎关闭失败时，C++底层资源可能泄漏，新连接只能收到部分交易所行情
        # 此时重启整个进程是最可靠的清理方式
        engine_dirty = hasattr(collector, '_engine_close_failed') and collector._engine_close_failed
        if engine_dirty:
            logger.warning("CTP引擎关闭异常，重启进程以清理底层资源...")
            notify("CTP数据采集", "引擎异常，正在重启进程")
            os.execv(sys.executable, [sys.executable] + sys.argv)

        logger.info("本时段采集结束，等待下一时段...")

    logger.info("守护进程退出")


def main():
    parser = argparse.ArgumentParser(description="CTP实时数据采集器（独立运行）")
    parser.add_argument("symbols", nargs="*", help="合约代码")
    parser.add_argument("--product", "-p", nargs="*", help="品种代码（含期货+期权）")
    parser.add_argument("--all-options", "-a", action="store_true",
                        help="采集全市场期权（含对应期货）")
    parser.add_argument("--full-options", "-f", nargs="*",
                        help="指定品种订阅全部期权（不限档数），如: --full-options AG AU")
    parser.add_argument("--daemon", "-d", action="store_true",
                        help="守护进程模式：持续运行，自动管理交易时段")
    parser.add_argument("--config", "-c",
                        default=str(Path(__file__).parent / "ctp_config.json"),
                        help="CTP配置文件路径")
    args = parser.parse_args()

    if not args.symbols and not args.product and not args.all_options:
        parser.error("请指定模式，例如:\n"
                     "  全市场期权: python3 ctp_data_collector.py --all-options\n"
                     "  守护进程:   python3 ctp_data_collector.py --all-options --daemon\n"
                     "  指定品种:   python3 ctp_data_collector.py --product AG SA TA")

    if not VNPY_OK:
        sys.exit(1)

    if args.daemon:
        run_daemon(args)
    else:
        collector = CTPDataCollector(
            config_path=args.config,
            symbols=args.symbols,
            products=args.product,
            all_options=args.all_options,
            full_option_products=getattr(args, 'full_options', None),
        )
        collector.run()


if __name__ == "__main__":
    main()
