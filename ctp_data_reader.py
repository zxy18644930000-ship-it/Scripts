# -*- coding: utf-8 -*-
"""
CTP数据读取模块 (Mac兼容版)
用于读取数据收集器收集的K线数据

功能：
1. 读取1分钟和5分钟K线数据
2. 支持按合约、时间范围查询
3. 提供期货K线查询（用于趋势判断）
4. 提供期权快照查询（用于VIX计算）
5. 支持Parquet历史数据读取
6. 支持vnpy数据库直接读取（Mac兼容）

Version: v2.0 (Mac兼容版)
Author: ChatGPT for 张孝禹
Date: 2025-12-11
Updated: 2026-02-24 (Mac适配)

使用示例:
    from ctp_data_reader import CTPDataReader

    # 自动检测数据源（优先vnpy）
    reader = CTPDataReader()

    # 强制使用vnpy数据源
    reader = CTPDataReader(data_source="vnpy")

    # 强制使用CTP数据源（需指定数据目录）
    reader = CTPDataReader(data_source="ctp", data_dir="~/CTPData")

    # 读取期货K线（用于趋势判断）
    df = reader.get_futures_klines("p2501", period="5min", bars=60)

    # 读取期权快照（用于VIX计算）
    df = reader.get_options_snapshot("p2501")

    # 读取特定合约K线
    df = reader.get_klines("ag2502", period="1min", start_time="2025-12-11 21:00:00")
"""

import sqlite3
import platform
import pandas as pd
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import Optional, List, Dict, Union


# ============================================================================
# 内置vnpy数据适配器（替代外部 vnpy_data_adapter 模块）
# ============================================================================

class _VnpyDataAdapter:
    """
    vnpy数据库适配器（内置版）
    直接读取vnpy的SQLite数据库（dbbardata表）
    """

    def __init__(self, db_path: Optional[str] = None):
        if db_path is None:
            # 自动查找vnpy数据库
            db_path = self._find_vnpy_db()
        self.db_path = Path(db_path) if db_path else None
        self._conn: Optional[sqlite3.Connection] = None

    @staticmethod
    def _find_vnpy_db() -> Optional[str]:
        """自动查找vnpy数据库文件"""
        home = Path.home()
        candidates = [
            home / ".vntrader" / "database.db",
            home / ".vntrader" / "vnpy_database.db",
            home / "vnpy_database.db",
        ]
        # 也搜索trade2026项目
        for p in (home / "Downloads" / "trade2026").rglob("*.db"):
            if "database" in p.name.lower() or "vnpy" in p.name.lower():
                candidates.append(p)

        for path in candidates:
            if path.exists() and path.stat().st_size > 0:
                return str(path)
        return None

    def _get_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            if self.db_path is None or not Path(self.db_path).exists():
                raise FileNotFoundError(
                    f"vnpy数据库未找到。请指定路径: CTPDataReader(vnpy_db_path='...')\n"
                    f"尝试过的路径: ~/.vntrader/database.db"
                )
            self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        return self._conn

    def get_bars(
        self,
        symbol: str,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        limit: Optional[int] = None,
        interval: str = "1min"
    ) -> pd.DataFrame:
        """从vnpy dbbardata表读取K线"""
        conn = self._get_conn()

        # vnpy的interval字段: "1m" 对应1分钟
        interval_map = {"1min": "1m", "5min": "5m", "1m": "1m", "5m": "5m"}
        db_interval = interval_map.get(interval, "1m")

        query = """
            SELECT datetime, open_price as open, high_price as high,
                   low_price as low, close_price as close,
                   volume, turnover, open_interest
            FROM dbbardata
            WHERE symbol = ? AND interval = ?
        """
        params: list = [symbol, db_interval]

        if start_time:
            query += " AND datetime >= ?"
            params.append(start_time)
        if end_time:
            query += " AND datetime <= ?"
            params.append(end_time)

        query += " ORDER BY datetime"

        if limit:
            # 取最新的N条：先倒序取limit条，再正序
            query_desc = query.replace("ORDER BY datetime", "ORDER BY datetime DESC")
            query_desc += f" LIMIT {limit}"
            df = pd.read_sql(query_desc, conn, params=params)
            if not df.empty:
                df = df.iloc[::-1].reset_index(drop=True)
            return df

        df = pd.read_sql(query, conn, params=params)
        return df

    def get_futures_bars(
        self,
        symbol: str,
        period: str = "5min",
        bars: int = 60
    ) -> pd.DataFrame:
        """获取期货K线"""
        if period == "5min":
            # 先尝试直接读5分钟数据
            df = self.get_bars(symbol, interval="5m", limit=bars)
            if not df.empty:
                return df
            # 没有5分钟数据，从1分钟聚合
            df_1m = self.get_bars(symbol, interval="1m", limit=bars * 5)
            if df_1m.empty:
                return df_1m
            return self._aggregate_to_5min(df_1m).tail(bars).reset_index(drop=True)
        else:
            return self.get_bars(symbol, interval=period, limit=bars)

    def get_options_snapshot(
        self,
        underlying: str,
        snapshot_time: Optional[str] = None
    ) -> pd.DataFrame:
        """获取期权快照"""
        conn = self._get_conn()

        # 期权合约名通常包含标的代码，如 p2501-C-7800
        like_pattern = f"{underlying}%"

        if snapshot_time is None:
            # 取最新时间
            cursor = conn.cursor()
            cursor.execute(
                "SELECT MAX(datetime) FROM dbbardata WHERE symbol LIKE ?",
                [like_pattern]
            )
            result = cursor.fetchone()
            if not result or not result[0]:
                return pd.DataFrame()
            snapshot_time = result[0]

        query = """
            SELECT symbol, datetime,
                   open_price as open, high_price as high,
                   low_price as low, close_price as close,
                   volume, open_interest
            FROM dbbardata
            WHERE symbol LIKE ? AND datetime = ?
            ORDER BY symbol
        """
        df = pd.read_sql(query, conn, params=[like_pattern, snapshot_time])

        # 解析期权信息
        if not df.empty:
            df['option_type'] = df['symbol'].apply(self._parse_option_type)
            df['strike'] = df['symbol'].apply(self._parse_strike)
            df['expiry'] = ''

        return df

    @staticmethod
    def _parse_option_type(symbol: str) -> str:
        """从合约代码解析期权类型"""
        symbol_upper = symbol.upper()
        if '-C-' in symbol_upper or 'C' in symbol_upper.split('-'):
            return 'C'
        elif '-P-' in symbol_upper or 'P' in symbol_upper.split('-'):
            return 'P'
        return ''

    @staticmethod
    def _parse_strike(symbol: str) -> float:
        """从合约代码解析行权价"""
        parts = symbol.split('-')
        if len(parts) >= 3:
            try:
                return float(parts[-1])
            except ValueError:
                pass
        return 0.0

    @staticmethod
    def _aggregate_to_5min(df_1m: pd.DataFrame) -> pd.DataFrame:
        """将1分钟K线聚合为5分钟"""
        if df_1m.empty:
            return df_1m

        df = df_1m.copy()
        df['datetime'] = pd.to_datetime(df['datetime'])
        df = df.set_index('datetime')

        agg = df.resample('5min', label='right', closed='right').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum',
        })

        if 'open_interest' in df.columns:
            agg['open_interest'] = df['open_interest'].resample('5min', label='right', closed='right').last()

        agg = agg.dropna(subset=['open']).reset_index()
        return agg

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None


# ============================================================================
# 数据源自动检测（替代外部 vnpy_recorder_config 模块）
# ============================================================================

def _detect_data_source() -> str:
    """自动检测应该使用哪个数据源"""
    system = platform.system()

    if system == "Darwin":
        # Mac系统：优先vnpy
        return "vnpy"

    # Windows：检查CTP数据目录是否存在
    ctp_dir = Path(r"D:\CTPData")
    if ctp_dir.exists():
        return "ctp"

    # 默认vnpy
    return "vnpy"


# ============================================================================
# 主类
# ============================================================================

class CTPDataReader:
    """
    CTP数据读取器

    提供从SQLite数据库和Parquet文件读取K线数据的接口
    支持两种数据源：
    - "ctp": 自定义CTP数据收集器数据库
    - "vnpy": vnpy DataRecorder数据库（Mac兼容）
    """

    def __init__(
        self,
        data_dir: Optional[str] = None,
        data_source: Optional[str] = None,
        vnpy_db_path: Optional[str] = None
    ):
        """
        初始化数据读取器

        Args:
            data_dir: CTP数据存储根目录（仅用于ctp数据源）
            data_source: 数据源类型 "ctp" 或 "vnpy"，默认自动检测
            vnpy_db_path: vnpy数据库路径（仅用于vnpy数据源）
        """
        # 自动检测数据源
        if data_source is None:
            data_source = _detect_data_source()

        self.data_source = data_source

        if self.data_source == "vnpy":
            self.vnpy_adapter = _VnpyDataAdapter(db_path=vnpy_db_path)
            self.data_dir = None
            self.realtime_dir = None
            self.archive_dir = None
            self._conn = None
            self._current_month = None
        else:
            # CTP数据源：自动适配路径
            if data_dir is None:
                system = platform.system()
                if system == "Darwin":
                    data_dir = str(Path.home() / "CTPData")
                else:
                    data_dir = r"D:\CTPData"

            self.data_dir = Path(data_dir).expanduser()
            self.realtime_dir = self.data_dir / "realtime"
            self.archive_dir = self.data_dir / "archive"
            self.vnpy_adapter = None

            # 缓存当前连接
            self._conn: Optional[sqlite3.Connection] = None
            self._current_month: Optional[str] = None

    # ========================================================================
    # 数据库连接管理
    # ========================================================================

    def _get_db_path(self, month: str) -> Path:
        """获取指定月份的数据库路径"""
        return self.realtime_dir / f"kline_{month}.db"

    def _get_connection(self, month: Optional[str] = None) -> sqlite3.Connection:
        """获取数据库连接（带缓存）"""
        if self.data_source == "vnpy":
            raise RuntimeError("vnpy数据源不应调用_get_connection方法")

        if month is None:
            month = datetime.now().strftime("%Y%m")

        if self._conn and self._current_month == month:
            return self._conn

        if self._conn:
            self._conn.close()

        db_path = self._get_db_path(month)
        if not db_path.exists():
            raise FileNotFoundError(f"数据库不存在: {db_path}")

        self._conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self._current_month = month
        return self._conn

    def close(self):
        """关闭数据库连接"""
        if self.data_source == "vnpy":
            if self.vnpy_adapter:
                self.vnpy_adapter.close()
        else:
            if self._conn:
                self._conn.close()
                self._conn = None
                self._current_month = None

    # ========================================================================
    # 基础查询方法
    # ========================================================================

    def get_klines(
        self,
        symbol: str,
        period: str = "1min",
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        limit: Optional[int] = None,
        month: Optional[str] = None
    ) -> pd.DataFrame:
        """获取指定合约的K线数据"""
        if self.data_source == "vnpy":
            if period == "5min":
                limit_1min = limit * 5 if limit else None
                df_1min = self.vnpy_adapter.get_bars(
                    symbol, start_time, end_time, limit_1min, interval="1min"
                )
                if df_1min.empty:
                    return df_1min
                df = self.vnpy_adapter._aggregate_to_5min(df_1min)
                if limit:
                    df = df.tail(limit).reset_index(drop=True)
                return df
            else:
                return self.vnpy_adapter.get_bars(
                    symbol, start_time, end_time, limit, interval=period
                )
        else:
            conn = self._get_connection(month)
            table = f"kline_{period}"

            query = f"SELECT * FROM {table} WHERE symbol = ?"
            params: List = [symbol]

            if start_time:
                query += " AND datetime >= ?"
                params.append(start_time)
            if end_time:
                query += " AND datetime <= ?"
                params.append(end_time)

            query += " ORDER BY datetime"

            if limit:
                query += f" DESC LIMIT {limit}"

            df = pd.read_sql(query, conn, params=params)

            if limit:
                df = df.iloc[::-1].reset_index(drop=True)

            return df

    def get_latest_klines(
        self,
        symbol: str,
        period: str = "1min",
        bars: int = 100,
        month: Optional[str] = None
    ) -> pd.DataFrame:
        """获取最近N根K线"""
        return self.get_klines(symbol, period, limit=bars, month=month)

    # ========================================================================
    # 期货K线查询（用于趋势判断）
    # ========================================================================

    def get_futures_klines(
        self,
        symbol: str,
        period: str = "5min",
        bars: int = 60,
        month: Optional[str] = None
    ) -> pd.DataFrame:
        """获取期货K线数据（用于趋势判断）"""
        if self.data_source == "vnpy":
            df = self.vnpy_adapter.get_futures_bars(symbol, period, bars)
            if df.empty:
                return df
            result_columns = ['datetime', 'open', 'high', 'low', 'close', 'volume', 'open_interest']
            available_columns = [col for col in result_columns if col in df.columns]
            return df[available_columns]
        else:
            conn = self._get_connection(month)
            table = f"kline_{period}"

            query = f"""
                SELECT datetime, open, high, low, close, volume, open_interest
                FROM {table}
                WHERE symbol = ? AND product_type = 'futures'
                ORDER BY datetime DESC
                LIMIT {bars}
            """

            df = pd.read_sql(query, conn, params=[symbol])

            if df.empty:
                return df

            df = df.iloc[::-1].reset_index(drop=True)
            df['datetime'] = pd.to_datetime(df['datetime'])

            return df

    def get_multi_period_klines(
        self,
        symbol: str,
        short_bars: int = 10,
        medium_bars: int = 30,
        long_bars: int = 60,
        period: str = "5min",
        month: Optional[str] = None
    ) -> Dict[str, pd.DataFrame]:
        """获取多周期K线数据"""
        df_full = self.get_futures_klines(symbol, period, long_bars, month)

        if df_full.empty:
            return {"short": pd.DataFrame(), "medium": pd.DataFrame(), "long": pd.DataFrame()}

        return {
            "short": df_full.tail(short_bars).reset_index(drop=True),
            "medium": df_full.tail(medium_bars).reset_index(drop=True),
            "long": df_full,
        }

    # ========================================================================
    # 期权数据查询（用于VIX计算）
    # ========================================================================

    def get_options_snapshot(
        self,
        underlying: str,
        snapshot_time: Optional[str] = None,
        month: Optional[str] = None
    ) -> pd.DataFrame:
        """获取指定标的的所有期权最新快照"""
        if self.data_source == "vnpy":
            df = self.vnpy_adapter.get_options_snapshot(underlying, snapshot_time)
            if df.empty:
                return df
            result_columns = [
                'symbol', 'option_type', 'strike', 'expiry',
                'open', 'high', 'low', 'close',
                'volume', 'open_interest'
            ]
            available_columns = [col for col in result_columns if col in df.columns]
            return df[available_columns]
        else:
            conn = self._get_connection(month)

            if snapshot_time is None:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT MAX(datetime) FROM kline_1min WHERE underlying = ?",
                    [underlying]
                )
                result = cursor.fetchone()
                if not result or not result[0]:
                    return pd.DataFrame()
                snapshot_time = result[0]

            query = """
                SELECT symbol, option_type, strike, expiry,
                       open, high, low, close,
                       bid_price, ask_price, bid_volume, ask_volume,
                       volume, open_interest
                FROM kline_1min
                WHERE underlying = ? AND datetime = ? AND product_type = 'option'
                ORDER BY option_type, strike
            """

            df = pd.read_sql(query, conn, params=[underlying, snapshot_time])
            return df

    def get_option_chain(
        self,
        underlying: str,
        expiry: Optional[str] = None,
        snapshot_time: Optional[str] = None,
        month: Optional[str] = None
    ) -> Dict[str, pd.DataFrame]:
        """获取期权链数据"""
        df = self.get_options_snapshot(underlying, snapshot_time, month)

        if df.empty:
            return {"calls": pd.DataFrame(), "puts": pd.DataFrame()}

        if expiry:
            df = df[df['expiry'] == expiry]

        calls = df[df['option_type'] == 'C'].reset_index(drop=True)
        puts = df[df['option_type'] == 'P'].reset_index(drop=True)

        return {"calls": calls, "puts": puts}

    # ========================================================================
    # 统计和元数据查询
    # ========================================================================

    def get_available_symbols(
        self,
        product_type: Optional[str] = None,
        month: Optional[str] = None
    ) -> List[str]:
        """获取所有可用的合约代码"""
        if self.data_source == "vnpy":
            conn = self.vnpy_adapter._get_conn()
            query = "SELECT DISTINCT symbol FROM dbbardata ORDER BY symbol"
            cursor = conn.cursor()
            cursor.execute(query)
            return [row[0] for row in cursor.fetchall()]
        else:
            conn = self._get_connection(month)
            query = "SELECT DISTINCT symbol FROM kline_1min"
            params = []
            if product_type:
                query += " WHERE product_type = ?"
                params.append(product_type)
            query += " ORDER BY symbol"
            cursor = conn.cursor()
            cursor.execute(query, params)
            return [row[0] for row in cursor.fetchall()]

    def get_available_underlyings(self, month: Optional[str] = None) -> List[str]:
        """获取所有有期权数据的标的"""
        if self.data_source == "vnpy":
            # vnpy模式：从合约名解析标的
            symbols = self.get_available_symbols()
            underlyings = set()
            for s in symbols:
                if '-' in s:
                    underlyings.add(s.split('-')[0])
            return sorted(underlyings)
        else:
            conn = self._get_connection(month)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT DISTINCT underlying FROM kline_1min
                WHERE underlying IS NOT NULL
                ORDER BY underlying
            """)
            return [row[0] for row in cursor.fetchall()]

    def get_data_range(
        self,
        symbol: Optional[str] = None,
        month: Optional[str] = None
    ) -> Dict[str, str]:
        """获取数据的时间范围"""
        if self.data_source == "vnpy":
            conn = self.vnpy_adapter._get_conn()
            if symbol:
                query = "SELECT MIN(datetime), MAX(datetime) FROM dbbardata WHERE symbol = ?"
                params = [symbol]
            else:
                query = "SELECT MIN(datetime), MAX(datetime) FROM dbbardata"
                params = []
            cursor = conn.cursor()
            cursor.execute(query, params)
            result = cursor.fetchone()
        else:
            conn = self._get_connection(month)
            if symbol:
                query = "SELECT MIN(datetime), MAX(datetime) FROM kline_1min WHERE symbol = ?"
                params = [symbol]
            else:
                query = "SELECT MIN(datetime), MAX(datetime) FROM kline_1min"
                params = []
            cursor = conn.cursor()
            cursor.execute(query, params)
            result = cursor.fetchone()

        return {
            "start": result[0] if result else None,
            "end": result[1] if result else None
        }

    def get_statistics(self, month: Optional[str] = None) -> Dict:
        """获取数据库统计信息"""
        if self.data_source == "vnpy":
            conn = self.vnpy_adapter._get_conn()
            cursor = conn.cursor()

            cursor.execute("SELECT COUNT(*) FROM dbbardata WHERE interval = '1m'")
            count_1min = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM dbbardata WHERE interval = '5m'")
            count_5min = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(DISTINCT symbol) FROM dbbardata")
            symbol_count = cursor.fetchone()[0]

            data_range = self.get_data_range()

            return {
                "data_source": "vnpy",
                "db_path": str(self.vnpy_adapter.db_path),
                "kline_1min_count": count_1min,
                "kline_5min_count": count_5min,
                "symbol_count": symbol_count,
                "data_start": data_range["start"],
                "data_end": data_range["end"],
            }
        else:
            conn = self._get_connection(month)
            cursor = conn.cursor()

            cursor.execute("SELECT COUNT(*) FROM kline_1min")
            count_1min = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM kline_5min")
            count_5min = cursor.fetchone()[0]

            cursor.execute("""
                SELECT product_type, COUNT(*) FROM kline_1min
                GROUP BY product_type
            """)
            product_dist = dict(cursor.fetchall())

            cursor.execute("SELECT COUNT(DISTINCT symbol) FROM kline_1min")
            symbol_count = cursor.fetchone()[0]

            data_range = self.get_data_range(month=month)

            return {
                "data_source": "ctp",
                "month": month or datetime.now().strftime("%Y%m"),
                "kline_1min_count": count_1min,
                "kline_5min_count": count_5min,
                "futures_count": product_dist.get("futures", 0),
                "option_count": product_dist.get("option", 0),
                "symbol_count": symbol_count,
                "data_start": data_range["start"],
                "data_end": data_range["end"],
            }

    # ========================================================================
    # Parquet历史数据读取
    # ========================================================================

    def read_parquet(
        self,
        date_str: str,
        period: str = "1min"
    ) -> pd.DataFrame:
        """读取Parquet归档数据"""
        if self.archive_dir is None:
            raise RuntimeError("Parquet读取仅支持CTP数据源")

        month = date_str[:6]
        parquet_path = self.archive_dir / month / f"{date_str}_{period}.parquet"

        if not parquet_path.exists():
            raise FileNotFoundError(f"Parquet文件不存在: {parquet_path}")

        return pd.read_parquet(parquet_path)

    def read_parquet_range(
        self,
        start_date: str,
        end_date: str,
        period: str = "1min",
        symbol: Optional[str] = None
    ) -> pd.DataFrame:
        """读取日期范围内的Parquet数据"""
        start = datetime.strptime(start_date, "%Y%m%d")
        end = datetime.strptime(end_date, "%Y%m%d")

        dfs = []
        current = start

        while current <= end:
            date_str = current.strftime("%Y%m%d")
            try:
                df = self.read_parquet(date_str, period)
                if symbol:
                    df = df[df['symbol'] == symbol]
                dfs.append(df)
            except FileNotFoundError:
                pass
            current += timedelta(days=1)

        if not dfs:
            return pd.DataFrame()

        return pd.concat(dfs, ignore_index=True)


# ============================================================================
# 便捷函数（直接导入使用）
# ============================================================================

_default_reader: Optional[CTPDataReader] = None


def get_reader() -> CTPDataReader:
    """获取默认的数据读取器实例"""
    global _default_reader
    if _default_reader is None:
        _default_reader = CTPDataReader()
    return _default_reader


def get_futures_klines(
    symbol: str,
    period: str = "5min",
    bars: int = 60
) -> pd.DataFrame:
    """快捷函数：获取期货K线"""
    return get_reader().get_futures_klines(symbol, period, bars)


def get_options_snapshot(underlying: str) -> pd.DataFrame:
    """快捷函数：获取期权快照"""
    return get_reader().get_options_snapshot(underlying)


def get_klines(
    symbol: str,
    period: str = "1min",
    bars: int = 100
) -> pd.DataFrame:
    """快捷函数：获取K线数据"""
    return get_reader().get_latest_klines(symbol, period, bars)


# ============================================================================
# 主程序（测试用）
# ============================================================================

if __name__ == "__main__":
    import sys

    print("=" * 60)
    print("  CTP数据读取模块测试 (Mac兼容版 v2.0)")
    print(f"  系统: {platform.system()} {platform.machine()}")
    print("=" * 60)

    try:
        reader = CTPDataReader()
        print(f"\n数据源: {reader.data_source}")
        if reader.data_source == "vnpy" and reader.vnpy_adapter.db_path:
            print(f"数据库: {reader.vnpy_adapter.db_path}")

        # 1. 获取统计信息
        print("\n【数据库统计】")
        stats = reader.get_statistics()
        for key, value in stats.items():
            print(f"  {key}: {value}")

        # 2. 获取可用合约
        print("\n【可用合约】")
        symbols = reader.get_available_symbols()
        print(f"  共{len(symbols)}个: {symbols[:10]}{'...' if len(symbols) > 10 else ''}")

        # 3. 读取期货K线示例
        if symbols:
            # 找一个期货合约（不含-的）
            futures = [s for s in symbols if '-' not in s]
            if futures:
                symbol = futures[0]
                print(f"\n【期货K线示例: {symbol}】")
                df = reader.get_futures_klines(symbol, period="1min", bars=5)
                if not df.empty:
                    print(df.to_string(index=False))
                else:
                    print("  无数据")

        # 4. 数据时间范围
        print("\n【数据时间范围】")
        data_range = reader.get_data_range()
        print(f"  开始: {data_range['start']}")
        print(f"  结束: {data_range['end']}")

        reader.close()
        print("\n测试完成！")

    except FileNotFoundError as e:
        print(f"\n[错误] {e}")
        print("\n提示: 如果你的vnpy数据库在其他位置，请用:")
        print("  reader = CTPDataReader(vnpy_db_path='/path/to/database.db')")
    except Exception as e:
        print(f"\n[错误] {e}")
        import traceback
        traceback.print_exc()
