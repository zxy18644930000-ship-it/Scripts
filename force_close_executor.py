#!/usr/bin/env python3
"""
独立强平执行器 — 由工作台一键强平按钮调用

工作流程：
1. 从策略状态文件读取当前持仓
2. 连接CTP（独立连接，不影响交易系统）
3. 按分轮配置发送平仓订单（限价单，对手价+滑点）
4. 等待成交，超时则追价
5. 全部平完后写入结果文件并退出

用法：
  python3 force_close_executor.py          # 平所有有持仓的品种
  python3 force_close_executor.py CF       # 只平CF
"""

import os
import sys
import json
import time
import logging
from pathlib import Path
from datetime import datetime

# trade2026 路径
TRADE2026 = os.path.expanduser('~/Downloads/trade2026')
sys.path.insert(0, TRADE2026)

STATE_DIR = os.path.expanduser('~/state')
RESULT_FILE = os.path.join(STATE_DIR, '.force_close_result.json')
CTP_CONFIG = os.path.join(TRADE2026, 'config/ctp_config.json')

# 日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join(STATE_DIR, 'force_close.log'), encoding='utf-8'),
    ]
)
logger = logging.getLogger('ForceClose')


def load_positions(product_filter=None):
    """从策略状态文件读取持仓"""
    positions = []
    for fname in os.listdir(STATE_DIR):
        if not fname.endswith('_state.json'):
            continue
        fpath = os.path.join(STATE_DIR, fname)
        if time.time() - os.path.getmtime(fpath) > 300:
            continue
        try:
            with open(fpath, 'r') as f:
                state = json.load(f)
            product = state.get('product_code', '')
            if product_filter and product.upper() != product_filter.upper():
                continue
            for p in state.get('positions', []):
                if p.get('volume', 0) > 0:
                    positions.append({
                        'symbol': p['symbol'],
                        'exchange': p['exchange'],
                        'direction': p['direction'],
                        'volume': p['volume'],
                        'product': product,
                    })
        except Exception as e:
            logger.warning(f"读取 {fname} 失败: {e}")
    return positions


def get_round_volumes(product, total_volume):
    """获取分轮手数"""
    try:
        from infra.config.commodity_config import COMMODITY_CONFIGS
        cfg = COMMODITY_CONFIGS.get(product.upper(), {})
        rounds = cfg.get('trading_params', {}).get('exit_round_volumes', [])
        if rounds and sum(rounds) >= total_volume:
            return rounds
    except Exception:
        pass
    # 默认：每轮5手
    rounds = []
    remaining = total_volume
    while remaining > 0:
        lot = min(5, remaining)
        rounds.append(lot)
        remaining -= lot
    return rounds


def write_result(status, message, details=None):
    """写入执行结果（供工作台读取）"""
    result = {
        'timestamp': datetime.now().isoformat(),
        'status': status,
        'message': message,
        'details': details or {},
    }
    try:
        with open(RESULT_FILE, 'w') as f:
            json.dump(result, f, ensure_ascii=False)
    except Exception:
        pass


def execute_force_close(positions):
    """执行强平"""
    from vnpy.event import EventEngine
    from vnpy.trader.engine import MainEngine
    from vnpy.trader.constant import (
        Exchange as VnExchange,
        Direction as VnDirection,
        Offset as VnOffset,
        OrderType as VnOrderType,
    )
    from vnpy.trader.object import OrderRequest, SubscribeRequest

    # 交易所映射
    exchange_map = {
        'CZCE': VnExchange.CZCE, 'DCE': VnExchange.DCE,
        'SHFE': VnExchange.SHFE, 'INE': VnExchange.INE,
        'GFEX': VnExchange.GFEX, 'CFFEX': VnExchange.CFFEX,
    }

    # 读取CTP配置
    with open(CTP_CONFIG, 'r') as f:
        ctp_config = json.load(f)

    ctp_setting = {
        "用户名": ctp_config["userid"],
        "密码": ctp_config["password"],
        "经纪商代码": ctp_config["brokerid"],
        "交易服务器": ctp_config["td_address"],
        "行情服务器": ctp_config["md_address"],
        "产品名称": ctp_config.get("appid", ""),
        "授权编码": ctp_config.get("auth_code", ""),
        "产品信息": ctp_config.get("product_info", ""),
    }

    logger.info("=" * 60)
    logger.info("强制平仓执行器启动")
    logger.info(f"待平仓 {len(positions)} 个持仓:")
    for p in positions:
        logger.info(f"  {p['symbol']} {p['direction']} {p['volume']}手")
    logger.info("=" * 60)

    write_result('connecting', '正在连接CTP...')

    # 连接CTP
    event_engine = EventEngine()
    main_engine = MainEngine(event_engine)
    gateway_name = "CTP"

    try:
        from vnpy_ctp import CtpGateway
    except ImportError:
        from vnpy.gateway.ctp import CtpGateway
    main_engine.add_gateway(CtpGateway)
    main_engine.connect(ctp_setting, gateway_name)

    # 等待连接和合约就绪
    logger.info("等待CTP连接...")
    for i in range(30):
        time.sleep(1)
        if main_engine.get_all_contracts():
            logger.info(f"CTP连接成功，已加载 {len(main_engine.get_all_contracts())} 个合约")
            break
    else:
        write_result('error', 'CTP连接超时')
        logger.error("CTP连接超时")
        main_engine.close()
        return False

    write_result('executing', '开始执行平仓...')

    # 订阅行情
    for p in positions:
        vn_exchange = exchange_map.get(p['exchange'])
        if vn_exchange:
            req = SubscribeRequest(symbol=p['symbol'], exchange=vn_exchange)
            main_engine.subscribe(req, gateway_name)

    time.sleep(2)  # 等待行情到达

    # 逐个持仓执行平仓
    total_filled = 0
    total_target = sum(p['volume'] for p in positions)
    results = []

    for pos in positions:
        symbol = pos['symbol']
        exchange_str = pos['exchange']
        direction = pos['direction']
        volume = pos['volume']
        product = pos['product']

        vn_exchange = exchange_map.get(exchange_str)
        if not vn_exchange:
            logger.error(f"未知交易所: {exchange_str}")
            continue

        vt_symbol = f"{symbol}.{exchange_str}"

        # 平空头 → 买入平仓
        if 'SHORT' in direction.upper():
            close_direction = VnDirection.LONG
        else:
            close_direction = VnDirection.SHORT

        # 郑商所期权用 CLOSE，上期所/能源用 CLOSETODAY
        if exchange_str in ('SHFE', 'INE'):
            offset = VnOffset.CLOSETODAY
        else:
            offset = VnOffset.CLOSE

        # 分轮平仓
        rounds = get_round_volumes(product, volume)
        logger.info(f"[{symbol}] 分{len(rounds)}轮平仓: {rounds}")

        filled_total = 0
        for round_idx, round_vol in enumerate(rounds):
            if filled_total >= volume:
                break
            actual_vol = min(round_vol, volume - filled_total)

            # 获取最新行情确定价格
            tick = main_engine.get_tick(vt_symbol)
            if tick:
                if close_direction == VnDirection.LONG:
                    # 买入平仓：用卖一价 + 滑点
                    price_tick = 1  # 默认滑点1个tick
                    price = tick.ask_price_1 + price_tick * 2 if tick.ask_price_1 else tick.last_price * 1.005
                else:
                    price = tick.bid_price_1 - price_tick * 2 if tick.bid_price_1 else tick.last_price * 0.995
            else:
                logger.warning(f"[{symbol}] 无行情，使用对手价估算")
                price = 0  # 会用市价单

            # 价格取整（根据品种价格精度）
            price = round(price, 2)

            logger.info(f"[{symbol}] 第{round_idx+1}轮: 平{actual_vol}手 @ {price:.2f}")

            req = OrderRequest(
                symbol=symbol,
                exchange=vn_exchange,
                direction=close_direction,
                offset=offset,
                type=VnOrderType.LIMIT,
                volume=actual_vol,
                price=price,
            )
            vt_orderid = main_engine.send_order(req, gateway_name)

            if not vt_orderid:
                logger.error(f"[{symbol}] 第{round_idx+1}轮订单提交失败")
                continue

            logger.info(f"[{symbol}] 订单已提交: {vt_orderid}")

            # 等待成交（最多30秒）
            for wait in range(30):
                time.sleep(1)
                order = main_engine.get_order(vt_orderid)
                if order:
                    from vnpy.trader.constant import Status
                    if order.status == Status.ALLTRADED:
                        logger.info(f"[{symbol}] 第{round_idx+1}轮全部成交: {actual_vol}手")
                        filled_total += actual_vol
                        break
                    elif order.status in (Status.CANCELLED, Status.REJECTED):
                        logger.warning(f"[{symbol}] 订单被撤销/拒绝: {order.status}")
                        break

            # 轮次间等待
            if round_idx < len(rounds) - 1:
                time.sleep(2)

        total_filled += filled_total
        results.append({
            'symbol': symbol,
            'target': volume,
            'filled': filled_total,
            'success': filled_total >= volume,
        })
        logger.info(f"[{symbol}] 平仓完成: {filled_total}/{volume}手")

    # 断开CTP
    time.sleep(2)
    main_engine.close()

    # 写入结果
    success = total_filled >= total_target
    status = 'success' if success else 'partial'
    message = f'已平{total_filled}/{total_target}手'
    write_result(status, message, {'results': results})
    logger.info(f"强平执行完毕: {message}")
    logger.info("=" * 60)
    return success


def main():
    product_filter = sys.argv[1] if len(sys.argv) > 1 else None

    positions = load_positions(product_filter)
    if not positions:
        logger.info("无持仓需要平仓")
        write_result('no_position', '无持仓')
        return

    try:
        execute_force_close(positions)
    except Exception as e:
        logger.error(f"强平执行异常: {e}", exc_info=True)
        write_result('error', f'执行异常: {e}')


if __name__ == '__main__':
    main()
