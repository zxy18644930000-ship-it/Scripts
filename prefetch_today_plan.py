#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""今日计划开盘前预计算：写入 ~/state/today_plan_cache_day.json 或 _night.json

用法:
  python3 prefetch_today_plan.py --session day    # 日盘语境（约 08:45 launchd）
  python3 prefetch_today_plan.py --session night # 夜盘语境（约 20:45 launchd）

依赖同目录 price_sum_workbench.py（import 会加载 Dash，略慢属正常）。
"""
from __future__ import annotations

import argparse
import os
import sys


def main() -> int:
    ap = argparse.ArgumentParser(description='预计算今日计划并写入缓存')
    ap.add_argument(
        '--session',
        choices=('day', 'night'),
        required=True,
        help='day=日盘过滤, night=夜盘过滤',
    )
    args = ap.parse_args()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    if script_dir not in sys.path:
        sys.path.insert(0, script_dir)

    import price_sum_workbench as wb  # noqa: E402

    force_night = args.session == 'night'
    payload = wb.compute_today_plan_payload(force_night_session=force_night)
    wb._save_today_plan_cache(args.session, payload)
    n = len(payload.get('top_picks') or [])
    print(
        f"[prefetch_today_plan] session={args.session} "
        f"computed_at={payload.get('computed_at')} top_picks={n}",
        flush=True,
    )
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
