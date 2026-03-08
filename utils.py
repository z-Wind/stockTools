"""
utils.py — StockTools 共用工具函式
===================================
4.12：將各模組中重複的通用函式集中至此，避免重複定義。

目前包含：
  - gen_iter_date_by_month：逐月產生日期序列
"""

from datetime import datetime
from typing import Generator

from dateutil.relativedelta import relativedelta


def gen_iter_date_by_month(
    start: datetime,
    end: datetime,
) -> Generator[datetime, None, None]:
    """逐月產生日期序列（含首尾）。

    原本只在 get_extraData.py 定義，4.12 抽取至共用模組。
    get_extraData.py 保留向後相容的 import：
        from utils import gen_iter_date_by_month

    Args:
        start: 起始月份日期（通常為月初）。
        end:   結束月份日期（通常為月初）。

    Yields:
        每個月的日期，從 start 到 end（含）。

    Example:
        >>> from datetime import datetime
        >>> dates = list(gen_iter_date_by_month(
        ...     datetime(2024, 1, 1), datetime(2024, 3, 1)
        ... ))
        >>> [d.strftime('%Y-%m') for d in dates]
        ['2024-01', '2024-02', '2024-03']
    """
    current = start
    while current <= end:
        yield current
        current = current + relativedelta(months=1)
