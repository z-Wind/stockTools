#!/usr/bin/env python3

import sys
import ctypes
from ctypes import c_size_t, c_double, Structure, c_int32, POINTER, c_char_p, c_uint64
import os
import pandas as pd
import pandas.api.types as ptypes

path = os.path.join(os.path.dirname(__file__), "rust_lib/target/release")
prefix = {"win32": ""}.get(sys.platform, "lib")
extension = {"darwin": ".dylib", "win32": ".dll"}.get(sys.platform, ".so")
lib = ctypes.cdll.LoadLibrary(os.path.join(path, prefix + "rust_lib" + extension))


class Stat(Structure):
    _fields_ = [
        ("year", c_int32),
        ("count", c_size_t),
        ("mean", c_double),
        ("std", c_double),
        ("min", c_double),
        ("q1", c_double),
        ("q2", c_double),
        ("q3", c_double),
        ("max", c_double),
    ]

    def __repr__(self):
        return (
            "(Stat: "
            f"{self.year}, "
            f"{self.count}, "
            f"{self.mean}, "
            f"{self.std}, "
            f"{self.min}, "
            f"{self.q1}, "
            f"{self.q2}, "
            f"{self.q3}, "
            f"{self.max})"
        )

    def __str__(self):
        return f"""
year:\t{self.year}
count:\t{self.count}
mean:\t{self.mean}
std:\t{self.std}
min:\t{self.min}
q1:\t{self.q1}
q2:\t{self.q2}
q3:\t{self.q3}
max:\t{self.max}
"""


class Price(Structure):
    _fields_ = [
        ("date", c_char_p),
        ("open", c_double),
        ("high", c_double),
        ("low", c_double),
        ("close", c_double),
        ("close_adj", c_double),
        ("volume", c_uint64),
    ]

    def __repr__(self):
        return (
            "(Price: "
            f"{self.date}, "
            f"{self.open}, "
            f"{self.high}, "
            f"{self.low}, "
            f"{self.close}, "
            f"{self.close_adj}, "
            f"{self.volume})"
        )

    def __str__(self):
        return f"""
date:\t{self.date}
open:\t{self.open}
high:\t{self.high}
low:\t{self.low}
close:\t{self.close}
close_adj:\t{self.close_adj}
volume:\t{self.volume}
"""


class Return(Structure):
    _fields_ = [
        ("year", c_int32),
        ("value", c_double),
    ]

    def __repr__(self):
        return f"(Return: {self.year}, {self.value})"

    def __str__(self):
        return f"""
year:\t{self.year}
value:\t{self.value}
"""


class StockS(Structure):
    pass


lib.stock_new.argtypes = (POINTER(Price), c_size_t)
lib.stock_new.restype = POINTER(StockS)
lib.stock_free.argtypes = (POINTER(StockS),)
lib.stock_stat_active_all.argtypes = (POINTER(StockS),)
lib.stock_stat_active_all.restype = Stat
lib.stock_stat_hold_all.argtypes = (POINTER(StockS),)
lib.stock_stat_hold_all.restype = Stat
lib.stock_n_years.argtypes = (POINTER(StockS),)
lib.stock_n_years.restype = c_size_t
lib.stock_stat_active_year.argtypes = (POINTER(StockS), POINTER(Stat), c_size_t)
lib.stock_stat_hold_year.argtypes = (POINTER(StockS), POINTER(Stat), c_size_t)
lib.stock_year_return.argtypes = (POINTER(StockS), POINTER(Return), c_size_t)


class Stock:
    def _convert(self, df):
        assert ptypes.is_datetime64_dtype(df.index)
        assert (df.columns == ["Open", "High", "Low", "Close", "CloseAdj", "Volume"]).all()
        assert not df.isnull().values.any()  # 任意為 nan
        assert not (df == 0).values.all(axis=1).any()  # 任意單 row 全為 0
        df = df.sort_index()

        data = []

        for _, row in df.reset_index().iterrows():
            data.append(
                Price(
                    date=row[0].strftime("%Y-%m-%d").encode("utf-8"),
                    # date=f"{row[0].year}-{row[0].month}-{row[0].day}".encode("utf-8"),
                    open=row[1],
                    high=row[2],
                    low=row[3],
                    close=row[4],
                    close_adj=row[5],
                    volume=row[6],
                )
            )
        return data

    def __init__(self, df):
        data = self._convert(df)
        buf_type = Price * len(data)
        buf = buf_type(*data)

        self.obj = lib.stock_new(buf, len(data))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        lib.stock_free(self.obj)

    def stat_active_all(self):
        activeAll = lib.stock_stat_active_all(self.obj)
        df = pd.DataFrame(
            {
                "All": {
                    "count": activeAll.count,
                    "mean": activeAll.mean,
                    "std": activeAll.std,
                    "min": activeAll.min,
                    "25%": activeAll.q1,
                    "50%": activeAll.q2,
                    "75%": activeAll.q3,
                    "max": activeAll.max,
                }
            },
        )
        return df

    def stat_hold_all(self):
        holdAll = lib.stock_stat_hold_all(self.obj)
        df = pd.DataFrame(
            {
                "All": {
                    "count": holdAll.count,
                    "mean": holdAll.mean,
                    "std": holdAll.std,
                    "min": holdAll.min,
                    "25%": holdAll.q1,
                    "50%": holdAll.q2,
                    "75%": holdAll.q3,
                    "max": holdAll.max,
                }
            },
        )
        return df

    def stat_active_year(self):
        data_len = lib.stock_n_years(self.obj)

        buf_type = Stat * data_len
        # 當回傳用，只需初始化，不用傳入資料，ex: buf_type(*data)
        buf = buf_type()
        lib.stock_stat_active_year(self.obj, buf, data_len)

        activeYear = buf
        df = pd.DataFrame(
            {
                data.year: {
                    "count": data.count,
                    "mean": data.mean,
                    "std": data.std,
                    "min": data.min,
                    "25%": data.q1,
                    "50%": data.q2,
                    "75%": data.q3,
                    "max": data.max,
                }
                for data in activeYear
            }
        ).sort_index(axis=1)
        return df

    def stat_hold_year(self):
        data_len = lib.stock_n_years(self.obj)

        buf_type = Stat * data_len
        # 當回傳用，只需初始化，不用傳入資料，ex: buf_type(*data)
        buf = buf_type()
        lib.stock_stat_hold_year(self.obj, buf, data_len)

        holdYear = buf
        df = pd.DataFrame(
            {
                data.year: {
                    "count": data.count,
                    "mean": data.mean,
                    "std": data.std,
                    "min": data.min,
                    "25%": data.q1,
                    "50%": data.q2,
                    "75%": data.q3,
                    "max": data.max,
                }
                for data in holdYear
            }
        ).sort_index(axis=1)
        return df

    def year_return(self):
        data_len = lib.stock_n_years(self.obj)

        buf_type = Return * data_len
        # 當回傳用，只需初始化，不用傳入資料，ex: buf_type(*data)
        buf = buf_type()
        lib.stock_year_return(self.obj, buf, data_len)

        yearReturn = buf
        df = pd.DataFrame(
            {
                data.year: {
                    "return": data.value,
                }
                for data in yearReturn
            }
        ).sort_index(axis=1)
        return df


def getSymbolStatistic(df):
    """
    df format
        Open, High, Low, Close, CloseAdj, Volume
    date
    """
    with Stock(df) as stock:
        activeAll = stock.stat_active_all()
        activeYear = stock.stat_active_year()
        holdAll = stock.stat_hold_all()
        holdYear = stock.stat_hold_year()
        yearReturn = stock.year_return()

    return activeAll, activeYear, holdAll, holdYear, yearReturn
