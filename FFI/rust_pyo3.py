#!/usr/bin/env python3

from rust_pyo3 import Price, Stock as Stock_rs, Stat
import pandas as pd
import pandas.api.types as ptypes

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
                    date=row.iloc[0],
                    open=row.iloc[1],
                    high=row.iloc[2],
                    low=row.iloc[3],
                    close=row.iloc[4],
                    close_adj=row.iloc[5],
                    volume=max(0, int(row.iloc[6])),
                )
            )
        return data

    def __init__(self, df):
        data = self._convert(df)

        self.stock = Stock_rs(data)

    def stat_active_all(self):
        activeAll = self.stock.stat_active_all()
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
        holdAll = self.stock.stat_hold_all()
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
        activeYear = self.stock.stat_active_year()
        df = pd.DataFrame(
            {
                year: {
                    "count": data.count,
                    "mean": data.mean,
                    "std": data.std,
                    "min": data.min,
                    "25%": data.q1,
                    "50%": data.q2,
                    "75%": data.q3,
                    "max": data.max,
                }
                for (year, data) in activeYear
            }
        ).sort_index(axis=1)
        return df

    def stat_hold_year(self):
        holdYear = self.stock.stat_hold_year()
        df = pd.DataFrame(
            {
                year: {
                    "count": data.count,
                    "mean": data.mean,
                    "std": data.std,
                    "min": data.min,
                    "25%": data.q1,
                    "50%": data.q2,
                    "75%": data.q3,
                    "max": data.max,
                }
                for (year, data) in holdYear
            }
        ).sort_index(axis=1)
        return df

    def year_return(self):
        yearReturn = self.stock.cal_years_return()
        df = pd.DataFrame(
            {
                year: {
                    "return": year_return,
                }
                for (year, year_return) in yearReturn
            }
        ).sort_index(axis=1)
        return df


def getSymbolStatistic(df):
    """
    df format
        Open, High, Low, Close, CloseAdj, Volume
    date
    """
    stock = Stock(df)
    activeAll = stock.stat_active_all()
    activeYear = stock.stat_active_year()
    holdAll = stock.stat_hold_all()
    holdYear = stock.stat_hold_year()
    yearReturn = stock.year_return()

    return activeAll, activeYear, holdAll, holdYear, yearReturn
