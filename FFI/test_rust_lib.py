#!/usr/bin/env python3

import pytest
import pandas as pd
from .rust_lib import getSymbolStatistic, Stock


@pytest.fixture(scope="session")
def test_df():
    df = pd.read_csv("rust_lib/tests/data.csv", index_col=0)
    df.index = pd.to_datetime(df.index)
    print(df)

    return df


def test_all(test_df):
    df = test_df

    with Stock(df) as stock:
        activeAll = stock.stat_active_all()
        print(activeAll)
        df = pd.read_csv("rust_lib/tests/activeAll.csv", index_col=0)
        assert ((df - activeAll) < 0.00001).all().all()

        activeYear = stock.stat_active_year()
        print(activeYear)
        df = pd.read_csv("rust_lib/tests/activeYear.csv", index_col=0)
        df.columns = df.columns.astype(int)
        assert ((df - activeYear) < 0.00001).all().all()

        holdAll = stock.stat_hold_all()
        print(holdAll)
        df = pd.read_csv("rust_lib/tests/holdAll.csv", index_col=0)
        assert ((df - holdAll) < 0.00001).all().all()

        holdYear = stock.stat_hold_year()
        print(holdYear)
        df = pd.read_csv("rust_lib/tests/holdYear.csv", index_col=0)
        df.columns = df.columns.astype(int)
        assert ((df - holdYear) < 0.00001).all().all()

        yearReturn = stock.year_return()
        print(yearReturn)
        df = pd.read_csv("rust_lib/tests/yearReturn.csv", index_col=0)
        df.columns = df.columns.astype(int)
        assert ((df - yearReturn) < 0.00001).all().all()


def test_getSymbolStatistic(benchmark, test_df):
    df = test_df

    benchmark(getSymbolStatistic, df)