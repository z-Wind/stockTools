import pandas as pd
import io
import requests
import os
import time
import numpy as np

from datetime import datetime
from dateutil.relativedelta import relativedelta


def transform_date(date):  # 民國轉西元
    y, m, d = date.split("/")
    return str(int(y) + 1911) + "/" + m + "/" + d


def process_data(data):
    data = data.replace(",", "")
    data = data.replace("--", "")

    if data == "":
        return np.nan

    return data


def gen_iter_date_by_month(start, end):
    while end >= start:
        yield start
        start = start + relativedelta(months=1)


def getDatas(path):
    datas = {}
    for dirPath, dirNames, fileNames in os.walk(path):
        if len(fileNames) == 0:
            continue

        fileNames.sort()
        # 移除當月資料，以免資料不全
        current = f"{(datetime.now() + relativedelta(day=1)).strftime('%Y%m%d')}.csv"
        if fileNames[-1] == current:
            os.remove(os.path.join(dirPath, fileNames[-1]))
            del fileNames[-1]
            print("remove", fileNames[-1])

        for f in fileNames:
            datas[os.path.splitext(f)[0]] = True

    return datas


def save_0050_index(s):
    symbol = "臺灣50指數"
    savePath = os.path.join("./extraData", symbol)
    os.makedirs(savePath, exist_ok=True)
    datas = getDatas(savePath)

    start = datetime(2002, 10, 1)
    end = datetime.now() + relativedelta(day=1)  # 設定為當月的 1 號

    for day in gen_iter_date_by_month(start, end):
        d = day.strftime("%Y%m%d")
        if datas.get(d, False):
            print(symbol, d, "already exists")
            continue

        print(symbol, d, "saving...")

        url = f"https://www.twse.com.tw/indicesReport/TAI50I?response=csv&date={d}"

        c = s.get(url).content
        df = pd.read_csv(io.StringIO(c.decode("big5"))).drop("Unnamed: 3", axis=1)
        df.loc[:, "Date"] = pd.to_datetime(df["日期"].apply(transform_date), format="%Y/%m/%d")
        df.loc[:, "Close"] = df["臺灣50指數"].apply(process_data).astype(float)
        df.loc[:, "Adj Close"] = df["臺灣50報酬指數"].apply(process_data).astype(float)
        df.loc[:, "Dividends"] = 0
        df.loc[:, "Stock Splits"] = 0

        time.sleep(5)

        df = df[df["Adj Close"] != 0]
        df.to_csv(os.path.join(savePath, f"{d}.csv"), index=False)


def save_TAIEX_index(s):
    symbol = "臺灣加權股價指數"
    savePath = os.path.join("./extraData", symbol)
    os.makedirs(savePath, exist_ok=True)
    datas = getDatas(savePath)

    start = datetime(2003, 1, 1)
    end = datetime.now() + relativedelta(day=1)  # 設定為當月的 1 號

    for day in gen_iter_date_by_month(start, end):
        d = day.strftime("%Y%m%d")
        if datas.get(d, False):
            print(symbol, d, "already exists")
            continue

        print(symbol, d, "saving...")

        histURL = f"https://www.twse.com.tw/indicesReport/MI_5MINS_HIST?response=csv&date={d}"
        totalReturnURL = f"https://www.twse.com.tw/indicesReport/MFI94U?response=csv&date={d}"

        c = s.get(histURL).content
        hist = pd.read_csv(io.StringIO(c.decode("big5")), header=1).drop("Unnamed: 5", axis=1)

        c = s.get(totalReturnURL).content
        totalReturn = pd.read_csv(io.StringIO(c.decode("big5")), header=1).drop(
            "Unnamed: 2", axis=1
        )

        df = hist.join(totalReturn)
        assert df[df["日期"] != df["日　期"]].empty

        df.loc[:, "Date"] = pd.to_datetime(df["日期"].apply(transform_date), format="%Y/%m/%d")
        df.loc[:, "Open"] = df["開盤指數"].apply(process_data).astype(float)
        df.loc[:, "High"] = df["最高指數"].apply(process_data).astype(float)
        df.loc[:, "Low"] = df["最低指數"].apply(process_data).astype(float)
        df.loc[:, "Close"] = df["收盤指數"].apply(process_data).astype(float)
        df.loc[:, "Adj Close"] = df["發行量加權股價報酬指數"].apply(process_data).astype(float)
        df.loc[:, "Dividends"] = 0
        df.loc[:, "Stock Splits"] = 0

        time.sleep(5)

        df = df[df["Adj Close"] != 0]
        df.to_csv(os.path.join(savePath, f"{d}.csv"), index=False)


if __name__ == "__main__":
    with requests.Session() as s:
        save_0050_index(s)
        save_TAIEX_index(s)
