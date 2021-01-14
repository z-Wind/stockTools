import pandas as pd
import io
import requests
import os
import time

from datetime import datetime
from dateutil.relativedelta import relativedelta


def transform_date(date):  # 民國轉西元
    y, m, d = date.split("/")
    return str(int(y) + 1911) + "/" + m + "/" + d


def gen_iter_date_by_month(start, end):
    while end >= start:
        yield start
        start = start + relativedelta(months=1)


def getDatas(path):
    datas = {}
    for dirPath, dirNames, fileNames in os.walk(path):
        fileNames.sort()
        # 移除最後一筆，以免資料不全
        os.remove(os.path.join(dirPath, fileNames[-1]))
        del fileNames[-1]
        for f in fileNames:
            datas[os.path.splitext(f)[0]] = True

    return datas


def save_0050_index():
    symbol = "臺灣50指數"
    savePath = os.path.join("./extraData", symbol)
    os.makedirs(savePath, exist_ok=True)
    datas = getDatas(savePath)

    start = datetime(2002, 10, 1)
    end = datetime.now() + relativedelta(day=1)  # 設定為當月的 1 號

    with requests.Session() as s:
        for day in gen_iter_date_by_month(start, end):
            d = day.strftime("%Y%m%d")
            if datas.get(d, False):
                print(d, "already exists")
                continue

            url = f"https://www.twse.com.tw/indicesReport/TAI50I?response=csv&date={d}"

            c = s.get(url).content
            df = pd.read_csv(io.StringIO(c.decode("big5"))).drop("Unnamed: 3", axis=1)
            df.loc[:, "Date"] = pd.to_datetime(df["日期"].apply(transform_date), format="%Y/%m/%d")
            df.loc[:, "Close"] = df["臺灣50指數"].str.replace(",", "").astype(float)
            df.loc[:, "Adj Close"] = df["臺灣50報酬指數"].str.replace(",", "").astype(float)
            df.loc[:, "Dividends"] = 0
            df.loc[:, "Stock Splits"] = 0

            time.sleep(5)

            df.to_csv(os.path.join(savePath, f"{d}.csv"), index=False)
            print(d, "saved")


if __name__ == "__main__":
    save_0050_index()
