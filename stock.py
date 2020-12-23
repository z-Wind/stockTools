import os
import yfinance as yf
import datetime
import pandas as pd
from pyquery import PyQuery
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from dateutil.relativedelta import relativedelta


class Stock:
    start = datetime.datetime.strptime("1970-01-02", "%Y-%m-%d")
    end = datetime.datetime.now()
    history = None
    yfinance = None
    rawData = None

    def __init__(self, symbol, start=None, end=None, extraDiv={}, replaceDiv=False):
        self.symbol = symbol
        if start:
            self.start = datetime.datetime.strptime(start, "%Y-%m-%d")
        if end:
            self.end = datetime.datetime.strptime(end, "%Y-%m-%d")

        self.extraDiv = extraDiv
        self.replaceDiv = self._getDiv_TW() if replaceDiv else {}

        self.history = self._getHistory()

    def _getDiv_TW(self):
        try:
            dom = PyQuery(
                "https://www.moneydj.com/ETF/X/Basic/Basic0005.xdjhtm?etfid=" + self.symbol
            )
            data = dom(".datalist")

            replaceDiv = {}
            for i in data.find(".col02").items():
                replaceDiv[i.text()] = float(i.nextAll(".col07").text())
        except:
            return {}

        return replaceDiv

    def _getHistory(self):
        if self.history:
            return self.history

        self.yfinance = yf.Ticker(self.symbol)
        hist = self.yfinance.history(
            start="1970-01-02", end=datetime.datetime.now(), auto_adjust=False
        )
        self.rawData = hist

        data = self._calAdjClose(hist)
        index = (self.start <= data["Date"]) & (data["Date"] <= self.end)
        data = data[index]

        return data

    def _calAdjClose(self, df):
        div = df[["Dividends"]]
        if self.replaceDiv:
            div.loc[:, "Dividends"] = 0

        div = div[div["Dividends"] != 0]

        for date, divVal in self.extraDiv.items():
            dt = datetime.datetime.strptime(date, "%Y/%m/%d")
            div.loc[dt, "Dividends"] = divVal

        for date, divVal in self.replaceDiv.items():
            dt = datetime.datetime.strptime(date, "%Y/%m/%d")
            div.loc[dt, "Dividends"] = divVal

        div = div.reset_index()
        print(self.symbol)
        print(div)

        data = df.reset_index()
        data.loc[:, "Adj Close Cal"] = 0.0
        data.loc[:, "Adj Ratio"] = 1.0

        for i, row in div.iterrows():
            divDate = row.Date
            divVal = row.Dividends
            index = data["Date"] < divDate
            if index.any():
                data.loc[index, "Adj Ratio"] *= 1 - divVal / data.loc[index, "Close"].iloc[-1]

        data.loc[:, "Adj Close Cal"] = data.loc[:, "Close"] * data.loc[:, "Adj Ratio"]

        return data

    @property
    def yearReturn(self):
        data = self.history

        years = data.Date.dt.year.drop_duplicates()

        yearReturn = {}
        for y in years:
            yearData = data[data.Date.dt.year == y]
            first = yearData.iloc[0]["Adj Close Cal"]
            end = yearData.iloc[-1]["Adj Close Cal"]
            yearReturn[y] = (end - first) / first * 100

        df = pd.DataFrame(yearReturn, index=[self.symbol])

        return df.T

    @property
    def totalReturn(self):
        data = self.history

        first = data.iloc[0]["Adj Close Cal"]
        end = data.iloc[-1]["Adj Close Cal"]
        totalReturn = (end - first) / first * 100

        return totalReturn

    def rollback(self, iYear):
        interval = relativedelta(years=iYear)
        data = self.history.iloc[::-1]

        pairs = []
        for i, row in data.iterrows():
            t = row["Date"] - interval
            start = data[data["Date"] <= t]
            if start.empty:
                break
            start = start.iloc[0, :]
            pairs.append((start, row))

        t = [p[1]["Date"] for p in pairs]
        r = [
            (p[1]["Adj Close Cal"] - p[0]["Adj Close Cal"]) / p[0]["Adj Close Cal"] * 100
            for p in pairs
        ]

        df = pd.DataFrame({self.symbol: r}, index=t)

        return df.sort_index()


def plotBar(df, title_text=None):
    datas = []
    for (symbol, data) in df.iteritems():
        datas.append(go.Bar(name=symbol, x=data.index, y=data))

    fig = go.Figure(data=datas)
    # Change the bar mode
    fig.update_layout(barmode="group", title_text=title_text)
    # fig.show()

    return fig


def plotArea(df, title_text=None):
    fig = go.Figure()
    for (symbol, data) in df.iteritems():
        fig.add_trace(go.Scatter(name=symbol, x=data.index, y=data, fill="tozeroy", mode="none",))

    fig.update_layout(title_text=title_text)
    # fig.show()

    return fig


def compare(
    symbols, start="2000-01-01", end=datetime.datetime.now().strftime("%Y-%m-%d"), prefix=""
):
    if not os.path.exists("images"):
        os.mkdir("images")

    stocks = []
    for symbol in symbols:
        stocks.append(
            Stock(
                symbol["name"],
                start,
                end,
                extraDiv=symbol.get("extraDiv", {}),
                replaceDiv=symbol.get("replaceDiv", False),
            )
        )

    # year return
    data = []
    for st in stocks:
        data.append(st.yearReturn)

    df = pd.concat(data, axis=1)
    print(df)
    fig = plotBar(df, title_text=f"Annual Return")
    fig.write_html(f"images/{prefix}_YearReturn.html")
    # fig.write_image(f"images/{prefix}_YearReturn.png", width=1920, height=1080, scale=2)

    # total return
    data = {}
    for st in stocks:
        data[st.symbol] = st.totalReturn

    df = pd.DataFrame(data, index=["Total Return"])
    print(df)
    fig = plotBar(df, title_text=f"Total Return")
    fig.write_html(f"images/{prefix}_TotalReturn.html")
    # fig.write_image(f"images/{prefix}_TotalReturn.png", width=1920, height=1080, scale=2)

    # roll back
    data = []
    iYear = 5
    for st in stocks:
        data.append(st.rollback(iYear))

    df = pd.concat(data, axis=1)
    print(df)
    fig = plotArea(df, title_text=f"{iYear} Years Roll Back")
    fig.write_html(f"images/{prefix}_RollBack.html")
    # fig.write_image(f"images/{prefix}_RollBack.png", width=1920, height=1080, scale=2)


if __name__ == "__main__":
    symbols = [
        {"name": "006208.TW", "replaceDiv": True},
        {"name": "0050.TW", "replaceDiv": True},
        {"name": "^TWII"},
        {"name": "0051.TW", "replaceDiv": True},
        {"name": "0056.TW", "replaceDiv": True},
        {"name": "2412.TW", "replaceDiv": True},
        {"name": "2002.TW", "replaceDiv": True},
        {"name": "2330.TW", "replaceDiv": True},
        {"name": "2317.TW", "replaceDiv": True},
        {"name": "6505.TW", "replaceDiv": True},
        {"name": "3481.TW", "replaceDiv": True},
        {"name": "2303.TW", "replaceDiv": True},
    ]
    compare(symbols, prefix="TW")

    symbols = [
        {"name": "VTI"},
        {"name": "VBR"},
        {"name": "VPL"},
        {"name": "VGK"},
        {"name": "VWO"},
        {"name": "BND"},
        {"name": "BNDX"},
        {"name": "BWX"},
        {"name": "VNQ"},
    ]
    compare(symbols, prefix="USA")
