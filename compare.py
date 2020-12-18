import yfinance as yf
import datetime
import pandas as pd
import plotly.graph_objects as go
from pyquery import PyQuery


def getDiv_TW(symbol):
    try:
        dom = PyQuery("https://www.moneydj.com/ETF/X/Basic/Basic0005.xdjhtm?etfid=" + symbol)
        data = dom(".datalist")

        extraDiv = {}
        for i in data.find(".col02").items():
            extraDiv[i.text()] = float(i.nextAll(".col07").text())
    except e:
        print(e)
        return {}

    return extraDiv


def getHistory(symbol, start="2000-01-01", end=datetime.datetime.now()):
    stock = yf.Ticker(symbol)
    hist = stock.history(start=start, end=end, auto_adjust=False)

    return hist


def calAdjClose(df, extraDiv={}, replaceDiv={}):
    div = df[["Dividends"]]
    if replaceDiv:
        div.loc[:, "Dividends"] = 0

    div = div[div["Dividends"] != 0]

    # 依需求補上正確的 dividends
    for date, divVal in extraDiv.items():
        dt = datetime.datetime.strptime(date, "%Y/%m/%d")
        div.loc[dt, "Dividends"] = divVal

    for date, divVal in replaceDiv.items():
        dt = datetime.datetime.strptime(date, "%Y/%m/%d")
        div.loc[dt, "Dividends"] = divVal

    div = div.reset_index()
    print(div)

    data = df.reset_index()
    data["Adj Close Cal"] = 0.0
    data["Adj Ratio"] = 1.0

    for i, row in div.iterrows():
        divDate = row.Date
        divVal = row.Dividends
        index = data["Date"] < divDate
        if index.any():
            data.loc[index, "Adj Ratio"] *= 1 - divVal / data.loc[index, "Close"].iloc[-1]

    data.loc[:, "Adj Close Cal"] = data.loc[:, "Close"] * data.loc[:, "Adj Ratio"]

    return data


def getYearReturn(
    symbol, start="2000-01-01", end=datetime.datetime.now(), extraDiv={}, replaceDiv={}
):
    hist = getHistory(symbol, start, end)
    print(symbol)
    data = calAdjClose(hist, extraDiv, replaceDiv)

    years = data.Date.dt.year.drop_duplicates()

    yearReturn = {}
    for y in years:
        yearData = data[data.Date.dt.year == y]
        first = yearData.iloc[0]["Adj Close Cal"]
        end = yearData.iloc[-1]["Adj Close Cal"]
        yearReturn[y] = (end - first) / first * 100

    df = pd.DataFrame(yearReturn, index=[symbol])

    return df.T


def getTotalReturn(
    symbol, start="2000-01-01", end=datetime.datetime.now(), extraDiv={}, replaceDiv={}
):
    hist = getHistory(symbol, start, end)
    print(symbol)
    data = calAdjClose(hist, extraDiv, replaceDiv)

    first = data.iloc[0]["Adj Close Cal"]
    end = data.iloc[-1]["Adj Close Cal"]
    totalReturn = (end - first) / first * 100

    return totalReturn


def plotBar(df):
    datas = []
    for (symbol, data) in df.iteritems():
        datas.append(go.Bar(name=symbol, x=data.index, y=data))

    fig = go.Figure(data=datas)
    # Change the bar mode
    fig.update_layout(barmode="group")
    fig.show()


def compare(symbols, start="2000-01-01", end=datetime.datetime.now()):
    data = []
    for symbol in symbols:
        data.append(
            getYearReturn(
                symbol["name"],
                start=start,
                end=end,
                extraDiv=symbol.get("extraDiv", {}),
                replaceDiv=symbol.get("replaceDiv", {}),
            )
        )

    df = pd.concat(data, axis=1)
    print(df)

    plotBar(df)

    data = {}
    for symbol in symbols:
        data[symbol["name"]] = getTotalReturn(
            symbol["name"],
            start=start,
            end=end,
            extraDiv=symbol.get("extraDiv", {}),
            replaceDiv=symbol.get("replaceDiv", {}),
        )

    df = pd.DataFrame(data, index=["Total Return"])
    print(df)

    plotBar(df)


if __name__ == "__main__":
    symbols = [
        {"name": "006208.TW", "replaceDiv": getDiv_TW("006208.TW")},
        {"name": "0050.TW", "extraDiv": getDiv_TW("0050.TW")},
        {"name": "^TWII"},
        {"name": "0051.TW", "replaceDiv": getDiv_TW("0051.TW")},
        {"name": "0056.TW", "replaceDiv": getDiv_TW("0056.TW")},
        {"name": "2412.TW", "replaceDiv": getDiv_TW("2412.TW")},
        {"name": "2002.TW", "replaceDiv": getDiv_TW("2002.TW")},
        {"name": "2330.TW", "replaceDiv": getDiv_TW("2330.TW")},
        {"name": "2317.TW", "replaceDiv": getDiv_TW("2317.TW")},
        {"name": "6505.TW", "replaceDiv": getDiv_TW("6505.TW")},
        {"name": "3481.TW", "replaceDiv": getDiv_TW("3481.TW")},
        {"name": "2303.TW", "replaceDiv": getDiv_TW("2303.TW")},
    ]
    compare(symbols, start="2013-01-01")
