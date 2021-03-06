import os
import yfinance as yf
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px

from datetime import datetime
from pyquery import PyQuery
from dateutil.relativedelta import relativedelta

path = "images/detail"


class Stock:
    start = datetime.strptime("1970-01-02", "%Y-%m-%d")
    end = datetime.now()
    history = None
    yfinance = None
    rawData = None

    def __init__(
        self, symbol, remark="", start=None, end=None, extraDiv={}, replaceDiv=False, fromPath=""
    ):
        """
        symbol: 代碼
        remark: 註解，方便辨識
        start:  開始時間
        end:    結束時間
        =======================================
        extraDiv:   額外的股息資料
        replaceDiv: 取代原本的股息資料
        =======================================
        fromPath: 外部匯入股價資訊，需有以下欄位資訊
        Date     |   Close |  Adj Close   |   Dividends  |    Stock Splits
        %Y-%m-%d |   Float |  Float       |   Float      |    Int
        """
        self.symbol = symbol
        self.remark = remark
        if start:
            self.start = datetime.strptime(start, "%Y-%m-%d")
        if end:
            self.end = datetime.strptime(end, "%Y-%m-%d")

        self.extraDiv = extraDiv
        self.replaceDiv = self._getDiv_TW() if replaceDiv else {}

        self.history = self._getHistory(fromPath)

    def _getDiv_TW(self):
        try:
            dom = PyQuery(
                "https://www.moneydj.com/ETF/X/Basic/Basic0005.xdjhtm?etfid=" + self.symbol
            )
            data = dom(".datalist")

            replaceDiv = {}
            for i in data.find(".col02").items():
                replaceDiv[i.text()] = float(i.nextAll(".col07").text())
        except Exception as e:
            print(e)
            return {}

        return replaceDiv

    def _getData(self, path):
        # 需有以下資訊
        # Date     |   Close |  Adj Close   |   Dividends  |    Stock Splits
        # %Y-%m-%d |   Float |  Float       |   Float      |    Int
        dfs = []
        for dirPath, dirNames, fileNames in os.walk(path):
            for f in fileNames:
                dfs.append(pd.read_csv(os.path.join(dirPath, f)))

        df = pd.concat(dfs, ignore_index=True)
        df.loc[:, "Date"] = pd.to_datetime(df["Date"], format="%Y-%m-%d")
        df = df[df["Adj Close"] != 0]

        assert set(["Date", "Close", "Adj Close", "Dividends", "Stock Splits"]).issubset(df.columns)

        df = df.set_index("Date")

        return df

    def _getHistory(self, fromPath):
        if self.history:
            return self.history

        if fromPath:
            hist = self._getData(fromPath)
        else:
            self.yfinance = yf.Ticker(self.symbol)
            hist = self.yfinance.history(start="1970-01-02", end=datetime.now(), auto_adjust=False)

        data = self._calAdjClose(hist)
        self.rawData = data
        index = (self.start <= data["Date"]) & (data["Date"] <= self.end)
        data = data[index]

        return data

    def _calAdjClose(self, df):
        div = df[["Dividends"]]
        if self.replaceDiv:
            div.loc[:, "Dividends"] = 0

        div = div[div["Dividends"] != 0]

        for date, divVal in self.extraDiv.items():
            dt = datetime.strptime(date, "%Y/%m/%d")
            div.loc[dt, "Dividends"] = divVal

        for date, divVal in self.replaceDiv.items():
            dt = datetime.strptime(date, "%Y/%m/%d")
            div.loc[dt, "Dividends"] = divVal

        div = div.reset_index()
        print(self.name)

        data = df.reset_index()
        if div.empty:
            print("empty Dividends")
            data.loc[:, "Adj Close Cal"] = data["Adj Close"]
            return data.sort_values("Date")

        print(div)

        data.loc[:, "Adj Close Cal"] = 0.0
        data.loc[:, "Adj Ratio"] = 1.0

        for i, row in div.iterrows():
            divDate = row.Date
            divVal = row.Dividends
            if data["Date"][data["Date"] >= divDate].empty:
                continue
            index = data["Date"] < divDate
            if index.any():
                data.loc[index, "Adj Ratio"] *= 1 - divVal / data.loc[index, "Close"].iloc[-1]

        data.loc[:, "Adj Close Cal"] = data.loc[:, "Close"] * data.loc[:, "Adj Ratio"]

        return data.sort_values("Date")

    @property
    def name(self):
        symbol = self.symbol.replace(".TW", "")
        if self.remark:
            return f"{symbol:7s}{self.remark}"
        else:
            return symbol

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

        df = pd.DataFrame(yearReturn, index=[self.name])

        return df.T

    @property
    def totalReturn(self):
        data = self.history

        first = data.iloc[0]["Adj Close Cal"]
        end = data.iloc[-1]["Adj Close Cal"]
        totalReturn = (end - first) / first * 100

        return totalReturn

    def rollback(self, iYear):
        start = self.rawData["Date"].iloc[0]
        end = self.rawData["Date"].iloc[-1]
        if end < start + relativedelta(years=iYear):
            raise ValueError(f"{self.name}: raw data {start} - {end} 間隔時間小於 iYear:{iYear}")

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

        df = pd.DataFrame({self.name: r}, index=t)

        return df.sort_index()


def plotBar(df, title_text=None):
    dataList = []
    for (symbol, data) in df.iteritems():
        dataList.append(go.Bar(name=symbol, x=data.index, y=data))

    fig = go.Figure(data=dataList)
    # Change the bar mode
    fig.update_layout(barmode="group", title_text=title_text)
    fig.update_layout(font_family="Courier New", title_font_family="Times New Roman")
    fig.update_layout(hovermode="x")
    # fig.show()

    return fig


def plotArea(df, title_text=None):
    fig = go.Figure()
    for (symbol, data) in df.iteritems():
        fig.add_trace(go.Scatter(name=symbol, x=data.index, y=data, fill="tozeroy", mode="none"))

    fig.update_layout(title_text=title_text, xaxis_title="End Date")
    fig.update_layout(font_family="Courier New", title_font_family="Times New Roman")
    fig.update_layout(hovermode="x")
    # fig.show()

    return fig


def plotViolin(df, title_text=None):
    fig = go.Figure()
    for (symbol, data) in df.iteritems():
        fig.add_trace(go.Violin(y=data, name=symbol, box_visible=True, meanline_visible=True))

    fig.update_xaxes(tickfont_family="Courier New", tickfont_size=14, tickangle=90)
    fig.update_layout(title_text=title_text)
    fig.update_layout(font_family="Courier New", title_font_family="Times New Roman")
    # fig.show()

    return fig


def plotImshow(df, title_text=None, range_color=[-1, 1]):
    fig = px.imshow(df, x=df.index, y=df.columns, range_color=range_color)

    fig.update_xaxes(side="top", tickfont_family="Courier New", tickangle=-90)
    fig.update_yaxes(side="right", tickfont_family="Courier New")
    fig.update_layout(title_text=title_text)
    fig.update_layout(font_family="Courier New", title_font_family="Times New Roman")
    # fig.show()

    return fig


def genFigure(
    symbols,
    start="1970-01-02",
    end=datetime.now().strftime("%Y-%m-%d"),
    prefix="",
    iYear=5,
    image=False,
):
    os.makedirs(path, exist_ok=True)
    if datetime.strptime(end, "%Y-%m-%d") < datetime.strptime(start, "%Y-%m-%d") + relativedelta(
        years=iYear
    ):
        raise ValueError(f"{start} - {end} 間隔時間小於 iYear:{iYear}")

    stocks = []
    for symbol in symbols:
        stocks.append(
            Stock(
                symbol["name"],
                remark=symbol["remark"],
                start=start,
                end=end,
                extraDiv=symbol.get("extraDiv", {}),
                replaceDiv=symbol.get("replaceDiv", False),
                fromPath=symbol.get("fromPath", False),
            )
        )

    # annual return
    data = []
    for st in stocks:
        data.append(st.yearReturn)

    df = pd.concat(data, axis=1)
    fig = plotBar(df, title_text=f"<b>Annual Return<b>")
    fig.write_html(os.path.join(path, f"{prefix}_AnnualReturn.html"))
    if image:
        fig.write_image(
            os.path.join(path, f"{prefix}_AnnualReturn.png"), width=1920, height=1080, scale=2
        )

    # total return
    # 只取交集時間故無法直接套用 stock.totalReturn
    data = []
    for st in stocks:
        s = pd.Series(
            data=st.history["Adj Close Cal"].to_numpy(),
            index=st.history["Date"].to_numpy(),
            name=st.name,
        )
        data.append(s)

    df = pd.concat(data, axis=1)
    df = df.dropna()
    start = df.index[0].strftime("%Y/%m/%d")
    end = df.index[-1].strftime("%Y/%m/%d")
    df = pd.DataFrame(
        (df.iloc[-1, :] - df.iloc[0, :]) / df.iloc[0, :] * 100, columns=["Total Return"]
    )
    df = df.T  # for df.iteritems()
    fig = plotBar(df, title_text=f"<b>Total Return<b><br><i>{start} ~ {end}<i>")
    fig.write_html(os.path.join(path, f"{prefix}_TotalReturn.html"))
    if image:
        fig.write_image(
            os.path.join(path, f"{prefix}_TotalReturn.png"), width=1920, height=1080, scale=2
        )

    # roll back
    data = []
    for st in stocks:
        data.append(st.rollback(iYear))

    df = pd.concat(data, axis=1)
    fig = plotArea(df, title_text=f"<b>{iYear} Years Roll Back<b>")
    fig.write_html(os.path.join(path, f"{prefix}_RollBack.html"))
    if image:
        fig.write_image(
            os.path.join(path, f"{prefix}_RollBack.png"), width=1920, height=1080, scale=2
        )

    # roll back volin
    # 只取交集時間
    df = df.dropna()
    if df.index.empty:
        raise ValueError("無交集時間")

    start = df.index[0] - pd.DateOffset(years=iYear)
    end = df.index[-1]
    fig = plotViolin(
        df,
        title_text=(
            f"<b>{iYear} Years Roll Back<b><br>"
            f"Start: <i>{start.strftime('%Y/%m/%d')} ~"
            f" {(end-relativedelta(years=iYear)).strftime('%Y/%m/%d')}<i><br>"
            f"End  : <i>{(start+relativedelta(years=iYear)).strftime('%Y/%m/%d')} ~"
            f" {end.strftime('%Y/%m/%d')}<i>"
        ),
    )
    fig.write_html(os.path.join(path, f"{prefix}_RollBack_Violin.html"))
    if image:
        fig.write_image(
            os.path.join(path, f"{prefix}_RollBack_Violin.png"), width=1920, height=1080, scale=2
        )

    # correlation
    data = []
    for st in stocks:
        s = pd.Series(
            data=st.rawData["Close"].to_numpy(), index=st.rawData["Date"].to_numpy(), name=st.name
        )
        data.append(s)

    df = pd.concat(data, axis=1)
    fig = plotImshow(df.corr(), title_text=f"<b>Correlation of Close<b>")
    fig.write_html(os.path.join(path, f"{prefix}_Correlation_Close.html"))
    if image:
        fig.write_image(
            os.path.join(path, f"{prefix}_Correlation_Close.png"), width=1920, height=1080, scale=2
        )

    data = []
    for st in stocks:
        s = pd.Series(
            data=st.rawData["Adj Close Cal"].to_numpy(),
            index=st.rawData["Date"].to_numpy(),
            name=st.name,
        )
        data.append(s)

    df = pd.concat(data, axis=1)
    fig = plotImshow(df.corr(), title_text=f"<b>Correlation of Adj Close <b>")
    fig.write_html(os.path.join(path, f"{prefix}_Correlation_AdjClose.html"))
    if image:
        fig.write_image(
            os.path.join(path, f"{prefix}_Correlation_AdjClose.png"),
            width=1920,
            height=1080,
            scale=2,
        )


def report(
    symbols,
    start="1970-01-02",
    end=datetime.now().strftime("%Y-%m-%d"),
    prefix="",
    iYear=5,
    image=False,
):
    genFigure(symbols, start=start, end=end, prefix=prefix, iYear=iYear, image=image)

    template = """
<html>
    <head>
        <title>Report</title>
        <meta charset="UTF-8">
        <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
        <script type="text/javascript" src="E_9_1.js"></script>
    </head>
    <body>
        <div id=totalReturn>
        </div>
        <div id=annualReturn>
        </div>
        <div id=rollBackVolin>
        </div>
        <div id=rollBack>
        </div>
        <div id=correlation style="display: flex">
            <div id=correlationClose>
            </div>
            <div id=correlationAdjClose>
            </div>
        </div>
    </body>
</html>"""

    dom = PyQuery(template)
    dom("head title").html(f"{prefix} Report")
    totalReturn = PyQuery(filename=os.path.join(path, f"{prefix}_TotalReturn.html"))
    annualReturn = PyQuery(filename=os.path.join(path, f"{prefix}_AnnualReturn.html"))
    rollBack = PyQuery(filename=os.path.join(path, f"{prefix}_RollBack.html"))
    rollBackVolin = PyQuery(filename=os.path.join(path, f"{prefix}_RollBack_Violin.html"))
    correlationClose = PyQuery(filename=os.path.join(path, f"{prefix}_Correlation_Close.html"))
    correlationAdjClose = PyQuery(
        filename=os.path.join(path, f"{prefix}_Correlation_AdjClose.html")
    )

    dom("#totalReturn").html(totalReturn("body"))
    dom("#annualReturn").html(annualReturn("body"))
    dom("#rollBack").html(rollBack("body"))
    dom("#rollBackVolin").html(rollBackVolin("body"))
    dom("#correlationClose").html(correlationClose("body"))
    dom("#correlationAdjClose").html(correlationAdjClose("body"))

    with open(os.path.join(path, f"../{prefix}_Report.html"), "w", encoding="UTF-8") as f:
        f.write(dom.outerHtml())


if __name__ == "__main__":
    symbols = [
        {"name": "^TWII", "remark": "台灣加權指數"},
        {
            "name": "^TAIEX",
            "remark": "台灣加權報酬指數",
            "fromPath": os.path.join("./extraData", "臺灣加權股價指數"),
        },
        {"name": "^0050", "remark": "0050報酬指數", "fromPath": os.path.join("./extraData", "臺灣50指數")},
        {"name": "0050.TW", "remark": "元大台灣50", "replaceDiv": True},
        {"name": "006208.TW", "remark": "富邦台50", "replaceDiv": True},
        {"name": "0051.TW", "remark": "元大中型100", "replaceDiv": True},
        {"name": "0056.TW", "remark": "元大高股息", "replaceDiv": True},
        {"name": "2412.TW", "remark": "中華電信", "replaceDiv": True},
        {"name": "2002.TW", "remark": "中鋼", "replaceDiv": True},
        {"name": "2330.TW", "remark": "台積電", "replaceDiv": True},
        {"name": "2317.TW", "remark": "鴻海", "replaceDiv": True},
        {"name": "6505.TW", "remark": "台塑石化", "replaceDiv": True},
        {"name": "3481.TW", "remark": "群創", "replaceDiv": True},
        {"name": "2303.TW", "remark": "聯電", "replaceDiv": True},
        {"name": "2308.TW", "remark": "台達電", "replaceDiv": True},
    ]
    report(symbols, start="1911-1-1", prefix="TW", iYear=5)

    symbols = [
        {"name": "VTI", "remark": "美股"},
        {"name": "VBR", "remark": "美小型價值股"},
        {"name": "VPL", "remark": "太平洋股"},
        {"name": "VGK", "remark": "歐股"},
        {"name": "VWO", "remark": "新興市場股"},
        {"name": "BND", "remark": "美債"},
        {"name": "BNDX", "remark": "國際債排美"},
        {"name": "BWX", "remark": "國際債排美"},
        {"name": "VNQ", "remark": "美房地產"},
    ]
    report(symbols, prefix="US")

    symbols = [
        # {"name": "00646.TW", "remark": "元大S&P 500", "replaceDiv": True},
        {"name": "VOO", "remark": "Vanguard S&P 500"},
        {"name": "0050.TW", "remark": "元大台灣50", "replaceDiv": True},
    ]
    report(symbols, start="1911-1-1", prefix="Mix", iYear=5)
