import os
import yfinance as yf
import pandas as pd
import time

from datetime import datetime
from pyquery import PyQuery
from dateutil.relativedelta import relativedelta
from flask import render_template
from FFI import rust_lib

from flask import Flask
import json
import plotly

app = Flask(__name__)


class Stock:
    start = datetime.strptime("1970-01-02", "%Y-%m-%d")
    end = datetime.now()
    history = None
    yfinance = None
    rawData = None
    name_width = 7

    def __init__(
        self,
        symbol,
        remark="",
        start=None,
        end=None,
        extraDiv={},
        replaceDiv=False,
        fromPath="",
        dateDuplcatedCombine=False,
        name_width=7,
        daily_return_mul=None,
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
        self.name_width = name_width
        if start:
            self.start = datetime.strptime(start, "%Y-%m-%d")
        if end:
            self.end = datetime.strptime(end, "%Y-%m-%d")

        self.extraDiv = extraDiv
        self.replaceDiv = self._getDiv_TW() if replaceDiv else {}
        self.daily_return_mul = daily_return_mul

        self.dateDuplcatedCombine = dateDuplcatedCombine
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

        df = pd.concat(dfs, ignore_index=True).copy()
        df.loc[:, "Date"] = pd.to_datetime(df["Date"], format="%Y-%m-%d")
        # 去掉 0
        df = df[df["Adj Close"] != 0]

        assert set(["Date", "Close", "Adj Close", "Dividends", "Stock Splits"]).issubset(df.columns)

        df = df.infer_objects().set_index("Date")

        return df

    def _getHistory(self, fromPath):
        if self.history:
            return self.history

        if fromPath:
            hist = self._getData(fromPath)
        else:
            n = 0
            while n < 60:
                self.yfinance = yf.Ticker(self.symbol)
                hist = self.yfinance.history(
                    start="1970-01-02", end=datetime.now(), auto_adjust=False
                )
                try:
                    assert set(
                        ["Date", "Close", "Adj Close", "Dividends", "Stock Splits"]
                    ).issubset(hist.reset_index().columns)
                    break
                except AssertionError:
                    time.sleep(60)
                    n += 1
                    continue
            if n >= 60:
                print(self.symbol)
                print(hist.reset_index().columns)
                assert set(["Date", "Close", "Adj Close", "Dividends", "Stock Splits"]).issubset(
                    hist.reset_index().columns
                )

        # 檢查 date 是否重覆
        df = hist[hist.index.duplicated(keep=False)]
        if not df.empty:
            print(self.name, df)
            if not self.dateDuplcatedCombine:
                assert not hist.index.has_duplicates
            else:
                hist = hist.groupby(level=0).sum()

        # 去掉 0
        hist = hist[hist["Close"] != 0]

        data = self._calAdjClose(hist)
        if self.daily_return_mul:
            data = self._adj_hist_by_daily_return_mul(data)
            self.symbol = f"{self.symbol}x{self.daily_return_mul}"

        self.rawData = data
        index = (self.start.date() <= data["Date"].dt.date) & (
            data["Date"].dt.date <= self.end.date()
        )
        data = data[index]

        return data

    def _calAdjClose(self, df):
        div = df[["Dividends"]].copy()
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

        data = df.reset_index().copy()
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

    def _adj_hist_by_daily_return_mul(self, df):
        result = df.copy()
        result.loc[0, "Adj Close Cal"] = result.loc[0, "Close"]
        for i in range(1, len(df["Adj Close Cal"])):
            day_return = (result.loc[i, "Close"] - result.loc[i-1, "Close"]) / result.loc[i-1, "Close"]
            result.loc[i, "Adj Close Cal"] = result.loc[i-1, "Adj Close Cal"] * (1 + day_return * self.daily_return_mul)

        return result

    @property
    def name(self):
        symbol = self.symbol.replace(".TW", "")
        if self.remark:
            return f"{symbol:{self.name_width}s} {self.remark}"
        else:
            return symbol

    @property
    def yearReturn(self):
        data = self.history

        first = data.iloc[0]["Adj Close Cal"]
        years = data.Date.dt.year.drop_duplicates()
        yearReturn = {}
        for y in years:
            yearData = data[data.Date.dt.year == y]
            end = yearData.iloc[-1]["Adj Close Cal"]
            yearReturn[y] = (end - first) / first * 100
            first = end

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


class Figure:
    default_layout = {
        "height": 600,
        # "autosize": False,
        "title": {"font": {"family": "Times New Roman"}, "x": 0.05, "y": 0.9},
        "font": {"family": "Courier New", "color": "#ffffff"},
        "xaxis": {"tickfont": {"family": "Courier New", "size": 14}, "automargin": True},
        "yaxis": {"tickfont": {"family": "Courier New"}, "automargin": True},
        "plot_bgcolor": "#000",
        "paper_bgcolor": "#000",
    }
    name_width = 7

    def __init__(
        self,
        symbols,
        start="1970-01-02",
        end=datetime.now().strftime("%Y-%m-%d"),
        prefix="",
        iYear=5,
        image=False,
        name_width=7,
    ):
        self.symbols = symbols
        self.start = start
        self.end = end
        self.prefix = prefix
        self.iYear = iYear
        self.image = image
        self.name_width = name_width

        if datetime.strptime(end, "%Y-%m-%d") < datetime.strptime(
            start, "%Y-%m-%d"
        ) + relativedelta(years=iYear):
            raise ValueError(f"{start} - {end} 間隔時間小於 iYear:{iYear}")

        self.stocks = []
        for symbol in symbols:
            self.stocks.append(
                Stock(
                    symbol["name"],
                    remark=symbol["remark"],
                    start=start,
                    end=end,
                    extraDiv=symbol.get("extraDiv", {}),
                    replaceDiv=symbol.get("replaceDiv", False),
                    fromPath=symbol.get("fromPath", False),
                    dateDuplcatedCombine=symbol.get("dateDuplcatedCombine", False),
                    name_width=name_width,
                    daily_return_mul=symbol.get("daily_return_mul", None)
                )
            )

    def _mergeDict(self, a, b, path=None, overwrite=False):
        "merges b into a"
        if path is None:
            path = []
        for key in b:
            if key in a:
                if isinstance(a[key], dict) and isinstance(b[key], dict):
                    self._mergeDict(a[key], b[key], path + [str(key)])
                elif a[key] == b[key]:
                    pass  # same leaf value
                else:
                    if overwrite:
                        a[key] = b[key]

                    # raise Exception("Conflict at %s" % ".".join(path + [str(key)]))
            else:
                a[key] = b[key]
        return a

    def _plotBar(self, df, title=None):
        dataList = []
        for symbol, data in df.items():
            data = {"type": "bar", "name": symbol, "x": data.index, "y": data}
            dataList.append(data)

        layout = {
            # "title": {"text": title, "font": {"family": "Times New Roman"}},
            # "font": {"family": "Courier New"},
            "title": {"text": title},
            "barmode": "group",
            "hovermode": "x",
        }
        layout = self._mergeDict(layout, self.default_layout)

        graph = {"data": dataList, "layout": layout}

        # 序列化
        return json.dumps(graph, cls=plotly.utils.PlotlyJSONEncoder)

    def _plotArea(self, df, title=None):
        dataList = []
        for symbol, data in df.items():
            data = {
                "type": "scatter",
                "name": symbol,
                "x": data.index,
                "y": data,
                "fill": "tozeroy",
                "mode": "none",
            }
            dataList.append(data)

        layout = {
            # "title": {"text": title, "font": {"family": "Times New Roman"}},
            "title": {"text": title},
            # "font": {"family": "Courier New"},
            "xaxis": {"title": "End Date"},
            "hovermode": "x",
        }
        layout = self._mergeDict(layout, self.default_layout)

        graph = {"data": dataList, "layout": layout}

        # 序列化
        return json.dumps(graph, cls=plotly.utils.PlotlyJSONEncoder)

    def _plotViolin(self, df, title=None):
        dataList = []
        for symbol, data in df.items():
            data = {
                "type": "violin",
                "name": symbol,
                "y": data,
                "box": {"visible": True},
                "meanline": {"visible": True},
            }
            dataList.append(data)

        layout = {
            # "title": {"text": title, "font": {"family": "Times New Roman"}},
            "title": {"text": title},
            # "font": {"family": "Courier New"},
            "xaxis": {
                # "tickfont": {"family": "Courier New", "size": 14},
                "tickangle": 90
            },
        }
        layout = self._mergeDict(layout, self.default_layout)

        graph = {"data": dataList, "layout": layout}

        # 序列化
        return json.dumps(graph, cls=plotly.utils.PlotlyJSONEncoder)

    def _plotHeatmap(self, df, title=None, range_color=[-1, 1]):
        data = {
            "type": "heatmap",
            "z": df.values.tolist(),
            "x": df.index,
            "y": df.columns,
            "zmin": range_color[0],
            "zmax": range_color[1],
            "colorbar": {"x": -0.2},
        }

        layout = {
            # "title": {"text": title, "font": {"family": "Times New Roman"}},
            # "font": {"family": "Courier New"},
            "title": {"text": title, "y": 0.05},
            # "autosize": False,
            "xaxis": {
                "side": "top",
                # "tickfont": {"family": "Courier New"},
                "tickangle": -90,
                # "automargin": True,
                "gridcolor": "rgba(0, 0, 0, 0)",
            },
            "yaxis": {
                "side": "right",
                # "tickfont": {"family": "Courier New"},
                # "automargin": True,
                # "scaleanchor": "x",
                "gridcolor": "rgba(0, 0, 0, 0)",
            },
        }
        layout = self._mergeDict(layout, self.default_layout)

        graph = {"data": [data], "layout": layout}

        # 序列化
        return json.dumps(graph, cls=plotly.utils.PlotlyJSONEncoder)

    def _plotBox(self, df, title=None):
        dataList = []
        for symbol, data in df.groupby(level=0):
            data = data.dropna(axis=1)
            data = {
                "type": "box",
                "name": symbol,
                "x": data.columns,
                "q1": data.loc[:, "25%", :].values[0],
                "median": data.loc[:, "50%", :].values[0],
                "q3": data.loc[:, "75%", :].values[0],
                "lowerfence": data.loc[:, "min", :].values[0],
                "upperfence": data.loc[:, "max", :].values[0],
                "mean": data.loc[:, "mean", :].values[0],
                "sd": data.loc[:, "std", :].values[0],
            }
            dataList.append(data)

        layout = {
            # "title": {"text": title, "font": {"family": "Times New Roman"}},
            # "font": {"family": "Courier New"},
            "title": {"text": title},
            "boxmode": "group",
            "hovermode": "x",
        }
        layout = self._mergeDict(layout, self.default_layout)

        graph = {"data": dataList, "layout": layout}

        # 序列化
        return json.dumps(graph, cls=plotly.utils.PlotlyJSONEncoder)

    def annual_return(self):
        data = []
        start = self.stocks[0].rawData["Date"].iloc[0].tz_localize(None)
        end = self.stocks[0].rawData["Date"].iloc[-1].tz_localize(None)
        for st in self.stocks:
            data.append(st.yearReturn)
            start = min(start, st.rawData["Date"].iloc[0].tz_localize(None))
            end = max(end, st.rawData["Date"].iloc[-1].tz_localize(None))

        df = pd.concat(data, axis=1)
        start = start.strftime("%Y/%m/%d");
        end = end.strftime("%Y/%m/%d");
        return self._plotBar(df, title=f"<b>Annual Return<b><br><i>{start} ~ {end}<i>")

    def total_return(self):
        # 只取交集時間故無法直接套用 stock.totalReturn
        data = []
        for st in self.stocks:
            s = pd.Series(
                data=st.history["Adj Close Cal"].to_numpy(),
                index=st.history["Date"].dt.tz_localize(None).to_numpy(),
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
        df = df.T  # for df.items()

        return self._plotBar(df, title=f"<b>Total Return<b><br><i>{start} ~ {end}<i>")

    def roll_back(self):
        # =========================================================================
        # area
        data = []
        for st in self.stocks:
            data.append(st.rollback(self.iYear))

        df = pd.concat(data, axis=1)
        start = df.index[0] - pd.DateOffset(years=self.iYear)
        end = df.index[-1]

        area = self._plotArea(df, title=(
                                        f"<b>{self.iYear} Years Roll Back<b><br>"
                                        f"Start: <i>{start.strftime('%Y/%m/%d')} ~"
                                        f" {(end-relativedelta(years=self.iYear)).strftime('%Y/%m/%d')}<i><br>"
                                        f"End  : <i>{(start+relativedelta(years=self.iYear)).strftime('%Y/%m/%d')} ~"
                                        f" {end.strftime('%Y/%m/%d')}<i>"))

        # =========================================================================
        # violin
        # 只取交集時間
        df = df.dropna()
        if df.index.empty:
            raise ValueError("無交集時間")

        start = df.index[0] - pd.DateOffset(years=self.iYear)
        end = df.index[-1]
        violin = self._plotViolin(
            df,
            title=(
                f"<b>{self.iYear} Years Roll Back<b><br>"
                f"Start: <i>{start.strftime('%Y/%m/%d')} ~"
                f" {(end-relativedelta(years=self.iYear)).strftime('%Y/%m/%d')}<i><br>"
                f"End  : <i>{(start+relativedelta(years=self.iYear)).strftime('%Y/%m/%d')} ~"
                f" {end.strftime('%Y/%m/%d')}<i>"
            ),
        )

        return area, violin

    def correlation(self):
        # =========================================================================
        # close
        data = []
        for st in self.stocks:
            s = pd.Series(
                data=st.rawData["Close"].to_numpy(),
                index=st.rawData["Date"].dt.tz_localize(None).to_numpy(),
                name=st.name,
            )
            data.append(s)

        df = pd.concat(data, axis=1)
        start = df.dropna().index[0].strftime("%Y/%m/%d")
        end = df.dropna().index[-1].strftime("%Y/%m/%d")
        close = self._plotHeatmap(df.corr(), title=f"<b>Correlation of Close<b><br><i>{start} ~ {end}<i>")

        # =========================================================================
        # closeAdj
        data = []
        for st in self.stocks:
            s = pd.Series(
                data=st.rawData["Adj Close Cal"].to_numpy(),
                index=st.rawData["Date"].dt.tz_localize(None).to_numpy(),
                name=st.name,
            )
            data.append(s)

        df = pd.concat(data, axis=1)
        start = df.dropna().index[0].strftime("%Y/%m/%d")
        end = df.dropna().index[-1].strftime("%Y/%m/%d")
        closeAdj = self._plotHeatmap(df.corr(), title=f"<b>Correlation of Adj Close <b><br><i>{start} ~ {end}<i>")

        return close, closeAdj

    def active_vs_passive(self):
        # =========================================================================
        # year
        data = {}

        for st in self.stocks:
            df = st.history
            date = df["Date"].dt.tz_localize(None)
            df = df.drop(["Date"], axis=1)
            df["Date"] = date
            df = df.set_index("Date")
            if "Open" not in df.columns:
                df["Open"] = 0
            if "High" not in df.columns:
                df["High"] = 0
            if "Low" not in df.columns:
                df["Low"] = 0
            if "Volume" not in df.columns:
                df["Volume"] = 0
            df = df[["Open", "High", "Low", "Close", "Adj Close Cal", "Volume"]]
            df = df.rename({"Adj Close Cal": "CloseAdj"}, axis="columns")
            data[st.name] = df

        df = pd.concat(data, axis=1)
        start = df.index[0].strftime("%Y/%m/%d")
        end = df.index[-1].strftime("%Y/%m/%d")

        data_stat_year = {}
        for st in self.stocks:
            df1 = df[st.name].copy()
            df1 = df1.dropna()
            df1.loc[:, "Volume"] = df1["Volume"].astype(int)

            with rust_lib.Stock(df1) as stock:
                activeYear = stock.stat_active_year()
                holdYear = stock.stat_hold_year()

            data_stat_year[f"{st.symbol:{self.name_width}s} A {st.remark}"] = activeYear * 100
            data_stat_year[f"{st.symbol:{self.name_width}s} P {st.remark}"] = holdYear * 100

        data_stat_year = pd.concat(data_stat_year)

        annual_return = self._plotBox(
            data_stat_year, title=(f"<b>Annual Return Active vs Passive<b><br><i>{start} ~ {end}<i>")
        )

        # =========================================================================
        # all
        # 只取交集時間
        df = df.dropna()
        if df.index.empty:
            raise ValueError("無交集時間")

        data_stat_all = {}
        for st in self.stocks:
            df1 = df[st.name].copy()
            df1.loc[:, "Volume"] = df1["Volume"].astype(int)

            with rust_lib.Stock(df1) as stock:
                activeAll = stock.stat_active_all()
                holdAll = stock.stat_hold_all()

            data_stat_all[f"{st.symbol:{self.name_width}s} A {st.remark}"] = activeAll * 100
            data_stat_all[f"{st.symbol:{self.name_width}s} P {st.remark}"] = holdAll * 100

        data_stat_all = pd.concat(data_stat_all)

        start = df.index[0]
        end = df.index[-1]
        total_return = self._plotBox(
            data_stat_all,
            title=(
                f"<b>Total Return Active vs Passive<b><br>"
                f"<i>{start.strftime('%Y/%m/%d')} ~"
                f" {end.strftime('%Y/%m/%d')}<i>"
            ),
        )

        return total_return, annual_return


def report(
    symbols,
    start="1970-01-02",
    end=datetime.now().strftime("%Y-%m-%d"),
    prefix="",
    iYear=5,
    image=False,
    path=os.path.join(os.path.dirname(__file__), "report"),
    name_width=7,
):
    os.makedirs(path, exist_ok=True)
    plots = {}
    fig = Figure(
        symbols,
        start=start,
        end=end,
        prefix=prefix,
        iYear=iYear,
        image=image,
        name_width=name_width,
    )

    plots["totalReturn"] = fig.total_return()
    plots["totalReturnStatic"], plots["annualReturnStatic"] = fig.active_vs_passive()
    plots["annualReturn"] = fig.annual_return()
    plots["rollBack"], plots["rollBackVolin"] = fig.roll_back()
    plots["correlationClose"], plots["correlationAdjClose"] = fig.correlation()

    with app.app_context():
        html = render_template("compare.html", plots=plots, title=f"{prefix} Report")

    with open(os.path.join(path, f"{prefix}_Report.html"), "w", encoding="UTF-8") as f:
        f.write(html)


if __name__ == "__main__":
    symbols = [
        {"name": "^TWII", "remark": "臺灣加權指數"},
        {
            "name": "^TWII",
            "remark": "臺灣加權指數正2",
            "daily_return_mul": 2,
        },
        {
            "name": "^TAIEX",
            "remark": "臺灣加權報酬指數",
            "fromPath": os.path.join(os.path.dirname(__file__), "extraData", "臺灣加權股價指數"),
        },
        {
            "name": "^TAI50I",
            "remark": "臺灣50報酬指數",
            "fromPath": os.path.join(os.path.dirname(__file__), "extraData", "臺灣50指數"),
        },
        {
            "name": "^TAI50I",
            "remark": "臺灣50正2",
            "fromPath": os.path.join(os.path.dirname(__file__), "extraData", "臺灣50指數"),
            "daily_return_mul": 2,
        },
        {
            "name": "^TAI100I",
            "remark": "臺灣中型100報酬指數",
            "fromPath": os.path.join(os.path.dirname(__file__), "extraData", "臺灣中型100指數"),
        },
        {
            "name": "^TAI100I",
            "remark": "臺灣中型100正2",
            "fromPath": os.path.join(os.path.dirname(__file__), "extraData", "臺灣中型100指數"),
            "daily_return_mul": 2,
        },
        {
            "name": "^TAIDIVIDI",
            "remark": "臺灣高股息報酬指數",
            "fromPath": os.path.join(os.path.dirname(__file__), "extraData", "臺灣高股息指數"),
        },
        {
            "name": "^TAIDIVIDI",
            "remark": "臺灣高股息報酬指數正2",
            "fromPath": os.path.join(os.path.dirname(__file__), "extraData", "臺灣高股息指數"),
            "daily_return_mul": 2,
        },
        {"name": "0050.TW", "remark": "元大臺灣50", "replaceDiv": True},
        {"name": "00631L.TW", "remark": "元大台灣50正2", "replaceDiv": True},
        {"name": "006208.TW", "remark": "富邦台50", "replaceDiv": True},
        {"name": "0051.TW", "remark": "元大中型100", "replaceDiv": True},
        {"name": "006204.TW", "remark": "永豐臺灣加權", "replaceDiv": True},
        {"name": "0056.TW", "remark": "元大高股息", "replaceDiv": True},
        {"name": "2412.TW", "remark": "中華電信", "replaceDiv": True},
        {"name": "2002.TW", "remark": "中鋼", "replaceDiv": True},
        {"name": "2330.TW", "remark": "台積電", "replaceDiv": True, "dateDuplcatedCombine": True},
        {"name": "2317.TW", "remark": "鴻海", "replaceDiv": True},
        {"name": "6505.TW", "remark": "台塑石化", "replaceDiv": True},
        {"name": "3481.TW", "remark": "群創", "replaceDiv": True},
        {"name": "2303.TW", "remark": "聯電", "replaceDiv": True},
        {"name": "2308.TW", "remark": "台達電", "replaceDiv": True},
        {"name": "2454.TW", "remark": "聯發科", "replaceDiv": True},
    ]
    report(symbols, start="1911-1-1", prefix="TW", iYear=5, name_width=12)

    symbols = [
        {"name": "VTI", "remark": "美股"},
        {"name": "VTI", "remark": "美股正2", "daily_return_mul": 2},
        {"name": "VBR", "remark": "美小型價值股"},
        {"name": "VEA", "remark": "歐太平洋股"},
        {"name": "VEA", "remark": "歐太平洋股正2", "daily_return_mul": 2},
        {"name": "VPL", "remark": "太平洋股"},
        {"name": "VPL", "remark": "太平洋股正2", "daily_return_mul": 2},
        {"name": "VGK", "remark": "歐股"},
        {"name": "VGK", "remark": "歐股正2", "daily_return_mul": 2},
        {"name": "VWO", "remark": "新興市場股"},
        {"name": "VWO", "remark": "新興市場股正2", "daily_return_mul": 2},
        {"name": "VXUS", "remark": "國際大中小型股排美"},
        {"name": "VXUS", "remark": "國際大中小型股排美正2", "daily_return_mul": 2},
        {"name": "VEU", "remark": "國際大中型股排美"},
        {"name": "VEU", "remark": "國際大中型股排美正2", "daily_return_mul": 2},
        {"name": "BND", "remark": "美債"},
        {"name": "BND", "remark": "美債正2", "daily_return_mul": 2},
        {"name": "BNDX", "remark": "國際債排美"},
        {"name": "BNDX", "remark": "國際債排美正2", "daily_return_mul": 2},
        {"name": "BWX", "remark": "國際債排美"},
        {"name": "BWX", "remark": "國際債排美正2", "daily_return_mul": 2},
        {"name": "VNQ", "remark": "美房地產"},
        {"name": "VNQ", "remark": "美房地產正2", "daily_return_mul": 2},
    ]
    report(symbols, prefix="US", name_width=6)

    # symbols = [
        # # {"name": "00646.TW", "remark": "元大S&P 500", "replaceDiv": True},
        # {"name": "VOO", "remark": "Vanguard S&P 500"},
        # {"name": "0050.TW", "remark": "元大臺灣50", "replaceDiv": True},
    # ]
    # report(symbols, start="1911-1-1", prefix="Mix", iYear=5)
