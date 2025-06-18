import copy
import os
import re
import yfinance as yf
import pandas as pd
import time
import json
import plotly

from datetime import datetime
from pyxirr import xirr
from pyquery import PyQuery
from dateutil.relativedelta import relativedelta
from flask import render_template
from flask import Flask

from FFI import rust_pyo3

app = Flask(__name__)

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": "https://www.google.com/",
}


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
        groups,
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
        self.groups = groups
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
        self.rollback_map = {}

    def _getDiv_TW(self):
        try:
            dom = PyQuery(
                url=f"https://tw.stock.yahoo.com/quote/{self.symbol}/dividend",
                headers=headers,
            )
            data = dom(r"#layout-col1 ul.List\(n\)")

            replaceDiv = {}
            for i in data.find(r"li.List\(n\)").items():
                date = i.find("div > div:nth-child(7)").text()
                if re.search(r"\d{4}/\d{2}/\d{2}", date) is None:
                    continue

                date += " 00:00:00+08:00"
                div = float(i.find("div > div:nth-child(3)").text())
                replaceDiv[date] = div
        except Exception as e:
            print(e)
            return {}

        print("replaceDiv:", replaceDiv)
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
        # 去掉非數字
        df = df[pd.to_numeric(df["Adj Close"], errors="coerce").notnull()]

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
            while n <= 5:
                self.yfinance = yf.Ticker(self.symbol)
                hist = self.yfinance.history(
                    start="1970-01-02", end=datetime.now(), auto_adjust=False
                )
                try:
                    assert set(
                        ["Date", "Close", "Adj Close", "Dividends", "Stock Splits"]
                    ).issubset(hist.reset_index().columns)
                    if self.remark == "":
                        info = self.yfinance.info
                        self.remark = info["longName"] + " type:" + info["typeDisp"]
                    break
                except AssertionError:
                    time.sleep(60)
                    n += 1
                    continue
            else:
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
                hist = hist.groupby(level=0, sort=False).sum()

        # 去掉 0
        hist = hist[hist["Close"] != 0]
        # 去掉非數字
        hist = hist[pd.to_numeric(hist["Close"], errors="coerce").notnull()]

        data = self._calAdjClose(hist)
        # data.to_csv(self.symbol + ".csv")
        if self.daily_return_mul:
            data = self._adj_hist_by_daily_return_mul(data)
            self.symbol = f"{self.symbol}x{self.daily_return_mul}"

        # remove timezone
        data["Date"] = data["Date"].dt.tz_localize(None)

        self.rawData = data
        index = (self.start.date() <= data["Date"].dt.date) & (
            data["Date"].dt.date <= self.end.date()
        )
        data = data[index]
        self.start = data.iloc[0]["Date"]
        self.end = data.iloc[-1]["Date"]

        return data

    def set_end_datetime(self, end):
        self.end = end
        index = self.history["Date"].dt.date <= self.end.date()
        self.history = self.history[index]

    def set_start_datetime(self, start):
        self.start = start
        index = self.start.date() <= self.history["Date"].dt.date
        self.history = self.history[index]

    def _calAdjClose(self, df):
        div = df[["Dividends"]].copy()

        if self.replaceDiv:
            div.loc[:, "Dividends"] = 0
        div = div[div["Dividends"] != 0]

        for date, divVal in self.extraDiv.items():
            dt = datetime.strptime(date, "%Y/%m/%d")
            div.loc[dt, "Dividends"] = divVal

        for date, divVal in self.replaceDiv.items():
            dt = datetime.strptime(date, "%Y/%m/%d %H:%M:%S%z")
            div.loc[dt, "Dividends"] = divVal

        div = div.reset_index()

        split = df[["Stock Splits"]].copy()
        split = split[split["Stock Splits"] != 0]
        split = split.reset_index()

        print(self.name)

        data = df.reset_index().copy()
        if div.empty:
            print("empty Dividends, so fill out 'Adj Close Cal' by 'Adj Close'")
            data.loc[:, "Adj Close Cal"] = data["Adj Close"]
            return data.sort_values("Date")

        print(split)

        data.loc[:, "Adj Close Cal"] = 0.0
        data.loc[:, "Adj Ratio"] = 1.0

        # yahoo 已先做過 split 的價格調整，所以股息也要調整，ratio 才會對
        div.loc[:, "Adj Dividends"] = div.loc[:, "Dividends"]
        for i, row in split.iterrows():
            splitDate = row.Date
            splitVal = 1 / row["Stock Splits"]
            if div["Date"][div["Date"] >= splitDate].empty:
                continue
            index = div["Date"] < splitDate
            if index.any():
                div.loc[index, "Adj Dividends"] = div.loc[index, "Dividends"] * splitVal
        print(div)

        for i, row in div.iterrows():
            divDate = row.Date
            divVal = row["Adj Dividends"]
            if data["Date"][data["Date"] >= divDate].empty:
                continue
            index = data["Date"] < divDate
            if index.any():
                data.loc[index, "Adj Ratio"] *= 1 - divVal / data.loc[index, "Close"].iloc[-1]

        data.loc[:, "Adj Close Cal"] = data.loc[:, "Close"] * data.loc[:, "Adj Ratio"]

        return data.sort_values("Date")

    def _adj_hist_by_daily_return_mul(self, df):
        result = df.copy()
        result["Adj Close Cal"].iat[0] = df["Adj Close Cal"].iat[0]
        for i in range(1, len(df["Adj Close Cal"])):
            day_return = (df["Adj Close Cal"].iat[i] - df["Adj Close Cal"].iat[i - 1]) / df[
                "Adj Close Cal"
            ].iat[i - 1]
            result["Adj Close Cal"].iat[i] = result["Adj Close Cal"].iat[i - 1] * (
                1 + day_return * self.daily_return_mul
            )
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
            yearReturn[y] = (end - first) / first
            first = end

        df = pd.DataFrame(yearReturn, index=[self.name])

        return df.T

    @property
    def totalReturn(self):
        data = self.history

        first = data.iloc[0]["Adj Close Cal"]
        end = data.iloc[-1]["Adj Close Cal"]
        totalReturn = (end - first) / first

        return totalReturn

    @property
    def dailyReturn(self):
        data = self.history

        pre = data.iloc[:-1].reset_index()
        cur = data.iloc[1:].reset_index()
        day_return = (cur["Adj Close Cal"] - pre["Adj Close Cal"]) / pre["Adj Close Cal"]
        day_return = day_return.to_frame("Return")
        day_return["Start"] = pre["Date"]
        day_return["End"] = cur["Date"]

        return day_return

    def rollback(self, iYear):
        if self.rollback_map.get(iYear) is not None:
            return self.rollback_map[iYear]

        start = self.history["Date"].iloc[0]
        end = self.history["Date"].iloc[-1]
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
        r = [(p[1]["Adj Close Cal"] - p[0]["Adj Close Cal"]) / p[0]["Adj Close Cal"] for p in pairs]

        df = pd.DataFrame({self.name: r}, index=t)

        rollback = df.sort_index()
        self.rollback_map[iYear] = rollback

        return rollback


class Figure:
    theme_template = plotly.io.templates["plotly_dark"].to_plotly_json()
    default_template = {
        "layout": {
            "height": 600,
            # "autosize": False,
            "title": {"font": {"family": "Times New Roman"}, "x": 0.05, "y": 0.9},
            "font": {"family": "Courier New"},
            "xaxis": {
                "tickfont": {"family": "Courier New", "size": 14},
                "automargin": True,
            },
            "yaxis": {
                "tickfont": {"family": "Courier New"},
                "automargin": True,
            },
        }
    }
    name_width = 7

    intersection_history_val = None
    total_return_val = None

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
        self.default_template = self._mergeDict(self.theme_template, self.default_template)

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
            # try:
            self.stocks.append(
                Stock(
                    symbol["name"],
                    remark=symbol.get("remark", ""),
                    groups=symbol["groups"],
                    start=start,
                    end=end,
                    extraDiv=symbol.get("extraDiv", {}),
                    replaceDiv=symbol.get("replaceDiv", False),
                    fromPath=symbol.get("fromPath", False),
                    dateDuplcatedCombine=symbol.get("dateDuplcatedCombine", False),
                    name_width=name_width,
                    daily_return_mul=symbol.get("daily_return_mul", None),
                )
            )
            # except Exception as error:
            #     print(f"{symbol} can not be created, it seems something wrong {error}")

        end = min([stock.end for stock in self.stocks])
        for stock in self.stocks:
            stock.set_end_datetime(end)

    def _mergeDict(self, a, b, path=None, overwrite=True):
        """Merges b into a. If overwrite is True, b's values will overwrite a's on conflict."""
        if path is None:
            path = []
        for key in b:
            if key in a:
                if isinstance(a[key], dict) and isinstance(b[key], dict):
                    self._mergeDict(a[key], b[key], path + [str(key)], overwrite)
                elif a[key] != b[key] and overwrite:
                    a[key] = b[key]
                elif a[key] == b[key]:
                    pass  # same leaf value
                elif not overwrite:
                    print(
                        f"Conflict at {'.'.join(path + [str(key)])} and overwrite is False. Keeping original value."
                    )
            else:
                a[key] = b[key]
        return a

    def _group_button(self, symbols):
        symbol_map = {}
        for s in self.stocks:
            symbol_map[s.name] = s.groups

        group_map = {}
        for i, symbol in enumerate(symbols):
            symbol = symbol.removeprefix("A ").removeprefix("P ")
            for group in symbol_map[symbol]:
                arr = group_map.get(group, [False] * len(symbols))
                arr[i] = True
                group_map[group] = arr

        # visible 兩種寫法
        # 提供指定動作的 index
        # "args": [{"visible": True}, arr]
        # 提供所有設定
        # "args": [{"visible": ["legendonly"] * len(symbols)}],
        buttons = [
            {
                "args": [{"visible": [True] * len(symbols)}],
                "args2": [{"visible": ["legendonly"] * len(symbols)}],
                "label": "All",
                "method": "restyle",
            },
        ]

        for group, arr in group_map.items():
            buttons.append(
                {
                    "args": [{"visible": arr}],
                    "args2": [{"visible": [not elem for elem in arr]}],
                    "label": group,
                    "method": "restyle",
                },
            )

        updatemenus = [
            {
                "x": 0.51,
                "y": 1.05,
                "xanchor": "center",
                "yanchor": "bottom",
                "pad": {"r": 10, "t": 10},
                "buttons": buttons,
                "type": "dropdown",
                "direction": "right",
                "font": {"color": "#AAAAAA"},
            }
        ]

        return updatemenus

    def _plotBar_without_group(self, df, title=None):
        dataList = []
        symbols = []
        for symbol, data in df.items():
            data = {"type": "bar", "name": symbol, "x": [symbol], "y": data}
            dataList.append(data)
            symbols.append(symbol)

        layout = {
            # "title": {"text": title, "font": {"family": "Times New Roman"}},
            # "font": {"family": "Courier New"},
            "title": {"text": title},
            "hovermode": "x",
            "updatemenus": self._group_button(symbols),
        }

        graph = {"data": dataList, "layout": layout}
        graph = self._mergeDict(copy.deepcopy(self.default_template), graph)

        # 序列化
        return json.dumps(graph, cls=plotly.utils.PlotlyJSONEncoder)

    def _plotBar_with_group(self, df, title=None):
        dataList = []
        symbols = []
        for symbol, data in df.items():
            data = {"type": "bar", "name": symbol, "x": data.index, "y": data}
            dataList.append(data)
            symbols.append(symbol)

        layout = {
            # "title": {"text": title, "font": {"family": "Times New Roman"}},
            # "font": {"family": "Courier New"},
            "title": {"text": title},
            "barmode": "group",
            "hovermode": "x",
            "updatemenus": self._group_button(symbols),
        }

        graph = {"data": dataList, "layout": layout}
        graph = self._mergeDict(copy.deepcopy(self.default_template), graph)

        # 序列化
        return json.dumps(graph, cls=plotly.utils.PlotlyJSONEncoder)

    def _plotArea(self, df, title=None):
        dataList = []
        symbols = []
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
            symbols.append(symbol)

        layout = {
            # "title": {"text": title, "font": {"family": "Times New Roman"}},
            "title": {"text": title},
            # "font": {"family": "Courier New"},
            "xaxis": {"title": "End Date"},
            "hovermode": "x",
            "updatemenus": self._group_button(symbols),
        }

        graph = {"data": dataList, "layout": layout}
        graph = self._mergeDict(copy.deepcopy(self.default_template), graph)

        # 序列化
        return json.dumps(graph, cls=plotly.utils.PlotlyJSONEncoder)

    def _plotLine_without_markers(self, df, title=None):
        dataList = []
        symbols = []
        for symbol, data in df.items():
            data = {
                "type": "scatter",
                "name": symbol,
                "x": data.index,
                "y": data,
                "mode": "lines",
            }
            dataList.append(data)
            symbols.append(symbol)

        layout = {
            # "title": {"text": title, "font": {"family": "Times New Roman"}},
            "title": {"text": title},
            # "font": {"family": "Courier New"},
            "xaxis": {"title": "End Date"},
            "hovermode": "x",
            "updatemenus": self._group_button(symbols),
        }

        graph = {"data": dataList, "layout": layout}
        graph = self._mergeDict(copy.deepcopy(self.default_template), graph)

        # 序列化
        return json.dumps(graph, cls=plotly.utils.PlotlyJSONEncoder)

    def _plotViolin(self, df, title=None):
        dataList = []
        symbols = []
        for symbol, data in df.items():
            data = {
                "type": "violin",
                "name": symbol,
                "y": data,
                "box": {"visible": True},
                "meanline": {"visible": True},
            }
            dataList.append(data)
            symbols.append(symbol)

        layout = {
            # "title": {"text": title, "font": {"family": "Times New Roman"}},
            "title": {"text": title},
            # "font": {"family": "Courier New"},
            "xaxis": {
                # "tickfont": {"family": "Courier New", "size": 14},
                "tickangle": 90
            },
            "updatemenus": self._group_button(symbols),
        }

        graph = {"data": dataList, "layout": layout}
        graph = self._mergeDict(copy.deepcopy(self.default_template), graph)

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

        graph = {"data": [data], "layout": layout}
        graph = self._mergeDict(copy.deepcopy(self.default_template), graph)

        # 序列化
        return json.dumps(graph, cls=plotly.utils.PlotlyJSONEncoder)

    def _plotBox_without_group(self, df, title=None):
        dataList = []
        symbols = []
        for symbol, data in df.groupby(level=0, sort=False):
            data = data.dropna(axis=1)
            data = {
                "type": "box",
                "name": symbol,
                "x": [symbol],
                "q1": data.loc[:, "25%", :].values[0],
                "median": data.loc[:, "50%", :].values[0],
                "q3": data.loc[:, "75%", :].values[0],
                "lowerfence": data.loc[:, "min", :].values[0],
                "upperfence": data.loc[:, "max", :].values[0],
                "mean": data.loc[:, "mean", :].values[0],
                "sd": data.loc[:, "std", :].values[0],
            }
            dataList.append(data)
            symbols.append(symbol)

        layout = {
            # "title": {"text": title, "font": {"family": "Times New Roman"}},
            # "font": {"family": "Courier New"},
            "title": {"text": title},
            "hovermode": "x",
            "updatemenus": self._group_button(symbols),
        }

        graph = {"data": dataList, "layout": layout}
        graph = self._mergeDict(copy.deepcopy(self.default_template), graph)

        # 序列化
        return json.dumps(graph, cls=plotly.utils.PlotlyJSONEncoder)

    def _plotBox_with_group(self, df, title=None):
        dataList = []
        symbols = []
        for symbol, data in df.groupby(level=0, sort=False):
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
            symbols.append(symbol)

        layout = {
            # "title": {"text": title, "font": {"family": "Times New Roman"}},
            # "font": {"family": "Courier New"},
            "title": {"text": title},
            "boxmode": "group",
            "hovermode": "x",
            "updatemenus": self._group_button(symbols),
        }

        graph = {"data": dataList, "layout": layout}
        graph = self._mergeDict(copy.deepcopy(self.default_template), graph)

        # 序列化
        return json.dumps(graph, cls=plotly.utils.PlotlyJSONEncoder)

    def _plotDailyReturn(self, data):
        dataList = []
        buttons = []
        for i, (symbol, df_daily, df_rollback) in enumerate(data):
            start = df_daily["Start"].iat[0].strftime("%Y/%m/%d")
            end = df_daily["End"].iat[-1].strftime("%Y/%m/%d")
            df_daily = df_daily.sort_values(by=["Return"])

            x = df_daily.apply(
                lambda x: "{}~{}".format(
                    x["Start"].strftime("%Y/%m/%d"), x["End"].strftime("%Y/%m/%d")
                ),
                axis=1,
            )
            dailyreturn = {
                "type": "bar",
                "name": symbol,
                "x": x,
                "y": df_daily["Return"],
                "visible": i == 0,
            }

            histogram_dailyreturn = {
                "type": "histogram",
                "name": symbol,
                "x": df_daily["Return"],
                "visible": i == 0,
                "xaxis": "x2",
                "yaxis": "y2",
            }

            cumgains = df_daily["Return"].map(lambda x: 1 + x).cumprod() - 1.0
            missedgains = {
                "type": "bar",
                "name": symbol,
                "y": cumgains.iloc[-1:-102:-1],
                "visible": i == 0,
                "xaxis": "x3",
                "yaxis": "y3",
            }

            cumlosses = df_daily["Return"].iloc[::-1].map(lambda x: 1 + x).cumprod() - 1.0
            avoidedlosses = {
                "type": "bar",
                "name": symbol,
                "y": cumlosses.iloc[-1:-102:-1],
                "visible": i == 0,
                "xaxis": "x3",
                "yaxis": "y4",
            }

            histogram_rollback = {
                "type": "histogram",
                "name": symbol,
                "x": df_rollback,
                "visible": i == 0,
                "xaxis": "x5",
                "yaxis": "y5",
            }

            graphs = [
                missedgains,
                avoidedlosses,
                histogram_dailyreturn,
                histogram_rollback,
                dailyreturn,
            ]
            dataList.extend(graphs)

            graphs_num = len(graphs)
            visible = [False] * graphs_num * len(data)
            for j in range(graphs_num):
                visible[i * graphs_num + j] = True

            title = f"<b>Daily Return Analysis<b><br><i>{start} ~ {end}<i>"
            return_range_daily = [min(df_daily["Return"]) - 0.01, max(df_daily["Return"]) + 0.01]
            return_range_rollback = [min(df_rollback) - 0.01, max(df_rollback) + 0.01]
            if i == 0:
                title_init = title
                range_init_daily = return_range_daily
                range_init_rollback = return_range_rollback
            button = {
                "method": "update",
                "args": [
                    {"visible": visible},
                    {
                        "title.text": title,
                        "yaxis.range": return_range_daily,
                        "xaxis2.range": return_range_daily,
                        "xaxis5.range": return_range_rollback,
                    },
                ],
                "label": symbol,
            }
            buttons.append(button)

        layout = {
            "title": {
                "text": title_init,
                "x": None,
                "y": None,
            },
            "hovermode": "x",
            "height": "1300",
            "updatemenus": [
                {
                    "x": 0.6,
                    "y": 1.03,
                    "xanchor": "left",
                    "yanchor": "bottom",
                    "pad": {"r": 10, "t": 10},
                    "buttons": buttons,
                    "type": "dropdown",
                    "direction": "down",
                    "font": {"color": "#AAAAAA"},
                }
            ],
            "grid": {
                "rows": graphs_num,
                "columns": 1,
                "pattern": "independent",
                "subplots": [["x3y3"], ["x3y4"], ["x2y2"], ["x5y5"], ["xy"]],
            },
            "yaxis": {"tickformat": ".2%", "range": range_init_daily},
            "yaxis3": {"tickformat": ".2%"},
            "yaxis4": {"tickformat": ".2%"},
            "xaxis2": {"tickformat": ".2%", "range": range_init_daily},
            "xaxis5": {"tickformat": ".2%", "range": range_init_rollback},
            "showlegend": False,
            "annotations": [
                {
                    "text": "<b>Missed Gains<b>",
                    "font": {"size": 16},
                    "showarrow": False,
                    "xref": "x3 domain",
                    "yref": "y3 domain",
                    "x": 0.5,
                    "y": 0.95,
                    "xanchor": "center",
                    "yanchor": "bottom",
                },
                {
                    "text": "<b>Avoided Losses<b>",
                    "font": {"size": 16},
                    "showarrow": False,
                    "xref": "x3 domain",
                    "yref": "y4 domain",
                    "x": 0.5,
                    "y": 1.05,
                    "xanchor": "center",
                    "yanchor": "bottom",
                },
                {
                    "text": "<b>Daily Return Histogram<b>",
                    "font": {"size": 16},
                    "showarrow": False,
                    "xref": "x2 domain",
                    "yref": "y2 domain",
                    "x": 0.5,
                    "y": 1.05,
                    "xanchor": "center",
                    "yanchor": "bottom",
                },
                {
                    "text": f"<b>{self.iYear} Years Rollback Histogram<b>",
                    "font": {"size": 16},
                    "showarrow": False,
                    "xref": "x5 domain",
                    "yref": "y5 domain",
                    "x": 0.5,
                    "y": 1.05,
                    "xanchor": "center",
                    "yanchor": "bottom",
                },
                {
                    "text": "<b>Daily Return<b>",
                    "font": {"size": 16},
                    "showarrow": False,
                    "xref": "x domain",
                    "yref": "y domain",
                    "x": 0.5,
                    "y": 1.05,
                    "xanchor": "center",
                    "yanchor": "bottom",
                },
            ],
        }

        graph = {"data": dataList, "layout": layout}
        graph = self._mergeDict(copy.deepcopy(self.default_template), graph)

        axis_n = 0
        for key in graph["layout"].keys():
            if ("xaxis" in key or "yaxis" in key) and len(key) > 5:
                n = int(str.split(key, "axis", 1)[1])
                axis_n = max(axis_n, n)

        for i in range(2, axis_n + 1):
            key = f"xaxis{i}"
            graph["layout"][key] = self._mergeDict(
                graph["layout"].get(key, {}), self.default_template["layout"]["xaxis"]
            )
            key = f"yaxis{i}"
            graph["layout"][key] = self._mergeDict(
                graph["layout"].get(key, {}), self.default_template["layout"]["yaxis"]
            )

        # 序列化
        return json.dumps(graph, cls=plotly.utils.PlotlyJSONEncoder)

    def annual_return_bar(self):
        data = []
        start = self.stocks[0].history["Date"].iloc[0]
        end = self.stocks[0].history["Date"].iloc[-1]
        for st in self.stocks:
            data.append(st.yearReturn)
            start = min(start, st.history["Date"].iloc[0])
            end = max(end, st.history["Date"].iloc[-1])

        df = pd.concat(data, axis=1)
        start = start.strftime("%Y/%m/%d")
        end = end.strftime("%Y/%m/%d")
        graph = self._plotBar_with_group(df, title=f"<b>Annual Return<b><br><i>{start} ~ {end}<i>")
        graph = self._mergeDict(json.loads(graph), {"layout": {"yaxis": {"tickformat": ".2%"}}})
        graph = json.dumps(graph)

        return graph

    def intersection_history(self):
        if self.intersection_history_val is not None:
            return self.intersection_history_val

        data = []
        for st in self.stocks:
            s = pd.Series(
                data=st.history["Adj Close Cal"].to_numpy(),
                index=st.history["Date"].to_numpy(),
                name=st.name,
            )
            data.append(s)

        df = pd.concat(data, axis=1)
        df = df.dropna()

        self.intersection_history_val = df

        return df

    def total_return(self):
        if self.total_return_val is not None:
            return self.total_return_val

        # 只取交集時間故無法直接套用 stock.totalReturn
        df = self.intersection_history()
        start = df.index[0]
        end = df.index[-1]

        df = (df.iloc[-1, :] - df.iloc[0, :]) / df.iloc[0, :]

        df = df.to_frame().T

        self.total_return_val = (start, end, df)

        return start, end, df

    def total_return_bar(self):
        start, end, df = self.total_return()
        start = start.strftime("%Y/%m/%d")
        end = end.strftime("%Y/%m/%d")

        graph = self._plotBar_without_group(
            df, title=f"<b>Total Return<b><br><i>{start} ~ {end}<i>"
        )
        graph = self._mergeDict(json.loads(graph), {"layout": {"yaxis": {"tickformat": ".2%"}}})
        graph = json.dumps(graph)
        return graph

    def irr_bar(self):
        start, end, df = self.total_return()
        year = pd.Timedelta(end - start).days / 365.0
        df = df.map(lambda x: ((1 + x) ** (1 / year) - 1))
        df = df.rename(index={"Total Return": "IRR"})
        start = start.strftime("%Y/%m/%d")
        end = end.strftime("%Y/%m/%d")

        graph = self._plotBar_without_group(df, title=f"<b>IRR<b><br><i>{start} ~ {end}<i>")
        graph = self._mergeDict(json.loads(graph), {"layout": {"yaxis": {"tickformat": ".2%"}}})
        graph = json.dumps(graph)

        return graph

    def year_regular_saving_plan_irr(self):
        df = self.intersection_history()
        start = df.index[0]
        end = df.index[-1]

        dates = []
        amounts = {}
        nums = {}
        money = 1000
        pre_date = None
        for i, (index, row) in enumerate(df.iterrows()):
            if i != 0 and pd.Timedelta(index - pre_date).days < 365:
                continue

            dates.append(index.date())
            pre_date = index

            for symbol, close_adj in row.items():
                ammount = amounts.get(symbol, [])
                ammount.append(-money)
                amounts[symbol] = ammount
                nums[symbol] = nums.get(symbol, 0) + money / close_adj

        dates.append(df.index[-1].date())
        for symbol, close_adj in df.iloc[-1].items():
            amounts[symbol].append(nums[symbol] * close_adj)

        data = {}
        for symbol, amount in amounts.items():
            r = xirr(dates, amount)
            data[symbol] = [r]

        df = pd.DataFrame(data, index=["RSP_IRR"])
        return start, end, df

    def year_regular_saving_plan_irr_bar(self):
        start, end, df = self.year_regular_saving_plan_irr()
        start = start.strftime("%Y/%m/%d")
        end = end.strftime("%Y/%m/%d")

        graph = self._plotBar_without_group(
            df, title=f"<b>Year Regular Saving Plan IRR<b><br><i>{start} ~ {end}<i>"
        )
        graph = self._mergeDict(json.loads(graph), {"layout": {"yaxis": {"tickformat": ".2%"}}})
        graph = json.dumps(graph)

        return graph

    def rollback_graph(self):
        # =========================================================================
        # area
        data = []
        for st in self.stocks:
            df = st.rollback(self.iYear)
            data.append(df)

        df = pd.concat(data, axis=1)
        start = df.index[0] - pd.DateOffset(years=self.iYear)
        end = df.index[-1]

        lines = self._plotLine_without_markers(
            df,
            title=(
                f"<b>{self.iYear} Years Rollback<b><br>"
                f"Start: <i>{start.strftime('%Y/%m/%d')} ~"
                f" {(end-relativedelta(years=self.iYear)).strftime('%Y/%m/%d')}<i><br>"
                f"End  : <i>{(start+relativedelta(years=self.iYear)).strftime('%Y/%m/%d')} ~"
                f" {end.strftime('%Y/%m/%d')}<i>"
            ),
        )
        lines = self._mergeDict(json.loads(lines), {"layout": {"yaxis": {"tickformat": ".2%"}}})
        lines = json.dumps(lines)

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
                f"<b>{self.iYear} Years Rollback<b><br>"
                f"Start: <i>{start.strftime('%Y/%m/%d')} ~"
                f" {(end-relativedelta(years=self.iYear)).strftime('%Y/%m/%d')}<i><br>"
                f"End  : <i>{(start+relativedelta(years=self.iYear)).strftime('%Y/%m/%d')} ~"
                f" {end.strftime('%Y/%m/%d')}<i>"
            ),
        )
        violin = self._mergeDict(json.loads(violin), {"layout": {"yaxis": {"tickformat": ".2%"}}})
        violin = json.dumps(violin)

        return lines, violin

    def correlation_heatmap(self):
        # =========================================================================
        # close
        data = []
        for st in self.stocks:
            s = pd.Series(
                data=st.rawData["Close"].to_numpy(),
                index=st.rawData["Date"].to_numpy(),
                name=st.name,
            )
            data.append(s)

        df = pd.concat(data, axis=1)
        start = df.dropna().index[0].strftime("%Y/%m/%d")
        end = df.dropna().index[-1].strftime("%Y/%m/%d")
        close = self._plotHeatmap(
            df.corr(), title=f"<b>Correlation of Close<b><br><i>{start} ~ {end}<i>"
        )

        # =========================================================================
        # closeAdj
        data = []
        for st in self.stocks:
            s = pd.Series(
                data=st.rawData["Adj Close Cal"].to_numpy(),
                index=st.rawData["Date"].to_numpy(),
                name=st.name,
            )
            data.append(s)

        df = pd.concat(data, axis=1)
        start = df.dropna().index[0].strftime("%Y/%m/%d")
        end = df.dropna().index[-1].strftime("%Y/%m/%d")
        closeAdj = self._plotHeatmap(
            df.corr(), title=f"<b>Correlation of Adj Close <b><br><i>{start} ~ {end}<i>"
        )

        return close, closeAdj

    def daily_return_graph(self):
        data = []
        for st in self.stocks:
            df_daily = st.dailyReturn
            df_rollback = st.rollback(self.iYear)
            data.append((st.name, df_daily, df_rollback[st.name]))

        return self._plotDailyReturn(data)

    def active_vs_passive(self):
        # =========================================================================
        # year
        data = {}

        for st in self.stocks:
            df = st.history
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

            stock = rust_pyo3.Stock(df1)
            activeYear = stock.stat_active_year()
            holdYear = stock.stat_hold_year()
            del stock
            del df1

            data_stat_year[f"A {st.name}"] = activeYear
            data_stat_year[f"P {st.name}"] = holdYear

        data_stat_year = pd.concat(data_stat_year)

        annual_return = self._plotBox_with_group(
            data_stat_year,
            title=(f"<b>Annual Return Active vs Passive<b><br><i>{start} ~ {end}<i>"),
        )
        annual_return = self._mergeDict(
            json.loads(annual_return), {"layout": {"yaxis": {"tickformat": ".2%"}}}
        )
        annual_return = json.dumps(annual_return)

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

            stock = rust_pyo3.Stock(df1)
            activeAll = stock.stat_active_all()
            holdAll = stock.stat_hold_all()
            del stock
            del df1

            data_stat_all[f"A {st.name}"] = activeAll
            data_stat_all[f"P {st.name}"] = holdAll

        data_stat_all = pd.concat(data_stat_all)

        start = df.index[0]
        end = df.index[-1]
        total_return = self._plotBox_without_group(
            data_stat_all,
            title=(
                f"<b>Total Return Active vs Passive<b><br>"
                f"<i>{start.strftime('%Y/%m/%d')} ~"
                f" {end.strftime('%Y/%m/%d')}<i>"
            ),
        )
        total_return = self._mergeDict(
            json.loads(total_return), {"layout": {"yaxis": {"tickformat": ".2%"}}}
        )
        total_return = json.dumps(total_return)

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

    plots["totalReturn"] = fig.total_return_bar()
    plots["IRR"] = fig.irr_bar()
    plots["yearRegularSavingPlanIRR"] = fig.year_regular_saving_plan_irr_bar()
    plots["totalReturnPassiveVsActive"], plots["annualReturnPassiveVsActive"] = (
        fig.active_vs_passive()
    )
    plots["annualReturn"] = fig.annual_return_bar()
    plots["rollback"], plots["rollbackVolin"] = fig.rollback_graph()
    plots["correlationClose"], plots["correlationAdjClose"] = fig.correlation_heatmap()
    plots["dailyReturn"] = fig.daily_return_graph()

    with app.app_context():
        jsfolder = f"{prefix}"
        os.makedirs(os.path.join(path, jsfolder), exist_ok=True)

        for key, item in plots.items():
            graph = render_template("graph.js.j2", key=key, item=item)
            with open(os.path.join(path, f"{jsfolder}/{key}.js"), "w", encoding="UTF-8") as f:
                f.write(graph)

        html = render_template(
            "compare.html.j2", plots=plots, jsfolder=jsfolder, title=f"{prefix} Report"
        )
        with open(os.path.join(path, f"{prefix}_Report.html"), "w", encoding="UTF-8") as f:
            f.write(html)


if __name__ == "__main__":
    symbols = [
        {
            "name": "^TAIEX",
            "remark": "臺灣加權報酬指數",
            "fromPath": os.path.join(os.path.dirname(__file__), "extraData", "臺灣加權股價指數"),
            "groups": ["常用", "ETF"],
        },
        {"name": "^TWII", "remark": "臺灣加權指數", "groups": ["ETF"]},
        {
            "name": "^TAIEX",
            "remark": "臺灣加權報酬指數_日正2",
            "fromPath": os.path.join(os.path.dirname(__file__), "extraData", "臺灣加權股價指數"),
            "daily_return_mul": 2,
            "groups": ["日正"],
        },
        {
            "name": "^TAI50I",
            "remark": "臺灣50報酬指數",
            "fromPath": os.path.join(os.path.dirname(__file__), "extraData", "臺灣50指數"),
            "groups": ["常用", "ETF"],
        },
        {
            "name": "^TAI50I",
            "remark": "臺灣50報酬指數_日正2",
            "fromPath": os.path.join(os.path.dirname(__file__), "extraData", "臺灣50指數"),
            "daily_return_mul": 2,
            "groups": ["日正"],
        },
        {
            "name": "^TAI100I",
            "remark": "臺灣中型100報酬指數",
            "fromPath": os.path.join(os.path.dirname(__file__), "extraData", "臺灣中型100指數"),
            "groups": ["常用", "ETF"],
        },
        {
            "name": "^TAI100I",
            "remark": "臺灣中型100報酬指數_日正2",
            "fromPath": os.path.join(os.path.dirname(__file__), "extraData", "臺灣中型100指數"),
            "daily_return_mul": 2,
            "groups": ["日正"],
        },
        {
            "name": "^TAIDIVIDI",
            "remark": "臺灣高股息報酬指數",
            "fromPath": os.path.join(os.path.dirname(__file__), "extraData", "臺灣高股息指數"),
            "groups": ["ETF"],
        },
        {
            "name": "^TAIDIVIDI",
            "remark": "臺灣高股息報酬指數_日正2",
            "fromPath": os.path.join(os.path.dirname(__file__), "extraData", "臺灣高股息指數"),
            "daily_return_mul": 2,
            "groups": ["日正"],
        },
        # =================================================================================
        {"name": "0050.TW", "remark": "元大臺灣50", "replaceDiv": True, "groups": ["常用", "ETF"]},
        {"name": "00631L.TW", "remark": "元大台灣50正2", "groups": ["日正"]},
        {"name": "00675L.TW", "remark": "富邦臺灣加權正2", "replaceDiv": True, "groups": ["日正"]},
        {"name": "006208.TW", "remark": "富邦台50", "replaceDiv": True, "groups": ["常用", "ETF"]},
        {"name": "0051.TW", "remark": "元大中型100", "replaceDiv": True, "groups": ["ETF"]},
        {"name": "006204.TW", "remark": "永豐臺灣加權", "replaceDiv": True, "groups": ["ETF"]},
        {"name": "0056.TW", "remark": "元大高股息", "replaceDiv": True, "groups": ["ETF"]},
        # =================================================================================
        {"name": "2412.TW", "remark": "中華電信", "replaceDiv": True, "groups": ["個股"]},
        {"name": "2002.TW", "remark": "中鋼", "replaceDiv": True, "groups": ["個股"]},
        {
            "name": "2330.TW",
            "remark": "台積電",
            "replaceDiv": True,
            "dateDuplcatedCombine": True,
            "groups": ["個股"],
        },
        {"name": "2317.TW", "remark": "鴻海", "replaceDiv": True, "groups": ["個股"]},
        {"name": "6505.TW", "remark": "台塑石化", "replaceDiv": True, "groups": ["個股"]},
        {"name": "3481.TW", "remark": "群創", "replaceDiv": True, "groups": ["個股"]},
        {"name": "2303.TW", "remark": "聯電", "replaceDiv": True, "groups": ["個股"]},
        {"name": "2308.TW", "remark": "台達電", "replaceDiv": True, "groups": ["個股"]},
        {"name": "2454.TW", "remark": "聯發科", "replaceDiv": True, "groups": ["個股"]},
    ]
    report(symbols, start="1911-1-1", prefix="TW", iYear=5, name_width=12)

    symbols = [
        {
            "name": "VT",
            "remark": "Vanguard 國際大中小型股",
            "groups": ["常用", "國際股", "Vanguard"],
        },
        {
            "name": "VTWAX",
            "remark": "Vanguard 國際大中小型股 指數基金",
            "groups": ["國際股", "Vanguard"],
        },
        {"name": "IOO", "remark": "iShares 國際超大型股", "groups": ["國際股"]},
        {"name": "ACWI", "remark": "iShares 國際大中型股", "groups": ["國際股"]},
        {"name": "URTH", "remark": "iShares 國際大中型股", "groups": ["國際股"]},
        {
            "name": "VT",
            "remark": "Vanguard 國際大中小型股_日正2",
            "daily_return_mul": 2,
            "groups": ["日正"],
        },
        # =================================================================================
        {"name": "^GSPC", "remark": "S&P500指數", "groups": ["美股", "美大型股"]},
        {"name": "^SP500TR", "remark": "S&P500報酬指數", "groups": ["美股", "美大型股"]},
        {"name": "FXAIX", "remark": "Fidelity S&P500 指數基金", "groups": ["美股", "美大型股"]},
        {"name": "FLCPX", "remark": "Fidelity S&P500 指數基金", "groups": ["美股", "美大型股"]},
        {"name": "FNILX", "remark": "Fidelity 大型股 指數基金", "groups": ["美股", "美大型股"]},
        {"name": "SPY", "remark": "SPDR S&P500", "groups": ["美股", "美大型股"]},
        {"name": "SPLG", "remark": "SPDR S&P500", "groups": ["美股", "美大型股"]},
        {"name": "IVV", "remark": "iShares S&P500", "groups": ["美股", "美大型股"]},
        {"name": "SWPPX", "remark": "Schwab S&P500 指數基金", "groups": ["美股", "美大型股"]},
        {"name": "SNXFX", "remark": "Schwab 大型股 指數基金", "groups": ["美股", "美大型股"]},
        {"name": "SCHX", "remark": "Schwab 大型股", "groups": ["美股", "美大型股"]},
        {"name": "VV", "remark": "Vanguard 大型股", "groups": ["美股", "美大型股", "Vanguard"]},
        {
            "name": "VLCAX",
            "remark": "Vanguard 大型股 指數基金",
            "groups": ["美股", "美大型股", "Vanguard"],
        },
        {
            "name": "VOO",
            "remark": "Vanguard S&P500",
            "groups": ["美股", "美大型股", "Vanguard"],
        },
        {
            "name": "VFIAX",
            "remark": "Vanguard S&P500 指數基金",
            "groups": ["美股", "美大型股", "Vanguard"],
        },
        {
            "name": "SSO",
            "remark": "ProShares S&P500_真實日正2",
            "groups": ["日正"],
        },
        {
            "name": "SPXL",
            "remark": "ProShares S&P500_真實日正3",
            "groups": ["日正"],
        },
        # =================================================================================
        {"name": "^NDX", "remark": "那斯達克100指數", "groups": ["美股", "那斯達克"]},
        {"name": "QQQ", "remark": "Invesco 那斯達克100", "groups": ["美股", "那斯達克"]},
        {"name": "QQQM", "remark": "Invesco 那斯達克100", "groups": ["美股", "那斯達克"]},
        {
            "name": "QLD",
            "remark": "ProShares 那斯達克100_真實日正2",
            "groups": ["日正"],
        },
        {
            "name": "TQQQ",
            "remark": "ProShares 那斯達克100_真實日正3",
            "groups": ["日正"],
        },
        # =================================================================================
        {"name": "^DJI", "remark": "道瓊工業平均指數", "groups": ["美股", "道瓊"]},
        {"name": "DIA", "remark": "SPDR 道瓊", "groups": ["美股", "道瓊"]},
        {"name": "DDM", "remark": "ProShares 道瓊_真實日正2", "groups": ["日正"]},
        {"name": "UDOW", "remark": "ProShares 道瓊_真實日正3", "groups": ["日正"]},
        # =================================================================================
        {"name": "IWV", "remark": "iShares 羅素3000", "groups": ["美股", "羅素"]},
        {"name": "VTHR", "remark": "Vanguard 羅素3000", "groups": ["美股", "Vanguard", "羅素"]},
        # =================================================================================
        {"name": "ITOT", "remark": "iShares 美股", "groups": ["美股"]},
        {"name": "SPTM", "remark": "SPDR 美股", "groups": ["美股"]},
        {"name": "FZROX", "remark": "Fidelity 美股 指數基金", "groups": ["美股"]},
        {"name": "FSKAX", "remark": "Fidelity 美股 指數基金", "groups": ["美股"]},
        {"name": "SWTSX", "remark": "Schwab 美股 指數基金", "groups": ["美股"]},
        {"name": "SCHB", "remark": "Schwab 美股", "groups": ["美股"]},
        {"name": "VTI", "remark": "Vanguard 美股", "groups": ["常用", "美股", "Vanguard"]},
        {
            "name": "VTSAX",
            "remark": "Vanguard 美股 指數基金",
            "groups": ["美股", "Vanguard"],
        },
        {
            "name": "VTI",
            "remark": "Vanguard 美股報酬_日正2",
            "daily_return_mul": 2,
            "groups": ["日正"],
        },
        # =================================================================================
        {"name": "AVUV", "remark": "Avantis 美小型價值股", "groups": ["美股", "美小型價值股"]},
        {
            "name": "BOSVX",
            "remark": "Bridgeway Omni 美小型價值股 指數基金",
            "groups": ["美股", "美小型價值股"],
        },
        {"name": "DFAT", "remark": "Dimensional 美小型價值股", "groups": ["美股", "美小型價值股"]},
        {"name": "DFSV", "remark": "Dimensional 美小型價值股", "groups": ["美股", "美小型價值股"]},
        {
            "name": "FISVX",
            "remark": "Fidelity 美小型價值股 指數基金",
            "groups": ["美股", "美小型價值股"],
        },
        {"name": "IJS", "remark": "iShares 美小型價值股", "groups": ["美股", "美小型價值股"]},
        {"name": "ISCV", "remark": "iShares 美小型價值股", "groups": ["美股", "美小型價值股"]},
        {"name": "SLYV", "remark": "SPDR 美小型價值股", "groups": ["美股", "美小型價值股"]},
        {
            "name": "VTWV",
            "remark": "Vanguard 美小型價值股",
            "groups": ["美股", "美小型價值股", "Vanguard"],
        },
        {
            "name": "VIOV",
            "remark": "Vanguard 美小型價值股",
            "groups": ["美股", "美小型價值股", "Vanguard"],
        },
        {
            "name": "VBR",
            "remark": "Vanguard 美小型價值股",
            "groups": ["美股", "美小型價值股", "Vanguard"],
        },
        {
            "name": "VSIAX",
            "remark": "Vanguard 美小型價值股 指數基金",
            "groups": ["美股", "美小型價值股", "Vanguard"],
        },
        # =================================================================================
        {"name": "EFA", "remark": "iShares 已開發國家大中型股排美", "groups": ["已開發國家排美"]},
        {"name": "IDEV", "remark": "iShares 已開發國家大中型股排美", "groups": ["已開發國家排美"]},
        {"name": "SPDW", "remark": "SPDR 已開發國家大中型股排美", "groups": ["已開發國家排美"]},
        {
            "name": "IEFA",
            "remark": "iShares 已開發國家大中小型股排美",
            "groups": ["已開發國家排美"],
        },
        {
            "name": "VEA",
            "remark": "Vanguard 已開發國家大中小型股排美",
            "groups": ["常用", "已開發國家排美", "Vanguard"],
        },
        {
            "name": "VTMGX",
            "remark": "Vanguard 已開發國家大中小型股排美 指數基金",
            "groups": ["已開發國家排美", "Vanguard"],
        },
        {
            "name": "VEA",
            "remark": "Vanguard 已開發國家排美股報酬_日正2",
            "daily_return_mul": 2,
            "groups": ["日正"],
        },
        # =================================================================================
        {"name": "IPAC", "remark": "iShares 太平洋股", "groups": ["太平洋股"]},
        {"name": "VPL", "remark": "Vanguard 太平洋股", "groups": ["常用", "太平洋股", "Vanguard"]},
        {
            "name": "VPADX",
            "remark": "Vanguard 太平洋股 指數基金",
            "groups": ["太平洋股", "Vanguard"],
        },
        {
            "name": "VPL",
            "remark": "Vanguard 太平洋股報酬_日正2",
            "daily_return_mul": 2,
            "groups": ["日正"],
        },
        # =================================================================================
        {"name": "IEUR", "remark": "iShares 歐股", "groups": ["歐股"]},
        {"name": "SPEU", "remark": "SPDR 歐股", "groups": ["歐股"]},
        {"name": "VGK", "remark": "Vanguard 歐股", "groups": ["常用", "歐股", "Vanguard"]},
        {
            "name": "VEUSX",
            "remark": "Vanguard 歐股 指數基金",
            "groups": ["歐股", "Vanguard"],
        },
        {
            "name": "VGK",
            "remark": "Vanguard 歐股報酬_日正2",
            "daily_return_mul": 2,
            "groups": ["日正"],
        },
        # =================================================================================
        {"name": "FPADX", "remark": "Fidelity 新興市場大中型股 指數基金", "groups": ["新興市場"]},
        {"name": "EEM", "remark": "iShares 新興市場大中型股", "groups": ["新興市場"]},
        {"name": "IEMG", "remark": "iShares 新興市場大中小型股", "groups": ["新興市場"]},
        {"name": "SPEM", "remark": "SPDR 新興市場大中小型股", "groups": ["新興市場"]},
        {"name": "SCHE", "remark": "Schwab 新興市場大中小型股", "groups": ["新興市場"]},
        {
            "name": "VWO",
            "remark": "Vanguard 新興市場大中小型股",
            "groups": ["常用", "新興市場", "Vanguard"],
        },
        {
            "name": "VEMAX",
            "remark": "Vanguard 新興市場大中小型股 指數基金",
            "groups": ["新興市場", "Vanguard"],
        },
        {
            "name": "VWO",
            "remark": "Vanguard 新興市場股報酬_日正2",
            "daily_return_mul": 2,
            "groups": ["日正"],
        },
        {
            "name": "EDC",
            "remark": "Direxion 新興市場股報酬_真實日正3",
            "groups": ["日正"],
        },
        # =================================================================================
        {"name": "IXUS", "remark": "iShares 國際大中小型股排美", "groups": ["國際股排美"]},
        {
            "name": "FZILX",
            "remark": "Fidelity 國際大中小型股排美 指數基金",
            "groups": ["國際股排美"],
        },
        {
            "name": "FTIHX",
            "remark": "Fidelity 國際大中小型股排美 指數基金",
            "groups": ["國際股排美"],
        },
        {"name": "SWISX", "remark": "Schwab 國際大中小型股排美 指數基金", "groups": ["國際股排美"]},
        {"name": "SCHF", "remark": "Schwab 國際大中小型股排美", "groups": ["國際股排美"]},
        {
            "name": "VXUS",
            "remark": "Vanguard 國際大中小型股排美",
            "groups": ["常用", "國際股排美", "Vanguard"],
        },
        {
            "name": "VTIAX",
            "remark": "Vanguard 國際大中小型股排美 指數基金",
            "groups": ["國際股排美", "Vanguard"],
        },
        {
            "name": "VXUS",
            "remark": "Vanguard 國際大中小型股排美報酬_日正2",
            "daily_return_mul": 2,
            "groups": ["日正"],
        },
        {
            "name": "VEU",
            "remark": "Vanguard 國際大中型股排美",
            "groups": ["國際股排美", "Vanguard"],
        },
        {
            "name": "VEU",
            "remark": "Vanguard 國際大中型股排美報酬_日正2",
            "daily_return_mul": 2,
            "groups": ["日正"],
        },
        # =================================================================================
        {
            "name": "FUMBX",
            "remark": "Fidelity 短期美國庫債 指數基金",
            "groups": ["美債", "短期美國債"],
        },
        {"name": "SGOV", "remark": "iShares 短期美國庫債", "groups": ["美債", "短期美國債"]},
        {"name": "SHV", "remark": "iShares 短期美國庫債", "groups": ["美債", "短期美國債"]},
        {"name": "SCHO", "remark": "Schwab 短期美國庫債", "groups": ["美債", "短期美國債"]},
        {
            "name": "VSBSX",
            "remark": "Vanguard 短期美國庫債 指數基金",
            "groups": ["美債", "Vanguard", "短期美國債"],
        },
        {
            "name": "VGSH",
            "remark": "Vanguard 短期美國庫債",
            "groups": ["美債", "Vanguard", "短期美國債"],
        },
        # =================================================================================
        {
            "name": "FIPDX",
            "remark": "Fidelity 美抗通膨公債 指數基金",
            "groups": ["美債", "美抗通膨公債"],
        },
        {"name": "STIP", "remark": "iShares 美抗通膨公債", "groups": ["美債", "美抗通膨公債"]},
        {
            "name": "TIP",
            "remark": "iShares 美抗通膨公債",
            "groups": ["常用", "美債", "美抗通膨公債"],
        },
        {"name": "LTPZ", "remark": "PIMCO 美抗通膨公債", "groups": ["美債", "美抗通膨公債"]},
        {"name": "SCHP", "remark": "Schwab 美抗通膨公債", "groups": ["美債", "美抗通膨公債"]},
        {
            "name": "SWRSX",
            "remark": "Schwab 美抗通膨公債 指數基金",
            "groups": ["美債", "美抗通膨公債"],
        },
        {"name": "SPIP", "remark": "SPDR 美抗通膨公債", "groups": ["美債", "美抗通膨公債"]},
        {
            "name": "VAIPX",
            "remark": "Vanguard 美抗通膨公債 指數基金",
            "groups": ["美債", "Vanguard", "美抗通膨公債"],
        },
        {
            "name": "VTIP",
            "remark": "Vanguard 美抗通膨公債",
            "groups": ["美債", "Vanguard", "美抗通膨公債"],
        },
        {
            "name": "VTAPX",
            "remark": "Vanguard 美抗通膨公債 指數基金",
            "groups": ["美債", "Vanguard", "美抗通膨公債"],
        },
        # =================================================================================
        {"name": "FXNAX", "remark": "Fidelity 美債 指數基金", "groups": ["美債"]},
        {"name": "IUSB", "remark": "iShares 美債", "groups": ["美債"]},
        {"name": "AGG", "remark": "iShares 美債", "groups": ["美債"]},
        {"name": "SWAGX", "remark": "Schwab 美債 指數基金", "groups": ["美債"]},
        {"name": "SPAB", "remark": "SPDR 美債", "groups": ["美債"]},
        {"name": "BND", "remark": "Vanguard 美債", "groups": ["常用", "美債", "Vanguard"]},
        {
            "name": "VBTLX",
            "remark": "Vanguard 美債 指數基金",
            "groups": ["美債", "Vanguard"],
        },
        {
            "name": "BND",
            "remark": "Vanguard 美債報酬_日正2",
            "daily_return_mul": 2,
            "groups": ["日正"],
        },
        # =================================================================================
        {"name": "IAGG", "remark": "iShares 國際債美元避險排美", "groups": ["國際債排美"]},
        {
            "name": "BNDX",
            "remark": "Vanguard 國際債美元避險排美",
            "groups": ["常用", "國際債排美", "Vanguard"],
        },
        {
            "name": "BNDX",
            "remark": "Vanguard 國際債美元避險排美報酬_日正2",
            "daily_return_mul": 2,
            "groups": ["日正"],
        },
        {"name": "BWX", "remark": "SPDR 國際政府債排美", "groups": ["常用", "國際債排美"]},
        {
            "name": "BWX",
            "remark": "SPDR 國際政府債排美報酬_日正2",
            "daily_return_mul": 2,
            "groups": ["日正"],
        },
        # =================================================================================
        {"name": "DFAR", "remark": "Dimensional 美房地產", "groups": ["美房地產"]},
        {"name": "FREL", "remark": "Fidelity 美房地產", "groups": ["美房地產"]},
        {"name": "IYR", "remark": "iShares 美房地產", "groups": ["美房地產"]},
        {"name": "USRT", "remark": "iShares 美房地產", "groups": ["美房地產"]},
        {"name": "XLRE", "remark": "SPDR 美房地產", "groups": ["美房地產"]},
        {"name": "SCHH", "remark": "Schwab 美房地產", "groups": ["美房地產"]},
        {"name": "RWR", "remark": "SPDR 美房地產", "groups": ["美房地產"]},
        {"name": "VNQ", "remark": "Vanguard 美房地產", "groups": ["美房地產", "Vanguard"]},
        {
            "name": "VGSLX",
            "remark": "Vanguard 美房地產 指數基金",
            "groups": ["美房地產", "Vanguard"],
        },
        {
            "name": "VNQ",
            "remark": "Vanguard 美房地產報酬_日正2",
            "daily_return_mul": 2,
            "groups": ["日正"],
        },
    ]
    report(symbols, prefix="US", iYear=2, name_width=6)
