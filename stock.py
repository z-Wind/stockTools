import os
import yfinance as yf
import pandas as pd
import time

from datetime import datetime
from pyxirr import xirr
from pyquery import PyQuery
from dateutil.relativedelta import relativedelta
from flask import render_template
from FFI import rust_pyo3

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
            print("empty Dividends, so fill out 'Adj Close Cal' by 'Adj Close'")
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

    @property
    def dailyReturn(self):
        data = self.history

        pre = data.iloc[:-1].reset_index()
        cur = data.iloc[1:].reset_index()
        day_return = (cur["Adj Close Cal"] - pre["Adj Close Cal"]) / pre["Adj Close Cal"] * 100.0
        day_return = day_return.to_frame("Return")
        day_return["Start"] = pre["Date"]
        day_return["End"] = cur["Date"]

        return day_return

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
        "xaxis": {
            "tickfont": {"family": "Courier New", "size": 14},
            "automargin": True,
        },
        "yaxis": {"tickfont": {"family": "Courier New"}, "automargin": True},
        "plot_bgcolor": "#000",
        "paper_bgcolor": "#000",
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
        self.symbols = []
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
            try:
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
                        daily_return_mul=symbol.get("daily_return_mul", None),
                    )
                )
                self.symbols.append(symbol)
            except:
                print(f"{symbol} can not be created, it seems something wrong")

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

    def _custom_button(self, symbols):
        legendonly = []
        for i, symbol in enumerate(symbols):
            if symbol.endswith("正2") and "x2" in symbol:
                legendonly.append(i)

        updatemenus = [
            {
                "buttons": [
                    {
                        "args": [{"visible": True}, legendonly],
                        "args2": [{"visible": "legendonly"}, legendonly],
                        "label": "正2",
                        "method": "restyle",
                    },
                ],
                "type": "buttons",
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
            "updatemenus": self._custom_button(symbols),
        }
        layout = self._mergeDict(layout, self.default_layout)

        graph = {"data": dataList, "layout": layout}

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
            "updatemenus": self._custom_button(symbols),
        }
        layout = self._mergeDict(layout, self.default_layout)

        graph = {"data": dataList, "layout": layout}

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
            "updatemenus": self._custom_button(symbols),
        }
        layout = self._mergeDict(layout, self.default_layout)

        graph = {"data": dataList, "layout": layout}

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
            "updatemenus": self._custom_button(symbols),
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
            "updatemenus": self._custom_button(symbols),
        }
        layout = self._mergeDict(layout, self.default_layout)

        graph = {"data": dataList, "layout": layout}

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
            "updatemenus": self._custom_button(symbols),
        }
        layout = self._mergeDict(layout, self.default_layout)

        graph = {"data": dataList, "layout": layout}

        # 序列化
        return json.dumps(graph, cls=plotly.utils.PlotlyJSONEncoder)

    def _plotDailyReturn(self, data):
        dataList = []
        buttons = []
        for i, (symbol, df) in enumerate(data):
            start = df["Start"].iat[0].strftime("%Y/%m/%d")
            end = df["End"].iat[-1].strftime("%Y/%m/%d")
            df = df.sort_values(by=["Return"])

            x = df.apply(
                lambda x: "{}~{}".format(
                    x["Start"].strftime("%Y/%m/%d"), x["End"].strftime("%Y/%m/%d")
                ),
                axis=1,
            )
            dailyreturn = {
                "type": "bar",
                "name": symbol,
                "x": x,
                "y": df["Return"],
                "visible": i == 0,
            }

            cumgains = 100.0 * (df["Return"].map(lambda x: 1 + x / 100.0).cumprod() - 1.0)
            missedgains = {
                "type": "bar",
                "name": symbol,
                "y": cumgains.iloc[-1:-102:-1],
                "visible": i == 0,
                "xaxis": "x2",
                "yaxis": "y2",
            }

            cumlosses = 100.0 * (
                df["Return"].iloc[::-1].map(lambda x: 1 + x / 100.0).cumprod() - 1.0
            )
            avoidedlosses = {
                "type": "bar",
                "name": symbol,
                "y": cumlosses.iloc[-1:-102:-1],
                "visible": i == 0,
                "xaxis": "x2",
                "yaxis": "y3",
            }

            dataList.extend([missedgains, avoidedlosses, dailyreturn])

            visible = [False] * 3 * len(data)
            visible[i * 3] = True
            visible[i * 3 + 1] = True
            visible[i * 3 + 2] = True

            title = f"<b>Daily Return Analysis<b><br><i>{start} ~ {end}<i>"
            if i == 0:
                title_init = title
            button = {
                "method": "update",
                "args": [{"visible": visible}, {"title": {"text": title}}],
                "label": symbol,
            }
            buttons.append(button)

        layout = {
            "title": title_init,
            "hovermode": "x",
            "height": "1300",
            "updatemenus": [
                {
                    "x": 1,
                    "font": {"color": "#AAAAAA"},
                    "buttons": buttons,
                }
            ],
            "grid": {
                "rows": 3,
                "columns": 1,
                "pattern": "independent",
                "subplots": [["x2y2"], ["x2y3"], ["xy"]],
            },
            "showlegend": False,
            "annotations": [
                {
                    "text": "<b>Missed Gains<b>",
                    "align": "center",
                    "showarrow": False,
                    "xref": "x2",
                    "yref": "y2",
                },
                {
                    "text": "<b>Avoided Losses<b>",
                    "align": "center",
                    "showarrow": False,
                    "xref": "x2",
                    "yref": "y3",
                },
                {
                    "text": "<b>Daily Return<b>",
                    "align": "center",
                    "showarrow": False,
                    "xref": "x",
                    "yref": "y",
                },
            ],
        }
        layout = self._mergeDict(layout, self.default_layout)

        graph = {"data": dataList, "layout": layout}

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
        return self._plotBar_with_group(df, title=f"<b>Annual Return<b><br><i>{start} ~ {end}<i>")

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

        df = (df.iloc[-1, :] - df.iloc[0, :]) / df.iloc[0, :] * 100

        df = df.to_frame().T

        self.total_return_val = (start, end, df)

        return start, end, df

    def total_return_bar(self):
        start, end, df = self.total_return()
        start = start.strftime("%Y/%m/%d")
        end = end.strftime("%Y/%m/%d")

        return self._plotBar_without_group(df, title=f"<b>Total Return<b><br><i>{start} ~ {end}<i>")

    def irr_bar(self):
        start, end, df = self.total_return()
        df = df.rename(index={"Total Return": "IRR"})
        year = pd.Timedelta(end - start).days / 365.0
        df = df.map(lambda x: 100 * ((1 + x / 100) ** (1 / year) - 1))
        start = start.strftime("%Y/%m/%d")
        end = end.strftime("%Y/%m/%d")

        return self._plotBar_without_group(df, title=f"<b>IRR<b><br><i>{start} ~ {end}<i>")

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
            data[symbol] = [r * 100.0]

        df = pd.DataFrame(data, index=["RSP_IRR"])
        return start, end, df

    def year_regular_saving_plan_irr_bar(self):
        start, end, df = self.year_regular_saving_plan_irr()
        start = start.strftime("%Y/%m/%d")
        end = end.strftime("%Y/%m/%d")

        return self._plotBar_without_group(
            df, title=f"<b>Year Regular Saving Plan IRR<b><br><i>{start} ~ {end}<i>"
        )

    def roll_back_graph(self):
        # =========================================================================
        # area
        data = []
        for st in self.stocks:
            df = st.rollback(self.iYear)
            data.append(df)

        df = pd.concat(data, axis=1)
        start = df.index[0] - pd.DateOffset(years=self.iYear)
        end = df.index[-1]

        area = self._plotArea(
            df,
            title=(
                f"<b>{self.iYear} Years Roll Back<b><br>"
                f"Start: <i>{start.strftime('%Y/%m/%d')} ~"
                f" {(end-relativedelta(years=self.iYear)).strftime('%Y/%m/%d')}<i><br>"
                f"End  : <i>{(start+relativedelta(years=self.iYear)).strftime('%Y/%m/%d')} ~"
                f" {end.strftime('%Y/%m/%d')}<i>"
            ),
        )

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
            df = st.dailyReturn
            data.append((st.name, df))

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

            data_stat_year[f"{st.symbol:{self.name_width}s} A {st.remark}"] = activeYear * 100
            data_stat_year[f"{st.symbol:{self.name_width}s} P {st.remark}"] = holdYear * 100

        data_stat_year = pd.concat(data_stat_year)

        annual_return = self._plotBox_with_group(
            data_stat_year,
            title=(f"<b>Annual Return Active vs Passive<b><br><i>{start} ~ {end}<i>"),
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

            stock = rust_pyo3.Stock(df1)
            activeAll = stock.stat_active_all()
            holdAll = stock.stat_hold_all()
            del stock
            del df1

            data_stat_all[f"{st.symbol:{self.name_width}s} A {st.remark}"] = activeAll * 100
            data_stat_all[f"{st.symbol:{self.name_width}s} P {st.remark}"] = holdAll * 100

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
    plots["rollBack"], plots["rollBackVolin"] = fig.roll_back_graph()
    plots["correlationClose"], plots["correlationAdjClose"] = fig.correlation_heatmap()
    plots["dailyReturn"] = fig.daily_return_graph()

    with app.app_context():
        html = render_template("compare.html", plots=plots, title=f"{prefix} Report")

    with open(os.path.join(path, f"{prefix}_Report.html"), "w", encoding="UTF-8") as f:
        f.write(html)


if __name__ == "__main__":
    symbols = [
        {"name": "^TWII", "remark": "臺灣加權指數"},
        {
            "name": "^TAIEX",
            "remark": "臺灣加權報酬指數",
            "fromPath": os.path.join(os.path.dirname(__file__), "extraData", "臺灣加權股價指數"),
        },
        {
            "name": "^TAIEX",
            "remark": "臺灣加權報酬指數_日正2",
            "fromPath": os.path.join(os.path.dirname(__file__), "extraData", "臺灣加權股價指數"),
            "daily_return_mul": 2,
        },
        {
            "name": "^TAI50I",
            "remark": "臺灣50報酬指數",
            "fromPath": os.path.join(os.path.dirname(__file__), "extraData", "臺灣50指數"),
        },
        {
            "name": "^TAI50I",
            "remark": "臺灣50報酬指數_日正2",
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
            "remark": "臺灣中型100報酬指數_日正2",
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
            "remark": "臺灣高股息報酬指數_日正2",
            "fromPath": os.path.join(os.path.dirname(__file__), "extraData", "臺灣高股息指數"),
            "daily_return_mul": 2,
        },
        {"name": "0050.TW", "remark": "元大臺灣50", "replaceDiv": True},
        {"name": "00631L.TW", "remark": "元大台灣50正2"},
        {"name": "00675L.TW", "remark": "富邦臺灣加權正2", "replaceDiv": True},
        {"name": "006208.TW", "remark": "富邦台50", "replaceDiv": True},
        {"name": "0051.TW", "remark": "元大中型100", "replaceDiv": True},
        {"name": "006204.TW", "remark": "永豐臺灣加權", "replaceDiv": True},
        {"name": "0056.TW", "remark": "元大高股息", "replaceDiv": True},
        {"name": "2412.TW", "remark": "中華電信", "replaceDiv": True},
        {"name": "2002.TW", "remark": "中鋼", "replaceDiv": True},
        {
            "name": "2330.TW",
            "remark": "台積電",
            "replaceDiv": True,
            "dateDuplcatedCombine": True,
        },
        {"name": "2317.TW", "remark": "鴻海", "replaceDiv": True},
        {"name": "6505.TW", "remark": "台塑石化", "replaceDiv": True},
        {"name": "3481.TW", "remark": "群創", "replaceDiv": True},
        {"name": "2303.TW", "remark": "聯電", "replaceDiv": True},
        {"name": "2308.TW", "remark": "台達電", "replaceDiv": True},
        {"name": "2454.TW", "remark": "聯發科", "replaceDiv": True},
    ]
    report(symbols, start="1911-1-1", prefix="TW", iYear=5, name_width=12)

    symbols = [
        {"name": "VT", "remark": "國際大中小型股"},
        {"name": "VT", "remark": "國際大中小型股_日正2", "daily_return_mul": 2},
        {"name": "^GSPC", "remark": "S&P500指數"},
        {"name": "^SP500TR", "remark": "S&P500報酬指數"},
        {"name": "VOO", "remark": "S&P500"},
        {"name": "SSO", "remark": "S&P500_真實日正2"},
        {"name": "SPXL", "remark": "S&P500_真實日正3"},
        {"name": "^NDX", "remark": "那斯達克100指數"},
        {"name": "QQQ", "remark": "那斯達克100"},
        {"name": "QLD", "remark": "那斯達克100_真實日正2"},
        {"name": "TQQQ", "remark": "那斯達克100_真實日正3"},
        {"name": "^DJI", "remark": "道瓊工業平均指數"},
        {"name": "DIA", "remark": "道瓊"},
        {"name": "DDM", "remark": "道瓊_真實日正2"},
        {"name": "UDOW", "remark": "道瓊_真實日正3"},
        {"name": "VTI", "remark": "美股"},
        {"name": "VTI", "remark": "美股報酬_日正2", "daily_return_mul": 2},
        {"name": "VBR", "remark": "美小型價值股"},
        {"name": "VEA", "remark": "歐太平洋股"},
        {"name": "VEA", "remark": "歐太平洋股報酬_日正2", "daily_return_mul": 2},
        {"name": "VPL", "remark": "太平洋股"},
        {"name": "VPL", "remark": "太平洋股報酬_日正2", "daily_return_mul": 2},
        {"name": "VGK", "remark": "歐股"},
        {"name": "VGK", "remark": "歐股報酬_日正2", "daily_return_mul": 2},
        {"name": "VWO", "remark": "新興市場股"},
        {"name": "VWO", "remark": "新興市場股報酬_日正2", "daily_return_mul": 2},
        {"name": "EDC", "remark": "新興市場股報酬_真實日正3"},
        {"name": "VXUS", "remark": "國際大中小型股排美"},
        {
            "name": "VXUS",
            "remark": "國際大中小型股排美報酬_日正2",
            "daily_return_mul": 2,
        },
        {"name": "VEU", "remark": "國際大中型股排美"},
        {"name": "VEU", "remark": "國際大中型股排美報酬_日正2", "daily_return_mul": 2},
        {"name": "BND", "remark": "美債"},
        {"name": "BND", "remark": "美債報酬_日正2", "daily_return_mul": 2},
        {"name": "BNDX", "remark": "國際債排美"},
        {"name": "BNDX", "remark": "國際債排美報酬_日正2", "daily_return_mul": 2},
        {"name": "BWX", "remark": "國際債排美"},
        {"name": "BWX", "remark": "國際債排美報酬_日正2", "daily_return_mul": 2},
        {"name": "VNQ", "remark": "美房地產"},
        {"name": "VNQ", "remark": "美房地產報酬_日正2", "daily_return_mul": 2},
    ]
    report(symbols, prefix="US", name_width=6)

    # symbols = [
    # # {"name": "00646.TW", "remark": "元大S&P 500", "replaceDiv": True},
    # {"name": "VOO", "remark": "Vanguard S&P 500"},
    # {"name": "0050.TW", "remark": "元大臺灣50", "replaceDiv": True},
    # ]
    # report(symbols, start="1911-1-1", prefix="Mix", iYear=5)
