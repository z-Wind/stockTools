"""
get stock price
"""
import time
import logging
import requests


class AlphaVantage:
    """
    Alpha Vantage API
    """
    def __init__(self, api_key):
        """
        api_key: 申請網址：https://www.alphavantage.co/support/#api-key
        """
        self.api_key = api_key
        self.logger = logging.getLogger(self.__class__.__name__)

    def _api(self, func, **params):
        """Return data
        func: alphaVantage 的 API
        params: API 的相關參數
        """
        url = "https://www.alphavantage.co/query"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 6.1; rv:57.0) "
                          "Gecko/20100101 Firefox/57.0",
            }
        base_params = {"function": func,
                       "apikey": self.api_key,
                       "datatype": "json",
                      }
        params = {**base_params, **params}
        r = requests.get(url, params=params, headers=headers)
        data = r.json()
        if "Error Message" in data:
            self.logger.debug("%s error => url: %s", func, r.ur)
            return None
        elif len(data) < 2:
            self.logger.debug("%s error => url: %s", func, r.url)
            # AlphaVantage 有限定 1s 一次
            time.sleep(1)
            return self._api(func, **params)

        return data

    def intraday(self, symbol, interval, **params):
        """Return data
        https://www.alphavantage.co/documentation/#intraday
        symbol: 股票代碼，其他國家可嘗試加上後繏，例：0050.TW，但不一定有資料
        interval: 間隔時間 (1min, 5min, 15min, 30min, 60min)
        params: 其他相關參數
        """
        return self._api("TIME_SERIES_INTRADAY",
                         symbol=symbol, interval=interval, **params)

    def daily(self, symbol, **params):
        """Return data
        https://www.alphavantage.co/documentation/#daily
        symbol: 股票代碼，其他國家可嘗試加上後繏，例：0050.TW，但不一定有資料
        params: 其他相關參數
        """
        return self._api("TIME_SERIES_DAILY", symbol=symbol, **params)

    def daily_adj(self, symbol, **params):
        """Return data
        https://www.alphavantage.co/documentation/#dailyadj
        symbol: 股票代碼，其他國家可嘗試加上後繏，例：0050.TW，但不一定有資料
        params: 其他相關參數
        """
        return self._api("TIME_SERIES_DAILY_ADJUSTED", symbol=symbol, **params)

    def weekly(self, symbol, **params):
        """Return data
        https://www.alphavantage.co/documentation/#weekly
        symbol: 股票代碼，其他國家可嘗試加上後繏，例：0050.TW，但不一定有資料
        params: 其他相關參數
        """
        return self._api("TIME_SERIES_WEEKLY", symbol=symbol, **params)

    def weekly_adj(self, symbol, **params):
        """Return data
        https://www.alphavantage.co/documentation/#weeklyadj
        symbol: 股票代碼，其他國家可嘗試加上後繏，例：0050.TW，但不一定有資料
        params: 其他相關參數
        """
        return self._api("TIME_SERIES_WEEKLY_ADJUSTED",
                         symbol=symbol, **params)

    def monthly(self, symbol, **params):
        """Return data
        https://www.alphavantage.co/documentation/#monthly
        symbol: 股票代碼，其他國家可嘗試加上後繏，例：0050.TW，但不一定有資料
        params: 其他相關參數
        """
        return self._api("TIME_SERIES_MONTHLY", symbol=symbol, **params)

    def monthly_adj(self, symbol, **params):
        """Return data
        https://www.alphavantage.co/documentation/#monthlyadj
        symbol: 股票代碼，其他國家可嘗試加上後繏，例：0050.TW，但不一定有資料
        params: 其他相關參數
        """
        return self._api("TIME_SERIES_MONTHLY_ADJUSTED",
                         symbol=symbol, **params)

    def batch_quotes(self, symbols, **params):
        """Return data
        https://www.alphavantage.co/documentation/#batchquotes
        symbols: 所有股票的代碼，只支援美股
        params: 其他相關參數
        """
        return self._api("BATCH_STOCK_QUOTES", symbols=symbols, **params)


class TWSE:
    """
    TWSE API
    """
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.headers = {"User-Agent": "Mozilla/5.0 (Windows NT 6.1; rv:57.0) "
                                      "Gecko/20100101 Firefox/57.0",
                        "Connection": "keep-alive",
                       }
        url = "http://mis.twse.com.tw/stock/fibest.jsp"
        r = requests.get(url, headers=self.headers)
        self.cookies = r.cookies

    def _rename(self, symbol):
        """Return quote symbol name
        symbol 命名規則 上市或上櫃_股票代號.Channel，例：tse_0050.tw
        上市/上櫃，tse/otc
        """
        symbol = symbol.upper()
        try:
            symbol, kind = symbol.split(".", 1)
            if kind == "TWO":
                return "otc_{}.tw".format(symbol)
        except ValueError:
            pass

        return "tse_{}.tw".format(symbol)

    def real_time(self, symbol):
        """Return real-time price
        https://github.com/Asoul/tsrtc
        symbol: 股票代碼，上市後繏 .TW，上櫃後繏 .TWO
        """
        symbol = self._rename(symbol)
        url = "http://mis.twse.com.tw/stock/api/getStockInfo.jsp"
        millis = int(round(time.time() * 1000))
        params = {"ex_ch": symbol,
                  "_": millis,
                  "json": 1,
                  "delay": 0,
                 }
        r = requests.get(url, params=params,
                         headers=self.headers, cookies=self.cookies)
        data = r.json()

        return data["msgArray"]


if __name__ == "__main__":
    # 基礎設定
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(name)-30s.%(funcName)-20s(%(lineno)4d): "
               "%(levelname)-8s %(message)s",
        datefmt="%m-%d %H:%M",
        handlers=[logging.FileHandler("get_price.log", "w", "utf-8"), ],
        )
    # 定義 handler 輸出 sys.stderr
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    # 設定輸出格式
    formatter = logging.Formatter("%(name)-12s %(levelname)-8s %(message)s")
    # handler 設定輸出格式
    console.setFormatter(formatter)
    # 加入 hander 到 root logger
    logging.getLogger("").addHandler(console)

    a = AlphaVantage(api_key="aaaaa")
    print(a.intraday("VTI", "1min")["Meta Data"]["3. Last Refreshed"])
    print(a.daily("VTI")["Meta Data"]["3. Last Refreshed"])
    print(a.daily_adj("VTI")["Meta Data"]["3. Last Refreshed"])
    print(a.weekly("VTI")["Meta Data"]["3. Last Refreshed"])
    print(a.weekly_adj("VTI")["Meta Data"]["3. Last Refreshed"])
    print(a.monthly("VTI")["Meta Data"]["3. Last Refreshed"])
    print(a.monthly_adj("VTI")["Meta Data"]["3. Last Refreshed"])
    print(a.batch_quotes("VTI, VBR")["Stock Quotes"])

    t = TWSE()
    print(t.real_time("0050.tw")[0]["z"])
    print(t.real_time("0050")[0]["z"])
    print(t.real_time("1258.two")[0]["z"])
    print(t.real_time("abc.atwo"))
    print(t.real_time("abcatwo"))
