from datetime import datetime
import json
import os
import re
import pandas as pd
import io
import plotly
import requests

from flask import Flask, render_template

app = Flask(__name__)


def read_xml(url, xpath):
    r = requests.get(url, verify=False)

    df = pd.read_xml(io.BytesIO(r.content), xpath=xpath)

    return df


def read_csv(url, encoding="utf-8"):
    r = requests.get(url, verify=False)

    df = pd.read_csv(io.BytesIO(r.content), encoding=encoding)

    return df


def read_json(url, encoding="utf-8"):
    r = requests.get(url, verify=False)

    df = pd.read_json(io.BytesIO(r.content), encoding=encoding)

    return df


default_layout = {
    # "height": 600,
    # "autosize": False,
    "title": {"font": {"family": "Times New Roman"}, "x": 0.05, "y": 0.9},
    "font": {"family": "Courier New", "color": "#ffffff"},
    "xaxis": {
        "tickfont": {"family": "Courier New", "size": 14},
        "automargin": True,
    },
    "yaxis": {
        "tickfont": {"family": "Courier New"},
        "automargin": True,
    },
    "plot_bgcolor": "#000",
    "paper_bgcolor": "#000",
}


def mergeDict(a, b, path=None, overwrite=False):
    "merges b into a"
    if path is None:
        path = []
    for key in b:
        if key in a:
            if isinstance(a[key], dict) and isinstance(b[key], dict):
                mergeDict(a[key], b[key], path + [str(key)])
            elif a[key] == b[key]:
                pass  # same leaf value
            else:
                if overwrite:
                    a[key] = b[key]

                # raise Exception("Conflict at %s" % ".".join(path + [str(key)]))
        else:
            a[key] = b[key]
    return a


def plotLine(df, title=None):
    dataList = []
    for name in df.columns:
        data = {
            "type": "scatter",
            "name": name,
            "x": df.index,
            "y": df[name],
            "mode": "lines",
        }
        dataList.append(data)

    layout = {
        "title": {"text": title},
        "hovermode": "x",
    }
    layout = mergeDict(layout, default_layout)

    graph = {"data": dataList, "layout": layout}

    # 序列化
    return json.dumps(graph, cls=plotly.utils.PlotlyJSONEncoder)


def plotBar(df, title=None):
    dataList = []
    for name in df.columns:
        data = {
            "type": "bar",
            "name": name,
            "x": [name],
            "y": df[name],
        }
        dataList.append(data)

    layout = {
        "title": {"text": title},
        "hovermode": "x",
        "yaxis": {"tickformat": ".2%"},
    }
    layout = mergeDict(layout, default_layout)

    graph = {"data": dataList, "layout": layout}

    # 序列化
    return json.dumps(graph, cls=plotly.utils.PlotlyJSONEncoder)


rstr = r'[- ,、()~∕\/－%*?:"<>|]+'


def index_原始值_年增率_plot(plots, key, url, xpath, item_remove_patt, title_suffix, fillna=False):
    key = re.sub(rstr, "_", key)

    df = read_xml(url, xpath)
    df["Item"] = df["Item"].str.replace(item_remove_patt, "", regex=True)
    date_range = f"{df["TIME_PERIOD"].iloc[0]}~{df["TIME_PERIOD"].iloc[-1]}"
    df["TIME_PERIOD"] = df["TIME_PERIOD"].apply(lambda x: datetime.strptime(x, "%YM%m"))

    pivot_df = df[df["TYPE"] == "原始值"].pivot_table(
        index="TIME_PERIOD", columns="Item", values="Item_VALUE", sort=False
    )
    plots[f"{key}_原始值"] = plotLine(pivot_df, f"{key} 原始值 {title_suffix} {date_range}")

    def irr(x):
        x = x.dropna()

        val = (x.iloc[-1] / x.iloc[0]) ** (365 / (x.index[-1] - x.index[0]).days) - 1
        return val

    irr_df = pivot_df.apply(irr, axis=0).to_frame()
    irr_df.columns = ["IRR"]
    irr_df = irr_df.T
    plots[f"{key}_IRR"] = plotBar(irr_df, f"{key} IRR(%) {title_suffix} {date_range}")

    pivot_df = df[df["TYPE"] == "年增率(%)"].pivot_table(
        index="TIME_PERIOD", columns="Item", values="Item_VALUE", sort=False
    )
    graph = plotLine(pivot_df / 100, f"{key} 年增率(%) {title_suffix} {date_range}")
    graph = mergeDict(json.loads(graph), {"layout": {"yaxis": {"tickformat": ".2%"}}})
    plots[f"{key}_年增率"] = json.dumps(graph)


def 年_plot(plots, key, url, columns_remove_patt, title_suffix):
    key = re.sub(rstr, "_", key)

    df = read_csv(url)
    df = df.set_index("年")
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)
    plots[key] = plotLine(df, f"{key}{title_suffix} {df.index[0]}~{df.index[-1]}")


def 年月混合_plot(plots, key, url, index, columns_remove_patt, title_suffix, encoding="utf-8"):
    key = re.sub(rstr, "_", key)

    df = read_csv(url, encoding)
    df = df.set_index(index)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    df_year = df.filter(regex=r"\d+年$", axis="index")
    plots[f"{key}_年"] = plotLine(
        df_year, f"{key}_年{title_suffix} {df_year.index[0]}~{df_year.index[-1]}"
    )
    df_month = df.filter(regex=r"\d+年 *\d+月$", axis="index")
    plots[f"{key}_月"] = plotLine(
        df_month, f"{key}_月{title_suffix} {df_month.index[0]}~{df_month.index[-1]}"
    )


if __name__ == "__main__":
    plots = {}
    items = {}

    # https://data.gov.tw/dataset/6019
    index_原始值_年增率_plot(
        plots,
        key="消費者物價基本分類指數",
        url="https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/230555/pr0101a1m.xml",
        xpath="//Obs",
        item_remove_patt=r"\(指數基期：民國110年=100\)",
        title_suffix="(指數基期：民國110年=100)",
    )

    # https://data.gov.tw/dataset/148439
    index_原始值_年增率_plot(
        plots,
        "生產者物價基本分類指數",
        "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/230534/pr0701a1m.xml",
        "//Obs",
        r"\(指數基期：民國110年=100\)",
        "(指數基期：民國110年=100)",
    )

    # https://data.gov.tw/dataset/8239
    index_原始值_年增率_plot(
        plots,
        "躉售物價基本分類指數",
        "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/230546/pr0202a1m.xml",
        "//Obs",
        r"\(民國105年=100\)",
        "(指數基期：民國105年=100)",
    )

    # ==================================================================

    # https://data.gov.tw/dataset/44232
    key = "國民所得統計-國民所得、儲蓄與投資-季"
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/230514/na8201a1q.xml"
    xpath = "//Obs"

    key = re.sub(rstr, "_", key)

    df = read_xml(url, xpath)
    df = df.fillna(0)

    pivot_df = df[df["TYPE"] == "原始值"].pivot_table(
        index="TIME_PERIOD", columns="Item", values="Item_VALUE", sort=False
    )

    pat_filter = "當期價格(新臺幣百萬元)"
    df_filter = pivot_df[[column for column in pivot_df.columns if pat_filter in column]]
    df_filter.columns = df_filter.columns.str.replace(f"{pat_filter}、", "")
    plots[f"{key}_原始值_當期價格"] = plotLine(
        df_filter, f"{key} 原始值 {pat_filter} {df_filter.index[0]}~{df_filter.index[-1]}"
    )

    pat_filter = "連鎖實質值(2021為參考年_新臺幣百萬元)"
    df_filter = pivot_df[[column for column in pivot_df.columns if pat_filter in column]]
    df_filter.columns = df_filter.columns.str.replace(f"{pat_filter}、", "")
    plots[f"{key}_原始值_連鎖實質值"] = plotLine(
        df_filter, f"{key} 原始值 {pat_filter} {df_filter.index[0]}~{df_filter.index[-1]}"
    )

    pat_filter = "平減指數(2021年=100)"
    df_filter = pivot_df[[column for column in pivot_df.columns if pat_filter in column]]
    df_filter.columns = df_filter.columns.str.replace(f"{pat_filter}、", "")
    plots[f"{key}_原始值_平減指數"] = plotLine(
        df_filter, f"{key} 原始值 {pat_filter} {df_filter.index[0]}~{df_filter.index[-1]}"
    )

    pivot_df = df[df["TYPE"] == "年增率(%)"].pivot_table(
        index="TIME_PERIOD", columns="Item", values="Item_VALUE", sort=False
    )

    pat_filter = "當期價格(新臺幣百萬元)"
    df_filter = pivot_df[[column for column in pivot_df.columns if pat_filter in column]]
    df_filter.columns = df_filter.columns.str.replace(f"{pat_filter}、", "")
    graph = plotLine(
        df_filter / 100, f"{key} 年增率(%) {pat_filter} {df_filter.index[0]}~{df_filter.index[-1]}"
    )
    graph = mergeDict(json.loads(graph), {"layout": {"yaxis": {"tickformat": ".2%"}}})
    plots[f"{key}_年增率_當期價格"] = json.dumps(graph)

    pat_filter = "連鎖實質值(2021為參考年_新臺幣百萬元)"
    df_filter = pivot_df[[column for column in pivot_df.columns if pat_filter in column]]
    df_filter.columns = df_filter.columns.str.replace(f"{pat_filter}、", "")
    graph = plotLine(
        df_filter / 100, f"{key} 年增率(%) {pat_filter} {df_filter.index[0]}~{df_filter.index[-1]}"
    )
    graph = mergeDict(json.loads(graph), {"layout": {"yaxis": {"tickformat": ".2%"}}})
    plots[f"{key}_年增率_連鎖實質值"] = json.dumps(graph)

    pat_filter = "平減指數(2021年=100)"
    df_filter = pivot_df[[column for column in pivot_df.columns if pat_filter in column]]
    df_filter.columns = df_filter.columns.str.replace(f"{pat_filter}、", "")
    graph = plotLine(
        df_filter / 100, f"{key} 年增率(%) {pat_filter} {df_filter.index[0]}~{df_filter.index[-1]}"
    )
    graph = mergeDict(json.loads(graph), {"layout": {"yaxis": {"tickformat": ".2%"}}})
    plots[f"{key}_年增率_平減指數"] = json.dumps(graph)

    # https://data.gov.tw/dataset/6799
    key = "國民所得統計-常用資料-季"
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/230514/na8101a1q.xml"
    xpath = "//Obs"

    key = re.sub(rstr, "_", key)

    df = read_xml(url, xpath)

    df_value = df.drop(df[df["TYPE"] == "年增率(%)"].index)
    pivot_df = df_value.pivot_table(
        index="TIME_PERIOD", columns="Item", values="Item_VALUE", sort=False
    )
    plots[f"{key}_原始值"] = plotLine(
        pivot_df, f"{key} 原始值 {pivot_df.index[0]}~{pivot_df.index[-1]}"
    )

    df_value = df.drop(df[df["TYPE"] == "原始值"].index)
    pivot_df = df_value.pivot_table(
        index="TIME_PERIOD", columns="Item", values="Item_VALUE", sort=False
    )
    graph = plotLine(pivot_df / 100, f"{key} 年增率(%) {pivot_df.index[0]}~{pivot_df.index[-1]}")
    graph = mergeDict(json.loads(graph), {"layout": {"yaxis": {"tickformat": ".2%"}}})
    plots[f"{key}_年增率"] = json.dumps(graph)

    # =================================================

    # https://data.gov.tw/dataset/93951
    年_plot(
        plots,
        "家庭收支調查-戶數五等分位之平均每戶所得總額",
        "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/067-%E6%88%B6%E6%95%B8%E4%BA%94%E7%AD%89%E5%88%86%E4%BD%8D%E7%B5%84%E4%B9%8B%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E6%89%80%E5%BE%97%E7%B8%BD%E9%A1%8D.csv",
        "可支配所得按戶數五等分位組之平均每戶所得總額-|-元",
        "-元",
    )

    # https://data.gov.tw/dataset/94752
    年_plot(
        plots,
        "家庭收支調查-戶數五等分位之平均每戶所得收入總計",
        "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/068-%E6%88%B6%E6%95%B8%E4%BA%94%E7%AD%89%E5%88%86%E4%BD%8D%E7%B5%84%E4%B9%8B%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E6%89%80%E5%BE%97%E6%94%B6%E5%85%A5%E7%B8%BD%E8%A8%88.csv",
        "可支配所得按戶數五等分位組之平均每戶所得收入總計-|-元",
        "-元 所得收入=所得總額-自用住宅及其他營建物設算折舊",
    )

    # https://data.gov.tw/dataset/98835
    年_plot(
        plots,
        "家庭收支調查-戶數五等分位之平均每戶非消費支出",
        "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/071-%E6%88%B6%E6%95%B8%E4%BA%94%E7%AD%89%E5%88%86%E4%BD%8D%E7%B5%84%E4%B9%8B%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E9%9D%9E%E6%B6%88%E8%B2%BB%E6%94%AF%E5%87%BA.csv",
        "可支配所得按戶數五等分位組之平均每戶非消費支出-|-元",
        "-元",
    )

    # https://data.gov.tw/dataset/91925
    年_plot(
        plots,
        "家庭收支調查-戶數五等分位之平均每戶可支配所得",
        "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/065-%E6%88%B6%E6%95%B8%E4%BA%94%E7%AD%89%E5%88%86%E4%BD%8D%E7%B5%84%E4%B9%8B%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E5%8F%AF%E6%94%AF%E9%85%8D%E6%89%80%E5%BE%97.csv",
        "可支配所得按戶數五等分位組-|-元",
        "-元 可支配所得=所得收入-非消費支出",
    )

    # https://data.gov.tw/dataset/98834
    年_plot(
        plots,
        "家庭收支調查-戶數五等分位之平均每戶消費支出",
        "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/070-%E6%88%B6%E6%95%B8%E4%BA%94%E7%AD%89%E5%88%86%E4%BD%8D%E7%B5%84%E4%B9%8B%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E6%B6%88%E8%B2%BB%E6%94%AF%E5%87%BA.csv",
        "可支配所得按戶數五等分位組之平均每戶消費支出-|-元",
        "-元",
    )

    # https://data.gov.tw/dataset/101338
    年_plot(
        plots,
        "家庭收支調查-戶數五等分位之平均每戶經常性支出",
        "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/072-%E6%88%B6%E6%95%B8%E4%BA%94%E7%AD%89%E5%88%86%E4%BD%8D%E7%B5%84%E4%B9%8B%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E7%B6%93%E5%B8%B8%E6%80%A7%E6%94%AF%E5%87%BA.csv",
        "可支配所得按戶數五等分位組之平均每戶經常性支出-|-元",
        "-元 經常性支出=消費支出+非消費支出",
    )

    # https://data.gov.tw/dataset/93950
    年_plot(
        plots,
        "家庭收支調查-戶數五等分位之平均每戶儲蓄",
        "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/066-%E6%88%B6%E6%95%B8%E4%BA%94%E7%AD%89%E5%88%86%E4%BD%8D%E7%B5%84%E4%B9%8B%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E5%84%B2%E8%93%84.csv",
        "可支配所得按戶數五等分位組之平均每戶儲蓄-|-元",
        "-元 儲蓄=所得收入-經常性支出=可支配所得-消費支出",
    )

    # https://data.gov.tw/dataset/9424
    年_plot(
        plots,
        "家庭收支調查-戶數五等分位組之平均每戶人口數",
        "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/019-%E6%88%B6%E6%95%B8%E4%BA%94%E7%AD%89%E5%88%86%E4%BD%8D%E7%B5%84%E4%B9%8B%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E4%BA%BA%E5%8F%A3%E6%95%B8.csv",
        "-人",
        "-人",
    )

    # https://data.gov.tw/dataset/9425
    年_plot(
        plots,
        "家庭收支調查-戶數五等分位組之平均每戶就業人數",
        "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/020-%E6%88%B6%E6%95%B8%E4%BA%94%E7%AD%89%E5%88%86%E4%BD%8D%E7%B5%84%E4%B9%8B%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E5%B0%B1%E6%A5%AD%E4%BA%BA%E6%95%B8.csv",
        "-人",
        "-人",
    )

    # https://data.gov.tw/dataset/132285
    年_plot(
        plots,
        "家庭收支調查-所得收入者五等分位平均每人可支配所得",
        "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/089-%E6%89%80%E5%BE%97%E6%94%B6%E5%85%A5%E8%80%85%E4%BA%94%E7%AD%89%E5%88%86%E4%BD%8D%E5%B9%B3%E5%9D%87%E6%AF%8F%E4%BA%BA%E5%8F%AF%E6%94%AF%E9%85%8D%E6%89%80%E5%BE%97.csv",
        "可支配所得按所得收入者人數五等分位組之平均每人可支配所得-",
        "",
    )

    # =================================================

    # https://data.gov.tw/dataset/54825
    年_plot(
        plots,
        "家庭收支調查-家庭組織型態別平均每戶所得總額",
        "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/058-%E5%AE%B6%E5%BA%AD%E7%B5%84%E7%B9%94%E5%9E%8B%E6%85%8B%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E6%89%80%E5%BE%97%E7%B8%BD%E9%A1%8D.csv",
        "-元",
        "-元",
    )

    # https://data.gov.tw/dataset/56525
    年_plot(
        plots,
        "家庭收支調查-家庭組織型態別平均每戶所得收入總計",
        "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/059-%E5%AE%B6%E5%BA%AD%E7%B5%84%E7%B9%94%E5%9E%8B%E6%85%8B%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E6%89%80%E5%BE%97%E6%94%B6%E5%85%A5%E7%B8%BD%E8%A8%88.csv",
        "-元",
        "-元 所得收入=所得總額-自用住宅及其他營建物設算折舊",
    )

    # https://data.gov.tw/dataset/79207
    年_plot(
        plots,
        "家庭收支調查-家庭組織型態別平均每戶非消費支出",
        "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/062-%E5%AE%B6%E5%BA%AD%E7%B5%84%E7%B9%94%E5%9E%8B%E6%85%8B%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E9%9D%9E%E6%B6%88%E8%B2%BB%E6%94%AF%E5%87%BA.csv",
        "-元",
        "-元",
    )

    # https://data.gov.tw/dataset/45126
    年_plot(
        plots,
        "家庭收支調查-家庭組織型態別平均每戶可支配所得",
        "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/056-%E5%AE%B6%E5%BA%AD%E7%B5%84%E7%B9%94%E5%9E%8B%E6%85%8B%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E5%8F%AF%E6%94%AF%E9%85%8D%E6%89%80%E5%BE%97.csv",
        "-元",
        "-元 可支配所得=所得收入-非消費支出",
    )

    # https://data.gov.tw/dataset/76265
    年_plot(
        plots,
        "家庭收支調查-家庭組織型態別平均每戶消費支出",
        "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/061-%E5%AE%B6%E5%BA%AD%E7%B5%84%E7%B9%94%E5%9E%8B%E6%85%8B%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E6%B6%88%E8%B2%BB%E6%94%AF%E5%87%BA.csv",
        "-元",
        "-元",
    )

    # https://data.gov.tw/dataset/79208
    年_plot(
        plots,
        "家庭收支調查-家庭組織型態別平均每戶經常性支出",
        "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/063-%E5%AE%B6%E5%BA%AD%E7%B5%84%E7%B9%94%E5%9E%8B%E6%85%8B%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E7%B6%93%E5%B8%B8%E6%80%A7%E6%94%AF%E5%87%BA.csv",
        "-元",
        "-元 經常性支出=消費支出+非消費支出",
    )

    # https://data.gov.tw/dataset/54823
    年_plot(
        plots,
        "家庭收支調查-家庭組織型態別平均每戶儲蓄",
        "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/057-%E5%AE%B6%E5%BA%AD%E7%B5%84%E7%B9%94%E5%9E%8B%E6%85%8B%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E5%84%B2%E8%93%84.csv",
        "-元",
        "-元 儲蓄=所得收入-經常性支出=可支配所得-消費支出",
    )

    # https://data.gov.tw/dataset/27963
    年_plot(
        plots,
        "家庭收支調查-性別統計指標-家戶狀況按家庭組織型態分",
        "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/046-%E6%80%A7%E5%88%A5%E7%B5%B1%E8%A8%88%E6%8C%87%E6%A8%99-%E5%AE%B6%E6%88%B6%E7%8B%80%E6%B3%81%E6%8C%89%E5%AE%B6%E5%BA%AD%E7%B5%84%E7%B9%94%E5%9E%8B%E6%85%8B%E5%88%86.csv",
        "戶數",
        "戶數",
    )

    # =====================================================

    # https://data.gov.tw/dataset/9417
    年_plot(
        plots,
        "家庭收支調查-各縣市別平均每戶所得總額",
        "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/008-%E5%90%84%E7%B8%A3%E5%B8%82%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E6%89%80%E5%BE%97%E7%B8%BD%E9%A1%8D.csv",
        "-元",
        "-元",
    )

    # https://data.gov.tw/dataset/9418
    年_plot(
        plots,
        "家庭收支調查-各縣市別平均每戶所得收入總計",
        "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/009-%E5%90%84%E7%B8%A3%E5%B8%82%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E6%89%80%E5%BE%97%E6%94%B6%E5%85%A5%E7%B8%BD%E8%A8%88.csv",
        "-元",
        "-元 所得收入=所得總額-自用住宅及其他營建物設算折舊",
    )

    # https://data.gov.tw/dataset/9804
    年_plot(
        plots,
        "家庭收支調查-各縣市別平均每戶非消費支出",
        "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/021-%E5%90%84%E7%B8%A3%E5%B8%82%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E9%9D%9E%E6%B6%88%E8%B2%BB%E6%94%AF%E5%87%BA.csv",
        "-元",
        "-元",
    )

    # https://data.gov.tw/dataset/9415
    年_plot(
        plots,
        "家庭收支調查-各縣市別平均每戶可支配所得",
        "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/006-%E5%90%84%E7%B8%A3%E5%B8%82%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E5%8F%AF%E6%94%AF%E9%85%8D%E6%89%80%E5%BE%97.csv",
        "-元",
        "-元 可支配所得=所得收入-非消費支出",
    )

    # https://data.gov.tw/dataset/9420
    年_plot(
        plots,
        "家庭收支調查-各縣市別平均每戶消費支出",
        "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/011-%E5%90%84%E7%B8%A3%E5%B8%82%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E6%B6%88%E8%B2%BB%E6%94%AF%E5%87%BA.csv",
        "-元",
        "-元",
    )

    # https://data.gov.tw/dataset/9805
    年_plot(
        plots,
        "家庭收支調查-各縣市別平均每戶經常性支出",
        "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/022-%E5%90%84%E7%B8%A3%E5%B8%82%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E7%B6%93%E5%B8%B8%E6%80%A7%E6%94%AF%E5%87%BA.csv",
        "-元",
        "-元 經常性支出=消費支出+非消費支出",
    )

    # https://data.gov.tw/dataset/9416
    年_plot(
        plots,
        "家庭收支調查-各縣市別平均每戶儲蓄",
        "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/007-%E5%90%84%E7%B8%A3%E5%B8%82%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E5%84%B2%E8%93%84.csv",
        "-元",
        "-元 儲蓄=所得收入-經常性支出=可支配所得-消費支出",
    )

    # ===================================================================

    # https://data.gov.tw/dataset/117930
    年_plot(
        plots,
        "家庭收支調查-所得收入者各縣市別平均每人所得收入總計",
        "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/081-%E6%89%80%E5%BE%97%E6%94%B6%E5%85%A5%E8%80%85%E5%90%84%E7%B8%A3%E5%B8%82%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E4%BA%BA%E6%89%80%E5%BE%97%E6%94%B6%E5%85%A5%E7%B8%BD%E8%A8%88.csv",
        "-元",
        "-元",
    )

    # https://data.gov.tw/dataset/119937
    年_plot(
        plots,
        "家庭收支調查-所得收入者各縣市別平均每人非消費支出",
        "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/083-%E6%89%80%E5%BE%97%E6%94%B6%E5%85%A5%E8%80%85%E5%90%84%E7%B8%A3%E5%B8%82%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E4%BA%BA%E9%9D%9E%E6%B6%88%E8%B2%BB%E6%94%AF%E5%87%BA.csv",
        "-元",
        "-元",
    )

    # https://data.gov.tw/dataset/117929
    年_plot(
        plots,
        "家庭收支調查-所得收入者各縣市別平均每人可支配所得",
        "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/080-%E6%89%80%E5%BE%97%E6%94%B6%E5%85%A5%E8%80%85%E5%90%84%E7%B8%A3%E5%B8%82%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E4%BA%BA%E5%8F%AF%E6%94%AF%E9%85%8D%E6%89%80%E5%BE%97.csv",
        "-元",
        "-元 可支配所得=所得收入-非消費支出",
    )

    # https://www.stat.gov.tw/cp.aspx?n=2773
    key = "家庭部門平均每戶資產負債"
    url = "https://ws.dgbas.gov.tw/001/Upload/463/relfile/10315/2515/112%E8%A1%A87.xlsx"

    r = requests.get(url, verify=False)
    df = pd.read_excel(
        io.BytesIO(r.content), engine="calamine", skiprows=2, nrows=19, usecols=range(0, 6)
    )
    df.columns = ["種類", "2019", "2020", "2021", "2022", "2023"]
    df["種類"] = df["種類"].str.replace(r"[\n \r]", "", regex=True)
    df = df.dropna().set_index("種類")
    # 負債轉為負值
    df.iloc[13:16] = -df.iloc[13:16]
    title = f"{key} {df.columns[0]}~{df.columns[-1]} （土地按市價重評價）（新臺幣萬元）"
    df = df.T

    dataList = []
    for name in df.columns[::-1]:
        if name in [
            "一、非金融性資產淨額",
            "二、金融性資產淨值",
            "(二)國內金融性資產淨值(A-B)",
            "(二)-1國內金融性資產淨值(不含人壽保險及退休基金準備)",
            "國內金融性資產(A)",
            "(減):國內金融性負債(B)",
            "三-1、淨值(不含人壽保險及退休基金準備)",
        ]:
            continue
        elif name in ["三、淨值"]:
            data = {
                "type": "scatter",
                "name": name,
                "x": df.index,
                "y": df[name],
            }
            dataList.append(data)
        else:
            data = {
                "type": "bar",
                "name": name,
                "x": df.index,
                "y": df[name],
            }
            dataList.append(data)

    layout = {
        "title": {"text": title},
        "hovermode": "x",
        "barmode": "stack",
    }
    layout = mergeDict(layout, default_layout)

    graph = {"data": dataList, "layout": layout}
    plots[f"{key}"] = json.dumps(graph, cls=plotly.utils.PlotlyJSONEncoder)

    # ============================================================

    # https://data.gov.tw/dataset/6742
    key = "全國賦稅收入實徵淨額日曆年別-按稅目別與地區別分"
    url = "https://web02.mof.gov.tw/njswww/webMain.aspx?sys=220&ym=9000&kind=21&type=5&funid=i0424&cycle=41&outmode=12&compmode=00&outkind=2&fldspc=0,30,&codspc0=0,39,40,3,&utf=1"

    key = re.sub(rstr, "_", key)

    df = read_csv(url)

    split = df["地區別"].str.split("/", expand=True)
    df["時間"] = split[0].str.strip()
    df["地區"] = split[1].str.strip()

    df_all = df.drop(["地區別"], axis=1).pivot_table(
        index="時間", columns="地區", sort=False, aggfunc="sum", fill_value=0
    )

    taxes = df_all.columns.get_level_values(0).unique().tolist()
    regions = df_all.columns.get_level_values(1).unique().tolist()

    num_traces = len(df_all.columns)  # 總線條數
    buttons_taxes = [
        {
            "args": [
                {
                    "visible": [True] * num_traces,
                }
            ],  # 顯示所有線條
            "label": "全部稅目",
            "method": "restyle",
        }
    ]
    # 加入個別稅目選項
    for tax in taxes:
        arr = [col[0] == tax for col in df_all.columns]  # 依稅目篩選

        buttons_taxes.append(
            {
                "args": [
                    {
                        "visible": arr,
                    }
                ],
                "label": tax,
                "method": "restyle",
            },
        )

    buttons_regions = [
        {
            "args": [
                {
                    "visible": [True] * num_traces,
                }
            ],  # 顯示所有線條
            "label": "全部地區",
            "method": "restyle",
        }
    ]
    for region in regions:
        arr = [col[1] == region for col in df_all.columns]  # 依地區篩選
        buttons_regions.append(
            {
                "args": [
                    {
                        "visible": arr,
                    }
                ],
                "label": region,
                "method": "restyle",
            },
        )

    updatemenus = [
        {
            "x": 0.02,
            "y": 0.98,
            "xanchor": "left",
            "yanchor": "bottom",
            "pad": {"r": 10, "t": 10},
            "buttons": buttons_taxes,
            "type": "dropdown",
            "direction": "down",
            "active": 0,
            "font": {"color": "#AAAAAA"},
            "name": "稅目選擇",
        },
        {
            "x": 0.32,
            "y": 0.98,
            "xanchor": "left",
            "yanchor": "bottom",
            "pad": {"r": 10, "t": 10},
            "buttons": buttons_regions,
            "type": "dropdown",
            "direction": "down",
            "active": 0,
            "font": {"color": "#AAAAAA"},
            "name": "地區選擇",
        },
    ]

    df_all.columns = [f"{region}_{tax}" for region, tax in df_all.columns]

    df_all_year = df_all.filter(regex=r"\d+年$", axis="index")
    graph = plotLine(df_all_year, f"{key}_年 {df_all_year.index[0]}~{df_all_year.index[-1]}")
    graph = mergeDict(json.loads(graph), {"layout": {"updatemenus": updatemenus}})
    plots[f"{key}_年"] = json.dumps(graph)

    df_all_month = df_all.filter(regex=r"\d+年 *\d+月$", axis="index")
    graph = plotLine(df_all_month, f"{key}_月 {df_all_month.index[0]}~{df_all_month.index[-1]}")
    graph = mergeDict(json.loads(graph), {"layout": {"updatemenus": updatemenus}})
    plots[f"{key}_月"] = json.dumps(graph)

    # https://data.gov.tw/dataset/16910
    key = "全國賦稅收入實徵淨額與預算數之比較"
    url = "https://web02.mof.gov.tw/njswww/webMain.aspx?sys=220&ym=9000&kind=21&type=3&funid=i3451&cycle=4&outmode=12&compmode=00&outkind=3&fldlst=111&codspc0=0,34,&utf=1"

    key = re.sub(rstr, "_", key)

    df = read_csv(url)
    split = df["項目別"].str.split("/", expand=True)
    df["時間"] = split[0].str.strip()
    df["類別"] = split[1].str.strip()

    columns = df.drop(["項目別", "時間", "類別"], axis=1).columns
    columns_revise = columns.str.replace(rstr, "_", regex=True)
    items[key] = columns_revise

    data = df.drop(["項目別"], axis=1)
    for values, col in zip(columns, columns_revise):
        df_item = data.pivot_table(
            index="時間", columns="類別", values=values, sort=False, aggfunc="sum", fill_value=0
        )

        plots[f"{key}_{col}"] = plotLine(
            df_item, f"{key}_{col} {df_item.index[0]}~{df_item.index[-1]}"
        )

    # ============================================================

    # https://data.gov.tw/dataset/15392
    年月混合_plot(
        plots,
        "出口貿易值_按洲別 ∕ 國別分(CY2001~)",
        "https://service.mof.gov.tw/public/data/statistic/trade/u2010ex.csv",
        "Exports to Country (Unit: US$ Thousand)",
        r"\(千元\)",
        "(千元 Unit: US$ Thousand)",
        encoding="big5",
    )
    年月混合_plot(
        plots,
        "進口貿易值_按洲別 ∕ 國別分(CY2001~)",
        "https://service.mof.gov.tw/public/data/statistic/trade/u2010im.csv",
        "Imports from Country (Unit : US$ Thousand)",
        r"\(千元\)",
        "(千元 Unit: US$ Thousand)",
        encoding="big5",
    )

    # https://data.gov.tw/dataset/28573
    key = "進出口貿易值_按國際商品統一分類制度(HS)及主要國別分"
    url = "https://web02.mof.gov.tw/njswww/webMain.aspx?sys=220&ym=9000&kind=21&type=4&funid=i9901&cycle=41&outmode=12&compmode=00&outkind=1&fldspc=0,1,3,4,&codlst0=11&codspc1=0,20,&utf=1"

    key = re.sub(rstr, "_", key)

    df = read_csv(url)
    df = df.set_index("國家別")
    df.columns = pd.MultiIndex.from_tuples(
        [[s.strip() for s in col.split("/")] for col in df.columns.str.replace("(千美元)", "")]
    )
    num_traces = len(df.columns)  # 總線條數

    countries = df.columns.get_level_values(0).unique().tolist()
    buttons_countries = [
        {
            "args": [
                {
                    "visible": [True] * num_traces,
                }
            ],  # 顯示所有線條
            "label": "全部國家",
            "method": "restyle",
        }
    ]
    for country in countries:
        arr = [col[0] == country for col in df.columns]

        buttons_countries.append(
            {
                "args": [
                    {
                        "visible": arr,
                    }
                ],
                "label": country,
                "method": "restyle",
            },
        )

    exports = df.columns.get_level_values(1).unique().tolist()
    buttons_exports = [
        {
            "args": [
                {
                    "visible": [True] * num_traces,
                }
            ],  # 顯示所有線條
            "label": "進出口",
            "method": "restyle",
        }
    ]
    for export in exports:
        arr = [col[1] == export for col in df.columns]
        buttons_exports.append(
            {
                "args": [
                    {
                        "visible": arr,
                    }
                ],
                "label": export,
                "method": "restyle",
            },
        )

    kinds = df.columns.get_level_values(2).unique().tolist()
    buttons_kinds = [
        {
            "args": [
                {
                    "visible": [True] * num_traces,
                }
            ],  # 顯示所有線條
            "label": "全部種類",
            "method": "restyle",
        }
    ]
    for kind in kinds:
        arr = [col[2] == kind for col in df.columns]
        buttons_kinds.append(
            {
                "args": [
                    {
                        "visible": arr,
                    }
                ],
                "label": kind,
                "method": "restyle",
            },
        )

    updatemenus = [
        {
            "x": 0.02,
            "y": 0.98,
            "xanchor": "left",
            "yanchor": "bottom",
            "pad": {"r": 10, "t": 10},
            "buttons": buttons_countries,
            "type": "dropdown",
            "direction": "down",
            "active": 0,
            "font": {"color": "#AAAAAA"},
            "name": "國家選擇",
        },
        {
            "x": 0.18,
            "y": 0.98,
            "xanchor": "left",
            "yanchor": "bottom",
            "pad": {"r": 10, "t": 10},
            "buttons": buttons_exports,
            "type": "dropdown",
            "direction": "down",
            "active": 0,
            "font": {"color": "#AAAAAA"},
            "name": "進出口選擇",
        },
        {
            "x": 0.3,
            "y": 0.98,
            "xanchor": "left",
            "yanchor": "bottom",
            "pad": {"r": 10, "t": 10},
            "buttons": buttons_kinds,
            "type": "dropdown",
            "direction": "down",
            "active": 0,
            "font": {"color": "#AAAAAA"},
            "name": "種類選擇",
        },
    ]

    df.columns = [f"{country}{export}{kind}" for country, export, kind in df.columns]

    df_year = df.filter(regex=r"\d+年$", axis="index")
    graph = plotLine(df_year, f"{key}_年(千美元) {df_year.index[0]}~{df_year.index[-1]}")
    graph = mergeDict(json.loads(graph), {"layout": {"updatemenus": updatemenus}})
    plots[f"{key}_年"] = json.dumps(graph)

    df_month = df.filter(regex=r"\d+年 *\d+月$", axis="index")
    graph = plotLine(df_month, f"{key}_月(千美元) {df_month.index[0]}~{df_month.index[-1]}")
    graph = mergeDict(json.loads(graph), {"layout": {"updatemenus": updatemenus}})
    plots[f"{key}_月"] = json.dumps(graph)

    # https://data.gov.tw/dataset/8380
    年月混合_plot(
        plots,
        "出口值_按主要貨品分",
        "https://web02.mof.gov.tw/njswww/webMain.aspx?sys=220&ym=9000&kind=21&type=4&funid=i8121&cycle=41&outmode=12&compmode=00&outkind=1&fld0=1&codlst0=1101111010100011110111100111110110100&utf=1",
        "幣別",
        r"按美元計算\(百萬美元\)\/ ",
        "按美元計算(百萬美元)",
    )

    # https://data.gov.tw/dataset/8388
    年月混合_plot(
        plots,
        "貿易指數－進口單位價值指數",
        "https://web02.mof.gov.tw/njswww/webMain.aspx?sys=220&ym=10000&kind=21&type=4&funid=i9303&cycle=41&outmode=12&compmode=00&outkind=11&fldspc=0,6,7,1,9,1,11,7,20,1,28,2,&utf=1",
        "貨品別",
        r"",
        "(採連鎖法，參考年為110年)",
    )

    # https://data.gov.tw/dataset/8386
    年月混合_plot(
        plots,
        "貿易指數－出口單位價值指數",
        "https://web02.mof.gov.tw/njswww/webMain.aspx?sys=220&ym=10000&kind=21&type=4&funid=i9302&cycle=41&outmode=12&compmode=00&outkind=11&fldspc=0,8,9,7,17,1,25,2,&utf=1",
        "貨品別",
        r"",
        "(採連鎖法，參考年為110年)",
    )

    # https://data.gov.tw/dataset/8387
    年月混合_plot(
        plots,
        "貿易指數－出口數量指數",
        "https://web02.mof.gov.tw/njswww/webMain.aspx?sys=220&ym=10000&kind=21&type=4&funid=i9304&cycle=41&outmode=12&compmode=00&outkind=11&fldspc=0,8,9,7,17,1,25,2,&utf=1",
        "貨品別",
        r"",
        "(參考年為110年)",
    )

    # https://data.gov.tw/dataset/8389
    年月混合_plot(
        plots,
        "貿易指數－進口數量指數",
        "https://web02.mof.gov.tw/njswww/webMain.aspx?sys=220&ym=10000&kind=21&type=4&funid=i9305&cycle=41&outmode=12&compmode=00&outkind=11&fldspc=0,6,7,1,9,1,11,7,20,1,28,2,&utf=1",
        "貨品別",
        r"",
        "(參考年為110年)",
    )

    # =======================================================================

    # https://data.gov.tw/dataset/41236
    key = "全國公立動物收容所收容處理情形統計表"
    url = "https://data.moa.gov.tw/Service/OpenData/TransService.aspx?UnitId=DyplMIk3U1hf&IsTransData=1"
    key = re.sub(rstr, "_", key)
    df = read_json(url)
    df = df.rename(
        columns={
            "rpt_year": "年度",
            "rpt_country_code": "縣市代碼",
            "rpt_country": "縣市名",
            "rpt_month": "月份",
            "accept_num": "收容隻數_隻",
            "adopt_num": "認領養數_隻",
            "adopt_rate": "認領養率_%",
            "adopt_total": "總認養數",
            "end_num": "人道處理數_隻",
            "end_rate": "人道處理率_%",
            "dead_num": "所內死亡數_隻",
            "dead_rate": "所內死亡率_%",
        },
    )
    df = df.drop(["ID", "縣市代碼"], axis=1)
    df["認領養率_%"] = df["認領養率_%"].str.rstrip("%").astype("float") / 100.0
    df["人道處理率_%"] = df["人道處理率_%"].str.rstrip("%").astype("float") / 100.0
    df["所內死亡率_%"] = df["所內死亡率_%"].str.rstrip("%").astype("float") / 100.0

    columns = df.drop(["年度", "月份", "縣市名"], axis=1).columns
    columns_revise = columns.str.replace(rstr, "_", regex=True)
    items[key] = columns_revise

    data = df
    for values, col in zip(columns, columns_revise):
        df_item = df.pivot_table(
            index="年度",
            columns="縣市名",
            values=values,
            sort=False,
            aggfunc="sum",
            fill_value=0,
        )
        plots[f"{key}_{col}"] = plotLine(
            df_item, f"{key}_{col} {df_item.index[-1]}~{df_item.index[0]}"
        )

    # ========================================================================

    prefix = "TW_Analysis"
    path = os.path.join(os.path.dirname(__file__), "report")
    with app.app_context():
        jsfolder = f"{prefix}"
        os.makedirs(os.path.join(path, jsfolder), exist_ok=True)

        for key, item in plots.items():
            graph = render_template("graph.js.j2", key=key, item=item)
            with open(os.path.join(path, f"{jsfolder}/{key}.js"), "w", encoding="UTF-8") as f:
                f.write(graph)

        html = render_template(
            "tw_analysis.html.j2",
            plots=plots,
            jsfolder=jsfolder,
            title=f"{prefix} Report",
            items=items,
        )
        with open(os.path.join(path, f"{prefix}_Report.html"), "w", encoding="UTF-8") as f:
            f.write(html)
