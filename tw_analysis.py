import json
import os
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


def read_csv(url):
    r = requests.get(url, verify=False)

    df = pd.read_csv(io.BytesIO(r.content))

    return df


default_layout = {
    "height": 600,
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


if __name__ == "__main__":
    plots = {}

    # 消費者物價基本分類指數
    # https://data.gov.tw/dataset/6019
    df = read_xml(
        "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/230555/pr0101a1m.xml", "//Obs"
    )
    patt = "(指數基期：民國110年=100)"
    df["Item"] = df["Item"].str.replace(patt, "")

    df_value = df.drop(df[df["TYPE"] == "年增率(%)"].index)
    pivot_df = df_value.pivot_table(
        index="TIME_PERIOD", columns="Item", values="Item_VALUE", sort=False
    )
    plots["消費者物價基本分類指數_原始值"] = plotLine(
        pivot_df, f"消費者物價基本分類指數 原始值 {patt}"
    )

    df_value = df.drop(df[df["TYPE"] == "原始值"].index)
    pivot_df = df_value.pivot_table(
        index="TIME_PERIOD", columns="Item", values="Item_VALUE", sort=False
    )
    plots["消費者物價基本分類指數_年增率"] = plotLine(
        pivot_df, f"消費者物價基本分類指數 年增率(%) {patt}"
    )

    # 家庭收支調查-所得收入者各縣市別平均每人所得收入總計
    # https://data.gov.tw/dataset/117930
    df = read_csv(
        "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/081-%E6%89%80%E5%BE%97%E6%94%B6%E5%85%A5%E8%80%85%E5%90%84%E7%B8%A3%E5%B8%82%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E4%BA%BA%E6%89%80%E5%BE%97%E6%94%B6%E5%85%A5%E7%B8%BD%E8%A8%88.csv"
    )
    df = df.set_index("年")
    patt = "-元"
    df.columns = df.columns.str.replace(patt, "")
    plots["家庭收支調查_所得收入者各縣市別平均每人所得收入總計"] = plotLine(
        df, f"家庭收支調查-所得收入者各縣市別平均每人所得收入總計{patt}"
    )

    # 家庭收支調查-各縣市別平均每戶所得總額
    # https://data.gov.tw/dataset/9417
    df = read_csv(
        "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/008-%E5%90%84%E7%B8%A3%E5%B8%82%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E6%89%80%E5%BE%97%E7%B8%BD%E9%A1%8D.csv"
    )
    df = df.set_index("年")
    patt = "-元"
    df.columns = df.columns.str.replace(patt, "")
    plots["家庭收支調查_各縣市別平均每戶所得總額"] = plotLine(
        df, f"家庭收支調查-各縣市別平均每戶所得總額{patt}"
    )

    # 生產者物價基本分類指數
    # https://data.gov.tw/dataset/148439
    df = read_xml(
        "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/230534/pr0701a1m.xml", "//Obs"
    )
    patt = "(指數基期：民國110年=100)"
    df["Item"] = df["Item"].str.replace(patt, "")

    df_value = df.drop(df[df["TYPE"] == "年增率(%)"].index)
    pivot_df = df_value.pivot_table(
        index="TIME_PERIOD", columns="Item", values="Item_VALUE", sort=False
    )
    plots["生產者物價基本分類指數_原始值"] = plotLine(
        pivot_df, f"生產者物價基本分類指數 原始值 {patt}"
    )

    df_value = df.drop(df[df["TYPE"] == "原始值"].index)
    pivot_df = df_value.pivot_table(
        index="TIME_PERIOD", columns="Item", values="Item_VALUE", sort=False
    )
    plots["生產者物價基本分類指數_年增率"] = plotLine(
        pivot_df, f"生產者物價基本分類指數 年增率(%) {patt}"
    )

    # 家庭收支調查-所得收入者五等分位平均每人可支配所得
    # https://data.gov.tw/dataset/132285
    df = read_csv(
        "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/089-%E6%89%80%E5%BE%97%E6%94%B6%E5%85%A5%E8%80%85%E4%BA%94%E7%AD%89%E5%88%86%E4%BD%8D%E5%B9%B3%E5%9D%87%E6%AF%8F%E4%BA%BA%E5%8F%AF%E6%94%AF%E9%85%8D%E6%89%80%E5%BE%97.csv"
    )
    df = df.set_index("年")
    patt = "可支配所得按所得收入者人數五等分位組之平均每人可支配所得-"
    df.columns = df.columns.str.replace(patt, "")
    plots["家庭收支調查_所得收入者五等分位平均每人可支配所得"] = plotLine(
        df, f"家庭收支調查-所得收入者五等分位平均每人可支配所得"
    )

    # 國民所得統計-國民所得、儲蓄與投資-季
    # https://data.gov.tw/dataset/44232
    df = read_xml(
        "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/230514/na8201a1q.xml", "//Obs"
    )
    df = df.fillna(0)
    patt = ""
    df["Item"] = df["Item"].str.replace(patt, "")

    df_value = df.drop(df[df["TYPE"] == "年增率(%)"].index)
    pivot_df = df_value.pivot_table(
        index="TIME_PERIOD", columns="Item", values="Item_VALUE", sort=False
    )
    plots["國民所得統計_國民所得_儲蓄與投資_季_原始值"] = plotLine(
        pivot_df, f"國民所得統計-國民所得、儲蓄與投資-季 原始值 {patt}"
    )

    df_value = df.drop(df[df["TYPE"] == "原始值"].index)
    pivot_df = df_value.pivot_table(
        index="TIME_PERIOD", columns="Item", values="Item_VALUE", sort=False
    )
    plots["國民所得統計_國民所得_儲蓄與投資_季_年增率"] = plotLine(
        pivot_df, f"國民所得統計-國民所得、儲蓄與投資-季 年增率(%) {patt}"
    )

    # 國民所得統計-常用資料-季
    # https://data.gov.tw/dataset/6799
    df = read_xml(
        "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/230514/na8101a1q.xml", "//Obs"
    )
    patt = ""
    df["Item"] = df["Item"].str.replace(patt, "")

    df_value = df.drop(df[df["TYPE"] == "年增率(%)"].index)
    pivot_df = df_value.pivot_table(
        index="TIME_PERIOD", columns="Item", values="Item_VALUE", sort=False
    )
    plots["國民所得統計_常用資料_季_原始值"] = plotLine(
        pivot_df, f"國民所得統計-常用資料-季 原始值 {patt}"
    )

    df_value = df.drop(df[df["TYPE"] == "原始值"].index)
    pivot_df = df_value.pivot_table(
        index="TIME_PERIOD", columns="Item", values="Item_VALUE", sort=False
    )
    plots["國民所得統計_常用資料_季_年增率"] = plotLine(
        pivot_df, f"國民所得統計-常用資料-季 年增率(%) {patt}"
    )

    # 躉售物價基本分類指數
    # https://data.gov.tw/dataset/8239
    df = read_xml(
        "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/230546/pr0202a1m.xml", "//Obs"
    )
    patt = ""
    df["Item"] = df["Item"].str.replace(patt, "")

    df_value = df.drop(df[df["TYPE"] == "年增率(%)"].index)
    pivot_df = df_value.pivot_table(
        index="TIME_PERIOD", columns="Item", values="Item_VALUE", sort=False
    )
    plots["躉售物價基本分類指數_原始值"] = plotLine(pivot_df, f"躉售物價基本分類指數 原始值 {patt}")

    df_value = df.drop(df[df["TYPE"] == "原始值"].index)
    pivot_df = df_value.pivot_table(
        index="TIME_PERIOD", columns="Item", values="Item_VALUE", sort=False
    )
    plots["躉售物價基本分類指數_年增率"] = plotLine(
        pivot_df, f"躉售物價基本分類指數 年增率(%) {patt}"
    )

    # 家庭收支調查-所得收入者各縣市別平均每人可支配所得
    # https://data.gov.tw/dataset/132285
    df = read_csv(
        "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/080-%E6%89%80%E5%BE%97%E6%94%B6%E5%85%A5%E8%80%85%E5%90%84%E7%B8%A3%E5%B8%82%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E4%BA%BA%E5%8F%AF%E6%94%AF%E9%85%8D%E6%89%80%E5%BE%97.csv"
    )
    df = df.set_index("年")
    patt = "可支配所得按所得收入者人數五等分位組之平均每人可支配所得-"
    df.columns = df.columns.str.replace(patt, "")
    plots["家庭收支調查_所得收入者各縣市別平均每人可支配所得"] = plotLine(
        df, f"家庭收支調查-所得收入者各縣市別平均每人可支配所得"
    )

    # 家庭收支調查-戶數五等分位之平均每戶經常性支出
    # https://data.gov.tw/dataset/101338
    df = read_csv(
        "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/072-%E6%88%B6%E6%95%B8%E4%BA%94%E7%AD%89%E5%88%86%E4%BD%8D%E7%B5%84%E4%B9%8B%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E7%B6%93%E5%B8%B8%E6%80%A7%E6%94%AF%E5%87%BA.csv"
    )
    df = df.set_index("年")
    patt = "可支配所得按戶數五等分位組之平均每戶經常性支出-"
    df.columns = df.columns.str.replace(patt, "")
    plots["家庭收支調查_戶數五等分位之平均每戶經常性支出"] = plotLine(
        df, f"家庭收支調查-戶數五等分位之平均每戶經常性支出"
    )

    # 家庭收支調查-戶數五等分位之平均每戶所得總額
    # https://data.gov.tw/dataset/93951
    df = read_csv(
        "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/067-%E6%88%B6%E6%95%B8%E4%BA%94%E7%AD%89%E5%88%86%E4%BD%8D%E7%B5%84%E4%B9%8B%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E6%89%80%E5%BE%97%E7%B8%BD%E9%A1%8D.csv"
    )
    df = df.set_index("年")
    patt = "可支配所得按戶數五等分位組之平均每戶所得總額-"
    df.columns = df.columns.str.replace(patt, "")
    plots["家庭收支調查_戶數五等分位之平均每戶所得總額"] = plotLine(
        df, f"家庭收支調查-戶數五等分位之平均每戶所得總額"
    )

    # 家庭收支調查-戶數五等分位之平均每戶可支配所得
    # https://data.gov.tw/dataset/91925
    df = read_csv(
        "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/065-%E6%88%B6%E6%95%B8%E4%BA%94%E7%AD%89%E5%88%86%E4%BD%8D%E7%B5%84%E4%B9%8B%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E5%8F%AF%E6%94%AF%E9%85%8D%E6%89%80%E5%BE%97.csv"
    )
    df = df.set_index("年")
    patt = "可支配所得按戶數五等分位組-"
    df.columns = df.columns.str.replace(patt, "")
    plots["家庭收支調查_戶數五等分位之平均每戶可支配所得"] = plotLine(
        df, f"家庭收支調查-戶數五等分位之平均每戶可支配所得"
    )

    # 家庭收支調查-家庭組織型態別平均每戶經常性支出
    # https://data.gov.tw/dataset/79208
    df = read_csv(
        "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/063-%E5%AE%B6%E5%BA%AD%E7%B5%84%E7%B9%94%E5%9E%8B%E6%85%8B%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E7%B6%93%E5%B8%B8%E6%80%A7%E6%94%AF%E5%87%BA.csv"
    )
    df = df.set_index("年")
    patt = "-元"
    df.columns = df.columns.str.replace(patt, "")
    plots["家庭收支調查_家庭組織型態別平均每戶經常性支出"] = plotLine(
        df, f"家庭收支調查-家庭組織型態別平均每戶經常性支出{patt}"
    )

    # 家庭收支調查-家庭組織型態別平均每戶所得總額
    # https://data.gov.tw/dataset/54825
    df = read_csv(
        "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/058-%E5%AE%B6%E5%BA%AD%E7%B5%84%E7%B9%94%E5%9E%8B%E6%85%8B%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E6%89%80%E5%BE%97%E7%B8%BD%E9%A1%8D.csv"
    )
    df = df.set_index("年")
    patt = "-元"
    df.columns = df.columns.str.replace(patt, "")
    plots["家庭收支調查_家庭組織型態別平均每戶所得總額"] = plotLine(
        df, f"家庭收支調查-家庭組織型態別平均每戶所得總額{patt}"
    )

    # 家庭收支調查-家庭組織型態別平均每戶可支配所得
    # https://data.gov.tw/dataset/45126
    df = read_csv(
        "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/056-%E5%AE%B6%E5%BA%AD%E7%B5%84%E7%B9%94%E5%9E%8B%E6%85%8B%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E5%8F%AF%E6%94%AF%E9%85%8D%E6%89%80%E5%BE%97.csv"
    )
    df = df.set_index("年")
    patt = "-元"
    df.columns = df.columns.str.replace(patt, "")
    plots["家庭收支調查_家庭組織型態別平均每戶可支配所得"] = plotLine(
        df, f"家庭收支調查-家庭組織型態別平均每戶可支配所得{patt}"
    )

    # 家庭收支調查-各縣市別平均每戶可支配所得
    # https://data.gov.tw/dataset/9415
    df = read_csv(
        "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/006-%E5%90%84%E7%B8%A3%E5%B8%82%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E5%8F%AF%E6%94%AF%E9%85%8D%E6%89%80%E5%BE%97.csv"
    )
    df = df.set_index("年")
    patt = "-元"
    df.columns = df.columns.str.replace(patt, "")
    plots["家庭收支調查_各縣市別平均每戶可支配所得"] = plotLine(
        df, f"家庭收支調查-各縣市別平均每戶可支配所得{patt}"
    )

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
            "tw_analysis.html.j2", plots=plots, jsfolder=jsfolder, title=f"{prefix} Report"
        )
        with open(os.path.join(path, f"{prefix}_Report.html"), "w", encoding="UTF-8") as f:
            f.write(html)
