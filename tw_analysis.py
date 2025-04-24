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


rstr = r"[- ,、()~∕\/－]+"


def 原始值_年增率_plot(plots, key, url, xpath, item_remove_patt, title_suffix, fillna=False):
    key = re.sub(rstr, "_", key)

    df = read_xml(url, xpath)
    if fillna:
        df = df.fillna(0)
    df["Item"] = df["Item"].str.replace(item_remove_patt, "", regex=True)

    df_value = df.drop(df[df["TYPE"] == "年增率(%)"].index)
    pivot_df = df_value.pivot_table(
        index="TIME_PERIOD", columns="Item", values="Item_VALUE", sort=False
    )
    plots[f"{key}_原始值"] = plotLine(pivot_df, f"{key} 原始值 {title_suffix}")

    df_value = df.drop(df[df["TYPE"] == "原始值"].index)
    pivot_df = df_value.pivot_table(
        index="TIME_PERIOD", columns="Item", values="Item_VALUE", sort=False
    )
    plots[f"{key}_年增率"] = plotLine(pivot_df, f"{key} 年增率(%) {title_suffix}")


def 年_plot(plots, key, url, columns_remove_patt, title_suffix):
    key = re.sub(rstr, "_", key)

    df = read_csv(url)
    df = df.set_index("年")
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)
    plots[key] = plotLine(df, f"{key}{title_suffix}")


def 年地區_plot(plots, key, url, index, columns, columns_remove_patt, title_suffix):
    key = re.sub(rstr, "_", key)

    df = read_csv(url)
    df = df.pivot(index=index, columns=columns)
    df.columns = [f"{value_type}_{region}" for value_type, region in df.columns]
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)
    plots[key] = plotLine(df, f"{key}{title_suffix}")


def 年月混合_plot(plots, key, url, index, columns_remove_patt, title_suffix, encoding="utf-8"):
    key = re.sub(rstr, "_", key)

    df = read_csv(url, encoding)
    df = df.set_index(index)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    plots[f"{key}_年"] = plotLine(
        df.filter(regex=r"\d+年$", axis="index"), f"{key}_年{title_suffix}"
    )
    plots[f"{key}_月"] = plotLine(
        df.filter(regex=r"\d+年 *\d+月$", axis="index"), f"{key}_月{title_suffix}"
    )


def 年月地區混合_plot(
    plots,
    key,
    url,
    split_column_name,
    split_pat,
    columns_remove_patt,
    title_suffix,
    encoding="utf-8",
):
    key = re.sub(rstr, "_", key)

    df = read_csv(url, encoding)
    split = df[split_column_name].str.split(split_pat, expand=True)
    df["時間"] = split[0].str.strip()
    df["地區"] = split[1].str.strip()
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)
    df = df.drop(["地區別"], axis=1)
    df = df.pivot(index="時間", columns="地區")
    df.columns = [f"{value_type}_{region}" for value_type, region in df.columns]

    plots[f"{key}_年"] = plotLine(
        df.filter(regex=r"\d+年$", axis="index"), f"{key}_年{title_suffix}"
    )
    plots[f"{key}_月"] = plotLine(
        df.filter(regex=r"\d+年 *\d+月$", axis="index"), f"{key}_月{title_suffix}"
    )


def 年種類混合_plot(
    plots,
    key,
    url,
    split_column_name,
    split_pat,
    columns_remove_patt,
    title_suffix,
    encoding="utf-8",
):
    key = re.sub(rstr, "_", key)

    df = read_csv(url, encoding)
    split = df[split_column_name].str.split(split_pat, expand=True)
    df["時間"] = split[0].str.strip()
    df["類別"] = split[1].str.strip()
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)
    df = df.drop(["項目別"], axis=1)
    df = df.pivot(index="時間", columns="類別")
    df.columns = [f"{value_type}_{kind}" for value_type, kind in df.columns]

    plots[f"{key}"] = plotLine(df, f"{key}{title_suffix}")


if __name__ == "__main__":
    plots = {}

    # https://data.gov.tw/dataset/6019
    原始值_年增率_plot(
        plots,
        "消費者物價基本分類指數",
        "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/230555/pr0101a1m.xml",
        "//Obs",
        r"\(指數基期：民國110年=100\)",
        "(指數基期：民國110年=100)",
    )

    # https://data.gov.tw/dataset/148439
    原始值_年增率_plot(
        plots,
        "生產者物價基本分類指數",
        "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/230534/pr0701a1m.xml",
        "//Obs",
        r"\(指數基期：民國110年=100\)",
        "(指數基期：民國110年=100)",
    )

    # https://data.gov.tw/dataset/8239
    原始值_年增率_plot(
        plots,
        "躉售物價基本分類指數",
        "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/230546/pr0202a1m.xml",
        "//Obs",
        r"\(民國105年=100\)",
        "(指數基期：民國105年=100)",
    )

    # https://data.gov.tw/dataset/117930
    年_plot(
        plots,
        "家庭收支調查-所得收入者各縣市別平均每人所得收入總計",
        "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/081-%E6%89%80%E5%BE%97%E6%94%B6%E5%85%A5%E8%80%85%E5%90%84%E7%B8%A3%E5%B8%82%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E4%BA%BA%E6%89%80%E5%BE%97%E6%94%B6%E5%85%A5%E7%B8%BD%E8%A8%88.csv",
        "-元",
        "-元",
    )

    # https://data.gov.tw/dataset/9417
    年_plot(
        plots,
        "家庭收支調查-各縣市別平均每戶所得總額",
        "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/008-%E5%90%84%E7%B8%A3%E5%B8%82%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E6%89%80%E5%BE%97%E7%B8%BD%E9%A1%8D.csv",
        "-元",
        "-元",
    )

    # https://data.gov.tw/dataset/132285
    年_plot(
        plots,
        "家庭收支調查-所得收入者五等分位平均每人可支配所得",
        "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/089-%E6%89%80%E5%BE%97%E6%94%B6%E5%85%A5%E8%80%85%E4%BA%94%E7%AD%89%E5%88%86%E4%BD%8D%E5%B9%B3%E5%9D%87%E6%AF%8F%E4%BA%BA%E5%8F%AF%E6%94%AF%E9%85%8D%E6%89%80%E5%BE%97.csv",
        "可支配所得按所得收入者人數五等分位組之平均每人可支配所得-",
        "",
    )

    # https://data.gov.tw/dataset/44232
    原始值_年增率_plot(
        plots,
        "國民所得統計-國民所得、儲蓄與投資-季",
        "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/230514/na8201a1q.xml",
        "//Obs",
        "",
        "",
        fillna=True,
    )

    # https://data.gov.tw/dataset/6799
    原始值_年增率_plot(
        plots,
        "國民所得統計-常用資料-季",
        "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/230514/na8101a1q.xml",
        "//Obs",
        "",
        "",
    )

    # https://data.gov.tw/dataset/132285
    年_plot(
        plots,
        "家庭收支調查-所得收入者各縣市別平均每人可支配所得",
        "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/080-%E6%89%80%E5%BE%97%E6%94%B6%E5%85%A5%E8%80%85%E5%90%84%E7%B8%A3%E5%B8%82%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E4%BA%BA%E5%8F%AF%E6%94%AF%E9%85%8D%E6%89%80%E5%BE%97.csv",
        "-元",
        "-元",
    )

    # https://data.gov.tw/dataset/101338
    年_plot(
        plots,
        "家庭收支調查-戶數五等分位之平均每戶經常性支出",
        "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/072-%E6%88%B6%E6%95%B8%E4%BA%94%E7%AD%89%E5%88%86%E4%BD%8D%E7%B5%84%E4%B9%8B%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E7%B6%93%E5%B8%B8%E6%80%A7%E6%94%AF%E5%87%BA.csv",
        "可支配所得按戶數五等分位組之平均每戶經常性支出-|-元",
        "-元",
    )

    # https://data.gov.tw/dataset/93951
    年_plot(
        plots,
        "家庭收支調查-戶數五等分位之平均每戶所得總額",
        "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/067-%E6%88%B6%E6%95%B8%E4%BA%94%E7%AD%89%E5%88%86%E4%BD%8D%E7%B5%84%E4%B9%8B%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E6%89%80%E5%BE%97%E7%B8%BD%E9%A1%8D.csv",
        "可支配所得按戶數五等分位組之平均每戶所得總額-|-元",
        "-元",
    )

    # https://data.gov.tw/dataset/91925
    年_plot(
        plots,
        "家庭收支調查-戶數五等分位之平均每戶可支配所得",
        "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/065-%E6%88%B6%E6%95%B8%E4%BA%94%E7%AD%89%E5%88%86%E4%BD%8D%E7%B5%84%E4%B9%8B%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E5%8F%AF%E6%94%AF%E9%85%8D%E6%89%80%E5%BE%97.csv",
        "可支配所得按戶數五等分位組-|-元",
        "-元",
    )

    # https://data.gov.tw/dataset/79208
    年_plot(
        plots,
        "家庭收支調查-家庭組織型態別平均每戶經常性支出",
        "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/063-%E5%AE%B6%E5%BA%AD%E7%B5%84%E7%B9%94%E5%9E%8B%E6%85%8B%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E7%B6%93%E5%B8%B8%E6%80%A7%E6%94%AF%E5%87%BA.csv",
        "-元",
        "-元",
    )

    # https://data.gov.tw/dataset/54825
    年_plot(
        plots,
        "家庭收支調查-家庭組織型態別平均每戶所得總額",
        "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/058-%E5%AE%B6%E5%BA%AD%E7%B5%84%E7%B9%94%E5%9E%8B%E6%85%8B%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E6%89%80%E5%BE%97%E7%B8%BD%E9%A1%8D.csv",
        "-元",
        "-元",
    )

    # https://data.gov.tw/dataset/45126
    年_plot(
        plots,
        "家庭收支調查-家庭組織型態別平均每戶可支配所得",
        "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/056-%E5%AE%B6%E5%BA%AD%E7%B5%84%E7%B9%94%E5%9E%8B%E6%85%8B%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E5%8F%AF%E6%94%AF%E9%85%8D%E6%89%80%E5%BE%97.csv",
        "-元",
        "-元",
    )

    # https://data.gov.tw/dataset/9415
    年_plot(
        plots,
        "家庭收支調查-各縣市別平均每戶可支配所得",
        "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/006-%E5%90%84%E7%B8%A3%E5%B8%82%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E5%8F%AF%E6%94%AF%E9%85%8D%E6%89%80%E5%BE%97.csv",
        "-元",
        "-元",
    )

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
    年月混合_plot(
        plots,
        "進出口貿易值_按國際商品統一分類制度(HS)及主要國別分",
        "https://web02.mof.gov.tw/njswww/webMain.aspx?sys=220&ym=9000&kind=21&type=4&funid=i9901&cycle=41&outmode=12&compmode=00&outkind=1&fldspc=0,1,3,4,&codlst0=11&codspc1=0,20,&utf=1",
        "國家別",
        r"\(千美元\)",
        "(千美元)",
    )

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

    # https://data.gov.tw/dataset/6742
    年月地區混合_plot(
        plots,
        "全國賦稅收入實徵淨額日曆年別-按稅目別與地區別分",
        "https://web02.mof.gov.tw/njswww/webMain.aspx?sys=220&ym=9000&kind=21&type=5&funid=i0424&cycle=41&outmode=12&compmode=00&outkind=2&fldspc=0,30,&codspc0=0,39,40,3,&utf=1",
        "地區別",
        "/",
        r"",
        "",
    )

    # https://data.gov.tw/dataset/16910
    年種類混合_plot(
        plots,
        "全國賦稅收入實徵淨額與預算數之比較",
        "https://web02.mof.gov.tw/njswww/webMain.aspx?sys=220&ym=9000&kind=21&type=3&funid=i3451&cycle=4&outmode=12&compmode=00&outkind=3&fldlst=111&codspc0=0,34,&utf=1",
        "項目別",
        "/",
        r"",
        "",
    )

    # https://data.gov.tw/dataset/28402
    年地區_plot(
        plots,
        "營利事業所得稅結算申報統計(縣市)",
        "https://www.fia.gov.tw/download/66006e8e34f748ada8faa1dcebc9a242",
        "'資料年度'",
        "'縣市代碼'",
        r"",
        "",
    )

    # https://data.gov.tw/dataset/41236
    key = "全國公立動物收容所收容處理情形統計表"
    url = "https://data.moa.gov.tw/Service/OpenData/TransService.aspx?UnitId=DyplMIk3U1hf&IsTransData=1"
    key = re.sub(rstr, "_", key)
    df = read_json(url)
    df = df.drop(["ID", "rpt_country_code"], axis=1)
    df = df.pivot(index=["rpt_year", "rpt_month"], columns="rpt_country")
    df.index = [f"{year}_{month}" for year, month in df.index]
    df.columns = [f"{value_type}_{region}" for value_type, region in df.columns]
    plots[key] = plotLine(df, f"{key}")

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
            "tw_analysis.html.j2", plots=plots, jsfolder=jsfolder, title=f"{prefix} Report"
        )
        with open(os.path.join(path, f"{prefix}_Report.html"), "w", encoding="UTF-8") as f:
            f.write(html)
