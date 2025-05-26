import gzip
import json
import os
import re
import pandas as pd
import io
import plotly
import requests
import copy

from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from datetime import datetime
from typing import Optional, Dict, Any
from pathlib import Path
from flask import Flask, render_template

app = Flask(__name__)

# Regular expression for sanitizing strings for use as keys or filenames
FILENAME_SANITIZE_PATTERN = r'[- ,、()~∕\/－%*?:"<>|（）]+'
# Base directory for caching downloaded data
EXTRA_DATA_DIR = Path("./extraData/TW_Analysis")

# define the retry strategy
retry_strategy = Retry(
    total=4,  # maximum number of retries
    backoff_factor=2,
    status_forcelist=[
        429,
        500,
        502,
        503,
        504,
    ],  # the HTTP status codes to retry on
)

# create an HTTP adapter with the retry strategy and mount it to the session
adapter = HTTPAdapter(max_retries=retry_strategy)

# create a new session object
session = requests.Session()
session.headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 6.1; rv:57.0) " "Gecko/20100101 Firefox/57.0",
}
session.mount("http://", adapter)
session.mount("https://", adapter)

# --- Helper Functions ---


def sanitize_filename(name: str) -> str:
    """Sanitizes a string to be used as a valid filename or key."""
    return re.sub(FILENAME_SANITIZE_PATTERN, "_", name)


def _ensure_dir_exists(path: Path):
    """Ensures that the directory for the given path exists."""
    path.parent.mkdir(parents=True, exist_ok=True)


def read_xml(url: str, xpath: str) -> pd.DataFrame:
    r = session.get(url, verify=False)
    df = pd.read_xml(io.BytesIO(r.content), xpath=xpath)

    return df


def read_csv(url: str, encoding: str = "utf-8") -> pd.DataFrame:
    r = session.get(url, verify=False)
    df = pd.read_csv(io.BytesIO(r.content), encoding=encoding)

    return df


def read_csv_with_cache(path: Path, url: str, encoding: str = "utf-8") -> pd.DataFrame:
    _ensure_dir_exists(path)

    if not path.is_file():
        r = session.get(url, verify=False)
        with gzip.open(path, "wb") as f:
            f.write(r.content)

    df = pd.read_csv(path, compression="gzip", encoding=encoding)

    return df


def read_json(url: str, encoding: str = "utf-8") -> pd.DataFrame:
    r = session.get(url, verify=False)

    df = pd.read_json(io.BytesIO(r.content), encoding=encoding)

    return df


def read_excel_with_cache(
    path: Path, url: str, skiprows=None, nrows=None, usecols=None
) -> pd.DataFrame:
    _ensure_dir_exists(path)

    if not path.is_file():
        r = session.get(url, verify=False)
        with gzip.open(path, "wb") as f:
            f.write(r.content)

    with gzip.open(path, "rb") as f_gz:
        # Read the gzipped content into BytesIO for pandas
        excel_bytes = io.BytesIO(f_gz.read())
    df = pd.read_excel(
        excel_bytes, engine="calamine", skiprows=skiprows, nrows=nrows, usecols=usecols
    )  # Use openpyxl

    return df


def merge_dict(a: Dict, b: Dict, path: Optional[list] = None, overwrite: bool = True) -> Dict:
    """Merges b into a. If overwrite is True, b's values will overwrite a's on conflict."""
    if path is None:
        path = []
    for key in b:
        if key in a:
            if isinstance(a[key], dict) and isinstance(b[key], dict):
                merge_dict(a[key], b[key], path + [str(key)], overwrite)
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


theme_template = plotly.io.templates["plotly_dark"].to_plotly_json()
default_template = merge_dict(
    theme_template,
    {
        "layout": {
            "height": 600,
            "margin": {"b": 135},
            # "autosize": False,
            "title": {"font": {"family": "Times New Roman"}, "x": 0.05, "y": 0.9},
            "font": {"family": "Courier New", "color": "#ffffff"},  # White font for dark background
            "xaxis": {
                "tickfont": {"family": "Courier New", "size": 14},
                "automargin": True,
                # "gridcolor": "#222",
                # "zerolinecolor": "#ccc",
                # "linecolor": "#ccc",
            },
            "yaxis": {
                "tickfont": {"family": "Courier New"},
                "automargin": True,
                # "gridcolor": "#222",
                # "zerolinecolor": "#ccc",
                # "linecolor": "#ccc",
            },
            # "plot_bgcolor": "#000",  # Black plot background
            # "paper_bgcolor": "#000",  # Black paper background
            # "legend": {"font": {"color": "#ffffff"}},  # White legend text
        }
    },
)


def plotly_json_dump(graph_dict: Dict) -> str:
    """Serializes a Plotly graph dictionary to JSON string."""
    return json.dumps(graph_dict, cls=plotly.utils.PlotlyJSONEncoder)


def plot_line(
    df: pd.DataFrame, title: Optional[str] = None, additional_layout: Optional[Dict] = None
) -> str:
    data_list = []
    for name in df.columns:
        data = {
            "type": "scatter",
            "name": name,
            "x": df.index.tolist(),  # Ensure x-axis is list for JSON
            "y": df[name].tolist(),  # Ensure y-axis is list for JSON
            "mode": "lines",
        }
        data_list.append(data)

    layout = {
        "title": {"text": title},
        "hovermode": "x",
        "xaxis": {
            "type": "category"
        },  # Ensure x-axis type is category for discrete values if needed
    }
    if additional_layout:
        layout = merge_dict(layout, additional_layout)

    graph = {"data": data_list, "layout": layout}
    graph = merge_dict(copy.deepcopy(default_template), graph)

    return plotly_json_dump(graph)


def plot_bar(
    df: pd.DataFrame, title: Optional[str] = None, additional_layout: Optional[Dict] = None
) -> str:
    data_list = []
    for name in df.columns:
        data = {
            "type": "bar",
            "name": name,
            "x": [name],
            "y": df[name].tolist(),
        }
        data_list.append(data)

    layout = {
        "title": {"text": title},
        "hovermode": "x",  # "closest" might be better for bar charts
        "yaxis": {"tickformat": ".2%"},  # Default, can be overridden by additional_layout
    }
    if additional_layout:
        layout = merge_dict(layout, additional_layout)

    graph = {"data": data_list, "layout": layout}
    graph = merge_dict(copy.deepcopy(default_template), graph)

    return plotly_json_dump(graph)


def plot_bar_group(
    df: pd.DataFrame, title: Optional[str] = None, additional_layout: Optional[Dict] = None
) -> str:
    data_list = []
    for name in df.columns:
        data = {
            "type": "bar",
            "name": name,
            "x": df.index.tolist(),
            "y": df[name].tolist(),
        }
        data_list.append(data)

    layout = {
        "title": {"text": title},
        "hovermode": "x",
        "barmode": "group",  # Explicitly set barmode for grouped bars
    }
    if additional_layout:
        layout = merge_dict(layout, additional_layout)

    graph = {"data": data_list, "layout": layout}
    graph = merge_dict(copy.deepcopy(default_template), graph)

    return plotly_json_dump(graph)


def plot_bar_stack_multi_index(
    df: pd.DataFrame, title: Optional[str] = None, additional_layout: Optional[Dict] = None
) -> str:
    data_list = []
    for name in df.columns:
        data = {
            "type": "bar",
            "name": name,
            "x": list(zip(*df.index.tolist())),
            "y": df[name].tolist(),
        }
        data_list.append(data)

    layout = {
        "title": {"text": title},
        "hovermode": "x",
        "barmode": "stack",
    }
    if additional_layout:
        layout = merge_dict(layout, additional_layout)

    graph = {"data": data_list[::-1], "layout": layout}
    graph = merge_dict(copy.deepcopy(default_template), graph)

    return plotly_json_dump(graph)


def plot_lines_bars(
    df: pd.DataFrame,
    lines_left_axis: list[str],
    lines_right_axis: list[str],
    bars: list[str],
    title: Optional[str] = None,
    additional_layout: Optional[Dict] = None,
    legendgroup: bool = False,
    sort: bool = True,
):
    data_list = []
    for name in lines_left_axis:
        data = {
            "type": "scatter",
            "name": name,
            "x": df.index.tolist(),
            "y": df[name].tolist(),
            "mode": "lines",
        }
        if legendgroup:
            data["legendgroup"] = str.rsplit(name, "_", 1)[1]
        data_list.append(data)

    for name in lines_right_axis:
        data = {
            "type": "scatter",
            "name": name,
            "x": df.index.tolist(),
            "y": df[name].tolist(),
            "mode": "lines",
            "yaxis": "y2",
        }
        if legendgroup:
            data["legendgroup"] = str.rsplit(name, "_", 1)[1]
        data_list.append(data)

    for name in bars:
        data = {
            "type": "bar",
            "name": name,
            "x": df.index.tolist(),
            "y": df[name].tolist(),
            "mode": "lines",
        }
        if legendgroup:
            data["legendgroup"] = str.rsplit(name, "_", 1)[1]
        data_list.append(data)
    if sort:
        data_list.sort(key=lambda x: str.rsplit(x["name"], "_", 1)[1])

    layout = {
        "title": {"text": title},
        "hovermode": "x unified",
        "xaxis": {"type": "category"},
        "yaxis2": {
            "overlaying": "y",
            "side": "right",
        },
    }

    if additional_layout:
        layout = merge_dict(layout, additional_layout)

    graph = {"data": data_list, "layout": layout}
    graph = merge_dict(copy.deepcopy(default_template), graph)

    return plotly_json_dump(graph)


# --- Specific Data Processing and Plotting Functions ---


def index_原始值_年增率_plot(
    plots: Dict,
    key: str,
    url: str,
    xpath: str,
    item_remove_patt: str,
    title_suffix: str,
    additional_layout: Optional[Dict] = None,
):
    key = sanitize_filename(key)

    df = read_xml(url, xpath)
    df["Item"] = df["Item"].str.replace(item_remove_patt, "", regex=True)
    date_range = f"{df["TIME_PERIOD"].iloc[0]}~{df["TIME_PERIOD"].iloc[-1]}"

    pivot_df = df[df["TYPE"] == "原始值"].pivot_table(
        index="TIME_PERIOD", columns="Item", values="Item_VALUE", sort=False
    )
    plots[f"{key}_原始值"] = plot_line(
        pivot_df, f"{key} 原始值 {title_suffix} {date_range}", additional_layout
    )

    def irr(x):
        x = x.dropna()

        val = (x.iloc[-1] / x.iloc[0]) ** (365 / (x.index[-1] - x.index[0]).days) - 1
        return val

    df["TIME_PERIOD_Datetime"] = df["TIME_PERIOD"].apply(lambda x: datetime.strptime(x, "%YM%m"))
    pivot_df_datetime = df[df["TYPE"] == "原始值"].pivot_table(
        index="TIME_PERIOD_Datetime", columns="Item", values="Item_VALUE", sort=False
    )
    irr_df = pivot_df_datetime.apply(irr, axis=0).to_frame()
    irr_df.columns = ["IRR"]
    irr_df = irr_df.T
    plots[f"{key}_IRR"] = plot_bar(
        irr_df, f"{key} IRR(%) {title_suffix} {date_range}", additional_layout
    )

    pivot_df = df[df["TYPE"] == "年增率(%)"].pivot_table(
        index="TIME_PERIOD", columns="Item", values="Item_VALUE", sort=False
    )
    additional_layout_年增率 = {"yaxis": {"tickformat": ".2%"}}
    if additional_layout:
        additional_layout_年增率 = merge_dict(additional_layout_年增率, additional_layout)
    plots[f"{key}_年增率"] = plot_line(
        pivot_df / 100,
        f"{key} 年增率(%) {title_suffix} {date_range}",
        additional_layout_年增率,
    )


def 年_plot(
    plots: Dict,
    key: str,
    url: str,
    columns_remove_patt: str,
    title_suffix: str,
    index_col: str = "年",
    additional_layout: Optional[Dict] = None,
):
    key = sanitize_filename(key)

    df = read_csv(url)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)
    plots[key] = plot_line(
        df, f"{key}{title_suffix} {df.index[0]}~{df.index[-1]}", additional_layout
    )


def 年月混合_plot(
    plots: Dict,
    key: str,
    url: str,
    index_col: str,
    columns_remove_patt: str,
    title_suffix: str,
    encoding: str = "utf-8",
    additional_layout: Optional[Dict] = None,
):
    key = sanitize_filename(key)

    df = read_csv(url, encoding)
    df = df.loc[(df[df.columns[1:]] != "－").any(axis=1)]
    df[df.columns[1:]] = df[df.columns[1:]].astype(float)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    df_year = df.filter(regex=r"\d+年$", axis="index")
    plots[f"{key}_年"] = plot_line(
        df_year, f"{key}_年{title_suffix} {df_year.index[0]}~{df_year.index[-1]}", additional_layout
    )
    df_month = df.filter(regex=r"\d+年 *\d+月$", axis="index")
    plots[f"{key}_月"] = plot_line(
        df_month,
        f"{key}_月{title_suffix} {df_month.index[0]}~{df_month.index[-1]}",
        additional_layout,
    )


if __name__ == "__main__":
    plots: Dict[str, str] = {}  # Stores Plotly JSON strings
    items: Dict[str, Any] = {}  # Stores other items like column lists for templates

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

    key = sanitize_filename(key)

    df = read_xml(url, xpath)
    df = df.fillna(0)

    pivot_df = df[df["TYPE"] == "原始值"].pivot_table(
        index="TIME_PERIOD", columns="Item", values="Item_VALUE", sort=False
    )

    pat_filter = "當期價格(新臺幣百萬元)"
    df_filter = pivot_df[[column for column in pivot_df.columns if pat_filter in column]]
    df_filter.columns = df_filter.columns.str.replace(f"{pat_filter}、", "")
    plots[f"{key}_原始值_當期價格"] = plot_line(
        df_filter,
        f"{key} 原始值 {pat_filter} {df_filter.index[0]}~{df_filter.index[-1]}",
        {"hovermode": "x unified"},
    )

    pat_filter = "連鎖實質值(2021為參考年_新臺幣百萬元)"
    df_filter = pivot_df[[column for column in pivot_df.columns if pat_filter in column]]
    df_filter.columns = df_filter.columns.str.replace(f"{pat_filter}、", "")
    plots[f"{key}_原始值_連鎖實質值"] = plot_line(
        df_filter,
        f"{key} 原始值 {pat_filter} {df_filter.index[0]}~{df_filter.index[-1]}",
        {"hovermode": "x unified"},
    )

    pat_filter = "平減指數(2021年=100)"
    df_filter = pivot_df[[column for column in pivot_df.columns if pat_filter in column]]
    df_filter.columns = df_filter.columns.str.replace(f"{pat_filter}、", "")
    plots[f"{key}_原始值_平減指數"] = plot_line(
        df_filter,
        f"{key} 原始值 {pat_filter} {df_filter.index[0]}~{df_filter.index[-1]}",
        {"hovermode": "x unified"},
    )

    pivot_df = df[df["TYPE"] == "年增率(%)"].pivot_table(
        index="TIME_PERIOD", columns="Item", values="Item_VALUE", sort=False
    )

    pat_filter = "當期價格(新臺幣百萬元)"
    df_filter = pivot_df[[column for column in pivot_df.columns if pat_filter in column]]
    df_filter.columns = df_filter.columns.str.replace(f"{pat_filter}、", "")
    plots[f"{key}_年增率_當期價格"] = plot_line(
        df_filter / 100,
        f"{key} 年增率(%) {pat_filter} {df_filter.index[0]}~{df_filter.index[-1]}",
        {"yaxis": {"tickformat": ".2%"}},
    )

    pat_filter = "連鎖實質值(2021為參考年_新臺幣百萬元)"
    df_filter = pivot_df[[column for column in pivot_df.columns if pat_filter in column]]
    df_filter.columns = df_filter.columns.str.replace(f"{pat_filter}、", "")
    plots[f"{key}_年增率_連鎖實質值"] = plot_line(
        df_filter / 100,
        f"{key} 年增率(%) {pat_filter} {df_filter.index[0]}~{df_filter.index[-1]}",
        {"yaxis": {"tickformat": ".2%"}},
    )

    pat_filter = "平減指數(2021年=100)"
    df_filter = pivot_df[[column for column in pivot_df.columns if pat_filter in column]]
    df_filter.columns = df_filter.columns.str.replace(f"{pat_filter}、", "")
    plots[f"{key}_年增率_平減指數"] = plot_line(
        df_filter / 100,
        f"{key} 年增率(%) {pat_filter} {df_filter.index[0]}~{df_filter.index[-1]}",
        {"yaxis": {"tickformat": ".2%"}},
    )

    # https://data.gov.tw/dataset/6799
    key = "國民所得統計-常用資料-季"
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/230514/na8101a1q.xml"
    xpath = "//Obs"

    key = sanitize_filename(key)

    df = read_xml(url, xpath)

    df_value = df.drop(df[df["TYPE"] == "年增率(%)"].index)
    pivot_df = df_value.pivot_table(
        index="TIME_PERIOD", columns="Item", values="Item_VALUE", sort=False
    )
    plots[f"{key}_原始值"] = plot_line(
        pivot_df,
        f"{key} 原始值 {pivot_df.index[0]}~{pivot_df.index[-1]}",
        {"hovermode": "x unified"},
    )

    df_value = df.drop(df[df["TYPE"] == "原始值"].index)
    pivot_df = df_value.pivot_table(
        index="TIME_PERIOD", columns="Item", values="Item_VALUE", sort=False
    )
    plots[f"{key}_年增率"] = plot_line(
        pivot_df / 100,
        f"{key} 年增率(%) {pivot_df.index[0]}~{pivot_df.index[-1]}",
        {"yaxis": {"tickformat": ".2%"}, "hovermode": "x unified"},
    )

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
        "平均每人可支配所得=平均每位所得收入者可支配所得",
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
        "-元 可支配所得=所得收入-非消費支出, 平均每人可支配所得=平均每位所得收入者可支配所得",
    )

    # https://www.stat.gov.tw/cp.aspx?n=2773
    key = "家庭部門平均每戶資產負債"
    url = "https://ws.dgbas.gov.tw/001/Upload/463/relfile/10315/2515/112%E8%A1%A87.xlsx"

    df = read_excel_with_cache(
        EXTRA_DATA_DIR / f"{key}.xlsx.gz", url, skiprows=2, nrows=19, usecols=range(0, 6)
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
        "hovermode": "x unified",
        "barmode": "stack",
    }

    graph = {"data": dataList, "layout": layout}
    graph = merge_dict(copy.deepcopy(default_template), graph)

    plots[f"{key}"] = plotly_json_dump(graph)

    # https://data.gov.tw/dataset/155869
    # https://data.gov.tw/dataset/156379
    key = "企業ESG資訊揭露彙總資料-人力發展"
    key = sanitize_filename(key)
    url_上市 = "https://mopsfin.twse.com.tw/opendata/t187ap46_L_5.csv"
    url_上櫃 = "https://mopsfin.twse.com.tw/opendata/t187ap46_O_5.csv"

    year = 112
    df_上市 = read_csv_with_cache(EXTRA_DATA_DIR / key / f"{year}_上市.csv.gz", url_上市)
    df_上櫃 = read_csv_with_cache(EXTRA_DATA_DIR / key / f"{year}_上櫃.csv.gz", url_上櫃)

    df = pd.concat([df_上市, df_上櫃])
    df["公司"] = df["公司代號"].astype(str) + "_" + df["公司名稱"]
    df = df.set_index("公司")
    df_薪資 = df[
        [
            "非擔任主管職務之全時員工薪資平均數(仟元/人)",
            "非擔任主管之全時員工薪資中位數(仟元/人)",
        ]
    ]
    df_薪資 = (
        df_薪資.rename(
            columns={
                "非擔任主管職務之全時員工薪資平均數(仟元/人)": "平均數",
                "非擔任主管之全時員工薪資中位數(仟元/人)": "中位數",
            }
        )
        * 1000
    )
    plots[f"{key}_非擔任主管職務之全時員工薪資"] = plot_bar_group(
        df_薪資,
        f"{key}_非擔任主管職務之全時員工薪資 {year}年",
    )
    plots[f"{key}_非擔任主管職務之全時員工薪資_排序"] = plot_bar_group(
        df_薪資.sort_values(["中位數", "平均數"]).dropna(),
        f"{key}_非擔任主管職務之全時員工薪資_排序 {year}年",
    )

    df_職災 = df[
        [
            "職業災害人數及比率-人數",
            "職業災害人數及比率-比率",
        ]
    ]
    df_職災 = df_職災.rename(
        columns={
            "職業災害人數及比率-人數": "人數",
            "職業災害人數及比率-比率": "比率",
        }
    )
    df_職災["比率"] = df_職災["比率"].str.removesuffix("%").astype(float) / 100.0
    plots[f"{key}_職業災害人數及比率"] = plot_lines_bars(
        df_職災,
        lines_left_axis=[],
        lines_right_axis=["比率"],
        bars=["人數"],
        title=f"{key}_職業災害人數及比率 {year}年",
        sort=False,
        additional_layout={"yaxis2": {"title": {"text": "比率(%)"}}},
    )
    plots[f"{key}_職業災害人數及比率_排序"] = plot_lines_bars(
        df_職災.sort_values(["人數", "比率"]),
        lines_left_axis=[],
        lines_right_axis=["比率"],
        bars=["人數"],
        title=f"{key}_職業災害人數及比率_排序 {year}年",
        sort=False,
        additional_layout={"yaxis2": {"title": {"text": "比率(%)"}}},
    )

    # https://data.gov.tw/dataset/17963
    key = "綜稅總所得各縣市申報統計分析表"
    key = sanitize_filename(key)
    url = "https://www.fia.gov.tw/WEB/fia/ias/ias{year}/{year}_8-1.csv"

    df = []
    lastyear = 110
    for year in range(101, lastyear + 1):
        path = EXTRA_DATA_DIR / key / f"{year}.csv.gz"
        data = read_csv_with_cache(path, url.format(year=year))
        data["年度"] = year
        data = data.rename(
            columns={
                "鄉鎮市區": "縣市別",
                "\ufeff縣市別": "縣市別",
                "\ufeff鄉鎮市區": "縣市別",
                "總所得": "綜合所得總額",
            }
        )
        df.append(data)

    df = pd.concat(df, ignore_index=True, axis="index")

    sorted = "中位數"
    df_縣市別 = df[df["年度"] == lastyear].set_index("縣市別").sort_values(sorted)
    data_list = []
    for name in df_縣市別.index:
        data = {
            "type": "box",
            "name": name,
            "x": [name],
            "q1": [df_縣市別.loc[name, "第一分位數"] * 1000],
            "median": [df_縣市別.loc[name, "中位數"] * 1000],
            "q3": [df_縣市別.loc[name, "第三分位數"] * 1000],
            "mean": [df_縣市別.loc[name, "平均數"] * 1000],
            "sd": [df_縣市別.loc[name, "標準差"] * 1000],
            "lowerfence": [],
            "upperfence": [],
        }
        data_list.append(data)

    layout = {
        "title": {"text": f"{key} {sorted}排序 {lastyear}年"},
        "hovermode": "x",
    }
    graph = {"data": data_list, "layout": layout}
    graph = merge_dict(copy.deepcopy(default_template), graph)

    plots[f"{key}"] = plotly_json_dump(graph)

    # https://data.gov.tw/dataset/103066
    key = "綜稅綜合所得總額全國各縣市鄉鎮村里統計分析表"
    key = sanitize_filename(key)
    url = "https://www.fia.gov.tw/WEB/fia/ias/ias{year}/{year}_165-9.csv"

    df = []
    lastyear = 110
    for year in range(101, lastyear + 1):
        path = EXTRA_DATA_DIR / key / f"{year}.csv.gz"
        data = read_csv_with_cache(path, url.format(year=year))
        data["年度"] = year
        data = data.rename(
            columns={"鄉鎮市區": "縣市別", "\ufeff縣市別": "縣市別", "\ufeff鄉鎮市區": "縣市別"}
        )
        df.append(data)

    df = pd.concat(df, ignore_index=True, axis="index")
    df["縣市別村里"] = df["縣市別"] + df["村里"]
    split = df["縣市別"].str.replace("(^.{3})", r"\1|", regex=True).str.split("|", n=1, expand=True)
    df["縣市"] = split[0].str.strip()
    df["鄉鎮"] = split[1].str.strip()

    sorted = "中位數"
    df_縣市別 = df[df["年度"] == lastyear].set_index("縣市別村里").sort_values(sorted)
    data_list = []
    for name in df_縣市別.index:
        data = {
            "type": "box",
            "name": name,
            "x": [name],
            "q1": [df_縣市別.loc[name, "第一分位數"] * 1000],
            "median": [df_縣市別.loc[name, "中位數"] * 1000],
            "q3": [df_縣市別.loc[name, "第三分位數"] * 1000],
            "mean": [df_縣市別.loc[name, "平均數"] * 1000],
            # "sd": [df_縣市別.loc[name, "標準差"] * 1000],
            # "lowerfence": [],
            # "upperfence": [],
        }
        data_list.append(data)

    buttons_regions_detail = [
        {
            "args": [
                {
                    "visible": [True] * len(df_縣市別.index),
                }
            ],  # 顯示所有線條
            "label": "全部地區",
            "method": "restyle",
        }
    ]
    buttons_regions_detail.append(
        {
            "args": [
                {
                    "visible": [
                        df_縣市別.loc[index, "村里"] == "合計" for index in df_縣市別.index
                    ],
                }
            ],
            "label": "合計",
            "method": "restyle",
        },
    )
    regions_detail = df["縣市別"].unique().tolist()
    for region in regions_detail:
        arr = [region in index for index in df_縣市別.index]  # 依地區篩選
        buttons_regions_detail.append(
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

    buttons_regions = [
        {
            "args": [
                {
                    "visible": [True] * len(df_縣市別.index),
                }
            ],  # 顯示所有線條
            "label": "全部地區",
            "method": "restyle",
        }
    ]
    regions = df["縣市"].unique().tolist()
    for region in regions:
        arr = [region in index for index in df_縣市別.index]  # 依地區篩選
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
            "x": 0.6,
            "y": 1.03,
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
        {
            "x": 0.7,
            "y": 1.03,
            "xanchor": "left",
            "yanchor": "bottom",
            "pad": {"r": 10, "t": 10},
            "buttons": buttons_regions_detail,
            "type": "dropdown",
            "direction": "down",
            "active": 0,
            "font": {"color": "#AAAAAA"},
            "name": "地區選擇",
        },
    ]

    layout = {
        "title": {"text": f"{key} {sorted}排序 {lastyear}年"},
        "hovermode": "x",
        "updatemenus": updatemenus,
    }
    graph = {"data": data_list, "layout": layout}
    graph = merge_dict(copy.deepcopy(default_template), graph)

    plots[f"{key}"] = plotly_json_dump(graph)

    # ============================================================

    # https://data.gov.tw/dataset/6742
    key = "全國賦稅收入實徵淨額日曆年別-按稅目別與地區別分"
    url = "https://web02.mof.gov.tw/njswww/webMain.aspx?sys=220&ym=9000&kind=21&type=5&funid=i0424&cycle=41&outmode=12&compmode=00&outkind=2&fldspc=0,30,&codspc0=0,39,40,3,&utf=1"

    key = sanitize_filename(key)

    df = read_csv(url)
    df = df.replace("－", 0.0)
    df[df.columns[1:]] = df[df.columns[1:]].astype(float)

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
    plots[f"{key}_年"] = plot_line(
        df_all_year,
        f"{key}_年 {df_all_year.index[0]}~{df_all_year.index[-1]}",
        {"updatemenus": updatemenus},
    )

    df_all_month = df_all.filter(regex=r"\d+年 *\d+月$", axis="index")
    plots[f"{key}_月"] = plot_line(
        df_all_month,
        f"{key}_月 {df_all_month.index[0]}~{df_all_month.index[-1]}",
        {"updatemenus": updatemenus},
    )

    # https://data.gov.tw/dataset/16910
    key = "全國賦稅收入實徵淨額與預算數之比較"
    url = "https://web02.mof.gov.tw/njswww/webMain.aspx?sys=220&ym=9000&kind=21&type=3&funid=i3451&cycle=4&outmode=12&compmode=00&outkind=3&fldlst=111&codspc0=0,34,&utf=1"

    key = sanitize_filename(key)

    df = read_csv(url)
    df = df.replace("－", 0.0)
    df[df.columns[1:]] = df[df.columns[1:]].astype(float)
    split = df["項目別"].str.split("/", expand=True)
    df["時間"] = split[0].str.strip()
    df["類別"] = split[1].str.strip()

    columns = df.drop(["項目別", "時間", "類別"], axis=1).columns
    columns_revise = columns.map(sanitize_filename)
    items[key] = columns_revise

    data = df.drop(["項目別"], axis=1)
    for values, col in zip(columns, columns_revise):
        df_item = data.pivot_table(
            index="時間", columns="類別", values=values, sort=False, aggfunc="sum", fill_value=0
        )

        plots[f"{key}_{col}"] = plot_line(
            df_item, f"{key}_{col} {df_item.index[0]}~{df_item.index[-1]}"
        )

    # https://www.stat.gov.tw/cp.aspx?n=3914
    key = "主計總處統計專區 家庭收支調查 統計表 常用資料"
    key = sanitize_filename(key)
    url_平均每人月消費 = "https://ws.dgbas.gov.tw/001/Upload/463/relfile/10315/1259/4.xls"
    url_可支配所得平均數 = "https://ws.dgbas.gov.tw/001/Upload/463/relfile/10315/1259/mean.xls"
    url_可支配所得中位數 = "https://ws.dgbas.gov.tw/001/Upload/463/relfile/10315/1259/med.xls"

    def get_data(content: bytes, sheets_num: list[int], skiprows: int) -> pd.DataFrame:

        dfs = []
        for i in sheets_num:
            df = pd.read_excel(
                io.BytesIO(content), engine="calamine", sheet_name=i, skiprows=skiprows
            ).dropna()
            dfs.append(df)
        dfs = pd.concat(dfs, ignore_index=True)

        return dfs

    r = session.get(url_平均每人月消費, verify=False)
    df_平均每人月消費 = get_data(r.content, [0, 1], 1)
    df_平均每人月消費 = df_平均每人月消費.set_index("年別")
    plots[f"{key}_平均每人月消費"] = plot_line(
        df_平均每人月消費,
        f"{key}_平均每人月消費 {df_平均每人月消費.index[0]}~{df_平均每人月消費.index[-1]}",
    )

    r = session.get(url_可支配所得平均數, verify=False)
    df_平均每戶可支配所得 = get_data(r.content, [0, 2], 2)
    df_平均每人可支配所得 = get_data(r.content, [1, 3], 2)
    df_平均每戶可支配所得 = df_平均每戶可支配所得.set_index("年別")
    plots[f"{key}_平均每戶可支配所得"] = plot_line(
        df_平均每戶可支配所得,
        f"{key}_平均每戶可支配所得 {df_平均每戶可支配所得.index[0]}~{df_平均每戶可支配所得.index[-1]}",
    )
    df_平均每人可支配所得 = df_平均每人可支配所得.set_index("年別")
    plots[f"{key}_平均每人可支配所得"] = plot_line(
        df_平均每人可支配所得,
        f"{key}_平均每人可支配所得_每人可支配所得=每戶可支配所得/每戶人數 {df_平均每人可支配所得.index[0]}~{df_平均每人可支配所得.index[-1]}",
    )

    r = session.get(url_可支配所得中位數, verify=False)
    df_每戶可支配所得中位數 = get_data(r.content, [0, 2], 2)
    df_每人可支配所得中位數 = get_data(r.content, [1, 3], 2)
    df_每戶可支配所得中位數 = df_每戶可支配所得中位數.set_index("年別")
    plots[f"{key}_每戶可支配所得中位數"] = plot_line(
        df_每戶可支配所得中位數,
        f"{key}_每戶可支配所得中位數 {df_每戶可支配所得中位數.index[0]}~{df_每戶可支配所得中位數.index[-1]}",
    )
    df_每人可支配所得中位數 = df_每人可支配所得中位數.set_index("年別")
    plots[f"{key}_每人可支配所得中位數"] = plot_line(
        df_每人可支配所得中位數,
        f"{key}_每人可支配所得中位數_每人可支配所得=每戶可支配所得/每戶人數 {df_每人可支配所得中位數.index[0]}~{df_每人可支配所得中位數.index[-1]}",
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

    key = sanitize_filename(key)

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
    plots[f"{key}_年"] = plot_line(
        df_year,
        f"{key}_年(千美元) {df_year.index[0]}~{df_year.index[-1]}",
        {"updatemenus": updatemenus},
    )

    df_month = df.filter(regex=r"\d+年 *\d+月$", axis="index")
    plots[f"{key}_月"] = plot_line(
        df_month,
        f"{key}_月(千美元) {df_month.index[0]}~{df_month.index[-1]}",
        {"updatemenus": updatemenus},
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

    # ========================================================================

    # https://data.gov.tw/dataset/77132
    # API 說明文件
    # https://www.ris.gov.tw/rs-opendata/api/Main/docs/v1
    # API 路徑
    # https://www.ris.gov.tw/rs-opendata/api/v1/datastore/ODRP014/{yyymm} 請指定年月
    key = "村里戶數、單一年齡人口（新增區域代碼）"
    url_year_page = (
        "https://www.ris.gov.tw/rs-opendata/api/v1/datastore/ODRP014/{year}{month:02d}?page={page}"
    )
    key = sanitize_filename(key)
    df = []

    def get_data(year, month, page):
        path = EXTRA_DATA_DIR / key / f"{year}" / f"{year}_{month:02d}_{page}.json.gz"
        _ensure_dir_exists(path)

        if not path.is_file():
            url = url_year_page.format(year=year, month=month, page=page)
            for _ in range(5):
                r = session.get(url, verify=False)
                try:
                    json_data = json.loads(r.content)
                    if "responseData" in json_data:
                        with gzip.open(path, "wb") as f:
                            f.write(r.content)
                        break
                    else:
                        return {}
                except:
                    print("no data", url)
                    continue
            else:
                return {}

        with gzip.open(path, "rb") as f:
            data = json.load(f)

        return data

    year = datetime.today().year - 1911
    month = datetime.today().month
    while True:
        page = 1
        json_data = get_data(year, month, page)
        if "responseData" not in json_data:
            if month == 1:
                year -= 1
                month = 12
            else:
                month -= 1

            if year * 100 + month < 11404:
                raise f"無法獲取資料 {key}"
            continue

        data = pd.json_normalize(json_data["responseData"])
        df.append(data)

        pages = int(json_data["totalPage"])
        for page in range(2, pages + 1):
            json_data = get_data(year, month, page)
            data = pd.json_normalize(json_data["responseData"])
            df.append(data)
        break

    df = pd.concat(df, ignore_index=True)
    df[df.columns[4:]] = df[df.columns[4:]].astype(int)
    split = df["區域別"].str.replace("(^.{3})", r"\1|", regex=True).str.split("|", n=1, expand=True)
    df["縣市"] = split[0].str.strip()
    df["鄉鎮"] = split[1].str.strip()

    df_男_年齡_縣市 = df.pivot_table(
        values=df.columns[8 : 202 + 8 : 2],
        columns="縣市",
        aggfunc="sum",
        sort=False,
    )
    df_男_年齡_縣市.index = df_男_年齡_縣市.index.str.removesuffix("-男")

    df_女_年齡_縣市 = df.pivot_table(
        values=df.columns[9 : 202 + 9 : 2],
        columns="縣市",
        aggfunc="sum",
        sort=False,
    )
    df_女_年齡_縣市.index = df_女_年齡_縣市.index.str.removesuffix("-女")

    df_男女_年齡_縣市 = pd.concat([df_男_年齡_縣市, df_女_年齡_縣市], keys=["男", "女"]).swaplevel()
    plots[f"{key}_年齡_縣市"] = plot_bar_stack_multi_index(
        df_男女_年齡_縣市, f"{key}_年齡_縣市 {year}年{month}月", {"bargap": 0}
    )

    # https://data.gov.tw/dataset/117986
    # API 說明文件
    # https://www.ris.gov.tw/rs-opendata/api/Main/docs/v1
    # API 路徑
    # https://www.ris.gov.tw/rs-opendata/api/v1/datastore/ODRP052/{yyy} 請指定年
    key = "現住人口性別、年齡、婚姻狀況(含同婚)"
    url_year_page = "https://www.ris.gov.tw/rs-opendata/api/v1/datastore/ODRP052/{year}?page={page}"
    key = sanitize_filename(key)
    df = []

    def get_data(year, page):
        path = EXTRA_DATA_DIR / key / f"{year}" / f"{year}_{page}.json.gz"
        _ensure_dir_exists(path)

        if not path.is_file():
            url = url_year_page.format(year=year, page=page)
            for _ in range(5):
                r = session.get(url, verify=False)
                try:
                    json_data = json.loads(r.content)
                    if "responseData" in json_data:
                        with gzip.open(path, "wb") as f:
                            f.write(r.content)
                        break
                    else:
                        return {}
                except:
                    print("no data", url)
                    continue
            else:
                return {}

        with gzip.open(path, "rb") as f:
            data = json.load(f)

        return data

    year = datetime.today().year - 1911
    while True:
        page = 1
        json_data = get_data(year, page)
        if "responseData" not in json_data:
            year -= 1
            if year < 113:
                raise f"無法獲取資料 {key}"
            continue

        data = pd.json_normalize(json_data["responseData"])
        df.append(data)

        pages = int(json_data["totalPage"])
        for page in range(2, pages + 1):
            json_data = get_data(year, page)
            data = pd.json_normalize(json_data["responseData"])
            df.append(data)
        break

    df = pd.concat(df, ignore_index=True)
    df["population"] = df["population"].astype(int)
    split = (
        df["site_id"].str.replace("(^.{3})", r"\1|", regex=True).str.split("|", n=1, expand=True)
    )
    df["縣市"] = split[0].str.strip()
    df["鄉鎮"] = split[1].str.strip()

    df_男_年齡_婚姻_縣市 = df[df["sex"] == "男"].pivot_table(
        values="population",
        index=["marital_status", "age"],
        columns="縣市",
        aggfunc="sum",
        sort=False,
    )
    plots[f"{key}_男_年齡_婚姻_縣市"] = plot_bar_stack_multi_index(
        df_男_年齡_婚姻_縣市, f"{key}_男_年齡_婚姻_縣市 {year}年"
    )

    df_女_年齡_婚姻_縣市 = df[df["sex"] == "女"].pivot_table(
        values="population",
        index=["marital_status", "age"],
        columns="縣市",
        aggfunc="sum",
        sort=False,
    )
    plots[f"{key}_女_年齡_婚姻_縣市"] = plot_bar_stack_multi_index(
        df_女_年齡_婚姻_縣市, f"{key}_女_年齡_婚姻_縣市 {year}年"
    )

    df_男女_年齡_婚姻 = df.pivot_table(
        values="population",
        index="age",
        columns=["sex", "marital_status"],
        aggfunc="sum",
        sort=False,
    )
    df_男女_年齡_婚姻[("男", "單身")] = (
        df_男女_年齡_婚姻[("男", "未婚")]
        + df_男女_年齡_婚姻[("男", "離婚_不同性別")]
        + df_男女_年齡_婚姻[("男", "離婚_相同性別")]
        + df_男女_年齡_婚姻[("男", "喪偶_不同性別")]
        + df_男女_年齡_婚姻[("男", "喪偶_相同性別")]
    )
    df_男女_年齡_婚姻[("女", "單身")] = (
        df_男女_年齡_婚姻[("女", "未婚")]
        + df_男女_年齡_婚姻[("女", "離婚_不同性別")]
        + df_男女_年齡_婚姻[("女", "離婚_相同性別")]
        + df_男女_年齡_婚姻[("女", "喪偶_不同性別")]
        + df_男女_年齡_婚姻[("女", "喪偶_相同性別")]
    )

    df_男女_年齡_未婚 = pd.concat(
        [df_男女_年齡_婚姻[("男", "未婚")], df_男女_年齡_婚姻[("女", "未婚")]],
        axis="columns",
        keys=["男", "女"],
    )
    df_男女_年齡_未婚["女男比"] = df_男女_年齡_未婚["女"] / df_男女_年齡_未婚["男"]
    plots[f"{key}_男女_年齡_未婚"] = plot_lines_bars(
        df_男女_年齡_未婚,
        lines_left_axis=[],
        lines_right_axis=["女男比"],
        bars=["女", "男"],
        title=f"{key}_男女_年齡_未婚 {year}年",
        sort=False,
    )

    df_男女_年齡_單身 = pd.concat(
        [df_男女_年齡_婚姻[("男", "單身")], df_男女_年齡_婚姻[("女", "單身")]],
        axis="columns",
        keys=["男", "女"],
    )
    df_男女_年齡_單身["女男比"] = df_男女_年齡_單身["女"] / df_男女_年齡_單身["男"]
    plots[f"{key}_男女_年齡_單身"] = plot_lines_bars(
        df_男女_年齡_單身,
        lines_left_axis=[],
        lines_right_axis=["女男比"],
        bars=["女", "男"],
        title=f"{key}_男女_年齡_單身(含離婚、喪偶) {year}年",
        sort=False,
    )

    df_男女_年齡_婚姻_縣市 = df.pivot_table(
        values="population",
        index="age",
        columns=["sex", "marital_status", "縣市"],
        aggfunc="sum",
        sort=False,
    )
    regions = df["縣市"].unique().tolist()
    for region in regions:
        df_男女_年齡_婚姻_縣市[("男", "單身", region)] = (
            df_男女_年齡_婚姻_縣市[("男", "未婚", region)]
            + df_男女_年齡_婚姻_縣市[("男", "離婚_不同性別", region)]
            + df_男女_年齡_婚姻_縣市[("男", "離婚_相同性別", region)]
            + df_男女_年齡_婚姻_縣市[("男", "喪偶_不同性別", region)]
            + df_男女_年齡_婚姻_縣市[("男", "喪偶_相同性別", region)]
        )
        df_男女_年齡_婚姻_縣市[("女", "單身", region)] = (
            df_男女_年齡_婚姻_縣市[("女", "未婚", region)]
            + df_男女_年齡_婚姻_縣市[("女", "離婚_不同性別", region)]
            + df_男女_年齡_婚姻_縣市[("女", "離婚_相同性別", region)]
            + df_男女_年齡_婚姻_縣市[("女", "喪偶_不同性別", region)]
            + df_男女_年齡_婚姻_縣市[("女", "喪偶_相同性別", region)]
        )

    df_男女_年齡_未婚_縣市 = pd.concat(
        sum(
            [
                [
                    df_男女_年齡_婚姻_縣市[("男", "未婚", region)],
                    df_男女_年齡_婚姻_縣市[("女", "未婚", region)],
                ]
                for region in regions
            ],
            [],
        ),
        axis="columns",
        keys=sum([[f"男_{region}", f"女_{region}"] for region in regions], []),
    )
    for region in regions:
        df_男女_年齡_未婚_縣市[f"女男比_{region}"] = (
            df_男女_年齡_未婚_縣市[f"女_{region}"] / df_男女_年齡_未婚_縣市[f"男_{region}"]
        )

    plots[f"{key}_男女_年齡_未婚_縣市"] = plot_lines_bars(
        df_男女_年齡_未婚_縣市,
        lines_left_axis=[],
        lines_right_axis=[f"女男比_{region}" for region in regions],
        bars=sum([[f"女_{region}", f"男_{region}"] for region in regions], []),
        title=f"{key}_男女_年齡_未婚_縣市 {year}年",
        legendgroup=True,
    )

    df_男女_年齡_單身_縣市 = pd.concat(
        sum(
            [
                [
                    df_男女_年齡_婚姻_縣市[("男", "單身", region)],
                    df_男女_年齡_婚姻_縣市[("女", "單身", region)],
                ]
                for region in regions
            ],
            [],
        ),
        axis="columns",
        keys=sum([[f"男_{region}", f"女_{region}"] for region in regions], []),
    )
    for region in regions:
        df_男女_年齡_單身_縣市[f"女男比_{region}"] = (
            df_男女_年齡_單身_縣市[f"女_{region}"] / df_男女_年齡_單身_縣市[f"男_{region}"]
        )

    plots[f"{key}_男女_年齡_單身_縣市"] = plot_lines_bars(
        df_男女_年齡_單身_縣市,
        lines_left_axis=[],
        lines_right_axis=[f"女男比_{region}" for region in regions],
        bars=sum([[f"女_{region}", f"男_{region}"] for region in regions], []),
        title=f"{key}_男女_年齡_單身(含離婚、喪偶)_縣市 {year}年",
        legendgroup=True,
    )

    # https://data.gov.tw/dataset/32970
    # https://data.gov.tw/dataset/77139
    # https://data.gov.tw/dataset/131135
    # API 說明文件
    # https://www.ris.gov.tw/rs-opendata/api/Main/docs/v1
    # API 路徑
    # https://www.ris.gov.tw/rs-opendata/api/v1/datastore/ODRP003/{yyymm} 請指定年月
    # https://www.ris.gov.tw/rs-opendata/api/v1/datastore/ODRP012/{yyymm} 請指定年月
    # https://www.ris.gov.tw/rs-opendata/api/v1/datastore/ODRP061/{yyymm} 請指定年月
    key = "動態資料統計表（含同婚）"
    url_year_page_10601_10612 = (
        "https://www.ris.gov.tw/rs-opendata/api/v1/datastore/ODRP003/{year}{month:02d}?page={page}"
    )
    url_year_page_10701_10908 = (
        "https://www.ris.gov.tw/rs-opendata/api/v1/datastore/ODRP012/{year}{month:02d}?page={page}"
    )
    url_year_page = (
        "https://www.ris.gov.tw/rs-opendata/api/v1/datastore/ODRP061/{year}{month:02d}?page={page}"
    )
    key = sanitize_filename(key)
    df = []

    def get_data(year, month, page):
        path = EXTRA_DATA_DIR / key / f"{year}_{month:02d}_{page}.json.gz"
        _ensure_dir_exists(path)

        if not path.is_file():
            yearmonth = year * 100 + month
            if 10601 <= yearmonth and yearmonth <= 10612:
                url = url_year_page_10601_10612.format(year=year, month=month, page=page)
            elif 10701 <= yearmonth and yearmonth <= 10908:
                url = url_year_page_10701_10908.format(year=year, month=month, page=page)
            else:
                url = url_year_page.format(year=year, month=month, page=page)

            for _ in range(5):
                r = session.get(url, verify=False)
                try:
                    json_data = json.loads(r.content)
                    if "responseData" in json_data:
                        with gzip.open(path, "wb") as f:
                            f.write(r.content)
                        break
                    else:
                        return {}
                except:
                    print("no data", url)
                    continue
            else:
                return {}

        with gzip.open(path, "rb") as f:
            data = json.load(f)

        return data

    def rename_columns_name(df: pd.DataFrame):
        columns = {
            "statistic_yyymm": "統計年月",
            "district_code": "區域別代碼",
            "site_id": "區域別",
            "village": "村里名稱",
            "neighbor_no2": "鄰數_戶籍登記",
            "household_no": "戶數",
            "people_total": "人口數_合計",
            "people_total_m": "人口數_男",
            "people_total_f": "人口數_女",
            "birth_total": "出生數_合計",
            "birth_total_m": "出生數_合計_男",
            "birth_total_f": "出生數_合計_女",
            "birth_legal_m": "出生數_婚生_男",
            "birth_legal_f": "出生數_婚生_女",
            "birth_illegal_recognized_m": "出生數_非婚生_已認領_男",
            "birth_illegal_recognized_f": "出生數_非婚生_已認領_女",
            "birth_illegal_unrecognized_m": "出生數_非婚生_未認領_男",
            "birth_illegal_unrecognized_f": "出生數_非婚生_未認領_女",
            "helpless_child_m": "出生數_無依兒童_男",
            "helpless_child_f": "出生數_無依兒童_女",
            "mother_mainland_m": "生母原屬國籍（地區）_大陸港澳地區_男",
            "mother_mainland_f": "生母原屬國籍（地區）_大陸港澳地區_女",
            "mother_foreigner_m": "生母原屬國籍（地區）_外國籍_男",
            "mother_foreigner_f": "生母原屬國籍（地區）_外國籍_女",
            "father_mainland_m": "生父原屬國籍（地區）_大陸港澳地區_男",
            "father_mainland_f": "生父原屬國籍（地區）_大陸港澳地區_女",
            "father_foreigner_m": "生父原屬國籍（地區）_外國籍_男",
            "father_foreigner_f": "生父原屬國籍（地區）_外國籍_女",
            "posthumous_child_m": "遺腹子_男",
            "posthumous_child_f": "遺腹子_女",
            "twinborn_child_m": "雙胞胎_男",
            "twinborn_child_f": "雙胞胎_女",
            "multi_child_m": "三胞胎以上_男",
            "multi_child_f": "三胞胎以上_女",
            "death_m": "死亡人數_男",
            "death_f": "死亡人數_女",
            "claimed_m": "認領人數_男",
            "claimed_f": "認領人數_女",
            "adpot_m": "收養人數_男",
            "adpot_f": "收養人數_女",
            "stop_adpot_m": "終止收養人數_男",
            "stop_adpot_f": "終止收養人數_女",
            "guardianship_m": "監護人數_男",
            "guardianship_f": "監護人數_女",
            "aid_m": "輔助人數_男",
            "aid_f": "輔助人數_女",
            "minorchildren_m": "未成年子女權利義務行使負擔人數_男",
            "minorchildren_f": "未成年子女權利義務行使負擔人數_女",
            "marry_pair_OppositeSex": "結婚對數_異性",
            "marry_pair_SameSex": "結婚對數_同性",
            "marry_pair_SameSex_m": "結婚對數_同性_男",
            "marry_pair_SameSex_f": "結婚對數_同性_女",
            "divorce_pair_OppositeSex": "離婚對數_異性",
            "divorce_pair_SameSex": "離婚對數_同性",
            "divorce_pair_SameSex_m": "離婚對數_同性_男",
            "divorce_pair_SameSex_f": "離婚對數_同性_女",
            "marry_pair": "結婚對數_異性",
            "divorce_pair": "離婚對數_異性",
        }
        df.columns = df.columns.str.replace("\ufeff", "")
        return df.rename(columns=columns)

    today = datetime.today()
    for year in range(106, today.year - 1911 + 1):
        for month in range(1, 13):
            if year + 1911 > today.year or (year + 1911 == today.year and month > today.month):
                break

            page = 1
            json_data = get_data(year, month, page)
            if "responseData" in json_data:
                data = pd.json_normalize(json_data["responseData"])
                data = rename_columns_name(data)
                df.append(data)

                pages = int(json_data["totalPage"])
                for page in range(2, pages + 1):
                    json_data = get_data(year, month, page)
                    data = pd.json_normalize(json_data["responseData"])
                    data = rename_columns_name(data)
                    df.append(data)

    df = pd.concat(df, ignore_index=True)
    df = df.fillna(0)
    df[df.columns[4:]] = df[df.columns[4:]].astype(int)
    split = (
        df["統計年月"].str.replace("(^.{3})", r"\1|", regex=True).str.split("|", n=1, expand=True)
    )
    df["年"] = split[0].str.strip()
    df["月"] = split[1].str.strip()
    split = df["區域別"].str.replace("(^.{3})", r"\1|", regex=True).str.split("|", n=1, expand=True)
    df["縣市"] = split[0].str.strip()
    df["鄉鎮"] = split[1].str.strip()
    df["死亡人數_合計"] = df["死亡人數_男"] + df["死亡人數_女"]
    df["結婚對數_合計"] = (
        df["結婚對數_異性"] + df["結婚對數_同性"] + df["結婚對數_同性_男"] + df["結婚對數_同性_女"]
    )
    df["離婚對數_合計"] = (
        df["離婚對數_異性"] + df["離婚對數_同性"] + df["離婚對數_同性_男"] + df["離婚對數_同性_女"]
    )
    yearsmonths = df["統計年月"].unique().tolist()

    def summary(df, suffix, 合計, 男, 女, index):
        df_total = df.pivot_table(values=合計, index=index, aggfunc="sum", sort=False)
        plots[f"{key}_總和_{suffix}"] = plot_line(
            df_total, f"{key}_總和_{suffix} {yearsmonths[0]}~{yearsmonths[-1]}"
        )

        df_total_男女 = df.pivot_table(values=[男, 女], index=index, aggfunc="sum", sort=False)
        plots[f"{key}_總和_男女_{suffix}"] = plot_line(
            df_total_男女,
            f"{key}_總和_男女_{suffix} {yearsmonths[0]}~{yearsmonths[-1]}",
        )

        df_區域別 = df.pivot_table(
            values=合計, index=index, columns="縣市", aggfunc="sum", sort=False
        )
        plots[f"{key}_區域別_{suffix}"] = plot_line(
            df_區域別, f"{key}_區域別_{suffix} {yearsmonths[0]}~{yearsmonths[-1]}"
        )

        df_區域別_男女 = df.pivot_table(
            values=[男, 女],
            index=index,
            columns="縣市",
            aggfunc="sum",
            sort=False,
        )
        df_區域別_男女.columns = [f"{region}_{sex}" for sex, region in df_區域別_男女.columns]
        plots[f"{key}_區域別_男女_{suffix}"] = plot_line(
            df_區域別_男女,
            f"{key}_區域別_男女_{suffix} {yearsmonths[0]}~{yearsmonths[-1]}",
        )

    df_人口_年 = df[df["統計年月"].str.contains(r"\d{3}12")]
    if df_人口_年["年"].iloc[-1] != df["年"].iloc[-1]:
        df_人口_年 = pd.concat(
            [df_人口_年, df[df["統計年月"] == df["統計年月"].iloc[-1]]], axis="index"
        )
    summary(df_人口_年, "人口數_年", "人口數_合計", "人口數_男", "人口數_女", "年")
    summary(df, "人口數_年月", "人口數_合計", "人口數_男", "人口數_女", "統計年月")
    summary(df, "出生數_年", "出生數_合計", "出生數_合計_男", "出生數_合計_女", "年")
    summary(df, "出生數_年月", "出生數_合計", "出生數_合計_男", "出生數_合計_女", "統計年月")
    summary(df, "死亡人數_年", "死亡人數_合計", "死亡人數_男", "死亡人數_女", "年")
    summary(df, "死亡人數_年月", "死亡人數_合計", "死亡人數_男", "死亡人數_女", "統計年月")

    for suffix, index in [("年月", "統計年月"), ("年", "年")]:
        df_人口_出生_死亡 = df.pivot_table(
            values=[
                "人口數_合計",
                "人口數_男",
                "人口數_女",
                "出生數_合計",
                "出生數_合計_男",
                "出生數_合計_女",
                "死亡人數_合計",
                "死亡人數_男",
                "死亡人數_女",
            ],
            index=index,
            aggfunc="sum",
            sort=False,
        )
        if index == "年":
            df_人口 = df_人口_年.pivot_table(
                values=[
                    "人口數_合計",
                    "人口數_男",
                    "人口數_女",
                ],
                index=index,
                aggfunc="sum",
                sort=False,
            )
            df_人口_出生_死亡["人口數_合計"] = df_人口["人口數_合計"]
            df_人口_出生_死亡["人口數_男"] = df_人口["人口數_男"]
            df_人口_出生_死亡["人口數_女"] = df_人口["人口數_女"]

        df_人口_出生_死亡["人口自然增加數_合計"] = (
            df_人口_出生_死亡["出生數_合計"] - df_人口_出生_死亡["死亡人數_合計"]
        )
        df_人口_出生_死亡["人口自然增加數_男"] = (
            df_人口_出生_死亡["出生數_合計_男"] - df_人口_出生_死亡["死亡人數_男"]
        )
        df_人口_出生_死亡["人口自然增加數_女"] = (
            df_人口_出生_死亡["出生數_合計_女"] - df_人口_出生_死亡["死亡人數_女"]
        )
        df_人口_出生_死亡["出生率_合計"] = (
            df_人口_出生_死亡["出生數_合計"] / df_人口_出生_死亡["人口數_合計"]
        )
        df_人口_出生_死亡["出生率_男"] = (
            df_人口_出生_死亡["出生數_合計_男"] / df_人口_出生_死亡["人口數_男"]
        )
        df_人口_出生_死亡["出生率_女"] = (
            df_人口_出生_死亡["出生數_合計_女"] / df_人口_出生_死亡["人口數_女"]
        )
        df_人口_出生_死亡["死亡率_合計"] = (
            df_人口_出生_死亡["死亡人數_合計"] / df_人口_出生_死亡["人口數_合計"]
        )
        df_人口_出生_死亡["死亡率_男"] = (
            df_人口_出生_死亡["死亡人數_男"] / df_人口_出生_死亡["人口數_男"]
        )
        df_人口_出生_死亡["死亡率_女"] = (
            df_人口_出生_死亡["死亡人數_女"] / df_人口_出生_死亡["人口數_女"]
        )

        plots[f"{key}_人口_出生_死亡_{suffix}"] = plot_lines_bars(
            df_人口_出生_死亡,
            lines_left_axis=["出生數_合計", "死亡人數_合計"],
            lines_right_axis=["人口數_合計"],
            bars=["人口自然增加數_合計"],
            title=f"{key}_人口_出生_死亡_{suffix} {yearsmonths[0]}~{yearsmonths[-1]}",
        )

        plots[f"{key}_人口_出生_死亡_男女_{suffix}"] = plot_lines_bars(
            df_人口_出生_死亡,
            lines_left_axis=[
                "出生數_合計_男",
                "死亡人數_男",
                "出生數_合計_女",
                "死亡人數_女",
            ],
            lines_right_axis=["人口數_男", "人口數_女"],
            bars=["人口自然增加數_男", "人口自然增加數_女"],
            title=f"{key}_人口_出生_死亡_男女_{suffix} {yearsmonths[0]}~{yearsmonths[-1]}",
        )

        plots[f"{key}_出生率_{suffix}"] = plot_line(
            df_人口_出生_死亡[["出生率_合計", "出生率_男", "出生率_女"]],
            title=f"{key}_出生率_{suffix} {yearsmonths[0]}~{yearsmonths[-1]}",
            additional_layout={"yaxis": {"tickformat": ".2%"}},
        )
        plots[f"{key}_死亡率_{suffix}"] = plot_line(
            df_人口_出生_死亡[["死亡率_合計", "死亡率_男", "死亡率_女"]],
            title=f"{key}_死亡率_{suffix} {yearsmonths[0]}~{yearsmonths[-1]}",
            additional_layout={"yaxis": {"tickformat": ".2%"}},
        )

        df_人口_出生_死亡_縣市 = df.pivot_table(
            values=[
                "人口數_合計",
                "人口數_男",
                "人口數_女",
                "出生數_合計",
                "出生數_合計_男",
                "出生數_合計_女",
                "死亡人數_合計",
                "死亡人數_男",
                "死亡人數_女",
            ],
            columns="縣市",
            index=index,
            aggfunc="sum",
            sort=False,
        )
        if index == "年":
            df_人口 = df_人口_年.pivot_table(
                values=[
                    "人口數_合計",
                    "人口數_男",
                    "人口數_女",
                ],
                columns="縣市",
                index=index,
                aggfunc="sum",
                sort=False,
            )
            df_人口_出生_死亡_縣市["人口數_合計"] = df_人口["人口數_合計"]
            df_人口_出生_死亡_縣市["人口數_男"] = df_人口["人口數_男"]
            df_人口_出生_死亡_縣市["人口數_女"] = df_人口["人口數_女"]

        regions = df["縣市"].unique().tolist()
        for region in regions:
            df_人口_出生_死亡_縣市 = df_人口_出生_死亡_縣市.copy()
            df_人口_出生_死亡_縣市[("人口自然增加數_合計", region)] = (
                df_人口_出生_死亡_縣市[("出生數_合計", region)]
                - df_人口_出生_死亡_縣市[("死亡人數_合計", region)]
            )
            df_人口_出生_死亡_縣市[("人口自然增加數_男", region)] = (
                df_人口_出生_死亡_縣市[("出生數_合計_男", region)]
                - df_人口_出生_死亡_縣市[("死亡人數_男", region)]
            )
            df_人口_出生_死亡_縣市[("人口自然增加數_女", region)] = (
                df_人口_出生_死亡_縣市[("出生數_合計_女", region)]
                - df_人口_出生_死亡_縣市[("死亡人數_女", region)]
            )
            df_人口_出生_死亡_縣市[("出生率_合計", region)] = (
                df_人口_出生_死亡_縣市[("出生數_合計", region)]
                / df_人口_出生_死亡_縣市[("人口數_合計", region)]
            )
            df_人口_出生_死亡_縣市[("出生率_男", region)] = (
                df_人口_出生_死亡_縣市[("出生數_合計_男", region)]
                / df_人口_出生_死亡_縣市[("人口數_男", region)]
            )
            df_人口_出生_死亡_縣市[("出生率_女", region)] = (
                df_人口_出生_死亡_縣市[("出生數_合計_女", region)]
                / df_人口_出生_死亡_縣市[("人口數_女", region)]
            )
            df_人口_出生_死亡_縣市[("死亡率_合計", region)] = (
                df_人口_出生_死亡_縣市[("死亡人數_合計", region)]
                / df_人口_出生_死亡_縣市[("人口數_合計", region)]
            )
            df_人口_出生_死亡_縣市[("死亡率_男", region)] = (
                df_人口_出生_死亡_縣市[("死亡人數_男", region)]
                / df_人口_出生_死亡_縣市[("人口數_男", region)]
            )
            df_人口_出生_死亡_縣市[("死亡率_女", region)] = (
                df_人口_出生_死亡_縣市[("死亡人數_女", region)]
                / df_人口_出生_死亡_縣市[("人口數_女", region)]
            )

        df_人口_出生_死亡_縣市.columns = [
            f"{num}_{region}" for num, region in df_人口_出生_死亡_縣市.columns
        ]

        plots[f"{key}_人口_出生_死亡_縣市_{suffix}"] = plot_lines_bars(
            df_人口_出生_死亡_縣市,
            lines_left_axis=sum(
                [
                    [
                        f"出生數_合計_{region}",
                        f"死亡人數_合計_{region}",
                    ]
                    for region in regions
                ],
                [],
            ),
            lines_right_axis=sum(
                [
                    [
                        f"人口數_合計_{region}",
                    ]
                    for region in regions
                ],
                [],
            ),
            bars=sum(
                [
                    [
                        f"人口自然增加數_合計_{region}",
                    ]
                    for region in regions
                ],
                [],
            ),
            title=f"{key}_人口_出生_死亡_縣市_{suffix} {yearsmonths[0]}~{yearsmonths[-1]}",
            legendgroup=True,
        )

        plots[f"{key}_人口_出生_死亡_縣市_男女_{suffix}"] = plot_lines_bars(
            df_人口_出生_死亡_縣市,
            lines_left_axis=sum(
                [
                    [
                        f"出生數_合計_男_{region}",
                        f"死亡人數_男_{region}",
                        f"出生數_合計_女_{region}",
                        f"死亡人數_女_{region}",
                    ]
                    for region in regions
                ],
                [],
            ),
            lines_right_axis=sum(
                [
                    [
                        f"人口數_男_{region}",
                        f"人口數_女_{region}",
                    ]
                    for region in regions
                ],
                [],
            ),
            bars=sum(
                [
                    [
                        f"人口自然增加數_男_{region}",
                        f"人口自然增加數_女_{region}",
                    ]
                    for region in regions
                ],
                [],
            ),
            title=f"{key}_人口_出生_死亡_縣市_男女_{suffix} {yearsmonths[0]}~{yearsmonths[-1]}",
            legendgroup=True,
        )
        plots[f"{key}_出生率_縣市_{suffix}"] = plot_lines_bars(
            df_人口_出生_死亡_縣市,
            lines_left_axis=sum(
                [
                    [
                        f"出生率_合計_{region}",
                        f"出生率_男_{region}",
                        f"出生率_女_{region}",
                    ]
                    for region in regions
                ],
                [],
            ),
            lines_right_axis=[],
            bars=[],
            title=f"{key}_出生率_縣市_{suffix} {yearsmonths[0]}~{yearsmonths[-1]}",
            additional_layout={
                "yaxis": {"tickformat": ".2%"},
                "hovermode": "x",
            },
            legendgroup=True,
        )
        plots[f"{key}_死亡率_縣市_{suffix}"] = plot_lines_bars(
            df_人口_出生_死亡_縣市,
            lines_left_axis=sum(
                [
                    [
                        f"死亡率_合計_{region}",
                        f"死亡率_男_{region}",
                        f"死亡率_女_{region}",
                    ]
                    for region in regions
                ],
                [],
            ),
            lines_right_axis=[],
            bars=[],
            title=f"{key}_死亡率_縣市_{suffix} {yearsmonths[0]}~{yearsmonths[-1]}",
            additional_layout={
                "yaxis": {"tickformat": ".2%"},
                "hovermode": "x",
            },
            legendgroup=True,
        )

        df_出生_結婚_離婚 = df.pivot_table(
            values=[
                "出生數_合計",
                "結婚對數_合計",
                "離婚對數_合計",
                "人口數_合計",
            ],
            index=index,
            aggfunc="sum",
            sort=False,
        )
        if index == "年":
            df_人口 = df_人口_年.pivot_table(
                values=[
                    "人口數_合計",
                ],
                index=index,
                aggfunc="sum",
                sort=False,
            )
            df_出生_結婚_離婚["人口數_合計"] = df_人口["人口數_合計"]

        df_出生_結婚_離婚["婚姻自然增加數_合計"] = (
            df_出生_結婚_離婚["結婚對數_合計"] - df_出生_結婚_離婚["離婚對數_合計"]
        )
        df_出生_結婚_離婚["結婚率_合計"] = (
            df_出生_結婚_離婚["結婚對數_合計"] / df_出生_結婚_離婚["人口數_合計"]
        )
        df_出生_結婚_離婚["離婚率_合計"] = (
            df_出生_結婚_離婚["離婚對數_合計"] / df_出生_結婚_離婚["人口數_合計"]
        )

        plots[f"{key}_出生_結婚_離婚_{suffix}"] = plot_lines_bars(
            df_出生_結婚_離婚,
            lines_left_axis=["結婚對數_合計", "離婚對數_合計"],
            lines_right_axis=["出生數_合計"],
            bars=["婚姻自然增加數_合計"],
            title=f"{key}_出生_結婚_離婚_{suffix} {yearsmonths[0]}~{yearsmonths[-1]}",
        )
        plots[f"{key}_結婚率_{suffix}"] = plot_line(
            df_出生_結婚_離婚[["結婚率_合計"]],
            title=f"{key}_結婚率_{suffix} {yearsmonths[0]}~{yearsmonths[-1]}",
            additional_layout={"yaxis": {"tickformat": ".2%"}},
        )
        plots[f"{key}_離婚率_{suffix}"] = plot_line(
            df_出生_結婚_離婚[["離婚率_合計"]],
            title=f"{key}_離婚率_{suffix} {yearsmonths[0]}~{yearsmonths[-1]}",
            additional_layout={"yaxis": {"tickformat": ".2%"}},
        )

        df_出生_結婚_離婚_縣市 = df.pivot_table(
            values=[
                "出生數_合計",
                "結婚對數_合計",
                "離婚對數_合計",
                "人口數_合計",
            ],
            columns="縣市",
            index=index,
            aggfunc="sum",
            sort=False,
        )
        if index == "年":
            df_人口 = df_人口_年.pivot_table(
                values=[
                    "人口數_合計",
                ],
                columns="縣市",
                index=index,
                aggfunc="sum",
                sort=False,
            )
            df_出生_結婚_離婚_縣市["人口數_合計"] = df_人口["人口數_合計"]

        regions = df["縣市"].unique().tolist()
        for region in regions:
            df_出生_結婚_離婚_縣市[("婚姻自然增加數_合計", region)] = (
                df_出生_結婚_離婚_縣市[("結婚對數_合計", region)]
                - df_出生_結婚_離婚_縣市[("離婚對數_合計", region)]
            )
            df_出生_結婚_離婚_縣市[("結婚率_合計", region)] = (
                df_出生_結婚_離婚_縣市[("結婚對數_合計", region)]
                / df_出生_結婚_離婚_縣市[("人口數_合計", region)]
            )
            df_出生_結婚_離婚_縣市[("離婚率_合計", region)] = (
                df_出生_結婚_離婚_縣市[("離婚對數_合計", region)]
                / df_出生_結婚_離婚_縣市[("人口數_合計", region)]
            )

        df_出生_結婚_離婚_縣市.columns = [
            f"{num}_{region}" for num, region in df_出生_結婚_離婚_縣市.columns
        ]

        plots[f"{key}_出生_結婚_離婚_縣市_{suffix}"] = plot_lines_bars(
            df_出生_結婚_離婚_縣市,
            lines_left_axis=sum(
                [
                    [
                        f"結婚對數_合計_{region}",
                        f"離婚對數_合計_{region}",
                    ]
                    for region in regions
                ],
                [],
            ),
            lines_right_axis=sum(
                [
                    [
                        f"出生數_合計_{region}",
                    ]
                    for region in regions
                ],
                [],
            ),
            bars=sum(
                [
                    [
                        f"婚姻自然增加數_合計_{region}",
                    ]
                    for region in regions
                ],
                [],
            ),
            title=f"{key}_出生_結婚_離婚_縣市_{suffix} {yearsmonths[0]}~{yearsmonths[-1]}",
            legendgroup=True,
        )
        plots[f"{key}_結婚率_縣市_{suffix}"] = plot_lines_bars(
            df_出生_結婚_離婚_縣市,
            lines_left_axis=sum(
                [
                    [
                        f"結婚率_合計_{region}",
                    ]
                    for region in regions
                ],
                [],
            ),
            lines_right_axis=[],
            bars=[],
            title=f"{key}_結婚率_縣市_{suffix} {yearsmonths[0]}~{yearsmonths[-1]}",
            additional_layout={
                "yaxis": {"tickformat": ".2%"},
                "hovermode": "x",
            },
            legendgroup=True,
        )
        plots[f"{key}_離婚率_縣市_{suffix}"] = plot_lines_bars(
            df_出生_結婚_離婚_縣市,
            lines_left_axis=sum(
                [
                    [
                        f"離婚率_合計_{region}",
                    ]
                    for region in regions
                ],
                [],
            ),
            lines_right_axis=[],
            bars=[],
            title=f"{key}_離婚率_縣市_{suffix} {yearsmonths[0]}~{yearsmonths[-1]}",
            additional_layout={
                "yaxis": {"tickformat": ".2%"},
                "hovermode": "x",
            },
            legendgroup=True,
        )

        df_total = df.pivot_table(values="結婚對數_合計", index=index, aggfunc="sum", sort=False)
        plots[f"{key}_總和_結婚對數_{suffix}"] = plot_line(
            df_total, f"{key}_總和_結婚對數_{suffix} {yearsmonths[0]}~{yearsmonths[-1]}"
        )
        df_區域別 = df.pivot_table(
            values="結婚對數_合計", index=index, columns="縣市", aggfunc="sum", sort=False
        )
        plots[f"{key}_區域別_結婚對數_{suffix}"] = plot_line(
            df_區域別, f"{key}_區域別_結婚對數_{suffix} {yearsmonths[0]}~{yearsmonths[-1]}"
        )

        df_total = df.pivot_table(values="離婚對數_合計", index=index, aggfunc="sum", sort=False)
        plots[f"{key}_總和_離婚對數_{suffix}"] = plot_line(
            df_total, f"{key}_總和_離婚對數_{suffix} {yearsmonths[0]}~{yearsmonths[-1]}"
        )
        df_區域別 = df.pivot_table(
            values="離婚對數_合計", index=index, columns="縣市", aggfunc="sum", sort=False
        )
        plots[f"{key}_區域別_離婚對數_{suffix}"] = plot_line(
            df_區域別, f"{key}_區域別_離婚對數_{suffix} {yearsmonths[0]}~{yearsmonths[-1]}"
        )

    # https://data.gov.tw/dataset/139388
    # API 說明文件
    # https://www.ris.gov.tw/rs-opendata/api/Main/docs/v1
    # API 路徑
    # https://www.ris.gov.tw/rs-opendata/api/v1/datastore/ODRP068/{yyy} 請指定年
    key = "結婚人數按婚姻類型、性別、年齡、原屬國籍（地區）及教育程度分(按登記)"
    url_year_page = "https://www.ris.gov.tw/rs-opendata/api/v1/datastore/ODRP068/{year}?page={page}"
    key = sanitize_filename(key)
    df = []

    def get_data(year, page):
        path = EXTRA_DATA_DIR / key / f"{year}_{page}.json.gz"
        _ensure_dir_exists(path)

        if not path.is_file():
            url = url_year_page.format(year=year, page=page)
            r = session.get(url, verify=False)

            with gzip.open(path, "wb") as f:
                f.write(r.content)

        with gzip.open(path, "rb") as f:
            data = json.load(f)

        return data

    for year in range(109, datetime.today().year - 1911 + 1):
        page = 1
        json_data = get_data(year, page)
        if "responseData" in json_data:
            data = pd.json_normalize(json_data["responseData"])
            df.append(data)

            pages = int(json_data["totalPage"])
            for page in range(2, pages + 1):
                json_data = get_data(year, page)
                data = pd.json_normalize(json_data["responseData"])
                df.append(data)

    df = pd.concat(df, ignore_index=True)
    df["number_of_marry"] = df["number_of_marry"].astype(int)
    split = (
        df["site_id"].str.replace("(^.{3})", r"\1|", regex=True).str.split("|", n=1, expand=True)
    )
    df["縣市"] = split[0].str.strip()
    df["鄉鎮"] = split[1].str.strip()
    years = df["year"].unique().tolist()

    df_total = df.pivot_table(values="number_of_marry", index="year", aggfunc="sum", sort=False)
    plots[f"{key}_總和"] = plot_line(
        df_total, f"{key}_總和 {df_total.index[0]}~{df_total.index[-1]}"
    )

    df_區域別 = df.pivot_table(
        values="number_of_marry", index="year", columns="縣市", aggfunc="sum", sort=False
    )
    plots[f"{key}_區域別"] = plot_line(
        df_區域別, f"{key}_區域別 {df_區域別.index[0]}~{df_區域別.index[-1]}"
    )

    df_婚姻類型 = df.pivot_table(
        values="number_of_marry", index="year", columns="marriage_type", aggfunc="sum", sort=False
    )
    plots[f"{key}_婚姻類型"] = plot_line(
        df_婚姻類型, f"{key}_婚姻類型 {df_婚姻類型.index[0]}~{df_婚姻類型.index[-1]}"
    )

    df_性別 = df.pivot_table(
        values="number_of_marry", index="year", columns="sex", aggfunc="sum", sort=False
    )
    plots[f"{key}_性別"] = plot_line(df_性別, f"{key}_性別 {df_性別.index[0]}~{df_性別.index[-1]}")

    df_原屬國籍 = df.pivot_table(
        values="number_of_marry", index="year", columns="nation", aggfunc="sum", sort=False
    )
    plots[f"{key}_原屬國籍"] = plot_line(
        df_原屬國籍, f"{key}_原屬國籍 {df_原屬國籍.index[0]}~{df_原屬國籍.index[-1]}"
    )

    df_教育程度 = df.pivot_table(
        values="number_of_marry", index="year", columns="edu", aggfunc="sum", sort=False
    )
    plots[f"{key}_教育程度"] = plot_line(
        df_教育程度, f"{key}_教育程度 {df_教育程度.index[0]}~{df_教育程度.index[-1]}"
    )

    df_年齡 = df.pivot_table(
        values="number_of_marry", index="year", columns="age", aggfunc="sum", sort=False
    )
    plots[f"{key}_年齡"] = plot_line(df_年齡, f"{key}_年齡 {df_年齡.index[0]}~{df_年齡.index[-1]}")

    df_女_年齡_縣市 = df[df["sex"] == "女"].pivot_table(
        values="number_of_marry", index="age", columns="縣市", aggfunc="sum", sort=False
    )
    plots[f"{key}_女_年齡_縣市"] = plot_line(
        df_女_年齡_縣市, f"{key}_女_年齡_縣市 {years[0]}~{years[-1]}"
    )

    df_男_年齡_縣市 = df[df["sex"] == "男"].pivot_table(
        values="number_of_marry", index="age", columns="縣市", aggfunc="sum", sort=False
    )
    plots[f"{key}_男_年齡_縣市"] = plot_line(
        df_男_年齡_縣市, f"{key}_男_年齡_縣市 {years[0]}~{years[-1]}"
    )

    df_女_年齡_教育 = df[df["sex"] == "女"].pivot_table(
        values="number_of_marry", index="age", columns="edu", aggfunc="sum", sort=False
    )
    plots[f"{key}_女_年齡_教育"] = plot_line(
        df_女_年齡_教育, f"{key}_女_年齡_教育 {years[0]}~{years[-1]}"
    )

    df_男_年齡_教育 = df[df["sex"] == "男"].pivot_table(
        values="number_of_marry", index="age", columns="edu", aggfunc="sum", sort=False
    )
    plots[f"{key}_男_年齡_教育"] = plot_line(
        df_男_年齡_教育, f"{key}_男_年齡_教育 {years[0]}~{years[-1]}"
    )

    # https://data.gov.tw/dataset/130547
    key = "結婚對數按婚姻類型、性別及年齡分(按登記)"
    key = sanitize_filename(key)
    urls = {
        108: "https://data.moi.gov.tw/MoiOD/System/DownloadFile.aspx?DATA=50E1F8E9-3A75-45A7-A50D-306CC625A700",
        109: "https://data.moi.gov.tw/MoiOD/System/DownloadFile.aspx?DATA=8BB88A9D-4F47-4798-9557-682D338923B9",
        110: "https://data.moi.gov.tw/MoiOD/System/DownloadFile.aspx?DATA=4540FE37-A4FC-4832-9028-F7CD3734B160",
        111: "https://data.moi.gov.tw/MoiOD/System/DownloadFile.aspx?DATA=70900CD1-5314-433D-BDE3-1DF5C971ECD1",
        112: "https://data.moi.gov.tw/MoiOD/System/DownloadFile.aspx?DATA=7DECF6DE-7AD6-4EC8-85A9-D2B8B703A014",
        113: "https://data.moi.gov.tw/MoiOD/System/DownloadFile.aspx?DATA=169B54F1-8C46-48FB-8E28-164755BE51F8",
    }

    df = []
    for filename, url in urls.items():
        path = EXTRA_DATA_DIR / key / f"{filename}.csv.gz"
        data = read_csv_with_cache(path, url)
        df.append(data)

    df = pd.concat(df, ignore_index=True)
    years = df["統計年度"].unique().tolist()
    split = df["區域別"].str.replace("(^.{3})", r"\1|", regex=True).str.split("|", n=1, expand=True)
    df["縣市"] = split[0].str.strip()
    df["鄉鎮"] = split[1].str.strip()

    kinds = df["婚姻類型"].unique().tolist()
    items[key] = kinds
    for kind in kinds:
        df_女 = df.pivot_table(
            values="結婚對數",
            index="女方年齡或配偶一方年齡",
            columns="男方年齡或配偶另一方年齡",
            aggfunc="sum",
            sort=False,
        )
        plots[f"{key}_女_{kind}"] = plot_line(
            df_女,
            f"{key}_女_{kind} {years[0]}~{years[-1]}",
            {"xaxis": {"title": {"text": "女方年齡或配偶一方年齡"}}},
        )

        df_男 = df.pivot_table(
            values="結婚對數",
            index="男方年齡或配偶另一方年齡",
            columns="女方年齡或配偶一方年齡",
            aggfunc="sum",
            sort=False,
        )
        plots[f"{key}_男_{kind}"] = plot_line(
            df_男,
            f"{key}_男_{kind} {years[0]}~{years[-1]}",
            {"xaxis": {"title": {"text": "男方年齡或配偶另一方年齡"}}},
        )

    df_女_縣市 = df.pivot_table(
        values="結婚對數",
        index="女方年齡或配偶一方年齡",
        columns="縣市",
        aggfunc="sum",
        sort=False,
    )
    plots[f"{key}_女_縣市"] = plot_line(
        df_女_縣市,
        f"{key}_女_縣市 {years[0]}~{years[-1]}",
        {"xaxis": {"title": {"text": "女方年齡或配偶一方年齡"}}},
    )

    df_男_縣市 = df.pivot_table(
        values="結婚對數",
        index="男方年齡或配偶另一方年齡",
        columns="縣市",
        aggfunc="sum",
        sort=False,
    )
    plots[f"{key}_男_縣市"] = plot_line(
        df_男_縣市,
        f"{key}_男_縣市 {years[0]}~{years[-1]}",
        {"xaxis": {"title": {"text": "男方年齡或配偶另一方年齡"}}},
    )

    # https://data.gov.tw/dataset/32945
    # API 說明文件
    # https://www.ris.gov.tw/rs-opendata/api/Main/docs/v1
    # API 路徑
    # https://www.ris.gov.tw/rs-opendata/api/v1/datastore/ODRP028/{yyy} 請指定年
    key = "嬰兒出生數按性別、生母原屬國籍（地區）、年齡及教育程度分(按登記)"
    url_year_page = "https://www.ris.gov.tw/rs-opendata/api/v1/datastore/ODRP028/{year}?page={page}"
    key = sanitize_filename(key)
    df = []

    def get_data(year, page):
        path = EXTRA_DATA_DIR / key / f"{year}_{page}.json.gz"
        _ensure_dir_exists(path)

        if not path.is_file():
            url = url_year_page.format(year=year, page=page)
            r = session.get(url, verify=False)

            with gzip.open(path, "wb") as f:
                f.write(r.content)

        with gzip.open(path, "rb") as f:
            data = json.load(f)

        return data

    def rename_columns_name(df: pd.DataFrame):
        columns = {
            "統計年度": "statistic_yyy",
            "按照別": "according",
            "區域別": "site_id",
            "出生者性別": "birth_sex",
            "生母原屬國籍或地區": "mother_nation",
            "生母年齡": "mother_age",
            "生母教育程度": "mother_education",
            "嬰兒出生數": "birth_count",
        }
        df.columns = df.columns.str.replace("\ufeff", "")
        return df.rename(columns=columns)

    for year in range(106, datetime.today().year - 1911 + 1):
        page = 1
        json_data = get_data(year, page)
        if "responseData" in json_data:
            data = pd.json_normalize(json_data["responseData"])
            data = rename_columns_name(data)
            df.append(data)

            pages = int(json_data["totalPage"])
            for page in range(2, pages + 1):
                json_data = get_data(year, page)
                data = pd.json_normalize(json_data["responseData"])
                data = rename_columns_name(data)
                df.append(data)

    df = pd.concat(df, ignore_index=True)
    df["birth_count"] = df["birth_count"].astype(int)
    split = (
        df["site_id"].str.replace("(^.{3})", r"\1|", regex=True).str.split("|", n=1, expand=True)
    )
    df["縣市"] = split[0].str.strip()
    df["鄉鎮"] = split[1].str.strip()
    years = df["statistic_yyy"].unique().tolist()
    df["mother_age"] = df["mother_age"].str.replace("～", "~")

    df_total = df.pivot_table(
        values="birth_count", index="statistic_yyy", aggfunc="sum", sort=False
    )
    plots[f"{key}_總和"] = plot_line(
        df_total, f"{key}_總和 {df_total.index[0]}~{df_total.index[-1]}"
    )

    df_區域別 = df.pivot_table(
        values="birth_count", index="statistic_yyy", columns="縣市", aggfunc="sum", sort=False
    )
    plots[f"{key}_區域別"] = plot_line(
        df_區域別, f"{key}_區域別 {df_區域別.index[0]}~{df_區域別.index[-1]}"
    )

    df_性別 = df.pivot_table(
        values="birth_count", index="statistic_yyy", columns="birth_sex", aggfunc="sum", sort=False
    )
    plots[f"{key}_性別"] = plot_line(df_性別, f"{key}_性別 {df_性別.index[0]}~{df_性別.index[-1]}")

    df_原屬國籍 = df.pivot_table(
        values="birth_count",
        index="statistic_yyy",
        columns="mother_nation",
        aggfunc="sum",
        sort=False,
    )
    plots[f"{key}_原屬國籍"] = plot_line(
        df_原屬國籍, f"{key}_原屬國籍 {df_原屬國籍.index[0]}~{df_原屬國籍.index[-1]}"
    )

    df_教育程度 = df.pivot_table(
        values="birth_count",
        index="statistic_yyy",
        columns="mother_education",
        aggfunc="sum",
        sort=False,
    )
    plots[f"{key}_教育程度"] = plot_line(
        df_教育程度, f"{key}_教育程度 {df_教育程度.index[0]}~{df_教育程度.index[-1]}"
    )

    df_年齡 = df.pivot_table(
        values="birth_count", index="statistic_yyy", columns="mother_age", aggfunc="sum", sort=False
    )
    plots[f"{key}_年齡"] = plot_line(df_年齡, f"{key}_年齡 {df_年齡.index[0]}~{df_年齡.index[-1]}")

    df_年齡_縣市 = df.pivot_table(
        values="birth_count", index="mother_age", columns="縣市", aggfunc="sum", sort=False
    )
    plots[f"{key}_年齡_縣市"] = plot_line(df_年齡_縣市, f"{key}_年齡_縣市 {years[0]}~{years[-1]}")

    df_年齡_教育 = df.pivot_table(
        values="birth_count",
        index="mother_age",
        columns="mother_education",
        aggfunc="sum",
        sort=False,
    )
    plots[f"{key}_年齡_教育"] = plot_line(df_年齡_教育, f"{key}_年齡_教育 {years[0]}~{years[-1]}")

    # https://data.gov.tw/dataset/102764
    key = "嬰兒出生數按性別、生父原屬國籍（地區）、年齡及教育程度分(按登記)"
    key = sanitize_filename(key)
    urls = {
        107: "https://data.moi.gov.tw/MoiOD/System/DownloadFile.aspx?DATA=5BAD1943-66B8-4641-93C2-E782756EBDA1",
        108: "https://data.moi.gov.tw/MoiOD/System/DownloadFile.aspx?DATA=4188B9D4-195C-423B-A936-0A61D9AE5F01",
        109: "https://data.moi.gov.tw/MoiOD/System/DownloadFile.aspx?DATA=58ED2363-BC18-4667-8D00-26C2AF9CB35C",
        110: "https://data.moi.gov.tw/MoiOD/System/DownloadFile.aspx?DATA=43DBD613-B4E8-4EBB-BD35-5B79E58166F9",
        111: "https://data.moi.gov.tw/MoiOD/System/DownloadFile.aspx?DATA=1C52FAFC-FEFC-4B10-8E51-81CD91CCB649",
        112: "https://data.moi.gov.tw/MoiOD/System/DownloadFile.aspx?DATA=F1806618-D721-426B-8A51-95C96AE728CF",
    }

    df = []
    for filename, url in urls.items():
        path = EXTRA_DATA_DIR / key / f"{filename}.csv.gz"
        data = read_csv_with_cache(path, url)
        df.append(data)

    df = pd.concat(df, ignore_index=True)
    df["嬰兒出生數"] = df["嬰兒出生數"].astype(int)
    split = df["區域別"].str.replace("(^.{3})", r"\1|", regex=True).str.split("|", n=1, expand=True)
    df["縣市"] = split[0].str.strip()
    df["鄉鎮"] = split[1].str.strip()
    years = df["統計年度"].unique().tolist()
    df["生父年齡"] = df["生父年齡"].str.replace("～", "~").str.replace(" ", "")

    df_total = df.pivot_table(values="嬰兒出生數", index="統計年度", aggfunc="sum", sort=False)
    plots[f"{key}_總和"] = plot_line(
        df_total, f"{key}_總和 {df_total.index[0]}~{df_total.index[-1]}"
    )

    df_區域別 = df.pivot_table(
        values="嬰兒出生數", index="統計年度", columns="縣市", aggfunc="sum", sort=False
    )
    plots[f"{key}_區域別"] = plot_line(
        df_區域別, f"{key}_區域別 {df_區域別.index[0]}~{df_區域別.index[-1]}"
    )

    df_性別 = df.pivot_table(
        values="嬰兒出生數", index="統計年度", columns="出生者性別", aggfunc="sum", sort=False
    )
    plots[f"{key}_性別"] = plot_line(df_性別, f"{key}_性別 {df_性別.index[0]}~{df_性別.index[-1]}")

    df_原屬國籍 = df.pivot_table(
        values="嬰兒出生數",
        index="統計年度",
        columns="生父原屬國籍或地區",
        aggfunc="sum",
        sort=False,
    )
    plots[f"{key}_原屬國籍"] = plot_line(
        df_原屬國籍, f"{key}_原屬國籍 {df_原屬國籍.index[0]}~{df_原屬國籍.index[-1]}"
    )

    df_教育程度 = df.pivot_table(
        values="嬰兒出生數",
        index="統計年度",
        columns="生父教育程度",
        aggfunc="sum",
        sort=False,
    )
    plots[f"{key}_教育程度"] = plot_line(
        df_教育程度, f"{key}_教育程度 {df_教育程度.index[0]}~{df_教育程度.index[-1]}"
    )

    df_年齡 = df.pivot_table(
        values="嬰兒出生數", index="統計年度", columns="生父年齡", aggfunc="sum", sort=False
    )
    plots[f"{key}_年齡"] = plot_line(df_年齡, f"{key}_年齡 {df_年齡.index[0]}~{df_年齡.index[-1]}")

    df_年齡_縣市 = df.pivot_table(
        values="嬰兒出生數", index="生父年齡", columns="縣市", aggfunc="sum", sort=False
    )
    plots[f"{key}_年齡_縣市"] = plot_line(df_年齡_縣市, f"{key}_年齡_縣市 {years[0]}~{years[-1]}")

    df_年齡_教育 = df.pivot_table(
        values="嬰兒出生數",
        index="生父年齡",
        columns="生父教育程度",
        aggfunc="sum",
        sort=False,
    )
    plots[f"{key}_年齡_教育"] = plot_line(df_年齡_教育, f"{key}_年齡_教育 {years[0]}~{years[-1]}")

    # https://data.gov.tw/dataset/127527
    # API 說明文件
    # https://www.ris.gov.tw/rs-opendata/api/Main/docs/v1
    # API 路徑
    # https://www.ris.gov.tw/rs-opendata/api/v1/datastore/ODRP064/{yyy} 請指定年
    key = "嬰兒出生數按嬰兒性別及生父母年齡分(按登記)"
    url_year_page = "https://www.ris.gov.tw/rs-opendata/api/v1/datastore/ODRP064/{year}?page={page}"
    key = sanitize_filename(key)
    df = []

    def get_data(year, page):
        path = EXTRA_DATA_DIR / key / f"{year}_{page}.json.gz"
        _ensure_dir_exists(path)

        if not path.is_file():
            url = url_year_page.format(year=year, page=page)
            r = session.get(url, verify=False)

            with gzip.open(path, "wb") as f:
                f.write(r.content)

        with gzip.open(path, "rb") as f:
            data = json.load(f)

        return data

    def rename_columns_name(df: pd.DataFrame):
        columns = {
            "統計年度": "statistic_yyy",
            "按照別": "according",
            "行政區域代碼": "district_code",
            "區域別": "site_id",
            "嬰兒性別": "sex",
            "生父年齡": "father_age",
            "生母年齡": "mother_age",
            "嬰兒出生數": "birth_count",
        }
        df.columns = df.columns.str.replace("\ufeff", "")
        return df.rename(columns=columns)

    for year in range(106, datetime.today().year - 1911 + 1):
        page = 1
        json_data = get_data(year, page)
        if "responseData" in json_data:
            data = pd.json_normalize(json_data["responseData"])
            data = rename_columns_name(data)
            df.append(data)

            pages = int(json_data["totalPage"])
            for page in range(2, pages + 1):
                json_data = get_data(year, page)
                data = pd.json_normalize(json_data["responseData"])
                data = rename_columns_name(data)
                df.append(data)

    df = pd.concat(df, ignore_index=True)
    df["birth_count"] = df["birth_count"].astype(int)
    split = (
        df["site_id"].str.replace("(^.{3})", r"\1|", regex=True).str.split("|", n=1, expand=True)
    )
    df["縣市"] = split[0].str.strip()
    df["鄉鎮"] = split[1].str.strip()
    years = df["statistic_yyy"].unique().tolist()
    df["mother_age"] = df["mother_age"].str.replace("～", "~")
    df["father_age"] = df["father_age"].str.replace("～", "~")

    df_total = df.pivot_table(
        values="birth_count", index="statistic_yyy", aggfunc="sum", sort=False
    )
    plots[f"{key}_總和"] = plot_line(
        df_total, f"{key}_總和 {df_total.index[0]}~{df_total.index[-1]}"
    )

    df_區域別 = df.pivot_table(
        values="birth_count", index="statistic_yyy", columns="縣市", aggfunc="sum", sort=False
    )
    plots[f"{key}_區域別"] = plot_line(
        df_區域別, f"{key}_區域別 {df_區域別.index[0]}~{df_區域別.index[-1]}"
    )

    df_性別 = df.pivot_table(
        values="birth_count", index="statistic_yyy", columns="sex", aggfunc="sum", sort=False
    )
    plots[f"{key}_性別"] = plot_line(df_性別, f"{key}_性別 {df_性別.index[0]}~{df_性別.index[-1]}")

    df_生父年齡 = df.pivot_table(
        values="birth_count",
        index="statistic_yyy",
        columns="father_age",
        aggfunc="sum",
        sort=False,
    )
    plots[f"{key}_生父年齡"] = plot_line(
        df_生父年齡, f"{key}_生父年齡 {df_生父年齡.index[0]}~{df_生父年齡.index[-1]}"
    )

    df_生母年齡 = df.pivot_table(
        values="birth_count", index="statistic_yyy", columns="mother_age", aggfunc="sum", sort=False
    )
    plots[f"{key}_生母年齡"] = plot_line(
        df_生母年齡, f"{key}_生母年齡 {df_生母年齡.index[0]}~{df_生母年齡.index[-1]}"
    )

    df_生母年齡_縣市 = df.pivot_table(
        values="birth_count", index="mother_age", columns="縣市", aggfunc="sum", sort=False
    )
    plots[f"{key}_生母年齡_縣市"] = plot_line(
        df_生母年齡_縣市, f"{key}_生母年齡_縣市 {years[0]}~{years[-1]}"
    )

    df_生父年齡_縣市 = df.pivot_table(
        values="birth_count", index="father_age", columns="縣市", aggfunc="sum", sort=False
    )
    plots[f"{key}_生父年齡_縣市"] = plot_line(
        df_生父年齡_縣市, f"{key}_生父年齡_縣市 {years[0]}~{years[-1]}"
    )

    df_生母年齡_生父年齡 = df.pivot_table(
        values="birth_count",
        index="mother_age",
        columns="father_age",
        aggfunc="sum",
        sort=False,
    )
    plots[f"{key}_生母年齡_生父年齡"] = plot_line(
        df_生母年齡_生父年齡,
        f"{key}_生母年齡_生父年齡 {years[0]}~{years[-1]}",
        {"xaxis": {"title": {"text": "生母年齡"}}},
    )

    # https://data.gov.tw/dataset/102765
    key = "嬰兒出生數按生母年齡及出生身分分(按登記)"
    key = sanitize_filename(key)
    urls = {
        107: "https://data.moi.gov.tw/MoiOD/System/DownloadFile.aspx?DATA=94BFACA3-69FD-4A0A-BEB9-44D0EC534415",
        108: "https://data.moi.gov.tw/MoiOD/System/DownloadFile.aspx?DATA=CCB9654B-B722-44A3-8A95-84D56B048F10",
        109: "https://data.moi.gov.tw/MoiOD/System/DownloadFile.aspx?DATA=0C7B49C4-2B84-434E-86D5-E933F9199B90",
        110: "https://data.moi.gov.tw/MoiOD/System/DownloadFile.aspx?DATA=401AD23D-9B8C-4180-B559-55D28B7F1FEA",
        111: "https://data.moi.gov.tw/MoiOD/System/DownloadFile.aspx?DATA=FC12D25D-AB81-45D8-8815-0E60181EFE45",
        112: "https://data.moi.gov.tw/MoiOD/System/DownloadFile.aspx?DATA=068CCDD8-DFEE-464A-A55B-90DB70388ECC",
        113: "https://data.moi.gov.tw/MoiOD/System/DownloadFile.aspx?DATA=6AF685B1-DA07-4E8C-AFE0-C59A3FB06021",
    }

    df = []
    for filename, url in urls.items():
        path = EXTRA_DATA_DIR / key / f"{filename}.csv.gz"
        data = read_csv_with_cache(path, url)
        df.append(data)

    df = pd.concat(df, ignore_index=True)
    df["嬰兒出生數"] = df["嬰兒出生數"].astype(int)
    split = df["區域別"].str.replace("(^.{3})", r"\1|", regex=True).str.split("|", n=1, expand=True)
    df["縣市"] = split[0].str.strip()
    df["鄉鎮"] = split[1].str.strip()
    years = df["統計年度"].unique().tolist()
    df["生母年齡"] = df["生母年齡"].str.replace("～", "~").str.replace(" ", "")

    df_total = df.pivot_table(values="嬰兒出生數", index="統計年度", aggfunc="sum", sort=False)
    plots[f"{key}_總和"] = plot_line(
        df_total, f"{key}_總和 {df_total.index[0]}~{df_total.index[-1]}"
    )

    df_區域別 = df.pivot_table(
        values="嬰兒出生數", index="統計年度", columns="縣市", aggfunc="sum", sort=False
    )
    plots[f"{key}_區域別"] = plot_line(
        df_區域別, f"{key}_區域別 {df_區域別.index[0]}~{df_區域別.index[-1]}"
    )

    df_身分 = df.pivot_table(
        values="嬰兒出生數", index="統計年度", columns="出生身分", aggfunc="sum", sort=False
    )
    plots[f"{key}_身分"] = plot_line(df_身分, f"{key}_身分 {df_身分.index[0]}~{df_身分.index[-1]}")

    df_年齡 = df.pivot_table(
        values="嬰兒出生數", index="統計年度", columns="生母年齡", aggfunc="sum", sort=False
    )
    plots[f"{key}_年齡"] = plot_line(df_年齡, f"{key}_年齡 {df_年齡.index[0]}~{df_年齡.index[-1]}")

    df_年齡_身分 = df.pivot_table(
        values="嬰兒出生數", index="生母年齡", columns="出生身分", aggfunc="sum", sort=False
    )
    plots[f"{key}_年齡_身分"] = plot_line(df_年齡_身分, f"{key}_年齡_身分 {years[0]}~{years[-1]}")

    df_身分_縣市 = df.pivot_table(
        values="嬰兒出生數", index="出生身分", columns="縣市", aggfunc="sum", sort=False
    )
    plots[f"{key}_身分_縣市"] = plot_bar_group(
        df_身分_縣市, f"{key}_身分_縣市 {years[0]}~{years[-1]}"
    )

    # https://data.gov.tw/dataset/100324
    key = "嬰兒出生數按性別、胎次及生母年齡分(按登記)"
    key = sanitize_filename(key)
    urls = {
        106: "https://data.moi.gov.tw/MoiOD/System/DownloadFile.aspx?DATA=E2E702A0-EA3B-4689-8CA9-DD9083E15534",
        107: "https://data.moi.gov.tw/MoiOD/System/DownloadFile.aspx?DATA=5F11E0E0-8F23-4F95-B187-E15F6EF8AAE6",
        108: "https://data.moi.gov.tw/MoiOD/System/DownloadFile.aspx?DATA=87673B9A-31D7-4900-9D7D-62C715EB54AC",
        109: "https://data.moi.gov.tw/MoiOD/System/DownloadFile.aspx?DATA=8B7C2D82-9F36-452C-86F5-2CD370C0CCA4",
        110: "https://data.moi.gov.tw/MoiOD/System/DownloadFile.aspx?DATA=205DD511-67C3-464C-8B47-A273B1258F61",
        111: "https://data.moi.gov.tw/MoiOD/System/DownloadFile.aspx?DATA=D7B783A1-90C2-4C8B-B143-20DBC826F4C2",
        112: "https://data.moi.gov.tw/MoiOD/System/DownloadFile.aspx?DATA=A5E5E762-D38E-463F-9262-0CFFCE2E5AE7",
        113: "https://data.moi.gov.tw/MoiOD/System/DownloadFile.aspx?DATA=1858BBBA-82EC-4E00-8C77-0E206E4CCED2",
    }

    df = []
    for filename, url in urls.items():
        path = EXTRA_DATA_DIR / key / f"{filename}.csv.gz"
        data = read_csv_with_cache(path, url)
        df.append(data)

    df = pd.concat(df, ignore_index=True)
    df["嬰兒出生數"] = df["嬰兒出生數"].astype(int)
    split = df["區域別"].str.replace("(^.{3})", r"\1|", regex=True).str.split("|", n=1, expand=True)
    df["縣市"] = split[0].str.strip()
    df["鄉鎮"] = split[1].str.strip()
    years = df["統計年度"].unique().tolist()
    df["生母年齡"] = df["生母年齡"].str.replace("～", "~").str.replace(" ", "")

    df_total = df.pivot_table(values="嬰兒出生數", index="統計年度", aggfunc="sum", sort=False)
    plots[f"{key}_總和"] = plot_line(
        df_total, f"{key}_總和 {df_total.index[0]}~{df_total.index[-1]}"
    )

    df_區域別 = df.pivot_table(
        values="嬰兒出生數", index="統計年度", columns="縣市", aggfunc="sum", sort=False
    )
    plots[f"{key}_區域別"] = plot_line(
        df_區域別, f"{key}_區域別 {df_區域別.index[0]}~{df_區域別.index[-1]}"
    )

    df_性別 = df.pivot_table(
        values="嬰兒出生數", index="統計年度", columns="出生者性別", aggfunc="sum", sort=False
    )
    plots[f"{key}_性別"] = plot_line(df_性別, f"{key}_性別 {df_性別.index[0]}~{df_性別.index[-1]}")

    df_胎次 = df.pivot_table(
        values="嬰兒出生數", index="統計年度", columns="胎次", aggfunc="sum", sort=False
    )
    plots[f"{key}_胎次"] = plot_line(df_胎次, f"{key}_胎次 {df_胎次.index[0]}~{df_胎次.index[-1]}")

    df_年齡 = df.pivot_table(
        values="嬰兒出生數", index="統計年度", columns="生母年齡", aggfunc="sum", sort=False
    )
    plots[f"{key}_年齡"] = plot_line(df_年齡, f"{key}_年齡 {df_年齡.index[0]}~{df_年齡.index[-1]}")

    df_年齡_胎次 = df.pivot_table(
        values="嬰兒出生數", index="生母年齡", columns="胎次", aggfunc="sum", sort=False
    )
    plots[f"{key}_年齡_胎次"] = plot_line(df_年齡_胎次, f"{key}_年齡_胎次 {years[0]}~{years[-1]}")

    df_胎次_縣市 = df.pivot_table(
        values="嬰兒出生數", index="胎次", columns="縣市", aggfunc="sum", sort=False
    )
    plots[f"{key}_胎次_縣市"] = plot_bar_group(
        df_胎次_縣市, f"{key}_胎次_縣市 {years[0]}~{years[-1]}"
    )

    # https://data.gov.tw/dataset/152789
    key = "嬰兒出生數按嬰兒性別及出生胎別分(按登記)"
    key = sanitize_filename(key)
    urls = {
        110: "https://data.moi.gov.tw/MoiOD/System/DownloadFile.aspx?DATA=C89BA952-9140-47C5-805A-34F3A0773978",
        111: "https://data.moi.gov.tw/MoiOD/System/DownloadFile.aspx?DATA=CBD76D1D-2609-4553-9B8E-06BA912249B8",
        112: "https://data.moi.gov.tw/MoiOD/System/DownloadFile.aspx?DATA=C7432B38-55F0-4DF1-8C91-33770B7A823C",
        113: "https://data.moi.gov.tw/MoiOD/System/DownloadFile.aspx?DATA=A6B28B8E-F601-452F-925A-75A6320A633F",
    }

    df = []
    for filename, url in urls.items():
        path = EXTRA_DATA_DIR / key / f"{filename}.csv.gz"
        data = read_csv_with_cache(path, url)
        df.append(data)

    df = pd.concat(df, ignore_index=True)
    df["嬰兒出生數"] = df["嬰兒出生數"].astype(int)
    split = df["區域別"].str.replace("(^.{3})", r"\1|", regex=True).str.split("|", n=1, expand=True)
    df["縣市"] = split[0].str.strip()
    df["鄉鎮"] = split[1].str.strip()
    years = df["統計年"].unique().tolist()

    df_total = df.pivot_table(values="嬰兒出生數", index="統計年", aggfunc="sum", sort=False)
    plots[f"{key}_總和"] = plot_line(
        df_total, f"{key}_總和 {df_total.index[0]}~{df_total.index[-1]}"
    )

    df_區域別 = df.pivot_table(
        values="嬰兒出生數", index="統計年", columns="縣市", aggfunc="sum", sort=False
    )
    plots[f"{key}_區域別"] = plot_line(
        df_區域別, f"{key}_區域別 {df_區域別.index[0]}~{df_區域別.index[-1]}"
    )

    df_性別 = df.pivot_table(
        values="嬰兒出生數", index="統計年", columns="性別", aggfunc="sum", sort=False
    )
    plots[f"{key}_性別"] = plot_line(df_性別, f"{key}_性別 {df_性別.index[0]}~{df_性別.index[-1]}")

    df_胎別 = df.pivot_table(
        values="嬰兒出生數", index="統計年", columns="胎別", aggfunc="sum", sort=False
    )
    plots[f"{key}_胎別"] = plot_line(df_胎別, f"{key}_胎別 {df_胎別.index[0]}~{df_胎別.index[-1]}")

    df_胎別_縣市 = df.pivot_table(
        values="嬰兒出生數", index="胎別", columns="縣市", aggfunc="sum", sort=False
    )
    plots[f"{key}_胎別_縣市"] = plot_bar_group(
        df_胎別_縣市, f"{key}_胎別_縣市 {years[0]}~{years[-1]}"
    )

    # https://data.gov.tw/dataset/139390
    # API 說明文件
    # https://www.ris.gov.tw/rs-opendata/api/Main/docs/v1
    # API 路徑
    # https://www.ris.gov.tw/rs-opendata/api/v1/datastore/ODRP070/{yyy} 請指定年
    key = "離婚/終止結婚人數按婚姻類型、性別、年齡、原屬國籍（地區）及教育程度分(按登記)"
    url_year_page = "https://www.ris.gov.tw/rs-opendata/api/v1/datastore/ODRP070/{year}?page={page}"
    key = sanitize_filename(key)
    df = []

    def get_data(year, page):
        path = EXTRA_DATA_DIR / key / f"{year}_{page}.json.gz"
        _ensure_dir_exists(path)

        if not path.is_file():
            url = url_year_page.format(year=year, page=page)
            r = session.get(url, verify=False)

            with gzip.open(path, "wb") as f:
                f.write(r.content)

        with gzip.open(path, "rb") as f:
            data = json.load(f)

        return data

    for year in range(111, datetime.today().year - 1911 + 1):
        page = 1
        json_data = get_data(year, page)
        if "responseData" in json_data:
            data = pd.json_normalize(json_data["responseData"])
            df.append(data)

            pages = int(json_data["totalPage"])
            for page in range(2, pages + 1):
                json_data = get_data(year, page)
                data = pd.json_normalize(json_data["responseData"])
                df.append(data)

    df = pd.concat(df, ignore_index=True)
    df["divorce_count"] = df["divorce_count"].astype(int)
    split = (
        df["site_id"].str.replace("(^.{3})", r"\1|", regex=True).str.split("|", n=1, expand=True)
    )
    df["縣市"] = split[0].str.strip()
    df["鄉鎮"] = split[1].str.strip()
    years = df["statistic_yyy"].unique().tolist()

    df_total = df.pivot_table(
        values="divorce_count", index="statistic_yyy", aggfunc="sum", sort=False
    )
    plots[f"{key}_總和"] = plot_line(
        df_total, f"{key}_總和 {df_total.index[0]}~{df_total.index[-1]}"
    )

    df_區域別 = df.pivot_table(
        values="divorce_count", index="statistic_yyy", columns="縣市", aggfunc="sum", sort=False
    )
    plots[f"{key}_區域別"] = plot_line(
        df_區域別, f"{key}_區域別 {df_區域別.index[0]}~{df_區域別.index[-1]}"
    )

    df_婚姻類型 = df.pivot_table(
        values="divorce_count",
        index="statistic_yyy",
        columns="marriage_type",
        aggfunc="sum",
        sort=False,
    )
    plots[f"{key}_婚姻類型"] = plot_line(
        df_婚姻類型, f"{key}_婚姻類型 {df_婚姻類型.index[0]}~{df_婚姻類型.index[-1]}"
    )

    df_性別 = df.pivot_table(
        values="divorce_count", index="statistic_yyy", columns="sex", aggfunc="sum", sort=False
    )
    plots[f"{key}_性別"] = plot_line(df_性別, f"{key}_性別 {df_性別.index[0]}~{df_性別.index[-1]}")

    df_原屬國籍 = df.pivot_table(
        values="divorce_count", index="statistic_yyy", columns="nation", aggfunc="sum", sort=False
    )
    plots[f"{key}_原屬國籍"] = plot_line(
        df_原屬國籍, f"{key}_原屬國籍 {df_原屬國籍.index[0]}~{df_原屬國籍.index[-1]}"
    )

    df_教育程度 = df.pivot_table(
        values="divorce_count", index="statistic_yyy", columns="edu", aggfunc="sum", sort=False
    )
    plots[f"{key}_教育程度"] = plot_line(
        df_教育程度, f"{key}_教育程度 {df_教育程度.index[0]}~{df_教育程度.index[-1]}"
    )

    df_年齡 = df.pivot_table(
        values="divorce_count", index="statistic_yyy", columns="age", aggfunc="sum", sort=False
    )
    plots[f"{key}_年齡"] = plot_line(df_年齡, f"{key}_年齡 {df_年齡.index[0]}~{df_年齡.index[-1]}")

    df_女_年齡_縣市 = df[df["sex"] == "女"].pivot_table(
        values="divorce_count", index="age", columns="縣市", aggfunc="sum", sort=False
    )
    plots[f"{key}_女_年齡_縣市"] = plot_line(
        df_女_年齡_縣市, f"{key}_女_年齡_縣市 {years[0]}~{years[-1]}"
    )

    df_男_年齡_縣市 = df[df["sex"] == "男"].pivot_table(
        values="divorce_count", index="age", columns="縣市", aggfunc="sum", sort=False
    )
    plots[f"{key}_男_年齡_縣市"] = plot_line(
        df_男_年齡_縣市, f"{key}_男_年齡_縣市 {years[0]}~{years[-1]}"
    )

    df_女_年齡_教育 = df[df["sex"] == "女"].pivot_table(
        values="divorce_count", index="age", columns="edu", aggfunc="sum", sort=False
    )
    plots[f"{key}_女_年齡_教育"] = plot_line(
        df_女_年齡_教育, f"{key}_女_年齡_教育 {years[0]}~{years[-1]}"
    )

    df_男_年齡_教育 = df[df["sex"] == "男"].pivot_table(
        values="divorce_count", index="age", columns="edu", aggfunc="sum", sort=False
    )
    plots[f"{key}_男_年齡_教育"] = plot_line(
        df_男_年齡_教育, f"{key}_男_年齡_教育 {years[0]}~{years[-1]}"
    )

    # =======================================================================

    # https://data.gov.tw/dataset/41236
    key = "全國公立動物收容所收容處理情形統計表"
    url = "https://data.moa.gov.tw/Service/OpenData/TransService.aspx?UnitId=DyplMIk3U1hf&IsTransData=1"
    key = sanitize_filename(key)
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
    columns_revise = columns.map(sanitize_filename)
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
        df_item = df_item.sort_index()
        plots[f"{key}_{col}"] = plot_line(
            df_item, f"{key}_{col} {df_item.index[0]}~{df_item.index[-1]}"
        )

    # ========================================================================

    prefix = "TW_Analysis"
    report_dir = Path("report")
    with app.app_context():
        jsfolder = Path("report") / prefix
        jsfolder.mkdir(parents=True, exist_ok=True)

        for key, item in plots.items():
            graph = render_template("graph.js.j2", key=key, item=item)
            with open(jsfolder / f"{key}.js", "w", encoding="UTF-8") as f:
                f.write(graph)

        html = render_template(
            "tw_analysis.html.j2",
            plots=plots,
            jsfolder=jsfolder.name,
            title=f"{prefix} Report",
            items=items,
        )
        with open(report_dir / f"{prefix}_Report.html", "w", encoding="UTF-8") as f:
            f.write(html)
