import json
import minify_html
import numpy as np
import pandas as pd
import plotly
import copy
import warnings

from jsmin import jsmin
from datetime import datetime
from typing import Callable, Optional, Dict, Any
from pathlib import Path
from flask import Flask, render_template

from tw_analysis_data import *

app = Flask(__name__)

# Regular expression for sanitizing strings for use as keys or filenames
FILENAME_SANITIZE_PATTERN = r'[- ,、()~∕\/－%*?:"<>|（）—]+'
# Base directory for caching downloaded data
EXTRA_DATA_DIR = Path("./extraData/TW_Analysis")


def merge_dict(a: Dict, b: Dict, path: Optional[list] = None, overwrite: bool = True) -> Dict:
    """Merges b into a. If overwrite is True, b's values will overwrite a's on conflict."""
    if path is None:
        path = []
    for key in b:
        if key in a:
            if isinstance(a[key], dict) and isinstance(b[key], dict):
                merge_dict(a[key], b[key], path + [str(key)], overwrite)
            elif isinstance(a[key], list) and isinstance(b[key], list):
                a[key] += b[key]
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
            # "height": 600,
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
            "annotations": [
                {
                    "text": "<b>@zwindr<b>",  # The text for your watermark
                    "textangle": 0,  # Angle of the text (e.g., for a diagonal watermark)
                    "opacity": 0.5,  # Transparency of the text (lower for a subtle watermark)
                    "font": {
                        "color": "white",  # Color of the text
                        "size": 12,  # Size of the text
                    },
                    "xref": "paper",  # Reference x-coordinate to the entire plot area (0 to 1)
                    "yref": "paper",  # Reference y-coordinate to the entire plot area (0 to 1)
                    "xanchor": "right",
                    "yanchor": "bottom",
                    "x": 1.0,  # X-position (0.5 for center)
                    "y": 1.0,  # Y-position (0.5 for center)
                    "showarrow": False,  # Hide the arrow typically associated with annotations
                }
            ],
        },
        "config": {
            "responsive": True,
            "toImageButtonOptions": {
                # "format": "png",  # one of png, svg, jpeg, webp
                "filename": "image",
                # "width": 1920,
                # "height": 1080,
                "scale": 5,  # Multiply title/legend/axis/canvas sizes by this factor,
            },
        },
    },
)


def plotly_json_dump(graph_dict: Dict) -> str:
    """Serializes a Plotly graph dictionary to JSON string."""
    return json.dumps(graph_dict, cls=plotly.utils.PlotlyJSONEncoder)


def plot_line(
    df: pd.DataFrame,
    title: Optional[str] = None,
    additional_layout: Optional[Dict] = None,
    mode="lines",
    visible={},
) -> str:
    data_list = []
    for name in df.columns:
        data = {
            "type": "scatter",
            "name": name,
            "x": df.index.tolist(),  # Ensure x-axis is list for JSON
            "y": df[name].tolist(),  # 若有錯，通常是 name 有重覆
            "mode": mode,
            "visible": visible.get(name, True),
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
    df: pd.DataFrame,
    title: Optional[str] = None,
    additional_layout: Optional[Dict] = None,
    visible={},
) -> str:
    data_list = []
    for name in df.columns:
        data = {
            "type": "bar",
            "name": name,
            "x": [name],
            "y": df[name].tolist(),
            "visible": visible.get(name, True),
        }
        data_list.append(data)

    layout = {
        "title": {"text": title},
        "hovermode": "x",  # "closest" might be better for bar charts
    }
    if additional_layout:
        layout = merge_dict(layout, additional_layout)

    graph = {"data": data_list, "layout": layout}
    graph = merge_dict(copy.deepcopy(default_template), graph)

    return plotly_json_dump(graph)


def plot_bar_group(
    df: pd.DataFrame,
    title: Optional[str] = None,
    additional_layout: Optional[Dict] = None,
    visible={},
) -> str:
    data_list = []
    for name in df.columns:
        data = {
            "type": "bar",
            "name": name,
            "x": df.index.tolist(),
            "y": df[name].tolist(),
            "visible": visible.get(name, True),
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
    df: pd.DataFrame,
    title: Optional[str] = None,
    additional_layout: Optional[Dict] = None,
    visible={},
) -> str:
    data_list = []
    for name in df.columns:
        data = {
            "type": "bar",
            "name": name,
            "x": list(zip(*df.index.tolist())),
            "y": df[name].tolist(),
            "visible": visible.get(name, True),
        }
        data_list.append(data)

    layout = {
        "title": {"text": title},
        "hovermode": "x",
        "barmode": "stack",
    }
    if additional_layout:
        layout = merge_dict(layout, additional_layout)

    graph = {"data": data_list, "layout": layout}
    graph = merge_dict(copy.deepcopy(default_template), graph)

    return plotly_json_dump(graph)


def plot_lines_bars(
    df: pd.DataFrame,
    lines_left_axis: list[str] = [],
    lines_right_axis: list[str] = [],
    lines_mode="lines",
    bars_left_axis: list[str] = [],
    bars_right_axis: list[str] = [],
    title: Optional[str] = None,
    additional_layout: Optional[Dict] = None,
    legendgroup: bool = False,
    sort: bool = False,
    visible={},
):
    data_list = []
    for name in lines_left_axis:
        data = {
            "type": "scatter",
            "name": name,
            "x": df.index.tolist(),
            "y": df[name].tolist(),
            "mode": lines_mode,
            "visible": visible.get(name, True),
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
            "mode": lines_mode,
            "yaxis": "y2",
            "visible": visible.get(name, True),
        }
        if legendgroup:
            data["legendgroup"] = str.rsplit(name, "_", 1)[1]
        data_list.append(data)

    for name in bars_left_axis:
        data = {
            "type": "bar",
            "name": name,
            "x": df.index.tolist(),
            "y": df[name].tolist(),
            "visible": visible.get(name, True),
        }
        if legendgroup:
            data["legendgroup"] = str.rsplit(name, "_", 1)[1]
        data_list.append(data)

    for name in bars_right_axis:
        data = {
            "type": "bar",
            "name": name,
            "x": df.index.tolist(),
            "y": df[name].tolist(),
            "yaxis": "y2",
            "visible": visible.get(name, True),
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


def plot_pyramid(title: str, df_男: pd.DataFrame, df_女: pd.DataFrame, regions: list[str], y: list):
    def visible(region):
        if "總計" in regions:
            if region == "總計":
                return True
            else:
                return "legendonly"
        else:
            return True

    data_list = []
    for region in regions:
        data = {
            "type": "bar",
            "name": region + "_女",
            "x": (-df_女[region]).tolist(),
            "y": y,
            "orientation": "h",
            "legendgroup": region,
            "visible": visible(region),
        }
        data_list.append(data)
        data = {
            "type": "bar",
            "name": region + "_男",
            "x": df_男[region].tolist(),
            "y": y,
            "yaxis": "y2",
            "orientation": "h",
            "legendgroup": region,
            "visible": visible(region),
        }
        data_list.append(data)
        data = {
            "type": "bar",
            "name": region + "_差異",
            "x": (df_男[region] - df_女[region]).tolist(),
            "y": y,
            "xaxis": "x2",
            "yaxis": "y",
            "orientation": "h",
            "legendgroup": region,
            "visible": visible(region),
        }
        data_list.append(data)
        data = {
            "type": "bar",
            "name": region + "_女男比值",
            "x": df_女[region] / df_男[region],
            "y": y,
            "xaxis": "x3",
            "yaxis": "y",
            "orientation": "h",
            "legendgroup": region,
            "visible": visible(region),
        }
        data_list.append(data)

    layout = {
        "title": {"text": title},
        "hovermode": "y",
        "hoversubplots": "axis",
        "bargap": 0.1,
        "xaxis": {
            "domain": [0, 0.95],
        },
        "xaxis2": {"domain": [0.95, 0.98], "showgrid": False, "visible": False},
        "xaxis3": {"domain": [0.98, 1], "showgrid": False, "visible": False},
        "yaxis2": {"overlaying": "y", "visible": False},
    }

    graph = {"data": data_list[::-1], "layout": layout}
    graph = merge_dict(copy.deepcopy(default_template), graph)

    return plotly_json_dump(graph)


def plot_histogram(
    df: pd.DataFrame, title: Optional[str] = None, additional_layout: Optional[Dict] = None
) -> str:
    data_list = []
    for idx in df.index:
        data = {
            "type": "histogram",
            "name": idx,
            "x": df.loc[idx],
        }
        data_list.append(data)

    layout = {
        "title": {"text": title},
        "barmode": "stack",
    }
    if additional_layout:
        layout = merge_dict(layout, additional_layout)

    graph = {"data": data_list, "layout": layout}
    graph = merge_dict(copy.deepcopy(default_template), graph)

    return plotly_json_dump(graph)


def plot_box(
    df: pd.DataFrame,
    title: Optional[str] = None,
    additional_layout: Optional[Dict] = None,
    visible={},
) -> str:
    data_list = []
    for name in df.columns:
        data = {
            "type": "box",
            "name": name,
            "y": df[name].tolist(),
            "visible": visible.get(name, True),
        }
        data_list.append(data)

    layout = {
        "title": {"text": title},
        "hovermode": "x",  # "closest" might be better for bar charts
    }
    if additional_layout:
        layout = merge_dict(layout, additional_layout)

    graph = {"data": data_list, "layout": layout}
    graph = merge_dict(copy.deepcopy(default_template), graph)

    return plotly_json_dump(graph)


def plot_violin(
    df: pd.DataFrame,
    title: Optional[str] = None,
    additional_layout: Optional[Dict] = None,
    visible={},
) -> str:
    data_list = []
    for name in df.columns:
        data = {
            "type": "violin",
            "name": name,
            "y": df[name].tolist(),  # 若有錯，通常是 name 有重覆
            "points": "suspectedoutliers",
            "meanline": {
                "visible": True,
            },
            "box": {
                "visible": True,
            },
            "boxpoints": False,
            "visible": visible.get(name, True),
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


# --- Specific Data Processing and Plotting Functions ---


def index_原始值_年增率_plot(
    plots: Dict,
    key: str,
    df_get: Callable[[], pd.DataFrame],
    title_suffix: str,
    additional_layout: Optional[Dict] = None,
):
    key = sanitize_filename(key)

    df = df_get()
    date_range = f"{df["TIME_PERIOD"].iloc[0]}~{df["TIME_PERIOD"].iloc[-1]}"

    kinds = df["Item"].unique().tolist()
    buttons_kinds = [
        {
            "args": [
                {
                    "visible": [True] * len(kinds),
                }
            ],  # 顯示所有線條
            "label": "全部類別",
            "method": "restyle",
        }
    ]
    primary_category = ["一", "二", "三", "四", "五", "六", "七", "八", "九"]
    secondary_category = [f"{i}." for i in range(1, 100)]

    def check_string_contains_any(text, substrings):
        return any(substring in text for substring in substrings)

    for i, kind in enumerate(kinds):
        if check_string_contains_any(kind, primary_category):
            category = primary_category
        elif check_string_contains_any(kind, secondary_category):
            category = primary_category + secondary_category
        else:
            continue

        for j in range(i + 1, len(kinds)):
            if check_string_contains_any(kinds[j], category):
                break
        else:
            j = len(kinds)

        arr = [i <= k and k < j for k in range(len(kinds))]
        buttons_kinds.append(
            {
                "args": [
                    {"visible": arr},
                ],
                "label": kind,
                "method": "restyle",
            },
        )

    updatemenus = [
        {
            "x": 0.5,
            "y": 1.09,
            "xanchor": "center",
            "yanchor": "top",
            "pad": {"r": 10, "t": 10},
            "buttons": buttons_kinds,
            "type": "dropdown",
            "direction": "down",
            "active": 0,
            "font": {"color": "#AAAAAA"},
            "name": "類別選擇",
        },
    ]
    if additional_layout:
        additional_layout = merge_dict({"updatemenus": updatemenus}, additional_layout)
    else:
        additional_layout = {"updatemenus": updatemenus}

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
        irr_df,
        f"{key} IRR(%) {title_suffix} {date_range}",
        additional_layout | {"yaxis": {"tickformat": ".2%"}},
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
    df_get: Callable[[], pd.DataFrame],
    title_suffix: str,
    additional_layout: Optional[Dict] = None,
):
    key = sanitize_filename(key)
    df = df_get()

    plots[key] = plot_line(
        df, f"{key}{title_suffix} {df.index[0]}~{df.index[-1]}年", additional_layout
    )


def 年月混合_plot(
    plots: Dict,
    key: str,
    df_get: Callable[[], pd.DataFrame],
    title_suffix: str,
    additional_layout: Optional[Dict] = None,
    sum_or_keep="sum",
):
    key = sanitize_filename(key)
    df = df_get()

    df_year = df.filter(regex=r"\d+年$", axis="index")
    df_month = df.filter(regex=r"\d+年 *\d+月$", axis="index")

    if not df.index[-1].startswith(df_year.index[-1]):
        last_year = df.filter(regex=rf"^{df.index[-1][:5]} *\d+月$", axis="index")
        if sum_or_keep == "sum":
            last_year = pd.DataFrame({last_year.index[-1]: last_year.sum(axis="index")}).T
        df_year = pd.concat([df_year, last_year], axis="index")

    plots[f"{key}_年"] = plot_line(
        df_year,
        f"{key}_年{title_suffix} {df_month.index[0]}~{df_year.index[-1]}",
        additional_layout,
    )
    plots[f"{key}_月"] = plot_line(
        df_month,
        f"{key}_月{title_suffix} {df_month.index[0]}~{df_month.index[-1]}",
        additional_layout,
    )


def plot_消費者物價基本分類指數(plots):
    index_原始值_年增率_plot(
        plots,
        key="消費者物價基本分類指數",
        df_get=df_消費者物價基本分類指數,
        title_suffix="(指數基期：民國110年=100)",
    )


def plot_生產者物價基本分類指數(plots):
    index_原始值_年增率_plot(
        plots,
        key="生產者物價基本分類指數",
        df_get=df_生產者物價基本分類指數,
        title_suffix="(指數基期：民國110年=100)",
    )


def plot_躉售物價基本分類指數(plots):
    index_原始值_年增率_plot(
        plots,
        key="躉售物價基本分類指數",
        df_get=df_躉售物價基本分類指數,
        title_suffix="(指數基期：民國105年=100)",
    )


def plot_人力資源調查失業率(plots):
    key = "人力資源調查失業率"
    key = sanitize_filename(key)
    df = df_人力資源調查失業率()

    df_year = df.filter(regex=r"^\d{4}$", axis="index").dropna(axis="index", how="all")
    index_year = int(re.sub(r"[^0-9]*", "", df_year.index[-1]))
    if index_year != datetime.today().year:
        last_year = df.filter(regex=rf"^{datetime.today().year}M", axis="index")
        df_year = pd.concat([df_year, last_year])

    plots[f"{key}_年"] = plot_line(
        df_year,
        f"{key}_年 {df_year.index[0]}~{df_year.index[-1]}年",
        additional_layout={"hovermode": "x", "yaxis": {"tickformat": ".2%"}},
    )

    df_month = df.filter(regex=r"^\d{4}M", axis="index")
    plots[f"{key}_月"] = plot_line(
        df_month,
        f"{key}_月 {df_month.index[0]}~{df_month.index[-1]}",
        additional_layout={"hovermode": "x", "yaxis": {"tickformat": ".2%"}},
    )


def plot_人力資源調查縣市別失業率(plots):
    key = "人力資源調查縣市別失業率"
    key = sanitize_filename(key)
    df = df_人力資源調查縣市別失業率()

    df_year = df.filter(regex=r"^\d{4}$", axis="index")
    df_year = df_year.dropna(axis="index", how="all")
    index_year = int(re.sub(r"[^0-9]*", "", df_year.index[-1]))
    if index_year != datetime.today().year:
        last_year = df.filter(regex=rf"^{datetime.today().year}[a-zA-Z]", axis="index")
        df_year = pd.concat([df_year, last_year])
    plots[f"{key}_年"] = plot_line(
        df_year,
        f"{key}_年 {df_year.index[0]}~{df_year.index[-1]}年",
        additional_layout={"hovermode": "x", "yaxis": {"tickformat": ".2%"}},
    )

    df_month = df.filter(regex=r"[a-zA-Z]", axis="index")
    plots[f"{key}_月"] = plot_line(
        df_month,
        f"{key}_月 {df_month.index[0]}~{df_month.index[-1]}",
        additional_layout={"hovermode": "x", "yaxis": {"tickformat": ".2%"}},
    )


def plot_歷年人力資源調查重要指標(plots):
    key = "歷年人力資源調查重要指標"
    key = sanitize_filename(key)
    df = df_歷年人力資源調查重要指標()

    plots[f"{key}"] = plot_line(
        df,
        f"{key} {df.index[0]}~{df.index[-1]}年",
        additional_layout={"hovermode": "x", "yaxis": {"tickformat": ".2%"}},
    )


def plot_人力資源調查重要指標(plots):
    key = "人力資源調查重要指標"
    key = sanitize_filename(key)
    df = df_人力資源調查重要指標()
    num_columns = [
        column for column in df.columns if "年度" not in column and "地區別分" not in column
    ]

    df_類別_地區 = df.pivot_table(
        values=num_columns, index="年度", columns="按地區別分", sort=False
    )

    buttons_kinds = [
        {
            "args": [
                {
                    "visible": [True] * len(df_類別_地區.columns),
                }
            ],  # 顯示所有線條
            "label": "全部指標",
            "method": "restyle",
        }
    ]
    for kind in num_columns:
        arr = [kind_col == kind for kind_col, region_col in df_類別_地區.columns]
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

    buttons_region = [
        {
            "args": [
                {
                    "visible": [True] * len(df_類別_地區.columns),
                }
            ],  # 顯示所有線條
            "label": "全部區域",
            "method": "restyle",
        }
    ]
    regions = df["按地區別分"].unique().tolist()
    for region in regions:
        arr = [region_col == region for kind_col, region_col in df_類別_地區.columns]
        buttons_region.append(
            {
                "args": [
                    {"visible": arr},
                ],
                "label": region,
                "method": "restyle",
            },
        )

    updatemenus = [
        {
            "x": 0.5,
            "y": 1.09,
            "xanchor": "center",
            "yanchor": "top",
            "pad": {"r": 10, "t": 10},
            "buttons": buttons_kinds,
            "type": "dropdown",
            "direction": "down",
            "active": 0,
            "font": {"color": "#AAAAAA"},
            "name": "指標選擇",
        },
        {
            "x": 0.8,
            "y": 1.09,
            "xanchor": "center",
            "yanchor": "top",
            "pad": {"r": 10, "t": 10},
            "buttons": buttons_region,
            "type": "dropdown",
            "direction": "down",
            "active": 0,
            "font": {"color": "#AAAAAA"},
            "name": "地區選擇",
        },
    ]

    df_類別_地區.columns = [f"{region}_{kind}" for kind, region in df_類別_地區.columns]
    plots[f"{key}"] = plot_line(
        df_類別_地區,
        f"{key} {df_類別_地區.index[0]}~{df_類別_地區.index[-1]}年",
        additional_layout={
            "hovermode": "x",
            "yaxis": {"tickformat": ".2%"},
            "updatemenus": updatemenus,
        },
    )


def plot_歷年教育程度別失業率(plots):
    key = "歷年教育程度別失業率"
    key = sanitize_filename(key)
    df = df_歷年教育程度別失業率()

    plots[f"{key}"] = plot_line(
        df,
        f"{key} {df.index[0]}~{df.index[-1]}年",
        additional_layout={"hovermode": "x", "yaxis": {"tickformat": ".2%"}},
    )


def plot_教育程度別失業率(plots):
    key = "教育程度別失業率"
    key = sanitize_filename(key)
    df = df_教育程度別失業率()
    num_columns = [
        column for column in df.columns if "年度" not in column and "地區別分" not in column
    ]

    df_類別_地區 = df.pivot_table(
        values=num_columns, index="年度", columns="按地區別分_District_or_region", sort=False
    )

    buttons_kinds = [
        {
            "args": [
                {
                    "visible": [True] * len(df_類別_地區.columns),
                }
            ],  # 顯示所有線條
            "label": "全部類別",
            "method": "restyle",
        }
    ]
    for kind in num_columns:
        arr = [kind_col == kind for kind_col, region_col in df_類別_地區.columns]
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

    buttons_region = [
        {
            "args": [
                {
                    "visible": [True] * len(df_類別_地區.columns),
                }
            ],  # 顯示所有線條
            "label": "全部區域",
            "method": "restyle",
        }
    ]
    regions = df["按地區別分_District_or_region"].unique().tolist()
    for region in regions:
        arr = [region_col == region for kind_col, region_col in df_類別_地區.columns]
        buttons_region.append(
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
            "x": 0.5,
            "y": 1.09,
            "xanchor": "center",
            "yanchor": "top",
            "pad": {"r": 10, "t": 10},
            "buttons": buttons_kinds,
            "type": "dropdown",
            "direction": "down",
            "active": 0,
            "font": {"color": "#AAAAAA"},
            "name": "類別選擇",
        },
        {
            "x": 0.65,
            "y": 1.09,
            "xanchor": "center",
            "yanchor": "top",
            "pad": {"r": 10, "t": 10},
            "buttons": buttons_region,
            "type": "dropdown",
            "direction": "down",
            "active": 0,
            "font": {"color": "#AAAAAA"},
            "name": "區域選擇",
        },
    ]

    df_類別_地區.columns = [f"{region}_{kind}" for kind, region in df_類別_地區.columns]
    plots[f"{key}"] = plot_line(
        df_類別_地區,
        f"{key} {df_類別_地區.index[0]}~{df_類別_地區.index[-1]}年",
        additional_layout={
            "hovermode": "x",
            "yaxis": {"tickformat": ".2%"},
            "updatemenus": updatemenus,
        },
    )


def plot_歷年年齡組別失業率(plots):
    key = "歷年年齡組別失業率"
    key = sanitize_filename(key)
    df = df_歷年年齡組別失業率()

    plots[f"{key}"] = plot_line(
        df,
        f"{key} {df.index[0]}~{df.index[-1]}年",
        additional_layout={"hovermode": "x", "yaxis": {"tickformat": ".2%"}},
    )


def plot_年齡組別失業率(plots):
    key = "年齡組別失業率"
    key = sanitize_filename(key)
    df = df_年齡組別失業率()
    num_columns = [
        column for column in df.columns if "年度" not in column and "地區別分" not in column
    ]

    df_年齡_地區 = df.pivot_table(
        values=num_columns, index="年度", columns="按地區別分_District_or_region", sort=False
    )

    buttons_kinds = [
        {
            "args": [
                {
                    "visible": [True] * len(df_年齡_地區.columns),
                }
            ],  # 顯示所有線條
            "label": "全部年齡",
            "method": "restyle",
        }
    ]
    for age in num_columns:
        arr = [age_col == age for age_col, region_col in df_年齡_地區.columns]
        buttons_kinds.append(
            {
                "args": [
                    {
                        "visible": arr,
                    }
                ],
                "label": age,
                "method": "restyle",
            },
        )

    buttons_region = [
        {
            "args": [
                {
                    "visible": [True] * len(df_年齡_地區.columns),
                }
            ],  # 顯示所有線條
            "label": "全部區域",
            "method": "restyle",
        }
    ]
    regions = df["按地區別分_District_or_region"].unique().tolist()
    for region in regions:
        arr = [region_col == region for age_col, region_col in df_年齡_地區.columns]
        buttons_region.append(
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
            "x": 0.5,
            "y": 1.09,
            "xanchor": "center",
            "yanchor": "top",
            "pad": {"r": 10, "t": 10},
            "buttons": buttons_kinds,
            "type": "dropdown",
            "direction": "down",
            "active": 0,
            "font": {"color": "#AAAAAA"},
            "name": "年齡選擇",
        },
        {
            "x": 0.65,
            "y": 1.09,
            "xanchor": "center",
            "yanchor": "top",
            "pad": {"r": 10, "t": 10},
            "buttons": buttons_region,
            "type": "dropdown",
            "direction": "down",
            "active": 0,
            "font": {"color": "#AAAAAA"},
            "name": "地區選擇",
        },
    ]

    df_年齡_地區.columns = [f"{region}_{age}" for age, region in df_年齡_地區.columns]
    plots[f"{key}"] = plot_line(
        df_年齡_地區,
        f"{key} {df_年齡_地區.index[0]}~{df_年齡_地區.index[-1]}年",
        additional_layout={
            "hovermode": "x",
            "yaxis": {"tickformat": ".2%"},
            "updatemenus": updatemenus,
        },
    )


def plot_教育程度別失業率_按年齡分(plots):
    key = "教育程度別失業率—按年齡分"
    key = sanitize_filename(key)
    df = df_教育程度別失業率_按年齡分()
    num_columns = [
        column for column in df.columns if "年度" not in column and "項目別" not in column
    ]

    df_年齡_教育 = df.pivot_table(
        values=num_columns, index="年度", columns="項目別_Item", sort=False
    )

    buttons_edu = [
        {
            "args": [
                {
                    "visible": [True] * len(df_年齡_教育.columns),
                }
            ],  # 顯示所有線條
            "label": "全部教育程度",
            "method": "restyle",
        }
    ]
    for edu in num_columns:
        arr = [edu_col == edu for edu_col, age_col in df_年齡_教育.columns]
        buttons_edu.append(
            {
                "args": [
                    {
                        "visible": arr,
                    }
                ],
                "label": edu,
                "method": "restyle",
            },
        )

    buttons_age = [
        {
            "args": [
                {
                    "visible": [True] * len(df_年齡_教育.columns),
                }
            ],  # 顯示所有線條
            "label": "全部年齡",
            "method": "restyle",
        }
    ]
    ages = df["項目別_Item"].unique().tolist()
    for age in ages:
        arr = [age_col == age for edu_col, age_col in df_年齡_教育.columns]
        buttons_age.append(
            {
                "args": [
                    {
                        "visible": arr,
                    }
                ],
                "label": age,
                "method": "restyle",
            },
        )

    updatemenus = [
        {
            "x": 0.5,
            "y": 1.09,
            "xanchor": "center",
            "yanchor": "top",
            "pad": {"r": 10, "t": 10},
            "buttons": buttons_edu,
            "type": "dropdown",
            "direction": "down",
            "active": 0,
            "font": {"color": "#AAAAAA"},
            "name": "教育選擇",
        },
        {
            "x": 0.7,
            "y": 1.09,
            "xanchor": "center",
            "yanchor": "top",
            "pad": {"r": 10, "t": 10},
            "buttons": buttons_age,
            "type": "dropdown",
            "direction": "down",
            "active": 0,
            "font": {"color": "#AAAAAA"},
            "name": "年齡選擇",
        },
    ]

    df_年齡_教育.columns = [f"{edu}_{age}" for age, edu in df_年齡_教育.columns]
    plots[f"{key}"] = plot_line(
        df_年齡_教育,
        f"{key} {df_年齡_教育.index[0]}~{df_年齡_教育.index[-1]}年",
        additional_layout={
            "hovermode": "x",
            "yaxis": {"tickformat": ".2%"},
            "updatemenus": updatemenus,
        },
    )


def plot_就業率(plots):
    key = "就業率"
    key = sanitize_filename(key)
    df_教育程度別, df_年齡別 = df_就業率()

    plots[f"{key}_教育程度別"] = plot_line(
        df_教育程度別,
        f"{key}_教育程度別 {df_教育程度別.index[0]}~{df_教育程度別.index[-1]}年",
        additional_layout={"hovermode": "x", "yaxis": {"tickformat": ".2%"}},
    )

    plots[f"{key}_年齡別"] = plot_line(
        df_年齡別,
        f"{key}_年齡別 {df_年齡別.index[0]}~{df_年齡別.index[-1]}年",
        additional_layout={"hovermode": "x", "yaxis": {"tickformat": ".2%"}},
    )


def plot_主要國家零歲平均餘命(plots):
    key = "主要國家零歲平均餘命"
    key = sanitize_filename(key)
    df = df_主要國家零歲平均餘命()

    df = copy.deepcopy(df)
    df.columns = [f"{國別}_{性別}" for 國別, 性別 in df.columns]

    plots[f"{key}_歷年"] = plot_line(
        df,
        f"{key}(歲) {df.index[0]}~{df.index[-1]}年",
    )

    df_實際值 = df.loc[df.index <= datetime.today().year]
    df_當年度 = df_實際值.loc[[df_實際值.index[-1]]]
    df_當年度 = df_當年度.sort_values(by=df_實際值.index[-1], axis="columns", ascending=False)
    plots[f"{key}_當年度"] = plot_bar_group(
        df_當年度,
        f"{key}(歲) {df_實際值.index[-1]}年",
    )


def plot_房價所得比(plots):
    key = "房價所得比"
    key = sanitize_filename(key)
    df = df_房價所得比()

    plots[f"{key}"] = plot_line(
        df,
        f"{key} = 中位數住宅價格/中位數家戶可支配所得 {df.index[0]}~{df.index[-1]}",
    )


def plot_貸款負擔率(plots):
    key = "貸款負擔率"
    key = sanitize_filename(key)
    df = df_貸款負擔率()

    plots[f"{key}"] = plot_line(
        df,
        f"{key} = 中位數房價貸款月攤還額/家戶月可支配所得中位數 {df.index[0]}~{df.index[-1]}",
        additional_layout={"yaxis": {"tickformat": ".2%"}},
    )


def plot_國民所得統計_國民所得_儲蓄與投資_季(plots):
    key = "國民所得統計-國民所得、儲蓄與投資-季"
    key = sanitize_filename(key)
    df = df_國民所得統計_國民所得_儲蓄與投資_季()

    pivot_df = df[df["TYPE"] == "原始值"].pivot_table(
        index="TIME_PERIOD", columns="Item", values="Item_VALUE", sort=False
    )

    pat_filter = "當期價格(新臺幣元)"
    df_filter = pivot_df[[column for column in pivot_df.columns if pat_filter in column]]
    df_filter.columns = df_filter.columns.str.replace(f"{pat_filter}、", "")
    plots[f"{key}_原始值_當期價格"] = plot_line(
        df_filter,
        f"{key} 原始值 {pat_filter} {df_filter.index[0]}~{df_filter.index[-1]}",
        {"hovermode": "x unified"},
    )

    pat_filter = "連鎖實質值(2021為參考年_新臺幣元)"
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

    pat_filter = "當期價格(新臺幣元)"
    df_filter = pivot_df[[column for column in pivot_df.columns if pat_filter in column]]
    df_filter.columns = df_filter.columns.str.replace(f"{pat_filter}、", "")
    plots[f"{key}_年增率_當期價格"] = plot_line(
        df_filter / 100,
        f"{key} 年增率(%) {pat_filter} {df_filter.index[0]}~{df_filter.index[-1]}",
        {"yaxis": {"tickformat": ".2%"}},
    )

    pat_filter = "連鎖實質值(2021為參考年_新臺幣元)"
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


def plot_國民所得統計_常用資料_季(plots):
    key = "國民所得統計-常用資料-季"
    key = sanitize_filename(key)
    df = df_國民所得統計_常用資料_季()

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


def plot_家庭收支調查_家庭收支重要指標(plots):
    key = "家庭收支調查-家庭收支重要指標"
    key = sanitize_filename(key)
    df = df_家庭收支調查_家庭收支重要指標()

    percent_columns = [col for col in df.columns if "百分比" in col]
    df[percent_columns] = df[percent_columns] / 100

    plots[f"{key}_人數"] = plot_line(
        df[
            [
                "平均每戶人數-人",
                "平均每戶就業人數-人",
            ]
        ],
        title=f"{key}_人數 {df.index[0]}~{df.index[-1]}年",
    )

    plots[f"{key}_收支"] = plot_lines_bars(
        df,
        lines_left_axis=[
            "每戶可支配所得平均數-元",
            "每人可支配所得平均數-元",
            "每戶可支配所得中位數-元",
            "每人可支配所得中位數-元",
            "消費支出-元",
            "儲蓄-元",
        ],
        lines_right_axis=[
            "消費傾向-百分比",
            "儲蓄率-百分比",
        ],
        title=f"{key}_收支 {df.index[0]}~{df.index[-1]}年",
        additional_layout={"yaxis2": {"tickformat": ".2%"}},
    )

    plots[f"{key}_居住"] = plot_lines_bars(
        df,
        lines_left_axis=["住宅坪數平均數-坪", "住宅坪數中位數-坪"],
        lines_right_axis=["自有住宅率-百分比"],
        title=f"{key}_居住 {df.index[0]}~{df.index[-1]}年",
        additional_layout={"yaxis2": {"tickformat": ".2%"}},
    )


def plot_家庭收支調查_所得總額按來源別分(plots):
    key = "家庭收支調查-所得總額按來源別分"
    key = sanitize_filename(key)
    df = df_家庭收支調查_所得總額按來源別分()

    percent_columns = [col for col in df.columns if "百分比" in col]
    df[percent_columns] = df[percent_columns] / 100

    plots[key] = plot_lines_bars(
        df,
        lines_left_axis=percent_columns,
        bars_right_axis=[col for col in df.columns if "百分比" not in col],
        title=f"{key} {df.index[0]}~{df.index[-1]}年",
        additional_layout={"yaxis": {"tickformat": ".2%"}},
    )


def plot_家庭收支調查_所得總額與可支配所得(plots):
    key = "家庭收支調查-所得總額與可支配所得"
    key = sanitize_filename(key)
    df = df_家庭收支調查_所得總額與可支配所得()

    plots[key] = plot_lines_bars(
        df,
        lines_left_axis=[col for col in df.columns if "平均每戶" in col],
        bars_right_axis=[col for col in df.columns if "平均每戶" not in col],
        title=f"{key}-元 {df.index[0]}~{df.index[-1]}年",
    )


def plot_家庭收支調查_所得收入者人數與按年齡組別及性別之分配(plots):
    key = "家庭收支調查-所得收入者人數與按年齡組別及性別之分配"
    key = sanitize_filename(key)
    df = df_家庭收支調查_所得收入者人數與按年齡組別及性別之分配()

    plots[f"{key}_年齡"] = plot_lines_bars(
        df,
        bars_left_axis=[col for col in df.columns if "歲" in col],
        lines_right_axis=["所得收入者人數合計"],
        title=f"{key}_年齡 {df.index[0]}~{df.index[-1]}年",
        additional_layout={
            "barmode": "stack",
            "yaxis": {
                "tickformat": ".2%",
            },
        },
    )
    plots[f"{key}_性別"] = plot_lines_bars(
        df,
        bars_left_axis=[col for col in df.columns if "性" in col],
        lines_right_axis=["所得收入者人數合計"],
        title=f"{key}_性別 {df.index[0]}~{df.index[-1]}年",
        additional_layout={
            "barmode": "stack",
            "yaxis": {
                "tickformat": ".2%",
            },
        },
    )


def plot_家庭收支調查_戶內人數別平均每戶所得總額(plots):
    年_plot(
        plots,
        key="家庭收支調查-戶內人數別平均每戶所得總額",
        df_get=df_家庭收支調查_戶內人數別平均每戶所得總額,
        title_suffix="-元",
    )


def plot_家庭收支調查_戶內人數別平均每戶所得收入總計(plots):
    年_plot(
        plots,
        key="家庭收支調查-戶內人數別平均每戶所得收入總計",
        df_get=df_家庭收支調查_戶內人數別平均每戶所得收入總計,
        title_suffix="-元 所得收入=所得總額-自用住宅及其他營建物設算折舊",
    )


def plot_家庭收支調查_戶內人數別平均每戶非消費支出(plots):
    年_plot(
        plots,
        key="家庭收支調查-戶內人數別平均每戶非消費支出",
        df_get=df_家庭收支調查_戶內人數別平均每戶非消費支出,
        title_suffix="-元",
    )


def plot_家庭收支調查_戶內人數別平均每戶可支配所得(plots):
    年_plot(
        plots,
        key="家庭收支調查-戶內人數別平均每戶可支配所得",
        df_get=df_家庭收支調查_戶內人數別平均每戶可支配所得,
        title_suffix="-元 可支配所得=所得收入-非消費支出",
    )


def plot_家庭收支調查_戶內人數別平均每戶消費支出(plots):
    年_plot(
        plots,
        key="家庭收支調查-戶內人數別平均每戶消費支出",
        df_get=df_家庭收支調查_戶內人數別平均每戶消費支出,
        title_suffix="-元",
    )


def plot_家庭收支調查_戶內人數別平均每戶經常性支出(plots):
    年_plot(
        plots,
        key="家庭收支調查-戶內人數別平均每戶經常性支出",
        df_get=df_家庭收支調查_戶內人數別平均每戶經常性支出,
        title_suffix="-元 經常性支出=消費支出+非消費支出",
    )


def plot_家庭收支調查_戶內人數別平均每戶儲蓄(plots):
    年_plot(
        plots,
        key="家庭收支調查-戶內人數別平均每戶儲蓄",
        df_get=df_家庭收支調查_戶內人數別平均每戶儲蓄,
        title_suffix="-元 儲蓄=所得收入-經常性支出=可支配所得-消費支出",
    )


def plot_家庭收支調查_戶內人數別平均每戶儲蓄率(plots):
    def df_家庭收支調查_戶內人數別平均每戶儲蓄率():
        儲蓄 = df_家庭收支調查_戶內人數別平均每戶儲蓄()
        儲蓄 = 儲蓄.rename(columns={"平均每戶儲蓄": "平均每戶"})
        所得總額 = df_家庭收支調查_戶內人數別平均每戶所得總額()
        所得總額 = 所得總額.rename(columns={"平均每戶所得總額": "平均每戶"})

        return 儲蓄 / 所得總額[儲蓄.columns]

    年_plot(
        plots,
        key="家庭收支調查-戶內人數別平均每戶儲蓄率",
        df_get=df_家庭收支調查_戶內人數別平均每戶儲蓄率,
        title_suffix=" 儲蓄率=儲蓄/所得總額",
        additional_layout={
            "yaxis": {
                "tickformat": ".2%",
            }
        },
    )


def plot_家庭收支調查_戶內人數別平均每戶受僱人員報酬(plots):
    年_plot(
        plots,
        key="家庭收支調查-戶內人數別平均每戶受僱人員報酬",
        df_get=df_家庭收支調查_戶內人數別平均每戶受僱人員報酬,
        title_suffix="-元",
    )


def plot_家庭收支調查_家庭戶數按戶內人口規模別之分配(plots):
    key = "家庭收支調查-家庭戶數按戶內人口規模別之分配"
    key = sanitize_filename(key)
    df = df_家庭收支調查_家庭戶數按戶內人口規模別之分配()
    df = df.drop(columns="臺灣地區合計")

    plots[key] = plot_bar_group(
        df,
        f"{key} {df.index[0]}~{df.index[-1]}年",
        additional_layout={
            "barmode": "stack",
            "yaxis": {
                "tickformat": ".2%",
            },
        },
    )


def plot_家庭收支調查_農家平均每戶所得總額按來自農業與非農業分(plots):
    年_plot(
        plots,
        key="家庭收支調查-農家平均每戶所得總額按來自農業與非農業分",
        df_get=df_家庭收支調查_農家平均每戶所得總額按來自農業與非農業分,
        title_suffix="-元",
    )


def plot_家庭收支調查_農家與非農家平均每戶可支配所得_所得總額及儲蓄(plots):
    年_plot(
        plots,
        key="家庭收支調查-農家與非農家平均每戶可支配所得、所得總額及儲蓄",
        df_get=df_家庭收支調查_農家與非農家平均每戶可支配所得_所得總額及儲蓄,
        title_suffix="-元",
    )


def plot_家庭收支調查_農家與非農家平均每戶消費支出_非消費支出及經常性支出(plots):
    年_plot(
        plots,
        key="家庭收支調查-農家與非農家平均每戶消費支出、非消費支出及經常性支出",
        df_get=df_家庭收支調查_農家與非農家平均每戶消費支出_非消費支出及經常性支出,
        title_suffix="-元",
    )


def plot_家庭收支調查_農家與非農家平均每戶所得收入總計及受僱人員報酬(plots):
    年_plot(
        plots,
        key="家庭收支調查-農家與非農家平均每戶所得收入總計及受僱人員報酬",
        df_get=df_家庭收支調查_農家與非農家平均每戶所得收入總計及受僱人員報酬,
        title_suffix="-元",
    )


def plot_家庭收支調查_戶數五等分位之平均每戶所得總額(plots):
    年_plot(
        plots,
        key="家庭收支調查-戶數五等分位之平均每戶所得總額",
        df_get=df_家庭收支調查_戶數五等分位之平均每戶所得總額,
        title_suffix="-元",
    )


def plot_家庭收支調查_戶數五等分位之平均每戶所得收入總計(plots):
    年_plot(
        plots,
        key="家庭收支調查-戶數五等分位之平均每戶所得收入總計",
        df_get=df_家庭收支調查_戶數五等分位之平均每戶所得收入總計,
        title_suffix="-元 所得收入=所得總額-自用住宅及其他營建物設算折舊",
    )


def plot_家庭收支調查_戶數五等分位之平均每戶非消費支出(plots):
    年_plot(
        plots,
        key="家庭收支調查-戶數五等分位之平均每戶非消費支出",
        df_get=df_家庭收支調查_戶數五等分位之平均每戶非消費支出,
        title_suffix="-元",
    )


def plot_家庭收支調查_戶數五等分位之平均每戶可支配所得(plots):
    年_plot(
        plots,
        key="家庭收支調查-戶數五等分位之平均每戶可支配所得",
        df_get=df_家庭收支調查_戶數五等分位之平均每戶可支配所得,
        title_suffix="-元 可支配所得=所得收入-非消費支出",
    )


def plot_家庭收支調查_戶數五等分位之平均每戶消費支出(plots):
    年_plot(
        plots,
        key="家庭收支調查-戶數五等分位之平均每戶消費支出",
        df_get=df_家庭收支調查_戶數五等分位之平均每戶消費支出,
        title_suffix="-元",
    )


def plot_家庭收支調查_戶數五等分位之平均每戶經常性支出(plots):
    年_plot(
        plots,
        key="家庭收支調查-戶數五等分位之平均每戶經常性支出",
        df_get=df_家庭收支調查_戶數五等分位之平均每戶經常性支出,
        title_suffix="-元 經常性支出=消費支出+非消費支出",
    )


def plot_家庭收支調查_戶數五等分位之平均每戶儲蓄(plots):
    年_plot(
        plots,
        key="家庭收支調查-戶數五等分位之平均每戶儲蓄",
        df_get=df_家庭收支調查_戶數五等分位之平均每戶儲蓄,
        title_suffix="-元 儲蓄=所得收入-經常性支出=可支配所得-消費支出",
    )


def plot_家庭收支調查_戶數五等分位之平均每戶儲蓄率(plots):
    def df_家庭收支調查_戶數五等分位之平均每戶儲蓄率():
        儲蓄 = df_家庭收支調查_戶數五等分位之平均每戶儲蓄()
        儲蓄 = 儲蓄.rename(columns={"平均每戶儲蓄": "平均每戶"})
        所得總額 = df_家庭收支調查_戶數五等分位之平均每戶所得總額()
        所得總額 = 所得總額.rename(columns={"平均每戶所得總額": "平均每戶"})

        return 儲蓄 / 所得總額[儲蓄.columns]

    年_plot(
        plots,
        key="家庭收支調查-戶數五等分位之平均每戶儲蓄率",
        df_get=df_家庭收支調查_戶數五等分位之平均每戶儲蓄率,
        title_suffix=" 儲蓄率=儲蓄/所得總額",
        additional_layout={
            "yaxis": {
                "tickformat": ".2%",
            }
        },
    )


def plot_家庭收支調查_戶數五等分位之平均每戶受僱人員報酬(plots):
    年_plot(
        plots,
        key="家庭收支調查-戶數五等分位之平均每戶受僱人員報酬",
        df_get=df_家庭收支調查_戶數五等分位之平均每戶受僱人員報酬,
        title_suffix="-元",
    )


def plot_家庭收支調查_戶數五等分位組之平均每戶人口數(plots):
    年_plot(
        plots,
        key="家庭收支調查-戶數五等分位組之平均每戶人口數",
        df_get=df_家庭收支調查_戶數五等分位組之平均每戶人口數,
        title_suffix="-人",
    )


def plot_家庭收支調查_戶數五等分位組之平均每戶就業人數(plots):
    年_plot(
        plots,
        key="家庭收支調查-戶數五等分位組之平均每戶就業人數",
        df_get=df_家庭收支調查_戶數五等分位組之平均每戶就業人數,
        title_suffix="-人",
    )


def plot_家庭收支調查_戶數十等分位組分界點之可支配所得(plots):
    年_plot(
        plots,
        key="家庭收支調查-戶數十等分位組分界點之可支配所得",
        df_get=df_家庭收支調查_戶數十等分位組分界點之可支配所得,
        title_suffix="-元",
    )


def plot_家庭收支調查_所得收入者五等分位平均每人可支配所得(plots):
    年_plot(
        plots,
        key="家庭收支調查-所得收入者五等分位平均每人可支配所得",
        df_get=df_家庭收支調查_所得收入者五等分位平均每人可支配所得,
        title_suffix="平均每人可支配所得=平均每位所得收入者可支配所得",
    )


def plot_家庭收支調查_所得收入者十等分位組分界點之可支配所得(plots):
    年_plot(
        plots,
        key="家庭收支調查-所得收入者十等分位組分界點之可支配所得",
        df_get=df_家庭收支調查_所得收入者十等分位組分界點之可支配所得,
        title_suffix="-元 平均每人可支配所得=平均每位所得收入者可支配所得",
    )


def plot_家庭收支調查_家庭組織型態別平均每戶所得總額(plots):
    年_plot(
        plots,
        key="家庭收支調查-家庭組織型態別平均每戶所得總額",
        df_get=df_家庭收支調查_家庭組織型態別平均每戶所得總額,
        title_suffix="-元",
    )


def plot_家庭收支調查_家庭組織型態別平均每戶所得收入總計(plots):
    年_plot(
        plots,
        key="家庭收支調查-家庭組織型態別平均每戶所得收入總計",
        df_get=df_家庭收支調查_家庭組織型態別平均每戶所得收入總計,
        title_suffix="-元 所得收入=所得總額-自用住宅及其他營建物設算折舊",
    )


def plot_家庭收支調查_家庭組織型態別平均每戶非消費支出(plots):
    年_plot(
        plots,
        key="家庭收支調查-家庭組織型態別平均每戶非消費支出",
        df_get=df_家庭收支調查_家庭組織型態別平均每戶非消費支出,
        title_suffix="-元",
    )


def plot_家庭收支調查_家庭組織型態別平均每戶可支配所得(plots):
    年_plot(
        plots,
        key="家庭收支調查-家庭組織型態別平均每戶可支配所得",
        df_get=df_家庭收支調查_家庭組織型態別平均每戶可支配所得,
        title_suffix="-元 可支配所得=所得收入-非消費支出",
    )


def plot_家庭收支調查_家庭組織型態別平均每戶消費支出(plots):
    年_plot(
        plots,
        key="家庭收支調查-家庭組織型態別平均每戶消費支出",
        df_get=df_家庭收支調查_家庭組織型態別平均每戶消費支出,
        title_suffix="-元",
    )


def plot_家庭收支調查_家庭組織型態別平均每戶經常性支出(plots):
    年_plot(
        plots,
        key="家庭收支調查-家庭組織型態別平均每戶經常性支出",
        df_get=df_家庭收支調查_家庭組織型態別平均每戶經常性支出,
        title_suffix="-元 經常性支出=消費支出+非消費支出",
    )


def plot_家庭收支調查_家庭組織型態別平均每戶儲蓄(plots):
    年_plot(
        plots,
        key="家庭收支調查-家庭組織型態別平均每戶儲蓄",
        df_get=df_家庭收支調查_家庭組織型態別平均每戶儲蓄,
        title_suffix="-元 儲蓄=所得收入-經常性支出=可支配所得-消費支出",
    )


def plot_家庭收支調查_家庭組織型態別平均每戶儲蓄率(plots):
    def df_家庭收支調查_家庭組織型態別平均每戶儲蓄率():
        儲蓄 = df_家庭收支調查_家庭組織型態別平均每戶儲蓄()
        所得總額 = df_家庭收支調查_家庭組織型態別平均每戶所得總額()
        return 儲蓄 / 所得總額[儲蓄.columns]

    年_plot(
        plots,
        key="家庭收支調查-家庭組織型態別平均每戶儲蓄率",
        df_get=df_家庭收支調查_家庭組織型態別平均每戶儲蓄率,
        title_suffix=" 儲蓄率=儲蓄/所得總額",
        additional_layout={
            "yaxis": {
                "tickformat": ".2%",
            }
        },
    )


def plot_家庭收支調查_家庭組織型態別平均每戶受僱人員報酬(plots):
    年_plot(
        plots,
        key="家庭收支調查-家庭組織型態別平均每戶受僱人員報酬",
        df_get=df_家庭收支調查_家庭組織型態別平均每戶受僱人員報酬,
        title_suffix="-元",
    )


def plot_家庭收支調查_性別統計指標_家戶狀況按家庭組織型態分(plots):
    年_plot(
        plots,
        key="家庭收支調查-性別統計指標-家戶狀況按家庭組織型態分",
        df_get=df_家庭收支調查_性別統計指標_家戶狀況按家庭組織型態分,
        title_suffix="戶數",
    )


def plot_家庭收支調查_各縣市別平均每戶所得總額(plots):
    年_plot(
        plots,
        key="家庭收支調查-各縣市別平均每戶所得總額",
        df_get=df_家庭收支調查_各縣市別平均每戶所得總額,
        title_suffix="-元",
    )


def plot_家庭收支調查_各縣市別平均每戶所得收入總計(plots):
    年_plot(
        plots,
        key="家庭收支調查-各縣市別平均每戶所得收入總計",
        df_get=df_家庭收支調查_各縣市別平均每戶所得收入總計,
        title_suffix="-元 所得收入=所得總額-自用住宅及其他營建物設算折舊",
    )


def plot_家庭收支調查_各縣市別平均每戶非消費支出(plots):
    年_plot(
        plots,
        key="家庭收支調查-各縣市別平均每戶非消費支出",
        df_get=df_家庭收支調查_各縣市別平均每戶非消費支出,
        title_suffix="-元",
    )


def plot_家庭收支調查_各縣市別平均每戶可支配所得(plots):
    年_plot(
        plots,
        key="家庭收支調查-各縣市別平均每戶可支配所得",
        df_get=df_家庭收支調查_各縣市別平均每戶可支配所得,
        title_suffix="-元 可支配所得=所得收入-非消費支出",
    )


def plot_家庭收支調查_各縣市別平均每戶消費支出(plots):
    年_plot(
        plots,
        key="家庭收支調查-各縣市別平均每戶消費支出",
        df_get=df_家庭收支調查_各縣市別平均每戶消費支出,
        title_suffix="-元",
    )


def plot_家庭收支調查_各縣市別平均每戶經常性支出(plots):
    年_plot(
        plots,
        key="家庭收支調查-各縣市別平均每戶經常性支出",
        df_get=df_家庭收支調查_各縣市別平均每戶經常性支出,
        title_suffix="-元 經常性支出=消費支出+非消費支出",
    )


def plot_家庭收支調查_各縣市別平均每戶儲蓄(plots):
    年_plot(
        plots,
        key="家庭收支調查-各縣市別平均每戶儲蓄",
        df_get=df_家庭收支調查_各縣市別平均每戶儲蓄,
        title_suffix="-元 儲蓄=所得收入-經常性支出=可支配所得-消費支出",
    )


def plot_家庭收支調查_各縣市別平均每戶儲蓄率(plots):
    def df_家庭收支調查_各縣市別平均每戶儲蓄率():
        儲蓄 = df_家庭收支調查_各縣市別平均每戶儲蓄()
        所得總額 = df_家庭收支調查_各縣市別平均每戶所得總額()
        return 儲蓄 / 所得總額[儲蓄.columns]

    年_plot(
        plots,
        key="家庭收支調查-各縣市別平均每戶儲蓄率",
        df_get=df_家庭收支調查_各縣市別平均每戶儲蓄率,
        title_suffix=" 儲蓄率=儲蓄/所得總額",
        additional_layout={
            "yaxis": {
                "tickformat": ".2%",
            }
        },
    )


def plot_家庭收支調查_各縣市別平均每戶受僱人員報酬(plots):
    年_plot(
        plots,
        key="家庭收支調查-各縣市別平均每戶受僱人員報酬",
        df_get=df_家庭收支調查_各縣市別平均每戶受僱人員報酬,
        title_suffix="-元",
    )


def plot_家庭收支調查_所得收入者各縣市別平均每人所得收入總計(plots):
    年_plot(
        plots,
        key="家庭收支調查-所得收入者各縣市別平均每人所得收入總計",
        df_get=df_家庭收支調查_所得收入者各縣市別平均每人所得收入總計,
        title_suffix="-元",
    )


def plot_家庭收支調查_所得收入者各縣市別平均每人非消費支出(plots):
    年_plot(
        plots,
        key="家庭收支調查-所得收入者各縣市別平均每人非消費支出",
        df_get=df_家庭收支調查_所得收入者各縣市別平均每人非消費支出,
        title_suffix="-元",
    )


def plot_家庭收支調查_所得收入者各縣市別平均每人可支配所得(plots):
    年_plot(
        plots,
        key="家庭收支調查-所得收入者各縣市別平均每人可支配所得",
        df_get=df_家庭收支調查_所得收入者各縣市別平均每人可支配所得,
        title_suffix="-元 可支配所得=所得收入-非消費支出, 平均每人可支配所得=平均每位所得收入者可支配所得",
    )


def plot_家庭收支調查_所得收入者各縣市別平均每人受僱人員報酬(plots):
    年_plot(
        plots,
        key="家庭收支調查-所得收入者各縣市別平均每人受僱人員報酬",
        df_get=df_家庭收支調查_所得收入者各縣市別平均每人受僱人員報酬,
        title_suffix="-元",
    )


def plot_家庭收支調查_性別統計指標_可支配所得按戶數五等分位經濟戶長性別比率(plots):
    年_plot(
        plots,
        key="家庭收支調查-性別統計指標-可支配所得按戶數五等分位經濟戶長性別比率",
        df_get=df_家庭收支調查_性別統計指標_可支配所得按戶數五等分位經濟戶長性別比率,
        title_suffix="",
        additional_layout={"yaxis": {"tickformat": ".2%"}},
    )


def plot_家庭收支調查_性別統計指標_戶數與結構按經濟戶長性別分(plots):
    key = "家庭收支調查-性別統計指標-戶數與結構按經濟戶長性別分"
    key = sanitize_filename(key)
    df = df_家庭收支調查_性別統計指標_戶數與結構按經濟戶長性別分()

    plots[key] = plot_lines_bars(
        df,
        bars_left_axis=["男性比率", "女性比率"],
        lines_right_axis=["總計戶數", "男性戶數", "女性戶數"],
        title=f"{key} {df.index[0]}~{df.index[-1]}年",
        additional_layout={"barmode": "stack", "yaxis": {"tickformat": ".2%"}},
    )


def plot_家庭收支調查_平均每戶可支配所得及消費支出依可支配所得按戶數五等分位分及經濟戶長年齡組別分(
    plots,
):
    key = (
        "家庭收支調查-平均每戶可支配所得及消費支出依可支配所得按戶數五等分位分及經濟戶長年齡組別分"
    )

    key = sanitize_filename(key)
    df, last_year = (
        df_家庭收支調查_平均每戶可支配所得及消費支出依可支配所得按戶數五等分位分及經濟戶長年齡組別分()
    )

    df_可支配 = df[[col for col in df.columns if "可支配" in col]]
    df_可支配.columns = df_可支配.columns.str.replace("平均每戶可支配所得-", "", regex=True)
    plots[f"{key}_平均每戶可支配所得"] = plot_bar_group(
        df_可支配,
        title=f"{key}_平均每戶可支配所得-元 {last_year}年",
    )

    df_消費 = df[[col for col in df.columns if "消費" in col]]
    df_消費.columns = df_消費.columns.str.replace("平均每戶消費支出-", "", regex=True)
    plots[f"{key}_平均每戶消費支出"] = plot_bar_group(
        df_消費,
        title=f"{key}_平均每戶消費支出-元 {last_year}年",
    )


def plot_家庭收支調查_平均每戶可支配所得及消費支出依可支配所得按戶數五等分位分及經濟戶長教育程度別分(
    plots,
):
    key = "家庭收支調查-平均每戶可支配所得及消費支出依可支配所得按戶數五等分位分及經濟戶長教育程度別分"
    key = sanitize_filename(key)
    df, last_year = (
        df_家庭收支調查_平均每戶可支配所得及消費支出依可支配所得按戶數五等分位分及經濟戶長教育程度別分()
    )

    df_可支配 = df[[col for col in df.columns if "可支配" in col]]
    df_可支配.columns = df_可支配.columns.str.replace("平均每戶可支配所得-", "", regex=True)
    plots[f"{key}_平均每戶可支配所得"] = plot_bar_group(
        df_可支配,
        title=f"{key}_平均每戶可支配所得-元 {last_year}年",
    )

    df_消費 = df[[col for col in df.columns if "消費" in col]]
    df_消費.columns = df_消費.columns.str.replace("平均每戶消費支出-", "", regex=True)
    plots[f"{key}_平均每戶消費支出"] = plot_bar_group(
        df_消費,
        title=f"{key}_平均每戶消費支出-元 {last_year}年",
    )


def plot_家庭收支調查_平均每戶可支配所得及消費支出依可支配所得按戶數五等分位分及經濟戶長性別分(
    plots,
):
    key = "家庭收支調查-平均每戶可支配所得及消費支出依可支配所得按戶數五等分位分及經濟戶長性別分"
    key = sanitize_filename(key)
    df, last_year = (
        df_家庭收支調查_平均每戶可支配所得及消費支出依可支配所得按戶數五等分位分及經濟戶長性別分()
    )

    df_可支配 = df[[col for col in df.columns if "可支配" in col]]
    df_可支配.columns = df_可支配.columns.str.replace("平均每戶可支配所得-", "", regex=True)
    plots[f"{key}_平均每戶可支配所得"] = plot_bar_group(
        df_可支配,
        title=f"{key}_平均每戶可支配所得-元 {last_year}年",
    )

    df_消費 = df[[col for col in df.columns if "消費" in col]]
    df_消費.columns = df_消費.columns.str.replace("平均每戶消費支出-", "", regex=True)
    plots[f"{key}_平均每戶消費支出"] = plot_bar_group(
        df_消費,
        title=f"{key}_平均每戶消費支出-元 {last_year}年",
    )


def plot_家庭收支調查_家庭戶數按所得總額組別及經濟戶長性別分(plots):
    key = "家庭收支調查-家庭戶數按所得總額組別及經濟戶長性別分"
    key = sanitize_filename(key)
    df, last_year = df_家庭收支調查_家庭戶數按所得總額組別及經濟戶長性別分()

    idx = [i for i in df.index if i != "總計"]
    plots[key] = plot_bar_group(
        df.loc[idx, :],
        title=f"{key}-戶數 {last_year}年",
    )


def plot_家庭收支調查_所得收入者人數按性別及可支配所得組別分(plots):
    key = "家庭收支調查-所得收入者人數按性別及可支配所得組別分"
    key = sanitize_filename(key)
    df, last_year = df_家庭收支調查_所得收入者人數按性別及可支配所得組別分()

    idx = [i for i in df.index if i != "總計"]
    plots[key] = plot_bar_group(
        df.loc[idx, :],
        title=f"{key}-人數 {last_year}年",
    )


def plot_家庭收支調查_性別統計指標_經濟戶長人數按性別_教育程度及年齡分(plots):
    年_plot(
        plots,
        key="家庭收支調查-性別統計指標-經濟戶長人數按性別、教育程度及年齡分",
        df_get=df_家庭收支調查_性別統計指標_經濟戶長人數按性別_教育程度及年齡分,
        title_suffix="-人",
    )


def plot_家庭收支調查_性別統計指標_單親戶長人數按性別_教育程度及年齡分(plots):
    年_plot(
        plots,
        key="家庭收支調查-性別統計指標-單親戶長人數按性別、教育程度及年齡分",
        df_get=df_家庭收支調查_性別統計指標_單親戶長人數按性別_教育程度及年齡分,
        title_suffix="-人",
    )


def plot_家庭收支調查_經濟戶長職業別平均每戶所得總額(plots):
    年_plot(
        plots,
        key="家庭收支調查-經濟戶長職業別平均每戶所得總額",
        df_get=df_家庭收支調查_經濟戶長職業別平均每戶所得總額,
        title_suffix="-元",
    )


def plot_家庭收支調查_經濟戶長職業別平均每戶所得收入總計(plots):
    年_plot(
        plots,
        key="家庭收支調查-經濟戶長職業別平均每戶所得收入總計",
        df_get=df_家庭收支調查_經濟戶長職業別平均每戶所得收入總計,
        title_suffix="-元 所得收入=所得總額-自用住宅及其他營建物設算折舊",
    )


def plot_家庭收支調查_經濟戶長職業別平均每戶非消費支出(plots):
    年_plot(
        plots,
        key="家庭收支調查-經濟戶長職業別平均每戶非消費支出",
        df_get=df_家庭收支調查_經濟戶長職業別平均每戶非消費支出,
        title_suffix="-元",
    )


def plot_家庭收支調查_經濟戶長職業別平均每戶可支配所得(plots):
    年_plot(
        plots,
        key="家庭收支調查-經濟戶長職業別平均每戶可支配所得",
        df_get=df_家庭收支調查_經濟戶長職業別平均每戶可支配所得,
        title_suffix="-元 可支配所得=所得收入-非消費支出",
    )


def plot_家庭收支調查_經濟戶長職業別平均每戶消費支出(plots):
    年_plot(
        plots,
        key="家庭收支調查-經濟戶長職業別平均每戶消費支出",
        df_get=df_家庭收支調查_經濟戶長職業別平均每戶消費支出,
        title_suffix="-元",
    )


def plot_家庭收支調查_經濟戶長職業別平均每戶經常性支出(plots):
    年_plot(
        plots,
        key="家庭收支調查-經濟戶長職業別平均每戶經常性支出",
        df_get=df_家庭收支調查_經濟戶長職業別平均每戶經常性支出,
        title_suffix="-元 經常性支出=消費支出+非消費支出",
    )


def plot_家庭收支調查_經濟戶長職業別平均每戶儲蓄(plots):
    年_plot(
        plots,
        key="家庭收支調查-經濟戶長職業別平均每戶儲蓄",
        df_get=df_家庭收支調查_經濟戶長職業別平均每戶儲蓄,
        title_suffix="-元 儲蓄=所得收入-經常性支出=可支配所得-消費支出",
    )


def plot_家庭收支調查_經濟戶長職業別平均每戶儲蓄率(plots):
    def df_家庭收支調查_經濟戶長職業別平均每戶儲蓄率():
        儲蓄 = df_家庭收支調查_經濟戶長職業別平均每戶儲蓄()
        所得總額 = df_家庭收支調查_經濟戶長職業別平均每戶所得總額()
        return 儲蓄 / 所得總額[儲蓄.columns]

    年_plot(
        plots,
        key="家庭收支調查-經濟戶長職業別平均每戶儲蓄率",
        df_get=df_家庭收支調查_經濟戶長職業別平均每戶儲蓄率,
        title_suffix=" 儲蓄率=儲蓄/所得總額",
        additional_layout={
            "yaxis": {
                "tickformat": ".2%",
            }
        },
    )


def plot_家庭收支調查_經濟戶長職業別平均每戶受僱人員報酬(plots):
    年_plot(
        plots,
        key="家庭收支調查-經濟戶長職業別平均每戶受僱人員報酬",
        df_get=df_家庭收支調查_經濟戶長職業別平均每戶受僱人員報酬,
        title_suffix="-元",
    )


def plot_家庭收支調查_所得收入者職業別平均每人所得收入總計(plots):
    年_plot(
        plots,
        key="家庭收支調查-所得收入者職業別平均每人所得收入總計",
        df_get=df_家庭收支調查_所得收入者職業別平均每人所得收入總計,
        title_suffix="-元",
    )


def plot_家庭收支調查_所得收入者職業別平均每人非消費支出(plots):
    年_plot(
        plots,
        key="家庭收支調查-所得收入者職業別平均每人非消費支出",
        df_get=df_家庭收支調查_所得收入者職業別平均每人非消費支出,
        title_suffix="-元",
    )


def plot_家庭收支調查_所得收入者職業別平均每人可支配所得(plots):
    年_plot(
        plots,
        key="家庭收支調查-所得收入者職業別平均每人可支配所得",
        df_get=df_家庭收支調查_所得收入者職業別平均每人可支配所得,
        title_suffix="-元 可支配所得=所得收入-非消費支出",
    )


def plot_家庭收支調查_所得收入者職業別平均每人受僱人員報酬(plots):
    年_plot(
        plots,
        key="家庭收支調查-所得收入者職業別平均每人受僱人員報酬",
        df_get=df_家庭收支調查_所得收入者職業別平均每人受僱人員報酬,
        title_suffix="-元",
    )


def plot_家庭部門平均每戶資產負債(plots):
    key = "家庭部門平均每戶資產負債"
    key = sanitize_filename(key)
    df = df_家庭部門平均每戶資產負債()

    df = df.T
    title = f"{key} {df.index[0]}~{df.index[-1]}年 （土地按市價重評價）（新臺幣萬元）"

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


def plot_公開資訊觀測站_財務報告附註揭露之員工福利_薪資_資訊(plots):
    key = "公開資訊觀測站_財務報告附註揭露之員工福利(薪資)資訊"
    key = sanitize_filename(key)
    df, last_year = df_公開資訊觀測站_財務報告附註揭露之員工福利薪資資訊()

    df_薪資 = df.sort_values("公司代號").set_index("公司")
    df_薪資 = df_薪資[
        [
            "平均員工福利費用(人)",
            f"平均員工薪資費用{last_year}年度(人)",
            f"平均員工薪資費用{last_year-1}年度(人)",
            "平均員工薪資費用調整變動情形",
        ]
    ]

    plots[f"{key}"] = plot_lines_bars(
        df_薪資,
        title=f"{key} {last_year}年度",
        lines_left_axis=[],
        lines_right_axis=[
            "平均員工薪資費用調整變動情形",
        ],
        bars_left_axis=[
            "平均員工福利費用(人)",
            f"平均員工薪資費用{last_year}年度(人)",
            f"平均員工薪資費用{last_year-1}年度(人)",
        ],
        sort=False,
        additional_layout={
            "yaxis": {"title": {"text": "平均"}},
            "yaxis2": {
                "title": {"text": f"{last_year}年度/{last_year-1}年度"},
                "tickformat": ".2%",
            },
        },
    )
    plots[f"{key}_排序"] = plot_lines_bars(
        df_薪資.sort_values(["平均員工福利費用(人)"], ascending=False),
        title=f"{key}_排序 {last_year}年度",
        lines_left_axis=[],
        lines_right_axis=[
            "平均員工薪資費用調整變動情形",
        ],
        bars_left_axis=[
            "平均員工福利費用(人)",
            f"平均員工薪資費用{last_year}年度(人)",
            f"平均員工薪資費用{last_year-1}年度(人)",
        ],
        sort=False,
        additional_layout={
            "yaxis": {"title": {"text": "平均"}},
            "yaxis2": {
                "title": {"text": f"{last_year}年度/{last_year-1}年度"},
                "tickformat": ".2%",
            },
        },
    )

    df_薪資_產業類別 = df.pivot_table(
        values=[
            "平均員工福利費用(人)",
            f"平均員工薪資費用{last_year}年度(人)",
            f"平均員工薪資費用{last_year-1}年度(人)",
            "平均員工薪資費用調整變動情形",
        ],
        index="產業類別",
        aggfunc="mean",
        sort=False,
    )

    plots[f"{key}_產業類別_排序"] = plot_lines_bars(
        df_薪資_產業類別.sort_values(["平均員工福利費用(人)"], ascending=False),
        title=f"{key}_產業類別_排序 {last_year}年度",
        lines_left_axis=[],
        lines_right_axis=[
            "平均員工薪資費用調整變動情形",
        ],
        bars_left_axis=[
            "平均員工福利費用(人)",
            f"平均員工薪資費用{last_year}年度(人)",
            f"平均員工薪資費用{last_year-1}年度(人)",
        ],
        sort=False,
        additional_layout={
            "yaxis": {"title": {"text": "平均"}},
            "yaxis2": {
                "title": {"text": f"{last_year}年度/{last_year-1}年度"},
                "tickformat": ".2%",
            },
        },
    )

    data_list = []
    df_公司 = df.set_index("公司").sort_values(
        ["平均員工福利費用(人)"],
        ascending=False,
    )
    for company in df_公司.index:
        names = [
            "平均員工福利費用(人)",
            f"平均員工薪資費用{last_year}年度(人)",
            f"平均員工薪資費用{last_year-1}年度(人)",
        ]
        data = {
            "type": "scatter",
            "name": f"{company}_金額",
            "x": names,
            "y": df_公司.loc[company, names].tolist(),
            "mode": "markers",
            "legendgroup": company,
        }
        data_list.append(data)

        names = [
            "平均員工薪資費用調整變動情形",
        ]
        data = {
            "type": "scatter",
            "name": f"{company}_薪資變動",
            "x": names,
            "y": df_公司.loc[company, names].tolist(),
            "yaxis": "y2",
            "mode": "markers",
            "legendgroup": company,
        }
        data_list.append(data)

    buttons_kinds = [
        {
            "args": [
                {
                    "visible": [True] * len(df_公司.index) * 4,
                }
            ],  # 顯示所有線條
            "label": "全部公司",
            "method": "restyle",
        }
    ]
    kinds = df["產業類別"].dropna().unique().tolist()
    for kind in kinds:
        arr = [
            [True] * 2 if df_公司.loc[index, "產業類別"] == kind else [False] * 2
            for index in df_公司.index
        ]
        arr = sum(arr, [])
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
            "x": 0.5,
            "y": 1.09,
            "xanchor": "center",
            "yanchor": "top",
            "pad": {"r": 10, "t": 10},
            "buttons": buttons_kinds,
            "type": "dropdown",
            "direction": "down",
            "active": 0,
            "font": {"color": "#AAAAAA"},
            "name": "產業選擇",
        },
    ]

    layout = {
        "title": {"text": f"{key}_公司_產業類別_排序 {last_year}年度"},
        "hovermode": "x",
        "xaxis": {"type": "category"},
        "yaxis2": {
            "overlaying": "y",
            "side": "right",
            "tickformat": ".2%",
            "title": {"text": f"{last_year}年度/{last_year-1}年度"},
        },
        "yaxis": {"title": {"text": "平均"}},
        "barmode": "group",
        "updatemenus": updatemenus,
    }

    graph = {"data": data_list, "layout": layout}
    graph = merge_dict(copy.deepcopy(default_template), graph)
    plots[f"{key}_公司_產業_排序"] = plotly_json_dump(graph)


def plot_公開資訊觀測站_非擔任主管職務之全時員工薪資資訊(plots):
    key = "公開資訊觀測站_非擔任主管職務之全時員工薪資資訊"
    key = sanitize_filename(key)
    df, last_year = df_公開資訊觀測站_非擔任主管職務之全時員工薪資資訊()

    df_薪資 = df.sort_values("公司代號").set_index("公司")
    df_薪資 = df_薪資[
        [
            f"員工薪資-中位數_{last_year}年(人)",
            f"員工薪資-中位數_{last_year-1}年(人)",
            "員工薪資-中位數調整變動情形",
            f"員工薪資-平均數_{last_year}年(人)",
            f"員工薪資-平均數_{last_year-1}年(人)",
            "員工薪資-平均數調整變動情形",
        ]
    ]

    plots[f"{key}"] = plot_lines_bars(
        df_薪資,
        title=f"{key} {last_year}年度",
        lines_left_axis=[],
        lines_right_axis=[
            "員工薪資-中位數調整變動情形",
            "員工薪資-平均數調整變動情形",
        ],
        bars_left_axis=[
            f"員工薪資-中位數_{last_year}年(人)",
            f"員工薪資-中位數_{last_year-1}年(人)",
            f"員工薪資-平均數_{last_year}年(人)",
            f"員工薪資-平均數_{last_year-1}年(人)",
        ],
        sort=False,
        additional_layout={
            "yaxis2": {
                "title": {"text": f"{last_year}年度/{last_year-1}年度"},
                "tickformat": ".2%",
            },
        },
    )
    plots[f"{key}_排序"] = plot_lines_bars(
        df_薪資.sort_values([f"員工薪資-中位數_{last_year}年(人)"], ascending=False),
        title=f"{key}_排序 {last_year}年度",
        lines_left_axis=[],
        lines_right_axis=[
            "員工薪資-中位數調整變動情形",
            "員工薪資-平均數調整變動情形",
        ],
        bars_left_axis=[
            f"員工薪資-中位數_{last_year}年(人)",
            f"員工薪資-中位數_{last_year-1}年(人)",
            f"員工薪資-平均數_{last_year}年(人)",
            f"員工薪資-平均數_{last_year-1}年(人)",
        ],
        sort=False,
        additional_layout={
            "yaxis2": {
                "title": {"text": f"{last_year}年度/{last_year-1}年度"},
                "tickformat": ".2%",
            },
        },
    )

    df_薪資_產業類別 = df.pivot_table(
        values=[
            f"員工薪資-中位數_{last_year}年(人)",
            f"員工薪資-中位數_{last_year-1}年(人)",
            "員工薪資-中位數調整變動情形",
            f"員工薪資-平均數_{last_year}年(人)",
            f"員工薪資-平均數_{last_year-1}年(人)",
            "員工薪資-平均數調整變動情形",
        ],
        index="產業類別",
        aggfunc="mean",
        sort=False,
    )

    plots[f"{key}_產業類別_排序"] = plot_lines_bars(
        df_薪資_產業類別.sort_values([f"員工薪資-中位數_{last_year}年(人)"], ascending=False),
        title=f"{key}_產業類別_排序 {last_year}年度",
        lines_left_axis=[],
        lines_right_axis=[
            "員工薪資-中位數調整變動情形",
            "員工薪資-平均數調整變動情形",
        ],
        bars_left_axis=[
            f"員工薪資-中位數_{last_year}年(人)",
            f"員工薪資-中位數_{last_year-1}年(人)",
            f"員工薪資-平均數_{last_year}年(人)",
            f"員工薪資-平均數_{last_year-1}年(人)",
        ],
        sort=False,
        additional_layout={
            "yaxis": {"title": {"text": "平均"}},
            "yaxis2": {
                "title": {"text": f"{last_year}年度/{last_year-1}年度"},
                "tickformat": ".2%",
            },
        },
    )

    data_list = []
    df_公司 = df.set_index("公司").sort_values(
        [f"員工薪資-中位數_{last_year}年(人)"],
        ascending=False,
    )
    for company in df_公司.index:
        names = [
            f"員工薪資-中位數_{last_year}年(人)",
            f"員工薪資-中位數_{last_year-1}年(人)",
            f"員工薪資-平均數_{last_year}年(人)",
            f"員工薪資-平均數_{last_year-1}年(人)",
        ]
        data = {
            "type": "scatter",
            "name": f"{company}_金額",
            "x": names,
            "y": df_公司.loc[company, names].tolist(),
            "mode": "markers",
            "legendgroup": company,
        }
        data_list.append(data)

        names = [
            "員工薪資-中位數調整變動情形",
            "員工薪資-平均數調整變動情形",
        ]
        data = {
            "type": "scatter",
            "name": f"{company}_薪資變動",
            "x": names,
            "y": df_公司.loc[company, names].tolist(),
            "yaxis": "y2",
            "mode": "markers",
            "legendgroup": company,
        }
        data_list.append(data)

    buttons_kinds = [
        {
            "args": [
                {
                    "visible": [True] * len(df_公司.index) * 4,
                }
            ],  # 顯示所有線條
            "label": "全部公司",
            "method": "restyle",
        }
    ]
    kinds = df["產業類別"].dropna().unique().tolist()
    for kind in kinds:
        arr = [
            [True] * 2 if df_公司.loc[index, "產業類別"] == kind else [False] * 2
            for index in df_公司.index
        ]
        arr = sum(arr, [])
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
            "x": 0.5,
            "y": 1.09,
            "xanchor": "center",
            "yanchor": "top",
            "pad": {"r": 10, "t": 10},
            "buttons": buttons_kinds,
            "type": "dropdown",
            "direction": "down",
            "active": 0,
            "font": {"color": "#AAAAAA"},
            "name": "產業選擇",
        },
    ]

    layout = {
        "title": {"text": f"{key}_公司_產業類別_排序 {last_year}年度"},
        "hovermode": "x",
        "xaxis": {"type": "category"},
        "yaxis2": {
            "overlaying": "y",
            "side": "right",
            "tickformat": ".2%",
            "title": {"text": f"{last_year}年度/{last_year-1}年度"},
        },
        "yaxis": {"title": {"text": "平均"}},
        "barmode": "group",
        "updatemenus": updatemenus,
    }

    graph = {"data": data_list, "layout": layout}
    graph = merge_dict(copy.deepcopy(default_template), graph)
    plots[f"{key}_公司_產業_排序"] = plotly_json_dump(graph)


def plot_企業ESG資訊揭露彙總資料_人力發展_vs_公司合併報表董事酬金相關資訊(plots):
    key = "企業ESG資訊揭露彙總資料-人力發展 vs 公司合併報表董事酬金相關資訊"
    key = sanitize_filename(key)
    df_ESG = df_企業ESG資訊揭露彙總資料_人力發展()
    df_董事酬金 = df_公司合併報表董事酬金相關資訊()

    year_ESG = df_ESG.iloc[0]["報告年度"]
    year_董事酬金 = df_董事酬金.iloc[0]["出表日期"]

    df = pd.merge(
        df_ESG,
        df_董事酬金,
        how="outer",
        suffixes=["_ESG", "_董事酬金"],
        on=["公司代號", "公司名稱"],
    )
    df["公司"] = df["公司代號"].astype(str) + "_" + df["公司名稱"]
    df["董事總酬金平均/員工福利平均"] = (
        df["平均每位董事酬金-加計兼任員工酬金"] / df["員工福利平均數"]
    )
    df["董事總酬金平均/員工薪資平均"] = (
        df["平均每位董事酬金-加計兼任員工酬金"] / df["員工薪資平均數"]
    )
    df["董事總酬金平均/非主管員工薪資平均"] = (
        df["平均每位董事酬金-加計兼任員工酬金"] / df["非擔任主管職務之全時員工薪資平均數"]
    )
    df["董事總酬金平均/非主管員工薪資中位數"] = (
        df["平均每位董事酬金-加計兼任員工酬金"] / df["非擔任主管之全時員工薪資中位數"]
    )

    df_公司 = df.set_index("公司")
    plots[f"{key}"] = plot_lines_bars(
        df_公司,
        title=f"{key} 董事酬金出表日期:{year_董事酬金} ESG報告年度:{year_ESG}年",
        lines_left_axis=[
            "董事總酬金平均/非主管員工薪資中位數",
            "董事總酬金平均/非主管員工薪資平均",
            "董事總酬金平均/員工薪資平均",
            "董事總酬金平均/員工福利平均",
        ],
        bars_right_axis=[
            "非擔任主管之全時員工薪資中位數",
            "非擔任主管職務之全時員工薪資平均數",
            "員工薪資平均數",
            "員工福利平均數",
            "平均每位董事酬金-加計兼任員工酬金",
        ],
        additional_layout={
            "yaxis": {"title": {"text": "比值"}},
            "hovermode": "x unified",
        },
        sort=False,
    )
    plots[f"{key}_排序"] = plot_lines_bars(
        df_公司.sort_values(
            [
                "董事總酬金平均/非主管員工薪資中位數",
                "董事總酬金平均/非主管員工薪資平均",
                "董事總酬金平均/員工薪資平均",
                "董事總酬金平均/員工福利平均",
            ],
            ascending=False,
        ),
        title=f"{key}_排序 董事酬金出表日期:{year_董事酬金} ESG報告年度:{year_ESG}年",
        lines_left_axis=[
            "董事總酬金平均/非主管員工薪資中位數",
            "董事總酬金平均/非主管員工薪資平均",
            "董事總酬金平均/員工薪資平均",
            "董事總酬金平均/員工福利平均",
        ],
        bars_right_axis=[
            "非擔任主管之全時員工薪資中位數",
            "非擔任主管職務之全時員工薪資平均數",
            "員工薪資平均數",
            "員工福利平均數",
            "平均每位董事酬金-加計兼任員工酬金",
        ],
        additional_layout={
            "yaxis": {"title": {"text": "比值"}},
            "hovermode": "x unified",
        },
        sort=False,
    )

    df_產業 = df.pivot_table(
        [
            "董事總酬金平均/非主管員工薪資中位數",
            "董事總酬金平均/非主管員工薪資平均",
            "董事總酬金平均/員工薪資平均",
            "董事總酬金平均/員工福利平均",
            "非擔任主管之全時員工薪資中位數",
            "非擔任主管職務之全時員工薪資平均數",
            "員工薪資平均數",
            "員工福利平均數",
            "平均每位董事酬金-加計兼任員工酬金",
        ],
        index="產業類別",
        sort=False,
        aggfunc="mean",
    )
    plots[f"{key}_產業"] = plot_lines_bars(
        df_產業,
        title=f"{key} 董事酬金出表日期:{year_董事酬金} ESG報告年度:{year_ESG}年",
        lines_left_axis=[
            "董事總酬金平均/非主管員工薪資中位數",
            "董事總酬金平均/非主管員工薪資平均",
            "董事總酬金平均/員工薪資平均",
            "董事總酬金平均/員工福利平均",
        ],
        bars_right_axis=[
            "非擔任主管之全時員工薪資中位數",
            "非擔任主管職務之全時員工薪資平均數",
            "員工薪資平均數",
            "員工福利平均數",
            "平均每位董事酬金-加計兼任員工酬金",
        ],
        additional_layout={
            "yaxis": {"title": {"text": "比值"}},
            "hovermode": "x unified",
        },
        sort=False,
    )
    plots[f"{key}_產業_排序"] = plot_lines_bars(
        df_產業.sort_values(
            [
                "董事總酬金平均/非主管員工薪資中位數",
                "董事總酬金平均/非主管員工薪資平均",
                "董事總酬金平均/員工薪資平均",
                "董事總酬金平均/員工福利平均",
            ],
            ascending=False,
        ),
        title=f"{key}_排序 董事酬金出表日期:{year_董事酬金} ESG報告年度:{year_ESG}年",
        lines_left_axis=[
            "董事總酬金平均/非主管員工薪資中位數",
            "董事總酬金平均/非主管員工薪資平均",
            "董事總酬金平均/員工薪資平均",
            "董事總酬金平均/員工福利平均",
        ],
        bars_right_axis=[
            "非擔任主管之全時員工薪資中位數",
            "非擔任主管職務之全時員工薪資平均數",
            "員工薪資平均數",
            "員工福利平均數",
            "平均每位董事酬金-加計兼任員工酬金",
        ],
        additional_layout={
            "yaxis": {"title": {"text": "比值"}},
            "hovermode": "x unified",
        },
        sort=False,
    )

    data_list = []
    df_公司 = df.set_index("公司").sort_values(
        [
            "董事總酬金平均/非主管員工薪資中位數",
            "董事總酬金平均/非主管員工薪資平均",
            "董事總酬金平均/員工薪資平均",
            "董事總酬金平均/員工福利平均",
        ],
        ascending=False,
    )
    for company in df_公司.index:
        names = [
            "董事總酬金平均/非主管員工薪資中位數",
            "董事總酬金平均/非主管員工薪資平均",
            "董事總酬金平均/員工薪資平均",
            "董事總酬金平均/員工福利平均",
        ]
        data = {
            "type": "scatter",
            "name": f"{company}_比值",
            "x": names,
            "y": df_公司.loc[company, names].tolist(),
            "mode": "markers",
            "legendgroup": company,
            "visible": False if df_公司.loc[company, "稅後純益"] > 0 else True,
        }
        data_list.append(data)

        names = [
            "非擔任主管之全時員工薪資中位數",
            "非擔任主管職務之全時員工薪資平均數",
            "員工薪資平均數",
            "員工福利平均數",
            "平均每位董事酬金-加計兼任員工酬金",
        ]
        data = {
            "type": "bar",
            "name": f"{company}_金額",
            "x": names,
            "y": df_公司.loc[company, names].tolist(),
            "yaxis": "y2",
            "legendgroup": company,
            "visible": False if df_公司.loc[company, "稅後純益"] > 0 else True,
        }
        data_list.append(data)

    buttons_kinds = [
        {
            "args": [
                {
                    "visible": [True] * len(df_公司.index) * 2,
                }
            ],  # 顯示所有線條
            "label": "全部公司",
            "method": "restyle",
        }
    ]
    kinds = df["產業類別"].dropna().unique().tolist()
    for kind in kinds:
        arr = [
            [True, True] if df_公司.loc[index, "產業類別"] == kind else [False, False]
            for index in df_公司.index
        ]
        arr = sum(arr, [])
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

    buttons_稅後 = [
        {
            "args": [
                {
                    "visible": [True] * len(df_公司.index) * 2,
                }
            ],  # 顯示所有線條
            "label": "全部公司",
            "method": "restyle",
        }
    ]

    arr_pos = sum(
        [
            [True, True] if df_公司.loc[index, "稅後純益"] > 0 else [False, False]
            for index in df_公司.index
        ],
        [],
    )
    buttons_稅後.append(
        {
            "args": [
                {
                    "visible": arr_pos,
                }
            ],
            "label": "稅後純益 > 0",
            "method": "restyle",
        },
    )
    buttons_稅後.append(
        {
            "args": [
                {
                    "visible": [not show for show in arr_pos],
                }
            ],
            "label": "稅後純益 ≤ 0",
            "method": "restyle",
        },
    )

    updatemenus = [
        {
            "x": 0.5,
            "y": 1.09,
            "xanchor": "center",
            "yanchor": "top",
            "pad": {"r": 10, "t": 10},
            "buttons": buttons_kinds,
            "type": "dropdown",
            "direction": "down",
            "active": 0,
            "font": {"color": "#AAAAAA"},
            "name": "產業選擇",
        },
        {
            "x": 0.65,
            "y": 1.09,
            "xanchor": "center",
            "yanchor": "top",
            "pad": {"r": 10, "t": 10},
            "buttons": buttons_稅後,
            "type": "dropdown",
            "direction": "down",
            "active": 2,
            "font": {"color": "#AAAAAA"},
            "name": "稅後純益選擇",
        },
    ]

    layout = {
        "title": {
            "text": f"{key}_公司_產業_排序 董事酬金出表日期:{year_董事酬金} ESG報告年度:{year_ESG}年"
        },
        "hovermode": "x",
        "xaxis": {"type": "category"},
        "yaxis2": {
            "overlaying": "y",
            "side": "right",
        },
        "yaxis": {"title": {"text": "比值"}},
        "barmode": "group",
        "updatemenus": updatemenus,
    }

    graph = {"data": data_list, "layout": layout}
    graph = merge_dict(copy.deepcopy(default_template), graph)
    plots[f"{key}_公司_產業_排序"] = plotly_json_dump(graph)


def plot_企業ESG資訊揭露彙總資料_人力發展(plots):
    key = "企業ESG資訊揭露彙總資料-人力發展"
    key = sanitize_filename(key)
    df = df_企業ESG資訊揭露彙總資料_人力發展()

    year = df.iloc[0]["報告年度"]
    df = df.set_index("公司")

    df_薪資 = df.rename(
        columns={
            "非擔任主管職務之全時員工薪資平均數": "平均數",
            "非擔任主管之全時員工薪資中位數": "中位數",
        }
    )
    plots[f"{key}_非擔任主管職務之全時員工薪資"] = plot_lines_bars(
        df_薪資,
        title=f"{key}_非擔任主管職務之全時員工薪資 {year}年",
        lines_left_axis=["平均數"],
        lines_right_axis=[],
        bars_left_axis=["中位數"],
        sort=False,
    )
    plots[f"{key}_非擔任主管職務之全時員工薪資_排序"] = plot_lines_bars(
        df_薪資.sort_values(["中位數", "平均數"], ascending=False).dropna(),
        title=f"{key}_非擔任主管職務之全時員工薪資_排序 {year}年",
        lines_left_axis=["平均數"],
        lines_right_axis=[],
        bars_left_axis=["中位數"],
        sort=False,
    )

    if "職業災害人數及比率-人數" in df.columns:
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
            bars_left_axis=["人數"],
            title=f"{key}_職業災害人數及比率 {year}年",
            sort=False,
            additional_layout={"yaxis2": {"title": {"text": "比率(%)"}}},
        )
        plots[f"{key}_職業災害人數及比率_排序"] = plot_lines_bars(
            df_職災.sort_values(["人數", "比率"], ascending=False),
            lines_left_axis=[],
            lines_right_axis=["比率"],
            bars_left_axis=["人數"],
            title=f"{key}_職業災害人數及比率_排序 {year}年",
            sort=False,
            additional_layout={"yaxis2": {"title": {"text": "比率(%)"}}},
        )


def plot_歷年受僱員工每人每月總薪資平均數(plots):
    key = "歷年受僱員工每人每月總薪資平均數"
    key = sanitize_filename(key)
    df = df_歷年受僱員工每人每月總薪資平均數()

    df_year = df.filter(regex=r"^\d{4}$", axis="index")
    plots[f"{key}_年"] = plot_line(
        df_year,
        f"{key}_全體受僱員工(含本國籍、外國籍之全時員工及部分工時員工)_年 {df_year.index[0]}~{df_year.index[-1]}年",
    )
    df_month = df.filter(regex=r"^\d{6}$", axis="index")
    plots[f"{key}_月"] = plot_line(
        df_month,
        f"{key}_全體受僱員工(含本國籍、外國籍之全時員工及部分工時員工)_月 {df_month.index[0]}~{df_month.index[-1]}",
    )


def plot_歷年受僱員工每人每月經常性薪資平均數(plots):
    key = "歷年受僱員工每人每月經常性薪資平均數"
    key = sanitize_filename(key)
    df = df_歷年受僱員工每人每月經常性薪資平均數()

    df_year = df.filter(regex=r"^\d{4}$", axis="index")
    plots[f"{key}_年"] = plot_line(
        df_year,
        f"{key}_全體受僱員工(含本國籍、外國籍之全時員工及部分工時員工)_年 {df_year.index[0]}~{df_year.index[-1]}年",
    )
    df_month = df.filter(regex=r"^\d{6}$", axis="index")
    plots[f"{key}_月"] = plot_line(
        df_month,
        f"{key}_全體受僱員工(含本國籍、外國籍之全時員工及部分工時員工)_月 {df_month.index[0]}~{df_month.index[-1]}",
    )


def plot_工業及服務業全體受僱員工全年總薪資統計表(plots):
    key = "工業及服務業全體受僱員工全年總薪資統計表"
    key = sanitize_filename(key)
    (
        df_按性別及教育程度分,
        df_按年齡別分,
        df_按員工特性,
        df_按年齡及教育程度分,
        df_按工作場所所在縣市別及年齡別分,
    ) = df_工業及服務業全體受僱員工全年總薪資統計表()

    years = df_按性別及教育程度分["年度"].unique().tolist()
    df = df_按性別及教育程度分.pivot_table(
        index=["行業", "年度"], columns=["統計", "性別教育程度"], values="值", sort=False
    )

    df.columns = [f"{統計}_{教育程度}" for 統計, 教育程度 in df.columns]
    plots[f"{key}_行業及教育程度"] = plot_bar_stack_multi_index(
        df, f"{key}_行業及教育程度 {years[0]}~{years[-1]}年", additional_layout={"barmode": "group"}
    )

    years = df_按年齡別分["年度"].unique().tolist()
    df = df_按年齡別分.pivot_table(
        index=["行業", "年度"], columns=["統計", "年齡別"], values="值", sort=False
    )
    df.columns = [f"{統計}_{年齡別}" for 統計, 年齡別 in df.columns]
    plots[f"{key}_行業及年齡別"] = plot_bar_stack_multi_index(
        df, f"{key}_行業及年齡別 {years[0]}~{years[-1]}年", additional_layout={"barmode": "group"}
    )

    years = df_按員工特性["年度"].unique().tolist()
    df = df_按員工特性.pivot_table(
        index=["年齡教育別", "年度"], columns=["統計", "性別"], values="值", sort=False
    )
    df.columns = [f"{統計}_{性別}" for 統計, 性別 in df.columns]
    plots[f"{key}_員工特性"] = plot_bar_stack_multi_index(
        df, f"{key}_員工特性 {years[0]}~{years[-1]}年", additional_layout={"barmode": "group"}
    )

    years = df_按年齡及教育程度分["年度"].unique().tolist()
    df = df_按年齡及教育程度分.pivot_table(
        index=["年齡別", "年度"], columns=["統計", "教育程度"], values="值", sort=False
    )
    df.columns = [f"{統計}_{教育程度}" for 統計, 教育程度 in df.columns]
    plots[f"{key}_年齡及教育程度"] = plot_bar_stack_multi_index(
        df, f"{key}_年齡及教育程度 {years[0]}~{years[-1]}年", additional_layout={"barmode": "group"}
    )

    years = df_按工作場所所在縣市別及年齡別分["年度"].unique().tolist()
    df = df_按工作場所所在縣市別及年齡別分.pivot_table(
        index=["年齡別", "年度"], columns=["統計", "縣市"], values="值", sort=False
    )
    df.columns = [f"{統計}_{縣市}" for 統計, 縣市 in df.columns]
    plots[f"{key}_年齡及縣市"] = plot_bar_stack_multi_index(
        df, f"{key}_年齡及縣市 {years[0]}~{years[-1]}年", additional_layout={"barmode": "group"}
    )


def plot_工業及服務業受僱員工全年總薪資中位數及分布統計結果(plots):
    key = "工業及服務業受僱員工全年總薪資中位數及分布統計結果"
    key = sanitize_filename(key)
    (
        df_總薪資中位數_全體受僱員工按特性別分,
        df_總薪資中位數_本國籍全時受僱員工按特性別分,
        df_四等分位組分界點之全年總薪資_全體受僱員工按特性別分,
        df_四等分位組分界點之全年總薪資_本國籍全時受僱員工按特性別分,
        df_十等分位組分界點之全年總薪資_全體受僱員工,
    ) = df_工業及服務業受僱員工全年總薪資中位數及分布統計結果()

    df_總薪資中位數_全體受僱員工按特性別分 = df_總薪資中位數_全體受僱員工按特性別分.T
    plots[f"{key}_總薪資中位數_全體受僱員工按特性別分"] = plot_line(
        df_總薪資中位數_全體受僱員工按特性別分,
        f"{key}_總薪資中位數_全體受僱員工按特性別分 {df_總薪資中位數_全體受僱員工按特性別分.index[0]}~{df_總薪資中位數_全體受僱員工按特性別分.index[-1]}",
    )

    df_總薪資中位數_本國籍全時受僱員工按特性別分 = df_總薪資中位數_本國籍全時受僱員工按特性別分.T
    plots[f"{key}_總薪資中位數_本國籍全時受僱員工按特性別分"] = plot_line(
        df_總薪資中位數_本國籍全時受僱員工按特性別分,
        f"{key}_總薪資中位數_本國籍全時受僱員工按特性別分 {df_總薪資中位數_本國籍全時受僱員工按特性別分.index[0]}~{df_總薪資中位數_本國籍全時受僱員工按特性別分.index[-1]}",
    )

    def plot_box(df, title):
        data_list = []
        for name in df.index:
            data = {
                "type": "box",
                "name": f"{name}",
                "x": [name],
                "q1": [df.loc[name, "Q1"]],
                "median": [df.loc[name, "中位數"]],
                "q3": [df.loc[name, "Q3"]],
                "mean": [df.loc[name, "平均"]],
            }
            data_list.append(data)

        layout = {
            "title": {"text": title},
            "hovermode": "x",
        }
        graph = {"data": data_list, "layout": layout}
        graph = merge_dict(copy.deepcopy(default_template), graph)

        return plotly_json_dump(graph)

    lastyear = max(df_四等分位組分界點之全年總薪資_全體受僱員工按特性別分.index.get_level_values(0))
    plots[f"{key}_四等分位組分界點之全年總薪資_全體受僱員工按特性別分"] = plot_box(
        df_四等分位組分界點之全年總薪資_全體受僱員工按特性別分.loc[(lastyear,)],
        f"{key}_四等分位組分界點之全年總薪資_全體受僱員工按特性別分 {lastyear}年",
    )
    lastyear = max(
        df_四等分位組分界點之全年總薪資_本國籍全時受僱員工按特性別分.index.get_level_values(0)
    )
    plots[f"{key}_四等分位組分界點之全年總薪資_本國籍全時受僱員工按特性別分"] = plot_box(
        df_四等分位組分界點之全年總薪資_本國籍全時受僱員工按特性別分.loc[(lastyear,)],
        f"{key}_四等分位組分界點之全年總薪資_本國籍全時受僱員工按特性別分 {lastyear}年",
    )

    plots[f"{key}_十等分位組分界點之全年總薪資_全體受僱員工"] = plot_line(
        df_十等分位組分界點之全年總薪資_全體受僱員工,
        f"{key}_十等分位組分界點之全年總薪資_全體受僱員工 {df_十等分位組分界點之全年總薪資_全體受僱員工.index[0]}~{df_十等分位組分界點之全年總薪資_全體受僱員工.index[-1]}",
    )


def plot_工業及服務業每人每月工時_時_(plots):
    key = "工業及服務業每人每月工時(時)"
    key = sanitize_filename(key)
    df = df_受僱員工每人每月工時()

    df_年 = df.pivot_table(values=df.columns[1:-2], index="年", aggfunc="mean", sort=False)
    plots[f"{key}_年"] = plot_line(
        df_年,
        f"{key}_年 {df_年.index[0]}~{df_年.index[-1]}",
    )

    df_月 = df.pivot_table(values=df.columns[1:-2], index="年月", aggfunc="mean", sort=False)
    plots[f"{key}_月"] = plot_line(
        df_月,
        f"{key}_月 {df_月.index[0]}~{df_月.index[-1]}",
    )


def plot_各業廠商僱用職缺按月計薪者每人每月平均最低薪資_按職類及員工規模分(plots):
    key = "各業廠商僱用職缺按月計薪者每人每月平均最低薪資－按職類及員工規模分"
    key = sanitize_filename(key)
    df, lastyear = df_各業廠商僱用職缺按月計薪者每人每月平均最低薪資_按職類及員工規模分()

    plots[f"{key}"] = plot_bar_group(
        df.T,
        f"{key} {lastyear}年",
    )

    df_stacked = df.stack().reset_index()
    df_stacked.columns = ["項目別", "產業_園區", "月平均最低薪資"]
    df_stacked["資料標籤"] = (
        df_stacked["項目別"].str.strip() + " [" + df_stacked["產業_園區"].str.strip() + "]"
    )
    df_final = df_stacked.dropna(subset=["月平均最低薪資"]).sort_values(
        by="月平均最低薪資", ascending=False
    )
    df_output = df_final[["資料標籤", "月平均最低薪資"]].set_index("資料標籤")

    buttons_items = [
        {
            "args": [
                {
                    "visible": [True] * len(df_output.index),
                }
            ],  # 顯示所有線條
            "label": "全部項目",
            "method": "restyle",
        }
    ]
    items = df_stacked["項目別"].unique().tolist()
    for industry in items:
        arr = [i == industry for i in df_final["項目別"]]  # 依地區篩選
        buttons_items.append(
            {
                "args": [
                    {
                        "visible": arr,
                    }
                ],
                "label": industry,
                "method": "restyle",
            },
        )

    buttons_industries = [
        {
            "args": [
                {
                    "visible": [True] * len(df_output.index),
                }
            ],  # 顯示所有線條
            "label": "全部產業",
            "method": "restyle",
        }
    ]
    industries = df_stacked["產業_園區"].unique().tolist()
    for industry in industries:
        arr = [i == industry for i in df_final["產業_園區"]]  # 依地區篩選
        buttons_industries.append(
            {
                "args": [
                    {
                        "visible": arr,
                    }
                ],
                "label": industry,
                "method": "restyle",
            },
        )

    updatemenus = [
        {
            "x": 1,
            "y": 1.12,
            "xanchor": "left",
            "yanchor": "bottom",
            "pad": {"r": 10, "t": 10},
            "buttons": buttons_items,
            "type": "dropdown",
            "direction": "down",
            "active": 0,
            "font": {"color": "#AAAAAA"},
            "name": "職業選擇",
        },
        {
            "x": 1,
            "y": 1,
            "xanchor": "left",
            "yanchor": "bottom",
            "pad": {"r": 10, "t": 10},
            "buttons": buttons_industries,
            "type": "dropdown",
            "direction": "down",
            "active": 0,
            "font": {"color": "#AAAAAA"},
            "name": "產業選擇",
        },
    ]

    plots[f"{key}_排列"] = plot_bar_group(
        df_output.T, f"{key}_排列 {lastyear}年", additional_layout={"updatemenus": updatemenus}
    )


def plot_各業廠商調升經常性薪資參考各項因素之廠商比率_按行業分(plots):
    key = "各業廠商調升經常性薪資參考各項因素之廠商比率－按行業分"
    key = sanitize_filename(key)
    df, lastyear = df_各業廠商調升經常性薪資參考各項因素之廠商比率_按行業分()

    first_columns = [
        column for column in df.columns if "視為第一重要因素之廠商比率_百分比" in column
    ]
    df_first = df[first_columns]
    df_first.columns = df_first.columns.str.removesuffix("_視為第一重要因素之廠商比率_百分比")
    plots[f"{key}_視為第一重要因素之廠商比率_百分比"] = plot_bar_group(
        df_first.T,
        f"{key}_視為第一重要因素之廠商比率_百分比 {lastyear}年",
        additional_layout={"yaxis": {"tickformat": ".2%"}},
    )

    normal_columns = [column for column in df.columns if "視為重要因素之廠商比率_百分比" in column]
    df_normal = df[normal_columns]
    df_normal.columns = df_normal.columns.str.removesuffix("_視為重要因素之廠商比率_百分比")
    plots[f"{key}_視為重要因素之廠商比率_百分比"] = plot_bar_group(
        df_normal.T,
        f"{key}_視為重要因素之廠商比率_百分比 {lastyear}年",
        additional_layout={"yaxis": {"tickformat": ".2%"}},
    )


def plot_各業廠商調升員工經常性薪資之廠商與員工人數比率_按行業分(plots):
    key = "各業廠商調升員工經常性薪資之廠商與員工人數比率－按行業分"
    key = sanitize_filename(key)
    df = df_各業廠商調升員工經常性薪資之廠商與員工人數比率_按行業分()
    lastyear = df["年度"].max()

    df_lastyear = df[df["年度"] == lastyear].set_index("項目別")

    df_廠商 = df_lastyear[
        [
            "有調升敘薪標準_不含年資晉級_調薪人數超過百分之50之廠商比率_百分比",
            "有調升敘薪標準_不含年資晉級_調薪人數超過百分之50之廠商比率_平均每人調薪幅度_未滿3個百分點_百分比",
            "有調升敘薪標準_不含年資晉級_調薪人數超過百分之50之廠商比率_平均每人調薪幅度_3個百分點-未滿6個百分點_百分比",
            "有調升敘薪標準_不含年資晉級_調薪人數超過百分之50之廠商比率_平均每人調薪幅度_6個百分點-未滿9個百分點_百分比",
            "有調升敘薪標準_不含年資晉級_調薪人數超過百分之50之廠商比率_平均每人調薪幅度_9個百分點-未滿12個百分點_百分比",
            "有調升敘薪標準_不含年資晉級_調薪人數超過百分之50之廠商比率_平均每人調薪幅度_12個百分點-未滿15個百分點_百分比",
            "有調升敘薪標準_不含年資晉級_調薪人數超過百分之50之廠商比率_平均每人調薪幅度_15個百分點以上_百分比",
            "有調升敘薪標準_不含年資晉級_百分比",
            "有年資晉級_廠商比率_百分比",
            "加計年資晉級_廠商調升經常性薪資比率_百分比",
        ]
    ]
    df_廠商 = df_廠商.rename(
        columns={
            "有調升敘薪標準_不含年資晉級_調薪人數超過百分之50之廠商比率_百分比": "不含年資晉級_調薪人數超過百分之50",
            "有調升敘薪標準_不含年資晉級_調薪人數超過百分之50之廠商比率_平均每人調薪幅度_未滿3個百分點_百分比": "  (1)不含年資晉級_調薪人數超過百分之50_平均每人調薪幅度_<3%",
            "有調升敘薪標準_不含年資晉級_調薪人數超過百分之50之廠商比率_平均每人調薪幅度_3個百分點-未滿6個百分點_百分比": "  (2)不含年資晉級_調薪人數超過百分之50_平均每人調薪幅度_3% ≤ x < 6%",
            "有調升敘薪標準_不含年資晉級_調薪人數超過百分之50之廠商比率_平均每人調薪幅度_6個百分點-未滿9個百分點_百分比": "  (3)不含年資晉級_調薪人數超過百分之50_平均每人調薪幅度_6% ≤ x < 9%",
            "有調升敘薪標準_不含年資晉級_調薪人數超過百分之50之廠商比率_平均每人調薪幅度_9個百分點-未滿12個百分點_百分比": "  (4)不含年資晉級_調薪人數超過百分之50_平均每人調薪幅度_9% ≤ x < 12%",
            "有調升敘薪標準_不含年資晉級_調薪人數超過百分之50之廠商比率_平均每人調薪幅度_12個百分點-未滿15個百分點_百分比": "  (5)不含年資晉級_調薪人數超過百分之50_平均每人調薪幅度_12% ≤ x < 15%",
            "有調升敘薪標準_不含年資晉級_調薪人數超過百分之50之廠商比率_平均每人調薪幅度_15個百分點以上_百分比": "  (6)不含年資晉級_調薪人數超過百分之50_平均每人調薪幅度_15% ≤ x",
            "有調升敘薪標準_不含年資晉級_百分比": "不含年資晉級_調薪",
            "有年資晉級_廠商比率_百分比": "有年資晉級_廠商比率",
            "加計年資晉級_廠商調升經常性薪資比率_百分比": "加計年資晉級_廠商調升經常性薪資比率",
        }
    )

    plots[f"{key}_廠商比率"] = plot_bar_group(
        df_廠商,
        f"{key}_廠商比率 {lastyear}年",
        additional_layout={"yaxis": {"tickformat": ".2%"}, "hovermode": "x unified"},
    )

    df_員工 = df_lastyear[
        [
            "有調升敘薪標準_不含年資晉級_有調升經常性薪資之員工人數比率_百分比",
            "有年資晉級_員工人數比率_百分比",
            "加計年資晉級_經常性薪資調升之員工人數比率_百分比",
        ]
    ]
    df_員工 = df_員工.rename(
        columns={
            "有調升敘薪標準_不含年資晉級_有調升經常性薪資之員工人數比率_百分比": "不含年資晉級_有調升經常性薪資",
            "有年資晉級_員工人數比率_百分比": "有年資晉級_員工人數比率",
            "加計年資晉級_經常性薪資調升之員工人數比率_百分比": "加計年資晉級_經常性薪資調升",
        }
    )

    plots[f"{key}_員工比率"] = plot_bar_group(
        df_員工,
        f"{key}_員工比率 {lastyear}年",
        additional_layout={"yaxis": {"tickformat": ".2%"}, "hovermode": "x unified"},
    )

    df_調薪_歷史 = df.pivot_table(
        values=[
            "有調升敘薪標準_不含年資晉級_百分比",
            "有年資晉級_廠商比率_百分比",
            "加計年資晉級_廠商調升經常性薪資比率_百分比",
        ],
        columns="項目別",
        index="年度",
        sort=False,
    )

    for s in [
        "有調升敘薪標準_不含年資晉級_百分比",
        "有年資晉級_廠商比率_百分比",
        "加計年資晉級_廠商調升經常性薪資比率_百分比",
    ]:
        plots[f"{key}_調薪_歷史_{s}"] = plot_line(
            df_調薪_歷史.loc[:, (s,)],
            f"{key}_調薪_歷史_{s} {df_調薪_歷史.index[0]}~{df_調薪_歷史.index[-1]}年",
            additional_layout={"yaxis": {"tickformat": ".2%"}},
        )


def plot_公司合併報表董事酬金相關資訊(plots):
    key = "公司合併報表董事酬金相關資訊"
    key = sanitize_filename(key)
    df = df_公司合併報表董事酬金相關資訊()

    date_pub = df.iloc[0]["出表日期"]

    df_董事酬金 = df.sort_values("公司代號").set_index("公司")
    df_董事酬金 = df_董事酬金[
        [
            "董事酬金-合計",
            "平均每位董事酬金-董事酬金",
            "加計兼任員工酬金-合計",
            "平均每位董事酬金-加計兼任員工酬金",
            "稅後純益",
        ]
    ]

    plots[f"{key}_董事酬金"] = plot_lines_bars(
        df_董事酬金,
        title=f"{key}_董事酬金 出表日期:{date_pub}",
        lines_left_axis=["平均每位董事酬金-加計兼任員工酬金"],
        lines_right_axis=[
            "加計兼任員工酬金-合計",
            "稅後純益",
            "董事酬金-合計",
        ],
        bars_left_axis=["平均每位董事酬金-董事酬金"],
        sort=False,
        additional_layout={
            "yaxis": {"title": {"text": "平均"}},
            "yaxis2": {"title": {"text": "合計"}},
        },
    )
    plots[f"{key}_董事酬金_排序"] = plot_lines_bars(
        df_董事酬金.sort_values(
            ["平均每位董事酬金-加計兼任員工酬金", "平均每位董事酬金-董事酬金"], ascending=False
        ),
        title=f"{key}_董事酬金_排序 出表日期:{date_pub}",
        lines_left_axis=["平均每位董事酬金-加計兼任員工酬金"],
        lines_right_axis=[
            "加計兼任員工酬金-合計",
            "稅後純益",
            "董事酬金-合計",
        ],
        bars_left_axis=["平均每位董事酬金-董事酬金"],
        sort=False,
        additional_layout={
            "yaxis": {"title": {"text": "平均"}},
            "yaxis2": {"title": {"text": "合計"}},
        },
    )

    df_董事酬金_產業類別 = df.pivot_table(
        values=[
            "董事酬金-合計",
            "平均每位董事酬金-董事酬金",
            "加計兼任員工酬金-合計",
            "平均每位董事酬金-加計兼任員工酬金",
            "稅後純益",
        ],
        index="產業類別",
        aggfunc="mean",
        sort=False,
    )

    plots[f"{key}_董事酬金_產業類別"] = plot_lines_bars(
        df_董事酬金_產業類別,
        title=f"{key}_董事酬金_產業類別 出表日期:{date_pub}",
        lines_left_axis=["平均每位董事酬金-加計兼任員工酬金"],
        lines_right_axis=[
            "加計兼任員工酬金-合計",
            "稅後純益",
            "董事酬金-合計",
        ],
        bars_left_axis=["平均每位董事酬金-董事酬金"],
        sort=False,
        additional_layout={
            "yaxis": {"title": {"text": "平均"}},
            "yaxis2": {"title": {"text": "合計"}},
        },
    )
    plots[f"{key}_董事酬金_產業類別_排序"] = plot_lines_bars(
        df_董事酬金_產業類別.sort_values(
            ["平均每位董事酬金-加計兼任員工酬金", "平均每位董事酬金-董事酬金"], ascending=False
        ),
        title=f"{key}_董事酬金_產業類別_排序 出表日期:{date_pub}",
        lines_left_axis=["平均每位董事酬金-加計兼任員工酬金"],
        lines_right_axis=[
            "加計兼任員工酬金-合計",
            "稅後純益",
            "董事酬金-合計",
        ],
        bars_left_axis=["平均每位董事酬金-董事酬金"],
        sort=False,
        additional_layout={
            "yaxis": {"title": {"text": "平均"}},
            "yaxis2": {"title": {"text": "合計"}},
        },
    )


def plot_公司合併報表監察人酬金相關資訊(plots):
    key = "公司合併報表監察人酬金相關資訊"
    key = sanitize_filename(key)
    df = df_公司合併報表監察人酬金相關資訊()

    date_pub = df.iloc[0]["出表日期"]

    df_監察人酬金 = df.sort_values("公司代號").set_index("公司")
    df_監察人酬金 = df_監察人酬金[
        [
            "監察人酬金-合計",
            "平均每位監察人酬金",
            "稅後純益",
        ]
    ]

    plots[f"{key}_監察人酬金"] = plot_lines_bars(
        df_監察人酬金,
        title=f"{key}_監察人酬金 出表日期:{date_pub}",
        lines_left_axis=[],
        lines_right_axis=[
            "監察人酬金-合計",
            "稅後純益",
        ],
        bars_left_axis=["平均每位監察人酬金"],
        sort=False,
        additional_layout={
            "yaxis": {"title": {"text": "平均"}},
            "yaxis2": {"title": {"text": "合計"}},
        },
    )
    plots[f"{key}_監察人酬金_排序"] = plot_lines_bars(
        df_監察人酬金.sort_values(["平均每位監察人酬金"], ascending=False),
        title=f"{key}_監察人酬金_排序 出表日期:{date_pub}",
        lines_left_axis=[],
        lines_right_axis=[
            "監察人酬金-合計",
            "稅後純益",
        ],
        bars_left_axis=["平均每位監察人酬金"],
        sort=False,
        additional_layout={
            "yaxis": {"title": {"text": "平均"}},
            "yaxis2": {"title": {"text": "合計"}},
        },
    )

    df_監察人酬金_產業類別 = df.pivot_table(
        values=[
            "監察人酬金-合計",
            "平均每位監察人酬金",
            "稅後純益",
        ],
        index="產業類別",
        aggfunc="mean",
        sort=False,
    )

    plots[f"{key}_監察人酬金_產業類別"] = plot_lines_bars(
        df_監察人酬金_產業類別,
        title=f"{key}_監察人酬金_產業類別 出表日期:{date_pub}",
        lines_left_axis=[],
        lines_right_axis=[
            "監察人酬金-合計",
            "稅後純益",
        ],
        bars_left_axis=["平均每位監察人酬金"],
        sort=False,
        additional_layout={
            "yaxis": {"title": {"text": "平均"}},
            "yaxis2": {"title": {"text": "合計"}},
        },
    )
    plots[f"{key}_監察人酬金_產業類別_排序"] = plot_lines_bars(
        df_監察人酬金_產業類別.sort_values(["平均每位監察人酬金"], ascending=False),
        title=f"{key}_監察人酬金_產業類別_排序 出表日期:{date_pub}",
        lines_left_axis=[],
        lines_right_axis=[
            "監察人酬金-合計",
            "稅後純益",
        ],
        bars_left_axis=["平均每位監察人酬金"],
        sort=False,
        additional_layout={
            "yaxis": {"title": {"text": "平均"}},
            "yaxis2": {"title": {"text": "合計"}},
        },
    )


def plot_綜稅總所得各縣市申報統計分析表(plots):
    key = "綜稅總所得各縣市申報統計分析表"
    key = sanitize_filename(key)
    df = df_綜稅總所得各縣市申報統計分析表()
    lastyear = df["年度"].max()

    df["下限"] = df["平均數"] - df["標準差"]
    df["上限"] = df["平均數"] + df["標準差"] * 3

    sorted_column = "中位數"
    df_縣市別 = (
        df[df["年度"] == lastyear].set_index("縣市別").sort_values(sorted_column, ascending=False)
    )
    data_list = []
    for name in df_縣市別.index:
        data = {
            "type": "box",
            "name": f"{name}_{df_縣市別.loc[name, "納稅單位(戶)"]}戶",
            "x": [name],
            "q1": [df_縣市別.loc[name, "第一分位數"]],
            "median": [df_縣市別.loc[name, "中位數"]],
            "q3": [df_縣市別.loc[name, "第三分位數"]],
            "mean": [df_縣市別.loc[name, "平均數"]],
            "sd": [df_縣市別.loc[name, "標準差"]],
            "lowerfence": [df_縣市別.loc[name, "下限"]],
            "upperfence": [df_縣市別.loc[name, "上限"]],
        }
        data_list.append(data)

    layout = {
        "title": {"text": f"{key} {sorted_column}排序 {lastyear}年"},
        "hovermode": "x",
    }
    graph = {"data": data_list, "layout": layout}
    graph = merge_dict(copy.deepcopy(default_template), graph)

    plots[f"{key}"] = plotly_json_dump(graph)


def plot_綜稅綜合所得總額全國各縣市鄉鎮村里統計分析表(plots):
    key = "綜稅綜合所得總額全國各縣市鄉鎮村里統計分析表"
    key = sanitize_filename(key)
    df = df_綜稅綜合所得總額全國各縣市鄉鎮村里統計分析表()
    lastyear = df["年度"].max()

    df["下限"] = df["平均數"] - df["標準差"]
    df["上限"] = df["平均數"] + df["標準差"] * 3

    for sorted_column in ["中位數", "上限", "標準差"]:
        df_縣市別 = (
            df[df["年度"] == lastyear]
            .set_index("縣市鄉鎮村里")
            .sort_values(sorted_column, ascending=False)
        )
        data_list = []
        for name in df_縣市別.index:
            data = {
                "type": "box",
                "name": f"{name}_{df_縣市別.loc[name, "納稅單位(戶)"]}戶",
                "x": [name],
                "q1": [df_縣市別.loc[name, "第一分位數"]],
                "median": [df_縣市別.loc[name, "中位數"]],
                "q3": [df_縣市別.loc[name, "第三分位數"]],
                "mean": [df_縣市別.loc[name, "平均數"]],
                "sd": [df_縣市別.loc[name, "標準差"]],
                "lowerfence": [df_縣市別.loc[name, "下限"]],
                "upperfence": [df_縣市別.loc[name, "上限"]],
                "visible": True if df_縣市別.loc[name, "村里"] == "合計" else False,
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
        regions_detail = df["縣市鄉鎮"].unique().tolist()
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
                "active": 1,
                "font": {"color": "#AAAAAA"},
                "name": "地區選擇",
            },
        ]

        layout = {
            "title": {"text": f"{key} {sorted_column}排序 {lastyear}年"},
            "hovermode": "x",
            "updatemenus": updatemenus,
        }
        graph = {"data": data_list, "layout": layout}
        graph = merge_dict(copy.deepcopy(default_template), graph)

        plots[f"{key}_{sorted_column}"] = plotly_json_dump(graph)


def plot_財政統計年報_綜合所得稅結算申報_按淨所得級距別分(plots):
    key = "財政統計年報_綜合所得稅結算申報－按淨所得級距別分"
    key = sanitize_filename(key)
    df, lastyear = df_財政統計年報_綜合所得稅結算申報_按淨所得級距別分()

    plots[f"{key}_戶數"] = plot_bar_group(df[["申報戶數"]], f"{key}_戶數 {lastyear}年")
    plots[f"{key}_戶數比例"] = plot_bar_group(
        df[["申報戶數"]] / df["申報戶數"].sum(),
        f"{key}_戶數比例 {lastyear}年",
        additional_layout={"yaxis": {"tickformat": ".2%"}},
    )

    cols = [
        col
        for col in df.columns
        if "各類所得_" in col
        and "合計" not in col
        and col != "各類所得_股利所得_分開計稅之股利所得"
    ]

    plots[f"{key}_各類所得平均"] = plot_bar_group(
        df[["各類所得_合計"] + cols].div(df["申報戶數"], axis="index"),
        f"{key}_各類所得平均 {lastyear}年",
    )
    plots[f"{key}_各類所得比例"] = plot_bar_group(
        df[cols].div(df["各類所得_合計"], axis="index").loc[:, ::-1],
        f"{key}_各類所得比例 {lastyear}年",
        additional_layout={"barmode": "stack", "yaxis": {"tickformat": ".2%"}},
    )


def plot_勞工退休金提繳統計年報_按地區_行業及規模別(plots):
    key = "勞工退休金提繳統計年報-按地區、行業及規模別"
    key = sanitize_filename(key)
    df = df_勞工退休金提繳統計年報_按地區_行業及規模別()

    year = df.iloc[0]["計費年度"]

    df = df.pivot_table(values=["月底人數", "總提繳工資金額"], index="地區別", sort=False)
    df["平均提繳工資金額"] = df["總提繳工資金額"] / df["月底人數"]

    plots[f"{key}"] = plot_bar_group(
        df.drop(["總提繳工資金額", "月底人數"], axis="columns").T, f"{key} {year}年"
    )


def plot_歷史_勞工退休金提繳統計年報_按地區_行業及規模別_按地區_行業及規模別(plots):
    key = "歷史_勞工退休金提繳統計年報_按地區_行業及規模別-按地區、行業及規模別"
    key = sanitize_filename(key)
    df = df_歷史_勞工退休金提繳統計年報_按地區_行業及規模別()

    df = df.pivot_table(values="平均提繳工資", columns="地區", index="年度", sort=False)

    plots[f"{key}"] = plot_line(df, f"{key} {df.index[0]}~{df.index[-1]}年")


def plot_勞工退休準備金專戶餘額統計(plots):
    key = "勞工退休準備金專戶餘額統計"
    key = sanitize_filename(key)
    df = df_勞工退休準備金專戶餘額統計()

    df = df.pivot_table(
        values=["家數", "佔總數比率"], index="年度", columns="專戶餘額（級距）", sort=False
    )

    plots[f"{key}_佔總數比率"] = plot_bar_group(
        df.loc[:, ("佔總數比率",)] / 100,
        f"{key}_佔總數比率 {df.index[0]}~{df.index[-1]}",
        additional_layout={"yaxis": {"tickformat": ".2%"}},
    )
    plots[f"{key}_家數"] = plot_bar_group(
        df.loc[:, ("家數",)], f"{key}_家數 {df.index[0]}~{df.index[-1]}"
    )


def plot_勞工退休準備金提撥率統計(plots):
    key = "勞工退休準備金提撥率統計"
    key = sanitize_filename(key)
    df = df_勞工退休準備金提撥率統計()

    df = df.pivot_table(
        values=["家數", "佔總數比率"], index="年度", columns="提撥率（級距）", sort=False
    )

    plots[f"{key}_佔總數比率"] = plot_bar_group(
        df.loc[:, ("佔總數比率",)] / 100,
        f"{key}_佔總數比率 {df.index[0]}~{df.index[-1]}",
        additional_layout={"yaxis": {"tickformat": ".2%"}},
    )
    plots[f"{key}_家數"] = plot_bar_group(
        df.loc[:, ("家數",)], f"{key}_家數 {df.index[0]}~{df.index[-1]}"
    )


def plot_勞工退休金提繳單位_提繳人數_提繳工資_提繳金額概況(plots):
    key = "勞工退休金提繳單位、提繳人數、提繳工資、提繳金額概況"
    key = sanitize_filename(key)
    df = df_勞工退休金提繳單位_提繳人數_提繳工資_提繳金額概況()

    df = df.pivot_table(values="提繳工資平均金額", index="年度/底", sort=False)

    plots[f"{key}"] = plot_line(
        df, f"{key} {df.index[0]}~{df.index[-1]}年", additional_layout={"showlegend": True}
    )


def plot_勞工退休金平均提繳工資_按行業別(plots):
    key = "勞工退休金平均提繳工資-按行業別"
    key = sanitize_filename(key)
    df = df_勞工退休金平均提繳工資_按行業別()

    df = df.pivot_table(values="平均提繳工資金額", index="年度", columns="行業別", sort=False)

    plots[f"{key}"] = plot_line(df, f"{key} {df.index[0]}~{df.index[-1]}年")


def plot_勞工退休金平均提繳工資_按年齡組別(plots):
    key = "勞工退休金平均提繳工資-按年齡組別"
    key = sanitize_filename(key)
    df = df_勞工退休金平均提繳工資_按年齡組別()

    df = df.pivot_table(values="平均提繳工資金額", index="年度", columns="年齡級距", sort=False)

    plots[f"{key}"] = plot_line(df, f"{key} {df.index[0]}~{df.index[-1]}年")


def plot_新制勞工退休基金歷年最近月份收益率(plots):
    key = "新制勞工退休基金歷年最近月份收益率"
    key = sanitize_filename(key)
    df = df_新制勞工退休基金歷年最近月份收益率()

    df = df.pivot_table(values="最近月份收益率", index="年月別", sort=False) / 100
    df.index = "20" + df.index

    df_year = df.filter(regex=r"-Dec", axis="index")
    df_year.index = df_year.index.str.replace("-Dec", "")
    period = len(df_year)
    if "-Dec" not in df.index[-1]:
        df_year = pd.concat([df_year, df.iloc[[-1]]], axis="index")
        now = str(datetime.today().year)[2:]
        df_rest = df.filter(regex=rf"{now}-", axis="index")
        period += len(df_rest) / 12

    total_return = (1 + df_year["最近月份收益率"]).product()

    irr_return = total_return ** (1 / period) - 1

    plots[f"{key}"] = plot_line(
        df,
        f"{key} {df.index[0]}~{df.index[-1]}",
        additional_layout={"yaxis": {"tickformat": ".2%"}},
    )

    plots[f"{key}_年回報率"] = plot_bar_group(
        df_year,
        f"{key}_年回報率 {df.index[0]}~{df.index[-1]} 總報酬率:{(total_return-1)*100:.2f}% 年化報酬率:{irr_return*100:.2f}%",
        additional_layout={"yaxis": {"tickformat": ".2%"}},
    )


def plot_全國賦稅收入實徵淨額日曆年別_按稅目別與地區別分(plots):
    key = "全國賦稅收入實徵淨額日曆年別-按稅目別與地區別分"
    key = sanitize_filename(key)
    df = df_全國賦稅收入實徵淨額日曆年別_按稅目別與地區別分()

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

    if not df_all.index[-1].startswith(df_all_year.index[-1]):
        last_year = df_all.filter(regex=rf"^{df_all.index[-1][:5]} *\d+月$", axis="index")
        last_year = pd.DataFrame({last_year.index[-1]: last_year.sum(axis="index")})
        df_all_year = pd.concat([df_all_year, last_year.T], axis="index")
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


def plot_全國賦稅收入實徵淨額與預算數之比較(plots, items):
    key = "全國賦稅收入實徵淨額與預算數之比較"
    key = sanitize_filename(key)
    df = df_全國賦稅收入實徵淨額與預算數之比較()

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


def plot_主計總處統計專區_家庭收支調查_統計表_常用資料(plots):
    key = "主計總處統計專區 家庭收支調查 統計表 常用資料"
    key = sanitize_filename(key)
    (
        df_平均每人月消費,
        df_平均每戶可支配所得,
        df_平均每人可支配所得,
        df_每戶可支配所得中位數,
        df_每人可支配所得中位數,
    ) = df_主計總處統計專區_家庭收支調查_統計表_常用資料()

    df_平均每人月消費 = df_平均每人月消費.set_index("年別")
    plots[f"{key}_平均每人月消費"] = plot_line(
        df_平均每人月消費,
        f"{key}_平均每人月消費 {df_平均每人月消費.index[0]}~{df_平均每人月消費.index[-1]}年",
    )

    df_平均每戶可支配所得 = df_平均每戶可支配所得.set_index("年別")
    plots[f"{key}_平均每戶可支配所得"] = plot_line(
        df_平均每戶可支配所得,
        f"{key}_平均每戶可支配所得 {df_平均每戶可支配所得.index[0]}~{df_平均每戶可支配所得.index[-1]}年",
    )
    df_平均每人可支配所得 = df_平均每人可支配所得.set_index("年別")
    plots[f"{key}_平均每人可支配所得"] = plot_line(
        df_平均每人可支配所得,
        f"{key}_平均每人可支配所得_每人可支配所得=每戶可支配所得/每戶人數 {df_平均每人可支配所得.index[0]}~{df_平均每人可支配所得.index[-1]}年",
    )

    df_每戶可支配所得中位數 = df_每戶可支配所得中位數.set_index("年別")
    plots[f"{key}_每戶可支配所得中位數"] = plot_line(
        df_每戶可支配所得中位數,
        f"{key}_每戶可支配所得中位數 {df_每戶可支配所得中位數.index[0]}~{df_每戶可支配所得中位數.index[-1]}年",
    )
    df_每人可支配所得中位數 = df_每人可支配所得中位數.set_index("年別")
    plots[f"{key}_每人可支配所得中位數"] = plot_line(
        df_每人可支配所得中位數,
        f"{key}_每人可支配所得中位數_每人可支配所得=每戶可支配所得/每戶人數 {df_每人可支配所得中位數.index[0]}~{df_每人可支配所得中位數.index[-1]}年",
    )


def plot_進口貿易值_按洲別___國別分_CY2001__(plots):
    年月混合_plot(
        plots,
        key="進口貿易值_按洲別 ∕ 國別分(CY2001~)",
        df_get=df_進口貿易值_按洲別_國別分,
        title_suffix="(美元)",
    )


def plot_出口貿易值_按洲別___國別分_CY2001__(plots):
    年月混合_plot(
        plots,
        key="出口貿易值_按洲別 ∕ 國別分(CY2001~)",
        df_get=df_出口貿易值_按洲別_國別分,
        title_suffix="(美元)",
    )


def plot_進出口貿易值_按國際商品統一分類制度_HS_及主要國別分(plots):
    key = "進出口貿易值_按國際商品統一分類制度(HS)及主要國別分"
    key = sanitize_filename(key)
    df = df_進出口貿易值_按國際商品統一分類制度_HS_及主要國別分()

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

    buttons_countries_exports = [
        {
            "args": [
                {
                    "visible": [True] * num_traces,
                }
            ],  # 顯示所有線條
            "label": "全部國家進出口",
            "method": "restyle",
        }
    ]
    for country in countries:
        for export in exports:
            arr = [col[0] == country and col[1] == export for col in df.columns]

            buttons_countries_exports.append(
                {
                    "args": [
                        {
                            "visible": arr,
                        }
                    ],
                    "label": country + export,
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
        {
            "x": 0.6,
            "y": 0.98,
            "xanchor": "left",
            "yanchor": "bottom",
            "pad": {"r": 10, "t": 10},
            "buttons": buttons_countries_exports,
            "type": "dropdown",
            "direction": "down",
            "active": 0,
            "font": {"color": "#AAAAAA"},
            "name": "國家進出口選擇",
        },
    ]

    df.columns = [f"{country}{export}{kind}" for country, export, kind in df.columns]

    df_year = df.filter(regex=r"\d+年$", axis="index")

    if not df.index[-1].startswith(df_year.index[-1]):
        last_year = df.filter(regex=rf"^{df.index[-1][:5]} *\d+月$", axis="index")
        last_year = pd.DataFrame({last_year.index[-1]: last_year.sum(axis="index")})
        df_year = pd.concat([df_year, last_year.T], axis="index")
    plots[f"{key}_年"] = plot_line(
        df_year,
        f"{key}_年(美元) {df_year.index[0]}~{df_year.index[-1]}",
        {"updatemenus": updatemenus},
    )

    df_month = df.filter(regex=r"\d+年 *\d+月$", axis="index")
    plots[f"{key}_月"] = plot_line(
        df_month,
        f"{key}_月(美元) {df_month.index[0]}~{df_month.index[-1]}",
        {"updatemenus": updatemenus},
    )


def plot_進口值_按主要貨品分(plots):
    年月混合_plot(
        plots,
        key="進口值_按主要貨品分",
        df_get=df_進口值_按主要貨品分,
        title_suffix="按美元計算",
    )


def plot_出口值_按主要貨品分(plots):
    年月混合_plot(
        plots,
        key="出口值_按主要貨品分",
        df_get=df_出口值_按主要貨品分,
        title_suffix="按美元計算",
    )


def plot_貿易指數_進口單位價值指數(plots):
    年月混合_plot(
        plots,
        key="貿易指數－進口單位價值指數",
        df_get=df_貿易指數_進口單位價值指數,
        title_suffix="(採連鎖法，參考年為2021年)",
        sum_or_keep="keep",
    )


def plot_貿易指數_出口單位價值指數(plots):
    年月混合_plot(
        plots,
        key="貿易指數－出口單位價值指數",
        df_get=df_貿易指數_出口單位價值指數,
        title_suffix="(採連鎖法，參考年為2021年)",
        sum_or_keep="keep",
    )


def plot_貿易指數_進口數量指數(plots):
    年月混合_plot(
        plots,
        key="貿易指數－進口數量指數",
        df_get=df_貿易指數_進口數量指數,
        title_suffix="(參考年為2021年)",
        sum_or_keep="keep",
    )


def plot_貿易指數_出口數量指數(plots):
    年月混合_plot(
        plots,
        key="貿易指數－出口數量指數",
        df_get=df_貿易指數_出口數量指數,
        title_suffix="(參考年為2021年)",
        sum_or_keep="keep",
    )


def plot_村里戶數_單一年齡人口_新增區域代碼_(plots):
    key = "村里戶數、單一年齡人口（新增區域代碼）"
    key = sanitize_filename(key)
    df = df_村里戶數_單一年齡人口()
    yearmonth = df["統計年月"].max()

    df_男_年齡_縣市 = df.pivot_table(
        values=df.columns[8 : 202 + 8 : 2],
        columns="縣市",
        aggfunc="sum",
        sort=False,
    )
    df_男_年齡_縣市.index = df_男_年齡_縣市.index.str.removesuffix("-男")
    df_男_年齡_縣市["總計"] = df_男_年齡_縣市.sum(axis="columns")

    df_女_年齡_縣市 = df.pivot_table(
        values=df.columns[9 : 202 + 9 : 2],
        columns="縣市",
        aggfunc="sum",
        sort=False,
    )
    df_女_年齡_縣市.index = df_女_年齡_縣市.index.str.removesuffix("-女")
    df_女_年齡_縣市["總計"] = df_女_年齡_縣市.sum(axis="columns")

    plots[f"{key}_年齡_縣市"] = plot_pyramid(
        f"{key}_年齡_縣市 {yearmonth}",
        df_男_年齡_縣市,
        df_女_年齡_縣市,
        regions=df_男_年齡_縣市.columns,
        y=df_男_年齡_縣市.index.tolist(),
    )


def plot_現住人口性別_年齡_婚姻狀況_含同婚_(plots):
    key = "現住人口性別、年齡、婚姻狀況(含同婚)"
    key = sanitize_filename(key)
    df = df_現住人口性別_年齡_婚姻狀況()
    year = df["statistic_yyy"].max()
    df = df[df["statistic_yyy"] == year]

    df_婚姻_總人數 = df.pivot_table(
        values="population",
        index="sex",
        columns="marital_status",
        aggfunc="sum",
        sort=False,
    )
    plots[f"{key}_人數分佈"] = plot_bar_group(
        df_婚姻_總人數,
        f"{key}_人數分佈 {year}年",
    )

    df_男_年齡_婚姻_縣市 = df[df["sex"] == "男"].pivot_table(
        values="population",
        index=["marital_status", "age"],
        columns="縣市",
        aggfunc="sum",
        sort=False,
    )
    df_男_年齡_婚姻_縣市["總計"] = df_男_年齡_婚姻_縣市.sum(axis="columns")
    df_single = (
        df_男_年齡_婚姻_縣市.loc[("未婚",), :]
        + df_男_年齡_婚姻_縣市.loc[("離婚_不同性別",), :]
        + df_男_年齡_婚姻_縣市.loc[("離婚_相同性別",), :]
        + df_男_年齡_婚姻_縣市.loc[("喪偶_不同性別",), :]
        + df_男_年齡_婚姻_縣市.loc[("喪偶_相同性別",), :]
    )
    df_single.index = [("單身", i) for i in df_single.index]
    df_男_年齡_婚姻_縣市 = pd.concat([df_男_年齡_婚姻_縣市, df_single])

    df_女_年齡_婚姻_縣市 = df[df["sex"] == "女"].pivot_table(
        values="population",
        index=["marital_status", "age"],
        columns="縣市",
        aggfunc="sum",
        sort=False,
    )
    df_女_年齡_婚姻_縣市["總計"] = df_女_年齡_婚姻_縣市.sum(axis="columns")
    df_single = (
        df_女_年齡_婚姻_縣市.loc[("未婚",), :]
        + df_女_年齡_婚姻_縣市.loc[("離婚_不同性別",), :]
        + df_女_年齡_婚姻_縣市.loc[("離婚_相同性別",), :]
        + df_女_年齡_婚姻_縣市.loc[("喪偶_不同性別",), :]
        + df_女_年齡_婚姻_縣市.loc[("喪偶_相同性別",), :]
    )
    df_single.index = [("單身", i) for i in df_single.index]
    df_女_年齡_婚姻_縣市 = pd.concat([df_女_年齡_婚姻_縣市, df_single])

    plots[f"{key}_男女_年齡_婚姻_縣市"] = plot_pyramid(
        f"{key}_男女_年齡_婚姻_縣市 {year}年",
        df_男_年齡_婚姻_縣市,
        df_女_年齡_婚姻_縣市,
        regions=df_男_年齡_婚姻_縣市.columns,
        y=list(zip(*df_男_年齡_婚姻_縣市.index.tolist())),
    )

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=pd.errors.PerformanceWarning)

        plots[f"{key}_男女_年齡_未婚"] = plot_pyramid(
            f"{key}_男女_年齡_未婚 {year}年",
            df_男_年齡_婚姻_縣市.loc[("未婚",), :],
            df_女_年齡_婚姻_縣市.loc[("未婚",), :],
            regions=df_男_年齡_婚姻_縣市.loc[("未婚",), :].columns,
            y=df_男_年齡_婚姻_縣市.loc[("未婚",), :].index.tolist(),
        )

        plots[f"{key}_男女_年齡_單身"] = plot_pyramid(
            f"{key}_男女_年齡_單身(含離婚、喪偶) {year}年",
            df_男_年齡_婚姻_縣市.loc[("單身",), :],
            df_女_年齡_婚姻_縣市.loc[("單身",), :],
            regions=df_男_年齡_婚姻_縣市.loc[("單身",), :].columns,
            y=df_男_年齡_婚姻_縣市.loc[("單身",), :].index.tolist(),
        )


def plot_動態資料統計表_含同婚_(plots):
    key = "動態資料統計表（含同婚）"
    key = sanitize_filename(key)
    df = df_動態資料統計表()

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
            [df_人口_年, df[df["統計年月"] == df["統計年月"].iloc[-1]]],
            axis="index",
            ignore_index=True,
        )
    summary(df_人口_年, "人口數_年", "人口數_合計", "人口數_男", "人口數_女", "年")
    summary(df, "人口數_年月", "人口數_合計", "人口數_男", "人口數_女", "統計年月")
    summary(
        df,
        "人口自然增加數_年",
        "人口自然增加數_合計",
        "人口自然增加數_男",
        "人口自然增加數_女",
        "年",
    )
    summary(
        df,
        "人口自然增加數_年月",
        "人口自然增加數_合計",
        "人口自然增加數_男",
        "人口自然增加數_女",
        "統計年月",
    )
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
                "人口自然增加數_合計",
                "人口自然增加數_男",
                "人口自然增加數_女",
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

        df_人口_出生_死亡["人口社會增加數_合計"] = (
            df_人口_出生_死亡["人口數_合計"]
            - df_人口_出生_死亡["人口數_合計"].shift(1, axis="index")
            - df_人口_出生_死亡["人口自然增加數_合計"]
        )
        df_人口_出生_死亡["人口社會增加數_男"] = (
            df_人口_出生_死亡["人口數_男"]
            - df_人口_出生_死亡["人口數_男"].shift(1, axis="index")
            - df_人口_出生_死亡["人口自然增加數_男"]
        )
        df_人口_出生_死亡["人口社會增加數_女"] = (
            df_人口_出生_死亡["人口數_女"]
            - df_人口_出生_死亡["人口數_女"].shift(1, axis="index")
            - df_人口_出生_死亡["人口自然增加數_女"]
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
        df_人口_出生_死亡["人口自然增加率_合計"] = (
            df_人口_出生_死亡["人口自然增加數_合計"] / df_人口_出生_死亡["人口數_合計"]
        )
        df_人口_出生_死亡["人口自然增加率_男"] = (
            df_人口_出生_死亡["人口自然增加數_男"] / df_人口_出生_死亡["人口數_男"]
        )
        df_人口_出生_死亡["人口自然增加率_女"] = (
            df_人口_出生_死亡["人口自然增加數_女"] / df_人口_出生_死亡["人口數_女"]
        )
        df_人口_出生_死亡["人口社會增加率_合計"] = (
            df_人口_出生_死亡["人口社會增加數_合計"] / df_人口_出生_死亡["人口數_合計"]
        )
        df_人口_出生_死亡["人口社會增加率_男"] = (
            df_人口_出生_死亡["人口社會增加數_男"] / df_人口_出生_死亡["人口數_男"]
        )
        df_人口_出生_死亡["人口社會增加率_女"] = (
            df_人口_出生_死亡["人口社會增加數_女"] / df_人口_出生_死亡["人口數_女"]
        )

        plots[f"{key}_人口_出生_死亡_{suffix}"] = plot_lines_bars(
            df_人口_出生_死亡,
            lines_left_axis=["出生數_合計", "死亡人數_合計"],
            lines_right_axis=["人口數_合計"],
            bars_left_axis=["人口自然增加數_合計", "人口社會增加數_合計"],
            title=f"{key}_人口_出生_死亡_{suffix} {yearsmonths[0]}~{yearsmonths[-1]}",
            sort=True,
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
            bars_left_axis=[
                "人口自然增加數_男",
                "人口自然增加數_女",
                "人口社會增加數_男",
                "人口社會增加數_女",
            ],
            title=f"{key}_人口_出生_死亡_男女_{suffix} {yearsmonths[0]}~{yearsmonths[-1]}",
            sort=True,
        )

        plots[f"{key}_自然增加率_{suffix}"] = plot_line(
            df_人口_出生_死亡[["人口自然增加率_合計", "人口自然增加率_男", "人口自然增加率_女"]],
            title=f"{key}_自然增加率_{suffix} {yearsmonths[0]}~{yearsmonths[-1]}",
            additional_layout={"yaxis": {"tickformat": ".2%"}},
        )
        plots[f"{key}_社會增加率_{suffix}"] = plot_line(
            df_人口_出生_死亡[["人口社會增加率_合計", "人口社會增加率_男", "人口社會增加率_女"]],
            title=f"{key}_社會增加率_{suffix} {yearsmonths[0]}~{yearsmonths[-1]}",
            additional_layout={"yaxis": {"tickformat": ".2%"}},
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
                "人口自然增加數_合計",
                "人口自然增加數_男",
                "人口自然增加數_女",
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
            df_人口_出生_死亡_縣市[("人口社會增加數_合計", region)] = (
                df_人口_出生_死亡_縣市[("人口數_合計", region)]
                - df_人口_出生_死亡_縣市[("人口數_合計", region)].shift(1, axis="index")
                - df_人口_出生_死亡_縣市[("人口自然增加數_合計", region)]
            )
            df_人口_出生_死亡_縣市[("人口社會增加數_男", region)] = (
                df_人口_出生_死亡_縣市[("人口數_男", region)]
                - df_人口_出生_死亡_縣市[("人口數_男", region)].shift(1, axis="index")
                - df_人口_出生_死亡_縣市[("人口自然增加數_男", region)]
            )
            df_人口_出生_死亡_縣市[("人口社會增加數_女", region)] = (
                df_人口_出生_死亡_縣市[("人口數_女", region)]
                - df_人口_出生_死亡_縣市[("人口數_女", region)].shift(1, axis="index")
                - df_人口_出生_死亡_縣市[("人口自然增加數_女", region)]
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
            df_人口_出生_死亡_縣市[("人口自然增加率_合計", region)] = (
                df_人口_出生_死亡_縣市[("人口自然增加數_合計", region)]
                / df_人口_出生_死亡_縣市[("人口數_合計", region)]
            )
            df_人口_出生_死亡_縣市[("人口自然增加率_男", region)] = (
                df_人口_出生_死亡_縣市[("人口自然增加數_男", region)]
                / df_人口_出生_死亡_縣市[("人口數_男", region)]
            )
            df_人口_出生_死亡_縣市[("人口自然增加率_女", region)] = (
                df_人口_出生_死亡_縣市[("人口自然增加數_女", region)]
                / df_人口_出生_死亡_縣市[("人口數_女", region)]
            )
            df_人口_出生_死亡_縣市[("人口社會增加率_合計", region)] = (
                df_人口_出生_死亡_縣市[("人口社會增加數_合計", region)]
                / df_人口_出生_死亡_縣市[("人口數_合計", region)]
            )
            df_人口_出生_死亡_縣市[("人口社會增加率_男", region)] = (
                df_人口_出生_死亡_縣市[("人口社會增加數_男", region)]
                / df_人口_出生_死亡_縣市[("人口數_男", region)]
            )
            df_人口_出生_死亡_縣市[("人口社會增加率_女", region)] = (
                df_人口_出生_死亡_縣市[("人口社會增加數_女", region)]
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
            bars_left_axis=sum(
                [
                    [
                        f"人口自然增加數_合計_{region}",
                        f"人口社會增加數_合計_{region}",
                    ]
                    for region in regions
                ],
                [],
            ),
            title=f"{key}_人口_出生_死亡_縣市_{suffix} {yearsmonths[0]}~{yearsmonths[-1]}",
            legendgroup=True,
            sort=True,
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
            bars_left_axis=sum(
                [
                    [
                        f"人口自然增加數_男_{region}",
                        f"人口自然增加數_女_{region}",
                        f"人口社會增加數_男_{region}",
                        f"人口社會增加數_女_{region}",
                    ]
                    for region in regions
                ],
                [],
            ),
            title=f"{key}_人口_出生_死亡_縣市_男女_{suffix} {yearsmonths[0]}~{yearsmonths[-1]}",
            legendgroup=True,
            sort=True,
        )
        plots[f"{key}_自然增加率_縣市_{suffix}"] = plot_lines_bars(
            df_人口_出生_死亡_縣市,
            lines_left_axis=sum(
                [
                    [
                        f"人口自然增加率_合計_{region}",
                        f"人口自然增加率_男_{region}",
                        f"人口自然增加率_女_{region}",
                    ]
                    for region in regions
                ],
                [],
            ),
            lines_right_axis=[],
            bars_left_axis=[],
            title=f"{key}_自然增加率_縣市_{suffix} {yearsmonths[0]}~{yearsmonths[-1]}",
            additional_layout={
                "yaxis": {"tickformat": ".2%"},
                "hovermode": "x",
            },
            legendgroup=True,
            sort=True,
        )
        plots[f"{key}_社會增加率_縣市_{suffix}"] = plot_lines_bars(
            df_人口_出生_死亡_縣市,
            lines_left_axis=sum(
                [
                    [
                        f"人口社會增加率_合計_{region}",
                        f"人口社會增加率_男_{region}",
                        f"人口社會增加率_女_{region}",
                    ]
                    for region in regions
                ],
                [],
            ),
            lines_right_axis=[],
            bars_left_axis=[],
            title=f"{key}_社會增加率_縣市_{suffix} {yearsmonths[0]}~{yearsmonths[-1]}",
            additional_layout={
                "yaxis": {"tickformat": ".2%"},
                "hovermode": "x",
            },
            legendgroup=True,
            sort=True,
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
            bars_left_axis=[],
            title=f"{key}_出生率_縣市_{suffix} {yearsmonths[0]}~{yearsmonths[-1]}",
            additional_layout={
                "yaxis": {"tickformat": ".2%"},
                "hovermode": "x",
            },
            legendgroup=True,
            sort=True,
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
            bars_left_axis=[],
            title=f"{key}_死亡率_縣市_{suffix} {yearsmonths[0]}~{yearsmonths[-1]}",
            additional_layout={
                "yaxis": {"tickformat": ".2%"},
                "hovermode": "x",
            },
            legendgroup=True,
            sort=True,
        )

        df_出生_結婚_離婚 = df.pivot_table(
            values=[
                "出生數_合計",
                "結婚對數_合計",
                "結婚對數_異性",
                "結婚對數_同性",
                "結婚對數_同性_男",
                "結婚對數_同性_女",
                "離婚對數_合計",
                "離婚對數_異性",
                "離婚對數_同性",
                "離婚對數_同性_男",
                "離婚對數_同性_女",
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
        df_出生_結婚_離婚["婚姻自然增加率_合計"] = (
            df_出生_結婚_離婚["婚姻自然增加數_合計"] / df_出生_結婚_離婚["人口數_合計"]
        )
        df_出生_結婚_離婚["粗結婚率_合計"] = (
            df_出生_結婚_離婚["結婚對數_合計"] / df_出生_結婚_離婚["人口數_合計"]
        )
        df_出生_結婚_離婚["粗結婚率_異性"] = (
            df_出生_結婚_離婚["結婚對數_異性"] / df_出生_結婚_離婚["人口數_合計"]
        )
        df_出生_結婚_離婚["粗結婚率_同性"] = (
            df_出生_結婚_離婚["結婚對數_同性"] / df_出生_結婚_離婚["人口數_合計"]
        )
        df_出生_結婚_離婚["粗結婚率_同性_男"] = (
            df_出生_結婚_離婚["結婚對數_同性_男"] / df_出生_結婚_離婚["人口數_合計"]
        )
        df_出生_結婚_離婚["粗結婚率_同性_女"] = (
            df_出生_結婚_離婚["結婚對數_同性_女"] / df_出生_結婚_離婚["人口數_合計"]
        )
        df_出生_結婚_離婚["粗離婚率_合計"] = (
            df_出生_結婚_離婚["離婚對數_合計"] / df_出生_結婚_離婚["人口數_合計"]
        )
        df_出生_結婚_離婚["粗離婚率_異性"] = (
            df_出生_結婚_離婚["離婚對數_異性"] / df_出生_結婚_離婚["人口數_合計"]
        )
        df_出生_結婚_離婚["粗離婚率_同性"] = (
            df_出生_結婚_離婚["離婚對數_同性"] / df_出生_結婚_離婚["人口數_合計"]
        )
        df_出生_結婚_離婚["粗離婚率_同性_男"] = (
            df_出生_結婚_離婚["離婚對數_同性_男"] / df_出生_結婚_離婚["人口數_合計"]
        )
        df_出生_結婚_離婚["粗離婚率_同性_女"] = (
            df_出生_結婚_離婚["離婚對數_同性_女"] / df_出生_結婚_離婚["人口數_合計"]
        )

        plots[f"{key}_出生_結婚_離婚_{suffix}"] = plot_lines_bars(
            df_出生_結婚_離婚,
            lines_left_axis=[
                "結婚對數_合計",
                "結婚對數_異性",
                "結婚對數_同性",
                "結婚對數_同性_男",
                "結婚對數_同性_女",
                "離婚對數_合計",
                "離婚對數_異性",
                "離婚對數_同性",
                "離婚對數_同性_男",
                "離婚對數_同性_女",
            ],
            lines_right_axis=["出生數_合計"],
            bars_left_axis=["婚姻自然增加數_合計"],
            title=f"{key}_出生_結婚_離婚_{suffix} {yearsmonths[0]}~{yearsmonths[-1]}",
            sort=True,
        )
        plots[f"{key}_婚姻自然增加率_{suffix}"] = plot_line(
            df_出生_結婚_離婚[["婚姻自然增加率_合計"]],
            title=f"{key}_婚姻自然增加率_{suffix} {yearsmonths[0]}~{yearsmonths[-1]}",
            additional_layout={"yaxis": {"tickformat": ".2%"}},
        )
        plots[f"{key}_粗結婚率_{suffix}"] = plot_line(
            df_出生_結婚_離婚[
                [
                    "粗結婚率_合計",
                    "粗結婚率_異性",
                    "粗結婚率_同性",
                    "粗結婚率_同性_男",
                    "粗結婚率_同性_女",
                ]
            ],
            title=f"{key}_粗結婚率_{suffix} {yearsmonths[0]}~{yearsmonths[-1]}",
            additional_layout={"yaxis": {"tickformat": ".2%"}},
        )
        plots[f"{key}_粗離婚率_{suffix}"] = plot_line(
            df_出生_結婚_離婚[
                [
                    "粗離婚率_合計",
                    "粗離婚率_異性",
                    "粗離婚率_同性",
                    "粗離婚率_同性_男",
                    "粗離婚率_同性_女",
                ]
            ],
            title=f"{key}_粗離婚率_{suffix} {yearsmonths[0]}~{yearsmonths[-1]}",
            additional_layout={"yaxis": {"tickformat": ".2%"}},
        )

        df_出生_結婚_離婚_縣市 = df.pivot_table(
            values=[
                "出生數_合計",
                "結婚對數_合計",
                "結婚對數_異性",
                "結婚對數_同性",
                "結婚對數_同性_男",
                "結婚對數_同性_女",
                "離婚對數_合計",
                "離婚對數_異性",
                "離婚對數_同性",
                "離婚對數_同性_男",
                "離婚對數_同性_女",
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
            df_出生_結婚_離婚_縣市 = df_出生_結婚_離婚_縣市.copy()
            df_出生_結婚_離婚_縣市[("婚姻自然增加數_合計", region)] = (
                df_出生_結婚_離婚_縣市[("結婚對數_合計", region)]
                - df_出生_結婚_離婚_縣市[("離婚對數_合計", region)]
            )
            df_出生_結婚_離婚_縣市[("婚姻自然增加率_合計", region)] = (
                df_出生_結婚_離婚_縣市[("婚姻自然增加數_合計", region)]
                / df_出生_結婚_離婚_縣市[("人口數_合計", region)]
            )
            df_出生_結婚_離婚_縣市[("粗結婚率_合計", region)] = (
                df_出生_結婚_離婚_縣市[("結婚對數_合計", region)]
                / df_出生_結婚_離婚_縣市[("人口數_合計", region)]
            )
            df_出生_結婚_離婚_縣市[("粗結婚率_異性", region)] = (
                df_出生_結婚_離婚_縣市[("結婚對數_異性", region)]
                / df_出生_結婚_離婚_縣市[("人口數_合計", region)]
            )
            df_出生_結婚_離婚_縣市[("粗結婚率_同性", region)] = (
                df_出生_結婚_離婚_縣市[("結婚對數_同性", region)]
                / df_出生_結婚_離婚_縣市[("人口數_合計", region)]
            )
            df_出生_結婚_離婚_縣市[("粗結婚率_同性_男", region)] = (
                df_出生_結婚_離婚_縣市[("結婚對數_同性_男", region)]
                / df_出生_結婚_離婚_縣市[("人口數_合計", region)]
            )
            df_出生_結婚_離婚_縣市[("粗結婚率_同性_女", region)] = (
                df_出生_結婚_離婚_縣市[("結婚對數_同性_女", region)]
                / df_出生_結婚_離婚_縣市[("人口數_合計", region)]
            )
            df_出生_結婚_離婚_縣市[("粗離婚率_合計", region)] = (
                df_出生_結婚_離婚_縣市[("離婚對數_合計", region)]
                / df_出生_結婚_離婚_縣市[("人口數_合計", region)]
            )
            df_出生_結婚_離婚_縣市[("粗離婚率_異性", region)] = (
                df_出生_結婚_離婚_縣市[("離婚對數_異性", region)]
                / df_出生_結婚_離婚_縣市[("人口數_合計", region)]
            )
            df_出生_結婚_離婚_縣市[("粗離婚率_同性", region)] = (
                df_出生_結婚_離婚_縣市[("離婚對數_同性", region)]
                / df_出生_結婚_離婚_縣市[("人口數_合計", region)]
            )
            df_出生_結婚_離婚_縣市[("粗離婚率_同性_男", region)] = (
                df_出生_結婚_離婚_縣市[("離婚對數_同性_男", region)]
                / df_出生_結婚_離婚_縣市[("人口數_合計", region)]
            )
            df_出生_結婚_離婚_縣市[("粗離婚率_同性_女", region)] = (
                df_出生_結婚_離婚_縣市[("離婚對數_同性_女", region)]
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
                        f"結婚對數_異性_{region}",
                        f"結婚對數_同性_{region}",
                        f"結婚對數_同性_男_{region}",
                        f"結婚對數_同性_女_{region}",
                        f"離婚對數_合計_{region}",
                        f"離婚對數_異性_{region}",
                        f"離婚對數_同性_{region}",
                        f"離婚對數_同性_男_{region}",
                        f"離婚對數_同性_女_{region}",
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
            bars_left_axis=sum(
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
            sort=True,
        )
        plots[f"{key}_婚姻自然增加率_縣市_{suffix}"] = plot_lines_bars(
            df_出生_結婚_離婚_縣市,
            lines_left_axis=sum(
                [
                    [
                        f"婚姻自然增加率_合計_{region}",
                    ]
                    for region in regions
                ],
                [],
            ),
            lines_right_axis=[],
            bars_left_axis=[],
            title=f"{key}_婚姻自然增加率_縣市_{suffix} {yearsmonths[0]}~{yearsmonths[-1]}",
            additional_layout={
                "yaxis": {"tickformat": ".2%"},
                "hovermode": "x",
            },
            legendgroup=True,
            sort=True,
        )
        plots[f"{key}_粗結婚率_縣市_{suffix}"] = plot_lines_bars(
            df_出生_結婚_離婚_縣市,
            lines_left_axis=sum(
                [
                    [
                        f"粗結婚率_合計_{region}",
                        f"粗結婚率_合計_{region}",
                        f"粗結婚率_異性_{region}",
                        f"粗結婚率_同性_{region}",
                        f"粗結婚率_同性_男_{region}",
                        f"粗結婚率_同性_女_{region}",
                    ]
                    for region in regions
                ],
                [],
            ),
            lines_right_axis=[],
            bars_left_axis=[],
            title=f"{key}_粗結婚率_縣市_{suffix} {yearsmonths[0]}~{yearsmonths[-1]}",
            additional_layout={
                "yaxis": {"tickformat": ".2%"},
                "hovermode": "x",
            },
            legendgroup=True,
            sort=True,
        )
        plots[f"{key}_粗離婚率_縣市_{suffix}"] = plot_lines_bars(
            df_出生_結婚_離婚_縣市,
            lines_left_axis=sum(
                [
                    [
                        f"粗離婚率_合計_{region}",
                        f"粗離婚率_異性_{region}",
                        f"粗離婚率_同性_{region}",
                        f"粗離婚率_同性_男_{region}",
                        f"粗離婚率_同性_女_{region}",
                    ]
                    for region in regions
                ],
                [],
            ),
            lines_right_axis=[],
            bars_left_axis=[],
            title=f"{key}_粗離婚率_縣市_{suffix} {yearsmonths[0]}~{yearsmonths[-1]}",
            additional_layout={
                "yaxis": {"tickformat": ".2%"},
                "hovermode": "x",
            },
            legendgroup=True,
            sort=True,
        )

        df_total = df.pivot_table(
            values="婚姻自然增加數_合計", index=index, aggfunc="sum", sort=False
        )
        plots[f"{key}_總和_婚姻自然增加數_{suffix}"] = plot_line(
            df_total, f"{key}_總和_婚姻自然增加數_{suffix} {yearsmonths[0]}~{yearsmonths[-1]}"
        )
        df_區域別 = df.pivot_table(
            values="婚姻自然增加數_合計", index=index, columns="縣市", aggfunc="sum", sort=False
        )
        plots[f"{key}_區域別_婚姻自然增加數_{suffix}"] = plot_line(
            df_區域別, f"{key}_區域別_婚姻自然增加數_{suffix} {yearsmonths[0]}~{yearsmonths[-1]}"
        )

        df_total = df.pivot_table(
            values=[
                "結婚對數_合計",
                "結婚對數_異性",
                "結婚對數_同性",
                "結婚對數_同性_男",
                "結婚對數_同性_女",
            ],
            index=index,
            aggfunc="sum",
            sort=False,
        )
        plots[f"{key}_總和_結婚對數_{suffix}"] = plot_line(
            df_total, f"{key}_總和_結婚對數_{suffix} {yearsmonths[0]}~{yearsmonths[-1]}"
        )
        df_區域別 = df.pivot_table(
            values=[
                "結婚對數_合計",
                "結婚對數_異性",
                "結婚對數_同性",
                "結婚對數_同性_男",
                "結婚對數_同性_女",
            ],
            index=index,
            columns="縣市",
            aggfunc="sum",
            sort=False,
        )
        df_區域別.columns = [f"{類別}_{縣市}" for 類別, 縣市 in df_區域別.columns]
        plots[f"{key}_區域別_結婚對數_{suffix}"] = plot_line(
            df_區域別, f"{key}_區域別_結婚對數_{suffix} {yearsmonths[0]}~{yearsmonths[-1]}"
        )

        df_total = df.pivot_table(
            values=[
                "離婚對數_合計",
                "離婚對數_異性",
                "離婚對數_同性",
                "離婚對數_同性_男",
                "離婚對數_同性_女",
            ],
            index=index,
            aggfunc="sum",
            sort=False,
        )
        plots[f"{key}_總和_離婚對數_{suffix}"] = plot_line(
            df_total, f"{key}_總和_離婚對數_{suffix} {yearsmonths[0]}~{yearsmonths[-1]}"
        )
        df_區域別 = df.pivot_table(
            values=[
                "離婚對數_合計",
                "離婚對數_異性",
                "離婚對數_同性",
                "離婚對數_同性_男",
                "離婚對數_同性_女",
            ],
            index=index,
            columns="縣市",
            aggfunc="sum",
            sort=False,
        )
        df_區域別.columns = [f"{類別}_{縣市}" for 類別, 縣市 in df_區域別.columns]
        plots[f"{key}_區域別_離婚對數_{suffix}"] = plot_line(
            df_區域別, f"{key}_區域別_離婚對數_{suffix} {yearsmonths[0]}~{yearsmonths[-1]}"
        )


def plot_結婚人數按婚姻類型_性別_年齡_原屬國籍_地區_及教育程度分_按登記_(plots):
    key = "結婚人數按婚姻類型、性別、年齡、原屬國籍（地區）及教育程度分(按登記)"
    key = sanitize_filename(key)
    df = df_結婚人數按婚姻類型_性別_年齡_原屬國籍_地區_及教育程度分_按登記()

    years = df["year"].unique().tolist()

    df_現住人口 = df_現住人口性別_年齡_婚姻狀況()
    df_現住人口 = df_現住人口[df_現住人口["statistic_yyy"].isin(years)]

    df_total = df.pivot_table(values="number_of_marry", index="year", aggfunc="sum", sort=False)
    plots[f"{key}_總和"] = plot_line(
        df_total, f"{key}_總和 {df_total.index[0]}~{df_total.index[-1]}年"
    )

    df_單身人數 = df_現住人口.pivot_table(
        values="population",
        index="statistic_yyy",
        columns=["marital_status", "sex", "age"],
        aggfunc="sum",
        sort=True,
    )
    cols = [col for col in df_單身人數.columns if col[2] != "未滿15歲" and col[2] != "15~19歲"]
    df_單身滿20人數 = df_單身人數.loc[:, cols].T.groupby(level=["marital_status", "sex"]).sum().T
    df_結婚人數 = df.pivot_table(
        values="number_of_marry",
        index="year",
        columns=["marriage_type", "sex"],
        aggfunc="sum",
        sort=True,
    )

    df_結婚率 = pd.DataFrame()
    df_結婚率["結婚人數_不同性別"] = df_結婚人數.loc[:, ("不同性別",)].sum(axis=1)
    df_結婚率["單身人數_不同性別"] = df_單身滿20人數.loc[
        :, (["未婚", "離婚_不同性別", "喪偶_不同性別"],)
    ].sum(axis=1)
    df_結婚率["結婚率_不同性別"] = df_結婚率["結婚人數_不同性別"] / df_結婚率["單身人數_不同性別"]
    df_結婚率["結婚人數_相同性別"] = df_結婚人數.loc[:, ("相同性別",)].sum(axis=1)
    df_結婚率["單身人數_相同性別"] = df_單身滿20人數.loc[
        :, (["未婚", "離婚_相同性別", "喪偶_相同性別"],)
    ].sum(axis=1)
    df_結婚率["結婚率_相同性別"] = df_結婚率["結婚人數_相同性別"] / df_結婚率["單身人數_相同性別"]
    df_結婚率["結婚人數_相同性別_男"] = df_結婚人數.loc[:, ("相同性別", "男")]
    df_結婚率["單身人數_相同性別_男"] = df_單身滿20人數.loc[
        :, (["未婚", "離婚_相同性別", "喪偶_相同性別"], "男")
    ].sum(axis=1)
    df_結婚率["結婚率_相同性別_男"] = (
        df_結婚率["結婚人數_相同性別_男"] / df_結婚率["單身人數_相同性別_男"]
    )
    df_結婚率["結婚人數_相同性別_女"] = df_結婚人數.loc[:, ("相同性別", "女")]
    df_結婚率["單身人數_相同性別_女"] = df_單身滿20人數.loc[
        :, (["未婚", "離婚_相同性別", "喪偶_相同性別"], "女")
    ].sum(axis=1)
    df_結婚率["結婚率_相同性別_女"] = (
        df_結婚率["結婚人數_相同性別_女"] / df_結婚率["單身人數_相同性別_女"]
    )

    plots[f"{key}_結婚率"] = plot_line(
        df_結婚率.filter(like="結婚率", axis="columns"),
        f"{key}_結婚率 {df_結婚率.index[0]}~{df_結婚率.index[-1]}年",
        additional_layout={"yaxis": {"tickformat": ".2%"}},
    )

    df_區域別 = df.pivot_table(
        values="number_of_marry", index="year", columns="縣市", aggfunc="sum", sort=False
    )
    plots[f"{key}_區域別"] = plot_line(
        df_區域別, f"{key}_區域別 {df_區域別.index[0]}~{df_區域別.index[-1]}年"
    )

    df_區域別_單身人數 = df_現住人口.pivot_table(
        values="population",
        index="statistic_yyy",
        columns=["縣市", "marital_status", "sex", "age"],
        aggfunc="sum",
        sort=True,
    )
    cols = [
        col for col in df_區域別_單身人數.columns if col[3] != "未滿15歲" and col[3] != "15~19歲"
    ]
    df_區域別_單身滿20人數 = (
        df_區域別_單身人數.loc[:, cols].T.groupby(level=["縣市", "marital_status", "sex"]).sum().T
    )
    df_區域別_結婚人數 = df.pivot_table(
        values="number_of_marry",
        index="year",
        columns=["縣市", "marriage_type", "sex"],
        aggfunc="sum",
        sort=True,
    )

    regions = df["縣市"].unique().tolist()
    df_區域別_結婚率 = pd.DataFrame()
    for region in regions:
        df_區域別_結婚率[f"{region}_結婚人數_不同性別"] = df_區域別_結婚人數.loc[
            :,
            (
                region,
                "不同性別",
                slice(None),
            ),
        ].sum(axis=1)
        df_區域別_結婚率[f"{region}_單身人數_不同性別"] = df_區域別_單身滿20人數.loc[
            :,
            (
                region,
                ["未婚", "離婚_不同性別", "喪偶_不同性別"],
                slice(None),
            ),
        ].sum(axis=1)
        df_區域別_結婚率[f"{region}_結婚率_不同性別"] = (
            df_區域別_結婚率[f"{region}_結婚人數_不同性別"]
            / df_區域別_結婚率[f"{region}_單身人數_不同性別"]
        )
        df_區域別_結婚率[f"{region}_結婚人數_相同性別"] = df_區域別_結婚人數.loc[
            :,
            (
                region,
                "相同性別",
                slice(None),
            ),
        ].sum(axis=1)
        df_區域別_結婚率[f"{region}_單身人數_相同性別"] = df_區域別_單身滿20人數.loc[
            :,
            (
                region,
                ["未婚", "離婚_相同性別", "喪偶_相同性別"],
                slice(None),
            ),
        ].sum(axis=1)
        df_區域別_結婚率[f"{region}_結婚率_相同性別"] = (
            df_區域別_結婚率[f"{region}_結婚人數_相同性別"]
            / df_區域別_結婚率[f"{region}_單身人數_相同性別"]
        )
        df_區域別_結婚率[f"{region}_結婚人數_相同性別_男"] = df_區域別_結婚人數.loc[
            :, (region, "相同性別", "男")
        ]
        df_區域別_結婚率[f"{region}_單身人數_相同性別_男"] = df_區域別_單身滿20人數.loc[
            :, (region, ["未婚", "離婚_相同性別", "喪偶_相同性別"], "男")
        ].sum(axis=1)
        df_區域別_結婚率[f"{region}_結婚率_相同性別_男"] = (
            df_區域別_結婚率[f"{region}_結婚人數_相同性別_男"]
            / df_區域別_結婚率[f"{region}_單身人數_相同性別_男"]
        )
        df_區域別_結婚率[f"{region}_結婚人數_相同性別_女"] = df_區域別_結婚人數.loc[
            :, (region, "相同性別", "女")
        ]
        df_區域別_結婚率[f"{region}_單身人數_相同性別_女"] = df_區域別_單身滿20人數.loc[
            :, (region, ["未婚", "離婚_相同性別", "喪偶_相同性別"], "女")
        ].sum(axis=1)
        df_區域別_結婚率[f"{region}_結婚率_相同性別_女"] = (
            df_區域別_結婚率[f"{region}_結婚人數_相同性別_女"]
            / df_區域別_結婚率[f"{region}_單身人數_相同性別_女"]
        )

        df_區域別_結婚率 = df_區域別_結婚率.copy()

    plots[f"{key}_區域別_結婚率"] = plot_line(
        df_區域別_結婚率.filter(like="結婚率", axis="columns"),
        f"{key}_區域別_結婚率 {df_區域別_結婚率.index[0]}~{df_區域別_結婚率.index[-1]}年",
        additional_layout={"yaxis": {"tickformat": ".2%"}},
    )

    df_婚姻類型 = df.pivot_table(
        values="number_of_marry",
        index="year",
        columns=["marriage_type", "sex"],
        aggfunc="sum",
        sort=False,
    )
    df_婚姻類型.columns = [f"{類型}_{性別}" for 類型, 性別 in df_婚姻類型.columns]
    plots[f"{key}_婚姻類型"] = plot_line(
        df_婚姻類型, f"{key}_婚姻類型 {df_婚姻類型.index[0]}~{df_婚姻類型.index[-1]}年"
    )

    df_性別 = df.pivot_table(
        values="number_of_marry", index="year", columns="sex", aggfunc="sum", sort=False
    )
    plots[f"{key}_性別"] = plot_line(
        df_性別, f"{key}_性別 {df_性別.index[0]}~{df_性別.index[-1]}年"
    )

    df_原屬國籍 = df.pivot_table(
        values="number_of_marry", index="year", columns="nation", aggfunc="sum", sort=False
    )
    plots[f"{key}_原屬國籍"] = plot_line(
        df_原屬國籍, f"{key}_原屬國籍 {df_原屬國籍.index[0]}~{df_原屬國籍.index[-1]}年"
    )

    df_教育程度 = df.pivot_table(
        values="number_of_marry", index="year", columns="edu", aggfunc="sum", sort=False
    )
    plots[f"{key}_教育程度"] = plot_line(
        df_教育程度, f"{key}_教育程度 {df_教育程度.index[0]}~{df_教育程度.index[-1]}年"
    )

    df_年齡 = df.pivot_table(
        values="number_of_marry", index="year", columns="age", aggfunc="sum", sort=False
    )
    plots[f"{key}_年齡"] = plot_line(
        df_年齡, f"{key}_年齡 {df_年齡.index[0]}~{df_年齡.index[-1]}年"
    )

    df_女_年齡_縣市 = df[df["sex"] == "女"].pivot_table(
        values="number_of_marry", index="age", columns="縣市", aggfunc="sum", sort=False
    )
    plots[f"{key}_女_年齡_縣市"] = plot_line(
        df_女_年齡_縣市, f"{key}_女_年齡_縣市 {years[0]}~{years[-1]}年"
    )

    df_男_年齡_縣市 = df[df["sex"] == "男"].pivot_table(
        values="number_of_marry", index="age", columns="縣市", aggfunc="sum", sort=False
    )
    plots[f"{key}_男_年齡_縣市"] = plot_line(
        df_男_年齡_縣市, f"{key}_男_年齡_縣市 {years[0]}~{years[-1]}年"
    )

    df_女_年齡_教育 = df[df["sex"] == "女"].pivot_table(
        values="number_of_marry",
        index="age",
        columns=["edu", "marriage_type"],
        aggfunc="sum",
        sort=False,
    )
    df_女_年齡_教育.columns = ["_".join(col) for col in df_女_年齡_教育.columns]
    plots[f"{key}_女_年齡_教育"] = plot_line(
        df_女_年齡_教育, f"{key}_女_年齡_教育 {years[0]}~{years[-1]}年"
    )

    df_男_年齡_教育 = df[df["sex"] == "男"].pivot_table(
        values="number_of_marry",
        index="age",
        columns=["edu", "marriage_type"],
        aggfunc="sum",
        sort=False,
    )
    df_男_年齡_教育.columns = ["_".join(col) for col in df_男_年齡_教育.columns]
    plots[f"{key}_男_年齡_教育"] = plot_line(
        df_男_年齡_教育, f"{key}_男_年齡_教育 {years[0]}~{years[-1]}年"
    )

    df_男女_年齡_婚姻類型 = df.pivot_table(
        values="number_of_marry",
        index="age",
        columns=["sex", "marriage_type"],
        aggfunc="sum",
        sort=False,
    )
    plots[f"{key}_男女_年齡_婚姻類型"] = plot_pyramid(
        f"{key}_男女_年齡_婚姻類型 {years[0]}~{years[-1]}年",
        df_男女_年齡_婚姻類型["男"],
        df_男女_年齡_婚姻類型["女"],
        df_男女_年齡_婚姻類型["男"].columns,
        df_男女_年齡_婚姻類型["男"].index,
    )


def plot_結婚對數按婚姻類型_性別及年齡分_按登記_(plots, items):
    key = "結婚對數按婚姻類型、性別及年齡分(按登記)"
    key = sanitize_filename(key)
    df = df_結婚對數按婚姻類型_性別及年齡分_按登記()

    years = df["統計年度"].unique().tolist()
    kinds = df["婚姻類型"].unique().tolist()

    items[key] = kinds
    for kind in kinds:
        df_女 = df.pivot_table(
            values="結婚對數",
            index="女方年齡或配偶一方年齡",
            columns=["婚姻類型", "男方年齡或配偶另一方年齡"],
            aggfunc="sum",
            sort=False,
        )
        plots[f"{key}_{kind}_登記為女方"] = plot_line(
            df_女.loc[:, (kind,)],
            f"{key}_{kind}_登記為女方 {years[0]}~{years[-1]}年",
            {"xaxis": {"title": {"text": "女方年齡或配偶一方年齡"}}},
        )

        df_男 = df.pivot_table(
            values="結婚對數",
            index="男方年齡或配偶另一方年齡",
            columns=["婚姻類型", "女方年齡或配偶一方年齡"],
            aggfunc="sum",
            sort=False,
        )
        plots[f"{key}_{kind}_登記為男方"] = plot_line(
            df_男.loc[:, (kind,)],
            f"{key}_{kind}_登記為男方 {years[0]}~{years[-1]}年",
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
        f"{key}_女_縣市 {years[0]}~{years[-1]}年",
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
        f"{key}_男_縣市 {years[0]}~{years[-1]}年",
        {"xaxis": {"title": {"text": "男方年齡或配偶另一方年齡"}}},
    )


def plot_嬰兒出生數按性別_生母原屬國籍_地區__年齡及教育程度分_按登記_(plots):
    key = "嬰兒出生數按性別、生母原屬國籍（地區）、年齡及教育程度分(按登記)"
    key = sanitize_filename(key)
    df = df_嬰兒出生數按性別_生母原屬國籍_地區_年齡及教育程度分_按登記()

    years = df["statistic_yyy"].unique().tolist()

    df_total = df.pivot_table(
        values="birth_count", index="statistic_yyy", aggfunc="sum", sort=False
    )
    plots[f"{key}_總和"] = plot_line(
        df_total, f"{key}_總和 {df_total.index[0]}~{df_total.index[-1]}年"
    )

    df_區域別 = df.pivot_table(
        values="birth_count", index="statistic_yyy", columns="縣市", aggfunc="sum", sort=False
    )
    plots[f"{key}_區域別"] = plot_line(
        df_區域別, f"{key}_區域別 {df_區域別.index[0]}~{df_區域別.index[-1]}年"
    )

    df_性別 = df.pivot_table(
        values="birth_count", index="statistic_yyy", columns="birth_sex", aggfunc="sum", sort=False
    )
    plots[f"{key}_性別"] = plot_line(
        df_性別, f"{key}_性別 {df_性別.index[0]}~{df_性別.index[-1]}年"
    )

    df_原屬國籍 = df.pivot_table(
        values="birth_count",
        index="statistic_yyy",
        columns="mother_nation",
        aggfunc="sum",
        sort=False,
    )
    plots[f"{key}_原屬國籍"] = plot_line(
        df_原屬國籍, f"{key}_原屬國籍 {df_原屬國籍.index[0]}~{df_原屬國籍.index[-1]}年"
    )

    df_教育程度 = df.pivot_table(
        values="birth_count",
        index="statistic_yyy",
        columns="mother_education",
        aggfunc="sum",
        sort=False,
    )
    plots[f"{key}_教育程度"] = plot_line(
        df_教育程度, f"{key}_教育程度 {df_教育程度.index[0]}~{df_教育程度.index[-1]}年"
    )

    df_年齡 = df.pivot_table(
        values="birth_count", index="statistic_yyy", columns="mother_age", aggfunc="sum", sort=False
    )
    plots[f"{key}_年齡"] = plot_line(
        df_年齡, f"{key}_年齡 {df_年齡.index[0]}~{df_年齡.index[-1]}年"
    )

    df_年齡_縣市 = df.pivot_table(
        values="birth_count", index="mother_age", columns="縣市", aggfunc="sum", sort=False
    )
    plots[f"{key}_年齡_縣市"] = plot_line(df_年齡_縣市, f"{key}_年齡_縣市 {years[0]}~{years[-1]}年")

    df_年齡_教育 = df.pivot_table(
        values="birth_count",
        index="mother_age",
        columns="mother_education",
        aggfunc="sum",
        sort=False,
    )
    plots[f"{key}_年齡_教育"] = plot_line(df_年齡_教育, f"{key}_年齡_教育 {years[0]}~{years[-1]}年")


def plot_嬰兒出生數按性別_生父原屬國籍_地區__年齡及教育程度分_按登記_(plots):
    key = "嬰兒出生數按性別、生父原屬國籍（地區）、年齡及教育程度分(按登記)"
    key = sanitize_filename(key)
    df = df_嬰兒出生數按性別_生父原屬國籍_地區_年齡及教育程度分_按登記()

    years = df["統計年度"].unique().tolist()

    df_total = df.pivot_table(values="嬰兒出生數", index="統計年度", aggfunc="sum", sort=False)
    plots[f"{key}_總和"] = plot_line(
        df_total, f"{key}_總和 {df_total.index[0]}~{df_total.index[-1]}年"
    )

    df_區域別 = df.pivot_table(
        values="嬰兒出生數", index="統計年度", columns="縣市", aggfunc="sum", sort=False
    )
    plots[f"{key}_區域別"] = plot_line(
        df_區域別, f"{key}_區域別 {df_區域別.index[0]}~{df_區域別.index[-1]}年"
    )

    df_性別 = df.pivot_table(
        values="嬰兒出生數", index="統計年度", columns="出生者性別", aggfunc="sum", sort=False
    )
    plots[f"{key}_性別"] = plot_line(
        df_性別, f"{key}_性別 {df_性別.index[0]}~{df_性別.index[-1]}年"
    )

    df_原屬國籍 = df.pivot_table(
        values="嬰兒出生數",
        index="統計年度",
        columns="生父原屬國籍或地區",
        aggfunc="sum",
        sort=False,
    )
    plots[f"{key}_原屬國籍"] = plot_line(
        df_原屬國籍, f"{key}_原屬國籍 {df_原屬國籍.index[0]}~{df_原屬國籍.index[-1]}年"
    )

    df_教育程度 = df.pivot_table(
        values="嬰兒出生數",
        index="統計年度",
        columns="生父教育程度",
        aggfunc="sum",
        sort=False,
    )
    plots[f"{key}_教育程度"] = plot_line(
        df_教育程度, f"{key}_教育程度 {df_教育程度.index[0]}~{df_教育程度.index[-1]}年"
    )

    df_年齡 = df.pivot_table(
        values="嬰兒出生數", index="統計年度", columns="生父年齡", aggfunc="sum", sort=False
    )
    plots[f"{key}_年齡"] = plot_line(
        df_年齡, f"{key}_年齡 {df_年齡.index[0]}~{df_年齡.index[-1]}年"
    )

    df_年齡_縣市 = df.pivot_table(
        values="嬰兒出生數", index="生父年齡", columns="縣市", aggfunc="sum", sort=False
    )
    plots[f"{key}_年齡_縣市"] = plot_line(df_年齡_縣市, f"{key}_年齡_縣市 {years[0]}~{years[-1]}年")

    df_年齡_教育 = df.pivot_table(
        values="嬰兒出生數",
        index="生父年齡",
        columns="生父教育程度",
        aggfunc="sum",
        sort=False,
    )
    plots[f"{key}_年齡_教育"] = plot_line(df_年齡_教育, f"{key}_年齡_教育 {years[0]}~{years[-1]}年")


def plot_嬰兒出生數按嬰兒性別及生父母年齡分_按登記_(plots):
    key = "嬰兒出生數按嬰兒性別及生父母年齡分(按登記)"
    key = sanitize_filename(key)
    df = df_嬰兒出生數按嬰兒性別及生父母年齡分_按登記()

    years = df["statistic_yyy"].unique().tolist()

    df_total = df.pivot_table(
        values="birth_count", index="statistic_yyy", aggfunc="sum", sort=False
    )
    plots[f"{key}_總和"] = plot_line(
        df_total, f"{key}_總和 {df_total.index[0]}~{df_total.index[-1]}年"
    )

    df_區域別 = df.pivot_table(
        values="birth_count", index="statistic_yyy", columns="縣市", aggfunc="sum", sort=False
    )
    plots[f"{key}_區域別"] = plot_line(
        df_區域別, f"{key}_區域別 {df_區域別.index[0]}~{df_區域別.index[-1]}年"
    )

    df_性別 = df.pivot_table(
        values="birth_count", index="statistic_yyy", columns="sex", aggfunc="sum", sort=False
    )
    plots[f"{key}_性別"] = plot_line(
        df_性別, f"{key}_性別 {df_性別.index[0]}~{df_性別.index[-1]}年"
    )

    df_生父年齡 = df.pivot_table(
        values="birth_count",
        index="statistic_yyy",
        columns="father_age",
        aggfunc="sum",
        sort=False,
    )
    plots[f"{key}_生父年齡"] = plot_line(
        df_生父年齡, f"{key}_生父年齡 {df_生父年齡.index[0]}~{df_生父年齡.index[-1]}年"
    )

    df_生母年齡 = df.pivot_table(
        values="birth_count", index="statistic_yyy", columns="mother_age", aggfunc="sum", sort=False
    )
    plots[f"{key}_生母年齡"] = plot_line(
        df_生母年齡, f"{key}_生母年齡 {df_生母年齡.index[0]}~{df_生母年齡.index[-1]}年"
    )

    df_生母年齡_縣市 = df.pivot_table(
        values="birth_count", index="mother_age", columns="縣市", aggfunc="sum", sort=False
    )
    plots[f"{key}_生母年齡_縣市"] = plot_line(
        df_生母年齡_縣市, f"{key}_生母年齡_縣市 {years[0]}~{years[-1]}年"
    )

    df_生父年齡_縣市 = df.pivot_table(
        values="birth_count", index="father_age", columns="縣市", aggfunc="sum", sort=False
    )
    plots[f"{key}_生父年齡_縣市"] = plot_line(
        df_生父年齡_縣市, f"{key}_生父年齡_縣市 {years[0]}~{years[-1]}年"
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
        f"{key}_生母年齡_生父年齡 {years[0]}~{years[-1]}年",
        {"xaxis": {"title": {"text": "生母年齡"}}},
    )


def plot_嬰兒出生數按生母年齡及出生身分分_按登記_(plots):
    key = "嬰兒出生數按生母年齡及出生身分分(按登記)"
    key = sanitize_filename(key)
    df = df_嬰兒出生數按生母年齡及出生身分分_按登記()

    years = df["統計年度"].unique().tolist()

    df_total = df.pivot_table(values="嬰兒出生數", index="統計年度", aggfunc="sum", sort=False)
    plots[f"{key}_總和"] = plot_line(
        df_total, f"{key}_總和 {df_total.index[0]}~{df_total.index[-1]}年"
    )

    df_區域別 = df.pivot_table(
        values="嬰兒出生數", index="統計年度", columns="縣市", aggfunc="sum", sort=False
    )
    plots[f"{key}_區域別"] = plot_line(
        df_區域別, f"{key}_區域別 {df_區域別.index[0]}~{df_區域別.index[-1]}年"
    )

    df_身分 = df.pivot_table(
        values="嬰兒出生數", index="統計年度", columns="出生身分", aggfunc="sum", sort=False
    )
    plots[f"{key}_身分"] = plot_line(
        df_身分, f"{key}_身分 {df_身分.index[0]}~{df_身分.index[-1]}年"
    )

    df_年齡 = df.pivot_table(
        values="嬰兒出生數", index="統計年度", columns="生母年齡", aggfunc="sum", sort=False
    )
    plots[f"{key}_年齡"] = plot_line(
        df_年齡, f"{key}_年齡 {df_年齡.index[0]}~{df_年齡.index[-1]}年"
    )

    df_年齡_身分 = df.pivot_table(
        values="嬰兒出生數", index="生母年齡", columns="出生身分", aggfunc="sum", sort=False
    )
    plots[f"{key}_年齡_身分"] = plot_line(df_年齡_身分, f"{key}_年齡_身分 {years[0]}~{years[-1]}年")

    df_身分_縣市 = df.pivot_table(
        values="嬰兒出生數", index="出生身分", columns="縣市", aggfunc="sum", sort=False
    )
    plots[f"{key}_身分_縣市"] = plot_bar_group(
        df_身分_縣市, f"{key}_身分_縣市 {years[0]}~{years[-1]}年"
    )


def plot_嬰兒出生數按性別_胎次及生母年齡分_按登記_(plots):
    key = "嬰兒出生數按性別、胎次及生母年齡分(按登記)"
    key = sanitize_filename(key)
    df = df_嬰兒出生數按性別_胎次及生母年齡分_按登記()

    years = df["統計年度"].unique().tolist()

    df_total = df.pivot_table(values="嬰兒出生數", index="統計年度", aggfunc="sum", sort=False)
    plots[f"{key}_總和"] = plot_line(
        df_total, f"{key}_總和 {df_total.index[0]}~{df_total.index[-1]}年"
    )

    df_區域別 = df.pivot_table(
        values="嬰兒出生數", index="統計年度", columns="縣市", aggfunc="sum", sort=False
    )
    plots[f"{key}_區域別"] = plot_line(
        df_區域別, f"{key}_區域別 {df_區域別.index[0]}~{df_區域別.index[-1]}年"
    )

    df_性別 = df.pivot_table(
        values="嬰兒出生數", index="統計年度", columns="出生者性別", aggfunc="sum", sort=False
    )
    plots[f"{key}_性別"] = plot_line(
        df_性別, f"{key}_性別 {df_性別.index[0]}~{df_性別.index[-1]}年"
    )

    df_胎次 = df.pivot_table(
        values="嬰兒出生數", index="統計年度", columns="胎次", aggfunc="sum", sort=False
    )
    plots[f"{key}_胎次"] = plot_line(
        df_胎次, f"{key}_胎次 {df_胎次.index[0]}~{df_胎次.index[-1]}年"
    )

    df_年齡 = df.pivot_table(
        values="嬰兒出生數", index="統計年度", columns="生母年齡", aggfunc="sum", sort=False
    )
    plots[f"{key}_年齡"] = plot_line(
        df_年齡, f"{key}_年齡 {df_年齡.index[0]}~{df_年齡.index[-1]}年"
    )

    df_年齡_胎次 = df.pivot_table(
        values="嬰兒出生數", index="生母年齡", columns="胎次", aggfunc="sum", sort=False
    )
    plots[f"{key}_年齡_胎次"] = plot_line(df_年齡_胎次, f"{key}_年齡_胎次 {years[0]}~{years[-1]}年")

    df_胎次_縣市 = df.pivot_table(
        values="嬰兒出生數", index="胎次", columns="縣市", aggfunc="sum", sort=False
    )
    plots[f"{key}_胎次_縣市"] = plot_bar_group(
        df_胎次_縣市, f"{key}_胎次_縣市 {years[0]}~{years[-1]}年"
    )


def plot_嬰兒胎次_vs_綜稅綜合所得總額(plots):
    key = "嬰兒胎次 vs 綜稅綜合所得總額"
    key = sanitize_filename(key)

    df_胎次 = df_嬰兒出生數按性別_胎次及生母年齡分_按登記()
    df_所得總額 = df_綜稅綜合所得總額全國各縣市鄉鎮村里統計分析表()
    df_所得總額 = df_所得總額.rename(columns={"縣市鄉鎮": "區域別", "年度": "統計年度"})
    df_所得總額 = df_所得總額.pivot_table(
        values=["綜合所得總額", "納稅單位(戶)"],
        index=["區域別", "統計年度"],
        aggfunc="sum",
        sort=False,
    ).reset_index()
    df_所得總額["平均數"] = df_所得總額["綜合所得總額"] / df_所得總額["納稅單位(戶)"]

    df = pd.merge(
        df_胎次,
        df_所得總額,
        how="outer",
        suffixes=["_胎次", "_所得總額"],
        on=[
            "統計年度",
            "區域別",
        ],
    )
    split = df["區域別"].str.replace("(^.{3})", r"\1|", regex=True).str.split("|", n=1, expand=True)
    df["縣市"] = split[0].str.strip()
    df["鄉鎮"] = split[1].str.strip()
    df = df[~df["按照別"].isna()]
    df = df[~df["綜合所得總額"].isna()]
    df["所得平均數區間"] = pd.cut(
        df["平均數"] / 10000, [0] + list(range(50, 201, 10)) + [300, np.inf], right=False
    )

    years = sorted(df["統計年度"].unique().tolist())

    df_胎次_所得平均數 = df.pivot_table(
        values="嬰兒出生數",
        index="胎次",
        columns="所得平均數區間",
        aggfunc="sum",
        dropna=False,
        sort=False,
        observed=False,
    )

    df_胎次_所得平均數.columns = df_胎次_所得平均數.columns.astype(str)
    plots[f"{key}_平均數區間"] = plot_bar_group(
        df_胎次_所得平均數,
        f"{key}_平均數區間(萬元) {years[0]}~{years[-1]}年",
        additional_layout={"yaxis": {"title": {"text": "出生數"}}},
    )

    plots[f"{key}_平均數區間2"] = plot_bar_group(
        df_胎次_所得平均數.T,
        f"{key}_平均數區間(萬元) {years[0]}~{years[-1]}年",
        additional_layout={
            "barmode": "stack",
            "yaxis": {"title": {"text": "出生數"}},
        },
    )

    df_胎次_所得平均數_縣市 = df.pivot_table(
        values="嬰兒出生數",
        index="胎次",
        columns=["縣市", "所得平均數區間"],
        aggfunc="sum",
        dropna=False,
        sort=False,
        observed=False,
    )

    buttons_region = [
        {
            "args": [
                {
                    "visible": [True] * len(df_胎次_所得平均數_縣市.columns),
                }
            ],  # 顯示所有線條
            "label": "全部區域",
            "method": "restyle",
        }
    ]
    regions = df["縣市"].unique().tolist()
    for region in regions:
        arr = [region_col == region for region_col, _ in df_胎次_所得平均數_縣市.columns]
        buttons_region.append(
            {
                "args": [
                    {"visible": arr},
                ],
                "label": region,
                "method": "restyle",
            },
        )

    updatemenus = [
        {
            "x": 0.5,
            "y": 1.09,
            "xanchor": "center",
            "yanchor": "top",
            "pad": {"r": 10, "t": 10},
            "buttons": buttons_region,
            "type": "dropdown",
            "direction": "down",
            "active": 0,
            "font": {"color": "#AAAAAA"},
            "name": "地區選擇",
        },
    ]

    df_胎次_所得平均數_縣市.columns = [
        f"{region}_{interval}" for region, interval in df_胎次_所得平均數_縣市.columns
    ]
    plots[f"{key}_平均數區間_縣市"] = plot_bar_group(
        df_胎次_所得平均數_縣市,
        f"{key}_平均數區間(萬元)_縣市 {years[0]}~{years[-1]}年",
        additional_layout={"updatemenus": updatemenus},
    )


def plot_嬰兒出生數按嬰兒性別及出生胎別分_按登記_(plots):
    key = "嬰兒出生數按嬰兒性別及出生胎別分(按登記)"
    key = sanitize_filename(key)
    df = df_嬰兒出生數按嬰兒性別及出生胎別分_按登記()

    years = df["統計年"].unique().tolist()

    df_total = df.pivot_table(values="嬰兒出生數", index="統計年", aggfunc="sum", sort=False)
    plots[f"{key}_總和"] = plot_line(
        df_total, f"{key}_總和 {df_total.index[0]}~{df_total.index[-1]}年"
    )

    df_區域別 = df.pivot_table(
        values="嬰兒出生數", index="統計年", columns="縣市", aggfunc="sum", sort=False
    )
    plots[f"{key}_區域別"] = plot_line(
        df_區域別, f"{key}_區域別 {df_區域別.index[0]}~{df_區域別.index[-1]}年"
    )

    df_性別 = df.pivot_table(
        values="嬰兒出生數", index="統計年", columns="性別", aggfunc="sum", sort=False
    )
    plots[f"{key}_性別"] = plot_line(
        df_性別, f"{key}_性別 {df_性別.index[0]}~{df_性別.index[-1]}年"
    )

    df_胎別 = df.pivot_table(
        values="嬰兒出生數", index="統計年", columns="胎別", aggfunc="sum", sort=False
    )
    plots[f"{key}_胎別"] = plot_line(
        df_胎別, f"{key}_胎別 {df_胎別.index[0]}~{df_胎別.index[-1]}年"
    )

    df_胎別_縣市 = df.pivot_table(
        values="嬰兒出生數", index="胎別", columns="縣市", aggfunc="sum", sort=False
    )
    plots[f"{key}_胎別_縣市"] = plot_bar_group(
        df_胎別_縣市, f"{key}_胎別_縣市 {years[0]}~{years[-1]}年"
    )


def plot_離婚_終止結婚人數按婚姻類型_性別_年齡_原屬國籍_地區_及教育程度分_按登記_(plots):
    key = "離婚/終止結婚人數按婚姻類型、性別、年齡、原屬國籍（地區）及教育程度分(按登記)"
    key = sanitize_filename(key)
    df = df_離婚_終止結婚人數按婚姻類型_性別_年齡_原屬國籍_地區_及教育程度分_按登記()
    years = df["statistic_yyy"].unique().tolist()

    df_現住人口 = df_現住人口性別_年齡_婚姻狀況()
    df_現住人口 = df_現住人口[df_現住人口["statistic_yyy"].isin(years)]

    df_total = df.pivot_table(
        values="divorce_count", index="statistic_yyy", aggfunc="sum", sort=False
    )
    plots[f"{key}_總和"] = plot_line(
        df_total, f"{key}_總和 {df_total.index[0]}~{df_total.index[-1]}年"
    )

    df_已婚人數 = df_現住人口.pivot_table(
        values="population",
        index="statistic_yyy",
        columns=["marital_status", "sex"],
        aggfunc="sum",
        sort=True,
    )
    df_離婚人數 = df.pivot_table(
        values="divorce_count",
        index="statistic_yyy",
        columns=["marriage_type", "sex"],
        aggfunc="sum",
        sort=True,
    )

    df_離婚率 = pd.DataFrame()
    df_離婚率["離婚人數_不同性別"] = df_離婚人數.loc[:, ("不同性別",)].sum(axis=1)
    df_離婚率["已婚人數_不同性別"] = df_已婚人數.loc[:, ("有偶_不同性別",)].sum(axis=1)
    df_離婚率["離婚率_不同性別"] = df_離婚率["離婚人數_不同性別"] / df_離婚率["已婚人數_不同性別"]
    df_離婚率["離婚人數_相同性別"] = df_離婚人數.loc[:, ("相同性別",)].sum(axis=1)
    df_離婚率["已婚人數_相同性別"] = df_已婚人數.loc[:, ("有偶_相同性別",)].sum(axis=1)
    df_離婚率["離婚率_相同性別"] = df_離婚率["離婚人數_相同性別"] / df_離婚率["已婚人數_相同性別"]
    df_離婚率["離婚人數_相同性別_男"] = df_離婚人數.loc[:, ("相同性別", "男")]
    df_離婚率["已婚人數_相同性別_男"] = df_已婚人數.loc[:, ("有偶_相同性別", "男")]
    df_離婚率["離婚率_相同性別_男"] = (
        df_離婚率["離婚人數_相同性別_男"] / df_離婚率["已婚人數_相同性別_男"]
    )
    df_離婚率["離婚人數_相同性別_女"] = df_離婚人數.loc[:, ("相同性別", "女")]
    df_離婚率["已婚人數_相同性別_女"] = df_已婚人數.loc[:, ("有偶_相同性別", "女")]
    df_離婚率["離婚率_相同性別_女"] = (
        df_離婚率["離婚人數_相同性別_女"] / df_離婚率["已婚人數_相同性別_女"]
    )

    plots[f"{key}_離婚率"] = plot_line(
        df_離婚率.filter(like="離婚率", axis="columns"),
        f"{key}_離婚率 {df_離婚率.index[0]}~{df_離婚率.index[-1]}年",
        additional_layout={"yaxis": {"tickformat": ".2%"}},
    )

    df_區域別 = df.pivot_table(
        values="divorce_count", index="statistic_yyy", columns="縣市", aggfunc="sum", sort=False
    )
    plots[f"{key}_區域別"] = plot_line(
        df_區域別, f"{key}_區域別 {df_區域別.index[0]}~{df_區域別.index[-1]}年"
    )

    df_區域別_已婚人數 = df_現住人口.pivot_table(
        values="population",
        index="statistic_yyy",
        columns=["縣市", "marital_status", "sex"],
        aggfunc="sum",
        sort=True,
    )
    df_區域別_離婚人數 = df.pivot_table(
        values="divorce_count",
        index="statistic_yyy",
        columns=["縣市", "marriage_type", "sex"],
        aggfunc="sum",
        sort=True,
    )

    regions = df["縣市"].unique().tolist()
    df_區域別_離婚率 = pd.DataFrame()
    for region in regions:
        df_區域別_離婚率[f"{region}_離婚人數_不同性別"] = df_區域別_離婚人數.loc[
            :,
            (
                region,
                "不同性別",
                slice(None),
            ),
        ].sum(axis=1)
        df_區域別_離婚率[f"{region}_已婚人數_不同性別"] = df_區域別_已婚人數.loc[
            :,
            (
                region,
                "有偶_不同性別",
                slice(None),
            ),
        ].sum(axis=1)
        df_區域別_離婚率[f"{region}_離婚率_不同性別"] = (
            df_區域別_離婚率[f"{region}_離婚人數_不同性別"]
            / df_區域別_離婚率[f"{region}_已婚人數_不同性別"]
        )
        df_區域別_離婚率[f"{region}_離婚人數_相同性別"] = df_區域別_離婚人數.loc[
            :,
            (
                region,
                "相同性別",
                slice(None),
            ),
        ].sum(axis=1)
        df_區域別_離婚率[f"{region}_已婚人數_相同性別"] = df_區域別_已婚人數.loc[
            :,
            (
                region,
                "有偶_相同性別",
                slice(None),
            ),
        ].sum(axis=1)
        df_區域別_離婚率[f"{region}_離婚率_相同性別"] = (
            df_區域別_離婚率[f"{region}_離婚人數_相同性別"]
            / df_區域別_離婚率[f"{region}_已婚人數_相同性別"]
        )
        df_區域別_離婚率[f"{region}_離婚人數_相同性別_男"] = df_區域別_離婚人數.loc[
            :, (region, "相同性別", "男")
        ]
        df_區域別_離婚率[f"{region}_已婚人數_相同性別_男"] = df_區域別_已婚人數.loc[
            :, (region, "有偶_相同性別", "男")
        ]
        df_區域別_離婚率[f"{region}_離婚率_相同性別_男"] = (
            df_區域別_離婚率[f"{region}_離婚人數_相同性別_男"]
            / df_區域別_離婚率[f"{region}_已婚人數_相同性別_男"]
        )
        df_區域別_離婚率[f"{region}_離婚人數_相同性別_女"] = df_區域別_離婚人數.loc[
            :, (region, "相同性別", "女")
        ]
        df_區域別_離婚率[f"{region}_已婚人數_相同性別_女"] = df_區域別_已婚人數.loc[
            :, (region, "有偶_相同性別", "女")
        ]
        df_區域別_離婚率[f"{region}_離婚率_相同性別_女"] = (
            df_區域別_離婚率[f"{region}_離婚人數_相同性別_女"]
            / df_區域別_離婚率[f"{region}_已婚人數_相同性別_女"]
        )

        df_區域別_離婚率 = df_區域別_離婚率.copy()

    plots[f"{key}_區域別_離婚率"] = plot_line(
        df_區域別_離婚率.filter(like="離婚率", axis="columns"),
        f"{key}_區域別_離婚率 {df_區域別_離婚率.index[0]}~{df_區域別_離婚率.index[-1]}年",
        additional_layout={"yaxis": {"tickformat": ".2%"}},
    )

    df_婚姻類型 = df.pivot_table(
        values="divorce_count",
        index="statistic_yyy",
        columns=["marriage_type", "sex"],
        aggfunc="sum",
        sort=False,
    )
    df_婚姻類型.columns = [f"{類型}_{性別}" for 類型, 性別 in df_婚姻類型.columns]
    plots[f"{key}_婚姻類型"] = plot_line(
        df_婚姻類型, f"{key}_婚姻類型 {df_婚姻類型.index[0]}~{df_婚姻類型.index[-1]}年"
    )

    df_性別 = df.pivot_table(
        values="divorce_count",
        index=["statistic_yyy", "sex"],
        columns="marriage_type",
        aggfunc="sum",
        sort=False,
    )
    plots[f"{key}_性別"] = plot_bar_stack_multi_index(
        df_性別, f"{key}_性別 {years[0]}~{years[-1]}年"
    )

    df_原屬國籍 = df.pivot_table(
        values="divorce_count", index="statistic_yyy", columns="nation", aggfunc="sum", sort=False
    )
    plots[f"{key}_原屬國籍"] = plot_line(
        df_原屬國籍, f"{key}_原屬國籍 {df_原屬國籍.index[0]}~{df_原屬國籍.index[-1]}年"
    )

    df_教育程度 = df.pivot_table(
        values="divorce_count", index="statistic_yyy", columns="edu", aggfunc="sum", sort=False
    )
    plots[f"{key}_教育程度"] = plot_line(
        df_教育程度, f"{key}_教育程度 {df_教育程度.index[0]}~{df_教育程度.index[-1]}年"
    )

    df_年齡 = df.pivot_table(
        values="divorce_count", index="statistic_yyy", columns="age", aggfunc="sum", sort=False
    )
    plots[f"{key}_年齡"] = plot_line(
        df_年齡, f"{key}_年齡 {df_年齡.index[0]}~{df_年齡.index[-1]}年"
    )

    df_女_年齡_縣市 = df[df["sex"] == "女"].pivot_table(
        values="divorce_count", index="age", columns="縣市", aggfunc="sum", sort=False
    )
    plots[f"{key}_女_年齡_縣市"] = plot_line(
        df_女_年齡_縣市, f"{key}_女_年齡_縣市 {years[0]}~{years[-1]}年"
    )

    df_男_年齡_縣市 = df[df["sex"] == "男"].pivot_table(
        values="divorce_count", index="age", columns="縣市", aggfunc="sum", sort=False
    )
    plots[f"{key}_男_年齡_縣市"] = plot_line(
        df_男_年齡_縣市, f"{key}_男_年齡_縣市 {years[0]}~{years[-1]}年"
    )

    df_女_年齡_教育 = df[df["sex"] == "女"].pivot_table(
        values="divorce_count",
        index="age",
        columns=["edu", "marriage_type"],
        aggfunc="sum",
        sort=False,
    )
    df_女_年齡_教育.columns = ["_".join(col) for col in df_女_年齡_教育.columns]
    plots[f"{key}_女_年齡_教育"] = plot_line(
        df_女_年齡_教育, f"{key}_女_年齡_教育 {years[0]}~{years[-1]}年"
    )

    df_男_年齡_教育 = df[df["sex"] == "男"].pivot_table(
        values="divorce_count",
        index="age",
        columns=["edu", "marriage_type"],
        aggfunc="sum",
        sort=False,
    )
    df_男_年齡_教育.columns = ["_".join(col) for col in df_男_年齡_教育.columns]
    plots[f"{key}_男_年齡_教育"] = plot_line(
        df_男_年齡_教育, f"{key}_男_年齡_教育 {years[0]}~{years[-1]}年"
    )

    df_男女_年齡_婚姻類型 = df.pivot_table(
        values="divorce_count",
        index="age",
        columns=["sex", "marriage_type"],
        aggfunc="sum",
        sort=False,
    )
    plots[f"{key}_男女_年齡_婚姻類型"] = plot_pyramid(
        f"{key}_男女_年齡_婚姻類型 {years[0]}~{years[-1]}年",
        df_男女_年齡_婚姻類型["男"],
        df_男女_年齡_婚姻類型["女"],
        df_男女_年齡_婚姻類型["男"].columns,
        df_男女_年齡_婚姻類型["男"].index,
    )


def plot_全國公立動物收容所收容處理情形統計表(plots, items):
    key = "全國公立動物收容所收容處理情形統計表"
    key = sanitize_filename(key)
    df = df_全國公立動物收容所收容處理情形統計表()

    columns = df.drop(["年度", "月份", "縣市名"], axis=1).columns
    columns_revise = columns.map(sanitize_filename)
    items[key] = columns_revise

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
            df_item, f"{key}_{col} {df_item.index[0]}~{df_item.index[-1]}年"
        )


def plot_定期定額交易戶數統計排行月報表(plots):
    key = "定期定額交易戶數統計排行月報表"
    key = sanitize_filename(key)
    df = df_定期定額交易戶數統計排行月報表()

    df["全名"] = df["代號"] + "_" + df["名稱"]
    df = df.pivot_table(
        index="年月",
        columns="全名",
        values="交易戶數",
        sort=False,
        aggfunc="sum",
    )

    df_sorted = df.sort_values(by=df.index[-1], axis="columns", ascending=False)
    plots[f"{key}"] = plot_line(df_sorted, f"{key} {df_sorted.index[0]}~{df_sorted.index[-1]}")


def plot_集保戶股權分散表(plots):
    key = "集保戶股權分散表"
    key = sanitize_filename(key)
    df = df_集保戶股權分散表()
    date = df.loc[0, "資料日期"]

    df_合計 = df[df["持股分級"] == 17].set_index("全名")

    df_人數 = df_合計.sort_values(by="人數", ascending=False)[["人數"]].T
    plots[f"{key}_人數"] = plot_bar(df_人數, f"{key}_人數 {date}")

    df_股數 = df_合計.sort_values(by="股數", ascending=False)[["股數"]].T
    plots[f"{key}_股數"] = plot_bar(df_股數, f"{key}_股數 {date}")

    df_分級 = df[df["持股分級"].isin(range(1, 16))].pivot_table(
        values="人數", index="持股分級說明", columns="全名", aggfunc="sum", sort=False
    )
    plots[f"{key}_分級"] = plot_bar_group(
        df_分級,
        f"{key}_分級 {date}",
        additional_layout={
            "yaxis": {"tickformat": None, "title": {"text": "人數"}},
            "xaxis": {"title": {"text": "持股分級"}},
        },
    )


def plot_投信投顧公會基金費用比率(plots):
    key = "投信投顧公會基金費用比率"
    key = sanitize_filename(key)
    df = df_投信投顧公會基金費用比率()

    # https://www.sitca.org.tw/ROC/Industry/IN2002.aspx?PGMID=IN0202 統計資料 > 境內基金各項資料 > 明細資料 > 類型代號說明
    類型代號 = df["類型代號"].unique().tolist()
    類型代號.sort()
    name_map = {}
    for _, x in df.iterrows():
        name_map[x["基金統編"]] = (x["基金名稱"].split(" (", 1)[0], x["類型代號"])

    df_總費用率 = df.pivot_table(values="合計_比率", index="年度", columns="基金統編")
    df_總費用率 = df_總費用率.sort_values(by=df_總費用率.index[-1], axis="columns")
    # 移除最後一年為空的資料
    # df_總費用率 = df_總費用率.dropna(axis="columns", subset=df_總費用率.index[-1])

    plots[f"{key}_統計"] = plot_histogram(
        df_總費用率,
        f"{key}_統計 {df_總費用率.index[0]}~{df_總費用率.index[-1]}",
        additional_layout={
            "xaxis": {"tickformat": ".2%"},
        },
    )

    最大_總費用率 = df_總費用率.max(axis="index")

    buttons_kinds = [
        {
            "args": [
                {
                    "visible": [True] * len(df_總費用率.columns),
                }
            ],  # 顯示所有線條
            "label": "全部類型",
            "method": "restyle",
        },
        {
            "args": [
                {
                    "visible": [費用 > 10 / 100 for 費用 in 最大_總費用率],
                }
            ],  # 顯示所有線條
            "label": "最大總費用率 > 10%",
            "method": "restyle",
        },
        {
            "args": [
                {
                    "visible": [10 / 100 >= 費用 and 費用 > 5 / 100 for 費用 in 最大_總費用率],
                }
            ],  # 顯示所有線條
            "label": "10% ≥ 最大總費用率 > 5%",
            "method": "restyle",
        },
        {
            "args": [
                {
                    "visible": [5 / 100 >= 費用 and 費用 > 1 / 100 for 費用 in 最大_總費用率],
                }
            ],  # 顯示所有線條
            "label": "5% ≥ 最大總費用率 > 1%",
            "method": "restyle",
        },
        {
            "args": [
                {
                    "visible": [1 / 100 >= 費用 and 費用 > 0.5 / 100 for 費用 in 最大_總費用率],
                }
            ],  # 顯示所有線條
            "label": "1% ≥ 最大總費用率 > 0.5%",
            "method": "restyle",
        },
        {
            "args": [
                {
                    "visible": [0.5 / 100 >= 費用 and 費用 > 0.1 / 100 for 費用 in 最大_總費用率],
                }
            ],  # 顯示所有線條
            "label": "0.5% ≥ 最大總費用率 > 0.1%",
            "method": "restyle",
        },
        {
            "args": [
                {
                    "visible": [0.1 / 100 >= 費用 for 費用 in 最大_總費用率],
                }
            ],  # 顯示所有線條
            "label": "0.1% ≥ 最大總費用率",
            "method": "restyle",
        },
    ]
    for kind in 類型代號:
        arr = [name_map[統編][1] == kind for 統編 in df_總費用率.columns]
        if all(not i for i in arr):
            continue

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
            "x": 1.0,
            "y": 1.05,
            "xanchor": "left",
            "yanchor": "bottom",
            "pad": {"r": 10, "t": 10},
            "buttons": buttons_kinds,
            "type": "dropdown",
            "direction": "down",
            "active": 1,
            "font": {"color": "#AAAAAA"},
            "name": "類型選擇",
        },
    ]

    df_總費用率.columns = df_總費用率.columns.map(lambda x: name_map[x][0]).str.strip()
    plots[f"{key}"] = plot_line(
        df_總費用率,
        f"{key} {df_總費用率.index[0]}~{df_總費用率.index[-1]}",
        additional_layout={
            "yaxis": {"tickformat": ".2%", "autorange": "reversed"},
            "updatemenus": updatemenus,
            "showlegend": True,
        },
        mode="lines+markers",
        visible={
            key: show
            for key, show in zip(df_總費用率.columns, buttons_kinds[1]["args"][0]["visible"])
        },
    )


def plot_基金績效評比(plots, items):
    key = "基金績效評比"
    key = sanitize_filename(key)
    df = df_基金績效評比()

    n = 3
    idx = (datetime.now() - df["基金成立日"]).dt.days > 365.25 * n
    val_cols = [
        "一年_年化報酬率",
        "二年_年化報酬率",
        "三年_年化報酬率",
        "五年_年化報酬率",
        "十年_年化報酬率",
        "自成立日_年化報酬率",
    ]
    items[key] = val_cols
    df_各報酬率 = df[idx].pivot_table(
        values=val_cols,
        index="資料日期",
        columns=["最新_基金統編", "基金成立日", "訂正_基金名稱"],
        sort=False,
    )
    df_各報酬率 = df_各報酬率.loc[:, df_各報酬率.iloc[-1].notna()]
    df_各報酬率.index = df_各報酬率.index.date

    # https://www.sitca.org.tw/ROC/Industry/IN2002.aspx?PGMID=IN0202 統計資料 > 境內基金各項資料 > 明細資料 > 類型代號說明
    df_代碼 = df_投信投顧公會基金費用比率()
    類型代號 = df_代碼["類型代號"].unique().tolist()
    類型代號.sort()
    name_map = {}
    for _, x in df_代碼.iterrows():
        name_map[x["基金統編"]] = x["類型代號"]

    for col in val_cols:
        df_報酬率 = df_各報酬率[col]
        df_報酬率_排名 = df_報酬率.rank(axis="columns", ascending=False)
        報酬率排名中位數 = df_報酬率_排名.median().sort_values()
        df_報酬率 = df_報酬率.reindex(報酬率排名中位數.index, axis="columns")
        df_報酬率_排名 = df_報酬率_排名.reindex(報酬率排名中位數.index, axis="columns")

        total_n = len(報酬率排名中位數)

        buttons_kinds = [
            {
                "args": [
                    {
                        "visible": [True] * len(df_報酬率.columns),
                    }
                ],  # 顯示所有線條
                "label": "全部類型",
                "method": "restyle",
            },
            {
                "args": [
                    {
                        "visible": [True] * 10 + [False] * (len(df_報酬率.columns) - 10),
                    }
                ],  # 顯示所有線條
                "label": "前十名",
                "method": "restyle",
            },
        ]

        for kind in 類型代號:
            arr = [
                name_map.get(統編) == kind
                for 統編 in df_報酬率.columns.get_level_values("最新_基金統編")
            ]
            if all(not i for i in arr):
                continue

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
                "x": 1.0,
                "y": 1.05,
                "xanchor": "left",
                "yanchor": "bottom",
                "pad": {"r": 10, "t": 10},
                "buttons": buttons_kinds,
                "type": "dropdown",
                "direction": "down",
                "active": 1,
                "font": {"color": "#AAAAAA"},
                "name": "類型選擇",
            },
        ]

        new_columns = []
        counts = {}
        for i, (統編, 成立日, 名稱) in enumerate(df_報酬率.columns, start=1):
            counts[名稱] = counts.get(名稱, 0) + 1
            if counts[名稱] > 1:
                名稱 += str(counts[名稱])

            new_columns.append(f"{i:2d}/{total_n}_{名稱}_{成立日.date()}")
        df_報酬率.columns = new_columns

        plots[f"{key}_{col}_折線圖"] = plot_line(
            df_報酬率,
            f"{key}_{col}_成立時間超過 {n} 年 {df_報酬率.index[0]}~{df_報酬率.index[-1]}",
            additional_layout={
                "yaxis": {"tickformat": ".2%"},
                "updatemenus": updatemenus,
                "showlegend": True,
            },
            visible={
                key: show
                for key, show in zip(df_報酬率.columns, buttons_kinds[1]["args"][0]["visible"])
            },
        )

        plots[f"{key}_{col}_小提琴圖"] = plot_violin(
            df_報酬率,
            f"{key}_{col}_成立時間超過 {n} 年 {df_報酬率.index[0]}~{df_報酬率.index[-1]}",
            additional_layout={
                "xaxis": {"showticklabels": False},
                "yaxis": {"tickformat": ".2%"},
                "updatemenus": updatemenus,
                "showlegend": True,
            },
            visible={
                key: show
                for key, show in zip(df_報酬率.columns, buttons_kinds[1]["args"][0]["visible"])
            },
        )

        buttons_kinds = [
            {
                "args": [
                    {
                        "visible": [True] * len(df_報酬率_排名.columns),
                    }
                ],  # 顯示所有線條
                "label": "全部類型",
                "method": "restyle",
            },
            {
                "args": [
                    {
                        "visible": [True] * 10 + [False] * (len(df_報酬率.columns) - 10),
                    }
                ],  # 顯示所有線條
                "label": "前十名",
                "method": "restyle",
            },
        ]

        for kind in 類型代號:
            arr = [
                name_map.get(統編) == kind
                for 統編 in df_報酬率_排名.columns.get_level_values("最新_基金統編")
            ]
            if all(not i for i in arr):
                continue

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
                "x": 1.0,
                "y": 1.05,
                "xanchor": "left",
                "yanchor": "bottom",
                "pad": {"r": 10, "t": 10},
                "buttons": buttons_kinds,
                "type": "dropdown",
                "direction": "down",
                "active": 1,
                "font": {"color": "#AAAAAA"},
                "name": "類型選擇",
            },
        ]

        new_columns = []
        counts = {}
        for i, (統編, 成立日, 名稱) in enumerate(df_報酬率_排名.columns, start=1):
            counts[名稱] = counts.get(名稱, 0) + 1
            if counts[名稱] > 1:
                名稱 += str(counts[名稱])

            new_columns.append(f"{i:2d}/{total_n}_{名稱}_{成立日.date()}")
        df_報酬率_排名.columns = new_columns

        plots[f"{key}_{col}_排名點線圖"] = plot_line(
            df_報酬率_排名,
            f"{key}_{col}_排名_成立時間超過 {n} 年 {df_報酬率_排名.index[0]}~{df_報酬率_排名.index[-1]}",
            additional_layout={
                "updatemenus": updatemenus,
                "showlegend": True,
                "yaxis": {"autorange": "reversed"},
            },
            mode="lines+markers",
            visible={
                key: show
                for key, show in zip(df_報酬率_排名.columns, buttons_kinds[1]["args"][0]["visible"])
            },
        )

        plots[f"{key}_{col}_排名箱型圖"] = plot_box(
            df_報酬率_排名,
            f"{key}_{col}_排名_成立時間超過 {n} 年 {df_報酬率.index[0]}~{df_報酬率.index[-1]}",
            additional_layout={
                "xaxis": {"showticklabels": False},
                "updatemenus": updatemenus,
                "showlegend": True,
                "yaxis": {"autorange": "reversed"},
            },
            visible={
                key: show
                for key, show in zip(df_報酬率_排名.columns, buttons_kinds[1]["args"][0]["visible"])
            },
        )


def main():
    plots: Dict[str, str] = {}  # Stores Plotly JSON strings
    items: Dict[str, Any] = {}  # Stores other items like column lists for templates

    tasks = [
        # ========================================================================
        lambda: plot_消費者物價基本分類指數(plots),
        lambda: plot_生產者物價基本分類指數(plots),
        lambda: plot_躉售物價基本分類指數(plots),
        lambda: plot_人力資源調查失業率(plots),
        lambda: plot_人力資源調查縣市別失業率(plots),
        lambda: plot_歷年人力資源調查重要指標(plots),
        lambda: plot_人力資源調查重要指標(plots),
        lambda: plot_歷年教育程度別失業率(plots),
        lambda: plot_教育程度別失業率(plots),
        lambda: plot_歷年年齡組別失業率(plots),
        lambda: plot_年齡組別失業率(plots),
        lambda: plot_教育程度別失業率_按年齡分(plots),
        lambda: plot_就業率(plots),
        lambda: plot_主要國家零歲平均餘命(plots),
        lambda: plot_房價所得比(plots),
        lambda: plot_貸款負擔率(plots),
        # ========================================================================
        lambda: plot_國民所得統計_國民所得_儲蓄與投資_季(plots),
        lambda: plot_國民所得統計_常用資料_季(plots),
        lambda: plot_家庭收支調查_家庭收支重要指標(plots),
        lambda: plot_家庭收支調查_所得總額按來源別分(plots),
        lambda: plot_家庭收支調查_所得總額與可支配所得(plots),
        lambda: plot_家庭收支調查_所得收入者人數與按年齡組別及性別之分配(plots),
        lambda: plot_家庭收支調查_戶內人數別平均每戶所得總額(plots),
        lambda: plot_家庭收支調查_戶內人數別平均每戶所得收入總計(plots),
        lambda: plot_家庭收支調查_戶內人數別平均每戶非消費支出(plots),
        lambda: plot_家庭收支調查_戶內人數別平均每戶可支配所得(plots),
        lambda: plot_家庭收支調查_戶內人數別平均每戶消費支出(plots),
        lambda: plot_家庭收支調查_戶內人數別平均每戶經常性支出(plots),
        lambda: plot_家庭收支調查_戶內人數別平均每戶儲蓄(plots),
        lambda: plot_家庭收支調查_戶內人數別平均每戶儲蓄率(plots),
        lambda: plot_家庭收支調查_戶內人數別平均每戶受僱人員報酬(plots),
        lambda: plot_家庭收支調查_家庭戶數按戶內人口規模別之分配(plots),
        lambda: plot_家庭收支調查_農家平均每戶所得總額按來自農業與非農業分(plots),
        lambda: plot_家庭收支調查_農家與非農家平均每戶可支配所得_所得總額及儲蓄(plots),
        lambda: plot_家庭收支調查_農家與非農家平均每戶消費支出_非消費支出及經常性支出(plots),
        lambda: plot_家庭收支調查_農家與非農家平均每戶所得收入總計及受僱人員報酬(plots),
        lambda: plot_家庭收支調查_戶數五等分位之平均每戶所得總額(plots),
        lambda: plot_家庭收支調查_戶數五等分位之平均每戶所得收入總計(plots),
        lambda: plot_家庭收支調查_戶數五等分位之平均每戶非消費支出(plots),
        lambda: plot_家庭收支調查_戶數五等分位之平均每戶可支配所得(plots),
        lambda: plot_家庭收支調查_戶數五等分位之平均每戶消費支出(plots),
        lambda: plot_家庭收支調查_戶數五等分位之平均每戶經常性支出(plots),
        lambda: plot_家庭收支調查_戶數五等分位之平均每戶儲蓄(plots),
        lambda: plot_家庭收支調查_戶數五等分位之平均每戶儲蓄率(plots),
        lambda: plot_家庭收支調查_戶數五等分位之平均每戶受僱人員報酬(plots),
        lambda: plot_家庭收支調查_戶數五等分位組之平均每戶人口數(plots),
        lambda: plot_家庭收支調查_戶數五等分位組之平均每戶就業人數(plots),
        lambda: plot_家庭收支調查_戶數十等分位組分界點之可支配所得(plots),
        lambda: plot_家庭收支調查_所得收入者五等分位平均每人可支配所得(plots),
        lambda: plot_家庭收支調查_所得收入者十等分位組分界點之可支配所得(plots),
        lambda: plot_家庭收支調查_家庭組織型態別平均每戶所得總額(plots),
        lambda: plot_家庭收支調查_家庭組織型態別平均每戶所得收入總計(plots),
        lambda: plot_家庭收支調查_家庭組織型態別平均每戶非消費支出(plots),
        lambda: plot_家庭收支調查_家庭組織型態別平均每戶可支配所得(plots),
        lambda: plot_家庭收支調查_家庭組織型態別平均每戶消費支出(plots),
        lambda: plot_家庭收支調查_家庭組織型態別平均每戶經常性支出(plots),
        lambda: plot_家庭收支調查_家庭組織型態別平均每戶儲蓄(plots),
        lambda: plot_家庭收支調查_家庭組織型態別平均每戶儲蓄率(plots),
        lambda: plot_家庭收支調查_家庭組織型態別平均每戶受僱人員報酬(plots),
        lambda: plot_家庭收支調查_性別統計指標_家戶狀況按家庭組織型態分(plots),
        lambda: plot_家庭收支調查_各縣市別平均每戶所得總額(plots),
        lambda: plot_家庭收支調查_各縣市別平均每戶所得收入總計(plots),
        lambda: plot_家庭收支調查_各縣市別平均每戶非消費支出(plots),
        lambda: plot_家庭收支調查_各縣市別平均每戶可支配所得(plots),
        lambda: plot_家庭收支調查_各縣市別平均每戶消費支出(plots),
        lambda: plot_家庭收支調查_各縣市別平均每戶經常性支出(plots),
        lambda: plot_家庭收支調查_各縣市別平均每戶儲蓄(plots),
        lambda: plot_家庭收支調查_各縣市別平均每戶儲蓄率(plots),
        lambda: plot_家庭收支調查_各縣市別平均每戶受僱人員報酬(plots),
        lambda: plot_家庭收支調查_所得收入者各縣市別平均每人所得收入總計(plots),
        lambda: plot_家庭收支調查_所得收入者各縣市別平均每人非消費支出(plots),
        lambda: plot_家庭收支調查_所得收入者各縣市別平均每人可支配所得(plots),
        lambda: plot_家庭收支調查_所得收入者各縣市別平均每人受僱人員報酬(plots),
        lambda: plot_主計總處統計專區_家庭收支調查_統計表_常用資料(plots),
        lambda: plot_家庭收支調查_性別統計指標_可支配所得按戶數五等分位經濟戶長性別比率(plots),
        lambda: plot_家庭收支調查_性別統計指標_戶數與結構按經濟戶長性別分(plots),
        lambda: plot_家庭收支調查_平均每戶可支配所得及消費支出依可支配所得按戶數五等分位分及經濟戶長年齡組別分,
        lambda: plot_家庭收支調查_平均每戶可支配所得及消費支出依可支配所得按戶數五等分位分及經濟戶長教育程度別分,
        lambda: plot_家庭收支調查_平均每戶可支配所得及消費支出依可支配所得按戶數五等分位分及經濟戶長性別分,
        lambda: plot_家庭收支調查_家庭戶數按所得總額組別及經濟戶長性別分(plots),
        lambda: plot_家庭收支調查_所得收入者人數按性別及可支配所得組別分(plots),
        lambda: plot_家庭收支調查_性別統計指標_經濟戶長人數按性別_教育程度及年齡分(plots),
        lambda: plot_家庭收支調查_性別統計指標_單親戶長人數按性別_教育程度及年齡分(plots),
        lambda: plot_家庭收支調查_經濟戶長職業別平均每戶所得總額(plots),
        lambda: plot_家庭收支調查_經濟戶長職業別平均每戶所得收入總計(plots),
        lambda: plot_家庭收支調查_經濟戶長職業別平均每戶非消費支出(plots),
        lambda: plot_家庭收支調查_經濟戶長職業別平均每戶可支配所得(plots),
        lambda: plot_家庭收支調查_經濟戶長職業別平均每戶消費支出(plots),
        lambda: plot_家庭收支調查_經濟戶長職業別平均每戶經常性支出(plots),
        lambda: plot_家庭收支調查_經濟戶長職業別平均每戶儲蓄(plots),
        lambda: plot_家庭收支調查_經濟戶長職業別平均每戶儲蓄率(plots),
        lambda: plot_家庭收支調查_經濟戶長職業別平均每戶受僱人員報酬(plots),
        lambda: plot_家庭收支調查_所得收入者職業別平均每人所得收入總計(plots),
        lambda: plot_家庭收支調查_所得收入者職業別平均每人非消費支出(plots),
        lambda: plot_家庭收支調查_所得收入者職業別平均每人可支配所得(plots),
        lambda: plot_家庭收支調查_所得收入者職業別平均每人受僱人員報酬(plots),
        lambda: plot_家庭部門平均每戶資產負債(plots),
        lambda: plot_公開資訊觀測站_財務報告附註揭露之員工福利_薪資_資訊(plots),
        lambda: plot_公開資訊觀測站_非擔任主管職務之全時員工薪資資訊(plots),
        lambda: plot_企業ESG資訊揭露彙總資料_人力發展_vs_公司合併報表董事酬金相關資訊(plots),
        lambda: plot_企業ESG資訊揭露彙總資料_人力發展(plots),
        lambda: plot_歷年受僱員工每人每月總薪資平均數(plots),
        lambda: plot_歷年受僱員工每人每月經常性薪資平均數(plots),
        lambda: plot_工業及服務業全體受僱員工全年總薪資統計表(plots),
        lambda: plot_工業及服務業受僱員工全年總薪資中位數及分布統計結果(plots),
        lambda: plot_工業及服務業每人每月工時_時_(plots),
        lambda: plot_各業廠商僱用職缺按月計薪者每人每月平均最低薪資_按職類及員工規模分(plots),
        lambda: plot_各業廠商調升經常性薪資參考各項因素之廠商比率_按行業分(plots),
        lambda: plot_各業廠商調升員工經常性薪資之廠商與員工人數比率_按行業分(plots),
        lambda: plot_公司合併報表董事酬金相關資訊(plots),
        lambda: plot_公司合併報表監察人酬金相關資訊(plots),
        lambda: plot_綜稅總所得各縣市申報統計分析表(plots),
        lambda: plot_綜稅綜合所得總額全國各縣市鄉鎮村里統計分析表(plots),
        lambda: plot_財政統計年報_綜合所得稅結算申報_按淨所得級距別分(plots),
        lambda: plot_勞工退休金提繳統計年報_按地區_行業及規模別(plots),
        lambda: plot_歷史_勞工退休金提繳統計年報_按地區_行業及規模別_按地區_行業及規模別(plots),
        lambda: plot_勞工退休準備金專戶餘額統計(plots),
        lambda: plot_勞工退休準備金提撥率統計(plots),
        lambda: plot_勞工退休金提繳單位_提繳人數_提繳工資_提繳金額概況(plots),
        lambda: plot_勞工退休金平均提繳工資_按行業別(plots),
        lambda: plot_勞工退休金平均提繳工資_按年齡組別(plots),
        lambda: plot_新制勞工退休基金歷年最近月份收益率(plots),
        # ========================================================================
        lambda: plot_全國賦稅收入實徵淨額日曆年別_按稅目別與地區別分(plots),
        lambda: plot_全國賦稅收入實徵淨額與預算數之比較(plots, items),
        # ========================================================================
        lambda: plot_進口貿易值_按洲別___國別分_CY2001__(plots),
        lambda: plot_出口貿易值_按洲別___國別分_CY2001__(plots),
        lambda: plot_進出口貿易值_按國際商品統一分類制度_HS_及主要國別分(plots),
        lambda: plot_進口值_按主要貨品分(plots),
        lambda: plot_出口值_按主要貨品分(plots),
        lambda: plot_貿易指數_進口單位價值指數(plots),
        lambda: plot_貿易指數_出口單位價值指數(plots),
        lambda: plot_貿易指數_進口數量指數(plots),
        lambda: plot_貿易指數_出口數量指數(plots),
        # ========================================================================
        lambda: plot_村里戶數_單一年齡人口_新增區域代碼_(plots),
        lambda: plot_現住人口性別_年齡_婚姻狀況_含同婚_(plots),
        lambda: plot_動態資料統計表_含同婚_(plots),
        lambda: plot_結婚人數按婚姻類型_性別_年齡_原屬國籍_地區_及教育程度分_按登記_(plots),
        lambda: plot_結婚對數按婚姻類型_性別及年齡分_按登記_(plots, items),
        lambda: plot_嬰兒出生數按性別_生母原屬國籍_地區__年齡及教育程度分_按登記_(plots),
        lambda: plot_嬰兒出生數按性別_生父原屬國籍_地區__年齡及教育程度分_按登記_(plots),
        lambda: plot_嬰兒出生數按嬰兒性別及生父母年齡分_按登記_(plots),
        lambda: plot_嬰兒出生數按生母年齡及出生身分分_按登記_(plots),
        lambda: plot_嬰兒出生數按性別_胎次及生母年齡分_按登記_(plots),
        lambda: plot_嬰兒胎次_vs_綜稅綜合所得總額(plots),
        lambda: plot_嬰兒出生數按嬰兒性別及出生胎別分_按登記_(plots),
        lambda: plot_離婚_終止結婚人數按婚姻類型_性別_年齡_原屬國籍_地區_及教育程度分_按登記_(
            plots
        ),
        # ========================================================================
        lambda: plot_全國公立動物收容所收容處理情形統計表(plots, items),
        # ========================================================================
        lambda: plot_定期定額交易戶數統計排行月報表(plots),
        lambda: plot_集保戶股權分散表(plots),
        lambda: plot_投信投顧公會基金費用比率(plots),
        lambda: plot_基金績效評比(plots, items),
        # ========================================================================
    ]

    do_tasks(tasks)

    prefix = "TW_Analysis"
    report_dir = Path("report")
    with app.app_context():
        jsfolder = report_dir / prefix
        jsfolder.mkdir(parents=True, exist_ok=True)

        for key, item in plots.items():
            graph = render_template("graph.js.j2", key=key, item=item)
            with open(jsfolder / f"{key}.js", "w", encoding="UTF-8") as f:
                minified_graph = jsmin(graph)
                f.write(minified_graph)

        html = render_template(
            "tw_analysis.html.j2",
            plots=plots,
            jsfolder=jsfolder.name,
            title=f"{prefix} Report",
            items=items,
        )
        with open(report_dir / f"{prefix}_Report.html", "w", encoding="UTF-8") as f:
            minified_html = minify_html.minify(
                html, keep_comments=False, keep_html_and_head_opening_tags=False
            )
            f.write(minified_html)


def do_tasks(tasks):
    print("開始執行所有繪圖任務...")

    for task in tasks:
        # 獲取函數的名稱以便於錯誤報告
        func_name = task.__code__.co_names[0] if task.__code__.co_names else "未知函數"

        try:
            # 執行當前任務 (即調用函數)
            task()
            print(f"✅ 成功執行：{func_name}")
        except Exception as e:
            # 捕捉所有類型的例外
            print(f"❌ 執行失敗：{func_name}")
            print(f"錯誤類型：{type(e).__name__}")
            print(f"錯誤訊息：{e}")
            # print("\n 詳細追蹤：")
            # import traceback
            # print(traceback.format_exc())
        finally:
            # 無論成功或失敗，都會執行這裡的程式碼
            print("-" * 30)

    print("所有任務執行完畢。")


if __name__ == "__main__":
    main()
