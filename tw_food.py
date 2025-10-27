import json
import io
import zipfile
import pandas as pd
import plotly
import minify_html
import requests

from flask import Flask, render_template
from pathlib import Path

from tw_analysis_data import sanitize_filename

app = Flask(__name__)


def _mergeDict(a, b, path=None, overwrite=True):
    """Merges b into a. If overwrite is True, b's values will overwrite a's on conflict."""
    if path is None:
        path = []
    for key in b:
        if key in a:
            if isinstance(a[key], dict) and isinstance(b[key], dict):
                _mergeDict(a[key], b[key], path + [str(key)], overwrite)
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


def main():
    data = get_data()

    data_filter = data[data["分析項"] != "P/M/S"]
    data_filter.loc[:, "每100克含量"] = data_filter["每100克含量"].astype(float)

    df = data_filter.pivot_table(
        values="每100克含量",
        index=["食品分類", "樣品名稱", "俗名"],
        columns=["分析項分類", "分析項", "含量單位"],
    )
    df = df.astype(float)

    營養素種類 = df.columns
    食品種類 = data["食品分類"].unique()

    ranks_df = df.rank(axis="index", method="max", na_option="bottom", ascending=False)
    for col in df.columns:
        s = ranks_df[col].astype(int).astype(str) + "/" + str(len(df))
        s.name = (col[0], col[1], "排名")
        df = pd.concat([df, s], axis="columns")
    df = df.sort_index(axis="columns")

    prefix = "TW_Food"
    report_dir = Path("report")
    with app.app_context():
        subreport_folder = report_dir / prefix
        subreport_folder.mkdir(parents=True, exist_ok=True)

        detail_links = {}
        for idx, row in df.iterrows():
            detail_folder = subreport_folder / "Detail"
            detail_folder.mkdir(parents=True, exist_ok=True)

            row = row.to_frame().reset_index(level=2)
            df_含量 = row[row["level_2"] != "排名"]
            df_排名 = row[row["level_2"] == "排名"][idx]
            df_detail = pd.concat([df_含量, df_排名], axis="columns")
            df_detail.columns = pd.MultiIndex.from_product(
                [[i] for i in idx] + [["單位", "含量", "排名"]]
            )

            dataList = []
            for i in df_detail.index:
                name = f"{i[0]}_{i[1]}"
                data = [int(str.split(df_detail.loc[i, idx + ("排名",)], "/")[0])]
                dataList.append({"type": "bar", "name": name, "x": ["排名"], "y": data})

            layout = {
                "title": {"text": f"{idx}"},
                "hovermode": "x",
                "yaxis": {
                    "autorange": "reversed",
                },
                "barmode": "group",
            }

            graph = {"data": dataList, "layout": layout, "config": {}}
            graph = _mergeDict(plotly.io.templates["plotly_dark"].to_plotly_json(), graph)

            html = render_template(
                "tw_food.html.j2",
                links={},
                tables={},
                detail=df_detail.to_html(
                    classes=["table", "table-dark", "table-striped", "table-hover"],
                ),
                graph=json.dumps(graph, cls=plotly.utils.PlotlyJSONEncoder),
                title=f"{"_".join(idx)} Report",
            )
            filename = f"{sanitize_filename("_".join(idx))}_Report.html"
            with open(detail_folder / f"{filename}", "w", encoding="UTF-8") as f:
                minified_html = minify_html.minify(
                    html, keep_comments=False, keep_html_and_head_opening_tags=False
                )
                f.write(minified_html)

            detail_links[idx] = f"./Detail/{filename}"

        df["細項資料"] = [
            f"<a href='{detail_links[idx]}' target='_blank'>連結</a>" for idx in df.index
        ]
        df = df.set_index("細項資料", append=True)

        links = {}
        for 分析項分類, 分析項, 含量單位 in 營養素種類:
            col = (分析項分類, 分析項, 含量單位)

            df = df.sort_values(
                col,
                ascending=False,
            )

            # 關注項移到第一行
            new_df = pd.concat(
                [
                    df[[col]],
                    df.drop(
                        columns=(
                            分析項分類,
                            分析項,
                        )
                    ),
                ],
                axis="columns",
            )

            df_全部 = new_df.reset_index()
            df_全部.index += 1
            tables = {
                "全部": df_全部.to_html(
                    classes=["table", "table-dark", "table-striped", "table-hover"],
                    escape=False,
                    render_links=True,
                )
            }

            for 食品大類 in 食品種類:
                df_食品大類 = new_df.loc[食品大類].reset_index()
                df_食品大類.index += 1
                tables[食品大類] = df_食品大類.to_html(
                    classes=["table", "table-dark", "table-striped", "table-hover"],
                    escape=False,
                    render_links=True,
                )

            html = render_template(
                "tw_food.html.j2",
                links={},
                tables=tables,
                kinds=col,
                title=f"{分析項分類}_{分析項}_{含量單位} Report",
            )
            filename = f"{sanitize_filename(分析項分類)}_{sanitize_filename(分析項)}_Report.html"
            with open(subreport_folder / f"{filename}", "w", encoding="UTF-8") as f:
                minified_html = minify_html.minify(
                    html, keep_comments=False, keep_html_and_head_opening_tags=False
                )
                f.write(minified_html)
            links[f"{分析項分類}_{分析項}"] = f"./{prefix}/{filename}"

        html = render_template(
            "tw_food.html.j2",
            links=links,
            tables={},
            kinds=None,
            title=f"{prefix} Report",
        )
        with open(report_dir / f"{prefix}_Report.html", "w", encoding="UTF-8") as f:
            minified_html = minify_html.minify(
                html, keep_comments=False, keep_html_and_head_opening_tags=False
            )
            f.write(minified_html)


def get_data() -> pd.DataFrame:
    # data = pd.read_csv(r"C:\Users\chihchieh.sun\Downloads\20_2.csv")
    # https://data.gov.tw/dataset/8543
    url = "https://data.fda.gov.tw/opendata/exportDataList.do?method=ExportData&InfoId=20&logType=2"
    response = requests.get(url)
    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        with z.open("20_2.csv") as f:
            data = pd.read_csv(f)

    data = data.fillna(value={"俗名": ""})
    data = data.fillna(0)
    return data


if __name__ == "__main__":
    main()
