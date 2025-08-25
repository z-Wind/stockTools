import io
import zipfile
import pandas as pd
import requests
import minify_html

from flask import Flask, render_template
from pathlib import Path

from tw_analysis_data import sanitize_filename

app = Flask(__name__)


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

    prefix = "TW_Food"
    report_dir = Path("report")
    with app.app_context():
        subreport_folder = report_dir / prefix
        subreport_folder.mkdir(parents=True, exist_ok=True)

        links = {}
        for 分析項分類, 分析項, 含量單位 in 營養素種類:
            col = (分析項分類, 分析項, 含量單位)

            df = df.sort_values(col, ascending=False)

            # 移到第一行
            new_df = df.copy()
            column_to_move = new_df.pop(col)
            new_df.insert(0, col, column_to_move)

            tables = {
                "全部": new_df.reset_index().to_html(
                    classes=["table", "table-dark", "table-striped", "table-hover"],
                )
            }

            for 食品大類 in 食品種類:
                tables[食品大類] = (
                    new_df.loc[食品大類]
                    .reset_index()
                    .to_html(
                        classes=["table", "table-dark", "table-striped", "table-hover"],
                    )
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
