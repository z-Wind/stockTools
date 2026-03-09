import copy
import io
import json
import os
import time
import zipfile
import minify_html
import pandas as pd
import plotly
import plotly.utils
import requests

from pathlib import Path
from typing import Optional
from jinja2 import Environment, FileSystemLoader

from tw_analysis_data import sanitize_filename
from tw_analysis import merge_dict


_t0 = time.time()


def _log(msg: str) -> None:
    """印出帶時間戳記的進度訊息，格式：[+NNN.Ns] msg"""
    elapsed = time.time() - _t0
    print(f"[+{elapsed:6.1f}s] {msg}", flush=True)


_jinja_env: Optional[Environment] = None


def _get_jinja_env() -> Environment:
    """延遲初始化 Jinja2 Environment，避免在 import 時就讀取檔案系統。"""
    global _jinja_env
    if _jinja_env is None:
        template_dir = os.path.join(os.path.dirname(__file__), "templates")
        _jinja_env = Environment(loader=FileSystemLoader(template_dir))
    return _jinja_env


def render_template(template_name: str, **kwargs) -> str:
    return _get_jinja_env().get_template(template_name).render(**kwargs)


# ---------------------------------------------------------------------------
# Plotly dark 模板快取：避免在每次迭代都呼叫 plotly.io.templates[...]
# ---------------------------------------------------------------------------

_dark_template: Optional[dict] = None


def _get_dark_template() -> dict:
    global _dark_template
    if _dark_template is None:
        _dark_template = plotly.io.templates["plotly_dark"].to_plotly_json()
    return _dark_template


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------


def main() -> None:
    _log("開始執行")
    data = get_data()
    _log(f"資料下載完成：{len(data):,} 筆")

    # .copy() 避免 SettingWithCopyWarning（pandas 對 slice 的寫入警告）
    data_filter = data[data["分析項"] != "P/M/S"].copy()
    data_filter["每100克含量"] = data_filter["每100克含量"].astype(float)

    _log("建立 pivot_table 中...")
    df = data_filter.pivot_table(
        values="每100克含量",
        index=["食品分類", "樣品名稱", "俗名"],
        columns=["分析項分類", "分析項", "含量單位"],
    )
    df = df.astype(float)
    _log(f"pivot_table 完成：{len(df):,} 種食品 × {len(df.columns):,} 種營養素")

    營養素種類 = df.columns
    食品種類 = data["食品分類"].unique()

    # 計算各食品每種營養素的排名欄位
    _log("計算排名欄位中...")
    ranks_df = df.rank(axis="index", method="max", na_option="bottom", ascending=False)
    for col in df.columns:
        s = ranks_df[col].astype(int).astype(str) + "/" + str(len(df))
        s.name = (col[0], col[1], "排名")
        df = pd.concat([df, s], axis="columns")
    df = df.sort_index(axis="columns")
    _log("排名欄位計算完成")

    prefix = "TW_Food"
    report_dir = Path("report")

    subreport_folder = report_dir / prefix
    subreport_folder.mkdir(parents=True, exist_ok=True)

    detail_links = _build_detail_pages(df, subreport_folder)

    df["細項資料"] = [f"<a href='{detail_links[idx]}' target='_blank'>連結</a>" for idx in df.index]
    df = df.set_index("細項資料", append=True)

    links = _build_nutrient_pages(df, 營養素種類, 食品種類, subreport_folder, prefix)

    _build_index_page(links, report_dir, prefix)
    _log("全部完成")


# ---------------------------------------------------------------------------
# 輔助函式：拆分 main() 以符合單一職責原則
# ---------------------------------------------------------------------------


def _build_detail_pages(df: pd.DataFrame, subreport_folder: Path) -> dict:
    """為每個食品樣品建立細項報表頁，回傳 {index_tuple: relative_link}。"""
    detail_folder = subreport_folder / "Detail"
    detail_folder.mkdir(parents=True, exist_ok=True)  # 移出迴圈，只建立一次

    total = len(df)
    _log(f"開始產生食品細項頁：共 {total:,} 筆")

    detail_links: dict = {}
    for n, (idx, row) in enumerate(df.iterrows(), 1):
        if n == 1 or n % 100 == 0 or n == total:
            _log(f"  細項頁 {n:,}/{total:,}  {idx[0]} / {idx[1]}")

        row = row.to_frame().reset_index(level=2)
        df_含量 = row[row["level_2"] != "排名"]
        df_排名 = row[row["level_2"] == "排名"][idx]
        df_detail = pd.concat([df_含量, df_排名], axis="columns")
        df_detail.columns = pd.MultiIndex.from_product(
            [[i] for i in idx] + [["單位", "含量", "排名"]]
        )

        data_list = []
        for i in df_detail.index:
            rank_val = int(df_detail.loc[i, idx + ("排名",)].split("/")[0])
            data_list.append(
                {"type": "bar", "name": f"{i[0]}_{i[1]}", "x": ["排名"], "y": [rank_val]}
            )

        layout = {
            "title": {"text": str(idx)},
            "hovermode": "x",
            "yaxis": {"autorange": "reversed"},
            "barmode": "group",
        }
        graph = {"data": data_list, "layout": layout, "config": {}}
        # deepcopy 避免修改到快取的全域模板
        graph = merge_dict(copy.deepcopy(_get_dark_template()), graph)

        html = render_template(
            "tw_food.html.j2",
            links={},
            tables={},
            detail=df_detail.to_html(
                classes=["table", "table-dark", "table-striped", "table-hover"],
            ),
            graph=json.dumps(graph, cls=plotly.utils.PlotlyJSONEncoder),
            title=f"{'_'.join(idx)} Report",
        )
        filename = f"{sanitize_filename('_'.join(idx))}_Report.html"
        _write_html(detail_folder / filename, html)
        detail_links[idx] = f"./Detail/{filename}"

    _log(f"食品細項頁產生完成：共 {total:,} 個檔案")
    return detail_links


def _build_nutrient_pages(
    df: pd.DataFrame,
    營養素種類,
    食品種類,
    subreport_folder: Path,
    prefix: str,
) -> dict:
    """為每種營養素建立排行報表頁，回傳 {key: relative_link}。"""
    total = len(營養素種類)
    _log(f"開始產生營養素排行頁：共 {total:,} 種")

    links: dict = {}
    for n, (分析項分類, 分析項, 含量單位) in enumerate(營養素種類, 1):
        if n == 1 or n % 20 == 0 or n == total:
            _log(f"  排行頁 {n:,}/{total:,}  {分析項分類} / {分析項}")
        col = (分析項分類, 分析項, 含量單位)

        df_sorted = df.sort_values(col, ascending=False)

        # 將關注欄移到最前面
        new_df = pd.concat(
            [df_sorted[[col]], df_sorted.drop(columns=(分析項分類, 分析項))],
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
        _write_html(subreport_folder / filename, html)
        links[f"{分析項分類}_{分析項}"] = f"./{prefix}/{filename}"

    _log(f"營養素排行頁產生完成：共 {total:,} 個檔案")
    return links


def _build_index_page(links: dict, report_dir: Path, prefix: str) -> None:
    """建立首頁（各營養素連結清單）。"""
    _log("產生首頁...")
    html = render_template(
        "tw_food.html.j2",
        links=links,
        tables={},
        kinds=None,
        title=f"{prefix} Report",
    )
    _write_html(report_dir / f"{prefix}_Report.html", html)


def _write_html(path: Path, html: str) -> None:
    """壓縮並寫入 HTML 檔案（原本每個呼叫點都重複這 4 行）。"""
    minified = minify_html.minify(html, keep_comments=False, keep_html_and_head_opening_tags=False)
    path.write_text(minified, encoding="UTF-8")


# ---------------------------------------------------------------------------
# 資料下載
# ---------------------------------------------------------------------------


def get_data() -> pd.DataFrame:
    """下載台灣食品營養成分資料庫（食藥署開放資料 #8543）。"""
    # https://data.gov.tw/dataset/8543
    url = (
        "https://data.fda.gov.tw/opendata/exportDataList.do"
        "?method=ExportData&InfoId=20&logType=2"
    )
    _log("下載資料中...")
    response = requests.get(url)
    response.raise_for_status()  # 明確拋出 HTTP 錯誤，取代靜默失敗
    _log(f"下載完成（{len(response.content) / 1024:.0f} KB），解壓縮與讀取 CSV...")

    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        with z.open("20_2.csv") as fh:  # fh 避免與外層變數 f 同名衝突
            data = pd.read_csv(fh)

    data = data.fillna(value={"俗名": ""})
    data = data.fillna(0)
    return data


if __name__ == "__main__":
    main()
