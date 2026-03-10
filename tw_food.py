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

from concurrent.futures import ProcessPoolExecutor
from functools import partial
from tqdm import tqdm
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

    rank_cols = []  # 用來收集所有新產生的排名 Series
    for col in df.columns:
        s = ranks_df[col].astype(int).astype(str) + "/" + str(len(df))
        s.name = (col[0], col[1], "排名")
        rank_cols.append(s)

    # 關鍵：一次性合併所有新欄位，並立即呼叫 .copy() 強制重新整合記憶體佈局
    df = pd.concat([df] + rank_cols, axis="columns").copy()

    df = df.sort_index(axis="columns")
    _log("排名欄位計算完成")

    prefix = "TW_Food"
    report_dir = Path("report")

    subreport_folder = report_dir / prefix
    subreport_folder.mkdir(parents=True, exist_ok=True)

    detail_links = _build_detail_pages(df, subreport_folder)

    df["細項資料"] = [f"<a href='{detail_links[idx]}' target='_blank'>連結</a>" for idx in df.index]
    df = df.set_index("細項資料", append=True)
    df_sorted_idx = df.sort_index().copy()

    links = _build_nutrient_pages(df_sorted_idx, 營養素種類, 食品種類, subreport_folder, prefix)

    _build_index_page(links, report_dir, prefix)
    _log("全部完成")


# ---------------------------------------------------------------------------
# 輔助函式：拆分 main() 以符合單一職責原則
# ---------------------------------------------------------------------------


def _process_single_detail(item, template_json, detail_folder):
    """
    子進程執行的單一頁面生成任務。
    """
    idx, row = item

    # --- 1. 資料處理 (Data Prep) ---
    row_df = row.to_frame().reset_index(level=2)
    df_含量 = row_df[row_df["level_2"] != "排名"]
    df_排名 = row_df[row_df["level_2"] == "排名"][idx]
    df_detail = pd.concat([df_含量, df_排名], axis="columns")

    # 這裡的列名邏輯需與你原始邏輯一致
    df_detail.columns = pd.MultiIndex.from_product([[i] for i in idx] + [["單位", "含量", "排名"]])

    # --- 2. 圖表邏輯 (Graphing) ---
    data_list = []
    for i in df_detail.index:
        rank_str = df_detail.loc[i, idx + ("排名",)]
        rank_val = int(rank_str.split("/")[0])
        data_list.append({"type": "bar", "name": f"{i[0]}_{i[1]}", "x": ["排名"], "y": [rank_val]})

    layout = {
        "title": {"text": str(idx)},
        "hovermode": "x",
        "yaxis": {"autorange": "reversed"},
        "barmode": "group",
    }

    # 合併模板
    graph = copy.deepcopy(template_json)
    graph.update({"data": data_list, "layout": {**graph.get("layout", {}), **layout}})

    # --- 3. 渲染與寫入 (Rendering & IO) ---
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

    return idx, f"./Detail/{filename}"


def _build_detail_pages(df: pd.DataFrame, subreport_folder: Path) -> dict:
    """使用多進程並行生成食品細項頁。"""
    detail_folder = subreport_folder / "Detail"
    detail_folder.mkdir(parents=True, exist_ok=True)

    _log(f"開始並行產生食品細項頁：共 {len(df):,} 筆")

    # 預先抓取模板，避免在子進程中重複呼叫 Plotly IO
    template_json = _get_dark_template()

    # 使用 partial 固定不變的參數
    worker = partial(
        _process_single_detail, template_json=template_json, detail_folder=detail_folder
    )

    detail_links = {}
    # 使用 ProcessPoolExecutor 進行多工處理
    # 建議 max_workers 不設限，自動使用 CPU 核心數
    with ProcessPoolExecutor() as executor:
        # 將 df.iterrows() 轉為 list 以供多進程調度
        tasks = list(df.iterrows())

        # 使用 tqdm 顯示進度
        results = list(
            tqdm(
                executor.map(worker, tasks), total=len(tasks), desc="[Parallel] Generating Details"
            )
        )

    # 整理回傳結果
    detail_links = {idx: link for idx, link in results}
    _log("食品細項頁產生完成")
    return detail_links


def _process_single_nutrient(col_info, df_shared, foods_unique, subreport_folder, prefix):
    """子進程任務：處理單一營養素的所有報表表格"""
    # 解開欄位資訊
    分析項分類, 分析項, 含量單位 = col_info
    col = (分析項分類, 分析項, 含量單位)

    # 1. 排序與重新排列欄位 (避免使用 drop，改用選取法更有效率)
    # 我們只需要該營養素欄位與原本的 index，其他欄位在營養素頁面通常是多餘的
    # 或是依你需求保留必要欄位
    df_sorted = df_shared.sort_values(col, ascending=False)

    # 2. 產生 HTML 表格 - 全部
    df_all = df_sorted.reset_index()
    df_all.index += 1
    tables = {
        "全部": df_all.to_html(
            classes=["table", "table-dark", "table-sm", "table-hover"],  # 加入 table-sm 減小體積
            escape=False,
            render_links=True,
        )
    }

    # 3. 產生 HTML 表格 - 各食品大類
    # 由於主進程已做了 sort_index，這裡的 loc 是極速切片
    for category in foods_unique:
        try:
            if category in df_sorted.index.get_level_values(0):
                df_sub = df_sorted.loc[category].reset_index()
                df_sub.index += 1
                tables[category] = df_sub.to_html(
                    classes=["table", "table-dark", "table-sm", "table-hover"],
                    escape=False,
                    render_links=True,
                )
        except KeyError:
            continue

    # 4. 渲染與寫入
    html = render_template(
        "tw_food.html.j2",
        links={},
        tables=tables,
        kinds=col,
        title=f"{分析項分類}_{分析項} Report",
    )

    filename = f"{sanitize_filename(分析項分類)}_{sanitize_filename(分析項)}_Report.html"
    _write_html(subreport_folder / filename, html)

    return f"{分析項分類}_{分析項}", f"./{prefix}/{filename}"


def _build_nutrient_pages(
    df: pd.DataFrame, 營養素種類, 食品種類, subreport_folder: Path, prefix: str
) -> dict:
    """優化後的營養素頁面產生器"""
    _log(f"開始並行產生營養素排行頁：共 {len(營養素種類):,} 種")

    # [重要] 預先對 Index 排序，這會讓子進程中的 .loc[category] 速度大幅提升
    df_sorted_idx = df.sort_index()

    # 準備並行任務
    worker = partial(
        _process_single_nutrient,
        df_shared=df_sorted_idx,
        foods_unique=食品種類,
        subreport_folder=subreport_folder,
        prefix=prefix,
    )

    # 執行並行處理
    with ProcessPoolExecutor() as executor:
        # 使用 list 確保 tqdm 能正確取得總量
        results = list(
            tqdm(
                executor.map(worker, 營養素種類),
                total=len(營養素種類),
                desc="[Parallel] Nutrient Pages",
            )
        )

    _log("營養素排行頁產生完成")
    return dict(results)


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

    # debug 用
    # file_path = r"C:\Users\chihchieh.sun\Downloads\20_2.csv.zip"
    # _log(f"讀取本地檔案：{file_path}...")
    # # 使用 'rb' 模式開啟二進位檔案
    # with open(file_path, "rb") as f:
    #     with zipfile.ZipFile(f) as z:
    #         with z.open("20_2.csv") as fh:
    #             data = pd.read_csv(fh)
    # _log(f"讀取完成，資料筆數：{len(data)}")

    data = data.fillna(value={"俗名": ""})
    data = data.fillna(0)
    return data


if __name__ == "__main__":
    main()
