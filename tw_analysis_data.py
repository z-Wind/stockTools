from datetime import datetime
import gzip
import itertools
import json
import re
import time
import numpy as np
import pandas as pd
import io
import requests
import warnings

from requests.adapters import HTTPAdapter
import urllib3
from urllib3.util import Retry
from pathlib import Path

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.simplefilter(action="ignore", category=FutureWarning)

# Regular expression for sanitizing strings for use as keys or filenames
FILENAME_SANITIZE_PATTERN = r'[- ,、()~∕\/－%*?:"<>|（）—]+'

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


def read_xml_with_cache(path: Path, url: str, xpath: str) -> pd.DataFrame:
    _ensure_dir_exists(path)

    if not path.is_file():
        r = session.get(url, verify=False)
        with gzip.open(path, "wb") as f:
            f.write(r.content)

    df = pd.read_xml(path, compression="gzip", xpath=xpath)

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
    path: Path,
    url: str,
    skiprows=None,
    nrows=None,
    usecols=None,
    sheet_name=0,
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
        excel_bytes,
        engine="calamine",
        skiprows=skiprows,
        nrows=nrows,
        usecols=usecols,
        sheet_name=sheet_name,
    )

    return df


# data =========================================================


# https://data.gov.tw/dataset/6019 消費者物價基本分類指數
def df_消費者物價基本分類指數():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/230555/pr0101a1m.xml"
    xpath = "//Obs"
    item_remove_patt = r"\(指數基期：民國110年=100\)"

    df = read_xml(url, xpath)
    df["Item"] = df["Item"].str.replace(item_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/148439 生產者物價基本分類指數
def df_生產者物價基本分類指數():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/230534/pr0701a1m.xml"
    xpath = "//Obs"
    item_remove_patt = r"\(指數基期：民國110年=100\)"

    df = read_xml(url, xpath)
    df["Item"] = df["Item"].str.replace(item_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/8239 躉售物價基本分類指數
def df_躉售物價基本分類指數():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/230546/pr0202a1m.xml"
    xpath = "//Obs"
    item_remove_patt = r"\(民國105年=100\)"

    df = read_xml(url, xpath)
    df["Item"] = df["Item"].str.replace(item_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/6637 人力資源調查失業率
def df_人力資源調查失業率():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/230038/mp0101a07.xml"
    xpath = "//失業率"

    df = read_xml(url, xpath)
    df = df.set_index("年月別_Year_and_month")
    df.columns = df.columns.str.replace(r"_?[a-zA-Z_]*_百分比$", r"", regex=True).str.replace(
        r"_[a-zA-Z_]+$", r"", regex=True
    )
    df = df.replace("-", np.nan)
    df = df.astype(float) / 100

    return df


# https://data.gov.tw/dataset/6640 人力資源調查縣市別失業率
def df_人力資源調查縣市別失業率():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/230038/mp0101a10.xml"
    xpath = "//縣市別失業率"

    df = read_xml(url, xpath)
    df = df.set_index("年月別_Year_and_month")
    df.columns = df.columns.str.replace(r"_[a-zA-Z_]+_百分比$", r"", regex=True)
    df = df.replace("-", np.nan)
    df = df.astype(float) / 100

    return df


# https://data.gov.tw/dataset/31055 歷年人力資源調查重要指標
def df_歷年人力資源調查重要指標():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/234748/mp04014.xml"
    xpath = "//歷年人力資源調查重要指標"

    df = read_xml(url, xpath)
    df = df.set_index("年月別_Year_and_month")
    df = df.filter(regex=r"\d{4}$", axis="index")
    df.index = df.index.str.replace(r".*(\d{4})$", r"\1", regex=True)
    df.columns = df.columns.str.replace(r"_[0-9a-zA-Z_]+_百分比$", r"", regex=True).str.replace(
        r"_[a-zA-Z_]+$", r"", regex=True
    )
    df = df.replace("-", np.nan)
    df = df.astype(float) / 100

    return df


# https://data.gov.tw/dataset/33442 人力資源調查重要指標
def df_人力資源調查重要指標():
    key = "人力資源調查重要指標"
    key = sanitize_filename(key)
    urls = {
        104: "https://www.dgbas.gov.tw/public/data/open/Cen/Mp04037.xml",
        105: "https://www.dgbas.gov.tw/public/data/open/Cen/Mp04037A105.xml",
        106: "https://www.dgbas.gov.tw/public/data/open/Cen/Mp04037A106.xml",
        107: "https://www.dgbas.gov.tw/public/data/open/Cen/Mp04037A107.xml",
        108: "https://www.dgbas.gov.tw/public/data/open/Cen/Mp04037A108.xml",
        109: "https://www.dgbas.gov.tw/public/data/open/Cen/Mp04037A109.xml",
        110: "https://www.dgbas.gov.tw/public/data/open/Cen/Mp04037A110.xml",
        111: "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/231127/mp04037a111.xml",
        112: "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/233304/mp04037a112.xml",
        113: "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/234748/mp04037a113.xml",
    }
    xpath = "//人力資源調查重要指標"

    if max(urls.keys()) + 1911 + 1 < datetime.now().year and datetime.now().month > 4:
        print(f"請更新 {key}")

    df = []
    for year, url in urls.items():
        data = read_xml_with_cache(EXTRA_DATA_DIR / key / f"{year}.xml.gz", url, xpath)
        data["年度"] = year
        data = data.rename(
            columns={
                "地區別_District_or_region": "按地區別分_District_or_region",
                "年齡15歲以上民間人口占總人口之比率_Proportion_of_civilian_population_aged_15_years_and_over_to_total_population_百分比": "年齡15歲以上民間人口占總人口之比率_Proportion_of_civilian_population_aged_15_years_and_over_to_total_population",
                "勞動力_占總人口之比率_Labor_force_Proportion_of_labor_force_to_total_population_百分比": "勞動力_占總人口之比率_Labor_force_Proportion_of_labor_force_to_total_population",
                "勞動力_占15歲以上民間人口之比率_勞動力參與率_總計_Labor_force_Labor_force_participation_rate_Total_百分比": "勞動力_占15歲以上民間人口之比率_勞動力參與率_總計_Labor_force_Labor_force_participation_rate_Total",
                "勞動力_占15歲以上民間人口之比率_勞動力參與率_男_Labor_force_Labor_force_participation_rate_Male_百分比": "勞動力_占15歲以上民間人口之比率_勞動力參與率_男_Labor_force_Labor_force_participation_rate_Male",
                "勞動力_占15歲以上民間人口之比率_勞動力參與率_女_Labor_force_Labor_force_participation_rate_Female_百分比": "勞動力_占15歲以上民間人口之比率_勞動力參與率_女_Labor_force_Labor_force_participation_rate_Female",
                "就業者_占總人口之比率_Employed_Proportion_of_employed_persons_to_total_population_百分比": "就業者_占總人口之比率_Employed_Proportion_of_employed_persons_to_total_population",
                "就業者_占15歲以上民間人口之比率_Employed_Proportion_of_employed_persons_to_civilian_population_aged_15_years_and_over_百分比": "就業者_占15歲以上民間人口之比率_Employed_Proportion_of_employed_persons_to_civilian_population_aged_15_years_and_over",
                "就業者_占勞動力之比率_Employed_Proportion_of_employed_persons_to_labor_force_百分比": "就業者_占勞動力之比率_Employed_Proportion_of_employed_persons_to_labor_force",
                "失業率_總計_Unemployment_rate_Total_百分比": "失業率_總計_Unemployment_rate_Total",
                "失業率_男_Unemployment_rate_Male_百分比": "失業率_男_Unemployment_rate_Male",
                "失業率_女_Unemployment_rate_Female_百分比": "失業率_女_Unemployment_rate_Female",
                "年齡15歲以上民間人口占總人口之比率_Proportion_of_civilian_population_age_15_and_above_to_total_population_百分比": "年齡15歲以上民間人口占總人口之比率_Proportion_of_civilian_population_aged_15_years_and_over_to_total_population",
                "就業者_占15歲以上民間人口之比率_Employed_Proportion_of_employed_persons_to_civilian_population_age_15_and_above_百分比": "就業者_占15歲以上民間人口之比率_Employed_Proportion_of_employed_persons_to_civilian_population_aged_15_years_and_over",
            }
        )
        df.append(data)
    df = pd.concat(df, ignore_index=True)

    df["按地區別分_District_or_region"] = (
        df["按地區別分_District_or_region"].str.strip().str.replace(r"[a-zA-Z ]+", "", regex=True)
    )

    df.columns = df.columns.str.replace(r"[_a-zA-Z0-9]+$", "", regex=True)
    num_columns = [
        column for column in df.columns if "年度" not in column and "地區別分" not in column
    ]
    df.loc[:, num_columns] = df.loc[:, num_columns].replace("-", np.nan).astype(float) / 100

    return df


# https://data.gov.tw/dataset/32741 歷年教育程度別失業率
def df_歷年教育程度別失業率():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/234748/mp04033.xml"
    xpath = "//歷年教育程度別失業率"

    df = read_xml(url, xpath)
    df = df.set_index("年月別_Year_and_month")
    df = df.filter(regex=r"\d{4}$", axis="index")
    df.index = df.index.str.replace(r".*(\d{4})$", r"\1", regex=True)
    df.columns = df.columns.str.replace(r"_[_a-zA-Z]+_百分比", "", regex=True)
    df = df.replace("-", np.nan)
    df = df.astype(float) / 100

    return df


# https://data.gov.tw/dataset/34118 教育程度別失業率
def df_教育程度別失業率():
    key = "教育程度別失業率"
    key = sanitize_filename(key)
    urls = {
        104: "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/231813/Mp04049.xml",
        105: "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/231813/Mp04049A105.xml",
        106: "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/231813/Mp04049A106.xml",
        107: "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/231813/Mp04049A107.xml",
        108: "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/231813/Mp04049A108.xml",
        109: "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/231813/Mp04049A109.xml",
        110: "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/231813/Mp04049A110.xml",
        111: "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/231813/mp04049a111.xml",
        112: "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/233304/mp04049a112.xml",
        113: "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/234748/mp04049a113.xml",
    }
    xpath = "//教育程度別失業率"

    if max(urls.keys()) + 1911 + 1 < datetime.now().year and datetime.now().month > 4:
        print(f"請更新 {key}")

    df = []
    for year, url in urls.items():
        data = read_xml_with_cache(EXTRA_DATA_DIR / key / f"{year}.xml.gz", url, xpath)
        data["年度"] = year
        data = data.rename(
            columns={
                "地區別_District_or_region": "按地區別分_District_or_region",
                "高級中等_高中_高職_女_Senior_high_school_regular_and_vocational_Senior_high_and_vocational_Female_百分比": "高級中等_高中_高職_女_Senior_high_school_regular_and_vocational_Female_百分比",
            }
        )
        df.append(data)
    df = pd.concat(df, ignore_index=True)
    df["按地區別分_District_or_region"] = (
        df["按地區別分_District_or_region"].str.strip().str.replace(r"[a-zA-Z ]+", "", regex=True)
    )
    df.columns = df.columns.str.replace(r"_[_a-zA-Z]+_百分比", "", regex=True)
    num_columns = [
        column for column in df.columns if "年度" not in column and "地區別分" not in column
    ]
    df.loc[:, num_columns] = df.loc[:, num_columns].replace("-", np.nan).astype(float) / 100

    return df


# https://data.gov.tw/dataset/32743 歷年年齡組別失業率
def df_歷年年齡組別失業率():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/234748/mp04031.xml"
    xpath = "//歷年年齡組別失業率"

    df = read_xml(url, xpath)
    df = df.set_index("年月別_Year_and_month")
    df = df.filter(regex=r"\d{4}$", axis="index")
    df.index = df.index.str.replace(r".*(\d{4})$", r"\1", regex=True)
    df.columns = df.columns.str.replace(
        r"_(合計|小計|男|女).*_百分比$", r"_\1", regex=True
    ).str.replace(r"_[a-zA-Z]+_百分比$", "", regex=True)
    df = df.replace("-", np.nan)
    df = df.astype(float) / 100

    return df


# https://data.gov.tw/dataset/34117 年齡組別失業率
def df_年齡組別失業率():
    key = "年齡組別失業率"
    key = sanitize_filename(key)
    urls = {
        104: "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/231813/Mp04048.xml",
        105: "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/231813/Mp04048A105.xml",
        106: "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/231813/Mp04048A106.xml",
        107: "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/231813/Mp04048A107.xml",
        108: "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/231813/Mp04048A108.xml",
        109: "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/231813/Mp04048A109.xml",
        110: "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/231813/Mp04048A110.xml",
        111: "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/231813/mp04048a111.xml",
        112: "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/233304/mp04048a112.xml",
        113: "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/234748/mp04048a113.xml",
    }
    xpath = "//年齡組別失業率"

    if max(urls.keys()) + 1911 + 1 < datetime.now().year and datetime.now().month > 4:
        print(f"請更新 {key}")

    df = []
    for year, url in urls.items():
        data = read_xml_with_cache(EXTRA_DATA_DIR / key / f"{year}.xml.gz", url, xpath)
        data["年度"] = year
        data = data.rename(
            columns={
                "地區別_District_or_region": "按地區別分_District_or_region",
                "年齡65歲以上_男_65_years_and_above_Male_百分比": "年齡65歲以上_男_65_years_and_over_Male_百分比",
                "年齡65歲以上_女_65_years_and_above_Female_百分比": "年齡65歲以上_女_65_years_and_over_Female_百分比",
                "年齡65歲以上_合計_65_years_and_above_Total_百分比": "年齡65歲以上_合計_65_years_and_over_Total_百分比",
            }
        )
        df.append(data)
    df = pd.concat(df, ignore_index=True)
    df["按地區別分_District_or_region"] = (
        df["按地區別分_District_or_region"].str.strip().str.replace(r"[a-zA-Z ]+", "", regex=True)
    )
    df.columns = df.columns.str.replace(
        r"_(合計|小計|男|女).*_百分比$", r"_\1", regex=True
    ).str.replace(r"_[a-zA-Z]+_百分比$", "", regex=True)

    num_columns = [
        column for column in df.columns if "年度" not in column and "地區別分" not in column
    ]
    df.loc[:, num_columns] = df.loc[:, num_columns].replace("-", np.nan).astype(float) / 100

    return df


# https://data.gov.tw/dataset/37971 教育程度別失業率—按年齡分
def df_教育程度別失業率_按年齡分():
    key = "教育程度別失業率—按年齡分"
    key = sanitize_filename(key)
    urls = {
        104: "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/231804/Mp04068.xml",
        105: "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/231804/Mp04068A105.xml",
        106: "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/231804/Mp04068A106.xml",
        107: "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/231804/Mp04068A107.xml",
        108: "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/231804/Mp04068A108.xml",
        109: "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/231804/Mp04068A109.xml",
        110: "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/231804/Mp04068A110.xml",
        111: "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/231804/mp04068a111.xml",
        112: "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/233304/mp04068a112.xml",
        113: "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/234748/mp04068a113.xml",
    }
    xpath = "//教育程度別失業率_按年齡分"

    if max(urls.keys()) + 1911 + 1 < datetime.now().year and datetime.now().month > 4:
        print(f"請更新 {key}")

    df = []
    for year, url in urls.items():
        data = read_xml_with_cache(EXTRA_DATA_DIR / key / f"{year}.xml.gz", url, xpath)
        data["年度"] = year
        data = data[~data["項目別_Item"].str.contains("按")]
        df.append(data)
    df = pd.concat(df, ignore_index=True)

    df["項目別_Item"] = (
        df["項目別_Item"]
        .str.strip()
        .str.replace(r"[a-zA-Z ]+", "", regex=True)
        .str.replace("～", "~")
        .str.replace("０", "0")
        .str.replace("１", "1")
        .str.replace("２", "2")
        .str.replace("３", "3")
        .str.replace("４", "4")
        .str.replace("５", "5")
        .str.replace("６", "6")
        .str.replace("９", "9")
    )
    df.columns = df.columns.str.replace(r"_[a-zA-Z_]+_百分比$", "", regex=True)

    num_columns = [
        column for column in df.columns if "年度" not in column and "項目別" not in column
    ]
    df.loc[:, num_columns] = df.loc[:, num_columns].replace("-", np.nan).astype(float) / 100

    return df


# https://data.gov.tw/dataset/151323 就業率
def df_就業率():
    url = "https://www.gender.ey.gov.tw/GecDB/Common/OpenXML.ashx?sn=$mQvpHYEayTTt8pmhMjRvA@@"
    xpath = "//DataTable"

    df = read_xml(url, xpath)
    df["Period"] /= 100
    df["Period"] = df["Period"].astype(int)
    df = df.pivot_table(values="Val", columns=["Category1Title", "Category2Title"], index="Period")
    df.columns = [f"{kind}_{edu}" for kind, edu in df.columns]
    df = df.replace("-", np.nan)
    df = df.astype(float) / 100

    return df


# https://data.gov.tw/dataset/44232 國民所得統計-國民所得、儲蓄與投資-季
def df_國民所得統計_國民所得_儲蓄與投資_季():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/230514/na8201a1q.xml"
    xpath = "//Obs"

    df = read_xml(url, xpath)
    df = df.fillna(0)

    return df


# https://data.gov.tw/dataset/6799 國民所得統計-常用資料-季
def df_國民所得統計_常用資料_季():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/230514/na8101a1q.xml"
    xpath = "//Obs"

    df = read_xml(url, xpath)

    return df


# https://data.gov.tw/dataset/6429 家庭收支調查-家庭收支重要指標
def df_家庭收支調查_家庭收支重要指標():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/001-%E5%AE%B6%E5%BA%AD%E6%94%B6%E6%94%AF%E9%87%8D%E8%A6%81%E6%8C%87%E6%A8%99.csv"
    index_col = "年"

    df = read_csv(url)

    df = df.set_index(index_col)

    return df


# https://data.gov.tw/dataset/9421 家庭收支調查-所得總額按來源別分
def df_家庭收支調查_所得總額按來源別分():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/013-%E6%89%80%E5%BE%97%E7%B8%BD%E9%A1%8D%E6%8C%89%E4%BE%86%E6%BA%90%E5%88%A5%E5%88%86.csv"
    columns_remove_patt = "-億元"
    index_col = "年"

    df = read_csv(url)

    億元_columns = [col for col in df.columns if "億元" in col]
    df[億元_columns] = df[億元_columns] * 100000000

    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/24137 家庭收支調查-所得總額與可支配所得
def df_家庭收支調查_所得總額與可支配所得():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/034-%E6%89%80%E5%BE%97%E7%B8%BD%E9%A1%8D%E8%88%87%E5%8F%AF%E6%94%AF%E9%85%8D%E6%89%80%E5%BE%97.csv"
    index_col = "年"

    df = read_csv(url)

    億元_columns = [col for col in df.columns if "億元" in col]
    df[億元_columns] = df[億元_columns] * 100000000

    df = df.set_index(index_col)
    df.columns = df.columns.str.replace("-億元", "", regex=True)
    df.columns = df.columns.str.replace("-元", "", regex=True)

    return df


# https://data.gov.tw/dataset/22230 家庭收支調查-所得收入者人數與按年齡組別及性別之分配
def df_家庭收支調查_所得收入者人數與按年齡組別及性別之分配():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/029-%E6%89%80%E5%BE%97%E6%94%B6%E5%85%A5%E8%80%85%E4%BA%BA%E6%95%B8%E8%88%87%E6%8C%89%E5%B9%B4%E9%BD%A1%E7%B5%84%E5%88%A5%E5%8F%8A%E6%80%A7%E5%88%A5%E4%B9%8B%E5%88%86%E9%85%8D.csv"
    index_col = "年"

    df = read_csv(url)

    df = df.set_index(index_col)
    df.columns = df.columns.str.replace("-百分比", "", regex=True)

    df[df.columns[1:]] = df[df.columns[1:]] / 100

    return df


# https://data.gov.tw/dataset/24140 家庭收支調查-戶內人數別平均每戶所得總額
def df_家庭收支調查_戶內人數別平均每戶所得總額():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/037-%E6%88%B6%E5%85%A7%E4%BA%BA%E6%95%B8%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E6%89%80%E5%BE%97%E7%B8%BD%E9%A1%8D.csv"
    columns_remove_patt = "-元"
    index_col = "年"

    df = read_csv(url)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/24141 家庭收支調查-戶內人數別平均每戶所得收入總計
def df_家庭收支調查_戶內人數別平均每戶所得收入總計():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/038-%E6%88%B6%E5%85%A7%E4%BA%BA%E6%95%B8%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E6%89%80%E5%BE%97%E6%94%B6%E5%85%A5%E7%B8%BD%E8%A8%88.csv"
    columns_remove_patt = "-元"
    index_col = "年"

    df = read_csv(url)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/24734 家庭收支調查-戶內人數別平均每戶非消費支出
def df_家庭收支調查_戶內人數別平均每戶非消費支出():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/041-%E6%88%B6%E5%85%A7%E4%BA%BA%E6%95%B8%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E9%9D%9E%E6%B6%88%E8%B2%BB%E6%94%AF%E5%87%BA.csv"
    columns_remove_patt = "-元"
    index_col = "年"

    df = read_csv(url)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/24138 家庭收支調查-戶內人數別平均每戶可支配所得
def df_家庭收支調查_戶內人數別平均每戶可支配所得():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/035-%E6%88%B6%E5%85%A7%E4%BA%BA%E6%95%B8%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E5%8F%AF%E6%94%AF%E9%85%8D%E6%89%80%E5%BE%97.csv"
    columns_remove_patt = "-元"
    index_col = "年"

    df = read_csv(url)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/24733 家庭收支調查-戶內人數別平均每戶消費支出
def df_家庭收支調查_戶內人數別平均每戶消費支出():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/040-%E6%88%B6%E5%85%A7%E4%BA%BA%E6%95%B8%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E6%B6%88%E8%B2%BB%E6%94%AF%E5%87%BA.csv"
    columns_remove_patt = "-元"
    index_col = "年"

    df = read_csv(url)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/24735 家庭收支調查-戶內人數別平均每戶經常性支出
def df_家庭收支調查_戶內人數別平均每戶經常性支出():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/042-%E6%88%B6%E5%85%A7%E4%BA%BA%E6%95%B8%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E7%B6%93%E5%B8%B8%E6%80%A7%E6%94%AF%E5%87%BA.csv"
    columns_remove_patt = "-元"
    index_col = "年"

    df = read_csv(url)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/24139 家庭收支調查-戶內人數別平均每戶儲蓄
def df_家庭收支調查_戶內人數別平均每戶儲蓄():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/036-%E6%88%B6%E5%85%A7%E4%BA%BA%E6%95%B8%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E5%84%B2%E8%93%84.csv"
    columns_remove_patt = "-元"
    index_col = "年"

    df = read_csv(url)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/24731 家庭收支調查-戶內人數別平均每戶受僱人員報酬
def df_家庭收支調查_戶內人數別平均每戶受僱人員報酬():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/039-%E6%88%B6%E5%85%A7%E4%BA%BA%E6%95%B8%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E5%8F%97%E5%83%B1%E4%BA%BA%E5%93%A1%E5%A0%B1%E9%85%AC.csv"
    columns_remove_patt = "-元"
    index_col = "年"

    df = read_csv(url)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/9559 家庭收支調查-家庭戶數按戶內人口規模別之分配
def df_家庭收支調查_家庭戶數按戶內人口規模別之分配():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/017-%E5%AE%B6%E5%BA%AD%E6%88%B6%E6%95%B8%E6%8C%89%E6%88%B6%E5%85%A7%E4%BA%BA%E5%8F%A3%E8%A6%8F%E6%A8%A1%E5%88%A5%E4%B9%8B%E5%88%86%E9%85%8D.csv"
    columns_remove_patt = r"\(%\)"
    index_col = "年"

    df = read_csv(url)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    df = df / 100

    return df


# https://data.gov.tw/dataset/22236 家庭收支調查-農家平均每戶所得總額按來自農業與非農業分
def df_家庭收支調查_農家平均每戶所得總額按來自農業與非農業分():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/032-%E8%BE%B2%E5%AE%B6%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E6%89%80%E5%BE%97%E7%B8%BD%E9%A1%8D%E6%8C%89%E4%BE%86%E8%87%AA%E8%BE%B2%E6%A5%AD%E8%88%87%E9%9D%9E%E8%BE%B2%E6%A5%AD%E5%88%86.csv"
    columns_remove_patt = "-元"
    index_col = "年"

    df = read_csv(url)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/24736 家庭收支調查-農家與非農家平均每戶可支配所得、所得總額及儲蓄
def df_家庭收支調查_農家與非農家平均每戶可支配所得_所得總額及儲蓄():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/043-%E8%BE%B2%E5%AE%B6%E8%88%87%E9%9D%9E%E8%BE%B2%E5%AE%B6%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E5%8F%AF%E6%94%AF%E9%85%8D%E6%89%80%E5%BE%97%E3%80%81%E6%89%80%E5%BE%97%E7%B8%BD%E9%A1%8D%E5%8F%8A%E5%84%B2%E8%93%84.csv"
    columns_remove_patt = "-元"
    index_col = "年"

    df = read_csv(url)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/27962 家庭收支調查-農家與非農家平均每戶消費支出、非消費支出及經常性支出
def df_家庭收支調查_農家與非農家平均每戶消費支出_非消費支出及經常性支出():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/045-%E8%BE%B2%E5%AE%B6%E8%88%87%E9%9D%9E%E8%BE%B2%E5%AE%B6%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E6%B6%88%E8%B2%BB%E6%94%AF%E5%87%BA%E3%80%81%E9%9D%9E%E6%B6%88%E8%B2%BB%E6%94%AF%E5%87%BA%E5%8F%8A%E7%B6%93%E5%B8%B8%E6%80%A7%E6%94%AF%E5%87%BA.csv"
    columns_remove_patt = "-元"
    index_col = "年"

    df = read_csv(url)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/24737 家庭收支調查-農家與非農家平均每戶所得收入總計及受僱人員報酬
def df_家庭收支調查_農家與非農家平均每戶所得收入總計及受僱人員報酬():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/044-%E8%BE%B2%E5%AE%B6%E8%88%87%E9%9D%9E%E8%BE%B2%E5%AE%B6%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E6%89%80%E5%BE%97%E6%94%B6%E5%85%A5%E7%B8%BD%E8%A8%88%E5%8F%8A%E5%8F%97%E5%83%B1%E4%BA%BA%E5%93%A1%E5%A0%B1%E9%85%AC.csv"
    columns_remove_patt = "-元"
    index_col = "年"

    df = read_csv(url)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/93951 家庭收支調查-戶數五等分位之平均每戶所得總額
def df_家庭收支調查_戶數五等分位之平均每戶所得總額():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/067-%E6%88%B6%E6%95%B8%E4%BA%94%E7%AD%89%E5%88%86%E4%BD%8D%E7%B5%84%E4%B9%8B%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E6%89%80%E5%BE%97%E7%B8%BD%E9%A1%8D.csv"
    columns_remove_patt = "可支配所得按戶數五等分位組之平均每戶所得總額-|-元"
    index_col = "年"

    df = read_csv(url)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/94752 家庭收支調查-戶數五等分位之平均每戶所得收入總計
def df_家庭收支調查_戶數五等分位之平均每戶所得收入總計():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/068-%E6%88%B6%E6%95%B8%E4%BA%94%E7%AD%89%E5%88%86%E4%BD%8D%E7%B5%84%E4%B9%8B%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E6%89%80%E5%BE%97%E6%94%B6%E5%85%A5%E7%B8%BD%E8%A8%88.csv"
    columns_remove_patt = "可支配所得按戶數五等分位組之平均每戶所得收入總計-|-元"
    index_col = "年"

    df = read_csv(url)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/98835 家庭收支調查-戶數五等分位之平均每戶非消費支出
def df_家庭收支調查_戶數五等分位之平均每戶非消費支出():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/071-%E6%88%B6%E6%95%B8%E4%BA%94%E7%AD%89%E5%88%86%E4%BD%8D%E7%B5%84%E4%B9%8B%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E9%9D%9E%E6%B6%88%E8%B2%BB%E6%94%AF%E5%87%BA.csv"
    columns_remove_patt = "可支配所得按戶數五等分位組之平均每戶非消費支出-|-元"
    index_col = "年"

    df = read_csv(url)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/91925 家庭收支調查-戶數五等分位之平均每戶可支配所得
def df_家庭收支調查_戶數五等分位之平均每戶可支配所得():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/065-%E6%88%B6%E6%95%B8%E4%BA%94%E7%AD%89%E5%88%86%E4%BD%8D%E7%B5%84%E4%B9%8B%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E5%8F%AF%E6%94%AF%E9%85%8D%E6%89%80%E5%BE%97.csv"
    columns_remove_patt = "可支配所得按戶數五等分位組-|-元"
    index_col = "年"

    df = read_csv(url)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/98834 家庭收支調查-戶數五等分位之平均每戶消費支出
def df_家庭收支調查_戶數五等分位之平均每戶消費支出():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/070-%E6%88%B6%E6%95%B8%E4%BA%94%E7%AD%89%E5%88%86%E4%BD%8D%E7%B5%84%E4%B9%8B%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E6%B6%88%E8%B2%BB%E6%94%AF%E5%87%BA.csv"
    columns_remove_patt = "可支配所得按戶數五等分位組之平均每戶消費支出-|-元"
    index_col = "年"

    df = read_csv(url)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/101338 家庭收支調查-戶數五等分位之平均每戶經常性支出
def df_家庭收支調查_戶數五等分位之平均每戶經常性支出():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/072-%E6%88%B6%E6%95%B8%E4%BA%94%E7%AD%89%E5%88%86%E4%BD%8D%E7%B5%84%E4%B9%8B%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E7%B6%93%E5%B8%B8%E6%80%A7%E6%94%AF%E5%87%BA.csv"
    columns_remove_patt = "可支配所得按戶數五等分位組之平均每戶經常性支出-|-元"
    index_col = "年"

    df = read_csv(url)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/93950 家庭收支調查-戶數五等分位之平均每戶儲蓄
def df_家庭收支調查_戶數五等分位之平均每戶儲蓄():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/066-%E6%88%B6%E6%95%B8%E4%BA%94%E7%AD%89%E5%88%86%E4%BD%8D%E7%B5%84%E4%B9%8B%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E5%84%B2%E8%93%84.csv"
    columns_remove_patt = "可支配所得按戶數五等分位組之平均每戶儲蓄-|-元"
    index_col = "年"

    df = read_csv(url)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/94754 家庭收支調查-戶數五等分位之平均每戶受僱人員報酬
def df_家庭收支調查_戶數五等分位之平均每戶受僱人員報酬():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/069-%E6%88%B6%E6%95%B8%E4%BA%94%E7%AD%89%E5%88%86%E4%BD%8D%E7%B5%84%E4%B9%8B%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E5%8F%97%E5%83%B1%E4%BA%BA%E5%93%A1%E5%A0%B1%E9%85%AC.csv"
    columns_remove_patt = "可支配所得按戶數五等分位組之平均每戶受僱人員報酬-|-元"
    index_col = "年"

    df = read_csv(url)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/9424 家庭收支調查-戶數五等分位組之平均每戶人口數
def df_家庭收支調查_戶數五等分位組之平均每戶人口數():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/019-%E6%88%B6%E6%95%B8%E4%BA%94%E7%AD%89%E5%88%86%E4%BD%8D%E7%B5%84%E4%B9%8B%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E4%BA%BA%E5%8F%A3%E6%95%B8.csv"
    columns_remove_patt = "-人"
    index_col = "年"

    df = read_csv(url)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/9425 家庭收支調查-戶數五等分位組之平均每戶就業人數
def df_家庭收支調查_戶數五等分位組之平均每戶就業人數():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/020-%E6%88%B6%E6%95%B8%E4%BA%94%E7%AD%89%E5%88%86%E4%BD%8D%E7%B5%84%E4%B9%8B%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E5%B0%B1%E6%A5%AD%E4%BA%BA%E6%95%B8.csv"
    columns_remove_patt = "-人"
    index_col = "年"

    df = read_csv(url)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/9422 家庭收支調查-戶數十等分位組分界點之可支配所得
def df_家庭收支調查_戶數十等分位組分界點之可支配所得():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/014-%E6%88%B6%E6%95%B8%E5%8D%81%E7%AD%89%E5%88%86%E4%BD%8D%E7%B5%84%E5%88%86%E7%95%8C%E9%BB%9E%E4%B9%8B%E5%8F%AF%E6%94%AF%E9%85%8D%E6%89%80%E5%BE%97.csv"
    columns_remove_patt = "-元"
    index_col = "年"

    df = read_csv(url)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/132285 家庭收支調查-所得收入者五等分位平均每人可支配所得
def df_家庭收支調查_所得收入者五等分位平均每人可支配所得():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/089-%E6%89%80%E5%BE%97%E6%94%B6%E5%85%A5%E8%80%85%E4%BA%94%E7%AD%89%E5%88%86%E4%BD%8D%E5%B9%B3%E5%9D%87%E6%AF%8F%E4%BA%BA%E5%8F%AF%E6%94%AF%E9%85%8D%E6%89%80%E5%BE%97.csv"
    columns_remove_patt = "可支配所得按所得收入者人數五等分位組之平均每人可支配所得-"
    index_col = "年"

    df = read_csv(url)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/11573 家庭收支調查-所得收入者十等分位組分界點之可支配所得
def df_家庭收支調查_所得收入者十等分位組分界點之可支配所得():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/024-%E6%89%80%E5%BE%97%E6%94%B6%E5%85%A5%E8%80%85%E5%8D%81%E7%AD%89%E5%88%86%E4%BD%8D%E7%B5%84%E5%88%86%E7%95%8C%E9%BB%9E%E4%B9%8B%E5%8F%AF%E6%94%AF%E9%85%8D%E6%89%80%E5%BE%97.csv"
    columns_remove_patt = "-元"
    index_col = "年"

    df = read_csv(url)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/54825 家庭收支調查-家庭組織型態別平均每戶所得總額
def df_家庭收支調查_家庭組織型態別平均每戶所得總額():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/058-%E5%AE%B6%E5%BA%AD%E7%B5%84%E7%B9%94%E5%9E%8B%E6%85%8B%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E6%89%80%E5%BE%97%E7%B8%BD%E9%A1%8D.csv"
    columns_remove_patt = "-元"
    index_col = "年"

    df = read_csv(url)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/56525 家庭收支調查-家庭組織型態別平均每戶所得收入總計
def df_家庭收支調查_家庭組織型態別平均每戶所得收入總計():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/059-%E5%AE%B6%E5%BA%AD%E7%B5%84%E7%B9%94%E5%9E%8B%E6%85%8B%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E6%89%80%E5%BE%97%E6%94%B6%E5%85%A5%E7%B8%BD%E8%A8%88.csv"
    columns_remove_patt = "-元"
    index_col = "年"

    df = read_csv(url)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/79207 家庭收支調查-家庭組織型態別平均每戶非消費支出
def df_家庭收支調查_家庭組織型態別平均每戶非消費支出():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/062-%E5%AE%B6%E5%BA%AD%E7%B5%84%E7%B9%94%E5%9E%8B%E6%85%8B%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E9%9D%9E%E6%B6%88%E8%B2%BB%E6%94%AF%E5%87%BA.csv"
    columns_remove_patt = "-元"
    index_col = "年"

    df = read_csv(url)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/45126 家庭收支調查-家庭組織型態別平均每戶可支配所得
def df_家庭收支調查_家庭組織型態別平均每戶可支配所得():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/056-%E5%AE%B6%E5%BA%AD%E7%B5%84%E7%B9%94%E5%9E%8B%E6%85%8B%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E5%8F%AF%E6%94%AF%E9%85%8D%E6%89%80%E5%BE%97.csv"
    columns_remove_patt = "-元"
    index_col = "年"

    df = read_csv(url)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/76265 家庭收支調查-家庭組織型態別平均每戶消費支出
def df_家庭收支調查_家庭組織型態別平均每戶消費支出():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/061-%E5%AE%B6%E5%BA%AD%E7%B5%84%E7%B9%94%E5%9E%8B%E6%85%8B%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E6%B6%88%E8%B2%BB%E6%94%AF%E5%87%BA.csv"
    columns_remove_patt = "-元"
    index_col = "年"

    df = read_csv(url)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/79208 家庭收支調查-家庭組織型態別平均每戶經常性支出
def df_家庭收支調查_家庭組織型態別平均每戶經常性支出():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/063-%E5%AE%B6%E5%BA%AD%E7%B5%84%E7%B9%94%E5%9E%8B%E6%85%8B%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E7%B6%93%E5%B8%B8%E6%80%A7%E6%94%AF%E5%87%BA.csv"
    columns_remove_patt = "-元"
    index_col = "年"

    df = read_csv(url)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/54823 家庭收支調查-家庭組織型態別平均每戶儲蓄
def df_家庭收支調查_家庭組織型態別平均每戶儲蓄():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/057-%E5%AE%B6%E5%BA%AD%E7%B5%84%E7%B9%94%E5%9E%8B%E6%85%8B%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E5%84%B2%E8%93%84.csv"
    columns_remove_patt = "-元"
    index_col = "年"

    df = read_csv(url)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# hhttps://data.gov.tw/dataset/76263 家庭收支調查-家庭組織型態別平均每戶受僱人員報酬
def df_家庭收支調查_家庭組織型態別平均每戶受僱人員報酬():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/060-%E5%AE%B6%E5%BA%AD%E7%B5%84%E7%B9%94%E5%9E%8B%E6%85%8B%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E5%8F%97%E5%83%B1%E4%BA%BA%E5%93%A1%E5%A0%B1%E9%85%AC.csv"
    columns_remove_patt = "-元"
    index_col = "年"

    df = read_csv(url)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/27963 家庭收支調查-性別統計指標-家戶狀況按家庭組織型態分
def df_家庭收支調查_性別統計指標_家戶狀況按家庭組織型態分():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/046-%E6%80%A7%E5%88%A5%E7%B5%B1%E8%A8%88%E6%8C%87%E6%A8%99-%E5%AE%B6%E6%88%B6%E7%8B%80%E6%B3%81%E6%8C%89%E5%AE%B6%E5%BA%AD%E7%B5%84%E7%B9%94%E5%9E%8B%E6%85%8B%E5%88%86.csv"
    columns_remove_patt = "戶數"
    index_col = "年"

    df = read_csv(url)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/9417 家庭收支調查-各縣市別平均每戶所得總額
def df_家庭收支調查_各縣市別平均每戶所得總額():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/008-%E5%90%84%E7%B8%A3%E5%B8%82%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E6%89%80%E5%BE%97%E7%B8%BD%E9%A1%8D.csv"
    columns_remove_patt = "-元"
    index_col = "年"

    df = read_csv(url)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/9418 家庭收支調查-各縣市別平均每戶所得收入總計
def df_家庭收支調查_各縣市別平均每戶所得收入總計():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/009-%E5%90%84%E7%B8%A3%E5%B8%82%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E6%89%80%E5%BE%97%E6%94%B6%E5%85%A5%E7%B8%BD%E8%A8%88.csv"
    columns_remove_patt = "-元"
    index_col = "年"

    df = read_csv(url)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/9804 家庭收支調查-各縣市別平均每戶非消費支出
def df_家庭收支調查_各縣市別平均每戶非消費支出():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/021-%E5%90%84%E7%B8%A3%E5%B8%82%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E9%9D%9E%E6%B6%88%E8%B2%BB%E6%94%AF%E5%87%BA.csv"
    columns_remove_patt = "-元"
    index_col = "年"

    df = read_csv(url)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/9415 家庭收支調查-各縣市別平均每戶可支配所得
def df_家庭收支調查_各縣市別平均每戶可支配所得():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/006-%E5%90%84%E7%B8%A3%E5%B8%82%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E5%8F%AF%E6%94%AF%E9%85%8D%E6%89%80%E5%BE%97.csv"
    columns_remove_patt = "-元"
    index_col = "年"

    df = read_csv(url)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/9420 家庭收支調查-各縣市別平均每戶消費支出
def df_家庭收支調查_各縣市別平均每戶消費支出():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/011-%E5%90%84%E7%B8%A3%E5%B8%82%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E6%B6%88%E8%B2%BB%E6%94%AF%E5%87%BA.csv"
    columns_remove_patt = "-元"
    index_col = "年"

    df = read_csv(url)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/9805 家庭收支調查-各縣市別平均每戶經常性支出
def df_家庭收支調查_各縣市別平均每戶經常性支出():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/022-%E5%90%84%E7%B8%A3%E5%B8%82%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E7%B6%93%E5%B8%B8%E6%80%A7%E6%94%AF%E5%87%BA.csv"
    columns_remove_patt = "-元"
    index_col = "年"

    df = read_csv(url)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/9416 家庭收支調查-各縣市別平均每戶儲蓄
def df_家庭收支調查_各縣市別平均每戶儲蓄():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/007-%E5%90%84%E7%B8%A3%E5%B8%82%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E5%84%B2%E8%93%84.csv"
    columns_remove_patt = "-元"
    index_col = "年"

    df = read_csv(url)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/9419 家庭收支調查-各縣市別平均每戶受僱人員報酬
def df_家庭收支調查_各縣市別平均每戶受僱人員報酬():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/010-%E5%90%84%E7%B8%A3%E5%B8%82%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E5%8F%97%E5%83%B1%E4%BA%BA%E5%93%A1%E5%A0%B1%E9%85%AC.csv"
    columns_remove_patt = "-元"
    index_col = "年"

    df = read_csv(url)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/117930 家庭收支調查-所得收入者各縣市別平均每人所得收入總計
def df_家庭收支調查_所得收入者各縣市別平均每人所得收入總計():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/081-%E6%89%80%E5%BE%97%E6%94%B6%E5%85%A5%E8%80%85%E5%90%84%E7%B8%A3%E5%B8%82%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E4%BA%BA%E6%89%80%E5%BE%97%E6%94%B6%E5%85%A5%E7%B8%BD%E8%A8%88.csv"
    columns_remove_patt = "-元"
    index_col = "年"

    df = read_csv(url)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/119937 家庭收支調查-所得收入者各縣市別平均每人非消費支出
def df_家庭收支調查_所得收入者各縣市別平均每人非消費支出():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/083-%E6%89%80%E5%BE%97%E6%94%B6%E5%85%A5%E8%80%85%E5%90%84%E7%B8%A3%E5%B8%82%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E4%BA%BA%E9%9D%9E%E6%B6%88%E8%B2%BB%E6%94%AF%E5%87%BA.csv"
    columns_remove_patt = "-元"
    index_col = "年"

    df = read_csv(url)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/117929 家庭收支調查-所得收入者各縣市別平均每人可支配所得
def df_家庭收支調查_所得收入者各縣市別平均每人可支配所得():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/080-%E6%89%80%E5%BE%97%E6%94%B6%E5%85%A5%E8%80%85%E5%90%84%E7%B8%A3%E5%B8%82%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E4%BA%BA%E5%8F%AF%E6%94%AF%E9%85%8D%E6%89%80%E5%BE%97.csv"
    columns_remove_patt = "-元"
    index_col = "年"

    df = read_csv(url)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/119936 家庭收支調查-所得收入者各縣市別平均每人受僱人員報酬
def df_家庭收支調查_所得收入者各縣市別平均每人受僱人員報酬():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/082-%E6%89%80%E5%BE%97%E6%94%B6%E5%85%A5%E8%80%85%E5%90%84%E7%B8%A3%E5%B8%82%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E4%BA%BA%E5%8F%97%E5%83%B1%E4%BA%BA%E5%93%A1%E5%A0%B1%E9%85%AC.csv"
    columns_remove_patt = "-元"
    index_col = "年"

    df = read_csv(url)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/144913 家庭收支調查-性別統計指標-可支配所得按戶數五等分位經濟戶長性別比率
def df_家庭收支調查_性別統計指標_可支配所得按戶數五等分位經濟戶長性別比率():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/090-%E6%80%A7%E5%88%A5%E7%B5%B1%E8%A8%88%E6%8C%87%E6%A8%99-%E5%8F%AF%E6%94%AF%E9%85%8D%E6%89%80%E5%BE%97%E6%8C%89%E6%88%B6%E6%95%B8%E4%BA%94%E7%AD%89%E5%88%86%E4%BD%8D%E7%B6%93%E6%BF%9F%E6%88%B6%E9%95%B7%E6%80%A7%E5%88%A5%E6%AF%94%E7%8E%87.csv"
    columns_remove_patt = "經濟戶長比率"
    index_col = "年"

    df = read_csv(url)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    df = df / 100

    return df


# https://data.gov.tw/dataset/91919 家庭收支調查-性別統計指標-戶數與結構按經濟戶長性別分
def df_家庭收支調查_性別統計指標_戶數與結構按經濟戶長性別分():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/064-%E6%80%A7%E5%88%A5%E7%B5%B1%E8%A8%88%E6%8C%87%E6%A8%99-%E6%88%B6%E6%95%B8%E8%88%87%E7%B5%90%E6%A7%8B%E6%8C%89%E7%B6%93%E6%BF%9F%E6%88%B6%E9%95%B7%E6%80%A7%E5%88%A5%E5%88%86.csv"
    columns_remove_patt = "經濟戶長"
    index_col = "年"

    df = read_csv(url)
    df = df.set_index(index_col)

    df["男性經濟戶長比率"] = df["男性經濟戶長比率"] / 100
    df["女性經濟戶長比率"] = df["女性經濟戶長比率"] / 100

    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/111591 家庭收支調查-家庭戶數按所得總額組別及經濟戶長性別分
def df_家庭收支調查_家庭戶數按所得總額組別及經濟戶長性別分():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/079-%E5%AE%B6%E5%BA%AD%E6%88%B6%E6%95%B8%E6%8C%89%E6%89%80%E5%BE%97%E7%B8%BD%E9%A1%8D%E7%B5%84%E5%88%A5%E5%8F%8A%E7%B6%93%E6%BF%9F%E6%88%B6%E9%95%B7%E6%80%A7%E5%88%A5%E5%88%86.csv"
    index_col = "所得總額組別"

    df = read_csv(url)
    df = df.set_index(index_col)

    df.columns = df.columns.str.replace("-戶數", "", regex=True)

    return df, datetime.today().year - 2


# https://www.stat.gov.tw/cp.aspx?n=3913 主計總處統計專區 -> 家庭收支調查 -> 統計表 -> 性別指標
def df_家庭收支調查_所得收入者人數按性別及可支配所得組別分():
    key = "家庭收支調查-所得收入者人數按性別及可支配所得組別分"
    url = "https://ws.dgbas.gov.tw/001/Upload/463/relfile/10315/2015/gender112.xls"
    year = 112

    df = read_excel_with_cache(
        EXTRA_DATA_DIR / key / f"{year}.xlsx.gz",
        url,
        sheet_name=10,
        skiprows=3,
        nrows=44,
        usecols=range(0, 4),
    )

    df.columns = ["可支配所得組別", "合計", "男", "女"]
    df = df.dropna()
    df["合計"] = df["合計"].astype(int)
    df["男"] = df["男"].astype(int)
    df["女"] = df["女"].astype(int)

    df = df.set_index("可支配所得組別")

    if (
        year + 1911 + 1 < datetime.now().year
        and datetime.now().month > 7
        and datetime.now().day > 20
    ):
        print(f"請更新 {key}")

    return df, year + 1911


# https://data.gov.tw/dataset/108265 家庭收支調查-平均每戶可支配所得及消費支出依可支配所得按戶數五等分位分及經濟戶長年齡組別分
def df_家庭收支調查_平均每戶可支配所得及消費支出依可支配所得按戶數五等分位分及經濟戶長年齡組別分():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/074-%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E5%8F%AF%E6%94%AF%E9%85%8D%E6%89%80%E5%BE%97%E5%8F%8A%E6%B6%88%E8%B2%BB%E6%94%AF%E5%87%BA%E4%BE%9D%E5%8F%AF%E6%94%AF%E9%85%8D%E6%89%80%E5%BE%97%E6%8C%89%E6%88%B6%E6%95%B8%E4%BA%94%E7%AD%89%E5%88%86%E4%BD%8D%E5%88%86%E5%8F%8A%E7%B6%93%E6%BF%9F%E6%88%B6%E9%95%B7%E5%B9%B4%E9%BD%A1%E7%B5%84%E5%88%A5%E5%88%86.csv"
    index_col = "依可支配所得按戶數五等分位分"

    df = read_csv(url)
    df = df.set_index(index_col)

    df.columns = df.columns.str.replace("經濟戶長-元", "", regex=True)
    df.columns = df.columns.str.replace("-元", "", regex=True)

    return df, datetime.today().year - 2


# https://data.gov.tw/dataset/108266 家庭收支調查-平均每戶可支配所得及消費支出依可支配所得按戶數五等分位分及經濟戶長教育程度別分
def df_家庭收支調查_平均每戶可支配所得及消費支出依可支配所得按戶數五等分位分及經濟戶長教育程度別分():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/075-%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E5%8F%AF%E6%94%AF%E9%85%8D%E6%89%80%E5%BE%97%E5%8F%8A%E6%B6%88%E8%B2%BB%E6%94%AF%E5%87%BA%E4%BE%9D%E5%8F%AF%E6%94%AF%E9%85%8D%E6%89%80%E5%BE%97%E6%8C%89%E6%88%B6%E6%95%B8%E4%BA%94%E7%AD%89%E5%88%86%E4%BD%8D%E5%88%86%E5%8F%8A%E7%B6%93%E6%BF%9F%E6%88%B6%E9%95%B7%E6%95%99%E8%82%B2%E7%A8%8B%E5%BA%A6%E5%88%A5%E5%88%86.csv"
    index_col = "依可支配所得按戶數五等分位分"

    df = read_csv(url)
    df = df.set_index(index_col)

    df.columns = df.columns.str.replace("經濟戶長", "", regex=True)
    df.columns = df.columns.str.replace("-元", "", regex=True)

    return df, datetime.today().year - 2


# https://data.gov.tw/dataset/101340 家庭收支調查-平均每戶可支配所得及消費支出依可支配所得按戶數五等分位分及經濟戶長性別分
def df_家庭收支調查_平均每戶可支配所得及消費支出依可支配所得按戶數五等分位分及經濟戶長性別分():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/073-%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E5%8F%AF%E6%94%AF%E9%85%8D%E6%89%80%E5%BE%97%E5%8F%8A%E6%B6%88%E8%B2%BB%E6%94%AF%E5%87%BA%E4%BE%9D%E5%8F%AF%E6%94%AF%E9%85%8D%E6%89%80%E5%BE%97%E6%8C%89%E6%88%B6%E6%95%B8%E4%BA%94%E7%AD%89%E5%88%86%E4%BD%8D%E5%88%86%E5%8F%8A%E7%B6%93%E6%BF%9F%E6%88%B6%E9%95%B7%E6%80%A7%E5%88%A5%E5%88%86.csv"
    index_col = "依可支配所得按戶數五等分位分"

    df = read_csv(url)
    df = df.set_index(index_col)

    df.columns = df.columns.str.replace("經濟戶長", "", regex=True)
    df.columns = df.columns.str.replace("-元", "", regex=True)

    return df, datetime.today().year - 2


# https://data.gov.tw/dataset/45125 家庭收支調查-性別統計指標-經濟戶長人數按性別、教育程度及年齡分
def df_家庭收支調查_性別統計指標_經濟戶長人數按性別_教育程度及年齡分():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/055-%E6%80%A7%E5%88%A5%E7%B5%B1%E8%A8%88%E6%8C%87%E6%A8%99-%E7%B6%93%E6%BF%9F%E6%88%B6%E9%95%B7%E4%BA%BA%E6%95%B8%E6%8C%89%E6%80%A7%E5%88%A5%E3%80%81%E6%95%99%E8%82%B2%E7%A8%8B%E5%BA%A6%E5%8F%8A%E5%B9%B4%E9%BD%A1%E5%88%86.csv"
    columns_remove_patt = "-人"
    index_col = "年"

    df = read_csv(url)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/130026 家庭收支調查-性別統計指標-單親戶長人數按性別、教育程度及年齡分
def df_家庭收支調查_性別統計指標_單親戶長人數按性別_教育程度及年齡分():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/084-%E6%80%A7%E5%88%A5%E7%B5%B1%E8%A8%88%E6%8C%87%E6%A8%99-%E5%96%AE%E8%A6%AA%E6%88%B6%E9%95%B7%E4%BA%BA%E6%95%B8%E6%8C%89%E6%80%A7%E5%88%A5%E3%80%81%E6%95%99%E8%82%B2%E7%A8%8B%E5%BA%A6%E5%8F%8A%E5%B9%B4%E9%BD%A1%E5%88%86.csv"
    columns_remove_patt = "-人"
    index_col = "年"

    df = read_csv(url)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/36369 家庭收支調查-經濟戶長職業別平均每戶所得總額
def df_家庭收支調查_經濟戶長職業別平均每戶所得總額():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/049-%E7%B6%93%E6%BF%9F%E6%88%B6%E9%95%B7%E8%81%B7%E6%A5%AD%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E6%89%80%E5%BE%97%E7%B8%BD%E9%A1%8D.csv"
    columns_remove_patt = "-元"
    index_col = "年"

    df = read_csv(url)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/36370 家庭收支調查-經濟戶長職業別平均每戶所得收入總計
def df_家庭收支調查_經濟戶長職業別平均每戶所得收入總計():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/050-%E7%B6%93%E6%BF%9F%E6%88%B6%E9%95%B7%E8%81%B7%E6%A5%AD%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E6%89%80%E5%BE%97%E6%94%B6%E5%85%A5%E7%B8%BD%E8%A8%88.csv"
    columns_remove_patt = "-元"
    index_col = "年"

    df = read_csv(url)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/43462 家庭收支調查-經濟戶長職業別平均每戶非消費支出
def df_家庭收支調查_經濟戶長職業別平均每戶非消費支出():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/053-%E7%B6%93%E6%BF%9F%E6%88%B6%E9%95%B7%E8%81%B7%E6%A5%AD%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E9%9D%9E%E6%B6%88%E8%B2%BB%E6%94%AF%E5%87%BA.csv"
    columns_remove_patt = "-元"
    index_col = "年"

    df = read_csv(url)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/30901 家庭收支調查-經濟戶長職業別平均每戶可支配所得
def df_家庭收支調查_經濟戶長職業別平均每戶可支配所得():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/047-%E7%B6%93%E6%BF%9F%E6%88%B6%E9%95%B7%E8%81%B7%E6%A5%AD%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E5%8F%AF%E6%94%AF%E9%85%8D%E6%89%80%E5%BE%97.csv"
    columns_remove_patt = "-元"
    index_col = "年"

    df = read_csv(url)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/37450 家庭收支調查-經濟戶長職業別平均每戶消費支出
def df_家庭收支調查_經濟戶長職業別平均每戶消費支出():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/052-%E7%B6%93%E6%BF%9F%E6%88%B6%E9%95%B7%E8%81%B7%E6%A5%AD%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E6%B6%88%E8%B2%BB%E6%94%AF%E5%87%BA.csv"
    columns_remove_patt = "-元"
    index_col = "年"

    df = read_csv(url)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# hhttps://data.gov.tw/dataset/43463 家庭收支調查-經濟戶長職業別平均每戶經常性支出
def df_家庭收支調查_經濟戶長職業別平均每戶經常性支出():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/054-%E7%B6%93%E6%BF%9F%E6%88%B6%E9%95%B7%E8%81%B7%E6%A5%AD%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E7%B6%93%E5%B8%B8%E6%80%A7%E6%94%AF%E5%87%BA.csv"
    columns_remove_patt = "-元"
    index_col = "年"

    df = read_csv(url)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/30900 家庭收支調查-經濟戶長職業別平均每戶儲蓄
def df_家庭收支調查_經濟戶長職業別平均每戶儲蓄():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/048-%E7%B6%93%E6%BF%9F%E6%88%B6%E9%95%B7%E8%81%B7%E6%A5%AD%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E5%84%B2%E8%93%84.csv"
    columns_remove_patt = "-元"
    index_col = "年"

    df = read_csv(url)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/37444 家庭收支調查-經濟戶長職業別平均每戶受僱人員報酬
def df_家庭收支調查_經濟戶長職業別平均每戶受僱人員報酬():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/051-%E7%B6%93%E6%BF%9F%E6%88%B6%E9%95%B7%E8%81%B7%E6%A5%AD%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E6%88%B6%E5%8F%97%E5%83%B1%E4%BA%BA%E5%93%A1%E5%A0%B1%E9%85%AC.csv"
    columns_remove_patt = "-元"
    index_col = "年"

    df = read_csv(url)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/131148 家庭收支調查-所得收入者職業別平均每人所得收入總計
def df_家庭收支調查_所得收入者職業別平均每人所得收入總計():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/086-%E6%89%80%E5%BE%97%E6%94%B6%E5%85%A5%E8%80%85%E8%81%B7%E6%A5%AD%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E4%BA%BA%E6%89%80%E5%BE%97%E6%94%B6%E5%85%A5%E7%B8%BD%E8%A8%88.csv"
    columns_remove_patt = "-元"
    index_col = "年"

    df = read_csv(url)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/132281 家庭收支調查-所得收入者職業別平均每人非消費支出
def df_家庭收支調查_所得收入者職業別平均每人非消費支出():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/088-%E6%89%80%E5%BE%97%E6%94%B6%E5%85%A5%E8%80%85%E8%81%B7%E6%A5%AD%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E4%BA%BA%E9%9D%9E%E6%B6%88%E8%B2%BB%E6%94%AF%E5%87%BA.csv"
    columns_remove_patt = "-元"
    index_col = "年"

    df = read_csv(url)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/130027 家庭收支調查-所得收入者職業別平均每人可支配所得
def df_家庭收支調查_所得收入者職業別平均每人可支配所得():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/085-%E6%89%80%E5%BE%97%E6%94%B6%E5%85%A5%E8%80%85%E8%81%B7%E6%A5%AD%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E4%BA%BA%E5%8F%AF%E6%94%AF%E9%85%8D%E6%89%80%E5%BE%97.csv"
    columns_remove_patt = "-元"
    index_col = "年"

    df = read_csv(url)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/131149 家庭收支調查-所得收入者職業別平均每人受僱人員報酬
def df_家庭收支調查_所得收入者職業別平均每人受僱人員報酬():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232214/087-%E6%89%80%E5%BE%97%E6%94%B6%E5%85%A5%E8%80%85%E8%81%B7%E6%A5%AD%E5%88%A5%E5%B9%B3%E5%9D%87%E6%AF%8F%E4%BA%BA%E5%8F%97%E5%83%B1%E4%BA%BA%E5%93%A1%E5%A0%B1%E9%85%AC.csv"
    columns_remove_patt = "-元"
    index_col = "年"

    df = read_csv(url)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://www.stat.gov.tw/cp.aspx?n=2773 家庭部門平均每戶資產負債
def df_家庭部門平均每戶資產負債():
    key = "家庭部門平均每戶資產負債"
    key = sanitize_filename(key)
    url = "https://ws.dgbas.gov.tw/001/Upload/463/relfile/10315/2515/112%E8%A1%A87.xlsx"

    df = read_excel_with_cache(
        EXTRA_DATA_DIR / f"{key}.xlsx.gz", url, skiprows=2, nrows=19, usecols=range(0, 6)
    )
    df.columns = ["種類", "2019", "2020", "2021", "2022", "2023"]
    df["種類"] = df["種類"].str.replace(r"[\n \r]", "", regex=True)
    df = df.dropna().set_index("種類")
    # 負債轉為負值
    df.iloc[13:16] = -df.iloc[13:16]

    if int(df.columns[-1]) + 2 < datetime.now().year and datetime.now().month > 4:
        print(f"請更新 {key}")

    return df


# https://mopsov.twse.com.tw/mops/web/t100sb14 公開資訊觀測站_財務報告附註揭露之員工福利(薪資)資訊
def df_公開資訊觀測站_財務報告附註揭露之員工福利薪資資訊():
    key = "公開資訊觀測站_財務報告附註揭露之員工福利(薪資)資訊"
    key = sanitize_filename(key)
    url = "https://mopsov.twse.com.tw/mops/web/ajax_t100sb14"
    last_year = 113

    def get_df(path, url, data):
        _ensure_dir_exists(path)

        if not path.is_file():
            r = session.post(url, data)
            with gzip.open(path, "wb") as f:
                f.write(r.content)

        return pd.read_html(path, encoding="utf8")[0]

    for year in range(113, datetime.today().year - 1911):
        data_上市 = {
            "encodeURIComponent": 1,
            "step": 1,
            "firstin": 1,
            "TYPEK": "sii",
            "RYEAR": year,
            "code": "",
        }
        data_上櫃 = {
            "encodeURIComponent": 1,
            "step": 1,
            "firstin": 1,
            "TYPEK": "otc",
            "RYEAR": year,
            "code": "",
        }

        try:
            df_上市 = get_df(EXTRA_DATA_DIR / key / "上市" / f"{year}.html.gz", url, data_上市)
            df_上櫃 = get_df(EXTRA_DATA_DIR / key / "上櫃" / f"{year}.html.gz", url, data_上櫃)
            last_year = year
        except Exception as e:
            print(key)
            print("no data", year, url, e)
            break

    def clear_data(df: pd.DataFrame) -> pd.DataFrame:
        df = df[df.columns[:-4]]

        df.columns = [
            "產業類別",
            "公司代號",
            "公司名稱",
            "公司類別",
            "員工福利費用(仟元)",
            "員工薪資費用(仟元)",
            "員工人數(人)",
            "平均員工福利費用(仟元/人)",
            f"平均員工薪資費用{last_year}年度(仟元/人)",
            f"平均員工薪資費用{last_year-1}年度(仟元/人)",
            "平均員工薪資費用調整變動情形(%)",
            "每股盈餘(元/股)",
        ]

        df.loc[:, ["員工福利費用"]] = (
            df["員工福利費用(仟元)"].replace("-", np.nan).astype(float) * 1000
        )
        df.loc[:, ["員工薪資費用"]] = (
            df["員工薪資費用(仟元)"].replace("-", np.nan).astype(float) * 1000
        )
        df.loc[:, ["平均員工福利費用(人)"]] = (
            df["平均員工福利費用(仟元/人)"].replace("-", np.nan).astype(float) * 1000
        )
        df.loc[:, [f"平均員工薪資費用{last_year}年度(人)"]] = (
            df[f"平均員工薪資費用{last_year}年度(仟元/人)"].replace("-", np.nan).astype(float)
            * 1000
        )
        df.loc[:, [f"平均員工薪資費用{last_year-1}年度(人)"]] = (
            df[f"平均員工薪資費用{last_year-1}年度(仟元/人)"].replace("-", np.nan).astype(float)
            * 1000
        )

        df.loc[:, ["平均員工薪資費用調整變動情形"]] = (
            df["平均員工薪資費用調整變動情形(%)"]
            .str.replace("%", "")
            .replace("-", np.nan)
            .astype(float)
            / 100
        )

        return df

    df_上市 = clear_data(df_上市)
    df_上櫃 = clear_data(df_上櫃)

    df = pd.concat([df_上市, df_上櫃], ignore_index=True)
    df["公司"] = df["公司代號"].astype(str) + "_" + df["公司名稱"] + "_" + df["產業類別"]

    return df, last_year


# https://mopsov.twse.com.tw/mops/web/t100sb15 公開資訊觀測站_非擔任主管職務之全時員工薪資資訊
def df_公開資訊觀測站_非擔任主管職務之全時員工薪資資訊():
    key = "公開資訊觀測站_非擔任主管職務之全時員工薪資資訊"
    key = sanitize_filename(key)
    url = "https://mopsov.twse.com.tw/mops/web/ajax_t100sb15"
    last_year = 113

    def get_df(path, url, data):
        _ensure_dir_exists(path)

        if not path.is_file():
            r = session.post(url, data)
            if "查無資料" in r.text:
                raise f"無法獲取資料 {data.RYEAR} {data.TYPEK}"

            with gzip.open(path, "wb") as f:
                f.write(r.content)

        return pd.read_html(path, encoding="utf8")[0]

    for year in range(113, datetime.today().year - 1911):
        data_上市 = {
            "encodeURIComponent": 1,
            "step": 1,
            "firstin": 1,
            "TYPEK": "sii",
            "RYEAR": year,
            "code": "",
        }
        data_上櫃 = {
            "encodeURIComponent": 1,
            "step": 1,
            "firstin": 1,
            "TYPEK": "otc",
            "RYEAR": year,
            "code": "",
        }

        try:
            df_上市 = get_df(EXTRA_DATA_DIR / key / "上市" / f"{year}.html.gz", url, data_上市)
            df_上櫃 = get_df(EXTRA_DATA_DIR / key / "上櫃" / f"{year}.html.gz", url, data_上櫃)
            last_year = year
        except Exception as e:
            print(key)
            print("no data", year, url, e)
            break

    def clear_data(df: pd.DataFrame) -> pd.DataFrame:
        df = df[df.columns[:12]]

        df.columns = [
            "產業類別",
            "公司代號",
            "公司名稱",
            "員工薪資總額(仟元)",
            "員工人數-年度平均(人)",
            f"員工薪資-平均數_{last_year}年(仟元/人)",
            f"員工薪資-平均數_{last_year-1}年(仟元/人)",
            "員工薪資-平均數調整變動情形(%)",
            f"員工薪資-中位數_{last_year}年(仟元/人)",
            f"員工薪資-中位數_{last_year-1}年(仟元/人)",
            "員工薪資-中位數調整變動情形(%)",
            "每股盈餘(元/股)",
        ]

        df.loc[:, ["員工薪資總額"]] = (
            df["員工薪資總額(仟元)"].replace("-", np.nan).astype(float) * 1000
        )

        df.loc[:, [f"員工薪資-平均數_{last_year}年(人)"]] = (
            df[f"員工薪資-平均數_{last_year}年(仟元/人)"].replace("-", np.nan).astype(float) * 1000
        )
        df.loc[:, [f"員工薪資-平均數_{last_year-1}年(人)"]] = (
            df[f"員工薪資-平均數_{last_year-1}年(仟元/人)"].replace("-", np.nan).astype(float)
            * 1000
        )
        df.loc[:, ["員工薪資-平均數調整變動情形"]] = (
            df["員工薪資-平均數調整變動情形(%)"]
            .str.replace("%", "")
            .replace("-", np.nan)
            .astype(float)
            / 100
        )

        df.loc[:, [f"員工薪資-中位數_{last_year}年(人)"]] = (
            df[f"員工薪資-中位數_{last_year}年(仟元/人)"].replace("-", np.nan).astype(float) * 1000
        )
        df.loc[:, [f"員工薪資-中位數_{last_year-1}年(人)"]] = (
            df[f"員工薪資-中位數_{last_year-1}年(仟元/人)"].replace("-", np.nan).astype(float)
            * 1000
        )
        df.loc[:, ["員工薪資-中位數調整變動情形"]] = (
            df["員工薪資-中位數調整變動情形(%)"]
            .str.replace("%", "")
            .replace("-", np.nan)
            .astype(float)
            / 100
        )

        return df

    df_上市 = clear_data(df_上市)
    df_上櫃 = clear_data(df_上櫃)

    df = pd.concat([df_上市, df_上櫃], ignore_index=True)
    df["公司"] = df["公司代號"].astype(str) + "_" + df["公司名稱"] + "_" + df["產業類別"]

    return df, last_year


# https://data.gov.tw/dataset/155869 企業ESG資訊揭露彙總資料-人力發展上市
# https://data.gov.tw/dataset/156379 企業ESG資訊揭露彙總資料-人力發展上櫃
def df_企業ESG資訊揭露彙總資料_人力發展():
    url_上市 = "https://mopsfin.twse.com.tw/opendata/t187ap46_L_5.csv"
    url_上櫃 = "https://mopsfin.twse.com.tw/opendata/t187ap46_O_5.csv"

    df_上市 = read_csv(url_上市)
    df_上櫃 = read_csv(url_上櫃)

    df = pd.concat([df_上市, df_上櫃], ignore_index=True)
    df["公司"] = df["公司代號"].astype(str) + "_" + df["公司名稱"]
    df[
        [
            "員工福利平均數",
            "員工薪資平均數",
            "非擔任主管職務之全時員工薪資平均數",
            "非擔任主管之全時員工薪資中位數",
        ]
    ] = (
        df[
            [
                "員工福利平均數(仟元/人)",
                "員工薪資平均數(仟元/人)",
                "非擔任主管職務之全時員工薪資平均數(仟元/人)",
                "非擔任主管之全時員工薪資中位數(仟元/人)",
            ]
        ]
        * 1000
    )

    return df


# https://data.gov.tw/dataset/9634 歷年受僱員工每人每月總薪資
def df_歷年受僱員工每人每月總薪資平均數():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/230037/mp05001.xml"

    df = read_xml(url, "//每人每月總薪資")
    df["年月別_Year_and_month"] = df["年月別_Year_and_month"].str.replace("[^0-9]", "", regex=True)
    df = df.set_index("年月別_Year_and_month")
    splits = df.columns.str.split("_", n=1, expand=True)
    df.columns = [split[0] for split in splits]

    return df


# https://data.gov.tw/dataset/9663 歷年受僱員工每人每月經常性薪資
def df_歷年受僱員工每人每月經常性薪資平均數():
    url = "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/230037/mp05002.xml"

    df = read_xml(url, "//每人每月經常性薪資")
    df["年月別_Year_and_month"] = df["年月別_Year_and_month"].str.replace("[^0-9]", "", regex=True)
    df = df.set_index("年月別_Year_and_month")
    splits = df.columns.str.split("_", n=1, expand=True)
    df.columns = [split[0] for split in splits]

    return df


# https://www.stat.gov.tw/News_Content.aspx?n=4580&s=232642 工業及服務業全體受僱員工全年總薪資統計表
def df_工業及服務業全體受僱員工全年總薪資統計表():
    url_按性別及教育程度分 = "https://ws.dgbas.gov.tw/001/Upload/463/relfile/11753/232642/%E8%A1%A81%E3%80%80%E5%90%84%E6%A5%AD%E5%8F%97%E5%83%B1%E5%93%A1%E5%B7%A5%E5%85%A8%E5%B9%B4%E7%B8%BD%E8%96%AA%E8%B3%87%E7%B5%B1%E8%A8%88%EF%BC%8D%E6%8C%89%E6%80%A7%E5%88%A5%E5%8F%8A%E6%95%99%E8%82%B2%E7%A8%8B%E5%BA%A6%E5%88%86.xlsx"
    r = session.get(url_按性別及教育程度分, verify=False)

    df_按性別及教育程度分 = []
    for i in range(0, 3):
        data = pd.read_excel(
            io.BytesIO(r.content),
            engine="calamine",
            sheet_name=i,
            skiprows=7,
            nrows=20,
            header=None,
        )
        val_columns = list(
            itertools.product(
                ["平均數", "中位數"],
                ["全體", "男", "女", "國中及以下", "高級中等(高中、高職)", "專科及大學", "研究所"],
            )
        )
        data.columns = ["行業"] + val_columns

        data["行業"] = data["行業"].str.strip()
        data = data.melt(id_vars="行業", value_vars=val_columns, value_name="值")
        data["值"] *= 10000
        data["統計"], data["性別教育程度"] = zip(*data["variable"])
        data = data.drop("variable", axis="columns")
        data["年度"] = 112 - i

        df_按性別及教育程度分.append(data)

    df_按性別及教育程度分 = pd.concat(df_按性別及教育程度分[::-1], axis="index", ignore_index=True)

    url_按年齡別分 = "https://ws.dgbas.gov.tw/001/Upload/463/relfile/11753/232642/%E8%A1%A82%E3%80%80%E5%90%84%E6%A5%AD%E5%8F%97%E5%83%B1%E5%93%A1%E5%B7%A5%E5%85%A8%E5%B9%B4%E7%B8%BD%E8%96%AA%E8%B3%87%E7%B5%B1%E8%A8%88%EF%BC%8D%E6%8C%89%E5%B9%B4%E9%BD%A1%E5%88%A5%E5%88%86.xlsx"
    r = session.get(url_按年齡別分, verify=False)

    df_按年齡別分 = []
    for i in range(0, 3):
        data = pd.read_excel(
            io.BytesIO(r.content),
            engine="calamine",
            sheet_name=i,
            skiprows=7,
            nrows=20,
            header=None,
        )
        val_columns = list(
            itertools.product(
                ["平均數", "中位數"],
                [
                    "全體",
                    "未滿30歲",
                    "未滿25歲",
                    "25-29歲",
                    "30-39歲",
                    "40-49歲",
                    "50-64歲",
                    "65歲以上",
                ],
            )
        )
        data.columns = ["行業"] + val_columns

        data["行業"] = data["行業"].str.strip()
        data = data.melt(id_vars="行業", value_vars=val_columns, value_name="值")
        data["值"] *= 10000
        data["統計"], data["年齡別"] = zip(*data["variable"])
        data = data.drop("variable", axis="columns")
        data["年度"] = 112 - i

        df_按年齡別分.append(data)

    df_按年齡別分 = pd.concat(df_按年齡別分[::-1], axis="index", ignore_index=True)

    url_按員工特性 = "https://ws.dgbas.gov.tw/001/Upload/463/relfile/11753/232642/%E8%A1%A84%E3%80%80%E5%B7%A5%E6%A5%AD%E5%8F%8A%E6%9C%8D%E5%8B%99%E6%A5%AD%E5%8F%97%E5%83%B1%E5%93%A1%E5%B7%A5%E5%85%A8%E5%B9%B4%E7%B8%BD%E8%96%AA%E8%B3%87%E7%B5%B1%E8%A8%88%EF%BC%8D%E6%8C%89%E5%93%A1%E5%B7%A5%E7%89%B9%E6%80%A7%E5%8F%8A%E5%93%A1%E5%B7%A5%E8%A6%8F%E6%A8%A1%E5%88%A5%E5%88%86.xlsx"
    r = session.get(url_按員工特性, verify=False)

    df_按員工特性 = []
    for i in range(0, 3):
        data = pd.read_excel(
            io.BytesIO(r.content),
            engine="calamine",
            sheet_name=i,
            skiprows=8,
            nrows=20,
            header=None,
        )
        data = data.dropna(axis="index", how="any")
        val_columns = list(
            itertools.product(
                ["平均數", "中位數"],
                [
                    "全體",
                    "男",
                    "女",
                ],
            )
        )
        data.columns = ["年齡教育別"] + val_columns

        data["年齡教育別"] = data["年齡教育別"].str.strip()
        data = data.melt(id_vars="年齡教育別", value_vars=val_columns, value_name="值")
        data["值"] *= 10000
        data["統計"], data["性別"] = zip(*data["variable"])
        data = data.drop("variable", axis="columns")
        data["年度"] = 112 - i

        df_按員工特性.append(data)

    df_按員工特性 = pd.concat(df_按員工特性[::-1], axis="index", ignore_index=True)

    url_按年齡及教育程度分 = "https://ws.dgbas.gov.tw/001/Upload/463/relfile/11753/232642/%E8%A1%A85%E3%80%80%E5%B7%A5%E6%A5%AD%E5%8F%8A%E6%9C%8D%E5%8B%99%E6%A5%AD%E5%8F%97%E5%83%B1%E5%93%A1%E5%B7%A5%E5%85%A8%E5%B9%B4%E7%B8%BD%E8%96%AA%E8%B3%87%E7%B5%B1%E8%A8%88%EF%BC%8D%E6%8C%89%E5%B9%B4%E9%BD%A1%E5%8F%8A%E6%95%99%E8%82%B2%E7%A8%8B%E5%BA%A6%E5%88%86.xlsx"
    r = session.get(url_按年齡及教育程度分, verify=False)

    df_按年齡及教育程度分 = []
    for i in range(0, 3):
        data = pd.read_excel(
            io.BytesIO(r.content),
            engine="calamine",
            sheet_name=i,
            skiprows=8,
            nrows=8,
            header=None,
        )
        data = data.dropna(axis="index", how="any")
        val_columns = list(
            itertools.product(
                ["平均數", "中位數"],
                ["全體", "國中及以下", "高級中等(高中、高職)", "專科及大學", "研究所"],
            )
        )
        data.columns = ["年齡別"] + val_columns

        data["年齡別"] = data["年齡別"].str.strip()
        data = data.melt(id_vars="年齡別", value_vars=val_columns, value_name="值")
        data["值"] *= 10000
        data["統計"], data["教育程度"] = zip(*data["variable"])
        data = data.drop("variable", axis="columns")
        data["年度"] = 112 - i

        df_按年齡及教育程度分.append(data)

    df_按年齡及教育程度分 = pd.concat(df_按年齡及教育程度分[::-1], axis="index", ignore_index=True)

    url_按工作場所所在縣市別及年齡別分 = "https://ws.dgbas.gov.tw/001/Upload/463/relfile/11753/232642/%E8%A1%A86%E3%80%80%E5%B7%A5%E6%A5%AD%E5%8F%8A%E6%9C%8D%E5%8B%99%E6%A5%AD%E5%85%A8%E5%B9%B4%E7%B8%BD%E8%96%AA%E8%B3%87%E7%B5%B1%E8%A8%88%EF%BC%8D%E6%9C%AC%E5%9C%8B%E7%B1%8D%E5%85%A8%E6%99%82%E5%8F%97%E5%83%B1%E5%93%A1%E5%B7%A5%E6%8C%89%E5%B7%A5%E4%BD%9C%E5%A0%B4%E6%89%80%E6%89%80%E5%9C%A8%E7%B8%A3%E5%B8%82%E5%88%A5%E5%8F%8A%E5%B9%B4%E9%BD%A1%E5%88%A5%E5%88%86.xlsx"
    r = session.get(url_按工作場所所在縣市別及年齡別分, verify=False)

    df_按工作場所所在縣市別及年齡別分 = []
    for i in range(0, 5):
        data = pd.read_excel(
            io.BytesIO(r.content),
            engine="calamine",
            sheet_name=i,
            skiprows=7,
            nrows=21,
            header=None,
        )
        data = data.dropna(axis="index", how="any")
        val_columns = list(
            itertools.product(
                ["平均數", "中位數"],
                [
                    "全體",
                    "未滿30歲",
                    "未滿25歲",
                    "25-29歲",
                    "30-39歲",
                    "40-49歲",
                    "50-64歲",
                    "65歲以上",
                ],
            )
        )
        data.columns = ["縣市"] + val_columns

        data["縣市"] = data["縣市"].str.strip()
        data = data.melt(id_vars="縣市", value_vars=val_columns, value_name="值")
        data["值"] *= 10000
        data["統計"], data["年齡別"] = zip(*data["variable"])
        data = data.drop("variable", axis="columns")
        data["年度"] = 112 - i

        df_按工作場所所在縣市別及年齡別分.append(data)

    df_按工作場所所在縣市別及年齡別分 = pd.concat(
        df_按工作場所所在縣市別及年齡別分[::-1], axis="index", ignore_index=True
    )

    return (
        df_按性別及教育程度分,
        df_按年齡別分,
        df_按員工特性,
        df_按年齡及教育程度分,
        df_按工作場所所在縣市別及年齡別分,
    )


# https://www.stat.gov.tw/Point.aspx?sid=t.4&n=3583&sms=11480 工業及服務業每人每月工時(時)
def df_受僱員工每人每月工時():
    url = "https://nstatdb.dgbas.gov.tw/dgbasall/webMain.aspx?sdmx/A046401010/1+2+3+4+31+32+33+34+35+36+37+38+39+40+41+42+43+44+45+46.1+2+3..M.&startTime={first_year}&endTime={year}-M{month}"

    first_year = 1973
    year = datetime.today().year
    month = datetime.today().month
    r = session.get(url.format(first_year=first_year, year=year, month=month), verify=False)
    json_data = json.loads(r.content)

    datas = json_data["data"]["dataSets"][0]["series"].items()
    kinds = json_data["data"]["structure"]["dimensions"]["series"][0]["values"]
    sexes = json_data["data"]["structure"]["dimensions"]["series"][1]["values"]
    df = []
    for key, data in datas:
        periods = json_data["data"]["structure"]["dimensions"]["observation"][0]["values"]

        kind, sex = key.split(":", 1)
        kind = int(kind)
        sex = int(sex)

        colname = f"{kinds[kind]["name"]}_{sexes[sex]["name"]}"
        ser = pd.Series(
            [x[0] for x in data["observations"].values()],
            index=[p["id"] for p in periods],
            name=colname,
        )

        df.append(ser[ser != 0.0])

    df = pd.concat(df, axis="columns")
    df = df.reset_index()
    df = df.rename(columns={"index": "年月"})

    split = df["年月"].str.split("-", n=1, expand=True)
    df["年"] = split[0].str.strip()
    df["月"] = split[1].str.strip()

    df = df.sort_values(["年", "月"])

    return df


# https://data.gov.tw/dataset/34125 各業廠商僱用職缺按月計薪者每人每月平均最低薪資－按職類及員工規模分
def df_各業廠商僱用職缺按月計薪者每人每月平均最低薪資_按職類及員工規模分():
    key = "各業廠商僱用職缺按月計薪者每人每月平均最低薪資－按職類及員工規模分"
    key = sanitize_filename(key)
    url = {113: "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/234463/mp05044a113.xml"}
    lastyear = max(url.keys())

    if lastyear + 1911 + 1 < datetime.now().year and datetime.now().month > 1:
        print(f"請更新 {key}")

    df = read_xml_with_cache(
        EXTRA_DATA_DIR / key / f"{lastyear}.xml.gz",
        url[lastyear],
        "//各業廠商僱用職缺按月計薪者每人每月平均最低薪資_按職類及員工規模分",
    )
    df = df.set_index("項目別")
    df.columns = df.columns.str.removesuffix("_新臺幣元").str.removesuffix("_金額")
    df = df.replace("-", np.nan).astype(float)

    return df, lastyear


# https://data.gov.tw/dataset/32751 各業廠商調升經常性薪資參考各項因素之廠商比率－按行業分
def df_各業廠商調升經常性薪資參考各項因素之廠商比率_按行業分():
    key = "各業廠商調升經常性薪資參考各項因素之廠商比率－按行業分"
    key = sanitize_filename(key)
    url = {112: "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/234005/mp05025a112.xml"}
    lastyear = max(url.keys())

    if lastyear + 1911 + 2 < datetime.now().year and datetime.now().month > 10:
        print(f"請更新 {key}")

    df = read_xml_with_cache(
        EXTRA_DATA_DIR / key / f"{lastyear}.xml.gz",
        url[lastyear],
        "//各業廠商調升經常性薪資參考各項因素之廠商比率_按行業分",
    )
    df = df.set_index("項目別")
    df = df.replace("-", np.nan).astype(float) / 100

    return df, lastyear


# https://data.gov.tw/dataset/32749 各業廠商調升員工經常性薪資之廠商與員工人數比率－按行業分
def df_各業廠商調升員工經常性薪資之廠商與員工人數比率_按行業分():
    key = "各業廠商調升員工經常性薪資之廠商與員工人數比率－按行業分"
    key = sanitize_filename(key)
    url = {
        103: [
            "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/231704/Mp05023.xml",
            "各業廠商調整員工經常性薪資之廠商與員工人數比率_百分比",
        ],
        104: [
            "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/231704/Mp05023A104.xml",
            "各業廠商調整員工經常性薪資之廠商與員工人數比率_百分比",
        ],
        105: [
            "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/231704/MP05023A105.xml",
            "各業廠商調整員工經常性薪資之廠商與員工人數比率_按行業分_百分比",
        ],
        106: [
            "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/231704/MP05023A106.xml",
            "各業廠商調整員工經常性薪資之廠商與員工人數比率-按行業分_百分比",
        ],
        107: [
            "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/231704/MP05023A107.xml",
            "各業廠商調整員工經常性薪資之廠商與員工人數比率-按行業分_百分比",
        ],
        108: [
            "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/231704/MP05023A108.xml",
            "各業廠商調升員工經常性薪資之廠商與員工人數比率_按行業分_百分比",
        ],
        109: [
            "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/231704/MP05023A109.xml",
            "各業廠商調升員工經常性薪資之廠商與員工人數比率_按行業分_百分比",
        ],
        110: [
            "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/231704/MP05023A110.xml",
            "各業廠商調升員工經常性薪資之廠商與員工人數比率_按行業分_百分比",
        ],
        111: [
            "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/232494/mp05023a111.xml",
            "各業廠商調升員工經常性薪資之廠商與員工人數比率_按行業分",
        ],
        112: [
            "https://ws.dgbas.gov.tw/001/Upload/461/relfile/11525/234005/mp05023a112.xml",
            "各業廠商調升員工經常性薪資之廠商與員工人數比率_按行業分",
        ],
    }
    lastyear = max(url.keys())

    if lastyear + 1911 + 2 < datetime.now().year and datetime.now().month > 10:
        print(f"請更新 {key}")

    df = []
    for year in range(106, lastyear + 1):
        data = read_xml_with_cache(
            EXTRA_DATA_DIR / key / f"{year}.xml.gz",
            url[year][0],
            url[year][1],
        )
        data[data.columns[1:]] = data[data.columns[1:]].replace("-", np.nan).astype(float) / 100
        data["年度"] = year
        data = data.rename(
            columns={
                "項目別_百分比": "項目別",
            }
        )
        df.append(data)
    df = pd.concat(df, ignore_index=True)

    return df, lastyear


# https://data.gov.tw/dataset/24274 公司合併報表董事酬金相關資訊上市
# https://data.gov.tw/dataset/24278 公司合併報表董事酬金相關資訊上櫃
def df_公司合併報表董事酬金相關資訊():
    url_上市 = "https://mopsfin.twse.com.tw/opendata/t187ap29_C_L.csv"
    url_上櫃 = "https://mopsfin.twse.com.tw/opendata/t187ap29_C_O.csv"

    df_上市 = read_csv(url_上市)
    df_上櫃 = read_csv(url_上櫃)

    df = pd.concat([df_上市, df_上櫃], ignore_index=True)
    num_index = [
        "董事酬金-去年支付",
        "董事酬金-今年支付",
        "董事酬金-合計",
        "董事酬金加計兼任員工酬金-去年支付",
        "董事酬金加計兼任員工酬金-今年支付",
        "加計兼任員工酬金-合計",
        "酬金總額占稅後損益百分比(%)-董事酬金",
        "酬金總額占稅後損益百分比(%)-加計兼任員工酬金",
        "平均每位董事酬金-董事酬金",
        "平均每位董事酬金-加計兼任員工酬金",
        "領取來自子公司以外轉投資事業或母公司酬金",
        "稅後純益",
        "每股盈餘",
        "股東權益報酬率(%)",
        "實收資本額(千元)",
    ]
    df[num_index] = df[num_index].replace(",", "", regex=True)
    df[num_index] = df[num_index].astype(float)
    df.loc[:, "稅後純益"] = df.loc[:, "稅後純益"] * 1000
    df["公司"] = df["公司代號"].astype(str) + "_" + df["公司名稱"] + "_" + df["產業類別"]

    return df


# https://data.gov.tw/dataset/24275 公司合併報表監察人酬金相關資訊上市
# https://data.gov.tw/dataset/24279 公司合併報表監察人酬金相關資訊上櫃
def df_公司合併報表監察人酬金相關資訊():
    url_上市 = "https://mopsfin.twse.com.tw/opendata/t187ap29_D_L.csv"
    url_上櫃 = "https://mopsfin.twse.com.tw/opendata/t187ap29_D_O.csv"

    df_上市 = read_csv(url_上市)
    df_上櫃 = read_csv(url_上櫃)

    df = pd.concat([df_上市, df_上櫃], ignore_index=True)
    num_index = [
        "監察人酬金-去年支付",
        "監察人酬金-今年支付",
        "監察人酬金-合計",
        "酬金總額占稅後損益百分比(%)",
        "平均每位監察人酬金",
        "領取來自子公司以外轉投資事業或母公司酬金",
        "稅後純益",
        "每股盈餘",
        "股東權益報酬率(%)",
        "實收資本額(千元)",
    ]
    df[num_index] = df[num_index].replace(",", "", regex=True).astype(float)
    df.loc[:, "稅後純益"] = df.loc[:, "稅後純益"] * 1000
    df["公司"] = df["公司代號"].astype(str) + "_" + df["公司名稱"] + "_" + df["產業類別"]

    return df


# https://data.gov.tw/dataset/17963 綜稅總所得各縣市申報統計分析表
def df_綜稅總所得各縣市申報統計分析表():
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
    df["納稅單位(戶)"] = df["納稅單位(戶)"].astype(int)

    return df, lastyear


# https://www.fia.gov.tw/WEB/fia/ias/ISA-index.html 綜合所得稅申報核定統計專冊
# https://data.gov.tw/dataset/103066 綜稅綜合所得總額全國各縣市鄉鎮村里統計分析表
def df_綜稅綜合所得總額全國各縣市鄉鎮村里統計分析表():
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

    return df, lastyear


# https://www.mof.gov.tw/singlehtml/285?cntId=64525 財政部「財政統計年報」 -> 綜合所得稅結算申報－按淨所得級距別分
def df_財政統計年報_綜合所得稅結算申報_按淨所得級距別分():
    key = "財政統計年報_綜合所得稅結算申報－按淨所得級距別分"
    key = sanitize_filename(key)
    urls = {
        97: ("csv", "https://www.mof.gov.tw/download/pub82938"),
        98: ("csv", "https://www.mof.gov.tw/download/pub83857"),
        99: ("xls", "https://www.mof.gov.tw/download/pub84529"),
        100: ("xls", "https://www.mof.gov.tw/download/pub76111"),
        101: ("xls", "https://www.mof.gov.tw/download/pub76629"),
        102: ("xls", "https://www.mof.gov.tw/download/pub77174"),
        103: ("xls", "https://www.mof.gov.tw/download/pub77720"),
        104: (
            "xls",
            "https://service.mof.gov.tw/public/Data/statistic/Year_Fin/104%E9%9B%BB%E5%AD%90%E6%9B%B8/htm/33120.xls",
        ),
        105: ("xls", "https://www.mof.gov.tw/download/pub78801"),
        106: ("xls", "https://www.mof.gov.tw/download/pub79311"),
        107: ("xls", "https://www.mof.gov.tw/download/pub79760"),
        108: (
            "xls",
            "https://service.mof.gov.tw/public/Data/statistic/Year_Fin/108%E9%9B%BB%E5%AD%90%E6%9B%B8/htm/33130.xls",
        ),
        109: (
            "xls",
            "https://service.mof.gov.tw/public/Data/statistic/Year_Fin/109%E9%9B%BB%E5%AD%90%E6%9B%B8/htm/33130.xls",
        ),
        110: (
            "xls",
            "https://service.mof.gov.tw/public/Data/statistic/Year_Fin/110%E9%9B%BB%E5%AD%90%E6%9B%B8/htm/33130.xls",
        ),
        111: (
            "xlsx",
            "https://service.mof.gov.tw/public/Data/statistic/Year_Fin/111%E9%9B%BB%E5%AD%90%E6%9B%B8/htm/33130.xlsx",
        ),
        112: (
            "xlsx",
            "https://service.mof.gov.tw/public/Data/statistic/Year_Fin/112%E9%9B%BB%E5%AD%90%E6%9B%B8/htm/33130.xlsx",
        ),
        113: (
            "xlsx",
            "https://service.mof.gov.tw/public/Data/statistic/Year_Fin/113%E9%9B%BB%E5%AD%90%E6%9B%B8/htm/33130.xlsx",
        ),
    }

    if max(urls.keys()) + 1911 + 1 < datetime.now().year and datetime.now().month > 4:
        print(f"請更新 {key}")

    df = []
    for year, (filetype, url) in urls.items():
        path = EXTRA_DATA_DIR / key / f"{year}.{filetype}.gz"
        _ensure_dir_exists(path)
        if not path.is_file():
            r = session.get(url, verify=False)

            with gzip.open(path, "wb") as f:
                f.write(r.content)

    last_year = max(urls.keys())
    filetype, _ = urls[last_year]

    path = EXTRA_DATA_DIR / key / f"{last_year}.{filetype}.gz"
    with gzip.open(path, "rb") as f_gz:
        # Read the gzipped content into BytesIO for pandas
        excel_bytes = io.BytesIO(f_gz.read())

    df1 = pd.read_excel(
        excel_bytes,
        engine="calamine",
        header=None,
        skiprows=13,
        nrows=8,
        usecols=list(range(0, 11)),
        names=[
            "所得級距",
            "申報戶數",
            "核定所得淨額",
            "核定所得淨額_分開計稅之股利所得",
            "核定應納稅額",
            "核定應納稅額_分開計稅之股利所得",
            "各類所得_合計",
            "各類所得_營利所得",
            "各類所得_執行業務所得",
            "各類所得_薪資所得",
            "各類所得_利息所得",
        ],
    ).set_index("所得級距")
    df1.index = df1.index.str.strip().str.replace("\r\n(NT$10,000)", "")

    df2 = pd.read_excel(
        excel_bytes,
        engine="calamine",
        header=None,
        skiprows=13,
        nrows=8,
        usecols=list(range(11, 11 + 12)),
        names=[
            "所得級距",
            "各類所得_租賃及權利金所得",
            "各類所得_財產交易所得",
            "各類所得_機會中獎所得",
            "各類所得_股利所得",
            "各類所得_股利所得_分開計稅之股利所得",
            "各類所得_退職所得",
            "各類所得_其他所得",
            "各類所得_稿費所得",
            "各類所得_未能歸類所得",
            "薪資收入",
            "稿費收入額",
        ],
    ).set_index("所得級距")
    df2.index = df2.index.str.strip().str.replace("\r\n(NT$10,000)", "")

    df = pd.concat([df1, df2], axis="columns")

    cols = [col for col in df.columns if col != "申報戶數"]
    df[cols] *= 1000

    return df, last_year


# https://data.gov.tw/dataset/102667 勞工退休金提繳統計年報-按地區、行業及規模別
def df_勞工退休金提繳統計年報_按地區_行業及規模別():
    url = "https://apiservice.mol.gov.tw/OdService/rest/datastore/A17000000J-030214-cOJ"

    r = session.get(url, verify=False)
    json_data = json.loads(r.content)

    df = pd.json_normalize(json_data["result"]["records"])

    df[["月底單位數量", "月底人數", "應計提繳金額", "平均提繳工資金額"]] = df[
        ["月底單位數量", "月底人數", "應計提繳金額", "平均提繳工資金額"]
    ].astype(int)

    df["總提繳工資金額"] = df["平均提繳工資金額"] * df["月底人數"]

    return df


# https://data.gov.tw/dataset/102667
# https://www.bli.gov.tw/0013087.html 統計年報 -> 113年 -> 勞工退休金 -> 提繳統計 -> 勞工退休金提繳單位、人數及平均提繳工資－按行業及地區分
def df_歷史_勞工退休金提繳統計年報_按地區_行業及規模別():
    key = "歷史_勞工退休金提繳統計年報_按地區_行業及規模別"
    key = sanitize_filename(key)
    urls = {
        96: "https://events.bli.gov.tw/report/attachment_file/report/year/096/h1400.csv",
        97: "https://events.bli.gov.tw/report/attachment_file/report/year/097/h1310.csv",
        98: "https://events.bli.gov.tw/report/attachment_file/report/year/098/h1010.csv",
        99: "https://events.bli.gov.tw/report/attachment_file/report/year/099/h1030.csv",
        100: "https://events.bli.gov.tw/report/attachment_file/report/year/100/h1030.csv",
        101: "https://events.bli.gov.tw/report/attachment_file/report/year/101/h1030.csv",
        102: "https://events.bli.gov.tw/report/attachment_file/report/year/102/h1030.csv",
        103: "https://events.bli.gov.tw/report/attachment_file/report/year/103/h1000.csv",
        104: "https://events.bli.gov.tw/report/attachment_file/report/year/104/h1010.csv",
        105: "https://events.bli.gov.tw/report/attachment_file/report/year/105/h1010.csv",
        106: "https://events.bli.gov.tw/report/attachment_file/report/year/106/h1010.csv",
        107: "https://events.bli.gov.tw/report/attachment_file/report/year/107/h1090.csv",
        108: "https://events.bli.gov.tw/report/attachment_file/report/year/108/h1090.csv",
        109: "https://events.bli.gov.tw/report/attachment_file/report/year/109/h790.csv",
        110: "https://events.bli.gov.tw/report/attachment_file/report/year/110/h790.csv",
        111: "https://events.bli.gov.tw/report/attachment_file/report/year/111/h40040.csv",
        112: "https://events.bli.gov.tw/report/attachment_file/report/year/112/h40040.csv",
        113: "https://events.bli.gov.tw/report/attachment_file/report/year/113/h40040.csv",
    }

    if max(urls.keys()) + 1911 + 1 < datetime.now().year and datetime.now().month > 6:
        print(f"請更新 {key}")

    df = []
    for filename, url in urls.items():
        path = EXTRA_DATA_DIR / key / f"{filename}.csv.gz"
        _ensure_dir_exists(path)

        if not path.is_file():
            r = session.get(url, verify=False)
            with gzip.open(path, "wb") as f:
                f.write(r.content)

        if filename in [111, 112, 113]:
            data = pd.read_csv(path, compression="gzip", skiprows=list(range(0, 6)), header=None)
            data = data.iloc[:23, [0, 4]]
        elif filename in [97, 99]:
            data = pd.read_csv(
                path, compression="gzip", skiprows=list(range(0, 10)), header=None, encoding="BIG5"
            )
            data = data.iloc[:28, [0, 3]]
        elif filename in [100, 101]:
            data = pd.read_csv(
                path, compression="gzip", skiprows=list(range(0, 10)), header=None, encoding="BIG5"
            )
            data = data.iloc[:23, [0, 3]]
        elif filename in [96, 98]:
            data = pd.read_csv(
                path,
                compression="gzip",
                skiprows=list(range(0, 9)),
                header=None,
                encoding="BIG5",
                nrows=28,
            )
            data = data.iloc[:28, [0, 3]]
        elif filename in [102]:
            data = pd.read_csv(
                path,
                compression="gzip",
                skiprows=list(range(0, 9)),
                header=None,
                encoding="BIG5",
            )
            data = data.iloc[:23, [0, 3]]
        elif filename in [104, 106]:
            data = pd.read_csv(
                path, compression="gzip", skiprows=list(range(0, 10)), header=None, encoding="BIG5"
            )
            data = data.iloc[:23, [0, 2]]
        elif filename in [103, 105]:
            data = pd.read_csv(
                path, compression="gzip", skiprows=list(range(0, 9)), header=None, encoding="BIG5"
            )
            data = data.iloc[:23, [0, 2]]
        elif filename in [107, 108, 109, 110]:
            data = pd.read_csv(
                path, compression="gzip", skiprows=list(range(0, 11)), header=None, encoding="BIG5"
            )
            data = data.iloc[:23, [0, 2]]

        data.columns = ["地區", "平均提繳工資"]
        data["年度"] = filename
        data["地區"] = (
            data["地區"]
            .str.strip()
            .str.replace("[ 　　]", "", regex=True)
            .str.replace(
                "總平均",
                "總計",
            )
        )

        df.append(data)

    df = pd.concat(df, ignore_index=True)
    df["平均提繳工資"] = df["平均提繳工資"].astype(float)

    return df


# https://apiservice.mol.gov.tw/OdService/openapi/OAS.html
# F00 類別 33379
def df_勞工退休準備金專戶餘額統計():
    url = "https://apiservice.mol.gov.tw/OdService/rest/datastore/A17000000J-020123-FRH"

    r = session.get(url, verify=False)
    json_data = json.loads(r.content)

    df = pd.json_normalize(json_data["result"]["records"])

    df["家數"] = df["家數"].astype(int)
    df["佔總數比率"] = df["佔總數比率"].astype(float)

    return df


# https://apiservice.mol.gov.tw/OdService/openapi/OAS.html
# F00 類別 33379
def df_勞工退休準備金提撥率統計():
    url = "https://apiservice.mol.gov.tw/OdService/rest/datastore/A17000000J-020123-Wmz"

    r = session.get(url, verify=False)
    json_data = json.loads(r.content)

    df = pd.json_normalize(json_data["result"]["records"])

    df["家數"] = df["家數"].astype(int)
    df["佔總數比率"] = df["佔總數比率"].astype(float)

    return df


# https://data.gov.tw/dataset/34057 勞工退休金提繳單位、提繳人數、提繳工資、提繳金額概況
def df_勞工退休金提繳單位_提繳人數_提繳工資_提繳金額概況():
    key = "勞工退休金提繳單位、提繳人數、提繳工資、提繳金額概況"
    key = sanitize_filename(key)
    urls = {
        "94_107": "https://apiservice.mol.gov.tw/OdService/download/A17010000J-000121-0bO",
        108: "https://apiservice.mol.gov.tw/OdService/download/A17010000J-000121-abu",
        109: "https://apiservice.mol.gov.tw/OdService/download/A17010000J-000121-HZ9",
        110: "https://apiservice.mol.gov.tw/OdService/download/A17010000J-000121-PH7",
        111: "https://apiservice.mol.gov.tw/OdService/download/A17010000J-000121-1UF",
        112: "https://apiservice.mol.gov.tw/OdService/download/A17010000J-000121-LF0",
        113: "https://apiservice.mol.gov.tw/OdService/download/A17010000J-000121-s48",
    }

    if (
        max([x for x in urls.keys() if isinstance(x, int)]) + 1911 + 1 < datetime.now().year
        and datetime.now().month > 6
    ):
        print(f"請更新 {key}")

    df = []
    for filename, url in urls.items():
        path = EXTRA_DATA_DIR / key / f"{filename}.csv.gz"
        data = read_csv_with_cache(path, url)
        df.append(data)

    df = pd.concat(df, ignore_index=True)

    return df


# https://data.gov.tw/dataset/46102 勞工退休金平均提繳工資-按行業別
def df_勞工退休金平均提繳工資_按行業別():
    key = "勞工退休金平均提繳工資-按行業別"
    key = sanitize_filename(key)
    urls = {
        "94_107": "https://apiservice.mol.gov.tw/OdService/download/A17000000J-030156-dhl",
        108: "https://apiservice.mol.gov.tw/OdService/download/A17000000J-030156-hTM",
        109: "https://apiservice.mol.gov.tw/OdService/download/A17000000J-030156-nbz",
        110: "https://apiservice.mol.gov.tw/OdService/download/A17000000J-030156-igd",
        111: "https://apiservice.mol.gov.tw/OdService/download/A17000000J-030156-o9Y",
        112: "https://apiservice.mol.gov.tw/OdService/download/A17000000J-030156-UFg",
        113: "https://apiservice.mol.gov.tw/OdService/download/A17000000J-030156-2d5",
    }

    if (
        max([x for x in urls.keys() if isinstance(x, int)]) + 1911 + 1 < datetime.now().year
        and datetime.now().month > 6
    ):
        print(f"請更新 {key}")

    df = []
    for filename, url in urls.items():
        path = EXTRA_DATA_DIR / key / f"{filename}.csv.gz"
        data = read_csv_with_cache(path, url)
        df.append(data)

    df = pd.concat(df, ignore_index=True)

    return df


# https://data.gov.tw/dataset/46103 勞工退休金平均提繳工資-按年齡組別
def df_勞工退休金平均提繳工資_按年齡組別():
    key = "勞工退休金平均提繳工資-按年齡組別"
    key = sanitize_filename(key)
    urls = {
        "94_107": "https://apiservice.mol.gov.tw/OdService/download/A17000000J-030157-0Ho",
        108: "https://apiservice.mol.gov.tw/OdService/download/A17000000J-030157-955",
        109: "https://apiservice.mol.gov.tw/OdService/download/A17000000J-030157-tqe",
        110: "https://apiservice.mol.gov.tw/OdService/download/A17000000J-030157-zzg",
        111: "https://apiservice.mol.gov.tw/OdService/download/A17000000J-030157-wAG",
        112: "https://apiservice.mol.gov.tw/OdService/download/A17000000J-030157-jwL",
        113: "https://apiservice.mol.gov.tw/OdService/download/A17000000J-030157-ESm",
    }

    if (
        max([x for x in urls.keys() if isinstance(x, int)]) + 1911 + 1 < datetime.now().year
        and datetime.now().month > 6
    ):
        print(f"請更新 {key}")

    df = []
    for filename, url in urls.items():
        path = EXTRA_DATA_DIR / key / f"{filename}.csv.gz"
        data = read_csv_with_cache(path, url)
        df.append(data)

    df = pd.concat(df, ignore_index=True)

    return df


# https://data.gov.tw/dataset/6449 新制勞工退休基金歷年最近月份收益率
def df_新制勞工退休基金歷年最近月份收益率():
    url = "https://apiservice.mol.gov.tw/OdService/download/A17000000J-020044-Wkt"

    df = read_csv(url)
    df = df.loc[::-1]

    df["最近月份收益率"] = df["最近月份收益率"].astype(float)

    return df


# https://data.gov.tw/dataset/6742 全國賦稅收入實徵淨額日曆年別-按稅目別與地區別分
def df_全國賦稅收入實徵淨額日曆年別_按稅目別與地區別分():
    url = "https://web02.mof.gov.tw/njswww/webMain.aspx?sys=220&ym=9000&kind=21&type=5&funid=i0424&cycle=41&outmode=12&compmode=00&outkind=2&fldspc=0,30,&codspc0=0,39,40,3,&utf=1"

    df = read_csv(url)
    df = df.replace("－", "0.0")
    df[df.columns[1:]] = df[df.columns[1:]].astype(float)

    split = df["地區別"].str.split("/", expand=True)
    df["時間"] = split[0].str.strip()
    df["地區"] = split[1].str.strip()

    return df


# https://data.gov.tw/dataset/16910 全國賦稅收入實徵淨額與預算數之比較
def df_全國賦稅收入實徵淨額與預算數之比較():
    url = "https://web02.mof.gov.tw/njswww/webMain.aspx?sys=220&ym=9000&kind=21&type=3&funid=i3451&cycle=4&outmode=12&compmode=00&outkind=3&fldlst=111&codspc0=0,34,&utf=1"

    df = read_csv(url)
    df = df.replace("－", 0.0)
    df[df.columns[1:]] = df[df.columns[1:]].astype(float)
    split = df["項目別"].str.split("/", expand=True)
    df["時間"] = split[0].str.strip()
    df["類別"] = split[1].str.strip()

    return df


# https://www.stat.gov.tw/cp.aspx?n=3914 主計總處統計專區 家庭收支調查 統計表 常用資料
def df_主計總處統計專區_家庭收支調查_統計表_常用資料():
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

    r = session.get(url_可支配所得平均數, verify=False)
    df_平均每戶可支配所得 = get_data(r.content, [0, 2], 2)
    df_平均每人可支配所得 = get_data(r.content, [1, 3], 2)

    r = session.get(url_可支配所得中位數, verify=False)
    df_每戶可支配所得中位數 = get_data(r.content, [0, 2], 2)
    df_每人可支配所得中位數 = get_data(r.content, [1, 3], 2)

    return (
        df_平均每人月消費,
        df_平均每戶可支配所得,
        df_平均每人可支配所得,
        df_每戶可支配所得中位數,
        df_每人可支配所得中位數,
    )


# https://data.gov.tw/dataset/15392 進出口貿易值_按洲別 ∕ 國別分(CY2001~)
def df_進口貿易值_按洲別_國別分():
    url = "https://service.mof.gov.tw/public/data/statistic/trade/u2010im.csv"
    index_col = "Imports from Country (Unit : US$ Thousand)"
    columns_remove_patt = r"\(千元\)"
    encoding = "big5"

    df = read_csv(url, encoding)
    df = df.loc[(df[df.columns[1:]] != "－").any(axis=1)]
    df[df.columns[1:]] = df[df.columns[1:]].astype(float)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/15392 進出口貿易值_按洲別 ∕ 國別分(CY2001~)
def df_出口貿易值_按洲別_國別分():
    url = "https://service.mof.gov.tw/public/data/statistic/trade/u2010ex.csv"
    index_col = "Exports to Country (Unit: US$ Thousand)"
    columns_remove_patt = r"\(千元\)"
    encoding = "big5"

    df = read_csv(url, encoding)
    df = df.loc[(df[df.columns[1:]] != "－").any(axis=1)]
    df[df.columns[1:]] = df[df.columns[1:]].astype(float)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/28573 進出口貿易值_按國際商品統一分類制度(HS)及主要國別分
def df_進出口貿易值_按國際商品統一分類制度_HS_及主要國別分():
    url = "https://web02.mof.gov.tw/njswww/webMain.aspx?sys=220&ym=9000&kind=21&type=4&funid=i9901&cycle=41&outmode=12&compmode=00&outkind=1&fldspc=0,1,3,4,&codlst0=11&codspc1=0,20,&utf=1"

    df = read_csv(url)
    df = df.set_index("國家別")
    df.columns = pd.MultiIndex.from_tuples(
        [[s.strip() for s in col.split("/")] for col in df.columns.str.replace("(千美元)", "")]
    )

    return df


# https://data.gov.tw/dataset/8381 進口值_按主要貨品分
def df_進口值_按主要貨品分():
    url = "https://web02.mof.gov.tw/njswww/webMain.aspx?sys=220&ym=9000&kind=21&type=4&funid=i8122&cycle=41&outmode=12&compmode=00&outkind=1&fld0=1&codlst0=11100011010101000111111101001000001101&utf=1"
    index_col = "幣別"
    columns_remove_patt = r"按美元計算\(百萬美元\)\/ "
    encoding = "utf-8"

    df = read_csv(url, encoding)
    df = df.loc[(df[df.columns[1:]] != "－").any(axis=1)]
    df[df.columns[1:]] = df[df.columns[1:]].astype(float)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/8380 出口值_按主要貨品分
def df_出口值_按主要貨品分():
    url = "https://web02.mof.gov.tw/njswww/webMain.aspx?sys=220&ym=9000&kind=21&type=4&funid=i8121&cycle=41&outmode=12&compmode=00&outkind=1&fld0=1&codlst0=1101111010100011110111100111110110100&utf=1"
    index_col = "幣別"
    columns_remove_patt = r"按美元計算\(百萬美元\)\/ "
    encoding = "utf-8"

    df = read_csv(url, encoding)
    df = df.loc[(df[df.columns[1:]] != "－").any(axis=1)]
    df[df.columns[1:]] = df[df.columns[1:]].astype(float)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/8388 貿易指數－進口單位價值指數
def df_貿易指數_進口單位價值指數():
    url = "https://web02.mof.gov.tw/njswww/webMain.aspx?sys=220&ym=10000&kind=21&type=4&funid=i9303&cycle=41&outmode=12&compmode=00&outkind=11&fldspc=0,6,7,1,9,1,11,7,20,1,28,2,&utf=1"
    index_col = "貨品別"
    columns_remove_patt = r""
    encoding = "utf-8"

    df = read_csv(url, encoding)
    df = df.loc[(df[df.columns[1:]] != "－").any(axis=1)]
    df[df.columns[1:]] = df[df.columns[1:]].astype(float)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/8386 貿易指數－出口單位價值指數
def df_貿易指數_出口單位價值指數():
    url = "https://web02.mof.gov.tw/njswww/webMain.aspx?sys=220&ym=10000&kind=21&type=4&funid=i9302&cycle=41&outmode=12&compmode=00&outkind=11&fldspc=0,8,9,7,17,1,25,2,&utf=1"
    index_col = "貨品別"
    columns_remove_patt = r""
    encoding = "utf-8"

    df = read_csv(url, encoding)
    df = df.loc[(df[df.columns[1:]] != "－").any(axis=1)]
    df[df.columns[1:]] = df[df.columns[1:]].astype(float)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/8389 貿易指數－進口數量指數
def df_貿易指數_進口數量指數():
    url = "https://web02.mof.gov.tw/njswww/webMain.aspx?sys=220&ym=10000&kind=21&type=4&funid=i9305&cycle=41&outmode=12&compmode=00&outkind=11&fldspc=0,6,7,1,9,1,11,7,20,1,28,2,&utf=1"
    index_col = "貨品別"
    columns_remove_patt = r""
    encoding = "utf-8"

    df = read_csv(url, encoding)
    df = df.loc[(df[df.columns[1:]] != "－").any(axis=1)]
    df[df.columns[1:]] = df[df.columns[1:]].astype(float)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/8387 貿易指數－出口數量指數
def df_貿易指數_出口數量指數():
    url = "https://web02.mof.gov.tw/njswww/webMain.aspx?sys=220&ym=10000&kind=21&type=4&funid=i9304&cycle=41&outmode=12&compmode=00&outkind=11&fldspc=0,8,9,7,17,1,25,2,&utf=1"
    index_col = "貨品別"
    columns_remove_patt = r""
    encoding = "utf-8"

    df = read_csv(url, encoding)
    df = df.loc[(df[df.columns[1:]] != "－").any(axis=1)]
    df[df.columns[1:]] = df[df.columns[1:]].astype(float)
    df = df.set_index(index_col)
    df.columns = df.columns.str.replace(columns_remove_patt, "", regex=True)

    return df


# https://data.gov.tw/dataset/77132 村里戶數、單一年齡人口（新增區域代碼）
# API 說明文件
# https://www.ris.gov.tw/rs-opendata/api/Main/docs/v1
# API 路徑
# https://www.ris.gov.tw/rs-opendata/api/v1/datastore/ODRP014/{yyymm} 請指定年月
def df_村里戶數_單一年齡人口():
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
                except Exception as e:
                    print(key)
                    print("no data", year, month, url, e)
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
    df = df.copy()
    df["縣市"] = split[0].str.strip()
    df["鄉鎮"] = split[1].str.strip()

    return df, year, month


# https://data.gov.tw/dataset/117986 現住人口性別、年齡、婚姻狀況(含同婚)
# API 說明文件
# https://www.ris.gov.tw/rs-opendata/api/Main/docs/v1
# API 路徑
# https://www.ris.gov.tw/rs-opendata/api/v1/datastore/ODRP052/{yyy} 請指定年
def df_現住人口性別_年齡_婚姻狀況():
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
                except Exception as e:
                    print(key)
                    print("no data", year, url, e)
                    continue
            else:
                return {}

        with gzip.open(path, "rb") as f:
            data = json.load(f)

        return data

    for year in range(108, datetime.today().year - 1911):
        page = 1
        json_data = get_data(year, page)
        if "responseData" not in json_data:
            print(f"無法獲取資料 {year} {key}")
            continue

        data = pd.json_normalize(json_data["responseData"])
        df.append(data)

        pages = int(json_data["totalPage"])
        for page in range(2, pages + 1):
            json_data = get_data(year, page)
            data = pd.json_normalize(json_data["responseData"])
            df.append(data)

    df = pd.concat(df, ignore_index=True)
    df["population"] = df["population"].astype(int)
    split = (
        df["site_id"].str.replace("(^.{3})", r"\1|", regex=True).str.split("|", n=1, expand=True)
    )
    df["縣市"] = split[0].str.strip()
    df["鄉鎮"] = split[1].str.strip()

    return df


# https://data.gov.tw/dataset/32970 動態資料統計表
# https://data.gov.tw/dataset/77139 動態資料統計表（新增區域代碼）
# https://data.gov.tw/dataset/131135 動態資料統計表（含同婚）
# API 說明文件
# https://www.ris.gov.tw/rs-opendata/api/Main/docs/v1
# API 路徑
# https://www.ris.gov.tw/rs-opendata/api/v1/datastore/ODRP003/{yyymm} 請指定年月
# https://www.ris.gov.tw/rs-opendata/api/v1/datastore/ODRP012/{yyymm} 請指定年月
# https://www.ris.gov.tw/rs-opendata/api/v1/datastore/ODRP061/{yyymm} 請指定年月
def df_動態資料統計表():
    key = "動態資料統計表（含同婚）"
    key = sanitize_filename(key)
    url_year_page_10601_10612 = (
        "https://www.ris.gov.tw/rs-opendata/api/v1/datastore/ODRP003/{year}{month:02d}?page={page}"
    )
    url_year_page_10701_10908 = (
        "https://www.ris.gov.tw/rs-opendata/api/v1/datastore/ODRP012/{year}{month:02d}?page={page}"
    )
    url_year_page = (
        "https://www.ris.gov.tw/rs-opendata/api/v1/datastore/ODRP061/{year}{month:02d}?page={page}"
    )
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
                except Exception as e:
                    print(key)
                    print("no data", yearmonth, url, e)
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
    df["人口自然增加數_合計"] = df["出生數_合計"] - df["死亡人數_合計"]
    df["人口自然增加數_男"] = df["出生數_合計_男"] - df["死亡人數_男"]
    df["人口自然增加數_女"] = df["出生數_合計_女"] - df["死亡人數_女"]
    df["婚姻自然增加數_合計"] = df["結婚對數_合計"] - df["離婚對數_合計"]

    return df


# https://data.gov.tw/dataset/139388 結婚人數按婚姻類型、性別、年齡、原屬國籍（地區）及教育程度分(按登記)
# API 說明文件
# https://www.ris.gov.tw/rs-opendata/api/Main/docs/v1
# API 路徑
# https://www.ris.gov.tw/rs-opendata/api/v1/datastore/ODRP068/{yyy} 請指定年
def df_結婚人數按婚姻類型_性別_年齡_原屬國籍_地區_及教育程度分_按登記():
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

    return df


# https://data.gov.tw/dataset/130547 結婚對數按婚姻類型、性別及年齡分(按登記)
def df_結婚對數按婚姻類型_性別及年齡分_按登記():
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

    if max(urls.keys()) + 1911 + 1 < datetime.now().year and datetime.now().month > 3:
        print(f"請更新 {key}")

    df = []
    for filename, url in urls.items():
        path = EXTRA_DATA_DIR / key / f"{filename}.csv.gz"
        data = read_csv_with_cache(path, url)
        df.append(data)

    df = pd.concat(df, ignore_index=True)
    split = df["區域別"].str.replace("(^.{3})", r"\1|", regex=True).str.split("|", n=1, expand=True)
    df["縣市"] = split[0].str.strip()
    df["鄉鎮"] = split[1].str.strip()

    return df


# https://data.gov.tw/dataset/32945 嬰兒出生數按性別、生母原屬國籍（地區）、年齡及教育程度分(按登記)
# API 說明文件
# https://www.ris.gov.tw/rs-opendata/api/Main/docs/v1
# API 路徑
# https://www.ris.gov.tw/rs-opendata/api/v1/datastore/ODRP028/{yyy} 請指定年
def df_嬰兒出生數按性別_生母原屬國籍_地區_年齡及教育程度分_按登記():
    key = "嬰兒出生數按性別、生母原屬國籍（地區）、年齡及教育程度分(按登記)"
    key = sanitize_filename(key)
    url_year_page = "https://www.ris.gov.tw/rs-opendata/api/v1/datastore/ODRP028/{year}?page={page}"
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
    df["mother_age"] = df["mother_age"].str.replace("～", "~")

    return df


# https://data.gov.tw/dataset/102764 嬰兒出生數按性別、生父原屬國籍（地區）、年齡及教育程度分(按登記)
def df_嬰兒出生數按性別_生父原屬國籍_地區_年齡及教育程度分_按登記():
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

    if max(urls.keys()) + 1911 + 2 < datetime.now().year and datetime.now().month > 3:
        print(f"請更新 {key}")

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
    df["生父年齡"] = df["生父年齡"].str.replace("～", "~").str.replace(" ", "")

    return df


# https://data.gov.tw/dataset/127527 嬰兒出生數按嬰兒性別及生父母年齡分(按登記)
# API 說明文件
# https://www.ris.gov.tw/rs-opendata/api/Main/docs/v1
# API 路徑
# https://www.ris.gov.tw/rs-opendata/api/v1/datastore/ODRP064/{yyy} 請指定年
def df_嬰兒出生數按嬰兒性別及生父母年齡分_按登記():
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
    df["mother_age"] = df["mother_age"].str.replace("～", "~")
    df["father_age"] = df["father_age"].str.replace("～", "~")

    return df


# https://data.gov.tw/dataset/102765 嬰兒出生數按生母年齡及出生身分分(按登記)
def df_嬰兒出生數按生母年齡及出生身分分_按登記():
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

    if max(urls.keys()) + 1911 + 1 < datetime.now().year and datetime.now().month > 3:
        print(f"請更新 {key}")

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
    df["生母年齡"] = df["生母年齡"].str.replace("～", "~").str.replace(" ", "")

    return df


# https://data.gov.tw/dataset/100324 嬰兒出生數按性別、胎次及生母年齡分(按登記)
def df_嬰兒出生數按性別_胎次及生母年齡分_按登記():
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

    if max(urls.keys()) + 1911 + 1 < datetime.now().year and datetime.now().month > 3:
        print(f"請更新 {key}")

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
    df["生母年齡"] = df["生母年齡"].str.replace("～", "~").str.replace(" ", "")

    return df


# https://data.gov.tw/dataset/152789 嬰兒出生數按嬰兒性別及出生胎別分(按登記)
def df_嬰兒出生數按嬰兒性別及出生胎別分_按登記():
    key = "嬰兒出生數按嬰兒性別及出生胎別分(按登記)"
    key = sanitize_filename(key)
    urls = {
        110: "https://data.moi.gov.tw/MoiOD/System/DownloadFile.aspx?DATA=C89BA952-9140-47C5-805A-34F3A0773978",
        111: "https://data.moi.gov.tw/MoiOD/System/DownloadFile.aspx?DATA=CBD76D1D-2609-4553-9B8E-06BA912249B8",
        112: "https://data.moi.gov.tw/MoiOD/System/DownloadFile.aspx?DATA=C7432B38-55F0-4DF1-8C91-33770B7A823C",
        113: "https://data.moi.gov.tw/MoiOD/System/DownloadFile.aspx?DATA=A6B28B8E-F601-452F-925A-75A6320A633F",
    }

    if max(urls.keys()) + 1911 + 1 < datetime.now().year and datetime.now().month > 3:
        print(f"請更新 {key}")

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

    return df


# https://data.gov.tw/dataset/139390 離婚/終止結婚人數按婚姻類型、性別、年齡、原屬國籍（地區）及教育程度分(按登記)
# API 說明文件
# https://www.ris.gov.tw/rs-opendata/api/Main/docs/v1
# API 路徑
# https://www.ris.gov.tw/rs-opendata/api/v1/datastore/ODRP070/{yyy} 請指定年
def df_離婚_終止結婚人數按婚姻類型_性別_年齡_原屬國籍_地區_及教育程度分_按登記():
    key = "離婚/終止結婚人數按婚姻類型、性別、年齡、原屬國籍（地區）及教育程度分(按登記)"
    key = sanitize_filename(key)
    url_year_page = "https://www.ris.gov.tw/rs-opendata/api/v1/datastore/ODRP070/{year}?page={page}"
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

    return df


# https://data.gov.tw/dataset/41236 全國公立動物收容所收容處理情形統計表
def df_全國公立動物收容所收容處理情形統計表():
    url = "https://data.moa.gov.tw/Service/OpenData/TransService.aspx?UnitId=DyplMIk3U1hf&IsTransData=1"
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

    return df


# https://www.twse.com.tw/zh/products/broker/month-rank.html 定期定額交易戶數統計排行月報表
# https://www.twse.com.tw/rwd/zh/brokerService/ETFRank?date={year}{month}01&response=json&_={timestamp} 請指定年月 timestamp
def df_定期定額交易戶數統計排行月報表():
    key = "定期定額交易戶數統計排行月報表"
    key = sanitize_filename(key)
    url_year_month = "https://www.twse.com.tw/rwd/zh/brokerService/ETFRank?date={year}{month:02d}01&response=json&_={timestamp}"
    df = []

    def get_data(year, month):
        path = EXTRA_DATA_DIR / key / f"{year}{month:02d}.json.gz"
        _ensure_dir_exists(path)

        if not path.is_file():
            timestamp = int(datetime.now().timestamp() * 1000)
            url = url_year_month.format(year=year, month=month, timestamp=timestamp)
            time.sleep(1)
            r = session.get(url)

            if r.json()["stat"] != "OK":
                return {}

            with gzip.open(path, "wb") as f:
                f.write(r.content)

        with gzip.open(path, "rb") as f:
            data = json.load(f)

        return data

    for year in range(2020, datetime.today().year + 1):
        for month in range(1, 12 + 1):
            if year == 2020 and month < 10:
                continue
            elif year == datetime.today().year and month >= datetime.today().month:
                continue

            json_data = get_data(year, month)
            if "data" in json_data:
                datas = [
                    [[data[1], data[2], data[3]], [data[4], data[5], data[6]]]
                    for data in json_data["data"]
                ]
                datas = [data for data_two in datas for data in data_two]
                data = pd.DataFrame(datas, columns=["代號", "名稱", "交易戶數"])
                data["交易戶數"] = data["交易戶數"].str.replace(",", "").astype(int)
                data["年月"] = f"{year}{month:02d}"
                df.append(data)

    df = pd.concat(df, ignore_index=True)

    return df


# https://www.tdcc.com.tw/portal/zh/smWeb/qryStock
# https://data.gov.tw/dataset/11452 集保戶股權分散表
# https://schema.nat.gov.tw/gsp/frontstage/resource.download/9f7d9184-20a0-49eb-b7dd-96e23686c4ef
def df_集保戶股權分散表():
    url = "https://opendata.tdcc.com.tw/getOD.ashx?id=1-5"

    df = read_csv(url)
    df["證券代號"] = df["證券代號"].str.strip()

    symbols = df["證券代號"].unique().tolist()
    infos = 查詢_證券編碼(symbols)

    def 分級(x):
        match x:
            case 1:
                return "1-999"
            case 2:
                return "1,000-5,000"
            case 3:
                return "5,001-10,000"
            case 4:
                return "10,001-15,000"
            case 5:
                return "15,001-20,000"
            case 6:
                return "20,001-30,000"
            case 7:
                return "30,001-40,000"
            case 8:
                return "40,001-50,000"
            case 9:
                return "50,001-100,000"
            case 10:
                return "100,001-200,000"
            case 11:
                return "200,001-400,000"
            case 12:
                return "400,001-600,000"
            case 13:
                return "600,001-800,000"
            case 14:
                return "800,001-1,000,000"
            case 15:
                return "1,000,001 以上"
            case 16:
                return "差異數調整"
            case 17:
                return "合計"

    df["持股分級說明"] = df["持股分級"].apply(分級)

    def 全名(symbol):
        info = infos[infos["有價證券代號"] == symbol]
        if len(info) == 1:
            return f"{symbol:7s} {info["有價證券名稱"].iat[0]}"
        else:
            return symbol

    df["全名"] = df["證券代號"].apply(全名)

    return df


# https://isin.twse.com.tw/isin/single_i.jsp 證券編碼_單筆/多筆查詢
def 查詢_證券編碼(symbols):
    url = "https://isin.twse.com.tw/isin/single_main.jsp"

    datas = []
    interval = 1000
    for i in range(0, len(symbols), interval):
        r = session.get(
            url, params={"owncode": ",".join(symbols[i : i + interval]), "stockname": ""}
        )
        try:
            data = pd.read_html(io.StringIO(r.text), encoding="big5")[0]
            data.columns = data.iloc[0]
            data = data.drop(0)
            datas.append(data)
        except Exception as e:
            print("查詢_證券編碼")
            print(e)
            print(r.url)

        time.sleep(1)

    df = pd.concat(datas, ignore_index=True)
    df.index.name = None
    df.columns.name = None

    return df


# https://data.gov.tw/dataset/7232 銀行間市場新臺幣對美元收盤匯率
def df_銀行間市場新臺幣對美元收盤匯率():
    key = "銀行間市場新臺幣對美元收盤匯率"
    key = sanitize_filename(key)
    path = EXTRA_DATA_DIR / key / "data.csv"
    _ensure_dir_exists(path)

    url = (
        "https://www.cbc.gov.tw/public/data/OpenData/%E5%A4%96%E5%8C%AF%E5%B1%80/FTDOpenData015.csv"
    )
    df = read_csv(url)

    df.loc[:, "Date"] = pd.to_datetime(df["日期"], format="%Y%m%d")
    df.loc[:, "Close"] = df["NTD/USD"].astype(float)
    df.loc[:, "Adj Close"] = df["Close"]
    df.loc[:, "Dividends"] = 0
    df.loc[:, "Stock Splits"] = 0

    df.to_csv(path, index=False, lineterminator="\n")

    return df


def update():
    df_銀行間市場新臺幣對美元收盤匯率()

    df_公開資訊觀測站_財務報告附註揭露之員工福利薪資資訊()
    df_村里戶數_單一年齡人口()
    df_現住人口性別_年齡_婚姻狀況()
    df_動態資料統計表()
    df_結婚人數按婚姻類型_性別_年齡_原屬國籍_地區_及教育程度分_按登記()
    df_離婚_終止結婚人數按婚姻類型_性別_年齡_原屬國籍_地區_及教育程度分_按登記()
    df_嬰兒出生數按性別_生母原屬國籍_地區_年齡及教育程度分_按登記()
    df_嬰兒出生數按嬰兒性別及生父母年齡分_按登記()
    df_定期定額交易戶數統計排行月報表()
    df_綜稅總所得各縣市申報統計分析表()
    df_綜稅綜合所得總額全國各縣市鄉鎮村里統計分析表()

    df_人力資源調查重要指標()
    df_教育程度別失業率()
    df_年齡組別失業率()
    df_教育程度別失業率_按年齡分()
    df_家庭部門平均每戶資產負債()
    df_各業廠商僱用職缺按月計薪者每人每月平均最低薪資_按職類及員工規模分()
    df_各業廠商調升經常性薪資參考各項因素之廠商比率_按行業分()
    df_各業廠商調升員工經常性薪資之廠商與員工人數比率_按行業分()
    df_結婚對數按婚姻類型_性別及年齡分_按登記()
    df_嬰兒出生數按性別_生父原屬國籍_地區_年齡及教育程度分_按登記()
    df_嬰兒出生數按生母年齡及出生身分分_按登記()
    df_嬰兒出生數按性別_胎次及生母年齡分_按登記()
    df_嬰兒出生數按嬰兒性別及出生胎別分_按登記()
    df_歷史_勞工退休金提繳統計年報_按地區_行業及規模別()
    df_勞工退休金提繳單位_提繳人數_提繳工資_提繳金額概況()
    df_勞工退休金平均提繳工資_按行業別()
    df_勞工退休金平均提繳工資_按年齡組別()


if __name__ == "__main__":
    update()
