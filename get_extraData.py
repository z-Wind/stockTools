import pandas as pd
import io
import requests
import time

from pathlib import Path
from datetime import datetime
from dateutil.relativedelta import relativedelta

from utils import gen_iter_date_by_month


def transform_date(date: str) -> str:  # 民國轉西元
    y, m, d = date.split("/")
    return str(int(y) + 1911) + "/" + m + "/" + d


def process_data(data: str) -> float | None:
    """清理原始字串並轉為 float，空值或無效值回傳 None。"""
    data = data.replace(",", "").replace("--", "").strip()
    return float(data) if data else None


def get_existing_dates(dir_path) -> set[str]:
    dir_path = Path(dir_path)

    """獲取已下載的日期集合，並安全地移除當月未完整的檔案。"""
    if not dir_path.exists():
        return set()

    # 僅撈取當前目錄下的 .csv 檔案，避免 os.walk 的遞迴隱患
    csv_files = sorted([f.name for f in dir_path.glob("*.csv")])
    if not csv_files:
        return set()

    # 計算當月1號的檔名檔名 (e.g., "20260601.csv")
    current_month_filename = (
        f"{(datetime.now() + relativedelta(day=1)).strftime('%Y%m%d')}.csv"
    )

    # 如果最後一個檔案是當月資料，將其刪除以防資料不全
    if csv_files[-1] == current_month_filename:
        target_file = dir_path / current_month_filename
        try:
            target_file.unlink(missing_ok=True)  # 安全刪除，若不存在也不報錯
            csv_files.pop()
            print(f"安全移除未完整的當月暫存檔: {current_month_filename}")
        except OSError as e:
            print(f"無法刪除檔案 {target_file}: {e}")

    # 回傳主檔名集合 (移除 .csv 尾綴)，改用 set 讓查詢複雜度從 O(N) 降到 O(1)
    return {Path(f).stem for f in csv_files}


def save_twse_ftse_index(
    s: requests.Session, symbol: str, url_symbol: str, start: datetime
) -> None:
    """下載並儲存台灣證交所 FTSE 指數歷史資料。"""
    save_dir = Path("./extraData") / symbol
    save_dir.mkdir(parents=True, exist_ok=True)

    # 獲取已存在的日期 set
    existing_dates = get_existing_dates(save_dir)
    end_date = datetime.now() + relativedelta(day=1)

    for day in gen_iter_date_by_month(start, end_date):
        date_str = day.strftime("%Y%m%d")

        # Set 查詢速度極快
        if date_str in existing_dates:
            print(f"{symbol} {date_str} already exists")
            continue

        url = f"https://www.twse.com.tw/rwd/zh/FTSE/{url_symbol}?response=csv&date={date_str}"
        print(f"{symbol} {url_symbol} {date_str} saving... from {url}")

        try:
            response = s.get(url, timeout=150)
            response.raise_for_status()  # 確保 HTTP 狀態碼為 200
            content = response.content
        except requests.RequestException as e:
            print(f"網路請求失敗 {date_str}: {e}")
            continue

        is_valid, csv_text = is_valid_csv_response(content, date_str, symbol)
        if not is_valid:
            # 可能是維護中或無資料，上面函式已印出日誌，這裡直接安全跳過
            continue

        # 走到這裡，確保拿到的絕對是乾淨的 CSV 文字內容
        try:
            df = pd.read_csv(io.StringIO(csv_text), header=1)
        except pd.errors.EmptyDataError:
            print(f"{symbol} {date_str} is empty")
            continue

        # --- 數據清洗與轉換 (Pandas 最佳實踐) ---
        df = df.dropna(axis=1)

        # 避免 SettingWithCopyWarning，明確建立副本
        reward_symbol = symbol.replace("指數", "報酬指數")
        if reward_symbol in df.columns:
            df = df[df[reward_symbol] != "--"].copy()
        else:
            print(f"警告: 找不到欄位 {reward_symbol}")
            continue

        if df.empty:
            print(f"{symbol} {date_str} 過濾後無有效資料")
            continue

        # 資料欄位建立
        df["Date"] = pd.to_datetime(df["日期"].apply(transform_date), format="mixed")
        df["Close"] = df[symbol].apply(process_data).astype(float)
        df["Adj Close"] = df[reward_symbol].apply(process_data).astype(float)
        df["Dividends"] = 0
        df["Stock Splits"] = 0

        # 過濾掉 Adj Close 為 0 的資料
        df = df[df["Adj Close"] != 0]

        # 儲存結果
        output_file = save_dir / f"{date_str}.csv"
        df.to_csv(output_file, index=False, lineterminator="\n")

        # 禮貌爬蟲，延時放最後，確保下載完才冷卻
        time.sleep(5)

# https://www.twse.com.tw/zh/indices/ftse/tai50i.html
def save_TAI50I_index(s):
    save_twse_ftse_index(s, "臺灣50指數", "TAI50I", start=datetime(2002, 10, 1))

# https://www.twse.com.tw/zh/indices/ftse/tai100i.html
def save_TAI100I_index(s):
    save_twse_ftse_index(s, "臺灣中型100指數", "TAI100I", start=datetime(2004, 11, 1))

# https://www.twse.com.tw/zh/indices/ftse/taidividi.html
def save_TAIDIVIDI_index(s):
    save_twse_ftse_index(s, "臺灣高股息指數", "TAIDIVIDI", start=datetime(2007, 1, 1))

# https://www.twse.com.tw/zh/indices/taiex/mi-5min-hist.html
def save_TAIEX_index(s: requests.Session) -> None:
    symbol = "臺灣加權股價指數"
    save_dir = Path("./extraData") / symbol
    save_dir.mkdir(parents=True, exist_ok=True)

    # 取得已下載的日期集合 (Set 格式)
    datas = get_existing_dates(save_dir)

    start = datetime(2003, 1, 1)
    end = datetime.now() + relativedelta(day=1)  # 設定為當月的 1 號

    for day in gen_iter_date_by_month(start, end):
        d = day.strftime("%Y%m%d")

        if d in datas:
            print(f"{symbol} {d} already exists")
            continue

        histURL = f"https://www.twse.com.tw/rwd/zh/TAIEX/MI_5MINS_HIST?response=csv&date={d}"
        totalReturnURL = f"https://www.twse.com.tw/rwd/zh/TAIEX/MFI94U?response=csv&date={d}"

        # ------------------ 1. 下載並解析歷史價格 (hist) ------------------
        print(f"{symbol} {d} get history... from {histURL}")
        try:
            res_hist = s.get(histURL, timeout=150)
            res_hist.raise_for_status()
            c_hist = res_hist.content
        except requests.RequestException as e:
            print(f"{symbol} {d} 歷史資料網路請求失敗: {e}，跳過此月")
            continue

        # 使用統一驗證器，自動過濾維護網頁與無資料 JSON
        is_hist_valid, hist_text = is_valid_csv_response(c_hist, d, f"{symbol}(歷史)")
        if not is_hist_valid:
            continue

        try:
            hist = pd.read_csv(io.StringIO(hist_text), header=1).dropna(axis=1)
        except pd.errors.EmptyDataError:
            print(f"{symbol} {d} 歷史資料為空，跳過此月")
            continue

        # ------------------ 2. 下載並解析報酬指數 (totalReturn) ------------------
        print(f"{symbol} {d} get total return... from {totalReturnURL}")
        try:
            res_return = s.get(totalReturnURL, timeout=150)
            res_return.raise_for_status()
            c_return = res_return.content
        except requests.RequestException as e:
            print(f"{symbol} {d} 報酬指數網路請求失敗: {e}，跳過此月")
            continue

        # 使用統一驗證器，自動過濾維護網頁與無資料 JSON
        is_return_valid, return_text = is_valid_csv_response(
            c_return, d, f"{symbol}(報酬)"
        )
        if not is_return_valid:
            continue

        try:
            totalReturn = pd.read_csv(io.StringIO(return_text), header=1).dropna(
                axis=1
            )
        except pd.errors.EmptyDataError:
            print(f"{symbol} {d} 報酬指數資料為空，跳過此月")
            continue

        # ------------------ 3. 資料合併與對齊 (Merge 最佳實踐) ------------------
        hist.columns = hist.columns.str.replace(r"\s+", "", regex=True)
        totalReturn.columns = totalReturn.columns.str.replace(
            r"\s+", "", regex=True
        )

        if "日期" not in hist.columns or "日期" not in totalReturn.columns:
            print(f"{symbol} {d} 欄位解析異常，無法對齊")
            continue

        df = pd.merge(hist, totalReturn, on="日期", how="inner")
        if df.empty:
            print(f"{symbol} {d} 合併後無交集資料，跳過")
            continue

        # ------------------ 4. 欄位清洗與轉換 ------------------
        df["Date"] = pd.to_datetime(
            df["日期"].apply(transform_date), format="mixed"
        )
        df["Open"] = df["開盤指數"].apply(process_data).astype(float)
        df["High"] = df["最高指數"].apply(process_data).astype(float)
        df["Low"] = df["最低指數"].apply(process_data).astype(float)
        df["Close"] = df["收盤指數"].apply(process_data).astype(float)
        df["Adj Close"] = (
            df["發行量加權股價報酬指數"].apply(process_data).astype(float)
        )
        df["Dividends"] = 0
        df["Stock Splits"] = 0

        # 過濾無效資料並儲存
        df = df[df["Adj Close"] != 0]
        output_file = save_dir / f"{d}.csv"
        df.to_csv(output_file, index=False, lineterminator="\n")

        # 禮貌爬蟲延時
        time.sleep(5)

def is_valid_csv_response(content: bytes, date_str: str, symbol: str) -> tuple[bool, str]:
    """
    檢查證交所回傳的內容是否為有效的 CSV 資料。
    攔截網站維護、假日無資料等非預期網頁。
    
    回傳: (是否有效, 解碼後的文字內容)
    """
    # 1. 優先嘗試用 big5 解碼 (正常 CSV 格式)
    try:
        text = content.decode("big5")
    except UnicodeDecodeError:
        # 2. 解碼失敗時，嘗試用 utf-8 解碼 (通常是錯誤 JSON 或維護 HTML 網頁)
        try:
            text = content.decode("utf-8", errors="ignore")
        except Exception:
            print(f"❌ {symbol} {date_str}: 完全無法解碼的未知內容")
            return False, ""

    # 3. 特徵值攔截：檢查是否為證交所維護網頁
    maintenance_keywords = ["網站維護", "系統維護", "維護中", "<!DOCTYPE html", "<html"]
    if any(kw in text for kw in maintenance_keywords):
        print(f"⚠️ {symbol} {date_str}: 證交所網站維護中！伺服器目前拒絕服務，跳過此月。")
        return False, ""

    # 4. 特徵值攔截：檢查是否為無資料提示 (JSON)
    error_keywords = ["很抱歉", '"stat"', "沒有符合條件的資料"]
    if any(kw in text for kw in error_keywords):
        print(f"ℹ️ {symbol} {date_str}: 證交所回傳無資料 (假日或尚未開盤)。")
        return False, ""

    return True, text

if __name__ == "__main__":
    with requests.Session() as s:
        save_TAI50I_index(s)
        save_TAI100I_index(s)
        save_TAIEX_index(s)
        save_TAIDIVIDI_index(s)
