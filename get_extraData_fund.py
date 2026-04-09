import asyncio
import os
import aiohttp
import csv
import random
import ssl
import time
from datetime import date, timedelta, datetime, timezone
from pathlib import Path
from pyquery import PyQuery
from dateutil.relativedelta import relativedelta

# --- 設定與常數 ---
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

taiwan_timezone = timezone(timedelta(hours=8))
BASE_URL = "https://www.sitca.org.tw/ROC/Industry/IN2106.aspx?pid=IN2213_02"


# --- 輔助函式 ---
def read_csv_to_dict(file_path: Path) -> dict:
    data_map = {}
    if not file_path.exists():
        return data_map
    with open(file_path, "r", newline="", encoding="utf-8") as csvfile:
        csv_reader = csv.reader(csvfile)
        next(csv_reader, None)  # 跳過表頭
        for row in csv_reader:
            if not row:
                continue
            dt = datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S%z")
            val = None if row[1] == "" else float(row[1])
            data_map[dt] = val
    return data_map


def parse_response(text: str, fund_query: dict) -> float | None:
    """
    解析投信投顧公會網頁，取得指定基金的淨值。
    支援雙重過濾：公司代號 (comid) 與 基金名稱 (filter)。
    """
    if not text:
        return None

    dom = PyQuery(text)
    # 同時選取偶數列 (DTeven) 與 奇數列 (DTodd) 以免遺漏
    rows = dom("tr.DTeven, tr.DTodd")

    if not rows:
        return None

    try:
        # 遍歷每一列進行精確匹配
        for i in range(len(rows)):
            row = rows.eq(i)
            tds = row("td")

            # 欄位索引說明：
            # index 1: 公司代號 (如 A0005)
            # index 5: 基金名稱 (如 元大台灣卓越50基金)
            # index 7: 淨值

            current_comid = tds.eq(1).text().strip()
            current_name = tds.eq(5).text().strip()

            # 檢查公司代號是否符合，且基金名稱包含 filter 關鍵字
            if current_comid == fund_query.get("comid") and fund_query["filter"] in current_name:
                val_text = tds.eq(7).text().replace(",", "").strip()
                return float(val_text) if val_text else None

        return None
    except (ValueError, Exception) as e:
        print(f"Error parsing fund {fund_query['name']}: {e}")
        return None


# --- 非同步核心任務 ---
async def do_requests(
    session: aiohttp.ClientSession,
    url: str,
    request_date: datetime,
    payload: dict,
    semaphore: asyncio.Semaphore,
    fund_name: str,
    max_retries: int = 3,
) -> tuple[datetime, str]:
    async with semaphore:
        for attempt in range(max_retries):
            try:
                # 指數型延遲：1s, 2s, 4s... 加上隨機抖動
                wait_time = (2**attempt) + random.uniform(0, 1)
                await asyncio.sleep(wait_time)

                start_time = time.time()
                async with session.post(url, data=payload, timeout=20) as r:
                    if r.status == 200:
                        text = await r.text(errors="ignore")
                        print(
                            f"[{fund_name}][{request_date.strftime('%Y-%m-%d')}] 成功 (第{attempt+1}次), 耗時: {time.time()-start_time:.2f}s"
                        )
                        return request_date, text

                    elif r.status in [429, 503]:  # 伺服器忙碌或被限速
                        print(f"[{fund_name}] 伺服器忙碌({r.status}), 嘗試重試...")
                        await asyncio.sleep(5 * (attempt + 1))
                    else:
                        print(f"[{fund_name}] HTTP 錯誤 {r.status} @ {request_date.date()}")

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                if attempt == max_retries - 1:
                    print(f"[{fund_name}] 達到最大重試次數，失敗原因: {type(e).__name__}")
                continue

        return request_date, ""


async def get_data(
    session: aiohttp.ClientSession,
    fund_query: dict,
    url: str,
    payload_base: dict,
    start_datetime: datetime,
    end_datetime: datetime,
    semaphore: asyncio.Semaphore,
) -> None:
    fund_name = fund_query["name"]
    root = Path("./extraData") / fund_name
    root.mkdir(parents=True, exist_ok=True)

    # 從起始月份的第一天開始迭代
    curr_month_start = datetime(
        start_datetime.year, start_datetime.month, 1, tzinfo=taiwan_timezone
    )

    while curr_month_start <= end_datetime:
        year, month = curr_month_start.year, curr_month_start.month
        month_end = curr_month_start + relativedelta(months=1) - timedelta(days=1)

        actual_start = max(curr_month_start, start_datetime)
        actual_end = min(month_end, end_datetime)

        filename = f"{year}{month:02d}.csv"
        file_path = root / filename

        # --- 邏輯：讀取舊資料並判斷是否需要抓取 ---
        existing_history = read_csv_to_dict(file_path)

        # 如果是「很久以前」的月份且已經有資料，就跳過
        is_old_month = (datetime.now(taiwan_timezone) - actual_end).days > 10
        has_any_data = any(v is not None for v in existing_history.values())

        if is_old_month and has_any_data:
            print(f"[{fund_name}] {filename} 歷史完整，跳過。")
            curr_month_start += relativedelta(months=1)
            continue

        # --- 準備抓取任務 ---
        tasks = []
        current = actual_end
        while current >= actual_start:
            # 只有當「舊資料裡沒有這天」或「舊資料這天是空的」才抓取
            if existing_history.get(current) is None:
                p = payload_base.copy()
                p["ctl00$ContentPlaceHolder1$txtQ_Date"] = current.strftime("%Y%m%d")
                tasks.append(do_requests(session, url, current, p, semaphore, fund_name))
            current -= timedelta(days=1)

        if tasks:
            print(f"[{fund_name}] 正在更新 {year}-{month:02d}，共 {len(tasks)} 天...")
            results = await asyncio.gather(*tasks)

            # --- 合併新舊資料 ---
            for req_date, text in results:
                val = parse_response(text, fund_query)
                if val is not None:
                    existing_history[req_date] = val
                elif req_date not in existing_history:
                    existing_history[req_date] = None

            # --- 寫入 CSV ---
            # 轉換為 [Date, Close, Adj Close, Dividends, Stock Splits] 格式
            history_list = []
            for d, v in existing_history.items():
                val_str = "" if v is None else v
                history_list.append([d, val_str, val_str, 0, 0])

            history_list.sort(key=lambda x: x[0], reverse=True)

            with open(file_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f, lineterminator="\n")
                writer.writerow(["Date", "Close", "Adj Close", "Dividends", "Stock Splits"])
                writer.writerows(history_list)

        curr_month_start += relativedelta(months=1)


async def main(
    session: aiohttp.ClientSession, fund_query: dict, semaphore: asyncio.Semaphore
) -> None:
    try:
        async with session.get(BASE_URL) as resp:
            resp.raise_for_status()
            html = await resp.text(errors="ignore")
            dom = PyQuery(html)

            # 提取 ASP.NET 隱藏欄位
            payload_base = {
                "__VIEWSTATE": dom("#__VIEWSTATE").val(),
                "__VIEWSTATEGENERATOR": dom("#__VIEWSTATEGENERATOR").val(),
                "__EVENTVALIDATION": dom("#__EVENTVALIDATION").val(),
                "ctl00$ContentPlaceHolder1$ddlQ_Comid": fund_query["comid"],
                "ctl00$ContentPlaceHolder1$BtnQuery": "查詢",
            }

            start_dt = fund_query["start_date"]
            end_dt = datetime.combine(date.today(), datetime.min.time()).replace(
                tzinfo=taiwan_timezone
            )

            await get_data(session, fund_query, BASE_URL, payload_base, start_dt, end_dt, semaphore)

    except Exception as e:
        print(f"基金 {fund_query['name']} 初始化失敗: {e}")


async def run_all(queries):
    semaphore = asyncio.Semaphore(2)
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    timeout = aiohttp.ClientTimeout(total=300, connect=20, sock_read=15)

    async with aiohttp.ClientSession(
        headers={**headers, "Referer": "https://www.google.com/"},
        connector=aiohttp.TCPConnector(ssl=ssl_context, force_close=True),
        timeout=timeout,
    ) as session:
        tasks = [main(session, q, semaphore) for q in queries]
        await asyncio.gather(*tasks)


# --- 執行入口 ---
fund_querys = [
    {
        "filter": "台灣卓越50基金",
        "name": "元大台灣卓越50基金",
        "start_date": datetime(2012, 5, 7, tzinfo=taiwan_timezone),
        "comid": "A0005",
    },
    {
        "filter": "台灣釆吉50基金",
        "name": "富邦台灣釆吉50基金",
        "start_date": datetime(2012, 6, 22, tzinfo=taiwan_timezone),
        "comid": "A0010",
    },
]

if __name__ == "__main__":
    try:
        asyncio.run(run_all(fund_querys))
        print("所有資料更新完成，準備退出...")
    except Exception as e:
        print(f"程式執行發生錯誤: {e}")
    finally:
        os._exit(0)
