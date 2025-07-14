import time
from pyquery import PyQuery
from datetime import date, timedelta, datetime, timezone
import csv
from pathlib import Path
import aiohttp
import asyncio

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": "https://www.google.com/",
}

taiwan_timezone = timezone(timedelta(hours=8))


def read_csv_to_dict(file_path: Path):
    data_map = {}
    with open(file_path, "r", newline="") as csvfile:
        csv_reader = csv.reader(csvfile)
        for i, row in enumerate(csv_reader):
            if i == 0:
                continue

            date = datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S%z")
            val = None if row[1] == "" else float(row[1])
            data_map[date] = val

    return data_map


def parse_response(text: aiohttp.ClientResponse.text, fund_query: dict):
    dom = PyQuery(text)
    funds = dom(r"tr.DTeven")
    if len(list(funds.eq(0))) == 0:
        return None

    fund = funds.filter(
        lambda i, this: fund_query["filter"] in PyQuery(this)(r"td:nth-last-child(5)").text()
    )
    val = fund(r"td:nth-last-child(3)").text()
    if val == "":
        return None

    val = float(val)
    return val


async def do_requests(
    session: aiohttp.ClientSession,
    url: str,
    date: datetime,
    data: dict,
    semaphore: asyncio.Semaphore,
):
    async with semaphore:  # Acquire the semaphore before making the request
        await asyncio.sleep(1.0)
        start_time = time.time()
        async with session.post(url, data=data) as r:
            text = await r.text()
            print(
                f"Post： {time.time()-start_time:.2f} 秒",
            )
            return date, text


async def get_data(
    session: aiohttp.ClientSession,
    fund_query: dict,
    url: str,
    data: dict,
    start_datetime: datetime,
    end_datetime: datetime,
):
    semaphore = asyncio.Semaphore(5)  # Allow n concurrent requests
    root = Path("./extraData") / fund_query["name"]
    root.mkdir(parents=True, exist_ok=True)

    for year in range(start_datetime.year, end_datetime.year + 1):
        for month in range(1, 12 + 1):
            start = datetime(year, month, 1, 0, 0, 0, tzinfo=taiwan_timezone)
            if start < start_datetime and start.month != start_datetime.month:
                continue
            end = datetime(
                year + month // 12, month % 12 + 1, 1, 0, 0, 0, tzinfo=taiwan_timezone
            ) - timedelta(days=1)
            if end > end_datetime and end.month != end_datetime.month:
                continue

            start = max(start, start_datetime)
            end = min(end, end_datetime)
            current = end

            history = {}
            filename = f"{year}{month:02d}.csv"
            if (root / filename).exists():
                if end_datetime - end > timedelta(days=10):
                    print(f"{filename} pass")
                    continue
                history = read_csv_to_dict(root / filename)

            tasks = []
            while current >= start:
                if history.get(current) is None:
                    data["ctl00$ContentPlaceHolder1$txtQ_Date"] = current.strftime("%Y%m%d")
                    tasks.append(do_requests(session, url, current, data.copy(), semaphore))

                current -= timedelta(days=1)

            start_time = time.time()
            for task in asyncio.as_completed(tasks):
                date, text = await task
                # with open("a.txt", "w", encoding="utf-8") as f:
                #     f.write(text)
                # raise
                val = parse_response(text, fund_query)
                print(date, val)
                history[date] = val
            print(
                f"總執行時間： {time.time()-start_time:.2f} 秒",
            )

            history_list = [[d, val, val, 0, 0] for d, val in history.items()]
            history_list.sort(key=lambda x: x[0], reverse=True)
            history_list = [
                ["Date", "Close", "Adj Close", "Dividends", "Stock Splits"]
            ] + history_list
            with open(root / filename, "w", newline="") as file:
                csv_writer = csv.writer(file)

                # Write all rows to the CSV file
                csv_writer.writerows(history_list)


async def main(fund_query: dict):
    url = "https://www.sitca.org.tw/ROC/Industry/IN2106.aspx?pid=IN2213_02"

    async with aiohttp.ClientSession(headers=headers) as session:
        async with session.get(url) as resp:
            dom = PyQuery(await resp.text())

        __VIEWSTATE = dom(r"#__VIEWSTATE").val()
        __VIEWSTATEGENERATOR = dom(r"#__VIEWSTATEGENERATOR").val()
        __EVENTVALIDATION = dom(r"#__EVENTVALIDATION").val()

        data = {
            "__VIEWSTATE": __VIEWSTATE,
            "__VIEWSTATEGENERATOR": __VIEWSTATEGENERATOR,
            "__EVENTVALIDATION": __EVENTVALIDATION,
            "ctl00$ContentPlaceHolder1$ddlQ_Comid": fund_query["comid"],
            "ctl00$ContentPlaceHolder1$BtnQuery": "查詢",
        }

        start_datetime = fund_query["start_date"]
        end_datetime = datetime.combine(date.today(), datetime.min.time()).replace(
            tzinfo=taiwan_timezone
        )

        await get_data(session, fund_query, url, data, start_datetime, end_datetime)


fund_querys = [
    {
        "filter": "台灣卓越50基金",
        "name": "元大台灣卓越50基金",
        "start_date": datetime(
            2012, 5, 7, 0, 0, 0, tzinfo=taiwan_timezone
        ),  # 投顧會的資料只到這一天不到成立日 # datetime(2003, 6, 25, 0, 0, 0, tzinfo=taiwan_timezone),
        "comid": "A0005",
    },
    {
        "filter": "台灣釆吉50基金",
        "name": "富邦台灣釆吉50基金",
        "start_date": datetime(2012, 6, 22, 0, 0, 0, tzinfo=taiwan_timezone),
        "comid": "A0010",
    },
]

if __name__ == "__main__":
    url = "https://www.sitca.org.tw/ROC/Industry/IN2106.aspx?pid=IN2213_02"

    for fund_query in fund_querys:
        asyncio.run(main(fund_query))
