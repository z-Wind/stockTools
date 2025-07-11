import requests
import time
from pyquery import PyQuery
from datetime import date, timedelta, datetime, timezone
import csv
from pathlib import Path


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


headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": "https://www.google.com/",
}

session = requests.Session()
session.headers = headers

url = "https://www.sitca.org.tw/ROC/Industry/IN2106.aspx?pid=IN2213_02"

r = session.get(url)

taiwan_timezone = timezone(timedelta(hours=8))
fund_querys = [
    {
        "symbol": "0050",
        "name": "元大台灣卓越50基金",
        "start_date": datetime(2003, 6, 25, 0, 0, 0, tzinfo=taiwan_timezone),
        "comid": "A0005",
    },
    {
        "symbol": "006208",
        "name": "富邦台灣釆吉50基金",
        "start_date": datetime(2012, 6, 22, 0, 0, 0, tzinfo=taiwan_timezone),
        "comid": "A0010",
    },
]

for fund_query in fund_querys:
    start_datetime = fund_query["start_date"] + timedelta(days=1)
    end_datetime = datetime.combine(date.today(), datetime.min.time()).replace(
        tzinfo=taiwan_timezone
    ) + timedelta(days=1)

    current_datetime = end_datetime
    root = Path("./extraData") / fund_query["name"]
    root.mkdir(parents=True, exist_ok=True)

    history = {}
    filename = current_datetime.strftime("%Y%m") + ".csv"
    if (root / filename).exists():
        history = read_csv_to_dict(root / filename)

    while current_datetime >= start_datetime:
        current_datetime -= timedelta(days=1)

        if filename != current_datetime.strftime("%Y%m") + ".csv":
            if len(history) > 0:
                history_list = [[d, val, val, 0, 0] for d, val in history.items()]
                history_list.sort(key=lambda x: x[0], reverse=True)
                history_list = [
                    ["Date", "Close", "Adj Close", "Dividends", "Stock Splits"]
                ] + history_list
                with open(root / filename, "w", newline="") as file:
                    csv_writer = csv.writer(file)

                    # Write all rows to the CSV file
                    csv_writer.writerows(history_list)

                history = {}

            filename = current_datetime.strftime("%Y%m") + ".csv"
            if (root / filename).exists():
                history = read_csv_to_dict(root / filename)

        if history.__contains__(current_datetime):
            print(current_datetime, history.get(current_datetime))
            continue

        dom = PyQuery(r.text)

        __VIEWSTATE = dom(r"#__VIEWSTATE").val()
        __VIEWSTATEGENERATOR = dom(r"#__VIEWSTATEGENERATOR").val()
        __EVENTVALIDATION = dom(r"#__EVENTVALIDATION").val()

        data = {
            "__VIEWSTATE": __VIEWSTATE,
            "__VIEWSTATEGENERATOR": __VIEWSTATEGENERATOR,
            "__EVENTVALIDATION": __EVENTVALIDATION,
            "ctl00$ContentPlaceHolder1$txtQ_Date": current_datetime.strftime("%Y%m%d"),
            "ctl00$ContentPlaceHolder1$ddlQ_Comid": fund_query["comid"],
            "ctl00$ContentPlaceHolder1$BtnQuery": "查詢",
        }

        time.sleep(1)
        r = session.post(url, data)

        if "本日查無符合資料!!" in r.text:
            continue

        # with open("a.txt", "w") as f:
        # f.write(r.content)

        dom = PyQuery(r.text)
        funds = dom(r"tr.DTeven")
        if len(list(funds.eq(0))) == 0:
            print(current_datetime, None)
            history[current_datetime] = None
            continue

        fund = funds.filter(
            lambda i, this: PyQuery(this)(r"td:nth-child(4)").text() == fund_query["symbol"]
        )
        val = fund(r"td:nth-child(8)").text()
        if val == "":
            print(current_datetime, None)
            history[current_datetime] = None
            continue

        print(current_datetime, val)
        history[current_datetime] = float(val)

    history_list = [[d, val, val, 0, 0] for d, val in history.items()]
    history_list.sort(key=lambda x: x[0], reverse=True)
    history_list = [["Date", "Close", "Adj Close", "Dividends", "Stock Splits"]] + history_list
    with open(root / filename, "w", newline="") as file:
        csv_writer = csv.writer(file)

        # Write all rows to the CSV file
        csv_writer.writerows(history_list)
