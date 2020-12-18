from pyquery import PyQuery
import csv
import os

TWSE_url = "https://isin.twse.com.tw/isin/C_public.jsp?strMode=2"
TPEx_url = "https://isin.twse.com.tw/isin/C_public.jsp?strMode=4"


def getSymbolList(url):
    dom = PyQuery(url=url)
    table = dom(".h4")
    tds = table.find("tr td:not(:only-child):first")

    symbols = {}
    for td in tds.items():
        s = td.text().split("\u3000")
        if len(s) == 2:
            symbols[s[0]] = s[1]

    return symbols


def writeToCSV(dict, filepath):
    with open(filepath, "w", newline="", encoding="UTF-8") as csvfile:
        writer = csv.writer(csvfile)
        for key, val in dict.items():
            writer.writerow([key, val])


def loadData(filepath, url):
    symbols = {}

    if not os.path.isfile(filepath):
        symbols = getSymbolList(url)
        writeToCSV(symbols, filepath)
        return symbols

    with open(filepath, newline="", encoding="UTF-8") as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            symbols[row[0]] = row[1]

    return symbols


def isInTWSE(symbol):
    symbols = loadData("TWSE.csv", TWSE_url)
    check = symbols.get(symbol, None)
    return True if check else False


def isInTPEx(symbol):
    symbols = loadData("TPEx.csv", TPEx_url)
    check = symbols.get(symbol, None)
    return True if check else False


if __name__ == "__main__":
    print("0050", isInTWSE("0050"))
    print("1234a", isInTWSE("1234a"))
    print("707491", isInTPEx("707491"))
    print("abc", isInTPEx("abc"))
