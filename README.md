# stockTools
[![Get Data and Deploy](https://github.com/z-Wind/stockTools/actions/workflows/deploy.yml/badge.svg)](https://github.com/z-Wind/stockTools/actions/workflows/deploy.yml)

## 範例
* [TW_Report](https://z-wind.github.io/stockTools/TW_Report.html)
* [US_Report](https://z-wind.github.io/stockTools/US_Report.html)
* [TW_Analysis_Report](https://z-wind.github.io/stockTools/TW_Analysis_Report.html)
* [TW_Food_Report](https://z-wind.github.io/stockTools/TW_Food_Report.html)

## 事前準備
需安裝 rust
```bash
cd FFI/rust_pyo3
maturin develop --release
```

## 抓資料
```bash
python get_extraData.py
python get_extraData_fund.py
python tw_analysis_data.py
```

## 各式比較
```bash
python stock.py
python tw_analysis.py
python tw_food.py
```
