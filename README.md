# stockTools
[![Get Data and Deploy](https://github.com/z-Wind/stockTools/actions/workflows/deploy.yml/badge.svg)](https://github.com/z-Wind/stockTools/actions/workflows/deploy.yml)

## 範例
* [TW_Report](https://z-wind.github.io/stockTools/TW_Report.html)
* [US_Report](https://z-wind.github.io/stockTools/US_Report.html)
* [US_美國(美股,美債,美房地產)_Report](https://z-wind.github.io/stockTools/US_美國(美股,美債,美房地產)_Report.html)
* [US_日正_Report](https://z-wind.github.io/stockTools/US_日正_Report.html)

## 事前準備
需安裝 rust
```
cd FFI/rust_pyo3
maturin develop
```

## 抓資料
```
python get_extraData.py
```

## 比較股票
```
python stock.py
```
