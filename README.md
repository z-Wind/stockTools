# stockTools
[![Get Data and Deploy](https://github.com/z-Wind/stockTools/actions/workflows/deploy.yml/badge.svg)](https://github.com/z-Wind/stockTools/actions/workflows/deploy.yml)

## 範例
* [TW_Report](https://z-wind.github.io/stockTools/TW_Report.html)
* [US_Report](https://z-wind.github.io/stockTools/US_Report.html)

## 事前準備
需安裝 rust
```
cd FFI/rust_lib
cargo build --release
```

## 抓資料
```
python get_extraData.py
```

## 比較股票
```
python stock.py
```
