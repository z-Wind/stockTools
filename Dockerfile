FROM rust:1.62.1-slim-buster as rust-lib

WORKDIR /app
COPY FFI /app/FFI
WORKDIR /app/FFI/rust_lib
RUN cargo build --release

FROM python:3.8.13-slim-bullseye
WORKDIR /app
COPY --from=rust-lib /app/FFI /app/FFI
COPY templates /app/templates
COPY stock.py get_extraData.py /app
RUN pip install \
    "yfinance==0.1.70" "pandas==1.2.3" "pyquery==1.4.3" "flask==2.0.0" "plotly==4.14.3" "requests==2.27.1" "numpy==1.20.3"
