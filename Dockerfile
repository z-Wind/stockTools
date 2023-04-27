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
    "yfinance==0.2.18" "pandas==2.0.1" "pyquery==2.0.0" "flask==2.3.1" "plotly==5.14.1" "requests==2.29.0" "numpy==1.24.3"
