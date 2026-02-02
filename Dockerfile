FROM rust:latest as builder
WORKDIR /usr/src/app
COPY . .
RUN cargo build --release

FROM debian:latest
# TLS 需要 CA 证书（wss://）
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
  && rm -rf /var/lib/apt/lists/*

# 可选：把 db 和 static 放到容器里
WORKDIR /app
COPY --from=builder /usr/src/app/target/release/binance_cvd /usr/local/bin/binance_cvd
COPY static ./static

EXPOSE 8000

ENV SQLITE_URL="sqlite:/data/cvd.db"
RUN mkdir -p /data

CMD ["binance_cvd"]