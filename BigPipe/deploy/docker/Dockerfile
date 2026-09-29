# BigPipe node (bigpiped + bpctl). Dibuat oleh Gravicode Studios dipimpin oleh Kang Fadhil.
FROM rust:1-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates crates
COPY tools/bpctl tools/bpctl
RUN cargo build --release -p bigpiped -p bpctl

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --home /var/lib/bigpipe bigpipe && mkdir -p /var/lib/bigpipe && chown bigpipe /var/lib/bigpipe
COPY --from=build /src/target/release/bigpiped /src/target/release/bpctl /usr/local/bin/
USER bigpipe
ENV BIGPIPE_DATA_DIR=/var/lib/bigpipe BIGPIPE_ADVERTISED_HOST=localhost
VOLUME /var/lib/bigpipe
EXPOSE 9092 8082 9644 9645
ENTRYPOINT ["bigpiped"]
CMD ["--mode", "dev"]
