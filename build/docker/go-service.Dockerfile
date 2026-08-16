FROM golang:1.25.13-bookworm AS builder

ARG TARGET
ARG GOPROXY=https://goproxy.cn,direct
ENV GOPROXY=${GOPROXY}

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN test -n "$TARGET" \
    && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
       go build -trimpath -ldflags="-s -w" -o /out/service "./cmd/${TARGET}"

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /out/service /usr/local/bin/service

USER 65534:65534

ENTRYPOINT ["/usr/local/bin/service"]
