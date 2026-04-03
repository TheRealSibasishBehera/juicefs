# syntax=docker/dockerfile:1.7

FROM --platform=$TARGETPLATFORM golang:1.23-bookworm AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN apt-get update \
    && apt-get install -y --no-install-recommends musl-tools ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN STATIC=1 make juicefs

FROM juicedata/mount:nightly

COPY --from=builder /src/juicefs /usr/local/bin/juicefs

RUN /usr/local/bin/juicefs version
