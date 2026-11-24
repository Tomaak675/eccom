# Go agent image (receiver.implementation: go). Built natively on each target
# host by the SSH deployment from the same synced source tree as the Python
# image: go/ holds the module, this file the build. Two stages: the toolchain
# compiles a static binary, the runtime stage carries only that binary plus
# iperf3 for the link-capacity probe, which runs from whichever agent image
# the host has. Layer order: the module download is cached until go.mod or
# go.sum change, so code-only rebuilds only recompile.
FROM golang:1.27-bookworm AS build

WORKDIR /src
COPY go/go.mod go/go.sum ./
RUN go mod download

COPY go/ ./
# -p 1 compiles one package at a time: lower peak memory on a small host.
RUN CGO_ENABLED=0 go build -p 1 -trimpath -ldflags="-s -w" -o /eccom-agent ./cmd/eccom-agent

FROM debian:bookworm-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends iperf3 \
 && rm -rf /var/lib/apt/lists/*

COPY --from=build /eccom-agent /usr/local/bin/eccom-agent

# Deployments pass provenance through the environment:
#   ECCOM_GIT_COMMIT, ECCOM_IMAGE_DIGEST
ENTRYPOINT ["eccom-agent"]
