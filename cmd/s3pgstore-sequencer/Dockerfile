# syntax=docker/dockerfile:1.7
#
# Multi-stage, multi-arch image for s3pgstore-sequencer.
# Build with `docker buildx build --platform linux/amd64,linux/arm64`
# from the repository root.

ARG GO_VERSION=1.26

FROM --platform=$BUILDPLATFORM golang:${GO_VERSION} AS build
WORKDIR /src

# Cache module downloads independently of source changes.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

ARG TARGETOS
ARG TARGETARCH
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" \
        -o /out/s3pgstore-sequencer \
        ./cmd/s3pgstore-sequencer

FROM gcr.io/distroless/static-debian13:nonroot
COPY --from=build /out/s3pgstore-sequencer /usr/local/bin/s3pgstore-sequencer
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/s3pgstore-sequencer"]
