FROM golang:1.26-alpine AS build

ARG VERSION=dev
ARG COMMIT=unknown
ARG DATE=unknown

WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath \
    -ldflags="-s -w \
      -X github.com/Rionlyu/spoold/internal/buildinfo.Version=${VERSION} \
      -X github.com/Rionlyu/spoold/internal/buildinfo.Commit=${COMMIT} \
      -X github.com/Rionlyu/spoold/internal/buildinfo.Date=${DATE}" \
    -o /spoold ./cmd/spoold

FROM scratch

ARG VERSION=dev

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build --chown=65532:65532 /spoold /spoold
COPY --chown=65532:65532 data/.keep /var/lib/spoold/.keep

LABEL org.opencontainers.image.source="https://github.com/Rionlyu/spoold" \
      org.opencontainers.image.description="Crash-safe local HTTP delivery spool" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.version="${VERSION}"

USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/spoold"]
CMD ["-listen", "0.0.0.0:8080", "-journal", "/var/lib/spoold/spoold.journal"]
