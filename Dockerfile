FROM golang:1.26-alpine AS build

WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /spoold ./cmd/spoold

FROM scratch

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build --chown=65532:65532 /spoold /spoold
COPY --chown=65532:65532 data/.keep /var/lib/spoold/.keep

USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/spoold"]
CMD ["-listen", "0.0.0.0:8080", "-journal", "/var/lib/spoold/spoold.journal"]

