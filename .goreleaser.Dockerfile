FROM alpine:3.23

ARG TARGETPLATFORM

RUN apk add --no-cache ca-certificates \
    && mkdir -p /var/lib/spoold \
    && chown 65532:65532 /var/lib/spoold

COPY $TARGETPLATFORM/spoold /usr/local/bin/spoold

USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/spoold"]
CMD ["-listen", "0.0.0.0:8080", "-journal", "/var/lib/spoold/spoold.journal"]
