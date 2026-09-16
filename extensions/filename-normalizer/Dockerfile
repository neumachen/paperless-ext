# syntax=docker/dockerfile:1.7
#
# File Normalizer container build.
#
# Both executables come from one build stage and ship as separate scratch
# images. Nothing is downloaded at container start: every dependency is
# resolved and compiled here, and the runtime layer contains one static binary,
# a passwd/group pair for the non-root account, and CA certificates.

# ---------------------------------------------------------------------------
# Toolchain. Pinned by tag and digest; go.mod pins the language version and
# GOTOOLCHAIN=local forbids fetching a different toolchain.
# ---------------------------------------------------------------------------
FROM golang:1.26-alpine@sha256:ce864e7223ac17b1775e6fd0b4c0db580c2eb50e7953a427916379e4b92a1628 AS toolchain
ENV GOTOOLCHAIN=local \
    CGO_ENABLED=0 \
    GOFLAGS=-mod=readonly
# Pinned so a rebuild cannot silently pick up a different certificate or
# timezone set. ca-certificates is already installed in the base image.
RUN apk add --no-cache ca-certificates=20260611-r0 tzdata=2026d-r0
WORKDIR /src

# Dependency layer: changes only when the module graph changes.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod,sharing=locked \
    --mount=type=cache,target=/root/.cache/go-build,sharing=locked \
    go mod download && go mod verify

COPY . .

# A content hash of the build context, computed here so every artifact of one
# build carries the same value.
#
# It answers "which candidate is actually running?". A matching Go version and
# a shared git revision label do not exclude a stale image: an uncommitted edit
# leaves the revision unchanged, and a build that did not run leaves an older
# image in place. The integration suite requires the applications it tests to
# report the same digest as the suite binary, so a stale application image
# fails the run instead of being tested silently.
RUN find . -type f -print0 | LC_ALL=C sort -z | xargs -0 sha256sum \
      | sha256sum | cut -c1-16 > /source-digest && \
    echo "source digest: $(cat /source-digest)"

# ---------------------------------------------------------------------------
# Static build.
# ---------------------------------------------------------------------------
FROM toolchain AS build
ARG VERSION=0.0.0-dev
ARG REVISION=unknown
ARG BUILD_DATE=unknown
RUN --mount=type=cache,target=/go/pkg/mod,sharing=locked \
    --mount=type=cache,target=/root/.cache/go-build,sharing=locked \
    LDFLAGS="-s -w \
      -X github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/buildinfo.Version=${VERSION} \
      -X github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/buildinfo.Revision=${REVISION} \
      -X github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/buildinfo.BuildDate=${BUILD_DATE} \
      -X github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/buildinfo.SourceDigest=$(cat /source-digest)" && \
    go build -trimpath -buildvcs=false -ldflags "${LDFLAGS}" -o /out/fn-watcher ./cmd/watcher && \
    go build -trimpath -buildvcs=false -ldflags "${LDFLAGS}" -o /out/fn-renamer ./cmd/renamer && \
    go build -trimpath -buildvcs=false -ldflags "${LDFLAGS}" -o /out/fnctl ./cmd/fnctl

# The runtime account. UID/GID 65532 is the conventional unprivileged
# "nonroot" identity; it is created here so the scratch image can name it.
RUN printf 'nonroot:x:65532:65532:nonroot:/nonexistent:/sbin/nologin\n' > /out/passwd && \
    printf 'nonroot:x:65532:\n' > /out/group

# ---------------------------------------------------------------------------
# The inspection client. It is a separate image so the gRPC API is exercised
# by a real client over the network, from a container that holds no
# credentials and mounts no document storage.
# ---------------------------------------------------------------------------
FROM scratch AS fnctl
COPY --from=build /out/passwd /out/group /etc/
COPY --from=toolchain /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/fnctl /usr/local/bin/fnctl
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/fnctl"]

# ---------------------------------------------------------------------------
# Verification stage: formatting, vet and unit tests run inside the image
# build, so a host Go installation is never required for them.
# ---------------------------------------------------------------------------
FROM toolchain AS verify
RUN --mount=type=cache,target=/go/pkg/mod,sharing=locked \
    --mount=type=cache,target=/root/.cache/go-build,sharing=locked \
    test -z "$(gofmt -l . | tee /dev/stderr)" && \
    go vet ./... && \
    go test ./internal/... ./cmd/...

# ---------------------------------------------------------------------------
# Integration-test runner. It holds the compiled test binary's sources and the
# toolchain; it is executed against the running stack by compose.
# ---------------------------------------------------------------------------
FROM toolchain AS integration
ENV CGO_ENABLED=0
RUN --mount=type=cache,target=/go/pkg/mod,sharing=locked \
    --mount=type=cache,target=/root/.cache/go-build,sharing=locked \
    go build -o /dev/null ./... && \
    go vet -tags integration ./test/... && \
    go test -tags integration -c \
      -ldflags "-X github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/buildinfo.SourceDigest=$(cat /source-digest)" \
      -o /usr/local/bin/fn-integration.test ./test/integration
# The test binary is fully linked here, so the runner performs no module
# download and no compilation when it executes.
WORKDIR /work
ENTRYPOINT ["/usr/local/bin/fn-integration.test"]

# ---------------------------------------------------------------------------
# Watcher runtime.
# ---------------------------------------------------------------------------
FROM scratch AS watcher
COPY --from=build /out/passwd /out/group /etc/
COPY --from=toolchain /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=toolchain /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=build /out/fn-watcher /usr/local/bin/fn-watcher
USER 65532:65532
ENV FN_HTTP_ADDR=:8080
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/fn-watcher"]

# ---------------------------------------------------------------------------
# Renamer runtime.
# ---------------------------------------------------------------------------
FROM scratch AS renamer
COPY --from=build /out/passwd /out/group /etc/
COPY --from=toolchain /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=toolchain /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=build /out/fn-renamer /usr/local/bin/fn-renamer
USER 65532:65532
ENV FN_HTTP_ADDR=:8080
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/fn-renamer"]
