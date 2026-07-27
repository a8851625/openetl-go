# ── Frontend build stage ───────────────────────────────────────────
# Builds web/ → resource/public so the image always ships a fresh UI
# instead of whatever stale dist happens to be committed in the tree.
FROM node:20-alpine AS frontend

WORKDIR /src/web

# Install deps first for better layer caching.
COPY web/package.json web/package-lock.json ./
RUN npm ci --no-audit --no-fund

COPY web/ ./
# vite.config.ts outDir is ../resource/public → /src/resource/public
RUN npm run build

# ── Go build stage ─────────────────────────────────────────────────
FROM golang:1.24-alpine AS builder

WORKDIR /app

# bash/curl: required by hack/pack.sh (gf download + pack).
# git/ca-certificates/tzdata: normal Go build deps.
RUN apk add --no-cache bash curl git ca-certificates tzdata

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Prefer the freshly built UI over any host-side resource/public.
COPY --from=frontend /src/resource/public ./resource/public

# Embed resource/ (including fresh UI) into internal/packed/packed.go so the
# binary can serve the SPA via gres even without a sibling resource/ directory.
# SKIP_UI=1: frontend already built in the node stage above.
RUN SKIP_UI=1 ./hack/pack.sh

# Build binary (imports internal/packed, so UI is embedded)
ARG GO_BUILD_TAGS=""
RUN CGO_ENABLED=0 GOOS=linux go build -p 1 -tags="${GO_BUILD_TAGS}" -ldflags="-s -w" -o /app/main .

# ── Runtime stage ──────────────────────────────────────────────────
FROM alpine:3.19

WORKDIR /app

# Install ca-certificates and timezone data
RUN apk add --no-cache ca-certificates tzdata

# Create non-root user
RUN addgroup -g 1001 etl && adduser -D -u 1001 -G etl etl

# Binary already embeds UI via gres; also ship resource/ on disk so
# readStaticAsset prefers the same fresh dist (disk first, then gres).
COPY --from=builder /app/main .
COPY --from=builder /app/resource ./resource
COPY --from=builder /app/manifest ./manifest

# Create empty pipes/ and data dirs; pipeline specs are mounted by the operator.
RUN mkdir -p pipes data/checkpoint data/dlq data/output && chown -R etl:etl /app

USER etl

# Expose port
EXPOSE 8000 8001

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD wget -qO- http://localhost:8000/api/v2/health >/dev/null 2>&1 || \
      wget --no-check-certificate -qO- https://localhost:8000/api/v2/health >/dev/null 2>&1 || exit 1

# Run
CMD ["./main"]
