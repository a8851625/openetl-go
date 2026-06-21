# Build stage
FROM golang:1.24-alpine AS builder

WORKDIR /app

# Install dependencies
RUN apk add --no-cache git ca-certificates tzdata

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build binary
RUN CGO_ENABLED=0 GOOS=linux go build -p 1 -ldflags="-s -w" -o /app/main .

# Runtime stage
FROM alpine:3.19

WORKDIR /app

# Install ca-certificates and timezone data
RUN apk add --no-cache ca-certificates tzdata

# Create non-root user
RUN addgroup -g 1001 etl && adduser -D -u 1001 -G etl etl

# Copy binary and resources
COPY --from=builder /app/main .
COPY --from=builder /app/resource ./resource
COPY --from=builder /app/manifest ./manifest
COPY --from=builder /app/pipes ./pipes
COPY --from=builder /app/testdata ./testdata

# Create data directories with correct permissions
RUN mkdir -p data/checkpoint data/dlq data/output && chown -R etl:etl /app

USER etl

# Expose port
EXPOSE 8000 8001

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD wget -qO- http://localhost:8000/api/v2/health || exit 1

# Run
CMD ["./main"]
