FROM golang:1.23-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git make gcc musl-dev linux-headers

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build PulseHA daemon and CLI
RUN go build -o pulseha cmd/pulseha/main.go
RUN go build -o pulsectl cmd/pulsectl/main.go

# Runtime image
FROM alpine:latest

# Install runtime dependencies
RUN apk add --no-cache \
    iproute2 \
    iptables \
    curl \
    bash \
    net-tools \
    procps

# Create pulseha user
RUN addgroup -g 1001 pulseha && \
    adduser -D -s /bin/bash -u 1001 -G pulseha pulseha

# Create necessary directories
RUN mkdir -p /etc/pulseha /var/log/pulseha /home/pulseha/.pulseha && \
    chown -R pulseha:pulseha /etc/pulseha /var/log/pulseha /home/pulseha/.pulseha

# Copy binaries from builder
COPY --from=builder /app/pulseha /usr/local/bin/pulseha
COPY --from=builder /app/pulsectl /usr/local/bin/pulsectl

# Make binaries executable
RUN chmod +x /usr/local/bin/pulseha /usr/local/bin/pulsectl

# Switch to pulseha user
USER pulseha
WORKDIR /home/pulseha

# Expose gRPC port
EXPOSE 8080

# Health check using pulsectl
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
  CMD /usr/local/bin/pulsectl status --json > /dev/null || exit 1

# Default command
CMD ["/usr/local/bin/pulseha"]