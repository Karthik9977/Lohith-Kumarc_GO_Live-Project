# Stage 1: Build the Go app
FROM golang:1.17 as builder

WORKDIR /app

# Copy go.mod and go.sum and download dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy the source code
COPY . .

# Build the Go binary
RUN go build -o order-service

# Stage 2: Create a smaller final image
FROM debian:bullseye-slim

# Set working directory
WORKDIR /app

# Copy the built binary from builder
COPY --from=builder /app/order-service .

# Expose the port
EXPOSE 8080

# Command to run the binary
CMD ["./order-service"]
