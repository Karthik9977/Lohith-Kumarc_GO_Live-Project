# Stage 1: Build the Go app
FROM golang:1.17 as builder

# Set the working directory
WORKDIR /app

# Copy go.mod and go.sum files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod tidy

# Copy the rest of the source code
COPY . .

# Build the binary
RUN go build -o shipper-consumer .

# Stage 2: Create a smaller runtime image
FROM debian:bullseye-slim

# Set working directory
WORKDIR /app

# Copy the compiled binary from the builder stage
COPY --from=builder /app/shipper-consumer .

# Expose the application port (adjust if needed)
EXPOSE 8082

# Run the shipper-consumer app
CMD ["./shipper-consumer"]
