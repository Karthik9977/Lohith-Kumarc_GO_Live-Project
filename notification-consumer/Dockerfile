# Use official Go image
FROM golang:1.17

# Set the working directory inside the container
WORKDIR /app

# Copy the Go module files and download dependencies first (for better caching)
COPY go.mod ./
COPY go.sum ./
RUN go mod download

# Copy the rest of the source code
COPY . .

# Build the Go binary
RUN go build -o notification-consumer

# Run the binary
CMD ["./notification-consumer"]
