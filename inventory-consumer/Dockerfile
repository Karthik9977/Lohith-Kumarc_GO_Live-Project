# Use the official Golang base image
FROM golang:1.17

# Set the working directory in the container
WORKDIR /app

# Copy go mod files first for better caching
COPY go.mod ./
COPY go.sum ./
RUN go mod download

# Copy the rest of the source code
COPY . .

# Build the binary
RUN go build -o inventory-consumer

# Run the binary
CMD ["./inventory-consumer"]
