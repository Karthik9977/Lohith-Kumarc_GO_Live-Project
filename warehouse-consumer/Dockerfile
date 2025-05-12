# Stage 1: Build the Go app
FROM golang:1.17 as builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod tidy

COPY . .

RUN go build -o warehouse-consumer .

# Stage 2: Create a smaller runtime image
FROM debian:bullseye-slim

WORKDIR /app

COPY --from=builder /app/warehouse-consumer .

EXPOSE 8083

CMD ["./warehouse-consumer"]
