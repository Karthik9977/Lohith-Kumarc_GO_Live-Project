FROM golang:1.17 as builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o order-service

FROM debian:bullseye-slim

WORKDIR /app

COPY --from=builder /app/order-service .

EXPOSE 8080

CMD ["./order-service"]
