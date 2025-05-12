package main

import (
	"log"
	"os"
	"os/signal"
	"shipper-consumer/shipper"
	"syscall"

	"github.com/segmentio/kafka-go"
)

func main() {
	// Set up graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	// Initialize Kafka reader and writer
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		GroupID:  "shipper-group",
		Topic:    "order_picked_packed",
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "order_shipped",
		Balancer: &kafka.LeastBytes{},
	})

	defer reader.Close()
	defer writer.Close()

	log.Println("Shipper Service Starting...")

	// Start shipper consumer handling
	go shipper.HandleShipping(reader, writer)

	// Wait for SIGINT or SIGTERM signal
	<-quit
	log.Println("Shutting down gracefully...")
}
