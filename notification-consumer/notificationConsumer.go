package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

var notificationProcessedCount int

// Structure for 'order_notified' event
type OrderNotified struct {
	OrderID string `json:"orderId"`
	Status  string `json:"status"`
	Time    string `json:"time"`
}

func notificationConsumer() {
	// Connect to Kafka
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{"localhost:9092"},
		GroupID:     "notification-consumer-group",
		Topic:       "order-picked",
		MinBytes:    10e3,              // 10KB
		MaxBytes:    10e6,              // 10MB
		StartOffset: kafka.FirstOffset, // Start from the first offset
	})

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "order-notified", // Topic to publish the 'order_notified' event
		Balancer: &kafka.LeastBytes{},
	})

	defer reader.Close()
	defer writer.Close()

	// Set up graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	fmt.Println("Listening for 'order-picked' events...")

	for {
		select {
		case <-quit:
			log.Println("Received shutdown signal. Exiting...")
			return

		default:
			// Fetch message
			startTime := time.Now()
			msg, err := reader.FetchMessage(context.Background())
			if err != nil {
				log.Printf("[ERROR] Failed to fetch message: %v", err)
				continue
			}

			log.Printf("[INFO] Received message: %s", string(msg.Value))
			log.Printf("[INFO] Order picked: %s", string(msg.Value))
			log.Println("[INFO] Sending notification to customer...")

			// Simulate processing
			time.Sleep(500 * time.Millisecond)

			// Emit 'order_notified' event
			orderNotified := OrderNotified{
				OrderID: string(msg.Value), // Assuming msg.Value is the order ID
				Status:  "order_notified",
				Time:    time.Now().Format(time.RFC3339),
			}

			data, err := json.Marshal(orderNotified)
			if err != nil {
				log.Printf("[ERROR] Failed to marshal order_notified: %v", err)
				continue
			}

			err = writer.WriteMessages(context.Background(), kafka.Message{
				Value: data,
			})
			if err != nil {
				log.Printf("[ERROR] Failed to publish order_notified: %v", err)
				continue
			}

			// Process Metrics
			notificationProcessedCount++
			duration := time.Since(startTime)
			log.Printf("[METRIC] Processed notification event #%d in %v", notificationProcessedCount, duration)
		}
	}
}
