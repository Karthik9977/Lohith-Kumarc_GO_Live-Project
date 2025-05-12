package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type OrderEvent struct {
	OrderID string `json:"orderId"`
	Status  string `json:"status"`
	Time    string `json:"time"`
}

func inventoryConsumer() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		GroupID:  "inventory-consumer-group",
		Topic:    "order-received",
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "order-confirmed",
		Balancer: &kafka.LeastBytes{},
	})

	defer reader.Close()
	defer writer.Close()

	log.Println("Inventory Consumer is listening for 'order-received' events...")

	var successCount int

	for {
		start := time.Now()
		msg, err := reader.FetchMessage(context.Background())
		if err != nil {
			log.Printf("Failed to fetch message: %v\n", err)
			continue
		}

		log.Printf("Received message: %s\n", msg.Value)

		// Simulate processing
		time.Sleep(1 * time.Second)

		// Create order-confirmed event
		event := OrderEvent{
			OrderID: string(msg.Value),
			Status:  "order_confirmed",
			Time:    time.Now().Format(time.RFC3339),
		}

		eventBytes, err := json.Marshal(event)
		if err != nil {
			log.Printf("Error marshalling event: %v\n", err)
			continue
		}

		err = writer.WriteMessages(context.Background(), kafka.Message{
			Value: eventBytes,
		})
		if err != nil {
			log.Printf("❌ Failed to write to 'order-confirmed': %v\n", err)
			continue
		}

		// Log metrics
		successCount++
		duration := time.Since(start)
		log.Printf("✅ Event processed and forwarded. Count: %d | Duration: %v\n", successCount, duration)

		// Retry logic in case the message fetching fails
		for {
			_, err := reader.ReadMessage(context.Background())
			if err != nil {
				log.Printf("Error reading message: %v", err)
				time.Sleep(2 * time.Second) // Retry delay
				continue
			}
			// process message
		}
	}
}
