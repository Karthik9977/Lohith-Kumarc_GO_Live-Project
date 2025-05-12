package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type OrderConfirmed struct {
	OrderID string `json:"orderId"`
	Status  string `json:"status"`
	Time    string `json:"time"`
}

type OrderPickedPacked struct {
	OrderID string `json:"orderId"`
	Status  string `json:"status"`
	Time    string `json:"time"`
}

var successCount int

func HandleWarehouseEvents() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		GroupID:  "warehouse-consumer-group",
		Topic:    "order-confirmed",
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "order_picked_packed",
		Balancer: &kafka.LeastBytes{},
	})

	defer reader.Close()
	defer writer.Close()

	log.Println("Listening for 'order-confirmed' events...")

	for {
		start := time.Now()

		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Error reading message: %v", err)
			continue
		}

		var confirmed OrderConfirmed
		err = json.Unmarshal(msg.Value, &confirmed)
		if err != nil {
			log.Printf("JSON unmarshal failed: %v", err)
			continue
		}

		// Simulate warehouse pick and pack
		time.Sleep(1 * time.Second)

		event := OrderPickedPacked{
			OrderID: confirmed.OrderID,
			Status:  "order_picked_packed",
			Time:    time.Now().Format(time.RFC3339),
		}

		eventBytes, err := json.Marshal(event)
		if err != nil {
			log.Printf("Error marshalling JSON: %v", err)
			continue
		}

		err = writer.WriteMessages(context.Background(), kafka.Message{
			Value: eventBytes,
		})
		if err != nil {
			log.Printf("Failed to publish 'order_picked_packed' event: %v", err)
			continue
		}

		successCount++
		duration := time.Since(start)
		log.Printf("Order ID %s packed! ⏱️ Time: %v | Count: %d",
			confirmed.OrderID, duration, successCount)
	}
}
