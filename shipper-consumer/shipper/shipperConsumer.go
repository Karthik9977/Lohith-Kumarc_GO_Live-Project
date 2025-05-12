package shipper

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

type OrderPacked struct {
	OrderID string `json:"orderId"`
	Status  string `json:"status"`
	Time    string `json:"time"`
}

type OrderShipped struct {
	OrderID string `json:"orderId"`
	Status  string `json:"status"`
	Time    string `json:"time"`
}

var successCount int

func HandleShipping(r *kafka.Reader, w *kafka.Writer) {
	// Set up graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	log.Println("Shipper is listening for 'order_picked_packed' events...")

	for {
		select {
		case <-quit:
			log.Println("Received shutdown signal. Exiting...")
			return

		default:
			start := time.Now()

			m, err := r.ReadMessage(context.Background())
			if err != nil {
				log.Printf("Error reading message: %v\n", err)
				continue
			}

			var packed OrderPacked
			err = json.Unmarshal(m.Value, &packed)
			if err != nil {
				log.Printf("Error unmarshaling JSON: %v", err)
				continue
			}

			// Simulate shipping delay
			time.Sleep(1 * time.Second)

			// Emit 'order_shipped' event
			shipped := OrderShipped{
				OrderID: packed.OrderID,
				Status:  "order_shipped",
				Time:    time.Now().Format(time.RFC3339),
			}
			data, err := json.Marshal(shipped)
			if err != nil {
				log.Printf("Failed to marshal order_shipped: %v", err)
				continue
			}

			err = w.WriteMessages(context.Background(), kafka.Message{
				Value: data,
			})
			if err != nil {
				log.Printf("Failed to publish order_shipped: %v", err)
				continue
			}

			successCount++
			duration := time.Since(start)
			log.Printf("Order ID %s shipped! ⏱️ Time: %v | Count: %d",
				shipped.OrderID, duration, successCount)
		}
	}
}
