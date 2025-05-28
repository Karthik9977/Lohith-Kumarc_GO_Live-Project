package kafka

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"order-service/model"

	"github.com/segmentio/kafka-go"
)

func PublishOrderEvent(event model.Order) {
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:      []string{"localhost:9094"},
		Topic:        "order-received",
		RequiredAcks: 1,
	})

	defer writer.Close()

	event.Timestamp = time.Now().Format(time.RFC3339)

	message, err := json.Marshal(event)
	if err != nil {
		log.Println("Error marshaling event:", err)
		return
	}

	err = writer.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(event.OrderID),
		Value: message,
	})
	if err != nil {
		log.Println("Error writing message to Kafka:", err)
		return
	}

	log.Println("Produced event to topic 'order-received'")
}
