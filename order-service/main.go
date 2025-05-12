package main

import (
	"encoding/json"
	"log"
	"net/http"
	"order-service/kafka"
	"order-service/model"
)

func placeOrder(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}

	var order model.Order
	if err := json.NewDecoder(r.Body).Decode(&order); err != nil {
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}

	log.Printf("Order received: %+v", order)

	go kafka.PublishOrderEvent(order)

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Order placed and event emitted"))
}

func main() {
	http.HandleFunc("/place-order", placeOrder)
	log.Println("Order Service is running on port 8080...")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
