package main

import (
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

func main() {
	fmt.Println("Notification Consumer Service Starting...")

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		notificationConsumer() // Start notification consumer
	}()

	// Listen for termination signals
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit // Wait for SIGINT or SIGTERM

	fmt.Println("Shutting down gracefully...")
	wg.Wait() // Wait for the consumer goroutine to finish
}
