package main

import (
	"fmt"
	"sync"
)

func main() {
	fmt.Println("Warehouse Consumer Service Starting...")

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		HandleWarehouseEvents() // this is defined in warehouseConsumer.go
	}()

	wg.Wait()
}
