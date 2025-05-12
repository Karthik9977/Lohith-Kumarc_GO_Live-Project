// main.go
package main

import (
	"fmt"
	"sync"
)

func main() {
	fmt.Println("Inventory Consumer Service Starting...")

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		inventoryConsumer() // This should work if it's defined in the same package
	}()

	wg.Wait()
}
