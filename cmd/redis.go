package main

import (
	"sync"

	"github.com/Shenmin-Z/toyredis"
)

func main() {
	//cache.NewServer("6789", 20)
  toyredis.NewServer("6379", 20)

	wg := sync.WaitGroup{}
	wg.Add(1)
	wg.Wait()
}
