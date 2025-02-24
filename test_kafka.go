package main

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	config := kafka.ConfigMap{}
	fmt.Println(config)
}
