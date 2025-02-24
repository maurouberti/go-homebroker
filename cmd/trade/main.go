package main

import (
	"encoding/json"
	"fmt"
	"sync"

	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/maurouberti/go-homebroker/internal/infra/kafka"
	"github.com/maurouberti/go-homebroker/internal/market/dto"
	"github.com/maurouberti/go-homebroker/internal/market/entity"
	"github.com/maurouberti/go-homebroker/internal/market/transformer"
)

func main() {
	ordersIn := make(chan *entity.Order)

	ordersOut := make(chan *entity.Order)
	wg := &sync.WaitGroup{}
	defer wg.Wait()

	kafkaMsgChan := make(chan *ckafka.Message)

	consumerConfig := &ckafka.ConfigMap{
		"bootstrap.servers": "localhost:9094",
		"group.id":          "trade",
		"auto.offset.reset": "latest",
	}

	producerConfig := &ckafka.ConfigMap{
		"bootstrap.servers": "localhost:9094",
	}

	producer := kafka.NewKafkaProducer(producerConfig)
	consumer := kafka.NewConsumer(consumerConfig, []string{"orders"})

	go consumer.Consume(kafkaMsgChan)

	book := entity.NewBook(ordersIn, ordersOut, wg)
	go book.Trade()

	go func() {
		for msg := range kafkaMsgChan {
			wg.Add(1)
			fmt.Println(string(msg.Value))

			tradeInput := dto.TradeInput{}
			err := json.Unmarshal(msg.Value, &tradeInput)
			if err != nil {
				panic(err)
			}

			order := transformer.TransformInput(tradeInput)
			ordersIn <- order
		}
	}()

	for res := range ordersOut {
		output := transformer.TransformOutput(res)
		jsonOutput, err := json.MarshalIndent(output, "", "  ")
		if err != nil {
			panic(err)
		}

		fmt.Println(string(jsonOutput))
		producer.Publish(jsonOutput, []byte("processed_orders"), "processed_orders")
	}
}
