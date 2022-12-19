package connector

import (
	"os"

	"github.com/Shopify/sarama"
	"github.com/joho/godotenv"
)

func Producer() (sarama.SyncProducer, error) {

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	godotenv.Load()
	conn, err := sarama.NewSyncProducer([]string{os.Getenv("brokersUrl")}, config)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func Consumer() (sarama.Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	godotenv.Load()
	conn, err := sarama.NewConsumer([]string{os.Getenv("brokersUrl")}, config)
	if err != nil {
		return nil, err
	}

	return conn, nil
}