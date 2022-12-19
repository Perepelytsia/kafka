package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/Shopify/sarama"
	"github.com/gofiber/fiber/v2"
	"github.com/perepelytsia/kafka/connector"
)

// Comment struct
type Comment struct {
	Text string `form:"text" json:"text"`
}

func main() {

	app := fiber.New()
	api := app.Group("/api/v1") // /api

	api.Post("/comments", createComment)

	app.Listen(":3000")

}

func PushCommentToQueue(topic string, message []byte) error {

	brokersUrl := []string{"127.0.0.1:9999"}
	producer, err := connect.Producer(brokersUrl)
	if err != nil {
		return err
	}

	defer producer.Close()

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return err
	}

	fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)

	return nil
}

// createComment handler
func createComment(c *fiber.Ctx) error {

	// Instantiate new Message struct
	cmt := new(Comment)

	//  Parse body into comment struct
	// {"text":"keep up the good work"}
	if err := c.BodyParser(cmt); err != nil {
		log.Println(err)
		c.Status(400).JSON(&fiber.Map{
			"success": false,
			"message": err,
		})
		return err
	}

	// convert body into bytes and send it to kafka
	cmtInBytes, err := json.Marshal(cmt)
	PushCommentToQueue("comments", cmtInBytes)

	// Return Comment in JSON format
	err = c.JSON(&fiber.Map{
		"success": true,
		"message": "Comment pushed successfully",
		"comment": cmt,
	})
	if err != nil {
		c.Status(500).JSON(&fiber.Map{
			"success": false,
			"message": "Error creating product",
		})
		return err
	}

	return err
}
