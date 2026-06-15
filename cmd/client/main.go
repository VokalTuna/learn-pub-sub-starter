package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	const rabbitConnString = "amqp://guest:guest@localhost:5672/"

	conn, err := amqp.Dial(rabbitConnString)
	if err != nil {
		log.Fatalf("Could not connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	userName, err := gamelogic.ClientWelcome()
	pubsub.DeclareAndBind(conn, routing.ExchangePerilDirect, fmt.Sprintf("%s.%s", routing.PauseKey, userName), routing.PauseKey, pubsub.QueueTypeTransient)

	fmt.Println("Starting Peril client...")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)
	<-sigChan
}
