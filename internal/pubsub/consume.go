package pubsub

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	SimpleQueueDurable SimpleQueueType = iota
	SimpleQueueTransient
)

type AckType int

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
)

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queuType SimpleQueueType,
	handler func(T) AckType,
) error {
	ch, queue, err := DeclareAndBind(conn, exchange, queueName, key, queuType)
	if err != nil {
		return fmt.Errorf("Could not declare and bind queue: %v", err)
	}
	msgs, err := ch.Consume(
		queue.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("Could not consume message: %v", err)
	}

	unmarshaller := func(data []byte) (T, error) {
		var target T
		err := json.Unmarshal(data, &target)
		return target, err
	}

	go func() {
		defer ch.Close()
		for msg := range msgs {
			target, err := unmarshaller(msg.Body)
			if err != nil {
				continue
			}
			ackValue := handler(target)
			switch ackValue {
			case Ack:
				msg.Ack(false)
				log.Println("Ack")
			case NackRequeue:
				msg.Nack(false, true)
				log.Println("NackRequeue")
			case NackDiscard:
				msg.Nack(false, false)
				log.Println("NackDiscard")
			}
		}
	}()
	return nil
}

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {
	chann, err := conn.Channel()
	if err != nil {
		return &amqp.Channel{}, amqp.Queue{}, fmt.Errorf("could not create channel: %v", err)
	}
	queue, err := chann.QueueDeclare(
		queueName,
		queueType == SimpleQueueDurable,
		queueType != SimpleQueueDurable,
		queueType != SimpleQueueDurable,
		false,
		amqp.Table{
			"x-dead-letter-exchange": routing.ExchangePerilDlx,
		},
	)
	if err != nil {
		return &amqp.Channel{}, amqp.Queue{}, err
	}
	chann.QueueBind(
		queueName,
		key,
		exchange,
		false,
		nil,
	)
	return chann, queue, nil
}
