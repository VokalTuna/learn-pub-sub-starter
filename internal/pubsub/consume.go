package pubsub

import (
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	SimpleQueueDurable SimpleQueueType = iota
	SimpleQueueTransient
)

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queuType SimpleQueueType,
	handler func(T),
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
			handler(target)
			msg.Ack(false)
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
		nil,
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
