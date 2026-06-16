package pubsub

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	QueueTypeDurable SimpleQueueType = iota
	QueueTypeTransient
)

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
		queueType == QueueTypeDurable,
		queueType != QueueTypeDurable,
		queueType != QueueTypeDurable,
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
