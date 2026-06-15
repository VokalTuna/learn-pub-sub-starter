package pubsub

import amqp "github.com/rabbitmq/amqp091-go"

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
		return &amqp.Channel{}, amqp.Queue{}, err
	}
	var durable, autoDelete, exclusive bool
	if queueType == QueueTypeDurable {
		durable, autoDelete, exclusive = true, false, false
	}
	if queueType == QueueTypeTransient {
		durable, autoDelete, exclusive = false, true, true
	}
	queue, err := chann.QueueDeclare(queueName, durable, autoDelete, exclusive, false, nil)
	if err != nil {
		return &amqp.Channel{}, amqp.Queue{}, err
	}
	chann.QueueBind(queueName, key, exchange, false, nil)
	return chann, queue, nil
}
