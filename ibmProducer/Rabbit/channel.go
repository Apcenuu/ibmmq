package Rabbit

import amqp "github.com/rabbitmq/amqp091-go"

type Channel interface {
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
	Ack(tag uint64, multiple bool) error
	Nack(tag uint64, multiple, requeue bool) error
	Close() error
	Qos(prefetchCount, prefetchSize int, global bool) error
}
