package Rabbit

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
)

type FakeChannel struct {
}

func (f FakeChannel) PublishWithContext(ctx context.Context, exchange string, queue string, b bool, b2 bool, publishing amqp.Publishing) error {
	//TODO implement me
	panic("implement me")
}

func (f FakeChannel) Ack(tag uint64, multiple bool) error {
	return nil
}

func (f FakeChannel) Nack(tag uint64, multiple, requeue bool) error {
	//TODO implement me
	panic("implement me")
}

func (f FakeChannel) Close() error {
	//TODO implement me
	panic("implement me")
}

func (f FakeChannel) Qos(prefetchCount, prefetchSize int, global bool) error {
	return nil
}

func Qos(prefetchCount, prefetchSize int, global bool) error {
	return nil
}

func (f FakeChannel) Consume(queue string, consumer string, autoAck bool, exclusive bool, noLocal bool, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	delivery := make(chan amqp.Delivery, 1)

	delivery <- amqp.Delivery{
		Acknowledger: new(FakeAcknowledger),
		Body:         []byte("Message_1"),
		Headers:      amqp.Table{"header1": "value1"},
	}

	return delivery, nil
}
