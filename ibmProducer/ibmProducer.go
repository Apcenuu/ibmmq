package main

import (
	"context"
	"ibmProducer/ibmmq/Ibm"
	"ibmProducer/ibmmq/Rabbit"
	"ibmProducer/ibmmq/Translator"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	zerolog.TimeFieldFormat = time.RFC3339Nano
	zerolog.TimestampFieldName = "datetime"

	ibmQueueManager, err := Ibm.ConnectToIbmMq(Ibm.Credentials{
		Host:             os.Getenv("IBM_HOST"),
		Port:             os.Getenv("IBM_PORT"),
		QueueManagerName: os.Getenv("IBM_QUEUE_MANAGER"),
		QueueName:        os.Getenv("IBM_QUEUE"),
		ChannelName:      os.Getenv("IBM_CHANNEL"),
	})

	if err != nil {
		log.Error().
			Err(err).
			Msg("Error while connect to IbmMq")
		os.Exit(1)
	}

	log.Debug().Msg("Connected to IbmMq")

	rabbitMqCredentials := Rabbit.Credentials{
		Host:      os.Getenv("RABBIT_HOST"),
		Port:      os.Getenv("RABBIT_PORT"),
		Login:     os.Getenv("RABBIT_LOGIN"),
		Password:  os.Getenv("RABBIT_PASSWORD"),
		QueueName: os.Getenv("RABBIT_QUEUE"),
		Vhost:     os.Getenv("RABBIT_VHOST"),
	}
	rabbitChannel, err := Rabbit.ConnectToRabbit(rabbitMqCredentials)

	if err != nil {
		log.Error().
			Err(err).
			Msg("Error while connect to RabbitMq")
		os.Exit(1)
	}

	log.Debug().Msg("Connected to Rabbit")

	args := amqp.Table{"x-queue-type": "quorum"}

	rabbitQueue, err := rabbitChannel.QueueDeclare(
		rabbitMqCredentials.QueueName,
		true,
		false,
		false,
		false,
		args,
	)

	if err != nil {
		log.Error().
			Err(err).
			Msg("Error while queue declaration in RabbitMq")
		os.Exit(1)
	}

	ibmQueue := Ibm.GetQueue(ibmQueueManager, "create")

	rabbitChannel.Qos(10, 0, false)

	messages, err := rabbitChannel.Consume(
		rabbitQueue.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		log.Error().
			Err(err).
			Msg("Error while consume messages from RabbitMq")
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	Translator.MoveMessagesFromRabbitToIbm(ctx, messages, ibmQueue, ibmQueue.Name, 3*time.Second, func() {
		log.Error().Msg("Сработала Exit-функция")
		os.Exit(1)
	})
}
