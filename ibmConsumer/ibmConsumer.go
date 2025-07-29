package main

import (
	"ibmConsumer/ibmmq/Ibm"
	"ibmConsumer/ibmmq/Rabbit"
	"ibmConsumer/ibmmq/Translator"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func server() {
	http.Handle("/metrics", promhttp.Handler())
	http.ListenAndServe(":2112", nil)
}

func main() {
	zerolog.TimeFieldFormat = time.RFC3339Nano
	zerolog.TimestampFieldName = "datetime"

	go server()

	logMsgDir := os.Getenv("APP_LOG_MESSAGES_PATH")

	rabbitQueueName := os.Getenv("RABBIT_QUEUE")
	rabbitExchange := os.Getenv("RABBIT_EXCHANGE")

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

	log.Debug().
		Msg("Connected to IbmMq")

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

	log.Debug().
		Msg("Connected to Rabbit")

	args := amqp.Table{"x-queue-type": "quorum"}

	_, err = rabbitChannel.QueueDeclare(
		rabbitQueueName,
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

	if rabbitExchange != "" {
		err = rabbitChannel.ExchangeDeclarePassive(rabbitExchange, "direct", true, false, false, false, nil)

		if err != nil {
			log.Error().
				Err(err).
				Msg("Error while exchange declaration in RabbitMq")
			os.Exit(1)
		}

		err = rabbitChannel.QueueBind(rabbitQueueName, rabbitQueueName, rabbitExchange, false, nil)

		if err != nil {
			log.Error().
				Err(err).
				Msg("Error while queue binding to exchange in RabbitMq")
			os.Exit(1)
		}
	}

	Translator.MoveMessagesFromIbmToRabbit(&ibmQueueManager, rabbitChannel, rabbitExchange, rabbitQueueName, logMsgDir, func() {
		os.Exit(1)
	})
}
