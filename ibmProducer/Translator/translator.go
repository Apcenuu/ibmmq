package Translator

import (
	"context"
	"errors"
	"fmt"
	"ibmProducer/ibmmq/Ibm"
	"time"

	"github.com/google/uuid"
	"github.com/ibm-messaging/mq-golang/v5/ibmmq"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog/log"
)

var start = make(chan string, 1)
var done = make(chan string, 1)

func MoveMessagesFromRabbitToIbm(
	ctx context.Context,
	messages <-chan amqp.Delivery,
	ibmQueue Ibm.Queue,
	ibmQueueName string,
	interval time.Duration,
	exitFunction func()) {

	go keepAlive(interval, exitFunction)

	for {
		select {
		case rabbitMessage := <-messages:
			// Фиксируем момент начала обработки
			start <- "success"
			moveMessage(rabbitMessage, ibmQueue, ibmQueueName, exitFunction)
		case <-ctx.Done():
			log.Info().Msg("Завершение по сигналу контекста")
			return
		}

	}
}

func keepAlive(interval time.Duration, exitFunction func()) {
	// Создаём таймер и сразу же останавливаем,
	// чтобы если нет сообщений, то контейнер не перезапускался бесконечно
	ticker := time.NewTicker(interval)
	ticker.Stop()

	for {
		select {
		// На старте запускаем таймер
		case <-start:
			ticker = time.NewTicker(interval)
			log.Info().Msg("START")
		// Если сообщение обработано успешно, останавливаем текущий таймер
		case <-done:
			ticker.Stop()
			log.Info().Msg("SUCCESS")
		// Если произошла ошибка выходим с пользовательской функцией
		case <-ticker.C:
			log.Info().Msg("BAD")
			exitFunction()
		}
	}
}

func moveMessage(rabbitMessage amqp.Delivery, ibmQueue Ibm.Queue, queueName string, exitFunction func()) {

	correlationId := uuid.New()
	messageId := fmt.Sprintf("%v", rabbitMessage.Headers["X-Message-Id"])

	log.Info().
		Str("CorrelationId", correlationId.String()).
		Str("MessageId", messageId).
		Msg("Received rabbit message")

	_, err := Ibm.PutInQueue(ibmQueue, queueName, rabbitMessage.Body, correlationId)

	if err != nil {
		var mqret *ibmmq.MQReturn
		redelivery := false
		errors.As(err, &mqret)
		if mqret.MQRC == ibmmq.MQRC_CONNECTION_BROKEN {
			redelivery = true
		}
		_ = rabbitMessage.Nack(false, redelivery)
		log.Error().
			Err(err).
			Bool("Redelivery", redelivery).
			Str("CorrelationId", correlationId.String()).
			Msg("Error while move messages from RabbitMq to IbmMq")
		exitFunction()
	}
	// Фиксируем момент успешной записи в очередь
	done <- "success"

	err = rabbitMessage.Ack(false)

	if err != nil {
		_ = rabbitMessage.Nack(false, false)
		log.Error().
			Err(err).
			Str("CorrelationId", correlationId.String()).
			Msg("Error while ack message in IbmMq")
		exitFunction()
	}

	log.Info().
		Str("CorrelationId", correlationId.String()).
		Msg("Message moved to IbmMq")
}
