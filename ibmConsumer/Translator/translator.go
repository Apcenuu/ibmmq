package Translator

import (
	"context"
	"encoding/hex"
	"ibmConsumer/ibmmq/Ibm"
	"ibmConsumer/ibmmq/Rabbit"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/ibm-messaging/mq-golang/v5/ibmmq"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog/log"
)

var (
	messageProcessed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ibm_consumer_processed_messages",
		Help: "The total number of moved messages from Ibm to RabbitMq",
	})
	maxMsgLength int32
)

func MoveMessagesFromIbmToRabbit(
	queueManager Ibm.QueueManager,
	rabbitChannel Rabbit.Channel,
	rabbitExchange string,
	rabbitQueue string,
	logMsgDir string,
	exitFunction func()) {

	var dataLength int
	var err error

	queue := getIbmQueue(queueManager, "read", exitFunction)

	for {
		correlationId := uuid.New().String()
		mqMessageDescriptor := ibmmq.NewMQMD()
		mqMessageDescriptor.Version = ibmmq.MQMD_VERSION_2

		getMessageOptions := ibmmq.NewMQGMO()

		getMessageOptions.Options = ibmmq.MQGMO_SYNCPOINT
		getMessageOptions.Options |= ibmmq.MQGMO_NO_WAIT
		getMessageOptions.Options |= ibmmq.MQGMO_ACCEPT_TRUNCATED_MSG

		getMessageOptions.WaitInterval = 3 * 1000
		getMessageOptions.Version = ibmmq.MQGMO_VERSION_2

		message := make([]byte, 0, maxMsgLength)

		startGet := time.Now()

		message, dataLength, err = queue.GetSlice(mqMessageDescriptor, getMessageOptions, message)
		messageId := hex.EncodeToString(mqMessageDescriptor.MsgId)

		// Выходим когда соединение поломалось
		if err != nil && err.(*ibmmq.MQReturn).MQRC == ibmmq.MQRC_CONNECTION_BROKEN {
			err = queueManager.Back()

			log.Error().
				Err(err).
				Str("IbmMessageId", messageId).
				Str("correlationId", correlationId).
				Msg("Connection broken. Exiting")

			exitFunction()
		}

		if err != nil && err.(*ibmmq.MQReturn).MQRC != ibmmq.MQRC_TRUNCATED_MSG_ACCEPTED && err.(*ibmmq.MQReturn).MQRC != ibmmq.MQRC_NO_MSG_AVAILABLE {
			err = queueManager.Back()

			log.Error().
				Err(err).
				Str("IbmMessageId", messageId).
				Str("correlationId", correlationId).
				Msg(err.Error())

			continue
		}

		// Message has been obtained, put it to the rabbit
		if dataLength > 0 {

			log.Info().
				TimeDiff("durationOfGetCall", time.Now(), startGet).
				Time("getCallStartedAt", startGet).
				Int("length", dataLength).
				Int("msgLen", len(message)).
				Str("IbmMessageId", messageId).
				Str("correlationId", correlationId).
				Msg("Message from ibmmq has been read")

			// Log message if log path is declared
			if logMsgDir != "" {
				logFile := logMsgDir + "/" + time.Now().Format("2006-01-02_03:04:05.000000") + ".log"
				log.Info().
					Str("logFile", logFile).
					Str("IbmMessageId", messageId).
					Str("correlationId", correlationId).
					Msg("Writing message to log file")
				os.WriteFile(logFile, message, 0644)
			}

			putMessageInRabbitQueue(rabbitChannel, rabbitExchange, rabbitQueue, string(message), correlationId, queue, exitFunction)

			err = queueManager.Cmit()

			continue // Do not wait after the message
		}

		time.Sleep(300 * time.Millisecond)
	}
}

func putMessageInRabbitQueue(
	rabbitChannel Rabbit.Channel,
	rabbitExchange string,
	rabbitQueue string,
	message string,
	correlationId string,
	queue ibmmq.MQObject,
	exitFunction func()) {

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	now := time.Now()
	body := message
	messageId := uuid.New().String()

	err := rabbitChannel.PublishWithContext(
		ctx,
		rabbitExchange,
		rabbitQueue,
		false,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			Headers: amqp.Table{
				"X-Ibmmq-Consume-Time": now.Format(time.RFC3339Nano),
				"X-Ibmmq-Queue":        queue.Name,
				"X-Message-Id":         messageId,
			},
			ContentType: "text/plain",
			Body:        []byte(body),
		})

	if err != nil {
		log.Error().
			Err(err).
			Str("correlationId", correlationId).
			Msg("Error while add message in RabbitMq")
		exitFunction()
	}

	log.Debug().
		Str("correlationId", correlationId).
		Str("messageId", messageId).
		Msg("Message moved from IbmMq to rabbit")

	messageProcessed.Inc()
}

func getIbmQueue(queueManager Ibm.QueueManager, mode string, exitFunction func()) ibmmq.MQObject {
	var queueError error
	var queue ibmmq.MQObject
	var openOptions int32

	mqObjectDescriptor := ibmmq.NewMQOD()

	if mode == "create" {
		openOptions = ibmmq.MQOO_OUTPUT
	}
	if mode == "read" {
		openOptions |= ibmmq.MQOO_BROWSE
		openOptions |= ibmmq.MQOO_INPUT_SHARED
		openOptions |= ibmmq.MQOO_INQUIRE
	}

	mqObjectDescriptor.ObjectType = ibmmq.MQOT_Q
	mqObjectDescriptor.ObjectName = os.Getenv("IBM_QUEUE")

	queue, queueError = queueManager.Open(mqObjectDescriptor, openOptions)

	if queueError != nil {
		log.Error().
			Err(queueError).
			Msg("Error while opening queue")
		exitFunction()
	}

	log.Debug().
		Str("Queue", queue.Name).
		Msg("Opened queue")

	maxMsgLength = getMessageMaxLength(queue, exitFunction)

	return queue
}

// Запрос максимальной длины сообщения у очереди
func getMessageMaxLength(queue ibmmq.MQObject, exitFunction func()) int32 {

	// Get message max length for the queue
	result, err := queue.Inq([]int32{ibmmq.MQIA_MAX_MSG_LENGTH})

	if err != nil {
		log.Error().Err(err).
			Msg("Error while querying max message length")
		exitFunction()
	}

	value := result[ibmmq.MQIA_MAX_MSG_LENGTH]
	maxMessageLength, ok := value.(int32)

	if ok != true {
		log.Error().Msg("Could not convert max message length to int32")
		exitFunction()
	}

	log.Info().
		Int32("maxMsgLength", maxMessageLength).
		Msg("Got max message length from queue")

	return maxMessageLength
}
