package main

import (
	"encoding/hex"
	"github.com/ibm-messaging/mq-golang/v5/ibmmq"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"os"
	"strings"
	"time"
)

func main() {
	zerolog.TimeFieldFormat = time.RFC3339Nano
	zerolog.TimestampFieldName = "datetime"

	queueManagerName := os.Getenv("QUEUE_MANAGER")
	queue := os.Getenv("QUEUE")
	channel := os.Getenv("CHANNEL")
	ibmHost := os.Getenv("IBM_HOST")
	ibmPort := os.Getenv("IBM_PORT")
	ibmLogin := os.Getenv("IBM_LOGIN")
	ibmPassword := os.Getenv("IBM_PASSWORD")

	ibmQueueManager, err := connectToIbmMq(
		ibmHost,
		ibmPort,
		queueManagerName,
		queue,
		channel,
		ibmLogin,
		ibmPassword)

	if err != nil {
		log.Error().
			Err(err).
			Msg("Error while connect to IbmMq")
		os.Exit(1)
	}

	log.Debug().
		Msg("Connected to IbmMq")

	ibmQueue := getIbmQueue(ibmQueueManager, "create")

	//messageText := "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n<Channel>\n<Title>test</Title>\n<Description>this is a test</Description>\n</Channel>"
	//data, _ := os.ReadFile("inventario.xml")
	for {
		putInIbmQueue(ibmQueue)
		time.Sleep(time.Second * 1)
	}

}

func getIbmQueue(queueManager ibmmq.MQQueueManager, mode string) ibmmq.MQObject {
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
	}

	mqObjectDescriptor.ObjectType = ibmmq.MQOT_Q
	mqObjectDescriptor.ObjectName = "DEV.QUEUE.1"

	queue, queueError = queueManager.Open(mqObjectDescriptor, openOptions)

	if queueError != nil {
		log.Error().
			Err(queueError).
			Msg("Error while opening queue")
		os.Exit(1)
	}

	log.Debug().
		Str("Queue", queue.Name).
		Msg("Opened queue")

	return queue
}

func connectToIbmMq(host string, port string, queueManagerName string, queueName string, channelName string, login string, password string) (ibmmq.MQQueueManager, error) {
	log.Debug().
		Str("queueManagerName", queueManagerName).
		Str("queueName", queueName).
		Str("channelName", channelName).
		Str("host", host).
		Str("port", port).
		Msg("Trying to connect to IbmMq")

	var queueManagerError error
	var queueManager ibmmq.MQQueueManager

	mqConnectionDefault := ibmmq.NewMQCD()
	mqConnectionDefault.ChannelName = channelName
	mqConnectionDefault.ConnectionName = host + "(" + port + ")"
	mqConnectionDefault.MaxMsgLength = 104857600

	connectObject := ibmmq.NewMQCNO()
	connectObject.ClientConn = mqConnectionDefault
	connectObject.Options = ibmmq.MQCNO_CLIENT_BINDING

	clientSecurityParams := ibmmq.NewMQCSP()
	clientSecurityParams.AuthenticationType = ibmmq.MQCSP_AUTH_USER_ID_AND_PWD
	clientSecurityParams.UserId = login
	clientSecurityParams.Password = password

	connectObject.SecurityParms = clientSecurityParams

	queueManager, queueManagerError = ibmmq.Connx(queueManagerName, connectObject)

	if queueManagerError != nil {
		return ibmmq.MQQueueManager{}, queueManagerError
	}

	return queueManager, nil
}

func putInIbmQueue(queue ibmmq.MQObject) (string, error) {
	var err error
	var messageId string
	messageId = "none"

	mqMessageDescriptor := ibmmq.NewMQMD()
	putMessageObject := ibmmq.NewMQPMO()

	putMessageObject.Options = ibmmq.MQPMO_NO_SYNCPOINT
	//putMessageObject.Options = ibmmq.MQMF_SEGMENTATION_ALLOWED
	mqMessageDescriptor.Format = ibmmq.MQFMT_STRING

	messageText := "hello its test"
	data := []byte(messageText)
	err = queue.Put(mqMessageDescriptor, putMessageObject, data)

	if err != nil {
		log.Error().
			Err(err).
			Msg("Error while put message in IbmMq")
	} else {
		messageId = hex.EncodeToString(mqMessageDescriptor.MsgId)
		log.Info().
			Str("Queue", strings.TrimSpace(queue.Name)).
			Str("MessageId", messageId).
			Msg("Message added to ibm")
	}

	return messageId, nil
}
