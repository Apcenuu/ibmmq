package Ibm

import (
	"encoding/hex"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/ibm-messaging/mq-golang/v5/ibmmq"
	"github.com/rs/zerolog/log"
)

func ConnectToIbmMq(credentials Credentials) (ibmmq.MQQueueManager, error) {
	log.Debug().
		Str("queueManagerName", credentials.QueueManagerName).
		Str("queueName", credentials.QueueName).
		Str("channelName", credentials.ChannelName).
		Str("host", credentials.Host).
		Str("port", credentials.Port).
		Msg("Trying to connect to Credentials")

	var queueManagerError error
	var queueManager ibmmq.MQQueueManager

	mqConnectionDefault := ibmmq.NewMQCD()
	mqConnectionDefault.ChannelName = credentials.ChannelName
	mqConnectionDefault.ConnectionName = credentials.Host + "(" + credentials.Port + ")"
	mqConnectionDefault.MaxMsgLength = 104857600

	connectObject := ibmmq.NewMQCNO()
	connectObject.ClientConn = mqConnectionDefault
	connectObject.Options = ibmmq.MQCNO_CLIENT_BINDING

	ibmLogin, havingIbmLogin := os.LookupEnv("IBM_LOGIN")
	ibmPassword, havingIbmPass := os.LookupEnv("IBM_PASSWORD")

	if havingIbmLogin && havingIbmPass {
		clientSecurityParams := ibmmq.NewMQCSP()
		clientSecurityParams.AuthenticationType = ibmmq.MQCSP_AUTH_USER_ID_AND_PWD
		clientSecurityParams.UserId = ibmLogin
		clientSecurityParams.Password = ibmPassword
		connectObject.SecurityParms = clientSecurityParams
	}

	queueManager, queueManagerError = ibmmq.Connx(credentials.QueueManagerName, connectObject)

	if queueManagerError != nil {
		return ibmmq.MQQueueManager{}, queueManagerError
	}

	return queueManager, nil
}

func GetQueue(queueManager ibmmq.MQQueueManager, mode string) ibmmq.MQObject {
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
	mqObjectDescriptor.ObjectName = os.Getenv("IBM_QUEUE")

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

func PutInQueue(queue Queue, queueName string, message []byte, correlationId uuid.UUID) (string, error) {
	var err error
	var messageId string
	messageId = "none"

	mqMessageDescriptor := ibmmq.NewMQMD()
	//mqMessageDescriptor.Version = ibmmq.MQMD_VERSION_1
	//mqMessageDescriptor.Expiry = ibmmq.MQEI_UNLIMITED
	//mqMessageDescriptor.Report = ibmmq.MQRO_NONE
	//mqMessageDescriptor.MsgType = ibmmq.MQMT_DATAGRAM
	mqMessageDescriptor.Format = ibmmq.MQFMT_NONE
	mqMessageDescriptor.Priority = 0
	mqMessageDescriptor.Persistence = ibmmq.MQPER_PERSISTENT
	//mqMessageDescriptor.Encoding = 546
	mqMessageDescriptor.CodedCharSetId = 1208

	messageId = hex.EncodeToString(mqMessageDescriptor.MsgId)

	putMessageObject := ibmmq.NewMQPMO()
	putMessageObject.Options = ibmmq.MQPMO_NO_SYNCPOINT | ibmmq.MQPMO_FAIL_IF_QUIESCING | ibmmq.MQPMO_NEW_MSG_ID

	startPut := time.Now()
	log.Info().Msg("Starting PUT")

	err = queue.Put(mqMessageDescriptor, putMessageObject, message)

	if err != nil {
		return "", err
	}

	messageId = hex.EncodeToString(mqMessageDescriptor.MsgId)
	log.Info().
		TimeDiff("durationOfPutCall", time.Now(), startPut).
		Time("startPutCallAt", startPut).
		Str("Queue", strings.TrimSpace(queueName)).
		Str("IbmMessageId", messageId).
		Str("CorrelationId", correlationId.String()).
		Msg("Message added to IBM")

	return messageId, nil
}
