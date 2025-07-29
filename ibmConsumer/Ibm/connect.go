package Ibm

import (
	"os"

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
