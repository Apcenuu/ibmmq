package Ibm

import "github.com/ibm-messaging/mq-golang/v5/ibmmq"

type Message struct {
	MessageDescriptor *ibmmq.MQMD
	PutMessageOptions *ibmmq.MQPMO
	Buffer            []byte
}
