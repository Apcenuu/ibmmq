package Ibm

import (
	"github.com/ibm-messaging/mq-golang/v5/ibmmq"
)

type QueueManager interface {
	Open(descriptor *ibmmq.MQOD, options int32) (ibmmq.MQObject, error)
	Cmit() error
	Back() error
}
