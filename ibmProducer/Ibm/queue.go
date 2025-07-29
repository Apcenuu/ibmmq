package Ibm

import "github.com/ibm-messaging/mq-golang/v5/ibmmq"

type Queue interface {
	Inq(goSelectors []int32) (map[int32]interface{}, error)

	Get(md *ibmmq.MQMD, gmo *ibmmq.MQGMO, buffer []byte) (int, error)
	Put(md *ibmmq.MQMD, pmo *ibmmq.MQPMO, buffer []byte) error
}
