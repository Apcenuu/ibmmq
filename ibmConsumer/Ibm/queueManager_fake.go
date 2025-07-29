package Ibm

import "C"
import (
	"sync"

	"github.com/ibm-messaging/mq-golang/v5/ibmmq"
)

type FakeQueueManager struct {
	mu sync.Mutex
	//queues map[string]*ibmmq.MQObject
}

func (f *FakeQueueManager) Open(descriptor *ibmmq.MQOD, options int32) (ibmmq.MQObject, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	obj := ibmmq.MQObject{
		Name: "fake_queue",
	}

	return obj, nil
}

func (f *FakeQueueManager) Cmit() error {
	//TODO implement me
	panic("implement me")
}

func (f *FakeQueueManager) Back() error {
	//TODO implement me
	panic("implement me")
}
