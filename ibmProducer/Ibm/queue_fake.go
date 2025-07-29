package Ibm

import (
	"sync"

	"github.com/ibm-messaging/mq-golang/v5/ibmmq"
)

type FakeQueue struct {
	mu       sync.Mutex
	messages []Message
	success  bool
}

func (f *FakeQueue) Inq(goSelectors []int32) (map[int32]interface{}, error) {
	//TODO implement me
	panic("implement me")
}

func (f *FakeQueue) Get(messageDescriptor *ibmmq.MQMD, gmo *ibmmq.MQGMO, buffer []byte) (int, error) {
	//TODO implement me
	panic("implement me")
}

func (f *FakeQueue) Put(messageDescriptor *ibmmq.MQMD, putMessageOptions *ibmmq.MQPMO, buffer []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.success {
		f.messages = append(f.messages, Message{
			MessageDescriptor: messageDescriptor,
			PutMessageOptions: putMessageOptions,
			Buffer:            buffer,
		})
		return nil
	} else {
		return &ibmmq.MQReturn{
			MQCC: 2,
			MQRC: 2009,
		}
	}

}

func (f *FakeQueue) WithSuccess() {
	f.success = true
}

func (f *FakeQueue) GetMessages() []Message {
	return f.messages
}
