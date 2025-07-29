package Rabbit

import (
	"errors"
	"sync"
)

type FakeAcknowledger struct {
	mu      sync.Mutex
	acks    []Ack
	nacks   []Nack
	success bool
}

func (f *FakeAcknowledger) Ack(tag uint64, multiple bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.success {
		f.acks = append(f.acks, Ack{
			Tag:      tag,
			Multiple: multiple,
		})
		return nil
	} else {
		return errors.New("rabbit ack error")
	}

}

func (f *FakeAcknowledger) Nack(tag uint64, multiple bool, requeue bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nacks = append(f.nacks, Nack{
		Tag:      tag,
		Multiple: multiple,
		Requeue:  requeue,
	})
	return nil
}

func (f *FakeAcknowledger) Reject(tag uint64, requeue bool) error {
	//TODO implement me
	panic("implement me")
}

func (f *FakeAcknowledger) GetAcks() []Ack {
	return f.acks
}
func (f *FakeAcknowledger) GetNacks() []Nack {
	return f.nacks
}

func (f *FakeAcknowledger) WithSuccess() {
	f.success = true
}
