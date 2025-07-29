package Translator

import (
	"ibmConsumer/ibmmq/Ibm"
	"ibmConsumer/ibmmq/Rabbit"
	"testing"
)

func TestMoveMessagesFromIbmToRabbit(t *testing.T) {
	queueManager := new(Ibm.FakeQueueManager)
	rabbitChannel := new(Rabbit.FakeChannel)

	MoveMessagesFromIbmToRabbit(
		queueManager, rabbitChannel, "default", "test_rabbit", "/logs", func() {
		})

}
