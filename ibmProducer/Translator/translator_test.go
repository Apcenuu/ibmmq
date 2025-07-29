package Translator

import (
	"context"
	"encoding/hex"
	"ibmProducer/ibmmq/Ibm"
	"ibmProducer/ibmmq/Rabbit"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

func TestMoveMessagesFromRabbitToIbm(t *testing.T) {

	testIbmQueue := new(Ibm.FakeQueue)
	acknowledger := new(Rabbit.FakeAcknowledger)

	// Ибм успешно запишет сообщение
	testIbmQueue.WithSuccess()
	// Кролик успешно акнет сообщение
	acknowledger.WithSuccess()

	messages := make(chan amqp.Delivery, 1)

	messages <- amqp.Delivery{
		Acknowledger: acknowledger,
		Body:         []byte("Message_1"),
		Headers:      amqp.Table{"header1": "value1"},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	MoveMessagesFromRabbitToIbm(
		ctx,
		messages,
		&*testIbmQueue,
		"test_ibm",
		3*time.Second,
		func() {})

	// Проверяем, что ибм получил сообщение
	ibmMessages := testIbmQueue.GetMessages()
	require.Len(t, ibmMessages, 1, "Массив должен содержать ровно 1 элемент")
	// Проверяем, что записался MsgId
	// TODO Почему в MsgId передаётся нулл
	require.Equal(t, "000000000000000000000000000000000000000000000000", hex.EncodeToString(ibmMessages[0].MessageDescriptor.MsgId))
	// Проверяем опции мессадж дескриптора
	// С помощью этого теста нашёл, что большинство опций стоит по дефолту. Но некоторые требуются
	require.Equal(t, int32(1), ibmMessages[0].MessageDescriptor.Version)
	require.Equal(t, int32(-1), ibmMessages[0].MessageDescriptor.Expiry)
	require.Equal(t, int32(0), ibmMessages[0].MessageDescriptor.Report)
	require.Equal(t, "", ibmMessages[0].MessageDescriptor.Format)
	require.Equal(t, int32(0), ibmMessages[0].MessageDescriptor.Priority)
	require.Equal(t, int32(1), ibmMessages[0].MessageDescriptor.Persistence)
	require.Equal(t, int32(546), ibmMessages[0].MessageDescriptor.Encoding)
	require.Equal(t, int32(8), ibmMessages[0].MessageDescriptor.MsgType)
	require.Equal(t, int32(1208), ibmMessages[0].MessageDescriptor.CodedCharSetId)
	// Проверяем опции записи
	require.Equal(t, int32(8260), ibmMessages[0].PutMessageOptions.Options)
	// Проверяем само тело сообщения
	require.Equal(t, "Message_1", string(ibmMessages[0].Buffer))

	// Проверить, что сообщение акнуто в кролике (то есть кролик знает, что всё ушло успешно)
	rabbitAcks := acknowledger.GetAcks()
	require.Len(t, rabbitAcks, 1, "Массив должен содержать ровно 1 элемент")
	// Проверяем опцию multiple
	require.False(t, rabbitAcks[0].Multiple)
	// Проверяем что накнутых сообщений нет
	rabbitNacks := acknowledger.GetNacks()
	require.Len(t, rabbitNacks, 0)
}

func TestConnectionBrokenError(t *testing.T) {
	testIbmQueue := new(Ibm.FakeQueue)
	acknowledger := new(Rabbit.FakeAcknowledger)

	// Кролик успешно акнет сообщение
	acknowledger.WithSuccess()

	messages := make(chan amqp.Delivery, 1)

	messages <- amqp.Delivery{
		Acknowledger: acknowledger,
		Body:         []byte("Message_1"),
		Headers:      amqp.Table{"header1": "value1"},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	// Не вызываем testIbmQueue.WithSuccess(), чтобы получить ошибку при попытке положить в Ибм

	MoveMessagesFromRabbitToIbm(
		ctx,
		messages,
		&*testIbmQueue,
		"test_ibm",
		3*time.Second,
		func() {})

	// Проверяем, что сообщение не записалось в ибм при ошибке
	ibmMessages := testIbmQueue.GetMessages()
	require.Len(t, ibmMessages, 0)

	// Проверяем, что сообщение накнуто и redelivery == true
	rabbitNacks := acknowledger.GetNacks()
	require.Len(t, rabbitNacks, 1)
	require.False(t, rabbitNacks[0].Multiple)
	require.True(t, rabbitNacks[0].Requeue)
}

func TestRabbitError(t *testing.T) {
	testIbmQueue := new(Ibm.FakeQueue)
	acknowledger := new(Rabbit.FakeAcknowledger)

	testIbmQueue.WithSuccess()

	messages := make(chan amqp.Delivery, 1)

	messages <- amqp.Delivery{
		Acknowledger: acknowledger,
		Body:         []byte("Message_1"),
		Headers:      amqp.Table{"header1": "value1"},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	MoveMessagesFromRabbitToIbm(
		ctx,
		messages,
		&*testIbmQueue,
		"test_ibm",
		3*time.Second,
		func() {})

	// В ибм сообщение ложится без проблем
	ibmMessages := testIbmQueue.GetMessages()
	require.Len(t, ibmMessages, 1, "Массив должен содержать ровно 1 элемент")

	// Проверяем, что сообщение накнуто и redelivery == false
	rabbitNacks := acknowledger.GetNacks()
	require.Len(t, rabbitNacks, 1)
	require.False(t, rabbitNacks[0].Multiple)
	require.False(t, rabbitNacks[0].Requeue)
}
