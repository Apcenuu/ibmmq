package Rabbit

import (
	"fmt"
	"net/url"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog/log"
)

func ConnectToRabbit(credentials Credentials) (*amqp.Channel, error) {

	escapedPassword := url.QueryEscape(credentials.Password)

	connectUrl := fmt.Sprintf("amqp://%s:%s@%s:%s/%s",
		credentials.Login,
		escapedPassword,
		credentials.Host,
		credentials.Port,
		credentials.Vhost)

	log.Debug().
		Str("vhost", credentials.Vhost).
		Str("connect_url", connectUrl).
		Msg("Trying to connect to rabbit")
	rabbitConnection, err := amqp.Dial(connectUrl)

	if err != nil {
		return nil, err
	}

	rabbitChannel, err := rabbitConnection.Channel()

	if err != nil {
		return nil, err
	}

	return rabbitChannel, nil
}
