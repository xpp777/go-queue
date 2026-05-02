package rabbitmq

import (
	"context"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type PublishOption func(*amqp.Publishing)

type (
	Sender interface {
		Send(exchange string, routeKey string, msg []byte) error
		SendWithOption(exchange, routeKey string, msg []byte, opts ...PublishOption) error
	}

	RabbitMqSender struct {
		conn        *amqp.Connection
		channel     *amqp.Channel
		ContentType string
	}
)

func WithDelay(delayMs int32) PublishOption {
	return func(p *amqp.Publishing) {
		if p.Headers == nil {
			p.Headers = amqp.Table{}
		}
		p.Headers["x-delay"] = delayMs
	}
}

func MustNewSender(rabbitMqConf RabbitSenderConf) *RabbitMqSender {
	sender := &RabbitMqSender{ContentType: rabbitMqConf.ContentType}
	conn, err := amqp.Dial(getRabbitURL(rabbitMqConf.RabbitConf))
	if err != nil {
		log.Fatalf("failed to connect rabbitmq, error: %v", err)
	}

	sender.conn = conn
	channel, err := sender.conn.Channel()
	if err != nil {
		log.Fatalf("failed to open a channel, error: %v", err)
	}

	sender.channel = channel
	return sender
}

func (q *RabbitMqSender) Send(exchange string, routeKey string, msg []byte) error {
	return q.channel.PublishWithContext(
		context.Background(),
		exchange,
		routeKey,
		false,
		false,
		amqp.Publishing{
			ContentType: q.ContentType,
			Body:        msg,
		},
	)
}

func (q *RabbitMqSender) SendWithOption(exchange, routeKey string, msg []byte, opts ...PublishOption) error {
	pub := amqp.Publishing{
		ContentType: q.ContentType,
		Body:        msg,
	}

	for _, opt := range opts {
		opt(&pub)
	}

	return q.channel.PublishWithContext(
		context.Background(),
		exchange,
		routeKey,
		false,
		false,
		pub,
	)
}
