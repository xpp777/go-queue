package rabbitmq

import (
	"context"
	"log"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

type PublishOption func(*amqp.Publishing)

type (
	Sender interface {
		Send(exchange string, routeKey string, msg []byte) error
		SendWithOption(exchange, routeKey string, msg []byte, opts ...PublishOption) error
		Close() error
	}

	senderChannel interface {
		PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool,
			msg amqp.Publishing) error
		Close() error
	}

	RabbitMqSender struct {
		conn        listenerConnection
		channel     senderChannel
		ContentType string
		closeOnce   sync.Once
		closeErr    error
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
	channel, err := conn.Channel()
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

func (q *RabbitMqSender) Close() error {
	q.closeOnce.Do(func() {
		if q.channel != nil {
			if err := q.channel.Close(); err != nil {
				q.closeErr = err
			}
		}
		if q.conn != nil {
			if err := q.conn.Close(); err != nil && q.closeErr == nil {
				q.closeErr = err
			}
		}
	})

	return q.closeErr
}
