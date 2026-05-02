package rabbitmq

import (
	"fmt"
	"log"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/queue"
)

type (
	ConsumeHandle func(message string) error

	ConsumeHandler interface {
		Consume(message string) error
	}

	listenerChannel interface {
		Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool,
			args amqp.Table) (<-chan amqp.Delivery, error)
		Cancel(consumer string, noWait bool) error
		Close() error
	}

	listenerConnection interface {
		Close() error
	}

	RabbitListener struct {
		conn         listenerConnection
		channel      listenerChannel
		forever      chan struct{}
		handler      ConsumeHandler
		queues       RabbitListenerConf
		wg           sync.WaitGroup
		startOnce    sync.Once
		stopOnce     sync.Once
		lifecycleMu  sync.Mutex
		mutex        sync.Mutex
		stopping     bool
		consumerTags []string
	}
)

func MustNewListener(listenerConf RabbitListenerConf, handler ConsumeHandler) queue.MessageQueue {
	listener := &RabbitListener{queues: listenerConf, handler: handler, forever: make(chan struct{})}
	conn, err := amqp.Dial(getRabbitURL(listenerConf.RabbitConf))
	if err != nil {
		log.Fatalf("failed to connect rabbitmq, error: %v", err)
	}

	listener.conn = conn
	channel, err := conn.Channel()
	if err != nil {
		log.Fatalf("failed to open a channel: %v", err)
	}

	listener.channel = channel
	return listener
}

func (q *RabbitListener) Start() {
	q.startOnce.Do(func() {
		q.lifecycleMu.Lock()
		defer q.lifecycleMu.Unlock()

		for i, que := range q.queues.ListenerQueues {
			if q.isStopping() {
				break
			}

			queueConf := que
			consumerTag := buildConsumerTag(queueConf.Name, i)
			msg, err := q.channel.Consume(
				queueConf.Name,
				consumerTag,
				queueConf.AutoAck,
				queueConf.Exclusive,
				queueConf.NoLocal,
				queueConf.NoWait,
				nil,
			)
			if err != nil {
				log.Fatalf("failed to listener, error: %v", err)
			}

			q.appendConsumerTag(consumerTag)
			q.wg.Add(1)
			go func(conf ConsumerConf, deliveries <-chan amqp.Delivery) {
				defer q.wg.Done()
				for d := range deliveries {
					q.consumeMessage(conf, d)
				}
			}(queueConf, msg)
		}

		<-q.forever
	})
}

func (q *RabbitListener) Stop() {
	q.stopOnce.Do(func() {
		q.markStopping()
		close(q.forever)
		q.lifecycleMu.Lock()
		q.lifecycleMu.Unlock()
		q.cancelConsumers()
		q.wg.Wait()
		q.closeResources()
	})
}

func (q *RabbitListener) consumeMessage(conf ConsumerConf, d amqp.Delivery) {
	if err := q.handler.Consume(string(d.Body)); err != nil {
		logx.Errorf("Error on consuming: %s, error: %v", string(d.Body), err)
		if conf.AutoAck {
			return
		}

		if err = d.Nack(false, conf.RequeueOnError); err != nil {
			logx.Errorf("Error on nacking: %s, error: %v", string(d.Body), err)
		}
		return
	}

	if conf.AutoAck {
		return
	}

	if err := d.Ack(false); err != nil {
		logx.Errorf("Error on acking: %s, error: %v", string(d.Body), err)
	}
}

func (q *RabbitListener) appendConsumerTag(tag string) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	q.consumerTags = append(q.consumerTags, tag)
}

func (q *RabbitListener) isStopping() bool {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	return q.stopping
}

func (q *RabbitListener) markStopping() {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	q.stopping = true
}

func (q *RabbitListener) cancelConsumers() {
	if q.channel == nil {
		return
	}

	q.mutex.Lock()
	tags := append([]string(nil), q.consumerTags...)
	q.mutex.Unlock()

	for _, tag := range tags {
		if err := q.channel.Cancel(tag, false); err != nil {
			logx.Errorf("Error on canceling consumer %s, error: %v", tag, err)
		}
	}
}

func (q *RabbitListener) closeResources() {
	if q.channel != nil {
		if err := q.channel.Close(); err != nil {
			logx.Errorf("Error on closing channel, error: %v", err)
		}
	}
	if q.conn != nil {
		if err := q.conn.Close(); err != nil {
			logx.Errorf("Error on closing connection, error: %v", err)
		}
	}
}

func buildConsumerTag(queueName string, index int) string {
	return fmt.Sprintf("%s-%d", queueName, index)
}
