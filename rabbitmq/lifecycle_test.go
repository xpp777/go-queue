package rabbitmq

import (
	"context"
	"sync"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

type mockListenerChannel struct {
	deliveries   chan amqp.Delivery
	consumeTag   string
	cancelTags   []string
	consumeCalls int
	closeCalls   int
	cancelOnce   sync.Once
	mutex        sync.Mutex
}

func (m *mockListenerChannel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool,
	args amqp.Table) (<-chan amqp.Delivery, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.consumeCalls++
	m.consumeTag = consumer
	return m.deliveries, nil
}

func (m *mockListenerChannel) Cancel(consumer string, noWait bool) error {
	m.mutex.Lock()
	m.cancelTags = append(m.cancelTags, consumer)
	m.mutex.Unlock()

	m.cancelOnce.Do(func() {
		close(m.deliveries)
	})

	return nil
}

func (m *mockListenerChannel) Close() error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.closeCalls++
	return nil
}

type startupRaceChannel struct {
	deliveries          []chan amqp.Delivery
	closed              []bool
	secondConsumeReady  chan struct{}
	allowSecondConsume  chan struct{}
	cancelTags          []string
	consumeCalls        int
	closeCalls          int
	mutex               sync.Mutex
}

func (m *startupRaceChannel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool,
	args amqp.Table) (<-chan amqp.Delivery, error) {
	m.mutex.Lock()
	callIndex := m.consumeCalls
	m.consumeCalls++
	m.mutex.Unlock()

	if callIndex == 1 {
		close(m.secondConsumeReady)
		<-m.allowSecondConsume
	}

	return m.deliveries[callIndex], nil
}

func (m *startupRaceChannel) Cancel(consumer string, noWait bool) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.cancelTags = append(m.cancelTags, consumer)
	for i, deliveries := range m.deliveries {
		if m.closed[i] {
			continue
		}

		close(deliveries)
		m.closed[i] = true
	}

	return nil
}

func (m *startupRaceChannel) Close() error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.closeCalls++
	return nil
}

type mockSenderChannel struct {
	closeCalls int
	mutex      sync.Mutex
}

func (m *mockSenderChannel) PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool,
	msg amqp.Publishing) error {
	return nil
}

func (m *mockSenderChannel) Close() error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.closeCalls++
	return nil
}

type mockConnection struct {
	closeCalls int
	mutex      sync.Mutex
}

func (m *mockConnection) Close() error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.closeCalls++
	return nil
}

type blockingHandler struct {
	started chan struct{}
	release chan struct{}
}

func (h blockingHandler) Consume(message string) error {
	close(h.started)
	<-h.release
	return nil
}

func TestRabbitListenerStopWaitsForHandlers(t *testing.T) {
	deliveries := make(chan amqp.Delivery, 1)
	channel := &mockListenerChannel{
		deliveries: deliveries,
	}
	conn := &mockConnection{}
	handler := blockingHandler{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	listener := &RabbitListener{
		conn:    conn,
		channel: channel,
		forever: make(chan struct{}),
		handler: handler,
		queues: RabbitListenerConf{
			ListenerQueues: []ConsumerConf{
				{Name: "jobs", AutoAck: true},
			},
		},
	}

	startDone := make(chan struct{})
	go func() {
		listener.Start()
		close(startDone)
	}()

	deliveries <- amqp.Delivery{Body: []byte("hello")}
	<-handler.started

	stopDone := make(chan struct{})
	go func() {
		listener.Stop()
		close(stopDone)
	}()

	select {
	case <-stopDone:
		t.Fatal("stop returned before the in-flight message finished")
	default:
	}

	close(handler.release)

	<-stopDone
	<-startDone

	if len(channel.cancelTags) != 1 {
		t.Fatalf("expected 1 canceled consumer, got %d", len(channel.cancelTags))
	}
	if channel.cancelTags[0] != "jobs-0" {
		t.Fatalf("expected canceled consumer jobs-0, got %s", channel.cancelTags[0])
	}
	if channel.closeCalls != 1 {
		t.Fatalf("expected channel close once, got %d", channel.closeCalls)
	}
	if conn.closeCalls != 1 {
		t.Fatalf("expected connection close once, got %d", conn.closeCalls)
	}
}

func TestRabbitMqSenderCloseIsIdempotent(t *testing.T) {
	channel := &mockSenderChannel{}
	conn := &mockConnection{}
	sender := &RabbitMqSender{
		conn:    conn,
		channel: channel,
	}

	if err := sender.Close(); err != nil {
		t.Fatalf("unexpected close error: %v", err)
	}
	if err := sender.Close(); err != nil {
		t.Fatalf("unexpected close error on second close: %v", err)
	}

	if channel.closeCalls != 1 {
		t.Fatalf("expected channel close once, got %d", channel.closeCalls)
	}
	if conn.closeCalls != 1 {
		t.Fatalf("expected connection close once, got %d", conn.closeCalls)
	}
}

func TestRabbitListenerStopDuringStartupCancelsLateConsumers(t *testing.T) {
	channel := &startupRaceChannel{
		deliveries: []chan amqp.Delivery{
			make(chan amqp.Delivery),
			make(chan amqp.Delivery),
		},
		closed:             []bool{false, false},
		secondConsumeReady: make(chan struct{}),
		allowSecondConsume: make(chan struct{}),
	}
	conn := &mockConnection{}
	listener := &RabbitListener{
		conn:    conn,
		channel: channel,
		forever: make(chan struct{}),
		handler: stubConsumeHandler{},
		queues: RabbitListenerConf{
			ListenerQueues: []ConsumerConf{
				{Name: "jobs-a", AutoAck: true},
				{Name: "jobs-b", AutoAck: true},
			},
		},
	}

	startDone := make(chan struct{})
	go func() {
		listener.Start()
		close(startDone)
	}()

	<-channel.secondConsumeReady

	stopDone := make(chan struct{})
	go func() {
		listener.Stop()
		close(stopDone)
	}()

	close(channel.allowSecondConsume)

	<-stopDone
	<-startDone

	if len(channel.cancelTags) != 2 {
		t.Fatalf("expected 2 canceled consumers, got %d", len(channel.cancelTags))
	}
	if conn.closeCalls != 1 {
		t.Fatalf("expected connection close once, got %d", conn.closeCalls)
	}
}
