package rabbitmq

import (
	"errors"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

type stubConsumeHandler struct {
	err error
}

func (h stubConsumeHandler) Consume(message string) error {
	return h.err
}

type ackRecorder struct {
	ackCount     int
	nackCount    int
	rejectCount  int
	lastMultiple bool
	lastRequeue  bool
	lastTag      uint64
}

func (r *ackRecorder) Ack(tag uint64, multiple bool) error {
	r.ackCount++
	r.lastTag = tag
	r.lastMultiple = multiple
	return nil
}

func (r *ackRecorder) Nack(tag uint64, multiple bool, requeue bool) error {
	r.nackCount++
	r.lastTag = tag
	r.lastMultiple = multiple
	r.lastRequeue = requeue
	return nil
}

func (r *ackRecorder) Reject(tag uint64, requeue bool) error {
	r.rejectCount++
	r.lastTag = tag
	r.lastRequeue = requeue
	return nil
}

func TestRabbitListenerConsumeMessageAutoAck(t *testing.T) {
	recorder := &ackRecorder{}
	listener := RabbitListener{
		handler: stubConsumeHandler{},
	}

	listener.consumeMessage(ConsumerConf{AutoAck: true}, amqp.Delivery{
		Body:         []byte("hello"),
		DeliveryTag:  1,
		Acknowledger: recorder,
	})

	if recorder.ackCount != 0 {
		t.Fatalf("expected no ack, got %d", recorder.ackCount)
	}
	if recorder.nackCount != 0 {
		t.Fatalf("expected no nack, got %d", recorder.nackCount)
	}
}

func TestRabbitListenerConsumeMessageManualAckSuccess(t *testing.T) {
	recorder := &ackRecorder{}
	listener := RabbitListener{
		handler: stubConsumeHandler{},
	}

	listener.consumeMessage(ConsumerConf{AutoAck: false}, amqp.Delivery{
		Body:         []byte("hello"),
		DeliveryTag:  2,
		Acknowledger: recorder,
	})

	if recorder.ackCount != 1 {
		t.Fatalf("expected 1 ack, got %d", recorder.ackCount)
	}
	if recorder.lastTag != 2 {
		t.Fatalf("expected ack tag 2, got %d", recorder.lastTag)
	}
	if recorder.nackCount != 0 {
		t.Fatalf("expected no nack, got %d", recorder.nackCount)
	}
}

func TestRabbitListenerConsumeMessageManualAckFailure(t *testing.T) {
	recorder := &ackRecorder{}
	listener := RabbitListener{
		handler: stubConsumeHandler{err: errors.New("consume failed")},
	}

	listener.consumeMessage(ConsumerConf{
		AutoAck:        false,
		RequeueOnError: true,
	}, amqp.Delivery{
		Body:         []byte("hello"),
		DeliveryTag:  3,
		Acknowledger: recorder,
	})

	if recorder.ackCount != 0 {
		t.Fatalf("expected no ack, got %d", recorder.ackCount)
	}
	if recorder.nackCount != 1 {
		t.Fatalf("expected 1 nack, got %d", recorder.nackCount)
	}
	if !recorder.lastRequeue {
		t.Fatal("expected requeue=true on nack")
	}
	if recorder.lastTag != 3 {
		t.Fatalf("expected nack tag 3, got %d", recorder.lastTag)
	}
}

func TestRabbitListenerConsumeMessageManualAckFailureWithoutRequeue(t *testing.T) {
	recorder := &ackRecorder{}
	listener := RabbitListener{
		handler: stubConsumeHandler{err: errors.New("consume failed")},
	}

	listener.consumeMessage(ConsumerConf{
		AutoAck:        false,
		RequeueOnError: false,
	}, amqp.Delivery{
		Body:         []byte("hello"),
		DeliveryTag:  4,
		Acknowledger: recorder,
	})

	if recorder.nackCount != 1 {
		t.Fatalf("expected 1 nack, got %d", recorder.nackCount)
	}
	if recorder.lastRequeue {
		t.Fatal("expected requeue=false on nack")
	}
}
