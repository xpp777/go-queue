package rabbitmq

import (
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestWithDelay(t *testing.T) {
	pub := amqp.Publishing{}

	WithDelay(5000)(&pub)

	if pub.Headers["x-delay"] != int32(5000) {
		t.Fatalf("expected x-delay 5000, got %v", pub.Headers["x-delay"])
	}
}
