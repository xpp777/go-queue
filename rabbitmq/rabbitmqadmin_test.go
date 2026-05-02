package rabbitmq

import (
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestBuildExchangeArgsForNormalExchange(t *testing.T) {
	args := amqp.Table{
		"alternate-exchange": "backup",
	}

	got := buildExchangeArgs(ExchangeConf{
		ExchangeName: "normal",
		Type:         "direct",
	}, args)

	if got["alternate-exchange"] != "backup" {
		t.Fatalf("expected alternate-exchange to be preserved, got %v", got["alternate-exchange"])
	}

	if _, ok := got["x-delayed-type"]; ok {
		t.Fatalf("did not expect x-delayed-type for normal exchange")
	}
}

func TestBuildExchangeArgsForDelayedExchange(t *testing.T) {
	args := amqp.Table{
		"custom": "value",
	}

	got := buildExchangeArgs(ExchangeConf{
		ExchangeName: "delay",
		Type:         "x-delayed-message",
		DelayedType:  "topic",
	}, args)

	if got["custom"] != "value" {
		t.Fatalf("expected custom arg to be preserved, got %v", got["custom"])
	}

	if got["x-delayed-type"] != "topic" {
		t.Fatalf("expected x-delayed-type topic, got %v", got["x-delayed-type"])
	}

	if _, ok := args["x-delayed-type"]; ok {
		t.Fatalf("expected source args to remain unchanged")
	}
}

func TestBuildExchangeArgsForDelayedExchangeUsesDefaultType(t *testing.T) {
	got := buildExchangeArgs(ExchangeConf{
		ExchangeName: "delay",
		Type:         "x-delayed-message",
	}, nil)

	if got["x-delayed-type"] != "direct" {
		t.Fatalf("expected default x-delayed-type direct, got %v", got["x-delayed-type"])
	}
}
