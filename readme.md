# go-queue

## dq

High available beanstalkd.

### consumer example
```go
consumer := dq.NewConsumer(dq.DqConf{
	Beanstalks: []dq.Beanstalk{
		{
			Endpoint: "localhost:11300",
			Tube:     "tube",
		},
		{
			Endpoint: "localhost:11300",
			Tube:     "tube",
		},
	},
	Redis: redis.RedisConf{
		Host: "localhost:6379",
		Type: redis.NodeType,
	},
})
consumer.Consume(func(body []byte) {
	fmt.Println(string(body))
})
```
### producer example
```go
producer := dq.NewProducer([]dq.Beanstalk{
	{
		Endpoint: "localhost:11300",
		Tube:     "tube",
	},
	{
		Endpoint: "localhost:11300",
		Tube:     "tube",
	},
})	

for i := 1000; i < 1005; i++ {
	_, err := producer.Delay([]byte(strconv.Itoa(i)), time.Second*5)
	if err != nil {
		fmt.Println(err)
	}
}
```

## kq

Kafka Pub/Sub framework

### consumer example

config.yaml
```yaml
Name: kq
Brokers:
- 127.0.0.1:19092
- 127.0.0.1:19092
- 127.0.0.1:19092
Group: adhoc
Topic: kq
Offset: first
Consumers: 1
```

example code
```go
var c kq.KqConf
conf.MustLoad("config.json", &c)

q := kq.MustNewQueue(c, kq.WithHandle(func(k, v string) error {
	fmt.Printf("=> %s\n", v)
	return nil
}))
defer q.Stop()
q.Start()
```

### producer example

```go
type message struct {
	Key     string `json:"key"`
	Value   string `json:"value"`
	Payload string `json:"message"`
}


pusher := kq.NewPusher([]string{
	"127.0.0.1:19092",
	"127.0.0.1:19092",
	"127.0.0.1:19092",
}, "kq")

ticker := time.NewTicker(time.Millisecond)
for round := 0; round < 3; round++ {
	select {
	case <-ticker.C:
		count := rand.Intn(100)
		m := message{
			Key:     strconv.FormatInt(time.Now().UnixNano(), 10),
			Value:   fmt.Sprintf("%d,%d", round, count),
			Payload: fmt.Sprintf("%d,%d", round, count),
		}
		body, err := json.Marshal(m)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println(string(body))
		if err := pusher.Push(string(body)); err != nil {
			log.Fatal(err)
		}
	}
}
cmdline.EnterToContinue()
```

## rabbitmq

RabbitMQ sender/listener wrapper.

### listener ack behavior

```yaml
ListenerConf:
  Username: guest
  Password: guest
  Host: 127.0.0.1
  Port: 5672
  ListenerQueues:
    -
      Name: jxj
      AutoAck: false
      RequeueOnError: true
```

When `AutoAck` is `false`, the listener will:

- call `Ack` after `Consume()` returns `nil`
- call `Nack` after `Consume()` returns error
- requeue failed messages when `RequeueOnError` is `true`

### delayed message example

```go
admin := rabbitmq.MustNewAdmin(conf)
err := admin.DeclareExchange(rabbitmq.ExchangeConf{
	ExchangeName: "jiang",
	Type:         "x-delayed-message",
	DelayedType:  "direct",
}, nil)
if err != nil {
	log.Fatal(err)
}

sender := rabbitmq.MustNewSender(rabbitmq.RabbitSenderConf{
	RabbitConf:  conf,
	ContentType: "text/plain",
})
defer sender.Close()

err = sender.SendWithOption("jiang", "jxj", []byte("hello"), rabbitmq.WithDelay(5000))
if err != nil {
	log.Fatal(err)
}
```
