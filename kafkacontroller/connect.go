package kafka2

import (
	"context"
	"log"
	"time"

	kafkaWrapper "github.com/sean0427/outbox-function-distributed-system-p/kafkawrapper"
	"github.com/segmentio/kafka-go"
)

var create = kafkaWrapper.New

type kafkaController struct {
	conns   map[string]chan []byte
	errChan chan error
	path    string
}

func New(path string) *kafkaController {
	k := &kafkaController{
		conns: make(map[string]chan []byte),
		path:  path,
	}

	k.start()

	return k
}

const defaultClientTimeout = 30 * time.Second

func (ka *kafkaController) start() {
	log.Println("kafka2 start")

	ka.errChan = make(chan error)

	go func() {
		if err, done := <-ka.errChan; !done {
			ka.handleError(err)
		} else {
			log.Println("kafka2 error minor stopped")
		}
	}()
}

func (ka *kafkaController) GetOrCreate(topic string) chan []byte {
	if v, found := ka.conns[topic]; found {
		return v
	}

	return ka.create(topic)
}

func (ka *kafkaController) create(topic string) chan []byte {
	conn := make(chan []byte)
	connKafka := make(chan *kafka.Message)

	go func() {
		for {
			if msg, done := <-conn; !done {
				connKafka <- &kafka.Message{
					Value: msg,
				}
			} else {
				return
			}
		}
	}()

	defer func() {
		defer close(connKafka)
		defer close(conn)

		ctx, cancel := context.WithTimeout(context.Background(), defaultClientTimeout)
		defer cancel()

		client, err := create(ctx, topic, ka.path, connKafka)
		if err != nil {
			log.Print(err.Error())
			cancel()

			return
		}

		client.Wait(ctx, ka.errChan)
		delete(ka.conns, topic)
	}()

	ka.conns[topic] = conn
	return conn
}

func (ka *kafkaController) Close() {
	for k, v := range ka.conns {
		log.Printf("Closing connection to %s", k)
		close(v)
	}

	ka.conns = nil
	close(ka.errChan)
	ka.errChan = nil
}

func (ka *kafkaController) handleError(err error) {
	log.Print(err.Error())
}
