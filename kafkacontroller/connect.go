package kafka2

import (
	"context"
	"log"
	"strconv"
	"sync"
	"time"

	kafkaWrapper "github.com/sean0427/outbox-function-distributed-system-p/kafkawrapper"
	"github.com/segmentio/kafka-go"
)

var create = kafkaWrapper.New

type kafkaController struct {
	conns *sync.Map

	errChan chan error
	path    string
}

func New(path string) *kafkaController {
	k := &kafkaController{
		conns: &sync.Map{},
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
	v, _ := ka.conns.LoadOrStore(topic, ka.create(topic))

	return v.(chan []byte)
}

func (ka *kafkaController) create(topic string) chan []byte {
	conn := make(chan []byte)
	connKafka := make(chan *kafka.Message)

	go func() {
		count := 0

		for {
			if msg, done := <-conn; !done {
				count++
				connKafka <- &kafka.Message{
					Key:   []byte(strconv.Itoa(count)),
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
		ka.conns.Delete(topic)
	}()

	return conn
}

// for graceful shutdown
func (ka *kafkaController) Close() {
	c := ka.conns
	ka.conns = nil
	c.Range(func(k, v any) bool {
		log.Printf("Closing connection to %s", k)
		close(v.(chan []byte))
		return true
	})

	close(ka.errChan)
	ka.errChan = nil
}

func (ka *kafkaController) handleError(err error) {
	log.Print(err.Error())
}
