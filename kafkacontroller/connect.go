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
	conns sync.Map

	errChan chan error
	path    string
}

func New(path string) *kafkaController {
	k := &kafkaController{
		conns: sync.Map{},
		path:  path,
	}

	k.start()

	return k
}

const defaultClientTimeout = 30 * time.Second

func (ka *kafkaController) start() {
	log.Println("kafka controller start")

	ka.errChan = make(chan error)

	go func() {
		if err, done := <-ka.errChan; !done {
			ka.handleError(err)
		} else {
			log.Println("kafka conntroller error minor stopped")
		}
	}()
}

func (ka *kafkaController) getOrCreate(topic string) chan []byte {
	if v, ok := ka.conns.Load(topic); ok {
		return v.(chan []byte)
	}

	return ka.create(topic)
}

func (ka *kafkaController) create(topic string) chan []byte {
	conn := make(chan []byte)
	ka.conns.Store(topic, conn)

	connKafka := make(chan *kafka.Message)

	var wg sync.WaitGroup
	wg.Add(1)
	context.Background()

	ctx, cancel := context.WithTimeout(context.Background(), defaultClientTimeout)
	client, err := create(ctx, topic, ka.path, connKafka, ka.errChan)
	if err != nil {
		log.Print(err.Error())
		cancel()

		return conn
	}

	go func() {
		defer close(connKafka)
		defer cancel()

		log.Printf("client start waiting message")
		client.Wait(ctx, ka.errChan)
		<-ctx.Done()
		ka.conns.Delete(topic)
		close(conn)
		wg.Wait()
	}()

	go func() {
		defer wg.Done()
		log.Println("start receive message")

		count := 0
		for {
			if msg, ok := <-conn; ok {
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

	return conn
}

// for graceful shutdown
func (ka *kafkaController) Close() {
	c := ka.conns
	ka.conns = sync.Map{}
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

func (ka *kafkaController) Send(topic string, message []byte) {
	ch := ka.getOrCreate(topic)
	go func() {
		ch <- message
	}()
}
