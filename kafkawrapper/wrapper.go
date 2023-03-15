package kafka

import (
	"context"
	"log"

	kafka "github.com/segmentio/kafka-go"
)

type kafkaConn interface {
	WriteMessages(msgs ...kafka.Message) (int, error)
	Close() error
}

var _ kafkaConn = (*kafka.Conn)(nil)

type DataSteam struct {
	client  *kafka.Writer
	msgChan chan *kafka.Message
	errChan chan error
}

// TODO
// var clientId = "outbox-service"

func New(ctx context.Context, topic,
	path string,
	msg chan *kafka.Message,
	errChan chan error) (*DataSteam, error) {

	log.Printf("kafka connect to %s %s", path, topic)

	w := &kafka.Writer{
		Addr:                   kafka.TCP(path),
		Topic:                  topic,
		AllowAutoTopicCreation: true,
	}

	w.WriteMessages(ctx, kafka.Message{Value: []byte("hello")})
	return &DataSteam{
		client:  w,
		msgChan: msg,
		errChan: errChan,
	}, nil
}

func (ds *DataSteam) Wait(ctx context.Context, errChan chan error) {
	go func() {
		for {
			select {
			case b, ok := <-ds.msgChan:
				if ok {
					return
				}
				log.Printf("sending message key: %s", (*b).Key)
				err := ds.client.WriteMessages(ctx, *b)
				if err != nil {
					// error not be block message
					errChan <- err
				}
			case <-ctx.Done():
				log.Printf("Conn stop by context stop")
				ds.client.Close()
				return
			}
		}
	}()
}
