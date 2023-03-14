package kafka

import (
	"context"
	"log"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

type kafkaConn interface {
	WriteMessages(msgs ...kafka.Message) (int, error)
	Close() error
}

var _ kafkaConn = (*kafka.Conn)(nil)

type DataSteam struct {
	client  kafkaConn
	msgChan chan *kafka.Message
	errChan chan error
}

const defaultDeadLine time.Duration = 30 * time.Second

// const maxProcessingTime time.Duration = 10 * time.Minute

// TODO
// var clientId = "outbox-service"

func New(ctx context.Context, topic,
	path string,
	msg chan *kafka.Message) (*DataSteam, error) {

	log.Printf("kafka connect to %s %s", path, topic)

	conn, err := kafka.DialLeader(ctx, "tcp", path, topic, 0)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}

	conn.SetDeadline(time.Now().Add(defaultDeadLine))
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}

	return &DataSteam{
		client:  conn,
		msgChan: make(chan *kafka.Message),
		errChan: make(chan error),
	}, nil
}

func (ds *DataSteam) Wait(ctx context.Context, errChan chan error) {
	go func() {
		for {
			select {
			case b := <-ds.msgChan:
				_, err := ds.client.WriteMessages(*b)
				if err != nil {
					// error not be block message
					errChan <- err
				}
			case <-time.After(1 * time.Millisecond):
				continue
			case <-ctx.Done():
				log.Printf("Conn stop by context stop")
				ds.client.Close()
				return
			}
		}
	}()
	<-ctx.Done()
}

func (ds *DataSteam) Send(msg []byte) {
	ds.msgChan <- &kafka.Message{Value: msg}
}
