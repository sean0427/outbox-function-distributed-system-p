package kafka

import (
	"context"
	"log"
	"net"
	"time"

	"github.com/google/uuid"
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
const maxProcessingTime time.Duration = 10 * time.Minute

// TODO
var clientId = uuid.New().String()

func New(ctx context.Context, topic,
	path string,
	msg chan *kafka.Message) (*DataSteam, error) {
	tcpConn, err := net.Dial("tcp", path)
	if err != nil {
		return nil, err
	}
	kafConfig := kafka.ConnConfig{
		ClientID:  clientId,
		Topic:     topic,
		Partition: 0,
	}

	conn := kafka.NewConnWith(tcpConn, kafConfig)
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

func (ds *DataSteam) Wait(waitTime time.Duration, errChan chan error) {
	maxTime := make(chan time.Time, 1)

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
			case v := <-maxTime:
				log.Printf("Conn stop on %s", v.Format(time.RFC1123))
				ds.client.Close()
				return
			}
		}
	}()

	v := <-time.After(waitTime)
	maxTime <- v
}

func (ds *DataSteam) Send(msg []byte) {
	ds.msgChan <- &kafka.Message{Value: msg}
}
