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

type dataSteam struct {
	client  kafkaConn
	msgChan chan *kafka.Message
	errChan chan error
}

const defaultDeadLine time.Duration = 30 * time.Second
const max_processing_time time.Duration = 10 * time.Minute

func New(ctx context.Context, topic string) (*dataSteam, error) {
	conn, err := kafka.DialLeader(ctx, "tcp", "localhost:9092", topic, 0)
	conn.SetDeadline(time.Now().Add(defaultDeadLine))
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}

	return &dataSteam{
		client:  conn,
		msgChan: make(chan *kafka.Message),
		errChan: make(chan error),
	}, nil
}

func (ds *dataSteam) Wait(can context.CancelFunc) {
	maxTime := make(chan struct{}, 1)
	go func() {
		maxTime <- struct{}{}
		can()
	}()

	go func() {
		for {
			select {
			case b := <-ds.msgChan:
				_, err := ds.client.WriteMessages(*b)
				if err != nil {
					// error not be block message
					ds.errChan <- err
				}
			case <-time.After(1 * time.Millisecond):
				//do nothing
			case <-maxTime:
				ds.client.Close()
				close(ds.errChan)
				return
			}
		}
	}()

	go func() {
		for {
			if err, done := <-ds.errChan; !done {
				log.Println(err.Error())
			} else if done {
				// consume to end
				return

			}
		}
	}()

	<-maxTime
}

func (ds *dataSteam) Send(msg []byte) {
	ds.msgChan <- &kafka.Message{Value: msg}
}
