package message

import (
	"context"
	"io"
)

type messageHandler interface {
	Send(topic string, data []byte)
}

type service struct {
	handlers messageHandler
}

func NewWithSingle(handler messageHandler) *service {
	return &service{
		handler,
	}
}

func (s *service) SendTo(ctx context.Context, body io.Reader, topic string, id string) error {
	data, err := io.ReadAll(body)
	if err != nil {
		return err
	}
	s.handlers.Send(topic, data)

	return nil
}
