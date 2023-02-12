package message

import (
	"log"
)

type messageHandler interface {
	Send(msg []byte)
}

type service struct {
	handlers map[string]messageHandler
}

func New(handler map[string]messageHandler) *service {
	return &service{
		handlers: handler,
	}
}

func NewWithSingle(key string, handler messageHandler) *service {
	return &service{
		handlers: map[string]messageHandler{
			key: handler,
		}}
}

func (s *service) Send(msg []byte) {
	for key, handler := range s.handlers {
		log.Printf("sending message to handler: %s", key)
		handler.Send(msg)
	}
}
