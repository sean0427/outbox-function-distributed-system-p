package net

import (
	"context"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"
)

type messageService interface {
	SendTo(context.Context, io.Reader, string, string) error
}

type handler struct {
	service messageService
}

func New(service messageService) *handler {
	return &handler{
		service: service,
	}
}

func (h *handler) PushMessage(c *gin.Context) {
	c.ContentType()

	topic := c.Query("topic")
	entityId := c.Query("entity_id")

	err := h.service.SendTo(c, c.Request.Body, topic, entityId)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.Status(http.StatusOK)
}

func (h *handler) Register(r *gin.Engine) {
	r.POST("/message/push", h.PushMessage)
}
