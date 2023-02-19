package net_helper

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// TODO move to general

func health(c *gin.Context) {
	c.Status(http.StatusOK)
}

func readiness(c *gin.Context) {
	//TODO
	c.Status(http.StatusOK)
}

func BasicHandler(r *gin.Engine) {
	r.GET("/health", health)
	r.GET("readiness", readiness)
}
