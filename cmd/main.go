package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	message "github.com/sean0427/outbox-function-distributed-system-p"
	"github.com/sean0427/outbox-function-distributed-system-p/config"
	net_helper "github.com/sean0427/outbox-function-distributed-system-p/helper"
	kafka_controller "github.com/sean0427/outbox-function-distributed-system-p/kafkacontroller"
	"github.com/sean0427/outbox-function-distributed-system-p/net"
)

var (
	port = flag.Int("port", 8080, "port")
)

func getKafkaConfig() string {
	path, err := config.GetKafkaPath()
	if err != nil {
		panic(err)
	}

	return path
}

func main() {
	path := getKafkaConfig()

	ka := kafka_controller.New(path)

	se := message.NewWithSingle(ka)
	h := net.New(se)

	g := gin.Default()
	g.Use(func(ctx *gin.Context) {
		ctx.Header("X-Served-Date", time.Now().String())
	})

	net_helper.BasicHandler(g)
	h.Register(g)

	server := &http.Server{
		Addr:           fmt.Sprintf(":%d", 8080),
		ReadTimeout:    3 * time.Second,
		WriteTimeout:   3 * time.Second,
		MaxHeaderBytes: 1 << 20, // Max header of 1MB
		Handler:        g,
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := server.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server stopped: %v", err)
		}
	}()
	log.Printf("server started and listening on %d", *port)

	// gracefully shutdown
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	<-signalChan

	log.Print("received kill signal")
	if err := server.Shutdown(context.Background()); err != nil {
		log.Fatalf("server shutdown failed:%+v", err)
	}
	wg.Wait()
}
