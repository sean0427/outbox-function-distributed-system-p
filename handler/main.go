package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	message "github.com/sean0427/outbox-function-distributed-system-p"
	"github.com/sean0427/outbox-function-distributed-system-p/config"
	"github.com/sean0427/outbox-function-distributed-system-p/kafka"
)

type messageService interface {
	Send([]byte)
}

func getHandler(service messageService, client *kafka.DataSteam) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		var input []byte

		if r.Body == nil {
			w.WriteHeader(http.StatusBadRequest)
		}

		defer r.Body.Close()

		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(fmt.Sprintf("%v", err)))
			return
		}

		service.Send(body)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(fmt.Sprintf("Body: %s", string(input))))
	}
}

func getKafkaConfig() (string, string) {
	path, err := config.GetKafkaPath()
	if err != nil {
		panic(err)
	}

	topic, err := config.GetKafkaTopic()
	if err != nil {
		panic(err)
	}

	return path, topic
}

func addServedHeader(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("X-Served-Date", time.Now().String())
}

func makeRequestHandler(middleware http.HandlerFunc, handler http.HandlerFunc) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		middleware(w, r)

		handler(w, r)
	}
}
func main() {
	path, topic := getKafkaConfig()

	ctx, cancelFunc := context.WithTimeout(context.Background(), 10*time.Minute)
	ka, err := kafka.New(ctx, path, topic)
	if err != nil {
		cancelFunc()
		return
	}

	se := message.NewWithSingle(topic, ka)
	h := getHandler(se, ka)
	s := &http.Server{
		Addr:           fmt.Sprintf(":%d", 8080),
		ReadTimeout:    3 * time.Second,
		WriteTimeout:   3 * time.Second,
		MaxHeaderBytes: 1 << 20, // Max header of 1MB
	}

	next := addServedHeader
	http.HandleFunc("/", makeRequestHandler(h, next))
	go func() {
		ka.Wait()
	}()

	log.Fatal(s.ListenAndServe())
	cancelFunc()
}
