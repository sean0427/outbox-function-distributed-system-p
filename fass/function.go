package function

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	message "github.com/sean0427/outbox-function-distributed-system-p"
	"github.com/sean0427/outbox-function-distributed-system-p/kafka"
)

const topic = "outbox-function-distributed-system-p"

func Handle(w http.ResponseWriter, r *http.Request) {
	var input []byte

	if r.Body != nil {
		ctx, cancelFunc := context.WithTimeout(r.Context(), 10*time.Minute)
		defer r.Body.Close()

		ka, err := kafka.New(ctx, topic)
		if err != nil {
			panic(err)
		}
		se := message.NewWithSingle(topic, ka)

		body, _ := io.ReadAll(r.Body)
		se.Send(body)

		ka.Wait(cancelFunc)
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf("Body: %s", string(input))))
}
