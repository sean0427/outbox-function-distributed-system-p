package config

import (
	"errors"
	"os"
)

func GetKafkaPath() (string, error) {
	if v, found := os.LookupEnv("KAFKA_PATH"); found {
		return v, nil
	} else {
		return "", errors.New("KAFKA_PATH environment variable is not set")
	}
}
