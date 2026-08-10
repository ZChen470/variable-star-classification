package kafka

import (
	"errors"
	"strings"

	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

func NewSCRAMSHA256Mechanism(username, password string) (sasl.Mechanism, error) {
	username = strings.TrimSpace(username)

	if username == "" {
		return nil, errors.New("Kafka SASL username must not be blank")
	}

	if strings.TrimSpace(password) == "" {
		return nil, errors.New("Kafka SASL password must not be blank")
	}

	return scram.Auth{
		User: username,
		Pass: password,
	}.AsSha256Mechanism(), nil
}
