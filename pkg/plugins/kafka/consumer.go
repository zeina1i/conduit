// Copyright © 2022 Meroxa, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:generate mockgen -destination mock/consumer.go -package mock -mock_names=Consumer=Consumer . Consumer

package kafka

import (
	"context"
	"fmt"

	"github.com/conduitio/conduit/pkg/foundation/cerrors"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

type Consumer interface {
	// Get returns a message from the configured topic, waiting at most 'timeoutMs' milliseconds.
	// Returns:
	// A message and the client's 'position' in Kafka, if there's no error, OR
	// A nil message, the client's position in Kafka, and a nil error,
	// if no message was retrieved within the specified timeout, OR
	// A nil message, nil position and an error if there was an error while retrieving the message (e.g. broker down).
	Get() (*kafka.Message, string, error)

	Ack() error

	// Close this consumer and the associated resources (e.g. connections to the broker)
	Close()

	// StartFrom reads messages from the given topic, starting from the given positions.
	// For new partitions or partitions not found in the 'position',
	// the reading behavior is specified by 'readFromBeginning' parameter:
	// if 'true', then all messages will be read, if 'false', only new messages will be read.
	// Returns: An error, if the consumer could not be set to read from the given position, nil otherwise.
	StartFrom(topic string, position map[int]int64, readFromBeginning bool) error
}

type segmentConsumer struct {
	// maps partition IDs to respective readers
	// segmentio/kafka-go requires a reader per partition
	reader      *kafka.Reader
	lastMsgRead *kafka.Message
}

// NewConsumer creates a new Kafka consumer.
func NewConsumer(config Config) (Consumer, error) {
	// todo if we can assume that a new Config instance will always be created by calling Parse(),
	// and that the instance will not be mutated, then we can leave it out these checks.
	if len(config.Servers) == 0 {
		return nil, ErrServersMissing
	}
	if config.Topic == "" {
		return nil, ErrTopicMissing
	}

	return &segmentConsumer{
		reader:    newReader(config),
	}, nil
}

func newReader(c Config) *kafka.Reader {
	// todo add note about new partitions
	var startOffset int64
	if c.ReadFromBeginning {
		startOffset = kafka.FirstOffset
	} else {
		startOffset = kafka.LastOffset
	}
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:     c.Servers,
		Topic:       c.Topic,
		StartOffset: startOffset,
		GroupID: uuid.NewString(),
	})
}

func (c *segmentConsumer) Get() (*kafka.Message, string, error) {
	msg, err := c.reader.ReadMessage(context.Background())
	if err != nil {
		return nil, "", cerrors.Errorf("couldn't read message: %w", err)
	}
	c.lastMsgRead = &msg
	return &msg, c.readerID(), nil
}

func (c *segmentConsumer) Ack() error {
	err := c.reader.CommitMessages(context.Background(), *c.lastMsgRead)
	if err != nil {
		return cerrors.Errorf("could't commit messages: %w", err)
	}
	return nil
}

func (c *segmentConsumer) StartFrom(topic string, position map[int]int64, readFromBeginning bool) error {
	return nil
}

func (c *segmentConsumer) Close() {
	if c.reader == nil {
		return
	}
	// this will also make the loops in the reader goroutines stop
	err := c.reader.Close()
	if err != nil {
		fmt.Printf("couldn't close reader: %v\n", err)
	}
}

func (c *segmentConsumer) readerID() string {
	return c.reader.Config().GroupID
}
