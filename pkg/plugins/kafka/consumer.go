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
	"github.com/segmentio/kafka-go"
)

type Consumer interface {
	// Get returns a message from the configured topic, waiting at most 'timeoutMs' milliseconds.
	// Returns:
	// A message and the client's 'position' in Kafka, if there's no error, OR
	// A nil message, the client's position in Kafka, and a nil error,
	// if no message was retrieved within the specified timeout, OR
	// A nil message, nil position and an error if there was an error while retrieving the message (e.g. broker down).
	Get() (*kafka.Message, map[int]int64, error)

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
	brokers []string
	topic   string

	// readers is a map of IDs of partition to respective readers
	// segmentio/kafka-go requires a reader per partition
	readers map[int]*kafka.Reader
	// client is used for getting topic-level information
	client *kafka.Client
	// positions maps partition IDs to this consumer's position (offset) with the respective partition
	positions map[int]int64
	// msgs is a channel where all partition readers push messages to
	msgs chan kafka.Message
	// errors is a channel where all partition readers push errors to
	errors chan error
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
	c := &kafka.Client{Addr: kafka.TCP(config.Servers...)}

	consumer := &segmentConsumer{
		brokers:   config.Servers,
		topic:     config.Topic,
		readers:   make(map[int]*kafka.Reader),
		client:    c,
		positions: map[int]int64{},
		msgs:      make(chan kafka.Message),
		errors:    make(chan error),
	}
	err := consumer.refreshReaders()
	if err != nil {
		return nil, cerrors.Errorf("couldn't refresh readers: %w", err)
	}
	return consumer, nil
}

func (c *segmentConsumer) Get() (*kafka.Message, map[int]int64, error) {
	for {
		select {
		case msg := <-c.msgs:
			return &msg, c.updatePosition(&msg), nil
		case err := <-c.errors:
			return nil, nil, cerrors.Errorf("couldn't read message: %w", err)
		}
	}
}

func (c *segmentConsumer) StartFrom(topic string, position map[int]int64, readFromBeginning bool) error {
	return nil
}

func (c *segmentConsumer) updatePosition(msg *kafka.Message) map[int]int64 {
	if msg == nil {
		return c.positions
	}
	c.positions[msg.Partition] = msg.Offset + 1
	return c.positions
}

func (c *segmentConsumer) Close() {
	// todo also make sure reader goroutines are stopped
	if len(c.readers) == 0 {
		return
	}
	for p, r := range c.readers {
		err := r.Close()
		if err != nil {
			fmt.Printf("couldn't close reader for partition %v due to error: %v\n", p, err)
		}
	}
}

// refreshReaders is creating a kafka.Reader for each partition, for which no reader exists yet.
// todo call it periodically, since partitions can be added to a topic
func (c *segmentConsumer) refreshReaders() error {
	req := &kafka.MetadataRequest{Topics: []string{c.topic}}
	metadata, err := c.client.Metadata(context.Background(), req)
	if err != nil {
		return cerrors.Errorf("couldn't fetch topic metadata: %w", err)
	}
	for _, partition := range metadata.Topics[0].Partitions {
		// reader for this partition already exists
		if _, ok := c.readers[partition.ID]; ok {
			continue
		}
		// NB: NewReader will panic if the ReaderConfig is invalid.
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:   c.brokers,
			Topic:     c.topic,
			Partition: partition.ID,
		})
		c.start(reader)
		c.readers[partition.ID] = reader
	}
	return nil
}

func (c *segmentConsumer) start(r *kafka.Reader) {
	go func() {
		for {
			m, err := r.ReadMessage(context.Background())
			if err != nil {
				c.errors <- err
				break
			}
			c.msgs <- m
		}
	}()
}
