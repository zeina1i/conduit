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
	// Important note: The Consumer's position in Kafka is the position of the NEXT message.
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

	// maps partition IDs to respective readers
	// segmentio/kafka-go requires a reader per partition
	readers map[int]*kafka.Reader
	// used for getting topic-level information
	client *kafka.Client
	// maps partition IDs to this consumer's position (offset) with the respective partition
	positions map[int]int64
	// a channel where all partition readers push messages to
	msgs chan kafka.Message
	// a channel where all partition readers push errors to
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

	return consumer, nil
}

func (c *segmentConsumer) Get() (*kafka.Message, map[int]int64, error) {
	for {
		select {
		case msg := <-c.msgs:
			// todo consider getting offsets from the readers.
			// while that would eliminate the need for a field (segmentConsumer.positions)
			// it would add an overhead to each messages being read
			// (iterate over all readers)
			return &msg, c.updatePosition(&msg), nil
		case err := <-c.errors:
			return nil, nil, cerrors.Errorf("couldn't read message: %w", err)
		}
	}
}

func (c *segmentConsumer) StartFrom(topic string, position map[int]int64, readFromBeginning bool) error {
	// readFromBeginning is relevant only when consuming messages from a topic for the first time.
	// All messages from newly created partitions are new messages, so we read all of them.
	var realPositions = copyMap(position)
	if len(realPositions) == 0 {
		fetched, err := c.fetchOffsets(topic, readFromBeginning)
		if err != nil {
			return cerrors.Errorf("couldn't fetch topic metadata: %w", err)
		}
		realPositions = fetched
	}

	c.positions = realPositions
	return c.configureReaders()
}

func copyMap(orig map[int]int64) map[int]int64 {
	var mcopy = make(map[int]int64)
	for k, v := range orig {
		mcopy[k] = v
	}
	return mcopy
}

func (c *segmentConsumer) configureReaders() error {
	partitions, err := c.countPartitions()
	if err != nil {
		return cerrors.Errorf("couldn't fetch topic metadata: %w", err)
	}
	// go through partitions which are in the topic, compare with the positions and readers we have
	for i := 0; i < partitions; i++ {
		offset, ok := c.positions[i]
		// new partition (i.e. created after the consumer was started)
		if !ok {
			offset = 0
		}
		// create reader, if it doesn't already exist
		reader, ok := c.readers[i]
		if !ok {
			reader = kafka.NewReader(kafka.ReaderConfig{
				Brokers:   c.brokers,
				Topic:     c.topic,
				Partition: i,
			})
			c.readers[i] = reader
		}
		err = reader.SetOffset(offset)
		if err != nil {
			return cerrors.Errorf("couldn't set offset for reader for %v/%v: %w", c.topic, i, err)
		}
		c.start(reader)
	}
	return nil
}

// todo make sure semantics about positions is consistent
func (c *segmentConsumer) updatePosition(msg *kafka.Message) map[int]int64 {
	if msg == nil {
		return c.positions
	}
	c.positions[msg.Partition] = msg.Offset + 1
	return c.positions
}

func (c *segmentConsumer) Close() {
	if len(c.readers) == 0 {
		return
	}
	// closing the readers will also cause the for loop in start(r *kafka.Reader) to stop
	for p, r := range c.readers {
		err := r.Close()
		if err != nil {
			fmt.Printf("couldn't close reader for partition %v due to error: %v\n", p, err)
		}
	}
}

func (c *segmentConsumer) countPartitions() (int, error) {
	req := &kafka.MetadataRequest{Topics: []string{c.topic}}
	metadata, err := c.client.Metadata(context.Background(), req)
	if err != nil {
		return 0, cerrors.Errorf("couldn't fetch topic metadata: %w", err)
	}
	return len(metadata.Topics[0].Partitions), nil
}

func (c *segmentConsumer) start(r *kafka.Reader) {
	go func() {
		for {
			m, err := r.ReadMessage(context.Background())
			// this also includes io.EOF which will be returned when the reader gets closed
			if err != nil {
				c.errors <- err
				break
			}
			c.msgs <- m
		}
	}()
}

func (c *segmentConsumer) fetchOffsets(topic string, readFromBeginning bool) (map[int]int64, error) {
	mreq := &kafka.MetadataRequest{Topics: []string{topic}}
	metadata, err := c.client.Metadata(context.Background(), mreq)
	if err != nil {
		return nil, cerrors.Errorf("couldn't fetch offsets for %v: %w", topic, err)
	}
	offsets := make(map[int]int64)
	for _, partition := range metadata.Topics[0].Partitions {
		req := &kafka.FetchRequest{Topic: topic, Partition: partition.ID, Offset: kafka.LastOffset}
		resp, err := c.client.Fetch(context.Background(), req)
		if err != nil {
			return nil, cerrors.Errorf("couldn't fetch offsets for %v/%v: %w", topic, partition.ID, err)
		}
		if readFromBeginning {
			// todo can we simply put 0 here?
			offsets[partition.ID] = resp.LogStartOffset
		} else {
			// todo HighWatermark?
			offsets[partition.ID] = resp.LastStableOffset
		}
	}

	return offsets, nil
}
