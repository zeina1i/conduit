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

//go:build integration

package kafka

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/conduitio/conduit/pkg/foundation/assert"
	"github.com/conduitio/conduit/pkg/foundation/cerrors"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

type staticBalancer struct {
	partition int
}

func (s staticBalancer) Balance(msg kafka.Message, partitions ...int) int {
	return s.partition
}

func TestConfluentClient_StartFrom_EmptyPosition(t *testing.T) {
	t.Parallel()

	cfg := Config{Topic: "TestConfluentClient_" + uuid.NewString(), Servers: []string{"localhost:9092"}}
	createTopic(t, cfg, 1)

	consumer, err := NewConsumer(cfg)
	assert.Ok(t, err)

	err = consumer.StartFrom(cfg.Topic, map[int]int64{}, true)
	defer consumer.Close()
	assert.Ok(t, err)
}

func TestConfluentClient_StartFrom_FromBeginning(t *testing.T) {
	t.Parallel()

	cfg := Config{
		Topic:             "TestConfluentClient_" + uuid.NewString(),
		Servers:           []string{"localhost:9092"},
		ReadFromBeginning: true,
	}
	// other two partitions should be consumed from beginning
	positions := map[int]int64{0: 1}

	partitions := 3
	createTopic(t, cfg, partitions)

	sendTestMessages(t, cfg, partitions)

	consumer, err := NewConsumer(cfg)
	defer consumer.Close()
	assert.Ok(t, err)

	err = consumer.StartFrom(cfg.Topic, positions, cfg.ReadFromBeginning)
	assert.Ok(t, err)

	// 1 message from first partition
	// +4 messages from 2 partitions which need to be read fully
	messagesUnseen := map[string]bool{
		"test-key-1": true,
		"test-key-2": true,
		"test-key-4": true,
		"test-key-5": true,
		"test-key-6": true,
	}
	var updatedPos map[int]int64
	for i := 1; i <= 5; i++ {
		message, pos, err := consumer.Get()
		updatedPos = pos
		assert.NotNil(t, message)
		assert.Ok(t, err)
		delete(messagesUnseen, string(message.Key))
	}
	assert.Equal(t, 0, len(messagesUnseen))

	time.AfterFunc(100*time.Millisecond, func() { consumer.Close() })
	message, _, err := consumer.Get()
	assert.True(t, cerrors.Is(err, io.EOF), "expected io.EOF")
	assert.Nil(t, message)
	assert.Equal(
		t,
		map[int]int64{0: 2, 1: 2, 2: 2},
		updatedPos,
	)
}

func TestConfluentClient_StartFrom_OnlyNew(t *testing.T) {
	cfg := Config{
		Topic:             "TestConfluentClient_" + uuid.NewString(),
		Servers:           []string{"localhost:9092"},
		ReadFromBeginning: false,
	}
	positions := map[int]int64{0: 1, 1: 2, 2: 2}
	partitions := 3
	createTopic(t, cfg, partitions)

	sendTestMessages(t, cfg, partitions)

	consumer, err := NewConsumer(cfg)
	defer consumer.Close()
	assert.Ok(t, err)

	err = consumer.StartFrom(cfg.Topic, positions, cfg.ReadFromBeginning)
	assert.Ok(t, err)

	message, updatedPos, err := consumer.Get()
	assert.NotNil(t, message)
	assert.Ok(t, err)
	assert.Equal(t, "test-key-6", string(message.Key))
	assert.Equal(t, "test-payload-6", string(message.Value))

	time.AfterFunc(100*time.Millisecond, func() { consumer.Close() })
	message, _, err = consumer.Get()
	assert.True(t, cerrors.Is(err, io.EOF), "expected io.EOF")
	assert.Nil(t, message)
	assert.Equal(
		t,
		map[int]int64{0: 2, 1: 2, 2: 2},
		updatedPos,
	)
}

// partition 0 has messages: 3 and 6
// partition 1 has messages: 1 and 4
// partition 2 has messages: 2 and 5
func sendTestMessages(t *testing.T, cfg Config, partitions int) {
	writers := createWriters(t, cfg, 0, 1, 2)
	for _, w := range writers {
		defer w.Close()
	}

	for i := 1; i <= 6; i++ {
		writer := writers[i%partitions]
		err := sendTestMessage(
			&writer,
			fmt.Sprintf("test-key-%d", i),
			fmt.Sprintf("test-payload-%d", i),
		)
		assert.Ok(t, err)
	}
}

func createWriters(t *testing.T, cfg Config, partitions ...int) map[int]kafka.Writer {
	writers := make(map[int]kafka.Writer)
	for _, partition := range partitions {
		writer := kafka.Writer{
			Addr:         kafka.TCP(cfg.Servers...),
			Topic:        cfg.Topic,
			BatchSize:    1,
			BatchTimeout: 10 * time.Millisecond,
			WriteTimeout: cfg.DeliveryTimeout,
			RequiredAcks: cfg.Acks,
			MaxAttempts:  2,
			Balancer:     staticBalancer{partition: partition},
		}
		writers[partition] = writer
	}
	return writers
}

func sendTestMessage(writer *kafka.Writer, key string, payload string) error {
	return writer.WriteMessages(
		context.Background(),
		kafka.Message{
			Key:   []byte(key),
			Value: []byte(payload),
		},
	)
}

func TestGet_KafkaDown(t *testing.T) {
	t.Parallel()

	cfg := Config{Topic: "client_integration_test_topic", Servers: []string{"localhost:12345"}}
	consumer, err := NewConsumer(cfg)
	assert.Ok(t, err)

	err = consumer.StartFrom(cfg.Topic, map[int]int64{0: 123}, true)
	assert.Error(t, err)
	var expErr *net.OpError
	assert.True(t, cerrors.As(err, &expErr), "expected net.OpError")
}

func createTopic(t *testing.T, cfg Config, partitions int) {
	c, err := kafka.Dial("tcp", cfg.Servers[0])
	assert.Ok(t, err)
	defer c.Close()

	kt := kafka.TopicConfig{Topic: cfg.Topic, NumPartitions: partitions, ReplicationFactor: 1}
	err = c.CreateTopics(kt)
	assert.Ok(t, err)
}
