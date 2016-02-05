/* Licensed to Elodina Inc. under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. */

package gonzo

import (
	"fmt"
	"github.com/elodina/siesta"
	"github.com/rcrowley/go-metrics"
	"gopkg.in/stretchr/testify.v1/assert"
	"testing"
	"time"
)

type MockPartitionConsumer struct {
	offset int64
	lag    int64
}

func NewMockPartitionConsumer(client Client, config *ConsumerConfig, topic string, partition int32, strategy Strategy) PartitionConsumer {
	return new(MockPartitionConsumer)
}

func (mpc *MockPartitionConsumer) Start() {
	Logger.Info("MockPartitionConsumer.Start()")
}

func (mpc *MockPartitionConsumer) Stop() {
	Logger.Info("MockPartitionConsumer.Stop()")
}

func (mpc *MockPartitionConsumer) Offset() int64 {
	Logger.Info("MockPartitionConsumer.Offset()")
	return mpc.offset
}

func (mpc *MockPartitionConsumer) Commit(offset int64) error {
	Logger.Info("MockPartitionConsumer.Commit()")
	return nil
}

func (mpc *MockPartitionConsumer) SetOffset(offset int64) {
	Logger.Info("MockPartitionConsumer.SetOffset()")
	mpc.offset = offset
}

func (mpc *MockPartitionConsumer) Lag() int64 {
	Logger.Info("MockPartitionConsumer.Lag()")
	return mpc.lag
}

func (mpc *MockPartitionConsumer) Metrics() (metrics.Registry, error) {
	Logger.Info("MockPartitionConsumer.Metrics()")
	return nil, nil
}

func TestPartitionConsumerSingleFetch(t *testing.T) {
	topic := "test"
	partition := int32(0)

	strategy := func(data *FetchData, consumer *KafkaPartitionConsumer) {
		assert.Equal(t, nil, data.Error)

		for i, msg := range data.Messages {
			assert.Equal(t, fmt.Sprintf("message-%d", i), string(msg.Value))
			assert.Equal(t, topic, msg.Topic)
			assert.Equal(t, partition, msg.Partition)
			assert.Equal(t, int64(i), msg.Offset)
		}

		assert.Len(t, data.Messages, 100)
		consumer.Stop()
	}

	client := NewMockClient(0, 100)
	consumer := NewPartitionConsumer(client, testConsumerConfig(), topic, partition, strategy)

	consumer.Start()
}

func TestPartitionConsumerMultipleFetchesFromStart(t *testing.T) {
	topic := "test"
	partition := int32(0)
	expectedMessages := 512
	actualMessages := 0

	strategy := func(data *FetchData, consumer *KafkaPartitionConsumer) {
		assert.Equal(t, nil, data.Error)

		for _, msg := range data.Messages {
			assert.Equal(t, fmt.Sprintf("message-%d", msg.Offset), string(msg.Value))
			assert.Equal(t, topic, msg.Topic)
			assert.Equal(t, partition, msg.Partition)
		}

		actualMessages += len(data.Messages)

		if actualMessages == expectedMessages {
			consumer.Stop()
		}
	}

	client := NewMockClient(0, int64(expectedMessages))
	consumer := NewPartitionConsumer(client, testConsumerConfig(), topic, partition, strategy)

	consumer.Start()
}

func TestPartitionConsumerMultipleFetches(t *testing.T) {
	topic := "test"
	partition := int32(0)
	startOffset := 1624
	expectedMessages := 512
	actualMessages := 0

	strategy := func(data *FetchData, consumer *KafkaPartitionConsumer) {
		assert.Equal(t, nil, data.Error)

		for _, msg := range data.Messages {
			assert.Equal(t, fmt.Sprintf("message-%d", msg.Offset), string(msg.Value))
			assert.Equal(t, topic, msg.Topic)
			assert.Equal(t, partition, msg.Partition)
		}

		actualMessages += len(data.Messages)

		if actualMessages == expectedMessages {
			consumer.Stop()
		}
	}

	client := NewMockClient(int64(startOffset), int64(startOffset+expectedMessages))
	consumer := NewPartitionConsumer(client, testConsumerConfig(), topic, partition, strategy)

	consumer.Start()
}

func TestPartitionConsumerEmptyFetch(t *testing.T) {
	topic := "test"
	partition := int32(0)
	expectedMessages := 512
	actualMessages := 0

	strategy := func(data *FetchData, consumer *KafkaPartitionConsumer) {
		assert.Equal(t, nil, data.Error)

		for _, msg := range data.Messages {
			assert.Equal(t, fmt.Sprintf("message-%d", msg.Offset), string(msg.Value))
			assert.Equal(t, topic, msg.Topic)
			assert.Equal(t, partition, msg.Partition)
		}

		actualMessages += len(data.Messages)

		if actualMessages == expectedMessages {
			consumer.Stop()
		}
	}

	client := NewMockClient(0, int64(expectedMessages))
	client.emptyFetches = 2
	consumer := NewPartitionConsumer(client, testConsumerConfig(), topic, partition, strategy)

	consumer.Start()
}

func TestPartitionConsumerFetchError(t *testing.T) {
	strategy := func(data *FetchData, consumer *KafkaPartitionConsumer) {
		assert.Equal(t, siesta.ErrEOF, data.Error)

		consumer.Stop()
	}

	client := NewMockClient(0, 200)
	client.fetchError = siesta.ErrEOF
	client.fetchErrorTimes = 1
	consumer := NewPartitionConsumer(client, testConsumerConfig(), "test", 0, strategy)

	consumer.Start()
}

func TestPartitionConsumerFetchResponseError(t *testing.T) {
	topic := "test"
	partition := int32(0)
	expectedMessages := 200
	actualMessages := 0

	strategy := func(data *FetchData, consumer *KafkaPartitionConsumer) {
		assert.Equal(t, nil, data.Error)

		for _, msg := range data.Messages {
			assert.Equal(t, fmt.Sprintf("message-%d", msg.Offset), string(msg.Value))
		}

		actualMessages += len(data.Messages)

		if actualMessages == expectedMessages {
			consumer.Stop()
		}
	}

	client := NewMockClient(0, int64(expectedMessages))
	client.fetchError = siesta.ErrUnknownTopicOrPartition
	consumer := NewPartitionConsumer(client, testConsumerConfig(), topic, partition, strategy)

	consumer.Start()
}

func TestPartitionConsumerGetOffsetErrors(t *testing.T) {
	topic := "test"
	partition := int32(0)
	expectedMessages := 200
	actualMessages := 0

	strategy := func(data *FetchData, consumer *KafkaPartitionConsumer) {
		assert.Equal(t, nil, data.Error)

		for _, msg := range data.Messages {
			assert.Equal(t, fmt.Sprintf("message-%d", msg.Offset), string(msg.Value))
		}

		actualMessages += len(data.Messages)

		if actualMessages == expectedMessages {
			consumer.Stop()
		}
	}

	client := NewMockClient(0, int64(expectedMessages))
	client.getOffsetError = siesta.ErrUnknownTopicOrPartition
	client.getOffsetErrorTimes = 2
	client.getAvailableOffsetError = siesta.ErrEOF
	client.getAvailableOffsetErrorTimes = 2
	consumer := NewPartitionConsumer(client, testConsumerConfig(), topic, partition, strategy)

	consumer.Start()
}

func TestPartitionConsumerStopOnInitOffset(t *testing.T) {
	topic := "test"
	partition := int32(0)
	expectedMessages := 200

	strategy := func(data *FetchData, consumer *KafkaPartitionConsumer) {
		t.Fatal("Should not reach here")
	}

	client := NewMockClient(0, int64(expectedMessages))
	client.getOffsetError = siesta.ErrUnknownTopicOrPartition
	client.getOffsetErrorTimes = 3
	client.getAvailableOffsetError = siesta.ErrEOF
	client.getAvailableOffsetErrorTimes = 3
	consumer := NewPartitionConsumer(client, testConsumerConfig(), topic, partition, strategy)

	go func() {
		time.Sleep(1 * time.Second)
		consumer.Stop()
	}()
	consumer.Start()
}

func TestPartitionConsumerStopOnOffsetReset(t *testing.T) {
	topic := "test"
	partition := int32(0)
	expectedMessages := 200

	strategy := func(data *FetchData, consumer *KafkaPartitionConsumer) {
		t.Fatal("Should not reach here")
	}

	client := NewMockClient(0, int64(expectedMessages))
	client.getOffsetError = siesta.ErrNotCoordinatorForConsumerCode
	client.getOffsetErrorTimes = 3
	client.getAvailableOffsetError = siesta.ErrEOF
	client.getAvailableOffsetErrorTimes = 3
	consumer := NewPartitionConsumer(client, testConsumerConfig(), topic, partition, strategy)

	go func() {
		time.Sleep(1 * time.Second)
		consumer.Stop()
	}()
	consumer.Start()
}

func TestPartitionConsumerOffsetAndLag(t *testing.T) {
	topic := "test"
	partition := int32(0)
	startOffset := 134
	highwaterMarkOffset := 17236

	strategy := func(data *FetchData, consumer *KafkaPartitionConsumer) {
		assert.Equal(t, nil, data.Error)
		assert.Equal(t, int64(startOffset+len(data.Messages)), consumer.Offset())
		assert.Equal(t, int64(highwaterMarkOffset-(startOffset+len(data.Messages))), consumer.Lag())

		consumer.Stop()
	}

	client := NewMockClient(int64(startOffset), int64(highwaterMarkOffset))
	consumer := NewPartitionConsumer(client, testConsumerConfig(), topic, partition, strategy)

	consumer.Start()
}

func TestPartitionConsumerSetOffset(t *testing.T) {
	topic := "test"
	partition := int32(0)
	startOffset := 134
	setOffsetDone := false

	strategy := func(data *FetchData, consumer *KafkaPartitionConsumer) {
		assert.Equal(t, nil, data.Error)

		assert.Equal(t, int64(startOffset), data.Messages[0].Offset)

		if setOffsetDone {
			consumer.Stop()
			return
		}

		consumer.SetOffset(int64(startOffset))
		setOffsetDone = true
	}

	client := NewMockClient(int64(startOffset), int64(startOffset+100))
	consumer := NewPartitionConsumer(client, testConsumerConfig(), topic, partition, strategy)

	consumer.Start()
}

func TestPartitionConsumerCommit(t *testing.T) {
	config := testConsumerConfig()
	topic := "test"
	partition := int32(0)
	startOffset := 134
	hwOffset := int64(startOffset + 100)

	strategy := func(data *FetchData, consumer *KafkaPartitionConsumer) {
		assert.Equal(t, nil, data.Error)

		consumer.Commit(data.Messages[len(data.Messages)-1].Offset)
		consumer.Stop()
	}

	client := NewMockClient(int64(startOffset), hwOffset)
	consumer := NewPartitionConsumer(client, config, topic, partition, strategy)
	client.initOffsets(config.Group, topic, partition)
	assert.Equal(t, int64(0), client.offsets[config.Group][topic][partition])

	consumer.Start()
	assert.Equal(t, hwOffset-1, client.offsets[config.Group][topic][partition])

	assert.Equal(t, 1, client.commitCount[config.Group][topic][partition])
}

func TestPartitionConsumerAutoCommit(t *testing.T) {
	config := testConsumerConfig()
	config.AutoCommitEnable = true
	topic := "test"
	partition := int32(0)
	startOffset := 134
	hwOffset := int64(startOffset + 100)

	strategy := func(data *FetchData, consumer *KafkaPartitionConsumer) {
		assert.Equal(t, nil, data.Error)

		consumer.Stop()
	}

	client := NewMockClient(int64(startOffset), hwOffset)
	consumer := NewPartitionConsumer(client, config, topic, partition, strategy)
	client.initOffsets(config.Group, topic, partition)
	assert.Equal(t, int64(0), client.offsets[config.Group][topic][partition])

	consumer.Start()
	assert.Equal(t, hwOffset-1, client.offsets[config.Group][topic][partition])

	assert.Equal(t, 1, client.commitCount[config.Group][topic][partition])
}

func testConsumerConfig() *ConsumerConfig {
	config := NewConsumerConfig()
	config.EnableMetrics = true
	return config
}
