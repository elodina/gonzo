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
	"github.com/stealthly/siesta"
	"testing"
	"time"
)

func TestPartitionConsumerSingleFetch(t *testing.T) {
	topic := "test"
	partition := int32(0)

	strategy := func(data *FetchData, consumer *PartitionConsumer) {
		assertFatal(t, data.Error, nil)

		for i, msg := range data.Messages {
			assert(t, string(msg.Value), fmt.Sprintf("message-%d", i))
			assert(t, msg.Topic, topic)
			assert(t, msg.Partition, partition)
			assert(t, msg.Offset, int64(i))
		}

		assertFatal(t, len(data.Messages), 100)
		consumer.Stop()
	}

	client := NewMockClient(0, 100)
	consumer := NewPartitionConsumer(client, NewConsumerConfig(), topic, partition, strategy)

	consumer.Start()
}

func TestPartitionConsumerMultipleFetchesFromStart(t *testing.T) {
	topic := "test"
	partition := int32(0)
	expectedMessages := 512
	actualMessages := 0

	strategy := func(data *FetchData, consumer *PartitionConsumer) {
		assertFatal(t, data.Error, nil)

		for _, msg := range data.Messages {
			assert(t, string(msg.Value), fmt.Sprintf("message-%d", msg.Offset))
			assert(t, msg.Topic, topic)
			assert(t, msg.Partition, partition)
		}

		actualMessages += len(data.Messages)

		if actualMessages == expectedMessages {
			consumer.Stop()
		}
	}

	client := NewMockClient(0, int64(expectedMessages))
	consumer := NewPartitionConsumer(client, NewConsumerConfig(), topic, partition, strategy)

	consumer.Start()
}

func TestPartitionConsumerMultipleFetches(t *testing.T) {
	topic := "test"
	partition := int32(0)
	startOffset := 1624
	expectedMessages := 512
	actualMessages := 0

	strategy := func(data *FetchData, consumer *PartitionConsumer) {
		assertFatal(t, data.Error, nil)

		for _, msg := range data.Messages {
			assert(t, string(msg.Value), fmt.Sprintf("message-%d", msg.Offset))
			assert(t, msg.Topic, topic)
			assert(t, msg.Partition, partition)
		}

		actualMessages += len(data.Messages)

		if actualMessages == expectedMessages {
			consumer.Stop()
		}
	}

	client := NewMockClient(int64(startOffset), int64(startOffset+expectedMessages))
	consumer := NewPartitionConsumer(client, NewConsumerConfig(), topic, partition, strategy)

	consumer.Start()
}

func TestPartitionConsumerEmptyFetch(t *testing.T) {
	topic := "test"
	partition := int32(0)
	expectedMessages := 512
	actualMessages := 0

	strategy := func(data *FetchData, consumer *PartitionConsumer) {
		assertFatal(t, data.Error, nil)

		for _, msg := range data.Messages {
			assert(t, string(msg.Value), fmt.Sprintf("message-%d", msg.Offset))
			assert(t, msg.Topic, topic)
			assert(t, msg.Partition, partition)
		}

		actualMessages += len(data.Messages)

		if actualMessages == expectedMessages {
			consumer.Stop()
		}
	}

	client := NewMockClient(0, int64(expectedMessages))
	client.emptyFetches = 2
	consumer := NewPartitionConsumer(client, NewConsumerConfig(), topic, partition, strategy)

	consumer.Start()
}

func TestPartitionConsumerFetchError(t *testing.T) {
	strategy := func(data *FetchData, consumer *PartitionConsumer) {
		assertFatal(t, data.Error, siesta.ErrEOF)

		consumer.Stop()
	}

	client := NewMockClient(0, 200)
	client.fetchError = siesta.ErrEOF
	client.fetchErrorTimes = 1
	consumer := NewPartitionConsumer(client, NewConsumerConfig(), "test", 0, strategy)

	consumer.Start()
}

func TestPartitionConsumerFetchResponseError(t *testing.T) {
	topic := "test"
	partition := int32(0)
	expectedMessages := 200
	actualMessages := 0

	strategy := func(data *FetchData, consumer *PartitionConsumer) {
		assertFatal(t, data.Error, nil)

		for _, msg := range data.Messages {
			assert(t, string(msg.Value), fmt.Sprintf("message-%d", msg.Offset))
		}

		actualMessages += len(data.Messages)

		if actualMessages == expectedMessages {
			consumer.Stop()
		}
	}

	client := NewMockClient(0, int64(expectedMessages))
	client.fetchError = siesta.ErrUnknownTopicOrPartition
	consumer := NewPartitionConsumer(client, NewConsumerConfig(), topic, partition, strategy)

	consumer.Start()
}

func TestPartitionConsumerGetOffsetErrors(t *testing.T) {
	topic := "test"
	partition := int32(0)
	expectedMessages := 200
	actualMessages := 0

	strategy := func(data *FetchData, consumer *PartitionConsumer) {
		assertFatal(t, data.Error, nil)

		for _, msg := range data.Messages {
			assert(t, string(msg.Value), fmt.Sprintf("message-%d", msg.Offset))
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
	consumer := NewPartitionConsumer(client, NewConsumerConfig(), topic, partition, strategy)

	consumer.Start()
}

func TestPartitionConsumerStopOnInitOffset(t *testing.T) {
	topic := "test"
	partition := int32(0)
	expectedMessages := 200

	strategy := func(data *FetchData, consumer *PartitionConsumer) {
		t.Fatal("Should not reach here")
	}

	client := NewMockClient(0, int64(expectedMessages))
	client.getOffsetError = siesta.ErrUnknownTopicOrPartition
	client.getOffsetErrorTimes = 3
	client.getAvailableOffsetError = siesta.ErrEOF
	client.getAvailableOffsetErrorTimes = 3
	consumer := NewPartitionConsumer(client, NewConsumerConfig(), topic, partition, strategy)

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

	strategy := func(data *FetchData, consumer *PartitionConsumer) {
		t.Fatal("Should not reach here")
	}

	client := NewMockClient(0, int64(expectedMessages))
	client.getOffsetError = siesta.ErrNotCoordinatorForConsumerCode
	client.getOffsetErrorTimes = 3
	client.getAvailableOffsetError = siesta.ErrEOF
	client.getAvailableOffsetErrorTimes = 3
	consumer := NewPartitionConsumer(client, NewConsumerConfig(), topic, partition, strategy)

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

	strategy := func(data *FetchData, consumer *PartitionConsumer) {
		assertFatal(t, data.Error, nil)
		assert(t, consumer.Offset(), int64(startOffset+len(data.Messages)))
		assert(t, consumer.Lag(), int64(highwaterMarkOffset-(startOffset+len(data.Messages))))

		consumer.Stop()
	}

	client := NewMockClient(int64(startOffset), int64(highwaterMarkOffset))
	consumer := NewPartitionConsumer(client, NewConsumerConfig(), topic, partition, strategy)

	consumer.Start()
}

func TestPartitionConsumerSetOffset(t *testing.T) {
	topic := "test"
	partition := int32(0)
	startOffset := 134
	setOffsetDone := false

	strategy := func(data *FetchData, consumer *PartitionConsumer) {
		assertFatal(t, data.Error, nil)

		assert(t, data.Messages[0].Offset, int64(startOffset))

		if setOffsetDone {
			consumer.Stop()
			return
		}

		consumer.SetOffset(int64(startOffset))
		setOffsetDone = true
	}

	client := NewMockClient(int64(startOffset), int64(startOffset+100))
	consumer := NewPartitionConsumer(client, NewConsumerConfig(), topic, partition, strategy)

	consumer.Start()
}

func TestPartitionConsumerCommit(t *testing.T) {
	config := NewConsumerConfig()
	topic := "test"
	partition := int32(0)
	startOffset := 134
	hwOffset := int64(startOffset + 100)

	strategy := func(data *FetchData, consumer *PartitionConsumer) {
		assertFatal(t, data.Error, nil)

		consumer.Commit(data.Messages[len(data.Messages)-1].Offset)
		consumer.Stop()
	}

	client := NewMockClient(int64(startOffset), hwOffset)
	consumer := NewPartitionConsumer(client, config, topic, partition, strategy)
	client.initOffsets(config.Group, topic, partition)
	assertFatal(t, client.offsets[config.Group][topic][partition], int64(0))

	consumer.Start()
	assertFatal(t, client.offsets[config.Group][topic][partition], hwOffset-1)

	assertFatal(t, client.commitCount[config.Group][topic][partition], 1)
}
