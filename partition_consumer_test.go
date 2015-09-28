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

	strategy := func(messages []*siesta.MessageAndMetadata, err error, consumer *PartitionConsumer) {
		assertFatal(t, err, nil)

		for i, msg := range messages {
			assert(t, string(msg.Value), fmt.Sprintf("message-%d", i))
			assert(t, msg.Topic, topic)
			assert(t, msg.Partition, partition)
			assert(t, msg.Offset, int64(i))
		}

		assertFatal(t, len(messages), 100)
		consumer.Stop()
	}

	client := NewMockClient(0, 100)
	consumer := NewPartitionConsumer(client, "group", topic, partition, strategy)

	consumer.Start()
}

func TestPartitionConsumerMultipleFetchesFromStart(t *testing.T) {
	topic := "test"
	partition := int32(0)
	expectedMessages := 512
	actualMessages := 0

	strategy := func(messages []*siesta.MessageAndMetadata, err error, consumer *PartitionConsumer) {
		assertFatal(t, err, nil)

		for _, msg := range messages {
			assert(t, string(msg.Value), fmt.Sprintf("message-%d", msg.Offset))
			assert(t, msg.Topic, topic)
			assert(t, msg.Partition, partition)
		}

		actualMessages += len(messages)

		if actualMessages == expectedMessages {
			consumer.Stop()
		}
	}

	client := NewMockClient(0, int64(expectedMessages))
	consumer := NewPartitionConsumer(client, "group", topic, partition, strategy)

	consumer.Start()
}

func TestPartitionConsumerMultipleFetches(t *testing.T) {
	topic := "test"
	partition := int32(0)
	startOffset := 1624
	expectedMessages := 512
	actualMessages := 0

	strategy := func(messages []*siesta.MessageAndMetadata, err error, consumer *PartitionConsumer) {
		assertFatal(t, err, nil)

		for _, msg := range messages {
			assert(t, string(msg.Value), fmt.Sprintf("message-%d", msg.Offset))
			assert(t, msg.Topic, topic)
			assert(t, msg.Partition, partition)
		}

		actualMessages += len(messages)

		if actualMessages == expectedMessages {
			consumer.Stop()
		}
	}

	client := NewMockClient(int64(startOffset), int64(startOffset+expectedMessages))
	consumer := NewPartitionConsumer(client, "group", topic, partition, strategy)

	consumer.Start()
}

func TestPartitionConsumerEmptyFetch(t *testing.T) {
	topic := "test"
	partition := int32(0)
	expectedMessages := 512
	actualMessages := 0

	strategy := func(messages []*siesta.MessageAndMetadata, err error, consumer *PartitionConsumer) {
		assertFatal(t, err, nil)

		for _, msg := range messages {
			assert(t, string(msg.Value), fmt.Sprintf("message-%d", msg.Offset))
			assert(t, msg.Topic, topic)
			assert(t, msg.Partition, partition)
		}

		actualMessages += len(messages)

		if actualMessages == expectedMessages {
			consumer.Stop()
		}
	}

	client := NewMockClient(0, int64(expectedMessages))
	client.emptyFetches = 2
	consumer := NewPartitionConsumer(client, "group", topic, partition, strategy)

	consumer.Start()
}

func TestPartitionConsumerFetchError(t *testing.T) {
	topic := "test"
	partition := int32(0)
	expectedMessages := 200
	actualMessages := 0

	strategy := func(messages []*siesta.MessageAndMetadata, err error, consumer *PartitionConsumer) {
		assertFatal(t, err, nil)

		for _, msg := range messages {
			assert(t, string(msg.Value), fmt.Sprintf("message-%d", msg.Offset))
		}

		actualMessages += len(messages)

		if actualMessages == expectedMessages {
			consumer.Stop()
		}
	}

	client := NewMockClient(0, int64(expectedMessages))
	client.fetchError = siesta.ErrEOF
	client.fetchErrorTimes = 1
	consumer := NewPartitionConsumer(client, "group", topic, partition, strategy)

	consumer.Start()
}

func TestPartitionConsumerFetchResponseError(t *testing.T) {
	topic := "test"
	partition := int32(0)
	expectedMessages := 200
	actualMessages := 0

	strategy := func(messages []*siesta.MessageAndMetadata, err error, consumer *PartitionConsumer) {
		assertFatal(t, err, nil)

		for _, msg := range messages {
			assert(t, string(msg.Value), fmt.Sprintf("message-%d", msg.Offset))
		}

		actualMessages += len(messages)

		if actualMessages == expectedMessages {
			consumer.Stop()
		}
	}

	client := NewMockClient(0, int64(expectedMessages))
	client.fetchError = siesta.ErrUnknownTopicOrPartition
	consumer := NewPartitionConsumer(client, "group", topic, partition, strategy)

	consumer.Start()
}

func TestPartitionConsumerGetOffsetErrors(t *testing.T) {
	topic := "test"
	partition := int32(0)
	expectedMessages := 200
	actualMessages := 0

	strategy := func(messages []*siesta.MessageAndMetadata, err error, consumer *PartitionConsumer) {
		assertFatal(t, err, nil)

		for _, msg := range messages {
			assert(t, string(msg.Value), fmt.Sprintf("message-%d", msg.Offset))
		}

		actualMessages += len(messages)

		if actualMessages == expectedMessages {
			consumer.Stop()
		}
	}

	client := NewMockClient(0, int64(expectedMessages))
	client.getOffsetError = siesta.ErrUnknownTopicOrPartition
	client.getOffsetErrorTimes = 2
	client.getAvailableOffsetError = siesta.ErrEOF
	client.getAvailableOffsetErrorTimes = 2
	consumer := NewPartitionConsumer(client, "group", topic, partition, strategy)

	consumer.Start()
}

func TestPartitionConsumerStopOnInitOffset(t *testing.T) {
	topic := "test"
	partition := int32(0)
	expectedMessages := 200

	strategy := func(messages []*siesta.MessageAndMetadata, err error, consumer *PartitionConsumer) {
		t.Fatal("Should not reach here")
	}

	client := NewMockClient(0, int64(expectedMessages))
	client.getOffsetError = siesta.ErrUnknownTopicOrPartition
	client.getOffsetErrorTimes = 3
	client.getAvailableOffsetError = siesta.ErrEOF
	client.getAvailableOffsetErrorTimes = 3
	consumer := NewPartitionConsumer(client, "group", topic, partition, strategy)

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

	strategy := func(messages []*siesta.MessageAndMetadata, err error, consumer *PartitionConsumer) {
		t.Fatal("Should not reach here")
	}

	client := NewMockClient(0, int64(expectedMessages))
	client.getOffsetError = siesta.ErrNotCoordinatorForConsumerCode
	client.getOffsetErrorTimes = 3
	client.getAvailableOffsetError = siesta.ErrEOF
	client.getAvailableOffsetErrorTimes = 3
	consumer := NewPartitionConsumer(client, "group", topic, partition, strategy)

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

	strategy := func(messages []*siesta.MessageAndMetadata, err error, consumer *PartitionConsumer) {
		assertFatal(t, err, nil)
		assert(t, consumer.Offset(), int64(startOffset+len(messages)))
		assert(t, consumer.Lag(), int64(highwaterMarkOffset-(startOffset+len(messages))))

		consumer.Stop()
	}

	client := NewMockClient(int64(startOffset), int64(highwaterMarkOffset))
	consumer := NewPartitionConsumer(client, "group", topic, partition, strategy)

	consumer.Start()
}

func TestPartitionConsumerSetOffset(t *testing.T) {
	topic := "test"
	partition := int32(0)
	startOffset := 134
	setOffsetDone := false

	strategy := func(messages []*siesta.MessageAndMetadata, err error, consumer *PartitionConsumer) {
		assertFatal(t, err, nil)

		assert(t, messages[0].Offset, int64(startOffset))

		if setOffsetDone {
			consumer.Stop()
			return
		}

		consumer.SetOffset(int64(startOffset))
		setOffsetDone = true
	}

	client := NewMockClient(int64(startOffset), int64(startOffset+100))
	consumer := NewPartitionConsumer(client, "group", topic, partition, strategy)

	consumer.Start()
}

func TestPartitionConsumerCommit(t *testing.T) {
	group := "group"
	topic := "test"
	partition := int32(0)
	startOffset := 134
	hwOffset := int64(startOffset + 100)

	strategy := func(messages []*siesta.MessageAndMetadata, err error, consumer *PartitionConsumer) {
		assertFatal(t, err, nil)

		consumer.Commit(messages[len(messages)-1].Offset)
		consumer.Stop()
	}

	client := NewMockClient(int64(startOffset), hwOffset)
	consumer := NewPartitionConsumer(client, group, topic, partition, strategy)
	client.initOffsets(group, topic, partition)
	assertFatal(t, client.offsets[group][topic][partition], int64(0))

	consumer.Start()
	assertFatal(t, client.offsets[group][topic][partition], hwOffset-1)

	assertFatal(t, client.commitCount[group][topic][partition], 1)
}
