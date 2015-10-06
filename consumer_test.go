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
	"strings"
	"testing"
	"time"
)

var NoOpStrategy = func(data *FetchData, consumer *PartitionConsumer) {}

func TestConsumerAssignments(t *testing.T) {
	client := NewMockClient(0, 0)
	consumer := NewConsumer(client, NewConsumerConfig(), NoOpStrategy)
	consumer.partitionConsumerFactory = NewMockPartitionConsumer

	// no assignments
	assignments := consumer.Assignment()
	assertFatal(t, len(assignments), 0)

	// add one
	err := consumer.Add("test", 0)
	assertFatal(t, err, nil)
	assignments = consumer.Assignment()
	_, exists := assignments["test"]
	assertFatal(t, exists, true)
	assertFatal(t, len(assignments["test"]), 1)
	assertFatal(t, assignments["test"][0], int32(0))

	// add existing
	err = consumer.Add("test", 0)
	assertNot(t, err, nil)
	assignments = consumer.Assignment()
	assertFatal(t, len(assignments), 1)

	// add another
	err = consumer.Add("test1", 1)
	assertFatal(t, err, nil)
	assignments = consumer.Assignment()
	_, exists = assignments["test1"]
	assertFatal(t, exists, true)
	assertFatal(t, len(assignments["test1"]), 1)
	assertFatal(t, assignments["test1"][0], int32(1))

	assertFatal(t, len(assignments), 2)

	// remove one
	err = consumer.Remove("test", 0)
	assertFatal(t, err, nil)
	assignments = consumer.Assignment()
	assertFatal(t, len(assignments), 1)

	// remove non existing
	err = consumer.Remove("test", 0)
	assertNot(t, err, nil)
	assignments = consumer.Assignment()
	assertFatal(t, len(assignments), 1)

	// remove one that never existed
	err = consumer.Remove("asdasd", 32)
	assertNot(t, err, nil)
	assignments = consumer.Assignment()
	assertFatal(t, len(assignments), 1)

	// remove last
	err = consumer.Remove("test1", 1)
	assertFatal(t, err, nil)
	assignments = consumer.Assignment()
	assertFatal(t, len(assignments), 0)
}

func TestConsumerOffset(t *testing.T) {
	client := NewMockClient(0, 0)
	consumer := NewConsumer(client, NewConsumerConfig(), NoOpStrategy)
	consumer.partitionConsumerFactory = NewMockPartitionConsumer

	// offset for non-existing
	offset, err := consumer.Offset("asd", 0)
	assert(t, offset, int64(-1))
	assertNot(t, err, nil)
	if !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("Error message should contain 'does not exist' text")
	}

	// add partition consumer and ensure it has offset 0 and no error
	err = consumer.Add("test", 0)
	assertFatal(t, err, nil)
	offset, err = consumer.Offset("test", 0)
	assert(t, offset, int64(0))
	assert(t, err, nil)

	// move offset and ensure Offset returns this value
	expectedOffset := int64(123)
	consumer.partitionConsumers["test"][0].(*MockPartitionConsumer).offset = expectedOffset
	offset, err = consumer.Offset("test", 0)
	assert(t, offset, expectedOffset)
	assert(t, err, nil)

	// offset for existing topic but non-existing partition should return error
	offset, err = consumer.Offset("test", 1)
	assert(t, offset, int64(-1))
	assertNot(t, err, nil)
	if !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("Error message should contain 'does not exist' text")
	}
}

func TestConsumerCommitOffset(t *testing.T) {
	client := NewMockClient(0, 0)
	config := NewConsumerConfig()
	consumer := NewConsumer(client, config, NoOpStrategy)
	consumer.partitionConsumerFactory = NewMockPartitionConsumer

	err := consumer.Commit("asd", 0, 123)
	assert(t, err, nil)
	assert(t, client.commitCount[config.Group]["asd"][0], 1) //expect one commit
	assert(t, client.offsets[config.Group]["asd"][0], int64(123))
}

func TestConsumerSetOffset(t *testing.T) {
	client := NewMockClient(0, 0)
	consumer := NewConsumer(client, NewConsumerConfig(), NoOpStrategy)
	consumer.partitionConsumerFactory = NewMockPartitionConsumer

	// set for non-existing
	err := consumer.SetOffset("asd", 0, 123)
	if !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("Error message should contain 'does not exist' text")
	}

	topic := "test"
	partition := int32(0)
	offset := int64(-1)
	seekOffset := int64(123)

	// add a topic-partition and make sure SetOffset overrides offset
	err = consumer.Add(topic, partition)
	assertFatal(t, err, nil)
	offset, err = consumer.Offset(topic, partition)
	assert(t, offset, int64(0))
	assert(t, err, nil)

	err = consumer.SetOffset(topic, partition, seekOffset)
	assert(t, err, nil)

	offset, err = consumer.Offset(topic, partition)
	assert(t, offset, seekOffset)
	assert(t, err, nil)
}

func TestConsumerLag(t *testing.T) {
	client := NewMockClient(0, 0)
	consumer := NewConsumer(client, NewConsumerConfig(), NoOpStrategy)
	consumer.partitionConsumerFactory = NewMockPartitionConsumer

	// lag for non-existing
	lag, err := consumer.Lag("asd", 0)
	assert(t, lag, int64(-1))
	if !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("Error message should contain 'does not exist' text")
	}

	topic := "test"
	partition := int32(0)

	err = consumer.Add(topic, partition)
	assertFatal(t, err, nil)
	lag, err = consumer.Lag(topic, partition)
	assert(t, lag, int64(0))
	assert(t, err, nil)

	// move lag value and ensure Lag returns this value
	expectedLag := int64(123)
	consumer.partitionConsumers[topic][partition].(*MockPartitionConsumer).lag = expectedLag
	lag, err = consumer.Lag(topic, partition)
	assert(t, lag, expectedLag)
	assert(t, err, nil)
}

func TestConsumerAwaitTermination(t *testing.T) {
	timeout := time.Second
	client := NewMockClient(0, 0)
	consumer := NewConsumer(client, NewConsumerConfig(), NoOpStrategy)
	consumer.partitionConsumerFactory = NewMockPartitionConsumer

	success := make(chan struct{})
	go func() {
		consumer.AwaitTermination()
		success <- struct{}{}
	}()

	err := consumer.Add("test", 0)
	assertFatal(t, err, nil)
	consumer.Stop()
	select {
	case <-success:
	case <-time.After(timeout):
		t.Fatalf("Await termination failed to unblock within %s", timeout)
	}
}

func TestConsumerJoin(t *testing.T) {
	timeout := time.Second
	topic := "test"
	partition := int32(0)

	client := NewMockClient(0, 0)
	consumer := NewConsumer(client, NewConsumerConfig(), NoOpStrategy)
	consumer.partitionConsumerFactory = NewMockPartitionConsumer

	success := make(chan struct{})

	go func() {
		consumer.Join() //should exit immediately as no topic/partitions are being consumed
		success <- struct{}{}
	}()

	select {
	case <-success:
	case <-time.After(timeout):
		t.Fatalf("Join failed to unblock within %s", timeout)
	}

	// add one topic/partition and make sure Join does not unblock before we need it to
	err := consumer.Add(topic, partition)
	assertFatal(t, err, nil)

	go func() {
		consumer.Join()
		success <- struct{}{}
	}()

	select {
	case <-success:
		t.Fatalf("Join unblocked while it shouldn't")
	case <-time.After(timeout):
	}

	//now remove topic-partition and make sure Join now unblocks fine
	err = consumer.Remove(topic, partition)
	assertFatal(t, err, nil)

	select {
	case <-success:
	case <-time.After(timeout):
		t.Fatalf("Join failed to unblock within %s", timeout)
	}
}
