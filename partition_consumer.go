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
	"github.com/stealthly/siesta"
	"sync/atomic"
	"time"
)

// PartitionConsumerInterface is an interface responsible for consuming exactly one topic/partition
// from Kafka. Used to switch between PartitionConsumer in live mode and MockPartitionConsumer in tests.
type PartitionConsumer interface {
	// Start starts consuming given topic/partition.
	Start()

	// Stop stops consuming given topic/partition.
	Stop()

	// Offset returns the last fetched offset for this partition consumer.
	Offset() int64

	// Commit commits the given offset to Kafka. Returns an error on unsuccessful commit.
	Commit(offset int64) error

	// SetOffset overrides the current fetch offset value for given topic/partition.
	// This does not commit offset but allows you to move back and forth throughout the partition.
	SetOffset(offset int64)

	// Lag returns the difference between the latest available offset in the partition and the
	// latest fetched offset by this consumer. This allows you to see how much behind the consumer is.
	Lag() int64
}

// PartitionConsumer serves to consume exactly one topic/partition from Kafka.
// This is very similar to JVM SimpleConsumer except the PartitionConsumer is able to handle
// leader changes and supports committing offsets to Kafka via Siesta client.
type KafkaPartitionConsumer struct {
	client              Client
	config              *ConsumerConfig
	topic               string
	partition           int32
	offset              int64
	highwaterMarkOffset int64
	strategy            Strategy
	stop                chan struct{}
}

// NewPartitionConsumer creates a new PartitionConsumer for given client and config that will
// consume given topic and partition.
// The message processing logic is passed via strategy.
func NewPartitionConsumer(client Client, config *ConsumerConfig, topic string, partition int32, strategy Strategy) PartitionConsumer {
	return &KafkaPartitionConsumer{
		client:    client,
		config:    config,
		topic:     topic,
		partition: partition,
		strategy:  strategy,
		stop:      make(chan struct{}, 1),
	}
}

// Start starts consuming a single partition from Kafka.
// This call blocks until Stop() is called.
func (pc *KafkaPartitionConsumer) Start() {
	proceed := pc.initOffset()
	if !proceed {
		return
	}

	for {
		response, err := pc.client.Fetch(pc.topic, pc.partition, atomic.LoadInt64(&pc.offset))
		select {
		case <-pc.stop:
			return
		default:
			{
				if err != nil {
					Logger.Warn("Fetch error: %s", err)
					pc.strategy(&FetchData{
						Messages: nil,
						Error:    err,
					}, pc)
					continue
				}

				data := response.Data[pc.topic][pc.partition]
				atomic.StoreInt64(&pc.highwaterMarkOffset, data.HighwaterMarkOffset)
				if len(data.Messages) == 0 {
					continue
				}

				// store the offset before we actually hand off messages to user
				if len(data.Messages) > 0 {
					offsetIndex := len(data.Messages) - 1
					atomic.StoreInt64(&pc.offset, data.Messages[offsetIndex].Offset+1)
				}

				//TODO siesta could probably support size hints? feel like quick traversal of messages should be quicker
				// than appending to a slice if it resizes internally, should benchmark this
				var messages []*MessageAndMetadata
				collector := pc.collectorFunc(&messages)
				err := response.CollectMessages(collector)

				pc.strategy(&FetchData{
					Messages: messages,
					Error:    err,
				}, pc)

				if pc.config.AutoCommitEnable && len(messages) > 0 {
					offset := messages[len(messages)-1].Offset
					err = pc.Commit(offset)
					if err != nil {
						Logger.Warn("Could not commit offset %d for topic %s, partition %d", offset, pc.topic, pc.partition)
					}
				}
			}
		}
	}
}

// Stop stops consuming partition from Kafka.
func (pc *KafkaPartitionConsumer) Stop() {
	pc.stop <- struct{}{}
}

// Commit commits the given offset to Kafka. Returns an error on unsuccessful commit.
func (pc *KafkaPartitionConsumer) Commit(offset int64) error {
	return pc.client.CommitOffset(pc.config.Group, pc.topic, pc.partition, offset)
}

// SetOffset overrides the current fetch offset value for given topic/partition.
// This does not commit offset but allows you to move back and forth throughout the partition.
func (pc *KafkaPartitionConsumer) SetOffset(offset int64) {
	atomic.StoreInt64(&pc.offset, offset)
}

// Offset returns the last fetched offset for this partition consumer.
func (pc *KafkaPartitionConsumer) Offset() int64 {
	return atomic.LoadInt64(&pc.offset)
}

// Lag returns the difference between the latest available offset in the partition and the
// latest fetched offset by this consumer. This allows you to see how much behind the consumer is.
func (pc *KafkaPartitionConsumer) Lag() int64 {
	return atomic.LoadInt64(&pc.highwaterMarkOffset) - atomic.LoadInt64(&pc.offset)
}

func (pc *KafkaPartitionConsumer) initOffset() bool {
	for {
		offset, err := pc.client.GetOffset(pc.config.Group, pc.topic, pc.partition)
		if err != nil {
			if err == siesta.ErrUnknownTopicOrPartition {
				return pc.resetOffset()
			}
			Logger.Warn("Cannot get offset for group %s, topic %s, partition %d: %s\n", pc.config.Group, pc.topic, pc.partition, err)
			select {
			case <-pc.stop:
				{
					Logger.Warn("PartitionConsumer told to stop trying to get offset, returning")
					return false
				}
			default:
			}
		} else {
			atomic.StoreInt64(&pc.offset, offset)
			atomic.StoreInt64(&pc.highwaterMarkOffset, offset)
			return true
		}
		time.Sleep(1 * time.Second) // TODO configurable
	}
}

func (pc *KafkaPartitionConsumer) resetOffset() bool {
	for {
		offset, err := pc.client.GetAvailableOffset(pc.topic, pc.partition, pc.config.AutoOffsetReset)
		if err != nil {
			Logger.Warn("Cannot get available offset for topic %s, partition %d: %s", pc.topic, pc.partition, err)
			select {
			case <-pc.stop:
				{
					Logger.Warn("PartitionConsumer told to stop trying to get offset, returning")
					return false
				}
			default:
			}
		} else {
			atomic.StoreInt64(&pc.offset, offset)
			atomic.StoreInt64(&pc.highwaterMarkOffset, offset)
			return true
		}
		time.Sleep(1 * time.Second) // TODO configurable
	}
}

func (pc *KafkaPartitionConsumer) collectorFunc(messages *[]*MessageAndMetadata) func(topic string, partition int32, offset int64, key []byte, value []byte) {
	return func(topic string, partition int32, offset int64, key []byte, value []byte) {
		decodedKey, err := pc.config.KeyDecoder.Decode(key)
		if err != nil {
			//TODO siesta should support collector function to return an error
			Logger.Warn(err.Error())
		}
		decodedValue, err := pc.config.ValueDecoder.Decode(value)
		if err != nil {
			//TODO siesta should support collector function to return an error
			Logger.Warn(err.Error())
		}

		*messages = append(*messages, &MessageAndMetadata{
			Key:          key,
			Value:        value,
			Topic:        topic,
			Partition:    partition,
			Offset:       offset,
			DecodedKey:   decodedKey,
			DecodedValue: decodedValue,
		})
	}
}

// Strategy is a function that actually processes Kafka messages.
// FetchData contains actual messages, highwater mark offset and fetch error.
// PartitionConsumer which is passed to this function allows to commit/rewind offset if necessary,
// track offset/lag, stop the consumer. Please note that you should NOT stop the consumer if using
// Consumer but rather use consumer.Remove(topic, partition) call.
// The processing happens on per-partition level - the amount of strategies running simultaneously is defined by the
// number of partitions being consumed. The next batch for topic/partition won't start until the previous one
// finishes.
type Strategy func(data *FetchData, consumer *KafkaPartitionConsumer)
