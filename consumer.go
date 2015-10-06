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
	"sync"
)

// Consumer is essentially a collection of PartitionConsumers and exposes nearly the same API
// but on a bit higher level.
// Consumer is something similar to JVM High Level Consumer except the load balancing functionality
// is not implemented here thus allowing the Consumer to be independent from Zookeeper.
type Consumer struct {
	config                 *ConsumerConfig
	client                 Client
	strategy               Strategy
	partitionConsumers     map[string]map[int32]PartitionConsumerInterface
	partitionConsumersLock sync.Mutex
	assignmentsWaitGroup   sync.WaitGroup
	stopped                chan struct{}

	// for testing purposes
	partitionConsumerFactory func(client Client, config *ConsumerConfig, topic string, partition int32, strategy Strategy) PartitionConsumerInterface
}

// NewConsumer creates a new Consumer using the given client and config.
// The message processing logic is passed via strategy.
func NewConsumer(client Client, config *ConsumerConfig, strategy Strategy) *Consumer {
	return &Consumer{
		config:                   config,
		client:                   client,
		strategy:                 strategy,
		partitionConsumers:       make(map[string]map[int32]PartitionConsumerInterface),
		partitionConsumerFactory: NewPartitionConsumer,
		stopped:                  make(chan struct{}, 1),
	}
}

// Add adds a topic/partition to consume for this consumer and starts consuming it immediately.
// Returns an error if PartitionConsumer for this topic/partition already exists.
func (c *Consumer) Add(topic string, partition int32) error {
	c.partitionConsumersLock.Lock()
	defer c.partitionConsumersLock.Unlock()

	if _, exists := c.partitionConsumers[topic]; !exists {
		c.partitionConsumers[topic] = make(map[int32]PartitionConsumerInterface)
	}

	if _, exists := c.partitionConsumers[topic][partition]; exists {
		Logger.Info("Partition consumer for topic %s, partition %d already exists", topic, partition)
		return fmt.Errorf("Partition consumer for topic %s, partition %d already exists", topic, partition)
	}

	c.partitionConsumers[topic][partition] = c.partitionConsumerFactory(c.client, c.config, topic, partition, c.strategy)
	c.assignmentsWaitGroup.Add(1)
	go c.partitionConsumers[topic][partition].Start()
	return nil
}

// Remove stops consuming a topic/partition by this consumer immediately.
// Returns an error if PartitionConsumer for this topic/partition does not exist.
func (c *Consumer) Remove(topic string, partition int32) error {
	c.partitionConsumersLock.Lock()
	defer c.partitionConsumersLock.Unlock()

	if !c.exists(topic, partition) {
		Logger.Info("Partition consumer for topic %s, partition %d does not exist", topic, partition)
		return fmt.Errorf("Partition consumer for topic %s, partition %d does not exist", topic, partition)
	}

	c.partitionConsumers[topic][partition].Stop()
	c.assignmentsWaitGroup.Done()
	delete(c.partitionConsumers[topic], partition)
	return nil
}

// Assignment returns a map of topic/partitions being consumer at the moment by this consumer.
// The keys are topic names and values are slices of partitions.
func (c *Consumer) Assignment() map[string][]int32 {
	c.partitionConsumersLock.Lock()
	defer c.partitionConsumersLock.Unlock()

	assignments := make(map[string][]int32)
	for topic, partitions := range c.partitionConsumers {
		for partition := range partitions {
			assignments[topic] = append(assignments[topic], partition)
		}
	}

	return assignments
}

// Offset returns the current consuming offset for a given topic/partition.
// Please note that this value does not correspond to the latest committed offset but the latest fetched offset.
// This call will return an error if the PartitionConsumer for given topic/partition does not exist.
func (c *Consumer) Offset(topic string, partition int32) (int64, error) {
	c.partitionConsumersLock.Lock()
	defer c.partitionConsumersLock.Unlock()

	if !c.exists(topic, partition) {
		Logger.Info("Can't get offset as partition consumer for topic %s, partition %d does not exist", topic, partition)
		return -1, fmt.Errorf("Partition consumer for topic %s, partition %d does not exist", topic, partition)
	}

	return c.partitionConsumers[topic][partition].Offset(), nil
}

// Commit commits the given offset for a given topic/partition to Kafka.
// Returns an error if the commit was unsuccessful.
func (c *Consumer) Commit(topic string, partition int32, offset int64) error {
	return c.client.CommitOffset(c.config.Group, topic, partition, offset)
}

// SetOffset overrides the current fetch offset value for given topic/partition.
// This does not commit offset but allows you to move back and forth throughout the partition.
// Returns an error if the PartitionConsumer for this topic/partition does not exist.
func (c *Consumer) SetOffset(topic string, partition int32, offset int64) error {
	c.partitionConsumersLock.Lock()
	defer c.partitionConsumersLock.Unlock()

	if !c.exists(topic, partition) {
		Logger.Info("Can't set offset as partition consumer for topic %s, partition %d does not exist", topic, partition)
		return fmt.Errorf("Partition consumer for topic %s, partition %d does not exist", topic, partition)
	}

	c.partitionConsumers[topic][partition].SetOffset(offset)
	return nil
}

// Lag returns the difference between the latest available offset in the partition and the
// latest fetched offset by this consumer. This allows you to see how much behind the consumer is.
// Returns lag value for a given topic/partition and an error if the PartitionConsumer for given
// topic/partition does not exist.
func (c *Consumer) Lag(topic string, partition int32) (int64, error) {
	c.partitionConsumersLock.Lock()
	defer c.partitionConsumersLock.Unlock()

	if !c.exists(topic, partition) {
		Logger.Info("Can't get lag as partition consumer for topic %s, partition %d does not exist", topic, partition)
		return -1, fmt.Errorf("Partition consumer for topic %s, partition %d does not exist", topic, partition)
	}

	return c.partitionConsumers[topic][partition].Lag(), nil
}

// Stop stops consuming all topics and partitions with this consumer.
func (c *Consumer) Stop() {
	for topic, partitions := range c.Assignment() {
		for _, partition := range partitions {
			c.Remove(topic, partition)
		}
	}
	c.stopped <- struct{}{}
}

// AwaitTermination blocks until Stop() is called.
func (c *Consumer) AwaitTermination() {
	<-c.stopped
}

// Join blocks until consumer has at least one topic/partition to consume, e.g. until len(Assignment()) > 0.
func (c *Consumer) Join() {
	c.assignmentsWaitGroup.Wait()
}

func (c *Consumer) exists(topic string, partition int32) bool {
	if _, exists := c.partitionConsumers[topic]; !exists {
		return false
	}

	if _, exists := c.partitionConsumers[topic][partition]; !exists {
		return false
	}

	return true
}
