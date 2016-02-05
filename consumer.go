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
	"github.com/rcrowley/go-metrics"
	"sync"
)

// Consumer is essentially a collection of PartitionConsumers and exposes nearly the same API
// but on a bit higher level.
// Consumer is something similar to JVM High Level Consumer except the load balancing functionality
// is not implemented here thus allowing the Consumer to be independent from Zookeeper.
type Consumer interface {
	// Add adds a topic/partition to consume for this consumer and starts consuming it immediately.
	// Returns an error if PartitionConsumer for this topic/partition already exists.
	Add(topic string, partition int32) error

	// Remove stops consuming a topic/partition by this consumer once it is done with the current batch.
	// This means the PartitionConsumer will stop accepting new batches but will have a chance to finish its current work.
	// Returns an error if PartitionConsumer for this topic/partition does not exist.
	Remove(topic string, partition int32) error

	// Assignment returns a map of topic/partitions being consumed at the moment by this consumer.
	// The keys are topic names and values are slices of partitions.
	Assignment() map[string][]int32

	// Offset returns the current consuming offset for a given topic/partition.
	// Please note that this value does not correspond to the latest committed offset but the latest fetched offset.
	// This call will return an error if the PartitionConsumer for given topic/partition does not exist.
	Offset(topic string, partition int32) (int64, error)

	// Commit commits the given offset for a given topic/partition to Kafka.
	// Returns an error if the commit was unsuccessful.
	Commit(topic string, partition int32, offset int64) error

	// SetOffset overrides the current fetch offset value for given topic/partition.
	// This does not commit offset but allows you to move back and forth throughout the partition.
	// Returns an error if the PartitionConsumer for this topic/partition does not exist.
	SetOffset(topic string, partition int32, offset int64) error

	// Lag returns the difference between the latest available offset in the partition and the
	// latest fetched offset by this consumer. This allows you to see how much behind the consumer is.
	// Returns lag value for a given topic/partition and an error if the PartitionConsumer for given
	// topic/partition does not exist.
	Lag(topic string, partition int32) (int64, error)

	// Stop stops consuming all topics and partitions with this consumer.
	Stop()

	// AwaitTermination blocks until Stop() is called.
	AwaitTermination()

	// ConsumerMetrics returns a metrics registry for this consumer. An error is returned if metrics are disabled.
	ConsumerMetrics() (metrics.Registry, error)

	// PartitionConsumerMetrics returns a metrics registry for a given topic and partition. An error is returned
	// if metrics are disabled or PartitionConsumer for given topic and partition does not exist
	PartitionConsumerMetrics(topic string, partition int32) (metrics.Registry, error)

	// AllMetrics returns metrics registries for this consumer and all its PartitionConsumers. An error is returned
	// if metrics are disabled.
	AllMetrics() (*Metrics, error)

	// Join blocks until consumer has at least one topic/partition to consume, e.g. until len(Assignment()) > 0.
	Join()
}

// KafkaConsumer implements Consumer and is something similar to JVM High Level Consumer except the load
// balancing functionality is not implemented here thus allowing to be independent from Zookeeper.
type KafkaConsumer struct {
	config                 *ConsumerConfig
	client                 Client
	strategy               Strategy
	metrics                ConsumerMetrics
	partitionConsumers     map[string]map[int32]PartitionConsumer
	partitionConsumersLock sync.Mutex
	assignmentsWaitGroup   sync.WaitGroup
	stopped                chan struct{}

	// for testing purposes
	partitionConsumerFactory func(client Client, config *ConsumerConfig, topic string, partition int32, strategy Strategy) PartitionConsumer
}

// NewConsumer creates a new Consumer using the given client and config.
// The message processing logic is passed via strategy.
func NewConsumer(client Client, config *ConsumerConfig, strategy Strategy) Consumer {
	var metrics ConsumerMetrics = noOpConsumerMetrics
	if config.EnableMetrics {
		metrics = NewKafkaConsumerMetrics(config.Group)
	}

	return &KafkaConsumer{
		config:                   config,
		client:                   client,
		strategy:                 strategy,
		metrics:                  metrics,
		partitionConsumers:       make(map[string]map[int32]PartitionConsumer),
		partitionConsumerFactory: NewPartitionConsumer,
		stopped:                  make(chan struct{}),
	}
}

// Add adds a topic/partition to consume for this consumer and starts consuming it immediately.
// Returns an error if PartitionConsumer for this topic/partition already exists.
func (c *KafkaConsumer) Add(topic string, partition int32) error {
	c.partitionConsumersLock.Lock()
	defer c.partitionConsumersLock.Unlock()

	if _, exists := c.partitionConsumers[topic]; !exists {
		c.partitionConsumers[topic] = make(map[int32]PartitionConsumer)
	}

	if _, exists := c.partitionConsumers[topic][partition]; exists {
		Logger.Info("Partition consumer for topic %s, partition %d already exists", topic, partition)
		return ErrPartitionConsumerAlreadyExists
	}

	c.metrics.NumOwnedTopicPartitions(func(numOwnedTopicPartitions metrics.Counter) {
		numOwnedTopicPartitions.Inc(1)
	})

	c.partitionConsumers[topic][partition] = c.partitionConsumerFactory(c.client, c.config, topic, partition, c.strategy)
	c.assignmentsWaitGroup.Add(1)
	go c.partitionConsumers[topic][partition].Start()
	return nil
}

// Remove stops consuming a topic/partition by this consumer once it is done with the current batch.
// This means the PartitionConsumer will stop accepting new batches but will have a chance to finish its current work.
// Returns an error if PartitionConsumer for this topic/partition does not exist.
func (c *KafkaConsumer) Remove(topic string, partition int32) error {
	c.partitionConsumersLock.Lock()
	defer c.partitionConsumersLock.Unlock()

	if !c.exists(topic, partition) {
		Logger.Info("Partition consumer for topic %s, partition %d does not exist", topic, partition)
		return ErrPartitionConsumerDoesNotExist
	}

	c.metrics.NumOwnedTopicPartitions(func(numOwnedTopicPartitions metrics.Counter) {
		numOwnedTopicPartitions.Dec(1)
	})

	c.partitionConsumers[topic][partition].Stop()
	c.assignmentsWaitGroup.Done()
	delete(c.partitionConsumers[topic], partition)
	return nil
}

// Assignment returns a map of topic/partitions being consumer at the moment by this consumer.
// The keys are topic names and values are slices of partitions.
func (c *KafkaConsumer) Assignment() map[string][]int32 {
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
func (c *KafkaConsumer) Offset(topic string, partition int32) (int64, error) {
	c.partitionConsumersLock.Lock()
	defer c.partitionConsumersLock.Unlock()

	if !c.exists(topic, partition) {
		Logger.Info("Can't get offset as partition consumer for topic %s, partition %d does not exist", topic, partition)
		return -1, ErrPartitionConsumerDoesNotExist
	}

	return c.partitionConsumers[topic][partition].Offset(), nil
}

// Commit commits the given offset for a given topic/partition to Kafka.
// Returns an error if the commit was unsuccessful.
func (c *KafkaConsumer) Commit(topic string, partition int32, offset int64) error {
	return c.client.CommitOffset(c.config.Group, topic, partition, offset)
}

// SetOffset overrides the current fetch offset value for given topic/partition.
// This does not commit offset but allows you to move back and forth throughout the partition.
// Returns an error if the PartitionConsumer for this topic/partition does not exist.
func (c *KafkaConsumer) SetOffset(topic string, partition int32, offset int64) error {
	c.partitionConsumersLock.Lock()
	defer c.partitionConsumersLock.Unlock()

	if !c.exists(topic, partition) {
		Logger.Info("Can't set offset as partition consumer for topic %s, partition %d does not exist", topic, partition)
		return ErrPartitionConsumerDoesNotExist
	}

	c.partitionConsumers[topic][partition].SetOffset(offset)
	return nil
}

// Lag returns the difference between the latest available offset in the partition and the
// latest fetched offset by this consumer. This allows you to see how much behind the consumer is.
// Returns lag value for a given topic/partition and an error if the PartitionConsumer for given
// topic/partition does not exist.
func (c *KafkaConsumer) Lag(topic string, partition int32) (int64, error) {
	c.partitionConsumersLock.Lock()
	defer c.partitionConsumersLock.Unlock()

	if !c.exists(topic, partition) {
		Logger.Info("Can't get lag as partition consumer for topic %s, partition %d does not exist", topic, partition)
		return -1, ErrPartitionConsumerDoesNotExist
	}

	return c.partitionConsumers[topic][partition].Lag(), nil
}

// Stop stops consuming all topics and partitions with this consumer.
func (c *KafkaConsumer) Stop() {
	for topic, partitions := range c.Assignment() {
		for _, partition := range partitions {
			c.Remove(topic, partition)
		}
	}
	close(c.stopped)
}

// AwaitTermination blocks until Stop() is called.
func (c *KafkaConsumer) AwaitTermination() {
	<-c.stopped
}

// Join blocks until consumer has at least one topic/partition to consume, e.g. until len(Assignment()) > 0.
func (c *KafkaConsumer) Join() {
	c.assignmentsWaitGroup.Wait()
}

// ConsumerMetrics returns a metrics registry for this consumer. An error is returned if metrics are disabled.
func (c *KafkaConsumer) ConsumerMetrics() (metrics.Registry, error) {
	if !c.config.EnableMetrics {
		return nil, ErrMetricsDisabled
	}

	return c.metrics.Registry(), nil
}

// PartitionConsumerMetrics returns a metrics registry for a given topic and partition. An error is returned
// if metrics are disabled or PartitionConsumer for given topic and partition does not exist
func (c *KafkaConsumer) PartitionConsumerMetrics(topic string, partition int32) (metrics.Registry, error) {
	if !c.config.EnableMetrics {
		return nil, ErrMetricsDisabled
	}

	c.partitionConsumersLock.Lock()
	defer c.partitionConsumersLock.Unlock()

	if !c.exists(topic, partition) {
		Logger.Info("Partition consumer for topic %s, partition %d does not exist", topic, partition)
		return nil, ErrPartitionConsumerDoesNotExist
	}

	return c.partitionConsumers[topic][partition].Metrics()
}

// AllMetrics returns metrics registries for this consumer and all its PartitionConsumers. An error is returned
// if metrics are disabled.
func (c *KafkaConsumer) AllMetrics() (*Metrics, error) {
	if !c.config.EnableMetrics {
		return nil, ErrMetricsDisabled
	}

	c.partitionConsumersLock.Lock()
	defer c.partitionConsumersLock.Unlock()

	partitionConsumerMetrics := make(map[string]map[int32]metrics.Registry)
	for topic, partitionConsumers := range c.partitionConsumers {
		partitionConsumerMetrics[topic] = make(map[int32]metrics.Registry)
		for partition, consumer := range partitionConsumers {
			metrics, err := consumer.Metrics()
			if err != nil {
				return nil, err
			}
			partitionConsumerMetrics[topic][partition] = metrics
		}
	}

	metrics, err := c.ConsumerMetrics()
	if err != nil {
		return nil, err
	}

	return &Metrics{
		Consumer:           metrics,
		PartitionConsumers: partitionConsumerMetrics,
	}, nil
}

func (c *KafkaConsumer) exists(topic string, partition int32) bool {
	if _, exists := c.partitionConsumers[topic]; !exists {
		return false
	}

	if _, exists := c.partitionConsumers[topic][partition]; !exists {
		return false
	}

	return true
}
