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

type Consumer struct {
	config                 *ConsumerConfig
	client                 Client
	strategy               Strategy
	partitionConsumers     map[string]map[int32]PartitionConsumerInterface
	partitionConsumersLock sync.Mutex

	// for testing purposes
	partitionConsumerFactory func(client Client, group string, topic string, partition int32, strategy Strategy) PartitionConsumerInterface
}

func NewConsumer(client Client, config *ConsumerConfig, strategy Strategy) *Consumer {
	return &Consumer{
		config:                   config,
		client:                   client,
		strategy:                 strategy,
		partitionConsumers:       make(map[string]map[int32]PartitionConsumerInterface),
		partitionConsumerFactory: NewPartitionConsumer,
	}
}

func (c *Consumer) Add(topic string, partition int32) {
	c.partitionConsumersLock.Lock()
	defer c.partitionConsumersLock.Unlock()

	if _, exists := c.partitionConsumers[topic]; !exists {
		c.partitionConsumers[topic] = make(map[int32]PartitionConsumerInterface)
	}

	if _, exists := c.partitionConsumers[topic][partition]; exists {
		Logger.Info("Partition consumer for topic %s, partition %d already exists, ignoring add call...", topic, partition)
		return
	}

	c.partitionConsumers[topic][partition] = NewPartitionConsumer(c.client, c.config.Group, topic, partition, c.strategy)
	go c.partitionConsumers[topic][partition].Start()
}

func (c *Consumer) Remove(topic string, partition int32) {
	c.partitionConsumersLock.Lock()
	defer c.partitionConsumersLock.Unlock()

	if !c.exists(topic, partition) {
		Logger.Info("Partition consumer for topic %s, partition %d does not exist, ignoring remove call...", topic, partition)
		return
	}

	c.partitionConsumers[topic][partition].Stop()
	delete(c.partitionConsumers[topic], partition)
}

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

func (c *Consumer) Offset(topic string, partition int32) (int64, error) {
	c.partitionConsumersLock.Lock()
	defer c.partitionConsumersLock.Unlock()

	if !c.exists(topic, partition) {
		Logger.Info("Can't get offset as partition consumer for topic %s, partition %d does not exist", topic, partition)
		return -1, fmt.Errorf("Partition consumer for topic %s, partition %d does not exist", topic, partition)
	}

	return c.partitionConsumers[topic][partition].Offset(), nil
}

func (c *Consumer) Commit(topic string, partition int32, offset int64) error {
	return c.client.CommitOffset(c.config.Group, topic, partition, offset)
}

func (c *Consumer) SetOffset(topic string, partition int32, offset int64) {
	c.partitionConsumersLock.Lock()
	defer c.partitionConsumersLock.Unlock()

	if !c.exists(topic, partition) {
		Logger.Info("Can't set offset as partition consumer for topic %s, partition %d does not exist", topic, partition)
		return
	}

	c.partitionConsumers[topic][partition].SetOffset(offset)
}

func (c *Consumer) Lag(topic string, partition int32) int64 {
	c.partitionConsumersLock.Lock()
	defer c.partitionConsumersLock.Unlock()

	if !c.exists(topic, partition) {
		Logger.Info("Can't get lag as partition consumer for topic %s, partition %d does not exist", topic, partition)
		return -1
	}

	return c.partitionConsumers[topic][partition].Lag()
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
