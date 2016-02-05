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

package tcp

import (
	"fmt"
	"github.com/elodina/gonzo"
	"github.com/rcrowley/go-metrics"
	"sync"
)

type MockConsumer struct {
	partitionConsumers     map[string]map[int32]struct{}
	partitionConsumersLock sync.Mutex
	assignmentsWaitGroup   sync.WaitGroup
	stopped                chan struct{}

	offsets           map[string]map[int32]int64
	commitOffsetError error
}

func NewMockConsumer() *MockConsumer {
	return &MockConsumer{
		partitionConsumers: make(map[string]map[int32]struct{}),
		stopped:            make(chan struct{}),
		offsets:            make(map[string]map[int32]int64),
	}
}

func (mc *MockConsumer) Add(topic string, partition int32) error {
	mc.partitionConsumersLock.Lock()
	defer mc.partitionConsumersLock.Unlock()

	if _, exists := mc.partitionConsumers[topic]; !exists {
		mc.partitionConsumers[topic] = make(map[int32]struct{})
	}

	if _, exists := mc.partitionConsumers[topic][partition]; exists {
		gonzo.Logger.Info("Partition consumer for topic %s, partition %d already exists", topic, partition)
		return fmt.Errorf("Partition consumer for topic %s, partition %d already exists", topic, partition)
	}

	mc.partitionConsumers[topic][partition] = struct{}{}
	mc.assignmentsWaitGroup.Add(1)

	return nil
}

func (mc *MockConsumer) Remove(topic string, partition int32) error {
	mc.partitionConsumersLock.Lock()
	defer mc.partitionConsumersLock.Unlock()

	if !mc.exists(topic, partition) {
		gonzo.Logger.Info("Partition consumer for topic %s, partition %d does not exist", topic, partition)
		return fmt.Errorf("Partition consumer for topic %s, partition %d does not exist", topic, partition)
	}

	mc.assignmentsWaitGroup.Done()
	delete(mc.partitionConsumers[topic], partition)
	return nil
}

func (mc *MockConsumer) Assignment() map[string][]int32 {
	mc.partitionConsumersLock.Lock()
	defer mc.partitionConsumersLock.Unlock()

	assignments := make(map[string][]int32)
	for topic, partitions := range mc.partitionConsumers {
		for partition := range partitions {
			assignments[topic] = append(assignments[topic], partition)
		}
	}

	return assignments
}

func (mc *MockConsumer) Offset(topic string, partition int32) (int64, error) {
	mc.partitionConsumersLock.Lock()
	defer mc.partitionConsumersLock.Unlock()

	if !mc.exists(topic, partition) {
		gonzo.Logger.Info("Can't get offset as partition consumer for topic %s, partition %d does not exist", topic, partition)
		return -1, fmt.Errorf("Partition consumer for topic %s, partition %d does not exist", topic, partition)
	}

	return mc.offsets[topic][partition], nil
}

func (mc *MockConsumer) Commit(topic string, partition int32, offset int64) error {
	mc.partitionConsumersLock.Lock()
	defer mc.partitionConsumersLock.Unlock()

	mc.initOffsets(topic, partition)
	mc.offsets[topic][partition] = offset
	return mc.commitOffsetError
}

func (mc *MockConsumer) SetOffset(topic string, partition int32, offset int64) error {
	mc.partitionConsumersLock.Lock()
	defer mc.partitionConsumersLock.Unlock()

	if !mc.exists(topic, partition) {
		gonzo.Logger.Info("Can't set offset as partition consumer for topic %s, partition %d does not exist", topic, partition)
		return fmt.Errorf("Partition consumer for topic %s, partition %d does not exist", topic, partition)
	}

	mc.initOffsets(topic, partition)
	mc.offsets[topic][partition] = offset
	return nil
}

func (mc *MockConsumer) Lag(topic string, partition int32) (int64, error) {
	mc.partitionConsumersLock.Lock()
	defer mc.partitionConsumersLock.Unlock()

	if !mc.exists(topic, partition) {
		gonzo.Logger.Info("Can't get lag as partition consumer for topic %s, partition %d does not exist", topic, partition)
		return -1, fmt.Errorf("Partition consumer for topic %s, partition %d does not exist", topic, partition)
	}

	return 100, nil
}

func (mc *MockConsumer) Stop() {
	for topic, partitions := range mc.Assignment() {
		for _, partition := range partitions {
			mc.Remove(topic, partition)
		}
	}
	close(mc.stopped)
}

func (mc *MockConsumer) AwaitTermination() {
	<-mc.stopped
}

func (mc *MockConsumer) Join() {
	mc.assignmentsWaitGroup.Wait()
}

func (mc *MockConsumer) ConsumerMetrics() (metrics.Registry, error) {
	return nil, nil
}

func (mc *MockConsumer) PartitionConsumerMetrics(topic string, partition int32) (metrics.Registry, error) {
	return nil, nil
}

func (mc *MockConsumer) AllMetrics() (*gonzo.Metrics, error) {
	return nil, nil
}

func (mc *MockConsumer) exists(topic string, partition int32) bool {
	if _, exists := mc.partitionConsumers[topic]; !exists {
		return false
	}

	if _, exists := mc.partitionConsumers[topic][partition]; !exists {
		return false
	}

	return true
}

func (mc *MockConsumer) initOffsets(topic string, partition int32) {
	if mc.offsets[topic] == nil {
		mc.offsets[topic] = make(map[int32]int64)
	}
}
