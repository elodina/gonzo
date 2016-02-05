/* Licensed to the Apache Software Foundation (ASF) under one or more
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
	"github.com/rcrowley/go-metrics"
	"github.com/satori/go.uuid"
)

// Metrics is a set of all metrics for one Consumer instance.
type Metrics struct {
	// Consumer is a metrics registry for enclosing Consumer instance.
	Consumer metrics.Registry

	// PartitionConsumers is a map of topic/partitions to PartitionConsumer metrics registries.
	PartitionConsumers map[string]map[int32]metrics.Registry
}

// ConsumerMetrics is an interface for accessing and modifying Consumer metrics.
type ConsumerMetrics interface {
	// NumOwnedTopicPartitions is a counter which value is the number of currently owned topic partitions by
	// enclosing Consumer.
	NumOwnedTopicPartitions(func(metrics.Counter))

	// Registry provides access to metrics registry for enclosing Consumer.
	Registry() metrics.Registry

	// Stop unregisters all metrics from the registry.
	Stop()
}

// KafkaConsumerMetrics implements ConsumerMetrics and is used when ConsumerConfig.EnableMetrics is set to true.
type KafkaConsumerMetrics struct {
	registry metrics.Registry

	numOwnedTopicPartitions metrics.Counter
}

// NewKafkaConsumerMetrics creates new KafkaConsumerMetrics for a given consumer group.
func NewKafkaConsumerMetrics(group string) *KafkaConsumerMetrics {
	registry := metrics.NewPrefixedRegistry(fmt.Sprintf("%s.%s.", group, uuid.NewV4()))

	return &KafkaConsumerMetrics{
		registry: registry,

		numOwnedTopicPartitions: metrics.NewRegisteredCounter("numOwnedTopicPartitions", registry),
	}
}

// NumOwnedTopicPartitions is a counter which value is the number of currently owned topic partitions by
// enclosing Consumer.
func (cm *KafkaConsumerMetrics) NumOwnedTopicPartitions(f func(metrics.Counter)) {
	f(cm.numOwnedTopicPartitions)
}

// Registry provides access to metrics registry for enclosing Consumer.
func (cm *KafkaConsumerMetrics) Registry() metrics.Registry {
	return cm.registry
}

// Stop unregisters all metrics from the registry.
func (cm *KafkaConsumerMetrics) Stop() {
	cm.registry.UnregisterAll()
}

var noOpConsumerMetrics = new(noOpKafkaConsumerMetrics)

type noOpKafkaConsumerMetrics struct{}

func (*noOpKafkaConsumerMetrics) NumOwnedTopicPartitions(func(metrics.Counter)) {}
func (*noOpKafkaConsumerMetrics) Registry() metrics.Registry {
	panic("Registry() call on no op metrics")
}
func (*noOpKafkaConsumerMetrics) Stop() {}
