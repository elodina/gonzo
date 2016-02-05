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
)

// PartitionConsumerMetrics is an interface for accessing and modifying PartitionConsumer metrics.
type PartitionConsumerMetrics interface {
	// NumFetches is a counter with a total number of fetches done by enclosing PartitionConsumer.
	NumFetches(func(metrics.Counter))

	// NumFailedFetches is a counter with a number of failed fetches done by enclosing PartitionConsumer.
	NumFailedFetches(func(metrics.Counter))

	// NumEmptyFetches is a counter with a number of fetches that returned 0 messages done by enclosing PartitionConsumer.
	NumEmptyFetches(func(metrics.Counter))

	// NumFetchedMessages is a counter with a total number of fetched messages by enclosing PartitionConsumer.
	NumFetchedMessages(func(metrics.Counter))

	// NumOffsetCommits is a counter with a total number of offset commits done by enclosing PartitionConsumer.
	NumOffsetCommits(func(metrics.Counter))

	// NumFailedOffsetCommits is a counter with a number of failed offset commits done by enclosing PartitionConsumer.
	NumFailedOffsetCommits(func(metrics.Counter))

	// Lag is a gauge with a current lag value for enclosing PartitionConsumer.
	Lag(func(metrics.Gauge))

	// Registry provides access to metrics registry for enclosing PartitionConsumer.
	Registry() metrics.Registry

	// Stop unregisters all metrics from the registry.
	Stop()
}

// KafkaPartitionConsumerMetrics implements PartitionConsumerMetrics and is used when ConsumerConfig.EnableMetrics is set to true.
type KafkaPartitionConsumerMetrics struct {
	registry metrics.Registry

	numFetches             metrics.Counter
	numFailedFetches       metrics.Counter
	numEmptyFetches        metrics.Counter
	numFetchedMessages     metrics.Counter
	numOffsetCommits       metrics.Counter
	numFailedOffsetCommits metrics.Counter
	lag                    metrics.Gauge
}

// NewKafkaPartitionConsumerMetrics creates new KafkaPartitionConsumerMetrics for a given topic and partition.
func NewKafkaPartitionConsumerMetrics(topic string, partition int32) *KafkaPartitionConsumerMetrics {
	registry := metrics.NewPrefixedRegistry(fmt.Sprintf("%s.%d.", topic, partition))

	return &KafkaPartitionConsumerMetrics{
		registry:               registry,
		numFetches:             metrics.NewRegisteredCounter("numFetches", registry),
		numFailedFetches:       metrics.NewRegisteredCounter("numFailedFetches", registry),
		numEmptyFetches:        metrics.NewRegisteredCounter("numEmptyFetches", registry),
		numFetchedMessages:     metrics.NewRegisteredCounter("numFetchedMessages", registry),
		numOffsetCommits:       metrics.NewRegisteredCounter("numOffsetCommits", registry),
		numFailedOffsetCommits: metrics.NewRegisteredCounter("numFailedOffsetCommits", registry),
		lag: metrics.NewRegisteredGauge("lag", registry),
	}
}

// NumFetches is a counter with a total number of fetches done by enclosing PartitionConsumer.
func (kpcm *KafkaPartitionConsumerMetrics) NumFetches(f func(metrics.Counter)) {
	f(kpcm.numFetches)
}

// NumFailedFetches is a counter with a number of failed fetches done by enclosing PartitionConsumer.
func (kpcm *KafkaPartitionConsumerMetrics) NumFailedFetches(f func(metrics.Counter)) {
	f(kpcm.numFailedFetches)
}

// NumEmptyFetches is a counter with a number of fetches that returned 0 messages done by enclosing PartitionConsumer.
func (kpcm *KafkaPartitionConsumerMetrics) NumEmptyFetches(f func(metrics.Counter)) {
	f(kpcm.numEmptyFetches)
}

// NumFetchedMessages is a counter with a total number of fetched messages by enclosing PartitionConsumer.
func (kpcm *KafkaPartitionConsumerMetrics) NumFetchedMessages(f func(metrics.Counter)) {
	f(kpcm.numFetchedMessages)
}

// NumOffsetCommits is a counter with a total number of offset commits done by enclosing PartitionConsumer.
func (kpcm *KafkaPartitionConsumerMetrics) NumOffsetCommits(f func(metrics.Counter)) {
	f(kpcm.numOffsetCommits)
}

// NumFailedOffsetCommits is a counter with a number of failed offset commits done by enclosing PartitionConsumer.
func (kpcm *KafkaPartitionConsumerMetrics) NumFailedOffsetCommits(f func(metrics.Counter)) {
	f(kpcm.numFailedOffsetCommits)
}

// Lag is a gauge with a current lag value for enclosing PartitionConsumer.
func (kpcm *KafkaPartitionConsumerMetrics) Lag(f func(metrics.Gauge)) {
	f(kpcm.lag)
}

// Registry provides access to metrics registry for enclosing PartitionConsumer.
func (kpcm *KafkaPartitionConsumerMetrics) Registry() metrics.Registry {
	return kpcm.registry
}

// Stop unregisters all metrics from the registry.
func (kpcm *KafkaPartitionConsumerMetrics) Stop() {
	kpcm.registry.UnregisterAll()
}

var noOpPartitionConsumerMetrics = new(noOpKafkaPartitionConsumerMetrics)

type noOpKafkaPartitionConsumerMetrics struct{}

func (*noOpKafkaPartitionConsumerMetrics) NumFetches(f func(metrics.Counter))             {}
func (*noOpKafkaPartitionConsumerMetrics) NumFailedFetches(f func(metrics.Counter))       {}
func (*noOpKafkaPartitionConsumerMetrics) NumEmptyFetches(f func(metrics.Counter))        {}
func (*noOpKafkaPartitionConsumerMetrics) NumFetchedMessages(f func(metrics.Counter))     {}
func (*noOpKafkaPartitionConsumerMetrics) NumOffsetCommits(f func(metrics.Counter))       {}
func (*noOpKafkaPartitionConsumerMetrics) NumFailedOffsetCommits(f func(metrics.Counter)) {}
func (*noOpKafkaPartitionConsumerMetrics) Lag(f func(metrics.Gauge))                      {}
func (*noOpKafkaPartitionConsumerMetrics) Registry() metrics.Registry {
	panic("Registry() call on no op metrics")
}
func (*noOpKafkaPartitionConsumerMetrics) Stop() {}
