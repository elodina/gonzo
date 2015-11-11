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

type PartitionConsumerMetrics interface {
	NumFetches(func(metrics.Counter))
	NumFailedFetches(func(metrics.Counter))
	NumEmptyFetches(func(metrics.Counter))
	NumFetchedMessages(func(metrics.Counter))
	NumOffsetCommits(func(metrics.Counter))
	Lag(func(metrics.Gauge))
	Stop()
}

type KafkaPartitionConsumerMetrics struct {
	registry metrics.Registry

	numFetches         metrics.Counter
	numFailedFetches   metrics.Counter
	numEmptyFetches    metrics.Counter
	numFetchedMessages metrics.Counter
	numOffsetCommits   metrics.Counter
	lag                metrics.Gauge
}

func NewKafkaPartitionConsumerMetrics(topic string, partition int32) *KafkaPartitionConsumerMetrics {
	registry := metrics.NewPrefixedRegistry(fmt.Sprintf("%s.%d.", topic, partition))

	return &KafkaPartitionConsumerMetrics{
		registry:           registry,
		numFetches:         metrics.NewRegisteredCounter("numFetches", registry),
		numFailedFetches:   metrics.NewRegisteredCounter("numFailedFetches", registry),
		numEmptyFetches:    metrics.NewRegisteredCounter("numEmptyFetches", registry),
		numFetchedMessages: metrics.NewRegisteredCounter("numFetchedMessages", registry),
		numOffsetCommits:   metrics.NewRegisteredCounter("numOffsetCommits", registry),
		lag:                metrics.NewRegisteredGauge("lag", registry),
	}
}

func (kpcm *KafkaPartitionConsumerMetrics) NumFetches(f func(metrics.Counter)) {
	f(kpcm.numFetches)
}

func (kpcm *KafkaPartitionConsumerMetrics) NumFailedFetches(f func(metrics.Counter)) {
	f(kpcm.numFailedFetches)
}

func (kpcm *KafkaPartitionConsumerMetrics) NumEmptyFetches(f func(metrics.Counter)) {
	f(kpcm.numEmptyFetches)
}

func (kpcm *KafkaPartitionConsumerMetrics) NumFetchedMessages(f func(metrics.Counter)) {
	f(kpcm.numFetchedMessages)
}

func (kpcm *KafkaPartitionConsumerMetrics) NumOffsetCommits(f func(metrics.Counter)) {
	f(kpcm.numOffsetCommits)
}

func (kpcm *KafkaPartitionConsumerMetrics) Lag(f func(metrics.Gauge)) {
	f(kpcm.lag)
}

func (kpcm *KafkaPartitionConsumerMetrics) Stop() {
	kpcm.registry.UnregisterAll()
}

var noOpPartitionConsumerMetrics = new(noOpKafkaPartitionConsumerMetrics)

type noOpKafkaPartitionConsumerMetrics struct{}

func (*noOpKafkaPartitionConsumerMetrics) NumFetches(f func(metrics.Counter))         {}
func (*noOpKafkaPartitionConsumerMetrics) NumFailedFetches(f func(metrics.Counter))   {}
func (*noOpKafkaPartitionConsumerMetrics) NumEmptyFetches(f func(metrics.Counter))    {}
func (*noOpKafkaPartitionConsumerMetrics) NumFetchedMessages(f func(metrics.Counter)) {}
func (*noOpKafkaPartitionConsumerMetrics) NumOffsetCommits(f func(metrics.Counter))   {}
func (*noOpKafkaPartitionConsumerMetrics) Lag(f func(metrics.Gauge))                  {}
func (*noOpKafkaPartitionConsumerMetrics) Stop()                                      {}
