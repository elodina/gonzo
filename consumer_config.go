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
	"github.com/elodina/siesta"
	"time"
)

// ConsumerConfig provides configuration options for both Consumer and PartitionConsumer.
type ConsumerConfig struct {
	// Group is a string that uniquely identifies a set of consumers within the same consumer group.
	Group string

	// KeyDecoder is a function that turns plain bytes into a decoded message key.
	KeyDecoder Decoder

	// ValueDecoder is a function that turns plain bytes into a decoded message value.
	ValueDecoder Decoder

	// AutoOffsetReset defines what to do when there is no committed offset or committed offset is out of range.
	// siesta.EarliestTime - automatically reset the offset to the smallest offset.
	// siesta.LatestTime - automatically reset the offset to the largest offset.
	// Defaults to siesta.EarliestTime.
	AutoOffsetReset int64

	// AutoCommitEnable determines whether the consumer will automatically commit offsets after each batch
	// is finished (e.g. the call to strategy function returns). Turned off by default.
	AutoCommitEnable bool

	// EnableMetrics determines whether the consumer will collect all kinds of metrics to better understand what's
	// going on under the hood. Turned off by default as it may significantly affect performance.
	EnableMetrics bool

	// Backoff between attempts to initialize consumer offset.
	InitOffsetBackoff time.Duration
}

// NewConsumerConfig creates a consumer config with sane defaults.
func NewConsumerConfig() *ConsumerConfig {
	return &ConsumerConfig{
		Group:             "gonzo-group",
		KeyDecoder:        new(ByteDecoder),
		ValueDecoder:      new(ByteDecoder),
		AutoOffsetReset:   siesta.EarliestTime,
		InitOffsetBackoff: 500 * time.Millisecond,
	}
}
