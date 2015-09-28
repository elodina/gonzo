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

type PartitionConsumerInterface interface {
	Start()
	Stop()
	Offset() int64
	Commit(offset int64) error
	SetOffset(offset int64)
	Lag() int64
}

type PartitionConsumer struct {
	AutoOffsetReset int64

	client              Client
	topic               string
	group               string
	partition           int32
	offset              int64
	highwaterMarkOffset int64
	strategy            Strategy
	stop                chan struct{}
}

func NewPartitionConsumer(client Client, group string, topic string, partition int32, strategy Strategy) PartitionConsumerInterface {
	return &PartitionConsumer{
		AutoOffsetReset: siesta.EarliestTime,
		client:          client,
		group:           group,
		topic:           topic,
		partition:       partition,
		strategy:        strategy,
		stop:            make(chan struct{}, 1),
	}
}

func (pc *PartitionConsumer) Start() {
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
					continue
				}

				data := response.Data[pc.topic][pc.partition]
				atomic.StoreInt64(&pc.highwaterMarkOffset, data.HighwaterMarkOffset)
				if len(data.Messages) == 0 {
					continue
				}

				messages, err := response.GetMessages()

				// store the offset before we actually hand off messages to user
				if len(data.Messages) > 0 {
					offsetIndex := len(data.Messages) - 1
					atomic.StoreInt64(&pc.offset, data.Messages[offsetIndex].Offset+1)
				}

				pc.strategy(messages, err, pc)
			}
		}
	}
}

func (pc *PartitionConsumer) Stop() {
	pc.stop <- struct{}{}
}

func (pc *PartitionConsumer) Commit(offset int64) error {
	return pc.client.CommitOffset(pc.group, pc.topic, pc.partition, offset)
}

func (pc *PartitionConsumer) SetOffset(offset int64) {
	atomic.StoreInt64(&pc.offset, offset)
}

func (pc *PartitionConsumer) Offset() int64 {
	return atomic.LoadInt64(&pc.offset)
}

func (pc *PartitionConsumer) Lag() int64 {
	return atomic.LoadInt64(&pc.highwaterMarkOffset) - atomic.LoadInt64(&pc.offset)
}

func (pc *PartitionConsumer) initOffset() bool {
	for {
		offset, err := pc.client.GetOffset(pc.group, pc.topic, pc.partition)
		if err != nil {
			if err == siesta.ErrUnknownTopicOrPartition {
				return pc.resetOffset()
			}
			Logger.Warn("Cannot get offset for group %s, topic %s, partition %d: %s\n", pc.group, pc.topic, pc.partition, err)
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

func (pc *PartitionConsumer) resetOffset() bool {
	for {
		offset, err := pc.client.GetAvailableOffset(pc.topic, pc.partition, pc.AutoOffsetReset)
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
