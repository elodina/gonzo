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

type MockPartitionConsumer struct {
	offset int64
	lag    int64
}

func NewMockPartitionConsumer(client Client, config *ConsumerConfig, topic string, partition int32, strategy Strategy) PartitionConsumerInterface {
	return new(MockPartitionConsumer)
}

func (mpc *MockPartitionConsumer) Start() {
	Logger.Info("MockPartitionConsumer.Start()")
}

func (mpc *MockPartitionConsumer) Stop() {
	Logger.Info("MockPartitionConsumer.Stop()")
}

func (mpc *MockPartitionConsumer) Offset() int64 {
	Logger.Info("MockPartitionConsumer.Offset()")
	return mpc.offset
}

func (mpc *MockPartitionConsumer) Commit(offset int64) error {
	Logger.Info("MockPartitionConsumer.Commit()")
	return nil
}

func (mpc *MockPartitionConsumer) SetOffset(offset int64) {
	Logger.Info("MockPartitionConsumer.SetOffset()")
	mpc.offset = offset
}

func (mpc *MockPartitionConsumer) Lag() int64 {
	Logger.Info("MockPartitionConsumer.Lag()")
	return mpc.lag
}
