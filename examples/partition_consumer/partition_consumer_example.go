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

package main

import (
	"fmt"
	"github.com/elodina/gonzo"
	"github.com/elodina/siesta"
)

func main() {
	config := siesta.NewConnectorConfig()
	config.BrokerList = []string{"localhost:9092"}

	client, err := siesta.NewDefaultConnector(config)
	if err != nil {
		panic(err)
	}

	consumer := gonzo.NewPartitionConsumer(client, gonzo.NewConsumerConfig(), "gonzo", 0, partitionConsumerStrategy)

	consumer.Start()
}

func partitionConsumerStrategy(data *gonzo.FetchData, consumer *gonzo.KafkaPartitionConsumer) {
	if data.Error != nil {
		panic(data.Error)
	}

	for _, msg := range data.Messages {
		fmt.Printf("%s from partition %d\n", string(msg.Value), msg.Partition)
	}
}
