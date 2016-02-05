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

import "github.com/elodina/siesta"

// Client is an interface responsible for low-level Kafka interaction.
// The only supported implmentation now is Siesta.
// One other implementation, MockClient, is used for testing purposes.
type Client interface {
	// Fetch is responsible for fetching messages for given topic, partition and offset from Kafka broker.
	// Leader change handling happens inside Fetch and is hidden from user so he should not handle such cases.
	// Returns a fetch response and error if it occurred.
	Fetch(topic string, partition int32, offset int64) (*siesta.FetchResponse, error)

	// GetAvailableOffset issues an offset request to a specified topic and partition with a given offset time.
	// Returns an offet for given topic, partition and offset time and an error if it occurs.
	GetAvailableOffset(topic string, partition int32, offsetTime int64) (int64, error)

	// GetOffset gets the latest committed offset for a given group, topic and partition from Kafka.
	// Returns an offset and an error if it occurs.
	GetOffset(group string, topic string, partition int32) (int64, error)

	// CommitOffset commits the given offset for a given group, topic and partition to Kafka.
	// Returns an error if commit was unsuccessful.
	CommitOffset(group string, topic string, partition int32, offset int64) error
}
