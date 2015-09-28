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

import "testing"

func TestConsumerAssignments(t *testing.T) {
	client := NewMockClient(0, 0)
	consumer := NewConsumer(client, NewConsumerConfig(), NoOpStrategy)
	consumer.partitionConsumerFactory = NewMockPartitionConsumer

	// no assignments
	assignments := consumer.Assignment()
	assertFatal(t, len(assignments), 0)

	// add one
	consumer.Add("test", 0)
	assignments = consumer.Assignment()
	_, exists := assignments["test"]
	assertFatal(t, exists, true)
	assertFatal(t, len(assignments["test"]), 1)
	assertFatal(t, assignments["test"][0], int32(0))

	// add existing
	consumer.Add("test", 0)
	assignments = consumer.Assignment()
	assertFatal(t, len(assignments), 1)

	// add another
	consumer.Add("test1", 1)
	assignments = consumer.Assignment()
	_, exists = assignments["test1"]
	assertFatal(t, exists, true)
	assertFatal(t, len(assignments["test1"]), 1)
	assertFatal(t, assignments["test1"][0], int32(1))

	assertFatal(t, len(assignments), 2)

	// remove one
	consumer.Remove("test", 0)
	assignments = consumer.Assignment()
	assertFatal(t, len(assignments), 1)

	// remove non existing
	consumer.Remove("test", 0)
	assignments = consumer.Assignment()
	assertFatal(t, len(assignments), 1)

	// remove one that never existed
	consumer.Remove("asdasd", 32)
	assignments = consumer.Assignment()
	assertFatal(t, len(assignments), 1)

	// remove last
	consumer.Remove("test1", 1)
	assignments = consumer.Assignment()
	assertFatal(t, len(assignments), 0)
}
