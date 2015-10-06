/* Licensed to the Elodina Inc. under one or more
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

// FetchData is a slightly processed FetchResponse to be more user-friendly to use.
type FetchData struct {
	// Messages is a slice of actually fetched messages from Kafka.
	Messages []*MessageAndMetadata

	// HighwaterMarkOffset is an offset in the end of topic/partition this FetchData comes from.
	HighwaterMarkOffset int64

	// Error is an error that occurs both on fetch level or topic/partition level.
	Error error
}

// MessageAndMetadata is a single Kafka message and its metadata.
type MessageAndMetadata struct {
	// Key is a raw message key.
	Key []byte

	// Value is a raw message value.
	Value []byte

	// Topic is a Kafka topic this message comes from.
	Topic string

	// Partition is a Kafka partition this message comes from.
	Partition int32

	// Offset is an offset for this message.
	Offset int64

	// DecodedKey is a message key processed by KeyDecoder.
	DecodedKey interface{}

	// DecodedValue is a message value processed by ValueDecoder.
	DecodedValue interface{}
}
