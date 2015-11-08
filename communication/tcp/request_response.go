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

package tcp

import "encoding/json"

const (
	// RequestKeyAdd is a request key for consumer's Add function
	RequestKeyAdd = "add"

	// RequestKeyRemove is a request key for consumer's Remove function
	RequestKeyRemove = "remove"

	// RequestKeyAssignments is a request key for consumer's Assignment function
	RequestKeyAssignments = "assignments"

	// RequestKeyOffset is a request key for consumer's Offset function
	RequestKeyOffset = "offset"

	// RequestKeyCommit is a request key for consumer's Commit function
	RequestKeyCommit = "commit"

	// RequestKeySetOffset is a request key for consumer's SetOffset function
	RequestKeySetOffset = "setoffset"

	// RequestKeyLag is a request key for consumer's Lag function
	RequestKeyLag = "lag"
)

type clientRequest struct {
	Key  string
	Data interface{}
}

type request struct {
	Key  string
	Data json.RawMessage
}

// Response defines the response message format to exchange data.
type Response struct {
	// Success is a flag whether the request associated with this response succeeded.
	Success bool

	// Message is any description or error message associated with this response.
	Message string

	// Data is any additional payload associated with this response.
	Data interface{}
}

// NewResponse creates a new Response.
func NewResponse(success bool, message string, data interface{}) *Response {
	return &Response{
		Success: success,
		Message: message,
		Data:    data,
	}
}

type topicPartition struct {
	Topic     string
	Partition int32
}

type topicPartitionOffset struct {
	Topic     string
	Partition int32
	Offset    int64
}
