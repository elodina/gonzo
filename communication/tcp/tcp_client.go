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

import (
	"bufio"
	"encoding/json"
	"errors"
	"net"
)

// Client is a TCP client that can talk to a Gonzo consumer instance wrapped by TCP layer.
type Client struct {
	address        string
	connection     net.Conn
	responseReader *bufio.Reader
}

// NewClient creates a new TCP client to talk to a Gonzo consumer instance located via given address.
// Returns a tcp client and an error if TCP connection failed.
func NewClient(addr string) (*Client, error) {
	connection, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	return &Client{
		address:        addr,
		connection:     connection,
		responseReader: bufio.NewReader(connection),
	}, nil
}

// Add adds a topic/partition to consume for associated consumer and starts consuming it immediately.
// Returns an error if PartitionConsumer for this topic/partition already exists.
func (c *Client) Add(topic string, partition int32) error {
	request := &clientRequest{
		Key: RequestKeyAdd,
		Data: &topicPartition{
			Topic:     topic,
			Partition: partition,
		},
	}

	return c.handleRequest(request, nil)
}

// Remove stops consuming a topic/partition by associated consumer once it is done with the current batch.
// This means the PartitionConsumer will stop accepting new batches but will have a chance to finish its current work.
// Returns an error if PartitionConsumer for this topic/partition does not exist.
func (c *Client) Remove(topic string, partition int32) error {
	request := &clientRequest{
		Key: RequestKeyRemove,
		Data: &topicPartition{
			Topic:     topic,
			Partition: partition,
		},
	}

	return c.handleRequest(request, nil)
}

// Assignment returns a map of topic/partitions being consumed at the moment by associated consumer.
// The keys are topic names and values are slices of partitions.
func (c *Client) Assignment() (map[string][]int32, error) {
	request := &clientRequest{
		Key: RequestKeyAssignments,
	}

	assignments := make(map[string][]int32)
	err := c.handleRequest(request, &assignments)
	return assignments, err
}

// Offset returns the current consuming offset for a given topic/partition.
// Please note that this value does not correspond to the latest committed offset but the latest fetched offset.
// This call will return an error if the PartitionConsumer for given topic/partition does not exist.
func (c *Client) Offset(topic string, partition int32) (int64, error) {
	request := &clientRequest{
		Key: RequestKeyOffset,
		Data: &topicPartition{
			Topic:     topic,
			Partition: partition,
		},
	}

	offset := int64(-1)
	err := c.handleRequest(request, &offset)
	return offset, err
}

// Commit commits the given offset for a given topic/partition to Kafka.
// Returns an error if the commit was unsuccessful.
func (c *Client) Commit(topic string, partition int32, offset int64) error {
	request := &clientRequest{
		Key: RequestKeyCommit,
		Data: &topicPartitionOffset{
			Topic:     topic,
			Partition: partition,
			Offset:    offset,
		},
	}

	return c.handleRequest(request, nil)
}

// SetOffset overrides the current fetch offset value for given topic/partition.
// This does not commit offset but allows you to move back and forth throughout the partition.
// Returns an error if the PartitionConsumer for this topic/partition does not exist.
func (c *Client) SetOffset(topic string, partition int32, offset int64) error {
	request := &clientRequest{
		Key: RequestKeySetOffset,
		Data: &topicPartitionOffset{
			Topic:     topic,
			Partition: partition,
			Offset:    offset,
		},
	}

	return c.handleRequest(request, nil)
}

// Lag returns the difference between the latest available offset in the partition and the
// latest fetched offset by associated consumer. This allows you to see how much behind the consumer is.
// Returns lag value for a given topic/partition and an error if the PartitionConsumer for given
// topic/partition does not exist.
func (c *Client) Lag(topic string, partition int32) (int64, error) {
	request := &clientRequest{
		Key: RequestKeyLag,
		Data: &topicPartition{
			Topic:     topic,
			Partition: partition,
		},
	}

	lag := int64(-1)
	err := c.handleRequest(request, &lag)
	return lag, err
}

// CustomRequest allows to add any custom actions on the consumer.
func (c *Client) CustomRequest(key string, data interface{}) ([]byte, error) {
	request := &clientRequest{
		Key:  key,
		Data: data,
	}

	response := make(json.RawMessage, 0)
	err := c.handleRequest(request, &response)
	return []byte(response), err
}

func (c *Client) handleRequest(request *clientRequest, responseData interface{}) error {
	requestBytes, err := json.Marshal(request)
	if err != nil {
		return err
	}

	_, err = c.connection.Write(append(requestBytes, messageDelimiter))
	if err != nil {
		return err
	}

	rawResponse, err := c.responseReader.ReadSlice(messageDelimiter)
	if err != nil {
		return err
	}

	response := new(Response)
	response.Data = responseData
	err = json.Unmarshal(rawResponse, &response)
	if err != nil {
		return err
	}

	if !response.Success {
		return errors.New(response.Message)
	}

	return nil
}
