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
	"encoding/json"
	"errors"
	"github.com/elodina/gonzo"
	"gopkg.in/stretchr/testify.v1/assert"
	"regexp"
	"testing"
	"time"
)

var addr = "0.0.0.0:61721"
var timeout = time.Second

func TestTCPConsumerStart(t *testing.T) {
	gonzo.Logger = gonzo.NewDefaultLogger(gonzo.DebugLevel)
	consumer := NewMockConsumer()

	tcpConsumer := NewConsumer(addr, consumer)
	err := tcpConsumer.Start()
	assert.Equal(t, nil, err)

	tcpConsumer.Stop()
}

func TestTCPConsumerStartBadCases(t *testing.T) {
	consumer := NewMockConsumer()

	tcpConsumer := NewConsumer("qiuweyqjw", consumer)
	err := tcpConsumer.Start()
	assert.Regexp(t, regexp.MustCompile("missing port.*"), err.Error())

	tcpConsumer = NewConsumer("0.0.0.0:qwe", consumer)
	err = tcpConsumer.Start()
	assert.Regexp(t, regexp.MustCompile("unknown port.*"), err.Error())

	tcpConsumer = NewConsumer("0.0.0.0:22", consumer)
	err = tcpConsumer.Start()
	assert.NotEqual(t, nil, err.Error())

	_, err = NewClient(addr)
	assert.Regexp(t, regexp.MustCompile(".*connection refused.*"), err.Error())
}

func TestTCPConsumerAdd(t *testing.T) {
	consumer := NewMockConsumer()

	tcpConsumer := NewConsumer(addr, consumer)
	err := tcpConsumer.Start()
	assert.Equal(t, nil, err)

	client, err := NewClient(addr)
	assert.Equal(t, nil, err)

	// good case
	err = client.Add("asd", 1)
	assert.Equal(t, nil, err)

	// add existing
	err = client.Add("asd", 1)
	assert.Regexp(t, regexp.MustCompile(".*already exists.*"), err.Error())

	tcpConsumer.Stop()
}

func TestTCPConsumerRemove(t *testing.T) {
	consumer := NewMockConsumer()

	tcpConsumer := NewConsumer(addr, consumer)
	err := tcpConsumer.Start()
	assert.Equal(t, nil, err)

	client, err := NewClient(addr)
	assert.Equal(t, nil, err)

	// add first
	err = client.Add("asd", 1)
	assert.Equal(t, nil, err)

	// good case
	err = client.Remove("asd", 1)
	assert.Equal(t, nil, err)

	// remove non-existing
	err = client.Remove("asd", 1)
	assert.Regexp(t, regexp.MustCompile(".*does not exist.*"), err.Error())

	tcpConsumer.Stop()
}

func TestTCPConsumerAssignments(t *testing.T) {
	consumer := NewMockConsumer()

	tcpConsumer := NewConsumer(addr, consumer)
	err := tcpConsumer.Start()
	assert.Equal(t, nil, err)

	client, err := NewClient(addr)
	assert.Equal(t, nil, err)

	// empty assignments
	assignments, err := client.Assignment()
	assert.Equal(t, nil, err)
	assert.Empty(t, assignments)

	// add one
	err = client.Add("asd", 1)
	assert.Equal(t, nil, err)

	// check now
	assignments, err = client.Assignment()
	assert.Equal(t, nil, err)
	assert.Len(t, assignments, 1)

	// remove that one and check again
	err = client.Remove("asd", 1)
	assert.Equal(t, nil, err)

	assignments, err = client.Assignment()
	assert.Equal(t, nil, err)
	assert.Empty(t, assignments)

	tcpConsumer.Stop()
}

func TestTCPConsumerCommit(t *testing.T) {
	consumer := NewMockConsumer()

	tcpConsumer := NewConsumer(addr, consumer)
	err := tcpConsumer.Start()
	assert.Equal(t, nil, err)

	client, err := NewClient(addr)
	assert.Equal(t, nil, err)

	// commit something
	err = client.Commit("asd", 1, 123)
	assert.Equal(t, nil, err)

	// commit error
	consumer.commitOffsetError = errors.New("boom")
	err = client.Commit("asd", 1, 123)
	assert.Regexp(t, regexp.MustCompile(".*boom.*"), err)

	tcpConsumer.Stop()
}

func TestTCPConsumerOffset(t *testing.T) {
	consumer := NewMockConsumer()

	tcpConsumer := NewConsumer(addr, consumer)
	err := tcpConsumer.Start()
	assert.Equal(t, nil, err)

	client, err := NewClient(addr)
	assert.Equal(t, nil, err)

	// non-existing
	_, err = client.Offset("asd", 1)
	assert.Regexp(t, regexp.MustCompile(".*does not exist.*"), err.Error())

	// add first
	err = client.Add("asd", 1)
	assert.Equal(t, nil, err)

	// check again
	offset, err := client.Offset("asd", 1)
	assert.Equal(t, nil, err)
	assert.Equal(t, int64(0), offset)

	// commit offset
	err = client.Commit("asd", 1, 234)
	assert.Equal(t, nil, err)

	// check once more
	offset, err = client.Offset("asd", 1)
	assert.Equal(t, nil, err)
	assert.Equal(t, int64(234), offset)

	tcpConsumer.Stop()
}

func TestTCPConsumerSetOffset(t *testing.T) {
	consumer := NewMockConsumer()

	tcpConsumer := NewConsumer(addr, consumer)
	err := tcpConsumer.Start()
	assert.Equal(t, nil, err)

	client, err := NewClient(addr)
	assert.Equal(t, nil, err)

	// non-existing
	err = client.SetOffset("asd", 1, 123)
	assert.Regexp(t, regexp.MustCompile(".*does not exist.*"), err.Error())

	// add first
	err = client.Add("asd", 1)
	assert.Equal(t, nil, err)

	// check again
	err = client.SetOffset("asd", 1, 123)
	assert.Equal(t, nil, err)

	// get offset
	offset, err := client.Offset("asd", 1)
	assert.Equal(t, nil, err)
	assert.Equal(t, int64(123), offset)

	tcpConsumer.Stop()
}

func TestTCPConsumerLag(t *testing.T) {
	consumer := NewMockConsumer()

	tcpConsumer := NewConsumer(addr, consumer)
	err := tcpConsumer.Start()
	assert.Equal(t, nil, err)

	client, err := NewClient(addr)
	assert.Equal(t, nil, err)

	// non-existing
	_, err = client.Lag("asd", 1)
	assert.Regexp(t, regexp.MustCompile(".*does not exist.*"), err.Error())

	// add first
	err = client.Add("asd", 1)
	assert.Equal(t, nil, err)

	// check again
	lag, err := client.Lag("asd", 1)
	assert.Equal(t, nil, err)
	assert.Equal(t, int64(100), lag)

	tcpConsumer.Stop()
}

func TestTCPConsumerAwaitTermination(t *testing.T) {
	consumer := NewMockConsumer()

	tcpConsumer := NewConsumer(addr, consumer)
	err := tcpConsumer.Start()
	assert.Equal(t, nil, err)

	success := make(chan struct{})
	go func() {
		tcpConsumer.AwaitTermination()
		success <- struct{}{}
	}()

	select {
	case <-success:
		t.Fatal("Await termination shouldn't unblock until consumer is not stopped")
	case <-time.After(timeout):
	}

	tcpConsumer.Stop()
	select {
	case <-success:
	case <-time.After(timeout):
		t.Fatalf("Await termination failed to unblock within %s", timeout)
	}
}

func TestHttpConsumerJoin(t *testing.T) {
	consumer := NewMockConsumer()

	tcpConsumer := NewConsumer(addr, consumer)
	err := tcpConsumer.Start()
	assert.Equal(t, nil, err)

	client, err := NewClient(addr)
	assert.Equal(t, nil, err)

	// add one
	err = client.Add("asd", 1)
	assert.Equal(t, nil, err)

	success := make(chan struct{})
	go func() {
		tcpConsumer.Join()
		success <- struct{}{}
	}()

	select {
	case <-success:
		t.Fatal("Join shouldn't unblock until consumer has something to consume")
	case <-time.After(timeout):
	}

	// remove that one
	err = client.Remove("asd", 1)
	assert.Equal(t, nil, err)

	select {
	case <-success:
	case <-time.After(timeout):
		t.Fatalf("Join failed to unblock within %s", timeout)
	}

	tcpConsumer.Stop()
}

func TestTCPConsumerCustomHandler(t *testing.T) {
	consumer := NewMockConsumer()

	tcpConsumer := NewConsumer(addr, consumer)
	err := tcpConsumer.Start()
	assert.Equal(t, nil, err)

	tcpConsumer.RegisterCustomHandler("custom", func(body []byte) (*Response, error) {
		return NewResponse(true, "", "hello!"), nil
	})

	client, err := NewClient(addr)
	assert.Equal(t, nil, err)

	response, err := client.CustomRequest("custom", nil)
	assert.Equal(t, nil, err)
	var responseValue string
	err = json.Unmarshal(response, &responseValue)
	assert.Equal(t, nil, err)
	assert.Equal(t, "hello!", responseValue)

	tcpConsumer.Stop()
}

func TestTCPConsumerUnknownKey(t *testing.T) {
	consumer := NewMockConsumer()

	tcpConsumer := NewConsumer(addr, consumer)
	err := tcpConsumer.Start()
	assert.Equal(t, nil, err)

	client, err := NewClient(addr)
	assert.Equal(t, nil, err)

	_, err = client.CustomRequest("foobar", nil)
	assert.Regexp(t, regexp.MustCompile("Unknown request key.*"), err.Error())

	tcpConsumer.Stop()
}
