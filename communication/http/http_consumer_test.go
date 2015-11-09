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

package http

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/elodina/gonzo"
	"gopkg.in/stretchr/testify.v1/assert"
	"net/http"
	"regexp"
	"testing"
	"time"
)

func TestHttpConsumerStartBadCases(t *testing.T) {
	consumer := NewMockConsumer()

	httpConsumer := NewHttpConsumer("qiuweyqjw", consumer)
	err := httpConsumer.Start()
	assert.Regexp(t, regexp.MustCompile("listen tcp.*"), err.Error())

	httpConsumer = NewHttpConsumer("0.0.0.0:qwe", consumer)
	err = httpConsumer.Start()
	assert.Regexp(t, regexp.MustCompile("listen tcp.*"), err.Error())

	httpConsumer = NewHttpConsumer("", consumer)
	err = httpConsumer.Start()
	assert.Regexp(t, regexp.MustCompile("listen tcp.*"), err.Error())
}

func TestHttpConsumer(t *testing.T) {
	addr := "0.0.0.0:61721"

	consumer := NewMockConsumer()
	httpConsumer := NewHttpConsumer(addr, consumer)
	go func() {
		err := httpConsumer.Start()
		assert.Equal(t, nil, err)
	}()

	testHttpConsumerAdd(t, addr)
	testHttpConsumerRemove(t, addr)
	testHttpConsumerAssignments(t, addr)
	testHttpConsumerCommit(t, addr, consumer)
	testHttpConsumerGetOffset(t, addr)
	testHttpConsumerSetOffset(t, addr)
	testHttpConsumerLag(t, addr)
	testHttpConsumerCustomHandler(t, addr, httpConsumer)
	testHttpConsumerAwaitTermination(t, httpConsumer)
}

func testHttpConsumerAdd(t *testing.T, addr string) {
	// good case
	_, err := http.Get(fmt.Sprintf("http://%s/add?topic=asd&partition=1", addr))
	assert.Equal(t, nil, err)

	// add existing
	rawResponse, err := http.Get(fmt.Sprintf("http://%s/add?topic=asd&partition=1", addr))
	assert.Equal(t, nil, err)
	response, err := ReadHttpResponse(rawResponse, nil)
	assert.Regexp(t, regexp.MustCompile(".*already exists.*"), response.Message)

	// missing topic
	rawResponse, err = http.Get(fmt.Sprintf("http://%s/add?partition=1", addr))
	assert.Equal(t, nil, err)
	response, err = ReadHttpResponse(rawResponse, nil)
	assert.Regexp(t, regexp.MustCompile(".*'topic' is not present.*"), response.Message)

	// missing partition
	rawResponse, err = http.Get(fmt.Sprintf("http://%s/add?topic=asd", addr))
	assert.Equal(t, nil, err)
	response, err = ReadHttpResponse(rawResponse, nil)
	assert.Regexp(t, regexp.MustCompile(".*'partition' is not present.*"), response.Message)

	// partition not int
	rawResponse, err = http.Get(fmt.Sprintf("http://%s/add?topic=asd&partition=zxc", addr))
	assert.Equal(t, nil, err)
	response, err = ReadHttpResponse(rawResponse, nil)
	assert.Regexp(t, regexp.MustCompile(".*strconv.ParseInt.*"), response.Message) // strconv.ParseInt error message
}

func testHttpConsumerRemove(t *testing.T, addr string) {
	// good case
	_, err := http.Get(fmt.Sprintf("http://%s/remove?topic=asd&partition=1", addr))
	assert.Equal(t, nil, err)

	// remove non-existing
	rawResponse, err := http.Get(fmt.Sprintf("http://%s/remove?topic=asd&partition=1", addr))
	assert.Equal(t, nil, err)
	response, err := ReadHttpResponse(rawResponse, nil)
	assert.Regexp(t, regexp.MustCompile(".*does not exist.*"), response.Message)

	// missing topic
	rawResponse, err = http.Get(fmt.Sprintf("http://%s/remove?partition=1", addr))
	assert.Equal(t, nil, err)
	response, err = ReadHttpResponse(rawResponse, nil)
	assert.Regexp(t, regexp.MustCompile(".*'topic' is not present.*"), response.Message)

	// missing partition
	rawResponse, err = http.Get(fmt.Sprintf("http://%s/remove?topic=asd", addr))
	assert.Equal(t, nil, err)
	response, err = ReadHttpResponse(rawResponse, nil)
	assert.Regexp(t, regexp.MustCompile(".*'partition' is not present.*"), response.Message)

	// partition not int
	rawResponse, err = http.Get(fmt.Sprintf("http://%s/remove?topic=asd&partition=zxc", addr))
	assert.Equal(t, nil, err)
	response, err = ReadHttpResponse(rawResponse, nil)
	assert.Regexp(t, regexp.MustCompile(".*strconv.ParseInt.*"), response.Message) // strconv.ParseInt error message
}

func testHttpConsumerAssignments(t *testing.T, addr string) {
	// empty assignments
	rawResponse, err := http.Get(fmt.Sprintf("http://%s/assignments", addr))
	assert.Equal(t, nil, err)
	r := make(map[string][]int32)
	response, err := ReadHttpResponse(rawResponse, &r)
	assert.Equal(t, nil, err)
	assert.Len(t, response.Data.(map[string][]int32), 0)

	// add one
	_, err = http.Get(fmt.Sprintf("http://%s/add?topic=asd&partition=1", addr))
	assert.Equal(t, nil, err)

	// check now
	rawResponse, err = http.Get(fmt.Sprintf("http://%s/assignments", addr))
	assert.Equal(t, nil, err)
	r = make(map[string][]int32)
	response, err = ReadHttpResponse(rawResponse, &r)
	assert.Equal(t, nil, err)
	assert.Len(t, response.Data.(map[string][]int32), 1)

	// remove that one and check again
	_, err = http.Get(fmt.Sprintf("http://%s/remove?topic=asd&partition=1", addr))
	assert.Equal(t, nil, err)

	rawResponse, err = http.Get(fmt.Sprintf("http://%s/assignments", addr))
	assert.Equal(t, nil, err)
	r = make(map[string][]int32)
	response, err = ReadHttpResponse(rawResponse, &r)
	assert.Equal(t, nil, err)
	assert.Len(t, response.Data.(map[string][]int32), 0)
}

func testHttpConsumerCommit(t *testing.T, addr string, consumer *MockConsumer) {
	// commit something
	rawResponse, err := http.Get(fmt.Sprintf("http://%s/commit?topic=asd&partition=1&offset=123", addr))
	assert.Equal(t, nil, err)
	response, err := ReadHttpResponse(rawResponse, nil)
	assert.True(t, response.Success)

	// missing topic
	rawResponse, err = http.Get(fmt.Sprintf("http://%s/commit?&partition=1", addr))
	assert.Equal(t, nil, err)
	response, err = ReadHttpResponse(rawResponse, nil)
	assert.Regexp(t, regexp.MustCompile(".*'topic' is not present.*"), response.Message)

	// missing partition
	rawResponse, err = http.Get(fmt.Sprintf("http://%s/commit?topic=asd", addr))
	assert.Equal(t, nil, err)
	response, err = ReadHttpResponse(rawResponse, nil)
	assert.Regexp(t, regexp.MustCompile(".*'partition' is not present.*"), response.Message)

	// missing offset
	rawResponse, err = http.Get(fmt.Sprintf("http://%s/commit?topic=asd&partition=1", addr))
	assert.Equal(t, nil, err)
	response, err = ReadHttpResponse(rawResponse, nil)
	assert.Regexp(t, regexp.MustCompile(".*'offset' is not present.*"), response.Message)

	// commit error
	consumer.commitOffsetError = errors.New("boom!")
	rawResponse, err = http.Get(fmt.Sprintf("http://%s/commit?topic=asd&partition=1&offset=123", addr))
	assert.Equal(t, nil, err)
	response, err = ReadHttpResponse(rawResponse, nil)
	assert.Regexp(t, regexp.MustCompile(".*boom.*"), response.Message)
	consumer.commitOffsetError = nil
}

func testHttpConsumerGetOffset(t *testing.T, addr string) {
	// remove previous
	_, err := http.Get(fmt.Sprintf("http://%s/remove?topic=asd&partition=1", addr))
	assert.Equal(t, nil, err)

	// non-existing
	rawResponse, err := http.Get(fmt.Sprintf("http://%s/offset?topic=asd&partition=1", addr))
	assert.Equal(t, nil, err)
	response, err := ReadHttpResponse(rawResponse, nil)
	assert.Regexp(t, regexp.MustCompile(".*does not exist.*"), response.Message)

	// add first
	_, err = http.Get(fmt.Sprintf("http://%s/add?topic=asd&partition=1", addr))
	assert.Equal(t, nil, err)

	// check again
	rawResponse, err = http.Get(fmt.Sprintf("http://%s/offset?topic=asd&partition=1", addr))
	assert.Equal(t, nil, err)
	offset := int64(-1)
	response, err = ReadHttpResponse(rawResponse, &offset)
	assert.Equal(t, nil, err)
	assert.Equal(t, int64(123), response.Data.(int64))

	// commit offset
	_, err = http.Get(fmt.Sprintf("http://%s/commit?topic=asd&partition=1&offset=234", addr))
	assert.Equal(t, nil, err)

	// check once more
	rawResponse, err = http.Get(fmt.Sprintf("http://%s/offset?topic=asd&partition=1", addr))
	assert.Equal(t, nil, err)
	offset = int64(-1)
	response, err = ReadHttpResponse(rawResponse, &offset)
	assert.Equal(t, nil, err)
	assert.Equal(t, int64(234), response.Data.(int64))

	// missing topic
	rawResponse, err = http.Get(fmt.Sprintf("http://%s/offset?partition=1", addr))
	assert.Equal(t, nil, err)
	response, err = ReadHttpResponse(rawResponse, nil)
	assert.Regexp(t, regexp.MustCompile(".*'topic' is not present.*"), response.Message)

	// missing partition
	rawResponse, err = http.Get(fmt.Sprintf("http://%s/offset?topic=asd", addr))
	assert.Equal(t, nil, err)
	response, err = ReadHttpResponse(rawResponse, nil)
	assert.Regexp(t, regexp.MustCompile(".*'partition' is not present.*"), response.Message)
}

func testHttpConsumerSetOffset(t *testing.T, addr string) {
	// remove previous
	_, err := http.Get(fmt.Sprintf("http://%s/remove?topic=asd&partition=1", addr))
	assert.Equal(t, nil, err)

	// non-existing
	rawResponse, err := http.Get(fmt.Sprintf("http://%s/setoffset?topic=asd&partition=1&offset=123", addr))
	assert.Equal(t, nil, err)
	response, err := ReadHttpResponse(rawResponse, nil)
	assert.Regexp(t, regexp.MustCompile(".*does not exist.*"), response.Message)

	// add first
	_, err = http.Get(fmt.Sprintf("http://%s/add?topic=asd&partition=1", addr))
	assert.Equal(t, nil, err)

	// check again
	rawResponse, err = http.Get(fmt.Sprintf("http://%s/setoffset?topic=asd&partition=1&offset=123", addr))
	assert.Equal(t, nil, err)
	response, err = ReadHttpResponse(rawResponse, nil)
	assert.Equal(t, nil, err)
	assert.Equal(t, true, response.Success)

	// get offset
	rawResponse, err = http.Get(fmt.Sprintf("http://%s/offset?topic=asd&partition=1", addr))
	assert.Equal(t, nil, err)
	offset := int64(-1)
	response, err = ReadHttpResponse(rawResponse, &offset)
	assert.Equal(t, nil, err)
	assert.Equal(t, int64(123), response.Data.(int64))

	// missing topic
	rawResponse, err = http.Get(fmt.Sprintf("http://%s/setoffset?partition=1", addr))
	assert.Equal(t, nil, err)
	response, err = ReadHttpResponse(rawResponse, nil)
	assert.Regexp(t, regexp.MustCompile(".*'topic' is not present.*"), response.Message)

	// missing partition
	rawResponse, err = http.Get(fmt.Sprintf("http://%s/setoffset?topic=asd", addr))
	assert.Equal(t, nil, err)
	response, err = ReadHttpResponse(rawResponse, nil)
	assert.Regexp(t, regexp.MustCompile(".*'partition' is not present.*"), response.Message)

	// missing offset
	rawResponse, err = http.Get(fmt.Sprintf("http://%s/setoffset?topic=asd&partition=1", addr))
	assert.Equal(t, nil, err)
	response, err = ReadHttpResponse(rawResponse, nil)
	assert.Regexp(t, regexp.MustCompile(".*'offset' is not present.*"), response.Message)
}

func testHttpConsumerLag(t *testing.T, addr string) {
	// remove previous
	_, err := http.Get(fmt.Sprintf("http://%s/remove?topic=asd&partition=1", addr))
	assert.Equal(t, nil, err)

	// non-existing
	rawResponse, err := http.Get(fmt.Sprintf("http://%s/lag?topic=asd&partition=1", addr))
	assert.Equal(t, nil, err)
	response, err := ReadHttpResponse(rawResponse, nil)
	assert.Regexp(t, regexp.MustCompile(".*does not exist.*"), response.Message)

	// add first
	_, err = http.Get(fmt.Sprintf("http://%s/add?topic=asd&partition=1", addr))
	assert.Equal(t, nil, err)

	// check again
	rawResponse, err = http.Get(fmt.Sprintf("http://%s/lag?topic=asd&partition=1", addr))
	assert.Equal(t, nil, err)
	lag := int64(-1)
	response, err = ReadHttpResponse(rawResponse, &lag)
	assert.Equal(t, nil, err)
	assert.Equal(t, true, response.Success)
	assert.Equal(t, int64(100), lag)

	// missing topic
	rawResponse, err = http.Get(fmt.Sprintf("http://%s/lag?partition=1", addr))
	assert.Equal(t, nil, err)
	response, err = ReadHttpResponse(rawResponse, nil)
	assert.Regexp(t, regexp.MustCompile(".*'topic' is not present.*"), response.Message)

	// missing partition
	rawResponse, err = http.Get(fmt.Sprintf("http://%s/lag?topic=asd", addr))
	assert.Equal(t, nil, err)
	response, err = ReadHttpResponse(rawResponse, nil)
	assert.Regexp(t, regexp.MustCompile(".*'partition' is not present.*"), response.Message)
}

func testHttpConsumerCustomHandler(t *testing.T, addr string, httpConsumer *HttpConsumer) {
	httpConsumer.RegisterCustomHandler("/custom", func(w http.ResponseWriter, r *http.Request, _ gonzo.Consumer) {
		w.WriteHeader(http.StatusOK)
		js, err := json.Marshal("custom!")
		if err != nil {
			t.Fatal(err)
		}
		w.Write(js)
	})

	rawResponse, err := http.Get(fmt.Sprintf("http://%s/custom", addr))
	assert.Equal(t, nil, err)
	stringResponse := ""
	response, err := ReadHttpResponse(rawResponse, &stringResponse)
	assert.Equal(t, nil, err)
	assert.Equal(t, "custom!", response.Data)
}

func testHttpConsumerAwaitTermination(t *testing.T, httpConsumer *HttpConsumer) {
	timeout := time.Second
	success := make(chan struct{})
	go func() {
		httpConsumer.AwaitTermination()
		success <- struct{}{}
	}()

	select {
	case <-success:
		t.Fatal("Await termination shouldn't unblock until consumer is not stopped")
	case <-time.After(timeout):
	}

	httpConsumer.Stop()
	select {
	case <-success:
	case <-time.After(timeout):
		t.Fatalf("Await termination failed to unblock within %s", timeout)
	}
}

func TestHttpConsumerJoin(t *testing.T) {
	timeout := time.Second
	addr := "0.0.0.0:61789"
	consumer := NewMockConsumer()
	httpConsumer := NewHttpConsumer(addr, consumer)
	go func() {
		err := httpConsumer.Start()
		assert.Equal(t, nil, err)
	}()

	// add one
	_, err := http.Get(fmt.Sprintf("http://%s/add?topic=asd&partition=1", addr))
	assert.Equal(t, nil, err)

	success := make(chan struct{})
	go func() {
		httpConsumer.Join()
		success <- struct{}{}
	}()

	select {
	case <-success:
		t.Fatal("Join shouldn't unblock until consumer has something to consume")
	case <-time.After(timeout):
	}

	// remove that one
	_, err = http.Get(fmt.Sprintf("http://%s/remove?topic=asd&partition=1", addr))
	assert.Equal(t, nil, err)

	select {
	case <-success:
	case <-time.After(timeout):
		t.Fatalf("Join failed to unblock within %s", timeout)
	}
}
