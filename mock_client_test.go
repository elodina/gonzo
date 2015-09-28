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
	"fmt"
	"github.com/stealthly/siesta"
	"testing"
)

func TestMockClientGoodFetch(t *testing.T) {
	client := NewMockClient(0, 200)
	response, err := client.Fetch("test", 0, 0)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}

	if len(response.Data) != 1 {
		t.Fatalf("response.Data length should be 1, actual %s", len(response.Data))
	}

	if len(response.Data["test"]) != 1 {
		t.Fatalf(`response.Data["test"] length should be 1, actual %s`, len(response.Data["test"]))
	}

	if response.Data["test"][0].Error != siesta.ErrNoError {
		t.Fatalf(`Unexpected error at response.Data["test"][0].Error: %s`, response.Data["test"][0].Error)
	}

	if len(response.Data["test"][0].Messages) != int(client.fetchSize) {
		t.Fatalf(`response.Data["test"][0].Messages length should be 100, actual %d`, len(response.Data["test"][0].Messages))
	}

	if response.Data["test"][0].HighwaterMarkOffset != client.highwaterMarkOffset {
		t.Fatalf(`response.Data["test"][0].HighwaterMarkOffset should be 200, actual %d`, response.Data["test"][0].HighwaterMarkOffset)
	}

	for i := 0; i < 100; i++ {
		if response.Data["test"][0].Messages[i].Offset != int64(i) {
			t.Fatalf(`response.Data["test"][0].Messages[%d].Offset length should be %d, actual %d`, i, i, response.Data["test"][0].Messages[i].Offset)
		}

		if string(response.Data["test"][0].Messages[i].Message.Value) != fmt.Sprintf("message-%d", i) {
			t.Fatalf(`string(response.Data["test"][0].Messages[i].Message.Value) should be %s, actual %s`, fmt.Sprintf("message-%d", i), string(response.Data["test"][0].Messages[i].Message.Value))
		}
	}
}

func TestMockClientBadFetch(t *testing.T) {
	client := NewMockClient(0, 100)
	client.fetchResponseError = siesta.ErrBrokerNotAvailable
	response, err := client.Fetch("test", 0, 0)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}

	if len(response.Data) != 1 {
		t.Fatalf("response.Data length should be 1, actual %s", len(response.Data))
	}

	if len(response.Data["test"]) != 1 {
		t.Fatalf(`response.Data["test"] length should be 1, actual %s`, len(response.Data["test"]))
	}

	if response.Data["test"][0].Error != siesta.ErrBrokerNotAvailable {
		t.Fatalf(`response.Data["test"][0].Error should be %s, actual %s`, siesta.ErrBrokerNotAvailable, response.Data["test"][0].Error)
	}

	response.GetMessages()
}
