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
	"bytes"
	"errors"
	"gopkg.in/stretchr/testify.v1/assert"
	"net/http"
	"regexp"
	"testing"
)

type FailingReadCloser struct{}

func NewFailingReadCloser() *FailingReadCloser {
	return new(FailingReadCloser)
}

func (frc *FailingReadCloser) Read(p []byte) (n int, err error) {
	return 0, errors.New("boom!")
}

func (frc *FailingReadCloser) Close() error {
	return errors.New("boom!")
}

type TestReadCloser struct {
	*bytes.Buffer
}

func NewTestReadCloser(buffer []byte) *TestReadCloser {
	return &TestReadCloser{
		Buffer: bytes.NewBuffer(buffer),
	}
}

func (trc *TestReadCloser) Close() error {
	return nil
}

func TestHttpResponse(t *testing.T) {
	response := new(http.Response)
	response.Body = NewFailingReadCloser()
	_, err := ReadHttpResponse(response, nil)
	assert.Regexp(t, regexp.MustCompile("boom!"), err.Error())

	response = new(http.Response)
	response.Body = NewTestReadCloser([]byte(`{{`)) //invalid json
	response.StatusCode = http.StatusOK
	_, err = ReadHttpResponse(response, "asd") // non-pointer
	assert.Regexp(t, regexp.MustCompile(".*Non-pointer.*"), err.Error())

	value := make(map[string]interface{})
	_, err = ReadHttpResponse(response, &value)
	assert.Regexp(t, regexp.MustCompile(".*unexpected end of JSON input.*"), err.Error())
}
