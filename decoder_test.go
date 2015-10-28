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

import (
	"gopkg.in/stretchr/testify.v1/assert"
	"testing"
)

func TestDecoders(t *testing.T) {
	// normal bytes
	bytes := []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05}
	byteDecoder := new(ByteDecoder)
	decodedBytes, err := byteDecoder.Decode(bytes)

	assert.Equal(t, nil, err)
	assert.Equal(t, bytes, decodedBytes)
	//	assert(t, decodedBytes, bytes)

	// empty bytes
	bytes = nil
	decodedBytes, err = byteDecoder.Decode(bytes)

	assert.Equal(t, nil, err)
	assert.Equal(t, bytes, decodedBytes)
	//	assert(t, decodedBytes, bytes)

	// normal string
	str := "hello world"
	stringDecoder := new(StringDecoder)
	decodedString, err := stringDecoder.Decode([]byte(str))

	assert.Equal(t, nil, err)
	assert.Equal(t, str, decodedString)
	//	assert(t, decodedString, str)

	// empty string
	str = ""
	decodedString, err = stringDecoder.Decode([]byte(str))

	assert.Equal(t, nil, err)
	assert.Equal(t, str, decodedString)
	//	assert(t, decodedString, str)
}
