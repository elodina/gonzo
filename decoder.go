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

// Decoder is an interface that is used to decode raw message bytes to something meaningful.
type Decoder interface {
	// Decode decodes given byte slice to something meaningful.
	// Returns an error if fails to decode given bytes.
	Decode([]byte) (interface{}, error)
}

// ByteDecoder is a default decoder implementation that does nothing but returns a plain byte slice.
type ByteDecoder struct{}

// Decode does nothing for ByteDecoder and just returns the input untouched. Never returns an error.
func (*ByteDecoder) Decode(bytes []byte) (interface{}, error) {
	return bytes, nil
}

// StringDecoder turns the given bytes into a string.
type StringDecoder struct{}

// Decode converts the given bytes to string. Never returns an error.
func (*StringDecoder) Decode(bytes []byte) (interface{}, error) {
	return string(bytes), nil
}
