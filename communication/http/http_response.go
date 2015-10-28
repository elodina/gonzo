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
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
)

type HttpResponse struct {
	Success bool
	Message string
	Data    interface{}
}

func NewHttpResponse(success bool, message string, data interface{}) *HttpResponse {
	return &HttpResponse{
		Success: success,
		Message: message,
		Data:    data,
	}
}

func ReadHttpResponse(response *http.Response, data interface{}) (*HttpResponse, error) {
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	if response.StatusCode == http.StatusOK {
		rv := reflect.ValueOf(data)

		if data == nil {
			return NewHttpResponse(true, "", nil), nil
		}

		if rv.Kind() != reflect.Ptr {
			return nil, fmt.Errorf("Non-pointer type %T for HttpResponse", data)
		}

		err = json.Unmarshal(body, &data)
		if err != nil {
			return nil, err
		}

		return NewHttpResponse(true, "", rv.Elem().Interface()), nil
	}

	return NewHttpResponse(false, string(body), nil), nil
}
