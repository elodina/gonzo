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
	"github.com/elodina/gonzo"
	"net/http"
	"strconv"
)

const (
	ParamTopic     = "topic"
	ParamPartition = "partition"
	ParamOffset    = "offset"
)

type HttpConsumer struct {
	address  string
	serveMux *http.ServeMux
	consumer gonzo.Consumer
}

func NewHttpConsumer(addr string, consumer gonzo.Consumer) *HttpConsumer {
	return &HttpConsumer{
		address:  addr,
		serveMux: http.NewServeMux(),
		consumer: consumer,
	}
}

func (hc *HttpConsumer) Start() error {
	hc.serveMux.HandleFunc("/add", hc.handleAdd)
	hc.serveMux.HandleFunc("/remove", hc.handleRemove)
	hc.serveMux.HandleFunc("/assignments", hc.handleAssignments)
	hc.serveMux.HandleFunc("/offset", hc.handleOffset)
	hc.serveMux.HandleFunc("/commit", hc.handleCommit)
	hc.serveMux.HandleFunc("/setoffset", hc.handleSetOffset)
	hc.serveMux.HandleFunc("/lag", hc.handleLag)

	return http.ListenAndServe(hc.address, hc.serveMux)
}

func (hc *HttpConsumer) Stop() {
	hc.consumer.Stop()
}

func (hc *HttpConsumer) AwaitTermination() {
	hc.consumer.AwaitTermination()
}

func (hc *HttpConsumer) Join() {
	hc.consumer.Join()
}

func (hc *HttpConsumer) RegisterCustomHandler(pattern string, handler func(http.ResponseWriter, *http.Request, gonzo.Consumer)) {
	hc.serveMux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
		handler(w, r, hc.consumer)
	})
}

func (hc *HttpConsumer) handleAdd(w http.ResponseWriter, r *http.Request) {
	topic, err := hc.getString(r, ParamTopic)
	if err != nil {
		gonzo.Logger.Info(err.Error())
		hc.respond(w, http.StatusBadRequest, err.Error())
		return
	}
	partition, err := hc.getInt32(r, ParamPartition)
	if err != nil {
		gonzo.Logger.Info(err.Error())
		hc.respond(w, http.StatusBadRequest, err.Error())
		return
	}

	err = hc.consumer.Add(topic, partition)
	if err != nil {
		hc.respond(w, http.StatusBadRequest, err.Error())
		return
	}

	hc.respond(w, http.StatusOK, nil)
}

func (hc *HttpConsumer) handleRemove(w http.ResponseWriter, r *http.Request) {
	topic, err := hc.getString(r, ParamTopic)
	if err != nil {
		gonzo.Logger.Info(err.Error())
		hc.respond(w, http.StatusBadRequest, err.Error())
		return
	}
	partition, err := hc.getInt32(r, ParamPartition)
	if err != nil {
		gonzo.Logger.Info(err.Error())
		hc.respond(w, http.StatusBadRequest, err.Error())
		return
	}

	err = hc.consumer.Remove(topic, partition)
	if err != nil {
		hc.respond(w, http.StatusBadRequest, err.Error())
		return
	}

	hc.respond(w, http.StatusOK, nil)
}

func (hc *HttpConsumer) handleAssignments(w http.ResponseWriter, r *http.Request) {
	hc.respond(w, http.StatusOK, hc.consumer.Assignment())
}

func (hc *HttpConsumer) handleOffset(w http.ResponseWriter, r *http.Request) {
	topic, err := hc.getString(r, ParamTopic)
	if err != nil {
		gonzo.Logger.Info(err.Error())
		hc.respond(w, http.StatusBadRequest, err.Error())
		return
	}
	partition, err := hc.getInt32(r, ParamPartition)
	if err != nil {
		gonzo.Logger.Info(err.Error())
		hc.respond(w, http.StatusBadRequest, err.Error())
		return
	}

	offset, err := hc.consumer.Offset(topic, partition)
	if err != nil {
		hc.respond(w, http.StatusBadRequest, err.Error())
		return
	}

	hc.respond(w, http.StatusOK, offset)
}

func (hc *HttpConsumer) handleCommit(w http.ResponseWriter, r *http.Request) {
	topic, err := hc.getString(r, ParamTopic)
	if err != nil {
		gonzo.Logger.Info(err.Error())
		hc.respond(w, http.StatusBadRequest, err.Error())
		return
	}
	partition, err := hc.getInt32(r, ParamPartition)
	if err != nil {
		gonzo.Logger.Info(err.Error())
		hc.respond(w, http.StatusBadRequest, err.Error())
		return
	}
	offset, err := hc.getInt64(r, ParamOffset)
	if err != nil {
		gonzo.Logger.Info(err.Error())
		hc.respond(w, http.StatusBadRequest, err.Error())
		return
	}

	err = hc.consumer.Commit(topic, partition, offset)
	if err != nil {
		hc.respond(w, http.StatusBadRequest, err.Error())
		return
	}

	hc.respond(w, http.StatusOK, nil)
}

func (hc *HttpConsumer) handleSetOffset(w http.ResponseWriter, r *http.Request) {
	topic, err := hc.getString(r, ParamTopic)
	if err != nil {
		gonzo.Logger.Info(err.Error())
		hc.respond(w, http.StatusBadRequest, err.Error())
		return
	}
	partition, err := hc.getInt32(r, ParamPartition)
	if err != nil {
		gonzo.Logger.Info(err.Error())
		hc.respond(w, http.StatusBadRequest, err.Error())
		return
	}
	offset, err := hc.getInt64(r, ParamOffset)
	if err != nil {
		gonzo.Logger.Info(err.Error())
		hc.respond(w, http.StatusBadRequest, err.Error())
		return
	}

	err = hc.consumer.SetOffset(topic, partition, offset)
	if err != nil {
		hc.respond(w, http.StatusBadRequest, err.Error())
		return
	}

	hc.respond(w, http.StatusOK, nil)
}

func (hc *HttpConsumer) handleLag(w http.ResponseWriter, r *http.Request) {
	topic, err := hc.getString(r, ParamTopic)
	if err != nil {
		gonzo.Logger.Info(err.Error())
		hc.respond(w, http.StatusBadRequest, err.Error())
		return
	}
	partition, err := hc.getInt32(r, ParamPartition)
	if err != nil {
		gonzo.Logger.Info(err.Error())
		hc.respond(w, http.StatusBadRequest, err.Error())
		return
	}

	lag, err := hc.consumer.Lag(topic, partition)
	if err != nil {
		hc.respond(w, http.StatusBadRequest, err.Error())
		return
	}

	hc.respond(w, http.StatusOK, lag)
}

func (hc *HttpConsumer) getString(r *http.Request, name string) (string, error) {
	value := r.URL.Query().Get(name)
	if value == "" {
		return "", fmt.Errorf("Parameter '%s' is not present in request query string.", name)
	}

	return value, nil
}

func (hc *HttpConsumer) getInt(r *http.Request, name string) (int, error) {
	str, err := hc.getString(r, name)
	if err != nil {
		return -1, err
	}

	value, err := strconv.Atoi(str)
	if err != nil {
		return -1, err
	}
	return value, nil
}

func (hc *HttpConsumer) getInt32(r *http.Request, name string) (int32, error) {
	value, err := hc.getInt(r, name)
	return int32(value), err
}

func (hc *HttpConsumer) getInt64(r *http.Request, name string) (int64, error) {
	value, err := hc.getInt(r, name)
	return int64(value), err
}

func (hc *HttpConsumer) respond(w http.ResponseWriter, status int, body interface{}) {
	w.WriteHeader(status)

	bytes, err := json.Marshal(body)
	if err != nil {
		panic(err)
	}

	w.Write(bytes)
}
