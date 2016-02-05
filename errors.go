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

package gonzo

import "errors"

// ErrPartitionConsumerDoesNotExist is used when trying to perform any action on a topic/partition that is not
// owned by the given Consumer.
var ErrPartitionConsumerDoesNotExist = errors.New("Partition consumer does not exist")

// ErrPartitionConsumerAlreadyExists is used when trying to add a topic/partition that is already added to the
// given Consumer.
var ErrPartitionConsumerAlreadyExists = errors.New("Partition consumer already exists")

// ErrMetricsDisabled is used when trying to get consumer metrics while they are disabled.
var ErrMetricsDisabled = errors.New("Metrics are disabled. Use ConsumerConfig.EnableMetrics to enable")
