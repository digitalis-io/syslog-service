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

package syslog

import (
	"encoding/json"
	"fmt"
	mesos "github.com/mesos/mesos-go/mesosproto"
	"github.com/yanzay/log"
	"regexp"
)

const (
	TransformNone = "none"
	TransformAvro = "avro"
)

var Config *config = &config{
	FrameworkName: "syslog-kafka",
	FrameworkRole: "*",
	Cpus:          0.1,
	Mem:           64,
	NumProducers:  1,
	ChannelSize:   10000,
	TcpPort:       "auto",
	UdpPort:       "auto",
	Transform:     "none",
}

var executorMask = regexp.MustCompile("executor.*")

type config struct {
	Api                string
	Master             string
	FrameworkName      string
	FrameworkRole      string
	User               string
	Cpus               float64
	Mem                float64
	TcpPort            string
	UdpPort            string
	Executor           string
	BrokerList         string
	ProducerProperties string
	NumProducers       int
	ChannelSize        int
	Topic              string
	Transform          string
	SchemaRegistryUrl  string
	Namespace          string
}

func (c *config) CanStart() bool {
	if c.Transform == TransformAvro && c.SchemaRegistryUrl == "" {
		return false
	}
	return c.Topic != "" && c.BrokerList != ""
}

func (c *config) Read(task *mesos.TaskInfo) {
	config := new(config)
	log.Debugf("Task data: %s", string(task.GetData()))
	err := json.Unmarshal(task.GetData(), config)
	if err != nil {
		log.Fatal(err)
	}
	*c = *config
}

func (c *config) String() string {
	return fmt.Sprintf(`api:                 %s
master:              %s
framework name:      %s
framework role:      %s
user:                %s
cpus:                %.2f
mem:                 %.2f
TCP port:            %s
UDP port:            %s
executor:            %s
producer properties: %s
number of producers: %d
producer buffer:     %d
broker list:         %s
topic:               %s
transform:           %s
schema registry url: %s
namespace:           %s
`, c.Api, c.Master, c.FrameworkName, c.FrameworkRole, c.User, c.Cpus, c.Mem, c.TcpPort, c.UdpPort,
		c.Executor, c.ProducerProperties, c.NumProducers, c.ChannelSize, c.BrokerList, c.Topic, c.Transform,
		c.SchemaRegistryUrl, c.Namespace)
}
