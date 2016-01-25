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

package main

import (
	"errors"
	"flag"
	"fmt"
	"github.com/elodina/syslog-service/syslog"
	"math"
	"os"
)

func main() {
	if err := exec(); err != nil {
		fmt.Printf("Error: %s\n", err)
		os.Exit(1)
	}
}

func exec() error {
	args := os.Args
	if len(args) == 1 {
		handleHelp()
		return errors.New("No command supplied")
	}

	command := args[1]
	commandArgs := args[1:]
	os.Args = commandArgs

	switch command {
	case "help":
		return handleHelp()
	case "scheduler":
		return handleScheduler()
	case "start", "stop":
		return handleStartStop(command == "start")
	case "update":
		return handleUpdate()
	case "status":
		return handleStatus()
	}

	return fmt.Errorf("Unknown command: %s\n", command)
}

func handleHelp() error {
	fmt.Println(`Usage:
  help: show this message
  scheduler: configure scheduler
  start: start framework
  stop: stop framework
  update: update configuration
  status: get current status of cluster
More help you can get from ./cli <command> -h`)
	return nil
}

func handleStatus() error {
	var api string
	flag.StringVar(&api, "api", "", "Binding host:port for http/artifact server. Optional if SM_API env is set.")

	flag.Parse()

	if err := resolveApi(api); err != nil {
		return err
	}

	response := syslog.NewApiRequest(syslog.Config.Api + "/api/status").Get()
	fmt.Println(response.Message)
	return nil
}

func handleScheduler() error {
	var api string
	var logLevel string

	flag.StringVar(&syslog.Config.Master, "master", "", "Mesos Master addresses.")
	flag.StringVar(&api, "api", "", "Binding host:port for http/artifact server. Optional if SM_API env is set.")
	flag.StringVar(&syslog.Config.User, "user", "", "Mesos user. Defaults to current system user")
	flag.StringVar(&logLevel, "log.level", syslog.Config.LogLevel, "Log level. trace|debug|info|warn|error|critical. Defaults to info.")
	flag.StringVar(&syslog.Config.FrameworkName, "framework.name", syslog.Config.FrameworkName, "Framework name.")
	flag.StringVar(&syslog.Config.FrameworkRole, "framework.role", syslog.Config.FrameworkRole, "Framework role.")
	flag.StringVar(&syslog.Config.Namespace, "namespace", syslog.Config.Namespace, "Namespace.")

	flag.Parse()

	if err := resolveApi(api); err != nil {
		return err
	}

	if err := syslog.InitLogging(logLevel); err != nil {
		return err
	}

	if syslog.Config.Master == "" {
		return errors.New("--master flag is required.")
	}

	return new(syslog.Scheduler).Start()
}

func handleStartStop(start bool) error {
	var api string
	flag.StringVar(&api, "api", "", "Binding host:port for http/artifact server. Optional if SM_API env is set.")

	flag.Parse()

	if err := resolveApi(api); err != nil {
		return err
	}

	apiMethod := "start"
	if !start {
		apiMethod = "stop"
	}

	request := syslog.NewApiRequest(syslog.Config.Api + "/api/" + apiMethod)
	response := request.Get()

	fmt.Println(response.Message)

	return nil
}

func handleUpdate() error {
	var api string
	flag.StringVar(&api, "api", "", "Binding host:port for http/artifact server. Optional if SM_API env is set.")
	flag.StringVar(&syslog.Config.ProducerProperties, "producer.properties", "", "Producer.properties file name.")
	flag.StringVar(&syslog.Config.BrokerList, "broker.list", "", "Kafka broker list separated by comma.")
	flag.StringVar(&syslog.Config.Topic, "topic", "", "Topic to produce data to.")
	flag.StringVar(&syslog.Config.TcpPort, "tcp.port", "", "TCP port range to accept.")
	flag.StringVar(&syslog.Config.UdpPort, "udp.port", "", "UDP port range to accept.")
	flag.StringVar(&syslog.Config.Transform, "transform", "", "Transofmation to apply to each message. none|avro")
	flag.StringVar(&syslog.Config.SchemaRegistryUrl, "schema.registry.url", "", "Avro Schema Registry url for transform=avro")
	flag.IntVar(&syslog.Config.NumProducers, "num.producers", math.MinInt64, "Number of producers to launch.")
	flag.IntVar(&syslog.Config.ChannelSize, "channel.size", math.MinInt64, "Producer buffer size.")

	flag.Parse()

	if err := resolveApi(api); err != nil {
		return err
	}

	request := syslog.NewApiRequest(syslog.Config.Api + "/api/update")
	request.PutString("producer.properties", syslog.Config.ProducerProperties)
	request.PutString("broker.list", syslog.Config.BrokerList)
	request.PutString("topic", syslog.Config.Topic)
	request.PutString("tcp.port", syslog.Config.TcpPort)
	request.PutString("udp.port", syslog.Config.UdpPort)
	request.PutString("transform", syslog.Config.Transform)
	request.PutString("schema.registry.url", syslog.Config.SchemaRegistryUrl)
	request.PutInt("num.producers", int64(syslog.Config.NumProducers))
	request.PutInt("channel.size", int64(syslog.Config.ChannelSize))
	response := request.Get()

	fmt.Println(response.Message)

	return nil
}

func resolveApi(api string) error {
	if api != "" {
		syslog.Config.Api = api
		return nil
	}

	if os.Getenv("SM_API") != "" {
		syslog.Config.Api = os.Getenv("SM_API")
		return nil
	}

	return errors.New("Undefined API url. Please provide either a CLI --api option or SM_API env.")
}
