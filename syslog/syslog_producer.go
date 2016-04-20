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
	"bufio"
	"encoding/json"
	"github.com/elodina/siesta"
	"github.com/elodina/siesta-producer"
	"github.com/elodina/syslog-service/syslog/avro"
	"github.com/yanzay/log"
	"net"
	"strings"
	"time"
	"github.com/elodina/go-syslog-parser"
)

type SyslogMessage struct {
	RawMessage string
	Message    *parser.Message
	Hostname   string
	Timestamp  int64
}

// SyslogProducerConfig defines configuration options for SyslogProducer
type SyslogProducerConfig struct {
	// Syslog producer config.
	ProducerConfig *producer.ProducerConfig

	// Number of producer instances.
	NumProducers int

	// MetadataRoutines is a number of go routines to read record metadata.
	MetadataRoutines int

	Topic string

	// Receive messages from this TCP address and post them to topic.
	TCPAddr string

	// Receive messages from this UDP address and post them to topic.
	UDPAddr string

	// Kafka Broker List host:port,host:port
	BrokerList string

	// Hostname the message came from
	Hostname string

	Namespace string

	Transformer func(message *SyslogMessage, topic string) *producer.ProducerRecord

	KeySerializer producer.Serializer

	ValueSerializer producer.Serializer
}

// Creates an empty SyslogProducerConfig.
func NewSyslogProducerConfig() *SyslogProducerConfig {
	return &SyslogProducerConfig{
		MetadataRoutines: 10,
		Transformer:      simpleTransformFunc,
		KeySerializer:    producer.ByteSerializer,
		ValueSerializer:  producer.StringSerializer,
	}
}

type SyslogProducer struct {
	config        *SyslogProducerConfig
	parser        *parser.Parser
	incoming      chan *SyslogMessage
	metadata      chan (<-chan *producer.RecordMetadata)
	closeChannels []chan bool

	producers []*producer.KafkaProducer
}

func NewSyslogProducer(config *SyslogProducerConfig) *SyslogProducer {
	return &SyslogProducer{
		config:   config,
		parser:   parser.New(parser.TimestampFormats),
		incoming: make(chan *SyslogMessage),
		metadata: make(chan (<-chan *producer.RecordMetadata)),
	}
}

func (this *SyslogProducer) String() string {
	return "syslog-producer"
}

func (this *SyslogProducer) Start() {
	log.Debug("Starting syslog producer")
	this.startTCPServer()
	this.startUDPServer()
	for i := 0; i < this.config.MetadataRoutines; i++ {
		go this.readMetadata()
	}
	this.startProducers()
}

func (this *SyslogProducer) Stop() {
	log.Debug("Stopping syslog producer")

	for _, closeChannel := range this.closeChannels {
		closeChannel <- true
	}
	close(this.incoming)

	for _, producer := range this.producers {
		producer.Close()
	}
}

func (this *SyslogProducer) startTCPServer() {
	log.Debug("Starting TCP server")
	tcpAddr, err := net.ResolveTCPAddr("tcp", this.config.TCPAddr)
	if err != nil {
		panic(err)
	}

	listener, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		panic(err)
	}
	closeChannel := make(chan bool, 1)
	this.closeChannels = append(this.closeChannels, closeChannel)

	go func() {
		for {
			select {
			case <-closeChannel:
				return
			default:
			}
			connection, err := listener.Accept()
			if err != nil {
				return
			}

			this.scan(connection)
		}
	}()
	log.Infof("Listening for messages at TCP %s", this.config.TCPAddr)
}

func (this *SyslogProducer) startUDPServer() {
	log.Debug("Starting UDP server")
	udpAddr, err := net.ResolveUDPAddr("udp", this.config.UDPAddr)
	if err != nil {
		panic(err)
	}

	connection, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		panic(err)
	}
	closeChannel := make(chan bool, 1)
	this.closeChannels = append(this.closeChannels, closeChannel)

	go func() {
		for {
			select {
			case <-closeChannel:
				return
			default:
			}

			this.scan(connection)
		}
	}()
	log.Infof("Listening for messages at UDP %s", this.config.UDPAddr)
}

func (this *SyslogProducer) scan(connection net.Conn) {
	scanner := bufio.NewScanner(connection)
	for scanner.Scan() {
		timestamp := time.Now().UnixNano() / int64(time.Millisecond)
		rawMessage := scanner.Text()
		message, err := this.parser.ParseString(rawMessage)
		if err != nil {
			log.Errorf("Cannot parse syslog message: %s. Original message: %s", err, rawMessage)
			continue
		}
		this.incoming <- &SyslogMessage{rawMessage, message, this.config.Hostname, timestamp}
	}
}

func (this *SyslogProducer) startProducers() {
	brokerList := strings.Split(this.config.BrokerList, ",")
	config := producer.NewProducerConfig()

	connectorConfig := siesta.NewConnectorConfig()
	connectorConfig.BrokerList = brokerList
	connector, err := siesta.NewDefaultConnector(connectorConfig)
	if err != nil {
		panic(err)
	}

	for i := 0; i < this.config.NumProducers; i++ {
		log.Debugf("Starting new producer with config: %#v", config)
		producer := producer.NewKafkaProducer(config, this.config.KeySerializer, this.config.ValueSerializer, connector)
		this.producers = append(this.producers, producer)
		go this.produceRoutine(producer)
	}
}

func (this *SyslogProducer) produceRoutine(producer *producer.KafkaProducer) {
	for msg := range this.incoming {
		this.metadata <- producer.Send(this.config.Transformer(msg, this.config.Topic))
	}
}

func (this *SyslogProducer) readMetadata() {
	for metadataChan := range this.metadata {
		metadata := <-metadataChan
		if metadata.Error != siesta.ErrNoError {
			log.Error(metadata.Error)
		}
	}
}

func simpleTransformFunc(msg *SyslogMessage, topic string) *producer.ProducerRecord {
	return &producer.ProducerRecord{Topic: topic, Key: msg.Message.Tag, Value: msg.RawMessage}
}

func jsonTransformFunc(msg *SyslogMessage, topic string) *producer.ProducerRecord {
	data, err := json.Marshal(msg.Message)
	if err != nil {
		panic(err)
	}
	return &producer.ProducerRecord{Topic: topic, Key: msg.Message.Tag, Value: data}
}

func avroTransformFunc(msg *SyslogMessage, topic string) *producer.ProducerRecord {
	message := avro.NewSyslogMessage()
	message.Priority = int64(msg.Message.Priority)
	message.Severity = int64(msg.Message.Severity)
	message.Facility = int64(msg.Message.Facility)
	message.Timestamp = msg.Message.Timestamp.Format(time.Stamp)
	message.Hostname = msg.Message.Hostname
	if msg.Message.Tag != "" {
		message.Tag = msg.Message.Tag
	}
	if msg.Message.PID != -1 {
		message.Pid = msg.Message.PID
	}
	message.Message = msg.Message.Message
	message.Tags["namespace"] = Config.Namespace
	message.Tags["source_hostname"] = msg.Hostname

	return &producer.ProducerRecord{Topic: topic, Key: msg.Message.Tag, Value: message}
}
