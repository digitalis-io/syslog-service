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
	crand "crypto/rand"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"strings"
	"sync"

	"github.com/elodina/go-mesos-utils"
	"github.com/elodina/go-mesos-utils/pretty"
	"github.com/golang/protobuf/proto"
	mesos "github.com/mesos/mesos-go/mesosproto"
	util "github.com/mesos/mesos-go/mesosutil"
	"github.com/mesos/mesos-go/scheduler"
	"github.com/yanzay/log"
)

var sched *Scheduler // This is needed for HTTP server to be able to update this scheduler

type Scheduler struct {
	httpServer *HttpServer
	cluster    *Cluster
	active     bool
	activeLock sync.Mutex
	driver     scheduler.SchedulerDriver
	labels     string
}

func (s *Scheduler) Start() error {
	log.Infof("Starting scheduler with configuration: \n%s", Config)
	sched = s // set this scheduler reachable for http server

	ctrlc := make(chan os.Signal, 1)
	signal.Notify(ctrlc, os.Interrupt)

	if err := s.resolveDeps(); err != nil {
		return err
	}

	s.cluster = NewCluster()
	s.cluster.Load()

	listenAddr := s.listenAddr()
	s.httpServer = NewHttpServer(listenAddr)
	go s.httpServer.Start()

	s.labels = os.Getenv("STACK_LABELS")

	frameworkInfo := &mesos.FrameworkInfo{
		User:            proto.String(Config.User),
		Name:            proto.String(Config.FrameworkName),
		Role:            proto.String(Config.FrameworkRole),
		FailoverTimeout: proto.Float64(float64(Config.FrameworkTimeout / 1e9)),
		Checkpoint:      proto.Bool(true),
		Labels:          utils.StringToLabels(s.labels),
	}

	if s.cluster.frameworkID != "" {
		frameworkInfo.Id = util.NewFrameworkID(s.cluster.frameworkID)
	}

	driverConfig := scheduler.DriverConfig{
		Scheduler: s,
		Framework: frameworkInfo,
		Master:    Config.Master,
	}

	driver, err := scheduler.NewMesosSchedulerDriver(driverConfig)
	go func() {
		<-ctrlc
		s.Shutdown(driver)
	}()

	if err != nil {
		return fmt.Errorf("Unable to create SchedulerDriver: %s", err)
	}

	if stat, err := driver.Run(); err != nil {
		log.Infof("Framework stopped with status %s and error: %s\n", stat.String(), err)
		return err
	}

	//TODO stop http server

	return nil
}

func (s *Scheduler) SetActive(active bool) {
	s.activeLock.Lock()
	defer s.activeLock.Unlock()

	s.active = active
	if !s.active {
		for _, task := range s.cluster.GetAllTasks() {
			log.Debugf("Killing task %s", task.TaskID)
			_, err := s.driver.KillTask(util.NewTaskID(task.TaskID))
			if err != nil {
				log.Errorf("Failed to kill task %s: %s", task.TaskID, err)
			}
		}
	}
}

func (s *Scheduler) Registered(driver scheduler.SchedulerDriver, id *mesos.FrameworkID, master *mesos.MasterInfo) {
	log.Infof("[Registered] framework: %s master: %s:%d", id.GetValue(), master.GetHostname(), master.GetPort())

	s.cluster.frameworkID = id.GetValue()
	s.cluster.Save()

	s.driver = driver
}

func (s *Scheduler) Reregistered(driver scheduler.SchedulerDriver, master *mesos.MasterInfo) {
	log.Infof("[Reregistered] master: %s:%d", master.GetHostname(), master.GetPort())

	s.driver = driver
}

func (s *Scheduler) Disconnected(scheduler.SchedulerDriver) {
	log.Info("[Disconnected]")

	s.driver = nil
}

func (s *Scheduler) ResourceOffers(driver scheduler.SchedulerDriver, offers []*mesos.Offer) {
	log.Debugf("[ResourceOffers] %s", pretty.Offers(offers))

	s.activeLock.Lock()
	defer s.activeLock.Unlock()

	if !s.active {
		log.Debug("Scheduler is inactive. Declining all offers.")
		for _, offer := range offers {
			_, err := driver.DeclineOffer(offer.GetId(), &mesos.Filters{RefuseSeconds: proto.Float64(1)})
			if err != nil {
				log.Errorf("Failed to decline offer: %s", err)
			}
		}
		s.cluster.Save()
		return
	}

	for _, offer := range offers {
		declineReason := s.acceptOffer(driver, offer)
		if declineReason != "" {
			_, err := driver.DeclineOffer(offer.GetId(), &mesos.Filters{RefuseSeconds: proto.Float64(1)})
			if err != nil {
				log.Errorf("Failed to decline offer: %s", err)
			} else {
				log.Debugf("Declined offer: %s", declineReason)
			}
		}
	}

	s.cluster.Save()
}

func (s *Scheduler) OfferRescinded(driver scheduler.SchedulerDriver, id *mesos.OfferID) {
	log.Infof("[OfferRescinded] %s", id.GetValue())
}

func (s *Scheduler) StatusUpdate(driver scheduler.SchedulerDriver, status *mesos.TaskStatus) {
	log.Infof("[StatusUpdate] %s", pretty.Status(status))

	hostname := s.hostnameFromTaskId(status.GetTaskId().GetValue())

	if status.GetState() == mesos.TaskState_TASK_FAILED || status.GetState() == mesos.TaskState_TASK_KILLED ||
		status.GetState() == mesos.TaskState_TASK_LOST || status.GetState() == mesos.TaskState_TASK_ERROR ||
		status.GetState() == mesos.TaskState_TASK_FINISHED {
		s.cluster.Remove(hostname)
	}

	s.cluster.Save()
}

func (s *Scheduler) FrameworkMessage(driver scheduler.SchedulerDriver, executor *mesos.ExecutorID, slave *mesos.SlaveID, message string) {
	log.Infof("[FrameworkMessage] executor: %s slave: %s message: %s", executor, slave, message)
}

func (s *Scheduler) SlaveLost(driver scheduler.SchedulerDriver, slave *mesos.SlaveID) {
	log.Infof("[SlaveLost] %s", slave.GetValue())
}

func (s *Scheduler) ExecutorLost(driver scheduler.SchedulerDriver, executor *mesos.ExecutorID, slave *mesos.SlaveID, status int) {
	log.Infof("[ExecutorLost] executor: %s slave: %s status: %d", executor, slave, status)
}

func (s *Scheduler) Error(driver scheduler.SchedulerDriver, message string) {
	log.Errorf("[Error] %s", message)

	driver.Abort()
}

func (s *Scheduler) Shutdown(driver *scheduler.MesosSchedulerDriver) {
	log.Info("Shutdown triggered, stopping driver")
	_, err := driver.Stop(false)
	if err != nil {
		panic(err)
	}
}

func (s *Scheduler) acceptOffer(driver scheduler.SchedulerDriver, offer *mesos.Offer) string {
	if s.cluster.Exists(offer.GetHostname()) {
		return fmt.Sprintf("Server on hostname %s is already running.", offer.GetHostname())
	} else {
		declineReason := s.match(offer)
		if declineReason == "" {
			s.launchTask(driver, offer)
		}
		return declineReason
	}
}

func (s *Scheduler) match(offer *mesos.Offer) string {
	if Config.Cpus > getScalarResources(offer, "cpus") {
		return "no cpus"
	}

	if Config.Mem > getScalarResources(offer, "mem") {
		return "no mem"
	}

	tcpPort := s.getPort(Config.TcpPort, offer, -1)
	if tcpPort == -1 {
		return "no suitable port"
	}

	if s.getPort(Config.UdpPort, offer, tcpPort) == -1 {
		return "no suitable port"
	}

	return ""
}

func (s *Scheduler) getPort(targetPort string, offer *mesos.Offer, excludePort int) int {
	ports := getRangeResources(offer, "ports")
	portRanges := make([]*utils.Range, len(ports))
	for idx, rng := range ports {
		portRanges[idx] = utils.NewRange(int(rng.GetBegin()), int(rng.GetEnd()))
	}

	if len(portRanges) == 0 {
		return -1
	}

	if targetPort == "auto" {
		for _, rng := range portRanges {
			for _, rngValue := range rng.Values() {
				if rngValue != excludePort {
					return rngValue
				}
			}
		}
		return -1
	} else {
		rng, err := utils.ParseRange(targetPort)
		if err != nil {
			log.Warning(err)
			return -1
		}

		for _, offerRng := range portRanges {
			overlappingPorts := offerRng.Overlap(rng)
			if overlappingPorts != nil {
				for _, rngValue := range rng.Values() {
					if rngValue != excludePort {
						return rngValue
					}
				}
			}
		}

		return -1
	}
}

func (s *Scheduler) launchTask(driver scheduler.SchedulerDriver, offer *mesos.Offer) {
	taskName := fmt.Sprintf("syslog-%s", offer.GetSlaveId().GetValue())
	taskId := &mesos.TaskID{
		Value: proto.String(fmt.Sprintf("%s-%s", taskName, uuid())),
	}

	data, err := json.Marshal(Config)
	if err != nil {
		panic(err) //shouldn't happen
	}
	log.Debugf("Task data: %s", string(data))

	tcpPort := uint64(s.getPort(Config.TcpPort, offer, -1))
	udpPort := uint64(s.getPort(Config.UdpPort, offer, int(tcpPort)))

	taskInfo := &mesos.TaskInfo{
		Name:     proto.String(taskName),
		TaskId:   taskId,
		SlaveId:  offer.GetSlaveId(),
		Executor: s.createExecutor(offer, tcpPort, udpPort),
		Resources: []*mesos.Resource{
			util.NewScalarResource("cpus", Config.Cpus),
			util.NewScalarResource("mem", Config.Mem),
			util.NewRangesResource("ports", []*mesos.Value_Range{util.NewValueRange(tcpPort, tcpPort)}),
			util.NewRangesResource("ports", []*mesos.Value_Range{util.NewValueRange(udpPort, udpPort)}),
		},
		Data:   data,
		Labels: utils.StringToLabels(s.labels),
	}

	s.cluster.Add(NewTask(offer.GetHostname(), taskInfo))

	_, err = driver.LaunchTasks([]*mesos.OfferID{offer.GetId()}, []*mesos.TaskInfo{taskInfo}, &mesos.Filters{RefuseSeconds: proto.Float64(1)})
	if err != nil {
		log.Errorf("Failed to launch tasks: %s", err)
	}
}

func (s *Scheduler) createExecutor(offer *mesos.Offer, tcpPort uint64, udpPort uint64) *mesos.ExecutorInfo {
	name := fmt.Sprintf("syslog-%s", offer.GetSlaveId().GetValue())
	id := fmt.Sprintf("%s-%s", name, uuid())

	uris := []*mesos.CommandInfo_URI{
		&mesos.CommandInfo_URI{
			Value:      proto.String(fmt.Sprintf("%s/resource/%s", Config.Api, Config.Executor)),
			Executable: proto.Bool(true),
		},
	}

	if Config.ProducerProperties != "" {
		uris = append(uris, &mesos.CommandInfo_URI{
			Value: proto.String(fmt.Sprintf("%s/resource/%s", Config.Api, Config.ProducerProperties)),
		})
	}

	command := fmt.Sprintf("./%s --log-level %s --tcp %d --udp %d --host %s", Config.Executor, log.Level.String(), tcpPort, udpPort, offer.GetHostname())

	return &mesos.ExecutorInfo{
		ExecutorId: util.NewExecutorID(id),
		Name:       proto.String(name),
		Command: &mesos.CommandInfo{
			Value: proto.String(command),
			Uris:  uris,
		},
	}
}

func (s *Scheduler) hostnameFromTaskId(taskId string) string {
	tokens := strings.SplitN(taskId, "-", 2)
	hostname := tokens[len(tokens)-1]
	hostname = hostname[:len(hostname)-37] //strip uuid part
	log.Debugf("Hostname extracted from %s is %s", taskId, hostname)
	return hostname
}

func (s *Scheduler) resolveDeps() error {
	files, _ := ioutil.ReadDir("./")
	for _, file := range files {
		if !file.IsDir() && executorMask.MatchString(file.Name()) {
			Config.Executor = file.Name()
		}
	}

	if Config.Executor == "" {
		return fmt.Errorf("%s not found in current dir", executorMask)
	}

	return nil
}

func (s *Scheduler) listenAddr() string {
	address := Config.Api
	if strings.HasPrefix(address, "http://") {
		address = address[len("http://"):]
	}

	colonIndex := strings.LastIndex(address, ":")
	if colonIndex != -1 {
		address = "0.0.0.0" + address[colonIndex:]
	}

	return address
}

func getScalarResources(offer *mesos.Offer, resourceName string) float64 {
	resources := 0.0
	filteredResources := util.FilterResources(offer.Resources, func(res *mesos.Resource) bool {
		return res.GetName() == resourceName
	})
	for _, res := range filteredResources {
		resources += res.GetScalar().GetValue()
	}
	return resources
}

func getRangeResources(offer *mesos.Offer, resourceName string) []*mesos.Value_Range {
	resources := make([]*mesos.Value_Range, 0)
	filteredResources := util.FilterResources(offer.Resources, func(res *mesos.Resource) bool {
		return res.GetName() == resourceName
	})
	for _, res := range filteredResources {
		resources = append(resources, res.GetRanges().GetRange()...)
	}
	return resources
}

func uuid() string {
	b := make([]byte, 16)
	_, err := crand.Read(b)
	if err != nil {
		panic(err)
	}
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}
