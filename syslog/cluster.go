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
	"github.com/elodina/go-mesos-utils"
	"github.com/yanzay/log"
	"strings"
	"sync"
)

type Cluster struct {
	frameworkID string
	active      bool
	tasks       map[string]*Task
	taskLock    sync.Mutex
	storage     utils.Storage
}

func NewCluster() *Cluster {
	storage, err := NewStorage(Config.Storage)
	if err != nil {
		panic(err)
	}
	return &Cluster{
		storage: storage,
		tasks:   make(map[string]*Task),
	}
}

func (c *Cluster) Exists(hostname string) bool {
	c.taskLock.Lock()
	defer c.taskLock.Unlock()

	_, exists := c.tasks[hostname]
	return exists
}

func (c *Cluster) Add(task *Task) {
	c.taskLock.Lock()
	defer c.taskLock.Unlock()

	if _, exists := c.tasks[task.Hostname]; exists {
		// this should never happen. would mean a bug if so
		panic(fmt.Sprintf("syslog task on host %s already exists", task.Hostname))
	}

	c.tasks[task.Hostname] = task
}

func (c *Cluster) Remove(hostname string) {
	c.taskLock.Lock()
	defer c.taskLock.Unlock()

	if _, exists := c.tasks[hostname]; !exists {
		// can happen during reconciliation
		log.Warningf("syslog task on host %s does not exist, ignoring", hostname)
	}

	delete(c.tasks, hostname)
}

func (c *Cluster) GetAllTasks() []*Task {
	c.taskLock.Lock()
	defer c.taskLock.Unlock()

	tasks := make([]*Task, 0)
	for _, task := range c.tasks {
		tasks = append(tasks, task)
	}

	return tasks
}

func (c *Cluster) GetTaskIDs() []string {
	c.taskLock.Lock()
	defer c.taskLock.Unlock()

	tasks := make([]string, 0)
	for _, task := range c.tasks {
		tasks = append(tasks, task.TaskID)
	}

	return tasks
}

func (c *Cluster) Save() {
	jsonMap := make(map[string]interface{})
	jsonMap["frameworkID"] = c.frameworkID
	jsonMap["active"] = c.active
	jsonMap["config"] = Config
	jsonMap["tasks"] = c.GetAllTasks()

	js, err := json.MarshalIndent(jsonMap, "", " ")
	if err != nil {
		panic(err)
	}

	c.storage.Save(js)
}

func (c *Cluster) Load() {
	js, err := c.storage.Load()
	if err != nil || js == nil {
		log.Warningf("Could not load cluster state from %s, assuming no cluster state available...", c.storage)
		return
	}

	jsonMap := make(map[string]json.RawMessage)
	err = json.Unmarshal(js, &jsonMap)
	if err != nil {
		panic(err)
	}
	err = json.Unmarshal(jsonMap["frameworkID"], &c.frameworkID)
	if err != nil {
		panic(err)
	}

	err = json.Unmarshal(jsonMap["active"], &c.active)
	if err != nil {
		panic(err)
	}

	err = json.Unmarshal(jsonMap["config"], &Config)
	if err != nil {
		panic(err)
	}

	var tasks []*Task
	err = json.Unmarshal(jsonMap["tasks"], &tasks)
	if err != nil {
		panic(err)
	}
	for _, task := range tasks {
		c.Add(task)
	}
}

func NewStorage(storage string) (utils.Storage, error) {
	storageTokens := strings.SplitN(storage, ":", 2)
	if len(storageTokens) != 2 {
		return nil, fmt.Errorf("Unsupported storage")
	}

	switch storageTokens[0] {
	case "file":
		return utils.NewFileStorage(storageTokens[1]), nil
	case "zk":
		return utils.NewZKStorage(storageTokens[1])
	default:
		return nil, fmt.Errorf("Unsupported storage")
	}
}
