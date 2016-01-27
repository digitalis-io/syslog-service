Syslog-Service
======================
Go based Syslog service that can run within an infrastructure role on Mesos.

Pre-Requisites
==============

- [Golang](http://golang.org/doc/install)
- A standard and working Go workspace setup
- Apache Mesos 0.19 or newer

Build Instructions
=================

- Get the project
```
$ cd $GOPATH/src/
$ mkdir -p github.com/elodina
$ cd github.com/elodina
$ git clone https://github.com/elodina/syslog-service.git
$ cd syslog-service
```

- Build the scheduler and the executor
```
$ go build cli.go
$ go build executor.go
```

Usage
-----

Syslog framework ships with command-line utility to manage schedulers and executors:

    # ./cli help
    Usage:
        help: show this message
        scheduler: configure and start scheduler
        start: start syslog servers
        stop: stop syslog servers
        update: update configuration
        status: get current status of cluster
    More help you can get from ./cli <command> -h


Scheduler Configuration
-----------------------

The scheduler is configured through the command line.

    # ./cli scheduler <options>

Following options are available:

    -master="": Mesos Master addresses.
    -api="": Binding host:port for http/artifact server. Optional if SM_API env is set.
    -user="": Mesos user. Defaults to current system user.
    -log.level="info": Log level. trace|debug|info|warn|error|critical. Defaults to info.
    -framework.name="syslog-kafka": Framework name.
    -framework.role="*": Framework role.

Starting and Stopping Framework
------------------------------

    # ./cli start|stop <options>

Options available:

    -api="": Binding host:port for http/artifact server. Optional if SM_API env is set.

Updating Server Preferences
---------------------------

    # ./cli update <options>

Following options are available:

    -api: Binding host:port for http/artifact server. Optional if SM_API env is set.
    -producer.properties: Producer.properties file name.
	--broker.list: Kafka broker list separated by comma.
	-topic: Topic to produce data to.
	-tcp.port: TCP port range to accept.
	-udp.port: UDP port range to accept.
	-num.producers: Number of producers to launch.
	-channel.size: Producer buffer size.

Quick start:
-----------

```
# export SM_API=http://master:6666
# ./cli scheduler --master master:5050
# ./cli update --broker.list 192.168.3.1:9092 --topic syslog
# ./cli start
```